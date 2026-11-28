//! Unit and fixture-based tests for the pure domain core, log buffer, state machine, and
//! Zellij 0.46.1 JSON/NDJSON boundary parsers.

use std::num::{NonZeroI32, NonZeroUsize};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use zellij_mcp_server::domain::error::ParseError;
use zellij_mcp_server::domain::ids::{
    CommandSpec, CompiledPattern, EnterPolicy, KeyChord, LogSeq, ManagedKey, PaneId,
    PaneTitleCodec, ReplInput, ReplName, ServiceName, SessionName, SessionResolution, TabId,
    TaskId, TerminalPaneId,
};
use zellij_mcp_server::domain::layout::{
    EntityKind, FocusedPaneGeometry, LayoutConstraints, LayoutFallbackReason, LayoutRouter,
    LayoutState, LayoutTier, ManagedTabKind, PlacementRequest, ResolvedPlacement, TargetTab,
    ViewportSnapshot,
};
use zellij_mcp_server::domain::lifecycle::{
    EntityLifecycle, ReplLifecycle, ServiceLifecycle, TaskLifecycle, VisualAction,
};
use zellij_mcp_server::logs::{
    BoundedLogBuffer, diff_viewport, filter_and_tail_lines, strip_ansi, truncate_head_tail,
};
use zellij_mcp_server::runtime::{FreshManagedTab, PlaceholderPaneId};
use zellij_mcp_server::state::{
    CleanupExecutionMode, CleanupRequest, CleanupScope, CleanupTarget, EvictablePane,
    EvictionReason, FastTaskClosePolicy, ManagedRecord, ObservedPane, ReconciliationSnapshot,
    RetentionPolicy, RetentionQuota, StateStore, ZellijEvent,
};
use zellij_mcp_server::zellij::protocol::{
    is_active_session_in_list, is_session_not_found_stderr, parse_event_line,
    parse_list_panes_json, parse_list_tabs_json, parse_new_pane_output, parse_new_tab_output,
    parse_subscribe_line, parse_zellij_version,
};

#[test]
fn session_resolution_precedence_and_unique_fallback() -> Result<()> {
    let explicit = SessionResolution::resolve(
        Some("cli-sess".into()),
        Some("env-sess".into()),
        Some("inherited-sess".into()),
        "my-proj",
        0x1234,
    )?;
    assert_eq!(
        explicit,
        SessionResolution::Explicit(SessionName::parse("cli-sess")?)
    );

    let inherited =
        SessionResolution::resolve(None, None, Some("dev-zellij".into()), "my-proj", 0x1234)?;
    assert_eq!(
        inherited,
        SessionResolution::Inherited(SessionName::parse("dev-zellij")?)
    );

    let generated = SessionResolution::resolve(None, None, None, "My Cool Repo!", 0xabcd)?;
    assert_eq!(
        generated,
        SessionResolution::GeneratedUnique(SessionName::parse("mcp-my-cool-repo-abcd")?)
    );

    assert!(SessionName::parse("").is_err());
    assert!(SessionName::parse("bad name").is_err());
    Ok(())
}

#[test]
fn pane_ids_and_command_spec_enforced_at_boundary() -> Result<()> {
    assert_eq!(
        "terminal_7".parse::<PaneId>()?,
        PaneId::Terminal(TerminalPaneId::new(7))
    );
    assert_eq!("plugin_2".parse::<PaneId>()?, PaneId::Plugin(2));
    assert_eq!("7".parse::<TerminalPaneId>()?, TerminalPaneId::new(7));
    assert!("plugin_2".parse::<TerminalPaneId>().is_err());

    assert!(CommandSpec::parse(vec![]).is_err());
    let cmd = CommandSpec::parse(vec!["cargo".into(), "test".into()])?;
    assert_eq!(cmd.to_zellij_argv(), vec!["cargo", "test"]);

    assert_eq!(
        ReplInput::parse(Some("ls".into()), Some(vec!["Enter".into()]), true),
        Err(ParseError::BothReplInputAndKeys)
    );
    assert_eq!(
        ReplInput::parse(Some(String::new()), Some(vec!["Ctrl c".into()]), true),
        Err(ParseError::EmptyInputWithKeys)
    );
    assert_eq!(
        ReplInput::parse(None, None, true),
        Err(ParseError::MissingReplInput)
    );
    assert_eq!(
        ReplInput::parse(Some("ls".into()), None, true)?,
        ReplInput::Paste {
            text: "ls".into(),
            enter: EnterPolicy::SendEnter
        }
    );
    assert_eq!(
        ReplInput::parse(None, Some(vec!["Ctrl c".into()]), false)?,
        ReplInput::SendKeys(vec![KeyChord::parse("Ctrl c")?])
    );

    // Key chord normalization converts common agent notations into Zellij `send-keys` syntax.
    assert_eq!(KeyChord::parse("Ctrl+C")?.as_str(), "Ctrl c");
    assert_eq!(KeyChord::parse("ctrl+d")?.as_str(), "Ctrl d");
    assert_eq!(KeyChord::parse("Ctrl-C")?.as_str(), "Ctrl c");
    assert_eq!(KeyChord::parse("ctrl-d")?.as_str(), "Ctrl d");
    assert_eq!(KeyChord::parse("C-c")?.as_str(), "Ctrl c");
    assert_eq!(KeyChord::parse("S-Tab")?.as_str(), "Shift Tab");
    assert_eq!(KeyChord::parse("s-c")?.as_str(), "Super c");
    assert_eq!(KeyChord::parse("Alt+Enter")?.as_str(), "Alt Enter");
    assert_eq!(KeyChord::parse("Ctrl+Shift+A")?.as_str(), "Ctrl Shift A");
    assert_eq!(KeyChord::parse("return")?.as_str(), "Enter");
    assert_eq!(KeyChord::parse("esc")?.as_str(), "Esc");
    assert_eq!(KeyChord::parse("page up")?.as_str(), "PageUp");
    assert_eq!(KeyChord::parse("Ctrl page down")?.as_str(), "Ctrl PageDown");
    assert_eq!(KeyChord::parse("+")?.as_str(), "+");

    // Prompt matching requires the regex match to reach end-of-line so command echoes never match.
    let py_prompt = CompiledPattern::parse("prompt_pattern", ">>> ")?;
    assert!(py_prompt.matches_terminal_line(">>> "));
    assert!(py_prompt.matches_terminal_line(">>>"));
    assert!(!py_prompt.matches_terminal_line(">>> print('AAA')"));

    let sh_prompt = CompiledPattern::parse("prompt_pattern", r"\$ ")?;
    assert!(sh_prompt.matches_terminal_line("user@host:~/proj$ "));
    assert!(sh_prompt.matches_terminal_line("user@host:~/proj$"));
    assert!(!sh_prompt.matches_terminal_line("user@host:~/proj$ ls -la"));
    Ok(())
}

#[test]
fn pane_title_codec_round_trips_all_entity_kinds() -> Result<()> {
    let cases = [
        (
            "[RUN]",
            ManagedKey::Task(TaskId::parse("t_01")?),
            Some("cargo-test"),
        ),
        (
            "[RDY]",
            ManagedKey::Service(ServiceName::parse("web-dev")?),
            None,
        ),
        (
            "[WAIT]",
            ManagedKey::Repl(ReplName::parse("gdb-main")?),
            Some("debug"),
        ),
    ];

    for (badge, key, label) in cases {
        let formatted = PaneTitleCodec::format(badge, &key, label);
        let Some(parsed) = PaneTitleCodec::parse(&formatted) else {
            panic!("failed to parse formatted title: {formatted}");
        };
        assert_eq!(parsed.badge.as_deref(), Some(badge));
        assert_eq!(parsed.key, key);
        assert_eq!(parsed.label.as_deref(), label);
    }

    assert!(PaneTitleCodec::parse("Pane #1").is_none());
    Ok(())
}

#[test]
fn layout_router_handles_three_width_tiers_and_adaptive_stacking() -> Result<()> {
    let mut layout = LayoutState::new(SessionName::parse("demo")?);
    layout.active_tab_id = Some(TabId::new(0));
    layout.managed_tabs.tasks = Some(TabId::new(1));
    layout.managed_tabs.services = Some(TabId::new(2));

    // Narrow screen (<= 159 cols)
    layout.viewport = Some(ViewportSnapshot::new(159, 40, 1));
    assert_eq!(layout.viewport.map(|v| v.tier), Some(LayoutTier::Narrow));

    let constraints = LayoutConstraints::default();
    let repl_narrow = LayoutRouter::plan(
        &layout,
        EntityKind::Repl,
        PlacementRequest::Auto,
        0,
        0,
        constraints,
    );
    assert_eq!(repl_narrow.placement, ResolvedPlacement::Floating);

    let svc_second = LayoutRouter::plan(
        &layout,
        EntityKind::Service,
        PlacementRequest::Auto,
        0,
        1,
        constraints,
    );
    assert_eq!(svc_second.placement, ResolvedPlacement::BackgroundTab);
    assert!(svc_second.open_stacked);
    assert_eq!(
        svc_second.target_tab,
        TargetTab::ManagedTab {
            kind: ManagedTabKind::Services,
            existing: Some(TabId::new(2))
        }
    );

    // 4th task in agent:tasks triggers swap-to-stacked
    let fourth_task = LayoutRouter::plan(
        &layout,
        EntityKind::Task,
        PlacementRequest::Auto,
        3,
        0,
        constraints,
    );
    assert!(fourth_task.trigger_tab_stack_swap);

    // Wide screen (140..=219 cols)
    layout.viewport = Some(ViewportSnapshot::new(200, 50, 1));
    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 200,
        rows: 50,
    });
    let wide_task = LayoutRouter::plan(
        &layout,
        EntityKind::Task,
        PlacementRequest::Auto,
        0,
        0,
        constraints,
    );
    assert_eq!(wide_task.placement, ResolvedPlacement::SplitRight);
    assert_eq!(wide_task.fallback_reason, None);

    // Focused pane too narrow for another split (100 / 2 = 50 < 80) -> auto-downgrade
    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 100,
        rows: 50,
    });
    let guarded_task = LayoutRouter::plan(
        &layout,
        EntityKind::Task,
        PlacementRequest::SplitRight,
        0,
        0,
        constraints,
    );
    assert_eq!(guarded_task.placement, ResolvedPlacement::BackgroundTab);
    assert!(matches!(
        guarded_task.fallback_reason,
        Some(LayoutFallbackReason::MinWidthExceeded {
            focused_cols: 100,
            min_cols: 80,
            ..
        })
    ));

    let guarded_repl = LayoutRouter::plan(
        &layout,
        EntityKind::Repl,
        PlacementRequest::SplitRight,
        0,
        0,
        constraints,
    );
    assert_eq!(guarded_repl.placement, ResolvedPlacement::Floating);

    // Focused pane too short for vertical split (30 / 2 = 15 < 16) -> auto-downgrade
    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 200,
        rows: 30,
    });
    let guarded_down = LayoutRouter::plan(
        &layout,
        EntityKind::Task,
        PlacementRequest::SplitDown,
        0,
        0,
        constraints,
    );
    assert_eq!(guarded_down.placement, ResolvedPlacement::BackgroundTab);
    assert!(matches!(
        guarded_down.fallback_reason,
        Some(LayoutFallbackReason::MinHeightExceeded {
            focused_rows: 30,
            min_rows: 16,
            ..
        })
    ));

    // Exact boundary: 160 / 2 == 80 (allowed) vs 159 / 2 == 79 (downgraded); 32 / 2 == 16 vs 31 / 2 == 15.
    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 160,
        rows: 32,
    });
    assert_eq!(
        LayoutRouter::plan(
            &layout,
            EntityKind::Task,
            PlacementRequest::SplitRight,
            0,
            0,
            constraints,
        )
        .placement,
        ResolvedPlacement::SplitRight
    );
    assert_eq!(
        LayoutRouter::plan(
            &layout,
            EntityKind::Task,
            PlacementRequest::SplitDown,
            0,
            0,
            constraints,
        )
        .placement,
        ResolvedPlacement::SplitDown
    );

    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 159,
        rows: 31,
    });
    assert_eq!(
        LayoutRouter::plan(
            &layout,
            EntityKind::Task,
            PlacementRequest::SplitRight,
            0,
            0,
            constraints,
        )
        .placement,
        ResolvedPlacement::BackgroundTab
    );
    assert_eq!(
        LayoutRouter::plan(
            &layout,
            EntityKind::Task,
            PlacementRequest::SplitDown,
            0,
            0,
            constraints,
        )
        .placement,
        ResolvedPlacement::BackgroundTab
    );

    // Unknown focused pane geometry skips hard guard (left to Zellij no-room fallback).
    layout.focused_pane = None;
    assert_eq!(
        LayoutRouter::plan(
            &layout,
            EntityKind::Task,
            PlacementRequest::SplitRight,
            0,
            0,
            constraints,
        )
        .placement,
        ResolvedPlacement::SplitRight
    );

    // Zero disables the hard limit.
    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 200,
        rows: 30,
    });
    let unconstrained = LayoutRouter::plan(
        &layout,
        EntityKind::Task,
        PlacementRequest::SplitDown,
        0,
        0,
        LayoutConstraints {
            min_pane_cols: 0,
            min_pane_rows: 0,
        },
    );
    assert_eq!(unconstrained.placement, ResolvedPlacement::SplitDown);

    // Ultrawide screen (>= 220 cols)
    layout.viewport = Some(ViewportSnapshot::new(240, 60, 1));
    layout.focused_pane = Some(FocusedPaneGeometry {
        columns: 240,
        rows: 60,
    });
    let ultra_svc = LayoutRouter::plan(
        &layout,
        EntityKind::Service,
        PlacementRequest::Auto,
        0,
        0,
        constraints,
    );
    assert_eq!(ultra_svc.placement, ResolvedPlacement::Stacked);
    assert_eq!(
        ultra_svc.target_tab,
        TargetTab::ActiveTab(Some(TabId::new(0)))
    );

    // Ultrawide task caps tiled panes at 3 instead of splitting infinitely.
    layout.viewport = Some(ViewportSnapshot::new(240, 60, 3));
    let ultra_task_overflow = LayoutRouter::plan(
        &layout,
        EntityKind::Task,
        PlacementRequest::Auto,
        0,
        0,
        constraints,
    );
    assert_eq!(
        ultra_task_overflow.placement,
        ResolvedPlacement::BackgroundTab
    );
    Ok(())
}

#[test]
fn bounded_log_buffer_and_viewport_diff_and_ansi_strip() -> Result<()> {
    let mut buf = BoundedLogBuffer::with_capacity(3);
    let _s1 = buf.push_line("line 1".into());
    let s2 = buf.push_line("error: boom".into());
    let _s3 = buf.push_line("line 3".into());
    let _s4 = buf.push_line("error: again".into());

    assert_eq!(buf.len(), 3);
    let pat = CompiledPattern::parse("grep", "error:")?;
    let slice = buf.query(Some(s2), 10, Some(&pat));
    assert_eq!(slice.entries.len(), 2);
    assert_eq!(slice.entries[0].seq, s2);
    assert_eq!(slice.dropped_before, None);

    // Passing `next_seq` returns 0 lines now and starts at the very next appended line.
    let empty_next = buf.query(Some(slice.next_seq), 10, None);
    assert!(empty_next.entries.is_empty());
    let _s5 = buf.push_line("after cursor".into());
    let inc = buf.query(Some(slice.next_seq), 10, None);
    assert_eq!(inc.entries.len(), 1);
    assert_eq!(inc.entries[0].line, "after cursor");

    let dropped_slice = buf.query(Some(LogSeq::new(1)), 10, None);
    assert!(dropped_slice.dropped_before.is_some());

    let prev = vec!["a".into(), "b".into(), "".into()];
    let curr = vec!["b".into(), "c".into(), "d".into(), "".into()];
    assert_eq!(diff_viewport(&prev, &curr), vec!["c", "d"]);

    // Simultaneous scroll + in-place last-line rewrite does not duplicate scrolled lines.
    let prev_scroll = vec!["a".into(), "b".into(), "c".into(), ">>> ".into()];
    let curr_scroll = vec!["b".into(), "c".into(), ">>> cmd".into(), "ok".into()];
    assert_eq!(
        diff_viewport(&prev_scroll, &curr_scroll),
        vec![">>> cmd", "ok"]
    );

    // A single-line prompt `prev` must NOT match the newly rendered prompt at the bottom of `curr`.
    let single_prompt = vec!["sh-5.2$".into()];
    let after_cmd = vec![
        "sh-5.2$ echo E2E_REPL_42".into(),
        "E2E_REPL_42".into(),
        "sh-5.2$".into(),
    ];
    assert_eq!(diff_viewport(&single_prompt, &after_cmd), after_cmd);

    assert_eq!(strip_ansi("\x1b[31mred\x1b[0m\x1b(B text"), "red text");
    assert_eq!(
        filter_and_tail_lines(
            "\x1b[32mok\x1b[0m\nerr 1\nerr 2",
            1,
            Some(&CompiledPattern::parse("g", "err")?)
        ),
        vec!["err 2"]
    );

    let many: Vec<String> = (1..=10).map(|i| format!("l{i}")).collect();
    assert_eq!(
        truncate_head_tail(&many, 2, 2),
        "l1\nl2\n... (6 lines omitted) ...\nl9\nl10"
    );
    Ok(())
}

#[test]
fn state_store_transitions_gc_and_reconciliation() -> Result<()> {
    let mut store = StateStore::new(
        SessionName::parse("demo")?,
        RetentionPolicy {
            completed_tasks: RetentionQuota::KeepLatest(
                NonZeroUsize::new(5).unwrap_or(NonZeroUsize::MIN),
            ),
            ..RetentionPolicy::default()
        },
    );
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    // Register 6 tasks and complete all 6; the keep-5 quota closes only the oldest 1.
    for i in 0..6u32 {
        let task_id = store.allocate_task_id();
        let pane_id = TerminalPaneId::new(10 + i);
        let key = ManagedKey::Task(task_id);
        let title = PaneTitleCodec::format("[RUN]", &key, Some("job"));
        store.register(ManagedRecord {
            key,
            tab_id: TabId::new(1),
            pane_id,
            pane_title: title,
            label: Some("job".into()),
            placement: ResolvedPlacement::BackgroundTab,
            cmd: CommandSpec::parse(vec!["echo".into()])?,
            cwd: PathBuf::from("."),
            ready_pattern: None,
            prompt_pattern: None,
            lifecycle: EntityLifecycle::Task(TaskLifecycle::Running { started_at: t0 }),
            logs: BoundedLogBuffer::default(),
            last_viewport: Vec::new(),
        });

        let effects = store.apply_event(
            ZellijEvent::PaneExited {
                pane_id,
                exit_code: Some(0),
            },
            t0 + Duration::from_secs(u64::from(i) + 1),
        );
        if i == 5 {
            assert!(effects.visual_actions.contains(&VisualAction::ClosePane {
                pane_id: TerminalPaneId::new(10)
            }));
        }
    }

    // Service readiness via viewport frame + crash alert
    let svc_name = ServiceName::parse("web-dev")?;
    let svc_pane = TerminalPaneId::new(50);
    let svc_key = ManagedKey::Service(svc_name.clone());
    store.register(ManagedRecord {
        key: svc_key.clone(),
        tab_id: TabId::new(2),
        pane_id: svc_pane,
        pane_title: PaneTitleCodec::format("[RUN]", &svc_key, None),
        label: None,
        placement: ResolvedPlacement::BackgroundTab,
        cmd: CommandSpec::parse(vec!["pnpm".into(), "dev".into()])?,
        cwd: PathBuf::from("."),
        ready_pattern: None,
        prompt_pattern: None,
        lifecycle: EntityLifecycle::Service(ServiceLifecycle::Starting {
            started_at: t0,
            ready_pattern: Some(CompiledPattern::parse("ready_pattern", "Local: http://")?),
        }),
        logs: BoundedLogBuffer::default(),
        last_viewport: Vec::new(),
    });

    let ready_fx = store.apply_viewport_frame(
        svc_pane,
        true,
        vec!["starting...".into(), "Local: http://localhost:3000".into()],
        t0 + Duration::from_secs(2),
    );
    assert_eq!(ready_fx.changed_keys, vec![svc_key.clone()]);
    assert_eq!(
        store
            .get_service(&svc_name)
            .map(|r| r.lifecycle.status_label()),
        Some("ready")
    );

    // Reconnect `is_initial: true` frame resets baseline without replaying prior lines.
    let before_reconnect_len = store.get_service(&svc_name).map_or(0, |r| r.logs.len());
    let _ = store.apply_viewport_frame(
        svc_pane,
        true,
        vec!["starting...".into(), "Local: http://localhost:3000".into()],
        t0 + Duration::from_secs(3),
    );
    assert_eq!(
        store.get_service(&svc_name).map_or(0, |r| r.logs.len()),
        before_reconnect_len
    );

    // `prune_closed_history` evicts only oldest closed terminal records while keeping open panes.
    store.prune_closed_history(0);
    assert!(
        store
            .get(&ManagedKey::Task(TaskId::parse("t_01")?))
            .is_none()
    );
    assert!(
        store
            .get(&ManagedKey::Task(TaskId::parse("t_02")?))
            .is_some()
    );

    let crash_fx = store.apply_event(
        ZellijEvent::PaneExited {
            pane_id: svc_pane,
            exit_code: Some(137),
        },
        t0 + Duration::from_secs(5),
    );
    assert_eq!(crash_fx.alerts.len(), 1);
    assert!(
        crash_fx
            .visual_actions
            .contains(&VisualAction::MarkAttention { pane_id: svc_pane })
    );
    assert_eq!(store.drain_alerts().len(), 1);
    assert!(store.drain_alerts().is_empty());
    Ok(())
}

#[test]
fn zellij_0_46_1_fixtures_parse_and_reconcile() -> Result<()> {
    assert_eq!(parse_zellij_version("zellij 0.46.1\n")?, (0, 46, 1));
    assert_eq!(parse_new_tab_output("3\n")?, TabId::new(3));
    assert_eq!(
        parse_new_pane_output("terminal_12\n")?,
        TerminalPaneId::new(12)
    );

    let tabs = parse_list_tabs_json(include_str!("fixtures/list_tabs.json"))?;
    assert_eq!(tabs.len(), 2);
    assert_eq!(tabs[0].viewport.tier, LayoutTier::Wide);
    assert_eq!(tabs[1].name, "agent:services");

    let panes = parse_list_panes_json(include_str!("fixtures/list_panes.json"))?;
    // Plugin pane `tab-bar` is filtered out, leaving 2 terminal panes.
    assert_eq!(panes.len(), 2);
    assert_eq!(panes[1].pane_id, TerminalPaneId::new(1));
    assert!(panes[1].exited);
    assert_eq!(panes[1].exit_status, Some(0));

    let events: Vec<ZellijEvent> = include_str!("fixtures/events.ndjson")
        .lines()
        .filter_map(|line| parse_event_line(line).transpose())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(events.len(), 5);
    assert_eq!(
        events[0],
        ZellijEvent::PaneExited {
            pane_id: TerminalPaneId::new(1),
            exit_code: Some(0)
        }
    );

    let frames: Vec<_> = include_str!("fixtures/subscribe.ndjson")
        .lines()
        .filter_map(|line| parse_subscribe_line(line).transpose())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(frames.len(), 2);
    assert!(frames[0].is_initial);
    assert!(!frames[1].is_initial);
    assert_eq!(frames[1].viewport_lines[1], "probe-line-2");

    // Cold-start reconciliation claims `[RUN] mcp:task:t_01:probe` as completed.
    let mut store = StateStore::new(SessionName::parse("demo")?, RetentionPolicy::default());
    store.reconcile(
        ReconciliationSnapshot {
            tabs,
            panes,
            active_tab: None,
        },
        SystemTime::now(),
    );
    assert_eq!(store.layout.managed_tabs.services, Some(TabId::new(3)));
    let claimed = store.get(&ManagedKey::Task(TaskId::parse("t_01")?));
    assert_eq!(
        claimed.map(|r| r.lifecycle.status_label()),
        Some("completed")
    );
    Ok(())
}

#[test]
fn regression_dead_session_detection_prompt_whitespace_tail_lines_and_stopping_service()
-> Result<()> {
    // Dead-session stderr & active session list parsing (with ANSI codes & EXITED sessions).
    assert!(is_session_not_found_stderr(
        "Session 'ghost' not found. The following sessions are active:\n"
    ));
    assert!(is_session_not_found_stderr(
        "No active zellij sessions found.\n"
    ));
    assert!(!is_session_not_found_stderr(""));

    let list_out = "\x1b[32;1mdev\x1b[m [Created 5m ago]\nold [Created 1h ago] (EXITED - attach to resurrect)\n";
    assert!(is_active_session_in_list(list_out, "dev"));
    assert!(!is_active_session_in_list(list_out, "old"));
    assert!(!is_active_session_in_list(list_out, "missing"));

    // Trailing-whitespace prompt regex matches Zellij-trimmed terminal lines.
    let py_prompt = CompiledPattern::parse("prompt_pattern", r"^>>> $")?;
    assert!(py_prompt.matches_terminal_line(">>>"));
    assert!(py_prompt.matches_terminal_line(">>> "));
    assert!(!py_prompt.matches_terminal_line(">>> 2 + 2"));

    // `filter_and_tail_lines` ignores trailing blank cursor row from `dump-screen --full`.
    let dumped = "L1\nL2\nL3\n\n";
    assert_eq!(filter_and_tail_lines(dumped, 2, None), vec!["L2", "L3"]);

    // Stopping a service intentionally does not produce a crash alert when `PaneExited` arrives.
    let mut store = StateStore::new(SessionName::parse("demo")?, RetentionPolicy::default());
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let svc_name = ServiceName::parse("api")?;
    let svc_pane = TerminalPaneId::new(99);
    let svc_key = ManagedKey::Service(svc_name.clone());
    store.register(ManagedRecord {
        key: svc_key,
        tab_id: TabId::new(1),
        pane_id: svc_pane,
        pane_title: "[STOP] mcp:svc:api".into(),
        label: None,
        placement: ResolvedPlacement::BackgroundTab,
        cmd: CommandSpec::parse(vec!["node".into()])?,
        cwd: PathBuf::from("."),
        ready_pattern: None,
        prompt_pattern: None,
        lifecycle: EntityLifecycle::Service(ServiceLifecycle::Stopping { started_at: t0 }),
        logs: BoundedLogBuffer::default(),
        last_viewport: Vec::new(),
    });
    let stop_fx = store.apply_event(
        ZellijEvent::PaneExited {
            pane_id: svc_pane,
            exit_code: Some(130),
        },
        t0 + Duration::from_secs(1),
    );
    assert!(stop_fx.alerts.is_empty());
    assert!(store.drain_alerts().is_empty());
    assert_eq!(
        store
            .get_service(&svc_name)
            .map(|r| r.lifecycle.status_label()),
        Some("stopped")
    );
    Ok(())
}

#[test]
fn placeholder_panes_and_cleanup_receipt_enforce_spawn_ordering() -> Result<()> {
    let fresh_tab = TabId::new(4);
    let panes = vec![
        obs_pane(TerminalPaneId::new(10), fresh_tab, "Shell", false),
        obs_pane(
            TerminalPaneId::new(11),
            fresh_tab,
            "[RUN] mcp:task:t_01:job",
            false,
        ),
        obs_pane(TerminalPaneId::new(12), fresh_tab, "floating-shell", true),
        obs_pane(
            TerminalPaneId::new(13),
            TabId::new(5),
            "other-tab-shell",
            false,
        ),
    ];

    // Only untitled tiled panes of the fresh tab are placeholders.
    let placeholders = PlaceholderPaneId::collect_from_fresh_tab(fresh_tab, &panes);
    assert_eq!(placeholders.len(), 1);
    assert_eq!(
        placeholders[0].as_terminal_pane_id(),
        TerminalPaneId::new(10)
    );

    // A spawn redirected to another tab yields no receipt, so placeholders are never closed.
    let fresh = FreshManagedTab::new(fresh_tab, placeholders);
    assert!(
        fresh
            .into_cleanup_receipt(Some(TabId::new(5)), TerminalPaneId::new(20))
            .is_none()
    );

    // A spawn without a target tab cannot prove it landed in the fresh tab either.
    let placeholders = PlaceholderPaneId::collect_from_fresh_tab(fresh_tab, &panes);
    let fresh = FreshManagedTab::new(fresh_tab, placeholders);
    assert!(
        fresh
            .into_cleanup_receipt(None, TerminalPaneId::new(20))
            .is_none()
    );

    // A managed pane in the fresh tab yields the receipt for the placeholder only.
    let placeholders = PlaceholderPaneId::collect_from_fresh_tab(fresh_tab, &panes);
    let receipt = FreshManagedTab::new(fresh_tab, placeholders)
        .into_cleanup_receipt(Some(fresh_tab), TerminalPaneId::new(20));
    let Some(receipt) = receipt else {
        panic!("expected cleanup receipt");
    };
    assert_eq!(receipt.tab_id(), fresh_tab);
    assert_eq!(receipt.spawned_pane(), TerminalPaneId::new(20));
    assert_eq!(receipt.placeholders().len(), 1);

    // Defensive filter: a spawned pane that is also listed as a placeholder is never closed.
    let placeholders = PlaceholderPaneId::collect_from_fresh_tab(fresh_tab, &panes);
    let receipt = FreshManagedTab::new(fresh_tab, placeholders)
        .into_cleanup_receipt(Some(fresh_tab), TerminalPaneId::new(10));
    let Some(receipt) = receipt else {
        panic!("expected cleanup receipt");
    };
    assert!(receipt.placeholders().is_empty());
    Ok(())
}

/// Builds an observed pane for placeholder filtering tests.
fn obs_pane(
    pane_id: TerminalPaneId,
    tab_id: TabId,
    title: &str,
    is_floating: bool,
) -> ObservedPane {
    ObservedPane {
        pane_id,
        tab_id,
        title: title.to_owned(),
        is_focused: false,
        is_floating,
        columns: 80,
        rows: 24,
        exited: false,
        exit_status: None,
        cwd: None,
        command: None,
    }
}

#[test]
fn retention_quota_parses_zero_counts_and_unlimited() -> Result<()> {
    assert_eq!(
        RetentionQuota::parse("0")?,
        RetentionQuota::CloseImmediately
    );
    assert_eq!(
        RetentionQuota::parse(" 3 ")?,
        RetentionQuota::KeepLatest(NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN))
    );
    assert_eq!(
        RetentionQuota::parse("unlimited")?,
        RetentionQuota::Unlimited
    );
    assert_eq!(
        RetentionQuota::parse("Unlimited")?,
        RetentionQuota::Unlimited
    );
    assert!(RetentionQuota::parse("-1").is_err());
    assert!(RetentionQuota::parse("2.5").is_err());
    assert!(RetentionQuota::parse("many").is_err());
    Ok(())
}

#[test]
fn retention_policy_evicts_oldest_finished_panes_per_class() -> Result<()> {
    let session = SessionName::parse("retention")?;
    let policy = RetentionPolicy {
        completed_tasks: RetentionQuota::keep(1),
        failed_and_cancelled_tasks: RetentionQuota::keep(2),
        exited_repls: RetentionQuota::CloseImmediately,
        fast_task_close: FastTaskClosePolicy::FollowCompletedQuota,
    };
    let mut store = StateStore::new(session, policy);
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let at = |secs: u64| t0 + Duration::from_secs(secs);

    // Completed tasks keep only the newest one.
    for (seq, pane) in [(1u64, 20u32), (2, 21), (3, 22)] {
        let task_id = store.allocate_task_id();
        let key = ManagedKey::Task(task_id);
        store.register(managed_record(
            key,
            TerminalPaneId::new(pane),
            TabId::new(1),
            completed(at(seq * 10), at(seq * 10 + 1), false),
        )?);
    }
    let evicted = store.enforce_retention(EvictionReason::RetentionGc);
    assert_eq!(evicted.closed_panes.len(), 2);
    assert_eq!(
        evicted.visual_actions,
        vec![
            VisualAction::ClosePane {
                pane_id: TerminalPaneId::new(20)
            },
            VisualAction::ClosePane {
                pane_id: TerminalPaneId::new(21)
            },
        ]
    );

    // Failed and cancelled tasks share one quota of two, newest kept.
    for (pane, lifecycle) in [
        (
            30u32,
            EntityLifecycle::Task(TaskLifecycle::Failed {
                started_at: at(1),
                ended_at: at(2),
                exit_code: NonZeroI32::new(1).unwrap_or(NonZeroI32::MIN),
                attention_marked: true,
                pane_closed: false,
            }),
        ),
        (
            31,
            EntityLifecycle::Task(TaskLifecycle::Cancelled {
                started_at: at(3),
                ended_at: at(4),
                exit_code: None,
                pane_closed: false,
            }),
        ),
        (
            32,
            EntityLifecycle::Task(TaskLifecycle::Failed {
                started_at: at(5),
                ended_at: at(6),
                exit_code: NonZeroI32::new(2).unwrap_or(NonZeroI32::MIN),
                attention_marked: true,
                pane_closed: false,
            }),
        ),
    ] {
        let task_id = store.allocate_task_id();
        store.register(managed_record(
            ManagedKey::Task(task_id),
            TerminalPaneId::new(pane),
            TabId::new(1),
            lifecycle,
        )?);
    }
    let evicted = store.enforce_retention(EvictionReason::RetentionGc);
    assert_eq!(evicted.closed_panes, vec![TerminalPaneId::new(30)]);

    // Exited REPLs default to closing immediately.
    let repl = ReplName::parse("py")?;
    store.register(managed_record(
        ManagedKey::Repl(repl),
        TerminalPaneId::new(40),
        TabId::new(2),
        EntityLifecycle::Repl(ReplLifecycle::Exited {
            started_at: at(7),
            ended_at: at(8),
            exit_code: Some(0),
            pane_closed: false,
        }),
    )?);
    let evicted = store.enforce_retention(EvictionReason::RetentionGc);
    assert_eq!(evicted.closed_panes, vec![TerminalPaneId::new(40)]);

    // A second sweep is a no-op: already closed panes are never selected twice.
    let again = store.enforce_retention(EvictionReason::RetentionGc);
    assert!(again.visual_actions.is_empty());
    Ok(())
}

#[test]
fn cleanup_request_rejects_empty_scopes_and_selects_only_finished_panes() -> Result<()> {
    let mut store = StateStore::new(
        SessionName::parse("cleanup")?,
        RetentionPolicy {
            completed_tasks: RetentionQuota::Unlimited,
            failed_and_cancelled_tasks: RetentionQuota::Unlimited,
            exited_repls: RetentionQuota::Unlimited,
            fast_task_close: FastTaskClosePolicy::FollowCompletedQuota,
        },
    );
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let at = |secs: u64| t0 + Duration::from_secs(secs);

    assert!(matches!(
        CleanupRequest::parse(
            Vec::new(),
            CleanupTarget::EntireSession,
            CleanupExecutionMode::DryRun
        ),
        Err(ParseError::Empty { field: "scopes" })
    ));

    let running = ManagedKey::Task(TaskId::parse("t_01")?);
    store.register(managed_record(
        running.clone(),
        TerminalPaneId::new(10),
        TabId::new(1),
        EntityLifecycle::Task(TaskLifecycle::Running { started_at: at(1) }),
    )?);
    let done = ManagedKey::Task(TaskId::parse("t_02")?);
    store.register(managed_record(
        done.clone(),
        TerminalPaneId::new(11),
        TabId::new(1),
        completed(at(2), at(3), false),
    )?);
    let cancelled = ManagedKey::Task(TaskId::parse("t_03")?);
    store.register(managed_record(
        cancelled.clone(),
        TerminalPaneId::new(12),
        TabId::new(1),
        EntityLifecycle::Task(TaskLifecycle::Cancelled {
            started_at: at(4),
            ended_at: at(5),
            exit_code: None,
            pane_closed: false,
        }),
    )?);
    let repl = ManagedKey::Repl(ReplName::parse("py")?);
    store.register(managed_record(
        repl.clone(),
        TerminalPaneId::new(13),
        TabId::new(2),
        EntityLifecycle::Repl(ReplLifecycle::Exited {
            started_at: at(6),
            ended_at: at(7),
            exit_code: Some(0),
            pane_closed: false,
        }),
    )?);
    let closed = ManagedKey::Task(TaskId::parse("t_04")?);
    store.register(managed_record(
        closed,
        TerminalPaneId::new(14),
        TabId::new(1),
        completed(at(8), at(9), true),
    )?);

    let task_scopes = CleanupRequest::parse(
        vec![CleanupScope::CompletedTasks, CleanupScope::CancelledTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::DryRun,
    )?;
    let selected = store.select_panes_for_cleanup(&task_scopes);
    assert_eq!(
        selected
            .iter()
            .map(EvictablePane::pane_id)
            .collect::<Vec<_>>(),
        vec![TerminalPaneId::new(11), TerminalPaneId::new(12)]
    );
    assert!(
        selected
            .iter()
            .all(|pane| pane.reason() == EvictionReason::ExplicitCleanup)
    );
    // The running task of the same kind is reported as deliberately skipped.
    assert_eq!(store.count_skipped_active(&task_scopes), 1);

    // A tab filter keeps panes of other tabs out of the selection.
    let tab_scoped = CleanupRequest::parse(
        vec![CleanupScope::ExitedRepls],
        CleanupTarget::SingleTab(TabId::new(1)),
        CleanupExecutionMode::DryRun,
    )?;
    assert!(store.select_panes_for_cleanup(&tab_scoped).is_empty());
    let session_scoped = CleanupRequest::parse(
        vec![CleanupScope::ExitedRepls],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::DryRun,
    )?;
    assert_eq!(store.select_panes_for_cleanup(&session_scoped).len(), 1);

    // Executing marks panes closed and releases their close effects exactly once.
    let effects = store.mark_evicted(&selected);
    assert_eq!(effects.closed_panes.len(), 2);
    assert!(store.select_panes_for_cleanup(&task_scopes).is_empty());
    // A stale selection never closes a record whose pane id changed after selection.
    let stale = selected.clone();
    store.register(managed_record(
        done,
        TerminalPaneId::new(30),
        TabId::new(1),
        completed(at(12), at(13), false),
    )?);
    let effects = store.mark_evicted(&stale);
    assert!(effects.visual_actions.is_empty());
    assert_eq!(store.select_panes_for_cleanup(&task_scopes).len(), 1);
    // The re-registered task and the untouched exited REPL are still open.
    assert_eq!(store.count_terminated_open_panes(), 2);
    Ok(())
}

/// Builds a managed record for retention and cleanup tests.
fn managed_record(
    key: ManagedKey,
    pane_id: TerminalPaneId,
    tab_id: TabId,
    lifecycle: EntityLifecycle,
) -> Result<ManagedRecord> {
    Ok(ManagedRecord {
        key,
        tab_id,
        pane_id,
        pane_title: String::new(),
        label: None,
        placement: ResolvedPlacement::BackgroundTab,
        cmd: CommandSpec::parse(vec!["echo".into()])?,
        cwd: PathBuf::from("."),
        ready_pattern: None,
        prompt_pattern: None,
        lifecycle,
        logs: BoundedLogBuffer::default(),
        last_viewport: Vec::new(),
    })
}

fn completed(started_at: SystemTime, ended_at: SystemTime, pane_closed: bool) -> EntityLifecycle {
    EntityLifecycle::Task(TaskLifecycle::Completed {
        started_at,
        ended_at,
        pane_closed,
    })
}
