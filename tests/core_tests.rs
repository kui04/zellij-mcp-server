//! Unit and fixture-based tests for the pure domain core, log buffer, state machine, and
//! Zellij 0.46.1 JSON/NDJSON boundary parsers.

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
    EntityLifecycle, ServiceLifecycle, TaskLifecycle, VisualAction,
};
use zellij_mcp_server::logs::{
    BoundedLogBuffer, diff_viewport, filter_and_tail_lines, strip_ansi, truncate_head_tail,
};
use zellij_mcp_server::state::{ManagedRecord, ReconciliationSnapshot, StateStore, ZellijEvent};
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
    let mut store = StateStore::new(SessionName::parse("demo")?);
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    // Register 6 tasks and complete all 6; GC should close only the oldest 1.
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
    let mut store = StateStore::new(SessionName::parse("demo")?);
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
    let mut store = StateStore::new(SessionName::parse("demo")?);
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
