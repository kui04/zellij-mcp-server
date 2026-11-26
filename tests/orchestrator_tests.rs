//! Deterministic orchestration tests using `ZellijClient::new_mocked` (`test-util` feature).

#![cfg(feature = "test-util")]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use zellij_mcp_server::domain::error::DomainError;
use zellij_mcp_server::domain::ids::{
    CommandSpec, CompiledPattern, EnterPolicy, PaneClosePolicy, ReplInput, ReplName, ServiceName,
    SessionName, SessionResolution, TerminalPaneId, WaitMode,
};
use zellij_mcp_server::domain::layout::{LayoutConstraints, PlacementRequest, ResolvedPlacement};
use zellij_mcp_server::runtime::{
    OrchestratorConfig, ReplExecRequest, ReplOpenRequest, ServiceStartRequest, TaskRunOutcome,
    TaskRunRequest, WorkspaceOrchestrator,
};
use zellij_mcp_server::state::ZellijEvent;
use zellij_mcp_server::zellij::cli::ZellijClient;
use zellij_mcp_server::zellij::protocol::SubscribeFrame;

fn make_orchestrator(
    session: &str,
    confirm_destructive: bool,
) -> Result<(
    WorkspaceOrchestrator,
    zellij_mcp_server::zellij::cli::mock::MockZellijCtrl,
)> {
    make_orchestrator_with_keep(session, confirm_destructive, false)
}

fn make_orchestrator_with_keep(
    session: &str,
    confirm_destructive: bool,
    keep_session: bool,
) -> Result<(
    WorkspaceOrchestrator,
    zellij_mcp_server::zellij::cli::mock::MockZellijCtrl,
)> {
    let session_name = SessionName::parse(session)?;
    let (client, ctrl) = ZellijClient::new_mocked(session_name.clone());
    let orch = WorkspaceOrchestrator::new(
        OrchestratorConfig {
            session_resolution: SessionResolution::Explicit(session_name),
            confirm_destructive,
            keep_session,
            default_cwd: PathBuf::from("."),
            layout_constraints: LayoutConstraints::default(),
        },
        client,
    );
    Ok((orch, ctrl))
}

#[tokio::test]
async fn task_run_fast_and_slow_paths_with_task_wait() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-tasks", false)?;
    orch.initialize().await?;

    // Fast path: task exits within wait_timeout.
    ctrl.set_screen_dump(TerminalPaneId::new(1), "lint ok\n0 warnings");
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        orch_bg
            .inject_event(ZellijEvent::PaneExited {
                pane_id: TerminalPaneId::new(1),
                exit_code: Some(0),
            })
            .await;
    });

    let (fast, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["cargo".into(), "clippy".into()])?,
            cwd: None,
            name: Some("lint".into()),
            wait_timeout: Duration::from_millis(500),
            placement: PlacementRequest::Auto,
        })
        .await?;

    match fast {
        TaskRunOutcome::Completed {
            status,
            exit_code,
            output,
            ..
        } => {
            assert_eq!(status, "completed");
            assert_eq!(exit_code, Some(0));
            assert!(output.contains("lint ok"));
        }
        TaskRunOutcome::Running { .. } => panic!("expected fast-path completion"),
    }

    // Slow path: exceeds wait_timeout, then finishes while task_wait blocks.
    ctrl.set_screen_dump(TerminalPaneId::new(2), "tests passed");
    let (slow, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["cargo".into(), "test".into()])?,
            cwd: None,
            name: Some("test".into()),
            wait_timeout: Duration::from_millis(20),
            placement: PlacementRequest::Auto,
        })
        .await?;

    let slow_id = match slow {
        TaskRunOutcome::Running { task_id, .. } => task_id,
        TaskRunOutcome::Completed { .. } => panic!("expected slow-path running"),
    };

    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        orch_bg
            .inject_event(ZellijEvent::PaneExited {
                pane_id: TerminalPaneId::new(2),
                exit_code: Some(0),
            })
            .await;
    });

    let (wait_out, _) = orch
        .task_wait(vec![slow_id], WaitMode::All, Duration::from_millis(500))
        .await?;
    assert!(!wait_out.timed_out);
    assert_eq!(wait_out.tasks[0].exit_code, Some(0));
    assert!(wait_out.tasks[0].output.contains("tests passed"));
    Ok(())
}

#[tokio::test]
async fn service_start_idempotent_readiness_incremental_logs_and_crash_alert() -> Result<()> {
    let (orch, _ctrl) = make_orchestrator("mock-svc", false)?;
    orch.initialize().await?;

    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id: TerminalPaneId::new(1),
                is_initial: false,
                viewport_lines: vec!["booting...".into(), "Local: http://localhost:5173".into()],
            })
            .await;
    });

    let name = ServiceName::parse("web-dev")?;
    let (started, _) = orch
        .service_start(ServiceStartRequest {
            name: name.clone(),
            cmd: CommandSpec::parse(vec!["pnpm".into(), "dev".into()])?,
            cwd: None,
            ready_pattern: Some(CompiledPattern::parse("ready_pattern", "Local: http://")?),
            ready_timeout: Duration::from_millis(500),
            placement: PlacementRequest::Auto,
        })
        .await?;

    assert!(!started.reused);
    assert_eq!(started.status, "ready");
    assert_eq!(
        started.matched_line.as_deref(),
        Some("Local: http://localhost:5173")
    );

    // Calling service_start again with the same name reuses the active pane.
    let (reused, _) = orch
        .service_start(ServiceStartRequest {
            name: name.clone(),
            cmd: CommandSpec::parse(vec!["pnpm".into(), "dev".into()])?,
            cwd: None,
            ready_pattern: None,
            ready_timeout: Duration::from_millis(500),
            placement: PlacementRequest::Auto,
        })
        .await?;
    assert!(reused.reused);
    assert_eq!(reused.pane_id, started.pane_id);

    // Incremental logs via `next_seq` cursor (must not skip the first new line).
    let (slice1, _) = orch.service_logs(&name, 50, None, None).await?;
    assert_eq!(slice1.entries.len(), 2);
    let cursor = slice1.next_seq;

    orch.inject_viewport_frame(SubscribeFrame {
        pane_id: started.pane_id,
        is_initial: false,
        viewport_lines: vec![
            "booting...".into(),
            "Local: http://localhost:5173".into(),
            "GET /api/health 200".into(),
        ],
    })
    .await;

    let (slice2, _) = orch.service_logs(&name, 50, Some(cursor), None).await?;
    assert_eq!(slice2.entries.len(), 1);
    assert_eq!(slice2.entries[0].line, "GET /api/health 200");

    // Unexpected crash surfaces in alerts[] on the next tool call.
    orch.inject_event(ZellijEvent::PaneExited {
        pane_id: started.pane_id,
        exit_code: Some(1),
    })
    .await;

    let (_snap, alerts) = orch.workspace_status().await?;
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].pane_id, started.pane_id);
    Ok(())
}

#[tokio::test]
async fn repl_exec_delta_and_input_required_detection() -> Result<()> {
    let (orch, _ctrl) = make_orchestrator("mock-repl", false)?;
    orch.initialize().await?;

    let name = ReplName::parse("py")?;
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id: TerminalPaneId::new(1),
                is_initial: false,
                viewport_lines: vec!["Python 3.12".into(), ">>> ".into()],
            })
            .await;
    });

    let (opened, _) = orch
        .repl_open(ReplOpenRequest {
            name: name.clone(),
            cmd: CommandSpec::parse(vec!["python3".into()])?,
            cwd: None,
            prompt_pattern: Some(CompiledPattern::parse("prompt_pattern", r"^>>> ")?),
            placement: PlacementRequest::Floating,
        })
        .await?;
    assert_eq!(opened.status, "ready");

    // Execute expression and verify only delta output is returned.
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id: TerminalPaneId::new(1),
                is_initial: false,
                viewport_lines: vec![
                    "Python 3.12".into(),
                    ">>> 2 + 2".into(),
                    "4".into(),
                    ">>> ".into(),
                ],
            })
            .await;
    });

    let (res, _) = orch
        .repl_exec(ReplExecRequest {
            name: name.clone(),
            input: ReplInput::Paste {
                text: "2 + 2".into(),
                enter: EnterPolicy::SendEnter,
            },
            wait_for_prompt: None,
            timeout: Duration::from_millis(500),
        })
        .await?;
    assert!(!res.timed_out);
    assert!(res.delta_output.contains('4'));
    assert!(!res.delta_output.contains("Python 3.12"));

    // Password prompt timeout triggers `input_required`.
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(15)).await;
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id: TerminalPaneId::new(1),
                is_initial: false,
                viewport_lines: vec!["4".into(), ">>> ".into(), "Password: ".into()],
            })
            .await;
    });

    let (auth_res, _) = orch
        .repl_exec(ReplExecRequest {
            name: name.clone(),
            input: ReplInput::Paste {
                text: "sudo ls".into(),
                enter: EnterPolicy::SendEnter,
            },
            wait_for_prompt: None,
            timeout: Duration::from_millis(80),
        })
        .await?;
    assert!(auth_res.timed_out);
    assert_eq!(auth_res.status, "input_required");

    // Once the human enters the password and the prompt returns, REPL recovers to Idle.
    orch.inject_viewport_frame(SubscribeFrame {
        pane_id: TerminalPaneId::new(1),
        is_initial: false,
        viewport_lines: vec!["Password: ".into(), "file.txt".into(), ">>> ".into()],
    })
    .await;

    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id: TerminalPaneId::new(1),
                is_initial: false,
                viewport_lines: vec![
                    "file.txt".into(),
                    ">>> 1+1".into(),
                    "2".into(),
                    ">>> ".into(),
                ],
            })
            .await;
    });
    let (after_recovery, _) = orch
        .repl_exec(ReplExecRequest {
            name: name.clone(),
            input: ReplInput::Paste {
                text: "1+1".into(),
                enter: EnterPolicy::SendEnter,
            },
            wait_for_prompt: None,
            timeout: Duration::from_millis(500),
        })
        .await?;
    assert!(!after_recovery.timed_out);
    assert_eq!(after_recovery.status, "ready");
    assert!(after_recovery.delta_output.contains('2'));
    Ok(())
}

#[tokio::test]
async fn confirm_destructive_blocks_when_user_denies() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-confirm", true)?;
    orch.initialize().await?;
    ctrl.set_confirm_answer(false);

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: None,
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Auto,
        })
        .await?;

    let task_id = match running {
        TaskRunOutcome::Running { task_id, .. } => task_id,
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };

    let err = orch
        .task_cancel(&task_id, PaneClosePolicy::KeepPane)
        .await
        .err();
    let Some(err) = err else {
        panic!("expected UserDeniedConfirmation");
    };
    assert_eq!(
        err.downcast_ref::<DomainError>(),
        Some(&DomainError::UserDeniedConfirmation)
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_kills_only_server_created_sessions_unless_keep_session() -> Result<()> {
    // 1. Server-created session with keep_session=false -> killed on shutdown.
    let (orch_created, ctrl_created) = make_orchestrator_with_keep("created-sess", false, false)?;
    orch_created.initialize().await?;
    orch_created.shutdown().await;
    assert!(
        ctrl_created
            .recorded_actions()
            .iter()
            .any(|a| a == "kill_session")
    );

    // 2. Server-created session with keep_session=true -> NOT killed.
    let (orch_keep, ctrl_keep) = make_orchestrator_with_keep("keep-sess", false, true)?;
    orch_keep.initialize().await?;
    orch_keep.shutdown().await;
    assert!(
        !ctrl_keep
            .recorded_actions()
            .iter()
            .any(|a| a == "kill_session")
    );

    // 3. Reused / inherited session with keep_session=false -> NOT killed.
    let (orch_reused, ctrl_reused) = make_orchestrator_with_keep("reused-sess", false, false)?;
    ctrl_reused.set_session_preexisting(true);
    orch_reused.initialize().await?;
    orch_reused.shutdown().await;
    assert!(
        !ctrl_reused
            .recorded_actions()
            .iter()
            .any(|a| a == "kill_session")
    );
    Ok(())
}

#[tokio::test]
async fn dead_session_errors_propagate_across_all_tools() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-dead", false)?;
    orch.initialize().await?;

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("long".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Auto,
        })
        .await?;
    let task_id = match running {
        TaskRunOutcome::Running { task_id, .. } => task_id,
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };

    let svc_name = ServiceName::parse("svc")?;
    let _ = orch
        .service_start(ServiceStartRequest {
            name: svc_name.clone(),
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            ready_pattern: None,
            ready_timeout: Duration::from_millis(50),
            placement: PlacementRequest::Auto,
        })
        .await?;

    let repl_name = ReplName::parse("py")?;
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id: TerminalPaneId::new(3),
                is_initial: false,
                viewport_lines: vec![">>>".into()],
            })
            .await;
    });
    let _ = orch
        .repl_open(ReplOpenRequest {
            name: repl_name.clone(),
            cmd: CommandSpec::parse(vec!["python3".into()])?,
            cwd: None,
            prompt_pattern: Some(CompiledPattern::parse("prompt_pattern", r"^>>> $")?),
            placement: PlacementRequest::Floating,
        })
        .await?;

    // Externally kill the session.
    ctrl.kill_session_externally();

    let assert_session_not_found = |res: Result<_>| match res {
        Ok(_) => panic!("expected SessionNotFound error"),
        Err(err) => assert!(
            matches!(
                err.downcast_ref::<DomainError>(),
                Some(DomainError::SessionNotFound(_))
            ),
            "expected SessionNotFound, got: {err:?}"
        ),
    };

    assert_session_not_found(orch.workspace_status().await.map(|_| ()));
    assert_session_not_found(
        orch.service_stop(&svc_name, PaneClosePolicy::ClosePane)
            .await
            .map(|_| ()),
    );
    assert_session_not_found(
        orch.task_cancel(&task_id, PaneClosePolicy::KeepPane)
            .await
            .map(|_| ()),
    );
    assert_session_not_found(orch.repl_close(&repl_name).await.map(|_| ()));
    assert_session_not_found(
        orch.repl_exec(ReplExecRequest {
            name: repl_name,
            input: ReplInput::Paste {
                text: "1+1".into(),
                enter: EnterPolicy::SendEnter,
            },
            wait_for_prompt: None,
            timeout: Duration::from_secs(5),
        })
        .await
        .map(|_| ()),
    );
    Ok(())
}

#[tokio::test]
async fn headless_stacked_fallback_and_closed_task_logs_hint() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-stacked", false)?;
    orch.initialize().await?;
    ctrl.set_fail_stacked(true);

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("stacked-job".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Stacked,
        })
        .await?;
    let task_id = match running {
        TaskRunOutcome::Running { task_id, .. } => task_id,
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };

    let _ = orch
        .task_cancel(&task_id, PaneClosePolicy::ClosePane)
        .await?;
    let (logs, _) = orch.task_logs(&task_id, 20, None).await?;
    assert!(logs.pane_closed);
    Ok(())
}

#[tokio::test]
async fn layout_refreshes_and_enforces_min_size_and_no_room_fallback() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-layout", false)?;
    orch.initialize().await?;

    // Start with a 200x50 focused pane: 1st split_right succeeds (200 / 2 = 100 >= 80).
    let (first, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("first".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Auto,
        })
        .await?;
    match first {
        TaskRunOutcome::Running {
            placement,
            layout_hint,
            ..
        } => {
            assert_eq!(placement, ResolvedPlacement::SplitRight);
            assert!(layout_hint.is_none());
        }
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    }

    // 2nd explicit split_right sees focused pane shrunk to 100 cols (100 / 2 = 50 < 80) ->
    // automatically downgrades to background_tab and reports layout_hint.
    let (second, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("second".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::SplitRight,
        })
        .await?;
    match second {
        TaskRunOutcome::Running {
            placement,
            tab_name,
            layout_hint,
            ..
        } => {
            assert_eq!(placement, ResolvedPlacement::BackgroundTab);
            assert_eq!(tab_name.as_deref(), Some("agent:tasks"));
            assert!(
                layout_hint
                    .as_deref()
                    .is_some_and(|h| h.contains("min_pane_cols=80")),
                "expected min_pane_cols downgrade hint, got: {layout_hint:?}"
            );
        }
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    }

    // Even when focused pane appears wide enough, Zellij `No room for a new pane` triggers fallback.
    ctrl.set_focused_pane_geometry(200, 50);
    ctrl.set_fail_no_room(true);
    let (third, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("third".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::SplitRight,
        })
        .await?;
    match third {
        TaskRunOutcome::Running {
            placement,
            layout_hint,
            ..
        } => {
            assert_eq!(placement, ResolvedPlacement::BackgroundTab);
            assert!(
                layout_hint
                    .as_deref()
                    .is_some_and(|h| h.contains("no room")),
                "expected no-room fallback hint, got: {layout_hint:?}"
            );
        }
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    }
    Ok(())
}

#[tokio::test]
async fn failed_task_logs_survive_external_close_and_instant_exit_captures_output() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-backfill", false)?;
    orch.initialize().await?;

    // 1. Failed task whose pane is later closed externally still retains logs.
    ctrl.set_screen_dump(TerminalPaneId::new(1), "boom: fatal error\nline 2");
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        orch_bg
            .inject_event(ZellijEvent::PaneExited {
                pane_id: TerminalPaneId::new(1),
                exit_code: Some(2),
            })
            .await;
    });
    let (failed, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["false".into()])?,
            cwd: None,
            name: Some("fail".into()),
            wait_timeout: Duration::from_millis(300),
            placement: PlacementRequest::Auto,
        })
        .await?;
    let task_id = match failed {
        TaskRunOutcome::Completed {
            task_id,
            status,
            exit_code,
            ..
        } => {
            assert_eq!(status, "failed");
            assert_eq!(exit_code, Some(2));
            task_id
        }
        TaskRunOutcome::Running { .. } => panic!("expected failed completion"),
    };

    // External close clears the live pane, yet `task_logs` still returns backfilled logs.
    orch.inject_event(ZellijEvent::PaneClosed {
        pane_id: TerminalPaneId::new(1),
    })
    .await;
    let (logs, _) = orch.task_logs(&task_id, 50, None).await?;
    assert!(logs.pane_closed);
    assert!(logs.output.contains("boom: fatal error"));

    // 2. REPL that exits immediately surfaces exit_code and dump_screen error output.
    ctrl.set_screen_dump(
        TerminalPaneId::new(2),
        "Command not found: nonexistent_repl",
    );
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        orch_bg
            .inject_event(ZellijEvent::PaneExited {
                pane_id: TerminalPaneId::new(2),
                exit_code: Some(127),
            })
            .await;
    });
    let (repl_out, _) = orch
        .repl_open(ReplOpenRequest {
            name: ReplName::parse("broken")?,
            cmd: CommandSpec::parse(vec!["nonexistent_repl".into()])?,
            cwd: None,
            prompt_pattern: Some(CompiledPattern::parse("prompt_pattern", ">>> ")?),
            placement: PlacementRequest::Floating,
        })
        .await?;
    assert_eq!(repl_out.status, "stopped");
    assert_eq!(repl_out.exit_code, Some(127));
    assert!(repl_out.delta_output.contains("Command not found"));

    // 3. Slow async task exiting via PaneExited and then closed externally before task_logs still
    //    preserves backfilled output.
    ctrl.set_screen_dump(TerminalPaneId::new(3), "slow-bg-line-1\nslow-bg-line-2");
    let (slow, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "1".into()])?,
            cwd: None,
            name: Some("slow-bg".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::BackgroundTab,
        })
        .await?;
    let slow_id = match slow {
        TaskRunOutcome::Running { task_id, .. } => task_id,
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };
    orch.inject_event(ZellijEvent::PaneExited {
        pane_id: TerminalPaneId::new(3),
        exit_code: Some(1),
    })
    .await;
    orch.inject_event(ZellijEvent::PaneClosed {
        pane_id: TerminalPaneId::new(3),
    })
    .await;
    let (slow_logs, _) = orch.task_logs(&slow_id, 20, None).await?;
    assert!(slow_logs.pane_closed);
    assert!(slow_logs.output.contains("slow-bg-line-2"));

    // 4. `validate_cwd` rejects non-existent directories consistently even on mocked backends.
    let Err(cwd_err) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["pwd".into()])?,
            cwd: Some(PathBuf::from("/nope/missing-dir-for-mock-test")),
            name: None,
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Auto,
        })
        .await
    else {
        panic!("expected InvalidCwd");
    };
    assert!(matches!(
        cwd_err.downcast_ref::<DomainError>(),
        Some(DomainError::InvalidCwd { .. })
    ));
    Ok(())
}
