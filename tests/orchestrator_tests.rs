//! Deterministic orchestration tests using `ZellijClient::new_mocked` (`test-util` feature).

#![cfg(feature = "test-util")]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use zellij_mcp_server::domain::error::DomainError;
use zellij_mcp_server::domain::ids::{
    CommandSpec, CompiledPattern, EnterPolicy, ManagedKey, PaneClosePolicy, ReplInput, ReplName,
    ServiceName, SessionName, SessionResolution, TabId, TaskId, TerminalPaneId, WaitMode,
};
use zellij_mcp_server::domain::layout::{LayoutConstraints, PlacementRequest, ResolvedPlacement};
use zellij_mcp_server::runtime::{
    CleanupOutcome, OrchestratorConfig, ReplExecRequest, ReplOpenRequest, ServiceStartRequest,
    TaskRunOutcome, TaskRunRequest, WorkspaceOrchestrator,
};
use zellij_mcp_server::state::{
    CleanupExecutionMode, CleanupRequest, CleanupScope, CleanupTarget, FastTaskClosePolicy,
    RetentionPolicy, RetentionQuota, ZellijEvent,
};
use zellij_mcp_server::zellij::cli::ZellijClient;
use zellij_mcp_server::zellij::cli::mock::ClosePaneFailure;
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

fn make_orchestrator_with_retention(
    session: &str,
    retention: RetentionPolicy,
) -> Result<(
    WorkspaceOrchestrator,
    zellij_mcp_server::zellij::cli::mock::MockZellijCtrl,
)> {
    let session_name = SessionName::parse(session)?;
    let (client, ctrl) = ZellijClient::new_mocked(session_name.clone());
    let orch = WorkspaceOrchestrator::new(
        OrchestratorConfig {
            session_resolution: SessionResolution::Explicit(session_name),
            confirm_destructive: false,
            keep_session: false,
            default_cwd: PathBuf::from("."),
            layout_constraints: LayoutConstraints::default(),
            retention,
        },
        client,
    );
    Ok((orch, ctrl))
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
            retention: RetentionPolicy::default(),
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
    let (slow, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["cargo".into(), "test".into()])?,
            cwd: None,
            name: Some("test".into()),
            wait_timeout: Duration::from_millis(20),
            placement: PlacementRequest::Auto,
        })
        .await?;

    let (slow_id, slow_pane) = match slow {
        TaskRunOutcome::Running {
            task_id, pane_id, ..
        } => (task_id, pane_id),
        TaskRunOutcome::Completed { .. } => panic!("expected slow-path running"),
    };

    ctrl.set_screen_dump(slow_pane, "tests passed");
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        orch_bg
            .inject_event(ZellijEvent::PaneExited {
                pane_id: slow_pane,
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
    let (orch, ctrl) = make_orchestrator("mock-svc", false)?;
    orch.initialize().await?;

    let bg_ctrl = ctrl.clone();
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let Some(pane_id) = bg_ctrl.find_pane_id_by_title("mcp:svc:web-dev") else {
            return;
        };
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id,
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
    let bg_ctrl = ctrl.clone();
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let Some(pane_id) = bg_ctrl.find_pane_id_by_title("mcp:repl:py") else {
            return;
        };
        orch_bg
            .inject_viewport_frame(SubscribeFrame {
                pane_id,
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
    let (slow, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "1".into()])?,
            cwd: None,
            name: Some("slow-bg".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::BackgroundTab,
        })
        .await?;
    let (slow_id, slow_pane) = match slow {
        TaskRunOutcome::Running {
            task_id, pane_id, ..
        } => (task_id, pane_id),
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };
    ctrl.set_screen_dump(slow_pane, "slow-bg-line-1\nslow-bg-line-2");
    orch.inject_event(ZellijEvent::PaneExited {
        pane_id: slow_pane,
        exit_code: Some(1),
    })
    .await;
    orch.inject_event(ZellijEvent::PaneClosed { pane_id: slow_pane })
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

#[tokio::test]
async fn background_task_replaces_default_shell_pane_with_managed_pane() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-placeholder", false)?;
    orch.initialize().await?;

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("bg".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::BackgroundTab,
        })
        .await?;

    let (pane_id, tab_id) = match running {
        TaskRunOutcome::Running {
            pane_id,
            tab_id,
            tab_name,
            ..
        } => {
            assert_eq!(tab_name.as_deref(), Some("agent:tasks"));
            (pane_id, tab_id)
        }
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };

    let panes = ctrl.panes_in_tab(tab_id);
    assert_eq!(
        panes.len(),
        1,
        "fresh managed tab must hold only the managed pane, got: {panes:?}"
    );
    assert_eq!(panes[0].pane_id, pane_id);
    assert!(
        ctrl.recorded_actions()
            .iter()
            .any(|action| action.starts_with("close_pane:")),
        "expected the default shell placeholder to be closed, got: {:?}",
        ctrl.recorded_actions()
    );
    Ok(())
}

#[tokio::test]
async fn failed_spawn_never_closes_the_fresh_tab_placeholder() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-placeholder-fail", false)?;
    orch.initialize().await?;
    ctrl.set_fail_new_pane(true);

    let err = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("bg".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::BackgroundTab,
        })
        .await
        .err();
    assert!(
        err.is_some(),
        "expected the failed spawn to surface an error"
    );

    let panes = ctrl.panes_in_tab(TabId::new(1));
    assert_eq!(
        panes.len(),
        1,
        "a failed spawn must leave the placeholder pane open, got: {panes:?}"
    );
    assert_eq!(panes[0].title, "Shell");
    assert!(
        !ctrl
            .recorded_actions()
            .iter()
            .any(|action| action.starts_with("close_pane:")),
        "a failed spawn must never issue close-pane, got: {:?}",
        ctrl.recorded_actions()
    );
    Ok(())
}

#[tokio::test]
async fn fast_successful_task_closes_its_pane_and_keeps_logs_readable() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-fast-close", false)?;
    orch.initialize().await?;

    ctrl.set_screen_dump(TerminalPaneId::new(1), "fast-out");
    let orch_bg = orch.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        orch_bg
            .inject_event(ZellijEvent::PaneExited {
                pane_id: TerminalPaneId::new(1),
                exit_code: Some(0),
            })
            .await;
    });

    let (outcome, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["echo".into(), "fast-out".into()])?,
            cwd: None,
            name: Some("fast".into()),
            wait_timeout: Duration::from_millis(500),
            placement: PlacementRequest::Auto,
        })
        .await?;

    let task_id = match outcome {
        TaskRunOutcome::Completed {
            task_id,
            status,
            output,
            ..
        } => {
            assert_eq!(status, "completed");
            assert!(output.contains("fast-out"));
            task_id
        }
        TaskRunOutcome::Running { .. } => panic!("expected fast-path completion"),
    };

    assert!(
        ctrl.recorded_actions()
            .iter()
            .any(|action| action.starts_with("close_pane:")),
        "expected the fast task pane to be closed, got: {:?}",
        ctrl.recorded_actions()
    );
    assert!(
        !ctrl.has_pane(TerminalPaneId::new(1)),
        "the closed pane must be gone from Zellij"
    );
    assert!(
        ctrl.has_pane(TerminalPaneId::new(0)),
        "unrelated panes must stay open"
    );
    let dumps = ctrl
        .recorded_actions()
        .iter()
        .filter(|action| action.starts_with("dump_screen:"))
        .count();
    assert_eq!(
        dumps,
        2,
        "fast close must not dump the screen a third time: {:?}",
        ctrl.recorded_actions()
    );

    // The scrollback was backfilled before closing, so task_logs still answers.
    let (logs, _) = orch.task_logs(&task_id, 10, None).await?;
    assert!(logs.pane_closed);
    assert!(logs.output.contains("fast-out"));
    Ok(())
}

#[tokio::test]
async fn retention_close_reuses_the_exit_backfill() -> Result<()> {
    let (orch, ctrl) = make_orchestrator_with_retention(
        "mock-retention-backfill",
        RetentionPolicy {
            completed_tasks: RetentionQuota::CloseImmediately,
            fast_task_close: FastTaskClosePolicy::FollowCompletedQuota,
            ..RetentionPolicy::default()
        },
    )?;
    orch.initialize().await?;

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("gc".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Auto,
        })
        .await?;
    let pane_id = match running {
        TaskRunOutcome::Running { pane_id, .. } => pane_id,
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };
    ctrl.set_screen_dump(pane_id, "gc-out");
    orch.inject_event(ZellijEvent::PaneExited {
        pane_id,
        exit_code: Some(0),
    })
    .await;

    let dumps = ctrl
        .recorded_actions()
        .iter()
        .filter(|action| action.starts_with("dump_screen:"))
        .count();
    assert_eq!(
        dumps,
        1,
        "retention close must reuse the exit backfill: {:?}",
        ctrl.recorded_actions()
    );
    assert!(!ctrl.has_pane(pane_id));
    Ok(())
}

#[tokio::test]
async fn fast_task_close_policy_can_follow_the_completed_quota() -> Result<()> {
    let (orch, ctrl) = make_orchestrator_with_retention(
        "mock-fast-keep",
        RetentionPolicy {
            completed_tasks: RetentionQuota::keep(2),
            fast_task_close: FastTaskClosePolicy::FollowCompletedQuota,
            ..RetentionPolicy::default()
        },
    )?;
    orch.initialize().await?;

    // Three fast tasks: each pane appears under its own managed title, so the exit event can
    // target the right pane regardless of which tab the layout router chose.
    let mut panes = Vec::new();
    for seq in 1..=3u32 {
        let bg_ctrl = ctrl.clone();
        let orch_bg = orch.clone();
        let title = format!("mcp:task:t_0{seq}");
        let dump = format!("out-{seq}");
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let Some(pane_id) = bg_ctrl.find_pane_id_by_title(&title) else {
                return;
            };
            bg_ctrl.set_screen_dump(pane_id, dump);
            orch_bg
                .inject_event(ZellijEvent::PaneExited {
                    pane_id,
                    exit_code: Some(0),
                })
                .await;
        });
        let (outcome, _) = orch
            .task_run(TaskRunRequest {
                cmd: CommandSpec::parse(vec!["echo".into(), format!("out-{seq}")])?,
                cwd: None,
                name: Some(format!("job-{seq}")),
                wait_timeout: Duration::from_millis(300),
                placement: PlacementRequest::SplitRight,
            })
            .await?;
        match outcome {
            TaskRunOutcome::Completed { pane_id, .. } => panes.push(pane_id),
            TaskRunOutcome::Running { .. } => panic!("expected task {seq} to complete"),
        }
    }

    assert!(
        !ctrl.has_pane(panes[0]),
        "only the oldest completed pane exceeds the keep-2 quota"
    );
    assert!(ctrl.has_pane(panes[1]) && ctrl.has_pane(panes[2]));
    Ok(())
}

#[tokio::test]
async fn workspace_cleanup_previews_and_closes_only_finished_panes() -> Result<()> {
    let (orch, ctrl) = make_orchestrator_with_retention(
        "mock-cleanup",
        RetentionPolicy {
            completed_tasks: RetentionQuota::Unlimited,
            failed_and_cancelled_tasks: RetentionQuota::Unlimited,
            fast_task_close: FastTaskClosePolicy::FollowCompletedQuota,
            ..RetentionPolicy::default()
        },
    )?;
    orch.initialize().await?;

    // A completed task whose pane stays open (fast close follows the completed quota here).
    let (_, done_pane) = run_finishing_task(&orch, &ctrl, "done", "marker", Some(0)).await?;
    // A failed task whose pane also stays open.
    let (_, failed_pane) = run_finishing_task(&orch, &ctrl, "boom", "boom", Some(2)).await?;

    // A still-running task that cleanup must never touch.
    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("alive".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::SplitRight,
        })
        .await?;
    let alive_pane = match running {
        TaskRunOutcome::Running { pane_id, .. } => pane_id,
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };

    let dry_run = CleanupRequest::parse(
        vec![CleanupScope::CompletedTasks, CleanupScope::FailedTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::DryRun,
    )?;
    let (preview, _) = orch.workspace_cleanup(dry_run).await?;
    match preview {
        CleanupOutcome::DryRunPreview {
            would_close,
            skipped_active_count,
        } => {
            assert_eq!(would_close.len(), 2);
            assert_eq!(skipped_active_count, 1);
        }
        CleanupOutcome::Executed { .. } => panic!("expected a dry-run preview"),
    }
    assert!(
        ctrl.has_pane(done_pane) && ctrl.has_pane(failed_pane),
        "dry_run must not close anything, got: {:?}",
        ctrl.recorded_actions()
    );

    let execute = CleanupRequest::parse(
        vec![CleanupScope::CompletedTasks, CleanupScope::FailedTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::Execute,
    )?;
    let (outcome, _) = orch.workspace_cleanup(execute).await?;
    match outcome {
        CleanupOutcome::Executed {
            closed,
            skipped_active_count,
        } => {
            assert_eq!(closed.len(), 2);
            assert_eq!(skipped_active_count, 1);
        }
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(
        ctrl.has_pane(alive_pane),
        "the running task must never be closed by cleanup"
    );
    assert!(!ctrl.has_pane(done_pane) && !ctrl.has_pane(failed_pane));

    // A second run finds nothing left to close.
    let again = CleanupRequest::parse(
        vec![CleanupScope::CompletedTasks, CleanupScope::FailedTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::Execute,
    )?;
    let (outcome, _) = orch.workspace_cleanup(again).await?;
    match outcome {
        CleanupOutcome::Executed { closed, .. } => assert!(closed.is_empty()),
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    Ok(())
}

#[tokio::test]
async fn cancel_timeout_keeps_the_task_waitable_and_running() -> Result<()> {
    let (orch, ctrl) = make_orchestrator_with_retention(
        "mock-cancel-observed",
        RetentionPolicy {
            completed_tasks: RetentionQuota::Unlimited,
            failed_and_cancelled_tasks: RetentionQuota::Unlimited,
            ..RetentionPolicy::default()
        },
    )?;
    orch.initialize().await?;

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("stubborn".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::Auto,
        })
        .await?;
    let (task_id, pane_id) = match running {
        TaskRunOutcome::Running {
            task_id, pane_id, ..
        } => (task_id, pane_id),
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };

    let (status, _) = orch
        .task_cancel(&task_id, PaneClosePolicy::KeepPane)
        .await?;
    assert_eq!(status, "cancel_timeout");
    let (waited, _) = orch
        .task_wait(vec![task_id], WaitMode::All, Duration::from_millis(100))
        .await?;
    assert!(
        waited.timed_out,
        "cancel_timeout must leave the task waitable instead of reporting a terminal state"
    );
    assert_eq!(waited.tasks[0].status, "running");

    let cancelled_cleanup = CleanupRequest::parse(
        vec![CleanupScope::CancelledTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::Execute,
    )?;
    let (before_exit, _) = orch.workspace_cleanup(cancelled_cleanup).await?;
    match before_exit {
        CleanupOutcome::Executed { closed, .. } => assert!(closed.is_empty()),
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(
        ctrl.has_pane(pane_id),
        "a cancel that timed out must not let cleanup close a live pane"
    );

    orch.inject_event(ZellijEvent::PaneExited {
        pane_id,
        exit_code: Some(130),
    })
    .await;
    let failed_cleanup = CleanupRequest::parse(
        vec![CleanupScope::FailedTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::Execute,
    )?;
    let (after_exit, _) = orch.workspace_cleanup(failed_cleanup).await?;
    match after_exit {
        CleanupOutcome::Executed { closed, .. } => assert_eq!(closed.len(), 1),
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(!ctrl.has_pane(pane_id));
    Ok(())
}

#[tokio::test]
async fn stop_timeout_keeps_the_service_ready() -> Result<()> {
    let (orch, ctrl) = make_orchestrator_with_retention(
        "mock-stop-observed",
        RetentionPolicy {
            completed_tasks: RetentionQuota::Unlimited,
            failed_and_cancelled_tasks: RetentionQuota::Unlimited,
            ..RetentionPolicy::default()
        },
    )?;
    orch.initialize().await?;

    let name = ServiceName::parse("stubborn-svc")?;
    let (svc, _) = orch
        .service_start(ServiceStartRequest {
            name: name.clone(),
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            ready_pattern: None,
            ready_timeout: Duration::from_millis(500),
            placement: PlacementRequest::Auto,
        })
        .await?;
    assert_eq!(svc.status, "ready");

    let (status, _) = orch.service_stop(&name, PaneClosePolicy::KeepPane).await?;
    assert_eq!(status, "cancel_timeout");
    let key = ManagedKey::Service(name.clone());
    let Some(record) = orch.get_record(&key).await else {
        panic!("service record must exist");
    };
    assert_eq!(
        record.lifecycle.status_label(),
        "ready",
        "a stop that timed out must restore the service to its running state"
    );
    let request = CleanupRequest::parse(
        vec![CleanupScope::FailedServices],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::Execute,
    )?;
    let (before_exit, _) = orch.workspace_cleanup(request.clone()).await?;
    match before_exit {
        CleanupOutcome::Executed { closed, .. } => assert!(closed.is_empty()),
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(
        ctrl.has_pane(svc.pane_id),
        "a stop that timed out must not let cleanup close a live service"
    );

    orch.inject_event(ZellijEvent::PaneExited {
        pane_id: svc.pane_id,
        exit_code: Some(130),
    })
    .await;
    let (after_exit, _) = orch.workspace_cleanup(request).await?;
    match after_exit {
        CleanupOutcome::Executed { closed, .. } => assert_eq!(closed.len(), 1),
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(!ctrl.has_pane(svc.pane_id));
    Ok(())
}

#[tokio::test]
async fn cleanup_retries_panes_whose_close_failed() -> Result<()> {
    let (orch, ctrl) = make_orchestrator_with_retention(
        "mock-close-failure",
        RetentionPolicy {
            completed_tasks: RetentionQuota::Unlimited,
            failed_and_cancelled_tasks: RetentionQuota::Unlimited,
            fast_task_close: FastTaskClosePolicy::FollowCompletedQuota,
            ..RetentionPolicy::default()
        },
    )?;
    orch.initialize().await?;
    let (task_id, pane_id) =
        run_finishing_task(&orch, &ctrl, "close-fail", "marker", Some(0)).await?;
    let key = ManagedKey::Task(task_id);
    let request = CleanupRequest::parse(
        vec![CleanupScope::CompletedTasks],
        CleanupTarget::EntireSession,
        CleanupExecutionMode::Execute,
    )?;

    ctrl.set_close_pane_failure(ClosePaneFailure::Rejected);
    let (failed, _) = orch.workspace_cleanup(request.clone()).await?;
    match failed {
        CleanupOutcome::Executed { closed, .. } => {
            assert!(
                closed.is_empty(),
                "a failed close must not be reported as closed"
            );
        }
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(
        ctrl.has_pane(pane_id),
        "the pane must stay open after a failed close"
    );
    assert!(
        orch.get_record(&key)
            .await
            .is_some_and(|record| !record.lifecycle.is_pane_closed()),
        "reconcile must reopen the record so the close can be retried"
    );

    ctrl.set_close_pane_failure(ClosePaneFailure::None);
    let (retried, _) = orch.workspace_cleanup(request).await?;
    match retried {
        CleanupOutcome::Executed { closed, .. } => assert_eq!(closed.len(), 1),
        CleanupOutcome::DryRunPreview { .. } => panic!("expected execution"),
    }
    assert!(!ctrl.has_pane(pane_id));
    Ok(())
}

#[tokio::test]
async fn list_panes_failure_keeps_the_fresh_managed_tab() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-list-panes-fail", false)?;
    orch.initialize().await?;
    ctrl.fail_next_list_panes();

    let (running, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("bg".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::BackgroundTab,
        })
        .await?;
    let (pane_id, tab_id, tab_name) = match running {
        TaskRunOutcome::Running {
            pane_id,
            tab_id,
            tab_name,
            ..
        } => (pane_id, tab_id, tab_name),
        TaskRunOutcome::Completed { .. } => panic!("expected running"),
    };
    assert_eq!(tab_name.as_deref(), Some("agent:tasks"));
    let panes = ctrl.panes_in_tab(tab_id);
    assert_eq!(
        panes.len(),
        2,
        "a failed pane listing must keep the tab and its placeholder, got: {panes:?}"
    );
    assert!(panes.iter().any(|pane| pane.pane_id == pane_id));
    assert!(panes.iter().any(|pane| pane.title == "Shell"));
    Ok(())
}

#[tokio::test]
async fn placeholder_cleanup_session_loss_propagates() -> Result<()> {
    let (orch, ctrl) = make_orchestrator("mock-placeholder-session-loss", false)?;
    orch.initialize().await?;
    ctrl.set_close_pane_failure(ClosePaneFailure::SessionNotFound);

    let err = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sleep".into(), "60".into()])?,
            cwd: None,
            name: Some("bg".into()),
            wait_timeout: Duration::from_millis(10),
            placement: PlacementRequest::BackgroundTab,
        })
        .await
        .err();
    let Some(err) = err else {
        panic!("expected placeholder cleanup to surface session loss");
    };
    assert!(matches!(
        err.downcast_ref::<DomainError>(),
        Some(DomainError::SessionNotFound(_))
    ));

    let err = orch.workspace_status().await.err();
    assert!(
        matches!(
            err.and_then(|err| err.downcast::<DomainError>().ok()),
            Some(DomainError::SessionNotFound(_))
        ),
        "the orchestrator must remember that the session died"
    );
    Ok(())
}

/// Runs a task whose pane finishes while `task_run` waits, then returns its pane id.
async fn run_finishing_task(
    orch: &WorkspaceOrchestrator,
    ctrl: &zellij_mcp_server::zellij::cli::mock::MockZellijCtrl,
    label: &str,
    dump: &str,
    exit_code: Option<i32>,
) -> Result<(TaskId, TerminalPaneId)> {
    let bg_ctrl = ctrl.clone();
    let orch_bg = orch.clone();
    let needle = format!(":{label}");
    let dump = dump.to_owned();
    tokio::spawn(async move {
        // Poll for the registered pane instead of guessing a fixed startup delay, which keeps the
        // helper deterministic on a loaded test runner.
        for _ in 0..50 {
            if let Some(pane_id) = bg_ctrl.find_pane_id_by_title(&needle) {
                bg_ctrl.set_screen_dump(pane_id, dump);
                orch_bg
                    .inject_event(ZellijEvent::PaneExited { pane_id, exit_code })
                    .await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let command = if exit_code == Some(0) {
        "true".to_owned()
    } else {
        "false".to_owned()
    };
    let (outcome, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec![command])?,
            cwd: None,
            name: Some(label.to_owned()),
            wait_timeout: Duration::from_secs(1),
            placement: PlacementRequest::Auto,
        })
        .await?;
    match outcome {
        TaskRunOutcome::Completed {
            task_id,
            pane_id,
            exit_code: seen,
            ..
        } => {
            assert_eq!(seen, exit_code);
            Ok((task_id, pane_id))
        }
        TaskRunOutcome::Running { .. } => panic!("expected task '{label}' to finish"),
    }
}
