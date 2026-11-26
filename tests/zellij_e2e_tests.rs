//! End-to-end integration tests driving a real isolated `zellij 0.46.1` background session.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use zellij_mcp_server::domain::ids::{
    CommandSpec, CompiledPattern, EnterPolicy, PaneClosePolicy, ReplInput, ReplName, ServiceName,
    SessionName, SessionResolution, WaitMode,
};
use zellij_mcp_server::domain::layout::PlacementRequest;
use zellij_mcp_server::runtime::{
    OrchestratorConfig, ReplExecRequest, ReplOpenRequest, ServiceStartRequest, TaskRunOutcome,
    TaskRunRequest, WorkspaceOrchestrator,
};
use zellij_mcp_server::zellij::cli::ZellijClient;

struct SessionCleanupGuard {
    session: String,
}

impl Drop for SessionCleanupGuard {
    fn drop(&mut self) {
        let _ = Command::new("zellij")
            .args(["kill-session", &self.session])
            .output();
        let _ = Command::new("zellij")
            .args(["delete-session", "--force", &self.session])
            .output();
    }
}

#[tokio::test]
async fn real_zellij_task_service_and_repl_end_to_end() -> Result<()> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let session_raw = format!("mcp-e2e-{}-{:04x}", std::process::id(), nanos & 0xffff);
    let _cleanup = SessionCleanupGuard {
        session: session_raw.clone(),
    };

    let session_name = SessionName::parse(session_raw)?;
    let client = ZellijClient::new(PathBuf::from("zellij"), session_name.clone());
    let orch = WorkspaceOrchestrator::new(
        OrchestratorConfig {
            session_resolution: SessionResolution::GeneratedUnique(session_name.clone()),
            confirm_destructive: false,
            keep_session: false,
            default_cwd: std::env::current_dir()?,
        },
        client.clone(),
    );

    orch.initialize().await?;
    // Allow background `zellij events` subprocess a brief moment to attach.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 0. Restarting / attaching a second orchestrator against an already-running session succeeds
    //    and its shutdown must NOT kill the pre-existing session.
    let second_orch = WorkspaceOrchestrator::new(
        OrchestratorConfig {
            session_resolution: SessionResolution::Explicit(session_name.clone()),
            confirm_destructive: false,
            keep_session: false,
            default_cwd: std::env::current_dir()?,
        },
        client.clone(),
    );
    second_orch.initialize().await?;
    second_orch.shutdown().await;
    assert!(client.session_exists().await?);

    // 1. Fast-path finite task + exact tail_lines check (no off-by-one trailing blank row)
    let (fast, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec![
                "sh".into(),
                "-c".into(),
                "printf 'L1\\nL2\\nL3\\n'; exit 0".into(),
            ])?,
            cwd: None,
            name: Some("fast".into()),
            wait_timeout: Duration::from_secs(5),
            placement: PlacementRequest::Auto,
        })
        .await?;

    match fast {
        TaskRunOutcome::Completed {
            task_id,
            status,
            exit_code,
            output,
            ..
        } => {
            assert_eq!(status, "completed");
            assert_eq!(exit_code, Some(0));
            assert!(
                output.contains("L3"),
                "expected L3 in output, got: {output:?}"
            );
            let (tailed, _) = orch.task_logs(&task_id, 2, None).await?;
            assert_eq!(tailed.output, "L2\nL3");
        }
        TaskRunOutcome::Running { .. } => panic!("expected fast task to complete within 5s"),
    }

    // 1b. Headless placement="stacked" succeeds via floating fallback
    let (stacked_fast, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec!["sh".into(), "-c".into(), "echo stacked-ok".into()])?,
            cwd: None,
            name: Some("stacked".into()),
            wait_timeout: Duration::from_secs(5),
            placement: PlacementRequest::Stacked,
        })
        .await?;
    assert!(matches!(stacked_fast, TaskRunOutcome::Completed { .. }));

    // 2. Slow-path finite task + zero-polling task_wait
    let (slow, _) = orch
        .task_run(TaskRunRequest {
            cmd: CommandSpec::parse(vec![
                "sh".into(),
                "-c".into(),
                "sleep 0.5; echo e2e-slow-ok; exit 0".into(),
            ])?,
            cwd: None,
            name: Some("slow".into()),
            wait_timeout: Duration::from_millis(50),
            placement: PlacementRequest::Auto,
        })
        .await?;

    let slow_id = match slow {
        TaskRunOutcome::Running { task_id, .. } => task_id,
        TaskRunOutcome::Completed { .. } => panic!("expected slow task to hand off asynchronously"),
    };

    let (waited, _) = orch
        .task_wait(vec![slow_id], WaitMode::All, Duration::from_secs(5))
        .await?;
    assert!(!waited.timed_out);
    assert_eq!(waited.tasks[0].exit_code, Some(0));
    assert!(waited.tasks[0].output.contains("e2e-slow-ok"));

    // 3. Long-lived service with ready_pattern + idempotent reuse + stop
    let svc_name = ServiceName::parse("e2e-svc")?;
    let (svc, _) = orch
        .service_start(ServiceStartRequest {
            name: svc_name.clone(),
            cmd: CommandSpec::parse(vec![
                "sh".into(),
                "-c".into(),
                "echo E2E_READY_8080; sleep 30".into(),
            ])?,
            cwd: None,
            ready_pattern: Some(CompiledPattern::parse("ready_pattern", "E2E_READY_8080")?),
            ready_timeout: Duration::from_secs(5),
            placement: PlacementRequest::BackgroundTab,
        })
        .await?;
    assert!(!svc.reused);
    assert_eq!(svc.status, "ready");

    let (svc_reused, _) = orch
        .service_start(ServiceStartRequest {
            name: svc_name.clone(),
            cmd: CommandSpec::parse(vec!["sh".into(), "-c".into(), "sleep 30".into()])?,
            cwd: None,
            ready_pattern: None,
            ready_timeout: Duration::from_secs(2),
            placement: PlacementRequest::BackgroundTab,
        })
        .await?;
    assert!(svc_reused.reused);
    assert_eq!(svc_reused.pane_id, svc.pane_id);

    let (_stop_status, stop_alerts) = orch
        .service_stop(&svc_name, PaneClosePolicy::ClosePane)
        .await?;
    assert!(
        stop_alerts.is_empty(),
        "expected no false crash alerts on service_stop, got: {stop_alerts:?}"
    );

    // 4. Interactive REPL open + exec + close
    let repl_name = ReplName::parse("e2e-sh")?;
    let (opened, _) = orch
        .repl_open(ReplOpenRequest {
            name: repl_name.clone(),
            cmd: CommandSpec::parse(vec!["sh".into()])?,
            cwd: None,
            prompt_pattern: None,
            placement: PlacementRequest::Floating,
        })
        .await?;
    assert_eq!(opened.status, "ready");

    let (exec_res, _) = orch
        .repl_exec(ReplExecRequest {
            name: repl_name.clone(),
            input: ReplInput::Paste {
                text: "echo E2E_REPL_42".into(),
                enter: EnterPolicy::SendEnter,
            },
            wait_for_prompt: Some(CompiledPattern::parse("wait_for_prompt", r"\$\s*$")?),
            timeout: Duration::from_secs(5),
        })
        .await?;
    assert!(
        !exec_res.timed_out,
        "repl_exec timed out; status={}, delta={:?}",
        exec_res.status, exec_res.delta_output
    );
    assert!(exec_res.delta_output.contains("E2E_REPL_42"));

    let _ = orch.repl_close(&repl_name).await?;

    // 5. Graceful shutdown stops stream subprocesses and kills the server-created session.
    orch.shutdown().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!client.session_exists().await?);
    Ok(())
}
