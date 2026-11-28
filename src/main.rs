//! CLI entrypoint for `zellij-mcp-server` (stdio MCP transport only).

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use rmcp::ServiceExt;
use tracing::info;
use tracing_subscriber::EnvFilter;
use zellij_mcp_server::domain::ids::SessionResolution;
use zellij_mcp_server::domain::layout::{
    DEFAULT_MIN_PANE_COLS, DEFAULT_MIN_PANE_ROWS, LayoutConstraints,
};
use zellij_mcp_server::mcp::server::ZellijMcpServer;
use zellij_mcp_server::runtime::{OrchestratorConfig, WorkspaceOrchestrator};
use zellij_mcp_server::state::{FastTaskClosePolicy, RetentionPolicy, RetentionQuota};
use zellij_mcp_server::zellij::cli::ZellijClient;

/// Zellij-backed asynchronous terminal orchestration MCP server (stdio).
#[derive(Debug, Parser)]
#[command(name = "zellij-mcp-server", version, about)]
struct Cli {
    /// Target Zellij session name (overrides $ZELLIJ_SESSION_NAME).
    #[arg(long, env = "ZELLIJ_MCP_SESSION")]
    session: Option<String>,

    /// Path to the `zellij` executable (>= 0.46.1).
    #[arg(long, env = "ZELLIJ_BIN", default_value = "zellij")]
    zellij_bin: PathBuf,

    /// Gate destructive tools (`task_cancel`, `service_stop`, `repl_close`) with `zellij prompt confirm`.
    #[arg(long, default_value_t = false)]
    confirm_destructive: bool,

    /// Keep a server-created Zellij session alive when the MCP server exits.
    #[arg(long, default_value_t = false)]
    keep_session: bool,

    /// Hard minimum column width preserved per pane when splitting horizontally (0 disables).
    #[arg(long, env = "ZELLIJ_MCP_MIN_PANE_COLS", default_value_t = DEFAULT_MIN_PANE_COLS)]
    min_pane_cols: u16,

    /// Hard minimum row height preserved per pane when splitting vertically (0 disables).
    #[arg(long, env = "ZELLIJ_MCP_MIN_PANE_ROWS", default_value_t = DEFAULT_MIN_PANE_ROWS)]
    min_pane_rows: u16,

    /// Completed task panes kept open ("0" closes immediately, N keeps the newest N,
    /// "unlimited" never evicts).
    #[arg(long, env = "ZELLIJ_MCP_KEEP_COMPLETED_TASKS", default_value = "1")]
    keep_completed_tasks: RetentionQuota,

    /// Failed and cancelled task panes kept open ("0", N, or "unlimited").
    #[arg(long, env = "ZELLIJ_MCP_KEEP_FAILED_TASKS", default_value = "2")]
    keep_failed_tasks: RetentionQuota,

    /// Exited REPL panes kept open ("0", N, or "unlimited").
    #[arg(long, env = "ZELLIJ_MCP_KEEP_EXITED_REPLS", default_value = "0")]
    keep_exited_repls: RetentionQuota,

    /// Close the pane of a task that succeeds inside the `task_run` wait window after
    /// backfilling its logs.
    #[arg(
        long,
        env = "ZELLIJ_MCP_AUTO_CLOSE_FAST_TASKS",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    auto_close_fast_tasks: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // MCP stdio owns stdout; emit all structured telemetry to stderr.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let default_cwd = std::env::current_dir().context("failed to determine current directory")?;
    let cwd_slug = default_cwd
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("workspace");

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let unique_suffix = std::process::id() ^ nanos;

    let session_resolution = SessionResolution::resolve(
        cli.session,
        None,
        std::env::var("ZELLIJ_SESSION_NAME").ok(),
        cwd_slug,
        unique_suffix,
    )?;

    let session_name = session_resolution.session_name().clone();
    info!(
        session = %session_name,
        resolution = ?session_resolution,
        zellij_bin = %cli.zellij_bin.display(),
        "starting zellij-mcp-server over stdio"
    );

    let client = ZellijClient::new(cli.zellij_bin, session_name);
    let orchestrator = WorkspaceOrchestrator::new(
        OrchestratorConfig {
            session_resolution,
            confirm_destructive: cli.confirm_destructive,
            keep_session: cli.keep_session,
            default_cwd,
            layout_constraints: LayoutConstraints {
                min_pane_cols: cli.min_pane_cols,
                min_pane_rows: cli.min_pane_rows,
            },
            retention: RetentionPolicy {
                completed_tasks: cli.keep_completed_tasks,
                failed_and_cancelled_tasks: cli.keep_failed_tasks,
                exited_repls: cli.keep_exited_repls,
                fast_task_close: FastTaskClosePolicy::from_auto_close(cli.auto_close_fast_tasks),
            },
        },
        client,
    );

    orchestrator.initialize().await?;

    let server = ZellijMcpServer::new(orchestrator.clone());
    let running = server
        .serve(rmcp::transport::stdio())
        .await
        .context("failed to initialize MCP stdio transport")?;

    tokio::select! {
        res = running.waiting() => {
            let _ = res;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received ctrl-c; shutting down");
        }
    }

    orchestrator.shutdown().await;
    Ok(())
}
