//! CLI entrypoint for `zellij-mcp-server` (stdio MCP transport only).

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use rmcp::ServiceExt;
use tracing::info;
use tracing_subscriber::EnvFilter;
use zellij_mcp_server::domain::ids::SessionResolution;
use zellij_mcp_server::mcp::server::ZellijMcpServer;
use zellij_mcp_server::runtime::{OrchestratorConfig, WorkspaceOrchestrator};
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
            default_cwd,
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
            info!("received ctrl-c; shutting down streams without touching Zellij session");
        }
    }

    orchestrator.shutdown().await;
    Ok(())
}
