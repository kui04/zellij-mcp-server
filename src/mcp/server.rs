//! `rmcp 3.11.0` MCP server wiring all 12 tools, `ToolAnnotations`, and `SERVER_INSTRUCTIONS`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{ServerCapabilities, ServerConfig};
use rmcp::{ErrorData as McpError, ServerHandler, tool, tool_handler, tool_router};

use crate::domain::error::{DomainError, ParseError};
use crate::domain::ids::{
    CommandSpec, CompiledPattern, LogSeq, ManagedKey, PaneClosePolicy, ReplInput, ReplName,
    ServiceName, TaskId, WaitMode,
};
use crate::domain::layout::PlacementRequest;
use crate::domain::lifecycle::WorkspaceAlert;
use crate::mcp::dto::{
    ManagedEntityDto, PlacementDto, ReplCloseParams, ReplExecParams, ReplOpenParams, ReplResponse,
    ServiceLogsParams, ServiceLogsResponse, ServiceRestartParams, ServiceStartParams,
    ServiceStartResponse, ServiceStopParams, SimpleActionResponse, TaskCancelParams,
    TaskLogsParams, TaskLogsResponse, TaskRunParams, TaskRunResponse, TaskWaitItemDto,
    TaskWaitParams, TaskWaitResponse, ViewportDto, WaitModeDto, WorkspaceAlertDto,
    WorkspaceStatusParams, WorkspaceStatusResponse,
};
use crate::mcp::prompts::{
    SERVER_INSTRUCTIONS, hint_repl_input_required, hint_service_reused, hint_task_failed,
    hint_task_running,
};
use crate::runtime::{
    DEFAULT_REPL_EXEC_TIMEOUT, DEFAULT_SERVICE_READY_TIMEOUT, DEFAULT_TASK_FAST_WAIT,
    DEFAULT_TASK_WAIT_TIMEOUT, ReplExecRequest, ReplOpenRequest, ServiceStartRequest,
    TaskRunOutcome, TaskRunRequest, WorkspaceOrchestrator,
};

const MAX_TIMEOUT_MS: u64 = 3_600_000;

/// MCP server exposing 12 high-level Zellij workspace orchestration tools over stdio.
#[derive(Debug, Clone)]
pub struct ZellijMcpServer {
    orchestrator: WorkspaceOrchestrator,
}

impl ZellijMcpServer {
    pub fn new(orchestrator: WorkspaceOrchestrator) -> Self {
        Self { orchestrator }
    }

    pub fn orchestrator(&self) -> &WorkspaceOrchestrator {
        &self.orchestrator
    }
}

fn map_placement(dto: Option<PlacementDto>) -> PlacementRequest {
    match dto.unwrap_or_default() {
        PlacementDto::Auto => PlacementRequest::Auto,
        PlacementDto::SplitRight => PlacementRequest::SplitRight,
        PlacementDto::SplitDown => PlacementRequest::SplitDown,
        PlacementDto::Floating => PlacementRequest::Floating,
        PlacementDto::Stacked => PlacementRequest::Stacked,
        PlacementDto::BackgroundTab => PlacementRequest::BackgroundTab,
    }
}

fn map_wait_mode(dto: Option<WaitModeDto>) -> WaitMode {
    match dto.unwrap_or_default() {
        WaitModeDto::Any => WaitMode::Any,
        WaitModeDto::All => WaitMode::All,
    }
}

fn parse_duration_ms(raw: Option<u64>, default: Duration) -> Result<Duration, ParseError> {
    let Some(ms) = raw else {
        return Ok(default);
    };
    if ms > MAX_TIMEOUT_MS {
        return Err(ParseError::TimeoutOutOfRange {
            min_ms: 0,
            max_ms: MAX_TIMEOUT_MS,
            actual_ms: ms,
        });
    }
    Ok(Duration::from_millis(ms))
}

fn map_parse_err(err: ParseError) -> McpError {
    McpError::invalid_params(err.to_string(), None)
}

fn map_runtime_err(err: anyhow::Error) -> McpError {
    if err.downcast_ref::<ParseError>().is_some() || err.downcast_ref::<DomainError>().is_some() {
        McpError::invalid_params(err.to_string(), None)
    } else {
        McpError::internal_error(err.to_string(), None)
    }
}

fn map_alerts(alerts: Vec<WorkspaceAlert>) -> Vec<WorkspaceAlertDto> {
    alerts.into_iter().map(WorkspaceAlertDto::from).collect()
}

#[tool_router]
impl ZellijMcpServer {
    #[tool(
        name = "workspace_status",
        description = "Inspect all managed tasks, services, and REPLs along with live Zellij viewport dimensions, managed tabs, and recommended layout tier. Use for: resuming a conversation or checking what is already running. Do NOT use for: polling a running task in a loop (use task_wait instead).",
        annotations(read_only_hint = true)
    )]
    async fn workspace_status(
        &self,
        Parameters(params): Parameters<WorkspaceStatusParams>,
    ) -> Result<Json<WorkspaceStatusResponse>, McpError> {
        let active_session = self
            .orchestrator
            .config()
            .session_resolution
            .session_name()
            .as_str();
        if let Some(requested) = params.session.as_deref()
            && requested != active_session
        {
            return Err(McpError::invalid_params(
                format!("server is bound to Zellij session '{active_session}', not '{requested}'"),
                None,
            ));
        }

        let (snapshot, alerts) = self
            .orchestrator
            .workspace_status()
            .await
            .map_err(map_runtime_err)?;

        let session = snapshot.session_resolution.session_name().to_string();
        let attach_command = format!("zellij attach {session}");

        let viewport = snapshot.layout.viewport.map(|v| ViewportDto {
            columns: v.columns,
            rows: v.rows,
            tiled_panes_in_current_tab: v.tiled_panes_in_active_tab,
            estimated_col_if_split: v.estimated_col_if_split(),
            layout_tier: v.tier.as_str().to_owned(),
        });

        let mut managed_tabs = BTreeMap::new();
        if let Some(tab_id) = snapshot.layout.managed_tabs.tasks {
            let _ = managed_tabs.insert("agent:tasks".to_owned(), tab_id.get());
        }
        if let Some(tab_id) = snapshot.layout.managed_tabs.services {
            let _ = managed_tabs.insert("agent:services".to_owned(), tab_id.get());
        }

        let entities = snapshot
            .records
            .into_iter()
            .map(|rec| {
                let (kind, id) = match &rec.key {
                    ManagedKey::Task(id) => ("task", id.to_string()),
                    ManagedKey::Service(name) => ("service", name.to_string()),
                    ManagedKey::Repl(name) => ("repl", name.to_string()),
                };
                ManagedEntityDto {
                    id,
                    kind: kind.to_owned(),
                    pane_id: rec.pane_id.to_string(),
                    pane_name: rec.pane_title,
                    tab_id: rec.tab_id.get(),
                    placement: rec.placement.as_str().to_owned(),
                    status: rec.lifecycle.status_label().to_owned(),
                    exit_code: rec.lifecycle.exit_code(),
                }
            })
            .collect();

        Ok(Json(WorkspaceStatusResponse {
            session,
            attach_command,
            viewport,
            managed_tabs,
            entities,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "task_run",
        description = "Run a finite command with an exit code (build, test, lint, migration) in a Zellij pane. Fast commands (<= wait_ms, default 3000ms) return completed output immediately; longer ones return running + task_id for server-side task_wait. Do NOT use for: long-lived dev servers/watchers (use service_start) or interactive shells/REPLs (use repl_open).",
        annotations(destructive_hint = false)
    )]
    async fn task_run(
        &self,
        Parameters(params): Parameters<TaskRunParams>,
    ) -> Result<Json<TaskRunResponse>, McpError> {
        let cmd = CommandSpec::parse(params.cmd).map_err(map_parse_err)?;
        let wait_timeout =
            parse_duration_ms(params.wait_ms, DEFAULT_TASK_FAST_WAIT).map_err(map_parse_err)?;
        let env = params
            .env
            .map(|m| m.into_iter().collect())
            .unwrap_or_default();

        let (outcome, alerts) = self
            .orchestrator
            .task_run(TaskRunRequest {
                cmd,
                cwd: params.cwd.map(PathBuf::from),
                env,
                name: params.name,
                wait_timeout,
                placement: map_placement(params.placement),
            })
            .await
            .map_err(map_runtime_err)?;

        let resp = match outcome {
            TaskRunOutcome::Completed {
                task_id,
                pane_id,
                pane_title,
                status,
                exit_code,
                elapsed_ms,
                output,
            } => {
                let hint = if status == "failed" {
                    Some(hint_task_failed(&pane_title))
                } else {
                    None
                };
                TaskRunResponse {
                    task_id: task_id.to_string(),
                    pane_id: pane_id.to_string(),
                    pane_name: pane_title,
                    status: status.to_owned(),
                    elapsed_ms,
                    exit_code,
                    output: Some(output),
                    tail_preview: None,
                    alerts: map_alerts(alerts),
                    hint,
                }
            }
            TaskRunOutcome::Running {
                task_id,
                pane_id,
                pane_title,
                elapsed_ms,
                tail_preview,
            } => {
                let hint = hint_task_running(&task_id);
                TaskRunResponse {
                    task_id: task_id.to_string(),
                    pane_id: pane_id.to_string(),
                    pane_name: pane_title,
                    status: "running".to_owned(),
                    elapsed_ms,
                    exit_code: None,
                    output: None,
                    tail_preview: Some(tail_preview),
                    alerts: map_alerts(alerts),
                    hint: Some(hint),
                }
            }
        };

        Ok(Json(resp))
    }

    #[tool(
        name = "task_wait",
        description = "Block server-side on Zellij events until any or all specified tasks exit, with zero LLM polling. Use for: waiting on running task_ids returned by task_run. Do NOT use for: waiting on long-lived services.",
        annotations(read_only_hint = true)
    )]
    async fn task_wait(
        &self,
        Parameters(params): Parameters<TaskWaitParams>,
    ) -> Result<Json<TaskWaitResponse>, McpError> {
        if params.task_ids.is_empty() {
            return Err(map_parse_err(ParseError::Empty { field: "task_ids" }));
        }
        let task_ids = params
            .task_ids
            .into_iter()
            .map(TaskId::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_parse_err)?;
        let timeout = parse_duration_ms(params.timeout_ms, DEFAULT_TASK_WAIT_TIMEOUT)
            .map_err(map_parse_err)?;

        let (outcome, alerts) = self
            .orchestrator
            .task_wait(task_ids, map_wait_mode(params.mode), timeout)
            .await
            .map_err(map_runtime_err)?;

        let hint = if outcome.timed_out {
            Some("Wait timed out while tasks are still running. Call task_wait again instead of sleeping.".to_owned())
        } else {
            outcome
                .tasks
                .iter()
                .find(|t| t.status == "failed")
                .map(|t| hint_task_failed(&t.pane_title))
        };

        let tasks = outcome
            .tasks
            .into_iter()
            .map(|t| TaskWaitItemDto {
                task_id: t.task_id.to_string(),
                pane_id: t.pane_id.to_string(),
                pane_name: t.pane_title,
                status: t.status.to_owned(),
                exit_code: t.exit_code,
                output: t.output,
            })
            .collect();

        Ok(Json(TaskWaitResponse {
            timed_out: outcome.timed_out,
            tasks,
            alerts: map_alerts(alerts),
            hint,
        }))
    }

    #[tool(
        name = "task_logs",
        description = "Read ANSI-stripped logs for a task (full dump-screen if exited, bounded ring buffer if running) with optional tail_lines and regex grep. Do NOT use for: polling in a sleep loop to wait for completion (use task_wait).",
        annotations(read_only_hint = true)
    )]
    async fn task_logs(
        &self,
        Parameters(params): Parameters<TaskLogsParams>,
    ) -> Result<Json<TaskLogsResponse>, McpError> {
        let task_id = TaskId::parse(params.task_id).map_err(map_parse_err)?;
        let grep = params
            .grep
            .map(|raw| CompiledPattern::parse("grep", raw))
            .transpose()
            .map_err(map_parse_err)?;
        let tail_lines = params.tail_lines.unwrap_or(100);

        let (output, alerts) = self
            .orchestrator
            .task_logs(&task_id, tail_lines, grep.as_ref())
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(TaskLogsResponse {
            task_id: task_id.to_string(),
            output,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "task_cancel",
        description = "Cancel a running task via Ctrl+C (with grace period), keeping the pane open by default so the human can inspect output. Use for: aborting a stuck or obsolete task.",
        annotations(destructive_hint = true)
    )]
    async fn task_cancel(
        &self,
        Parameters(params): Parameters<TaskCancelParams>,
    ) -> Result<Json<SimpleActionResponse>, McpError> {
        let task_id = TaskId::parse(params.task_id).map_err(map_parse_err)?;
        let close_policy = if params.close_pane.unwrap_or(false) {
            PaneClosePolicy::ClosePane
        } else {
            PaneClosePolicy::KeepPane
        };

        let (status, alerts) = self
            .orchestrator
            .task_cancel(&task_id, close_policy)
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(SimpleActionResponse {
            id: task_id.to_string(),
            status,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "service_start",
        description = "Idempotently start a long-lived background process (dev server, proxy, watcher) by stable name and wait for optional ready_pattern via zellij subscribe. Reuses the service if already active. Do NOT use for: finite builds/tests (use task_run).",
        annotations(idempotent_hint = true)
    )]
    async fn service_start(
        &self,
        Parameters(params): Parameters<ServiceStartParams>,
    ) -> Result<Json<ServiceStartResponse>, McpError> {
        let name = ServiceName::parse(params.name).map_err(map_parse_err)?;
        let cmd = CommandSpec::parse(params.cmd).map_err(map_parse_err)?;
        let ready_pattern = params
            .ready_pattern
            .map(|raw| CompiledPattern::parse("ready_pattern", raw))
            .transpose()
            .map_err(map_parse_err)?;
        let ready_timeout =
            parse_duration_ms(params.ready_timeout_ms, DEFAULT_SERVICE_READY_TIMEOUT)
                .map_err(map_parse_err)?;

        let (outcome, alerts) = self
            .orchestrator
            .service_start(ServiceStartRequest {
                name: name.clone(),
                cmd,
                cwd: params.cwd.map(PathBuf::from),
                ready_pattern,
                ready_timeout,
                placement: map_placement(params.placement),
            })
            .await
            .map_err(map_runtime_err)?;

        let hint = if outcome.reused {
            Some(hint_service_reused(&name))
        } else if outcome.status == "starting" {
            Some(format!(
                "Service '{name}' did not match ready_pattern before timeout. Inspect service_logs(name='{name}')."
            ))
        } else {
            None
        };

        Ok(Json(ServiceStartResponse {
            name: outcome.name.to_string(),
            pane_id: outcome.pane_id.to_string(),
            pane_name: outcome.pane_title,
            reused: outcome.reused,
            status: outcome.status.to_owned(),
            matched_line: outcome.matched_line,
            tail_preview: outcome.tail_preview,
            alerts: map_alerts(alerts),
            hint,
        }))
    }

    #[tool(
        name = "service_logs",
        description = "Read incremental logs for a named service using `since_seq` cursor and optional regex `grep`, returning `next_seq` to avoid re-reading old lines.",
        annotations(read_only_hint = true)
    )]
    async fn service_logs(
        &self,
        Parameters(params): Parameters<ServiceLogsParams>,
    ) -> Result<Json<ServiceLogsResponse>, McpError> {
        let name = ServiceName::parse(params.name).map_err(map_parse_err)?;
        let grep = params
            .grep
            .map(|raw| CompiledPattern::parse("grep", raw))
            .transpose()
            .map_err(map_parse_err)?;
        let tail_lines = params.tail_lines.unwrap_or(80);
        let since_seq = params.since_seq.map(LogSeq::new);

        let (slice, alerts) = self
            .orchestrator
            .service_logs(&name, tail_lines, since_seq, grep.as_ref())
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(ServiceLogsResponse {
            name: name.to_string(),
            lines: slice.entries.into_iter().map(|e| e.line).collect(),
            next_seq: slice.next_seq.get(),
            dropped_before: slice.dropped_before.map(LogSeq::get),
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "service_restart",
        description = "Gracefully stop and restart a named service with its recorded configuration, waiting for readiness again.",
        annotations(idempotent_hint = true)
    )]
    async fn service_restart(
        &self,
        Parameters(params): Parameters<ServiceRestartParams>,
    ) -> Result<Json<ServiceStartResponse>, McpError> {
        let name = ServiceName::parse(params.name).map_err(map_parse_err)?;
        let ready_timeout =
            parse_duration_ms(params.ready_timeout_ms, DEFAULT_SERVICE_READY_TIMEOUT)
                .map_err(map_parse_err)?;

        let (outcome, alerts) = self
            .orchestrator
            .service_restart(&name, ready_timeout)
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(ServiceStartResponse {
            name: outcome.name.to_string(),
            pane_id: outcome.pane_id.to_string(),
            pane_name: outcome.pane_title,
            reused: outcome.reused,
            status: outcome.status.to_owned(),
            matched_line: outcome.matched_line,
            tail_preview: outcome.tail_preview,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "service_stop",
        description = "Gracefully stop a long-lived service, closing its pane by default and releasing its viewport subscription.",
        annotations(destructive_hint = true)
    )]
    async fn service_stop(
        &self,
        Parameters(params): Parameters<ServiceStopParams>,
    ) -> Result<Json<SimpleActionResponse>, McpError> {
        let name = ServiceName::parse(params.name).map_err(map_parse_err)?;
        let close_policy = if params.close_pane.unwrap_or(true) {
            PaneClosePolicy::ClosePane
        } else {
            PaneClosePolicy::KeepPane
        };

        let (status, alerts) = self
            .orchestrator
            .service_stop(&name, close_policy)
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(SimpleActionResponse {
            id: name.to_string(),
            status,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "repl_open",
        description = "Open or idempotently reuse a stateful interactive terminal session (`ssh`, `gdb`, `python3`, `psql`) and wait for the initial prompt or quiet settle. Do NOT use for: one-shot commands (use task_run).",
        annotations(idempotent_hint = true)
    )]
    async fn repl_open(
        &self,
        Parameters(params): Parameters<ReplOpenParams>,
    ) -> Result<Json<ReplResponse>, McpError> {
        let name = ReplName::parse(params.name).map_err(map_parse_err)?;
        let cmd = CommandSpec::parse(params.cmd).map_err(map_parse_err)?;
        let prompt_pattern = params
            .prompt_pattern
            .map(|raw| CompiledPattern::parse("prompt_pattern", raw))
            .transpose()
            .map_err(map_parse_err)?;

        let (outcome, alerts) = self
            .orchestrator
            .repl_open(ReplOpenRequest {
                name,
                cmd,
                cwd: params.cwd.map(PathBuf::from),
                prompt_pattern,
                placement: map_placement(params.placement),
            })
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(ReplResponse {
            name: outcome.name.to_string(),
            pane_id: outcome.pane_id.to_string(),
            pane_name: outcome.pane_title,
            status: outcome.status.to_owned(),
            output: outcome.delta_output,
            timed_out: outcome.timed_out,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }

    #[tool(
        name = "repl_exec",
        description = "Send bracketed-paste text (`input`) or special key chords (`keys`) to a named REPL under a per-pane write lock and return only the new incremental output after prompt match or 800ms quiet debounce.",
        annotations(destructive_hint = false)
    )]
    async fn repl_exec(
        &self,
        Parameters(params): Parameters<ReplExecParams>,
    ) -> Result<Json<ReplResponse>, McpError> {
        let name = ReplName::parse(params.name).map_err(map_parse_err)?;
        let input = ReplInput::parse(params.input, params.keys, params.send_enter.unwrap_or(true))
            .map_err(map_parse_err)?;
        let wait_for_prompt = params
            .wait_for_prompt
            .map(|raw| CompiledPattern::parse("wait_for_prompt", raw))
            .transpose()
            .map_err(map_parse_err)?;
        let timeout = parse_duration_ms(params.timeout_ms, DEFAULT_REPL_EXEC_TIMEOUT)
            .map_err(map_parse_err)?;

        let (outcome, alerts) = self
            .orchestrator
            .repl_exec(ReplExecRequest {
                name: name.clone(),
                input,
                wait_for_prompt,
                timeout,
            })
            .await
            .map_err(map_runtime_err)?;

        let hint = if outcome.status == "input_required" {
            Some(hint_repl_input_required(&name, &outcome.pane_title))
        } else {
            None
        };

        Ok(Json(ReplResponse {
            name: outcome.name.to_string(),
            pane_id: outcome.pane_id.to_string(),
            pane_name: outcome.pane_title,
            status: outcome.status.to_owned(),
            output: outcome.delta_output,
            timed_out: outcome.timed_out,
            alerts: map_alerts(alerts),
            hint,
        }))
    }

    #[tool(
        name = "repl_close",
        description = "Send Ctrl+D to exit a named REPL session, close its pane, and release its write lock and subscription.",
        annotations(destructive_hint = true)
    )]
    async fn repl_close(
        &self,
        Parameters(params): Parameters<ReplCloseParams>,
    ) -> Result<Json<SimpleActionResponse>, McpError> {
        let name = ReplName::parse(params.name).map_err(map_parse_err)?;

        let (status, alerts) = self
            .orchestrator
            .repl_close(&name)
            .await
            .map_err(map_runtime_err)?;

        Ok(Json(SimpleActionResponse {
            id: name.to_string(),
            status,
            alerts: map_alerts(alerts),
            hint: None,
        }))
    }
}

#[tool_handler]
impl ServerHandler for ZellijMcpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(SERVER_INSTRUCTIONS)
    }
}
