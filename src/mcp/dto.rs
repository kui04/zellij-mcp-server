//! Wire DTOs and JSON Schema definitions for all 13 MCP tools.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::lifecycle::WorkspaceAlert;

/// Wire representation of an asynchronous workspace alert (`alerts[]`).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct WorkspaceAlertDto {
    pub entity: String,
    pub pane_id: String,
    pub message: String,
}

impl From<WorkspaceAlert> for WorkspaceAlertDto {
    fn from(alert: WorkspaceAlert) -> Self {
        Self {
            entity: alert.key.to_string(),
            pane_id: alert.pane_id.to_string(),
            message: alert.message,
        }
    }
}

/// Semantic placement option accepted on `task_run`, `service_start`, and `repl_open`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlacementDto {
    #[default]
    Auto,
    SplitRight,
    SplitDown,
    Floating,
    Stacked,
    BackgroundTab,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct WorkspaceStatusParams {
    /// Optional Zellij session name check (defaults to the server's active session).
    pub session: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ViewportDto {
    pub columns: u16,
    pub rows: u16,
    pub tiled_panes_in_current_tab: u16,
    pub estimated_col_if_split: u16,
    pub layout_tier: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ManagedEntityDto {
    pub id: String,
    pub kind: String,
    pub pane_id: String,
    pub pane_name: String,
    pub tab_id: u32,
    pub placement: String,
    pub status: String,
    pub pane_closed: bool,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct WorkspaceStatusResponse {
    pub session: String,
    pub attach_command: String,
    pub viewport: Option<ViewportDto>,
    pub managed_tabs: BTreeMap<String, u32>,
    pub entities: Vec<ManagedEntityDto>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TaskRunParams {
    /// Command and arguments to execute directly (argv array, e.g. `["cargo", "test"]`).
    pub cmd: Vec<String>,
    /// Optional working directory.
    pub cwd: Option<String>,
    /// Optional short label included in the Zellij pane title.
    pub name: Option<String>,
    /// Fast-path synchronous wait window in milliseconds (default: 3000).
    pub wait_ms: Option<u64>,
    /// Semantic pane placement (default: `"auto"`).
    pub placement: Option<PlacementDto>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TaskRunResponse {
    pub task_id: String,
    pub pane_id: String,
    pub pane_name: String,
    pub tab_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tab_name: Option<String>,
    pub placement: String,
    pub status: String,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tail_preview: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WaitModeDto {
    Any,
    #[default]
    All,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TaskWaitParams {
    /// Task IDs to wait on.
    pub task_ids: Vec<String>,
    /// `"any"` returns on the first completion; `"all"` waits for all tasks (default: `"all"`).
    pub mode: Option<WaitModeDto>,
    /// Server-side blocking timeout in milliseconds (default: 60000).
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TaskWaitItemDto {
    pub task_id: String,
    pub pane_id: String,
    pub pane_name: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TaskWaitResponse {
    pub timed_out: bool,
    pub tasks: Vec<TaskWaitItemDto>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TaskLogsParams {
    pub task_id: String,
    /// Maximum trailing lines to return (default: 100).
    pub tail_lines: Option<usize>,
    /// Optional regex filter.
    pub grep: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct TaskLogsResponse {
    pub task_id: String,
    pub output: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TaskCancelParams {
    pub task_id: String,
    /// Whether to close the pane after cancelling (default: false to preserve failure context).
    pub close_pane: Option<bool>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SimpleActionResponse {
    pub id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ServiceStartParams {
    /// Stable service name used for idempotent reuse (e.g. `"web-dev"`).
    pub name: String,
    /// Command and arguments to start the service.
    pub cmd: Vec<String>,
    /// Optional working directory.
    pub cwd: Option<String>,
    /// Optional regex matched against streaming output to detect readiness.
    pub ready_pattern: Option<String>,
    /// Readiness timeout in milliseconds (default: 15000).
    pub ready_timeout_ms: Option<u64>,
    /// Semantic pane placement (default: `"auto"`).
    pub placement: Option<PlacementDto>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ServiceStartResponse {
    pub name: String,
    pub pane_id: String,
    pub pane_name: String,
    pub tab_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tab_name: Option<String>,
    pub placement: String,
    pub reused: bool,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_line: Option<String>,
    pub tail_preview: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ServiceLogsParams {
    pub name: String,
    /// Maximum trailing lines to return (default: 80).
    pub tail_lines: Option<usize>,
    /// Incremental cursor returned as `next_seq` from a previous call.
    pub since_seq: Option<u64>,
    /// Optional regex filter.
    pub grep: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ServiceLogsResponse {
    pub name: String,
    pub lines: Vec<String>,
    pub next_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropped_before: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ServiceRestartParams {
    pub name: String,
    /// Readiness timeout in milliseconds (default: 15000).
    pub ready_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ServiceStopParams {
    pub name: String,
    /// Whether to close the pane after stopping (default: true).
    pub close_pane: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ReplOpenParams {
    /// Stable REPL session name (e.g. `"gdb-main"`).
    pub name: String,
    /// Interactive program and arguments (e.g. `["python3"]`).
    pub cmd: Vec<String>,
    /// Optional working directory.
    pub cwd: Option<String>,
    /// Optional regex matching the waiting REPL prompt at end of line (e.g. `">>> "` or `"\(gdb\) "`).
    pub prompt_pattern: Option<String>,
    /// Semantic pane placement (default: `"auto"`).
    pub placement: Option<PlacementDto>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ReplExecParams {
    pub name: String,
    /// Text to inject via bracketed paste (mutually exclusive with `keys`).
    pub input: Option<String>,
    /// Special key chords sent via `send-keys` (mutually exclusive with `input`). Accepts Zellij
    /// space syntax (`"Ctrl c"`, `"Alt Enter"`, `"Enter"`) as well as `"Ctrl+C"` / `"C-c"`.
    pub keys: Option<Vec<String>>,
    /// Whether to press Enter after pasting `input` (default: true).
    pub send_enter: Option<bool>,
    /// Optional per-call prompt regex override.
    pub wait_for_prompt: Option<String>,
    /// Timeout in milliseconds (default: 15000).
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ReplResponse {
    pub name: String,
    pub pane_id: String,
    pub pane_name: String,
    pub tab_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tab_name: Option<String>,
    pub placement: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub output: String,
    pub timed_out: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct ReplCloseParams {
    pub name: String,
}

/// Cleanup scope accepted by `workspace_cleanup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CleanupScopeDto {
    /// Task panes that exited with code 0.
    Completed,
    /// Panes of tasks that exited with a non-zero code.
    Failed,
    /// Panes of tasks that were cancelled and kept open.
    Cancelled,
    /// Panes of REPLs that exited on their own.
    Exited,
    /// Panes of services that failed or were stopped without closing their pane.
    FailedServices,
    /// Every finished pane with an open pane, in any of the scopes above.
    AllInactive,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
pub struct WorkspaceCleanupParams {
    /// Scopes to clean; omitted means `["completed", "cancelled", "exited"]`.
    pub scopes: Option<Vec<CleanupScopeDto>>,
    /// Restrict cleanup to one Zellij tab id (default: the whole session).
    pub tab_id: Option<u32>,
    /// Preview the panes that would be closed without closing them (default: false).
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct WorkspaceCleanupResponse {
    pub dry_run: bool,
    pub pane_count: usize,
    /// Managed entity identifiers ("task:t_01", "svc:web-dev", "repl:py").
    pub entities: Vec<String>,
    /// Running entities of the requested kinds that cleanup deliberately left alone.
    pub skipped_active_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<WorkspaceAlertDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}
