//! Three-layer prompt engineering constants (`InitializeResult.instructions`, tool hints).

use crate::domain::ids::{ReplName, ServiceName, TaskId};

/// Layer 1 global routing instructions returned in MCP `InitializeResult.instructions`.
///
/// Line 1 is a self-contained summary because clients such as Pi (`codemode`/`deferred`) inject
/// only the first line into the system prompt; the body stays compact to avoid truncation in
/// clients with shared prompt budgets.
pub const SERVER_INSTRUCTIONS: &str = "\
Human-visible Zellij workspace (no `nohup`/`&`/`sleep` polling): `task_run`+`task_wait` for builds/tests, `service_start` for servers/watchers, `repl_open`+`repl_exec` for REPL/SSH, `workspace_status` to inspect; read full instructions for rules.

Routing rules:
- Finite commands (`task_run`): fast commands (<=3s) return output immediately; otherwise do parallel work or block server-side with `task_wait(task_ids)`. Never poll `task_logs` in a loop.
- Background processes (`service_start`): pass a stable `name` and `ready_pattern` when known; read incremental logs with `service_logs(name, since_seq=...)`.
- Interactive sessions (`repl_open` + `repl_exec`): pass `prompt_pattern` when known (matches only end of a waiting prompt line, never echoed input). If blocked on password, 2FA, or confirmation, STOP and ask the user to take over that Zellij pane name.

Layout & observability:
- Default to `placement=\"auto\"` (never raw x/y/width). The server enforces min pane bounds, downgrading crowded splits or headless tabs/stacks automatically; inspect `workspace_status.viewport` before overriding.
- Keep failed task panes open (`close_pane=false`) so the human can inspect terminal output, and call `workspace_status` first when resuming or checking active services.";

/// Layer 3 dynamic hint when a task exceeds the synchronous fast-path wait window.
pub fn hint_task_running(task_id: &TaskId) -> String {
    format!(
        "Still running. Continue parallel work or call task_wait(['{task_id}']) instead of sleeping."
    )
}

/// Layer 3 dynamic hint when a task exits with a non-zero status.
pub fn hint_task_failed(pane_title: &str) -> String {
    format!(
        "Task failed. Pane '{pane_title}' is kept open and highlighted for human inspection; use task_logs for full output."
    )
}

/// Layer 3 dynamic hint when a service is already running and reused idempotently.
pub fn hint_service_reused(name: &ServiceName) -> String {
    format!(
        "Service '{name}' is already active and was reused. Use service_logs(name='{name}', since_seq=...) to read output or service_restart to reload."
    )
}

/// Layer 3 dynamic hint when a REPL appears blocked on interactive authentication or confirmation.
pub fn hint_repl_input_required(name: &ReplName, pane_title: &str) -> String {
    format!(
        "REPL '{name}' appears blocked waiting for human input (password, 2FA, or confirmation). Stop and ask the user to take over Zellij pane '{pane_title}'."
    )
}

#[cfg(test)]
mod tests {
    use super::SERVER_INSTRUCTIONS;

    #[test]
    fn server_instructions_first_line_is_self_contained_summary_and_body_is_compact() {
        let first_line = SERVER_INSTRUCTIONS.lines().next().unwrap_or("");
        assert!(!first_line.is_empty());
        assert!(
            first_line.len() <= 280,
            "first line too long ({} bytes): {first_line}",
            first_line.len()
        );
        for required in [
            "task_run",
            "task_wait",
            "service_start",
            "repl_open",
            "repl_exec",
            "workspace_status",
        ] {
            assert!(
                first_line.contains(required),
                "first line must mention '{required}' for single-line MCP clients"
            );
        }
        assert!(
            SERVER_INSTRUCTIONS.len() <= 1500,
            "full instructions too long ({} bytes)",
            SERVER_INSTRUCTIONS.len()
        );
    }
}
