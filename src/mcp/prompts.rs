//! Three-layer prompt engineering constants (`InitializeResult.instructions`, tool hints).

use crate::domain::ids::{ReplName, ServiceName, TaskId};

/// Layer 1 global routing instructions returned in MCP `InitializeResult.instructions`.
pub const SERVER_INSTRUCTIONS: &str = "\
You have access to a Zellij-backed execution workspace visible to the human user in real time.
NEVER run long builds, dev servers, or interactive tools via shell `nohup`, `&`, `timeout`, or `sleep` polling loops.

Route every execution request to the matching abstraction:
1. Finite commands with an exit code (build, test, lint, migrations) -> `task_run`.
   - Fast commands (<=3s) return output immediately.
   - Long commands return `task_id` with status=\"running\". Do other useful work or call `task_wait(task_ids)` to block server-side until completion. NEVER poll `task_logs` in a loop.
2. Long-lived background processes (dev servers like `pnpm dev`, proxies, watchers) -> `service_start`.
   - Always pass a stable `name` (e.g. \"web-dev\") and a `ready_pattern` (e.g. \"Local: http://\") when known.
   - Read incremental logs via `service_logs(name, since_seq=...)`.
3. Stateful interactive sessions (`ssh`, `gdb`, `python3`, `psql`) -> `repl_open` + `repl_exec`.
   - Always pass `prompt_pattern` when known (e.g. \"(gdb) \", \">>> \"). Patterns match only at the end of a waiting prompt line, never echoed command lines.
   - If the pane appears blocked on a password, 2FA, or interactive confirmation, STOP and tell the user which Zellij pane name to take over.

Layout & placement rules (layout is managed dynamically by the MCP server; never specify raw x/y/width coordinates):
- Default to `placement=\"auto\"` unless context strongly favors a specific layout.
- The server enforces hard minimum pane dimensions (`--min-pane-cols` / `--min-pane-rows`) before splitting the focused pane and automatically downgrades to `background_tab` (tasks/services) or `floating` (REPLs) when a split would squeeze the focused pane below those bounds. Check the returned `placement`, `tab_id`, `tab_name`, and `hint` fields.
- Check `workspace_status.viewport` before overriding placement:
  - Narrow (<160 cols): prefer `floating` for REPLs and `background_tab` for tasks/services.
  - Wide (160-219 cols): prefer `split_right` in the current tab for active REPLs or focal builds when `tiled_panes_in_current_tab <= 1`; otherwise `stacked` or `floating`. Keep long-lived services in `background_tab`.
  - Ultrawide (>=220 cols): you may keep tasks, REPLs, and a stacked service column inside the default tab for single-screen visibility.
  - Headless sessions (0 attached clients): `background_tab` and `stacked` automatically fall back to `floating` until a terminal client attaches.

Human observability rule:
- Keep failed task panes open (`close_pane=false`) so the user can inspect the colored terminal output directly.
- Call `workspace_status` first if resuming a conversation or unsure what services are already running.";

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
