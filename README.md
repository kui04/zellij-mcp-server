# zellij-mcp-server

An asynchronous Model Context Protocol (MCP) server over stdio that orchestrates finite tasks,
long-lived background services, and stateful interactive REPL sessions inside real
[Zellij](https://zellij.dev) terminal panes.

## Features

- **13 high-level MCP tools** covering finite commands (`task_run`, `task_wait`, `task_logs`,
  `task_cancel`), long-lived services (`service_start`, `service_logs`, `service_restart`,
  `service_stop`), interactive REPLs (`repl_open`, `repl_exec`, `repl_close`), and workspace
  inspection and cleanup (`workspace_status`, `workspace_cleanup`).
- **Event-driven waiting**: uses `zellij events` and `zellij subscribe` to block server-side until
  commands exit, readiness regexes match, or REPL prompts return, avoiding LLM sleep/poll loops.
- **Incremental, ANSI-stripped logs**: maintains bounded per-entity log buffers with `since_seq`
  cursors and optional regex `grep` filtering.
- **Viewport-adaptive pane placement**: inspects live Zellij tab dimensions and routes panes across
  `narrow` (`<= 159` columns), `wide` (`160..219` columns), and `ultrawide` (`>= 220` columns)
  layout tiers, including managed `agent:tasks` and `agent:services` tabs.
- **Safe session lifecycle**: attaches to `--session` / `ZELLIJ_MCP_SESSION` or inherits
  `$ZELLIJ_SESSION_NAME` without tearing down user-owned sessions; when started outside Zellij,
  creates a unique `mcp-<cwd>-<hex>` session and cleans it up on exit unless `--keep-session` is
  set.
- **Bounded pane retention**: finished task, service, and REPL panes close on per-class quotas (or
  on `workspace_cleanup`), and scrollback is backfilled first so `task_logs` / `service_logs` keep
  working after the pane is gone.
- **Optional human confirmation gate**: `--confirm-destructive` gates `task_cancel`, `service_stop`,
  and `repl_close` behind `zellij prompt confirm`.

## Requirements

- **Zellij `>= 0.46.1`** available on `PATH` (or configured via `--zellij-bin` / `ZELLIJ_BIN`), as
  `zellij events` was introduced in `0.46.1`.
- **Rust toolchain** (Edition 2024) if building from source.

## Installation

### Prebuilt binaries

Download an archive for your 64-bit platform from
[GitHub Releases](https://github.com/kui04/zellij-mcp-server/releases):

- Linux (fully static musl binaries): `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`
- macOS: `x86_64-apple-darwin`, `aarch64-apple-darwin`
- Windows (static MSVC CRT): `x86_64-pc-windows-msvc`, `aarch64-pc-windows-msvc`

Extract the binary to a directory on your `PATH` (for example `~/.local/bin/zellij-mcp-server`).

### Build from source

```bash
cargo build --release
```

The release binary is written to `target/release/zellij-mcp-server`.

## Usage

`zellij-mcp-server` speaks MCP over `stdin`/`stdout` and writes structured logs (`tracing` via
`RUST_LOG`) to `stderr`.

```text
zellij-mcp-server [OPTIONS]
```

### CLI flags and environment variables

| Flag                             | Environment Variable               | Default  | Description                                                                            |
| -------------------------------- | ---------------------------------- | -------- | -------------------------------------------------------------------------------------- |
| `--session <SESSION>`            | `ZELLIJ_MCP_SESSION`               | Auto     | Target Zellij session name (overrides `$ZELLIJ_SESSION_NAME`).                         |
| `--zellij-bin <PATH>`            | `ZELLIJ_BIN`                       | `zellij` | Path to the `zellij` executable (`>= 0.46.1`).                                         |
| `--confirm-destructive`          | -                                  | `false`  | Prompt via `zellij prompt confirm` before `task_cancel`, `service_stop`, `repl_close`. |
| `--keep-session`                 | -                                  | `false`  | Keep a server-created Zellij session alive after the MCP server exits.                 |
| `--min-pane-cols <COLS>`         | `ZELLIJ_MCP_MIN_PANE_COLS`         | `80`     | Hard minimum columns per pane on horizontal split (`0` disables).                      |
| `--min-pane-rows <ROWS>`         | `ZELLIJ_MCP_MIN_PANE_ROWS`         | `16`     | Hard minimum rows per pane on vertical split (`0` disables).                           |
| `--keep-completed-tasks <QUOTA>` | `ZELLIJ_MCP_KEEP_COMPLETED_TASKS`  | `1`      | Completed task panes kept open: `0`, N, or `unlimited`.                                |
| `--keep-failed-tasks <QUOTA>`    | `ZELLIJ_MCP_KEEP_FAILED_TASKS`     | `2`      | Failed and cancelled task panes kept open: `0`, N, or `unlimited`.                     |
| `--keep-exited-repls <QUOTA>`    | `ZELLIJ_MCP_KEEP_EXITED_REPLS`     | `0`      | Exited REPL panes kept open: `0`, N, or `unlimited`.                                   |
| `--auto-close-fast-tasks <BOOL>` | `ZELLIJ_MCP_AUTO_CLOSE_FAST_TASKS` | `true`   | Close the pane of a task that succeeds inside the `task_run` wait window.              |
| -                                | `RUST_LOG`                         | -        | Standard `tracing_subscriber` filter (logs are emitted to `stderr`).                   |

Session resolution precedence at startup:

1. `--session`
2. `ZELLIJ_MCP_SESSION`
3. `$ZELLIJ_SESSION_NAME` (inherited when launched inside an active Zellij session; never killed on
   server shutdown)
4. Generated unique session `mcp-<cwd-slug>-<4-hex>` (killed on server shutdown unless
   `--keep-session` is set)

### MCP client configuration

Add `zellij-mcp-server` to your MCP client configuration (for example `.pi/mcp.json` or equivalent):

```json
{
  "mcpServers": {
    "zellij": {
      "command": "zellij-mcp-server",
      "args": [],
      "env": {
        "RUST_LOG": "zellij_mcp_server=info"
      }
    }
  }
}
```

## MCP Tools

### Workspace

| Tool                | Parameters                       | Description                                                                                                                                                                                                                                                                 |
| ------------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `workspace_status`  | `session?`                       | Lists managed tasks, services, and REPLs (each with `pane_closed`), live viewport dimensions, `layout_tier`, `managed_tabs`, and `attach_command`. Retains up to 50 closed records, and hints at `workspace_cleanup` once more than 4 finished panes stay open.             |
| `workspace_cleanup` | `scopes?`, `tab_id?`, `dry_run?` | Closes finished panes while keeping their logs readable. `scopes` defaults to `["completed", "cancelled", "exited"]`; also accepts `failed`, `failed_services`, and `all_inactive`. Never touches running entities, and `dry_run = true` previews without closing anything. |

### Tasks (finite commands with exit codes)

| Tool          | Parameters                                       | Description                                                                                                                                        |
| ------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `task_run`    | `cmd`, `cwd?`, `name?`, `wait_ms?`, `placement?` | Runs an argv command in a Zellij pane. Returns immediately if finished within `wait_ms` (default `3000`); otherwise returns `running` + `task_id`. |
| `task_wait`   | `task_ids`, `mode?`, `timeout_ms?`               | Blocks server-side until `any` or `all` (default) tasks finish or `timeout_ms` (default `60000`) elapses.                                          |
| `task_logs`   | `task_id`, `tail_lines?`, `grep?`                | Reads ANSI-stripped task output (`tail_lines` default `100`) with optional regex filtering.                                                        |
| `task_cancel` | `task_id`, `close_pane?`                         | Sends `Ctrl+C` to cancel a running task. Keeps the pane open by default (`close_pane = false`).                                                    |

### Services (long-lived background processes)

| Tool              | Parameters                                                                 | Description                                                                                                                       |
| ----------------- | -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `service_start`   | `name`, `cmd`, `cwd?`, `ready_pattern?`, `ready_timeout_ms?`, `placement?` | Idempotently starts or reuses a named service, optionally waiting up to `ready_timeout_ms` (default `15000`) for `ready_pattern`. |
| `service_logs`    | `name`, `tail_lines?`, `since_seq?`, `grep?`                               | Reads incremental service logs (`tail_lines` default `80`) using `since_seq` / `next_seq` cursors.                                |
| `service_restart` | `name`, `ready_timeout_ms?`                                                | Stops and restarts a service using its recorded configuration (`ready_timeout_ms` default `15000`).                               |
| `service_stop`    | `name`, `close_pane?`                                                      | Gracefully stops a service and closes its pane by default (`close_pane = true`).                                                  |

### REPLs (stateful interactive sessions)

| Tool         | Parameters                                                                  | Description                                                                                                                                                                                                          |
| ------------ | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `repl_open`  | `name`, `cmd`, `cwd?`, `prompt_pattern?`, `placement?`                      | Opens or reuses a stateful interactive session (`python3`, `gdb`, `psql`, `ssh`), waits for the initial prompt or quiet settle, and returns `exit_code` if the process exits immediately.                            |
| `repl_exec`  | `name`, `input?`, `keys?`, `send_enter?`, `wait_for_prompt?`, `timeout_ms?` | Injects bracketed-paste `input` or `send-keys` chords (accepts `"Ctrl c"`, `"Ctrl+C"`, `"C-c"`, `"Alt+Enter"`, `"Enter"`) under a per-pane write lock and returns incremental output (`timeout_ms` default `15000`). |
| `repl_close` | `name`                                                                      | Sends `Ctrl+D`, closes the REPL pane, and releases its write lock and stream subscription.                                                                                                                           |

## Placement and Layout Tiers

`task_run`, `service_start`, and `repl_open` accept `placement`:

- `auto` (default): adapts to the active Zellij tab viewport width
- `split_right`
- `split_down`
- `floating`
- `stacked` (falls back to `floating` in headless sessions when no terminal client is attached)
- `background_tab` (routes tasks to `agent:tasks` and services to `agent:services`, closing the
  default shell pane of a fresh tab once the first managed pane exists; falls back to `floating` in
  headless sessions with 0 attached clients)

Layout tiers reported by `workspace_status.viewport.layout_tier`:

- `narrow`: `<= 159` columns
- `wide`: `160..219` columns
- `ultrawide`: `>= 220` columns

Before splitting the focused pane (`split_right` or `split_down`), the server checks the focused
pane's live dimensions against `--min-pane-cols` (default `80`) and `--min-pane-rows` (default
`16`). If a split would squeeze either half below those limits (or if Zellij reports
`No room for a new pane`), the server automatically downgrades tasks/services to `background_tab`
and REPLs to `floating`, returning the effective `placement`, `tab_id`, `tab_name`, and a `hint`
explaining the change.

> Note: Commands run inside Zellij panes are spawned by the Zellij server process and therefore
> inherit the Zellij server's environment and `PATH` rather than the MCP client's `PATH`.

## Development

Enter the `devenv` shell (or use `direnv allow`) so `rustc`, `cargo`, nightly `rustfmt`, `prettier`,
and `zellij 0.46.1` are on `PATH`, then run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

`cargo test --all-features` runs unit/domain tests (`core_tests`), deterministic mocked orchestrator
tests (`orchestrator_tests`, gated on `test-util`), and real-Zellij end-to-end tests
(`zellij_e2e_tests`).
