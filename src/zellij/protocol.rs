//! Anti-corruption layer parsing Zellij 0.46.1 JSON / NDJSON output into domain types.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

use crate::domain::ids::{PaneId, TabId, TerminalPaneId};
use crate::domain::layout::ViewportSnapshot;
use crate::logs::strip_ansi;
use crate::state::{ObservedPane, ObservedTab, ZellijEvent};

/// Minimum Zellij version required for `zellij events` and JSON introspection.
pub const MIN_ZELLIJ_VERSION: (u32, u32, u32) = (0, 46, 1);

/// Returns `true` when Zellij stderr indicates the target session does not exist.
///
/// In Zellij 0.46, `zellij --session <missing> action ...` exits with status 0 while printing
/// `Session '<name>' not found.` to stderr and the active session list to stdout.
pub fn is_session_not_found_stderr(stderr: &str) -> bool {
    let clean = strip_ansi(stderr);
    clean.contains("No active zellij sessions found")
        || (clean.contains("Session") && clean.contains("not found"))
}

/// Checks whether `session` appears as a live (non-exited) entry in `zellij list-sessions -n`.
pub fn is_active_session_in_list(stdout: &str, session: &str) -> bool {
    stdout.lines().any(|line| {
        let clean = strip_ansi(line);
        let mut parts = clean.split_whitespace();
        let Some(name) = parts.next() else {
            return false;
        };
        name == session && !clean.contains("(EXITED")
    })
}

/// Parsed viewport update frame emitted by `zellij subscribe --format json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFrame {
    pub pane_id: TerminalPaneId,
    pub viewport_lines: Vec<String>,
}

/// Parses `zellij --version` output (e.g. `zellij 0.46.1`) into `(major, minor, patch)`.
pub fn parse_zellij_version(stdout: &str) -> Result<(u32, u32, u32)> {
    let token = stdout
        .split_whitespace()
        .find(|part| part.chars().next().is_some_and(|c| c.is_ascii_digit()))
        .ok_or_else(|| anyhow!("could not find version number in {stdout:?}"))?;

    let core = token.split('-').next().unwrap_or(token);
    let mut parts = core.split('.');
    let major = parts
        .next()
        .ok_or_else(|| anyhow!("missing major version in {token:?}"))?
        .parse::<u32>()
        .context("invalid major version")?;
    let minor = parts
        .next()
        .ok_or_else(|| anyhow!("missing minor version in {token:?}"))?
        .parse::<u32>()
        .context("invalid minor version")?;
    let patch = parts
        .next()
        .ok_or_else(|| anyhow!("missing patch version in {token:?}"))?
        .parse::<u32>()
        .context("invalid patch version")?;

    Ok((major, minor, patch))
}

/// Parses stdout of `zellij action new-tab` (single tab ID integer).
pub fn parse_new_tab_output(stdout: &str) -> Result<TabId> {
    let clean = strip_ansi(stdout);
    let trimmed = clean.trim();
    let id = trimmed
        .parse::<u32>()
        .with_context(|| format!("invalid tab id from new-tab: {trimmed:?}"))?;
    Ok(TabId::new(id))
}

/// Parses stdout of `zellij action new-pane` (`terminal_<id>`).
pub fn parse_new_pane_output(stdout: &str) -> Result<TerminalPaneId> {
    let clean = strip_ansi(stdout);
    let trimmed = clean.trim();
    trimmed
        .parse::<TerminalPaneId>()
        .with_context(|| format!("invalid pane id from new-pane: {trimmed:?}"))
}

#[derive(Debug, Deserialize)]
struct RawTabInfo {
    tab_id: u32,
    name: String,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    viewport_columns: u16,
    #[serde(default)]
    viewport_rows: u16,
    #[serde(default)]
    selectable_tiled_panes_count: u16,
}

impl From<RawTabInfo> for ObservedTab {
    fn from(raw: RawTabInfo) -> Self {
        Self {
            tab_id: TabId::new(raw.tab_id),
            name: raw.name,
            active: raw.active,
            viewport: ViewportSnapshot::new(
                raw.viewport_columns,
                raw.viewport_rows,
                raw.selectable_tiled_panes_count,
            ),
        }
    }
}

/// Parses JSON from `zellij action list-tabs --all --json`.
pub fn parse_list_tabs_json(json: &str) -> Result<Vec<ObservedTab>> {
    let raw: Vec<RawTabInfo> =
        serde_json::from_str(json).context("failed to parse list-tabs JSON")?;
    Ok(raw.into_iter().map(ObservedTab::from).collect())
}

/// Parses JSON from `zellij action current-tab-info --json`.
pub fn parse_current_tab_info_json(json: &str) -> Result<ObservedTab> {
    let raw: RawTabInfo =
        serde_json::from_str(json).context("failed to parse current-tab-info JSON")?;
    Ok(ObservedTab::from(raw))
}

#[derive(Debug, Deserialize)]
struct RawPaneInfo {
    id: u32,
    #[serde(default)]
    is_plugin: bool,
    #[serde(default)]
    is_floating: bool,
    #[serde(default)]
    title: String,
    #[serde(default)]
    exited: bool,
    #[serde(default)]
    exit_status: Option<i32>,
    #[serde(default)]
    tab_id: u32,
    #[serde(default)]
    pane_cwd: Option<String>,
    #[serde(default)]
    terminal_command: Option<String>,
    #[serde(default)]
    pane_command: Option<String>,
}

/// Parses JSON from `zellij action list-panes --all --json`.
pub fn parse_list_panes_json(json: &str) -> Result<Vec<ObservedPane>> {
    let raw: Vec<RawPaneInfo> =
        serde_json::from_str(json).context("failed to parse list-panes JSON")?;
    Ok(raw
        .into_iter()
        .filter(|p| !p.is_plugin)
        .map(|p| ObservedPane {
            pane_id: TerminalPaneId::new(p.id),
            tab_id: TabId::new(p.tab_id),
            title: p.title,
            is_floating: p.is_floating,
            exited: p.exited,
            exit_status: p.exit_status,
            cwd: p.pane_cwd.map(PathBuf::from),
            command: p.terminal_command.or(p.pane_command),
        })
        .collect())
}

#[derive(Debug, Deserialize)]
struct RawEvent {
    event: String,
    #[serde(default)]
    pane_id: Option<String>,
    #[serde(default)]
    tab_id: Option<u32>,
    #[serde(default)]
    exit_status: Option<i32>,
}

/// Parses a single NDJSON line from `zellij events`.
///
/// Returns `Ok(None)` for valid Zellij events that this server does not act on (such as plugin
/// pane events or ignored event types).
pub fn parse_event_line(line: &str) -> Result<Option<ZellijEvent>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let raw: RawEvent =
        serde_json::from_str(trimmed).with_context(|| format!("invalid event JSON: {trimmed}"))?;

    match raw.event.as_str() {
        "pane_exited" => {
            let Some(pane_str) = raw.pane_id else {
                return Ok(None);
            };
            let Some(pane_id) = pane_str.parse::<PaneId>()?.as_terminal() else {
                return Ok(None);
            };
            Ok(Some(ZellijEvent::PaneExited {
                pane_id,
                exit_code: raw.exit_status,
            }))
        }
        "pane_closed" => {
            let Some(pane_str) = raw.pane_id else {
                return Ok(None);
            };
            let Some(pane_id) = pane_str.parse::<PaneId>()?.as_terminal() else {
                return Ok(None);
            };
            Ok(Some(ZellijEvent::PaneClosed { pane_id }))
        }
        "tab_focused" => Ok(raw.tab_id.map(|id| ZellijEvent::TabFocused {
            tab_id: TabId::new(id),
        })),
        "tab_closed" => Ok(raw.tab_id.map(|id| ZellijEvent::TabClosed {
            tab_id: TabId::new(id),
        })),
        "session_ending" => Ok(Some(ZellijEvent::SessionEnding)),
        _ => Ok(None),
    }
}

#[derive(Debug, Deserialize)]
struct RawSubscribeFrame {
    event: String,
    #[serde(default)]
    pane_id: Option<String>,
    #[serde(default)]
    scrollback: Option<Vec<String>>,
    #[serde(default)]
    viewport: Vec<String>,
}

/// Parses a single NDJSON line from `zellij subscribe --format json`.
pub fn parse_subscribe_line(line: &str) -> Result<Option<SubscribeFrame>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let raw: RawSubscribeFrame = serde_json::from_str(trimmed)
        .with_context(|| format!("invalid subscribe JSON: {trimmed}"))?;
    if raw.event != "pane_update" {
        return Ok(None);
    }
    let Some(pane_str) = raw.pane_id else {
        return Ok(None);
    };
    let Some(pane_id) = pane_str.parse::<PaneId>()?.as_terminal() else {
        return Ok(None);
    };
    let mut viewport_lines = raw.scrollback.unwrap_or_default();
    viewport_lines.extend(raw.viewport);
    Ok(Some(SubscribeFrame {
        pane_id,
        viewport_lines,
    }))
}
