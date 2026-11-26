//! Strong domain identifiers, smart constructors, and pane title codec.

use std::fmt;
use std::str::FromStr;

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::domain::error::ParseError;

fn validate_slug(field: &'static str, raw: String) -> Result<String, ParseError> {
    if raw.is_empty() {
        return Err(ParseError::Empty { field });
    }
    if !raw
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(ParseError::InvalidCharacters { field, value: raw });
    }
    Ok(raw)
}

fn sanitize_slug_fragment(raw: &str, max_len: usize) -> String {
    let mut out = String::with_capacity(raw.len().min(max_len));
    let mut last_dash = false;
    for ch in raw.chars() {
        if out.len() >= max_len {
            break;
        }
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !out.is_empty() {
            out.push('-');
            last_dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_owned();
    if trimmed.is_empty() {
        "workspace".to_owned()
    } else {
        trimmed
    }
}

/// Validated Zellij session name (non-empty, no whitespace or control characters).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionName(String);

impl SessionName {
    /// Parses a raw session name, rejecting empty strings and whitespace/control characters.
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseError> {
        let value = raw.into();
        if value.is_empty() {
            return Err(ParseError::Empty { field: "session" });
        }
        if value.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(ParseError::InvalidCharacters {
                field: "session",
                value,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for SessionName {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SessionName> for String {
    fn from(value: SessionName) -> Self {
        value.0
    }
}

/// How the server resolved its target Zellij session at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionResolution {
    /// Explicitly configured via `--session` or `ZELLIJ_MCP_SESSION`.
    Explicit(SessionName),
    /// Inherited from `$ZELLIJ_SESSION_NAME` when running inside an active Zellij session.
    Inherited(SessionName),
    /// Generated unique session name when running outside Zellij.
    GeneratedUnique(SessionName),
}

impl SessionResolution {
    pub fn session_name(&self) -> &SessionName {
        match self {
            Self::Explicit(name) | Self::Inherited(name) | Self::GeneratedUnique(name) => name,
        }
    }

    /// Resolves the target session from CLI arg, env vars, or a generated unique name.
    pub fn resolve(
        explicit_cli: Option<String>,
        env_mcp_session: Option<String>,
        env_zellij_session: Option<String>,
        cwd_slug: &str,
        unique_suffix: u32,
    ) -> Result<Self, ParseError> {
        if let Some(raw) = explicit_cli.filter(|s| !s.is_empty()) {
            return Ok(Self::Explicit(SessionName::parse(raw)?));
        }
        if let Some(raw) = env_mcp_session.filter(|s| !s.is_empty()) {
            return Ok(Self::Explicit(SessionName::parse(raw)?));
        }
        if let Some(raw) = env_zellij_session.filter(|s| !s.is_empty()) {
            return Ok(Self::Inherited(SessionName::parse(raw)?));
        }
        let slug = sanitize_slug_fragment(cwd_slug, 24);
        let generated = format!("mcp-{slug}-{:04x}", unique_suffix & 0xffff);
        Ok(Self::GeneratedUnique(SessionName::parse(generated)?))
    }
}

/// Stable Zellij tab identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TabId(u32);

impl TabId {
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for TabId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Any Zellij pane identifier (`terminal_<id>` or `plugin_<id>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PaneId {
    Terminal(TerminalPaneId),
    Plugin(u32),
}

impl PaneId {
    pub const fn as_terminal(self) -> Option<TerminalPaneId> {
        match self {
            Self::Terminal(id) => Some(id),
            Self::Plugin(_) => None,
        }
    }
}

impl fmt::Display for PaneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Terminal(id) => write!(f, "{id}"),
            Self::Plugin(id) => write!(f, "plugin_{id}"),
        }
    }
}

impl FromStr for PaneId {
    type Err = ParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let trimmed = raw.trim();
        if let Some(num) = trimmed.strip_prefix("terminal_") {
            let id = num.parse::<u32>().map_err(|_| ParseError::InvalidPaneId {
                raw: raw.to_owned(),
            })?;
            return Ok(Self::Terminal(TerminalPaneId::new(id)));
        }
        if let Some(num) = trimmed.strip_prefix("plugin_") {
            let id = num.parse::<u32>().map_err(|_| ParseError::InvalidPaneId {
                raw: raw.to_owned(),
            })?;
            return Ok(Self::Plugin(id));
        }
        let id = trimmed
            .parse::<u32>()
            .map_err(|_| ParseError::InvalidPaneId {
                raw: raw.to_owned(),
            })?;
        Ok(Self::Terminal(TerminalPaneId::new(id)))
    }
}

/// Identifier of a Zellij terminal pane (`terminal_<id>`).
///
/// Managed tasks, services, and REPLs only operate on terminal panes; using a distinct type
/// prevents accidentally targeting plugin panes with `paste` or `send-keys`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TerminalPaneId(u32);

impl TerminalPaneId {
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn cli_arg(self) -> String {
        format!("terminal_{}", self.0)
    }
}

impl fmt::Display for TerminalPaneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "terminal_{}", self.0)
    }
}

impl FromStr for TerminalPaneId {
    type Err = ParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.parse::<PaneId>()? {
            PaneId::Terminal(id) => Ok(id),
            PaneId::Plugin(_) => Err(ParseError::InvalidPaneId {
                raw: raw.to_owned(),
            }),
        }
    }
}

impl Serialize for TerminalPaneId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TerminalPaneId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

/// Semantic identifier for a finite task (e.g. `t_01`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TaskId(String);

impl TaskId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseError> {
        Ok(Self(validate_slug("task_id", raw.into())?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for TaskId {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<TaskId> for String {
    fn from(value: TaskId) -> Self {
        value.0
    }
}

/// Stable semantic name for a long-lived service (e.g. `web-dev`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ServiceName(String);

impl ServiceName {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseError> {
        Ok(Self(validate_slug("service_name", raw.into())?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ServiceName {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ServiceName> for String {
    fn from(value: ServiceName) -> Self {
        value.0
    }
}

/// Stable semantic name for a stateful interactive REPL session (e.g. `gdb-main`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReplName(String);

impl ReplName {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseError> {
        Ok(Self(validate_slug("repl_name", raw.into())?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ReplName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ReplName {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ReplName> for String {
    fn from(value: ReplName) -> Self {
        value.0
    }
}

/// Unified key across the three managed entity namespaces.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum ManagedKey {
    Task(TaskId),
    Service(ServiceName),
    Repl(ReplName),
}

impl fmt::Display for ManagedKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task(id) => write!(f, "task:{id}"),
            Self::Service(name) => write!(f, "svc:{name}"),
            Self::Repl(name) => write!(f, "repl:{name}"),
        }
    }
}

/// Non-empty executable command (`argv[0]` + arguments).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct CommandSpec {
    program: String,
    args: Vec<String>,
}

impl CommandSpec {
    /// Returns a placeholder command specification used only when reconciling an external pane
    /// whose original argv is unavailable.
    pub fn unknown() -> Self {
        Self {
            program: "unknown".to_owned(),
            args: Vec::new(),
        }
    }

    pub fn parse(argv: Vec<String>) -> Result<Self, ParseError> {
        let mut iter = argv.into_iter();
        let Some(program) = iter.next() else {
            return Err(ParseError::Empty { field: "cmd" });
        };
        if program.trim().is_empty() {
            return Err(ParseError::Empty { field: "cmd[0]" });
        }
        Ok(Self {
            program,
            args: iter.collect(),
        })
    }

    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// Returns the full argv vector passed after `--` to `zellij action new-pane`.
    pub fn to_zellij_argv(&self) -> Vec<String> {
        let mut argv = Vec::with_capacity(1 + self.args.len());
        argv.push(self.program.clone());
        argv.extend(self.args.iter().cloned());
        argv
    }
}

impl TryFrom<Vec<String>> for CommandSpec {
    type Error = ParseError;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<CommandSpec> for Vec<String> {
    fn from(value: CommandSpec) -> Self {
        let mut out = Vec::with_capacity(1 + value.args.len());
        out.push(value.program);
        out.extend(value.args);
        out
    }
}

/// Validated, pre-compiled regular expression paired with its original source string.
#[derive(Debug, Clone)]
pub struct CompiledPattern {
    raw: String,
    regex: Regex,
}

impl CompiledPattern {
    pub fn parse(field: &'static str, raw: impl Into<String>) -> Result<Self, ParseError> {
        let raw = raw.into();
        let regex = Regex::new(&raw).map_err(|err| ParseError::InvalidRegex {
            field,
            reason: err.to_string(),
        })?;
        Ok(Self { raw, regex })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn is_match(&self, text: &str) -> bool {
        self.regex.is_match(text)
    }

    /// Matches a waiting terminal prompt line, tolerating Zellij's stripping of trailing spaces
    /// (e.g. `">>> "` dumped as `">>>"`) while rejecting echoed command lines that carry
    /// non-whitespace text after the prompt (e.g. `">>> print('AAA')"`).
    pub fn matches_terminal_line(&self, line: &str) -> bool {
        if self
            .regex
            .find_iter(line)
            .any(|m| line[m.end()..].trim().is_empty())
        {
            return true;
        }
        if !line.ends_with(' ') {
            let mut with_space = String::with_capacity(line.len() + 1);
            with_space.push_str(line);
            with_space.push(' ');
            return self
                .regex
                .find_iter(&with_space)
                .any(|m| with_space[m.end()..].trim().is_empty());
        }
        false
    }
}

impl PartialEq for CompiledPattern {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl Eq for CompiledPattern {}

/// Monotonically increasing sequence number for incremental log reads.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct LogSeq(u64);

impl LogSeq {
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Non-empty key chord passed to `zellij action send-keys` (e.g. `"Ctrl c"`, `"Enter"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KeyChord(String);

impl KeyChord {
    /// Parses and normalizes a key chord into Zellij `send-keys` syntax (e.g. `"Ctrl+C"` and
    /// `"C-c"` normalize to `"Ctrl c"`, `"Alt+Enter"` normalizes to `"Alt Enter"`).
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseError> {
        let trimmed = raw.into().trim().to_owned();
        if trimmed.is_empty() {
            return Err(ParseError::Empty { field: "keys" });
        }
        let normalized = normalize_key_chord(&trimmed);
        if normalized.is_empty() {
            return Err(ParseError::Empty { field: "keys" });
        }
        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn normalize_modifier(token: &str) -> Option<&'static str> {
    // Preserve Emacs case convention for single-letter `S` (Shift) vs `s` (Super).
    if token == "S" {
        return Some("Shift");
    }
    if token == "s" {
        return Some("Super");
    }
    match token.to_ascii_lowercase().as_str() {
        "ctrl" | "control" | "c" => Some("Ctrl"),
        "alt" | "meta" | "opt" | "option" | "m" => Some("Alt"),
        "shift" => Some("Shift"),
        "super" | "cmd" | "command" | "win" => Some("Super"),
        _ => None,
    }
}

fn normalize_named_key(token: &str) -> Option<&'static str> {
    match token.to_ascii_lowercase().as_str() {
        "enter" | "return" | "cr" => Some("Enter"),
        "esc" | "escape" => Some("Esc"),
        "tab" => Some("Tab"),
        "backspace" | "bs" => Some("Backspace"),
        "space" => Some("Space"),
        "del" | "delete" => Some("Delete"),
        "ins" | "insert" => Some("Insert"),
        "home" => Some("Home"),
        "end" => Some("End"),
        "pageup" | "pgup" | "page up" | "page_up" => Some("PageUp"),
        "pagedown" | "pgdn" | "page down" | "page_down" => Some("PageDown"),
        "up" => Some("Up"),
        "down" => Some("Down"),
        "left" => Some("Left"),
        "right" => Some("Right"),
        _ => None,
    }
}

fn split_delimited_chord(raw: &str, delim: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = raw;
    let delim_str = delim.to_string();
    while let Some(idx) = rest.find(delim) {
        let head = &rest[..idx];
        if !head.is_empty() {
            parts.push(head.to_owned());
        }
        rest = &rest[idx + 1..];
        if rest == delim_str {
            parts.push(delim_str.clone());
            rest = "";
            break;
        }
    }
    if !rest.is_empty() {
        parts.push(rest.to_owned());
    }
    parts
}

fn normalize_key_chord(raw: &str) -> String {
    if raw == "+" || raw == "-" {
        return raw.to_owned();
    }

    if let Some(named) = normalize_named_key(raw) {
        return named.to_owned();
    }

    let tokens: Vec<String> = if raw.contains('+') && !raw.contains(char::is_whitespace) {
        split_delimited_chord(raw, '+')
    } else if !raw.contains(char::is_whitespace)
        && raw.contains('-')
        && raw
            .split('-')
            .next()
            .is_some_and(|first| normalize_modifier(first).is_some())
    {
        split_delimited_chord(raw, '-')
    } else {
        raw.split_whitespace().map(ToOwned::to_owned).collect()
    };

    if tokens.is_empty() {
        return String::new();
    }

    if tokens.len() == 1 {
        let tok = &tokens[0];
        return normalize_named_key(tok)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| tok.clone());
    }

    // Merge trailing two-word named keys such as `"Ctrl page up"` -> `["Ctrl", "PageUp"]`.
    let mut tokens = tokens;
    if tokens.len() >= 2 {
        let tail_pair = format!("{} {}", tokens[tokens.len() - 2], tokens[tokens.len() - 1]);
        if let Some(named) = normalize_named_key(&tail_pair) {
            let new_len = tokens.len() - 2;
            tokens.truncate(new_len);
            tokens.push(named.to_owned());
            if tokens.len() == 1 {
                return tokens.into_iter().next().unwrap_or_default();
            }
        }
    }

    let mut out = Vec::with_capacity(tokens.len());
    let mut has_shift = false;
    for modifier in &tokens[..tokens.len() - 1] {
        if let Some(norm) = normalize_modifier(modifier) {
            if norm == "Shift" {
                has_shift = true;
            }
            out.push(norm.to_owned());
        } else {
            out.push(modifier.clone());
        }
    }

    let last = &tokens[tokens.len() - 1];
    let normalized_last = if let Some(named) = normalize_named_key(last) {
        named.to_owned()
    } else if last.len() == 1 && last.as_bytes()[0].is_ascii_uppercase() && !has_shift {
        last.to_ascii_lowercase()
    } else {
        last.clone()
    };
    out.push(normalized_last);
    out.join(" ")
}

impl TryFrom<String> for KeyChord {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<KeyChord> for String {
    fn from(value: KeyChord) -> Self {
        value.0
    }
}

/// Whether `repl_exec` sends an Enter key after pasting text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterPolicy {
    SendEnter,
    NoEnter,
}

/// Parsed, unambiguous input payload for `repl_exec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplInput {
    Paste { text: String, enter: EnterPolicy },
    SendKeys(Vec<KeyChord>),
}

impl ReplInput {
    pub fn parse(
        input: Option<String>,
        keys: Option<Vec<String>>,
        send_enter: bool,
    ) -> Result<Self, ParseError> {
        match (input, keys) {
            (Some(text), None) => Ok(Self::Paste {
                text,
                enter: if send_enter {
                    EnterPolicy::SendEnter
                } else {
                    EnterPolicy::NoEnter
                },
            }),
            (None, Some(raw_keys)) => {
                if raw_keys.is_empty() {
                    return Err(ParseError::Empty { field: "keys" });
                }
                let parsed = raw_keys
                    .into_iter()
                    .map(KeyChord::parse)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::SendKeys(parsed))
            }
            (Some(text), Some(_)) => {
                if text.is_empty() {
                    Err(ParseError::EmptyInputWithKeys)
                } else {
                    Err(ParseError::BothReplInputAndKeys)
                }
            }
            (None, None) => Err(ParseError::MissingReplInput),
        }
    }
}

/// Whether a cancel/stop operation closes the underlying Zellij pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneClosePolicy {
    ClosePane,
    KeepPane,
}

/// Wait aggregation mode for `task_wait`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    Any,
    All,
}

/// Parsed representation of a managed pane title (`[BADGE] mcp:<kind>:<id>[:label]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPaneTitle {
    pub badge: Option<String>,
    pub key: ManagedKey,
    pub label: Option<String>,
}

/// Formats and parses managed pane titles for human readability and cold-start reconciliation.
#[derive(Debug, Clone, Copy)]
pub struct PaneTitleCodec;

impl PaneTitleCodec {
    pub fn format(badge: &str, key: &ManagedKey, label: Option<&str>) -> String {
        match label
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(|l| sanitize_slug_fragment(l, 32))
        {
            Some(clean_label) => format!("{badge} mcp:{key}:{clean_label}"),
            None => format!("{badge} mcp:{key}"),
        }
    }

    pub fn parse(title: &str) -> Option<ParsedPaneTitle> {
        let trimmed = title.trim();
        let (badge, rest) = if let Some(after_open) = trimmed.strip_prefix('[') {
            let close_idx = after_open.find(']')?;
            let badge_text = format!("[{}]", &after_open[..close_idx]);
            let remainder = after_open[close_idx + 1..].trim_start();
            (Some(badge_text), remainder)
        } else {
            (None, trimmed)
        };

        let body = rest.strip_prefix("mcp:")?;
        let mut parts = body.splitn(3, ':');
        let kind = parts.next()?;
        let id_raw = parts.next()?;
        let label = parts
            .next()
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);

        let key = match kind {
            "task" => ManagedKey::Task(TaskId::parse(id_raw).ok()?),
            "svc" => ManagedKey::Service(ServiceName::parse(id_raw).ok()?),
            "repl" => ManagedKey::Repl(ReplName::parse(id_raw).ok()?),
            _ => return None,
        };

        Some(ParsedPaneTitle { badge, key, label })
    }
}
