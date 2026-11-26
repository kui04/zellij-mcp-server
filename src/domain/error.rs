//! Typed domain and boundary-parsing errors.

use std::path::PathBuf;

use thiserror::Error;

use crate::domain::ids::{ManagedKey, ReplName, ServiceName, SessionName, TaskId};

/// Failure when parsing raw untrusted input into a proof-carrying domain value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },

    #[error("{field} contains invalid characters: {value:?}")]
    InvalidCharacters { field: &'static str, value: String },

    #[error("invalid pane id: {raw:?}")]
    InvalidPaneId { raw: String },

    #[error("invalid managed pane title: {raw:?}")]
    InvalidPaneTitle { raw: String },

    #[error("invalid regex pattern for {field}: {reason}")]
    InvalidRegex { field: &'static str, reason: String },

    #[error("repl_exec received both `input` and `keys` (omit `input` when sending `keys`)")]
    BothReplInputAndKeys,

    #[error(
        "repl_exec received both empty `input` (\"\") and `keys` (omit `input` instead of passing an empty string when using `keys`)"
    )]
    EmptyInputWithKeys,

    #[error("repl_exec requires either `input` or `keys`")]
    MissingReplInput,

    #[error("timeout_ms must be between {min_ms} and {max_ms}, got {actual_ms}")]
    TimeoutOutOfRange {
        min_ms: u64,
        max_ms: u64,
        actual_ms: u64,
    },
}

/// Expected domain-level failures surfaced to MCP callers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error("zellij session not found (it may have been closed or killed): {0}")]
    SessionNotFound(SessionName),

    #[error("task not found: {0}")]
    TaskNotFound(TaskId),

    #[error("service not found: {0}")]
    ServiceNotFound(ServiceName),

    #[error("repl not found: {0}")]
    ReplNotFound(ReplName),

    #[error("managed entity is no longer running: {0}")]
    NotRunning(ManagedKey),

    #[error("managed entity was closed by the human user: {0}")]
    StoppedByUser(ManagedKey),

    #[error("repl requires human input before further commands can run: {0}")]
    InputRequired(ReplName),

    #[error("destructive action was cancelled by the human user")]
    UserDeniedConfirmation,

    #[error("invalid working directory '{}': {reason}", path.display())]
    InvalidCwd { path: PathBuf, reason: String },
}
