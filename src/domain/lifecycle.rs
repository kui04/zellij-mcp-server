//! Algebraic lifecycle state machines and derived visual feedback actions.

use std::fmt;
use std::num::NonZeroI32;
use std::time::SystemTime;

use crate::domain::ids::{CompiledPattern, ManagedKey, TabId, TerminalPaneId};

/// Dark red background highlight for failed tasks and crashed services.
pub const ERROR_PANE_BG_HEX: &str = "#3a0000";
/// Border style applied to failed panes to make them stand out visually.
pub const ERROR_PANE_BORDER_STYLE: &str = "double";

/// Lifecycle states legal for a finite `Task`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskLifecycle {
    Running {
        started_at: SystemTime,
    },
    Completed {
        started_at: SystemTime,
        ended_at: SystemTime,
        pane_closed: bool,
    },
    Failed {
        started_at: SystemTime,
        ended_at: SystemTime,
        exit_code: NonZeroI32,
        attention_marked: bool,
    },
    Cancelled {
        started_at: SystemTime,
        ended_at: SystemTime,
        exit_code: Option<i32>,
        pane_closed: bool,
    },
    StoppedByUser {
        started_at: SystemTime,
        ended_at: SystemTime,
    },
}

impl TaskLifecycle {
    pub const fn is_terminal(&self) -> bool {
        match self {
            Self::Running { .. } => false,
            Self::Completed { .. }
            | Self::Failed { .. }
            | Self::Cancelled { .. }
            | Self::StoppedByUser { .. } => true,
        }
    }

    pub const fn status_label(&self) -> &'static str {
        match self {
            Self::Running { .. } => "running",
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "stopped",
            Self::StoppedByUser { .. } => "stopped_by_user",
        }
    }

    pub const fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Running { .. } | Self::StoppedByUser { .. } => None,
            Self::Completed { .. } => Some(0),
            Self::Failed { exit_code, .. } => Some(exit_code.get()),
            Self::Cancelled { exit_code, .. } => *exit_code,
        }
    }

    pub const fn started_at(&self) -> SystemTime {
        match self {
            Self::Running { started_at }
            | Self::Completed { started_at, .. }
            | Self::Failed { started_at, .. }
            | Self::Cancelled { started_at, .. }
            | Self::StoppedByUser { started_at, .. } => *started_at,
        }
    }
}

/// Lifecycle states legal for a long-lived `Service`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceLifecycle {
    Starting {
        started_at: SystemTime,
        ready_pattern: Option<CompiledPattern>,
    },
    Ready {
        started_at: SystemTime,
        ready_at: SystemTime,
        matched_line: Option<String>,
    },
    Stopping {
        started_at: SystemTime,
    },
    Failed {
        started_at: SystemTime,
        ended_at: SystemTime,
        exit_code: Option<i32>,
        attention_marked: bool,
    },
    Stopped {
        started_at: SystemTime,
        ended_at: SystemTime,
    },
    StoppedByUser {
        started_at: SystemTime,
        ended_at: SystemTime,
    },
}

impl ServiceLifecycle {
    pub const fn is_active(&self) -> bool {
        match self {
            Self::Starting { .. } | Self::Ready { .. } | Self::Stopping { .. } => true,
            Self::Failed { .. } | Self::Stopped { .. } | Self::StoppedByUser { .. } => false,
        }
    }

    pub const fn is_reusable(&self) -> bool {
        matches!(self, Self::Starting { .. } | Self::Ready { .. })
    }

    pub const fn status_label(&self) -> &'static str {
        match self {
            Self::Starting { .. } => "starting",
            Self::Ready { .. } => "ready",
            Self::Stopping { .. } => "stopping",
            Self::Failed { .. } => "failed",
            Self::Stopped { .. } => "stopped",
            Self::StoppedByUser { .. } => "stopped_by_user",
        }
    }

    pub const fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Starting { .. }
            | Self::Ready { .. }
            | Self::Stopping { .. }
            | Self::Stopped { .. }
            | Self::StoppedByUser { .. } => None,
            Self::Failed { exit_code, .. } => *exit_code,
        }
    }

    pub const fn started_at(&self) -> SystemTime {
        match self {
            Self::Starting { started_at, .. }
            | Self::Ready { started_at, .. }
            | Self::Stopping { started_at }
            | Self::Failed { started_at, .. }
            | Self::Stopped { started_at, .. }
            | Self::StoppedByUser { started_at, .. } => *started_at,
        }
    }
}

/// Lifecycle states legal for a stateful interactive `REPL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplLifecycle {
    Starting {
        started_at: SystemTime,
    },
    Idle {
        started_at: SystemTime,
    },
    Busy {
        started_at: SystemTime,
        command_started_at: SystemTime,
    },
    InputRequired {
        started_at: SystemTime,
        prompt_excerpt: String,
        attention_marked: bool,
    },
    Exited {
        started_at: SystemTime,
        ended_at: SystemTime,
        exit_code: Option<i32>,
    },
    StoppedByUser {
        started_at: SystemTime,
        ended_at: SystemTime,
    },
}

impl ReplLifecycle {
    pub const fn is_active(&self) -> bool {
        match self {
            Self::Starting { .. }
            | Self::Idle { .. }
            | Self::Busy { .. }
            | Self::InputRequired { .. } => true,
            Self::Exited { .. } | Self::StoppedByUser { .. } => false,
        }
    }

    pub const fn status_label(&self) -> &'static str {
        match self {
            Self::Starting { .. } => "starting",
            Self::Idle { .. } => "ready",
            Self::Busy { .. } => "running",
            Self::InputRequired { .. } => "input_required",
            Self::Exited { .. } => "stopped",
            Self::StoppedByUser { .. } => "stopped_by_user",
        }
    }

    pub const fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Starting { .. }
            | Self::Idle { .. }
            | Self::Busy { .. }
            | Self::InputRequired { .. }
            | Self::StoppedByUser { .. } => None,
            Self::Exited { exit_code, .. } => *exit_code,
        }
    }

    pub const fn started_at(&self) -> SystemTime {
        match self {
            Self::Starting { started_at }
            | Self::Idle { started_at }
            | Self::Busy { started_at, .. }
            | Self::InputRequired { started_at, .. }
            | Self::Exited { started_at, .. }
            | Self::StoppedByUser { started_at, .. } => *started_at,
        }
    }
}

/// Entity-specific lifecycle sum type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityLifecycle {
    Task(TaskLifecycle),
    Service(ServiceLifecycle),
    Repl(ReplLifecycle),
}

impl EntityLifecycle {
    pub fn badge(&self) -> VisualBadge {
        match self {
            Self::Task(task) => match task {
                TaskLifecycle::Running { .. } => VisualBadge::Run,
                TaskLifecycle::Completed { .. } => VisualBadge::Ok,
                TaskLifecycle::Failed { exit_code, .. } => VisualBadge::Err(*exit_code),
                TaskLifecycle::Cancelled { .. } | TaskLifecycle::StoppedByUser { .. } => {
                    VisualBadge::Stop
                }
            },
            Self::Service(service) => match service {
                ServiceLifecycle::Starting { .. } => VisualBadge::Run,
                ServiceLifecycle::Ready { .. } => VisualBadge::Rdy,
                ServiceLifecycle::Failed { exit_code, .. } => VisualBadge::Dead(*exit_code),
                ServiceLifecycle::Stopping { .. }
                | ServiceLifecycle::Stopped { .. }
                | ServiceLifecycle::StoppedByUser { .. } => VisualBadge::Stop,
            },
            Self::Repl(repl) => match repl {
                ReplLifecycle::Starting { .. } | ReplLifecycle::Busy { .. } => VisualBadge::Run,
                ReplLifecycle::Idle { .. } => VisualBadge::Rdy,
                ReplLifecycle::InputRequired { .. } => VisualBadge::Wait,
                ReplLifecycle::Exited { .. } | ReplLifecycle::StoppedByUser { .. } => {
                    VisualBadge::Stop
                }
            },
        }
    }

    pub const fn is_active(&self) -> bool {
        match self {
            Self::Task(t) => !t.is_terminal(),
            Self::Service(s) => s.is_active(),
            Self::Repl(r) => r.is_active(),
        }
    }

    pub const fn status_label(&self) -> &'static str {
        match self {
            Self::Task(t) => t.status_label(),
            Self::Service(s) => s.status_label(),
            Self::Repl(r) => r.status_label(),
        }
    }

    pub const fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Task(t) => t.exit_code(),
            Self::Service(s) => s.exit_code(),
            Self::Repl(r) => r.exit_code(),
        }
    }

    pub const fn started_at(&self) -> SystemTime {
        match self {
            Self::Task(t) => t.started_at(),
            Self::Service(s) => s.started_at(),
            Self::Repl(r) => r.started_at(),
        }
    }
}

/// Human-visible status prefix rendered at the start of every managed pane title.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisualBadge {
    Run,
    Rdy,
    Ok,
    Err(NonZeroI32),
    Dead(Option<i32>),
    Wait,
    Stop,
}

impl fmt::Display for VisualBadge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run => f.write_str("[RUN]"),
            Self::Rdy => f.write_str("[RDY]"),
            Self::Ok => f.write_str("[OK]"),
            Self::Err(code) => write!(f, "[ERR:{code}]"),
            Self::Dead(Some(code)) => write!(f, "[DEAD:{code}]"),
            Self::Dead(None) => f.write_str("[DEAD]"),
            Self::Wait => f.write_str("[WAIT]"),
            Self::Stop => f.write_str("[STOP]"),
        }
    }
}

/// Side-effect command produced by pure state transitions for the Zellij adapter to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisualAction {
    RenamePane {
        pane_id: TerminalPaneId,
        title: String,
    },
    HighlightError {
        pane_id: TerminalPaneId,
        bg_hex: &'static str,
        border_style: &'static str,
    },
    ResetPaneColor {
        pane_id: TerminalPaneId,
    },
    MarkAttention {
        pane_id: TerminalPaneId,
    },
    ShowFloatingPanes {
        tab_id: TabId,
    },
    Notify {
        message: String,
    },
    ClosePane {
        pane_id: TerminalPaneId,
    },
}

/// Asynchronous workspace alert surfaced on the next MCP tool response (`alerts[]`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceAlert {
    pub key: ManagedKey,
    pub pane_id: TerminalPaneId,
    pub message: String,
}
