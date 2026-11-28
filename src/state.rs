//! Pure in-memory state projection (`StateStore`) driven by Zellij events and reconciliation.

use std::collections::{HashMap, HashSet};
use std::num::{NonZeroI32, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::SystemTime;

use crate::domain::error::ParseError;
use crate::domain::ids::{
    CommandSpec, CompiledPattern, ManagedKey, PaneTitleCodec, ReplName, ServiceName, SessionName,
    TabId, TaskId, TerminalPaneId,
};
use crate::domain::layout::{
    FocusedPaneGeometry, LayoutState, MANAGED_SERVICES_TAB_NAME, MANAGED_TASKS_TAB_NAME,
    ResolvedPlacement, ViewportSnapshot,
};
use crate::domain::lifecycle::{
    ERROR_PANE_BG_HEX, ERROR_PANE_BORDER_STYLE, EntityLifecycle, ReplLifecycle, ServiceLifecycle,
    TaskLifecycle, VisualAction, WorkspaceAlert,
};
use crate::logs::{BoundedLogBuffer, diff_viewport, strip_ansi};

/// Maximum number of closed terminal records retained in memory for historical inspection.
pub const MAX_RETAINED_CLOSED_RECORDS: usize = 50;
/// Open finished panes above which `workspace_status` suggests running `workspace_cleanup`.
pub const RETENTION_HINT_THRESHOLD: usize = 4;
/// Maximum number of buffered early exit/close events for panes not yet registered.
const MAX_EARLY_EVENT_BUFFER: usize = 64;

/// Compile-time non-zero fallback exit code when a task exits without a numeric status.
const FALLBACK_NONZERO_EXIT: NonZeroI32 = match NonZeroI32::new(1) {
    Some(code) => code,
    None => unreachable!(),
};

/// How many recently finished panes of one class stay open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RetentionQuota {
    /// Close the pane as soon as the entity reaches its terminal state.
    #[default]
    CloseImmediately,
    /// Keep the newest N finished panes open and evict older ones.
    KeepLatest(NonZeroUsize),
    /// Never evict automatically.
    Unlimited,
}

impl RetentionQuota {
    /// Parses `"0"`, a positive integer, or `"unlimited"`. Rejects negative numbers, empty
    /// strings, and unknown words at the boundary so the core never sees an ambiguous quota.
    pub fn parse(raw: &str) -> Result<Self, ParseError> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("unlimited") {
            return Ok(Self::Unlimited);
        }
        let count: usize = trimmed
            .parse()
            .map_err(|_| ParseError::InvalidRetentionQuota {
                raw: trimmed.to_owned(),
            })?;
        Ok(Self::keep(count))
    }

    /// Builds a quota from a literal count; `0` closes immediately.
    pub const fn keep(count: usize) -> Self {
        match NonZeroUsize::new(count) {
            Some(count) => Self::KeepLatest(count),
            None => Self::CloseImmediately,
        }
    }

    /// Number of panes to keep open, or `None` when the quota is unlimited.
    const fn keep_count(self) -> Option<usize> {
        match self {
            Self::CloseImmediately => Some(0),
            Self::KeepLatest(count) => Some(count.get()),
            Self::Unlimited => None,
        }
    }
}

impl FromStr for RetentionQuota {
    type Err = ParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

/// What happens to a task that finished within the `task_run` fast-path wait window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastTaskClosePolicy {
    /// Snapshot the full scrollback, then close the pane so short commands leave no trace on the
    /// screen; logs stay readable through `task_logs`.
    CloseAfterBackfill,
    /// Leave the pane to the completed-task quota.
    FollowCompletedQuota,
}

impl FastTaskClosePolicy {
    pub const fn from_auto_close(enabled: bool) -> Self {
        if enabled {
            Self::CloseAfterBackfill
        } else {
            Self::FollowCompletedQuota
        }
    }
}

/// Per-class pane retention limits applied after every terminal lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub completed_tasks: RetentionQuota,
    pub failed_and_cancelled_tasks: RetentionQuota,
    pub exited_repls: RetentionQuota,
    pub fast_task_close: FastTaskClosePolicy,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            completed_tasks: RetentionQuota::KeepLatest(NonZeroUsize::MIN),
            failed_and_cancelled_tasks: RetentionQuota::KeepLatest(match NonZeroUsize::new(2) {
                Some(count) => count,
                None => unreachable!(),
            }),
            exited_repls: RetentionQuota::CloseImmediately,
            fast_task_close: FastTaskClosePolicy::CloseAfterBackfill,
        }
    }
}

/// Why a pane was selected for closing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionReason {
    RetentionGc,
    FastPathAutoClose,
    ExplicitCleanup,
}

/// Proof that a managed pane is finished and still open, so closing it cannot kill a process.
///
/// Fields are private: only [`StateStore`] builds one, after checking that the lifecycle is
/// terminal, [`ManagedRecord::exit_observed`] is set, and the pane is still open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictablePane {
    key: ManagedKey,
    pane_id: TerminalPaneId,
    tab_id: TabId,
    reason: EvictionReason,
}

impl EvictablePane {
    pub fn key(&self) -> &ManagedKey {
        &self.key
    }

    pub const fn pane_id(&self) -> TerminalPaneId {
        self.pane_id
    }

    pub const fn tab_id(&self) -> TabId {
        self.tab_id
    }

    pub const fn reason(&self) -> EvictionReason {
        self.reason
    }
}

/// Which class of finished panes a retention quota governs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetentionClass {
    CompletedTasks,
    FailedAndCancelledTasks,
    ExitedRepls,
}

impl RetentionClass {
    const fn matches(self, lifecycle: &EntityLifecycle) -> bool {
        matches!(
            (self, lifecycle),
            (
                Self::CompletedTasks,
                EntityLifecycle::Task(TaskLifecycle::Completed {
                    pane_closed: false,
                    ..
                }),
            ) | (
                Self::FailedAndCancelledTasks,
                EntityLifecycle::Task(
                    TaskLifecycle::Failed {
                        pane_closed: false,
                        ..
                    } | TaskLifecycle::Cancelled {
                        pane_closed: false,
                        ..
                    },
                ),
            ) | (
                Self::ExitedRepls,
                EntityLifecycle::Repl(ReplLifecycle::Exited {
                    pane_closed: false,
                    ..
                }),
            )
        )
    }
}

/// Class of finished panes addressed by `workspace_cleanup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CleanupScope {
    CompletedTasks,
    FailedTasks,
    CancelledTasks,
    ExitedRepls,
    /// Services that failed or were stopped while their pane stayed open.
    FailedServices,
}

impl CleanupScope {
    const fn matches(self, lifecycle: &EntityLifecycle) -> bool {
        matches!(
            (self, lifecycle),
            (
                Self::CompletedTasks,
                EntityLifecycle::Task(TaskLifecycle::Completed {
                    pane_closed: false,
                    ..
                }),
            ) | (
                Self::FailedTasks,
                EntityLifecycle::Task(TaskLifecycle::Failed {
                    pane_closed: false,
                    ..
                }),
            ) | (
                Self::CancelledTasks,
                EntityLifecycle::Task(TaskLifecycle::Cancelled {
                    pane_closed: false,
                    ..
                }),
            ) | (
                Self::ExitedRepls,
                EntityLifecycle::Repl(ReplLifecycle::Exited {
                    pane_closed: false,
                    ..
                }),
            ) | (
                Self::FailedServices,
                EntityLifecycle::Service(
                    ServiceLifecycle::Failed {
                        pane_closed: false,
                        ..
                    } | ServiceLifecycle::Stopped {
                        pane_closed: false,
                        ..
                    },
                ),
            )
        )
    }

    /// Whether an active entity of the same kind would be protected by this scope.
    const fn covers_kind(self, lifecycle: &EntityLifecycle) -> bool {
        matches!(
            (self, lifecycle),
            (
                Self::CompletedTasks | Self::FailedTasks | Self::CancelledTasks,
                EntityLifecycle::Task(_),
            ) | (Self::ExitedRepls, EntityLifecycle::Repl(_))
                | (Self::FailedServices, EntityLifecycle::Service(_))
        )
    }
}

/// Whether cleanup covers the whole session or one tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupTarget {
    EntireSession,
    SingleTab(TabId),
}

impl CleanupTarget {
    /// Whether `tab_id` falls inside this target; the session target includes every tab.
    fn includes(self, tab_id: TabId) -> bool {
        match self {
            Self::EntireSession => true,
            Self::SingleTab(target) => target == tab_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupExecutionMode {
    Execute,
    DryRun,
}

/// Validated `workspace_cleanup` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupRequest {
    /// At least one scope; duplicates keep their first position.
    pub scopes: Vec<CleanupScope>,
    pub target: CleanupTarget,
    pub mode: CleanupExecutionMode,
}

impl CleanupRequest {
    /// Rejects an empty scope list so a bare call can never mean "close everything".
    pub fn parse(
        scopes: Vec<CleanupScope>,
        target: CleanupTarget,
        mode: CleanupExecutionMode,
    ) -> Result<Self, ParseError> {
        if scopes.is_empty() {
            return Err(ParseError::Empty { field: "scopes" });
        }
        let mut unique = Vec::with_capacity(scopes.len());
        for scope in scopes {
            if !unique.contains(&scope) {
                unique.push(scope);
            }
        }
        Ok(Self {
            scopes: unique,
            target,
            mode,
        })
    }
}

/// In-memory record for a single managed Zellij terminal pane.
#[derive(Debug, Clone)]
pub struct ManagedRecord {
    pub key: ManagedKey,
    pub tab_id: TabId,
    pub pane_id: TerminalPaneId,
    pub pane_title: String,
    pub label: Option<String>,
    pub placement: ResolvedPlacement,
    pub cmd: CommandSpec,
    pub cwd: PathBuf,
    pub ready_pattern: Option<CompiledPattern>,
    pub prompt_pattern: Option<CompiledPattern>,
    pub lifecycle: EntityLifecycle,
    /// True once `PaneExited`, `PaneClosed`, or reconciliation proved the process is gone.
    ///
    /// A terminal lifecycle alone is not enough to close a pane: `task_cancel` and `service_stop`
    /// mark an entity terminal after a fixed grace period even when the process ignored the
    /// signal. Retention and cleanup require this flag so they cannot kill a live process.
    pub exit_observed: bool,
    pub logs: BoundedLogBuffer,
    pub last_viewport: Vec<String>,
}

/// Pure effects returned by `StateStore` mutations for the imperative shell to execute.
#[derive(Debug, Clone, Default)]
pub struct TransitionEffects {
    pub visual_actions: Vec<VisualAction>,
    pub alerts: Vec<WorkspaceAlert>,
    pub changed_keys: Vec<ManagedKey>,
    pub exited_panes: Vec<TerminalPaneId>,
    pub closed_panes: Vec<TerminalPaneId>,
    pub subscription_dirty: bool,
}

impl TransitionEffects {
    pub fn extend(&mut self, other: Self) {
        self.visual_actions.extend(other.visual_actions);
        self.alerts.extend(other.alerts);
        self.changed_keys.extend(other.changed_keys);
        self.exited_panes.extend(other.exited_panes);
        self.closed_panes.extend(other.closed_panes);
        self.subscription_dirty |= other.subscription_dirty;
    }

    /// Effects for closing one pane, including the write-lock release keyed by `closed_panes`.
    pub fn close_pane(pane_id: TerminalPaneId) -> Self {
        Self {
            visual_actions: vec![VisualAction::ClosePane { pane_id }],
            closed_panes: vec![pane_id],
            ..Self::default()
        }
    }
}

/// Snapshot fetched from Zellij (`list-tabs`, `list-panes`, `current-tab-info`) for reconciliation.
#[derive(Debug, Clone)]
pub struct ReconciliationSnapshot {
    pub tabs: Vec<ObservedTab>,
    pub panes: Vec<ObservedPane>,
    pub active_tab: Option<ObservedTab>,
}

/// Parsed tab metadata from `zellij action list-tabs --all --json` or `current-tab-info --json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTab {
    pub tab_id: TabId,
    pub name: String,
    pub active: bool,
    pub viewport: ViewportSnapshot,
}

/// Parsed terminal pane metadata from `zellij action list-panes --all --json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedPane {
    pub pane_id: TerminalPaneId,
    pub tab_id: TabId,
    pub title: String,
    pub is_focused: bool,
    pub is_floating: bool,
    pub columns: u16,
    pub rows: u16,
    pub exited: bool,
    pub exit_status: Option<i32>,
    pub cwd: Option<PathBuf>,
    pub command: Option<String>,
}

/// Structured session event parsed from `zellij events`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZellijEvent {
    PaneExited {
        pane_id: TerminalPaneId,
        exit_code: Option<i32>,
    },
    PaneClosed {
        pane_id: TerminalPaneId,
    },
    TabFocused {
        tab_id: TabId,
    },
    TabClosed {
        tab_id: TabId,
    },
    SessionEnding,
}

/// Pure in-memory projection of all managed panes and dynamic layout state.
#[derive(Debug)]
pub struct StateStore {
    pub layout: LayoutState,
    pub session_dead: bool,
    panes_by_key: HashMap<ManagedKey, ManagedRecord>,
    key_by_pane_id: HashMap<TerminalPaneId, ManagedKey>,
    early_exits: HashMap<TerminalPaneId, Option<i32>>,
    early_closes: HashSet<TerminalPaneId>,
    pending_alerts: Vec<WorkspaceAlert>,
    next_task_seq: u64,
    retention: RetentionPolicy,
}

impl StateStore {
    pub fn new(session: SessionName, retention: RetentionPolicy) -> Self {
        Self {
            layout: LayoutState::new(session),
            session_dead: false,
            panes_by_key: HashMap::new(),
            key_by_pane_id: HashMap::new(),
            early_exits: HashMap::new(),
            early_closes: HashSet::new(),
            pending_alerts: Vec::new(),
            next_task_seq: 1,
            retention,
        }
    }

    pub fn allocate_task_id(&mut self) -> TaskId {
        loop {
            let seq = self.next_task_seq;
            self.next_task_seq = self.next_task_seq.saturating_add(1);
            let candidate_str = format!("t_{seq:02}");
            if let Ok(task_id) = TaskId::parse(candidate_str)
                && !self
                    .panes_by_key
                    .contains_key(&ManagedKey::Task(task_id.clone()))
            {
                return task_id;
            }
        }
    }

    /// Registers a managed record and replays any early exit/close event that arrived between
    /// `new-pane` and `register`.
    pub fn register(&mut self, record: ManagedRecord) -> TransitionEffects {
        self.register_inner(record, true)
    }

    fn register_inner(
        &mut self,
        record: ManagedRecord,
        apply_retention: bool,
    ) -> TransitionEffects {
        let pane_id = record.pane_id;
        if let Some(old) = self.panes_by_key.insert(record.key.clone(), record.clone())
            && old.pane_id != pane_id
        {
            let _ = self.key_by_pane_id.remove(&old.pane_id);
        }
        let _ = self.key_by_pane_id.insert(pane_id, record.key);

        let mut effects = TransitionEffects::default();
        if let Some(exit_code) = self.early_exits.remove(&pane_id) {
            effects.extend(self.apply_event_inner(
                ZellijEvent::PaneExited { pane_id, exit_code },
                SystemTime::now(),
                apply_retention,
            ));
        }
        if self.early_closes.remove(&pane_id) {
            effects.extend(self.apply_event_inner(
                ZellijEvent::PaneClosed { pane_id },
                SystemTime::now(),
                apply_retention,
            ));
        }
        effects
    }

    pub fn get(&self, key: &ManagedKey) -> Option<&ManagedRecord> {
        self.panes_by_key.get(key)
    }

    pub fn get_mut(&mut self, key: &ManagedKey) -> Option<&mut ManagedRecord> {
        self.panes_by_key.get_mut(key)
    }

    pub fn get_by_pane(&self, pane_id: TerminalPaneId) -> Option<&ManagedRecord> {
        self.key_by_pane_id
            .get(&pane_id)
            .and_then(|key| self.panes_by_key.get(key))
    }

    pub fn get_service(&self, name: &ServiceName) -> Option<&ManagedRecord> {
        self.panes_by_key.get(&ManagedKey::Service(name.clone()))
    }

    pub fn get_repl(&self, name: &ReplName) -> Option<&ManagedRecord> {
        self.panes_by_key.get(&ManagedKey::Repl(name.clone()))
    }

    pub fn all_records(&self) -> impl Iterator<Item = &ManagedRecord> {
        self.panes_by_key.values()
    }

    /// Returns all terminal pane IDs that currently require `zellij subscribe` streaming.
    pub fn subscribed_pane_ids(&self) -> Vec<TerminalPaneId> {
        let mut ids: Vec<TerminalPaneId> = self
            .panes_by_key
            .values()
            .filter(|rec| rec.lifecycle.is_active())
            .map(|rec| rec.pane_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Counts active managed panes in the given tab.
    pub fn count_active_in_tab(&self, tab_id: TabId) -> usize {
        self.panes_by_key
            .values()
            .filter(|rec| rec.tab_id == tab_id && rec.lifecycle.is_active())
            .count()
    }

    /// Applies a single `zellij events` notification to the state machine.
    pub fn apply_event(&mut self, event: ZellijEvent, now: SystemTime) -> TransitionEffects {
        self.apply_event_inner(event, now, true)
    }

    fn apply_event_inner(
        &mut self,
        event: ZellijEvent,
        now: SystemTime,
        apply_retention: bool,
    ) -> TransitionEffects {
        let mut effects = TransitionEffects::default();
        match event {
            ZellijEvent::PaneExited { pane_id, exit_code } => {
                let Some(key) = self.key_by_pane_id.get(&pane_id).cloned() else {
                    if self.early_exits.len() >= MAX_EARLY_EVENT_BUFFER {
                        self.early_exits.clear();
                    }
                    let _ = self.early_exits.insert(pane_id, exit_code);
                    return effects;
                };
                let Some(record) = self.panes_by_key.get_mut(&key) else {
                    return effects;
                };
                record.exit_observed = true;
                match &record.lifecycle {
                    EntityLifecycle::Task(TaskLifecycle::Running { started_at }) => {
                        let started_at = *started_at;
                        match exit_code.and_then(NonZeroI32::new) {
                            None if exit_code == Some(0) => {
                                record.lifecycle =
                                    EntityLifecycle::Task(TaskLifecycle::Completed {
                                        started_at,
                                        ended_at: now,
                                        pane_closed: false,
                                    });
                                let title = format_record_title(record);
                                record.pane_title = title.clone();
                                effects
                                    .visual_actions
                                    .push(VisualAction::RenamePane { pane_id, title });
                                effects.changed_keys.push(key);
                                effects.exited_panes.push(pane_id);
                                effects.subscription_dirty = true;
                            }
                            other_nonzero => {
                                let code = other_nonzero.unwrap_or(FALLBACK_NONZERO_EXIT);
                                record.lifecycle = EntityLifecycle::Task(TaskLifecycle::Failed {
                                    started_at,
                                    ended_at: now,
                                    exit_code: code,
                                    attention_marked: false,
                                    pane_closed: false,
                                });
                                let title = format_record_title(record);
                                record.pane_title = title.clone();
                                effects.visual_actions.push(VisualAction::RenamePane {
                                    pane_id,
                                    title: title.clone(),
                                });
                                effects.visual_actions.push(VisualAction::HighlightError {
                                    pane_id,
                                    bg_hex: ERROR_PANE_BG_HEX,
                                    border_style: ERROR_PANE_BORDER_STYLE,
                                });
                                effects.visual_actions.push(VisualAction::Notify {
                                    message: format!("Task failed ({code}): {title}"),
                                });
                                effects.changed_keys.push(key);
                                effects.exited_panes.push(pane_id);
                                effects.subscription_dirty = true;
                            }
                        }
                    }
                    EntityLifecycle::Task(
                        TaskLifecycle::Completed { .. }
                        | TaskLifecycle::Failed { .. }
                        | TaskLifecycle::Cancelled { .. }
                        | TaskLifecycle::StoppedByUser { .. },
                    ) => {}
                    EntityLifecycle::Service(
                        ServiceLifecycle::Starting { started_at, .. }
                        | ServiceLifecycle::Ready { started_at, .. },
                    ) => {
                        let started_at = *started_at;
                        record.lifecycle = EntityLifecycle::Service(ServiceLifecycle::Failed {
                            started_at,
                            ended_at: now,
                            exit_code,
                            attention_marked: true,
                            pane_closed: false,
                        });
                        let title = format_record_title(record);
                        record.pane_title = title.clone();
                        let alert = WorkspaceAlert {
                            key: key.clone(),
                            pane_id,
                            message: format!(
                                "Service crashed unexpectedly (exit_code={exit_code:?}): {title}"
                            ),
                        };
                        effects.visual_actions.push(VisualAction::RenamePane {
                            pane_id,
                            title: title.clone(),
                        });
                        effects.visual_actions.push(VisualAction::HighlightError {
                            pane_id,
                            bg_hex: ERROR_PANE_BG_HEX,
                            border_style: ERROR_PANE_BORDER_STYLE,
                        });
                        effects
                            .visual_actions
                            .push(VisualAction::MarkAttention { pane_id });
                        effects.visual_actions.push(VisualAction::Notify {
                            message: alert.message.clone(),
                        });
                        self.pending_alerts.push(alert.clone());
                        effects.alerts.push(alert);
                        effects.changed_keys.push(key);
                        effects.exited_panes.push(pane_id);
                        effects.subscription_dirty = true;
                    }
                    EntityLifecycle::Service(ServiceLifecycle::Stopping { started_at }) => {
                        let started_at = *started_at;
                        record.lifecycle = EntityLifecycle::Service(ServiceLifecycle::Stopped {
                            started_at,
                            ended_at: now,
                            pane_closed: false,
                        });
                        let title = format_record_title(record);
                        record.pane_title = title.clone();
                        effects
                            .visual_actions
                            .push(VisualAction::RenamePane { pane_id, title });
                        effects.changed_keys.push(key);
                        effects.exited_panes.push(pane_id);
                        effects.subscription_dirty = true;
                    }
                    EntityLifecycle::Service(
                        ServiceLifecycle::Failed { .. }
                        | ServiceLifecycle::Stopped { .. }
                        | ServiceLifecycle::StoppedByUser { .. },
                    ) => {}
                    EntityLifecycle::Repl(
                        ReplLifecycle::Starting { started_at }
                        | ReplLifecycle::Idle { started_at }
                        | ReplLifecycle::Busy { started_at, .. }
                        | ReplLifecycle::InputRequired { started_at, .. },
                    ) => {
                        let started_at = *started_at;
                        record.lifecycle = EntityLifecycle::Repl(ReplLifecycle::Exited {
                            started_at,
                            ended_at: now,
                            exit_code,
                            pane_closed: false,
                        });
                        let title = format_record_title(record);
                        record.pane_title = title.clone();
                        effects
                            .visual_actions
                            .push(VisualAction::RenamePane { pane_id, title });
                        effects.changed_keys.push(key);
                        effects.exited_panes.push(pane_id);
                        effects.subscription_dirty = true;
                    }
                    EntityLifecycle::Repl(
                        ReplLifecycle::Exited { .. } | ReplLifecycle::StoppedByUser { .. },
                    ) => {}
                }
            }
            ZellijEvent::PaneClosed { pane_id } => {
                effects.closed_panes.push(pane_id);
                let Some(key) = self.key_by_pane_id.get(&pane_id).cloned() else {
                    if self.early_closes.len() >= MAX_EARLY_EVENT_BUFFER {
                        self.early_closes.clear();
                    }
                    let _ = self.early_closes.insert(pane_id);
                    return effects;
                };
                let Some(record) = self.panes_by_key.get_mut(&key) else {
                    return effects;
                };
                record.exit_observed = true;
                mark_lifecycle_pane_closed(&mut record.lifecycle);
                if record.lifecycle.is_active() {
                    record.lifecycle = match &record.lifecycle {
                        EntityLifecycle::Task(t) => {
                            EntityLifecycle::Task(TaskLifecycle::StoppedByUser {
                                started_at: t.started_at(),
                                ended_at: now,
                            })
                        }
                        EntityLifecycle::Service(ServiceLifecycle::Stopping { started_at }) => {
                            EntityLifecycle::Service(ServiceLifecycle::Stopped {
                                started_at: *started_at,
                                ended_at: now,
                                pane_closed: true,
                            })
                        }
                        EntityLifecycle::Service(s) => {
                            EntityLifecycle::Service(ServiceLifecycle::StoppedByUser {
                                started_at: s.started_at(),
                                ended_at: now,
                            })
                        }
                        EntityLifecycle::Repl(r) => {
                            EntityLifecycle::Repl(ReplLifecycle::StoppedByUser {
                                started_at: r.started_at(),
                                ended_at: now,
                            })
                        }
                    };
                    effects.changed_keys.push(key);
                    effects.subscription_dirty = true;
                }
                self.prune_closed_history(MAX_RETAINED_CLOSED_RECORDS);
            }
            ZellijEvent::TabFocused { tab_id } => {
                self.layout.active_tab_id = Some(tab_id);
            }
            ZellijEvent::TabClosed { tab_id } => {
                if self.layout.managed_tabs.tasks == Some(tab_id) {
                    self.layout.managed_tabs.tasks = None;
                }
                if self.layout.managed_tabs.services == Some(tab_id) {
                    self.layout.managed_tabs.services = None;
                }
                let _ = self.layout.tab_modes.remove(&tab_id);
            }
            ZellijEvent::SessionEnding => {
                self.session_dead = true;
                effects.subscription_dirty = true;
            }
        }
        if apply_retention {
            effects.extend(self.enforce_retention(EvictionReason::RetentionGc));
        }
        effects
    }

    /// Applies a viewport frame from `zellij subscribe`, appending diffed lines, checking
    /// service readiness, and recovering REPLs from `InputRequired` once new output resumes.
    ///
    /// When `is_initial` is true on a pane that already has a baseline (`!last_viewport.is_empty()`),
    /// the frame came from a `zellij subscribe` mux restart; reset the viewport baseline without
    /// replaying historical lines into `logs`.
    pub fn apply_viewport_frame(
        &mut self,
        pane_id: TerminalPaneId,
        is_initial: bool,
        viewport_lines: Vec<String>,
        now: SystemTime,
    ) -> TransitionEffects {
        let mut effects = TransitionEffects::default();
        let Some(key) = self.key_by_pane_id.get(&pane_id).cloned() else {
            return effects;
        };
        let Some(record) = self.panes_by_key.get_mut(&key) else {
            return effects;
        };

        let cleaned: Vec<String> = viewport_lines.iter().map(|l| strip_ansi(l)).collect();
        if is_initial && !record.last_viewport.is_empty() {
            record.last_viewport = cleaned;
            return effects;
        }
        let delta = diff_viewport(&record.last_viewport, &cleaned);
        record.last_viewport = cleaned;

        if delta.is_empty() {
            return effects;
        }

        let _ = record.logs.push_lines(delta.iter().cloned());

        match &record.lifecycle {
            EntityLifecycle::Service(ServiceLifecycle::Starting {
                started_at,
                ready_pattern: Some(pattern),
            }) => {
                if let Some(matched) = delta.iter().find(|line| pattern.is_match(line)) {
                    record.lifecycle = EntityLifecycle::Service(ServiceLifecycle::Ready {
                        started_at: *started_at,
                        ready_at: now,
                        matched_line: Some(matched.clone()),
                    });
                    let title = format_record_title(record);
                    record.pane_title = title.clone();
                    effects
                        .visual_actions
                        .push(VisualAction::RenamePane { pane_id, title });
                    effects
                        .visual_actions
                        .push(VisualAction::ResetPaneColor { pane_id });
                    effects.changed_keys.push(key);
                }
            }
            EntityLifecycle::Repl(ReplLifecycle::InputRequired { started_at, .. }) => {
                let recovered = match &record.prompt_pattern {
                    Some(pat) => delta.iter().any(|line| pat.matches_terminal_line(line)),
                    None => true,
                };
                if recovered {
                    record.lifecycle = EntityLifecycle::Repl(ReplLifecycle::Idle {
                        started_at: *started_at,
                    });
                    let title = format_record_title(record);
                    record.pane_title = title.clone();
                    effects
                        .visual_actions
                        .push(VisualAction::RenamePane { pane_id, title });
                    effects.changed_keys.push(key);
                }
            }
            EntityLifecycle::Task(_)
            | EntityLifecycle::Service(
                ServiceLifecycle::Starting {
                    ready_pattern: None,
                    ..
                }
                | ServiceLifecycle::Ready { .. }
                | ServiceLifecycle::Stopping { .. }
                | ServiceLifecycle::Failed { .. }
                | ServiceLifecycle::Stopped { .. }
                | ServiceLifecycle::StoppedByUser { .. },
            )
            | EntityLifecycle::Repl(
                ReplLifecycle::Starting { .. }
                | ReplLifecycle::Idle { .. }
                | ReplLifecycle::Busy { .. }
                | ReplLifecycle::Exited { .. }
                | ReplLifecycle::StoppedByUser { .. },
            ) => {}
        }

        effects
    }

    /// Reconciles live Zellij tab/pane snapshots against in-memory state (cold start & refresh).
    pub fn reconcile(
        &mut self,
        snapshot: ReconciliationSnapshot,
        now: SystemTime,
    ) -> TransitionEffects {
        self.reconcile_inner(snapshot, now, true)
    }

    /// Reconciles without applying retention, so callers that must not close panes outside their
    /// own selection (such as cleanup previews) cannot trigger a background eviction.
    pub fn reconcile_without_retention(
        &mut self,
        snapshot: ReconciliationSnapshot,
        now: SystemTime,
    ) -> TransitionEffects {
        self.reconcile_inner(snapshot, now, false)
    }

    fn reconcile_inner(
        &mut self,
        snapshot: ReconciliationSnapshot,
        now: SystemTime,
        apply_retention: bool,
    ) -> TransitionEffects {
        let mut effects = TransitionEffects::default();
        self.session_dead = false;

        let live_tab_ids: HashSet<TabId> = snapshot.tabs.iter().map(|t| t.tab_id).collect();
        self.layout
            .tab_modes
            .retain(|id, _| live_tab_ids.contains(id));

        self.layout.managed_tabs.tasks = snapshot
            .tabs
            .iter()
            .find(|t| t.name == MANAGED_TASKS_TAB_NAME)
            .map(|t| t.tab_id);
        self.layout.managed_tabs.services = snapshot
            .tabs
            .iter()
            .find(|t| t.name == MANAGED_SERVICES_TAB_NAME)
            .map(|t| t.tab_id);

        // In headless background sessions `current-tab-info` fails with no attached client;
        // fall back to the active tab or the first tab from `list-tabs`.
        let effective_active = snapshot
            .active_tab
            .as_ref()
            .or_else(|| snapshot.tabs.iter().find(|t| t.active))
            .or_else(|| snapshot.tabs.first());

        if let Some(tab) = effective_active {
            self.layout.active_tab_id = Some(tab.tab_id);
            self.layout.active_tab_name = Some(tab.name.clone());
            self.layout.viewport = Some(tab.viewport);
            self.layout.focused_pane = find_focused_pane_geometry(tab.tab_id, &snapshot.panes);
        }

        let live_pane_ids: HashSet<TerminalPaneId> =
            snapshot.panes.iter().map(|p| p.pane_id).collect();

        // Mark previously tracked panes that disappeared as closed / StoppedByUser.
        for record in self.panes_by_key.values_mut() {
            if !live_pane_ids.contains(&record.pane_id) {
                record.exit_observed = true;
                mark_lifecycle_pane_closed(&mut record.lifecycle);
                if record.lifecycle.is_active() {
                    effects.closed_panes.push(record.pane_id);
                    effects.subscription_dirty = true;
                    record.lifecycle = match &record.lifecycle {
                        EntityLifecycle::Task(t) => {
                            EntityLifecycle::Task(TaskLifecycle::StoppedByUser {
                                started_at: t.started_at(),
                                ended_at: now,
                            })
                        }
                        EntityLifecycle::Service(s) => {
                            EntityLifecycle::Service(ServiceLifecycle::StoppedByUser {
                                started_at: s.started_at(),
                                ended_at: now,
                            })
                        }
                        EntityLifecycle::Repl(r) => {
                            EntityLifecycle::Repl(ReplLifecycle::StoppedByUser {
                                started_at: r.started_at(),
                                ended_at: now,
                            })
                        }
                    };
                }
            }
        }

        // Claim or refresh observed managed panes.
        for pane in snapshot.panes {
            let Some(parsed) = PaneTitleCodec::parse(&pane.title) else {
                continue;
            };
            if let Some(existing) = self.panes_by_key.get_mut(&parsed.key) {
                let old_pane_id = existing.pane_id;
                existing.tab_id = pane.tab_id;
                existing.pane_title = pane.title;
                // A pane id change means a new process; the previous exit observation no longer
                // applies, so reset it to what this pane reports instead of carrying it over.
                existing.exit_observed = if old_pane_id == pane.pane_id {
                    existing.exit_observed || pane.exited
                } else {
                    pane.exited
                };
                // Observing an open pane proves a previous close did not take effect, so clear the
                // stale flag and let retention or cleanup retry the close.
                if existing.lifecycle.is_pane_closed() {
                    mark_lifecycle_pane_open(&mut existing.lifecycle);
                }
                if old_pane_id != pane.pane_id {
                    existing.pane_id = pane.pane_id;
                    let _ = self.key_by_pane_id.remove(&old_pane_id);
                    let _ = self.key_by_pane_id.insert(pane.pane_id, parsed.key.clone());
                    effects.subscription_dirty = true;
                }
                if pane.exited && existing.lifecycle.is_active() {
                    let pane_id = existing.pane_id;
                    effects.extend(self.apply_event_inner(
                        ZellijEvent::PaneExited {
                            pane_id,
                            exit_code: pane.exit_status,
                        },
                        now,
                        apply_retention,
                    ));
                }
                continue;
            }

            let placement = if pane.is_floating {
                ResolvedPlacement::Floating
            } else if Some(pane.tab_id) == self.layout.managed_tabs.tasks
                || Some(pane.tab_id) == self.layout.managed_tabs.services
            {
                ResolvedPlacement::BackgroundTab
            } else {
                ResolvedPlacement::SplitRight
            };

            let cmd_vec: Vec<String> = pane
                .command
                .as_deref()
                .map(|c| c.split_whitespace().map(ToOwned::to_owned).collect())
                .unwrap_or_default();
            let cmd = CommandSpec::parse(cmd_vec).unwrap_or_else(|_| CommandSpec::unknown());

            let lifecycle = match &parsed.key {
                ManagedKey::Task(_) => {
                    if pane.exited {
                        match pane.exit_status.and_then(NonZeroI32::new) {
                            None if pane.exit_status == Some(0) => {
                                EntityLifecycle::Task(TaskLifecycle::Completed {
                                    started_at: now,
                                    ended_at: now,
                                    pane_closed: false,
                                })
                            }
                            other => EntityLifecycle::Task(TaskLifecycle::Failed {
                                started_at: now,
                                ended_at: now,
                                exit_code: other.unwrap_or(FALLBACK_NONZERO_EXIT),
                                attention_marked: false,
                                pane_closed: false,
                            }),
                        }
                    } else {
                        EntityLifecycle::Task(TaskLifecycle::Running { started_at: now })
                    }
                }
                ManagedKey::Service(_) => {
                    if pane.exited {
                        EntityLifecycle::Service(ServiceLifecycle::Failed {
                            started_at: now,
                            ended_at: now,
                            exit_code: pane.exit_status,
                            attention_marked: false,
                            pane_closed: false,
                        })
                    } else {
                        EntityLifecycle::Service(ServiceLifecycle::Ready {
                            started_at: now,
                            ready_at: now,
                            matched_line: None,
                        })
                    }
                }
                ManagedKey::Repl(_) => {
                    if pane.exited {
                        EntityLifecycle::Repl(ReplLifecycle::Exited {
                            started_at: now,
                            ended_at: now,
                            exit_code: pane.exit_status,
                            pane_closed: false,
                        })
                    } else {
                        EntityLifecycle::Repl(ReplLifecycle::Idle { started_at: now })
                    }
                }
            };

            effects.extend(self.register_inner(
                ManagedRecord {
                    key: parsed.key,
                    tab_id: pane.tab_id,
                    pane_id: pane.pane_id,
                    pane_title: pane.title,
                    label: parsed.label,
                    placement,
                    cmd,
                    cwd: pane.cwd.unwrap_or_else(|| PathBuf::from(".")),
                    ready_pattern: None,
                    prompt_pattern: None,
                    lifecycle,
                    exit_observed: pane.exited,
                    logs: BoundedLogBuffer::default(),
                    last_viewport: Vec::new(),
                },
                apply_retention,
            ));
        }

        self.prune_closed_history(MAX_RETAINED_CLOSED_RECORDS);
        if apply_retention {
            effects.extend(self.enforce_retention(EvictionReason::RetentionGc));
        }
        effects
    }

    /// Lightweight refresh of active tab viewport and focused pane geometry prior to placement.
    pub fn update_layout_snapshot(
        &mut self,
        tabs: &[ObservedTab],
        active_tab: Option<&ObservedTab>,
        panes: &[ObservedPane],
    ) {
        self.layout.managed_tabs.tasks = tabs
            .iter()
            .find(|t| t.name == MANAGED_TASKS_TAB_NAME)
            .map(|t| t.tab_id);
        self.layout.managed_tabs.services = tabs
            .iter()
            .find(|t| t.name == MANAGED_SERVICES_TAB_NAME)
            .map(|t| t.tab_id);

        let effective_active = active_tab
            .or_else(|| tabs.iter().find(|t| t.active))
            .or_else(|| tabs.first());
        if let Some(tab) = effective_active {
            self.layout.active_tab_id = Some(tab.tab_id);
            self.layout.active_tab_name = Some(tab.name.clone());
            self.layout.viewport = Some(tab.viewport);
            self.layout.focused_pane = find_focused_pane_geometry(tab.tab_id, panes);
        }
    }

    /// Resolves a human-readable tab name for a known managed or active tab ID.
    pub fn tab_name_for(&self, tab_id: TabId) -> Option<String> {
        if self.layout.managed_tabs.tasks == Some(tab_id) {
            Some(MANAGED_TASKS_TAB_NAME.to_owned())
        } else if self.layout.managed_tabs.services == Some(tab_id) {
            Some(MANAGED_SERVICES_TAB_NAME.to_owned())
        } else if self.layout.active_tab_id == Some(tab_id) {
            self.layout.active_tab_name.clone()
        } else {
            None
        }
    }

    /// Evicts oldest closed terminal records beyond `max_closed` to bound `workspace_status` size.
    pub fn prune_closed_history(&mut self, max_closed: usize) {
        let mut closed: Vec<(SystemTime, ManagedKey, TerminalPaneId)> = self
            .panes_by_key
            .values()
            .filter(|rec| !rec.lifecycle.is_active() && rec.lifecycle.is_pane_closed())
            .map(|rec| {
                let ts = rec
                    .lifecycle
                    .ended_at()
                    .unwrap_or_else(|| rec.lifecycle.started_at());
                (ts, rec.key.clone(), rec.pane_id)
            })
            .collect();

        if closed.len() <= max_closed {
            return;
        }
        closed.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.2.cmp(&a.2)));
        for (_, key, pane_id) in closed.into_iter().skip(max_closed) {
            let _ = self.panes_by_key.remove(&key);
            if self.key_by_pane_id.get(&pane_id) == Some(&key) {
                let _ = self.key_by_pane_id.remove(&pane_id);
            }
        }
    }

    /// Selects finished panes above the configured quotas and marks them closed.
    pub fn enforce_retention(&mut self, reason: EvictionReason) -> TransitionEffects {
        let mut evicted = Vec::new();
        for (class, quota) in [
            (
                RetentionClass::CompletedTasks,
                self.retention.completed_tasks,
            ),
            (
                RetentionClass::FailedAndCancelledTasks,
                self.retention.failed_and_cancelled_tasks,
            ),
            (RetentionClass::ExitedRepls, self.retention.exited_repls),
        ] {
            evicted.extend(self.select_exceeding_quota(class, quota, reason));
        }
        self.mark_evicted(&evicted)
    }

    /// Selects the oldest finished panes of `class` beyond `quota`; the newest `ended_at` wins.
    fn select_exceeding_quota(
        &self,
        class: RetentionClass,
        quota: RetentionQuota,
        reason: EvictionReason,
    ) -> Vec<EvictablePane> {
        let Some(keep) = quota.keep_count() else {
            return Vec::new();
        };
        let mut candidates: Vec<(SystemTime, TerminalPaneId, &ManagedRecord)> = self
            .panes_by_key
            .values()
            .filter(|rec| rec.exit_observed && class.matches(&rec.lifecycle))
            .map(|rec| {
                let ended_at = rec
                    .lifecycle
                    .ended_at()
                    .unwrap_or_else(|| rec.lifecycle.started_at());
                (ended_at, rec.pane_id, rec)
            })
            .collect();
        // Keep the newest `keep` panes and evict the rest oldest first, so the human sees the
        // oldest panes disappear first.
        candidates.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let evict_count = candidates.len().saturating_sub(keep);
        candidates
            .into_iter()
            .take(evict_count)
            .map(|(_, _, rec)| evictable(rec, reason))
            .collect()
    }

    /// Marks panes closed and returns their close effects.
    ///
    /// A selection can be stale by the time it runs, so panes that became active, already closed,
    /// or no longer have an observed process exit are skipped instead of closed.
    pub fn mark_evicted(&mut self, panes: &[EvictablePane]) -> TransitionEffects {
        let mut effects = TransitionEffects::default();
        for pane in panes {
            if self.key_by_pane_id.get(&pane.pane_id) != Some(&pane.key) {
                continue;
            }
            let Some(record) = self.panes_by_key.get_mut(&pane.key) else {
                continue;
            };
            if !record.exit_observed
                || record.lifecycle.is_active()
                || record.lifecycle.is_pane_closed()
            {
                continue;
            }
            mark_lifecycle_pane_closed(&mut record.lifecycle);
            effects.extend(TransitionEffects::close_pane(pane.pane_id));
        }
        effects
    }

    /// Selects finished panes with an open pane whose process exit was observed.
    pub fn select_panes_for_cleanup(&self, request: &CleanupRequest) -> Vec<EvictablePane> {
        let mut selected: Vec<(SystemTime, EvictablePane)> = self
            .panes_by_key
            .values()
            .filter(|rec| {
                rec.exit_observed && !rec.lifecycle.is_active() && !rec.lifecycle.is_pane_closed()
            })
            .filter(|rec| request.target.includes(rec.tab_id))
            .filter(|rec| {
                request
                    .scopes
                    .iter()
                    .any(|scope| scope.matches(&rec.lifecycle))
            })
            .map(|rec| {
                let ended_at = rec
                    .lifecycle
                    .ended_at()
                    .unwrap_or_else(|| rec.lifecycle.started_at());
                (ended_at, evictable(rec, EvictionReason::ExplicitCleanup))
            })
            .collect();
        selected.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.pane_id().cmp(&b.1.pane_id()))
        });
        selected.into_iter().map(|(_, pane)| pane).collect()
    }

    /// Counts active entities of the kinds the request addresses inside its target tab, which
    /// cleanup must never touch.
    pub fn count_skipped_active(&self, request: &CleanupRequest) -> usize {
        self.panes_by_key
            .values()
            .filter(|rec| request.target.includes(rec.tab_id))
            .filter(|rec| {
                rec.lifecycle.is_active()
                    && request
                        .scopes
                        .iter()
                        .any(|scope| scope.covers_kind(&rec.lifecycle))
            })
            .count()
    }

    /// Counts finished entities whose process exit was observed and whose pane is still open.
    pub fn count_terminated_open_panes(&self) -> usize {
        self.panes_by_key
            .values()
            .filter(|rec| {
                rec.exit_observed && !rec.lifecycle.is_active() && !rec.lifecycle.is_pane_closed()
            })
            .count()
    }

    /// Marks a freshly completed task for closing right after its logs were backfilled.
    pub fn mark_fast_task_for_close(&mut self, key: &ManagedKey) -> Option<EvictablePane> {
        let pane = {
            let record = self.panes_by_key.get(key)?;
            if !record.exit_observed
                || !matches!(
                    record.lifecycle,
                    EntityLifecycle::Task(TaskLifecycle::Completed {
                        pane_closed: false,
                        ..
                    })
                )
            {
                return None;
            }
            evictable(record, EvictionReason::FastPathAutoClose)
        };
        let _ = self.mark_evicted(std::slice::from_ref(&pane));
        Some(pane)
    }

    /// Drains queued asynchronous alerts to attach to the next MCP tool response.
    pub fn drain_alerts(&mut self) -> Vec<WorkspaceAlert> {
        std::mem::take(&mut self.pending_alerts)
    }
}

fn evictable(record: &ManagedRecord, reason: EvictionReason) -> EvictablePane {
    EvictablePane {
        key: record.key.clone(),
        pane_id: record.pane_id,
        tab_id: record.tab_id,
        reason,
    }
}

fn mark_lifecycle_pane_closed(lifecycle: &mut EntityLifecycle) {
    match lifecycle {
        EntityLifecycle::Task(
            TaskLifecycle::Completed { pane_closed, .. }
            | TaskLifecycle::Failed { pane_closed, .. }
            | TaskLifecycle::Cancelled { pane_closed, .. },
        )
        | EntityLifecycle::Service(
            ServiceLifecycle::Failed { pane_closed, .. }
            | ServiceLifecycle::Stopped { pane_closed, .. },
        )
        | EntityLifecycle::Repl(ReplLifecycle::Exited { pane_closed, .. }) => {
            *pane_closed = true;
        }
        EntityLifecycle::Task(
            TaskLifecycle::Running { .. } | TaskLifecycle::StoppedByUser { .. },
        )
        | EntityLifecycle::Service(
            ServiceLifecycle::Starting { .. }
            | ServiceLifecycle::Ready { .. }
            | ServiceLifecycle::Stopping { .. }
            | ServiceLifecycle::StoppedByUser { .. },
        )
        | EntityLifecycle::Repl(
            ReplLifecycle::Starting { .. }
            | ReplLifecycle::Idle { .. }
            | ReplLifecycle::Busy { .. }
            | ReplLifecycle::InputRequired { .. }
            | ReplLifecycle::StoppedByUser { .. },
        ) => {}
    }
}

fn mark_lifecycle_pane_open(lifecycle: &mut EntityLifecycle) {
    match lifecycle {
        EntityLifecycle::Task(
            TaskLifecycle::Completed { pane_closed, .. }
            | TaskLifecycle::Failed { pane_closed, .. }
            | TaskLifecycle::Cancelled { pane_closed, .. },
        )
        | EntityLifecycle::Service(
            ServiceLifecycle::Failed { pane_closed, .. }
            | ServiceLifecycle::Stopped { pane_closed, .. },
        )
        | EntityLifecycle::Repl(ReplLifecycle::Exited { pane_closed, .. }) => {
            *pane_closed = false;
        }
        EntityLifecycle::Task(
            TaskLifecycle::Running { .. } | TaskLifecycle::StoppedByUser { .. },
        )
        | EntityLifecycle::Service(
            ServiceLifecycle::Starting { .. }
            | ServiceLifecycle::Ready { .. }
            | ServiceLifecycle::Stopping { .. }
            | ServiceLifecycle::StoppedByUser { .. },
        )
        | EntityLifecycle::Repl(
            ReplLifecycle::Starting { .. }
            | ReplLifecycle::Idle { .. }
            | ReplLifecycle::Busy { .. }
            | ReplLifecycle::InputRequired { .. }
            | ReplLifecycle::StoppedByUser { .. },
        ) => {}
    }
}

/// Returns the live dimensions of the focused tiled pane in `tab_id`, if any.
///
/// Returns `None` when focus is on a floating pane or when pane geometry is unavailable so
/// `LayoutRouter` does not enforce split guards against an unrelated pane's dimensions.
fn find_focused_pane_geometry(
    tab_id: TabId,
    panes: &[ObservedPane],
) -> Option<FocusedPaneGeometry> {
    panes
        .iter()
        .find(|p| {
            p.tab_id == tab_id && !p.is_floating && p.is_focused && p.columns > 0 && p.rows > 0
        })
        .map(|p| FocusedPaneGeometry {
            columns: p.columns,
            rows: p.rows,
        })
}

fn format_record_title(record: &ManagedRecord) -> String {
    let badge = record.lifecycle.badge().to_string();
    PaneTitleCodec::format(&badge, &record.key, record.label.as_deref())
}
