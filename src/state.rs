//! Pure in-memory state projection (`StateStore`) driven by Zellij events and reconciliation.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroI32;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::domain::ids::{
    CommandSpec, CompiledPattern, ManagedKey, PaneTitleCodec, ReplName, ServiceName, SessionName,
    TabId, TaskId, TerminalPaneId,
};
use crate::domain::layout::{
    LayoutState, MANAGED_SERVICES_TAB_NAME, MANAGED_TASKS_TAB_NAME, ResolvedPlacement,
    ViewportSnapshot,
};
use crate::domain::lifecycle::{
    ERROR_PANE_BG_HEX, ERROR_PANE_BORDER_STYLE, EntityLifecycle, ReplLifecycle, ServiceLifecycle,
    TaskLifecycle, VisualAction, WorkspaceAlert,
};
use crate::logs::{BoundedLogBuffer, diff_viewport, strip_ansi};

/// Number of recently completed task panes kept open before automatic GC.
pub const MAX_RETAINED_COMPLETED_TASKS: usize = 5;
/// Maximum number of buffered early exit/close events for panes not yet registered.
const MAX_EARLY_EVENT_BUFFER: usize = 64;

/// Compile-time non-zero fallback exit code when a task exits without a numeric status.
const FALLBACK_NONZERO_EXIT: NonZeroI32 = match NonZeroI32::new(1) {
    Some(code) => code,
    None => unreachable!(),
};

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
    pub env: Vec<(String, String)>,
    pub ready_pattern: Option<CompiledPattern>,
    pub prompt_pattern: Option<CompiledPattern>,
    pub lifecycle: EntityLifecycle,
    pub logs: BoundedLogBuffer,
    pub last_viewport: Vec<String>,
}

/// Pure effects returned by `StateStore` mutations for the imperative shell to execute.
#[derive(Debug, Clone, Default)]
pub struct TransitionEffects {
    pub visual_actions: Vec<VisualAction>,
    pub alerts: Vec<WorkspaceAlert>,
    pub changed_keys: Vec<ManagedKey>,
    pub closed_panes: Vec<TerminalPaneId>,
    pub subscription_dirty: bool,
}

impl TransitionEffects {
    pub fn extend(&mut self, other: Self) {
        self.visual_actions.extend(other.visual_actions);
        self.alerts.extend(other.alerts);
        self.changed_keys.extend(other.changed_keys);
        self.closed_panes.extend(other.closed_panes);
        self.subscription_dirty |= other.subscription_dirty;
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
    pub is_floating: bool,
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
    panes_by_key: HashMap<ManagedKey, ManagedRecord>,
    key_by_pane_id: HashMap<TerminalPaneId, ManagedKey>,
    early_exits: HashMap<TerminalPaneId, Option<i32>>,
    early_closes: HashSet<TerminalPaneId>,
    pending_alerts: Vec<WorkspaceAlert>,
    next_task_seq: u64,
}

impl StateStore {
    pub fn new(session: SessionName) -> Self {
        Self {
            layout: LayoutState::new(session),
            panes_by_key: HashMap::new(),
            key_by_pane_id: HashMap::new(),
            early_exits: HashMap::new(),
            early_closes: HashSet::new(),
            pending_alerts: Vec::new(),
            next_task_seq: 1,
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
        let pane_id = record.pane_id;
        if let Some(old) = self.panes_by_key.insert(record.key.clone(), record.clone())
            && old.pane_id != pane_id
        {
            let _ = self.key_by_pane_id.remove(&old.pane_id);
        }
        let _ = self.key_by_pane_id.insert(pane_id, record.key);

        let mut effects = TransitionEffects::default();
        if let Some(exit_code) = self.early_exits.remove(&pane_id) {
            effects.extend(self.apply_event(
                ZellijEvent::PaneExited { pane_id, exit_code },
                SystemTime::now(),
            ));
        }
        if self.early_closes.remove(&pane_id) {
            effects
                .extend(self.apply_event(ZellijEvent::PaneClosed { pane_id }, SystemTime::now()));
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
                                effects.subscription_dirty = true;
                                for gc_pane in self.select_and_mark_completed_tasks_for_gc(
                                    MAX_RETAINED_COMPLETED_TASKS,
                                ) {
                                    effects
                                        .visual_actions
                                        .push(VisualAction::ClosePane { pane_id: gc_pane });
                                    effects.closed_panes.push(gc_pane);
                                }
                            }
                            other_nonzero => {
                                let code = other_nonzero.unwrap_or(FALLBACK_NONZERO_EXIT);
                                record.lifecycle = EntityLifecycle::Task(TaskLifecycle::Failed {
                                    started_at,
                                    ended_at: now,
                                    exit_code: code,
                                    attention_marked: false,
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
                        });
                        let title = format_record_title(record);
                        record.pane_title = title.clone();
                        effects
                            .visual_actions
                            .push(VisualAction::RenamePane { pane_id, title });
                        effects.changed_keys.push(key);
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
                if let EntityLifecycle::Task(TaskLifecycle::Completed { pane_closed, .. }) =
                    &mut record.lifecycle
                {
                    *pane_closed = true;
                }
                if record.lifecycle.is_active() {
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
                    effects.changed_keys.push(key);
                    effects.subscription_dirty = true;
                }
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
                effects.subscription_dirty = true;
            }
        }
        effects
    }

    /// Applies a viewport frame from `zellij subscribe`, appending diffed lines, checking
    /// service readiness, and recovering REPLs from `InputRequired` once new output resumes.
    pub fn apply_viewport_frame(
        &mut self,
        pane_id: TerminalPaneId,
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
                    Some(pat) => delta.iter().any(|line| pat.is_match(line)),
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
        let mut effects = TransitionEffects::default();

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
            self.layout.viewport = Some(tab.viewport);
        }

        let live_pane_ids: HashSet<TerminalPaneId> =
            snapshot.panes.iter().map(|p| p.pane_id).collect();

        // Mark previously tracked panes that disappeared as closed / StoppedByUser.
        for record in self.panes_by_key.values_mut() {
            if !live_pane_ids.contains(&record.pane_id) {
                if let EntityLifecycle::Task(TaskLifecycle::Completed { pane_closed, .. }) =
                    &mut record.lifecycle
                {
                    *pane_closed = true;
                }
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
                if old_pane_id != pane.pane_id {
                    existing.pane_id = pane.pane_id;
                    let _ = self.key_by_pane_id.remove(&old_pane_id);
                    let _ = self.key_by_pane_id.insert(pane.pane_id, parsed.key.clone());
                    effects.subscription_dirty = true;
                }
                if pane.exited && existing.lifecycle.is_active() {
                    let pane_id = existing.pane_id;
                    effects.extend(self.apply_event(
                        ZellijEvent::PaneExited {
                            pane_id,
                            exit_code: pane.exit_status,
                        },
                        now,
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
                        })
                    } else {
                        EntityLifecycle::Repl(ReplLifecycle::Idle { started_at: now })
                    }
                }
            };

            effects.extend(self.register(ManagedRecord {
                key: parsed.key,
                tab_id: pane.tab_id,
                pane_id: pane.pane_id,
                pane_title: pane.title,
                label: parsed.label,
                placement,
                cmd,
                cwd: pane.cwd.unwrap_or_else(|| PathBuf::from(".")),
                env: Vec::new(),
                ready_pattern: None,
                prompt_pattern: None,
                lifecycle,
                logs: BoundedLogBuffer::default(),
                last_viewport: Vec::new(),
            }));
        }

        effects
    }

    /// Selects oldest open `Completed` task panes exceeding `keep_recent` for GC; never selects
    /// failed or already-closed panes.
    pub fn select_completed_tasks_for_gc(&self, keep_recent: usize) -> Vec<TerminalPaneId> {
        let mut completed: Vec<(SystemTime, TerminalPaneId)> = self
            .panes_by_key
            .values()
            .filter_map(|rec| match &rec.lifecycle {
                EntityLifecycle::Task(TaskLifecycle::Completed {
                    ended_at,
                    pane_closed: false,
                    ..
                }) => Some((*ended_at, rec.pane_id)),
                EntityLifecycle::Task(
                    TaskLifecycle::Completed {
                        pane_closed: true, ..
                    }
                    | TaskLifecycle::Running { .. }
                    | TaskLifecycle::Failed { .. }
                    | TaskLifecycle::Cancelled { .. }
                    | TaskLifecycle::StoppedByUser { .. },
                )
                | EntityLifecycle::Service(_)
                | EntityLifecycle::Repl(_) => None,
            })
            .collect();

        // Sort newest first; evict everything beyond `keep_recent`.
        completed.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        completed
            .into_iter()
            .skip(keep_recent)
            .map(|(_, pane_id)| pane_id)
            .collect()
    }

    fn select_and_mark_completed_tasks_for_gc(
        &mut self,
        keep_recent: usize,
    ) -> Vec<TerminalPaneId> {
        let to_close = self.select_completed_tasks_for_gc(keep_recent);
        for pane_id in &to_close {
            if let Some(key) = self.key_by_pane_id.get(pane_id).cloned()
                && let Some(rec) = self.panes_by_key.get_mut(&key)
                && let EntityLifecycle::Task(TaskLifecycle::Completed { pane_closed, .. }) =
                    &mut rec.lifecycle
            {
                *pane_closed = true;
            }
        }
        to_close
    }

    /// Drains queued asynchronous alerts to attach to the next MCP tool response.
    pub fn drain_alerts(&mut self) -> Vec<WorkspaceAlert> {
        std::mem::take(&mut self.pending_alerts)
    }
}

fn format_record_title(record: &ManagedRecord) -> String {
    let badge = record.lifecycle.badge().to_string();
    PaneTitleCodec::format(&badge, &record.key, record.label.as_deref())
}
