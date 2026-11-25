//! Async workspace orchestrator coordinating `StateStore`, Zellij CLI, streams, locks, and GC.

pub mod cache;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;
use regex::Regex;
use tokio::sync::{Mutex, RwLock, mpsc, watch};
use tracing::warn;

use crate::domain::error::DomainError;
use crate::domain::ids::{
    CommandSpec, CompiledPattern, EnterPolicy, KeyChord, LogSeq, ManagedKey, PaneClosePolicy,
    PaneTitleCodec, ReplInput, ReplName, ServiceName, SessionResolution, TabId, TaskId,
    TerminalPaneId, WaitMode,
};
use crate::domain::layout::{
    EntityKind, LayoutRouter, ManagedTabKind, PlacementRequest, ResolvedPlacement, TabMode,
    TargetTab,
};
use crate::domain::lifecycle::{
    EntityLifecycle, ReplLifecycle, ServiceLifecycle, TaskLifecycle, VisualAction, WorkspaceAlert,
};
use crate::logs::{
    BoundedLogBuffer, DEFAULT_TRUNCATE_HEAD, DEFAULT_TRUNCATE_TAIL, LogSlice,
    filter_and_tail_lines, strip_ansi, truncate_head_tail,
};
use crate::runtime::cache::{CachedPaneEntry, SessionCache, load_cache, save_cache};
use crate::state::{
    ManagedRecord, ReconciliationSnapshot, StateStore, TransitionEffects, ZellijEvent,
};
use crate::zellij::cli::{NewPaneSpec, ZellijClient};
use crate::zellij::protocol::SubscribeFrame;
use crate::zellij::streams::{StreamConfig, StreamHandles, spawn_streams};

/// Default synchronous fast-path wait window for `task_run`.
pub const DEFAULT_TASK_FAST_WAIT: Duration = Duration::from_millis(3_000);
/// Default server-side blocking timeout for `task_wait`.
pub const DEFAULT_TASK_WAIT_TIMEOUT: Duration = Duration::from_millis(60_000);
/// Default readiness wait timeout for `service_start` / `service_restart`.
pub const DEFAULT_SERVICE_READY_TIMEOUT: Duration = Duration::from_millis(15_000);
/// Default execution wait timeout for `repl_exec`.
pub const DEFAULT_REPL_EXEC_TIMEOUT: Duration = Duration::from_millis(15_000);
/// Quiet debounce window used to detect settled REPL output when no prompt regex matches.
pub const DEFAULT_REPL_QUIET_DEBOUNCE: Duration = Duration::from_millis(800);
/// Grace period after `Ctrl c` before escalating during `task_cancel`.
pub const CANCEL_GRACE_PERIOD: Duration = Duration::from_secs(2);
/// Grace period after `Ctrl c` during `service_stop` before closing the pane.
pub const SERVICE_STOP_GRACE_PERIOD: Duration = Duration::from_secs(1);
/// Default timeout for interactive confirmation popups when `--confirm-destructive` is enabled.
pub const CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

fn interactive_auth_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        match Regex::new(
            r"(?i)(password\s*:|passphrase|verification\s+code|2fa|otp\s*:|\[y/n\]|\(yes/no|enter\s+pin|sudo.*password)",
        ) {
            Ok(re) => re,
            Err(err) => panic!("hardcoded interactive auth regex must compile: {err}"),
        }
    })
}

/// Runtime configuration for `WorkspaceOrchestrator`.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub session_resolution: SessionResolution,
    pub confirm_destructive: bool,
    pub default_cwd: PathBuf,
}

/// Request payload for `task_run` after boundary parsing.
#[derive(Debug, Clone)]
pub struct TaskRunRequest {
    pub cmd: CommandSpec,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
    pub name: Option<String>,
    pub wait_timeout: Duration,
    pub placement: PlacementRequest,
}

/// Outcome of `task_run` (fast-path terminal state or seamless async handoff).
#[derive(Debug, Clone)]
pub enum TaskRunOutcome {
    Completed {
        task_id: TaskId,
        pane_id: TerminalPaneId,
        pane_title: String,
        status: &'static str,
        exit_code: Option<i32>,
        elapsed_ms: u64,
        output: String,
    },
    Running {
        task_id: TaskId,
        pane_id: TerminalPaneId,
        pane_title: String,
        elapsed_ms: u64,
        tail_preview: String,
    },
}

/// Outcome of `task_wait`.
#[derive(Debug, Clone)]
pub struct TaskWaitOutcome {
    pub timed_out: bool,
    pub tasks: Vec<TaskSummary>,
}

/// Snapshot summary of a single task returned by `task_wait`.
#[derive(Debug, Clone)]
pub struct TaskSummary {
    pub task_id: TaskId,
    pub pane_id: TerminalPaneId,
    pub pane_title: String,
    pub status: &'static str,
    pub exit_code: Option<i32>,
    pub output: String,
}

/// Request payload for `service_start` after boundary parsing.
#[derive(Debug, Clone)]
pub struct ServiceStartRequest {
    pub name: ServiceName,
    pub cmd: CommandSpec,
    pub cwd: Option<PathBuf>,
    pub ready_pattern: Option<CompiledPattern>,
    pub ready_timeout: Duration,
    pub placement: PlacementRequest,
}

/// Outcome of `service_start` / `service_restart`.
#[derive(Debug, Clone)]
pub struct ServiceStartOutcome {
    pub name: ServiceName,
    pub pane_id: TerminalPaneId,
    pub pane_title: String,
    pub reused: bool,
    pub status: &'static str,
    pub matched_line: Option<String>,
    pub tail_preview: String,
}

/// Request payload for `repl_open` after boundary parsing.
#[derive(Debug, Clone)]
pub struct ReplOpenRequest {
    pub name: ReplName,
    pub cmd: CommandSpec,
    pub cwd: Option<PathBuf>,
    pub prompt_pattern: Option<CompiledPattern>,
    pub placement: PlacementRequest,
}

/// Request payload for `repl_exec` after boundary parsing.
#[derive(Debug, Clone)]
pub struct ReplExecRequest {
    pub name: ReplName,
    pub input: ReplInput,
    pub wait_for_prompt: Option<CompiledPattern>,
    pub timeout: Duration,
}

/// Outcome of `repl_exec` or `repl_open`.
#[derive(Debug, Clone)]
pub struct ReplExecOutcome {
    pub name: ReplName,
    pub pane_id: TerminalPaneId,
    pub pane_title: String,
    pub status: &'static str,
    pub delta_output: String,
    pub timed_out: bool,
}

/// Read-only workspace snapshot returned by `workspace_status`.
#[derive(Debug, Clone)]
pub struct StateSnapshot {
    pub session_resolution: SessionResolution,
    pub layout: crate::domain::layout::LayoutState,
    pub records: Vec<ManagedRecord>,
}

/// Cloneable async orchestrator (`M-SERVICES-CLONE`).
#[derive(Debug, Clone)]
pub struct WorkspaceOrchestrator {
    inner: Arc<OrchestratorInner>,
}

#[derive(Debug)]
struct OrchestratorInner {
    config: OrchestratorConfig,
    client: ZellijClient,
    state: RwLock<StateStore>,
    pane_write_locks: Mutex<HashMap<TerminalPaneId, Arc<Mutex<()>>>>,
    /// Serializes lazy managed-tab creation and idempotent service/REPL check-and-spawn.
    provision_lock: Mutex<()>,
    /// Monotonic state generation counter preventing lost wakeups across async waiters.
    state_gen: watch::Sender<u64>,
    streams: Mutex<Option<StreamHandles>>,
}

impl WorkspaceOrchestrator {
    pub fn new(config: OrchestratorConfig, client: ZellijClient) -> Self {
        let session = config.session_resolution.session_name().clone();
        let (state_gen, _) = watch::channel(0u64);
        Self {
            inner: Arc::new(OrchestratorInner {
                config,
                client,
                state: RwLock::new(StateStore::new(session)),
                pane_write_locks: Mutex::new(HashMap::new()),
                provision_lock: Mutex::new(()),
                state_gen,
                streams: Mutex::new(None),
            }),
        }
    }

    pub fn config(&self) -> &OrchestratorConfig {
        &self.inner.config
    }

    fn bump_state_gen(&self) {
        self.inner.state_gen.send_modify(|g| *g = g.wrapping_add(1));
    }

    fn subscribe_state_gen(&self) -> watch::Receiver<u64> {
        self.inner.state_gen.subscribe()
    }

    /// Runs startup checks (`zellij --version`, `ensure_session`), loads cache, reconciles
    /// against live Zellij state, and spawns the background `events` + `subscribe` streams.
    pub async fn initialize(&self) -> Result<()> {
        let _ = self.inner.client.check_version().await?;
        self.inner.client.ensure_session().await?;

        let cached = load_cache(self.inner.client.session()).await?;
        self.reconcile_now().await?;

        if let Some(cache) = cached {
            let mut store = self.inner.state.write().await;
            for entry in cache.panes {
                if let Some(record) = store.get_mut(&entry.key) {
                    record.cmd = entry.cmd;
                    record.cwd = entry.cwd;
                    if let Some(pat) = entry.ready_pattern
                        && let Ok(compiled) = CompiledPattern::parse("ready_pattern", pat)
                    {
                        record.ready_pattern = Some(compiled);
                    }
                    if let Some(pat) = entry.prompt_pattern
                        && let Ok(compiled) = CompiledPattern::parse("prompt_pattern", pat)
                    {
                        record.prompt_pattern = Some(compiled);
                    }
                }
            }
        }

        if let Some(binary) = self.inner.client.binary_path().cloned() {
            let (event_tx, mut event_rx) = mpsc::channel::<ZellijEvent>(256);
            let (frame_tx, mut frame_rx) = mpsc::channel::<SubscribeFrame>(256);
            let handles = spawn_streams(
                StreamConfig {
                    binary,
                    session: self.inner.client.session().clone(),
                },
                event_tx,
                frame_tx,
            );
            {
                let store = self.inner.state.read().await;
                handles.update_subscribed_panes(store.subscribed_pane_ids());
            }
            *self.inner.streams.lock().await = Some(handles);

            // Hold a Weak reference so dropping all external WorkspaceOrchestrator handles
            // allows the forwarding task and stream subprocesses to terminate cleanly.
            let weak_inner = Arc::downgrade(&self.inner);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        ev = event_rx.recv() => {
                            let Some(ev) = ev else { break; };
                            let Some(inner) = weak_inner.upgrade() else { break; };
                            WorkspaceOrchestrator { inner }.inject_event(ev).await;
                        }
                        frame = frame_rx.recv() => {
                            let Some(frame) = frame else { break; };
                            let Some(inner) = weak_inner.upgrade() else { break; };
                            WorkspaceOrchestrator { inner }.inject_viewport_frame(frame).await;
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Gracefully stops background stream subprocesses without destroying the Zellij session.
    pub async fn shutdown(&self) {
        let handles = self.inner.streams.lock().await.take();
        if let Some(handles) = handles {
            handles.shutdown().await;
        }
    }

    /// Applies an incoming `ZellijEvent`, executes resulting visual actions, updates subscriptions,
    /// and wakes all waiting tasks/services/REPLs.
    pub async fn inject_event(&self, event: ZellijEvent) {
        let effects = {
            let mut store = self.inner.state.write().await;
            store.apply_event(event, SystemTime::now())
        };
        self.apply_effects(effects).await;
    }

    /// Applies an incoming `SubscribeFrame`, executes resulting visual actions, and wakes waiters.
    pub async fn inject_viewport_frame(&self, frame: SubscribeFrame) {
        let effects = {
            let mut store = self.inner.state.write().await;
            store.apply_viewport_frame(frame.pane_id, frame.viewport_lines, SystemTime::now())
        };
        self.apply_effects(effects).await;
    }

    async fn apply_effects(&self, effects: TransitionEffects) {
        for action in &effects.visual_actions {
            if let Err(err) = self.inner.client.apply_visual_action(action).await {
                warn!(?action, error = %err, "visual action failed");
            }
        }
        if !effects.closed_panes.is_empty() {
            let mut locks = self.inner.pane_write_locks.lock().await;
            for pane_id in &effects.closed_panes {
                let _ = locks.remove(pane_id);
            }
        }
        if effects.subscription_dirty {
            self.sync_subscriptions().await;
        }
        self.bump_state_gen();
    }

    async fn sync_subscriptions(&self) {
        let pane_ids = {
            let store = self.inner.state.read().await;
            store.subscribed_pane_ids()
        };
        if let Some(handles) = self.inner.streams.lock().await.as_ref() {
            handles.update_subscribed_panes(pane_ids);
        }
    }

    async fn persist_cache_best_effort(&self) {
        let cache = {
            let store = self.inner.state.read().await;
            SessionCache {
                managed_tasks_tab: store.layout.managed_tabs.tasks,
                managed_services_tab: store.layout.managed_tabs.services,
                panes: store
                    .all_records()
                    .map(|r| CachedPaneEntry {
                        key: r.key.clone(),
                        pane_id: r.pane_id,
                        tab_id: r.tab_id,
                        placement: r.placement,
                        cmd: r.cmd.clone(),
                        cwd: r.cwd.clone(),
                        ready_pattern: r.ready_pattern.as_ref().map(|p| p.as_str().to_owned()),
                        prompt_pattern: r.prompt_pattern.as_ref().map(|p| p.as_str().to_owned()),
                    })
                    .collect(),
            }
        };
        if let Err(err) = save_cache(self.inner.client.session(), &cache).await {
            warn!(error = %err, "failed to persist session cache");
        }
    }

    async fn reconcile_now(&self) -> Result<()> {
        let (tabs, panes, active_tab) = tokio::try_join!(
            self.inner.client.list_tabs(),
            self.inner.client.list_panes(),
            self.inner.client.current_tab_info(),
        )?;
        let effects = {
            let mut store = self.inner.state.write().await;
            store.reconcile(
                ReconciliationSnapshot {
                    tabs,
                    panes,
                    active_tab,
                },
                SystemTime::now(),
            )
        };
        self.apply_effects(effects).await;
        self.sync_subscriptions().await;
        Ok(())
    }

    async fn resolve_target_tab_and_spawn(
        &self,
        kind: EntityKind,
        request: PlacementRequest,
        pane_title: String,
        cwd: PathBuf,
        cmd: CommandSpec,
        env: Vec<(String, String)>,
    ) -> Result<(TabId, TerminalPaneId, ResolvedPlacement)> {
        let plan = {
            let store = self.inner.state.read().await;
            let active_tasks = store
                .layout
                .managed_tabs
                .tasks
                .map_or(0, |t| store.count_active_in_tab(t));
            let active_services = store
                .layout
                .managed_tabs
                .services
                .map_or(0, |t| store.count_active_in_tab(t));
            LayoutRouter::plan(&store.layout, kind, request, active_tasks, active_services)
        };

        let target_tab_id = match plan.target_tab {
            TargetTab::ActiveTab(id) => id,
            TargetTab::ManagedTab {
                kind: tab_kind,
                existing: Some(tab_id),
            } => {
                let _ = tab_kind;
                Some(tab_id)
            }
            TargetTab::ManagedTab {
                kind: tab_kind,
                existing: None,
            } => {
                // Zellij 0.46.1 in headless `--create-background` sessions without an attached
                // client may return empty stdout or a non-materialized tab ID from `new-tab`.
                // Verify via `list-tabs` and fall back cleanly to the existing tab if absent.
                let maybe_created = match self.inner.client.new_tab(tab_kind.tab_name()).await {
                    Ok(created) => {
                        let materialized = self
                            .inner
                            .client
                            .list_tabs()
                            .await
                            .map(|tabs| tabs.iter().any(|t| t.tab_id == created))
                            .unwrap_or(false);
                        if materialized { Some(created) } else { None }
                    }
                    Err(err) => {
                        warn!(
                            tab = tab_kind.tab_name(),
                            error = %err,
                            "lazy new-tab failed; falling back to active tab"
                        );
                        None
                    }
                };
                if let Some(created) = maybe_created {
                    let mut store = self.inner.state.write().await;
                    match tab_kind {
                        ManagedTabKind::Tasks => store.layout.managed_tabs.tasks = Some(created),
                        ManagedTabKind::Services => {
                            store.layout.managed_tabs.services = Some(created)
                        }
                    }
                    Some(created)
                } else {
                    let store = self.inner.state.read().await;
                    store.layout.active_tab_id
                }
            }
        };

        let pane_id = self
            .inner
            .client
            .new_pane(&NewPaneSpec {
                tab_id: target_tab_id,
                name: pane_title,
                placement: plan.placement,
                stacked: plan.open_stacked,
                cwd: Some(cwd),
                cmd,
                env,
            })
            .await?;

        if plan.trigger_tab_stack_swap
            && let Some(tab_id) = target_tab_id
            && self.inner.client.apply_stacked_layout(tab_id).await?
        {
            let mut store = self.inner.state.write().await;
            let _ = store.layout.tab_modes.insert(tab_id, TabMode::Stacked);
        }

        let effective_tab_id = target_tab_id.unwrap_or(TabId::new(0));
        Ok((effective_tab_id, pane_id, plan.placement))
    }

    async fn maybe_confirm_destructive(&self, message: &str) -> Result<()> {
        if !self.inner.config.confirm_destructive {
            return Ok(());
        }
        let approved = self
            .inner
            .client
            .confirm_destructive(message, CONFIRM_TIMEOUT)
            .await?;
        if !approved {
            return Err(DomainError::UserDeniedConfirmation.into());
        }
        Ok(())
    }

    async fn read_task_output_truncated(
        &self,
        pane_id: TerminalPaneId,
        key: &ManagedKey,
    ) -> String {
        if let Ok(raw) = self.inner.client.dump_screen(pane_id, false).await {
            let cleaned = strip_ansi(&raw);
            let lines: Vec<String> = cleaned.lines().map(ToOwned::to_owned).collect();
            if !lines.is_empty() {
                return truncate_head_tail(&lines, DEFAULT_TRUNCATE_HEAD, DEFAULT_TRUNCATE_TAIL);
            }
        }
        let store = self.inner.state.read().await;
        store
            .get(key)
            .map(|r| r.logs.tail_preview(DEFAULT_TRUNCATE_TAIL))
            .unwrap_or_default()
    }

    /// Reconciles live Zellij tabs/panes and returns a full workspace snapshot plus pending alerts.
    pub async fn workspace_status(&self) -> Result<(StateSnapshot, Vec<WorkspaceAlert>)> {
        self.reconcile_now().await?;
        let mut store = self.inner.state.write().await;
        let alerts = store.drain_alerts();
        let snapshot = StateSnapshot {
            session_resolution: self.inner.config.session_resolution.clone(),
            layout: store.layout.clone(),
            records: store.all_records().cloned().collect(),
        };
        Ok((snapshot, alerts))
    }

    pub async fn task_run(
        &self,
        req: TaskRunRequest,
    ) -> Result<(TaskRunOutcome, Vec<WorkspaceAlert>)> {
        let started_instant = Instant::now();
        let now = SystemTime::now();
        let task_id = {
            let mut store = self.inner.state.write().await;
            store.allocate_task_id()
        };
        let key = ManagedKey::Task(task_id.clone());
        let label = req
            .name
            .clone()
            .or_else(|| Some(req.cmd.program().to_owned()));
        let pane_title = PaneTitleCodec::format("[RUN]", &key, label.as_deref());
        let cwd = req
            .cwd
            .unwrap_or_else(|| self.inner.config.default_cwd.clone());

        let (tab_id, pane_id, placement) = {
            let _guard = self.inner.provision_lock.lock().await;
            self.resolve_target_tab_and_spawn(
                EntityKind::Task,
                req.placement,
                pane_title.clone(),
                cwd.clone(),
                req.cmd.clone(),
                req.env.clone(),
            )
            .await?
        };

        let early_effects = {
            let mut store = self.inner.state.write().await;
            store.register(ManagedRecord {
                key: key.clone(),
                tab_id,
                pane_id,
                pane_title: pane_title.clone(),
                label,
                placement,
                cmd: req.cmd,
                cwd,
                env: req.env,
                ready_pattern: None,
                prompt_pattern: None,
                lifecycle: EntityLifecycle::Task(TaskLifecycle::Running { started_at: now }),
                logs: BoundedLogBuffer::default(),
                last_viewport: Vec::new(),
            })
        };
        self.apply_effects(early_effects).await;
        self.sync_subscriptions().await;
        self.persist_cache_best_effort().await;

        let deadline = tokio::time::Instant::now() + req.wait_timeout;
        let mut gen_rx = self.subscribe_state_gen();
        loop {
            // Mark current generation seen BEFORE checking state so no update can be lost.
            let _ = *gen_rx.borrow_and_update();
            {
                let store = self.inner.state.read().await;
                if let Some(rec) = store.get(&key)
                    && !rec.lifecycle.is_active()
                {
                    let status = rec.lifecycle.status_label();
                    let exit_code = rec.lifecycle.exit_code();
                    let title = rec.pane_title.clone();
                    drop(store);
                    let output = self.read_task_output_truncated(pane_id, &key).await;
                    let alerts = self.inner.state.write().await.drain_alerts();
                    return Ok((
                        TaskRunOutcome::Completed {
                            task_id,
                            pane_id,
                            pane_title: title,
                            status,
                            exit_code,
                            elapsed_ms: u64::try_from(started_instant.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                            output,
                        },
                        alerts,
                    ));
                }
            }
            if tokio::time::timeout_at(deadline, gen_rx.changed())
                .await
                .is_err()
            {
                break;
            }
        }

        let mut store = self.inner.state.write().await;
        let tail_preview = store
            .get(&key)
            .map(|r| r.logs.tail_preview(10))
            .unwrap_or_default();
        let alerts = store.drain_alerts();
        Ok((
            TaskRunOutcome::Running {
                task_id,
                pane_id,
                pane_title,
                elapsed_ms: u64::try_from(started_instant.elapsed().as_millis())
                    .unwrap_or(u64::MAX),
                tail_preview,
            },
            alerts,
        ))
    }

    pub async fn task_wait(
        &self,
        task_ids: Vec<TaskId>,
        mode: WaitMode,
        timeout: Duration,
    ) -> Result<(TaskWaitOutcome, Vec<WorkspaceAlert>)> {
        {
            let store = self.inner.state.read().await;
            for task_id in &task_ids {
                if store.get(&ManagedKey::Task(task_id.clone())).is_none() {
                    return Err(DomainError::TaskNotFound(task_id.clone()).into());
                }
            }
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let mut gen_rx = self.subscribe_state_gen();
        let mut timed_out = false;
        loop {
            let _ = *gen_rx.borrow_and_update();
            let done = {
                let store = self.inner.state.read().await;
                let terminal_count = task_ids
                    .iter()
                    .filter(|id| {
                        store
                            .get(&ManagedKey::Task((*id).clone()))
                            .is_some_and(|r| !r.lifecycle.is_active())
                    })
                    .count();
                match mode {
                    WaitMode::Any => terminal_count >= 1,
                    WaitMode::All => terminal_count == task_ids.len(),
                }
            };
            if done {
                break;
            }
            if tokio::time::timeout_at(deadline, gen_rx.changed())
                .await
                .is_err()
            {
                timed_out = true;
                break;
            }
        }

        let mut summaries = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            let key = ManagedKey::Task(task_id.clone());
            let record = {
                let store = self.inner.state.read().await;
                store.get(&key).cloned()
            };
            if let Some(rec) = record {
                let output = if !rec.lifecycle.is_active() {
                    self.read_task_output_truncated(rec.pane_id, &key).await
                } else {
                    rec.logs.tail_preview(20)
                };
                summaries.push(TaskSummary {
                    task_id,
                    pane_id: rec.pane_id,
                    pane_title: rec.pane_title,
                    status: rec.lifecycle.status_label(),
                    exit_code: rec.lifecycle.exit_code(),
                    output,
                });
            }
        }

        let alerts = self.inner.state.write().await.drain_alerts();
        Ok((
            TaskWaitOutcome {
                timed_out,
                tasks: summaries,
            },
            alerts,
        ))
    }

    pub async fn task_logs(
        &self,
        task_id: &TaskId,
        tail_lines: usize,
        grep: Option<&CompiledPattern>,
    ) -> Result<(String, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Task(task_id.clone());
        let record = {
            let store = self.inner.state.read().await;
            store
                .get(&key)
                .cloned()
                .ok_or_else(|| DomainError::TaskNotFound(task_id.clone()))?
        };

        let output = if !record.lifecycle.is_active() {
            match self.inner.client.dump_screen(record.pane_id, false).await {
                Ok(raw) => filter_and_tail_lines(&raw, tail_lines, grep).join("\n"),
                Err(_) => record
                    .logs
                    .query(None, tail_lines, grep)
                    .entries
                    .into_iter()
                    .map(|e| e.line)
                    .collect::<Vec<_>>()
                    .join("\n"),
            }
        } else {
            record
                .logs
                .query(None, tail_lines, grep)
                .entries
                .into_iter()
                .map(|e| e.line)
                .collect::<Vec<_>>()
                .join("\n")
        };

        let alerts = self.inner.state.write().await.drain_alerts();
        Ok((output, alerts))
    }

    pub async fn task_cancel(
        &self,
        task_id: &TaskId,
        close_policy: PaneClosePolicy,
    ) -> Result<(String, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Task(task_id.clone());
        let record = {
            let store = self.inner.state.read().await;
            store
                .get(&key)
                .cloned()
                .ok_or_else(|| DomainError::TaskNotFound(task_id.clone()))?
        };

        self.maybe_confirm_destructive(&format!("Cancel task {task_id}?"))
            .await?;

        let mut was_active = record.lifecycle.is_active();
        if was_active {
            let ctrl_c = vec![KeyChord::parse("Ctrl c")?];
            let lock = self.pane_write_lock(record.pane_id).await;
            {
                let _guard = lock.lock().await;
                let _ = self.inner.client.send_keys(record.pane_id, &ctrl_c).await;
            }

            let exited = self.wait_until_inactive(&key, CANCEL_GRACE_PERIOD).await;
            if !exited {
                // Escalate with SIGQUIT (`Ctrl \`) after the 2s grace period.
                let ctrl_backslash = vec![KeyChord::parse(r"Ctrl \")?];
                {
                    let _guard = lock.lock().await;
                    let _ = self
                        .inner
                        .client
                        .send_keys(record.pane_id, &ctrl_backslash)
                        .await;
                }
                let _ = self
                    .wait_until_inactive(&key, Duration::from_millis(500))
                    .await;
            }
            was_active = true;
        }

        let mut visual_actions = Vec::new();
        let final_status = {
            let mut store = self.inner.state.write().await;
            if let Some(rec) = store.get_mut(&key) {
                if was_active {
                    let started_at = rec.lifecycle.started_at();
                    let exit_code = rec.lifecycle.exit_code();
                    rec.lifecycle = EntityLifecycle::Task(TaskLifecycle::Cancelled {
                        started_at,
                        ended_at: SystemTime::now(),
                        exit_code,
                    });
                    let title = PaneTitleCodec::format("[STOP]", &key, rec.label.as_deref());
                    rec.pane_title = title.clone();
                    visual_actions.push(VisualAction::RenamePane {
                        pane_id: rec.pane_id,
                        title,
                    });
                }
                rec.lifecycle.status_label().to_owned()
            } else {
                "stopped".to_owned()
            }
        };

        if close_policy == PaneClosePolicy::ClosePane {
            visual_actions.push(VisualAction::ClosePane {
                pane_id: record.pane_id,
            });
            let _ = self
                .inner
                .pane_write_locks
                .lock()
                .await
                .remove(&record.pane_id);
        }
        for action in &visual_actions {
            let _ = self.inner.client.apply_visual_action(action).await;
        }
        self.sync_subscriptions().await;
        self.bump_state_gen();

        let alerts = self.inner.state.write().await.drain_alerts();
        Ok((final_status, alerts))
    }

    async fn wait_until_inactive(&self, key: &ManagedKey, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut gen_rx = self.subscribe_state_gen();
        loop {
            let _ = *gen_rx.borrow_and_update();
            let active = self
                .inner
                .state
                .read()
                .await
                .get(key)
                .is_some_and(|r| r.lifecycle.is_active());
            if !active {
                return true;
            }
            if tokio::time::timeout_at(deadline, gen_rx.changed())
                .await
                .is_err()
            {
                return false;
            }
        }
    }

    pub async fn service_start(
        &self,
        req: ServiceStartRequest,
    ) -> Result<(ServiceStartOutcome, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Service(req.name.clone());
        let (pane_id, pane_title, has_ready_pattern) = {
            let _guard = self.inner.provision_lock.lock().await;
            {
                let mut store = self.inner.state.write().await;
                if let Some(existing) = store.get_service(&req.name)
                    && existing.lifecycle.is_active()
                {
                    let matched_line = match &existing.lifecycle {
                        EntityLifecycle::Service(ServiceLifecycle::Ready {
                            matched_line, ..
                        }) => matched_line.clone(),
                        EntityLifecycle::Service(
                            ServiceLifecycle::Starting { .. }
                            | ServiceLifecycle::Failed { .. }
                            | ServiceLifecycle::Stopped { .. }
                            | ServiceLifecycle::StoppedByUser { .. },
                        )
                        | EntityLifecycle::Task(_)
                        | EntityLifecycle::Repl(_) => None,
                    };
                    let outcome = ServiceStartOutcome {
                        name: req.name,
                        pane_id: existing.pane_id,
                        pane_title: existing.pane_title.clone(),
                        reused: true,
                        status: existing.lifecycle.status_label(),
                        matched_line,
                        tail_preview: existing.logs.tail_preview(10),
                    };
                    let alerts = store.drain_alerts();
                    return Ok((outcome, alerts));
                }
            }

            let now = SystemTime::now();
            let initial_badge = if req.ready_pattern.is_some() {
                "[RUN]"
            } else {
                "[RDY]"
            };
            let pane_title = PaneTitleCodec::format(initial_badge, &key, None);
            let cwd = req
                .cwd
                .unwrap_or_else(|| self.inner.config.default_cwd.clone());

            let (tab_id, pane_id, placement) = self
                .resolve_target_tab_and_spawn(
                    EntityKind::Service,
                    req.placement,
                    pane_title.clone(),
                    cwd.clone(),
                    req.cmd.clone(),
                    Vec::new(),
                )
                .await?;

            let has_ready_pattern = req.ready_pattern.is_some();
            let lifecycle = if let Some(pat) = req.ready_pattern.clone() {
                EntityLifecycle::Service(ServiceLifecycle::Starting {
                    started_at: now,
                    ready_pattern: Some(pat),
                })
            } else {
                EntityLifecycle::Service(ServiceLifecycle::Ready {
                    started_at: now,
                    ready_at: now,
                    matched_line: None,
                })
            };

            let early_effects = {
                let mut store = self.inner.state.write().await;
                store.register(ManagedRecord {
                    key: key.clone(),
                    tab_id,
                    pane_id,
                    pane_title: pane_title.clone(),
                    label: None,
                    placement,
                    cmd: req.cmd,
                    cwd,
                    env: Vec::new(),
                    ready_pattern: req.ready_pattern,
                    prompt_pattern: None,
                    lifecycle,
                    logs: BoundedLogBuffer::default(),
                    last_viewport: Vec::new(),
                })
            };
            self.apply_effects(early_effects).await;
            self.sync_subscriptions().await;
            self.persist_cache_best_effort().await;
            (pane_id, pane_title, has_ready_pattern)
        };

        if has_ready_pattern {
            let deadline = tokio::time::Instant::now() + req.ready_timeout;
            let mut gen_rx = self.subscribe_state_gen();
            loop {
                let _ = *gen_rx.borrow_and_update();
                let settled = {
                    let store = self.inner.state.read().await;
                    store.get(&key).is_some_and(|r| {
                        !matches!(
                            r.lifecycle,
                            EntityLifecycle::Service(ServiceLifecycle::Starting { .. })
                        )
                    })
                };
                if settled {
                    break;
                }
                if tokio::time::timeout_at(deadline, gen_rx.changed())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        let mut store = self.inner.state.write().await;
        let outcome = if let Some(rec) = store.get(&key) {
            let matched_line = match &rec.lifecycle {
                EntityLifecycle::Service(ServiceLifecycle::Ready { matched_line, .. }) => {
                    matched_line.clone()
                }
                EntityLifecycle::Service(
                    ServiceLifecycle::Starting { .. }
                    | ServiceLifecycle::Failed { .. }
                    | ServiceLifecycle::Stopped { .. }
                    | ServiceLifecycle::StoppedByUser { .. },
                )
                | EntityLifecycle::Task(_)
                | EntityLifecycle::Repl(_) => None,
            };
            ServiceStartOutcome {
                name: req.name,
                pane_id: rec.pane_id,
                pane_title: rec.pane_title.clone(),
                reused: false,
                status: rec.lifecycle.status_label(),
                matched_line,
                tail_preview: rec.logs.tail_preview(10),
            }
        } else {
            ServiceStartOutcome {
                name: req.name,
                pane_id,
                pane_title,
                reused: false,
                status: "starting",
                matched_line: None,
                tail_preview: String::new(),
            }
        };
        let alerts = store.drain_alerts();
        Ok((outcome, alerts))
    }

    pub async fn service_logs(
        &self,
        name: &ServiceName,
        tail_lines: usize,
        since_seq: Option<LogSeq>,
        grep: Option<&CompiledPattern>,
    ) -> Result<(LogSlice, Vec<WorkspaceAlert>)> {
        let mut store = self.inner.state.write().await;
        let rec = store
            .get_service(name)
            .ok_or_else(|| DomainError::ServiceNotFound(name.clone()))?;
        let slice = rec.logs.query(since_seq, tail_lines, grep);
        let alerts = store.drain_alerts();
        Ok((slice, alerts))
    }

    pub async fn service_restart(
        &self,
        name: &ServiceName,
        ready_timeout: Duration,
    ) -> Result<(ServiceStartOutcome, Vec<WorkspaceAlert>)> {
        let prev = {
            let store = self.inner.state.read().await;
            store
                .get_service(name)
                .cloned()
                .ok_or_else(|| DomainError::ServiceNotFound(name.clone()))?
        };
        let _ = self.service_stop(name, PaneClosePolicy::ClosePane).await?;

        self.service_start(ServiceStartRequest {
            name: name.clone(),
            cmd: prev.cmd,
            cwd: Some(prev.cwd),
            ready_pattern: prev.ready_pattern,
            ready_timeout,
            placement: PlacementRequest::from(prev.placement),
        })
        .await
    }

    pub async fn service_stop(
        &self,
        name: &ServiceName,
        close_policy: PaneClosePolicy,
    ) -> Result<(String, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Service(name.clone());
        let record = {
            let store = self.inner.state.read().await;
            store
                .get_service(name)
                .cloned()
                .ok_or_else(|| DomainError::ServiceNotFound(name.clone()))?
        };

        self.maybe_confirm_destructive(&format!("Stop service {name}?"))
            .await?;

        if record.lifecycle.is_active() {
            let ctrl_c = vec![KeyChord::parse("Ctrl c")?];
            let lock = self.pane_write_lock(record.pane_id).await;
            {
                let _guard = lock.lock().await;
                let _ = self.inner.client.send_keys(record.pane_id, &ctrl_c).await;
            }
            // Wait briefly for the process to exit and release its port before closing the pane.
            if self.inner.client.binary_path().is_some() {
                let _ = self
                    .wait_until_inactive(&key, SERVICE_STOP_GRACE_PERIOD)
                    .await;
            }
        }

        let mut actions = Vec::new();
        {
            let mut store = self.inner.state.write().await;
            if let Some(rec) = store.get_mut(&key) {
                let started_at = rec.lifecycle.started_at();
                rec.lifecycle = EntityLifecycle::Service(ServiceLifecycle::Stopped {
                    started_at,
                    ended_at: SystemTime::now(),
                });
                let title = PaneTitleCodec::format("[STOP]", &key, None);
                rec.pane_title = title.clone();
                actions.push(VisualAction::RenamePane {
                    pane_id: rec.pane_id,
                    title,
                });
            }
        }
        if close_policy == PaneClosePolicy::ClosePane {
            actions.push(VisualAction::ClosePane {
                pane_id: record.pane_id,
            });
            let _ = self
                .inner
                .pane_write_locks
                .lock()
                .await
                .remove(&record.pane_id);
        }
        for action in &actions {
            let _ = self.inner.client.apply_visual_action(action).await;
        }
        self.sync_subscriptions().await;
        self.bump_state_gen();
        let alerts = self.inner.state.write().await.drain_alerts();
        Ok(("stopped".to_owned(), alerts))
    }

    pub async fn repl_open(
        &self,
        req: ReplOpenRequest,
    ) -> Result<(ReplExecOutcome, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Repl(req.name.clone());
        let (pane_id, now) = {
            let _guard = self.inner.provision_lock.lock().await;
            {
                let mut store = self.inner.state.write().await;
                if let Some(existing) = store.get_repl(&req.name)
                    && existing.lifecycle.is_active()
                {
                    let outcome = ReplExecOutcome {
                        name: req.name,
                        pane_id: existing.pane_id,
                        pane_title: existing.pane_title.clone(),
                        status: existing.lifecycle.status_label(),
                        delta_output: existing.logs.tail_preview(30),
                        timed_out: false,
                    };
                    let alerts = store.drain_alerts();
                    return Ok((outcome, alerts));
                }
            }

            let now = SystemTime::now();
            let pane_title = PaneTitleCodec::format("[RUN]", &key, None);
            let cwd = req
                .cwd
                .unwrap_or_else(|| self.inner.config.default_cwd.clone());

            let (tab_id, pane_id, placement) = self
                .resolve_target_tab_and_spawn(
                    EntityKind::Repl,
                    req.placement,
                    pane_title.clone(),
                    cwd.clone(),
                    req.cmd.clone(),
                    Vec::new(),
                )
                .await?;

            let early_effects = {
                let mut store = self.inner.state.write().await;
                store.register(ManagedRecord {
                    key: key.clone(),
                    tab_id,
                    pane_id,
                    pane_title,
                    label: None,
                    placement,
                    cmd: req.cmd,
                    cwd,
                    env: Vec::new(),
                    ready_pattern: None,
                    prompt_pattern: req.prompt_pattern.clone(),
                    lifecycle: EntityLifecycle::Repl(ReplLifecycle::Starting { started_at: now }),
                    logs: BoundedLogBuffer::default(),
                    last_viewport: Vec::new(),
                })
            };
            self.apply_effects(early_effects).await;
            self.sync_subscriptions().await;
            self.persist_cache_best_effort().await;
            (pane_id, now)
        };

        let (delta_output, timed_out) = self
            .await_repl_output(
                &key,
                LogSeq::new(1),
                req.prompt_pattern.as_ref(),
                DEFAULT_REPL_EXEC_TIMEOUT,
            )
            .await;

        let (status, pane_title, rename_action) = {
            let mut store = self.inner.state.write().await;
            if let Some(rec) = store.get_mut(&key) {
                if rec.lifecycle.is_active() {
                    let ready_title = PaneTitleCodec::format("[RDY]", &key, None);
                    rec.lifecycle = EntityLifecycle::Repl(ReplLifecycle::Idle { started_at: now });
                    rec.pane_title = ready_title.clone();
                    (
                        rec.lifecycle.status_label(),
                        ready_title.clone(),
                        Some(VisualAction::RenamePane {
                            pane_id,
                            title: ready_title,
                        }),
                    )
                } else {
                    (rec.lifecycle.status_label(), rec.pane_title.clone(), None)
                }
            } else {
                (
                    "stopped",
                    PaneTitleCodec::format("[STOP]", &key, None),
                    None,
                )
            }
        };

        if let Some(action) = rename_action {
            let _ = self.inner.client.apply_visual_action(&action).await;
        }

        let alerts = self.inner.state.write().await.drain_alerts();
        Ok((
            ReplExecOutcome {
                name: req.name,
                pane_id,
                pane_title,
                status,
                delta_output,
                timed_out,
            },
            alerts,
        ))
    }

    pub async fn repl_exec(
        &self,
        req: ReplExecRequest,
    ) -> Result<(ReplExecOutcome, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Repl(req.name.clone());
        let (pane_id, tab_id, placement, default_prompt) = {
            let store = self.inner.state.read().await;
            let rec = store
                .get_repl(&req.name)
                .ok_or_else(|| DomainError::ReplNotFound(req.name.clone()))?;
            match &rec.lifecycle {
                EntityLifecycle::Repl(ReplLifecycle::StoppedByUser { .. }) => {
                    return Err(DomainError::StoppedByUser(key).into());
                }
                EntityLifecycle::Repl(ReplLifecycle::Exited { .. }) => {
                    return Err(DomainError::NotRunning(key).into());
                }
                EntityLifecycle::Repl(ReplLifecycle::InputRequired { .. }) => {
                    return Err(DomainError::InputRequired(req.name.clone()).into());
                }
                EntityLifecycle::Repl(
                    ReplLifecycle::Starting { .. }
                    | ReplLifecycle::Idle { .. }
                    | ReplLifecycle::Busy { .. },
                )
                | EntityLifecycle::Task(_)
                | EntityLifecycle::Service(_) => {}
            }
            (
                rec.pane_id,
                rec.tab_id,
                rec.placement,
                rec.prompt_pattern.clone(),
            )
        };

        let lock = self.pane_write_lock(pane_id).await;
        let _guard = lock.lock().await;

        let start_seq = {
            let mut store = self.inner.state.write().await;
            let rec = store
                .get_mut(&key)
                .ok_or_else(|| DomainError::ReplNotFound(req.name.clone()))?;
            let started_at = rec.lifecycle.started_at();
            rec.lifecycle = EntityLifecycle::Repl(ReplLifecycle::Busy {
                started_at,
                command_started_at: SystemTime::now(),
            });
            rec.logs.next_seq()
        };

        match &req.input {
            ReplInput::Paste { text, enter } => {
                self.inner.client.paste(pane_id, text).await?;
                if *enter == EnterPolicy::SendEnter {
                    let enter_key = vec![KeyChord::parse("Enter")?];
                    self.inner.client.send_keys(pane_id, &enter_key).await?;
                }
            }
            ReplInput::SendKeys(keys) => {
                self.inner.client.send_keys(pane_id, keys).await?;
            }
        }

        let effective_prompt = req.wait_for_prompt.as_ref().or(default_prompt.as_ref());
        let (delta_output, timed_out) = self
            .await_repl_output(&key, start_seq, effective_prompt, req.timeout)
            .await;

        let needs_human_input = timed_out && interactive_auth_regex().is_match(&delta_output);
        let (status, pane_title) = {
            let mut store = self.inner.state.write().await;
            if let Some(rec) = store.get_mut(&key) {
                let started_at = rec.lifecycle.started_at();
                if needs_human_input {
                    let already_marked = matches!(
                        rec.lifecycle,
                        EntityLifecycle::Repl(ReplLifecycle::InputRequired {
                            attention_marked: true,
                            ..
                        })
                    );
                    rec.lifecycle = EntityLifecycle::Repl(ReplLifecycle::InputRequired {
                        started_at,
                        prompt_excerpt: delta_output.clone(),
                        attention_marked: true,
                    });
                    let title = PaneTitleCodec::format("[WAIT]", &key, None);
                    rec.pane_title = title.clone();
                    drop(store);
                    let _ = self
                        .inner
                        .client
                        .apply_visual_action(&VisualAction::RenamePane {
                            pane_id,
                            title: title.clone(),
                        })
                        .await;
                    if !already_marked {
                        let _ = self
                            .inner
                            .client
                            .apply_visual_action(&VisualAction::MarkAttention { pane_id })
                            .await;
                    }
                    if placement == ResolvedPlacement::Floating {
                        let _ = self
                            .inner
                            .client
                            .apply_visual_action(&VisualAction::ShowFloatingPanes { tab_id })
                            .await;
                    }
                    ("input_required", title)
                } else {
                    if rec.lifecycle.is_active() {
                        rec.lifecycle = EntityLifecycle::Repl(ReplLifecycle::Idle { started_at });
                    }
                    (rec.lifecycle.status_label(), rec.pane_title.clone())
                }
            } else {
                ("stopped", PaneTitleCodec::format("[STOP]", &key, None))
            }
        };

        let alerts = self.inner.state.write().await.drain_alerts();
        Ok((
            ReplExecOutcome {
                name: req.name,
                pane_id,
                pane_title,
                status,
                delta_output,
                timed_out,
            },
            alerts,
        ))
    }

    async fn await_repl_output(
        &self,
        key: &ManagedKey,
        since_seq: LogSeq,
        prompt_pattern: Option<&CompiledPattern>,
        timeout: Duration,
    ) -> (String, bool) {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut gen_rx = self.subscribe_state_gen();
        let mut last_next_seq = since_seq;
        let mut last_change = tokio::time::Instant::now();
        let mut saw_output = false;
        let probe_interval = Duration::from_millis(250);

        loop {
            let _ = *gen_rx.borrow_and_update();
            let (slice, is_active, pane_id) = {
                let store = self.inner.state.read().await;
                match store.get(key) {
                    Some(rec) => (
                        rec.logs.query(Some(since_seq), 200, None),
                        rec.lifecycle.is_active(),
                        rec.pane_id,
                    ),
                    None => return (String::new(), false),
                }
            };

            if slice.next_seq > last_next_seq {
                last_next_seq = slice.next_seq;
                last_change = tokio::time::Instant::now();
                saw_output = true;
            }

            let joined = slice
                .entries
                .iter()
                .map(|e| e.line.as_str())
                .collect::<Vec<_>>()
                .join("\n");

            if !is_active {
                return (joined, false);
            }

            // Match `prompt_pattern` only against the last non-empty line so an echoed command
            // line (e.g. `>>> 2 + 2`) does not prematurely satisfy the prompt check.
            if let Some(pat) = prompt_pattern
                && let Some(last_line) = slice
                    .entries
                    .iter()
                    .rev()
                    .find(|e| !e.line.trim().is_empty())
                && pat.is_match(&last_line.line)
            {
                return (joined, false);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return (joined, true);
            }

            let quiet_deadline = if prompt_pattern.is_none() && saw_output {
                (last_change + DEFAULT_REPL_QUIET_DEBOUNCE).min(deadline)
            } else {
                deadline
            };
            let wait_until = if self.inner.client.binary_path().is_some() {
                (now + probe_interval).min(quiet_deadline)
            } else {
                quiet_deadline
            };

            if tokio::time::timeout_at(wait_until, gen_rx.changed())
                .await
                .is_err()
            {
                let after = tokio::time::Instant::now();
                if after >= deadline {
                    return (joined, true);
                }
                if prompt_pattern.is_none() && saw_output && after >= quiet_deadline {
                    return (joined, false);
                }
                // Zellij 0.46.1 headless background sessions do not always emit incremental
                // `subscribe` ticks for floating panes when no client is attached; refresh the
                // visible viewport grid via `dump-screen` (without `--full`) and feed it through
                // the standard viewport differ.
                if let Ok(raw) = self.inner.client.dump_viewport(pane_id).await {
                    let lines: Vec<String> = raw.lines().map(ToOwned::to_owned).collect();
                    self.inject_viewport_frame(SubscribeFrame {
                        pane_id,
                        viewport_lines: lines,
                    })
                    .await;
                }
            }
        }
    }

    pub async fn repl_close(&self, name: &ReplName) -> Result<(String, Vec<WorkspaceAlert>)> {
        let key = ManagedKey::Repl(name.clone());
        let record = {
            let store = self.inner.state.read().await;
            store
                .get_repl(name)
                .cloned()
                .ok_or_else(|| DomainError::ReplNotFound(name.clone()))?
        };

        self.maybe_confirm_destructive(&format!("Close REPL {name}?"))
            .await?;

        let lock = self.pane_write_lock(record.pane_id).await;
        {
            let _guard = lock.lock().await;
            let ctrl_d = vec![KeyChord::parse("Ctrl d")?];
            let _ = self.inner.client.send_keys(record.pane_id, &ctrl_d).await;
            let _ = self
                .inner
                .client
                .apply_visual_action(&VisualAction::ClosePane {
                    pane_id: record.pane_id,
                })
                .await;
        }
        let _ = self
            .inner
            .pane_write_locks
            .lock()
            .await
            .remove(&record.pane_id);

        {
            let mut store = self.inner.state.write().await;
            if let Some(rec) = store.get_mut(&key) {
                let started_at = rec.lifecycle.started_at();
                rec.lifecycle = EntityLifecycle::Repl(ReplLifecycle::Exited {
                    started_at,
                    ended_at: SystemTime::now(),
                    exit_code: Some(0),
                });
            }
        }
        self.sync_subscriptions().await;
        self.bump_state_gen();
        let alerts = self.inner.state.write().await.drain_alerts();
        Ok(("closed".to_owned(), alerts))
    }

    /// Acquires (or lazily creates) the per-pane write serialization mutex.
    pub async fn pane_write_lock(&self, pane_id: TerminalPaneId) -> Arc<Mutex<()>> {
        let mut locks = self.inner.pane_write_locks.lock().await;
        locks
            .entry(pane_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Looks up a managed record snapshot by key.
    pub async fn get_record(&self, key: &ManagedKey) -> Option<ManagedRecord> {
        self.inner.state.read().await.get(key).cloned()
    }
}
