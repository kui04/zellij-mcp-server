//! Mockable Zellij CLI execution backend (`M-MOCKABLE-SYSCALLS`).

use std::path::PathBuf;
use std::process::Output;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::process::Command;
use tracing::debug;

use crate::domain::error::DomainError;
use crate::domain::ids::{CommandSpec, KeyChord, SessionName, TabId, TerminalPaneId};
use crate::domain::layout::ResolvedPlacement;
use crate::domain::lifecycle::VisualAction;
use crate::logs::strip_ansi;
use crate::state::{ObservedPane, ObservedTab};
use crate::zellij::protocol::{
    MIN_ZELLIJ_VERSION, is_active_session_in_list, is_session_not_found_stderr,
    parse_current_tab_info_json, parse_list_panes_json, parse_list_tabs_json,
    parse_new_pane_output, parse_new_tab_output, parse_zellij_version,
};

/// Whether `ensure_session` created a new Zellij session or attached to an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAttachment {
    /// A brand-new session was created by this server instance.
    Created,
    /// An already-running session was reused (inherited or pre-existing).
    Reused,
}

/// Parameters for opening a new terminal pane via `zellij action new-pane`.
#[derive(Debug, Clone)]
pub struct NewPaneSpec {
    pub tab_id: Option<TabId>,
    pub name: String,
    pub placement: ResolvedPlacement,
    pub stacked: bool,
    pub cwd: Option<PathBuf>,
    pub cmd: CommandSpec,
}

/// Dispatches Zellij CLI operations either to a real `zellij` binary or to an in-memory mock.
#[derive(Debug, Clone)]
pub struct ZellijClient {
    backend: ZellijBackend,
}

#[derive(Debug, Clone)]
enum ZellijBackend {
    Process {
        binary: PathBuf,
        session: SessionName,
    },
    #[cfg(feature = "test-util")]
    Mock(mock::MockZellijCtrl),
}

impl ZellijClient {
    /// Creates a production client backed by the given `zellij` executable and target session.
    pub fn new(binary: PathBuf, session: SessionName) -> Self {
        Self {
            backend: ZellijBackend::Process { binary, session },
        }
    }

    /// Creates a mocked client paired with its test controller (`M-MOCKABLE-SYSCALLS`).
    #[cfg(feature = "test-util")]
    pub fn new_mocked(session: SessionName) -> (Self, mock::MockZellijCtrl) {
        let ctrl = mock::MockZellijCtrl::new(session);
        let client = Self {
            backend: ZellijBackend::Mock(ctrl.clone()),
        };
        (client, ctrl)
    }

    pub fn session(&self) -> &SessionName {
        match &self.backend {
            ZellijBackend::Process { session, .. } => session,
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.session(),
        }
    }

    pub fn binary_path(&self) -> Option<&PathBuf> {
        match &self.backend {
            ZellijBackend::Process { binary, .. } => Some(binary),
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(_) => None,
        }
    }

    async fn run_raw(&self, args: &[String]) -> Result<Output> {
        let binary = match &self.backend {
            ZellijBackend::Process { binary, .. } => binary,
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(_) => bail!("run_raw called on mock backend"),
        };
        debug!(binary = %binary.display(), ?args, "executing zellij cli");
        Command::new(binary)
            .args(args)
            .output()
            .await
            .with_context(|| format!("failed to spawn {} {:?}", binary.display(), args))
    }

    async fn run_session(&self, sub_args: &[String]) -> Result<Output> {
        let mut args = Vec::with_capacity(2 + sub_args.len());
        args.push("--session".to_owned());
        args.push(self.session().as_str().to_owned());
        args.extend(sub_args.iter().cloned());
        let output = self.run_raw(&args).await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if is_session_not_found_stderr(&stderr) {
            return Err(DomainError::SessionNotFound(self.session().clone()).into());
        }
        Ok(output)
    }

    async fn run_session_checked(&self, sub_args: &[String]) -> Result<String> {
        let output = self.run_session(sub_args).await?;
        if !output.status.success() {
            let stderr = strip_ansi(&String::from_utf8_lossy(&output.stderr));
            let stdout = strip_ansi(&String::from_utf8_lossy(&output.stdout));
            bail!(
                "zellij {:?} exited with {}: stderr={}, stdout={}",
                sub_args,
                output.status,
                stderr.trim(),
                stdout.trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Verifies that the configured Zellij binary meets `MIN_ZELLIJ_VERSION`.
    pub async fn check_version(&self) -> Result<(u32, u32, u32)> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let output = self.run_raw(&["--version".to_owned()]).await?;
                if !output.status.success() {
                    bail!(
                        "zellij --version failed: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }
                let stdout = String::from_utf8_lossy(&output.stdout);
                let version = parse_zellij_version(&stdout)?;
                if version < MIN_ZELLIJ_VERSION {
                    bail!(
                        "zellij {}.{}.{} is too old; >= {}.{}.{} is required for `zellij events`",
                        version.0,
                        version.1,
                        version.2,
                        MIN_ZELLIJ_VERSION.0,
                        MIN_ZELLIJ_VERSION.1,
                        MIN_ZELLIJ_VERSION.2
                    );
                }
                Ok(version)
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.record("check_version");
                Ok(MIN_ZELLIJ_VERSION)
            }
        }
    }

    /// Returns `true` when the target session is currently listed as active in `zellij list-sessions`.
    pub async fn session_exists(&self) -> Result<bool> {
        match &self.backend {
            ZellijBackend::Process { session, .. } => {
                let output = self
                    .run_raw(&["list-sessions".to_owned(), "--no-formatting".to_owned()])
                    .await?;
                if !output.status.success() {
                    return Ok(false);
                }
                let stdout = String::from_utf8_lossy(&output.stdout);
                Ok(is_active_session_in_list(&stdout, session.as_str()))
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => Ok(ctrl.is_session_alive()),
        }
    }

    /// Ensures the target session exists, reusing it if already active or creating it via
    /// `zellij attach --create-background <session>`.
    pub async fn ensure_session(&self) -> Result<SessionAttachment> {
        match &self.backend {
            ZellijBackend::Process { session, .. } => {
                if self.session_exists().await? {
                    return Ok(SessionAttachment::Reused);
                }

                let args = vec![
                    "attach".to_owned(),
                    "--create-background".to_owned(),
                    session.as_str().to_owned(),
                ];
                let output = self.run_raw(&args).await?;
                if !output.status.success() {
                    let stderr = strip_ansi(&String::from_utf8_lossy(&output.stderr));
                    if stderr.contains("Session already exists") {
                        return Ok(SessionAttachment::Reused);
                    }
                    bail!(
                        "zellij attach --create-background {} failed ({}): {}",
                        session,
                        output.status,
                        stderr.trim()
                    );
                }
                // `attach --create-background` returns before the new session finishes initializing
                // its initial tab; wait briefly until `list-tabs --all --json` responds cleanly.
                for _ in 0..25 {
                    if let Ok(tabs) = self.list_tabs().await
                        && !tabs.is_empty()
                    {
                        return Ok(SessionAttachment::Created);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(SessionAttachment::Created)
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.record("ensure_session");
                Ok(ctrl.mock_ensure_session())
            }
        }
    }

    /// Kills and deletes the target Zellij session (used on shutdown for server-created sessions).
    pub async fn kill_session(&self) -> Result<()> {
        match &self.backend {
            ZellijBackend::Process { session, .. } => {
                let _ = self
                    .run_raw(&["kill-session".to_owned(), session.as_str().to_owned()])
                    .await?;
                let _ = self
                    .run_raw(&[
                        "delete-session".to_owned(),
                        "--force".to_owned(),
                        session.as_str().to_owned(),
                    ])
                    .await;
                Ok(())
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.record("kill_session");
                ctrl.kill_session_externally();
                Ok(())
            }
        }
    }

    /// Lazily creates a new tab without stealing focus (`--no-focus`).
    pub async fn new_tab(&self, name: &str) -> Result<TabId> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let args = vec![
                    "action".to_owned(),
                    "new-tab".to_owned(),
                    "--name".to_owned(),
                    name.to_owned(),
                    "--no-focus".to_owned(),
                ];
                let stdout = self.run_session_checked(&args).await?;
                parse_new_tab_output(&stdout)
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_new_tab(name),
        }
    }

    /// Opens a new terminal pane without stealing focus (`--no-focus`).
    pub async fn new_pane(&self, spec: &NewPaneSpec) -> Result<TerminalPaneId> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let mut args = vec![
                    "action".to_owned(),
                    "new-pane".to_owned(),
                    "--no-focus".to_owned(),
                    "--name".to_owned(),
                    spec.name.clone(),
                ];
                if let Some(tab_id) = spec.tab_id {
                    args.push("--tab-id".to_owned());
                    args.push(tab_id.to_string());
                }
                if let Some(cwd) = &spec.cwd {
                    args.push("--cwd".to_owned());
                    args.push(cwd.to_string_lossy().into_owned());
                }
                match spec.placement {
                    ResolvedPlacement::SplitRight => {
                        args.push("--direction".to_owned());
                        args.push("right".to_owned());
                    }
                    ResolvedPlacement::SplitDown => {
                        args.push("--direction".to_owned());
                        args.push("down".to_owned());
                    }
                    ResolvedPlacement::Floating => {
                        args.push("--floating".to_owned());
                        args.push("--x".to_owned());
                        args.push("12%".to_owned());
                        args.push("--y".to_owned());
                        args.push("12%".to_owned());
                        args.push("--width".to_owned());
                        args.push("75%".to_owned());
                        args.push("--height".to_owned());
                        args.push("75%".to_owned());
                    }
                    ResolvedPlacement::Stacked => {
                        args.push("--stacked".to_owned());
                    }
                    ResolvedPlacement::BackgroundTab => {
                        if spec.stacked {
                            args.push("--stacked".to_owned());
                        }
                    }
                }
                args.push("--".to_owned());
                args.extend(spec.cmd.to_zellij_argv());

                let stdout = self.run_session_checked(&args).await?;
                parse_new_pane_output(&stdout)
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_new_pane(spec),
        }
    }

    /// Switches a tab to the `stacked` swap layout; treats exit code 1 as non-fatal.
    pub async fn apply_stacked_layout(&self, tab_id: TabId) -> Result<bool> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let args = vec![
                    "action".to_owned(),
                    "apply-tiled-swap-layout".to_owned(),
                    "stacked".to_owned(),
                    "--tab-id".to_owned(),
                    tab_id.to_string(),
                ];
                let output = self.run_session(&args).await?;
                match output.status.code() {
                    Some(0) => Ok(true),
                    Some(1) => {
                        debug!(%tab_id, "stacked swap layout not available for current pane count");
                        Ok(false)
                    }
                    _ => bail!(
                        "apply-tiled-swap-layout failed ({}): {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    ),
                }
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.ensure_alive()?;
                ctrl.record(&format!("apply_stacked_layout:{tab_id}"));
                Ok(true)
            }
        }
    }

    /// Injects text into a pane via bracketed paste (`zellij action paste`).
    pub async fn paste(&self, pane_id: TerminalPaneId, text: &str) -> Result<()> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let args = vec![
                    "action".to_owned(),
                    "paste".to_owned(),
                    "--pane-id".to_owned(),
                    pane_id.cli_arg(),
                    "--".to_owned(),
                    text.to_owned(),
                ];
                let _ = self.run_session_checked(&args).await?;
                Ok(())
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.ensure_alive()?;
                ctrl.record(&format!("paste:{pane_id}:{text}"));
                Ok(())
            }
        }
    }

    /// Sends one or more key chords (`zellij action send-keys`).
    pub async fn send_keys(&self, pane_id: TerminalPaneId, keys: &[KeyChord]) -> Result<()> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let mut args = vec![
                    "action".to_owned(),
                    "send-keys".to_owned(),
                    "--pane-id".to_owned(),
                    pane_id.cli_arg(),
                    "--".to_owned(),
                ];
                args.extend(keys.iter().map(|k| k.as_str().to_owned()));
                let _ = self.run_session_checked(&args).await?;
                Ok(())
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.ensure_alive()?;
                let joined = keys
                    .iter()
                    .map(KeyChord::as_str)
                    .collect::<Vec<_>>()
                    .join(",");
                ctrl.record(&format!("send_keys:{pane_id}:{joined}"));
                Ok(())
            }
        }
    }

    /// Dumps only the visible viewport of a pane (`zellij action dump-screen --pane-id <id>`).
    pub async fn dump_viewport(&self, pane_id: TerminalPaneId) -> Result<String> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let args = vec![
                    "action".to_owned(),
                    "dump-screen".to_owned(),
                    "--pane-id".to_owned(),
                    pane_id.cli_arg(),
                ];
                self.run_session_checked(&args).await
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_dump_screen(pane_id),
        }
    }

    /// Dumps full scrollback of a pane (`zellij action dump-screen --full --pane-id <id>`).
    pub async fn dump_screen(
        &self,
        pane_id: TerminalPaneId,
        preserve_ansi: bool,
    ) -> Result<String> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let mut args = vec![
                    "action".to_owned(),
                    "dump-screen".to_owned(),
                    "--full".to_owned(),
                    "--pane-id".to_owned(),
                    pane_id.cli_arg(),
                ];
                if preserve_ansi {
                    args.push("--ansi".to_owned());
                }
                self.run_session_checked(&args).await
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_dump_screen(pane_id),
        }
    }

    pub async fn list_tabs(&self) -> Result<Vec<ObservedTab>> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let stdout = self
                    .run_session_checked(&[
                        "action".to_owned(),
                        "list-tabs".to_owned(),
                        "--all".to_owned(),
                        "--json".to_owned(),
                    ])
                    .await?;
                parse_list_tabs_json(&stdout)
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_list_tabs(),
        }
    }

    /// Queries the currently active tab. Returns `Ok(None)` in headless background sessions where
    /// no client is attached (Zellij exits with code 2).
    pub async fn current_tab_info(&self) -> Result<Option<ObservedTab>> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let output = self
                    .run_session(&[
                        "action".to_owned(),
                        "current-tab-info".to_owned(),
                        "--json".to_owned(),
                    ])
                    .await?;
                match output.status.code() {
                    Some(0) => {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        Ok(Some(parse_current_tab_info_json(&stdout)?))
                    }
                    Some(2) => Ok(None),
                    _ => Err(anyhow!(
                        "current-tab-info failed ({}): {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    )),
                }
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_current_tab_info(),
        }
    }

    pub async fn list_panes(&self) -> Result<Vec<ObservedPane>> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let stdout = self
                    .run_session_checked(&[
                        "action".to_owned(),
                        "list-panes".to_owned(),
                        "--all".to_owned(),
                        "--json".to_owned(),
                    ])
                    .await?;
                parse_list_panes_json(&stdout)
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => ctrl.mock_list_panes(),
        }
    }

    /// Executes a single visual feedback side effect derived from a state transition.
    pub async fn apply_visual_action(&self, action: &VisualAction) -> Result<()> {
        match &self.backend {
            ZellijBackend::Process { .. } => match action {
                VisualAction::RenamePane { pane_id, title } => {
                    let _ = self
                        .run_session_checked(&[
                            "action".to_owned(),
                            "rename-pane".to_owned(),
                            "--pane-id".to_owned(),
                            pane_id.cli_arg(),
                            "--".to_owned(),
                            title.clone(),
                        ])
                        .await?;
                    Ok(())
                }
                VisualAction::HighlightError {
                    pane_id,
                    bg_hex,
                    border_style,
                } => {
                    let _ = self
                        .run_session_checked(&[
                            "action".to_owned(),
                            "set-pane-color".to_owned(),
                            "--pane-id".to_owned(),
                            pane_id.cli_arg(),
                            "--bg".to_owned(),
                            (*bg_hex).to_owned(),
                        ])
                        .await?;
                    let _ = self
                        .run_session_checked(&[
                            "action".to_owned(),
                            "set-pane-border-style".to_owned(),
                            "--pane-id".to_owned(),
                            pane_id.cli_arg(),
                            "--border-style".to_owned(),
                            (*border_style).to_owned(),
                        ])
                        .await?;
                    Ok(())
                }
                VisualAction::ResetPaneColor { pane_id } => {
                    let _ = self
                        .run_session_checked(&[
                            "action".to_owned(),
                            "set-pane-color".to_owned(),
                            "--pane-id".to_owned(),
                            pane_id.cli_arg(),
                            "--reset".to_owned(),
                        ])
                        .await?;
                    Ok(())
                }
                VisualAction::MarkAttention { pane_id } => {
                    let _ = self
                        .run_session_checked(&[
                            "action".to_owned(),
                            "watch-pane".to_owned(),
                            "--pane-id".to_owned(),
                            pane_id.cli_arg(),
                        ])
                        .await?;
                    Ok(())
                }
                VisualAction::ShowFloatingPanes { tab_id } => {
                    let output = self
                        .run_session(&[
                            "action".to_owned(),
                            "show-floating-panes".to_owned(),
                            "--tab-id".to_owned(),
                            tab_id.to_string(),
                        ])
                        .await?;
                    match output.status.code() {
                        Some(0 | 2) => Ok(()),
                        _ => bail!(
                            "show-floating-panes failed ({}): {}",
                            output.status,
                            String::from_utf8_lossy(&output.stderr).trim()
                        ),
                    }
                }
                VisualAction::Notify { message } => {
                    let _ = self
                        .run_session(&[
                            "prompt".to_owned(),
                            "--timeout".to_owned(),
                            "5s".to_owned(),
                            "notify".to_owned(),
                            message.clone(),
                        ])
                        .await?;
                    Ok(())
                }
                VisualAction::ClosePane { pane_id } => {
                    let _ = self
                        .run_session_checked(&[
                            "action".to_owned(),
                            "close-pane".to_owned(),
                            "--pane-id".to_owned(),
                            pane_id.cli_arg(),
                        ])
                        .await?;
                    Ok(())
                }
            },
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.ensure_alive()?;
                ctrl.record(&format!("visual:{action:?}"));
                if let VisualAction::ClosePane { pane_id } = action {
                    ctrl.mock_remove_pane(*pane_id);
                }
                Ok(())
            }
        }
    }

    /// Prompts the human at the terminal to confirm a destructive action (`zellij prompt confirm`).
    pub async fn confirm_destructive(&self, message: &str, timeout: Duration) -> Result<bool> {
        match &self.backend {
            ZellijBackend::Process { .. } => {
                let secs = timeout.as_secs().max(1);
                let output = self
                    .run_session(&[
                        "prompt".to_owned(),
                        "--timeout".to_owned(),
                        format!("{secs}s"),
                        "confirm".to_owned(),
                        message.to_owned(),
                    ])
                    .await?;
                match output.status.code() {
                    Some(0) => Ok(true),
                    Some(1 | 124) => Ok(false),
                    _ => bail!(
                        "zellij prompt confirm failed ({}): {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    ),
                }
            }
            #[cfg(feature = "test-util")]
            ZellijBackend::Mock(ctrl) => {
                ctrl.ensure_alive()?;
                ctrl.record(&format!("confirm:{message}"));
                Ok(ctrl.confirm_answer())
            }
        }
    }
}

#[cfg(feature = "test-util")]
pub mod mock {
    //! In-memory mock controller for deterministic orchestration tests.

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use anyhow::{Result, bail};

    use crate::domain::error::DomainError;
    use crate::domain::ids::{SessionName, TabId, TerminalPaneId};
    use crate::domain::layout::{ResolvedPlacement, ViewportSnapshot};
    use crate::state::{ObservedPane, ObservedTab};
    use crate::zellij::cli::{NewPaneSpec, SessionAttachment};

    /// Cloneable handle to inspect recorded calls and script mock responses.
    #[derive(Debug, Clone)]
    pub struct MockZellijCtrl {
        inner: Arc<MockZellijInner>,
    }

    #[derive(Debug)]
    struct MockZellijInner {
        session: SessionName,
        state: Mutex<MockState>,
    }

    #[derive(Debug)]
    struct MockState {
        next_tab_id: u32,
        next_pane_id: u32,
        tabs: Vec<ObservedTab>,
        panes: Vec<ObservedPane>,
        screen_dumps: HashMap<TerminalPaneId, String>,
        confirm_answer: bool,
        session_alive: bool,
        session_preexisting: bool,
        fail_stacked: bool,
        fail_no_room: bool,
        fail_new_pane: bool,
        recorded_actions: Vec<String>,
    }

    impl MockZellijCtrl {
        pub fn new(session: SessionName) -> Self {
            let initial_tab = ObservedTab {
                tab_id: TabId::new(0),
                name: "Tab #1".to_owned(),
                active: true,
                viewport: ViewportSnapshot::new(200, 50, 1),
            };
            let initial_pane = ObservedPane {
                pane_id: TerminalPaneId::new(0),
                tab_id: TabId::new(0),
                title: "Pane #1".to_owned(),
                is_focused: true,
                is_floating: false,
                columns: 200,
                rows: 50,
                exited: false,
                exit_status: None,
                cwd: None,
                command: None,
            };
            Self {
                inner: Arc::new(MockZellijInner {
                    session,
                    state: Mutex::new(MockState {
                        next_tab_id: 1,
                        next_pane_id: 1,
                        tabs: vec![initial_tab],
                        panes: vec![initial_pane],
                        screen_dumps: HashMap::new(),
                        confirm_answer: true,
                        session_alive: true,
                        session_preexisting: false,
                        fail_stacked: false,
                        fail_no_room: false,
                        fail_new_pane: false,
                        recorded_actions: Vec::new(),
                    }),
                }),
            }
        }

        pub fn session(&self) -> &SessionName {
            &self.inner.session
        }

        fn lock_state(&self) -> std::sync::MutexGuard<'_, MockState> {
            match self.inner.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }

        pub fn record(&self, action: &str) {
            self.lock_state().recorded_actions.push(action.to_owned());
        }

        pub fn recorded_actions(&self) -> Vec<String> {
            self.lock_state().recorded_actions.clone()
        }

        pub fn is_session_alive(&self) -> bool {
            self.lock_state().session_alive
        }

        pub fn kill_session_externally(&self) {
            self.lock_state().session_alive = false;
        }

        pub fn set_session_preexisting(&self, preexisting: bool) {
            self.lock_state().session_preexisting = preexisting;
        }

        pub fn set_fail_stacked(&self, fail: bool) {
            self.lock_state().fail_stacked = fail;
        }

        pub fn set_fail_no_room(&self, fail: bool) {
            self.lock_state().fail_no_room = fail;
        }

        /// Makes every `new-pane` call fail, exercising spawn-failure paths. Limits: applies to
        /// all placements until cleared.
        pub fn set_fail_new_pane(&self, fail: bool) {
            self.lock_state().fail_new_pane = fail;
        }

        pub fn set_focused_pane_geometry(&self, columns: u16, rows: u16) {
            let mut state = self.lock_state();
            if let Some(pane) = state.panes.iter_mut().find(|p| p.is_focused) {
                pane.columns = columns;
                pane.rows = rows;
            }
        }

        pub(crate) fn ensure_alive(&self) -> Result<()> {
            if !self.lock_state().session_alive {
                return Err(DomainError::SessionNotFound(self.inner.session.clone()).into());
            }
            Ok(())
        }

        pub(crate) fn mock_ensure_session(&self) -> SessionAttachment {
            let mut state = self.lock_state();
            state.session_alive = true;
            if state.session_preexisting {
                SessionAttachment::Reused
            } else {
                SessionAttachment::Created
            }
        }

        pub fn set_viewport(&self, columns: u16, rows: u16, tiled_panes: u16) {
            let mut state = self.lock_state();
            if let Some(tab) = state.tabs.first_mut() {
                tab.viewport = ViewportSnapshot::new(columns, rows, tiled_panes);
            }
            if let Some(pane) = state.panes.iter_mut().find(|p| p.is_focused) {
                pane.columns = columns;
                pane.rows = rows;
            }
        }

        pub fn set_screen_dump(&self, pane_id: TerminalPaneId, content: impl Into<String>) {
            let _ = self
                .lock_state()
                .screen_dumps
                .insert(pane_id, content.into());
        }

        pub fn set_confirm_answer(&self, answer: bool) {
            self.lock_state().confirm_answer = answer;
        }

        pub fn confirm_answer(&self) -> bool {
            self.lock_state().confirm_answer
        }

        pub fn seed_pane(&self, pane: ObservedPane) {
            self.lock_state().panes.push(pane);
        }

        pub(crate) fn mock_new_tab(&self, name: &str) -> Result<TabId> {
            self.ensure_alive()?;
            let mut state = self.lock_state();
            let tab_id = TabId::new(state.next_tab_id);
            state.next_tab_id += 1;
            let viewport = state
                .tabs
                .first()
                .map_or(ViewportSnapshot::new(200, 50, 0), |t| t.viewport);
            state.tabs.push(ObservedTab {
                tab_id,
                name: name.to_owned(),
                active: false,
                viewport,
            });
            // Zellij materializes every new tab with one default shell pane that no managed
            // record owns; the orchestrator must close it after spawning its first pane.
            let placeholder_id = TerminalPaneId::new(state.next_pane_id);
            state.next_pane_id += 1;
            state.panes.push(ObservedPane {
                pane_id: placeholder_id,
                tab_id,
                title: "Shell".to_owned(),
                is_focused: false,
                is_floating: false,
                columns: viewport.columns,
                rows: viewport.rows,
                exited: false,
                exit_status: None,
                cwd: None,
                command: None,
            });
            state.recorded_actions.push(format!("new_tab:{name}"));
            Ok(tab_id)
        }

        /// Removes a pane the way `zellij action close-pane` does.
        pub(crate) fn mock_remove_pane(&self, pane_id: TerminalPaneId) {
            let mut state = self.lock_state();
            state.panes.retain(|pane| pane.pane_id != pane_id);
            state.recorded_actions.push(format!("close_pane:{pane_id}"));
        }

        /// Returns the first live pane whose title contains `needle`, mirroring `list-panes`
        /// queries used by tests that need a pane ID before the orchestrator call returns.
        pub fn find_pane_id_by_title(&self, needle: &str) -> Option<TerminalPaneId> {
            self.lock_state()
                .panes
                .iter()
                .find(|pane| pane.title.contains(needle))
                .map(|pane| pane.pane_id)
        }

        /// Whether `pane_id` is still open (mirrors `list-panes --all`).
        pub fn has_pane(&self, pane_id: TerminalPaneId) -> bool {
            self.lock_state()
                .panes
                .iter()
                .any(|pane| pane.pane_id == pane_id)
        }

        /// Returns the live panes of `tab_id` (mirrors `list-panes --all`).
        pub fn panes_in_tab(&self, tab_id: TabId) -> Vec<ObservedPane> {
            self.lock_state()
                .panes
                .iter()
                .filter(|pane| pane.tab_id == tab_id)
                .cloned()
                .collect()
        }

        pub(crate) fn mock_new_pane(&self, spec: &NewPaneSpec) -> Result<TerminalPaneId> {
            self.ensure_alive()?;
            let mut state = self.lock_state();
            if state.fail_new_pane {
                bail!(
                    "zellij [\"action\", \"new-pane\"] exited with exit status: 1: stderr=new-pane failed, stdout="
                );
            }
            if state.fail_stacked && (spec.placement == ResolvedPlacement::Stacked || spec.stacked)
            {
                bail!(
                    "zellij [\"action\", \"new-pane\", \"--stacked\"] exited with exit status: 2: stderr=A connected client or a target pane is required, stdout="
                );
            }
            if state.fail_no_room
                && matches!(
                    spec.placement,
                    ResolvedPlacement::SplitRight | ResolvedPlacement::SplitDown
                )
            {
                bail!(
                    "zellij [\"action\", \"new-pane\", \"--direction\", \"right\"] exited with exit status: 2: stderr=No room for a new pane, stdout="
                );
            }
            let pane_id = TerminalPaneId::new(state.next_pane_id);
            state.next_pane_id += 1;
            let tab_id = spec.tab_id.unwrap_or(TabId::new(0));
            if matches!(
                spec.placement,
                ResolvedPlacement::SplitRight | ResolvedPlacement::SplitDown
            ) {
                if let Some(focused) = state
                    .panes
                    .iter_mut()
                    .find(|p| p.tab_id == tab_id && p.is_focused && !p.is_floating)
                {
                    match spec.placement {
                        ResolvedPlacement::SplitRight => focused.columns /= 2,
                        ResolvedPlacement::SplitDown => focused.rows /= 2,
                        _ => {}
                    }
                }
                if let Some(tab) = state.tabs.iter_mut().find(|t| t.tab_id == tab_id) {
                    tab.viewport = ViewportSnapshot::new(
                        tab.viewport.columns,
                        tab.viewport.rows,
                        tab.viewport.tiled_panes_in_active_tab.saturating_add(1),
                    );
                }
            }
            state.panes.push(ObservedPane {
                pane_id,
                tab_id,
                title: spec.name.clone(),
                is_focused: false,
                is_floating: spec.placement == ResolvedPlacement::Floating,
                columns: 80,
                rows: 24,
                exited: false,
                exit_status: None,
                cwd: spec.cwd.clone(),
                command: Some(spec.cmd.program().to_owned()),
            });
            state
                .recorded_actions
                .push(format!("new_pane:{pane_id}:{}:tab={tab_id}", spec.name));
            Ok(pane_id)
        }

        pub(crate) fn mock_dump_screen(&self, pane_id: TerminalPaneId) -> Result<String> {
            self.ensure_alive()?;
            let mut state = self.lock_state();
            state
                .recorded_actions
                .push(format!("dump_screen:{pane_id}"));
            Ok(state
                .screen_dumps
                .get(&pane_id)
                .cloned()
                .unwrap_or_default())
        }

        pub(crate) fn mock_list_tabs(&self) -> Result<Vec<ObservedTab>> {
            self.ensure_alive()?;
            Ok(self.lock_state().tabs.clone())
        }

        pub(crate) fn mock_current_tab_info(&self) -> Result<Option<ObservedTab>> {
            self.ensure_alive()?;
            let state = self.lock_state();
            Ok(state.tabs.iter().find(|t| t.active).cloned())
        }

        pub(crate) fn mock_list_panes(&self) -> Result<Vec<ObservedPane>> {
            self.ensure_alive()?;
            Ok(self.lock_state().panes.clone())
        }
    }
}
