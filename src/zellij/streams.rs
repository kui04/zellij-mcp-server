//! Long-lived Zellij event bus (`zellij events`) and multiplexed viewport stream (`zellij subscribe`).

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::domain::ids::{SessionName, TerminalPaneId};
use crate::state::ZellijEvent;
use crate::zellij::protocol::{SubscribeFrame, parse_event_line, parse_subscribe_line};

const STREAM_RETRY_BACKOFF: Duration = Duration::from_millis(400);
const STREAM_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// Configuration for spawning the background Zellij streaming subprocesses.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub binary: PathBuf,
    pub session: SessionName,
}

/// Handle to stop background streaming subprocesses cleanly without touching the Zellij session.
#[derive(Debug)]
pub struct StreamHandles {
    shutdown_tx: watch::Sender<bool>,
    pane_set_tx: watch::Sender<Vec<TerminalPaneId>>,
    event_task: Option<JoinHandle<()>>,
    subscribe_task: Option<JoinHandle<()>>,
}

impl StreamHandles {
    /// Updates the set of pane IDs monitored by `zellij subscribe`.
    pub fn update_subscribed_panes(&self, mut pane_ids: Vec<TerminalPaneId>) {
        pane_ids.sort_unstable();
        pane_ids.dedup();
        if *self.pane_set_tx.borrow() != pane_ids {
            let _ = self.pane_set_tx.send(pane_ids);
        }
    }

    /// Signals both background stream tasks to terminate and waits for them to reap their child
    /// processes cleanly.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.event_task.take() {
            let _ = tokio::time::timeout(STREAM_SHUTDOWN_TIMEOUT, handle).await;
        }
        if let Some(handle) = self.subscribe_task.take() {
            let _ = tokio::time::timeout(STREAM_SHUTDOWN_TIMEOUT, handle).await;
        }
    }
}

impl Drop for StreamHandles {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Spawns the `zellij events` and `zellij subscribe` supervisor tasks.
pub fn spawn_streams(
    config: StreamConfig,
    event_tx: mpsc::Sender<ZellijEvent>,
    frame_tx: mpsc::Sender<SubscribeFrame>,
) -> StreamHandles {
    let (shutdown_tx, shutdown_rx_events) = watch::channel(false);
    let shutdown_rx_subscribe = shutdown_tx.subscribe();
    let (pane_set_tx, pane_set_rx) = watch::channel(Vec::<TerminalPaneId>::new());

    let event_cfg = config.clone();
    let event_task = tokio::spawn(async move {
        run_event_bus(event_cfg, event_tx, shutdown_rx_events).await;
    });

    let subscribe_task = tokio::spawn(async move {
        run_viewport_mux(config, pane_set_rx, frame_tx, shutdown_rx_subscribe).await;
    });

    StreamHandles {
        shutdown_tx,
        pane_set_tx,
        event_task: Some(event_task),
        subscribe_task: Some(subscribe_task),
    }
}

async fn run_event_bus(
    config: StreamConfig,
    event_tx: mpsc::Sender<ZellijEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let spawn_result = Command::new(&config.binary)
            .args([
                "--session",
                config.session.as_str(),
                "events",
                "--type",
                "pane_exited",
                "pane_closed",
                "tab_focused",
                "tab_closed",
                "session_ending",
                "--no-initial-state",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(err) => {
                warn!(error = %err, "failed to spawn zellij events; retrying");
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    () = tokio::time::sleep(STREAM_RETRY_BACKOFF) => continue,
                }
            }
        };

        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill().await;
            break;
        };
        let mut lines = BufReader::new(stdout).lines();

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        return;
                    }
                }
                line = lines.next_line() => {
                    match line {
                        Ok(Some(raw)) => {
                            match parse_event_line(&raw) {
                                Ok(Some(ev)) => {
                                    if event_tx.send(ev).await.is_err() {
                                        let _ = child.kill().await;
                                        let _ = child.wait().await;
                                        return;
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => debug!(error = %err, "ignoring unparseable event line"),
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = %err, "error reading zellij events stdout");
                            break;
                        }
                    }
                }
            }
        }

        let _ = child.kill().await;
        let _ = child.wait().await;

        tokio::select! {
            _ = shutdown_rx.changed() => break,
            () = tokio::time::sleep(STREAM_RETRY_BACKOFF) => {}
        }
    }
}

async fn run_viewport_mux(
    config: StreamConfig,
    mut pane_set_rx: watch::Receiver<Vec<TerminalPaneId>>,
    frame_tx: mpsc::Sender<SubscribeFrame>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let pane_ids = pane_set_rx.borrow_and_update().clone();
        if pane_ids.is_empty() {
            tokio::select! {
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                res = pane_set_rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }
            continue;
        }

        let mut args = vec![
            "--session".to_owned(),
            config.session.as_str().to_owned(),
            "subscribe".to_owned(),
            "--format".to_owned(),
            "json".to_owned(),
        ];
        for pane_id in &pane_ids {
            args.push("--pane-id".to_owned());
            args.push(pane_id.cli_arg());
        }

        let spawn_result = Command::new(&config.binary)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(err) => {
                warn!(error = %err, "failed to spawn zellij subscribe; retrying");
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = pane_set_rx.changed() => continue,
                    () = tokio::time::sleep(STREAM_RETRY_BACKOFF) => continue,
                }
            }
        };

        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill().await;
            break;
        };
        let mut lines = BufReader::new(stdout).lines();

        let mut restart_immediately = false;
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        return;
                    }
                }
                changed = pane_set_rx.changed() => {
                    if changed.is_err() {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        return;
                    }
                    restart_immediately = true;
                    break;
                }
                line = lines.next_line() => {
                    match line {
                        Ok(Some(raw)) => {
                            match parse_subscribe_line(&raw) {
                                Ok(Some(frame)) => {
                                    if frame_tx.send(frame).await.is_err() {
                                        let _ = child.kill().await;
                                        let _ = child.wait().await;
                                        return;
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => debug!(error = %err, "ignoring unparseable subscribe line"),
                            }
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = %err, "error reading zellij subscribe stdout");
                            break;
                        }
                    }
                }
            }
        }

        let _ = child.kill().await;
        let _ = child.wait().await;

        if !restart_immediately {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = pane_set_rx.changed() => {}
                () = tokio::time::sleep(STREAM_RETRY_BACKOFF) => {}
            }
        }
    }
}
