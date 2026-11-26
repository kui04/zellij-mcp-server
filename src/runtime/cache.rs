//! Lightweight session metadata cache (`~/.cache/zellij-mcp/<session>.json`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::domain::ids::{CommandSpec, ManagedKey, SessionName, TabId, TerminalPaneId};
use crate::domain::layout::ResolvedPlacement;

/// Persisted lightweight index aiding cold-start reconciliation across server restarts.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionCache {
    pub managed_tasks_tab: Option<TabId>,
    pub managed_services_tab: Option<TabId>,
    pub panes: Vec<CachedPaneEntry>,
}

/// Minimal metadata cached per managed pane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPaneEntry {
    pub key: ManagedKey,
    pub pane_id: TerminalPaneId,
    pub tab_id: TabId,
    pub placement: ResolvedPlacement,
    pub cmd: CommandSpec,
    pub cwd: PathBuf,
    pub ready_pattern: Option<String>,
    pub prompt_pattern: Option<String>,
}

/// Returns the cache file path `~/.cache/zellij-mcp/<session>.json`.
pub fn cache_path(session: &SessionName) -> Option<PathBuf> {
    dirs::cache_dir().map(|base| {
        base.join("zellij-mcp")
            .join(format!("{}.json", session.as_str()))
    })
}

/// Loads an existing session cache if present; returns `Ok(None)` when absent or corrupted.
pub async fn load_cache(session: &SessionName) -> Result<Option<SessionCache>> {
    let Some(path) = cache_path(session) else {
        return Ok(None);
    };
    match tokio::fs::read_to_string(&path).await {
        Ok(raw) => match serde_json::from_str::<SessionCache>(&raw) {
            Ok(cache) => Ok(Some(cache)),
            Err(err) => {
                warn!(path = %path.display(), error = %err, "ignoring corrupted session cache");
                Ok(None)
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("failed to read cache {}", path.display())),
    }
}

/// Atomically persists the session cache (write temp file + rename).
pub async fn save_cache(session: &SessionName, cache: &SessionCache) -> Result<()> {
    let Some(path) = cache_path(session) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create cache dir {}", parent.display()))?;
    }
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("json.tmp.{}.{seq}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(cache).context("failed to serialize session cache")?;
    tokio::fs::write(&tmp, bytes)
        .await
        .with_context(|| format!("failed to write temp cache {}", tmp.display()))?;
    tokio::fs::rename(&tmp, &path)
        .await
        .with_context(|| format!("failed to atomically replace cache {}", path.display()))?;
    Ok(())
}

/// Removes the persisted session cache file if it exists.
pub async fn remove_cache(session: &SessionName) {
    let Some(path) = cache_path(session) else {
        return;
    };
    if let Err(err) = tokio::fs::remove_file(&path).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %path.display(), error = %err, "failed to remove session cache");
    }
}
