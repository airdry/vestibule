//! Config hot-reload: the reload manager (atomic [`RuntimeSettings`]
//! swaps) and its triggers — a parent-directory file watcher with
//! debouncing (atomic Kubernetes `ConfigMap` symlink swaps included) and
//! `SIGHUP`; the Admin Plane's `POST /reload` goes through the exact same
//! [`ReloadManager::reload`] path.
//!
//! Startup-only fields (`server` listen addresses, `queue.dir`) cannot be
//! applied after startup: changes are validated but only warned about,
//! while the previous runtime settings stay active when validation fails.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use tokio::sync::mpsc;

use notify::Watcher;

use crate::config::{Config, SharedSettings, load_config};

/// How long reload triggers are debounced, absorbing editor write patterns
/// and the rename dance of atomic symlink swaps.
const DEBOUNCE: Duration = Duration::from_millis(200);

/// The outcome of one reload attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// The new configuration validated and was swapped in atomically.
    Applied,
    /// Validation failed; the previous configuration stays active.
    Rejected { error: String },
}

/// Reload status reported by `GET /status`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReloadState {
    /// Successful reloads since startup.
    pub reloads: u64,
    /// The error of the last rejected reload, if any.
    pub last_error: Option<String>,
}

/// Loads, validates, and atomically swaps the hot-reloadable runtime
/// settings; the single reload path shared by every trigger.
pub struct ReloadManager {
    path: PathBuf,
    settings: SharedSettings,
    startup: StartupSettings,
    state: Mutex<ReloadState>,
}

/// Startup-only settings compared on reload: they cannot be applied after
/// startup, so changes are only warned about.
struct StartupSettings {
    data_plane_addr: SocketAddr,
    admin_addr: SocketAddr,
    queue_dir: PathBuf,
}

impl ReloadManager {
    /// Builds the manager from the startup configuration and the shared
    /// settings store it will keep swapping.
    pub fn new(path: PathBuf, config: &Config, settings: SharedSettings) -> Self {
        Self {
            path,
            settings,
            startup: StartupSettings {
                data_plane_addr: config.server.data_plane_addr,
                admin_addr: config.server.admin_addr,
                queue_dir: config.queue.dir.clone(),
            },
            state: Mutex::new(ReloadState::default()),
        }
    }

    /// The current reload status (for `GET /status`).
    pub fn state(&self) -> ReloadState {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// One reload attempt: load and validate the YAML file, then swap the
    /// hot-reloadable runtime settings in atomically. On any error the
    /// previously active configuration stays in place.
    pub fn reload(&self) -> ReloadOutcome {
        match load_config(&self.path) {
            Ok(config) => {
                self.warn_startup_only_changes(&config);
                self.settings.store(Arc::new(config.runtime_settings()));
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.reloads += 1;
                state.last_error = None;
                ReloadOutcome::Applied
            }
            Err(error) => {
                let error = error.to_string();
                eprintln!(
                    "reload: rejected invalid configuration from {}: {error}",
                    self.path.display()
                );
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.last_error = Some(error.clone());
                ReloadOutcome::Rejected { error }
            }
        }
    }

    /// Warns about startup-only changes: they validate fine but cannot take
    /// effect without a restart.
    fn warn_startup_only_changes(&self, config: &Config) {
        if config.server.data_plane_addr != self.startup.data_plane_addr {
            eprintln!(
                "reload: server.data_plane_addr is startup-only and was not applied \
                 (restart required)"
            );
        }
        if config.server.admin_addr != self.startup.admin_addr {
            eprintln!(
                "reload: server.admin_addr is startup-only and was not applied \
                 (restart required)"
            );
        }
        if config.queue.dir != self.startup.queue_dir {
            eprintln!("reload: queue.dir is startup-only and was not applied (restart required)");
        }
    }
}

/// Watches the config file's parent directory (so both plain bind-mount
/// writes and atomic symlink swaps are seen) and triggers debounced
/// reloads.
pub struct ConfigWatcher {
    manager: Arc<ReloadManager>,
    debounce: Duration,
}

impl ConfigWatcher {
    /// Builds the watcher with the default debounce window.
    pub fn new(manager: Arc<ReloadManager>) -> Self {
        Self {
            manager,
            debounce: DEBOUNCE,
        }
    }

    /// Watches until shutdown is requested, reloading on (debounced)
    /// directory changes.
    async fn watch(&self, mut shutdown: ShutdownWatch) {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        // The watcher must outlive the loop: dropping it stops the events.
        // Bound to `_watcher`, the events keep flowing until the function
        // returns.
        let _watcher = match watch_directory(&self.manager.path, events_tx) {
            Ok(watcher) => watcher,
            Err(error) => {
                eprintln!("reload: failed to watch the config directory: {error}");
                return;
            }
        };

        loop {
            // Wait for the first event (or shutdown); then keep extending
            // the debounce deadline while more events arrive.
            tokio::select! {
                _ = shutdown.changed() => return,
                event = events_rx.recv() => {
                    if event.is_none() {
                        return;
                    }
                    let deadline = tokio::time::Instant::now() + self.debounce;
                    loop {
                        tokio::select! {
                            _ = shutdown.changed() => return,
                            _ = tokio::time::sleep_until(deadline) => break,
                            _ = events_rx.recv() => {}
                        }
                    }
                    self.manager.reload();
                }
            }
        }
    }
}

/// Starts a filesystem watcher on the config file's parent directory,
/// forwarding events into `events`. Only the parent directory is watched:
/// Kubernetes `ConfigMap` updates atomically swap the `..data` symlink in
/// the mount root, which surfaces as events there.
///
/// The returned watcher must be kept alive to keep receiving events.
fn watch_directory(
    config_path: &Path,
    events: mpsc::UnboundedSender<notify::Result<notify::Event>>,
) -> Result<notify::RecommendedWatcher, String> {
    let parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut watcher = notify::recommended_watcher(events).map_err(|error| error.to_string())?;
    watcher
        .watch(parent, notify::RecursiveMode::NonRecursive)
        .map_err(|error| error.to_string())?;
    Ok(watcher)
}

#[async_trait::async_trait]
impl BackgroundService for ConfigWatcher {
    async fn start(&self, shutdown: ShutdownWatch) {
        self.watch(shutdown).await;
    }
}

/// Reloads the configuration on every `SIGHUP`.
pub struct SighupReloader {
    manager: Arc<ReloadManager>,
}

impl SighupReloader {
    pub fn new(manager: Arc<ReloadManager>) -> Self {
        Self { manager }
    }
}

#[async_trait::async_trait]
impl BackgroundService for SighupReloader {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut sighup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(sighup) => sighup,
            Err(error) => {
                eprintln!("reload: failed to listen for SIGHUP: {error}");
                return;
            }
        };

        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = sighup.recv() => {
                    self.manager.reload();
                }
            }
        }
    }
}
