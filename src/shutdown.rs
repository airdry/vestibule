//! Graceful `SIGTERM` shutdown: flips `/readyz` to `503` as soon as
//! shutdown starts, and flushes plus `fsync`s the `Ingestion Queue` and
//! its checkpoint before the process exits.
//!
//! Pingora broadcasts the shutdown event to every service (stopping new
//! connections and letting in-flight requests finish within
//! `server.shutdown_timeout`), then exits the process with code `0`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;

use crate::ingestion::lock_wal;
use crate::wal::Wal;

/// Whether the process is draining: `/readyz` answers `503` while set.
#[derive(Clone)]
pub struct ShutdownFlag(Arc<AtomicBool>);

impl ShutdownFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// `true` once graceful shutdown has started.
    pub fn is_draining(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl Default for ShutdownFlag {
    fn default() -> Self {
        Self::new()
    }
}

/// Flips the [`ShutdownFlag`] as soon as Pingora broadcasts shutdown.
pub struct ReadinessDrainer {
    flag: ShutdownFlag,
}

impl ReadinessDrainer {
    pub fn new(flag: ShutdownFlag) -> Self {
        Self { flag }
    }
}

#[async_trait]
impl BackgroundService for ReadinessDrainer {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        if shutdown.changed().await.is_ok() {
            self.flag.0.store(true, Ordering::Release);
            crate::log_info!(
                "shutdown",
                "readiness flipped to draining (/readyz answers 503)"
            );
        }
    }
}

/// Flushes and `fsync`s the `Ingestion Queue` before exit: in-flight
/// ingestion requests finish appending while the shutdown broadcast
/// spreads, then this service durably syncs whatever they wrote.
pub struct WalFlusher {
    wal: Arc<Mutex<Wal>>,
}

impl WalFlusher {
    pub fn new(wal: Arc<Mutex<Wal>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl BackgroundService for WalFlusher {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        if shutdown.changed().await.is_ok()
            && let Err(error) = lock_wal(&self.wal).sync()
        {
            crate::log_error!("shutdown", "failed to sync the ingestion queue: {error}");
        }
    }
}
