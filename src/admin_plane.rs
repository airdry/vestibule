//! Admin Plane: serves the operational endpoints (`/healthz`, `/readyz`,
//! `/status`, `POST /reload`) on a listener of their own, separated from
//! `Pass-Through Traffic` on the Data Plane.
//!
//! Per ADR-0003, `/readyz` stays `200 OK` while `Upstream Health` is
//! `Unhealthy`; upstream availability is surfaced separately via
//! `GET /status`. `/readyz` answers `503` once graceful shutdown starts.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};

use crate::config::Config;
use crate::health::UpstreamHealthTracker;
use crate::ingestion::lock_wal;
use crate::reload::{ReloadManager, ReloadOutcome};
use crate::shutdown::ShutdownFlag;
use crate::wal::Wal;

/// Shared state reported on the Admin Plane status endpoints.
pub struct StatusReport {
    health: Arc<UpstreamHealthTracker>,
    wal: Arc<Mutex<Wal>>,
    /// The `Ingestion Queue` directory, checked for accessibility by
    /// `/healthz`.
    queue_dir: PathBuf,
    /// Reload manager, reporting hot-reload status.
    reload: Arc<ReloadManager>,
}

impl StatusReport {
    /// Builds the report from the assembled application state.
    pub fn new(
        health: Arc<UpstreamHealthTracker>,
        wal: Arc<Mutex<Wal>>,
        config: &Config,
        reload: Arc<ReloadManager>,
    ) -> Self {
        Self {
            health,
            wal,
            queue_dir: config.queue.dir.clone(),
            reload,
        }
    }

    /// The `GET /status` document as JSON.
    fn status_json(&self) -> Result<serde_json::Value, String> {
        let utilization = lock_wal(&self.wal)
            .utilization()
            .map_err(|error| error.to_string())?;
        let reload = self.reload.state();
        Ok(serde_json::json!({
            "upstream": {
                "health": if self.health.is_healthy() { "healthy" } else { "unhealthy" },
            },
            "queue": {
                "pending_bytes": utilization.pending_bytes,
                "segments": utilization.segments,
                "oldest_entry_age_seconds": utilization
                    .oldest_entry_age
                    .map(|age| age.as_secs_f64()),
            },
            "config": {
                "status": if reload.last_error.is_some() { "reload-failed" } else { "loaded" },
                "reloads": reload.reloads,
                "last_reload_error": reload.last_error,
            },
        }))
    }

    /// `true` when the `Ingestion Queue` directory is still accessible.
    fn queue_accessible(&self) -> bool {
        std::fs::metadata(&self.queue_dir).is_ok()
    }
}

/// Serves the Admin Plane endpoints.
pub struct AdminPlane {
    status: Arc<StatusReport>,
    reload: Arc<ReloadManager>,
    shutdown: ShutdownFlag,
}

impl AdminPlane {
    pub fn new(
        status: Arc<StatusReport>,
        reload: Arc<ReloadManager>,
        shutdown: ShutdownFlag,
    ) -> Self {
        Self {
            status,
            reload,
            shutdown,
        }
    }
}

#[async_trait]
impl ProxyHttp for AdminPlane {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        Err(Error::explain(
            InternalError,
            "admin plane requests are never proxied",
        ))
    }

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let path = session.req_header().uri.path().to_string();
        let method = session.req_header().method.as_str().to_string();
        match (path.as_str(), method.as_str()) {
            ("/readyz", _) => {
                if self.shutdown.is_draining() {
                    respond(session, 503, "draining\n").await?
                } else {
                    respond(session, 200, "ok\n").await?
                }
            }
            ("/reload", "POST") => match self.reload.reload() {
                ReloadOutcome::Applied => {
                    let body = format!("{}\n", serde_json::json!({ "status": "reloaded" }));
                    respond_json(session, 200, body).await?;
                }
                ReloadOutcome::Rejected { error } => {
                    let body = format!("{}\n", serde_json::json!({ "error": error }));
                    respond_json(session, 400, body).await?;
                }
            },
            ("/healthz", _) => {
                // Process liveness plus Ingestion Queue accessibility.
                if self.status.queue_accessible() {
                    respond(session, 200, "ok\n").await?
                } else {
                    respond(session, 503, "ingestion queue inaccessible\n").await?
                }
            }
            ("/status", _) => match self.status.status_json() {
                Ok(document) => {
                    let body = format!("{document}\n");
                    respond_json(session, 200, body).await?;
                }
                Err(message) => {
                    let body = format!("{}\n", serde_json::json!({ "error": message }));
                    respond_json(session, 500, body).await?;
                }
            },
            _ => respond(session, 404, "not found\n").await?,
        }
        Ok(true)
    }
}

/// Writes a plain-text response with the given status and body.
async fn respond(session: &mut Session, status: u16, body: &str) -> Result<()> {
    let mut header = ResponseHeader::build(status, Some(2))?;
    header.insert_header("Content-Type", "text/plain; charset=utf-8")?;
    header.insert_header("Content-Length", body.len().to_string())?;
    session
        .write_response_header(Box::new(header), false)
        .await?;
    session
        .write_response_body(Some(Bytes::copy_from_slice(body.as_bytes())), true)
        .await
}

/// Writes a JSON response with the given status and pre-encoded body.
async fn respond_json(session: &mut Session, status: u16, body: String) -> Result<()> {
    let mut header = ResponseHeader::build(status, Some(2))?;
    header.insert_header("Content-Type", "application/json")?;
    header.insert_header("Content-Length", body.len().to_string())?;
    session
        .write_response_header(Box::new(header), false)
        .await?;
    session
        .write_response_body(Some(Bytes::copy_from_slice(body.as_bytes())), true)
        .await
}
