//! Admin Plane: serves the operational endpoints (`/healthz`, `/readyz`).
//!
//! Kubernetes liveness and readiness probes live here, on a listener of
//! their own, separated from `Pass-Through Traffic` on the Data Plane.

use async_trait::async_trait;
use bytes::Bytes;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};

/// Serves the Admin Plane endpoints.
pub struct AdminPlane;

impl AdminPlane {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AdminPlane {
    fn default() -> Self {
        Self::new()
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
        // Admin Plane requests are fully answered in `request_filter`, so
        // proxying is never reached.
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
        match path.as_str() {
            "/healthz" | "/readyz" => respond(session, 200, "ok\n").await?,
            _ => respond(session, 404, "not found\n").await?,
        }
        // `true`: the response is already written; skip upstream proxying.
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
