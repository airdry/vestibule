//! Data Plane: transparently forwards `Pass-Through Traffic` to the `Upstream`.
//!
//! This is the Pingora adapter for reverse proxying. Request headers (including
//! `Host`), bodies, query parameters, and `Upstream` response status codes and
//! headers are preserved; Pingora itself strips hop-by-hop headers.

use async_trait::async_trait;
use pingora::http::RequestHeader;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};

use crate::config::UpstreamTarget;

/// Pingora proxy that forwards every request to the configured `Upstream`.
pub struct DataPlaneProxy {
    upstream: UpstreamTarget,
}

impl DataPlaneProxy {
    pub fn new(upstream: UpstreamTarget) -> Self {
        Self { upstream }
    }
}

#[async_trait]
impl ProxyHttp for DataPlaneProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let peer = HttpPeer::new(
            (self.upstream.host.as_str(), self.upstream.port),
            self.upstream.tls,
            self.upstream.host.clone(),
        );
        Ok(Box::new(peer))
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        apply_path_prefix(upstream_request, &self.upstream.path_prefix)
    }
}

/// Prefixes the request target with the `Upstream` URL path, if any.
fn apply_path_prefix(request: &mut RequestHeader, path_prefix: &str) -> Result<()> {
    if path_prefix.is_empty() {
        return Ok(());
    }

    let prefixed_target = match request.uri.path_and_query() {
        Some(target) => format!("{path_prefix}{}", target.as_str()),
        None => path_prefix.to_string(),
    };
    // The target originates from the client request and may not be a valid
    // URI once recombined; fail the request instead of forwarding it to an
    // unintended path.
    let uri = prefixed_target.parse().or_err(
        InvalidHTTPHeader,
        "failed to rebuild the request target with the upstream path prefix",
    )?;
    request.set_uri(uri);
    Ok(())
}
