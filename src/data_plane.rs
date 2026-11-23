use async_trait::async_trait;
use bytes::Bytes;
use pingora::http::RequestHeader;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};

use crate::config::UpstreamTarget;
use crate::ingestion::{self, ForwardedMetadata, IngestionOutcome, IngestionPipeline};

/// Pingora proxy that forwards `Pass-Through Traffic` to the configured
/// `Upstream` and intercepts `Ingestion Route` requests into the local
/// ingestion pipeline.
pub struct DataPlaneProxy {
    upstream: UpstreamTarget,
    ingestion: IngestionPipeline,
}

impl DataPlaneProxy {
    pub fn new(upstream: UpstreamTarget, ingestion: IngestionPipeline) -> Self {
        Self {
            upstream,
            ingestion,
        }
    }
}

#[async_trait]
impl ProxyHttp for DataPlaneProxy {
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool> {
        if !ingestion::is_ingestion_route(session.req_header().uri.path()) {
            return Ok(false);
        }

        let outcome = ingest(&self.ingestion, session).await?;
        match outcome {
            IngestionOutcome::Accepted => {
                acknowledge_ingestion(session, self.ingestion.success_status_code()).await?;
            }
            IngestionOutcome::PayloadTooLarge => {
                session.respond_error(413).await?;
            }
        }
        Ok(true)
    }

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

/// Reads the ingestion request body (enforcing the compressed size limit)
/// and appends it to the `Ingestion Queue`.
async fn ingest(pipeline: &IngestionPipeline, session: &mut Session) -> Result<IngestionOutcome> {
    let (route, auth) = capture_request_metadata(session);
    let max_body_bytes = pipeline.max_compressed_body_bytes() as usize;

    if content_length_exceeds(session, max_body_bytes) {
        return Ok(IngestionOutcome::PayloadTooLarge);
    }

    let body = match read_limited_body(session, max_body_bytes).await? {
        Some(body) => body,
        None => return Ok(IngestionOutcome::PayloadTooLarge),
    };
    session.as_downstream_mut().finish_body().await?;

    pipeline.accept(&route, &auth, &body).map_err(|error| {
        pingora::Error::because(
            InternalError,
            format!("ingestion queue append failed: {error}"),
            error,
        )
    })?;
    Ok(IngestionOutcome::Accepted)
}

/// Captures the route and forwarded metadata before the body is consumed.
fn capture_request_metadata(session: &Session) -> (String, String) {
    let header = session.req_header();
    let route = header.uri.path().to_string();
    let metadata = ForwardedMetadata {
        query: header.uri.query().unwrap_or_default().to_string(),
        headers: forwarded_headers(header),
    };
    (route, metadata.encode())
}

fn forwarded_headers(header: &RequestHeader) -> Vec<(String, String)> {
    const FORWARDED: [&str; 3] = ["x-sentry-auth", "content-type", "content-encoding"];

    FORWARDED
        .iter()
        .filter_map(|name| {
            header.headers.get(*name).map(|value| {
                (
                    name.to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
        })
        .collect()
}

/// `true` when the declared `Content-Length` already exceeds the limit.
fn content_length_exceeds(session: &Session, max_body_bytes: usize) -> bool {
    session
        .req_header()
        .headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > max_body_bytes)
}

/// Reads the whole request body, returning `None` when it exceeds
/// `max_body_bytes`.
async fn read_limited_body(
    session: &mut Session,
    max_body_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    while let Some(chunk) = session.as_downstream_mut().read_request_body().await? {
        if body.len() + chunk.len() > max_body_bytes {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// Immediately acknowledges the SDK with the configured success status and
/// an empty JSON body.
async fn acknowledge_ingestion(session: &mut Session, status: u16) -> Result<()> {
    let mut response = ResponseHeader::build(status, Some(2))
        .or_err(InternalError, "failed to build the ingestion response")?;
    response
        .insert_header("Content-Type", "application/json")
        .or_err(InternalError, "failed to set the ingestion response type")?;
    session
        .write_response_header(Box::new(response), false)
        .await?;
    session
        .write_response_body(Some(Bytes::from_static(b"{}")), true)
        .await
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
    let uri = prefixed_target.parse().or_err(
        InvalidHTTPHeader,
        "failed to rebuild the request target with the upstream path prefix",
    )?;
    request.set_uri(uri);
    Ok(())
}
