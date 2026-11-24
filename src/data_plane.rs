use async_trait::async_trait;
use bytes::Bytes;
use pingora::http::RequestHeader;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};

use crate::codec::Payload;
use crate::config::UpstreamTarget;
use crate::decompression::{DecompressError, DecompressedBody, decompress};
use crate::ingestion::{
    self, ForwardedMetadata, IngestionOutcome, IngestionPipeline, RateLimitDecision,
};

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
            IngestionOutcome::Accepted {
                event_id,
                rate_limits,
            } => {
                acknowledge_ingestion(
                    session,
                    self.ingestion.success_status_code(),
                    event_id,
                    rate_limits.as_deref(),
                )
                .await?;
            }
            IngestionOutcome::PayloadTooLarge => {
                session.respond_error(413).await?;
            }
            IngestionOutcome::Throttled {
                rate_limits,
                retry_after,
            } => {
                respond_throttled(session, &rate_limits, retry_after).await?;
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

/// Reads the ingestion request body, decompresses it for inspection,
/// and appends the original bytes to the `Ingestion Queue`.
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

    let payload = match inspect_body(
        &route,
        content_encoding(session),
        &body,
        pipeline.max_decompressed_body_bytes(),
    ) {
        Inspection::Accepted(payload) => payload,
        Inspection::TooLarge => return Ok(IngestionOutcome::PayloadTooLarge),
    };
    let event_id = match &payload {
        Payload::Envelope(envelope) => envelope.event_id(),
        Payload::StoreEvent { .. } | Payload::Unrecognized => None,
    };

    // Rate limiting: rebuilt envelopes replace the original bytes, so the
    // forwarded `content-encoding` header must be dropped for them.
    let (body, auth) = match pipeline.evaluate(&route, &payload) {
        RateLimitDecision::Allowed => (body, auth),
        RateLimitDecision::PartiallyDenied { body, rate_limits } => {
            let auth = ingestion::strip_content_encoding(&auth);
            accept(pipeline, &route, &auth, &body)?;
            return Ok(IngestionOutcome::Accepted {
                event_id,
                rate_limits: Some(rate_limits),
            });
        }
        RateLimitDecision::FullyDenied {
            rate_limits,
            retry_after,
        } => {
            return Ok(IngestionOutcome::Throttled {
                rate_limits,
                retry_after,
            });
        }
    };

    accept(pipeline, &route, &auth, &body)?;
    Ok(IngestionOutcome::Accepted {
        event_id,
        rate_limits: None,
    })
}

/// Appends a record to the `Ingestion Queue`, mapping WAL errors to
/// internal Pingora errors.
fn accept(pipeline: &IngestionPipeline, route: &str, auth: &str, body: &[u8]) -> Result<()> {
    pipeline.accept(route, auth, body).map_err(|error| {
        pingora::Error::because(
            InternalError,
            format!("ingestion queue append failed: {error}"),
            error,
        )
    })
}

/// The result of decompressing and parsing an ingestion body.
enum Inspection {
    /// The body is acceptable; carries the parsed payload.
    Accepted(Payload),
    /// The decompressed body exceeded `ingest.max_decompressed_body_bytes`.
    TooLarge,
}

/// Decompresses the body (failing open on unsupported or corrupt
/// encodings) and extracts the envelope header `event_id` when present.
fn inspect_body(
    route: &str,
    encoding: Option<&str>,
    body: &[u8],
    max_decompressed_body_bytes: u64,
) -> Inspection {
    let decompressed = match decompress(encoding.unwrap_or(""), body, max_decompressed_body_bytes) {
        Ok(DecompressedBody::Bytes(decompressed)) => decompressed,
        Ok(DecompressedBody::Passthrough) => body.to_vec(),
        // Corrupted compressed streams and decompressed oversizes fail open:
        // the raw bytes are preserved as-is.
        Err(DecompressError::TooLarge) => return Inspection::TooLarge,
        Err(DecompressError::Corrupt) => body.to_vec(),
    };

    Inspection::Accepted(parse_payload(route, &decompressed))
}

/// Parses the body according to the `Ingestion Route` shape.
fn parse_payload(route: &str, body: &[u8]) -> Payload {
    if route.ends_with("/envelope/") {
        Payload::parse_envelope(body)
    } else if route.ends_with("/store/") {
        Payload::parse_store_event(body)
    } else {
        Payload::Unrecognized
    }
}

/// The `Content-Encoding` header value, if any.
fn content_encoding(session: &Session) -> Option<&str> {
    session
        .req_header()
        .headers
        .get("content-encoding")
        .and_then(|value| value.to_str().ok())
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
/// an empty or `event_id`-bearing JSON body, attaching rate limit
/// information when any item was throttled.
async fn acknowledge_ingestion(
    session: &mut Session,
    status: u16,
    event_id: Option<String>,
    rate_limits: Option<&str>,
) -> Result<()> {
    let body = match event_id {
        Some(event_id) => serde_json::json!({ "id": event_id }).to_string(),
        None => "{}".to_string(),
    };
    let mut response = ResponseHeader::build(status, Some(2))
        .or_err(InternalError, "failed to build the ingestion response")?;
    response
        .insert_header("Content-Type", "application/json")
        .or_err(InternalError, "failed to set the ingestion response type")?;
    if let Some(rate_limits) = rate_limits {
        response
            .insert_header("X-Sentry-Rate-Limits", rate_limits)
            .or_err(InternalError, "failed to set the rate limits header")?;
    }
    session
        .write_response_header(Box::new(response), false)
        .await?;
    session
        .write_response_body(Some(Bytes::from(body)), true)
        .await
}

/// Rejects a fully throttled request with `429 Too Many Requests` and the
/// `X-Sentry-Rate-Limits` / `Retry-After` headers.
async fn respond_throttled(
    session: &mut Session,
    rate_limits: &str,
    retry_after: u64,
) -> Result<()> {
    let mut response = ResponseHeader::build(429, Some(2))
        .or_err(InternalError, "failed to build the throttled response")?;
    response
        .insert_header("X-Sentry-Rate-Limits", rate_limits)
        .or_err(InternalError, "failed to set the rate limits header")?;
    response
        .insert_header("Retry-After", retry_after.to_string())
        .or_err(InternalError, "failed to set the retry-after header")?;
    session
        .write_response_header(Box::new(response), true)
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
