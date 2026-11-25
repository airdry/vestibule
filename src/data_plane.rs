use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use pingora::http::RequestHeader;
use pingora::prelude::*;
use pingora::proxy::{ProxyHttp, Session};

use crate::codec::Payload;
use crate::config::{SharedSettings, UpstreamTarget};
use crate::decompression::{DecompressError, DecompressedBody, decompress};
use crate::ingestion::{
    self, ForwardedMetadata, IngestionOutcome, IngestionPipeline, RateLimitDecision,
};
use crate::metrics::{Metrics, OTHER_ENVIRONMENT, normalize_environment};

/// Pingora proxy that forwards `Pass-Through Traffic` to the configured
/// `Upstream` and intercepts `Ingestion Route` requests into the local
/// ingestion pipeline.
///
/// Every request accepted on the Data Plane port — ingestion and
/// pass-through alike — is recorded exactly once in the port-level RED
/// pair `http_requests_total{direction="inbound",...}` /
/// `http_request_duration_seconds`; what the service sends onwards to the
/// `Upstream` is counted separately in the `upstream_*` families.
pub struct DataPlaneProxy {
    upstream: UpstreamTarget,
    ingestion: IngestionPipeline,
    metrics: Arc<Metrics>,
    settings: SharedSettings,
}

impl DataPlaneProxy {
    pub fn new(
        upstream: UpstreamTarget,
        ingestion: IngestionPipeline,
        metrics: Arc<Metrics>,
        settings: SharedSettings,
    ) -> Self {
        Self {
            upstream,
            ingestion,
            metrics,
            settings,
        }
    }
}

/// How the Data Plane classifies one request arriving on its port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteKind {
    /// An `Ingestion Route` request intercepted into the pipeline.
    Ingest,
    /// `Pass-Through Traffic` forwarded transparently to the `Upstream`.
    PassThrough,
}

impl RouteKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::PassThrough => "passthrough",
        }
    }
}

/// Ingestion labels resolved while handling an `Ingestion Route` request.
struct IngestLabels {
    environment: String,
    outcome: &'static str,
}

/// Per-request metric context carried from `request_filter` to the
/// `logging` phase, where the port-level RED pair is recorded.
pub struct DataPlaneCtx {
    start: Option<Instant>,
    route_kind: RouteKind,
    project_id: String,
    ingest: Option<IngestLabels>,
    recorded: bool,
}

impl Default for DataPlaneCtx {
    fn default() -> Self {
        Self {
            start: None,
            route_kind: RouteKind::PassThrough,
            project_id: String::new(),
            ingest: None,
            recorded: false,
        }
    }
}

#[async_trait]
impl ProxyHttp for DataPlaneProxy {
    type CTX = DataPlaneCtx;

    fn new_ctx(&self) -> Self::CTX {
        DataPlaneCtx::default()
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        // The port-level clock starts as soon as the request reached the
        // proxy filter.
        ctx.start = Some(Instant::now());
        let path = session.req_header().uri.path();
        ctx.route_kind = if ingestion::is_ingestion_route(path) {
            RouteKind::Ingest
        } else {
            RouteKind::PassThrough
        };
        ctx.project_id = match ctx.route_kind {
            RouteKind::Ingest => ingestion::project_id(path)
                .map(|id| id.to_string())
                .unwrap_or_default(),
            RouteKind::PassThrough => String::new(),
        };
        if ctx.route_kind == RouteKind::PassThrough {
            // Metrics are recorded in `logging`, once the response status
            // is known.
            return Ok(false);
        }

        let (outcome, environment) = self.ingest(session).await?;
        ctx.ingest = Some(IngestLabels {
            environment,
            outcome: outcome_label(&outcome),
        });
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

    async fn logging(&self, session: &mut Session, error: Option<&Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let Some(start) = ctx.start else {
            return;
        };
        if ctx.recorded {
            return;
        }
        ctx.recorded = true;

        let route_kind = ctx.route_kind;
        let project_id = ctx.project_id.clone();
        let (environment, outcome) = match ctx.ingest.take() {
            Some(labels) => (labels.environment, labels.outcome),
            None => match route_kind {
                // The ingestion handler failed before resolving an outcome.
                RouteKind::Ingest => (OTHER_ENVIRONMENT.to_string(), "error"),
                RouteKind::PassThrough => (
                    OTHER_ENVIRONMENT.to_string(),
                    passthrough_outcome(session, error),
                ),
            },
        };
        self.metrics.record_http_request(
            route_kind.as_str(),
            &project_id,
            &environment,
            outcome,
            start.elapsed(),
        );
    }
}

/// The `http_requests_total` outcome label of an ingestion outcome.
fn outcome_label(outcome: &IngestionOutcome) -> &'static str {
    match outcome {
        IngestionOutcome::Accepted { .. } => "accepted",
        IngestionOutcome::PayloadTooLarge => "too_large",
        IngestionOutcome::Throttled { .. } => "throttled",
    }
}

/// The `http_requests_total` outcome label of a pass-through request:
/// `5xx` responses and proxy errors are `forwarded_error`.
fn passthrough_outcome(session: &Session, error: Option<&Error>) -> &'static str {
    if error.is_some() {
        return "forwarded_error";
    }
    match session.response_written() {
        Some(response) if (500..=599).contains(&response.status.as_u16()) => "forwarded_error",
        _ => "forwarded",
    }
}

impl DataPlaneProxy {
    /// Reads the ingestion request body, decompresses it for inspection,
    /// and appends the original bytes to the `Ingestion Queue`.
    ///
    /// Returns the outcome plus the normalized request-level `environment`
    /// label (the store event's or the first envelope Item's environment,
    /// bucketed by the allowlist).
    async fn ingest(&self, session: &mut Session) -> Result<(IngestionOutcome, String)> {
        let (route, auth) = capture_request_metadata(session);
        let max_body_bytes = self.ingestion.max_compressed_body_bytes() as usize;

        if content_length_exceeds(session, max_body_bytes) {
            return Ok((
                IngestionOutcome::PayloadTooLarge,
                OTHER_ENVIRONMENT.to_string(),
            ));
        }

        let body = match read_limited_body(session, max_body_bytes).await? {
            Some(body) => body,
            None => {
                return Ok((
                    IngestionOutcome::PayloadTooLarge,
                    OTHER_ENVIRONMENT.to_string(),
                ));
            }
        };
        session.as_downstream_mut().finish_body().await?;

        let payload = match inspect_body(
            &route,
            content_encoding(session),
            &body,
            self.ingestion.max_decompressed_body_bytes(),
        ) {
            Inspection::Accepted(payload) => payload,
            Inspection::TooLarge => {
                return Ok((
                    IngestionOutcome::PayloadTooLarge,
                    OTHER_ENVIRONMENT.to_string(),
                ));
            }
        };
        let environment = self.normalized_environment(&payload);
        let event_id = match &payload {
            Payload::Envelope(envelope) => envelope.event_id(),
            Payload::StoreEvent { .. } | Payload::Unrecognized => None,
        };

        // Rate limiting: rebuilt envelopes replace the original bytes, so the
        // forwarded `content-encoding` header must be dropped for them.
        let (body, auth) = match self.ingestion.evaluate(&route, &payload) {
            RateLimitDecision::Allowed => (body, auth),
            RateLimitDecision::PartiallyDenied { body, rate_limits } => {
                let auth = ingestion::strip_content_encoding(&auth);
                accept(self, &route, &auth, &body)?;
                return Ok((
                    IngestionOutcome::Accepted {
                        event_id,
                        rate_limits: Some(rate_limits),
                    },
                    environment,
                ));
            }
            RateLimitDecision::FullyDenied {
                rate_limits,
                retry_after,
            } => {
                return Ok((
                    IngestionOutcome::Throttled {
                        rate_limits,
                        retry_after,
                    },
                    environment,
                ));
            }
        };

        accept(self, &route, &auth, &body)?;
        Ok((
            IngestionOutcome::Accepted {
                event_id,
                rate_limits: None,
            },
            environment,
        ))
    }

    /// The normalized request-level `environment` metric label.
    fn normalized_environment(&self, payload: &Payload) -> String {
        let snapshot = self.settings.load();
        normalize_environment(
            &snapshot.observability.environment_label_allowlist,
            &payload_environment(payload),
        )
        .to_string()
    }
}

/// The raw request-level environment: the store event's environment or
/// the first envelope Item's environment.
fn payload_environment(payload: &Payload) -> String {
    match payload {
        Payload::StoreEvent { environment } => environment.clone(),
        Payload::Envelope(envelope) => envelope
            .summaries()
            .first()
            .map(|summary| summary.environment.clone())
            .unwrap_or_default(),
        Payload::Unrecognized => String::new(),
    }
}

/// Appends a record to the `Ingestion Queue`, mapping WAL errors to
/// internal Pingora errors.
fn accept(proxy: &DataPlaneProxy, route: &str, auth: &str, body: &[u8]) -> Result<()> {
    proxy.ingestion.accept(route, auth, body).map_err(|error| {
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
