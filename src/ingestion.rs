//! Local ingestion pipeline for `Ingestion Route` requests.
//!
//! Matching requests are appended to the `Ingestion Queue` (segmented disk
//! WAL) and acknowledged immediately, without any synchronous hop to
//! `Upstream` (per ADR-0002).

use std::sync::{Arc, Mutex};

use crate::codec::{ItemCategory, Payload};
use crate::config::{IngestSettings, RateLimitSettings};
use crate::rate_limit::{self, CheckItem, Decision, RateLimiter};
use crate::wal::{Wal, WalError};

/// Decides whether `path` is an `Ingestion Route`.
///
/// Matches `/api/{project_id}/...` where `project_id` is a positive integer
/// (no leading zero), and the legacy `/api/store/` endpoint.
pub fn is_ingestion_route(path: &str) -> bool {
    let Some(rest) = path.strip_prefix("/api/") else {
        return false;
    };

    let is_numeric_project = rest
        .split_once('/')
        .is_some_and(|(segment, _)| is_positive_numeric_project_id(segment));

    is_numeric_project || rest == "store/"
}

fn is_positive_numeric_project_id(segment: &str) -> bool {
    !segment.is_empty()
        && !segment.starts_with('0')
        && segment.bytes().all(|byte| byte.is_ascii_digit())
}

/// Credentials and replay metadata captured from an ingestion request.
///
/// Serialized into the WAL record's `auth` field so asynchronous delivery
/// can reconstruct an equivalent request to `Upstream`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForwardedMetadata {
    /// Raw query string, without the leading `?`.
    pub query: String,
    /// Header name/value pairs forwarded with the payload.
    pub headers: Vec<(String, String)>,
}

impl ForwardedMetadata {
    /// Serializes into the WAL `auth` field: one `Q <query>` line followed
    /// by `H <name>: <value>` lines.
    pub fn encode(&self) -> String {
        let mut encoded = format!("Q {}\n", self.query);
        for (name, value) in &self.headers {
            encoded.push_str(&format!("H {name}: {value}\n"));
        }
        encoded
    }

    /// Parses a WAL `auth` field produced by [`encode`](Self::encode).
    pub fn decode(raw: &str) -> Self {
        let mut metadata = Self::default();
        for line in raw.lines() {
            if let Some(query) = line.strip_prefix("Q ") {
                metadata.query = query.to_string();
            } else if let Some((name, value)) = line
                .strip_prefix("H ")
                .and_then(|header| header.split_once(": "))
            {
                metadata.headers.push((name.to_string(), value.to_string()));
            }
        }
        metadata
    }

    /// The normalized `sentry_key` credential carried by this request: from
    /// the `X-Sentry-Auth` header when present, otherwise from the
    /// `sentry_key` query parameter. `None` marks requests without a
    /// recognizable credential.
    pub fn sentry_key(&self) -> Option<&str> {
        let auth_header = self
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-sentry-auth"))
            .map(|(_, value)| value.as_str());
        if let Some(value) = auth_header {
            return sentry_key_from_auth_header(value);
        }
        sentry_key_from_query(&self.query)
    }
}

/// Extracts `sentry_key` from an `X-Sentry-Auth` header value such as
/// `"Sentry sentry_key=abc, sentry_version=7"`. Parameter names match
/// case-insensitively; the value is returned verbatim.
fn sentry_key_from_auth_header(value: &str) -> Option<&str> {
    value
        .trim_start_matches("Sentry")
        .split(',')
        .map(str::trim)
        .find_map(|parameter| {
            let (name, value) = parameter.split_once('=')?;
            name.trim()
                .eq_ignore_ascii_case("sentry_key")
                .then_some(value.trim())
        })
}

/// Extracts the `sentry_key` query parameter value, if present.
fn sentry_key_from_query(query: &str) -> Option<&str> {
    query.split('&').find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.eq_ignore_ascii_case("sentry_key").then_some(value)
    })
}

/// Locks the `Ingestion Queue`, tolerating lock poisoning: the queue state
/// itself is always valid, so we prefer continuing over failing the request.
pub fn lock_wal(wal: &Mutex<Wal>) -> std::sync::MutexGuard<'_, Wal> {
    wal.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The outcome of handling an `Ingestion Route` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestionOutcome {
    /// The payload was durably appended to the `Ingestion Queue`; carries
    /// the envelope header's `event_id` when one was parsed and the
    /// `X-Sentry-Rate-Limits` value when any item was throttled.
    Accepted {
        event_id: Option<String>,
        rate_limits: Option<String>,
    },
    /// The payload exceeded a configured size limit.
    PayloadTooLarge,
    /// Every item was throttled; nothing was written to the queue.
    Throttled {
        /// Value for the `X-Sentry-Rate-Limits` response header.
        rate_limits: String,
        /// Value for the `Retry-After` response header (seconds).
        retry_after: u64,
    },
}

/// The result of evaluating `Rate Limit Rules` against a parsed payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateLimitDecision {
    /// Every item passed (or the payload failed to parse: fail open).
    Allowed,
    /// Some items were throttled; carries the rebuilt envelope bytes with
    /// only the allowed items.
    PartiallyDenied { body: Vec<u8>, rate_limits: String },
    /// Every item was throttled.
    FullyDenied {
        rate_limits: String,
        retry_after: u64,
    },
}

/// Accepts `Ingestion Route` payloads into the durable `Ingestion Queue`.
pub struct IngestionPipeline {
    wal: Arc<Mutex<Wal>>,
    settings: IngestSettings,
    rate_limiter: Arc<RateLimiter>,
    rate_limits: Arc<RateLimitSettings>,
}

impl IngestionPipeline {
    pub fn new(
        wal: Arc<Mutex<Wal>>,
        settings: IngestSettings,
        rate_limiter: Arc<RateLimiter>,
        rate_limits: Arc<RateLimitSettings>,
    ) -> Self {
        Self {
            wal,
            settings,
            rate_limiter,
            rate_limits,
        }
    }

    /// The HTTP status used to immediately acknowledge accepted ingestion
    /// requests.
    pub fn success_status_code(&self) -> u16 {
        self.settings.success_status_code
    }

    /// Upper bound on the compressed request body size.
    pub fn max_compressed_body_bytes(&self) -> u64 {
        self.settings.max_compressed_body_bytes
    }

    /// Upper bound on the decompressed request body size.
    pub fn max_decompressed_body_bytes(&self) -> u64 {
        self.settings.max_decompressed_body_bytes
    }

    /// Appends the payload to the `Ingestion Queue`.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when the record cannot be durably written.
    pub fn accept(&self, route: &str, auth: &str, body: &[u8]) -> Result<(), WalError> {
        let mut wal = lock_wal(&self.wal);
        wal.append(route, auth, body)
    }

    /// Evaluates every item of the payload against its `Rate Limit Rule`.
    ///
    /// `Payload::Unrecognized` fails open (no rate limiting). For partially
    /// denied envelopes the decision carries the rebuilt envelope bytes with
    /// only the allowed items.
    pub fn evaluate(&self, route: &str, payload: &Payload) -> RateLimitDecision {
        evaluate(&self.rate_limiter, &self.rate_limits, route, payload)
    }
}

/// Evaluates every item of the payload against its `Rate Limit Rule`.
pub fn evaluate(
    rate_limiter: &RateLimiter,
    settings: &RateLimitSettings,
    route: &str,
    payload: &Payload,
) -> RateLimitDecision {
    let project = project_id(route);
    let mut denials = Denials::default();
    let keep = match payload {
        Payload::Unrecognized => return RateLimitDecision::Allowed,
        Payload::StoreEvent { environment } => {
            check_item(
                rate_limiter,
                settings,
                project,
                environment,
                ItemCategory::Event,
                &mut denials,
            );
            Vec::new()
        }
        Payload::Envelope(envelope) => envelope
            .summaries()
            .into_iter()
            .map(|summary| {
                check_item(
                    rate_limiter,
                    settings,
                    project,
                    &summary.environment,
                    summary.category,
                    &mut denials,
                )
            })
            .collect::<Vec<bool>>(),
    };
    if denials.entries.is_empty() {
        return RateLimitDecision::Allowed;
    }
    let rate_limits = denials.header_value();
    let kept_envelope = match payload {
        Payload::Envelope(envelope) if keep.iter().any(|allowed| *allowed) => {
            envelope.filter_items(&keep)
        }
        _ => None,
    };
    match kept_envelope {
        Some(envelope) => RateLimitDecision::PartiallyDenied {
            body: envelope.to_bytes(),
            rate_limits,
        },
        None => RateLimitDecision::FullyDenied {
            rate_limits,
            retry_after: denials.max_retry_after,
        },
    }
}

/// Checks one item against its resolved `Rate Limit Rule`, recording it in
/// `denials` when throttled. Returns whether the item passed.
fn check_item(
    rate_limiter: &RateLimiter,
    settings: &RateLimitSettings,
    project: Option<u64>,
    environment: &str,
    category: ItemCategory,
    denials: &mut Denials,
) -> bool {
    let rule = rate_limit::resolve_rule(settings, project, environment, category);
    match rate_limiter.check(CheckItem {
        project_id: project,
        environment,
        category,
        rule,
    }) {
        Decision::Allowed => true,
        Decision::Throttled { retry_after_secs } => {
            denials.record(category, project, retry_after_secs);
            false
        }
    }
}

/// Removes the `content-encoding` header from encoded `ForwardedMetadata`
/// so the rebuilt envelope is not forwarded as still-compressed.
pub(crate) fn strip_content_encoding(auth: &str) -> String {
    let mut metadata = ForwardedMetadata::decode(auth);
    metadata
        .headers
        .retain(|(name, _)| !name.eq_ignore_ascii_case("content-encoding"));
    metadata.encode()
}

/// Collects throttled items into `X-Sentry-Rate-Limits` header entries.
#[derive(Default)]
struct Denials {
    /// `(category, project scope)` → the largest `retry_after` seen.
    entries: Vec<(ItemCategory, Option<u64>, u64)>,
    max_retry_after: u64,
}

impl Denials {
    fn record(&mut self, category: ItemCategory, project: Option<u64>, retry_after: u64) {
        self.max_retry_after = self.max_retry_after.max(retry_after);
        match self
            .entries
            .iter_mut()
            .find(|(entry_category, entry_project, _)| {
                *entry_category == category && *entry_project == project
            }) {
            Some(entry) => entry.2 = entry.2.max(retry_after),
            None => self.entries.push((category, project, retry_after)),
        }
    }

    /// Renders the `X-Sentry-Rate-Limits` header value:
    /// `<retry_after>:<category>:<project>` entries joined by `;`. The
    /// project segment is empty when no project id is known (global
    /// `/api/store/` route).
    fn header_value(&self) -> String {
        self.entries
            .iter()
            .map(|(category, project, retry_after)| {
                format!(
                    "{retry_after}:{}:{}",
                    category.as_str(),
                    project.map(|id| id.to_string()).unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

/// Extracts the numeric `project_id` from an `Ingestion Route` path.
///
/// `/api/store/` carries no project id so global defaults apply.
pub fn project_id(path: &str) -> Option<u64> {
    let segment = path
        .strip_prefix("/api/")
        .and_then(|rest| rest.split_once('/'))?
        .0;
    is_positive_numeric_project_id(segment)
        .then(|| segment.parse().ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RateLimitSettings, Rule, ScopeRules};
    use crate::wal::Clock;
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn matches_ingestion_route_paths() {
        assert!(is_ingestion_route("/api/42/envelope/"));
        assert!(is_ingestion_route("/api/7/store/"));
        assert!(is_ingestion_route("/api/store/"));
        assert!(is_ingestion_route("/api/1/minidump/"));
        assert!(is_ingestion_route("/api/123/security/"));
    }

    #[test]
    fn rejects_non_ingestion_paths() {
        assert!(!is_ingestion_route("/"));
        assert!(!is_ingestion_route("/_static/app.css"));
        assert!(!is_ingestion_route("/js-sdk/bundle.js"));
        assert!(!is_ingestion_route("/api/0/envelope/"));
        assert!(!is_ingestion_route("/api/07/envelope/"));
        assert!(!is_ingestion_route("/api/abc/envelope/"));
        assert!(!is_ingestion_route("/api/42"));
        assert!(!is_ingestion_route("/api/"));
        assert!(!is_ingestion_route("/api/store"));
        assert!(!is_ingestion_route("/api/store/extra"));
    }

    #[test]
    fn extracts_project_id_from_ingestion_routes() {
        assert_eq!(project_id("/api/42/envelope/"), Some(42));
        assert_eq!(project_id("/api/7/store/"), Some(7));
        assert_eq!(project_id("/api/123/minidump/"), Some(123));
        assert_eq!(
            project_id("/api/store/"),
            None,
            "global store has no project id"
        );
        assert_eq!(project_id("/api/07/envelope/"), None);
        assert_eq!(project_id("/_static/app.css"), None);
    }

    #[test]
    fn forwarded_metadata_round_trips_through_encode() {
        let metadata = ForwardedMetadata {
            query: "sentry_key=abc&sts=1".to_string(),
            headers: vec![
                (
                    "X-Sentry-Auth".to_string(),
                    "Sentry sentry_key=abc".to_string(),
                ),
                ("Content-Type".to_string(), "application/json".to_string()),
            ],
        };

        let decoded = ForwardedMetadata::decode(&metadata.encode());

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn decodes_empty_metadata() {
        assert_eq!(ForwardedMetadata::decode(""), ForwardedMetadata::default());
    }

    #[test]
    fn sentry_key_is_extracted_from_the_auth_header() {
        let metadata = ForwardedMetadata {
            query: String::new(),
            headers: vec![(
                "x-sentry-auth".into(),
                "Sentry sentry_key=abc123, sentry_version=7".into(),
            )],
        };

        assert_eq!(metadata.sentry_key(), Some("abc123"));
    }

    #[test]
    fn sentry_key_falls_back_to_the_query_parameter() {
        let metadata = ForwardedMetadata {
            query: "sentry_version=7&sentry_key=abc123".into(),
            headers: vec![],
        };

        assert_eq!(metadata.sentry_key(), Some("abc123"));
    }

    #[test]
    fn sentry_key_matching_is_case_insensitive_on_names_only() {
        // The auth header wins over the query; parameter order does not
        // matter and parameter names are matched case-insensitively, but the
        // key value itself is compared verbatim.
        let header_only = ForwardedMetadata {
            query: String::new(),
            headers: vec![(
                "x-sentry-auth".into(),
                "Sentry SENTRY_VERSION=7, sentry_key=Key-Value".into(),
            )],
        };
        assert_eq!(header_only.sentry_key(), Some("Key-Value"));

        let query_only = ForwardedMetadata {
            query: "SENTRY_KEY=query-key".into(),
            headers: vec![],
        };
        assert_eq!(query_only.sentry_key(), Some("query-key"));
    }

    #[test]
    fn sentry_key_is_none_without_credentials() {
        let neither = ForwardedMetadata {
            query: "sentry_version=7".into(),
            headers: vec![("content-type".into(), "application/json".into())],
        };
        assert_eq!(neither.sentry_key(), None);

        let auth_without_key = ForwardedMetadata {
            query: String::new(),
            headers: vec![("x-sentry-auth".into(), "Sentry sentry_version=7".into())],
        };
        assert_eq!(auth_without_key.sentry_key(), None);
    }

    struct MockClock(std::sync::Mutex<u64>);

    impl MockClock {
        fn new(now_ms: u64) -> Arc<Self> {
            Arc::new(Self(std::sync::Mutex::new(now_ms)))
        }
    }

    impl Clock for MockClock {
        fn now_unix_ms(&self) -> u64 {
            *self.0.lock().unwrap()
        }
    }

    /// Settings where the `event` category allows 2 items and the
    /// `transaction` category allows 1.
    fn burst_settings() -> RateLimitSettings {
        let rule = Rule {
            rate: 1.0,
            burst: 2,
        };
        RateLimitSettings {
            max_buckets: 100,
            bucket_ttl: Duration::from_secs(600),
            defaults: ScopeRules {
                event: Some(rule),
                transaction: Some(Rule {
                    rate: 1.0,
                    burst: 1,
                }),
                other: None,
                environments: HashMap::new(),
            },
            projects: HashMap::new(),
        }
    }

    fn limiter(clock: &Arc<MockClock>) -> RateLimiter {
        RateLimiter::new(clock.clone(), 100, Duration::from_secs(600))
    }

    #[test]
    fn denials_merge_by_category_and_scope() {
        let mut denials = Denials::default();
        denials.record(ItemCategory::Transaction, Some(42), 1);
        denials.record(ItemCategory::Event, None, 2);
        // Same category/scope keeps the max retry_after.
        denials.record(ItemCategory::Transaction, Some(42), 3);
        assert_eq!(denials.max_retry_after, 3);
        assert_eq!(denials.header_value(), "3:transaction:42;2:event:");
    }

    #[test]
    fn strip_content_encoding_removes_only_that_header() {
        let metadata = ForwardedMetadata {
            query: String::new(),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("content-encoding".into(), "gzip".into()),
                ("x-sentry-auth".into(), "Sentry sentry_key=abc".into()),
            ],
        };

        let stripped = ForwardedMetadata::decode(&strip_content_encoding(&metadata.encode()));

        assert_eq!(
            stripped.headers,
            vec![
                ("content-type".to_string(), "application/json".to_string()),
                (
                    "x-sentry-auth".to_string(),
                    "Sentry sentry_key=abc".to_string()
                ),
            ]
        );
    }

    #[test]
    fn unrecognized_payloads_fail_open() {
        let clock = MockClock::new(1_000);
        let decision = evaluate(
            &limiter(&clock),
            &burst_settings(),
            "/api/42/envelope/",
            &Payload::Unrecognized,
        );
        assert_eq!(decision, RateLimitDecision::Allowed);
    }

    #[test]
    fn store_event_fully_denied_reports_retry_after_and_scope() {
        let clock = MockClock::new(1_000);
        let settings = burst_settings();
        let limiter = limiter(&clock);
        let payload = Payload::parse_store_event(br#"{"environment":"production"}"#);

        let first = evaluate(&limiter, &settings, "/api/42/store/", &payload);
        assert_eq!(first, RateLimitDecision::Allowed);
        // Exhaust the burst-2 event bucket with two more store requests.
        evaluate(&limiter, &settings, "/api/42/store/", &payload);
        let denied = evaluate(&limiter, &settings, "/api/42/store/", &payload);

        assert_eq!(
            denied,
            RateLimitDecision::FullyDenied {
                rate_limits: "1:event:42".to_string(),
                retry_after: 1
            }
        );
    }

    #[test]
    fn partially_denied_envelope_rebuilds_allowed_items() {
        let clock = MockClock::new(1_000);
        let settings = burst_settings();
        let limiter = limiter(&clock);
        // One event and two transactions: the second transaction exceeds the
        // burst-1 transaction bucket while the event passes.
        let mut body = b"{}\n".to_vec();
        body.extend_from_slice(b"{\"type\":\"event\",\"length\":2}\n{}\n");
        body.extend_from_slice(b"{\"type\":\"transaction\",\"length\":2}\n{}\n");
        body.extend_from_slice(b"{\"type\":\"transaction\",\"length\":2}\n{}\n");
        let payload = Payload::parse_envelope(&body);

        let decision = evaluate(&limiter, &settings, "/api/42/envelope/", &payload);

        let RateLimitDecision::PartiallyDenied { body, rate_limits } = decision else {
            panic!("expected a partial denial, got {decision:?}")
        };
        assert_eq!(rate_limits, "1:transaction:42");
        // The rebuilt envelope keeps the header line, the event item and the
        // first transaction item, dropping the throttled second transaction.
        let expected = b"{}\n{\"type\":\"event\",\"length\":2}\n{}\n{\"type\":\"transaction\",\"length\":2}\n{}\n";
        assert_eq!(body, expected);
    }
}
