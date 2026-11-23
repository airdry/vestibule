//! Local ingestion pipeline for `Ingestion Route` requests.
//!
//! Matching requests are appended to the `Ingestion Queue` (segmented disk
//! WAL) and acknowledged immediately, without any synchronous hop to
//! `Upstream` (per ADR-0002).

use std::sync::{Arc, Mutex};

use crate::config::IngestSettings;
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
}

/// The outcome of handling an `Ingestion Route` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionOutcome {
    /// The payload was durably appended to the `Ingestion Queue`.
    Accepted,
    /// The payload exceeded `ingest.max_compressed_body_bytes`.
    PayloadTooLarge,
}

/// Accepts `Ingestion Route` payloads into the durable `Ingestion Queue`.
pub struct IngestionPipeline {
    wal: Arc<Mutex<Wal>>,
    settings: IngestSettings,
}

impl IngestionPipeline {
    pub fn new(wal: Arc<Mutex<Wal>>, settings: IngestSettings) -> Self {
        Self { wal, settings }
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

    /// Appends the payload to the `Ingestion Queue`.
    ///
    /// # Errors
    ///
    /// Returns a [`WalError`] when the record cannot be durably written.
    pub fn accept(&self, route: &str, auth: &str, body: &[u8]) -> Result<(), WalError> {
        let mut wal = self.wal.lock().expect("lock ingestion queue");
        wal.append(route, auth, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
