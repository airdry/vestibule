//! Asynchronous delivery of `Ingestion Queue` records to the `Upstream`.
//!
//! The delivery worker forwards queued records one at a time, committing the
//! consumer checkpoint on `2xx` (and on unrecoverable `4xx`), retrying
//! transient failures with exponential backoff, and pausing on `429` for the
//! requested `Retry-After` duration (ADR-0002).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use reqwest::StatusCode;

use crate::config::UpstreamTarget;
use crate::ingestion::{ForwardedMetadata, lock_wal};
use crate::wal::{PendingRecord, Wal};

/// Tuning knobs of the delivery retry strategy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Backoff after the first retryable failure; doubles up to `max_backoff`.
    pub initial_backoff: Duration,
    /// Upper bound of the exponential backoff.
    pub max_backoff: Duration,
    /// Wait between polls while the queue is empty.
    pub empty_poll_interval: Duration,
    /// Upper bound for `Retry-After` pauses.
    pub retry_after_cap: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            empty_poll_interval: Duration::from_millis(100),
            retry_after_cap: Duration::from_secs(300),
        }
    }
}

/// What the worker should do after one pass over the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// The queue is empty.
    Idle,
    /// The head record was fully handled (delivered or dropped).
    Committed,
    /// The head record must be retried after a backoff.
    Retry,
    /// The `Upstream` requested a pause (`429` + `Retry-After`).
    Throttled(Duration),
}

/// How a single delivery attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// `2xx`, or an unrecoverable `4xx` whose record must be dropped.
    Commit,
    /// Transient failure (5xx, network error): try again later.
    Retry,
    /// `429 Throttled`: pause for the given duration.
    Throttle(Duration),
}

/// Forwards `Ingestion Queue` records to the `Upstream`.
pub struct DeliveryWorker {
    wal: Arc<Mutex<Wal>>,
    client: reqwest::Client,
    upstream: UpstreamTarget,
    policy: RetryPolicy,
}

impl DeliveryWorker {
    /// Builds the worker with a pooled HTTP client honoring the configured
    /// `Upstream` timeout.
    ///
    /// # Errors
    ///
    /// Returns the setup error when the HTTP client cannot be constructed.
    pub fn new(
        wal: Arc<Mutex<Wal>>,
        upstream: UpstreamTarget,
        policy: RetryPolicy,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(upstream.timeout)
            .build()
            .map_err(|error| format!("failed to build the delivery client: {error}"))?;
        Ok(Self {
            wal,
            client,
            upstream,
            policy,
        })
    }

    /// Processes queue records until shutdown is requested.
    pub async fn run(&self, mut shutdown: ShutdownWatch) {
        let mut backoff = self.policy.initial_backoff;
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                outcome = self.tick() => match outcome {
                    Outcome::Idle => {
                        backoff = self.policy.initial_backoff;
                        tokio::select! {
                            _ = shutdown.changed() => return,
                            _ = tokio::time::sleep(self.policy.empty_poll_interval) => {}
                        }
                    }
                    Outcome::Committed => {
                        backoff = self.policy.initial_backoff;
                    }
                    Outcome::Retry => {
                        tokio::select! {
                            _ = shutdown.changed() => return,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(self.policy.max_backoff);
                    }
                    Outcome::Throttled(pause) => {
                        tokio::select! {
                            _ = shutdown.changed() => return,
                            _ = tokio::time::sleep(pause) => {}
                        }
                    }
                },
            }
        }
    }

    /// Performs one delivery attempt over the head record, if any.
    async fn tick(&self) -> Outcome {
        let next = { lock_wal(&self.wal).next_record() };
        let pending = match next {
            Ok(pending) => pending,
            Err(error) => {
                eprintln!("delivery: failed to read the ingestion queue: {error}");
                return Outcome::Retry;
            }
        };
        let Some(pending) = pending else {
            return Outcome::Idle;
        };

        match self.deliver(&pending).await {
            Decision::Commit => {
                let committed = lock_wal(&self.wal).commit(pending.position);
                if let Err(error) = committed {
                    eprintln!("delivery: failed to commit the queue checkpoint: {error}");
                    return Outcome::Retry;
                }
                Outcome::Committed
            }
            Decision::Retry => Outcome::Retry,
            Decision::Throttle(pause) => Outcome::Throttled(pause),
        }
    }

    /// Sends a single record to the `Upstream` and classifies the outcome.
    async fn deliver(&self, pending: &PendingRecord) -> Decision {
        let metadata = ForwardedMetadata::decode(&pending.record.auth);
        let url = self.target_url(&pending.record.route, &metadata);
        let mut request = self.client.post(&url).body(pending.record.body.clone());
        for (name, value) in &metadata.headers {
            request = request.header(name, value);
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                eprintln!("delivery: request to Upstream failed: {error}");
                return Decision::Retry;
            }
        };
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok());
        classify_response(
            response.status(),
            retry_after,
            self.policy.initial_backoff,
            self.policy.retry_after_cap,
        )
    }

    /// Builds the absolute URL a record is forwarded to.
    fn target_url(&self, route: &str, metadata: &ForwardedMetadata) -> String {
        let scheme = if self.upstream.tls { "https" } else { "http" };
        let mut target = format!("{scheme}://{}", self.upstream.host);
        if !is_default_port(self.upstream.tls, self.upstream.port) {
            target.push_str(&format!(":{}", self.upstream.port));
        }
        target.push_str(&self.upstream.path_prefix);
        target.push_str(route);
        if !metadata.query.is_empty() {
            target.push('?');
            target.push_str(&metadata.query);
        }
        target
    }
}

/// Maps an `Upstream` response to a delivery decision.
fn classify_response(
    status: StatusCode,
    retry_after: Option<&str>,
    default_throttle: Duration,
    cap: Duration,
) -> Decision {
    match status.as_u16() {
        200..=299 => Decision::Commit,
        429 => {
            let pause = parse_retry_after(retry_after, cap).unwrap_or(default_throttle);
            Decision::Throttle(pause)
        }
        400..=499 => {
            eprintln!("delivery: dropping record after unrecoverable {status} response");
            Decision::Commit
        }
        _ => Decision::Retry,
    }
}

/// Parses the `Retry-After` header (seconds form), capped at `cap`.
fn parse_retry_after(value: Option<&str>, cap: Duration) -> Option<Duration> {
    let seconds = value?.trim().parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds).min(cap))
}

fn is_default_port(tls: bool, port: u16) -> bool {
    if tls { port == 443 } else { port == 80 }
}

#[async_trait]
impl BackgroundService for DeliveryWorker {
    async fn start(&self, shutdown: ShutdownWatch) {
        self.run(shutdown).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_response_statuses() {
        use Decision::{Commit, Retry, Throttle};

        let default_backoff = Duration::from_secs(1);
        let cap = Duration::from_secs(300);
        let classify = |status: u16, retry_after: Option<&str>| {
            classify_response(
                StatusCode::from_u16(status).expect("test status codes are valid"),
                retry_after,
                default_backoff,
                cap,
            )
        };

        assert_eq!(classify(200, None), Commit);
        assert_eq!(classify(201, None), Commit);
        assert_eq!(classify(400, None), Commit);
        assert_eq!(classify(401, None), Commit);
        assert_eq!(classify(500, None), Retry);
        assert_eq!(classify(503, None), Retry);
        assert_eq!(classify(429, Some("2")), Throttle(Duration::from_secs(2)));
        // Without Retry-After the default throttle applies.
        assert_eq!(classify(429, None), Throttle(default_backoff));
    }

    #[test]
    fn parses_and_caps_retry_after_seconds() {
        let cap = Duration::from_secs(300);
        assert_eq!(
            parse_retry_after(Some("30"), cap),
            Some(Duration::from_secs(30))
        );
        assert_eq!(parse_retry_after(None, cap), None);
        assert_eq!(parse_retry_after(Some("tomorrow"), cap), None);
        // Absurd values are capped.
        assert_eq!(
            parse_retry_after(Some("9999999"), cap),
            Some(Duration::from_secs(300))
        );
    }
}
