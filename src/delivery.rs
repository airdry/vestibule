//! Asynchronous batched delivery of `Ingestion Queue` records to the
//! `Upstream`.
//!
//! A dispatcher claims records from the queue and groups compatible
//! `/envelope/` records by `(project route, credentials)`. Groups are
//! flushed into outbound requests at `max_items`, `max_bytes` or
//! `flush_interval`. A pool of delivery workers sends each sealed batch to
//! the `Upstream`, retrying transient failures with exponential backoff and
//! pausing on `429` for the requested `Retry-After` duration (ADR-0002).
//! A shared token-bucket drain throttle bounds the outbound request rate;
//! the worker count bounds concurrency. Record completions flow back to the
//! dispatcher, which advances the durable checkpoint only over contiguous
//! prefixes so out-of-order retries never skip pending records.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use flate2::write::GzEncoder;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use reqwest::StatusCode;
use tokio::sync::mpsc;

use crate::codec::{Envelope, Payload};
use crate::config::{BatchSettings, UpstreamTarget};
use crate::ingestion::{ForwardedMetadata, lock_wal};
use crate::wal::{PendingRecord, Position, Wal};

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

/// How a single delivery attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// `2xx`, or an unrecoverable `4xx` whose records must be dropped.
    Commit,
    /// Transient failure (5xx, network error): try again later.
    Retry,
    /// `429 Throttled`: pause for the given duration.
    Throttle(Duration),
}

/// Forwards `Ingestion Queue` records to the `Upstream` through a bounded
/// worker pool with a shared drain-rate throttle.
pub struct DeliveryPool {
    wal: Arc<Mutex<Wal>>,
    client: reqwest::Client,
    upstream: UpstreamTarget,
    policy: RetryPolicy,
    settings: BatchSettings,
}

impl DeliveryPool {
    /// Builds the pool with a shared HTTP client honoring the configured
    /// `Upstream` timeout.
    ///
    /// # Errors
    ///
    /// Returns the setup error when the HTTP client cannot be constructed.
    pub fn new(
        wal: Arc<Mutex<Wal>>,
        upstream: UpstreamTarget,
        policy: RetryPolicy,
        settings: BatchSettings,
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
            settings,
        })
    }

    /// Drains the queue until shutdown is requested, then flushes open
    /// batches and returns.
    pub async fn run(&self, mut shutdown: ShutdownWatch) {
        let workers = self.settings.workers.max(1);
        let (jobs_tx, jobs_rx) = mpsc::channel::<Job>(workers * 4);
        let (events_tx, mut events_rx) = mpsc::channel::<Event>(workers * 4);
        let throttle = Arc::new(DrainThrottle::new(self.settings.max_requests_per_sec));
        let jobs_rx = Arc::new(tokio::sync::Mutex::new(jobs_rx));

        for _ in 0..workers {
            tokio::spawn(run_worker(
                Arc::clone(&jobs_rx),
                events_tx.clone(),
                self.client.clone(),
                self.upstream.clone(),
                self.policy.clone(),
                Arc::clone(&throttle),
                shutdown.clone(),
            ));
        }

        let mut dispatcher = Dispatcher {
            settings: &self.settings,
            ledger: Ledger::default(),
            groups: HashMap::new(),
            seq: 0,
            jobs_tx,
            wal: Arc::clone(&self.wal),
        };
        let mut interval = tokio::time::interval(self.settings.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            dispatcher.claim_records();
            tokio::select! {
                _ = shutdown.changed() => {
                    dispatcher.seal_all();
                    return;
                }
                event = events_rx.recv() => match event {
                    Some(event) => dispatcher.on_event(event),
                    None => return,
                },
                _ = interval.tick() => {
                    dispatcher.seal_all();
                }
            }
        }
    }
}

#[async_trait]
impl BackgroundService for DeliveryPool {
    async fn start(&self, shutdown: ShutdownWatch) {
        self.run(shutdown).await;
    }
}

/// One sealed outbound unit delivered by a worker as a single HTTP request.
struct Job {
    ids: Vec<usize>,
    request: OutboundRequest,
}

#[derive(Debug)]
struct OutboundRequest {
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// Completion report flowing from workers back to the dispatcher.
#[derive(Debug)]
enum Event {
    Done(Vec<usize>),
}

/// The dispatcher owns claim/group/ledger state for one `run` invocation.
struct Dispatcher<'a> {
    wal: Arc<Mutex<Wal>>,
    settings: &'a BatchSettings,
    ledger: Ledger,
    groups: HashMap<GroupKey, Vec<Claim>>,
    seq: usize,
    jobs_tx: mpsc::Sender<Job>,
}

/// A record claimed from the queue and pending delivery.
struct Claim {
    seq: usize,
    position: Position,
    record: QueuedRecord,
}

/// The grouping key of compatible envelope records: identical route
/// (project) and credentials.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    route: String,
    sentry_key: String,
}

/// A queued record split into its raw parts and its parsed envelope form.
struct QueuedRecord {
    route: String,
    metadata: ForwardedMetadata,
    body: Vec<u8>,
    envelope: Option<Envelope>,
}

impl Dispatcher<'_> {
    /// Claims records from the queue while the job channel has capacity.
    fn claim_records(&mut self) {
        while self.jobs_tx.capacity() > 0 {
            let pending = { lock_wal(&self.wal).next_record() };
            let pending = match pending {
                Ok(Some(pending)) => pending,
                Ok(None) => return,
                Err(error) => {
                    eprintln!("delivery: failed to read the ingestion queue: {error}");
                    return;
                }
            };
            let claim = self.register(pending);
            self.add(claim);
        }
    }

    fn register(&mut self, pending: PendingRecord) -> Claim {
        let claim = Claim {
            seq: self.seq,
            position: pending.position,
            record: QueuedRecord {
                route: pending.record.route.clone(),
                metadata: ForwardedMetadata::decode(&pending.record.auth),
                envelope: parse_envelope(&pending.record.route, &pending.record.body),
                body: pending.record.body,
            },
        };
        self.seq += 1;
        self.ledger.register(claim.seq, claim.position);
        claim
    }

    /// Routes a claim into a merge group or straight into an individual job.
    fn add(&mut self, claim: Claim) {
        if claim.record.envelope.is_some() {
            let key = GroupKey {
                route: claim.record.route.clone(),
                sentry_key: claim
                    .record
                    .metadata
                    .sentry_key()
                    .unwrap_or_default()
                    .to_string(),
            };
            let group = self.groups.entry(key.clone()).or_default();
            group.push(claim);
            let items = group.len();
            let bytes: usize = group.iter().map(|claim| claim.record.body.len()).sum();
            if items >= self.settings.max_items || bytes as u64 >= self.settings.max_bytes {
                self.seal(&key);
            }
        } else {
            let job = build_singleton_job(claim);
            self.send(job);
        }
    }

    /// Seals every open group.
    fn seal_all(&mut self) {
        let keys: Vec<GroupKey> = self.groups.keys().cloned().collect();
        for key in keys {
            self.seal(&key);
        }
    }

    fn seal(&mut self, key: &GroupKey) {
        if let Some(claims) = self.groups.remove(key) {
            for job in build_merged_jobs(key.route.clone(), claims) {
                self.send(job);
            }
        }
    }

    fn send(&self, job: Job) {
        if let Err(error) = self.jobs_tx.try_send(job) {
            eprintln!("delivery: failed to enqueue a batch: {error}");
        }
    }

    fn on_event(&mut self, event: Event) {
        match event {
            Event::Done(ids) => {
                if let Some(position) = self.ledger.apply(&ids)
                    && let Err(error) = lock_wal(&self.wal).commit(position)
                {
                    eprintln!("delivery: failed to commit the queue checkpoint: {error}");
                }
            }
        }
    }
}

/// Parses an envelope-route record body; `None` for non-envelope routes or
/// records the codec cannot parse (they are delivered individually).
fn parse_envelope(route: &str, body: &[u8]) -> Option<Envelope> {
    if !route.ends_with("/envelope/") {
        return None;
    }
    match Payload::parse_envelope(body) {
        Payload::Envelope(envelope) => Some(envelope),
        _ => None,
    }
}

/// Builds a single-record job that is forwarded verbatim.
fn build_singleton_job(claim: Claim) -> Job {
    let Claim {
        seq,
        position: _,
        record:
            QueuedRecord {
                route,
                metadata,
                body,
                ..
            },
    } = claim;
    Job {
        ids: vec![seq],
        request: OutboundRequest {
            url: target_url(&route, &metadata),
            headers: metadata.headers.clone(),
            body,
        },
    }
}

/// Builds jobs for one sealed group: a merged, gzip-compressed job for
/// compatible envelope claims, plus verbatim singleton jobs for any claim
/// without a parseable envelope.
fn build_merged_jobs(route: String, claims: Vec<Claim>) -> Vec<Job> {
    // A single-record batch is forwarded verbatim: nothing to merge.
    if claims.len() == 1 {
        let claim = claims
            .into_iter()
            .next()
            .unwrap_or_else(|| unreachable!("claims.len() == 1 was checked"));
        return vec![build_singleton_job(claim)];
    }
    let mut envelopes: Vec<Envelope> = Vec::with_capacity(claims.len());
    let mut ids = Vec::with_capacity(claims.len());
    let mut metadata: Option<ForwardedMetadata> = None;
    let mut fallbacks = Vec::new();
    for claim in claims {
        match claim.record.envelope {
            Some(envelope) => {
                if metadata.is_none() {
                    metadata = Some(claim.record.metadata.clone());
                }
                ids.push(claim.seq);
                envelopes.push(envelope);
            }
            // Defensive fallback: a non-envelope record cannot join a merge
            // group, so it is delivered verbatim rather than dropped.
            None => fallbacks.push(build_singleton_job(claim)),
        }
    }
    let mut jobs = fallbacks;
    if !envelopes.is_empty() {
        let metadata = metadata.unwrap_or_default();
        let body = gzip(Envelope::merge(&envelopes).to_bytes());
        jobs.push(Job {
            ids,
            request: OutboundRequest {
                url: target_url(&route, &metadata),
                headers: merged_headers(&metadata),
                body,
            },
        });
    }
    jobs
}

/// Headers for a merged request: credentials and framing headers, no
/// hop-by-hop content framing from the original requests.
fn merged_headers(metadata: &ForwardedMetadata) -> Vec<(String, String)> {
    let mut headers = vec![
        (
            "Content-Type".to_string(),
            "application/x-sentry-envelope".to_string(),
        ),
        ("Content-Encoding".to_string(), "gzip".to_string()),
    ];
    for (name, value) in &metadata.headers {
        let is_framing = name.eq_ignore_ascii_case("content-type")
            || name.eq_ignore_ascii_case("content-encoding")
            || name.eq_ignore_ascii_case("content-length");
        if !is_framing && !name.eq_ignore_ascii_case("host") {
            headers.push((name.clone(), value.clone()));
        }
    }
    headers
}

/// Gzips `bytes`; returns the input unchanged when compression fails.
fn gzip(bytes: Vec<u8>) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
    if encoder.write_all(&bytes).is_err() {
        return bytes;
    }
    encoder.finish().unwrap_or(bytes)
}

/// Builds the URL path (with query) a record is forwarded to.
fn target_url(route: &str, metadata: &ForwardedMetadata) -> String {
    let mut target = route.to_string();
    if !metadata.query.is_empty() {
        target.push('?');
        target.push_str(&metadata.query);
    }
    target
}

async fn run_worker(
    jobs_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Job>>>,
    events_tx: mpsc::Sender<Event>,
    client: reqwest::Client,
    target: UpstreamTarget,
    policy: RetryPolicy,
    throttle: Arc<DrainThrottle>,
    mut shutdown: ShutdownWatch,
) {
    while let Some(job) = { jobs_rx.lock().await.recv().await } {
        let mut backoff = policy.initial_backoff;
        loop {
            throttle.acquire().await;
            match send(&client, &target, &job.request, &policy).await {
                Decision::Commit => {
                    let _ = events_tx.send(Event::Done(job.ids.clone())).await;
                    break;
                }
                Decision::Retry => {
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(policy.max_backoff);
                }
                Decision::Throttle(pause) => {
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        _ = tokio::time::sleep(pause) => {}
                    }
                }
            }
        }
    }
}

/// Performs one delivery attempt and classifies the outcome.
async fn send(
    client: &reqwest::Client,
    target: &UpstreamTarget,
    request: &OutboundRequest,
    policy: &RetryPolicy,
) -> Decision {
    let url = format!(
        "{}://{}{}{}",
        if target.tls { "https" } else { "http" },
        target.host,
        if is_default_port(target.tls, target.port) {
            String::new()
        } else {
            format!(":{}", target.port)
        },
        request.url
    );
    let mut http = client.post(&url);
    for (name, value) in &request.headers {
        http = http.header(name, value);
    }
    let response = match http.body(request.body.clone()).send().await {
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
        policy.initial_backoff,
        policy.retry_after_cap,
    )
}

/// Contiguous-prefix completion ledger: records complete out of order, but
/// the durable checkpoint only advances over fully completed prefixes.
#[derive(Default)]
struct Ledger {
    positions: Vec<Option<Position>>,
    done: std::collections::HashSet<usize>,
    next: usize,
}

impl Ledger {
    fn register(&mut self, seq: usize, position: Position) {
        while self.positions.len() <= seq {
            self.positions.push(None);
        }
        self.positions[seq] = Some(position);
    }

    /// Marks `ids` complete and returns the next checkpoint to commit: the
    /// position of the highest record in the newly completed contiguous
    /// prefix, if the prefix advanced.
    fn apply(&mut self, ids: &[usize]) -> Option<Position> {
        for id in ids {
            self.done.insert(*id);
        }
        let mut last = None;
        while self.next < self.positions.len() && self.done.contains(&self.next) {
            last = self.positions[self.next];
            self.next += 1;
        }
        last
    }
}

/// Token-bucket throttle bounding the outbound request rate.
struct DrainThrottle {
    rate: f64,
    state: Mutex<ThrottleState>,
}

struct ThrottleState {
    tokens: f64,
    last: tokio::time::Instant,
}

impl DrainThrottle {
    fn new(rate: f64) -> Self {
        Self {
            rate,
            state: Mutex::new(ThrottleState {
                tokens: rate,
                last: tokio::time::Instant::now(),
            }),
        }
    }

    async fn acquire(&self) {
        loop {
            let wait = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let now = tokio::time::Instant::now();
                let elapsed = now.duration_since(state.last).as_secs_f64();
                state.tokens = (state.tokens + elapsed * self.rate).min(self.rate);
                state.last = now;
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return;
                }
                (1.0 - state.tokens) / self.rate
            };
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
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

    #[test]
    fn ledger_commits_only_contiguous_prefixes() {
        let mut ledger = Ledger::default();
        let positions = [
            (
                0,
                Position {
                    segment_id: 1,
                    offset: 10,
                },
            ),
            (
                1,
                Position {
                    segment_id: 1,
                    offset: 20,
                },
            ),
            (
                2,
                Position {
                    segment_id: 1,
                    offset: 30,
                },
            ),
        ];
        for (seq, position) in positions {
            ledger.register(seq, position);
        }

        // Completing seq 1 and 2 first must NOT advance anything: seq 0 is
        // still outstanding.
        assert!(ledger.apply(&[1, 2]).is_none());

        // Completing seq 0 releases the full 0..2 prefix.
        let commit = ledger.apply(&[0]);
        assert_eq!(
            commit,
            Some(Position {
                segment_id: 1,
                offset: 30
            }),
            "the prefix commits at the highest contiguous position"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drain_throttle_bounds_the_request_rate() {
        let throttle = DrainThrottle::new(10.0);
        let start = tokio::time::Instant::now();
        for _ in 0..12 {
            throttle.acquire().await;
        }
        // A full bucket of 10 tokens is consumed instantly; the next two
        // acquires must wait for the refill.
        assert!(start.elapsed() >= Duration::from_millis(190));
    }
}
