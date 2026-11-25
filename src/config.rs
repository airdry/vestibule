//! Startup YAML configuration: parsing and validation.
//!
//! The configuration surface grows ticket by ticket; this module owns the
//! schema, validation rules, and the clear startup error messages required
//! by the specification (`server` and `upstream` are validated here).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use serde::{Deserialize, Deserializer};
use url::Url;

/// Lock-free shared snapshot store of the hot-reloadable
/// [`RuntimeSettings`]: hot-path handlers load a consistent snapshot with
/// one atomic read while reloads swap in a new one.
pub type SharedSettings = Arc<ArcSwap<RuntimeSettings>>;

/// Builds the shared settings store from the startup configuration.
pub fn shared_settings(config: &Config) -> SharedSettings {
    Arc::new(ArcSwap::from(Arc::new(config.runtime_settings())))
}

/// Structured log verbosity of the service (hot-reloadable).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Parses a log level name (case-insensitive).
    ///
    /// # Errors
    ///
    /// Returns `Err` naming the offending value for unknown levels.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err(format!(
                "unknown log level '{raw}' (expected one of trace, debug, info, warn, error)"
            )),
        }
    }
}

/// Observability settings applied to logging and metrics (hot-reloadable).
#[derive(Debug, Clone)]
pub struct ObservabilitySettings {
    pub log_level: LogLevel,
    /// Environments kept as their own `environment` metric label; every
    /// unlisted value maps to `"other"` to bound Prometheus cardinality.
    pub environment_label_allowlist: Vec<String>,
}

/// Errors surfaced when loading or validating the startup configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid config {path}: {message}")]
    Invalid { path: String, message: String },
}

/// The validated startup configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerSettings,
    pub upstream: UpstreamTarget,
    pub ingest: IngestSettings,
    pub queue: QueueSettings,
    pub rate_limits: RateLimitSettings,
    pub batch: BatchSettings,
    pub observability: ObservabilitySettings,
}

/// The hot-reloadable slice of the [`Config`] captured as one atomic
/// snapshot.
///
/// Startup-only fields (the `server` listen addresses and `queue.dir`)
/// deliberately stay out: changing them after startup cannot take effect
/// and is only warned about during reload.
#[derive(Debug, Clone)]
pub struct RuntimeSettings {
    pub ingest: IngestSettings,
    /// `queue.max_age`: the record TTL applied during delivery.
    pub queue_max_age: Duration,
    pub batch: BatchSettings,
    pub rate_limits: RateLimitSettings,
    /// `upstream.healthcheck`: active probe settings.
    pub healthcheck: HealthcheckSettings,
    pub observability: ObservabilitySettings,
}

/// Outbound batched delivery settings of the delivery worker pool.
#[derive(Debug, Clone)]
pub struct BatchSettings {
    /// Maximum number of queued envelope records merged into one outbound
    /// request.
    pub max_items: usize,
    /// Upper bound on the accumulated (raw) body bytes of one merged
    /// outbound request.
    pub max_bytes: u64,
    /// Maximum time an incomplete batch waits for more records before
    /// being flushed.
    pub flush_interval: Duration,
    /// Number of concurrent delivery workers.
    pub workers: usize,
    /// Drain throttle: upper bound on outbound requests per second
    /// towards the `Upstream`.
    pub max_requests_per_sec: f64,
}

/// Ingestion pipeline settings applied to `Ingestion Route` requests.
#[derive(Debug, Clone)]
pub struct IngestSettings {
    /// HTTP status used to immediately acknowledge accepted ingestion
    /// requests (`201` by default, configurable to `200`).
    pub success_status_code: u16,
    /// Upper bound on the compressed request body size; larger bodies are
    /// rejected with `413 Payload Too Large` before touching the
    /// `Ingestion Queue`.
    pub max_compressed_body_bytes: u64,
    /// Upper bound on the decompressed request body size.
    pub max_decompressed_body_bytes: u64,
}

/// `Ingestion Queue` (segmented disk WAL) settings.
#[derive(Debug, Clone)]
pub struct QueueSettings {
    /// Directory holding the WAL segments and the consumer checkpoint.
    pub dir: PathBuf,
    /// Total WAL size budget enforced via the overflow policy.
    pub max_bytes: u64,
    /// Size at which the active segment is rotated into a closed one.
    pub segment_size_bytes: u64,
    /// Records older than this TTL are skipped during delivery.
    pub max_age: Duration,
    /// Behavior when the queue exceeds `max_bytes`.
    pub overflow_policy: OverflowPolicy,
    /// Durability strategy for WAL appends.
    pub fsync_mode: FsyncMode,
    /// fsync period for `FsyncMode::Periodic`.
    pub fsync_interval: Duration,
}

/// What to do when the `Ingestion Queue` exceeds `max_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Delete the oldest whole closed segment(s), preserving the freshest
    /// telemetry.
    DropOldest,
}

/// Durability strategy for WAL appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// fsync on a fixed interval (default).
    Periodic,
    /// Rely on the OS page cache only.
    Buffered,
    /// fsync after every appended record.
    EveryWrite,
}

const DEFAULT_QUEUE_DIR: &str = "queue";
const DEFAULT_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_COMPRESSED_BODY_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_MAX_DECOMPRESSED_BODY_BYTES: u64 = 50 * 1024 * 1024;
const DEFAULT_QUEUE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_FSYNC_INTERVAL: Duration = Duration::from_millis(200);
const DEFAULT_MAX_BUCKETS: usize = 10_000;
const DEFAULT_BUCKET_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_BATCH_MAX_ITEMS: usize = 100;
const DEFAULT_BATCH_MAX_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_BATCH_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_BATCH_WORKERS: usize = 4;
const DEFAULT_BATCH_MAX_REQUESTS_PER_SEC: f64 = 100.0;
const DEFAULT_HEALTHCHECK_PATH: &str = "/";
const DEFAULT_HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_HEALTHCHECK_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_HEALTHCHECK_UNHEALTHY_THRESHOLD: u32 = 3;
const DEFAULT_HEALTHCHECK_HEALTHY_THRESHOLD: u32 = 2;
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Active probe settings driving `Upstream Health` transitions.
#[derive(Debug, Clone)]
pub struct HealthcheckSettings {
    /// Path appended to the `Upstream` base URL for active probes.
    pub path: String,
    /// Probe response statuses counted as healthy.
    pub expected_statuses: Vec<u16>,
    /// Period between active probes.
    pub interval: Duration,
    /// Timeout of one active probe request.
    pub timeout: Duration,
    /// Consecutive failures (active or passive) that flip `Upstream
    /// Health` to `Unhealthy`.
    pub unhealthy_threshold: u32,
    /// Consecutive probe successes that restore `Upstream Health` to
    /// `Healthy`.
    pub healthy_threshold: u32,
}

impl Default for HealthcheckSettings {
    fn default() -> Self {
        Self {
            path: DEFAULT_HEALTHCHECK_PATH.to_string(),
            expected_statuses: vec![200],
            interval: DEFAULT_HEALTHCHECK_INTERVAL,
            timeout: DEFAULT_HEALTHCHECK_TIMEOUT,
            unhealthy_threshold: DEFAULT_HEALTHCHECK_UNHEALTHY_THRESHOLD,
            healthy_threshold: DEFAULT_HEALTHCHECK_HEALTHY_THRESHOLD,
        }
    }
}

/// `Rate Limit Rule` settings: hierarchical rules plus bucket table bounds.
#[derive(Debug, Clone, Default)]
pub struct RateLimitSettings {
    /// Maximum number of concurrently tracked buckets in the LRU/TTL table.
    pub max_buckets: usize,
    /// Buckets idle for longer than this TTL may be evicted.
    pub bucket_ttl: Duration,
    /// Rules applied when no project-specific rule matches.
    pub defaults: ScopeRules,
    /// Rules keyed by numeric `project_id`.
    pub projects: HashMap<u64, ScopeRules>,
}

/// `Rate Limit Rule`s for one scope (global defaults or a project).
#[derive(Debug, Clone, Default)]
pub struct ScopeRules {
    pub event: Option<Rule>,
    pub transaction: Option<Rule>,
    pub other: Option<Rule>,
    pub environments: HashMap<String, ScopeRules>,
}

/// A single `Rate Limit Rule`: refill `rate` in RPS with `burst` capacity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rule {
    /// Refill rate in requests per second.
    pub rate: f64,
    /// Bucket capacity, i.e. the maximum burst of items accepted at once.
    pub burst: u32,
}

/// Listener addresses of the two planes inside one process.
#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub data_plane_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    /// Graceful-shutdown deadline for in-flight work on `SIGTERM`.
    pub shutdown_timeout: Duration,
}

/// Where `Pass-Through Traffic` is forwarded.
#[derive(Debug, Clone)]
pub struct UpstreamTarget {
    /// `true` when the `Upstream` URL scheme is `https`.
    pub tls: bool,
    pub host: String,
    pub port: u16,
    /// Path prefix from the `Upstream` URL; empty when absent.
    pub path_prefix: String,
    /// Per-request delivery timeout towards the `Upstream`.
    pub timeout: Duration,
    /// Active probe settings for `Upstream Health` tracking.
    pub healthcheck: HealthcheckSettings,
}

/// Loads and validates the YAML configuration from `path`.
///
/// # Errors
///
/// Returns a [`ConfigError`] describing the first problem found: an
/// unreadable file, invalid YAML, a missing required setting, or a value
/// that fails validation.
pub fn load_config(path: &Path) -> Result<Config, ConfigError> {
    let yaml = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    Config::from_str(&yaml, &path.display().to_string())
}

impl Config {
    /// Parses and validates a YAML configuration string, using `path` in
    /// error messages.
    ///
    /// # Errors
    ///
    /// Returns a [`ConfigError`] for invalid YAML or validation failures.
    pub fn from_str(yaml: &str, path: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            serde_yaml_ng::from_str(yaml).map_err(|error| ConfigError::Invalid {
                path: path.to_string(),
                message: error.to_string(),
            })?;
        Self::from_raw(raw, path)
    }

    fn from_raw(raw: RawConfig, path: &str) -> Result<Self, ConfigError> {
        let server = raw
            .server
            .ok_or_else(|| Self::missing_section(path, "server", "data_plane_addr, admin_addr"))?;
        let upstream = raw
            .upstream
            .ok_or_else(|| Self::missing_section(path, "upstream", "url"))?;

        let data_plane_addr = parse_socket_addr(required_field(
            path,
            &server.data_plane_addr,
            "server.data_plane_addr",
        )?)
        .map_err(|message| Self::invalid(path, format!("server.data_plane_addr: {message}")))?;
        let admin_addr = parse_socket_addr(required_field(
            path,
            &server.admin_addr,
            "server.admin_addr",
        )?)
        .map_err(|message| Self::invalid(path, format!("server.admin_addr: {message}")))?;
        let shutdown_timeout = non_zero_duration(
            server.shutdown_timeout,
            DEFAULT_SHUTDOWN_TIMEOUT,
            "server.shutdown_timeout",
            path,
        )?;
        let url = required_field(path, &upstream.url, "upstream.url")?;
        let timeout = upstream.timeout;
        let raw_healthcheck = upstream.healthcheck;
        let mut upstream = UpstreamTarget::from_url(url)
            .map_err(|message| Self::invalid(path, format!("upstream.url: {message}")))?;
        if let Some(timeout) = timeout {
            if timeout.is_zero() {
                return Err(Self::invalid(
                    path,
                    "upstream.timeout must be greater than zero".to_string(),
                ));
            }
            upstream.timeout = timeout;
        };
        upstream.healthcheck = Self::healthcheck_from_raw(raw_healthcheck, path)?;

        let ingest = Self::ingest_from_raw(raw.ingest, path)?;
        let queue = Self::queue_from_raw(raw.queue, path)?;
        let rate_limits = Self::rate_limits_from_raw(raw.rate_limits, path)?;
        let batch = Self::batch_from_raw(raw.batch, path)?;
        let observability = Self::observability_from_raw(raw.observability, path)?;
        Ok(Self {
            server: ServerSettings {
                data_plane_addr,
                admin_addr,
                shutdown_timeout,
            },
            upstream,
            ingest,
            queue,
            rate_limits,
            batch,
            observability,
        })
    }

    /// Extracts the hot-reloadable settings as one atomic snapshot.
    pub fn runtime_settings(&self) -> RuntimeSettings {
        RuntimeSettings {
            ingest: self.ingest.clone(),
            queue_max_age: self.queue.max_age,
            batch: self.batch.clone(),
            rate_limits: self.rate_limits.clone(),
            healthcheck: self.upstream.healthcheck.clone(),
            observability: self.observability.clone(),
        }
    }

    fn batch_from_raw(raw: Option<RawBatch>, path: &str) -> Result<BatchSettings, ConfigError> {
        let raw = raw.unwrap_or_default();

        let max_items = positive_usize_or_default(
            raw.max_items,
            DEFAULT_BATCH_MAX_ITEMS,
            "batch.max_items",
            path,
        )?;
        let max_bytes = positive_or_default(
            raw.max_bytes,
            DEFAULT_BATCH_MAX_BYTES,
            "batch.max_bytes",
            path,
        )?;
        let flush_interval = non_zero_duration(
            raw.flush_interval,
            DEFAULT_BATCH_FLUSH_INTERVAL,
            "batch.flush_interval",
            path,
        )?;
        let workers =
            positive_usize_or_default(raw.workers, DEFAULT_BATCH_WORKERS, "batch.workers", path)?;
        let max_requests_per_sec = raw
            .max_requests_per_sec
            .unwrap_or(DEFAULT_BATCH_MAX_REQUESTS_PER_SEC);
        if max_requests_per_sec <= 0.0 {
            return Err(ConfigError::Invalid {
                path: path.to_string(),
                message: "batch.max_requests_per_sec must be greater than zero".to_string(),
            });
        }

        Ok(BatchSettings {
            max_items,
            max_bytes,
            flush_interval,
            workers,
            max_requests_per_sec,
        })
    }

    fn healthcheck_from_raw(
        raw: Option<RawHealthcheck>,
        path: &str,
    ) -> Result<HealthcheckSettings, ConfigError> {
        let Some(raw) = raw else {
            return Ok(HealthcheckSettings::default());
        };

        let healthcheck_path = raw
            .path
            .unwrap_or_else(|| DEFAULT_HEALTHCHECK_PATH.to_string());
        if !healthcheck_path.starts_with('/') {
            return Err(Self::invalid(
                path,
                format!("upstream.healthcheck.path must start with '/', got '{healthcheck_path}'"),
            ));
        }

        let expected_statuses = raw.expected_statuses.unwrap_or_else(|| vec![200]);
        if expected_statuses.is_empty() {
            return Err(Self::invalid(
                path,
                "upstream.healthcheck.expected_statuses must not be empty".to_string(),
            ));
        }
        if expected_statuses
            .iter()
            .any(|status| !(100..600).contains(status))
        {
            return Err(Self::invalid(
                path,
                format!(
                    "upstream.healthcheck.expected_statuses must be valid HTTP statuses (100-599), got {expected_statuses:?}"
                ),
            ));
        }

        let threshold =
            |value: Option<u32>, default: u32, name: &str| -> Result<u32, ConfigError> {
                match value.unwrap_or(default) {
                    0 => Err(Self::invalid(
                        path,
                        format!("upstream.healthcheck.{name} must be at least 1"),
                    )),
                    threshold => Ok(threshold),
                }
            };

        Ok(HealthcheckSettings {
            path: healthcheck_path,
            expected_statuses,
            interval: non_zero_duration(
                raw.interval,
                DEFAULT_HEALTHCHECK_INTERVAL,
                "upstream.healthcheck.interval",
                path,
            )?,
            timeout: non_zero_duration(
                raw.timeout,
                DEFAULT_HEALTHCHECK_TIMEOUT,
                "upstream.healthcheck.timeout",
                path,
            )?,
            unhealthy_threshold: threshold(
                raw.unhealthy_threshold,
                DEFAULT_HEALTHCHECK_UNHEALTHY_THRESHOLD,
                "unhealthy_threshold",
            )?,
            healthy_threshold: threshold(
                raw.healthy_threshold,
                DEFAULT_HEALTHCHECK_HEALTHY_THRESHOLD,
                "healthy_threshold",
            )?,
        })
    }

    fn ingest_from_raw(raw: Option<RawIngest>, path: &str) -> Result<IngestSettings, ConfigError> {
        let raw = raw.unwrap_or_default();

        let success_status_code = raw.success_status_code.unwrap_or(201);
        if success_status_code != 200 && success_status_code != 201 {
            return Err(ConfigError::Invalid {
                path: path.to_string(),
                message: format!(
                    "ingest.success_status_code must be 200 or 201, got {success_status_code}"
                ),
            });
        }

        Ok(IngestSettings {
            success_status_code,
            max_compressed_body_bytes: positive_or_default(
                raw.max_compressed_body_bytes,
                DEFAULT_MAX_COMPRESSED_BODY_BYTES,
                "ingest.max_compressed_body_bytes",
                path,
            )?,
            max_decompressed_body_bytes: positive_or_default(
                raw.max_decompressed_body_bytes,
                DEFAULT_MAX_DECOMPRESSED_BODY_BYTES,
                "ingest.max_decompressed_body_bytes",
                path,
            )?,
        })
    }

    fn queue_from_raw(raw: Option<RawQueue>, path: &str) -> Result<QueueSettings, ConfigError> {
        let raw = raw.unwrap_or_default();

        let dir = raw.dir.unwrap_or_else(|| DEFAULT_QUEUE_DIR.to_string());
        if dir.is_empty() {
            return Err(ConfigError::Invalid {
                path: path.to_string(),
                message: "queue.dir must not be empty".to_string(),
            });
        }

        let overflow_policy = match raw.overflow_policy.as_deref() {
            None | Some("drop_oldest") => OverflowPolicy::DropOldest,
            Some(value) => {
                return Err(ConfigError::Invalid {
                    path: path.to_string(),
                    message: format!("queue.overflow_policy must be drop_oldest, got '{value}'"),
                });
            }
        };

        let fsync_mode = match raw.fsync_mode.as_deref() {
            None | Some("periodic") => FsyncMode::Periodic,
            Some("buffered") => FsyncMode::Buffered,
            Some("every_write") => FsyncMode::EveryWrite,
            Some(value) => {
                return Err(ConfigError::Invalid {
                    path: path.to_string(),
                    message: format!(
                        "queue.fsync_mode must be one of periodic, buffered, every_write; \
                         got '{value}'"
                    ),
                });
            }
        };

        let max_age = non_zero_duration(raw.max_age, DEFAULT_MAX_AGE, "queue.max_age", path)?;
        let fsync_interval = non_zero_duration(
            raw.fsync_interval,
            DEFAULT_FSYNC_INTERVAL,
            "queue.fsync_interval",
            path,
        )?;

        Ok(QueueSettings {
            dir: PathBuf::from(dir),
            max_bytes: positive_or_default(
                raw.max_bytes,
                DEFAULT_QUEUE_MAX_BYTES,
                "queue.max_bytes",
                path,
            )?,
            segment_size_bytes: positive_or_default(
                raw.segment_size_bytes,
                DEFAULT_SEGMENT_SIZE_BYTES,
                "queue.segment_size_bytes",
                path,
            )?,
            max_age,
            overflow_policy,
            fsync_mode,
            fsync_interval,
        })
    }

    fn rate_limits_from_raw(
        raw: Option<RawRateLimits>,
        path: &str,
    ) -> Result<RateLimitSettings, ConfigError> {
        let raw = raw.unwrap_or_default();
        let defaults = scope_from_raw(raw.defaults, path)?;
        let mut projects = HashMap::new();
        for (project_id, raw_scope) in raw.projects.unwrap_or_default() {
            projects.insert(project_id, scope_from_raw(Some(raw_scope), path)?);
        }
        Ok(RateLimitSettings {
            max_buckets: raw.max_buckets.unwrap_or(DEFAULT_MAX_BUCKETS),
            bucket_ttl: non_zero_duration(
                raw.bucket_ttl,
                DEFAULT_BUCKET_TTL,
                "rate_limits.bucket_ttl",
                path,
            )?,
            defaults,
            projects,
        })
    }

    fn observability_from_raw(
        raw: Option<RawObservability>,
        path: &str,
    ) -> Result<ObservabilitySettings, ConfigError> {
        let raw = raw.unwrap_or_default();
        let log_level = raw
            .log_level
            .map(|level| LogLevel::parse(&level))
            .transpose()
            .map_err(|message| Self::invalid(path, format!("observability.log_level: {message}")))?
            .unwrap_or(LogLevel::Info);
        let environment_label_allowlist = raw.environment_label_allowlist.unwrap_or_else(|| {
            ["stage", "preprod", "prod", "production"]
                .into_iter()
                .map(String::from)
                .collect()
        });
        if environment_label_allowlist.is_empty() {
            return Err(Self::invalid(
                path,
                "observability.environment_label_allowlist must not be empty \
                 (every environment would map to other)"
                    .to_string(),
            ));
        }
        Ok(ObservabilitySettings {
            log_level,
            environment_label_allowlist,
        })
    }

    fn missing_section(path: &str, section: &str, expected_keys: &str) -> ConfigError {
        Self::invalid(
            path,
            format!("missing required section '{section}' (expected keys: {expected_keys})"),
        )
    }

    fn invalid(path: &str, message: String) -> ConfigError {
        ConfigError::Invalid {
            path: path.to_string(),
            message,
        }
    }
}

fn parse_socket_addr(raw: &str) -> Result<SocketAddr, String> {
    raw.parse()
        .map_err(|_| format!("invalid socket address '{raw}' (expected \"ip:port\")"))
}

fn required_field<'a>(
    path: &str,
    field: &'a Option<String>,
    name: &str,
) -> Result<&'a str, ConfigError> {
    field.as_deref().ok_or_else(|| ConfigError::Invalid {
        path: path.to_string(),
        message: format!("missing required field '{name}'"),
    })
}

fn positive_or_default(
    value: Option<u64>,
    default: u64,
    name: &str,
    path: &str,
) -> Result<u64, ConfigError> {
    match value {
        None => Ok(default),
        Some(0) => Err(ConfigError::Invalid {
            path: path.to_string(),
            message: format!("{name} must be greater than zero"),
        }),
        Some(value) => Ok(value),
    }
}

fn positive_usize_or_default(
    value: Option<usize>,
    default: usize,
    name: &str,
    path: &str,
) -> Result<usize, ConfigError> {
    match value {
        None => Ok(default),
        Some(0) => Err(ConfigError::Invalid {
            path: path.to_string(),
            message: format!("{name} must be greater than zero"),
        }),
        Some(value) => Ok(value),
    }
}

fn non_zero_duration(
    value: Option<Duration>,
    default: Duration,
    name: &str,
    path: &str,
) -> Result<Duration, ConfigError> {
    match value {
        None => Ok(default),
        Some(value) if value.is_zero() => Err(ConfigError::Invalid {
            path: path.to_string(),
            message: format!("{name} must be greater than zero"),
        }),
        Some(value) => Ok(value),
    }
}

/// Deserializes an optional duration from a whole number of seconds or a
/// string with a time unit suffix (`ms`, `s`, `m`, `h`, `d`), e.g.
/// `"200ms"`, `"2h"`.
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionalDurationVisitor;

    impl<'de> serde::de::Visitor<'de> for OptionalDurationVisitor {
        type Value = Option<Duration>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a duration like 30, 500ms, 30s, 5m, 2h, 1d, or null")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_some<E>(self, deserializer: E) -> Result<Self::Value, E::Error>
        where
            E: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(DurationVisitor).map(Some)
        }
    }

    struct DurationVisitor;

    impl<'de> serde::de::Visitor<'de> for DurationVisitor {
        type Value = Duration;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a duration like 30, 500ms, 30s, 5m, 2h or 1d")
        }

        fn visit_u64<E>(self, seconds: u64) -> Result<Duration, E>
        where
            E: serde::de::Error,
        {
            Ok(Duration::from_secs(seconds))
        }

        fn visit_str<E>(self, raw: &str) -> Result<Duration, E>
        where
            E: serde::de::Error,
        {
            parse_duration_str(raw).ok_or_else(|| E::custom(format!("invalid duration '{raw}'")))
        }
    }

    deserializer.deserialize_option(OptionalDurationVisitor)
}

fn parse_duration_str(raw: &str) -> Option<Duration> {
    let (digits, unit) = raw.split_at(raw.find(|c: char| c.is_ascii_alphabetic())?);
    let value: u64 = digits.parse().ok()?;
    let millis = match unit {
        "ms" => value,
        "s" => value.checked_mul(1_000)?,
        "m" => value.checked_mul(60_000)?,
        "h" => value.checked_mul(60_000)?.checked_mul(60)?,
        "d" => value
            .checked_mul(60_000)?
            .checked_mul(60)?
            .checked_mul(24)?,
        _ => return None,
    };
    Some(Duration::from_millis(millis))
}

impl UpstreamTarget {
    fn from_url(raw: &str) -> Result<Self, String> {
        let url = Url::parse(raw)
            .map_err(|source| format!("failed to parse '{raw}' as a URL: {source}"))?;

        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(format!(
                "unsupported scheme '{}' (only http and https are supported)",
                url.scheme()
            ));
        }
        let host = url
            .host_str()
            .ok_or_else(|| format!("failed to parse '{raw}' as a URL: missing host"))?
            .to_string();

        let port = url
            .port_or_known_default()
            .ok_or_else(|| format!("failed to parse '{raw}' as a URL: missing port"))?;

        let path_prefix = match url.path() {
            "" | "/" => String::new(),
            path => path.trim_end_matches('/').to_string(),
        };

        Ok(Self {
            tls: url.scheme() == "https",
            host,
            port,
            path_prefix,
            timeout: DEFAULT_UPSTREAM_TIMEOUT,
            healthcheck: HealthcheckSettings::default(),
        })
    }

    /// The `Upstream` origin (`scheme://host[:port]`) without any path.
    pub fn base_url(&self) -> String {
        let port = if is_default_port(self.tls, self.port) {
            String::new()
        } else {
            format!(":{}", self.port)
        };
        format!(
            "{}://{}{}",
            if self.tls { "https" } else { "http" },
            self.host,
            port
        )
    }
}

fn is_default_port(tls: bool, port: u16) -> bool {
    if tls { port == 443 } else { port == 80 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_yaml() -> String {
        String::from(
            "server:\n\
             \x20 data_plane_addr: \"127.0.0.1:18080\"\n\
             \x20 admin_addr: \"127.0.0.1:18081\"\n\
             upstream:\n\
             \x20 url: \"http://127.0.0.1:18082\"\n",
        )
    }

    #[test]
    fn parses_ingest_and_queue_settings() {
        let yaml = format!(
            "{}\
             ingest:\n\
             \x20 success_status_code: 200\n\
             \x20 max_compressed_body_bytes: 1024\n\
             \x20 max_decompressed_body_bytes: 4096\n\
             queue:\n\
             \x20 dir: \"/tmp/sentry-ratelimiter-wal\"\n\
             \x20 max_bytes: 52428800\n\
             \x20 segment_size_bytes: 1048576\n\
             \x20 max_age: \"2h\"\n\
             \x20 overflow_policy: drop_oldest\n\
             \x20 fsync_mode: every_write\n\
             \x20 fsync_interval: \"50ms\"\n",
            base_yaml()
        );

        let config = Config::from_str(&yaml, "<test config>").unwrap();

        assert_eq!(config.ingest.success_status_code, 200);
        assert_eq!(config.ingest.max_compressed_body_bytes, 1024);
        assert_eq!(config.ingest.max_decompressed_body_bytes, 4096);
        assert_eq!(config.queue.dir, Path::new("/tmp/sentry-ratelimiter-wal"));
        assert_eq!(config.queue.max_bytes, 52_428_800);
        assert_eq!(config.queue.segment_size_bytes, 1_048_576);
        assert_eq!(config.queue.max_age, Duration::from_secs(2 * 60 * 60));
        assert_eq!(config.queue.overflow_policy, OverflowPolicy::DropOldest);
        assert_eq!(config.queue.fsync_mode, FsyncMode::EveryWrite);
        assert_eq!(config.queue.fsync_interval, Duration::from_millis(50));
    }

    #[test]
    fn applies_default_ingest_and_queue_settings() {
        let config = Config::from_str(&base_yaml(), "<test config>").unwrap();

        assert_eq!(config.ingest.success_status_code, 201);
        assert_eq!(config.ingest.max_compressed_body_bytes, 10 * 1024 * 1024);
        assert_eq!(config.ingest.max_decompressed_body_bytes, 50 * 1024 * 1024);
        assert_eq!(config.queue.dir, Path::new("queue"));
        assert_eq!(config.queue.max_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.queue.segment_size_bytes, 16 * 1024 * 1024);
        assert_eq!(config.queue.max_age, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.queue.overflow_policy, OverflowPolicy::DropOldest);
        assert_eq!(config.queue.fsync_mode, FsyncMode::Periodic);
        assert_eq!(config.queue.fsync_interval, Duration::from_millis(200));
    }

    #[test]
    fn rejects_unsupported_success_status_code() {
        let yaml = format!(
            "{}\
             ingest:\n\
             \x20 success_status_code: 404\n",
            base_yaml()
        );

        let error = Config::from_str(&yaml, "<test config>").unwrap_err();
        assert!(error.to_string().contains("success_status_code"));
    }

    #[test]
    fn rejects_unsupported_fsync_mode() {
        let yaml = format!(
            "{}\
             queue:\n\
             \x20 dir: \"/tmp/wal\"\n\
             \x20 fsync_mode: sometimes\n",
            base_yaml()
        );

        let error = Config::from_str(&yaml, "<test config>").unwrap_err();
        assert!(error.to_string().contains("fsync_mode"));
    }

    #[test]
    fn parses_upstream_timeout_and_applies_default() {
        let default_config = Config::from_str(&base_yaml(), "<test config>").unwrap();
        assert_eq!(default_config.upstream.timeout, Duration::from_secs(10));

        let yaml = "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:18080\"\n\
         \x20 admin_addr: \"127.0.0.1:18081\"\n\
         upstream:\n\
         \x20 url: \"http://127.0.0.1:18082\"\n\
         \x20 timeout: 250ms\n";
        let config = Config::from_str(yaml, "<test config>").unwrap();
        assert_eq!(config.upstream.timeout, Duration::from_millis(250));
    }

    #[test]
    fn rejects_non_positive_queue_sizes() {
        let yaml = format!(
            "{}\
             queue:\n\
             \x20 dir: \"/tmp/wal\"\n\
             \x20 max_bytes: 0\n",
            base_yaml()
        );

        let error = Config::from_str(&yaml, "<test config>").unwrap_err();
        assert!(error.to_string().contains("max_bytes"));
    }

    #[test]
    fn parses_rate_limits_settings() {
        let yaml = format!(
            "{}\
             rate_limits:\n\
             \x20 max_buckets: 100\n\
             \x20 bucket_ttl: 5m\n\
             \x20 defaults:\n\
             \x20 \x20 event:\n\
             \x20 \x20 \x20 rate: 100.5\n\
             \x20 \x20 \x20 burst: 200\n\
             \x20 \x20 environments:\n\
             \x20 \x20 \x20 production:\n\
             \x20 \x20 \x20 \x20 transaction:\n\
             \x20 \x20 \x20 \x20 \x20 rate: 2.5\n\
             \x20 \x20 \x20 \x20 \x20 burst: 5\n\
             \x20 projects:\n\
             \x20 \x20 42:\n\
             \x20 \x20 \x20 other:\n\
             \x20 \x20 \x20 \x20 rate: 1\n\
             \x20 \x20 \x20 \x20 burst: 2\n\
             \x20 \x20 \x20 environments:\n\
             \x20 \x20 \x20 \x20 staging:\n\
             \x20 \x20 \x20 \x20 \x20 event:\n\
             \x20 \x20 \x20 \x20 \x20 \x20 rate: 0.5\n\
             \x20 \x20 \x20 \x20 \x20 \x20 burst: 1\n\
",
            base_yaml()
        );

        let config = Config::from_str(&yaml, "<test config>").unwrap();
        let limits = &config.rate_limits;

        assert_eq!(limits.max_buckets, 100);
        assert_eq!(limits.bucket_ttl, Duration::from_secs(5 * 60));
        assert_eq!(
            limits.defaults.event.as_ref().unwrap(),
            &Rule {
                rate: 100.5,
                burst: 200
            }
        );
        assert_eq!(
            limits.defaults.environments["production"]
                .transaction
                .as_ref()
                .unwrap(),
            &Rule {
                rate: 2.5,
                burst: 5
            }
        );
        assert_eq!(
            limits.projects[&42].other.as_ref().unwrap(),
            &Rule {
                rate: 1.0,
                burst: 2
            }
        );
        assert_eq!(
            limits.projects[&42].environments["staging"]
                .event
                .as_ref()
                .unwrap(),
            &Rule {
                rate: 0.5,
                burst: 1
            }
        );
    }

    #[test]
    fn rate_limits_default_to_unlimited() {
        let config = Config::from_str(&base_yaml(), "<test config>").unwrap();

        assert!(config.rate_limits.defaults.event.is_none());
        assert!(config.rate_limits.projects.is_empty());
        assert!(config.rate_limits.max_buckets > 0);
        assert!(config.rate_limits.bucket_ttl > Duration::ZERO);
    }

    #[test]
    fn rejects_zero_rate_limit_values() {
        let yaml = format!(
            "{}\
             rate_limits:\n\
             \x20 defaults:\n\
             \x20 \x20 event:\n\
             \x20 \x20 \x20 rate: 0\n\
             \x20 \x20 \x20 burst: 2\n\
",
            base_yaml()
        );
        let error = Config::from_str(&yaml, "<test config>").unwrap_err();
        assert!(error.to_string().contains("rate must be greater than zero"));
    }

    #[test]
    fn parses_batch_settings() {
        let yaml = format!(
            "{}batch:\n\x20 max_items: 10\n\x20 max_bytes: 1024\n\x20 flush_interval: 250ms\n\x20 workers: 2\n\x20 max_requests_per_sec: 5.5\n",
            base_yaml()
        );

        let config = Config::from_str(&yaml, "<test config>").unwrap();

        assert_eq!(config.batch.max_items, 10);
        assert_eq!(config.batch.max_bytes, 1024);
        assert_eq!(config.batch.flush_interval, Duration::from_millis(250));
        assert_eq!(config.batch.workers, 2);
        assert_eq!(config.batch.max_requests_per_sec, 5.5);
    }

    #[test]
    fn applies_default_batch_settings() {
        let config = Config::from_str(&base_yaml(), "<test config>").unwrap();

        assert_eq!(config.batch.max_items, 100);
        assert_eq!(config.batch.max_bytes, 10 * 1024 * 1024);
        assert_eq!(config.batch.flush_interval, Duration::from_millis(100));
        assert_eq!(config.batch.workers, 4);
        assert_eq!(config.batch.max_requests_per_sec, 100.0);
    }

    #[test]
    fn rejects_invalid_batch_values() {
        for bad in [
            "max_items: 0",
            "max_bytes: 0",
            "workers: 0",
            "flush_interval: 0",
            "max_requests_per_sec: 0",
        ] {
            let yaml = format!("{}batch:\n\x20 {bad}\n", base_yaml());
            let error = Config::from_str(&yaml, "<test config>").unwrap_err();
            assert!(
                error.to_string().contains("batch."),
                "error for {bad} must name the field: {error}"
            );
        }
    }

    #[test]
    fn parses_upstream_healthcheck_settings() {
        let yaml = format!(
            "{}\
\
             \x20 timeout: 5s\n\
             \x20 healthcheck:\n\
             \x20 \x20 path: \"/health\"\n\
             \x20 \x20 expected_statuses: [200, 204]\n\
             \x20 \x20 interval: 250ms\n\
             \x20 \x20 timeout: 1s\n\
             \x20 \x20 unhealthy_threshold: 2\n\
             \x20 \x20 healthy_threshold: 3\n",
            base_yaml()
        );

        let config = Config::from_str(&yaml, "<test config>").unwrap();
        let healthcheck = &config.upstream.healthcheck;

        assert_eq!(healthcheck.path, "/health");
        assert_eq!(healthcheck.expected_statuses, [200, 204]);
        assert_eq!(healthcheck.interval, Duration::from_millis(250));
        assert_eq!(healthcheck.timeout, Duration::from_secs(1));
        assert_eq!(healthcheck.unhealthy_threshold, 2);
        assert_eq!(healthcheck.healthy_threshold, 3);
    }

    #[test]
    fn applies_default_healthcheck_settings() {
        let config = Config::from_str(&base_yaml(), "<test config>").unwrap();
        let healthcheck = &config.upstream.healthcheck;

        assert_eq!(healthcheck.path, "/");
        assert_eq!(healthcheck.expected_statuses, [200]);
        assert_eq!(healthcheck.interval, Duration::from_secs(10));
        assert_eq!(healthcheck.timeout, Duration::from_secs(2));
        assert_eq!(healthcheck.unhealthy_threshold, 3);
        assert_eq!(healthcheck.healthy_threshold, 2);
    }

    #[test]
    fn rejects_invalid_healthcheck_values() {
        for bad in [
            "path: no-leading-slash",
            "expected_statuses: []",
            "expected_statuses: [99]",
            "expected_statuses: [600]",
            "interval: 0",
            "timeout: 0",
            "unhealthy_threshold: 0",
            "healthy_threshold: 0",
        ] {
            let yaml = format!(
                "{}\n\
                 \x20 healthcheck:\n\
                 \x20 \x20 {bad}\n",
                base_yaml()
            );
            let error = Config::from_str(&yaml, "<test config>").unwrap_err();
            assert!(
                error.to_string().contains("healthcheck"),
                "error for {bad} must name the section: {error}"
            );
        }
    }

    #[test]
    fn parses_shutdown_timeout_with_default() {
        let default_config = Config::from_str(&base_yaml(), "<test config>").unwrap();
        assert_eq!(
            default_config.server.shutdown_timeout,
            Duration::from_secs(30)
        );

        let yaml = "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:18080\"\n\
         \x20 admin_addr: \"127.0.0.1:18081\"\n\
         \x20 shutdown_timeout: 5s\n\
         upstream:\n\
         \x20 url: \"http://127.0.0.1:18082\"\n";
        let config = Config::from_str(yaml, "<test config>").unwrap();
        assert_eq!(config.server.shutdown_timeout, Duration::from_secs(5));
    }

    #[test]
    fn rejects_non_positive_shutdown_timeout() {
        let yaml = "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:18080\"\n\
         \x20 admin_addr: \"127.0.0.1:18081\"\n\
         \x20 shutdown_timeout: 0\n\
         upstream:\n\
         \x20 url: \"http://127.0.0.1:18082\"\n";
        let error = Config::from_str(yaml, "<test config>").unwrap_err();
        assert!(error.to_string().contains("shutdown_timeout"));
    }

    #[test]
    fn parses_observability_log_level_with_default() {
        let default_config = Config::from_str(&base_yaml(), "<test config>").unwrap();
        assert_eq!(default_config.observability.log_level, LogLevel::Info);

        let yaml = format!("{}observability:\n\x20 log_level: debug\n", base_yaml());
        let config = Config::from_str(&yaml, "<test config>").unwrap();
        assert_eq!(config.observability.log_level, LogLevel::Debug);
    }

    #[test]
    fn rejects_unknown_log_level() {
        let yaml = format!("{}observability:\n\x20 log_level: loud\n", base_yaml());
        let error = Config::from_str(&yaml, "<test config>").unwrap_err();
        assert!(error.to_string().contains("log_level"));
    }

    #[test]
    fn parses_environment_label_allowlist_with_default() {
        let default_config = Config::from_str(&base_yaml(), "<test config>").unwrap();
        assert_eq!(
            default_config.observability.environment_label_allowlist,
            ["stage", "preprod", "prod", "production"]
        );

        let yaml = format!(
            "{}observability:\n\x20 log_level: debug\n\x20 environment_label_allowlist: [dev, prod]\n",
            base_yaml()
        );
        let config = Config::from_str(&yaml, "<test config>").unwrap();
        assert_eq!(
            config.observability.environment_label_allowlist,
            ["dev", "prod"]
        );
    }

    #[test]
    fn rejects_empty_environment_label_allowlist() {
        let yaml = format!(
            "{}observability:\n\x20 environment_label_allowlist: []\n",
            base_yaml()
        );
        let error = Config::from_str(&yaml, "<test config>").unwrap_err();
        assert!(
            error.to_string().contains("environment_label_allowlist"),
            "an empty allowlist would map every environment to other: {error}"
        );
    }

    #[test]
    fn extracts_hot_reloadable_runtime_settings() {
        let yaml = format!(
            "{}\
             ingest:\n\
             \x20 success_status_code: 200\n\
             queue:\n\
             \x20 dir: \"/tmp/wal\"\n\
             \x20 max_age: 2h\n\
             batch:\n\
             \x20 max_items: 7\n\
             rate_limits:\n\
             \x20 defaults:\n\
             \x20 \x20 event:\n\
             \x20 \x20 \x20 rate: 5\n\
             \x20 \x20 \x20 burst: 5\n\
             observability:\n\
             \x20 log_level: warn\n",
            base_yaml()
        );
        let config = Config::from_str(&yaml, "<test config>").unwrap();
        let runtime = config.runtime_settings();

        assert_eq!(runtime.ingest.success_status_code, 200);
        assert_eq!(runtime.queue_max_age, Duration::from_secs(2 * 60 * 60));
        assert_eq!(runtime.batch.max_items, 7);
        assert_eq!(runtime.observability.log_level, LogLevel::Warn);
        assert_eq!(
            runtime.rate_limits.defaults.event,
            Some(Rule {
                rate: 5.0,
                burst: 5
            })
        );
        assert_eq!(
            runtime.healthcheck.path, "/",
            "the healthcheck section is part of the hot-reloadable runtime settings"
        );
    }
}

/// The raw (not yet validated) YAML schema.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    server: Option<RawServer>,
    upstream: Option<RawUpstream>,
    ingest: Option<RawIngest>,
    queue: Option<RawQueue>,
    rate_limits: Option<RawRateLimits>,
    batch: Option<RawBatch>,
    observability: Option<RawObservability>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawBatch {
    max_items: Option<usize>,
    max_bytes: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    flush_interval: Option<Duration>,
    workers: Option<usize>,
    max_requests_per_sec: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawRateLimits {
    max_buckets: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    bucket_ttl: Option<Duration>,
    defaults: Option<RawScope>,
    projects: Option<HashMap<u64, RawScope>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawScope {
    event: Option<RawRule>,
    transaction: Option<RawRule>,
    other: Option<RawRule>,
    environments: Option<HashMap<String, RawScope>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    rate: f64,
    burst: u32,
}

/// Builds validated `ScopeRules` from the raw YAML scope.
fn scope_from_raw(raw: Option<RawScope>, path: &str) -> Result<ScopeRules, ConfigError> {
    let Some(raw) = raw else {
        return Ok(ScopeRules::default());
    };
    let rule = |raw: Option<RawRule>| -> Result<Option<Rule>, ConfigError> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        if raw.rate <= 0.0 {
            return Err(ConfigError::Invalid {
                path: path.to_string(),
                message: "rate must be greater than zero".to_string(),
            });
        }
        if raw.burst == 0 {
            return Err(ConfigError::Invalid {
                path: path.to_string(),
                message: "burst must be at least 1".to_string(),
            });
        }
        Ok(Some(Rule {
            rate: raw.rate,
            burst: raw.burst,
        }))
    };
    let mut environments = HashMap::new();
    for (environment, raw_scope) in raw.environments.unwrap_or_default() {
        environments.insert(environment, scope_from_raw(Some(raw_scope), path)?);
    }
    Ok(ScopeRules {
        event: rule(raw.event)?,
        transaction: rule(raw.transaction)?,
        other: rule(raw.other)?,
        environments,
    })
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawIngest {
    success_status_code: Option<u16>,
    max_compressed_body_bytes: Option<u64>,
    max_decompressed_body_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawQueue {
    dir: Option<String>,
    max_bytes: Option<u64>,
    segment_size_bytes: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    max_age: Option<Duration>,
    overflow_policy: Option<String>,
    fsync_mode: Option<String>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    fsync_interval: Option<Duration>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawObservability {
    log_level: Option<String>,
    environment_label_allowlist: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    data_plane_addr: Option<String>,
    admin_addr: Option<String>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    shutdown_timeout: Option<Duration>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    timeout: Option<Duration>,
    healthcheck: Option<RawHealthcheck>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawHealthcheck {
    path: Option<String>,
    expected_statuses: Option<Vec<u16>>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    interval: Option<Duration>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    timeout: Option<Duration>,
    unhealthy_threshold: Option<u32>,
    healthy_threshold: Option<u32>,
}
