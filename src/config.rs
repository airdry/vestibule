//! Startup YAML configuration: parsing and validation.
//!
//! The configuration surface grows ticket by ticket; this module owns the
//! schema, validation rules, and the clear startup error messages required
//! by the specification (`server` and `upstream` are validated here).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use url::Url;

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

/// Listener addresses of the two planes inside one process.
#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub data_plane_addr: SocketAddr,
    pub admin_addr: SocketAddr,
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
        let url = required_field(path, &upstream.url, "upstream.url")?;
        let timeout = upstream.timeout;
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

        let ingest = Self::ingest_from_raw(raw.ingest, path)?;
        let queue = Self::queue_from_raw(raw.queue, path)?;
        Ok(Self {
            server: ServerSettings {
                data_plane_addr,
                admin_addr,
            },
            upstream,
            ingest,
            queue,
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
        })
    }
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
}

/// The raw (not yet validated) YAML schema.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    server: Option<RawServer>,
    upstream: Option<RawUpstream>,
    ingest: Option<RawIngest>,
    queue: Option<RawQueue>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServer {
    data_plane_addr: Option<String>,
    admin_addr: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_duration")]
    timeout: Option<Duration>,
}
