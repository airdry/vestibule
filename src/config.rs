//! Startup YAML configuration: parsing and validation.
//!
//! The configuration surface grows ticket by ticket; this module owns the
//! schema, validation rules, and the clear startup error messages required
//! by the specification (`server` and `upstream` are validated here).

use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;
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
}

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
        let upstream = UpstreamTarget::from_url(url)
            .map_err(|message| Self::invalid(path, format!("upstream.url: {message}")))?;

        Ok(Self {
            server: ServerSettings {
                data_plane_addr,
                admin_addr,
            },
            upstream,
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
        })
    }
}

/// The raw (not yet validated) YAML schema.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    server: Option<RawServer>,
    upstream: Option<RawUpstream>,
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
}
