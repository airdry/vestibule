//! Hot-reload integration tests: `POST /reload` applies validated YAML
//! atomically while preserving active Token Bucket state, and invalid YAML
//! is rejected without disturbing the previously active configuration.

mod support;

use std::net::TcpStream;
use std::path::Path;
use std::time::{Duration, Instant};

use support::{
    MockUpstream, ServiceProcess, UpstreamResponse, config_yaml_for, ephemeral_port, http_client,
    start_service_from_config, tempdir, write_config,
};

/// Starts the service from a config file and returns it with the config
/// path (kept for rewriting the file during reload).
fn start_with_config(
    upstream_url: &str,
    queue_dir: &Path,
    extra_sections: &str,
) -> (support::TestApp, std::path::PathBuf) {
    let yaml = config_yaml_for(
        upstream_url,
        ephemeral_port(),
        ephemeral_port(),
        queue_dir,
        extra_sections,
    );
    let path = write_config(&yaml);
    let app = start_service_from_config(&path);
    (app, path)
}

/// Triggers `POST /reload` on the Admin Plane.
fn reload(admin_url: &str) -> reqwest::blocking::Response {
    http_client()
        .post(format!("{admin_url}/reload"))
        .send()
        .expect("POST /reload")
}

/// `GET /status` on the Admin Plane, parsed as JSON.
fn status_document(admin_url: &str) -> serde_json::Value {
    http_client()
        .get(format!("{admin_url}/status"))
        .send()
        .expect("GET /status")
        .json()
        .expect("parse /status JSON")
}

/// POSTs one store event to the Data Plane.
fn store_event(app: &support::TestApp, project: &str) -> reqwest::blocking::Response {
    http_client()
        .post(format!("{}/api/{project}/store/", app.data_plane_url))
        .header("Content-Type", "application/json")
        .body(br#"{"environment":"production"}"#.to_vec())
        .send()
        .expect("POST store event")
}

#[test]
fn post_reload_applies_new_config_and_preserves_token_bucket_state() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let queue_dir = tempdir();
    let (app, config_path) = start_with_config(&upstream.url(), &queue_dir, "");

    // Initially unlimited: both requests are accepted with the default 201.
    assert_eq!(store_event(&app, "42").status().as_u16(), 201);
    assert_eq!(store_event(&app, "42").status().as_u16(), 201);

    // New config: `success_status_code: 200` plus a defaults event rule
    // refilling at 0.001 RPS with burst 1.
    let new_yaml = config_yaml_for(
        &upstream.url(),
        app.data_plane_addr_port(),
        app.admin_addr_port(),
        &queue_dir,
        "ingest:\n  success_status_code: 200\n\
         rate_limits:\n  defaults:\n    event: {rate: 0.001, burst: 1}\n",
    );
    std::fs::write(&config_path, new_yaml).expect("write new config");

    let response = reload(&app.admin_url);
    assert_eq!(
        response.status().as_u16(),
        200,
        "a valid reload must be acknowledged: {}",
        response.text().unwrap_or_default()
    );

    // The new success status code and the new rule are active: the first
    // event passes with 200 and consumes the only token; the second is
    // throttled with 429.
    assert_eq!(store_event(&app, "42").status().as_u16(), 200);
    assert_eq!(store_event(&app, "42").status().as_u16(), 429);

    // Reloading again must not reset the active Token Bucket state: the
    // exhausted bucket stays exhausted instead of refilling to burst.
    assert_eq!(reload(&app.admin_url).status().as_u16(), 200);
    assert_eq!(store_event(&app, "42").status().as_u16(), 429);
}

#[test]
fn invalid_yaml_reload_returns_400_and_keeps_previous_config() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let queue_dir = tempdir();
    let (app, config_path) = start_with_config(
        &upstream.url(),
        &queue_dir,
        "rate_limits:\n  defaults:\n    event: {rate: 0.001, burst: 1}\n",
    );

    // Consume the single event token; the next request is throttled.
    assert_eq!(store_event(&app, "42").status().as_u16(), 201);
    assert_eq!(store_event(&app, "42").status().as_u16(), 429);

    // A structurally invalid YAML file is rejected with 400.
    std::fs::write(&config_path, "::: not valid yaml [{{\n").expect("write invalid config");
    let response = reload(&app.admin_url);
    assert_eq!(response.status().as_u16(), 400);
    assert!(
        response.text().unwrap_or_default().contains("error"),
        "the 400 body must carry the validation error"
    );

    // The previous configuration stays active: the same throttling applies.
    assert_eq!(store_event(&app, "42").status().as_u16(), 429);

    // The failure is recorded on the status document.
    let status = status_document(&app.admin_url);
    assert_eq!(
        status["config"]["status"], "reload-failed",
        "unexpected status document: {status}"
    );

    // A schema-valid but rule-invalid YAML is also rejected (burst 0).
    let yaml = config_yaml_for(
        &upstream.url(),
        app.data_plane_addr_port(),
        app.admin_addr_port(),
        &queue_dir,
        "rate_limits:\n  defaults:\n    event: {rate: 1, burst: 0}\n",
    );
    std::fs::write(&config_path, yaml).expect("write rule-invalid config");
    assert_eq!(reload(&app.admin_url).status().as_u16(), 400);
    assert_eq!(store_event(&app, "42").status().as_u16(), 429);

    // A subsequent valid reload recovers.
    let yaml = config_yaml_for(
        &upstream.url(),
        app.data_plane_addr_port(),
        app.admin_addr_port(),
        &queue_dir,
        "ingest:\n  success_status_code: 200\n",
    );
    std::fs::write(&config_path, yaml).expect("write recovering config");
    assert_eq!(reload(&app.admin_url).status().as_u16(), 200);
    // No rate limits any more: the store route is accepted, with the new
    // success status code.
    assert_eq!(store_event(&app, "42").status().as_u16(), 200);

    let status = status_document(&app.admin_url);
    assert_eq!(status["config"]["status"], "loaded");
}

/// A Kubernetes-style `ConfigMap` mount: the config file is a symlink
/// atomically swapped to point at a fresh `..data_<id>` directory.
struct ConfigMount {
    dir: std::path::PathBuf,
}

impl ConfigMount {
    /// Creates the mount root with `config.yaml` symlinked to
    /// `..data_a/config.yaml`.
    fn new(yaml: &str) -> (Self, std::path::PathBuf) {
        let dir = tempdir();
        let data = dir.join("..data_a");
        std::fs::create_dir_all(&data).expect("create ..data_a");
        let file = data.join("config.yaml");
        std::fs::write(&file, yaml).expect("write config");
        let link = dir.join("config.yaml");
        std::os::unix::fs::symlink(&file, &link).expect("symlink config.yaml");
        (Self { dir }, link)
    }

    /// Atomically swaps `config.yaml` to point at a fresh `..data_<id>`
    /// directory holding `yaml` (the Kubernetes update dance: a new
    /// symlink is created and renamed over the old one).
    fn swap(&self, yaml: &str) {
        static SWAPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = SWAPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let data = self.dir.join(format!("..data_{id}"));
        std::fs::create_dir_all(&data).expect("create ..data directory");
        let file = data.join("config.yaml");
        std::fs::write(&file, yaml).expect("write config");
        let temp = self.dir.join(".config.yaml.swapping");
        std::os::unix::fs::symlink(&file, &temp).expect("symlink temp link");
        std::fs::rename(&temp, self.dir.join("config.yaml")).expect("atomic symlink swap");
    }
}

#[test]
fn atomic_symlink_swap_triggers_debounced_reload() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let queue_dir = tempdir();
    let initial = config_yaml_for(
        &upstream.url(),
        ephemeral_port(),
        ephemeral_port(),
        &queue_dir,
        "",
    );
    let (mount, config_path) = ConfigMount::new(&initial);
    let app = start_service_from_config(&config_path);

    assert_eq!(store_event(&app, "42").status().as_u16(), 201);

    // Swap in a config with a new success status code (and rules that
    // leave the store route unlimited, so the status code is observable).
    let data_port = app.data_plane_addr_port();
    let admin_port = app.admin_addr_port();
    let swapped = config_yaml_for(
        &upstream.url(),
        data_port,
        admin_port,
        &queue_dir,
        "ingest:\n  success_status_code: 200\n",
    );
    mount.swap(&swapped);

    // The watcher debounces and reloads without any manual trigger.
    support::wait_until(|| {
        status_document(&app.admin_url)["config"]["reloads"].as_u64() == Some(1)
    });
    assert_eq!(store_event(&app, "42").status().as_u16(), 200);

    // A second swap reloads again: every atomic swap is picked up.
    mount.swap(&swapped);
    support::wait_until(|| {
        status_document(&app.admin_url)["config"]["reloads"].as_u64() == Some(2)
    });
}

#[test]
fn startup_only_settings_are_immutable_across_reloads() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let queue_dir = tempdir();
    let data_port = ephemeral_port();
    let admin_port = ephemeral_port();
    let (app, config_path) = start_with_config(&upstream.url(), &queue_dir, "");
    let data_addr = app.data_plane_addr;

    // A reload that changes startup-only fields (listen addresses and
    // queue.dir) is still valid YAML: it applies the reloadable sections
    // but keeps the running listeners and queue directory in place.
    let yaml = config_yaml_for(
        &upstream.url(),
        data_port,
        admin_port,
        &tempdir(),
        "ingest:\n  success_status_code: 200\n",
    );
    std::fs::write(&config_path, yaml).expect("write startup-only config");

    let response = reload(&app.admin_url);
    assert_eq!(response.status().as_u16(), 200);

    // The service still serves on the original addresses and queue.
    assert_eq!(store_event(&app, "42").status().as_u16(), 200);
    assert!(TcpStream::connect_timeout(&data_addr, Duration::from_millis(500)).is_ok());
}

/// POSTs one store event to a running `ServiceProcess`.
fn store_event_on(url: &str) -> u16 {
    http_client()
        .post(format!("{url}/api/42/store/"))
        .header("Content-Type", "application/json")
        .body(br#"{"environment":"production"}"#.to_vec())
        .send()
        .expect("POST store event")
        .status()
        .as_u16()
}

/// The `config.reloads` counter of a running `ServiceProcess`.
fn reloads_count(admin_url: &str) -> u64 {
    status_document(admin_url)["config"]["reloads"]
        .as_u64()
        .unwrap_or(0)
}

#[test]
fn sighup_reloads_the_configuration() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let queue_dir = tempdir();
    let yaml = config_yaml_for(
        &upstream.url(),
        ephemeral_port(),
        ephemeral_port(),
        &queue_dir,
        "",
    );
    let config_path = write_config(&yaml);
    let service = ServiceProcess::start(&config_path);

    assert_eq!(store_event_on(&service.data_plane_url), 201);

    // Let startup settle: without filesystem changes the watcher stays
    // silent, so the next reload is attributable to SIGHUP alone.
    std::thread::sleep(Duration::from_millis(600));
    let baseline = reloads_count(&service.admin_url);

    service.sighup();
    support::wait_until(|| reloads_count(&service.admin_url) == baseline + 1);
    // Same configuration: the success status code is unchanged.
    assert_eq!(store_event_on(&service.data_plane_url), 201);

    // A changed file plus SIGHUP applies the new configuration.
    let yaml = config_yaml_for(
        &upstream.url(),
        data_port_of(&yaml),
        admin_port_of(&yaml),
        &queue_dir,
        "ingest:\n  success_status_code: 200\n",
    );
    std::fs::write(&config_path, yaml).expect("write new config");
    service.sighup();
    support::wait_until(|| reloads_count(&service.admin_url) == baseline + 2);
    assert_eq!(store_event_on(&service.data_plane_url), 200);

    service.kill();
}

/// The `server.data_plane_addr` port inside a config YAML.
fn data_port_of(yaml: &str) -> u16 {
    yaml.lines()
        .find_map(|line| line.trim().strip_prefix("data_plane_addr:"))
        .and_then(|value| {
            value
                .trim()
                .trim_matches('"')
                .rsplit(':')
                .next()
                .and_then(|port| port.parse().ok())
        })
        .expect("data plane port in test config")
}

/// The `server.admin_addr` port inside a config YAML.
fn admin_port_of(yaml: &str) -> u16 {
    yaml.lines()
        .find_map(|line| line.trim().strip_prefix("admin_addr:"))
        .and_then(|value| {
            value
                .trim()
                .trim_matches('"')
                .rsplit(':')
                .next()
                .and_then(|port| port.parse().ok())
        })
        .expect("admin port in test config")
}

#[test]
fn sigterm_shuts_down_gracefully_without_losing_accepted_payloads() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("{}"));
    let queue_dir = tempdir();
    let yaml = config_yaml_for(
        &upstream.url(),
        ephemeral_port(),
        ephemeral_port(),
        &queue_dir,
        // Defaults keep fsync periodic; durability across SIGTERM comes
        // from the shutdown flush of the WAL and checkpoint.
        "",
    );
    let config_path = write_config(&yaml);
    let service = ServiceProcess::start(&config_path);

    // Accept several distinct payloads right up to the shutdown signal.
    let payloads: Vec<String> = (0..5)
        .map(|i| format!("{{\"environment\":\"production\",\"i\":{i}}}"))
        .collect();
    for payload in &payloads {
        let response = http_client()
            .post(format!("{}/api/42/store/", service.data_plane_url))
            .header("Content-Type", "application/json")
            .body(payload.clone())
            .send()
            .expect("POST store event");
        assert_eq!(response.status().as_u16(), 201);
    }

    service.sigterm();

    // While draining, /readyz must answer 503 (never 200); the window
    // closes when the process exits and connections are refused.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if Instant::now() > deadline {
            panic!("service did not exit within 15s of SIGTERM");
        }
        match http_client()
            .get(format!("{}/readyz", service.admin_url))
            .send()
        {
            Ok(response) => {
                let status = response.status().as_u16();
                assert_eq!(status, 503, "/readyz must drain with 503, got {status}");
            }
            Err(_) => break,
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // The process exits cleanly with code 0.
    let code = service.wait();
    assert_eq!(code, 0, "SIGTERM must exit cleanly with code 0");

    // Restarting against the same queue drains every accepted payload:
    // the WAL and checkpoint were flushed and fsynced before exit.
    let service = ServiceProcess::start(&config_path);
    support::wait_until(|| {
        support::non_probe_requests(&upstream)
            .iter()
            .filter(|request| request.path == "/api/42/store/")
            .count()
            >= 5
    });
    let recorded = support::non_probe_requests(&upstream);
    for payload in &payloads {
        let body = payload.as_bytes();
        assert!(
            recorded
                .iter()
                .any(|request| request.path == "/api/42/store/" && request.body == body),
            "payload {payload} was lost across SIGTERM shutdown"
        );
    }
    service.kill();
}
