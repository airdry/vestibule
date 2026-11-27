//! Reusable localhost integration-test harness.
//!
//! Spins up the real service on ephemeral localhost ports against a
//! programmable mock HTTP `Upstream` (see the `Testing Decisions` section of
//! the specification: this is the primary test seam).

// Each integration test file is a separate binary that uses only a subset of
// these helpers.
#![allow(dead_code)]

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

/// A request received by the mock `Upstream`.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedRequest {
    pub method: String,
    /// Request target (path plus query).
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Arrival time, to reason about delivery retry pacing.
    pub recorded_at: Instant,
}

impl RecordedRequest {
    /// Returns the first value of the named header (case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Programmable response served by the mock `Upstream`.
#[derive(Debug, Clone)]
pub struct UpstreamResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Artificial delay before responding.
    pub latency: Duration,
}

impl UpstreamResponse {
    /// A minimal `200 OK` response.
    pub fn ok(body: &str) -> Self {
        Self::with_status(200).with_body(body)
    }

    /// Sets an artificial delay before the response is sent.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    /// A response with only a status code and an empty body.
    pub fn with_status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: Vec::new(),
            latency: Duration::ZERO,
        }
    }

    /// A response with a text body.
    pub fn with_body(mut self, body: &str) -> Self {
        self.body = body.as_bytes().to_vec();
        self
    }
}

/// A lightweight local HTTP server that records every request it receives
/// and replies with a programmable response.
pub struct MockUpstream {
    pub addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    response: Arc<Mutex<ResponsePlan>>,
    path_responses: Arc<Mutex<Vec<(String, ResponsePlan)>>>,
}

/// The mock `Upstream` response schedule.
enum ResponsePlan {
    Single(UpstreamResponse),
    Sequence(Vec<UpstreamResponse>),
}

/// The distinctive healthcheck path injected into every test configuration,
/// so active probes can be told apart from forwarded traffic.
pub const PROBE_PATH: &str = "/healthcheck-probe";

impl ResponsePlan {
    fn next(&mut self) -> UpstreamResponse {
        match self {
            ResponsePlan::Single(response) => response.clone(),
            ResponsePlan::Sequence(responses) => {
                if responses.is_empty() {
                    UpstreamResponse::with_status(500)
                } else {
                    responses.remove(0)
                }
            }
        }
    }
}

impl MockUpstream {
    /// Starts the mock `Upstream` on an ephemeral localhost port.
    pub fn start(response: UpstreamResponse) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock upstream");
        listener
            .set_nonblocking(true)
            .expect("make mock upstream listener non-blocking");
        let addr = listener.local_addr().expect("read mock upstream address");

        let requests: Arc<Mutex<Vec<RecordedRequest>>> = Arc::default();
        let response = Arc::new(Mutex::new(ResponsePlan::Single(response)));
        let path_responses: Arc<Mutex<Vec<(String, ResponsePlan)>>> = Arc::default();

        let requests_for_task = Arc::clone(&requests);
        let response_for_task = Arc::clone(&response);
        let path_responses_for_task = Arc::clone(&path_responses);
        thread::Builder::new()
            .name("mock-upstream".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build mock upstream runtime");
                runtime.block_on(async move {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).expect("bind async listener");
                    loop {
                        let (stream, _) = listener.accept().await.expect("accept connection");
                        let requests = Arc::clone(&requests_for_task);
                        let response = Arc::clone(&response_for_task);
                        let path_responses = Arc::clone(&path_responses_for_task);
                        tokio::spawn(async move {
                            serve_connection(stream, requests, response, path_responses).await;
                        });
                    }
                });
            })
            .expect("spawn mock upstream thread");

        Self {
            addr,
            requests,
            response,
            path_responses,
        }
    }

    /// URL of the mock `Upstream` (`http://127.0.0.1:<port>`).
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// All requests recorded so far, in arrival order.
    pub fn recorded_requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().expect("lock requests").clone()
    }

    /// Replaces the response served to subsequent requests.
    pub fn set_response(&self, response: UpstreamResponse) {
        *self.response.lock().expect("lock response") = ResponsePlan::Single(response);
    }

    /// Replaces the response schedule served to requests whose path starts
    /// with `path_prefix`, shadowing the default schedule. Active health
    /// probes (which hit `PROBE_PATH`) keep following the default schedule,
    /// keeping response sequences deterministic for delivery tests.
    pub fn set_responses_for_path(&self, path_prefix: &str, responses: Vec<UpstreamResponse>) {
        let mut plans = self.path_responses.lock().expect("lock path responses");
        plans.retain(|(prefix, _)| prefix != path_prefix);
        plans.push((path_prefix.to_string(), ResponsePlan::Sequence(responses)));
    }

    /// Serves the given responses one per incoming request; after the
    /// sequence is exhausted, every request is answered with `500`.
    pub fn set_responses(&self, responses: Vec<UpstreamResponse>) {
        *self.response.lock().expect("lock responses") = ResponsePlan::Sequence(responses);
    }
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    response: Arc<Mutex<ResponsePlan>>,
    path_responses: Arc<Mutex<Vec<(String, ResponsePlan)>>>,
) {
    let service = service_fn(move |request: Request<hyper::body::Incoming>| {
        let requests = Arc::clone(&requests);
        let response = Arc::clone(&response);
        let path_responses = Arc::clone(&path_responses);
        async move {
            let (parts, body) = request.into_parts();
            let body = body
                .collect()
                .await
                .expect("collect request body")
                .to_bytes()
                .to_vec();

            let recorded = RecordedRequest {
                method: parts.method.to_string(),
                path: parts
                    .uri
                    .path_and_query()
                    .map(|target| target.as_str().to_string())
                    .unwrap_or_default(),
                headers: parts
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect(),
                body,
                recorded_at: Instant::now(),
            };

            let spec = {
                let path = recorded.path.as_str();
                let mut path_plans = path_responses.lock().expect("lock path responses");
                match path_plans
                    .iter_mut()
                    .find(|(prefix, _)| path.strip_prefix(prefix.as_str()).is_some())
                {
                    Some((_, plan)) => plan.next(),
                    None => {
                        let mut plan = response.lock().expect("lock responses");
                        plan.next()
                    }
                }
            };
            requests.lock().expect("lock requests").push(recorded);
            tokio::time::sleep(spec.latency).await;
            let mut builder = Response::builder().status(spec.status);
            for (name, value) in spec.headers {
                builder = builder.header(name, value);
            }
            builder.body(Full::new(Bytes::from(spec.body)))
        }
    });

    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

/// All requests recorded so far except active health probes (which hit
/// the distinctive `PROBE_PATH` configured into every test service).
pub fn non_probe_requests(upstream: &MockUpstream) -> Vec<RecordedRequest> {
    upstream
        .recorded_requests()
        .into_iter()
        .filter(|request| request.path != PROBE_PATH)
        .collect()
}

/// Picks a free localhost port by briefly binding to `127.0.0.1:0`.
pub fn ephemeral_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read ephemeral port").port()
}

/// Blocks until `addr` accepts TCP connections (the service is listening).
pub fn wait_until_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        if Instant::now() > deadline {
            panic!("service did not start listening on {addr} within 10s");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// The running service under test, addressed over plain HTTP.
pub struct TestApp {
    /// Data Plane base URL (`http://127.0.0.1:<port>`).
    pub data_plane_url: String,
    /// Admin Plane base URL (`http://127.0.0.1:<port>`).
    pub admin_url: String,
    /// Data Plane listen address (for rebuilding config YAML).
    pub data_plane_addr: SocketAddr,
    /// Admin Plane listen address (for rebuilding config YAML).
    pub admin_addr: SocketAddr,
}

impl TestApp {
    /// The Data Plane port (for rebuilding config YAML).
    pub fn data_plane_addr_port(&self) -> u16 {
        self.data_plane_addr.port()
    }

    /// The Admin Plane port (for rebuilding config YAML).
    pub fn admin_addr_port(&self) -> u16 {
        self.admin_addr.port()
    }
}

/// Starts the real service in-process from a YAML configuration string
/// and waits until both planes accept connections.
pub fn start_service(yaml: &str) -> TestApp {
    start_service_from_config(&write_config(yaml))
}

/// Starts the real service in-process from a YAML configuration file (the
/// same file the service watches for hot-reloads) and waits until both
/// planes accept connections.
pub fn start_service_from_config(config_path: &std::path::Path) -> TestApp {
    let yaml = std::fs::read_to_string(config_path).expect("read test config");
    let config =
        vestibule::config::Config::from_str(&yaml, "<test config>").expect("parse test config");
    let data_plane_addr = config.server.data_plane_addr;
    let admin_addr = config.server.admin_addr;

    let config_path = config_path.to_path_buf();
    let app_thread = thread::Builder::new()
        .name("vestibule-under-test".to_string())
        .spawn(move || {
            // Runs until the test process exits; errors would fail every test
            // through the readiness wait below.
            let _run_result = vestibule::app::run(config, config_path);
        })
        .expect("spawn service thread");

    let _ = app_thread; // the thread runs `run_forever` and never joins

    wait_until_listening(data_plane_addr);
    wait_until_listening(admin_addr);

    TestApp {
        data_plane_url: format!("http://{data_plane_addr}"),
        admin_url: format!("http://{admin_addr}"),
        data_plane_addr,
        admin_addr,
    }
}

/// Blocks until `condition` holds or a 10s deadline passes.
pub fn wait_until<F: Fn() -> bool>(condition: F) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if condition() {
            return;
        }
        if Instant::now() > deadline {
            panic!("condition was not met within 10 seconds");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// The service running as a real OS process (for restart-resilience tests).
pub struct ServiceProcess {
    child: std::process::Child,
    pub data_plane_url: String,
    pub admin_url: String,
}

impl ServiceProcess {
    /// Starts the built binary with the given config file and waits until
    /// both planes accept connections.
    pub fn start(config_path: &std::path::Path) -> Self {
        let binary = env!("CARGO_BIN_EXE_vestibule");
        // Keep service logs for post-mortem debugging of crash tests.
        let stderr_path = std::env::temp_dir().join("vestibule-service-stderr.log");
        let stderr_file = std::fs::File::create(&stderr_path).expect("create stderr log");
        let child = std::process::Command::new(binary)
            .arg("--config")
            .arg(config_path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::from(stderr_file))
            .spawn()
            .expect("spawn service process");

        let yaml = std::fs::read_to_string(config_path).expect("read test config");
        let (data_plane_url, admin_url) = urls_from_config(&yaml);
        wait_until_listening(
            data_plane_url
                .trim_start_matches("http://")
                .parse()
                .expect("parse data plane addr"),
        );
        wait_until_listening(
            admin_url
                .trim_start_matches("http://")
                .parse()
                .expect("parse admin addr"),
        );

        Self {
            child,
            data_plane_url,
            admin_url,
        }
    }

    /// Kills the process without any chance to flush gracefully.
    pub fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Sends the given POSIX signal (e.g. `"TERM"`, `"HUP"`) to the
    /// process.
    pub fn signal(&self, signal: &str) {
        let status = std::process::Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("send signal to service process");
        assert!(status.success(), "failed to send SIG{signal}");
    }

    /// Sends `SIGTERM`, triggering a graceful shutdown.
    pub fn sigterm(&self) {
        self.signal("TERM");
    }

    /// Sends `SIGHUP`, triggering a config reload.
    pub fn sighup(&self) {
        self.signal("HUP");
    }

    /// Waits for the process to exit and returns its exit code.
    pub fn wait(mut self) -> i32 {
        self.child
            .wait()
            .expect("wait for service exit")
            .code()
            .unwrap_or(-1)
    }
}

/// Extracts the configured plane addresses from a config YAML (tests only).
fn urls_from_config(yaml: &str) -> (String, String) {
    let mut data_plane = String::new();
    let mut admin = String::new();
    for line in yaml.lines() {
        let line = line.trim();
        if let Some(addr) = line.strip_prefix("data_plane_addr: ") {
            data_plane = addr.trim_matches('"').to_string();
        } else if let Some(addr) = line.strip_prefix("admin_addr: ") {
            admin = addr.trim_matches('"').to_string();
        }
    }
    (format!("http://{data_plane}"), format!("http://{admin}"))
}

/// Writes `yaml` to `config.yaml` inside a fresh directory and returns the
/// file path.
pub fn write_config(yaml: &str) -> std::path::PathBuf {
    let dir = tempdir();
    std::fs::create_dir_all(&dir).expect("create config dir");
    let path = dir.join("config.yaml");
    std::fs::write(&path, yaml).expect("write config");
    path
}

/// A blocking HTTP client for talking to the service under test.
pub fn http_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .build()
        .expect("build test http client")
}

/// One sample parsed from Prometheus text exposition.
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub name: String,
    pub labels: std::collections::HashMap<String, String>,
    pub value: f64,
}

/// Parses Prometheus text exposition into samples, skipping `#` comment
/// lines. Label values are unescaped.
pub fn parse_metrics(body: &str) -> Vec<MetricSample> {
    let mut samples = Vec::new();
    for line in body.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((metric, value)) = line.split_once(' ') else {
            continue;
        };
        let (name, labels) = match metric.strip_suffix('}') {
            Some(name_without_braces) if metric.contains('{') => {
                let name = name_without_braces
                    .split('{')
                    .next()
                    .unwrap_or_default()
                    .to_string();
                let label_source = metric
                    .split_once('{')
                    .and_then(|(_, labels)| labels.strip_suffix('}'))
                    .unwrap_or_default();
                let labels = label_source
                    .split(",")
                    .filter_map(|pair| {
                        let (label, value) = pair.split_once("=")?;
                        Some((
                            label.to_string(),
                            value
                                .trim_matches('"')
                                .replace("\\\\", "\\")
                                .replace("\\\"", "\"")
                                .replace("\\n", "\n"),
                        ))
                    })
                    .collect();
                (name, labels)
            }
            _ => (metric.to_string(), Default::default()),
        };
        samples.push(MetricSample {
            name,
            labels,
            value: value.parse().unwrap_or_default(),
        });
    }
    samples
}

/// The value of the sample with exactly the given labels, if present.
pub fn metric_value(samples: &[MetricSample], name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let wanted: std::collections::HashMap<_, _> = labels
        .iter()
        .map(|(label, value)| (label.to_string(), value.to_string()))
        .collect();
    samples
        .iter()
        .find(|sample| sample.name == name && sample.labels == wanted)
        .map(|sample| sample.value)
}

/// Fetches and parses the Admin Plane `/metrics` exposition.
pub fn scrape_metrics(app: &TestApp) -> Vec<MetricSample> {
    let response = http_client()
        .get(format!("{}/metrics", app.admin_url))
        .send()
        .expect("GET /metrics on the Admin Plane");
    assert_eq!(response.status().as_u16(), 200);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain")),
        "the exposition must be served as text/plain"
    );
    parse_metrics(&response.text().expect("read the exposition body"))
}

/// Minimal YAML configuration for the given Upstream URL and ports.
pub fn config_yaml(upstream_url: &str, data_plane_port: u16, admin_port: u16) -> String {
    config_yaml_with(upstream_url, data_plane_port, admin_port, "")
}

/// Full YAML configuration with extra sections appended after `queue`.
///
/// The `queue.dir` section always included points at a temporary directory
/// kept for the lifetime of the process. The healthcheck probes a
/// distinctive `PROBE_PATH` so tests can filter probes out of recorded
/// traffic.
pub fn config_yaml_with(
    upstream_url: &str,
    data_plane_port: u16,
    admin_port: u16,
    extra_sections: &str,
) -> String {
    config_yaml_for(
        upstream_url,
        data_plane_port,
        admin_port,
        &tempdir(),
        extra_sections,
    )
}

/// Full YAML configuration with an explicit `queue.dir` (kept stable across
/// rewritten configs in hot-reload tests).
pub fn config_yaml_for(
    upstream_url: &str,
    data_plane_port: u16,
    admin_port: u16,
    queue_dir: &std::path::Path,
    extra_sections: &str,
) -> String {
    format!(
        "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:{data_plane_port}\"\n\
         \x20 admin_addr: \"127.0.0.1:{admin_port}\"\n\
         upstream:\n\
         \x20 url: \"{upstream_url}\"\n\
         \x20 healthcheck:\n\
         \x20 \x20 path: \"{PROBE_PATH}\"\n\
         queue:\n\
         \x20 dir: \"{}\"\n{extra_sections}",
        queue_dir.display(),
    )
}

/// A temporary directory that is kept (not deleted) until process exit.
pub fn tempdir() -> std::path::PathBuf {
    tempfile::TempDir::new().expect("create tempdir").keep()
}
