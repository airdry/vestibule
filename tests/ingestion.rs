//! End-to-end tests for `Ingestion Route` interception on the Data Plane:
//! matching requests are appended to the `Ingestion Queue` and acknowledged
//! immediately, while oversized payloads are rejected with `413`.

mod support;

use std::path::Path;

use support::{MockUpstream, UpstreamResponse, http_client};

/// Spins up a mock `Upstream` and the service with the given extra YAML
/// sections, returning the app and the queue directory in use.
fn start_with(extra_sections: &str) -> (support::TestApp, MockUpstream, std::path::PathBuf) {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        extra_sections,
    );
    let queue_dir = queue_dir_of(&config);
    (support::start_service(&config), upstream, queue_dir)
}

/// Extracts the `queue.dir` value from a config produced by the support
/// helpers.
fn queue_dir_of(yaml: &str) -> std::path::PathBuf {
    yaml.lines()
        .find_map(|line| line.trim_start().strip_prefix("dir: "))
        .expect("queue dir is configured")
        .trim_matches('"')
        .to_string()
        .into()
}

/// Asserts the queue directory contains a non-empty WAL segment.
fn assert_queue_has_records(queue_dir: &Path) {
    let segments = std::fs::read_dir(queue_dir)
        .expect("read queue dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
        .count();
    assert!(segments > 0, "expected WAL segments in {queue_dir:?}");
}

fn post_envelope(app: &support::TestApp, path: &str) -> reqwest::blocking::Response {
    http_client()
        .post(format!("{}{path}", app.data_plane_url))
        .header(
            "X-Sentry-Auth",
            "Sentry sentry_key=abc123, sentry_version=7, sentry_client=sdk/1.0",
        )
        .header("Content-Type", "application/json")
        .body(r#"{"event_id":"aaa"}"#)
        .send()
        .expect("POST ingestion request")
}

#[test]
fn ingestion_route_is_accepted_immediately_with_201() {
    let (app, upstream, queue_dir) = start_with("");

    let response = post_envelope(&app, "/api/42/envelope/");

    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.text().expect("read response body"),
        "{}",
        "accepted ingestion must return an empty JSON body"
    );
    assert!(
        upstream.recorded_requests().is_empty(),
        "ingestion must not reach Upstream synchronously"
    );
    assert_queue_has_records(&queue_dir);
}

#[test]
fn ingestion_success_status_code_is_configurable_to_200() {
    let (app, _upstream, _queue_dir) = start_with("ingest:\n  success_status_code: 200\n");

    let response = post_envelope(&app, "/api/42/envelope/");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.text().expect("read response body"), "{}");
}

#[test]
fn oversized_ingestion_body_is_rejected_with_413() {
    let (app, upstream, queue_dir) = start_with("ingest:\n  max_compressed_body_bytes: 16\n");

    let response = post_envelope(&app, "/api/42/envelope/");

    assert_eq!(response.status().as_u16(), 413);
    assert!(
        upstream.recorded_requests().is_empty(),
        "rejected payloads must not reach Upstream"
    );
    let segments = std::fs::read_dir(&queue_dir)
        .expect("read queue dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
        .map(|entry| entry.metadata().expect("metadata").len())
        .sum::<u64>();
    assert_eq!(segments, 0, "rejected payloads must not be written to disk");
}

#[test]
fn legacy_store_route_is_ingested() {
    let (app, upstream, _queue_dir) = start_with("");

    let response = post_envelope(&app, "/api/store/");

    assert_eq!(response.status().as_u16(), 201);
    assert!(upstream.recorded_requests().is_empty());
}

#[test]
fn ingestion_with_query_string_and_auth_is_accepted() {
    let (app, _upstream, _queue_dir) = start_with("");

    let response = http_client()
        .post(format!(
            "{}/api/42/store/?sentry_key=abc123",
            app.data_plane_url
        ))
        .body("{}")
        .send()
        .expect("POST with query");

    assert_eq!(response.status().as_u16(), 201);
}
