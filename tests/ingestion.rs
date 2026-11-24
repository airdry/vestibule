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

/// Gzip-compresses a request body for the compressed-payload tests.
fn gzip_compress(data: &[u8]) -> Vec<u8> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip body");
    encoder.finish().expect("finish gzip")
}

/// Builds a multi-item envelope wire payload, filling item `length` fields
/// with the real body sizes.
fn envelope_bytes(header: &str, items: &[(&str, &[u8])]) -> Vec<u8> {
    let mut payload = format!("{header}\n").into_bytes();
    for (index, (item_header, body)) in items.iter().enumerate() {
        let item_header = item_header.replace("{LEN}", &body.len().to_string());
        payload.extend_from_slice(item_header.as_bytes());
        payload.push(b'\n');
        payload.extend_from_slice(body);
        if index + 1 < items.len() {
            payload.push(b'\n');
        }
    }
    payload
}

fn multi_item_envelope() -> Vec<u8> {
    envelope_bytes(
        "{\"event_id\":\"84df5a5f-51d8-409a-9416-a1adb4b0e3d0\"}",
        &[
            (
                "{\"type\":\"event\",\"length\":{LEN}}",
                br#"{"environment":"production","message":"boom"}"#,
            ),
            (
                "{\"type\":\"attachment\",\"length\":{LEN}}",
                b"\x00\x01\x02\x03",
            ),
        ],
    )
}

#[test]
fn multi_item_envelope_reports_event_id_and_passes_through_upstream() {
    let (app, upstream, _queue_dir) = start_with("");

    let response = http_client()
        .post(format!("{}/api/42/envelope/", app.data_plane_url))
        .header("Content-Type", "application/x-sentry-envelope")
        .body(multi_item_envelope())
        .send()
        .expect("POST envelope");

    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.text().expect("read response body"),
        "{\"id\":\"84df5a5f-51d8-409a-9416-a1adb4b0e3d0\"}",
        "accepted envelopes echo their header event_id"
    );

    support::wait_until(|| !upstream.recorded_requests().is_empty());
    let forwarded = &upstream.recorded_requests()[0];
    assert_eq!(forwarded.path, "/api/42/envelope/");
    assert_eq!(forwarded.body, multi_item_envelope());
}

#[test]
fn compressed_envelope_is_ingested_and_delivered_intact() {
    let (app, upstream, _queue_dir) = start_with("");
    let envelope = multi_item_envelope();
    let compressed = gzip_compress(&envelope);

    let response = http_client()
        .post(format!("{}/api/42/envelope/", app.data_plane_url))
        .header("Content-Encoding", "gzip")
        .body(compressed.clone())
        .send()
        .expect("POST compressed envelope");

    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.text().expect("read response body"),
        "{\"id\":\"84df5a5f-51d8-409a-9416-a1adb4b0e3d0\"}"
    );

    support::wait_until(|| !upstream.recorded_requests().is_empty());
    let forwarded = &upstream.recorded_requests()[0];
    assert_eq!(
        forwarded.body, compressed,
        "raw compressed bytes must be forwarded"
    );
    assert_eq!(forwarded.header("content-encoding"), Some("gzip"));
}

#[test]
fn decompressed_body_over_the_limit_is_rejected_with_413() {
    // A body that decompresses to 1 MB under a 16-byte limit.
    let (app, upstream, queue_dir) = start_with("ingest:\n  max_decompressed_body_bytes: 16\n");
    let compressed = gzip_compress(&[0; 1024 * 1024]);

    let response = http_client()
        .post(format!("{}/api/42/envelope/", app.data_plane_url))
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .send()
        .expect("POST zip bomb");

    assert_eq!(response.status().as_u16(), 413);
    assert!(upstream.recorded_requests().is_empty());
    let segments = std::fs::read_dir(&queue_dir)
        .expect("read queue dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
        .map(|entry| entry.metadata().expect("metadata").len())
        .sum::<u64>();
    assert_eq!(
        segments, 0,
        "oversized decompressed payloads must not be queued"
    );
}

#[test]
fn corrupt_compressed_body_fails_open_as_raw_bytes() {
    let (app, upstream, _queue_dir) = start_with("");
    let corrupted = {
        let compressed = gzip_compress(b"{}");
        compressed[..compressed.len() - 1].to_vec()
    };

    let response = http_client()
        .post(format!("{}/api/42/envelope/", app.data_plane_url))
        .header("Content-Encoding", "gzip")
        .body(corrupted.clone())
        .send()
        .expect("POST corrupted body");

    assert_eq!(response.status().as_u16(), 201);
    support::wait_until(|| !upstream.recorded_requests().is_empty());
    assert_eq!(upstream.recorded_requests()[0].body, corrupted);
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
