//! End-to-end tests for `Pass-Through Traffic` on the Data Plane:
//! non-ingestion requests are synchronously reverse-proxied to the
//! `Upstream` preserving method, path, headers, body, and status code.

mod support;

use rstest::rstest;

use support::{MockUpstream, UpstreamResponse, http_client};

/// Spins up a mock `Upstream` and the service on ephemeral ports.
fn start() -> (support::TestApp, MockUpstream) {
    start_with(UpstreamResponse::ok("upstream body"))
}

fn start_with(response: UpstreamResponse) -> (support::TestApp, MockUpstream) {
    let upstream = MockUpstream::start(response);
    let config = support::config_yaml(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
    );
    (support::start_service(&config), upstream)
}

#[test]
fn proxies_root_path_to_upstream_preserving_status_and_body() {
    let (app, upstream) = start();

    let response = http_client()
        .get(format!("{}/", app.data_plane_url))
        .send()
        .expect("GET / through the Data Plane");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.text().expect("read body"), "upstream body");

    let forwarded = upstream.recorded_requests();
    assert_eq!(forwarded.len(), 1);
    assert_eq!(forwarded[0].method, "GET");
    assert_eq!(forwarded[0].path, "/");
}

#[test]
fn preserves_upstream_error_status_codes() {
    let (app, upstream) = start_with(UpstreamResponse::with_status(503));

    let response = http_client()
        .get(format!("{}/api/0/relays/", app.data_plane_url))
        .send()
        .expect("GET through the Data Plane");

    assert_eq!(response.status().as_u16(), 503);
    assert!(upstream.recorded_requests().len() == 1);
}

#[test]
fn preserves_upstream_response_headers() {
    let mut response = UpstreamResponse::ok("body");
    response.headers = vec![("X-Sentry-Test".to_string(), "kept".to_string())];
    let (app, _upstream) = start_with(response);

    let sent = http_client()
        .get(format!("{}/", app.data_plane_url))
        .send()
        .expect("GET through the Data Plane");

    assert_eq!(sent.headers().get("x-sentry-test").unwrap(), "kept");
}

#[test]
fn forwards_request_headers_query_and_body() {
    let (app, upstream) = start();

    let response = http_client()
        .post(format!(
            "{}/api/0/relays/relay1/?sentry_key=abc123&sts=1",
            app.data_plane_url
        ))
        .header("X-Sentry-Auth", "Sentry sentry_key=abc123, project=42")
        .header("Content-Type", "application/json")
        .body(r#"{"relay_id": "relay1"}"#)
        .send()
        .expect("POST through the Data Plane");

    assert_eq!(response.status().as_u16(), 200);

    let forwarded = &upstream.recorded_requests()[0];
    assert_eq!(forwarded.method, "POST");
    assert_eq!(
        forwarded.path,
        "/api/0/relays/relay1/?sentry_key=abc123&sts=1"
    );
    assert_eq!(
        forwarded.header("x-sentry-auth"),
        Some("Sentry sentry_key=abc123, project=42")
    );
    assert_eq!(forwarded.header("content-type"), Some("application/json"));
    assert_eq!(forwarded.body, br#"{"relay_id": "relay1"}"#.to_vec());
}

#[test]
fn preserves_client_host_header() {
    let (app, upstream) = start();

    http_client()
        .get(format!("{}/js-sdk/bundle.min.js", app.data_plane_url))
        .header("Host", "sentry.example.com")
        .send()
        .expect("GET through the Data Plane");

    let forwarded = &upstream.recorded_requests()[0];
    assert_eq!(forwarded.header("host"), Some("sentry.example.com"));
}

#[rstest]
#[case("/")]
#[case("/_static/1234/app.css")]
#[case("/_assets/vendor.js")]
#[case("/js-sdk/bundle.min.js")]
#[case("/api/0/relays/relay1/")]
#[case("/api/0/organizations/sentry/projects/")]
#[case("/auth/login/sentry/")]
fn proxies_every_pass_through_path_untouched(#[case] path: &str) {
    let (app, upstream) = start();

    let response = http_client()
        .get(format!("{}{path}", app.data_plane_url))
        .send()
        .expect("GET through the Data Plane");

    assert_eq!(response.status().as_u16(), 200, "path {path}");
    let forwarded = &upstream.recorded_requests()[0];
    assert_eq!(forwarded.path, path);
}
