//! End-to-end tests for the Admin Plane: Kubernetes probes run on their own
//! listener, independent of the Data Plane.

mod support;

use support::{MockUpstream, UpstreamResponse, http_client};

fn start() -> (support::TestApp, MockUpstream) {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let config = support::config_yaml(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
    );
    (support::start_service(&config), upstream)
}

#[test]
fn healthz_returns_200_once_initialized() {
    let (app, _upstream) = start();

    let response = http_client()
        .get(format!("{}/healthz", app.admin_url))
        .send()
        .expect("GET /healthz on the Admin Plane");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.text().expect("read body"), "ok\n");
}

#[test]
fn readyz_returns_200_once_initialized() {
    let (app, _upstream) = start();

    let response = http_client()
        .get(format!("{}/readyz", app.admin_url))
        .send()
        .expect("GET /readyz on the Admin Plane");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.text().expect("read body"), "ok\n");
}

#[test]
fn unknown_admin_path_returns_404() {
    let (app, _upstream) = start();

    let response = http_client()
        .get(format!("{}/nope", app.admin_url))
        .send()
        .expect("GET /nope on the Admin Plane");

    assert_eq!(response.status().as_u16(), 404);
}

#[test]
fn admin_plane_does_not_proxy_to_upstream() {
    let (app, upstream) = start();

    http_client()
        .get(format!("{}/healthz", app.admin_url))
        .send()
        .expect("GET /healthz on the Admin Plane");

    assert!(
        upstream
            .recorded_requests()
            .iter()
            .all(|request| request.path != "/healthz"),
        "admin probes must not be forwarded to the Upstream"
    );
}
