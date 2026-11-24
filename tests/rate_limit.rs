mod support;

use std::time::Duration;

use support::{MockUpstream, UpstreamResponse, http_client};

/// Starts the service with the given `rate_limits` YAML section and a mock
/// `Upstream`.
fn start_with_rate_limits(rate_limits: &str) -> (support::TestApp, MockUpstream) {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        &format!("rate_limits:\n{rate_limits}"),
    );
    (support::start_service(&config), upstream)
}

const PER_PROJECT_RULES: &str = "  max_buckets: 100\n\
                                  \x20 bucket_ttl: 10m\n\
                                  \x20 defaults:\n\
                                  \x20 \x20 event: {rate: 1, burst: 2}\n\
                                  \x20 projects:\n\
                                  \x20 \x20 42:\n\
                                  \x20 \x20 \x20 event: {rate: 1, burst: 2}\n\
                                  \x20 \x20 \x20 transaction: {rate: 1, burst: 1}\n\
                                  \x20 \x20 7:\n\
                                  \x20 \x20 \x20 event: {rate: 0.5, burst: 1}\n";

/// Builds an envelope with an event item and two transaction items in
/// `production`: per the burst-1 transaction rule the second transaction
/// is throttled while the event passes.
fn partial_envelope() -> Vec<u8> {
    let mut payload = b"{\"event_id\":\"84df5a5f-51d8-409a-9416-a1adb4b0e3d0\"}\n".to_vec();
    payload.extend_from_slice(
        b"{\"type\":\"event\",\"length\":34}\n{\"environment\":\"production\",\"m\":1}\n",
    );
    payload.extend_from_slice(
        b"{\"type\":\"transaction\",\"length\":34}\n{\"environment\":\"production\",\"t\":1}\n",
    );
    payload.extend_from_slice(
        b"{\"type\":\"transaction\",\"length\":34}\n{\"environment\":\"production\",\"t\":2}\n",
    );
    payload
}

fn post_body(app: &support::TestApp, path: &str, body: Vec<u8>) -> reqwest::blocking::Response {
    http_client()
        .post(format!("{}{path}", app.data_plane_url))
        .header("Content-Type", "application/x-sentry-envelope")
        .body(body)
        .send()
        .expect("POST ingestion request")
}

#[test]
fn all_items_rate_limited_receives_429_with_headers() {
    let (app, upstream) = start_with_rate_limits(PER_PROJECT_RULES);

    // Two single-item store events exhaust the project-42 event bucket.
    for _ in 0..2 {
        let response = post_body(
            &app,
            "/api/42/store/",
            br#"{"environment":"production"}"#.to_vec(),
        );
        assert_eq!(response.status().as_u16(), 201);
    }

    let response = post_body(
        &app,
        "/api/42/store/",
        br#"{"environment":"production"}"#.to_vec(),
    );

    assert_eq!(response.status().as_u16(), 429);
    assert_eq!(response.headers().get("Retry-After").unwrap(), "1");
    let limits = response
        .headers()
        .get("X-Sentry-Rate-Limits")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(limits.contains(":event:42"), "got {limits:?}");
    // Nothing was enqueued for the throttled request.
    support::wait_until(|| upstream.recorded_requests().len() >= 2);
    assert_eq!(upstream.recorded_requests().len(), 2);
}

#[test]
fn partially_throttled_envelope_strips_items_and_reports_limits() {
    let (app, upstream) = start_with_rate_limits(PER_PROJECT_RULES);

    // Burst 1 for transactions: the first transaction consumes it, so the
    // second transaction item in the same envelope is throttled while the
    // event item passes.
    let response = post_body(&app, "/api/42/envelope/", partial_envelope());

    assert_eq!(response.status().as_u16(), 201);
    let limits = response
        .headers()
        .get("X-Sentry-Rate-Limits")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(limits.contains("transaction"), "got {limits:?}");

    // Only the kept items are enqueued: the upstream receives the rebuilt
    // envelope without the throttled second transaction.
    support::wait_until(|| !upstream.recorded_requests().is_empty());
    let body = String::from_utf8(upstream.recorded_requests()[0].body.clone()).unwrap();
    assert!(body.contains("\"m\":1"), "event item missing: {body:?}");
    assert!(
        body.contains("\"t\":1"),
        "first transaction missing: {body:?}"
    );
    assert!(!body.contains("\"t\":2"), "throttled item leaked: {body:?}");
}

#[test]
fn environment_scoped_rules_apply_through_hierarchy() {
    let (app, _upstream) = start_with_rate_limits(
        "  max_buckets: 100\n\
         \x20 defaults:\n\
         \x20 \x20 environments:\n\
         \x20 \x20 \x20 production:\n\
         \x20 \x20 \x20 \x20 event: {rate: 1, burst: 1}\n",
    );

    // First event in production passes; the second is throttled.
    let body = br#"{"environment":"production"}"#.to_vec();
    assert_eq!(
        post_body(&app, "/api/9/store/", body.clone())
            .status()
            .as_u16(),
        201
    );
    let throttled = post_body(&app, "/api/9/store/", body);
    assert_eq!(throttled.status().as_u16(), 429);

    // Other environments are unlimited.
    let other = br#"{"environment":"staging"}"#.to_vec();
    assert_eq!(
        post_body(&app, "/api/9/store/", other).status().as_u16(),
        201
    );
}

#[test]
fn global_store_route_falls_back_to_defaults() {
    let (app, _upstream) = start_with_rate_limits(PER_PROJECT_RULES);

    // `/api/store/` has no project id: defaults apply (event burst 2).
    let body = br#"{"environment":"production"}"#.to_vec();
    assert_eq!(
        post_body(&app, "/api/store/", body.clone())
            .status()
            .as_u16(),
        201
    );
    assert_eq!(
        post_body(&app, "/api/store/", body.clone())
            .status()
            .as_u16(),
        201
    );
    assert_eq!(post_body(&app, "/api/store/", body).status().as_u16(), 429);
}

#[test]
fn bucket_refills_over_time() {
    let (app, _upstream) = start_with_rate_limits(PER_PROJECT_RULES);

    let body = br#"{"environment":"production"}"#.to_vec();
    // Exhaust project 7's event bucket (rate 0.5 RPS, burst 1).
    assert_eq!(
        post_body(&app, "/api/7/store/", body.clone())
            .status()
            .as_u16(),
        201
    );
    let throttled = post_body(&app, "/api/7/store/", body.clone());
    assert_eq!(throttled.status().as_u16(), 429);

    // At 0.5 RPS one token refills after 2s; allow generous slack.
    std::thread::sleep(Duration::from_millis(3500));
    assert_eq!(
        post_body(&app, "/api/7/store/", body).status().as_u16(),
        201
    );
}

#[test]
fn high_cardinality_environments_cannot_oom_the_bucket_table() {
    let (app, _upstream) = start_with_rate_limits(
        "  max_buckets: 2\n\
         \x20 bucket_ttl: 10m\n\
         \x20 defaults:\n\
         \x20 \x20 event: {rate: 1, burst: 1}\n",
    );

    // Fill the tiny table with three distinct environments.
    for env in ["one", "two", "three"] {
        let body = format!("{{\"environment\":\"{env}\"}}").into_bytes();
        assert_eq!(
            post_body(&app, "/api/42/store/", body).status().as_u16(),
            201
        );
    }

    // The first environment's bucket was evicted (LRU); its next request
    // starts with a fresh full bucket instead of the throttled state it
    // would have kept if the table were unbounded.
    let body = br#"{"environment":"one"}"#.to_vec();
    assert_eq!(
        post_body(&app, "/api/42/store/", body).status().as_u16(),
        201
    );
}

#[test]
fn unconfigured_categories_stay_unlimited() {
    let (app, _upstream) = start_with_rate_limits(PER_PROJECT_RULES);

    // `other` is unlimited by default: many requests still succeed.
    let body = br#"{"environment":"production"}"#.to_vec();
    for _ in 0..5 {
        assert_eq!(
            post_body(&app, "/api/13/security/", body.clone())
                .status()
                .as_u16(),
            201
        );
    }
}
