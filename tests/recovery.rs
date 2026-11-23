//! End-to-end resilience tests: payloads accepted by the `Ingestion Queue`
//! survive a full process crash and are delivered once the `Upstream`
//! recovers, without duplicates.

mod support;

use std::time::Duration;

use support::{MockUpstream, ServiceProcess, UpstreamResponse, http_client};

const PAYLOAD: &[u8] = b"{\"event_id\":\"aaa\"}";

fn recovery_config(upstream_url: &str, queue_dir: &str, port: u16, admin_port: u16) -> String {
    format!(
        "server:\n  data_plane_addr: \"127.0.0.1:{port}\"\n  admin_addr: \"127.0.0.1:{admin_port}\"\nupstream:\n  url: \"{upstream_url}\"\nqueue:\n  dir: \"{queue_dir}\"\n"
    )
}

fn post_payload(service: &ServiceProcess) {
    let response = http_client()
        .post(format!(
            "{}/api/42/envelope/?sentry_key=abc123",
            service.data_plane_url
        ))
        .header(
            "X-Sentry-Auth",
            "Sentry sentry_key=abc123, sentry_version=7",
        )
        .body(PAYLOAD.to_vec())
        .send()
        .expect("POST ingestion request");
    assert_eq!(response.status().as_u16(), 201);
}

fn deliveries_of(upstream: &MockUpstream, body: &[u8]) -> usize {
    upstream
        .recorded_requests()
        .into_iter()
        .filter(|request| request.body == body)
        .count()
}

#[test]
fn queued_records_survive_a_crash_and_are_delivered_after_recovery() {
    let upstream = MockUpstream::start(UpstreamResponse::with_status(500));
    let queue_dir = support::tempdir();

    let config_path = support::write_config(&recovery_config(
        &upstream.url(),
        &queue_dir.display().to_string(),
        49161,
        49162,
    ));

    let service = ServiceProcess::start(&config_path);
    post_payload(&service);
    // Give the delivery worker a chance to hit the failing Upstream first.
    std::thread::sleep(Duration::from_millis(300));

    // Hard crash (SIGKILL) in the middle of its retry loop.
    service.kill();

    // The Upstream recovers; the service restarts with the same queue.
    upstream.set_response(UpstreamResponse::ok("stored"));
    let restarted = ServiceProcess::start(&config_path);

    support::wait_until(|| deliveries_of(&upstream, PAYLOAD) > 0);
    let delivered = deliveries_of(&upstream, PAYLOAD);
    assert!(
        delivered >= 1,
        "the payload must be delivered after restart"
    );

    // No duplicate deliveries of the same record over time.
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(
        deliveries_of(&upstream, PAYLOAD),
        delivered,
        "the record must not be delivered more than once after the 2xx"
    );

    // A second restart must not redeliver the already-committed record.
    let count_after_first_delivery = deliveries_of(&upstream, PAYLOAD);
    restarted.kill();
    let third = ServiceProcess::start(&config_path);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        deliveries_of(&upstream, PAYLOAD),
        count_after_first_delivery,
        "committed records must not be redelivered after restart"
    );
    third.kill();
}
