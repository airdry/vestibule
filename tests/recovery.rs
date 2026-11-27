//! End-to-end resilience tests: payloads accepted by the `Ingestion Queue`
//! survive a full process crash and are delivered once the `Upstream`
//! recovers, without duplicates.

mod support;

use std::time::Duration;

use support::{MockUpstream, ServiceProcess, UpstreamResponse, http_client, metric_value};

const PAYLOAD: &[u8] = b"{\"event_id\":\"aaa\"}";

/// Budget for waits that span a process restart, WAL recovery and delivery:
/// a loaded CI runner can exceed the default 10s.
const RESTART_BUDGET: Duration = Duration::from_secs(30);

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

/// Records still pending beyond the checkpoint, as reported by the Admin
/// Plane `queue_items` gauge.
fn pending_items(service: &ServiceProcess) -> Option<f64> {
    metric_value(
        &support::scrape_metrics_at(&service.admin_url),
        "queue_items",
        &[],
    )
}

#[test]
fn queued_records_survive_a_crash_and_are_delivered_after_recovery() {
    let upstream = MockUpstream::start(UpstreamResponse::with_status(500));
    let queue_dir = support::tempdir();

    let config_path = support::write_config(&recovery_config(
        &upstream.url(),
        &queue_dir.display().to_string(),
        support::ephemeral_port(),
        support::ephemeral_port(),
    ));

    let service = ServiceProcess::start(&config_path);
    post_payload(&service);

    // The worker must have attempted the failing Upstream before the crash,
    // so the record is claimed but not committed.
    support::wait_until_within(RESTART_BUDGET, || deliveries_of(&upstream, PAYLOAD) > 0);

    // Hard crash (SIGKILL) in the middle of its retry loop.
    service.kill();
    let attempts_before_crash = deliveries_of(&upstream, PAYLOAD);

    // The Upstream recovers; the service restarts with the same queue and
    // redelivers the uncommitted record.
    upstream.set_response(UpstreamResponse::ok("stored"));
    let restarted = ServiceProcess::start(&config_path);

    support::wait_until_within(RESTART_BUDGET, || {
        deliveries_of(&upstream, PAYLOAD) > attempts_before_crash
    });
    // A drained queue means the checkpoint commit is durable, so the record
    // can never be redelivered again.
    support::wait_until_within(RESTART_BUDGET, || pending_items(&restarted) == Some(0.0));
    assert_eq!(
        deliveries_of(&upstream, PAYLOAD),
        attempts_before_crash + 1,
        "the record must be delivered exactly once after recovery"
    );

    // A second restart must not redeliver the already-committed record: a
    // drained queue after the restart proves recovery found nothing pending.
    restarted.kill();
    let third = ServiceProcess::start(&config_path);
    support::wait_until_within(RESTART_BUDGET, || pending_items(&third) == Some(0.0));
    assert_eq!(
        deliveries_of(&upstream, PAYLOAD),
        attempts_before_crash + 1,
        "committed records must not be redelivered after restart"
    );
    third.kill();
}
