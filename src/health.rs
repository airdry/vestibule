//! `Upstream Health` tracking: hysteresis state machine plus the active
//! prober background service.
//!
//! `UpstreamHealthTracker` flips between `Healthy` and `Unhealthy` using
//! consecutive-success/failure hysteresis thresholds. Active probe results
//! come from the `HealthProber`; delivery workers accelerate the transition
//! to `Unhealthy` by passively reporting transport, timeout and
//! `502`/`503`/`504` failures. While `Unhealthy`, delivery workers pause
//! outbound requests until probes restore `Healthy` (ADR-0003 keeps
//! `/readyz` independent of this state).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use tokio::sync::watch;

use crate::config::{HealthcheckSettings, SharedSettings, UpstreamTarget};

/// The tracked availability state of the `Upstream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamHealth {
    Healthy,
    Unhealthy,
}

impl UpstreamHealth {
    /// `true` for [`UpstreamHealth::Healthy`].
    pub fn is_healthy(self) -> bool {
        matches!(self, Self::Healthy)
    }
}

/// Hysteresis counters backing `Upstream Health` transitions.
#[derive(Debug, Default)]
struct Hysteresis {
    consecutive_failures: u32,
    consecutive_successes: u32,
}

/// Shared `Upstream Health` state, updated by active probes and passive
/// delivery failures, observable synchronously (`is_healthy`) and
/// asynchronously (`wait_healthy`).
///
/// The hysteresis thresholds are read from the shared runtime settings
/// snapshot on every observation, so healthcheck reloads take effect
/// immediately.
pub struct UpstreamHealthTracker {
    settings: SharedSettings,
    counters: Mutex<Hysteresis>,
    healthy: watch::Sender<bool>,
    /// Kept alive so the watch channel never closes: `Sender::send`
    /// silently fails once all receivers are dropped.
    healthy_rx: watch::Receiver<bool>,
}

impl UpstreamHealthTracker {
    /// Builds a tracker that starts `Healthy`.
    pub fn new(settings: SharedSettings) -> Self {
        let (healthy, healthy_rx) = watch::channel(true);
        Self {
            settings,
            counters: Mutex::new(Hysteresis::default()),
            healthy,
            healthy_rx,
        }
    }

    /// The current `Upstream Health`.
    pub fn health(&self) -> UpstreamHealth {
        if self.is_healthy() {
            UpstreamHealth::Healthy
        } else {
            UpstreamHealth::Unhealthy
        }
    }

    /// `true` while `Upstream Health` is `Healthy`.
    pub fn is_healthy(&self) -> bool {
        *self.healthy.borrow()
    }

    /// A receiver announcing every `Upstream Health` transition.
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.healthy_rx.clone()
    }

    /// Records a successful active probe: resets the failure streak while
    /// `Healthy`, counts toward `healthy_threshold` while `Unhealthy`.
    pub fn report_probe_success(&self) {
        self.report(true);
    }

    /// Records a failed active probe.
    pub fn report_probe_failure(&self) {
        self.report(false);
    }

    /// Records a passive delivery failure (transport error, timeout, or
    /// `502`/`503`/`504`), accelerating the transition to `Unhealthy`.
    pub fn report_passive_failure(&self) {
        self.report(false);
    }

    /// Resumes as soon as `Upstream Health` is `Healthy`. Returns `false`
    /// when shutdown is requested first.
    pub async fn wait_healthy(&self, shutdown: &mut ShutdownWatch) -> bool {
        let mut healthy = self.healthy.subscribe();
        loop {
            if *healthy.borrow() {
                return true;
            }
            tokio::select! {
                changed = healthy.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                _ = shutdown.changed() => return false,
            }
        }
    }

    /// Applies one observation to the hysteresis counters, flipping the
    /// state when a threshold is reached.
    fn report(&self, healthy_observation: bool) {
        let snapshot = self.settings.load();
        let unhealthy_threshold = snapshot.healthcheck.unhealthy_threshold;
        let healthy_threshold = snapshot.healthcheck.healthy_threshold;
        let flip = {
            let mut counters = self
                .counters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if healthy_observation {
                if self.is_healthy() {
                    counters.consecutive_failures = 0;
                    None
                } else {
                    counters.consecutive_successes += 1;
                    (counters.consecutive_successes >= healthy_threshold).then_some(true)
                }
            } else if self.is_healthy() {
                counters.consecutive_failures += 1;
                (counters.consecutive_failures >= unhealthy_threshold).then_some(false)
            } else {
                counters.consecutive_successes = 0;
                None
            }
        };
        // The guard is released: `transition` may lock the counters again.
        if let Some(healthy) = flip {
            self.transition(healthy);
        }
    }

    /// Flips the shared state and announces it to all waiters.
    fn transition(&self, healthy: bool) {
        *self
            .counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Hysteresis::default();
        let _ = self.healthy.send(healthy);
        crate::log_info!(
            "health",
            "Upstream Health transitioned to {}",
            if healthy { "Healthy" } else { "Unhealthy" }
        );
    }
}

/// Periodically GETs the `Upstream` healthcheck endpoint and feeds the
/// results into an [`UpstreamHealthTracker`].
///
/// Probe settings (path, expected statuses, interval, timeout) are read
/// from the shared runtime settings snapshot on every probe, so healthcheck
/// reloads apply to the next probe.
pub struct HealthProber {
    client: reqwest::Client,
    target: UpstreamTarget,
    settings: SharedSettings,
    tracker: Arc<UpstreamHealthTracker>,
}

impl HealthProber {
    /// Builds the prober with a client for the `Upstream`.
    ///
    /// # Errors
    ///
    /// Returns a message when the HTTP client cannot be constructed.
    pub fn new(
        target: UpstreamTarget,
        settings: SharedSettings,
        tracker: Arc<UpstreamHealthTracker>,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|error| format!("failed to build the healthcheck client: {error}"))?;
        Ok(Self {
            client,
            target,
            settings,
            tracker,
        })
    }

    /// Performs one probe and reports it to the tracker.
    async fn probe_once(&self) {
        let snapshot = self.settings.load();
        let healthcheck = &snapshot.healthcheck;
        let healthy = match self
            .client
            .get(probe_url(&self.target, healthcheck))
            .timeout(healthcheck.timeout)
            .send()
            .await
        {
            Ok(response) => healthcheck
                .expected_statuses
                .contains(&response.status().as_u16()),
            Err(error) => {
                crate::log_debug!("health", "probe request failed: {error}");
                false
            }
        };
        if healthy {
            self.tracker.report_probe_success();
        } else {
            self.tracker.report_probe_failure();
        }
    }
}

#[async_trait]
impl BackgroundService for HealthProber {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        loop {
            self.probe_once().await;
            let interval = self.settings.load().healthcheck.interval;
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }
}

/// The URL of one active probe: `Upstream` origin + path prefix +
/// healthcheck path.
fn probe_url(target: &UpstreamTarget, healthcheck: &HealthcheckSettings) -> String {
    format!(
        "{}{}{}",
        target.base_url(),
        target.path_prefix,
        healthcheck.path
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::config::Config;

    fn tracker(yaml_healthcheck: &str) -> UpstreamHealthTracker {
        let yaml = format!(
            "server:\n\
             \x20 data_plane_addr: \"127.0.0.1:18080\"\n\
             \x20 admin_addr: \"127.0.0.1:18081\"\n\
             upstream:\n\
             \x20 url: \"http://127.0.0.1:18082\"\n\
             \x20 healthcheck:\n{yaml_healthcheck}"
        );
        let config = Config::from_str(&yaml, "<test config>").expect("parse test config");
        UpstreamHealthTracker::new(crate::config::shared_settings(&config))
    }

    /// Tracker with the given hysteresis thresholds.
    fn simple_tracker(unhealthy_threshold: u32, healthy_threshold: u32) -> UpstreamHealthTracker {
        tracker(&format!(
            "\x20 \x20 unhealthy_threshold: {unhealthy_threshold}\n\
             \x20 \x20 healthy_threshold: {healthy_threshold}\n"
        ))
    }

    fn upstream_target(yaml_upstream: &str) -> UpstreamTarget {
        let yaml = format!(
            "server:\n\
             \x20 data_plane_addr: \"127.0.0.1:18080\"\n\
             \x20 admin_addr: \"127.0.0.1:18081\"\n\
             upstream:\n\
             {yaml_upstream}"
        );
        Config::from_str(&yaml, "<test config>")
            .expect("parse test config")
            .upstream
    }

    #[test]
    fn starts_healthy() {
        assert!(simple_tracker(3, 2).is_healthy());
        assert_eq!(simple_tracker(3, 2).health(), UpstreamHealth::Healthy);
    }

    #[test]
    fn flips_unhealthy_after_the_failure_threshold() {
        let tracker = simple_tracker(3, 2);
        tracker.report_probe_failure();
        tracker.report_probe_failure();
        assert!(
            tracker.is_healthy(),
            "two failures stay under the threshold of three"
        );
        tracker.report_passive_failure();
        assert_eq!(
            tracker.health(),
            UpstreamHealth::Unhealthy,
            "passive failures count toward the threshold"
        );
    }

    #[test]
    fn success_resets_the_failure_streak_while_healthy() {
        let tracker = simple_tracker(2, 2);
        tracker.report_probe_failure();
        tracker.report_probe_success();
        tracker.report_passive_failure();
        assert!(tracker.is_healthy());
    }

    #[test]
    fn flips_healthy_after_the_success_threshold() {
        let tracker = simple_tracker(1, 2);
        tracker.report_probe_failure();
        tracker.report_probe_success();
        assert!(
            !tracker.is_healthy(),
            "one success stays under the threshold of two"
        );
        tracker.report_probe_success();
        assert_eq!(tracker.health(), UpstreamHealth::Healthy);
    }

    #[test]
    fn failure_resets_the_success_streak_while_unhealthy() {
        let tracker = simple_tracker(1, 2);
        tracker.report_probe_failure();
        tracker.report_probe_success();
        tracker.report_passive_failure();
        tracker.report_probe_success();
        assert!(
            !tracker.is_healthy(),
            "a passive failure resets the recovery streak"
        );
        tracker.report_probe_success();
        assert!(tracker.is_healthy());
    }

    #[tokio::test]
    async fn wait_healthy_resumes_on_the_transition_to_healthy() {
        let tracker = Arc::new(simple_tracker(1, 1));
        tracker.report_probe_failure();
        let flipper = Arc::clone(&tracker);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flipper.report_probe_success();
        });
        // Keep the shutdown sender alive: a dropped sender makes
        // `changed()` resolve immediately, which `wait_healthy` treats as
        // shutdown.
        let (_shutdown_tx, mut shutdown) = watch::channel(false);

        assert!(tracker.wait_healthy(&mut shutdown).await);
    }

    #[tokio::test]
    async fn wait_healthy_returns_on_shutdown() {
        let tracker = simple_tracker(1, 1);
        tracker.report_probe_failure();
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        shutdown_tx.send(true).expect("signal shutdown");

        assert!(!tracker.wait_healthy(&mut shutdown).await);
    }

    #[test]
    fn builds_probe_urls_from_the_upstream_target() {
        let target = upstream_target(
            "\x20 url: \"http://127.0.0.1:18082\"\n\x20 healthcheck:\n\x20 \x20 path: \"/probe\"\n",
        );
        assert_eq!(
            probe_url(&target, &target.healthcheck),
            "http://127.0.0.1:18082/probe"
        );

        let prefixed = upstream_target(
            "\x20 url: \"https://example.com:8443/sentry\"\n\x20 healthcheck:\n\x20 \x20 path: \"/\"\n",
        );
        assert_eq!(
            probe_url(&prefixed, &prefixed.healthcheck),
            "https://example.com:8443/sentry/"
        );
    }
}
