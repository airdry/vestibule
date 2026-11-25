//! Composition root: wires the configuration, the reload manager, the
//! Data Plane, the Admin Plane, and the background services into a single
//! Pingora `Server`.
//!
//! Both `main` and the integration-test harness go through [`run`], so the
//! production binary and the service under test behave identically.

use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pingora::prelude::*;
use pingora::proxy::http_proxy_service_with_name;
use pingora::server::configuration::ServerConf;

use crate::admin_plane::{AdminPlane, StatusReport};
use crate::config::{Config, FsyncMode, shared_settings};
use crate::data_plane::DataPlaneProxy;
use crate::delivery::{DeliveryPool, RetryPolicy};
use crate::health::{HealthProber, UpstreamHealthTracker};
use crate::ingestion::{IngestionPipeline, lock_wal};
use crate::rate_limit::RateLimiter;
use crate::reload::{ConfigWatcher, ReloadManager, SighupReloader};
use crate::shutdown::{ShutdownFlag, WalFlusher};
use crate::wal::{SystemClock, Wal};

/// Errors while assembling or starting the server.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("failed to initialize the pingora server: {message}")]
    ServerInit { message: String },
    #[error("failed to open the ingestion queue: {message}")]
    QueueInit { message: String },
}

/// Runs the service on both planes until the process exits.
///
/// `config_path` is the YAML file watched for hot-reloads (`SIGHUP`, the
/// Admin Plane `POST /reload`, and filesystem changes all go through the
/// same reload manager).
///
/// # Errors
///
/// Returns an [`AppError`] when the Pingora server cannot be initialized or
/// the `Ingestion Queue` cannot be opened. Once the server is running, this
/// function never returns.
pub fn run(config: Config, config_path: PathBuf) -> Result<Infallible, AppError> {
    let shutdown_flag = ShutdownFlag::new();
    let mut server = Server::new_with_opt_and_conf(
        None,
        ServerConf {
            grace_period_seconds: Some(0),
            graceful_shutdown_timeout_seconds: Some(config.server.shutdown_timeout.as_secs()),
            ..ServerConf::default()
        },
    );
    server.bootstrap();

    let wal =
        Wal::open(&config.queue.dir, &config.queue, Arc::new(SystemClock)).map_err(|error| {
            AppError::QueueInit {
                message: error.to_string(),
            }
        })?;
    let wal = Arc::new(Mutex::new(wal));

    // The shared hot-reloadable settings snapshot, swapped by the reload
    // manager and read by every hot-path consumer.
    let shared = shared_settings(&config);
    let manager = Arc::new(ReloadManager::new(
        config_path,
        &config,
        Arc::clone(&shared),
    ));

    let rate_limiter = Arc::new(RateLimiter::new(Arc::new(SystemClock), Arc::clone(&shared)));
    let ingestion = IngestionPipeline::new(
        Arc::clone(&wal),
        Arc::clone(&rate_limiter),
        Arc::clone(&shared),
    );

    let health_tracker = Arc::new(UpstreamHealthTracker::new(Arc::clone(&shared)));

    let mut data_plane = http_proxy_service_with_name(
        &server.configuration,
        DataPlaneProxy::new(config.upstream.clone(), ingestion),
        "Data Plane",
    );
    data_plane.add_tcp(&config.server.data_plane_addr.to_string());

    let status = Arc::new(StatusReport::new(
        Arc::clone(&health_tracker),
        Arc::clone(&wal),
        &config,
        Arc::clone(&manager),
    ));
    let mut admin_plane = http_proxy_service_with_name(
        &server.configuration,
        AdminPlane::new(status, Arc::clone(&manager), shutdown_flag.clone()),
        "Admin Plane",
    );
    admin_plane.add_tcp(&config.server.admin_addr.to_string());

    let prober = HealthProber::new(
        config.upstream.clone(),
        Arc::clone(&shared),
        Arc::clone(&health_tracker),
    )
    .map_err(|message| AppError::ServerInit { message })?;
    server.add_service(background_service("Health Prober", prober));

    let delivery_worker = DeliveryPool::new(
        Arc::clone(&wal),
        config.upstream.clone(),
        RetryPolicy::default(),
        Arc::clone(&shared),
        health_tracker,
    )
    .map_err(|message| AppError::ServerInit { message })?;
    server.add_service(background_service("Delivery Worker", delivery_worker));

    if config.queue.fsync_mode == FsyncMode::Periodic {
        server.add_service(background_service(
            "Periodic Fsync",
            FsyncWorker::new(Arc::clone(&wal), config.queue.fsync_interval),
        ));
    }

    server.add_service(background_service(
        "Config Watcher",
        ConfigWatcher::new(Arc::clone(&manager)),
    ));
    server.add_service(background_service(
        "SIGHUP Reloader",
        SighupReloader::new(Arc::clone(&manager)),
    ));
    server.add_service(background_service(
        "Readiness Drainer",
        crate::shutdown::ReadinessDrainer::new(shutdown_flag),
    ));
    server.add_service(background_service(
        "WAL Flusher",
        WalFlusher::new(Arc::clone(&wal)),
    ));

    server.add_service(data_plane);
    server.add_service(admin_plane);

    server.run_forever()
}

/// Periodically fsyncs the `Ingestion Queue` while the server runs.
struct FsyncWorker {
    wal: Arc<Mutex<Wal>>,
    interval: Duration,
}

impl FsyncWorker {
    fn new(wal: Arc<Mutex<Wal>>, interval: Duration) -> Self {
        Self { wal, interval }
    }
}

#[async_trait]
impl pingora::services::background::BackgroundService for FsyncWorker {
    async fn start(&self, mut shutdown: pingora::server::ShutdownWatch) {
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(self.interval) => {}
            }
            if let Err(error) = lock_wal(&self.wal).sync() {
                eprintln!("fsync worker: failed to sync the ingestion queue: {error}");
            }
        }
    }
}
