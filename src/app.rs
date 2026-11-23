//! Composition root: wires the configuration, the Data Plane, and the Admin
//! Plane into a single Pingora `Server`.
//!
//! Both `main` and the integration-test harness go through [`run`], so the
//! production binary and the service under test behave identically.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use pingora::prelude::*;
use pingora::proxy::http_proxy_service_with_name;

use crate::admin_plane::AdminPlane;
use crate::config::Config;
use crate::data_plane::DataPlaneProxy;
use crate::ingestion::IngestionPipeline;
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
/// # Errors
///
/// Returns an [`AppError`] when the Pingora server cannot be initialized or
/// the `Ingestion Queue` cannot be opened. Once the server is running, this
/// function never returns.
pub fn run(config: Config) -> Result<Infallible, AppError> {
    let mut server = Server::new(None).map_err(|error| AppError::ServerInit {
        message: error.to_string(),
    })?;
    server.bootstrap();

    let wal =
        Wal::open(&config.queue.dir, &config.queue, Arc::new(SystemClock)).map_err(|error| {
            AppError::QueueInit {
                message: error.to_string(),
            }
        })?;
    let wal = Arc::new(Mutex::new(wal));
    let ingestion = IngestionPipeline::new(Arc::clone(&wal), config.ingest.clone());

    let mut data_plane = http_proxy_service_with_name(
        &server.configuration,
        DataPlaneProxy::new(config.upstream.clone(), ingestion),
        "Data Plane",
    );
    data_plane.add_tcp(&config.server.data_plane_addr.to_string());

    let mut admin_plane =
        http_proxy_service_with_name(&server.configuration, AdminPlane, "Admin Plane");
    admin_plane.add_tcp(&config.server.admin_addr.to_string());

    server.add_service(data_plane);
    server.add_service(admin_plane);

    server.run_forever()
}
