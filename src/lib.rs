//! `sentry-ratelimiter`: an HTTP rate-limiting and buffering proxy that sits
//! in front of a self-hosted Sentry `Upstream`.
//!
//! This crate is exposed as a library so integration tests can start the
//! service in-process; the binary entry point lives in `src/main.rs`.

pub mod admin_plane;
pub mod app;
pub mod config;
pub mod data_plane;
pub mod wal;
