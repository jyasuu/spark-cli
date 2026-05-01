//! Internal test modules.
//!
//! Compiled only during `cargo test` (or when the `integration` feature
//! is enabled for the docker-compose end-to-end suite).

pub mod client_unit;

#[cfg(feature = "integration")]
pub mod iceberg_snapshots;

#[cfg(feature = "integration")]
pub mod spark_curation;

#[cfg(feature = "integration")]
pub mod write_modes;
