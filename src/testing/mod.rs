//! Testing utilities shared by unit tests (wiremock) and integration tests
//! (real docker-compose services).
//!
//! # Unit tests
//! Use [`MockLivy`] to spin up a lightweight in-process HTTP server that
//! returns canned Livy JSON.  No docker, no network, no external deps.
//!
//! # Integration tests  (feature = "integration")
//! Use [`IntegEnv`] which reads service URLs from environment variables set
//! by the CI docker-compose stack:
//!
//! ```text
//! LIVY_URL      http://localhost:8998
//! THRIFT_HOST   localhost
//! THRIFT_PORT   10000
//! ICEBERG_URL   http://localhost:8181
//! MINIO_URL     http://localhost:9000
//! ```
//!
//! If the variables are absent the integration test calls `skip!()` so a plain
//! `cargo test` always passes.

pub mod mock_livy;

#[cfg(feature = "integration")]
pub mod integ_env;

#[cfg(feature = "integration")]
pub mod helpers;

// ── re-exports ──────────────────────────────────────────────────────────────

pub use mock_livy::MockLivy;

#[cfg(feature = "integration")]
pub use integ_env::IntegEnv;

#[cfg(feature = "integration")]
pub use helpers::{parse_count, sql as run_sql, latest_snapshot_op};

// ── shared profile builder ──────────────────────────────────────────────────

use crate::config::{Auth, Backend, Profile};
use std::collections::HashMap;

/// Build a minimal [`Profile`] pointing at `master_url`.
/// Used by both mock-based unit tests and integration tests.
pub fn test_profile(master_url: &str) -> Profile {
    Profile {
        backend:     Backend::Livy,
        master_url:  master_url.to_string(),
        thrift_url:  None,
        namespace:   None,
        description: None,
        auth:        Auth::default(),
        spark_conf:  HashMap::new(),
        aliases:     HashMap::new(),
    }
}

/// Build a profile with a Thrift URL set (needed for SQL inspection tests).
pub fn test_profile_with_thrift(master_url: &str, thrift_url: &str) -> Profile {
    Profile {
        thrift_url: Some(thrift_url.to_string()),
        ..test_profile(master_url)
    }
}
