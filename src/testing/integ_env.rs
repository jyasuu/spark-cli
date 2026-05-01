//! Integration test environment helper.
//!
//! Reads docker-compose service coordinates from well-known env vars and
//! exposes them as a typed struct.  When the vars are absent the test
//! infrastructure should call `skip!()` so the suite degrades gracefully
//! on developer machines that haven't run `docker compose up`.
//!
//! # Environment variables
//!
//! | Variable       | Default (docker-compose)      | Description                       |
//! |----------------|-------------------------------|-----------------------------------|
//! | `LIVY_URL`     | `http://localhost:8998`       | Apache Livy REST endpoint         |
//! | `ICEBERG_URL`  | `http://localhost:8181`       | Iceberg REST catalog              |
//! | `MINIO_URL`    | `http://localhost:9000`       | MinIO S3 endpoint                 |
//! | `POSTGRES_DSN` | `host=localhost ...`          | Postgres connection string        |
//! | `SPARK_THRIFT` | `localhost:10000`             | Spark Thrift Server (host:port)   |
//!
//! The `SPARK_CTRL_INTEGRATION` env var **must** be set to `1` for any
//! integration test to proceed; this prevents accidental runs against a
//! production cluster.

use crate::config::Profile;
use crate::testing::test_profile_with_thrift;

/// Coordinates for a live docker-compose integration stack.
#[derive(Debug, Clone)]
pub struct IntegEnv {
    /// Apache Livy REST URL (used as `master_url` in the CLI profile)
    pub livy_url: String,
    /// Iceberg REST catalog URL
    pub iceberg_url: String,
    /// MinIO S3 endpoint
    pub minio_url: String,
    /// Postgres DSN (libpq format)
    pub postgres_dsn: String,
    /// Spark Thrift Server address, e.g. "localhost:10000"
    pub spark_thrift: String,
}

impl IntegEnv {
    /// Try to build from environment variables.
    ///
    /// Returns `None` when `SPARK_CTRL_INTEGRATION != "1"`, so callers can
    /// skip gracefully with:
    /// ```ignore
    /// let env = match IntegEnv::from_env() {
    ///     Some(e) => e,
    ///     None => { eprintln!("skipping — set SPARK_CTRL_INTEGRATION=1"); return; }
    /// };
    /// ```
    pub fn from_env() -> Option<Self> {
        if std::env::var("SPARK_CTRL_INTEGRATION").as_deref() != Ok("1") {
            return None;
        }
        Some(IntegEnv {
            livy_url: std::env::var("LIVY_URL")
                .unwrap_or_else(|_| "http://localhost:8998".to_string()),
            iceberg_url: std::env::var("ICEBERG_URL")
                .unwrap_or_else(|_| "http://localhost:8181".to_string()),
            minio_url: std::env::var("MINIO_URL")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            postgres_dsn: std::env::var("POSTGRES_DSN")
                .unwrap_or_else(|_| {
                    "host=localhost port=5432 dbname=shop user=app password=secret sslmode=disable"
                        .to_string()
                }),
            spark_thrift: std::env::var("SPARK_THRIFT")
                .unwrap_or_else(|_| "localhost:10000".to_string()),
        })
    }

    /// A CLI [`Profile`] pointing at the Livy and Thrift endpoints.
    pub fn profile(&self) -> Profile {
        let thrift_url = format!("jdbc:hive2://{}/default", self.spark_thrift);
        test_profile_with_thrift(&self.livy_url, &thrift_url)
    }

    /// Convenience: build a [`crate::client::LivyClient`] from this env.
    pub fn livy_client(&self) -> anyhow::Result<crate::client::LivyClient> {
        crate::client::LivyClient::new(&self.profile())
    }

    /// Check that the Livy server is reachable before running a test.
    /// Returns an error string (suitable for `skip!` message) if not.
    pub async fn check_livy(&self) -> Result<(), String> {
        let client = self.livy_client().map_err(|e| e.to_string())?;
        let auth = self.profile().auth;
        client
            .health(&auth)
            .await
            .map_err(|e| format!("Livy unreachable at {}: {}", self.livy_url, e))?;
        Ok(())
    }

    /// The S3 bucket that iceberg-init writes to (matches docker-compose defaults).
    pub fn warehouse_bucket(&self) -> &str {
        "warehouse"
    }

    /// The Iceberg namespace seeded by iceberg-init (matches sync-init.yaml).
    pub fn demo_namespace(&self) -> &str {
        "demo"
    }
}
