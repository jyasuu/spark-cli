//! Integration test helper functions shared across all three test phases.
//!
//! Requires the `integration` feature — compiled only when running with
//! `SPARK_CTRL_INTEGRATION=1 cargo test --features integration`.

use super::IntegEnv;
use crate::client::LivyClient;
use crate::commands::session::{extract_text, open_session, run_sql};

/// Execute a single SQL statement against a fresh Livy session and return
/// the text/plain output as a `String`.  The session is deleted after use.
///
/// Uses `kind = "pyspark"` rather than `"sql"` so that the session gets a
/// full `SparkSession` where `spark.sql.catalog.*` conf keys — including the
/// Iceberg REST catalog registration — are honoured.  A bare Livy `"sql"`
/// session uses `SQLContext`/`HiveContext` which ignores those keys, causing
/// `demo.products.snapshots` and similar Iceberg metadata tables to return
/// empty results.
pub async fn sql(client: &LivyClient, env: &IntegEnv, sql_str: &str) -> String {
    let auth = env.profile().auth;
    let sid = open_session(client, "pyspark", &auth)
        .await
        .expect("failed to open Livy session");
    // Escape backslashes and single quotes before embedding the SQL in the
    // Python string literal.
    let escaped = sql_str.replace('\\', "\\\\").replace('\'', "\\'");
    let code = format!(
        "_rows = spark.sql('{escaped}').collect(); \
         '\\n'.join(['\\t'.join(str(v) for v in r) for r in _rows]) if _rows else ''"
    );
    let result = run_sql(client, sid, &code, &auth)
        .await
        .expect("failed to run SQL statement");
    client.delete_session(sid, &auth).await.ok();
    extract_text(&result).unwrap_or_default()
}

/// Parse the first integer found in `text` (tab-separated collect output from
/// a `SELECT COUNT(*)` query).
///
/// The pyspark collect snippet emits one tab-separated row per line, e.g.:
/// ```text
/// 3
/// ```
/// We try each field in every line until one parses as u64.
pub fn parse_count(text: &str) -> u64 {
    text.lines()
        .flat_map(|l| l.split('\t'))
        .filter_map(|cell| cell.trim().parse::<u64>().ok())
        .next()
        .unwrap_or(0)
}

/// Return the `operation` column from the most-recent snapshot of `table`.
///
/// Executes:
/// ```sql
/// SELECT operation FROM <table>.snapshots ORDER BY committed_at DESC LIMIT 1
/// ```
/// Output is a single tab-separated row, e.g. `"append"`.
pub async fn latest_snapshot_op(client: &LivyClient, env: &IntegEnv, table: &str) -> String {
    let q = format!("SELECT operation FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1");
    let raw = sql(client, env, &q).await;
    // Single-column query → the entire first line is the value.
    raw.lines()
        .flat_map(|l| l.split('\t'))
        .map(|cell| cell.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .next()
        .unwrap_or_default()
}