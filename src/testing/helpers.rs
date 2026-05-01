//! Integration test helper functions shared across all three test phases.
//!
//! Requires the `integration` feature — compiled only when running with
//! `SPARK_CTRL_INTEGRATION=1 cargo test --features integration`.

use super::IntegEnv;
use crate::client::LivyClient;
use crate::commands::session::{extract_text, open_session, run_sql};

/// Execute a single SQL statement against a fresh Livy session and return
/// the text/plain output as a `String`.  The session is deleted after use.
pub async fn sql(client: &LivyClient, env: &IntegEnv, sql_str: &str) -> String {
    let auth = env.profile().auth;
    let sid = open_session(client, "sql", &auth)
        .await
        .expect("failed to open Livy session");
    let result = run_sql(client, sid, sql_str, &auth)
        .await
        .expect("failed to run SQL statement");
    client.delete_session(sid, &auth).await.ok();
    extract_text(&result).unwrap_or_default()
}

/// Parse the first integer found in `text` (Livy `text/plain` output from
/// `SELECT COUNT(*)`).  Returns 0 if no integer is found.
pub fn parse_count(text: &str) -> u64 {
    text.lines()
        .filter_map(|l| l.trim().parse::<u64>().ok())
        .next()
        .unwrap_or(0)
}

/// Return the `operation` column from the most-recent snapshot of `table`.
///
/// Executes:
/// ```sql
/// SELECT operation FROM <table>.snapshots ORDER BY committed_at DESC LIMIT 1
/// ```
/// and returns the trimmed string value (e.g. `"append"`, `"overwrite"`,
/// `"delete"`).  Returns an empty string if no snapshots exist.
pub async fn latest_snapshot_op(client: &LivyClient, env: &IntegEnv, table: &str) -> String {
    let q = format!("SELECT operation FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1");
    let raw = sql(client, env, &q).await;
    // The Livy text/plain output has a header row followed by the value.
    // Skip any line that is literally "operation" (the column header).
    raw.lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && *l != "operation")
        .next()
        .unwrap_or("")
        .to_lowercase()
}
