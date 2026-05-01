//! Integration tests: Phase 2 — Spark silver/gold curation.
//!
//! Requires docker-compose stack + `SPARK_CTRL_INTEGRATION=1`.
//!
//! These tests follow the patterns described in the HTML guide:
//!   • Silver layer: raw → cleaned + enriched via Spark SQL
//!   • Gold layer:  silver → aggregated daily_revenue with MERGE INTO
//!   • Compaction:  CALL rewrite_data_files() reduces physical file count

use crate::client::LivyClient;
use crate::commands::session::{extract_text, one_shot_sql};
use crate::testing::IntegEnv;

async fn sql(client: &LivyClient, env: &IntegEnv, query: &str) -> String {
    let auth = env.profile().auth;
    one_shot_sql(client, query, &auth)
        .await
        .and_then(|r| extract_text(&r))
        .unwrap_or_else(|e| format!("ERROR: {e}"))
}

// ── Test 1: build silver layer from raw orders ────────────────────────────────

#[tokio::test]
async fn silver_orders_view_has_same_count_as_raw() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => { eprintln!("skip: SPARK_CTRL_INTEGRATION not set"); return; }
    };
    if let Err(msg) = env.check_livy().await { eprintln!("skip: {msg}"); return; }

    let client = env.livy_client().unwrap();

    // Raw row count
    let raw_count_text = sql(&client, &env, "SELECT COUNT(*) FROM demo.orders").await;
    let raw_count: u64 = raw_count_text.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    // Create a temporary silver view — clean + non-null status
    sql(&client, &env,
        "CREATE OR REPLACE TEMP VIEW orders_silver AS \
         SELECT id, user_id, status, total_amount, \
                TO_DATE(created_at) AS order_date \
         FROM demo.orders \
         WHERE status IS NOT NULL"
    ).await;

    let silver_count_text = sql(&client, &env,
        "SELECT COUNT(*) FROM orders_silver"
    ).await;
    let silver_count: u64 = silver_count_text.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    // All seed orders have a non-null status, so counts must match
    assert!(
        silver_count > 0,
        "silver view returned 0 rows"
    );
    assert_eq!(
        silver_count, raw_count,
        "silver row count ({silver_count}) should equal raw ({raw_count}) \
         because all seed rows have a non-null status"
    );
}

// ── Test 2: MERGE INTO gold table is idempotent ───────────────────────────────

#[tokio::test]
async fn merge_into_gold_table_is_idempotent() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => { eprintln!("skip: SPARK_CTRL_INTEGRATION not set"); return; }
    };
    if let Err(msg) = env.check_livy().await { eprintln!("skip: {msg}"); return; }

    let client = env.livy_client().unwrap();

    // Create the gold table (idempotent DDL)
    sql(&client, &env,
        "CREATE TABLE IF NOT EXISTS demo.daily_revenue ( \
             order_date DATE, \
             order_count BIGINT, \
             revenue DOUBLE \
         ) USING iceberg PARTITIONED BY (order_date)"
    ).await;

    // Source aggregation view
    sql(&client, &env,
        "CREATE OR REPLACE TEMP VIEW new_daily AS \
         SELECT TO_DATE(created_at) AS order_date, \
                COUNT(*) AS order_count, \
                COALESCE(SUM(total_amount), 0.0) AS revenue \
         FROM demo.orders \
         WHERE status = 'shipped' \
         GROUP BY TO_DATE(created_at)"
    ).await;

    // First MERGE
    sql(&client, &env,
        "MERGE INTO demo.daily_revenue t \
         USING new_daily s \
         ON t.order_date = s.order_date \
         WHEN MATCHED THEN UPDATE SET \
             t.order_count = s.order_count, t.revenue = s.revenue \
         WHEN NOT MATCHED THEN INSERT *"
    ).await;

    let count_after_first = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.daily_revenue"
    ).await;
    let n1: u64 = count_after_first.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    // Second identical MERGE — must not duplicate rows
    sql(&client, &env,
        "MERGE INTO demo.daily_revenue t \
         USING new_daily s \
         ON t.order_date = s.order_date \
         WHEN MATCHED THEN UPDATE SET \
             t.order_count = s.order_count, t.revenue = s.revenue \
         WHEN NOT MATCHED THEN INSERT *"
    ).await;

    let count_after_second = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.daily_revenue"
    ).await;
    let n2: u64 = count_after_second.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    assert!(n1 > 0, "gold table must have rows after first MERGE");
    assert_eq!(n1, n2, "MERGE INTO must be idempotent (first={n1}, second={n2})");
}

// ── Test 3: second MERGE creates a new Iceberg snapshot ──────────────────────

#[tokio::test]
async fn merge_into_creates_new_snapshot_each_time() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => { eprintln!("skip: SPARK_CTRL_INTEGRATION not set"); return; }
    };
    if let Err(msg) = env.check_livy().await { eprintln!("skip: {msg}"); return; }

    let client = env.livy_client().unwrap();

    // Ensure table exists (may have been created by the previous test)
    sql(&client, &env,
        "CREATE TABLE IF NOT EXISTS demo.daily_revenue ( \
             order_date DATE, order_count BIGINT, revenue DOUBLE \
         ) USING iceberg PARTITIONED BY (order_date)"
    ).await;

    let snap_before = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.daily_revenue.snapshots"
    ).await;
    let n_before: u64 = snap_before.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    // Run a write to create a new snapshot
    sql(&client, &env,
        "INSERT INTO demo.daily_revenue \
         VALUES (CURRENT_DATE(), 1, 99.99)"
    ).await;

    let snap_after = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.daily_revenue.snapshots"
    ).await;
    let n_after: u64 = snap_after.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    assert!(
        n_after > n_before,
        "snapshot count must increase after INSERT (before={n_before}, after={n_after})"
    );
}

// ── Test 4: compaction reduces file count ─────────────────────────────────────
//
// Writes 5 small single-row batches then calls rewrite_data_files().
// The file count must decrease (5 → 1 target file at 128 MB target size).

#[tokio::test]
async fn compaction_reduces_file_count() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => { eprintln!("skip: SPARK_CTRL_INTEGRATION not set"); return; }
    };
    if let Err(msg) = env.check_livy().await { eprintln!("skip: {msg}"); return; }

    let client = env.livy_client().unwrap();

    // Create a fresh table for this test
    sql(&client, &env,
        "CREATE TABLE IF NOT EXISTS demo.compaction_test ( \
             id BIGINT, value STRING \
         ) USING iceberg"
    ).await;

    // Write 5 small batches — each INSERT creates its own data file
    for i in 0..5u64 {
        sql(&client, &env,
            &format!("INSERT INTO demo.compaction_test VALUES ({i}, 'batch-{i}')")
        ).await;
    }

    // Count files before compaction
    let files_before_text = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.compaction_test.files"
    ).await;
    let files_before: u64 = files_before_text.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    // Run the compaction procedure (mirrors the HTML guide's rewrite_data_files call)
    sql(&client, &env,
        "CALL demo.system.rewrite_data_files( \
             table => 'demo.compaction_test', \
             options => map('target-file-size-bytes', '134217728') \
         )"
    ).await;

    // Count files after compaction
    let files_after_text = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.compaction_test.files"
    ).await;
    let files_after: u64 = files_after_text.lines()
        .filter_map(|l| l.trim().parse().ok()).next().unwrap_or(0);

    assert!(
        files_before >= 5,
        "expected ≥5 files before compaction (got {files_before})"
    );
    assert!(
        files_after < files_before,
        "compaction must reduce file count (before={files_before}, after={files_after})"
    );
}
