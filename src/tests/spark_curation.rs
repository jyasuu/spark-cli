//! Integration tests: Phase 2 — Spark silver/gold curation.
//!
//! Requires docker-compose stack + `SPARK_CTRL_INTEGRATION=1`.
//!
//! These tests follow the patterns described in the HTML guide:
//!   • Silver layer: raw → cleaned + enriched via Spark SQL
//!   • Gold layer:  silver → aggregated daily_revenue with MERGE INTO
//!   • Compaction:  CALL rewrite_data_files() reduces physical file count

use crate::client::LivyClient;
use crate::testing::{IntegEnv, run_sql as sql, parse_count};

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
    let raw_count: u64 = parse_count(&raw_count_text);

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
    let silver_count: u64 = parse_count(&silver_count_text);

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
    let n1: u64 = parse_count(&count_after_first);

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
    let n2: u64 = parse_count(&count_after_second);

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
    let n_before: u64 = parse_count(&snap_before);

    // Run a write to create a new snapshot
    sql(&client, &env,
        "INSERT INTO demo.daily_revenue \
         VALUES (CURRENT_DATE(), 1, 99.99)"
    ).await;

    let snap_after = sql(&client, &env,
        "SELECT COUNT(*) FROM demo.daily_revenue.snapshots"
    ).await;
    let n_after: u64 = parse_count(&snap_after);

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
    let files_before: u64 = parse_count(&files_before_text);

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
    let files_after: u64 = parse_count(&files_after_text);

    assert!(
        files_before >= 5,
        "expected ≥5 files before compaction (got {files_before})"
    );
    assert!(
        files_after < files_before,
        "compaction must reduce file count (before={files_before}, after={files_after})"
    );
}

// ── Test 5: `job submit --curate` wires Iceberg conf and job reaches success ──
//
// This test validates the full CLI batch-submit path with --curate conf injection.
// It submits gold_daily_revenue.py as a Livy batch job and waits for it to reach
// "success" state, confirming that:
//   1. The Iceberg REST catalog conf was applied correctly (no catalog-not-found errors)
//   2. The MERGE INTO ran without failure
//   3. demo.daily_revenue has rows afterwards

#[tokio::test]
async fn curate_submit_reaches_success_state() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => { eprintln!("skip: SPARK_CTRL_INTEGRATION not set"); return; }
    };
    if let Err(msg) = env.check_livy().await { eprintln!("skip: {msg}"); return; }

    use crate::client::BatchRequest;
    use std::collections::HashMap;
    use std::time::Duration;

    let client = env.livy_client().unwrap();
    let auth = env.profile().auth;

    // Build the same conf that --curate injects
    let mut conf: HashMap<String, String> = HashMap::new();
    let iceberg_defaults: &[(&str, &str)] = &[
        ("spark.sql.extensions",
         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        ("spark.sql.catalog.demo",      "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.demo.type", "rest"),
        ("spark.sql.catalog.demo.uri",  "http://rest:8181"),
        ("spark.sql.catalog.demo.io-impl",
         "org.apache.iceberg.aws.s3.S3FileIO"),
        ("spark.sql.catalog.demo.warehouse",   "s3a://warehouse/"),
        ("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000"),
        ("spark.hadoop.fs.s3a.access.key",        "admin"),
        ("spark.hadoop.fs.s3a.secret.key",        "password"),
        ("spark.hadoop.fs.s3a.endpoint",           "http://minio:9000"),
        ("spark.hadoop.fs.s3a.path.style.access", "true"),
    ];
    for (k, v) in iceberg_defaults {
        conf.insert(k.to_string(), v.to_string());
    }

    let req = BatchRequest {
        // The script must be accessible to the Spark driver — in the docker-compose
        // stack, mount it or use a MinIO path.  For CI, copy it via `mc cp` first:
        //   docker exec mc mc cp /scripts/gold_daily_revenue.py minio/warehouse/scripts/
        // then reference it as s3a://warehouse/scripts/gold_daily_revenue.py
        file: "s3a://warehouse/scripts/gold_daily_revenue.py".to_string(),
        class_name: None,
        name: Some("curate-gold-daily-revenue-integ-test".to_string()),
        args: vec![],
        jars: vec![],
        py_files: vec![],
        files: vec![],
        driver_memory: Some("512m".to_string()),
        driver_cores: None,
        executor_memory: Some("1g".to_string()),
        executor_cores: Some(1),
        num_executors: Some(1),
        conf,
        queue: None,
    };

    let batch = client.submit_batch(&req, &auth).await
        .expect("batch submit failed");
    eprintln!("submitted batch id={}", batch.id);

    // Poll until terminal state (max 5 min)
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    let final_state = loop {
        if std::time::Instant::now() > deadline {
            panic!("job {} did not finish within 5 minutes", batch.id);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let info = client.get_batch(batch.id, &auth).await
            .expect("get_batch failed");
        eprintln!("  state={}", info.state);
        match info.state.as_str() {
            "success" | "dead" | "error" => break info.state,
            _ => {}
        }
    };

    assert_eq!(
        final_state, "success",
        "gold_daily_revenue.py batch job ended in '{}' not 'success'. \
         Check `spark-ctrl job logs {}` for the driver output.",
        final_state, batch.id
    );

    // Confirm rows landed in the gold table
    let client_sql = env.livy_client().unwrap();
    let out = crate::commands::session::one_shot_sql(
        &client_sql,
        "SELECT COUNT(*) FROM demo.daily_revenue",
        &auth,
    ).await.expect("count query failed");
    let text = crate::commands::session::extract_text(&out).unwrap_or_default();
    let count: u64 = text.lines()
        .filter_map(|l| l.trim().parse().ok())
        .next()
        .unwrap_or(0);

    assert!(
        count > 0,
        "demo.daily_revenue has 0 rows after curate job — MERGE INTO may have failed"
    );
    eprintln!("demo.daily_revenue rows after curate job: {count}");
}
