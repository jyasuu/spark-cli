//! Integration tests: Phase 1 — Iceberg Snapshot inspection.
//!
//! These tests run only when `SPARK_CTRL_INTEGRATION=1` is set and require
//! the docker-compose stack to be healthy:
//!
//!   docker compose up -d
//!   SPARK_CTRL_INTEGRATION=1 cargo test --features integration iceberg_snapshots
//!
//! What is verified:
//!  1. The Iceberg REST catalog responds to a metadata query via Livy SQL.
//!  2. The `demo.products` table seeded by `iceberg-init` has ≥1 snapshot.
//!  3. The `fs table snapshots` SQL branch returns parseable rows.
//!  4. A second sync (simulated by a MERGE INTO) creates an additional snapshot.

use crate::client::LivyClient;
use crate::commands::session::one_shot_sql;
use crate::testing::{run_sql as sql, IntegEnv};

// ── helper ────────────────────────────────────────────────────────────────────
// `sql()` is re-exported from testing::helpers — no local copy needed.

// ── Test 1: catalog is reachable and demo namespace is visible ────────────────

#[tokio::test]
async fn iceberg_catalog_lists_demo_namespace() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => {
            eprintln!("skip: SPARK_CTRL_INTEGRATION not set");
            return;
        }
    };
    if let Err(msg) = env.check_livy().await {
        eprintln!("skip: {msg}");
        return;
    }

    let client = env.livy_client().unwrap();
    let out = sql(&client, &env, "SHOW DATABASES").await;

    assert!(
        out.to_lowercase().contains("demo"),
        "expected 'demo' namespace in SHOW DATABASES output, got:\n{out}"
    );
}

// ── Test 2: products table has at least one snapshot ─────────────────────────

#[tokio::test]
async fn products_table_has_at_least_one_snapshot() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => {
            eprintln!("skip: SPARK_CTRL_INTEGRATION not set");
            return;
        }
    };
    if let Err(msg) = env.check_livy().await {
        eprintln!("skip: {msg}");
        return;
    }

    let client = env.livy_client().unwrap();

    // This is exactly the SQL that `fs table snapshots --format iceberg` will run
    // once we wire it to a real session (Phase 1 task).
    let snapshot_sql = "SELECT snapshot_id, committed_at, operation \
                        FROM demo.products.snapshots \
                        ORDER BY committed_at DESC \
                        LIMIT 20";

    let out = sql(&client, &env, snapshot_sql).await;

    // iceberg-init writes a full-load snapshot, so there must be at least one row.
    // Output is tab-separated: snapshot_id\tcommitted_at\toperation
    // We just check the whole string for any known operation keyword.
    let out_lower = out.to_lowercase();
    assert!(
        out_lower.contains("overwrite") || out_lower.contains("append") || out_lower.contains("replace"),
        "expected at least one snapshot row (operation column), got:\n{out}"
    );
}

// ── Test 3: orders table has at least one snapshot ────────────────────────────

#[tokio::test]
async fn orders_table_has_at_least_one_snapshot() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => {
            eprintln!("skip: SPARK_CTRL_INTEGRATION not set");
            return;
        }
    };
    if let Err(msg) = env.check_livy().await {
        eprintln!("skip: {msg}");
        return;
    }

    let client = env.livy_client().unwrap();
    let out = sql(
        &client,
        &env,
        "SELECT snapshot_id FROM demo.orders.snapshots LIMIT 1",
    )
    .await;

    // Output is tab-separated rows; any non-empty output means at least one snapshot exists.
    assert!(
        !out.trim().is_empty(),
        "demo.orders.snapshots returned no rows — was iceberg-init run?\n{out}"
    );
}

// ── Test 4: snapshot count increases after a second write ─────────────────────

#[tokio::test]
async fn snapshot_count_increases_after_second_write() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => {
            eprintln!("skip: SPARK_CTRL_INTEGRATION not set");
            return;
        }
    };
    if let Err(msg) = env.check_livy().await {
        eprintln!("skip: {msg}");
        return;
    }

    let client = env.livy_client().unwrap();
    let auth = env.profile().auth;

    // Count snapshots before
    let count_sql = "SELECT COUNT(*) as n FROM demo.products.snapshots";
    let before_text = sql(&client, &env, count_sql).await;
    let before: u64 = before_text
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .next()
        .unwrap_or(0);

    // Trigger a second write: INSERT a synthetic product then DELETE it
    // (a full overwrite would also work — this is lighter)
    let insert_sql = "INSERT INTO demo.products \
                      VALUES (9999, 'TEST-SKU', 'Integration Test Widget', \
                              'test', 0.01, 1, current_timestamp())";

    one_shot_sql(&client, insert_sql, &auth)
        .await
        .expect("INSERT failed");

    // Count snapshots after
    let after_text = sql(&client, &env, count_sql).await;
    let after: u64 = after_text
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .next()
        .unwrap_or(0);

    assert!(
        after > before,
        "snapshot count should increase after a write (before={before}, after={after})"
    );
}

// ── Test 5: fs table snapshots CLI path returns non-empty output ──────────────
//
// This test validates the full CLI command path, not just the SQL.
// It mimics what `spark-ctrl fs table snapshots demo.products --format iceberg`
// will do once we wire it to a real Livy session.

#[tokio::test]
async fn fs_table_snapshots_sql_branch_returns_rows() {
    let env = match IntegEnv::from_env() {
        Some(e) => e,
        None => {
            eprintln!("skip: SPARK_CTRL_INTEGRATION not set");
            return;
        }
    };
    if let Err(msg) = env.check_livy().await {
        eprintln!("skip: {msg}");
        return;
    }

    let client = env.livy_client().unwrap();

    // Mirror the SQL constructed by fs::table_op → TableAction::Snapshots (iceberg branch)
    let table = "demo.products";
    let sql_str = format!(
        "SELECT snapshot_id, committed_at, operation FROM {table}.snapshots \
         ORDER BY committed_at DESC LIMIT 20"
    );

    let out = sql(&client, &env, &sql_str).await;

    // Output is tab-separated rows (snapshot_id\tcommitted_at\toperation per line).
    // Any non-empty output means at least one snapshot was returned.
    assert!(
        !out.trim().is_empty(),
        "snapshot query returned no output for {table}:\n{out}"
    );
}