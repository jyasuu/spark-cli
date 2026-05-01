//! Integration tests: Phase 3 — Write mode validation.
//!
//! Requires docker-compose stack + `SPARK_CTRL_INTEGRATION=1`:
//!
//!   docker compose up -d
//!   SPARK_CTRL_INTEGRATION=1 cargo test --features integration write_modes
//!
//! What is verified:
//!
//!   p3s1  Append mode  — N INSERT INTO rows create exactly N new rows and one
//!                        new snapshot with operation = "append".
//!
//!   p3s2  Upsert mode  — MERGE INTO on a keyed entity table is idempotent:
//!                        re-running the same merge does not duplicate rows.
//!                        The matched branch UPDATEs; the not-matched branch INSERTs.
//!
//!   p3s3  CDC merge    — A simulated CDC outbox drives MERGE INTO with
//!                        UPDATE (op='U') and DELETE (op='D') branches.
//!                        Changed rows carry new values; deleted rows are gone.
//!
//!   p3s4  Operation    — snapshot metadata reflects the write type:
//!                        append → "append", MERGE INTO → "overwrite",
//!                        DELETE FROM → "delete".
//!
//! Each test creates its own uniquely-named Iceberg table so tests are safe
//! to run in parallel and leave no shared state that could cause ordering
//! dependencies between Phase 2 and Phase 3 runs.

use crate::client::LivyClient;
use crate::testing::{IntegEnv, run_sql as sql, parse_count};

// ── helpers re-exported from testing::helpers ─────────────────────────────────
// `sql()` and `parse_count()` come from crate::testing::helpers.

/// Return the most-recent snapshot `operation` column value for a table.
async fn latest_operation(client: &LivyClient, env: &IntegEnv, table: &str) -> String {
    crate::testing::latest_snapshot_op(client, env, table).await
}

// ── p3s1: append mode ─────────────────────────────────────────────────────────
//
// INSERT INTO is the pure-append write path.  Each call creates a new snapshot
// and adds rows without touching existing data.  We verify:
//   • row count grows by exactly N after N inserts
//   • snapshot count grows by N (one snapshot per INSERT)
//   • the latest snapshot operation is "append"

#[tokio::test]
async fn append_mode_row_and_snapshot_counts_increase_correctly() {
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
    let table = "demo.wm_append_test";

    // Fresh table — using IF NOT EXISTS so the test is re-runnable
    sql(
        &client,
        &env,
        &format!(
            "CREATE TABLE IF NOT EXISTS {table} \
             (id BIGINT, label STRING, value BIGINT) \
             USING iceberg"
        ),
    )
    .await;

    // Baseline counts
    let rows_before = parse_count(&sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}")).await);
    let snaps_before = parse_count(
        &sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}.snapshots")).await,
    );

    // Append 3 rows in 3 separate INSERTs — each must produce its own snapshot
    let batch: &[(&str, &str, u64)] = &[
        ("evt-001", "click",    1),
        ("evt-002", "purchase", 2),
        ("evt-003", "refund",   3),
    ];
    for (i, (id, label, value)) in batch.iter().enumerate() {
        sql(
            &client,
            &env,
            &format!("INSERT INTO {table} VALUES ({i}, '{label}', {value})"),
        )
        .await;
    }

    let rows_after = parse_count(&sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}")).await);
    let snaps_after = parse_count(
        &sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}.snapshots")).await,
    );

    assert_eq!(
        rows_after,
        rows_before + 3,
        "expected exactly 3 new rows after 3 INSERTs \
         (before={rows_before} after={rows_after})"
    );
    assert_eq!(
        snaps_after,
        snaps_before + 3,
        "expected exactly 3 new snapshots after 3 INSERTs \
         (before={snaps_before} after={snaps_after})"
    );

    let op = latest_operation(&client, &env, table).await;
    assert_eq!(
        op, "append",
        "latest snapshot operation should be 'append' after INSERT INTO, got '{op}'"
    );
}

// ── p3s2: upsert mode ─────────────────────────────────────────────────────────
//
// MERGE INTO on a keyed entity table must be idempotent.  Running the exact
// same merge twice must not change the row count.  The WHEN MATCHED branch
// updates existing rows; the WHEN NOT MATCHED branch inserts new ones.
// We also verify that the updated value is visible after the second merge.

#[tokio::test]
async fn upsert_mode_merge_into_is_idempotent_and_updates_values() {
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
    let table = "demo.wm_upsert_entities";

    // Target table: keyed by entity_id
    sql(
        &client,
        &env,
        &format!(
            "CREATE TABLE IF NOT EXISTS {table} \
             (entity_id BIGINT, name STRING, status STRING) \
             USING iceberg"
        ),
    )
    .await;

    // Seed two rows (idempotent: INSERT OVERWRITE so the table always starts
    // from a known state regardless of previous test runs)
    sql(
        &client,
        &env,
        &format!(
            "INSERT OVERWRITE {table} \
             VALUES (1, 'alpha', 'active'), (2, 'beta', 'active')"
        ),
    )
    .await;

    let rows_seeded = parse_count(&sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}")).await);
    assert_eq!(rows_seeded, 2, "seed INSERT OVERWRITE should produce exactly 2 rows");

    // Source: update entity 1's status, add entity 3
    let merge = format!(
        "MERGE INTO {table} t \
         USING ( \
             SELECT 1 AS entity_id, 'alpha' AS name, 'inactive' AS status \
             UNION ALL \
             SELECT 3 AS entity_id, 'gamma' AS name, 'active'   AS status \
         ) s \
         ON t.entity_id = s.entity_id \
         WHEN MATCHED THEN UPDATE SET t.name = s.name, t.status = s.status \
         WHEN NOT MATCHED THEN INSERT *"
    );

    // First merge
    sql(&client, &env, &merge).await;
    let count_first = parse_count(&sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}")).await);

    // Second identical merge — idempotency check
    sql(&client, &env, &merge).await;
    let count_second = parse_count(&sql(&client, &env, &format!("SELECT COUNT(*) FROM {table}")).await);

    assert_eq!(
        count_first, 3,
        "after first merge: expected 3 rows (2 existing + 1 new), got {count_first}"
    );
    assert_eq!(
        count_first, count_second,
        "MERGE INTO must be idempotent: first={count_first} second={count_second}"
    );

    // Verify the update landed: entity 1 must now be 'inactive'
    let status_text = sql(
        &client,
        &env,
        &format!("SELECT status FROM {table} WHERE entity_id = 1 LIMIT 1"),
    )
    .await;
    let status = status_text
        .lines()
        .filter(|l| {
            let t = l.trim().to_lowercase();
            !t.is_empty() && t != "status"
        })
        .next()
        .unwrap_or("")
        .trim()
        .to_string();

    assert_eq!(
        status, "inactive",
        "entity 1 should be 'inactive' after upsert, got '{status}'"
    );
}

// ── p3s3: CDC merge ───────────────────────────────────────────────────────────
//
// A CDC outbox carries three operation types: I (insert), U (update), D (delete).
// The MERGE INTO handles each:
//   op='U'  → WHEN MATCHED UPDATE
//   op='D'  → WHEN MATCHED DELETE
//   op='I'  → WHEN NOT MATCHED INSERT
//
// After applying the outbox:
//   • Updated rows carry the new name/status values from the outbox
//   • Deleted rows are absent from the target table
//   • Inserted rows appear in the target table

#[tokio::test]
async fn cdc_merge_applies_updates_and_deletes_correctly() {
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
    let target = "demo.wm_cdc_target";
    let outbox = "demo.wm_cdc_outbox";

    // ── target table: three seed rows ────────────────────────────────────────
    sql(
        &client,
        &env,
        &format!(
            "CREATE TABLE IF NOT EXISTS {target} \
             (entity_id BIGINT, name STRING, status STRING) \
             USING iceberg"
        ),
    )
    .await;

    sql(
        &client,
        &env,
        &format!(
            "INSERT OVERWRITE {target} \
             VALUES \
               (10, 'apple',  'active'), \
               (20, 'banana', 'active'), \
               (30, 'cherry', 'active')"
        ),
    )
    .await;

    // ── CDC outbox: update 10, delete 20, insert 40 ───────────────────────────
    sql(
        &client,
        &env,
        &format!(
            "CREATE TABLE IF NOT EXISTS {outbox} \
             (entity_id BIGINT, name STRING, status STRING, op STRING) \
             USING iceberg"
        ),
    )
    .await;

    sql(
        &client,
        &env,
        &format!(
            "INSERT OVERWRITE {outbox} \
             VALUES \
               (10, 'apple-v2', 'inactive', 'U'), \
               (20, 'banana',   'active',   'D'), \
               (40, 'durian',   'active',   'I')"
        ),
    )
    .await;

    // ── CDC MERGE INTO ────────────────────────────────────────────────────────
    // Iceberg Spark extensions support WHEN MATCHED AND <condition> and
    // WHEN NOT MATCHED AND <condition>, so we can branch on the op column.
    sql(
        &client,
        &env,
        &format!(
            "MERGE INTO {target} t \
             USING {outbox} s \
             ON t.entity_id = s.entity_id \
             WHEN MATCHED AND s.op = 'D' THEN DELETE \
             WHEN MATCHED AND s.op = 'U' THEN UPDATE SET \
                 t.name = s.name, t.status = s.status \
             WHEN NOT MATCHED AND s.op = 'I' THEN INSERT *"
        ),
    )
    .await;

    // ── assertions ────────────────────────────────────────────────────────────

    let total = parse_count(&sql(&client, &env, &format!("SELECT COUNT(*) FROM {target}")).await);
    assert_eq!(
        total, 3,
        "after CDC merge: expected 3 rows \
         (apple-v2 updated, banana deleted, cherry untouched, durian inserted), got {total}"
    );

    // entity 10 must be updated
    let apple_status = sql(
        &client,
        &env,
        &format!("SELECT status FROM {target} WHERE entity_id = 10 LIMIT 1"),
    )
    .await;
    assert!(
        apple_status.contains("inactive"),
        "entity 10 (apple) should be 'inactive' after CDC UPDATE, got: {apple_status}"
    );

    let apple_name = sql(
        &client,
        &env,
        &format!("SELECT name FROM {target} WHERE entity_id = 10 LIMIT 1"),
    )
    .await;
    assert!(
        apple_name.contains("apple-v2"),
        "entity 10 name should be 'apple-v2' after CDC UPDATE, got: {apple_name}"
    );

    // entity 20 must be deleted
    let banana_count = parse_count(
        &sql(&client, &env, &format!("SELECT COUNT(*) FROM {target} WHERE entity_id = 20")).await,
    );
    assert_eq!(
        banana_count, 0,
        "entity 20 (banana) should be deleted after CDC DELETE, count={banana_count}"
    );

    // entity 30 must be untouched
    let cherry_count = parse_count(
        &sql(&client, &env, &format!("SELECT COUNT(*) FROM {target} WHERE entity_id = 30")).await,
    );
    assert_eq!(
        cherry_count, 1,
        "entity 30 (cherry) should be untouched — it was not in the outbox"
    );

    // entity 40 must be inserted
    let durian_count = parse_count(
        &sql(&client, &env, &format!("SELECT COUNT(*) FROM {target} WHERE entity_id = 40")).await,
    );
    assert_eq!(
        durian_count, 1,
        "entity 40 (durian) should be present after CDC INSERT, count={durian_count}"
    );
}

// ── p3s4: snapshot operation metadata ─────────────────────────────────────────
//
// Iceberg records the write operation in snapshot metadata.  This test
// verifies the mapping for all three write paths in a single table so the
// assertion sequence is deterministic.
//
// Expected mapping:
//   INSERT INTO           → operation = "append"
//   INSERT OVERWRITE      → operation = "overwrite"  (dynamic overwrite)
//   MERGE INTO            → operation = "overwrite"  (Iceberg merge-on-read)
//   DELETE FROM           → operation = "delete"

#[tokio::test]
async fn snapshot_operation_column_reflects_write_type() {
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
    let table = "demo.wm_op_metadata";

    sql(
        &client,
        &env,
        &format!(
            "CREATE TABLE IF NOT EXISTS {table} \
             (id BIGINT, val STRING) \
             USING iceberg"
        ),
    )
    .await;

    // ── 1. INSERT INTO → "append" ─────────────────────────────────────────────
    sql(
        &client,
        &env,
        &format!("INSERT INTO {table} VALUES (1, 'first')"),
    )
    .await;

    let op_append = latest_operation(&client, &env, table).await;
    assert_eq!(
        op_append, "append",
        "INSERT INTO should produce operation='append', got '{op_append}'"
    );

    // ── 2. INSERT OVERWRITE → "overwrite" ─────────────────────────────────────
    sql(
        &client,
        &env,
        &format!("INSERT OVERWRITE {table} VALUES (1, 'overwritten')"),
    )
    .await;

    let op_overwrite = latest_operation(&client, &env, table).await;
    assert_eq!(
        op_overwrite, "overwrite",
        "INSERT OVERWRITE should produce operation='overwrite', got '{op_overwrite}'"
    );

    // ── 3. MERGE INTO → "overwrite" ───────────────────────────────────────────
    // Iceberg's MERGE INTO uses copy-on-write (or merge-on-read) — both record
    // the snapshot as "overwrite" in the operation column.
    sql(
        &client,
        &env,
        &format!(
            "MERGE INTO {table} t \
             USING (SELECT 1 AS id, 'merged' AS val) s \
             ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.val = s.val \
             WHEN NOT MATCHED THEN INSERT *"
        ),
    )
    .await;

    let op_merge = latest_operation(&client, &env, table).await;
    assert_eq!(
        op_merge, "overwrite",
        "MERGE INTO should produce operation='overwrite', got '{op_merge}'"
    );

    // ── 4. DELETE FROM → "delete" ─────────────────────────────────────────────
    sql(
        &client,
        &env,
        &format!("DELETE FROM {table} WHERE id = 1"),
    )
    .await;

    let op_delete = latest_operation(&client, &env, table).await;
    assert_eq!(
        op_delete, "delete",
        "DELETE FROM should produce operation='delete', got '{op_delete}'"
    );

    // Final sanity: all four snapshot operations are recorded in the history
    let all_ops_text = sql(
        &client,
        &env,
        &format!(
            "SELECT operation FROM {table}.snapshots \
             ORDER BY committed_at ASC"
        ),
    )
    .await;

    // Collect non-header lines
    let ops: Vec<&str> = all_ops_text
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && *l != "operation")
        .collect();

    assert!(
        ops.len() >= 4,
        "expected at least 4 snapshots recorded, got {}: {all_ops_text}",
        ops.len()
    );
}
