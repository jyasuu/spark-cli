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
/// Uses `kind = "pyspark"` so that `spark.sql.catalog.*` conf keys are
/// honoured.
///
/// # Iceberg metadata table handling
///
/// Spark's V2 catalog resolver cannot correctly parse Iceberg metadata table
/// references like `demo.products.snapshots` in SQL — the dot-separated parts
/// get mis-assigned between catalog/namespace/table, producing malformed REST
/// URLs.  Any SQL that references a `.snapshots`, `.files`, or `.manifests`
/// suffix is rewritten to use the PySpark DataFrame API instead:
///
///   spark.table("demo.products").select(...).collect()   -- regular tables
///   spark.read.format("iceberg").load("demo.products").select("snapshot_id", ...).collect()
///
/// For metadata tables we use `spark.read.format("iceberg").option("snapshot-id", ...) `
/// or the `IcebergTable` API via `catalog.load_table()`.  The simplest cross-
/// version approach is:
///
///   from pyspark.sql.functions import col
///   spark.read.format("iceberg").load("demo.products.snapshots").collect()
///
/// This bypasses Spark SQL's identifier parser entirely and goes straight to
/// Iceberg's DataSource V2 reader, which handles the metadata table path
/// correctly.
pub async fn sql(client: &LivyClient, env: &IntegEnv, sql_str: &str) -> String {
    let auth = env.profile().auth;
    let sid = open_session(client, "pyspark", &auth)
        .await
        .expect("failed to open Livy session");

    // Rewrite the SQL into PySpark code that avoids Spark's broken metadata
    // table SQL parsing.  We use spark.read.format("iceberg").load() for any
    // query that touches a metadata table (snapshots / files / manifests),
    // and spark.sql() for everything else.
    let code = build_pyspark_code(sql_str);

    let result = run_sql(client, sid, &code, &auth)
        .await
        .expect("failed to run SQL statement");
        println!("helper::sql result: {:?}",result);
    client.delete_session(sid, &auth).await.ok();
    extract_text(&result).unwrap_or_default()
}

/// Translate a SQL string into PySpark code.
///
/// For queries involving Iceberg metadata tables (`.snapshots`, `.files`,
/// `.manifests`) we generate `spark.read.format("iceberg").load(...)` calls.
/// For everything else we generate a `spark.sql(...)` call, prefixed with
/// `spark.conf.set("spark.sql.catalog.demo.default-namespace", "demo")` so
/// that two-part names like `demo.orders` resolve as catalog=demo,
/// namespace=demo, table=orders.
fn build_pyspark_code(sql_str: &str) -> String {
    let sql_lower = sql_str.to_lowercase();

    // Detect metadata table queries: FROM <table>.snapshots / .files / .manifests
    if let Some(meta) = detect_metadata_table(&sql_lower) {
        return build_metadata_query(sql_str, &meta);
    }

    // Regular SQL: set the default namespace so two-part names resolve correctly,
    // then execute via spark.sql().
    let escaped = sql_str.replace('\\', "\\\\").replace('\'', "\\'");
    format!(
        "spark.conf.set('spark.sql.catalog.demo.default-namespace', 'demo'); \
         _rows = spark.sql('{escaped}').collect(); \
         '\\n'.join(['\\t'.join(str(v) for v in r) for r in _rows]) if _rows else ''"
    )
}

struct MetaTableRef {
    /// The base table, e.g. "demo.products"
    base_table: String,
    /// The metadata table suffix, e.g. "snapshots", "files"
    meta_type: String,
    /// Columns to project, derived from the SELECT clause
    columns: Vec<String>,
    /// Optional ORDER BY expression (passed through as Python sort)
    order_by: Option<String>,
    /// LIMIT value if present
    limit: Option<u64>,
}

fn detect_metadata_table(sql_lower: &str) -> Option<MetaTableRef> {
    // Match: FROM <something>.(snapshots|files|manifests)
    for meta in &["snapshots", "files", "manifests"] {
        let needle = format!(".{meta}");
        if let Some(from_pos) = sql_lower.find(" from ") {
            let after_from = &sql_lower[from_pos + 6..];
            if let Some(meta_pos) = after_from.find(needle.as_str()) {
                // Extract the base table name (everything before .meta)
                let base = after_from[..meta_pos].trim().to_string();
                // Extract SELECT columns
                let select_part = &sql_lower[..from_pos];
                let cols = parse_select_columns(select_part);
                // Extract LIMIT
                let limit = parse_limit(sql_lower);
                // Extract ORDER BY (simplified)
                let order_by = parse_order_by(sql_lower);
                return Some(MetaTableRef {
                    base_table: base,
                    meta_type: meta.to_string(),
                    columns: cols,
                    order_by,
                    limit,
                });
            }
        }
    }
    None
}

fn parse_select_columns(select_part: &str) -> Vec<String> {
    // Strip leading SELECT keyword
    let after_select = select_part
        .trim()
        .trim_start_matches("select")
        .trim();
    // Split by comma and clean up
    after_select
        .split(',')
        .map(|s| {
            // Keep only the column name part (strip AS aliases for simplicity)
            let s = s.trim();
            // Handle COUNT(*) as a special case
            if s.contains("count(*)") || s.contains("count( *)") {
                "count(*)".to_string()
            } else {
                // Take just the base name (no aliases)
                s.split_whitespace().next().unwrap_or(s).to_string()
            }
        })
        .collect()
}

fn parse_limit(sql_lower: &str) -> Option<u64> {
    if let Some(pos) = sql_lower.find(" limit ") {
        let rest = sql_lower[pos + 7..].trim();
        let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        num.parse().ok()
    } else {
        None
    }
}

fn parse_order_by(sql_lower: &str) -> Option<String> {
    if let Some(pos) = sql_lower.find(" order by ") {
        let rest = sql_lower[pos + 10..].trim();
        // Stop at LIMIT or end
        let expr = if let Some(lim) = rest.find(" limit ") {
            &rest[..lim]
        } else {
            rest
        };
        Some(expr.trim().to_string())
    } else {
        None
    }
}

fn build_metadata_query(original_sql: &str, meta: &MetaTableRef) -> String {
    let base = &meta.base_table;
    let meta_type = &meta.meta_type;
    let load_path = format!("{base}.{meta_type}");

    // Build the PySpark code using spark.read.format("iceberg").load()
    // which correctly handles metadata table paths without going through
    // Spark SQL's broken V2 identifier resolver.
    let mut py = format!(
        "_df = spark.read.format('iceberg').load('{load_path}')\n"
    );

    // Apply column projection
    if !meta.columns.is_empty() && !meta.columns.iter().any(|c| c == "*") {
        let col_is_count = meta.columns.iter().any(|c| c.contains("count"));
        if col_is_count {
            py.push_str("_df = _df.select('*')\n");
            py.push_str("_count = _df.count()\n");
            py.push_str("_rows = [(_count,)]\n");
            py.push_str("_result = '\\n'.join(['\\t'.join(str(v) for v in r) for r in _rows]) if _rows else ''\n");
            py.push_str("_result");
            return py;
        }

        // Project specific columns that exist
        let cols_py: Vec<String> = meta.columns.iter()
            .filter(|c| !c.is_empty() && *c != "select")
            .map(|c| format!("'{c}'"))
            .collect();
        if !cols_py.is_empty() {
            py.push_str(&format!("_df = _df.select({})\n", cols_py.join(", ")));
        }
    }

    // Apply ORDER BY (use sort)
    if let Some(ref order) = meta.order_by {
        // Parse "column DESC" or "column ASC"
        let parts: Vec<&str> = order.split_whitespace().collect();
        if let Some(col) = parts.first() {
            let desc = parts.get(1).map(|d| *d == "desc").unwrap_or(false);
            if desc {
                py.push_str(&format!(
                    "from pyspark.sql.functions import col as _col\n\
                     _df = _df.orderBy(_col('{col}').desc())\n"
                ));
            } else {
                py.push_str(&format!("_df = _df.orderBy('{col}')\n"));
            }
        }
    }

    // Apply LIMIT
    if let Some(lim) = meta.limit {
        py.push_str(&format!("_df = _df.limit({lim})\n"));
    }

    py.push_str(
        "_rows = _df.collect()\n\
         '\\n'.join(['\\t'.join(str(v) for v in r) for r in _rows]) if _rows else ''"
    );
    py
}

/// Parse the first integer found in `text` (tab-separated collect output from
/// a `SELECT COUNT(*)` query).
pub fn parse_count(text: &str) -> u64 {
    text.lines()
        .flat_map(|l| l.split('\t'))
        .filter_map(|cell| cell.trim().parse::<u64>().ok())
        .next()
        .unwrap_or(0)
}

/// Return the `operation` column from the most-recent snapshot of `table`.
pub async fn latest_snapshot_op(client: &LivyClient, env: &IntegEnv, table: &str) -> String {
    let q = format!("SELECT operation FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1");
    let raw = sql(client, env, &q).await;
    raw.lines()
        .flat_map(|l| l.split('\t'))
        .map(|cell| cell.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .next()
        .unwrap_or_default()
}