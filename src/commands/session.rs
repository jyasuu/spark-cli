//! Shared Livy session helpers used by both `sql` and `fs` commands.

use crate::client::{LivyClient, StatementResult};
use crate::config::Auth;
use anyhow::Result;
use colored::Colorize;
use std::io::Write;
use std::time::Duration;

/// Open a new Livy session of `kind` (e.g. `"sql"`, `"spark"`) and wait
/// until it reaches the `idle` state before returning the session id.
pub async fn open_session(client: &LivyClient, kind: &str, auth: &Auth) -> Result<u64> {
    let session = client.create_session(kind, auth).await?;
    let id = session.id;
    print!("{} creating session {}…", "⟳".cyan(), id);
    std::io::stdout().flush().ok();
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let s = client.get_session(id, auth).await?;
        match s.state.as_str() {
            "idle" => {
                println!(" {}", "ready".green());
                return Ok(id);
            }
            "error" | "dead" => {
                println!(" {}", "failed".red());
                anyhow::bail!("session {} failed to start", id);
            }
            _ => {
                print!(".");
                std::io::stdout().flush().ok();
            }
        }
    }
}

/// Run a single SQL/code statement in an existing session and return the
/// result.  Callers are responsible for deleting the session afterwards.
pub async fn run_sql(
    client: &LivyClient,
    session_id: u64,
    sql: &str,
    auth: &Auth,
) -> Result<StatementResult> {
    client.run_statement(session_id, sql, auth).await
}

/// Convenience: open a `"pyspark"` session, run one SQL statement via
/// `spark.sql(...)`, delete the session, and return the raw result.  Use
/// when you only need a single round-trip and don't want to manage the
/// session lifetime yourself.
///
/// Uses `"pyspark"` (not `"sql"`) so that `spark.sql.catalog.*` conf keys —
/// including the Iceberg REST catalog — are honoured by the full SparkSession.
pub async fn one_shot_sql(client: &LivyClient, sql: &str, auth: &Auth) -> Result<StatementResult> {
    let sid = open_session(client, "pyspark", auth).await?;
    // Set the default namespace on the demo catalog so two-part names like
    // `demo.orders` resolve as catalog=demo, namespace=demo, table=orders.
    // spark.conf.set() can update session-scoped conf at runtime, unlike
    // static SparkConf keys that are frozen at SparkContext startup.
    run_sql(client, sid,
        "spark.conf.set('spark.sql.catalog.demo.default-namespace', 'demo')",
        auth).await.ok();
    let escaped = sql.replace('\\', "\\\\").replace('\'', "\\'");
    let code = format!(
        "_rows = spark.sql('{escaped}').collect(); \
         '\\n'.join(['\\t'.join(str(v) for v in r) for r in _rows]) if _rows else ''"
    );
    let result = run_sql(client, sid, &code, auth).await;
    client.delete_session(sid, auth).await.ok();
    result
}

/// Extract the text payload from a Livy `StatementResult`, checking for
/// errors first.  Returns the `text/plain` value when present; falls back
/// to the raw JSON of the data map.
pub fn extract_text(result: &StatementResult) -> Result<String> {
    if result.status != "ok" {
        anyhow::bail!(
            "{}: {}",
            result.ename.as_deref().unwrap_or("error"),
            result.evalue.as_deref().unwrap_or("unknown")
        );
    }
    if let Some(data) = &result.data {
        if let Some(text) = data.get("text/plain") {
            return Ok(text.as_str().unwrap_or(&text.to_string()).to_string());
        }
        return Ok(serde_json::to_string_pretty(data)?);
    }
    Ok(String::new())
}