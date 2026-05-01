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

/// Convenience: open a `"sql"` session, run one statement, delete the
/// session, and return the raw result.  Use when you only need a single
/// round-trip and don't want to manage the session lifetime yourself.
pub async fn one_shot_sql(client: &LivyClient, sql: &str, auth: &Auth) -> Result<StatementResult> {
    let sid = open_session(client, "sql", auth).await?;
    let result = run_sql(client, sid, sql, auth).await;
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
