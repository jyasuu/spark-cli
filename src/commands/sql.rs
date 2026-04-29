use crate::client::LivyClient;
use crate::config::Config;
use crate::output::OutputFormat;
use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;
use std::time::Duration;

#[derive(Args)]
pub struct SqlArgs {
    #[command(subcommand)]
    pub action: SqlAction,
}

#[derive(Subcommand)]
pub enum SqlAction {
    /// Execute a SQL query against a Livy session or Thrift server
    Query {
        /// SQL statement to execute
        #[arg(value_name = "SQL")]
        sql: String,
        /// Session kind: sql | spark | pyspark
        #[arg(long, default_value = "sql")]
        kind: String,
    },
    /// List databases, tables, or describe a table schema
    Inspect {
        #[command(subcommand)]
        target: InspectTarget,
    },
    /// Run a SQL query and export results to a local file
    Export {
        /// SQL statement
        #[arg(value_name = "SQL")]
        sql: String,
        /// Output file path
        #[arg(short, long)]
        output: String,
        /// Output format: csv | json
        #[arg(short, long, default_value = "csv")]
        fmt: String,
    },
}

#[derive(Subcommand)]
pub enum InspectTarget {
    /// List all databases
    Databases,
    /// List tables in a database
    Tables {
        #[arg(value_name = "DATABASE", default_value = "default")]
        database: String,
    },
    /// Show schema for a table
    Schema {
        #[arg(value_name = "TABLE")]
        table: String,
        /// Database containing the table
        #[arg(long, default_value = "default")]
        database: String,
    },
    /// Show partitions for a table
    Partitions {
        #[arg(value_name = "TABLE")]
        table: String,
    },
}

pub async fn run(args: SqlArgs, cfg: &Config, fmt: OutputFormat) -> Result<()> {
    let (_, profile) = cfg.active_profile()?;
    let client = LivyClient::new(profile)?;
    let auth = &profile.auth;

    match args.action {
        SqlAction::Query { sql, kind } => {
            let session = open_session(&client, &kind, auth).await?;
            let result = client.run_statement(session, &sql, auth).await?;
            print_statement_result(&result, fmt)?;
            client.delete_session(session, auth).await.ok();
        }

        SqlAction::Inspect { target } => {
            let session = open_session(&client, "sql", auth).await?;
            let sql = match &target {
                InspectTarget::Databases         => "SHOW DATABASES".to_string(),
                InspectTarget::Tables { database } => format!("SHOW TABLES IN {}", database),
                InspectTarget::Schema { table, database } =>
                    format!("DESCRIBE {}.{}", database, table),
                InspectTarget::Partitions { table } =>
                    format!("SHOW PARTITIONS {}", table),
            };
            let result = client.run_statement(session, &sql, auth).await?;
            print_statement_result(&result, fmt)?;
            client.delete_session(session, auth).await.ok();
        }

        SqlAction::Export { sql, output, fmt: out_fmt } => {
            let session = open_session(&client, "sql", auth).await?;
            let result = client.run_statement(session, &sql, auth).await?;
            client.delete_session(session, auth).await.ok();

            if result.status != "ok" {
                anyhow::bail!("query failed: {} — {}", result.ename.unwrap_or_default(), result.evalue.unwrap_or_default());
            }
            let data = result.data.as_ref().and_then(|d| d.get("text/plain"))
                .or_else(|| result.data.as_ref())
                .ok_or_else(|| anyhow::anyhow!("no data in result"))?;

            let content = match out_fmt.as_str() {
                "json" => serde_json::to_string_pretty(data)?,
                _      => data.to_string(),  // plain text/csv passthrough
            };
            std::fs::write(&output, &content)?;
            println!("{} exported {} bytes to '{}'", "✓".green(), content.len(), output.cyan());
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────

/// Create a Livy session and wait until idle.
async fn open_session(client: &LivyClient, kind: &str, auth: &crate::config::Auth) -> Result<u64> {
    let session = client.create_session(kind, auth).await?;
    let id = session.id;
    print!("{} creating session {}…", "⟳".cyan(), id);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let s = client.get_session(id, auth).await?;
        match s.state.as_str() {
            "idle"  => { println!(" {}", "ready".green()); return Ok(id); }
            "error" | "dead" => {
                println!(" {}", "failed".red());
                anyhow::bail!("session {} failed to start", id);
            }
            _ => print!("."),
        }
        use std::io::Write;
        std::io::stdout().flush().ok();
    }
}

fn print_statement_result(result: &crate::client::StatementResult, _fmt: OutputFormat) -> Result<()> {
    if result.status != "ok" {
        eprintln!("{} {}: {}", "error".red().bold(),
            result.ename.as_deref().unwrap_or(""),
            result.evalue.as_deref().unwrap_or("unknown"));
        return Ok(());
    }

    if let Some(data) = &result.data {
        // Livy returns data keyed by MIME type
        if let Some(text) = data.get("text/plain") {
            println!("{}", text.as_str().unwrap_or(&text.to_string()));
        } else if let Some(json) = data.get("application/json") {
            println!("{}", serde_json::to_string_pretty(json)?);
        } else {
            println!("{}", serde_json::to_string_pretty(data)?);
        }
    } else {
        println!("{}", "(no output)".dimmed());
    }
    Ok(())
}
