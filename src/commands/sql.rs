use crate::client::LivyClient;
use crate::commands::session::{extract_text, one_shot_sql, open_session};
use crate::config::Config;
use crate::output::OutputFormat;
use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;
use chrono::Local;

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
    /// Show recent SQL query history
    History {
        /// Maximum number of entries to show
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },
    /// Launch an interactive SQL REPL session
    Repl {
        /// Session kind: sql | spark | pyspark
        #[arg(long, default_value = "sql")]
        kind: String,
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
            append_history(&sql);
            let session = open_session(&client, &kind, auth).await?;
            let result = client.run_statement(session, &sql, auth).await?;
            print_statement_result(&result, fmt)?;
            client.delete_session(session, auth).await.ok();
        }

        SqlAction::Inspect { target } => {
            let sql_str = match &target {
                InspectTarget::Databases           => "SHOW DATABASES".to_string(),
                InspectTarget::Tables { database } => format!("SHOW TABLES IN {}", database),
                InspectTarget::Schema { table, database } =>
                    format!("DESCRIBE {}.{}", database, table),
                InspectTarget::Partitions { table } =>
                    format!("SHOW PARTITIONS {}", table),
            };
            let result = one_shot_sql(&client, &sql_str, auth).await?;
            print_statement_result(&result, fmt)?;
        }

        SqlAction::Export { sql, output, fmt: out_fmt } => {
            append_history(&sql);
            let result = one_shot_sql(&client, &sql, auth).await?;

            let text = extract_text(&result)?;
            let content = match out_fmt.as_str() {
                "json" => {
                    // Try to pretty-print if the text payload is JSON; otherwise wrap it
                    match serde_json::from_str::<serde_json::Value>(&text) {
                        Ok(v)  => serde_json::to_string_pretty(&v)?,
                        Err(_) => text,
                    }
                }
                _ => text,   // csv / plain text passthrough
            };
            std::fs::write(&output, &content)?;
            println!("{} exported {} bytes to '{}'", "✓".green(), content.len(), output.cyan());
        }

        SqlAction::History { limit } => {
            let log_path = dirs::data_local_dir()
                .map(|d| d.join("spark-ctrl").join("sql_history.log"));
            match log_path {
                None => anyhow::bail!("cannot determine data directory"),
                Some(path) if !path.exists() => {
                    println!("{}", "No SQL history yet.".dimmed());
                }
                Some(path) => {
                    let content = std::fs::read_to_string(&path)?;
                    let lines: Vec<&str> = content.lines().collect();
                    let start = lines.len().saturating_sub(limit);
                    for line in &lines[start..] {
                        println!("{}", line);
                    }
                    println!("{}", format!("  (log: {})", path.display()).dimmed());
                }
            }
        }

        SqlAction::Repl { kind } => {
            crate::commands::repl::run(profile, auth, &kind).await?;
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────

/// Append a SQL statement to the history log at
/// `~/.local/share/spark-ctrl/sql_history.log`.
/// Failures are silently ignored so a missing directory never breaks queries.
fn append_history(sql: &str) {
    let Ok(data_dir) = dirs::data_local_dir().ok_or(()) else { return };
    let dir = data_dir.join("spark-ctrl");
    if std::fs::create_dir_all(&dir).is_err() { return; }
    let ts = Local::now().format("%Y-%m-%d %H:%M:%S");
    let line = format!("[{}] {}\n", ts, sql.replace('\n', " "));
    let _ = std::fs::OpenOptions::new()
        .create(true).append(true)
        .open(dir.join("sql_history.log"))
        .and_then(|mut f| { use std::io::Write; f.write_all(line.as_bytes()) });
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
