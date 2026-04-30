use crate::client::{BatchRequest, LivyClient};
use crate::config::Config;
use crate::output::{print_rows, OutputFormat};
use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::collections::HashMap;
use tabled::Tabled;
use std::time::Duration;

#[derive(Args)]
pub struct JobArgs {
    #[command(subcommand)]
    pub action: JobAction,
}

#[derive(Subcommand)]
pub enum JobAction {
    /// Submit a JAR/Python/R job to Spark via Livy
    Submit {
        /// Path to the JAR, Python, or R file (local path or HDFS/S3 URI)
        #[arg(value_name = "FILE")]
        file: String,
        /// Java main class (required for JARs)
        #[arg(long)]
        class: Option<String>,
        /// Application name
        #[arg(long)]
        name: Option<String>,
        /// Driver memory (e.g., 2g)
        #[arg(long)]
        driver_memory: Option<String>,
        /// Executor memory (e.g., 4g)
        #[arg(long)]
        executor_memory: Option<String>,
        /// Number of executors
        #[arg(long)]
        num_executors: Option<u32>,
        /// Executor cores
        #[arg(long)]
        executor_cores: Option<u32>,
        /// Extra spark conf KEY=VALUE (repeatable)
        #[arg(long = "conf", value_name = "KEY=VALUE")]
        conf: Vec<String>,
        /// Additional JARs (repeatable)
        #[arg(long)]
        jars: Vec<String>,
        /// Additional Python files (repeatable)
        #[arg(long)]
        py_files: Vec<String>,
        /// Arguments to pass to the application
        #[arg(last = true)]
        args: Vec<String>,
        /// Wait for completion and stream status
        #[arg(long, short)]
        wait: bool,
        /// Send a desktop notification when the job finishes
        #[arg(long)]
        notify: bool,
    },
    /// Show the status of a batch job
    Status {
        #[arg(value_name = "ID")]
        id: u64,
    },
    /// List all batch jobs
    List,
    /// Stream logs from a running or completed job
    Logs {
        #[arg(value_name = "ID")]
        id: u64,
        /// Follow (tail) the log output
        #[arg(long, short)]
        follow: bool,
    },
    /// Kill an active job by ID
    Kill {
        #[arg(value_name = "ID")]
        id: u64,
    },
}

pub async fn run(args: JobArgs, cfg: &Config, fmt: OutputFormat) -> Result<()> {
    let (_, profile) = cfg.active_profile()?;
    let client = LivyClient::new(profile)?;
    let auth = &profile.auth;

    match args.action {
        JobAction::Submit { file, class, name, driver_memory, executor_memory,
                            num_executors, executor_cores, conf, jars, py_files, args: app_args, wait, notify } => {
            let mut spark_conf: HashMap<String, String> = profile.spark_conf.clone();
            for kv in &conf {
                let parts: Vec<&str> = kv.splitn(2, '=').collect();
                if parts.len() != 2 { anyhow::bail!("--conf must be KEY=VALUE"); }
                spark_conf.insert(parts[0].into(), parts[1].into());
            }
            let req = BatchRequest {
                file, class_name: class, name: name.clone(),
                args: app_args, jars, py_files, files: vec![],
                driver_memory, driver_cores: None,
                executor_memory, executor_cores, num_executors,
                conf: spark_conf, queue: None,
            };

            let batch = client.submit_batch(&req, auth).await?;
            println!("{} job submitted — id: {}", "✓".green(), batch.id.to_string().cyan());
            if let Some(n) = &name { println!("  name: {}", n); }

            if wait || notify {
                watch_job(&client, batch.id, name.as_deref(), notify, auth).await?;
            }
        }

        JobAction::Status { id } => {
            let batch = client.get_batch(id, auth).await?;
            let state_colored = colorize_state(&batch.state);
            println!("Job {} — {}", id.to_string().cyan(), state_colored);
            if let Some(app_id) = &batch.app_id {
                println!("  appId: {}", app_id);
            }
        }

        JobAction::List => {
            let batches = client.list_batches(auth).await?;
            if batches.is_empty() {
                println!("{}", "No jobs found.".dimmed());
                return Ok(());
            }
            #[derive(Tabled, Serialize)]
            struct Row {
                #[tabled(rename = "ID")]   id: u64,
                #[tabled(rename = "State")] state: String,
                #[tabled(rename = "App ID")] app_id: String,
            }
            let rows: Vec<Row> = batches.iter().map(|b| Row {
                id: b.id,
                state: b.state.clone(),
                app_id: b.app_id.clone().unwrap_or_default(),
            }).collect();
            print_rows(&rows, fmt)?;
        }

        JobAction::Logs { id, follow } => {
            stream_logs(&client, id, follow, auth).await?;
        }

        JobAction::Kill { id } => {
            client.delete_batch(id, auth).await?;
            println!("{} job {} killed", "✓".green(), id.to_string().cyan());
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────

async fn watch_job(client: &LivyClient, id: u64, name: Option<&str>, notify: bool, auth: &crate::config::Auth) -> Result<()> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.cyan} [{elapsed}] {msg}")?
        .tick_strings(&["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]));
    pb.enable_steady_tick(Duration::from_millis(80));

    loop {
        let batch = client.get_batch(id, auth).await?;
        pb.set_message(format!("state: {}", batch.state));
        match batch.state.as_str() {
            "success" | "dead" | "error" => {
                pb.finish_and_clear();
                let final_state = colorize_state(&batch.state);
                println!("Final state: {}", final_state);
                if notify {
                    let job_label = name.unwrap_or("Job");
                    let emoji = if batch.state == "success" { "✅" } else { "❌" };
                    crate::notify::send(
                        &format!("spark-ctrl — {}", job_label),
                        &format!("{} {} (id: {})", emoji, batch.state.to_uppercase(), id),
                    );
                }
                break;
            }
            _ => {}
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    Ok(())
}

async fn stream_logs(client: &LivyClient, id: u64, follow: bool, auth: &crate::config::Auth) -> Result<()> {
    let mut from: i64 = 0;
    loop {
        let chunk = client.get_batch_log(id, from, 100, auth).await?;
        for line in &chunk.log {
            println!("{}", line);
        }
        from = chunk.from + chunk.log.len() as i64;
        if !follow || from >= chunk.total { break; }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

fn colorize_state(state: &str) -> String {
    match state {
        "success"   => state.green().to_string(),
        "dead" | "error" => state.red().to_string(),
        "running"   => state.yellow().to_string(),
        _           => state.dimmed().to_string(),
    }
}
