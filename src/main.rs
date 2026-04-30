mod commands;
mod config;
mod client;
mod output;
mod notify;
mod gantt;
mod webhdfs;

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;

#[derive(Parser)]
#[command(
    name = "spark-ctrl",
    about = "Spark Control Plane CLI — orchestrate and manage Apache Spark clusters",
    version,
    propagate_version = true,
)]
struct Cli {
    /// Override the active profile
    #[arg(short, long, global = true, env = "SPARK_CTRL_PROFILE")]
    profile: Option<String>,

    /// Output format: table, json, csv
    #[arg(short, long, global = true, default_value = "table")]
    format: output::OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage environment profiles (add, list, switch, show)
    Profile(commands::profile::ProfileArgs),
    /// Job lifecycle: submit, status, logs, kill
    Job(commands::job::JobArgs),
    /// SQL queries and metadata inspection against Thrift/Livy
    Sql(commands::sql::SqlArgs),
    /// Lakehouse & storage file operations (ls, cp, rm)
    Fs(commands::fs::FsArgs),
    /// Diagnostics: health check, UI proxy, skew detection
    Diag(commands::diag::DiagArgs),
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{} {}", "error:".red().bold(), e);
        for cause in e.chain().skip(1) {
            eprintln!("  {} {}", "caused by:".dimmed(), cause);
        }
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let mut cfg = config::Config::load()?;
    if let Some(profile) = &cli.profile {
        cfg.set_active_profile(profile)?;
    }
    // Resolve SPARK_CTRL_TOKEN env var into auth for the active profile.
    if let Some(name) = cfg.active_profile.clone() {
        if let Some(profile) = cfg.profiles.get_mut(&name) {
            profile.auth = profile.auth.resolved();
        }
    }
    let fmt = cli.format;
    match cli.command {
        Commands::Profile(args) => commands::profile::run(args, &mut cfg).await,
        Commands::Job(args)     => commands::job::run(args, &cfg, fmt).await,
        Commands::Sql(args)     => commands::sql::run(args, &cfg, fmt).await,
        Commands::Fs(args)      => commands::fs::run(args, &cfg, fmt).await,
        Commands::Diag(args)    => commands::diag::run(args, &cfg, fmt).await,
    }
}
