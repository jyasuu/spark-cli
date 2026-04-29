use crate::config::{Auth, Backend, Config, Profile};
use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;
use std::collections::HashMap;
use tabled::Tabled;
use serde::Serialize;

#[derive(Args)]
pub struct ProfileArgs {
    #[command(subcommand)]
    pub action: ProfileAction,
}

#[derive(Subcommand)]
pub enum ProfileAction {
    /// List all configured profiles
    List,
    /// Show details of the active (or named) profile
    Show {
        #[arg(value_name = "NAME")]
        name: Option<String>,
    },
    /// Add a new profile interactively via flags
    Add {
        #[arg(value_name = "NAME")]
        name: String,
        /// Cluster backend type: livy | kubernetes | yarn | standalone
        #[arg(long, default_value = "livy")]
        backend: Backend,
        /// Base URL of the master/Livy server
        #[arg(long)]
        master_url: String,
        /// Optional Thrift server (host:port) for SQL
        #[arg(long)]
        thrift_url: Option<String>,
        /// Kubernetes namespace (for k8s backend)
        #[arg(long)]
        namespace: Option<String>,
        /// Auth method: none | basic | bearer | kerberos
        #[arg(long, default_value = "none")]
        auth_method: String,
        /// Username for basic auth
        #[arg(long)]
        username: Option<String>,
        /// Token or password (prefer env var SPARK_CTRL_TOKEN)
        #[arg(long, env = "SPARK_CTRL_TOKEN")]
        token: Option<String>,
        /// Human-readable description
        #[arg(long)]
        description: Option<String>,
        /// Extra spark conf: key=value (repeatable)
        #[arg(long = "conf", value_name = "KEY=VALUE")]
        conf: Vec<String>,
    },
    /// Remove a profile
    Remove {
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Switch the active profile
    Switch {
        #[arg(value_name = "NAME")]
        name: String,
    },
}

pub async fn run(args: ProfileArgs, cfg: &mut Config) -> Result<()> {
    match args.action {
        ProfileAction::List => list(cfg),
        ProfileAction::Show { name } => show(cfg, name.as_deref()),
        ProfileAction::Add { name, backend, master_url, thrift_url, namespace,
                             auth_method, username, token, description, conf } => {
            add(cfg, name, backend, master_url, thrift_url, namespace,
                auth_method, username, token, description, conf)
        }
        ProfileAction::Remove { name } => remove(cfg, &name),
        ProfileAction::Switch { name } => switch(cfg, &name),
    }
}

// ──────────────────────────────────────────────────────────────────────────

#[derive(Tabled, Serialize)]
struct ProfileRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Active")]
    active: String,
    #[tabled(rename = "Backend")]
    backend: String,
    #[tabled(rename = "Master URL")]
    master_url: String,
    #[tabled(rename = "Description")]
    description: String,
}

fn list(cfg: &Config) -> Result<()> {
    if cfg.profiles.is_empty() {
        println!("{}", "No profiles configured. Run: spark-ctrl profile add <name> --master-url <url>".yellow());
        return Ok(());
    }
    let rows: Vec<ProfileRow> = cfg.profiles.iter().map(|(name, p)| {
        let is_active = cfg.active_profile.as_deref() == Some(name.as_str());
        ProfileRow {
            name: if is_active { format!("{} ★", name).green().to_string() } else { name.clone() },
            active: if is_active { "yes".green().to_string() } else { String::new() },
            backend: p.backend.to_string(),
            master_url: p.master_url.clone(),
            description: p.description.clone().unwrap_or_default(),
        }
    }).collect();
    use tabled::Table;
    println!("{}", Table::new(&rows));
    Ok(())
}

fn show(cfg: &Config, name: Option<&str>) -> Result<()> {
    let (name, profile) = if let Some(n) = name {
        let p = cfg.profiles.get(n)
            .ok_or_else(|| anyhow::anyhow!("profile '{}' not found", n))?;
        (n.to_string(), p)
    } else {
        let (n, p) = cfg.active_profile()?;
        (n.to_string(), p)
    };

    println!("{} {}", "Profile:".bold(), name.cyan());
    println!("  {:<16} {}", "backend:".dimmed(),    profile.backend);
    println!("  {:<16} {}", "master_url:".dimmed(),  profile.master_url);
    println!("  {:<16} {}", "thrift_url:".dimmed(),  profile.thrift_url.as_deref().unwrap_or("(none)"));
    println!("  {:<16} {}", "namespace:".dimmed(),   profile.namespace.as_deref().unwrap_or("(none)"));
    println!("  {:<16} {}", "auth_method:".dimmed(), profile.auth.method.as_deref().unwrap_or("none"));
    println!("  {:<16} {}", "description:".dimmed(), profile.description.as_deref().unwrap_or(""));
    if !profile.spark_conf.is_empty() {
        println!("  {}:", "spark_conf:".dimmed());
        for (k, v) in &profile.spark_conf {
            println!("    {} = {}", k.yellow(), v);
        }
    }
    Ok(())
}

fn add(
    cfg: &mut Config, name: String, backend: Backend,
    master_url: String, thrift_url: Option<String>, namespace: Option<String>,
    auth_method: String, username: Option<String>, token: Option<String>,
    description: Option<String>, conf: Vec<String>,
) -> Result<()> {
    let mut spark_conf = HashMap::new();
    for kv in &conf {
        let parts: Vec<&str> = kv.splitn(2, '=').collect();
        if parts.len() != 2 {
            anyhow::bail!("--conf must be KEY=VALUE, got '{}'", kv);
        }
        spark_conf.insert(parts[0].to_string(), parts[1].to_string());
    }
    let profile = Profile {
        backend,
        master_url,
        thrift_url,
        namespace,
        auth: Auth {
            method: if auth_method == "none" { None } else { Some(auth_method) },
            username,
            token,
            keytab_path: None,
        },
        spark_conf,
        description,
    };
    cfg.add_profile(name.clone(), profile)?;
    println!("{} profile '{}' added", "✓".green(), name.cyan());
    Ok(())
}

fn remove(cfg: &mut Config, name: &str) -> Result<()> {
    cfg.remove_profile(name)?;
    println!("{} profile '{}' removed", "✓".green(), name.cyan());
    Ok(())
}

fn switch(cfg: &mut Config, name: &str) -> Result<()> {
    cfg.set_active_profile(name)?;
    cfg.save()?;
    println!("{} switched to profile '{}'", "✓".green(), name.cyan());
    Ok(())
}
