use crate::client::LivyClient;
use crate::config::Config;
use crate::output::{print_kv, OutputFormat};
use anyhow::Result;
use clap::{Args, Subcommand};
use colored::Colorize;

#[derive(Args)]
pub struct DiagArgs {
    #[command(subcommand)]
    pub action: DiagAction,
}

#[derive(Subcommand)]
pub enum DiagAction {
    /// Check cluster health: Livy reachability, session count, resource summary
    Health,
    /// Open (or print) the Spark Web UI URL
    Ui {
        /// Application ID to navigate to in History Server
        #[arg(value_name = "APP_ID")]
        app_id: Option<String>,
        /// Just print the URL instead of opening a browser
        #[arg(long)]
        print_url: bool,
    },
    /// Detect task skew for an active/completed stage via Spark REST
    Skew {
        /// Spark application ID (e.g., application_1234_0001)
        #[arg(value_name = "APP_ID")]
        app_id: String,
        /// Stage ID to analyse
        #[arg(value_name = "STAGE_ID")]
        stage_id: u64,
        /// Flag stages where max_task_time / median_task_time > threshold
        #[arg(long, default_value = "3.0")]
        threshold: f64,
    },
    /// Show a Gantt chart of stage timelines for an application
    Timeline {
        /// Spark application ID
        #[arg(value_name = "APP_ID")]
        app_id: String,
        /// Width (columns) for the chart bar area
        #[arg(long, default_value = "55")]
        width: usize,
    },
}

pub async fn run(args: DiagArgs, cfg: &Config, fmt: OutputFormat) -> Result<()> {
    let (_, profile) = cfg.active_profile()?;
    let client = LivyClient::new(profile)?;
    let auth = &profile.auth;

    match args.action {
        DiagAction::Health               => health(&client, profile, auth, fmt).await,
        DiagAction::Ui { app_id, print_url } => ui(profile, app_id, print_url).await,
        DiagAction::Skew { app_id, stage_id, threshold } =>
            skew(&client, profile, &app_id, stage_id, threshold, fmt).await,
        DiagAction::Timeline { app_id, width } =>
            timeline(&client, profile, &app_id, width).await,
    }
}

// ─── health ───────────────────────────────────────────────────────────────────

async fn health(
    client: &LivyClient,
    profile: &crate::config::Profile,
    auth: &crate::config::Auth,
    fmt: OutputFormat,
) -> Result<()> {
    print!("{} Checking cluster health… ", "⟳".cyan());
    let v = client.health(auth).await?;
    let ok = v["ok"].as_bool().unwrap_or(false);
    let http_status = v["http_status"].as_u64().unwrap_or(0);

    if ok { println!("{}", "OK".green().bold()); }
    else  { println!("{}", "UNREACHABLE".red().bold()); }

    let batch_count = match client.list_batches(auth).await {
        Ok(b)  => b.len().to_string(),
        Err(_) => "—".to_string(),
    };

    print_kv(&[
        ("endpoint",    profile.master_url.clone()),
        ("backend",     profile.backend.to_string()),
        ("http_status", http_status.to_string()),
        ("reachable",   ok.to_string()),
        ("active_jobs", batch_count),
        ("thrift_url",  profile.thrift_url.clone().unwrap_or_else(|| "not configured".into())),
    ], fmt)
}

// ─── ui ───────────────────────────────────────────────────────────────────────

async fn ui(profile: &crate::config::Profile, app_id: Option<String>, print_url: bool) -> Result<()> {
    let base = profile.master_url.trim_end_matches('/');
    let url = match &app_id {
        Some(id) => format!("{}/history/{}/jobs/", base, id),
        None     => format!("{}/", base),
    };

    if print_url {
        println!("{}", url);
        return Ok(());
    }

    println!("{} Opening Spark UI: {}", "🌐".cyan(), url.cyan().underline());

    #[cfg(target_os = "macos")]
    std::process::Command::new("open").arg(&url).spawn().ok();
    #[cfg(target_os = "linux")]
    std::process::Command::new("xdg-open").arg(&url).spawn().ok();
    #[cfg(target_os = "windows")]
    std::process::Command::new("cmd").args(&["/c", "start", &url]).spawn().ok();

    println!("{}", "If the browser did not open, copy the URL above.".dimmed());
    Ok(())
}

// ─── skew detection ───────────────────────────────────────────────────────────

async fn skew(
    client: &LivyClient,
    profile: &crate::config::Profile,
    app_id: &str,
    stage_id: u64,
    threshold: f64,
    _fmt: OutputFormat,
) -> Result<()> {
    let path = format!("/api/v1/applications/{}/stages/{}", app_id, stage_id);
    println!("{} Fetching stage metrics for app={} stage={}", "⟳".cyan(), app_id.cyan(), stage_id);

    let stages = match client.spark_api_get(&path, &profile.auth).await {
        Err(e) => {
            println!("{} Could not reach Spark REST API: {}", "⚠".yellow(), e);
            println!("{}", "Ensure the History Server or Spark Master UI is reachable at master_url.".dimmed());
            return Ok(());
        }
        Ok(v) => v,
    };

    let arr = if stages.is_array() {
        stages.as_array().unwrap().clone()
    } else {
        vec![stages]
    };

    println!("{} Skew analysis — App: {} Stage: {}", "📊".cyan(), app_id.cyan(), stage_id);
    println!("{}", format!("  Threshold ratio: {:.1}×", threshold).dimmed());

    for stage in &arr {
        let tasks = match stage.get("tasks").and_then(|t| t.as_object()) {
            Some(t) => t,
            None => {
                println!("{}", "  Task metrics not yet available (stage may not have started).".dimmed());
                continue;
            }
        };

        let mut durations: Vec<f64> = tasks.values()
            .filter_map(|t| t["taskMetrics"]["executorRunTime"].as_f64())
            .collect();

        if durations.is_empty() {
            println!("{}", "  No executor run-time metrics found.".dimmed());
            continue;
        }

        durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = durations.len();
        let median = durations[n / 2];
        let max    = durations[n - 1];
        let p25    = durations[n / 4];
        let p75    = durations[(n * 3) / 4];
        let mean: f64 = durations.iter().sum::<f64>() / n as f64;
        let ratio  = if median > 0.0 { max / median } else { 0.0 };

        println!("  Task count       : {}", n);
        println!("  Min              : {:.1} ms", durations[0]);
        println!("  P25              : {:.1} ms", p25);
        println!("  Median (P50)     : {:.1} ms", median);
        println!("  Mean             : {:.1} ms", mean);
        println!("  P75              : {:.1} ms", p75);
        println!("  Max              : {:.1} ms", max);
        println!("  Skew ratio       : {:.2}×  (max / median)", ratio);

        if ratio >= threshold {
            println!("\n  {} {} SKEW DETECTED (ratio {:.1}× ≥ threshold {:.1}×)",
                "⚠".red(), "WARNING:".red().bold(), ratio, threshold);
            println!("  {}", "Recommended actions:".yellow().bold());
            println!("  {}", "  • Salt join keys if skew comes from a join".yellow());
            println!("  {}", "  • Use repartition() or coalesce() before the heavy stage".yellow());
            println!("  {}", "  • Filter or pre-aggregate the skewed partition".yellow());
            println!("  {}", "  • Enable AQE: spark.sql.adaptive.enabled=true".yellow());
        } else {
            println!("\n  {} Task distribution looks healthy (ratio {:.2}×).", "✓".green(), ratio);
        }
    }
    Ok(())
}

// ─── timeline (Gantt) ─────────────────────────────────────────────────────────

async fn timeline(
    client: &LivyClient,
    profile: &crate::config::Profile,
    app_id: &str,
    width: usize,
) -> Result<()> {
    let path = format!("/api/v1/applications/{}/stages", app_id);
    println!("{} Fetching stage list for app={}", "⟳".cyan(), app_id.cyan());

    let data = match client.spark_api_get(&path, &profile.auth).await {
        Err(e) => {
            println!("{} Could not reach Spark REST API: {}", "⚠".yellow(), e);
            println!("{}", "Ensure History Server is reachable at master_url.".dimmed());
            return Ok(());
        }
        Ok(v) => v,
    };

    let stages = crate::gantt::stages_from_json(&data);
    if stages.is_empty() {
        // No real data — show a demo so the user can see the format
        println!("{}", "(no stage data from API — showing demo chart)".yellow());
        let demo = vec![
            crate::gantt::GanttStage {
                stage_id: 0, name: "parallelize at script.py:5".into(),
                start_ms: 0, duration_ms: 1_200, status: "COMPLETE".into(), num_tasks: 4,
            },
            crate::gantt::GanttStage {
                stage_id: 1, name: "map at script.py:12".into(),
                start_ms: 1_100, duration_ms: 3_400, status: "COMPLETE".into(), num_tasks: 16,
            },
            crate::gantt::GanttStage {
                stage_id: 2, name: "reduceByKey at script.py:18".into(),
                start_ms: 4_300, duration_ms: 8_700, status: "ACTIVE".into(), num_tasks: 32,
            },
            crate::gantt::GanttStage {
                stage_id: 3, name: "saveAsTextFile at script.py:22".into(),
                start_ms: 12_900, duration_ms: 2_100, status: "PENDING".into(), num_tasks: 8,
            },
        ];
        crate::gantt::render(&demo, width);
    } else {
        println!("{} Stage timeline — App: {}", "📊".cyan(), app_id.cyan());
        crate::gantt::render(&stages, width);
    }
    Ok(())
}
