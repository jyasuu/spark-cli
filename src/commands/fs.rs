use crate::client::LivyClient;
use crate::commands::session::{extract_text, one_shot_sql};
use crate::config::Config;
use crate::output::{print_rows, OutputFormat};
use crate::webhdfs::WebHdfsClient;
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use colored::Colorize;
use serde::Serialize;
use tabled::Tabled;

#[derive(Args)]
pub struct FsArgs {
    #[command(subcommand)]
    pub action: FsAction,
}

#[derive(Subcommand)]
pub enum FsAction {
    /// List files/directories at a path (HDFS, S3, ADLS)
    Ls {
        #[arg(value_name = "PATH")]
        path: String,
        /// Show hidden files (starting with '.')
        #[arg(short, long)]
        all: bool,
        /// Long listing format
        #[arg(short, long)]
        long: bool,
    },
    /// Copy a file between paths
    Cp {
        #[arg(value_name = "SRC")]
        src: String,
        #[arg(value_name = "DST")]
        dst: String,
        /// Overwrite destination if it already exists
        #[arg(long, short)]
        overwrite: bool,
    },
    /// Delete a file or directory
    Rm {
        #[arg(value_name = "PATH")]
        path: String,
        /// Recursive delete
        #[arg(short, long)]
        recursive: bool,
    },
    /// Create a directory (and any missing parents)
    Mkdir {
        #[arg(value_name = "PATH")]
        path: String,
    },
    /// Inspect a Delta/Iceberg table (snapshot list, vacuum)
    Table {
        #[command(subcommand)]
        action: TableAction,
    },
}

#[derive(Subcommand)]
pub enum TableAction {
    /// List Delta snapshots or Iceberg snapshots
    Snapshots {
        #[arg(value_name = "TABLE")]
        table: String,
        /// Table format: delta | iceberg
        #[arg(long, default_value = "delta")]
        format: String,
    },
    /// Vacuum old Delta files (dry-run by default)
    Vacuum {
        #[arg(value_name = "TABLE")]
        table: String,
        /// Retention hours (Delta default is 168)
        #[arg(long, default_value = "168")]
        retain_hours: u64,
        /// Actually run the vacuum (omit for dry-run)
        #[arg(long)]
        execute: bool,
    },
}

pub async fn run(args: FsArgs, cfg: &Config, fmt: OutputFormat) -> Result<()> {
    let (_, profile) = cfg.active_profile()?;

    match args.action {
        FsAction::Ls { path, all, long } => ls(cfg, profile, &path, all, long, fmt).await,
        FsAction::Cp { src, dst, overwrite } => cp(cfg, profile, &src, &dst, overwrite).await,
        FsAction::Rm { path, recursive } => rm(cfg, profile, &path, recursive).await,
        FsAction::Mkdir { path } => mkdir(cfg, profile, &path).await,
        FsAction::Table { action } => table_op(cfg, profile, action, fmt).await,
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Helpers: For demonstration these emit the Livy SQL statements that would
// perform the FS/table operations via a Spark session.  A production
// implementation would call HDFS HttpFS, S3 API, or WebHDFS directly.
// ──────────────────────────────────────────────────────────────────────────

async fn ls(_cfg: &Config, profile: &crate::config::Profile, path: &str, all: bool, long: bool, fmt: OutputFormat) -> Result<()> {
    // Detect WebHDFS vs S3/ADLS path and pick strategy
    let hdfs_base = hdfs_base_url(profile, path);

    #[derive(Tabled, Serialize)]
    struct FileRow {
        #[tabled(rename = "Type")]     type_sym: String,
        #[tabled(rename = "Name")]     name: String,
        #[tabled(rename = "Size")]     size: String,
        #[tabled(rename = "Modified")] modified: String,
        #[tabled(rename = "Owner")]    owner: String,
        #[tabled(rename = "Perms")]    perms: String,
    }

    if let Some(base_url) = hdfs_base {
        // Real WebHDFS call (blocking, run in spawn_blocking)
        let path_owned = path.to_string();
        let auth = profile.auth.clone();
        let result = tokio::task::spawn_blocking(move || {
            WebHdfsClient::new(&base_url).ls(&path_owned, &auth)
        }).await?;

        match result {
            Err(e) => {
                println!("{} WebHDFS error: {}", "⚠".yellow(), e);
                println!("{}", "Check that the namenode is reachable and WebHDFS is enabled.".dimmed());
                return Ok(());
            }
            Ok(entries) => {
                let rows: Vec<FileRow> = entries.iter()
                    .filter(|e| all || !e.path_suffix.starts_with('.'))
                    .map(|e| FileRow {
                        type_sym: e.type_symbol().into(),
                        name:     e.path_suffix.clone(),
                        size:     if e.r#type == "DIRECTORY" { "-".into() } else { e.size_human() },
                        modified: e.modified(),
                        owner:    e.owner.clone(),
                        perms:    if long { format!("{} r={}", e.permission, e.replication) } else { e.permission.clone() },
                    })
                    .collect();

                if rows.is_empty() {
                    println!("{}", "(empty directory)".dimmed());
                } else {
                    print_rows(&rows, fmt)?;
                }
            }
        }
    } else {
        // Non-HDFS path: show informative stub
        println!("{} {}", "ls".cyan().bold(), path);
        println!("{}", "Path appears to be S3/ADLS/local — WebHDFS client only supports hdfs:// paths.".yellow());
        println!("{}", "For S3: configure AWS CLI and use 'aws s3 ls'. For ADLS: use 'az storage fs'.".dimmed());

        // Demo row so the output format is still visible
        let rows = vec![
            FileRow { type_sym: "-".into(), name: "part-00000.snappy.parquet".into(), size: "42.0 MB".into(),
                      modified: "2024-11-01 11:58".into(), owner: "spark".into(), perms: "644".into() },
            FileRow { type_sym: "-".into(), name: "part-00001.snappy.parquet".into(), size: "41.3 MB".into(),
                      modified: "2024-11-01 11:58".into(), owner: "spark".into(), perms: "644".into() },
        ];
        println!("{}", "(demo output)".dimmed());
        print_rows(&rows, fmt)?;
    }
    Ok(())
}

async fn cp(_cfg: &Config, profile: &crate::config::Profile, src: &str, dst: &str, overwrite: bool) -> Result<()> {
    let src_hdfs = hdfs_base_url(profile, src);
    let dst_hdfs = hdfs_base_url(profile, dst);

    match (src_hdfs, dst_hdfs) {
        // ── both paths are HDFS: stream via WebHDFS ──────────────────────────
        (Some(src_base), Some(dst_base)) => {
            println!("{} {} → {}", "cp".cyan().bold(), src, dst);

            let src_path  = hdfs_path_only(src);
            let dst_path  = hdfs_path_only(dst);
            let src_base2 = src_base.clone();
            let dst_base2 = dst_base.clone();
            let auth      = profile.auth.clone();
            let overwrite2 = overwrite;

            tokio::task::spawn_blocking(move || {
                let src_client = WebHdfsClient::new(&src_base2);
                let dst_client = WebHdfsClient::new(&dst_base2);

                // 1. Stat the source to get its size for progress reporting
                let stat = src_client.stat(&src_path, &auth)?;
                if stat.r#type == "DIRECTORY" {
                    anyhow::bail!(
                        "source '{}' is a directory — use 'fs cp' on individual files or \
                         'hadoop fs -cp' for recursive directory copies",
                        src_path
                    );
                }

                // 2. Read the full file into memory via OPEN
                let data = src_client.read(&src_path, &auth)?;

                // 3. Write to destination via CREATE
                let written = dst_client.write(&dst_path, &data, overwrite2, &auth)?;
                if !written {
                    anyhow::bail!("WebHDFS CREATE returned false for '{}'", dst_path);
                }

                Ok::<_, anyhow::Error>(stat.length)
            }).await??;

            println!("{} copied", "✓".green());
        }

        // ── local → HDFS upload ───────────────────────────────────────────────
        (None, Some(dst_base)) if !src.starts_with("s3") && !src.starts_with("adl") => {
            println!("{} {} → {} (local → HDFS)", "cp".cyan().bold(), src, dst);
            let data = std::fs::read(src)
                .with_context(|| format!("reading local file '{}'", src))?;
            let data_len  = data.len();
            let dst_path  = hdfs_path_only(dst);
            let auth      = profile.auth.clone();
            let overwrite2 = overwrite;
            tokio::task::spawn_blocking(move || {
                WebHdfsClient::new(&dst_base).write(&dst_path, &data, overwrite2, &auth)
            }).await??;
            println!("{} uploaded {} bytes", "✓".green(), data_len);
        }

        // ── HDFS → local download ─────────────────────────────────────────────
        (Some(src_base), None) if !dst.starts_with("s3") && !dst.starts_with("adl") => {
            println!("{} {} → {} (HDFS → local)", "cp".cyan().bold(), src, dst);
            let src_path = hdfs_path_only(src);
            let auth     = profile.auth.clone();
            let data = tokio::task::spawn_blocking(move || {
                WebHdfsClient::new(&src_base).read(&src_path, &auth)
            }).await??;
            std::fs::write(dst, &data)
                .with_context(|| format!("writing local file '{}'", dst))?;
            println!("{} downloaded {} bytes", "✓".green(), data.len());
        }

        // ── S3 / ADLS or unsupported combination ──────────────────────────────
        _ => {
            println!("{} {} → {}", "cp".cyan().bold(), src, dst);
            println!("{}", "Non-HDFS paths require the respective cloud CLI:".yellow());
            println!("{}", "  S3:   aws s3 cp <src> <dst>".dimmed());
            println!("{}", "  ADLS: az storage fs file upload/download".dimmed());
            println!("{}", "  GCS:  gsutil cp <src> <dst>".dimmed());
        }
    }
    Ok(())
}

async fn mkdir(_cfg: &Config, profile: &crate::config::Profile, path: &str) -> Result<()> {
    let hdfs_base = hdfs_base_url(profile, path);

    if let Some(base_url) = hdfs_base {
        let path_owned = hdfs_path_only(path);
        let auth = profile.auth.clone();
        let created = tokio::task::spawn_blocking(move || {
            WebHdfsClient::new(&base_url).mkdir(&path_owned, &auth)
        }).await??;

        if created {
            println!("{} created directory {}", "✓".green(), path.cyan());
        } else {
            println!("{} directory may already exist: {}", "⚠".yellow(), path);
        }
    } else {
        println!("{} mkdir only supported for HDFS paths (hdfs://...)", "⚠".yellow());
        println!("{}", "For S3: aws s3api put-object --bucket <b> --key <prefix>/".dimmed());
    }
    Ok(())
}

async fn rm(_cfg: &Config, profile: &crate::config::Profile, path: &str, recursive: bool) -> Result<()> {
    let hdfs_base = hdfs_base_url(profile, path);

    if let Some(base_url) = hdfs_base {
        let path_owned = path.to_string();
        let auth = profile.auth.clone();
        let deleted = tokio::task::spawn_blocking(move || {
            WebHdfsClient::new(&base_url).rm(&path_owned, recursive, &auth)
        }).await??;

        if deleted {
            println!("{} deleted {}", "✓".green(), path.cyan());
        } else {
            println!("{} path not found or already deleted: {}", "⚠".yellow(), path);
        }
    } else {
        if recursive {
            println!("{} {} {}", "rm -r".cyan().bold(), path, "(recursive)".red());
        } else {
            println!("{} {}", "rm".cyan().bold(), path);
        }
        println!("{}", "Non-HDFS paths: use aws s3 rm / az storage fs file delete.".yellow());
    }
    Ok(())
}

/// Derive a WebHDFS base URL from the profile's master_url or from the path itself.
/// Returns None if the path/profile doesn't look like HDFS.
fn hdfs_base_url(profile: &crate::config::Profile, path: &str) -> Option<String> {
    // If the path is an hdfs:// URI, extract host:port from it
    if let Some(rest) = path.strip_prefix("hdfs://") {
        let host_end = rest.find('/').unwrap_or(rest.len());
        return Some(format!("http://{}", &rest[..host_end]));
    }
    // If master_url looks like a namenode HTTP endpoint (port 9870/50070/14000)
    let url = &profile.master_url;
    if url.contains(":9870") || url.contains(":50070") || url.contains(":14000") {
        return Some(url.clone());
    }
    None
}

/// Extract just the HDFS path component from a full `hdfs://host:port/path` URI.
/// Returns the original string unchanged for bare paths (e.g. `/user/spark/data`).
fn hdfs_path_only(uri: &str) -> String {
    if let Some(rest) = uri.strip_prefix("hdfs://") {
        // rest = "host:port/path/to/file"
        if let Some(slash) = rest.find('/') {
            return rest[slash..].to_string();
        }
        return "/".to_string();
    }
    uri.to_string()
}

async fn table_op(_cfg: &Config, profile: &crate::config::Profile, action: TableAction, fmt: OutputFormat) -> Result<()> {
    match action {
        TableAction::Snapshots { table, format } => {
            let sql = match format.as_str() {
                "iceberg" => format!(
                    "SELECT snapshot_id, committed_at, operation \
                     FROM {table}.snapshots \
                     ORDER BY committed_at DESC \
                     LIMIT 20"
                ),
                _ => format!("DESCRIBE HISTORY {table} LIMIT 20"),
            };

            println!("{} snapshots for {} ({})", "📸".cyan(), table.cyan(), format.dimmed());
            println!("{} {}", "SQL:".dimmed(), sql.dimmed());

            let client = LivyClient::new(profile)?;
            let result = one_shot_sql(&client, &sql, &profile.auth).await?;
            let text = extract_text(&result)?;

            if text.trim().is_empty() {
                println!("{}", "(no snapshots found)".dimmed());
                return Ok(());
            }

            // Livy returns text/plain as a tab-separated table.
            // Skip the header line and parse into display rows.
            #[derive(Tabled, Serialize)]
            struct SnapRow {
                #[tabled(rename = "snapshot_id")] id: String,
                #[tabled(rename = "committed_at")] ts: String,
                #[tabled(rename = "operation")]    op: String,
            }

            let rows: Vec<SnapRow> = text
                .lines()
                .skip(1)
                .filter(|l| !l.trim().is_empty())
                .map(|l| {
                    let mut parts = l.splitn(3, '\t');
                    SnapRow {
                        id: parts.next().unwrap_or("").trim().to_string(),
                        ts: parts.next().unwrap_or("").trim().to_string(),
                        op: parts.next().unwrap_or("").trim().to_string(),
                    }
                })
                .collect();

            if rows.is_empty() {
                println!("{}", text);
            } else {
                print_rows(&rows, fmt)?;
            }
        }

        TableAction::Vacuum { table, retain_hours, execute } => {
            if !execute {
                println!("{} VACUUM {} RETAIN {} HOURS (dry-run)", "🧹".cyan(), table.cyan(), retain_hours);
                println!("{}", "Add --execute to actually run the vacuum.".yellow());
                return Ok(());
            }
            let sql = format!("VACUUM {table} RETAIN {retain_hours} HOURS");
            println!("{} Running {}…", "🧹".green(), sql.dimmed());
            let client = LivyClient::new(profile)?;
            let result = one_shot_sql(&client, &sql, &profile.auth).await?;
            extract_text(&result)?;
            println!("{} Vacuum complete.", "✓".green());
        }
    }
    Ok(())
}
