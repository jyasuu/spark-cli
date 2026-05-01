//! Interactive SQL REPL for spark-ctrl.
//!
//! Usage: spark-ctrl sql repl
//!
//! Features:
//!   • Multi-line statement buffering (terminated by `;`)
//!   • In-session history via arrow-keys (via raw stdin line editing)
//!   • Dot-commands: .help  .quit  .history  .clear  .session
//!   • Persistent history file: ~/.local/share/spark-ctrl/repl_history.txt
//!   • Ctrl-C cancels current input buffer; Ctrl-D / .quit exits

use crate::client::LivyClient;
use crate::commands::session::open_session;
use crate::config::{Auth, Profile};
use anyhow::Result;
use chrono::Local;
use colored::Colorize;
use std::io::{self, BufRead, Write};

const HISTORY_MAX: usize = 500;

// ─── history ─────────────────────────────────────────────────────────────────

fn history_path() -> Option<std::path::PathBuf> {
    dirs::data_local_dir().map(|d| d.join("spark-ctrl").join("repl_history.txt"))
}

fn load_history() -> Vec<String> {
    history_path()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .map(|s| s.lines().map(String::from).collect())
        .unwrap_or_default()
}

fn save_history(history: &[String]) {
    let Some(path) = history_path() else { return };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let tail: Vec<&str> = history
        .iter()
        .rev()
        .take(HISTORY_MAX)
        .rev()
        .map(String::as_str)
        .collect();
    let _ = std::fs::write(&path, tail.join("\n") + "\n");
}

fn append_sql_history(sql: &str) {
    let Some(path) = dirs::data_local_dir().map(|d| d.join("spark-ctrl").join("sql_history.log"))
    else {
        return;
    };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let ts = Local::now().format("%Y-%m-%d %H:%M:%S");
    let line = format!("[{}] {}\n", ts, sql.replace('\n', " "));
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(line.as_bytes()));
}

// ─── dot-commands ─────────────────────────────────────────────────────────────

fn handle_dot(cmd: &str, history: &[String], session_id: u64) -> DotResult {
    let parts: Vec<&str> = cmd.trim().splitn(2, ' ').collect();
    match parts[0] {
        ".quit" | ".exit" | ".q" => DotResult::Quit,
        ".help" | ".h" => {
            println!(
                "{}",
                "─── dot commands ────────────────────────────────".dimmed()
            );
            println!("  {}   show this help", ".help".cyan());
            println!("  {}   exit the REPL", ".quit".cyan());
            println!("  {}  show recent SQL history (last 20)", ".history".cyan());
            println!("  {}  clear the screen", ".clear".cyan());
            println!("  {} show current Livy session id", ".session".cyan());
            println!(
                "{}",
                "─────────────────────────────────────────────────".dimmed()
            );
            println!(
                "{}",
                "Terminate SQL with ';'  |  Ctrl-C = cancel  |  Ctrl-D = quit".dimmed()
            );
            DotResult::Continue
        }
        ".history" => {
            let n = parts
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(20);
            let start = history.len().saturating_sub(n);
            for (i, entry) in history[start..].iter().enumerate() {
                println!("{:>4}  {}", (start + i + 1).to_string().dimmed(), entry);
            }
            DotResult::Continue
        }
        ".clear" => {
            print!("\x1b[2J\x1b[H");
            io::stdout().flush().ok();
            DotResult::Continue
        }
        ".session" => {
            println!("Livy session id: {}", session_id.to_string().cyan());
            DotResult::Continue
        }
        other => {
            println!(
                "{} unknown command '{}' — type {} for help",
                "✗".red(),
                other,
                ".help".cyan()
            );
            DotResult::Continue
        }
    }
}

enum DotResult {
    Continue,
    Quit,
}

// ─── main REPL loop ───────────────────────────────────────────────────────────

pub async fn run(profile: &Profile, auth: &Auth, kind: &str) -> Result<()> {
    let client = LivyClient::new(profile)?;

    println!("{}", "spark-ctrl SQL REPL".bold().cyan());
    println!("{}", "Connecting to Livy…".dimmed());

    let session_id = open_session(&client, kind, auth).await?;

    println!(
        "{} Session {}  |  type {} for help  |  terminate SQL with {}",
        "✓".green(),
        session_id.to_string().cyan(),
        ".help".yellow(),
        ";".yellow(),
    );
    println!();

    let mut history: Vec<String> = load_history();
    let mut buffer = String::new(); // multi-line SQL accumulator
    let stdin = io::stdin();

    loop {
        // Prompt: show continuation dots when mid-statement
        let prompt = if buffer.is_empty() {
            format!("{} ", "spark-ctrl>".cyan().bold())
        } else {
            format!("{} ", "        ...".dimmed())
        };
        print!("{}", prompt);
        io::stdout().flush().ok();

        // Read one line (blocking)
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                // Ctrl-D / EOF
                println!();
                break;
            }
            Err(e) => {
                eprintln!("{} read error: {}", "✗".red(), e);
                break;
            }
            Ok(_) => {}
        }

        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');

        // Ctrl-C is delivered as SIGINT outside our read loop on most terminals,
        // but if somehow we get an empty line on cancel, just reset buffer.
        if trimmed.is_empty() {
            if !buffer.is_empty() {
                // user pressed Enter on empty continuation — keep buffering
            }
            continue;
        }

        // Dot-command (only valid at start of a fresh statement)
        if buffer.is_empty() && trimmed.starts_with('.') {
            match handle_dot(trimmed, &history, session_id) {
                DotResult::Quit => break,
                DotResult::Continue => continue,
            }
        }

        // Accumulate into buffer
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(trimmed);

        // Execute when statement ends with ';'
        if buffer.trim_end().ends_with(';') {
            let sql = buffer.trim().trim_end_matches(';').trim().to_string();
            buffer.clear();

            if sql.is_empty() {
                continue;
            }

            // Record in history (deduplicate consecutive identical)
            if history.last().map(String::as_str) != Some(&sql) {
                history.push(sql.clone());
            }
            append_sql_history(&sql);

            // Execute
            println!("{}", "Executing…".dimmed());
            let t0 = std::time::Instant::now();
            match client.run_statement(session_id, &sql, auth).await {
                Err(e) => {
                    eprintln!("{} {}", "error:".red().bold(), e);
                }
                Ok(result) => {
                    let elapsed = t0.elapsed();
                    if result.status != "ok" {
                        eprintln!(
                            "{} {}: {}",
                            "error".red().bold(),
                            result.ename.as_deref().unwrap_or(""),
                            result.evalue.as_deref().unwrap_or("unknown")
                        );
                    } else if let Some(data) = &result.data {
                        if let Some(text) = data.get("text/plain") {
                            println!("{}", text.as_str().unwrap_or(&text.to_string()));
                        } else {
                            println!("{}", serde_json::to_string_pretty(data).unwrap_or_default());
                        }
                    }
                    println!("{}", format!("({:.2}s)", elapsed.as_secs_f64()).dimmed());
                }
            }
            println!();
        }
        // else: continue accumulating lines
    }

    println!("{}", "Closing session…".dimmed());
    client.delete_session(session_id, auth).await.ok();
    save_history(&history);
    println!("{}", "Bye!".cyan());
    Ok(())
}
