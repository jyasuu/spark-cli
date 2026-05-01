//! Terminal Gantt chart renderer for Spark stage timelines.
//!
//! Draws a horizontal bar chart where each row is one stage, scaled
//! to fit an 80-character terminal width.  Uses only colored + stdout.

use colored::Colorize;

pub struct GanttStage {
    pub stage_id:    u64,
    pub name:        String,
    pub start_ms:    i64,
    pub duration_ms: i64,
    pub status:      String,    // "COMPLETE" | "ACTIVE" | "FAILED" | "PENDING"
    pub num_tasks:   u32,
}

/// Render a list of stages as a coloured terminal Gantt chart.
///
/// `chart_width` is the number of character columns available for the bar
/// area (recommended: 50–60 for an 80-col terminal).
pub fn render(stages: &[GanttStage], chart_width: usize) {
    if stages.is_empty() {
        println!("{}", "(no stage data)".dimmed());
        return;
    }

    let t_min = stages.iter().map(|s| s.start_ms).min().unwrap_or(0);
    let t_max = stages
        .iter()
        .map(|s| s.start_ms + s.duration_ms.max(1))
        .max()
        .unwrap_or(1);
    let total_span = (t_max - t_min).max(1) as f64;

    // Column widths
    let id_w   = 5;   // "Stage"
    let stat_w = 9;   // "Status"
    let task_w = 6;   // "Tasks"
    let name_w = 22;
    let bar_w  = chart_width;

    // Header
    println!(
        "{:<id_w$} {:<name_w$} {:<stat_w$} {:<task_w$} {}",
        "Stage".bold(), "Name".bold(), "Status".bold(), "Tasks".bold(), "Timeline".bold(),
        id_w = id_w, name_w = name_w, stat_w = stat_w, task_w = task_w,
    );
    println!("{}", "─".repeat(id_w + 1 + name_w + 1 + stat_w + 1 + task_w + 1 + bar_w));

    for s in stages {
        let offset_frac = (s.start_ms - t_min) as f64 / total_span;
        let dur_frac    = s.duration_ms as f64 / total_span;

        let offset_cols = (offset_frac * bar_w as f64).round() as usize;
        let bar_cols    = ((dur_frac * bar_w as f64).round() as usize).max(1);
        let bar_cols    = bar_cols.min(bar_w.saturating_sub(offset_cols));

        let bar_char = match s.status.to_uppercase().as_str() {
            "COMPLETE"  => "█".green(),
            "ACTIVE"    => "▓".yellow(),
            "FAILED"    => "█".red(),
            _           => "░".dimmed(),
        };

        let bar: String = " ".repeat(offset_cols)
            + &bar_char.to_string().repeat(bar_cols);

        // Truncate name
        let name_str = if s.name.len() > name_w {
            format!("{}…", &s.name[..name_w - 1])
        } else {
            s.name.clone()
        };

        let status_colored = match s.status.to_uppercase().as_str() {
            "COMPLETE" => s.status.green().to_string(),
            "ACTIVE"   => s.status.yellow().to_string(),
            "FAILED"   => s.status.red().to_string(),
            _          => s.status.dimmed().to_string(),
        };

        println!(
            "{:<id_w$} {:<name_w$} {:<stat_w$} {:<task_w$} {}",
            s.stage_id, name_str, status_colored, s.num_tasks, bar,
            id_w = id_w, name_w = name_w, stat_w = stat_w, task_w = task_w,
        );
    }

    // Time axis
    let axis = format!(
        "{:<id_w$} {:<name_w$} {:<stat_w$} {:<task_w$} |{:─<half$}+{:─<half$}|",
        "", "", "", "",
        "", "",
        id_w = id_w, name_w = name_w, stat_w = stat_w, task_w = task_w,
        half = bar_w / 2 - 1,
    );
    println!("{}", axis.dimmed());

    let elapsed_s = total_span as f64 / 1000.0;
    println!(
        "{:<id_w$} {:<name_w$} {:<stat_w$} {:<task_w$} 0{:>mid$}{:.1}s",
        "", "", "", "",
        "", elapsed_s,
        id_w = id_w, name_w = name_w, stat_w = stat_w, task_w = task_w,
        mid = bar_w / 2 - 1,
    );
}

/// Build `GanttStage` entries from the Spark REST `/api/v1/applications/{id}/stages` payload.
pub fn stages_from_json(value: &serde_json::Value) -> Vec<GanttStage> {
    let arr = match value.as_array() {
        Some(a) => a,
        None    => return vec![],
    };

    let mut stages: Vec<GanttStage> = arr.iter().filter_map(|s| {
        let stage_id    = s["stageId"].as_u64()?;
        let status      = s["status"].as_str().unwrap_or("UNKNOWN").to_string();
        let num_tasks   = s["numTasks"].as_u64().unwrap_or(0) as u32;

        // Spark REST uses ISO-8601 strings for submission/completion times
        let start_ms = parse_spark_time(s["submissionTime"].as_str())
            .or_else(|| parse_spark_time(s["firstTaskLaunchedTime"].as_str()))
            .unwrap_or(0);

        let end_ms = parse_spark_time(s["completionTime"].as_str())
            .unwrap_or(start_ms + 1);

        let duration_ms = (end_ms - start_ms).max(1);

        let name = s["name"].as_str().unwrap_or("stage").to_string();

        Some(GanttStage { stage_id, name, start_ms, duration_ms, status, num_tasks })
    }).collect();

    stages.sort_by_key(|s| s.start_ms);
    stages
}

fn parse_spark_time(s: Option<&str>) -> Option<i64> {
    let s = s?;

    // Fast path: plain integer milliseconds (e.g. from older Spark versions)
    if let Ok(ms) = s.trim().parse::<i64>() {
        return Some(ms);
    }

    // Spark REST timestamps come in several flavours:
    //   "2024-11-01T12:00:00.000GMT"   — suffix is a named timezone
    //   "2024-11-01T12:00:00.000Z"     — RFC-3339 UTC
    //   "2024-11-01T12:00:00.000+00:00"— RFC-3339 with offset
    //
    // Strategy: strip any trailing alphabetic timezone name (e.g. "GMT", "UTC",
    // "EST") that chrono can't parse, then try RFC-3339.  Named zones are
    // treated as UTC for timeline purposes — wall-clock accuracy matters more
    // than timezone correctness in a Gantt chart.
    let clean: &str = {
        // Find the 'T' separator; everything after must contain only
        // RFC-3339-legal characters: digits, ':', '.', '+', '-', 'Z'.
        // Strip trailing alpha chars that aren't 'Z' or 'T'.
        let bytes = s.as_bytes();
        let mut end = bytes.len();
        while end > 0 {
            let ch = bytes[end - 1] as char;
            if ch.is_ascii_alphabetic() && ch != 'Z' {
                end -= 1;
            } else {
                break;
            }
        }
        &s[..end]
    };

    chrono::DateTime::parse_from_rfc3339(clean)
        .ok()
        .map(|dt| dt.timestamp_millis())
        .or_else(|| {
            // Last resort: try parsing without fractional seconds
            // e.g. "2024-11-01T12:00:00Z"
            chrono::DateTime::parse_from_rfc3339(&format!("{}Z", clean.trim_end_matches('Z')))
                .ok()
                .map(|dt| dt.timestamp_millis())
        })
}
