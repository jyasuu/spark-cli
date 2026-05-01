use anyhow::Result;
use serde::Serialize;
use tabled::{Table, Tabled};

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl std::str::FromStr for OutputFormat {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "table" => Ok(OutputFormat::Table),
            "json" => Ok(OutputFormat::Json),
            "csv" => Ok(OutputFormat::Csv),
            other => anyhow::bail!("unknown format '{}'; choose: table, json, csv", other),
        }
    }
}

/// RFC-4180 CSV field escaping: wrap in quotes if the value contains
/// a comma, double-quote, or newline; double any internal quotes.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Print a list of serializable rows in the requested format.
pub fn print_rows<T: Tabled + Serialize>(rows: &[T], fmt: OutputFormat) -> Result<()> {
    match fmt {
        OutputFormat::Table => {
            println!("{}", Table::new(rows));
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(rows)?);
        }
        OutputFormat::Csv => {
            if rows.is_empty() {
                return Ok(());
            }
            let arr = serde_json::to_value(rows)?;
            if let serde_json::Value::Array(items) = arr {
                if let Some(serde_json::Value::Object(first)) = items.first() {
                    let headers: Vec<_> = first.keys().cloned().collect();
                    println!(
                        "{}",
                        headers
                            .iter()
                            .map(|h| csv_escape(h))
                            .collect::<Vec<_>>()
                            .join(",")
                    );
                    for item in &items {
                        if let serde_json::Value::Object(map) = item {
                            let vals: Vec<String> = headers
                                .iter()
                                .map(|k| {
                                    let raw = match &map[k] {
                                        serde_json::Value::String(s) => s.clone(),
                                        serde_json::Value::Null => String::new(),
                                        v => v.to_string(),
                                    };
                                    csv_escape(&raw)
                                })
                                .collect();
                            println!("{}", vals.join(","));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Print a single key-value record as a two-column table.
pub fn print_kv(pairs: &[(&str, String)], fmt: OutputFormat) -> Result<()> {
    #[derive(Tabled, Serialize)]
    struct KvRow {
        key: String,
        value: String,
    }
    let rows: Vec<KvRow> = pairs
        .iter()
        .map(|(k, v)| KvRow {
            key: k.to_string(),
            value: v.clone(),
        })
        .collect();
    print_rows(&rows, fmt)
}
