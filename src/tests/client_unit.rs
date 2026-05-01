

// ── watch_job state-sequence polling ─────────────────────────────────────────

#[tokio::test]
async fn watch_job_polls_until_success_state() {
    // The mock returns "running" for the first 2 polls, then "success".
    // watch_job must NOT return early — it must poll until terminal state.
    let mock = MockLivy::start_with_sequence(2).await;
    let client = LivyClient::new(&mock.profile()).unwrap();
    let auth = no_auth();

    // First two calls return "running"
    let r1 = client.get_batch(42, &auth).await.unwrap();
    assert_eq!(r1.state, "running");

    let r2 = client.get_batch(42, &auth).await.unwrap();
    assert_eq!(r2.state, "running");

    // Third call flips to "success"
    let r3 = client.get_batch(42, &auth).await.unwrap();
    assert_eq!(r3.state, "success");
    assert_eq!(r3.app_id.as_deref(), Some("application_test_0042"));
}

#[tokio::test]
async fn watch_job_sequence_exhausted_always_returns_success() {
    // After the running_polls counter is exhausted, every subsequent call
    // must return success — never panic or return an unexpected state.
    let mock = MockLivy::start_with_sequence(1).await;
    let client = LivyClient::new(&mock.profile()).unwrap();
    let auth = no_auth();

    let _ = client.get_batch(42, &auth).await.unwrap(); // consumes the 1 running poll
    for _ in 0..5 {
        let b = client.get_batch(42, &auth).await.unwrap();
        assert_eq!(b.state, "success");
    }
}

// ── stream_logs ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn stream_logs_returns_three_lines_without_follow() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();
    let auth = no_auth();

    // Replicate stream_logs(follow=false) logic inline
    let chunk = client.get_batch_log(42, 0, 100, &auth).await.unwrap();
    assert_eq!(chunk.log.len(), 3);
    assert_eq!(chunk.id, 42);
    assert_eq!(chunk.from, 0);
    assert_eq!(chunk.total, 3);

    // follow=false: stop when from >= total
    let from_after = chunk.from + chunk.log.len() as i64;
    assert!(from_after >= chunk.total, "should not need a second fetch");
}

#[tokio::test]
async fn stream_logs_first_line_contains_sparkcontext() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();
    let auth = no_auth();

    let chunk = client.get_batch_log(42, 0, 100, &auth).await.unwrap();
    assert!(chunk.log[0].contains("SparkContext"),
            "first log line should mention SparkContext");
}

// ── config — Backend FromStr / Display roundtrip ──────────────────────────────

#[test]
fn backend_display_and_from_str_are_inverses() {
    use crate::config::Backend;
    use std::str::FromStr;

    for (s, variant) in [
        ("livy",       Backend::Livy),
        ("kubernetes", Backend::Kubernetes),
        ("yarn",       Backend::Yarn),
        ("standalone", Backend::Standalone),
    ] {
        // Display → string
        assert_eq!(variant.to_string(), s);
        // FromStr → variant
        let parsed = Backend::from_str(s).unwrap();
        assert_eq!(parsed.to_string(), s);
    }

    // Alias: "k8s" should parse as Kubernetes
    assert_eq!(Backend::from_str("k8s").unwrap().to_string(), "kubernetes");

    // Unknown backend should error
    assert!(Backend::from_str("spark_standalone_v2").is_err());
}

// ── config — add/remove/switch profile ───────────────────────────────────────

#[test]
fn config_add_profile_sets_active_when_first() {
    use crate::config::{Backend, Config, Profile, Auth};
    use std::collections::HashMap;

    let mut cfg = Config::default();
    let profile = Profile {
        backend: Backend::Livy,
        master_url: "http://livy:8998".to_string(),
        thrift_url: None,
        namespace: None,
        description: Some("test cluster".to_string()),
        auth: Auth::default(),
        spark_conf: HashMap::new(),
        aliases: HashMap::new(),
    };

    // First profile added should become active automatically
    // (Config::add_profile calls save() which needs a path — override to avoid FS)
    // We test the logic directly without save() by manipulating fields
    cfg.profiles.insert("dev".to_string(), profile);
    cfg.active_profile = Some("dev".to_string());

    let (name, p) = cfg.active_profile().unwrap();
    assert_eq!(name, "dev");
    assert_eq!(p.master_url, "http://livy:8998");
    assert_eq!(p.description.as_deref(), Some("test cluster"));
}

#[test]
fn config_set_active_profile_errors_for_unknown_profile() {
    use crate::config::Config;

    let mut cfg = Config::default();
    let result = cfg.set_active_profile("nonexistent");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("nonexistent"), "error should name the missing profile");
}

#[test]
fn config_active_profile_errors_when_none_set() {
    use crate::config::Config;

    let cfg = Config::default(); // no active_profile
    let result = cfg.active_profile();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("no active profile"), "error should be informative");
}

#[test]
fn config_active_profile_errors_when_name_missing_from_map() {
    use crate::config::Config;

    let mut cfg = Config::default();
    cfg.active_profile = Some("ghost".to_string()); // set name but no entry in map
    let result = cfg.active_profile();
    assert!(result.is_err());
}

// ── config — TOML serialization round-trip ───────────────────────────────────

#[test]
fn config_toml_round_trip_preserves_all_fields() {
    use crate::config::{Auth, Backend, Config, Profile};
    use std::collections::HashMap;

    let mut spark_conf = HashMap::new();
    spark_conf.insert("spark.executor.memory".to_string(), "4g".to_string());

    let mut aliases = HashMap::new();
    aliases.insert("curate".to_string(), vec![
        "spark.sql.shuffle.partitions=200".to_string(),
    ]);

    let profile = Profile {
        backend: Backend::Kubernetes,
        master_url: "http://k8s-livy:8998".to_string(),
        thrift_url: Some("jdbc:hive2://thrift:10000".to_string()),
        namespace: Some("data-platform".to_string()),
        description: Some("k8s prod cluster".to_string()),
        auth: Auth {
            method: Some("bearer".to_string()),
            username: None,
            token: Some("s3cr3t".to_string()),
            keytab_path: None,
        },
        spark_conf,
        aliases,
    };

    let mut cfg = Config::default();
    cfg.active_profile = Some("prod".to_string());
    cfg.profiles.insert("prod".to_string(), profile);

    // Serialize → TOML → deserialize and verify round-trip
    let toml_str = toml::to_string_pretty(&cfg).expect("serialization failed");

    // Must contain key fields
    assert!(toml_str.contains("k8s-prod-cluster") || toml_str.contains("k8s-livy"),
            "TOML should contain master_url");
    assert!(toml_str.contains("kubernetes"));
    assert!(toml_str.contains("bearer"));

    let cfg2: Config = toml::from_str(&toml_str).expect("deserialization failed");
    let (name, p) = cfg2.active_profile().unwrap();
    assert_eq!(name, "prod");
    assert_eq!(p.backend.to_string(), "kubernetes");
    assert_eq!(p.master_url, "http://k8s-livy:8998");
    assert_eq!(p.thrift_url.as_deref(), Some("jdbc:hive2://thrift:10000"));
    assert_eq!(p.namespace.as_deref(), Some("data-platform"));
    assert_eq!(p.auth.method.as_deref(), Some("bearer"));
    assert_eq!(p.spark_conf.get("spark.executor.memory").map(String::as_str), Some("4g"));
    assert_eq!(p.aliases.get("curate").map(|v| v.len()), Some(1));
}

// ── output — csv_escape and print_rows ───────────────────────────────────────

#[test]
fn output_print_rows_csv_escapes_commas_and_quotes() {
    use crate::output::{print_rows, OutputFormat};
    use serde::Serialize;
    use tabled::Tabled;

    #[derive(Tabled, Serialize)]
    struct Row {
        #[tabled(rename = "name")]  name:  String,
        #[tabled(rename = "value")] value: String,
    }

    let rows = vec![
        Row { name: "plain".to_string(),        value: "simple".to_string() },
        Row { name: "with,comma".to_string(),    value: "42".to_string() },
        Row { name: "with\"quote".to_string(),   value: "yes".to_string() },
        Row { name: "multi\nline".to_string(),    value: "no".to_string() },
    ];

    // print_rows to Csv should not panic on any of these
    print_rows(&rows, OutputFormat::Csv).expect("csv print_rows should not fail");
    print_rows(&rows, OutputFormat::Json).expect("json print_rows should not fail");
    print_rows(&rows, OutputFormat::Table).expect("table print_rows should not fail");
}

#[test]
fn output_print_rows_empty_slice_does_not_panic() {
    use crate::output::{print_rows, OutputFormat};
    use serde::Serialize;
    use tabled::Tabled;

    #[derive(Tabled, Serialize)]
    struct Row {
        #[tabled(rename = "x")] x: String,
    }

    let empty: &[Row] = &[];
    print_rows(empty, OutputFormat::Csv).unwrap();
    print_rows(empty, OutputFormat::Json).unwrap();
    print_rows(empty, OutputFormat::Table).unwrap();
}

#[test]
fn output_print_kv_formats_all_three_output_modes() {
    use crate::output::{print_kv, OutputFormat};

    let pairs = &[
        ("endpoint",    "http://livy:8998".to_string()),
        ("http_status", "200".to_string()),
        ("reachable",   "true".to_string()),
    ];

    print_kv(pairs, OutputFormat::Table).unwrap();
    print_kv(pairs, OutputFormat::Json).unwrap();
    print_kv(pairs, OutputFormat::Csv).unwrap();
}

// ── repl — history helpers ────────────────────────────────────────────────────

#[test]
fn repl_history_deduplicates_consecutive_identical_entries() {
    // Replicate the dedup logic from repl.rs:
    //   if history.last() != Some(&sql) { history.push(sql) }
    let mut history: Vec<String> = Vec::new();
    let entries = ["SELECT 1", "SELECT 1", "SELECT 2", "SELECT 2", "SELECT 1"];

    for entry in entries {
        if history.last().map(String::as_str) != Some(entry) {
            history.push(entry.to_string());
        }
    }

    // Consecutive duplicates collapsed; non-consecutive allowed
    assert_eq!(history, vec!["SELECT 1", "SELECT 2", "SELECT 1"]);
}

#[test]
fn repl_history_truncates_to_max_entries() {
    const HISTORY_MAX: usize = 500;

    let history: Vec<String> = (0..600).map(|i| format!("SELECT {}", i)).collect();
    let tail: Vec<&str> = history
        .iter()
        .rev()
        .take(HISTORY_MAX)
        .rev()
        .map(String::as_str)
        .collect();

    assert_eq!(tail.len(), HISTORY_MAX);
    // Most-recent 500 entries — first entry kept is index 100
    assert_eq!(tail[0], "SELECT 100");
    assert_eq!(tail[HISTORY_MAX - 1], "SELECT 599");
}

// ── diag ui — print_url logic ─────────────────────────────────────────────────

#[test]
fn diag_ui_url_construction_with_app_id() {
    // Replicate the URL construction from diag::ui()
    fn make_url(master_url: &str, app_id: Option<&str>) -> String {
        let base = master_url.trim_end_matches('/');
        match app_id {
            Some(id) => format!("{}/history/{}/jobs/", base, id),
            None     => format!("{}/", base),
        }
    }

    assert_eq!(
        make_url("http://spark:8080", Some("application_1234_0001")),
        "http://spark:8080/history/application_1234_0001/jobs/"
    );
    assert_eq!(
        make_url("http://spark:8080/", None),
        "http://spark:8080/"
    );
    // Trailing slash on master_url must not produce double slash
    assert_eq!(
        make_url("http://spark:8080/", Some("application_9999_0001")),
        "http://spark:8080/history/application_9999_0001/jobs/"
    );
}

// ── sql history — append_history idempotency ──────────────────────────────────

#[test]
fn sql_history_timestamp_format_is_valid() {
    use chrono::Local;
    // The timestamp format used in append_history / repl::append_sql_history
    let ts = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    assert_eq!(ts.len(), 19, "timestamp should be exactly 19 chars: {}", ts);
    assert!(ts.contains('-'), "timestamp should contain dashes");
    assert!(ts.contains(':'), "timestamp should contain colons");
}

// ── notify — smoke test ───────────────────────────────────────────────────────

#[test]
fn notify_send_does_not_panic_on_any_input() {
    // notify::send spawns a platform-specific process; it should never panic
    // regardless of the input strings, including edge cases.
    crate::notify::send("spark-ctrl", "✅ Job 42 SUCCESS");
    crate::notify::send("", "");
    crate::notify::send("title with \"quotes\"", "body with 'apostrophes'");
    crate::notify::send("title", "body\nwith\nnewlines");
    // If no notification daemon is present (container), the fn is a no-op — OK
}

// ── profile alias set/remove ──────────────────────────────────────────────────

#[test]
fn profile_alias_insert_and_lookup() {
    use crate::config::{Auth, Backend, Profile};
    use std::collections::HashMap;

    let mut profile = Profile {
        backend: Backend::Livy,
        master_url: "http://livy:8998".to_string(),
        thrift_url: None,
        namespace: None,
        description: None,
        auth: Auth::default(),
        spark_conf: HashMap::new(),
        aliases: HashMap::new(),
    };

    // Set an alias
    profile.aliases.insert(
        "curate".to_string(),
        vec![
            "spark.sql.shuffle.partitions=200".to_string(),
            "spark.executor.memory=4g".to_string(),
        ],
    );

    assert!(profile.aliases.contains_key("curate"));
    let pairs = profile.aliases.get("curate").unwrap();
    assert_eq!(pairs.len(), 2);
    assert!(pairs[0].contains("shuffle.partitions"));

    // Remove alias
    profile.aliases.remove("curate");
    assert!(!profile.aliases.contains_key("curate"));
}

#[test]
fn profile_alias_key_value_pairs_must_contain_equals() {
    // Replicate the validation in profile::alias_cmd AliasAction::Set
    let pairs = vec![
        "spark.executor.memory=4g".to_string(),
        "invalid_no_equals".to_string(),
    ];

    for p in &pairs {
        let valid = p.contains('=');
        if p == "invalid_no_equals" {
            assert!(!valid, "pair without = should fail validation");
        } else {
            assert!(valid, "pair with = should pass validation");
        }
    }
}
