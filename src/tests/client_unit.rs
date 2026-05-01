//! Unit tests for [`crate::client::LivyClient`] backed by [`MockLivy`].
//!
//! These tests always run — no docker, no external services, no env vars.
//! They verify the HTTP contract between spark-ctrl and the Livy REST API.

use crate::client::{BatchRequest, LivyClient};
use crate::config::Auth;
use crate::testing::MockLivy;
use std::collections::HashMap;

// ── helpers ──────────────────────────────────────────────────────────────────

fn no_auth() -> Auth {
    Auth::default()
}

fn batch_req(file: &str) -> BatchRequest {
    BatchRequest {
        file: file.to_string(),
        class_name: None,
        args: vec![],
        jars: vec![],
        py_files: vec![],
        files: vec![],
        driver_memory: None,
        driver_cores: None,
        executor_memory: None,
        executor_cores: None,
        num_executors: None,
        conf: HashMap::new(),
        name: Some("test-job".to_string()),
        queue: None,
    }
}

// ── health ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_returns_ok_when_batches_endpoint_responds() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let v = client.health(&no_auth()).await.unwrap();

    assert!(v["ok"].as_bool().unwrap_or(false), "health.ok should be true");
    assert_eq!(v["http_status"].as_u64().unwrap_or(0), 200);
}

// ── batch lifecycle ───────────────────────────────────────────────────────────

#[tokio::test]
async fn submit_batch_returns_id_and_running_state() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let batch = client
        .submit_batch(&batch_req("s3a://warehouse/jobs/curate.py"), &no_auth())
        .await
        .unwrap();

    assert_eq!(batch.id, 42);
    assert_eq!(batch.state, "running");
}

#[tokio::test]
async fn get_batch_returns_success_state() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let batch = client.get_batch(42, &no_auth()).await.unwrap();

    assert_eq!(batch.state, "success");
    assert_eq!(batch.app_id.as_deref(), Some("application_test_0042"));
}

#[tokio::test]
async fn list_batches_returns_at_least_one_entry() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let batches = client.list_batches(&no_auth()).await.unwrap();

    assert!(!batches.is_empty(), "list should not be empty");
    assert_eq!(batches[0].state, "success");
}

#[tokio::test]
async fn delete_batch_does_not_error() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    // MockLivy returns 200 {"msg":"deleted"} for DELETE /batches/{id}
    client.delete_batch(42, &no_auth()).await.unwrap();
}

#[tokio::test]
async fn get_batch_log_returns_three_lines() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let chunk = client.get_batch_log(42, 0, 100, &no_auth()).await.unwrap();

    assert_eq!(chunk.log.len(), 3);
    assert!(chunk.log[0].contains("SparkContext"));
}

// ── session lifecycle ─────────────────────────────────────────────────────────

#[tokio::test]
async fn create_session_returns_idle_state() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let session = client.create_session("sql", &no_auth()).await.unwrap();

    assert_eq!(session.state, "idle");
    assert_eq!(session.id, 1);
}

#[tokio::test]
async fn get_session_returns_idle_state() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let session = client.get_session(1, &no_auth()).await.unwrap();

    assert_eq!(session.state, "idle");
}

#[tokio::test]
async fn delete_session_does_not_error() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    client.delete_session(1, &no_auth()).await.unwrap();
}

// ── SQL statement execution ───────────────────────────────────────────────────

#[tokio::test]
async fn run_statement_returns_ok_status_with_text_data() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let result = client
        .run_statement(1, "SELECT 'hello from mock livy'", &no_auth())
        .await
        .unwrap();

    assert_eq!(result.status, "ok");
    let text = result
        .data
        .as_ref()
        .and_then(|d| d.get("text/plain"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(text.contains("hello from mock livy"));
}

#[tokio::test]
async fn run_statement_execution_count_is_zero() {
    let mock = MockLivy::start().await;
    let client = LivyClient::new(&mock.profile()).unwrap();

    let result = client
        .run_statement(1, "SHOW DATABASES", &no_auth())
        .await
        .unwrap();

    assert_eq!(result.execution_count, 0);
}

// ── config round-trip ─────────────────────────────────────────────────────────

#[test]
fn test_profile_has_correct_master_url() {
    let profile = crate::testing::test_profile("http://localhost:8998");
    assert_eq!(profile.master_url, "http://localhost:8998");
}

#[test]
fn test_profile_with_thrift_sets_thrift_url() {
    let profile = crate::testing::test_profile_with_thrift(
        "http://localhost:8998",
        "jdbc:hive2://localhost:10000/default",
    );
    assert_eq!(
        profile.thrift_url.as_deref(),
        Some("jdbc:hive2://localhost:10000/default")
    );
}

// ── output format ─────────────────────────────────────────────────────────────

#[test]
fn output_format_parses_all_variants() {
    use crate::output::OutputFormat;
    use std::str::FromStr;

    assert_eq!(OutputFormat::from_str("table").unwrap(), OutputFormat::Table);
    assert_eq!(OutputFormat::from_str("json").unwrap(),  OutputFormat::Json);
    assert_eq!(OutputFormat::from_str("csv").unwrap(),   OutputFormat::Csv);
    assert!(OutputFormat::from_str("xml").is_err());
}

// ── gantt parsing ─────────────────────────────────────────────────────────────

#[test]
fn stages_from_json_parses_spark_rest_response() {
    use crate::gantt::stages_from_json;

    // Minimal shape of a real Spark REST /stages response
    let json = serde_json::json!([
        {
            "stageId": 0,
            "status": "COMPLETE",
            "numTasks": 4,
            "submissionTime": "2026-01-01T00:00:00.000Z",
            "completionTime": "2026-01-01T00:00:01.200Z",
            "name": "parallelize at test.py:5"
        },
        {
            "stageId": 1,
            "status": "COMPLETE",
            "numTasks": 16,
            "submissionTime": "2026-01-01T00:00:01.100Z",
            "completionTime": "2026-01-01T00:00:04.500Z",
            "name": "map at test.py:12"
        }
    ]);

    let stages = stages_from_json(&json);

    assert_eq!(stages.len(), 2);
    assert_eq!(stages[0].stage_id, 0);
    assert_eq!(stages[0].status, "COMPLETE");
    assert_eq!(stages[0].num_tasks, 4);
    assert!(stages[0].duration_ms > 0, "duration must be positive");
    // Sorted by start_ms ascending
    assert!(stages[0].start_ms <= stages[1].start_ms);
}

#[test]
fn stages_from_json_returns_empty_for_non_array() {
    use crate::gantt::stages_from_json;

    let json = serde_json::json!({"error": "not an array"});
    let stages = stages_from_json(&json);
    assert!(stages.is_empty());
}

// ── auth helpers ─────────────────────────────────────────────────────────────

#[test]
fn auth_resolved_prefers_env_var_over_stored_token() {
    use crate::config::Auth;

    std::env::set_var("SPARK_CTRL_TOKEN", "env-token");
    let auth = Auth {
        method:      Some("bearer".to_string()),
        username:    None,
        token:       Some("stored-token".to_string()),
        keytab_path: None,
    };
    let resolved = auth.resolved();
    assert_eq!(resolved.token.as_deref(), Some("env-token"));
    std::env::remove_var("SPARK_CTRL_TOKEN");
}

#[test]
fn auth_resolved_falls_back_to_stored_token_when_env_absent() {
    use crate::config::Auth;

    std::env::remove_var("SPARK_CTRL_TOKEN");
    let auth = Auth {
        method:      Some("bearer".to_string()),
        username:    None,
        token:       Some("stored-token".to_string()),
        keytab_path: None,
    };
    let resolved = auth.resolved();
    assert_eq!(resolved.token.as_deref(), Some("stored-token"));
}
