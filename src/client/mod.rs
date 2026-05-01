//! Livy REST API client built on `minreq` (run in `spawn_blocking` for async compat).

use crate::config::{Auth, Profile};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// ─── thin blocking wrapper ────────────────────────────────────────────────────

fn get_json<T: for<'de> Deserialize<'de>>(url: &str, auth: &Auth) -> Result<T> {
    let mut req = minreq::get(url).with_timeout(30);
    req = apply_auth(req, auth);
    let resp = req.send().with_context(|| format!("GET {}", url))?;
    if resp.status_code >= 400 {
        bail!(
            "HTTP {} from {}: {}",
            resp.status_code,
            url,
            resp.as_str().unwrap_or("")
        );
    }
    resp.json::<T>()
        .with_context(|| format!("parsing response from {}", url))
}

fn post_json<B: Serialize, T: for<'de> Deserialize<'de>>(
    url: &str,
    body: &B,
    auth: &Auth,
) -> Result<T> {
    let json_body = serde_json::to_vec(body)?;
    let mut req = minreq::post(url)
        .with_header("Content-Type", "application/json")
        .with_timeout(30)
        .with_body(json_body);
    req = apply_auth(req, auth);
    let resp = req.send().with_context(|| format!("POST {}", url))?;
    if resp.status_code >= 400 {
        bail!(
            "HTTP {} from {}: {}",
            resp.status_code,
            url,
            resp.as_str().unwrap_or("")
        );
    }
    resp.json::<T>()
        .with_context(|| format!("parsing response from {}", url))
}

fn delete(url: &str, auth: &Auth) -> Result<()> {
    let mut req = minreq::delete(url).with_timeout(30);
    req = apply_auth(req, auth);
    let resp = req.send().with_context(|| format!("DELETE {}", url))?;
    if resp.status_code >= 400 {
        bail!("HTTP {} from {}", resp.status_code, url);
    }
    Ok(())
}

fn apply_auth(req: minreq::Request, auth: &Auth) -> minreq::Request {
    match auth.method.as_deref() {
        Some("basic") => {
            if let (Some(u), Some(p)) = (&auth.username, &auth.token) {
                use base64::Engine;
                // minreq has no built-in basic auth helper; encode manually
                let encoded =
                    base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", u, p));
                return req.with_header("Authorization", format!("Basic {}", encoded));
            }
            req
        }
        Some("bearer") | Some("oauth") => {
            if let Some(tok) = &auth.token {
                return req.with_header("Authorization", format!("Bearer {}", tok));
            }
            req
        }
        _ => req,
    }
}

// ─── public async client ──────────────────────────────────────────────────────

pub struct LivyClient {
    base_url: String,
}

impl LivyClient {
    pub fn new(profile: &Profile) -> Result<Self> {
        Ok(Self {
            base_url: profile.master_url.trim_end_matches('/').to_string(),
        })
    }

    // ── batches ──────────────────────────────────────────────────────────────

    pub async fn submit_batch(&self, req: &BatchRequest, auth: &Auth) -> Result<BatchInfo> {
        let url = format!("{}/batches", self.base_url);
        let body = req.clone();
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || post_json(&url, &body, &auth)).await?
    }

    pub async fn get_batch(&self, id: u64, auth: &Auth) -> Result<BatchInfo> {
        let url = format!("{}/batches/{}", self.base_url, id);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || get_json(&url, &auth)).await?
    }

    pub async fn list_batches(&self, auth: &Auth) -> Result<Vec<BatchInfo>> {
        let url = format!("{}/batches", self.base_url);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<BatchInfo>> {
            #[derive(Deserialize)]
            struct ListResp {
                batches: Option<Vec<BatchInfo>>,
                sessions: Option<Vec<BatchInfo>>,
            }
            let r: ListResp = get_json(&url, &auth)?;
            Ok(r.batches.or(r.sessions).unwrap_or_default())
        })
        .await?
    }

    pub async fn delete_batch(&self, id: u64, auth: &Auth) -> Result<()> {
        let url = format!("{}/batches/{}", self.base_url, id);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || delete(&url, &auth)).await?
    }

    pub async fn get_batch_log(
        &self,
        id: u64,
        from: i64,
        size: usize,
        auth: &Auth,
    ) -> Result<LogChunk> {
        let url = format!(
            "{}/batches/{}/log?from={}&size={}",
            self.base_url, id, from, size
        );
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || get_json(&url, &auth)).await?
    }

    // ── sessions (SQL) ────────────────────────────────────────────────────────

    pub async fn create_session(&self, kind: &str, auth: &Auth) -> Result<SessionInfo> {
        let url = format!("{}/sessions", self.base_url);
        let conf = serde_json::json!({
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.demo.type": "rest",
            "spark.sql.catalog.demo.uri": "http://rest:8181",
            "spark.sql.catalog.demo.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.demo.warehouse": "s3a://warehouse/",
            "spark.sql.catalog.demo.s3.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.path.style.access": "true"
        });
        let body = serde_json::json!({ "kind": kind, "conf": conf });
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || post_json(&url, &body, &auth)).await?
    }

    pub async fn get_session(&self, id: u64, auth: &Auth) -> Result<SessionInfo> {
        let url = format!("{}/sessions/{}", self.base_url, id);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || get_json(&url, &auth)).await?
    }

    pub async fn run_statement(
        &self,
        session_id: u64,
        code: &str,
        auth: &Auth,
    ) -> Result<StatementResult> {
        // Submit
        let url = format!("{}/sessions/{}/statements", self.base_url, session_id);
        let body = serde_json::json!({ "code": code });
        let auth_c = auth.clone();
        let stmt: Statement =
            tokio::task::spawn_blocking(move || post_json(&url, &body, &auth_c)).await??;
        let stmt_id = stmt.id;

        // Poll until complete
        loop {
            tokio::time::sleep(Duration::from_millis(600)).await;
            let url = format!(
                "{}/sessions/{}/statements/{}",
                self.base_url, session_id, stmt_id
            );
            let auth_c = auth.clone();
            let s: Statement =
                tokio::task::spawn_blocking(move || get_json(&url, &auth_c)).await??;
            match s.state.as_str() {
                "available" | "error" | "cancelled" => {
                    return Ok(s.output.unwrap_or(StatementResult {
                        status: s.state,
                        execution_count: s.id,
                        data: None,
                        ename: None,
                        evalue: None,
                    }));
                }
                _ => {}
            }
        }
    }

    pub async fn delete_session(&self, id: u64, auth: &Auth) -> Result<()> {
        let url = format!("{}/sessions/{}", self.base_url, id);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || delete(&url, &auth)).await?
    }

    // ── health ────────────────────────────────────────────────────────────────

    pub async fn health(&self, auth: &Auth) -> Result<serde_json::Value> {
        let url = format!("{}/batches", self.base_url);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || -> Result<serde_json::Value> {
            let mut req = minreq::get(&url).with_timeout(10);
            req = apply_auth(req, &auth);
            match req.send() {
                Ok(r) => Ok(
                    serde_json::json!({ "http_status": r.status_code, "ok": r.status_code < 400 }),
                ),
                Err(e) => {
                    Ok(serde_json::json!({ "http_status": 0, "ok": false, "error": e.to_string() }))
                }
            }
        })
        .await?
    }

    // ── generic spark REST (for skew detection) ───────────────────────────────

    pub async fn spark_api_get(&self, path: &str, auth: &Auth) -> Result<serde_json::Value> {
        let url = format!("{}{}", self.base_url, path);
        let auth = auth.clone();
        tokio::task::spawn_blocking(move || get_json(&url, &auth)).await?
    }
}

// ─── data types ───────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BatchRequest {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub args: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub jars: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub py_files: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_cores: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor_cores: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_executors: Option<u32>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub conf: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchInfo {
    pub id: u64,
    #[serde(default)]
    pub state: String,
    #[serde(rename = "appId")]
    pub app_id: Option<String>,
    #[serde(rename = "appInfo")]
    pub app_info: Option<serde_json::Value>,
    pub log: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogChunk {
    pub id: u64,
    pub from: i64,
    pub total: i64,
    pub log: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: u64,
    pub state: String,
    pub kind: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Statement {
    pub id: u64,
    pub state: String,
    pub output: Option<StatementResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatementResult {
    pub status: String,
    pub execution_count: u64,
    pub data: Option<serde_json::Value>,
    pub ename: Option<String>,
    pub evalue: Option<String>,
}
