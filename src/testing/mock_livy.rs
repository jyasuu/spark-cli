//! Minimal mock Livy HTTP server built on raw tokio TCP.
//! Zero extra dependencies beyond what is already in Cargo.toml.

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

pub struct MockLivy {
    pub addr: SocketAddr,
    _shutdown: oneshot::Sender<()>,
}

impl MockLivy {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock: bind failed");
        let addr = listener.local_addr().expect("mock: local_addr");
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move { serve(listener, rx).await; });
        MockLivy { addr, _shutdown: tx }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn profile(&self) -> crate::config::Profile {
        crate::testing::test_profile(&self.url())
    }
}

async fn serve(listener: TcpListener, mut shutdown: oneshot::Receiver<()>) {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => { tokio::spawn(handle_conn(stream)); }
                    Err(_) => break,
                }
            }
            _ = &mut shutdown => break,
        }
    }
}

async fn handle_conn(mut stream: TcpStream) {
    let mut buf = vec![0u8; 16384];
    let n = match stream.read(&mut buf).await {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let req = match std::str::from_utf8(&buf[..n]) { Ok(s) => s, Err(_) => return };
    let first_line = req.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.splitn(3, ' ').collect();
    if parts.len() < 2 { return; }
    let method = parts[0];
    let path_qs = parts[1];
    let path_only = path_qs.split('?').next().unwrap_or(path_qs);
    let segs: Vec<&str> = path_only.trim_matches('/').split('/').collect();

    let (body, content_type) = dispatch(method, &segs, path_qs);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        ct = content_type, len = body.len(), body = body,
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

/// Returns `(body, content_type)`.
fn dispatch(method: &str, segs: &[&str], path: &str) -> (String, &'static str) {
    match (method, segs) {
        // ── Livy batch API ────────────────────────────────────────────────────
        ("GET",    ["batches"]) => json(serde_json::json!({
            "from":0, "total":1, "batches":[batch_info(0,"success")]
        })),
        ("POST",   ["batches"]) => json(batch_info(42, "running")),
        ("GET",    ["batches", _]) => json(batch_info(42, "success")),
        ("DELETE", ["batches", _]) => json(serde_json::json!({"msg":"deleted"})),
        ("GET",    ["batches", _, "log"]) => json(serde_json::json!({
            "id":42, "from":0, "total":3, "log":[
                "26/01/01 00:00:00 INFO SparkContext: Running Spark version 3.5.0",
                "26/01/01 00:00:01 INFO DAGScheduler: Job 0 finished",
                "26/01/01 00:00:02 INFO SparkContext: Successfully stopped SparkContext"
            ]
        })),

        // ── Livy session / statement API ──────────────────────────────────────
        ("POST",   ["sessions"]) => json(session_info(1, "idle")),
        ("GET",    ["sessions", _]) => json(session_info(1, "idle")),
        ("DELETE", ["sessions", _]) => json(serde_json::json!({"msg":"deleted"})),
        ("POST",   ["sessions", _, "statements"]) => json(statement_ok(0)),
        ("GET",    ["sessions", _, "statements", _]) => json(statement_ok(0)),

        // ── Spark REST API — stages ───────────────────────────────────────────
        // /api/v1/applications/{app_id}/stages
        ("GET", ["api", "v1", "applications", _, "stages"]) => {
            json(mock_stages())
        }
        // /api/v1/applications/{app_id}/stages/{stage_id}
        ("GET", ["api", "v1", "applications", _, "stages", _]) => {
            json(serde_json::json!([mock_stage_detail()]))
        }

        // ── WebHDFS API ───────────────────────────────────────────────────────
        // Routes are matched on path query-string because WebHDFS encodes the
        // op= parameter there, not in the path segments.
        ("GET", _) if path.contains("op=LISTSTATUS") => json(serde_json::json!({
            "FileStatuses": {
                "FileStatus": [
                    {
                        "pathSuffix": "part-00000.snappy.parquet",
                        "type": "FILE",
                        "length": 44_040_192u64,
                        "owner": "spark",
                        "modificationTime": 1_735_689_600_000u64,
                        "permission": "644",
                        "replication": 3,
                        "blockSize": 134_217_728u64
                    },
                    {
                        "pathSuffix": "part-00001.snappy.parquet",
                        "type": "FILE",
                        "length": 43_302_912u64,
                        "owner": "spark",
                        "modificationTime": 1_735_689_600_000u64,
                        "permission": "644",
                        "replication": 3,
                        "blockSize": 134_217_728u64
                    }
                ]
            }
        })),
        ("GET", _) if path.contains("op=GETFILESTATUS") => json(serde_json::json!({
            "FileStatus": {
                "pathSuffix": "",
                "type": "FILE",
                "length": 1024u64,
                "owner": "spark",
                "modificationTime": 1_735_689_600_000u64,
                "permission": "644",
                "replication": 3,
                "blockSize": 134_217_728u64
            }
        })),
        // OPEN returns raw bytes — content-type text/plain so minreq reads body directly
        ("GET", _) if path.contains("op=OPEN") => {
            ("hello from mock webhdfs".to_string(), "text/plain")
        }
        // CREATE: mock returns 200 with boolean true (single-step, no redirect)
        ("PUT", _) if path.contains("op=CREATE") => json(serde_json::json!({"boolean": true})),
        // MKDIRS
        ("PUT", _) if path.contains("op=MKDIRS") => json(serde_json::json!({"boolean": true})),
        // DELETE
        ("DELETE", _) if path.contains("/webhdfs/") => json(serde_json::json!({"boolean": true})),

        _ => json(serde_json::json!({"error":"not found","path": path})),
    }
}

// ── Spark REST mock payloads ──────────────────────────────────────────────────

/// Four-stage application timeline, mirrors the demo data in gantt.rs.
fn mock_stages() -> serde_json::Value {
    serde_json::json!([
        {
            "stageId": 0, "status": "COMPLETE", "numTasks": 4,
            "submissionTime": "2026-01-01T00:00:00.000Z",
            "completionTime": "2026-01-01T00:00:01.200Z",
            "name": "parallelize at script.py:5"
        },
        {
            "stageId": 1, "status": "COMPLETE", "numTasks": 16,
            "submissionTime": "2026-01-01T00:00:01.100Z",
            "completionTime": "2026-01-01T00:00:04.500Z",
            "name": "map at script.py:12"
        },
        {
            "stageId": 2, "status": "ACTIVE", "numTasks": 32,
            "submissionTime": "2026-01-01T00:00:04.300Z",
            "completionTime": "2026-01-01T00:00:13.000Z",
            "name": "reduceByKey at script.py:18"
        },
        {
            "stageId": 3, "status": "PENDING", "numTasks": 8,
            "submissionTime": "2026-01-01T00:00:12.900Z",
            "completionTime": "2026-01-01T00:00:15.000Z",
            "name": "saveAsTextFile at script.py:22"
        }
    ])
}

/// Single stage with task-level metrics for skew detection tests.
/// Two tasks: 100 ms and 800 ms → max/median = 8.0× (above 3.0× threshold).
fn mock_stage_detail() -> serde_json::Value {
    serde_json::json!({
        "stageId": 2, "status": "COMPLETE", "numTasks": 2,
        "submissionTime": "2026-01-01T00:00:04.300Z",
        "completionTime": "2026-01-01T00:00:13.000Z",
        "name": "reduceByKey at script.py:18",
        "tasks": {
            "0": {
                "taskId": 0, "index": 0, "status": "SUCCESS",
                "taskMetrics": { "executorRunTime": 100.0 }
            },
            "1": {
                "taskId": 1, "index": 1, "status": "SUCCESS",
                "taskMetrics": { "executorRunTime": 800.0 }
            }
        }
    })
}

fn batch_info(id: u64, state: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id, "state": state,
        "appId": if state == "success" { serde_json::json!("application_test_0042") } else { serde_json::json!(null) },
        "appInfo": {}, "log": []
    })
}

fn session_info(id: u64, state: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "state": state, "kind": "sql"})
}

fn statement_ok(id: u64) -> serde_json::Value {
    serde_json::json!({
        "id": id, "state": "available",
        "output": {
            "status": "ok", "execution_count": 0,
            "data": {"text/plain": "res0: String = hello from mock livy"}
        }
    })
}

fn json(v: serde_json::Value) -> (String, &'static str) {
    (v.to_string(), "application/json")
}
