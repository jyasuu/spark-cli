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

    let body = dispatch(method, &segs, path_only);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn dispatch(method: &str, segs: &[&str], path: &str) -> String {
    match (method, segs) {
        ("GET",    ["batches"]) => json(serde_json::json!({
            "from":0,"total":1,"batches":[batch_info(0,"success")]
        })),
        ("POST",   ["batches"]) => json(batch_info(42, "running")),
        ("GET",    ["batches", _]) => json(batch_info(42, "success")),
        ("DELETE", ["batches", _]) => json(serde_json::json!({"msg":"deleted"})),
        ("GET",    ["batches", _, "log"]) => json(serde_json::json!({
            "id":42,"from":0,"total":3,"log":[
                "26/01/01 00:00:00 INFO SparkContext: Running Spark version 3.5.0",
                "26/01/01 00:00:01 INFO DAGScheduler: Job 0 finished",
                "26/01/01 00:00:02 INFO SparkContext: Successfully stopped SparkContext"
            ]
        })),
        ("POST",   ["sessions"]) => json(session_info(1, "idle")),
        ("GET",    ["sessions", _]) => json(session_info(1, "idle")),
        ("DELETE", ["sessions", _]) => json(serde_json::json!({"msg":"deleted"})),
        ("POST",   ["sessions", _, "statements"]) => json(statement_ok(0)),
        ("GET",    ["sessions", _, "statements", _]) => json(statement_ok(0)),
        _ => json(serde_json::json!({"error":"not found","path": path})),
    }
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

fn json(v: serde_json::Value) -> String { v.to_string() }
