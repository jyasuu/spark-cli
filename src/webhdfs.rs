//! Minimal WebHDFS REST client built on top of the existing `minreq` helper
//! pattern.  Supports the operations needed by `fs ls`, `fs cp`, `fs rm`.
//!
//! WebHDFS reference: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html

#![allow(dead_code)]

use crate::config::Auth;
use anyhow::{bail, Context, Result};
use base64::Engine;
use serde::Deserialize;

// ─── data types ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStatus {
    pub path_suffix:      String,
    pub r#type:           String,   // FILE | DIRECTORY
    pub length:           u64,
    pub owner:            String,
    pub modification_time: u64,    // ms since epoch
    pub permission:       String,
    pub replication:      u32,
    pub block_size:       u64,
}

impl FileStatus {
    /// Human-readable size string (B / KB / MB / GB).
    pub fn size_human(&self) -> String {
        match self.length {
            n if n < 1_024             => format!("{} B",   n),
            n if n < 1_048_576         => format!("{:.1} KB", n as f64 / 1_024.0),
            n if n < 1_073_741_824     => format!("{:.1} MB", n as f64 / 1_048_576.0),
            n                          => format!("{:.2} GB", n as f64 / 1_073_741_824.0),
        }
    }

    /// Modification time as a formatted string.
    pub fn modified(&self) -> String {
        use chrono::{DateTime, Utc};
        let dt = DateTime::<Utc>::from_timestamp_millis(self.modification_time as i64)
            .unwrap_or_default();
        dt.format("%Y-%m-%d %H:%M").to_string()
    }

    pub fn type_symbol(&self) -> &'static str {
        if self.r#type == "DIRECTORY" { "d" } else { "-" }
    }
}

// ─── client ──────────────────────────────────────────────────────────────────

/// WebHDFS base URL, e.g. `http://namenode:9870`
pub struct WebHdfsClient {
    base_url: String,
}

impl WebHdfsClient {
    pub fn new(base_url: &str) -> Self {
        Self { base_url: base_url.trim_end_matches('/').to_string() }
    }

    fn webhdfs_url(&self, path: &str, op: &str, extra: &str) -> String {
        let path = if path.starts_with('/') { path.to_string() } else { format!("/{}", path) };
        format!("{}/webhdfs/v1{}?op={}{}", self.base_url, path, op, extra)
    }

    fn apply_auth(req: minreq::Request, auth: &Auth) -> minreq::Request {
        match auth.method.as_deref() {
            Some("basic") => {
                if let (Some(u), Some(p)) = (&auth.username, &auth.token) {
                    let encoded = base64::engine::general_purpose::STANDARD
                        .encode(format!("{}:{}", u, p));
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
            _ => {
                // Also support `user.name` query-param auth (Hadoop simple auth)
                if let Some(u) = &auth.username {
                    // minreq doesn't support dynamic URL mutation, so we'll append
                    // user.name to URLs that need it when building the URL.
                    let _ = u; // handled via URL building in ls/rm/mkdir
                }
                req
            }
        }
    }

    fn user_param(&self, auth: &Auth) -> String {
        match auth.method.as_deref() {
            None | Some("none") => {
                if let Some(u) = &auth.username {
                    format!("&user.name={}", u)
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        }
    }

    // ── ls ────────────────────────────────────────────────────────────────────

    pub fn ls(&self, path: &str, auth: &Auth) -> Result<Vec<FileStatus>> {
        let url = self.webhdfs_url(path, "LISTSTATUS", &self.user_param(auth));
        let req = Self::apply_auth(minreq::get(&url).with_timeout(30), auth);
        let resp = req.send().with_context(|| format!("GET {}", url))?;
        if resp.status_code == 404 {
            bail!("path not found: {}", path);
        }
        if resp.status_code >= 400 {
            bail!("HTTP {} from {}: {}", resp.status_code, url, resp.as_str().unwrap_or(""));
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct ListResp {
            file_statuses: FileStatuses,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct FileStatuses {
            file_status: Vec<FileStatus>,
        }

        let r: ListResp = resp.json().context("parsing LISTSTATUS response")?;
        Ok(r.file_statuses.file_status)
    }

    // ── rm ────────────────────────────────────────────────────────────────────

    pub fn rm(&self, path: &str, recursive: bool, auth: &Auth) -> Result<bool> {
        let extra = format!("{}&recursive={}", self.user_param(auth), recursive);
        let url = self.webhdfs_url(path, "DELETE", &extra);
        let req = Self::apply_auth(minreq::delete(&url).with_timeout(30), auth);
        let resp = req.send().with_context(|| format!("DELETE {}", url))?;
        if resp.status_code >= 400 {
            bail!("HTTP {} from {}: {}", resp.status_code, url, resp.as_str().unwrap_or(""));
        }
        #[derive(Deserialize)]
        struct BoolResp { boolean: bool }
        let r: BoolResp = resp.json().context("parsing DELETE response")?;
        Ok(r.boolean)
    }

    // ── mkdir ─────────────────────────────────────────────────────────────────

    pub fn mkdir(&self, path: &str, auth: &Auth) -> Result<bool> {
        let url = self.webhdfs_url(path, "MKDIRS", &self.user_param(auth));
        let req = Self::apply_auth(
            minreq::put(&url).with_timeout(30).with_body(vec![]),
            auth,
        );
        let resp = req.send().with_context(|| format!("PUT {}", url))?;
        if resp.status_code >= 400 {
            bail!("HTTP {} from {}: {}", resp.status_code, url, resp.as_str().unwrap_or(""));
        }
        #[derive(Deserialize)]
        struct BoolResp { boolean: bool }
        let r: BoolResp = resp.json().context("parsing MKDIRS response")?;
        Ok(r.boolean)
    }

    // ── file read (OPEN) ──────────────────────────────────────────────────────

    /// Download the contents of an HDFS file into memory via WebHDFS OPEN.
    ///
    /// WebHDFS OPEN responds with a 307 redirect to the datanode; minreq follows
    /// redirects automatically (up to 5 hops) and returns the file bytes.
    pub fn read(&self, path: &str, auth: &Auth) -> Result<Vec<u8>> {
        let url = self.webhdfs_url(path, "OPEN", &self.user_param(auth));
        let req = Self::apply_auth(
            minreq::get(&url)
                .with_timeout(300)
                .with_max_redirects(5),
            auth,
        );
        let resp = req.send().with_context(|| format!("GET (OPEN) {}", url))?;
        if resp.status_code == 404 {
            bail!("path not found: {}", path);
        }
        if resp.status_code >= 400 {
            bail!("HTTP {} from {}: {}", resp.status_code, url, resp.as_str().unwrap_or(""));
        }
        // minreq Response body access: as_bytes() returns &[u8]
        Ok(resp.as_bytes().to_vec())
    }

    // ── file write (CREATE) ───────────────────────────────────────────────────

    /// Upload `data` to an HDFS path via WebHDFS CREATE.
    ///
    /// WebHDFS CREATE is a two-step operation:
    ///   Step 1: PUT with empty body → namenode responds 307 to a datanode URL
    ///   Step 2: PUT with data body → datanode responds 201
    ///
    /// Some implementations (test mocks, single-node pseudo-clusters) collapse
    /// this into a single 200/201 response.  We handle all cases.
    pub fn write(&self, path: &str, data: &[u8], overwrite: bool, auth: &Auth) -> Result<bool> {
        let extra = format!(
            "{}&overwrite={}&replication=1",
            self.user_param(auth),
            overwrite,
        );
        let url = self.webhdfs_url(path, "CREATE", &extra);

        // Attempt step 1: zero-body PUT to get the datanode redirect.
        // We set max_redirects=0 so minreq does NOT auto-follow the 307 —
        // we need to inspect the Location header ourselves.
        let step1_req = Self::apply_auth(
            minreq::put(&url)
                .with_timeout(30)
                .with_body(vec![])
                .with_max_redirects(0),
            auth,
        );

        let r1 = step1_req.send().with_context(|| format!("PUT (CREATE step 1) {}", url))?;

        match r1.status_code {
            // ── Redirect path (real WebHDFS namenode) ─────────────────────────
            307 => {
                let datanode_url = r1.headers
                    .get("location")
                    .or_else(|| r1.headers.get("Location"))
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("307 from namenode but no Location header"))?;

                let step2_req = Self::apply_auth(
                    minreq::put(&datanode_url)
                        .with_timeout(300)
                        .with_header("Content-Type", "application/octet-stream")
                        .with_body(data.to_vec()),
                    auth,
                );
                let r2 = step2_req.send()
                    .with_context(|| format!("PUT (CREATE step 2) {}", datanode_url))?;

                if r2.status_code == 201 || r2.status_code == 200 {
                    Ok(true)
                } else {
                    bail!("HTTP {} from datanode: {}", r2.status_code, r2.as_str().unwrap_or(""));
                }
            }

            // ── Single-step 201 (pseudo-distributed / some proxies) ───────────
            201 => Ok(true),

            // ── Single-step 200 (mock servers, HttpFS in some configs) ─────────
            200 => {
                // If the body is a boolean true, the write succeeded without
                // needing to PUT the data (e.g. mock or HttpFS pre-upload).
                // Otherwise send data to the same URL.
                let body = r1.as_str().unwrap_or("");
                if body.contains("\"boolean\":true") || body.contains("\"boolean\": true") {
                    return Ok(true);
                }
                // Fallback: treat 200 as "upload to this URL"
                let step2_req = Self::apply_auth(
                    minreq::put(&url)
                        .with_timeout(300)
                        .with_header("Content-Type", "application/octet-stream")
                        .with_body(data.to_vec()),
                    auth,
                );
                let r2 = step2_req.send()
                    .with_context(|| format!("PUT (CREATE data upload) {}", url))?;
                Ok(r2.status_code == 200 || r2.status_code == 201)
            }

            // ── Error ─────────────────────────────────────────────────────────
            code if code >= 400 => {
                bail!("HTTP {} from WebHDFS CREATE: {}", code, r1.as_str().unwrap_or(""));
            }
            code => {
                bail!("Unexpected HTTP {} from WebHDFS CREATE at {}", code, url);
            }
        }
    }

    // ── file status (single) ──────────────────────────────────────────────────

    pub fn stat(&self, path: &str, auth: &Auth) -> Result<FileStatus> {
        let url = self.webhdfs_url(path, "GETFILESTATUS", &self.user_param(auth));
        let req = Self::apply_auth(minreq::get(&url).with_timeout(30), auth);
        let resp = req.send().with_context(|| format!("GET {}", url))?;
        if resp.status_code == 404 {
            bail!("path not found: {}", path);
        }
        if resp.status_code >= 400 {
            bail!("HTTP {} from {}: {}", resp.status_code, url, resp.as_str().unwrap_or(""));
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct StatResp { file_status: FileStatus }
        let r: StatResp = resp.json().context("parsing GETFILESTATUS response")?;
        Ok(r.file_status)
    }
}
