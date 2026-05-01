use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Top-level config file structure (~/.config/spark-ctrl/config.toml)
/// NOTE: with toml 0.5, inline values must come before table values (HashMaps).
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    pub active_profile: Option<String>,
    // profiles is a TOML table-of-tables — must come last
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
    #[serde(skip)]
    config_path: PathBuf,
}

/// A named cluster profile.
/// Fields are ordered so plain values come before any table/HashMap fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub backend: Backend,
    pub master_url: String,
    pub thrift_url: Option<String>,
    pub namespace: Option<String>,
    pub description: Option<String>,
    // auth and spark_conf are TOML sub-tables — keep them last
    #[serde(default)]
    pub auth: Auth,
    #[serde(default)]
    pub spark_conf: HashMap<String, String>,
    /// Named submit-arg shortcuts: alias_name → list of "KEY=VALUE" strings
    #[serde(default)]
    pub aliases: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    #[default]
    Livy,
    Kubernetes,
    Yarn,
    Standalone,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Livy => write!(f, "livy"),
            Backend::Kubernetes => write!(f, "kubernetes"),
            Backend::Yarn => write!(f, "yarn"),
            Backend::Standalone => write!(f, "standalone"),
        }
    }
}

impl std::str::FromStr for Backend {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "livy" => Ok(Backend::Livy),
            "kubernetes" | "k8s" => Ok(Backend::Kubernetes),
            "yarn" => Ok(Backend::Yarn),
            "standalone" => Ok(Backend::Standalone),
            other => bail!(
                "unknown backend '{}'; choose: livy, kubernetes, yarn, standalone",
                other
            ),
        }
    }
}

/// Auth config — plain string fields only, no sub-tables.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Auth {
    pub method: Option<String>,
    pub username: Option<String>,
    /// Token / password. Prefer env var SPARK_CTRL_TOKEN at runtime.
    pub token: Option<String>,
    pub keytab_path: Option<String>,
}

impl Auth {
    /// Returns the effective token: env var `SPARK_CTRL_TOKEN` takes precedence
    /// over the value stored in config, so secrets never need to be written to disk.
    pub fn effective_token(&self) -> Option<String> {
        std::env::var("SPARK_CTRL_TOKEN")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| self.token.clone())
    }

    /// Return a copy of self with the token resolved to the effective value.
    pub fn resolved(&self) -> Self {
        Self {
            method: self.method.clone(),
            username: self.username.clone(),
            token: self.effective_token(),
            keytab_path: self.keytab_path.clone(),
        }
    }
}

// ─── Config impl ──────────────────────────────────────────────────────────────

impl Config {
    pub fn config_dir() -> Result<PathBuf> {
        let base = dirs::config_dir().context("cannot determine config directory")?;
        Ok(base.join("spark-ctrl"))
    }

    pub fn load() -> Result<Self> {
        let dir = Self::config_dir()?;
        let path = dir.join("config.toml");
        if !path.exists() {
            return Ok(Config {
                config_path: path,
                ..Default::default()
            });
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        cfg.config_path = path;
        Ok(cfg)
    }

    pub fn save(&self) -> Result<()> {
        let dir = self.config_path.parent().unwrap();
        std::fs::create_dir_all(dir)?;
        let raw = toml::to_string_pretty(self)?;
        std::fs::write(&self.config_path, raw)?;
        Ok(())
    }

    pub fn active_profile(&self) -> Result<(&str, &Profile)> {
        let name = self
            .active_profile
            .as_deref()
            .context("no active profile — run: spark-ctrl profile switch <name>")?;
        let profile = self
            .profiles
            .get(name)
            .with_context(|| format!("active profile '{}' not found in config", name))?;
        Ok((name, profile))
    }

    pub fn set_active_profile(&mut self, name: &str) -> Result<()> {
        if !self.profiles.contains_key(name) {
            bail!("profile '{}' does not exist", name);
        }
        self.active_profile = Some(name.to_string());
        Ok(())
    }

    pub fn add_profile(&mut self, name: String, profile: Profile) -> Result<()> {
        if self.profiles.contains_key(&name) {
            bail!(
                "profile '{}' already exists; use 'profile remove' then 'profile add' to replace",
                name
            );
        }
        if self.active_profile.is_none() {
            self.active_profile = Some(name.clone());
        }
        self.profiles.insert(name, profile);
        self.save()
    }

    pub fn remove_profile(&mut self, name: &str) -> Result<()> {
        if !self.profiles.contains_key(name) {
            bail!("profile '{}' not found", name);
        }
        self.profiles.remove(name);
        if self.active_profile.as_deref() == Some(name) {
            self.active_profile = self.profiles.keys().next().cloned();
        }
        self.save()
    }
}
