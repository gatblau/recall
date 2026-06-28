//! X6 — Configuration. Load, validate, and freeze all configuration at startup with precedence
//! **env var > config file > built-in default**, failing fast on a missing required value or a
//! failed validation (SA-EMBED-01 startup check). Secrets are read from env only and never logged.
//!
//! The §2D variable table is the authoritative key list. A `Config` carries one typed field per key.

use std::collections::HashMap;
use std::net::SocketAddr;

use thiserror::Error;

use crate::error::Env;

/// Startup configuration failures. These never reach an HTTP client — the process refuses to start.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// A required key was absent across env, file, and default.
    #[error("config.missing key={0}")]
    Missing(String),
    /// A value failed to parse to its typed field.
    #[error("config.parse key={0}")]
    Parse(String),
    /// A value failed an enum-domain, range, or conditional rule.
    #[error("config.invalid key={0}")]
    Invalid(String),
}

/// Work-queue backend (SA-QUEUE-01).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QueueBackend {
    Store,
    Nats,
}

/// Embedded storage backend.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreBackend {
    SurrealKv,
    Rocksdb,
}

/// Operational log level.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    /// The `tracing` directive string (lower-case crate level).
    pub fn as_directive(self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }
}

/// A secret string (e.g. a provider API key) that never reveals its value through `Debug` or
/// `Display`. This keeps secrets out of logs even if the whole `Config` is `{:?}`-formatted —
/// upholding the X3 / C4 invariant that credentials are never logged. New secret-bearing config
/// fields should use this type rather than a bare `String`.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(v: String) -> Self {
        Self(v)
    }
    /// Reveal the underlying secret. Call only where the value must be used (e.g. building an
    /// `Authorization` header), never in a log or error message.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Typed configuration — one field per §2D key. Held immutable behind an `Arc` after load.
#[derive(Clone, Debug)]
pub struct Config {
    // --- C8 HTTP ---
    pub http_addr: SocketAddr,
    // --- C10 MCP edge ---
    pub mcp_http_addr: SocketAddr,
    pub mcp_path: String,
    // --- C1 store ---
    pub store_path: String,
    pub store_remote_url: Option<String>,
    /// Root username for a secured remote store; signin fires only when both user and pass are set (FU-019).
    pub store_remote_user: Option<String>,
    /// Root password for a secured remote store (redacted in logs).
    pub store_remote_pass: Option<Secret>,
    pub store_backend: StoreBackend,
    // --- C3 auth ---
    pub oidc_issuer: String,
    pub oidc_audience: String,
    pub oidc_subject_claim: String,
    pub oidc_teams_claim: String,
    pub oidc_tenant_claim: String,
    pub jwks_refresh_secs: u32,
    // --- C4/C6 embedding ---
    pub embed_url: String,
    pub embed_api_key: Secret,
    pub embed_dim: u32,
    // --- C6 rerank ---
    pub rerank_url: String,
    pub rerank_api_key: Secret,
    // --- C2 queue ---
    pub queue_backend: QueueBackend,
    pub queue_nats_url: Option<String>,
    pub job_max_attempts: u32,
    pub job_backoff_base_ms: u32,
    pub queue_reaper_secs: u32,
    pub queue_poll_ms: u32,
    // --- C6 retrieval ---
    pub result_cap_max: u8,
    pub stage1_k: u16,
    pub abstain_threshold: f64,
    pub recency_weight: f64,
    pub recency_tau_days: f64,
    pub reformulation_enabled: bool,
    // --- C4 write gate / pii ---
    pub trust_admit: f64,
    pub trust_quarantine: f64,
    pub pii_redact_conf: f64,
    pub source_trust_default: f64,
    // --- C7 maintenance ---
    pub salience_floor: f64,
    pub decay_k: f64,
    pub prune_retrievability: f64,
    pub idle_quiet_secs: u32,
    pub maint_max_interval_secs: u32,
    pub maint_batch_size: u32,
    pub reinforce_gain: f64,
    // --- C8 edge ---
    pub idempotency_ttl_secs: u32,
    pub rate_read_per_min: u32,
    pub rate_write_per_min: u32,
    pub max_body_bytes: u32,
    // --- shared embedding model ---
    pub embed_model_version: String,
    // --- observability / env (all components) ---
    pub otlp_endpoint: Option<String>,
    pub log_level: LogLevel,
    pub env: Env,
}

/// A source of string values, resolved env > file > default.
struct Source {
    file: HashMap<String, String>,
}

impl Source {
    /// Resolve a key: env wins, then the file layer.
    fn get(&self, key: &str) -> Option<String> {
        if let Ok(v) = std::env::var(key) {
            if !v.is_empty() {
                return Some(v);
            }
        }
        self.file.get(key).filter(|v| !v.is_empty()).cloned()
    }

    fn get_or(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or_else(|| default.to_string())
    }

    fn required(&self, key: &str) -> Result<String, ConfigError> {
        self.get(key).ok_or_else(|| ConfigError::Missing(key.to_string()))
    }
}

fn parse_field<T: std::str::FromStr>(key: &str, raw: &str) -> Result<T, ConfigError> {
    raw.parse::<T>().map_err(|_| ConfigError::Parse(key.to_string()))
}

/// Parse an optional `KEY=value` config file (lines starting with `#` and blank lines ignored).
/// A missing file is not an error (the file layer is optional).
fn load_file(path: Option<&str>) -> HashMap<String, String> {
    let Some(path) = path else {
        return HashMap::new();
    };
    let Ok(contents) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    let mut map = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    map
}

fn in_unit_range(key: &str, v: f64) -> Result<f64, ConfigError> {
    if (0.0..=1.0).contains(&v) {
        Ok(v)
    } else {
        Err(ConfigError::Invalid(key.to_string()))
    }
}

impl Config {
    /// Load configuration once at startup. The optional config-file path comes from
    /// `RECALL_CONFIG_FILE` (env-only, lowest-precedence layer below env vars).
    pub fn load() -> Result<Config, ConfigError> {
        let file_path = std::env::var("RECALL_CONFIG_FILE").ok();
        let src = Source {
            file: load_file(file_path.as_deref()),
        };
        Self::from_source(&src)
    }

    /// Build a `Config` from a resolved source. Separated from [`Config::load`] so the parse +
    /// validation logic is unit-testable without touching the filesystem.
    fn from_source(src: &Source) -> Result<Config, ConfigError> {
        // --- required keys (X6 Shared Context) ---
        let oidc_issuer = src.required("RECALL_OIDC_ISSUER")?;
        let oidc_audience = src.required("RECALL_OIDC_AUDIENCE")?;
        let embed_url = src.required("RECALL_EMBED_URL")?;
        let embed_api_key = Secret::new(src.required("RECALL_EMBED_API_KEY")?);
        let rerank_url = src.required("RECALL_RERANK_URL")?;
        let rerank_api_key = Secret::new(src.required("RECALL_RERANK_API_KEY")?);

        // --- C8 HTTP ---
        let http_addr_raw = src.get_or("RECALL_HTTP_ADDR", "0.0.0.0:8080");
        let http_addr: SocketAddr = parse_field("RECALL_HTTP_ADDR", &http_addr_raw)?;

        // --- C10 MCP edge ---
        let mcp_http_addr_raw = src.get_or("RECALL_MCP_HTTP_ADDR", "0.0.0.0:8081");
        let mcp_http_addr: SocketAddr = parse_field("RECALL_MCP_HTTP_ADDR", &mcp_http_addr_raw)?;
        let mcp_path = src.get_or("RECALL_MCP_PATH", "/mcp");

        // --- C1 store ---
        let store_path = src.get_or("RECALL_STORE_PATH", "./data/recall.db");
        let store_remote_url = src.get("RECALL_STORE_REMOTE_URL");
        let store_remote_user = src.get("RECALL_STORE_REMOTE_USER");
        let store_remote_pass = src.get("RECALL_STORE_REMOTE_PASS").map(Secret::new);
        // Both-or-neither (FU-019): a half-set credential is a misconfiguration, never a silent
        // unauthenticated connect to a server the operator believes is secured.
        if store_remote_user.is_some() != store_remote_pass.is_some() {
            return Err(ConfigError::Invalid(
                "RECALL_STORE_REMOTE_USER/RECALL_STORE_REMOTE_PASS".into(),
            ));
        }
        let store_backend = match src.get_or("RECALL_STORE_BACKEND", "surrealkv").as_str() {
            "surrealkv" => StoreBackend::SurrealKv,
            "rocksdb" => StoreBackend::Rocksdb,
            _ => return Err(ConfigError::Invalid("RECALL_STORE_BACKEND".into())),
        };

        // --- C3 auth ---
        let oidc_subject_claim = src.get_or("RECALL_OIDC_SUBJECT_CLAIM", "sub");
        let oidc_teams_claim = src.get_or("RECALL_OIDC_TEAMS_CLAIM", "groups");
        let oidc_tenant_claim = src.get_or("RECALL_OIDC_TENANT_CLAIM", "tenant");
        let jwks_refresh_secs =
            parse_field("RECALL_JWKS_REFRESH_SECS", &src.get_or("RECALL_JWKS_REFRESH_SECS", "3600"))?;

        // --- C4/C6 embedding ---
        let embed_dim: u32 =
            parse_field("RECALL_EMBED_DIM", &src.get_or("RECALL_EMBED_DIM", "1024"))?;
        if embed_dim == 0 {
            return Err(ConfigError::Invalid("RECALL_EMBED_DIM".into()));
        }

        // --- C2 queue ---
        let queue_backend = match src.get_or("RECALL_QUEUE_BACKEND", "store").as_str() {
            "store" => QueueBackend::Store,
            "nats" => QueueBackend::Nats,
            _ => return Err(ConfigError::Invalid("RECALL_QUEUE_BACKEND".into())),
        };
        let queue_nats_url = src.get("RECALL_QUEUE_NATS_URL");
        // Conditional: NATS URL required iff backend is nats.
        if queue_backend == QueueBackend::Nats && queue_nats_url.is_none() {
            return Err(ConfigError::Missing("RECALL_QUEUE_NATS_URL".into()));
        }
        let job_max_attempts =
            parse_field("RECALL_JOB_MAX_ATTEMPTS", &src.get_or("RECALL_JOB_MAX_ATTEMPTS", "5"))?;
        let job_backoff_base_ms = parse_field(
            "RECALL_JOB_BACKOFF_BASE_MS",
            &src.get_or("RECALL_JOB_BACKOFF_BASE_MS", "2000"),
        )?;
        let queue_reaper_secs =
            parse_field("RECALL_QUEUE_REAPER_SECS", &src.get_or("RECALL_QUEUE_REAPER_SECS", "30"))?;
        let queue_poll_ms =
            parse_field("RECALL_QUEUE_POLL_MS", &src.get_or("RECALL_QUEUE_POLL_MS", "500"))?;

        // --- C6 retrieval ---
        let result_cap_max: u8 =
            parse_field("RECALL_RESULT_CAP_MAX", &src.get_or("RECALL_RESULT_CAP_MAX", "50"))?;
        if result_cap_max == 0 || result_cap_max > 50 {
            return Err(ConfigError::Invalid("RECALL_RESULT_CAP_MAX".into()));
        }
        let stage1_k = parse_field("RECALL_STAGE1_K", &src.get_or("RECALL_STAGE1_K", "50"))?;
        let abstain_threshold = in_unit_range(
            "RECALL_ABSTAIN_THRESHOLD",
            parse_field("RECALL_ABSTAIN_THRESHOLD", &src.get_or("RECALL_ABSTAIN_THRESHOLD", "0.2"))?,
        )?;
        let recency_weight = in_unit_range(
            "RECALL_RECENCY_WEIGHT",
            parse_field("RECALL_RECENCY_WEIGHT", &src.get_or("RECALL_RECENCY_WEIGHT", "0.15"))?,
        )?;
        let recency_tau_days: f64 =
            parse_field("RECALL_RECENCY_TAU_DAYS", &src.get_or("RECALL_RECENCY_TAU_DAYS", "30"))?;
        let reformulation_enabled = parse_field(
            "RECALL_REFORMULATION_ENABLED",
            &src.get_or("RECALL_REFORMULATION_ENABLED", "false"),
        )?;

        // --- C4 write gate / pii ---
        let trust_admit = in_unit_range(
            "RECALL_TRUST_ADMIT",
            parse_field("RECALL_TRUST_ADMIT", &src.get_or("RECALL_TRUST_ADMIT", "0.7"))?,
        )?;
        let trust_quarantine = in_unit_range(
            "RECALL_TRUST_QUARANTINE",
            parse_field("RECALL_TRUST_QUARANTINE", &src.get_or("RECALL_TRUST_QUARANTINE", "0.4"))?,
        )?;
        let pii_redact_conf = in_unit_range(
            "RECALL_PII_REDACT_CONF",
            parse_field("RECALL_PII_REDACT_CONF", &src.get_or("RECALL_PII_REDACT_CONF", "0.9"))?,
        )?;
        let source_trust_default = in_unit_range(
            "RECALL_SOURCE_TRUST_DEFAULT",
            parse_field(
                "RECALL_SOURCE_TRUST_DEFAULT",
                &src.get_or("RECALL_SOURCE_TRUST_DEFAULT", "0.5"),
            )?,
        )?;

        // --- C7 maintenance ---
        let salience_floor = in_unit_range(
            "RECALL_SALIENCE_FLOOR",
            parse_field("RECALL_SALIENCE_FLOOR", &src.get_or("RECALL_SALIENCE_FLOOR", "0.3"))?,
        )?;
        let decay_k: f64 = parse_field("RECALL_DECAY_K", &src.get_or("RECALL_DECAY_K", "10.0"))?;
        let prune_retrievability = in_unit_range(
            "RECALL_PRUNE_RETRIEVABILITY",
            parse_field(
                "RECALL_PRUNE_RETRIEVABILITY",
                &src.get_or("RECALL_PRUNE_RETRIEVABILITY", "0.05"),
            )?,
        )?;
        let idle_quiet_secs =
            parse_field("RECALL_IDLE_QUIET_SECS", &src.get_or("RECALL_IDLE_QUIET_SECS", "300"))?;
        let maint_max_interval_secs = parse_field(
            "RECALL_MAINT_MAX_INTERVAL_SECS",
            &src.get_or("RECALL_MAINT_MAX_INTERVAL_SECS", "21600"),
        )?;
        let maint_batch_size =
            parse_field("RECALL_MAINT_BATCH_SIZE", &src.get_or("RECALL_MAINT_BATCH_SIZE", "500"))?;
        let reinforce_gain: f64 =
            parse_field("RECALL_REINFORCE_GAIN", &src.get_or("RECALL_REINFORCE_GAIN", "0.5"))?;

        // --- C8 edge ---
        let idempotency_ttl_secs = parse_field(
            "RECALL_IDEMPOTENCY_TTL_SECS",
            &src.get_or("RECALL_IDEMPOTENCY_TTL_SECS", "86400"),
        )?;
        let rate_read_per_min =
            parse_field("RECALL_RATE_READ_PER_MIN", &src.get_or("RECALL_RATE_READ_PER_MIN", "120"))?;
        let rate_write_per_min = parse_field(
            "RECALL_RATE_WRITE_PER_MIN",
            &src.get_or("RECALL_RATE_WRITE_PER_MIN", "30"),
        )?;
        let max_body_bytes =
            parse_field("RECALL_MAX_BODY_BYTES", &src.get_or("RECALL_MAX_BODY_BYTES", "1048576"))?;

        // --- embedding model ---
        let embed_model_version =
            src.get_or("RECALL_EMBED_MODEL_VERSION", "default");

        // --- observability / env ---
        let otlp_endpoint = src.get("RECALL_OTLP_ENDPOINT");
        let log_level = match src.get_or("RECALL_LOG_LEVEL", "info").as_str() {
            "error" => LogLevel::Error,
            "warn" => LogLevel::Warn,
            "info" => LogLevel::Info,
            "debug" => LogLevel::Debug,
            "trace" => LogLevel::Trace,
            _ => return Err(ConfigError::Invalid("RECALL_LOG_LEVEL".into())),
        };
        let env = match src.get_or("RECALL_ENV", "production").as_str() {
            "production" => Env::Production,
            "development" => Env::Development,
            _ => return Err(ConfigError::Invalid("RECALL_ENV".into())),
        };

        Ok(Config {
            http_addr,
            mcp_http_addr,
            mcp_path,
            store_path,
            store_remote_url,
            store_remote_user,
            store_remote_pass,
            store_backend,
            oidc_issuer,
            oidc_audience,
            oidc_subject_claim,
            oidc_teams_claim,
            oidc_tenant_claim,
            jwks_refresh_secs,
            embed_url,
            embed_api_key,
            embed_dim,
            rerank_url,
            rerank_api_key,
            queue_backend,
            queue_nats_url,
            job_max_attempts,
            job_backoff_base_ms,
            queue_reaper_secs,
            queue_poll_ms,
            result_cap_max,
            stage1_k,
            abstain_threshold,
            recency_weight,
            recency_tau_days,
            reformulation_enabled,
            trust_admit,
            trust_quarantine,
            pii_redact_conf,
            source_trust_default,
            salience_floor,
            decay_k,
            prune_retrievability,
            idle_quiet_secs,
            maint_max_interval_secs,
            maint_batch_size,
            reinforce_gain,
            idempotency_ttl_secs,
            rate_read_per_min,
            rate_write_per_min,
            max_body_bytes,
            embed_model_version,
            otlp_endpoint,
            log_level,
            env,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn secret_redacts_in_debug_and_display() {
        let s = Secret::new("super-secret-api-key".to_string());
        assert!(!format!("{s:?}").contains("super-secret"), "Debug leaked the secret");
        assert!(!format!("{s}").contains("super-secret"), "Display leaked the secret");
        assert_eq!(s.expose(), "super-secret-api-key");
    }

    /// Serialises all config tests: `from_source` reads process-global env, so a test that mutates
    /// env must not run concurrently with one that reads it. Every test acquires this lock.
    static ENV_GUARD: Mutex<()> = Mutex::new(());

    /// Build a `Source` whose file layer carries a minimal valid configuration, so a test can then
    /// override individual keys via env to exercise precedence and validation. The file layer is the
    /// lowest precedence above defaults, which lets these tests avoid mutating process-wide env for
    /// the required keys.
    fn minimal_file() -> HashMap<String, String> {
        let mut m = HashMap::new();
        for (k, v) in [
            ("RECALL_OIDC_ISSUER", "https://issuer.example"),
            ("RECALL_OIDC_AUDIENCE", "recall"),
            ("RECALL_EMBED_URL", "https://embed.example"),
            ("RECALL_EMBED_API_KEY", "secret-embed"),
            ("RECALL_RERANK_URL", "https://rerank.example"),
            ("RECALL_RERANK_API_KEY", "secret-rerank"),
        ] {
            m.insert(k.to_string(), v.to_string());
        }
        m
    }

    #[test]
    fn defaults_apply_when_only_required_present() {
        let _g = ENV_GUARD.lock().unwrap();
        let cfg = Config::from_source(&Source { file: minimal_file() }).unwrap();
        assert_eq!(cfg.result_cap_max, 50);
        assert_eq!(cfg.stage1_k, 50);
        assert_eq!(cfg.embed_dim, 1024);
        assert_eq!(cfg.queue_backend, QueueBackend::Store);
        assert_eq!(cfg.log_level, LogLevel::Info);
        assert_eq!(cfg.env, Env::Production);
        assert!(cfg.otlp_endpoint.is_none());
    }

    #[test]
    fn file_value_overrides_default() {
        let _g = ENV_GUARD.lock().unwrap();
        // The file layer overrides the built-in default (env > file > default).
        let mut file = minimal_file();
        file.insert("RECALL_STAGE1_K".to_string(), "80".to_string());
        let cfg = Config::from_source(&Source { file }).unwrap();
        assert_eq!(cfg.stage1_k, 80);
        assert_eq!(cfg.result_cap_max, 50);
    }

    #[test]
    fn missing_required_key_fails() {
        let _g = ENV_GUARD.lock().unwrap();
        let mut file = minimal_file();
        file.remove("RECALL_EMBED_API_KEY");
        let err = Config::from_source(&Source { file }).unwrap_err();
        assert_eq!(err, ConfigError::Missing("RECALL_EMBED_API_KEY".to_string()));
    }

    #[test]
    fn nats_backend_requires_url() {
        let _g = ENV_GUARD.lock().unwrap();
        let mut file = minimal_file();
        file.insert("RECALL_QUEUE_BACKEND".to_string(), "nats".to_string());
        let err = Config::from_source(&Source { file }).unwrap_err();
        assert_eq!(err, ConfigError::Missing("RECALL_QUEUE_NATS_URL".to_string()));
    }

    #[test]
    fn nats_backend_with_url_succeeds() {
        let _g = ENV_GUARD.lock().unwrap();
        let mut file = minimal_file();
        file.insert("RECALL_QUEUE_BACKEND".to_string(), "nats".to_string());
        file.insert(
            "RECALL_QUEUE_NATS_URL".to_string(),
            "nats://localhost:4222".to_string(),
        );
        let cfg = Config::from_source(&Source { file }).unwrap();
        assert_eq!(cfg.queue_backend, QueueBackend::Nats);
        assert_eq!(cfg.queue_nats_url.as_deref(), Some("nats://localhost:4222"));
    }

    #[test]
    fn invalid_store_backend_rejected() {
        let _g = ENV_GUARD.lock().unwrap();
        let mut file = minimal_file();
        file.insert("RECALL_STORE_BACKEND".to_string(), "lmdb".to_string());
        let err = Config::from_source(&Source { file }).unwrap_err();
        assert_eq!(err, ConfigError::Invalid("RECALL_STORE_BACKEND".to_string()));
    }

    #[test]
    fn out_of_range_threshold_rejected() {
        let _g = ENV_GUARD.lock().unwrap();
        let mut file = minimal_file();
        file.insert("RECALL_ABSTAIN_THRESHOLD".to_string(), "1.5".to_string());
        let err = Config::from_source(&Source { file }).unwrap_err();
        assert_eq!(err, ConfigError::Invalid("RECALL_ABSTAIN_THRESHOLD".to_string()));
    }

    #[test]
    fn result_cap_max_over_50_rejected() {
        let _g = ENV_GUARD.lock().unwrap();
        let mut file = minimal_file();
        file.insert("RECALL_RESULT_CAP_MAX".to_string(), "99".to_string());
        let err = Config::from_source(&Source { file }).unwrap_err();
        assert_eq!(err, ConfigError::Invalid("RECALL_RESULT_CAP_MAX".to_string()));
    }

    #[test]
    fn env_overrides_file_layer() {
        let _g = ENV_GUARD.lock().unwrap();
        // Env var has higher precedence than the file layer. Set the env value, build a source whose
        // file layer disagrees, and assert the env value wins. The ENV_GUARD lock serialises this
        // process-global env mutation against every other config test that reads env.
        let key = "RECALL_STAGE1_K";
        std::env::set_var(key, "77");
        let mut file = minimal_file();
        file.insert(key.to_string(), "11".to_string());
        let cfg = Config::from_source(&Source { file }).unwrap();
        std::env::remove_var(key);
        assert_eq!(cfg.stage1_k, 77);
    }
}
