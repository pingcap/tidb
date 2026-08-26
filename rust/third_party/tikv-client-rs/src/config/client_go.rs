// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::io::{BufReader, Cursor};
use std::net::Ipv6Addr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use lazy_static::lazy_static;
use rustls::RootCertStore;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;
use url::form_urlencoded;

use super::{Config, TxnLocalLatches};
use crate::{RuDetails, SecurityManager};

pub use crate::proto::kvrpcpb::{
    ExecDetailsV2 as ProtoExecDetailsV2, ExecutorInputs as ProtoExecutorInputs, Ruv2 as ProtoRuv2,
};

pub const DEF_STORES_REFRESH_INTERVAL: u64 = 60;
pub const DEF_STORE_LIVENESS_TIMEOUT: &str = "1s";
pub const DEF_GRPC_INITIAL_WINDOW_SIZE: i32 = 1 << 27;
pub const DEF_GRPC_INITIAL_CONN_WINDOW_SIZE: i32 = 1 << 27;
pub const DEF_MAX_CONCURRENCY_REQUEST_LIMIT: i64 = i64::MAX;
pub const MAX_TXN_CHUNK_SIZE_IN_PARALLEL: u64 = 4 << 30;

pub const BATCH_POLICY_BASIC: &str = "basic";
pub const BATCH_POLICY_STANDARD: &str = "standard";
pub const BATCH_POLICY_POSITIVE: &str = "positive";
pub const BATCH_POLICY_CUSTOM: &str = "custom";
pub const DEF_BATCH_POLICY: &str = BATCH_POLICY_STANDARD;

#[cfg(feature = "nextgen")]
pub const NEXT_GEN: bool = true;
#[cfg(not(feature = "nextgen"))]
pub const NEXT_GEN: bool = false;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct ConfigError(pub String);

impl TxnLocalLatches {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.capacity == 0 {
            return Err(ConfigError(
                "txn-local-latches.capacity can not be 0".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PdClient {
    pub pd_server_timeout: u64,
}

impl Default for PdClient {
    fn default() -> Self {
        Self {
            pd_server_timeout: 3,
        }
    }
}

impl PdClient {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.pd_server_timeout == 0 {
            return Err(ConfigError("pd-server-timeout can not be 0".to_owned()));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PessimisticTxn {
    pub max_retry_count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Security {
    pub cluster_ssl_ca: String,
    pub cluster_ssl_cert: String,
    pub cluster_ssl_key: String,
    pub cluster_verify_cn: Vec<String>,
}

impl Security {
    pub fn new(
        ssl_ca: impl Into<String>,
        ssl_cert: impl Into<String>,
        ssl_key: impl Into<String>,
        verify_cn: Vec<String>,
    ) -> Self {
        Self {
            cluster_ssl_ca: ssl_ca.into(),
            cluster_ssl_cert: ssl_cert.into(),
            cluster_ssl_key: ssl_key.into(),
            cluster_verify_cn: verify_cn,
        }
    }

    /// Validate and load the configured TLS materials. Empty CA means TLS is disabled.
    pub fn to_tls_config(&self) -> Result<Option<SecurityManager>, ConfigError> {
        if self.cluster_ssl_ca.is_empty() {
            return Ok(None);
        }

        let ca = fs::read(&self.cluster_ssl_ca)
            .map_err(|error| ConfigError(format!("could not read ca certificate: {error}")))?;
        let ca_certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(&ca)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ConfigError("failed to append ca certs".to_owned()))?;
        let mut roots = RootCertStore::empty();
        let (valid, _) = roots.add_parsable_certificates(ca_certs);
        if valid == 0 {
            return Err(ConfigError("failed to append ca certs".to_owned()));
        }

        if !self.cluster_ssl_cert.is_empty() && !self.cluster_ssl_key.is_empty() {
            let cert = fs::read(&self.cluster_ssl_cert)
                .map_err(|error| ConfigError(format!("could not load client key pair: {error}")))?;
            let key = fs::read(&self.cluster_ssl_key)
                .map_err(|error| ConfigError(format!("could not load client key pair: {error}")))?;
            let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(cert)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| ConfigError(format!("could not load client key pair: {error}")))?;
            let key = rustls_pemfile::private_key(&mut BufReader::new(Cursor::new(key)))
                .map_err(|error| ConfigError(format!("could not load client key pair: {error}")))?
                .ok_or_else(|| {
                    ConfigError("could not load client key pair: no private key found".to_owned())
                })?;
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(certs, key)
                .map_err(|error| ConfigError(format!("could not load client key pair: {error}")))?;
        }

        let manager = if self.cluster_ssl_cert.is_empty() || self.cluster_ssl_key.is_empty() {
            SecurityManager::load_ca(&self.cluster_ssl_ca)
        } else {
            SecurityManager::load(
                &self.cluster_ssl_ca,
                &self.cluster_ssl_cert,
                &self.cluster_ssl_key,
            )
        };
        manager
            .map(Some)
            .map_err(|error| ConfigError(error.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Ruv2TiKvConfig {
    pub ru_scale: f64,
    pub tikv_kv_engine_cache_miss: f64,
    pub resource_manager_write_cnt_tikv: f64,
    pub executor_inputs: f64,
    pub tikv_coprocessor_executor_iterations: f64,
    pub tikv_coprocessor_response_bytes: f64,
    pub tikv_raftstore_store_write_trigger_wb: f64,
    pub tikv_storage_processed_keys_batch_get: f64,
    pub tikv_storage_processed_keys_get: f64,
}

impl Default for Ruv2TiKvConfig {
    fn default() -> Self {
        Self {
            ru_scale: 2.10,
            tikv_kv_engine_cache_miss: 0.45975389,
            resource_manager_write_cnt_tikv: 0.09642181,
            executor_inputs: 0.00003150,
            tikv_coprocessor_executor_iterations: 0.05775369,
            tikv_coprocessor_response_bytes: 0.00000087,
            tikv_raftstore_store_write_trigger_wb: 0.00006100,
            tikv_storage_processed_keys_batch_get: 0.00266791,
            tikv_storage_processed_keys_get: 0.01416829,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct AsyncCommit {
    pub keys_limit: u64,
    pub total_key_size_limit: u64,
    pub safe_window: Duration,
    pub allowed_clock_drift: Duration,
}

impl Default for AsyncCommit {
    fn default() -> Self {
        Self {
            keys_limit: 256,
            total_key_size_limit: 4 * 1024,
            safe_window: Duration::from_secs(2),
            allowed_clock_drift: Duration::from_millis(500),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CoprocessorCache {
    pub capacity_mb: f64,
    #[serde(skip)]
    pub admission_max_ranges: u64,
    #[serde(skip)]
    pub admission_max_result_mb: f64,
    #[serde(skip)]
    pub admission_min_process_ms: u64,
}

impl Default for CoprocessorCache {
    fn default() -> Self {
        Self {
            capacity_mb: 1000.0,
            admission_max_ranges: 500,
            admission_max_result_mb: 10.0,
            admission_min_process_ms: 5,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TiKvClient {
    pub grpc_connection_count: u64,
    pub grpc_keep_alive_time: u64,
    pub grpc_keep_alive_timeout: f64,
    pub grpc_compression_type: String,
    pub grpc_shared_buffer_pool: bool,
    pub grpc_initial_window_size: i32,
    pub grpc_initial_conn_window_size: i32,
    pub commit_timeout: String,
    pub async_commit: AsyncCommit,
    pub batch_policy: String,
    pub max_batch_size: u64,
    pub overload_threshold: u64,
    pub max_batch_wait_time: Duration,
    pub batch_wait_size: u64,
    pub enable_chunk_rpc: bool,
    pub region_cache_ttl: u64,
    pub store_limit: i64,
    pub store_liveness_timeout: String,
    pub copr_cache: CoprocessorCache,
    pub copr_req_timeout: Duration,
    pub ttl_refreshed_txn_size: i64,
    pub resolve_lock_lite_threshold: u64,
    pub max_concurrency_request_limit: i64,
    pub enable_replica_selector_v2: bool,
    pub ruv2: Ruv2TiKvConfig,
    pub txn_chunk_writer_addr: String,
    pub txn_chunk_writer_concurrency: u64,
    pub txn_chunk_max_size: u64,
    pub txn_file_min_mutation_size: u64,
    pub txn_file_ru_discount_ratio: f64,
    pub txn_file_request_source_whitelist: Vec<String>,
}

impl Default for TiKvClient {
    fn default() -> Self {
        Self {
            grpc_connection_count: 4,
            grpc_keep_alive_time: 10,
            grpc_keep_alive_timeout: 3.0,
            grpc_compression_type: "none".to_owned(),
            grpc_shared_buffer_pool: false,
            grpc_initial_window_size: DEF_GRPC_INITIAL_WINDOW_SIZE,
            grpc_initial_conn_window_size: DEF_GRPC_INITIAL_CONN_WINDOW_SIZE,
            commit_timeout: "41s".to_owned(),
            async_commit: AsyncCommit::default(),
            batch_policy: DEF_BATCH_POLICY.to_owned(),
            max_batch_size: 128,
            overload_threshold: 200,
            max_batch_wait_time: Duration::ZERO,
            batch_wait_size: 8,
            enable_chunk_rpc: true,
            region_cache_ttl: 600,
            store_limit: 0,
            store_liveness_timeout: DEF_STORE_LIVENESS_TIMEOUT.to_owned(),
            copr_cache: CoprocessorCache::default(),
            copr_req_timeout: Duration::from_secs(60),
            ttl_refreshed_txn_size: 32 * 1024 * 1024,
            resolve_lock_lite_threshold: 512,
            max_concurrency_request_limit: DEF_MAX_CONCURRENCY_REQUEST_LIMIT,
            enable_replica_selector_v2: true,
            ruv2: Ruv2TiKvConfig::default(),
            txn_chunk_writer_addr: String::new(),
            txn_chunk_writer_concurrency: 4,
            txn_chunk_max_size: 128 * 1024 * 1024,
            txn_file_min_mutation_size: 16 * 1024 * 1024,
            txn_file_ru_discount_ratio: 0.125,
            txn_file_request_source_whitelist: Vec::new(),
        }
    }
}

impl TiKvClient {
    pub fn grpc_keep_alive_timeout(&self) -> Duration {
        let nanos = self.grpc_keep_alive_timeout * 1_000_000_000.0;
        if nanos.is_nan() || nanos <= 0.0 {
            return Duration::ZERO;
        }
        if nanos >= i64::MAX as f64 {
            return Duration::from_nanos(i64::MAX as u64);
        }
        Duration::from_nanos(nanos as u64)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.grpc_connection_count == 0 {
            return Err(ConfigError(
                "grpc-connection-count should be greater than 0".to_owned(),
            ));
        }
        if self.grpc_compression_type != "none" && self.grpc_compression_type != "gzip" {
            return Err(ConfigError(format!(
                "grpc-compression-type should be none or gzip, but got {}",
                self.grpc_compression_type
            )));
        }
        if self.grpc_keep_alive_timeout.is_nan() || self.grpc_keep_alive_timeout < 0.05 {
            return Err(ConfigError(format!(
                "grpc-keepalive-timeout should be at least 0.05, but got {:.6}",
                self.grpc_keep_alive_timeout
            )));
        }
        self.validate_txn_file()
    }

    fn validate_txn_file(&self) -> Result<(), ConfigError> {
        if self.txn_chunk_writer_addr.is_empty() {
            return Ok(());
        }
        if self.txn_chunk_max_size == 0 {
            return Err(ConfigError(
                "txn-chunk-max-size should be greater than 0".to_owned(),
            ));
        }
        if self.txn_chunk_max_size > isize::MAX as u64 {
            return Err(ConfigError(format!(
                "txn-chunk-max-size should not exceed {}, but got {}",
                isize::MAX,
                self.txn_chunk_max_size
            )));
        }
        if self.txn_chunk_max_size > MAX_TXN_CHUNK_SIZE_IN_PARALLEL {
            return Err(ConfigError(format!(
                "txn-chunk-max-size should not exceed {}, but got {}",
                MAX_TXN_CHUNK_SIZE_IN_PARALLEL, self.txn_chunk_max_size
            )));
        }
        if self.txn_chunk_writer_concurrency == 0 {
            return Err(ConfigError(
                "txn-chunk-writer-concurrency should be greater than 0".to_owned(),
            ));
        }
        if self.txn_chunk_writer_concurrency > isize::MAX as u64 {
            return Err(ConfigError(format!(
                "txn-chunk-writer-concurrency should not exceed {}, but got {}",
                isize::MAX,
                self.txn_chunk_writer_concurrency
            )));
        }
        Ok(())
    }
}

impl Config {
    pub(crate) fn security_manager(&self) -> Result<SecurityManager, ConfigError> {
        if !self.security.cluster_ssl_ca.is_empty() {
            return self
                .security
                .to_tls_config()
                .map(|manager| manager.unwrap_or_default());
        }
        match (&self.ca_path, &self.cert_path, &self.key_path) {
            (Some(ca), Some(cert), Some(key)) => {
                SecurityManager::load(ca, cert, key).map_err(|error| ConfigError(error.to_string()))
            }
            _ => Ok(SecurityManager::default()),
        }
    }
}

lazy_static! {
    static ref GLOBAL_CONFIG: RwLock<Arc<Config>> = RwLock::new(Arc::new(Config::default()));
}

pub fn get_global_config() -> Arc<Config> {
    GLOBAL_CONFIG.read().unwrap().clone()
}

pub fn store_global_config(config: impl Into<Arc<Config>>) {
    *GLOBAL_CONFIG.write().unwrap() = config.into();
}

pub fn update_global(update: impl FnOnce(&mut Config)) -> impl Fn() + Send + Sync + 'static {
    let previous = get_global_config();
    let mut next = (*previous).clone();
    update(&mut next);
    store_global_config(next);
    move || store_global_config(previous.clone())
}

pub fn get_txn_scope_from_config() -> String {
    let mut scope = get_global_config().txn_scope.clone();
    if let Some(injected) = fail::eval("injectTxnScope", |value| value.unwrap_or_default()) {
        scope = injected;
    }
    if scope.is_empty() {
        crate::oracle::GLOBAL_TXN_SCOPE.to_owned()
    } else {
        scope
    }
}

pub fn parse_path(path: &str) -> Result<(Vec<String>, bool, String), ConfigError> {
    if path
        .as_bytes()
        .iter()
        .any(|byte| *byte < b' ' || *byte == 0x7f)
    {
        return Err(ConfigError(
            "net/url: invalid control character in URL".to_owned(),
        ));
    }
    let (scheme, rest) = split_url_scheme(path)?.unwrap_or(("", path));
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "tikv" {
        return Err(ConfigError(format!(
            "Uri scheme expected [tikv] but found [{scheme}]"
        )));
    }
    let (without_fragment, fragment) = rest.split_once('#').unwrap_or((rest, ""));
    let (authority_and_path, query) = without_fragment
        .split_once('?')
        .unwrap_or((without_fragment, ""));
    let opaque = !authority_and_path.starts_with('/');
    if (!opaque && has_invalid_percent_escape(authority_and_path.as_bytes()))
        || has_invalid_percent_escape(fragment.as_bytes())
    {
        return Err(ConfigError("invalid URL escape".to_owned()));
    }
    let authority = match authority_and_path.strip_prefix("//") {
        Some(hierarchical) => {
            parse_url_authority(hierarchical.split('/').next().unwrap_or_default())?
        }
        None => String::new(),
    };
    let mut disable_gc = false;
    let mut keyspace_name = String::new();
    let mut saw_disable_gc = false;
    let mut saw_keyspace_name = false;
    for pair in query.split('&') {
        // net/url.URL.Query silently drops malformed value pairs. In
        // particular, a raw semicolon or a malformed percent escape does not
        // invalidate the whole URL or any well-formed sibling pair.
        if pair.is_empty() || pair.contains(';') || has_invalid_percent_escape(pair.as_bytes()) {
            continue;
        }
        let Some((key, value)) = form_urlencoded::parse(pair.as_bytes()).next() else {
            continue;
        };
        match key.as_ref() {
            "keyspaceName" if !saw_keyspace_name => {
                saw_keyspace_name = true;
                keyspace_name = value.into_owned();
            }
            "disableGC" if !saw_disable_gc => {
                saw_disable_gc = true;
                match value.to_ascii_lowercase().as_str() {
                    "true" => disable_gc = true,
                    "false" | "" => disable_gc = false,
                    _ => {
                        return Err(ConfigError(
                            "disableGC flag should be true/false".to_owned(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok((
        authority.split(',').map(str::to_owned).collect(),
        disable_gc,
        keyspace_name,
    ))
}

fn split_url_scheme(path: &str) -> Result<Option<(&str, &str)>, ConfigError> {
    for (index, byte) in path.bytes().enumerate() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' if index != 0 => {}
            b':' if index == 0 => {
                return Err(ConfigError("missing protocol scheme".to_owned()));
            }
            b':' => return Ok(Some((&path[..index], &path[index + 1..]))),
            _ => return Ok(None),
        }
    }
    Ok(None)
}

fn has_invalid_percent_escape(value: &[u8]) -> bool {
    value.iter().enumerate().any(|(index, byte)| {
        *byte == b'%'
            && (index + 2 >= value.len()
                || !value[index + 1].is_ascii_hexdigit()
                || !value[index + 2].is_ascii_hexdigit())
    })
}

fn parse_url_authority(authority: &str) -> Result<String, ConfigError> {
    let (userinfo, host) = authority
        .rsplit_once('@')
        .map_or((None, authority), |(userinfo, host)| (Some(userinfo), host));
    let host = parse_url_host(host)?;
    if let Some(userinfo) = userinfo {
        if !valid_url_userinfo(userinfo) || has_invalid_percent_escape(userinfo.as_bytes()) {
            return Err(ConfigError("net/url: invalid userinfo".to_owned()));
        }
    }
    Ok(host)
}

fn valid_url_userinfo(userinfo: &str) -> bool {
    userinfo.bytes().all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'.'
                    | b'_'
                    | b':'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b'%'
                    | b'@'
            )
    })
}

fn parse_url_host(host: &str) -> Result<String, ConfigError> {
    match host.rfind('[') {
        Some(index) if index > 0 => return Err(ConfigError("invalid IP-literal".to_owned())),
        Some(_) => {
            let Some(close) = host.rfind(']') else {
                return Err(ConfigError("missing ']' in host".to_owned()));
            };
            let port = &host[close + 1..];
            if !valid_optional_url_port(port) {
                return Err(ConfigError(format!("invalid port {port:?} after host")));
            }
            let hostname = &host[1..close];
            let decoded_hostname = match hostname.find("%25") {
                Some(zone) => {
                    let mut decoded = decode_url_host_component(&hostname[..zone], false)?;
                    decoded.push_str(&decode_url_host_component(&hostname[zone..], true)?);
                    decoded
                }
                None => decode_url_host_component(hostname, false)?,
            };
            let (address, zone) = decoded_hostname
                .split_once('%')
                .map_or((decoded_hostname.as_str(), None), |(address, zone)| {
                    (address, Some(zone))
                });
            if zone.is_some_and(str::is_empty) {
                return Err(ConfigError("invalid host: empty IPv6 zone".to_owned()));
            }
            if address.parse::<Ipv6Addr>().is_err() {
                return Err(ConfigError("invalid host".to_owned()));
            }
            return Ok(format!("[{decoded_hostname}]{port}"));
        }
        None => {}
    }

    if let Some((_, port)) = host.rsplit_once(':') {
        let port = &host[host.len() - port.len() - 1..];
        if !valid_optional_url_port(port) {
            return Err(ConfigError(format!("invalid port {port:?} after host")));
        }
    }
    decode_url_host_component(host, false)
}

fn valid_optional_url_port(port: &str) -> bool {
    port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|port| port.bytes().all(|byte| byte.is_ascii_digit()))
}

fn decode_url_host_component(value: &str, zone: bool) -> Result<String, ConfigError> {
    if has_invalid_percent_escape(value.as_bytes()) {
        return Err(ConfigError("invalid URL escape".to_owned()));
    }
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = char::from(bytes[index + 1]).to_digit(16).unwrap() as u8;
            let low = char::from(bytes[index + 2]).to_digit(16).unwrap() as u8;
            let byte = (high << 4) | low;
            let percent = &value[index..index + 3];
            if (!zone && byte.is_ascii() && percent != "%25")
                || (zone && percent != "%25" && byte != b' ' && !valid_raw_url_host_byte(byte))
            {
                return Err(ConfigError(format!("invalid URL escape {percent:?}")));
            }
            decoded.push(byte);
            index += 3;
        } else {
            if bytes[index].is_ascii() && !valid_raw_url_host_byte(bytes[index]) {
                return Err(ConfigError(format!(
                    "invalid character {:?} in host name",
                    char::from(bytes[index])
                )));
            }
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

fn valid_raw_url_host_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'"'
                | b'-'
                | b'_'
                | b'.'
                | b'~'
        )
}

pub fn update_tikv_ru_v2_from_exec_details_v2(
    details: Option<&mut ProtoExecDetailsV2>,
    read_rpc_count: i64,
    write_rpc_count: i64,
    ru_details: Option<&RuDetails>,
) {
    let (Some(details), Some(ru_details)) = (details, ru_details) else {
        return;
    };
    let Some(ru) = details.ru_v2.as_mut() else {
        return;
    };
    let weights = get_global_config().tikv_client.ruv2;
    if read_rpc_count != 0 {
        ru.read_rpc_count = ru.read_rpc_count.wrapping_add(read_rpc_count as u64);
    }
    if write_rpc_count != 0 {
        ru.write_rpc_count = ru.write_rpc_count.wrapping_add(write_rpc_count as u64);
    }
    ru_details.add_ru_v2(Some(ru));

    let executor_inputs = ru.executor_inputs.as_ref().map_or(0, |inputs| {
        [
            inputs.tikv_coprocessor_executor_work_total_batch_index_scan,
            inputs.tikv_coprocessor_executor_work_total_batch_table_scan,
            inputs.tikv_coprocessor_executor_work_total_batch_selection,
            inputs.tikv_coprocessor_executor_work_total_batch_top_n,
            inputs.tikv_coprocessor_executor_work_total_batch_limit,
            inputs.tikv_coprocessor_executor_work_total_batch_simple_aggr,
            inputs.tikv_coprocessor_executor_work_total_batch_fast_hash_aggr,
        ]
        .into_iter()
        .fold(0_u64, u64::wrapping_add)
    });
    let delta = ru.kv_engine_cache_miss as f64 * weights.tikv_kv_engine_cache_miss
        + executor_inputs as f64 * weights.executor_inputs
        + ru.coprocessor_executor_iterations as f64 * weights.tikv_coprocessor_executor_iterations
        + ru.coprocessor_response_bytes as f64 * weights.tikv_coprocessor_response_bytes
        + ru.raftstore_store_write_trigger_wb_bytes as f64
            * weights.tikv_raftstore_store_write_trigger_wb
        + ru.storage_processed_keys_batch_get as f64
            * weights.tikv_storage_processed_keys_batch_get
        + ru.storage_processed_keys_get as f64 * weights.tikv_storage_processed_keys_get
        + ru.write_rpc_count as f64 * weights.resource_manager_write_cnt_tikv;
    ru_details.add_tikv_ru_v2(delta * weights.ru_scale);
}

#[cfg(test)]
mod tests {
    use std::fs;

    use openssl::asn1::Asn1Time;
    use openssl::bn::BigNum;
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::rsa::Rsa;
    use openssl::x509::{X509NameBuilder, X509};
    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;
    use crate::proto::kvrpcpb::{ExecDetailsV2, ExecutorInputs, Ruv2};

    #[test]
    fn source_defaults_and_build_variant() {
        let config = Config::default();
        assert_eq!(config.committer_concurrency, 128);
        assert_eq!(config.max_txn_ttl, 3_600_000);
        assert_eq!(config.stores_refresh_interval, 60);
        assert!(!config.open_tracing_enable);
        assert!(config.path.is_empty());
        assert!(!config.enable_forwarding);
        assert!(config.txn_scope.is_empty());
        assert!(!config.enable_async_commit);
        assert!(!config.enable_1pc);
        assert_eq!(config.regions_refresh_interval, 0);
        assert!(!config.enable_preload);
        assert!(!config.enable_async_batch_get);
        assert!(config.zone_label.is_empty());
        assert_eq!(config.pd_client, PdClient::default());
        assert_eq!(config.pessimistic_txn, PessimisticTxn::default());
        assert_eq!(config.security, Security::default());

        let client = config.tikv_client;
        assert_eq!(client.grpc_connection_count, 4);
        assert_eq!(client.grpc_keep_alive_time, 10);
        assert_eq!(client.grpc_keep_alive_timeout(), Duration::from_secs(3));
        assert_eq!(client.grpc_compression_type, "none");
        assert!(!client.grpc_shared_buffer_pool);
        assert_eq!(client.grpc_initial_window_size, 1 << 27);
        assert_eq!(client.grpc_initial_conn_window_size, 1 << 27);
        assert_eq!(client.commit_timeout, "41s");
        assert_eq!(client.async_commit, AsyncCommit::default());
        assert_eq!(client.batch_policy, BATCH_POLICY_STANDARD);
        assert_eq!(client.max_batch_size, 128);
        assert_eq!(client.overload_threshold, 200);
        assert_eq!(client.max_batch_wait_time, Duration::ZERO);
        assert_eq!(client.batch_wait_size, 8);
        assert!(client.enable_chunk_rpc);
        assert_eq!(client.region_cache_ttl, 600);
        assert_eq!(client.store_limit, 0);
        assert_eq!(client.store_liveness_timeout, "1s");
        assert_eq!(client.copr_cache, CoprocessorCache::default());
        assert_eq!(client.copr_req_timeout, Duration::from_secs(60));
        assert_eq!(client.ttl_refreshed_txn_size, 32 * 1024 * 1024);
        assert_eq!(client.resolve_lock_lite_threshold, 512);
        assert_eq!(client.max_concurrency_request_limit, i64::MAX);
        assert!(client.enable_replica_selector_v2);
        assert_eq!(client.ruv2, Ruv2TiKvConfig::default());
        assert!(client.txn_chunk_writer_addr.is_empty());
        assert_eq!(client.txn_chunk_writer_concurrency, 4);
        assert_eq!(client.txn_chunk_max_size, 128 * 1024 * 1024);
        assert_eq!(client.txn_file_min_mutation_size, 16 * 1024 * 1024);
        assert_eq!(client.txn_file_ru_discount_ratio, 0.125);
        assert!(client.txn_file_request_source_whitelist.is_empty());
        assert_eq!(NEXT_GEN, cfg!(feature = "nextgen"));
    }

    #[test]
    fn validation_preserves_source_order_and_text() {
        assert_eq!(
            PdClient {
                pd_server_timeout: 0
            }
            .validate()
            .unwrap_err()
            .to_string(),
            "pd-server-timeout can not be 0"
        );
        assert!(TxnLocalLatches::default().validate().is_ok());
        assert_eq!(
            TxnLocalLatches {
                enabled: true,
                capacity: 0
            }
            .validate()
            .unwrap_err()
            .to_string(),
            "txn-local-latches.capacity can not be 0"
        );

        let mut client = TiKvClient::default();
        assert!(client.validate().is_ok());
        client.grpc_connection_count = 0;
        assert_eq!(
            client.validate().unwrap_err().to_string(),
            "grpc-connection-count should be greater than 0"
        );
        client.grpc_connection_count = 4;
        client.grpc_compression_type = "snappy".to_owned();
        assert_eq!(
            client.validate().unwrap_err().to_string(),
            "grpc-compression-type should be none or gzip, but got snappy"
        );
        client.grpc_compression_type = "gzip".to_owned();
        assert!(client.validate().is_ok());
    }

    #[test]
    fn source_test_validate_grpc_keep_alive_timeout() {
        let mut config = TiKvClient::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.grpc_keep_alive_timeout(), Duration::from_secs(3));

        config.grpc_keep_alive_timeout = 0.05;
        assert!(config.validate().is_ok());
        assert_eq!(config.grpc_keep_alive_timeout(), Duration::from_millis(50));

        config.grpc_keep_alive_timeout = 0.04;
        assert_eq!(
            config.validate().unwrap_err().to_string(),
            "grpc-keepalive-timeout should be at least 0.05, but got 0.040000"
        );
    }

    #[test]
    fn source_test_validate_txn_file_config() {
        let mut disabled = TiKvClient::default();
        disabled.txn_chunk_max_size = 0;
        assert!(disabled.validate().is_ok());

        let max_int = isize::MAX as u64;
        let cases: [(&str, Option<fn(&mut TiKvClient)>, Option<String>); 8] = [
            ("default", None, None),
            (
                "zero chunk size",
                Some(|config| config.txn_chunk_max_size = 0),
                Some("txn-chunk-max-size should be greater than 0".to_owned()),
            ),
            (
                "maximum chunk size",
                Some(|config| {
                    config.txn_chunk_max_size = MAX_TXN_CHUNK_SIZE_IN_PARALLEL;
                }),
                None,
            ),
            (
                "chunk size exceeds parallel budget",
                Some(|config| {
                    config.txn_chunk_max_size = MAX_TXN_CHUNK_SIZE_IN_PARALLEL + 1;
                }),
                Some(format!(
                    "txn-chunk-max-size should not exceed {}, but got {}",
                    MAX_TXN_CHUNK_SIZE_IN_PARALLEL,
                    MAX_TXN_CHUNK_SIZE_IN_PARALLEL + 1
                )),
            ),
            (
                "chunk size exceeds int",
                Some(|config| config.txn_chunk_max_size = isize::MAX as u64 + 1),
                Some(format!(
                    "txn-chunk-max-size should not exceed {max_int}, but got {}",
                    max_int + 1
                )),
            ),
            (
                "zero writer concurrency",
                Some(|config| config.txn_chunk_writer_concurrency = 0),
                Some("txn-chunk-writer-concurrency should be greater than 0".to_owned()),
            ),
            (
                "maximum writer concurrency",
                Some(|config| config.txn_chunk_writer_concurrency = isize::MAX as u64),
                None,
            ),
            (
                "writer concurrency exceeds int",
                Some(|config| {
                    config.txn_chunk_writer_concurrency = isize::MAX as u64 + 1;
                }),
                Some(format!(
                    "txn-chunk-writer-concurrency should not exceed {max_int}, but got {}",
                    max_int + 1
                )),
            ),
        ];

        for (name, configure, expected_error) in cases {
            let mut config = TiKvClient::default();
            if let Some(configure) = configure {
                configure(&mut config);
            }
            match expected_error {
                Some(expected_error) => {
                    config.txn_chunk_writer_addr = "127.0.0.1".to_owned();
                    assert_eq!(
                        config.validate().unwrap_err().to_string(),
                        expected_error,
                        "source row {name}"
                    );
                }
                None => assert!(config.validate().is_ok(), "source row {name}"),
            }
        }

        // The two source success rows leave txn-file mode disabled. Exercise
        // their effective enabled boundaries as well.
        let mut enabled = TiKvClient::default();
        enabled.txn_chunk_writer_addr = "127.0.0.1".to_owned();
        enabled.txn_chunk_max_size = MAX_TXN_CHUNK_SIZE_IN_PARALLEL;
        enabled.txn_chunk_writer_concurrency = max_int;
        assert!(enabled.validate().is_ok());
    }

    #[test]
    fn source_test_parse_path() {
        assert_eq!(
            parse_path("tikv://node1:2379,node2:2379").unwrap(),
            (
                vec!["node1:2379".to_owned(), "node2:2379".to_owned()],
                false,
                String::new()
            )
        );
        assert!(parse_path("tikv://node1:2379").is_ok());
        assert_eq!(
            parse_path("tikv://node1:2379?disableGC=true&keyspaceName=DEFAULT").unwrap(),
            (vec!["node1:2379".to_owned()], true, "DEFAULT".to_owned())
        );
    }

    #[test]
    fn parse_path_matches_net_url_query_error_suppression() {
        assert_eq!(
            parse_path(
                "tikv://user@node1:2379?disableGC=true&disableGC=false&keyspaceName=a%20b#ignored"
            )
            .unwrap(),
            (vec!["node1:2379".to_owned()], true, "a b".to_owned())
        );
        assert_eq!(
            parse_path("http://node1:2379").unwrap_err().to_string(),
            "Uri scheme expected [tikv] but found [http]"
        );
        assert_eq!(
            parse_path("tikv://node1:2379?disableGC=yes")
                .unwrap_err()
                .to_string(),
            "disableGC flag should be true/false"
        );
        assert_eq!(
            parse_path("tikv://node1:2379?keyspaceName=%zz").unwrap(),
            (vec!["node1:2379".to_owned()], false, String::new())
        );
        assert_eq!(
            parse_path("tikv://node1:2379?keyspaceName=%zz&disableGC=true").unwrap(),
            (vec!["node1:2379".to_owned()], true, String::new())
        );
        assert_eq!(
            parse_path("tikv://node1:2379?keyspaceName=bad;value&disableGC=true").unwrap(),
            (vec!["node1:2379".to_owned()], true, String::new())
        );
        assert!(parse_path("tikv://node1:2379?disableGC=1").is_err());
        assert_eq!(
            parse_path("tikv://node1:2379?disableGC=TrUe").unwrap(),
            (vec!["node1:2379".to_owned()], true, String::new())
        );
        assert_eq!(
            parse_path("tikv:node1:2379").unwrap(),
            (vec![String::new()], false, String::new())
        );
        assert_eq!(
            parse_path("tikv:opaque%zz?disableGC=true").unwrap(),
            (vec![String::new()], true, String::new())
        );
        assert_eq!(
            parse_path("tikv://node1:2379#fragment?disableGC=true&keyspaceName=wrong").unwrap(),
            (vec!["node1:2379".to_owned()], false, String::new())
        );
        assert_eq!(
            parse_path("tikv://[fe80::1%25eth0]:2379").unwrap(),
            (vec!["[fe80::1%eth0]:2379".to_owned()], false, String::new())
        );
        assert!(parse_path("tikv://[fe80::1%25]:2379").is_err());
        assert!(parse_path("tikv://[::1").is_err());
        assert!(parse_path("tikv://[127.0.0.1]:2379").is_err());
        assert!(parse_path("tikv://[::1]:2379,[::2]:2380").is_err());
        assert!(parse_path("tikv://node:abc").is_err());
        assert!(parse_path("tikv://node%zz:2379").is_err());
        assert!(parse_path("tikv://node%2Fname:2379").is_err());
        assert!(parse_path("tikv://bad user@node:2379").is_err());
        assert!(parse_path("tikv://node1:2379\n").is_err());
    }

    #[test]
    fn source_special_grpc_keep_alive_timeout_conversions_match_go() {
        for timeout in [f64::NAN, f64::NEG_INFINITY] {
            let mut client = TiKvClient::default();
            client.grpc_keep_alive_timeout = timeout;
            assert!(
                client.validate().is_err(),
                "timeout {timeout:?} was accepted"
            );
        }

        let mut positive_infinity = TiKvClient::default();
        positive_infinity.grpc_keep_alive_timeout = f64::INFINITY;
        assert!(positive_infinity.validate().is_ok());
        assert_eq!(
            positive_infinity.grpc_keep_alive_timeout(),
            Duration::from_nanos(i64::MAX as u64)
        );
    }

    #[test]
    #[serial]
    fn source_test_txn_scope_value() {
        let original = get_global_config();
        store_global_config(Config::default());
        fail::cfg("injectTxnScope", "return(bj)").unwrap();
        assert_eq!(get_txn_scope_from_config(), "bj");
        fail::cfg("injectTxnScope", "return()").unwrap();
        assert_eq!(get_txn_scope_from_config(), crate::oracle::GLOBAL_TXN_SCOPE);
        fail::cfg("injectTxnScope", "return(global)").unwrap();
        assert_eq!(get_txn_scope_from_config(), crate::oracle::GLOBAL_TXN_SCOPE);
        fail::remove("injectTxnScope");
        store_global_config(original);
    }

    #[test]
    #[serial]
    fn source_update_global_restore_is_reusable_and_preserves_identity() {
        let original = get_global_config();
        let restore = update_global(|config| config.txn_scope = "zone-a".to_owned());
        assert_eq!(get_txn_scope_from_config(), "zone-a");
        assert!(!Arc::ptr_eq(&original, &get_global_config()));
        restore();
        assert!(Arc::ptr_eq(&original, &get_global_config()));

        store_global_config(Config {
            txn_scope: "zone-b".to_owned(),
            ..Config::default()
        });
        restore();
        assert!(Arc::ptr_eq(&original, &get_global_config()));
    }

    #[test]
    #[serial]
    fn source_test_update_tikv_ru_v2_from_exec_details_v2() {
        let original = get_global_config();
        store_global_config(Config::default());
        let details = &mut ExecDetailsV2 {
            ru_v2: Some(Ruv2 {
                kv_engine_cache_miss: 43,
                executor_inputs: Some(ExecutorInputs {
                    tikv_coprocessor_executor_work_total_batch_selection: 53,
                    tikv_coprocessor_executor_work_total_batch_top_n: 59,
                    ..Default::default()
                }),
                coprocessor_executor_iterations: 61,
                coprocessor_response_bytes: 67,
                raftstore_store_write_trigger_wb_bytes: 71,
                storage_processed_keys_batch_get: 73,
                storage_processed_keys_get: 79,
                write_rpc_count: 31,
                ..Default::default()
            }),
            ..Default::default()
        };
        let ru_details = RuDetails::new();
        update_tikv_ru_v2_from_exec_details_v2(Some(details), 0, 0, Some(&ru_details));
        assert!((ru_details.tikv_ru_v2() - 57.96722001).abs() < 1e-9);
        let drained = ru_details.drain_ru_v2().unwrap();
        assert_eq!(drained.kv_engine_cache_miss, 43);
        assert_eq!(
            drained
                .executor_inputs
                .as_ref()
                .unwrap()
                .tikv_coprocessor_executor_work_total_batch_selection,
            53
        );
        assert_eq!(
            drained
                .executor_inputs
                .as_ref()
                .unwrap()
                .tikv_coprocessor_executor_work_total_batch_top_n,
            59
        );
        assert_eq!(drained.write_rpc_count, 31);
        assert!(ru_details.drain_ru_v2().is_none());
        store_global_config(original);
    }

    #[test]
    #[serial]
    fn source_test_update_tikv_ru_v2_patches_read_rpc_count_in_raw_ru_v2() {
        let original = get_global_config();
        store_global_config(Config::default());
        let ru_details = RuDetails::new();
        let details = &mut ExecDetailsV2 {
            ru_v2: Some(Ruv2 {
                storage_processed_keys_get: 7,
                ..Default::default()
            }),
            ..Default::default()
        };
        update_tikv_ru_v2_from_exec_details_v2(Some(details), 1, 0, Some(&ru_details));
        assert_eq!(details.ru_v2.as_ref().unwrap().read_rpc_count, 1);
        let drained = ru_details.drain_ru_v2().unwrap();
        assert_eq!(drained.read_rpc_count, 1);
        assert_eq!(drained.storage_processed_keys_get, 7);
        store_global_config(original);
    }

    #[test]
    fn source_test_tls_config() {
        assert!(Security::default().to_tls_config().unwrap().is_none());
        let missing = Security::new("not-present.pem", "", "", Vec::new());
        assert!(missing
            .to_tls_config()
            .unwrap_err()
            .to_string()
            .starts_with("could not read ca certificate:"));

        let temp = tempdir().unwrap();
        let ca = temp.path().join("cert.pem");
        let cert = temp.path().join("client.pem");
        let key = temp.path().join("key.pem");
        // Keep TLS material ephemeral: a checked-in private key would be
        // both unnecessary for this parser/builder test and unsafe to ship.
        let rsa = Rsa::generate(2048).unwrap();
        let private_key = PKey::from_rsa(rsa).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "localhost").unwrap();
        let name = name.build();
        let mut certificate = X509::builder().unwrap();
        certificate.set_version(2).unwrap();
        let serial = BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();
        certificate.set_serial_number(&serial).unwrap();
        certificate.set_subject_name(&name).unwrap();
        certificate.set_issuer_name(&name).unwrap();
        certificate.set_pubkey(&private_key).unwrap();
        certificate
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        certificate
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        certificate
            .sign(&private_key, MessageDigest::sha256())
            .unwrap();
        let certificate = certificate.build().to_pem().unwrap();
        fs::write(&ca, &certificate).unwrap();
        fs::write(&cert, certificate).unwrap();
        fs::write(&key, private_key.private_key_to_pem_pkcs8().unwrap()).unwrap();
        let security = Security::new(
            ca.to_string_lossy(),
            cert.to_string_lossy(),
            key.to_string_lossy(),
            vec!["localhost".to_owned()],
        );
        assert!(security.to_tls_config().unwrap().is_some());

        let ca_only = Security::new(ca.to_string_lossy(), "", "", Vec::new());
        assert!(ca_only.to_tls_config().unwrap().is_some());

        let configured = Config::default().with_security(&ca, &cert, &key);
        assert_eq!(
            configured.security,
            Security::new(
                ca.to_string_lossy(),
                cert.to_string_lossy(),
                key.to_string_lossy(),
                Vec::new()
            )
        );
        assert!(configured.security_manager().is_ok());

        fs::write(&key, b"not a private key").unwrap();
        assert!(security
            .to_tls_config()
            .unwrap_err()
            .to_string()
            .starts_with("could not load client key pair:"));
    }
}
