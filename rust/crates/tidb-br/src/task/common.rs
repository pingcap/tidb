// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `br/pkg/task/common.go`: BRIE's common task settings and key reader.
//! Fields not reached by the SQL builder and CLI flag parsing remain unported.

use tidb_proto::backup::CipherInfo;

/// Go `task.TLSConfig`.
#[derive(Default)]
pub struct TLSConfig {
    /// Go `CA`.
    pub ca: String,
    /// Go `Cert`.
    pub cert: String,
    /// Go `Key` (path only).
    pub key: String,
}

/// Go `task.Config`, fields consumed by `executorBuilder.buildBRIE`.
/// Deliberately no Debug: Storage and CipherInfo can contain credentials.
pub struct Config {
    /// Go's embedded `objstore.BackendOptions`.
    pub backend_options: tidb_objstore::BackendOptions,
    /// Go `Storage` (private task input, never SHOW output).
    pub storage: String,
    /// Go `PD`.
    pub pd: Vec<String>,
    /// Go `TLS`.
    pub tls: TLSConfig,
    /// Go `RateLimit`, bytes per second.
    pub rate_limit: u64,
    /// Go `Concurrency`.
    pub concurrency: u32,
    /// Go `Checksum`.
    pub checksum: bool,
    /// Go `ChecksumConcurrency`.
    pub checksum_concurrency: usize,
    /// Go `SendCreds`.
    pub send_creds: bool,
    /// Go `CipherInfo`.
    pub cipher_info: CipherInfo,
    /// Go `FilterStr`.
    pub filter_str: Vec<String>,
    /// Go `TableFilter`.
    pub table_filter: Box<dyn tidb_table_filter::Filter>,
    /// Go `LogProgress`.
    pub log_progress: bool,
    /// Go `NoCreds`.
    pub no_creds: bool,
    /// Go `CheckRequirements`.
    pub check_requirements: bool,
    /// Go `GRPCKeepaliveTime`.
    pub grpc_keepalive_time: std::time::Duration,
    /// Go `GRPCKeepaliveTimeout`.
    pub grpc_keepalive_timeout: std::time::Duration,
    /// Go `KeyspaceName`.
    pub keyspace_name: String,
    /// Go `MetadataDownloadBatchSize`.
    pub metadata_download_batch_size: u32,
}

/// Go `defaultGRPCKeepaliveTime`.
const DEFAULT_GRPC_KEEPALIVE_TIME: std::time::Duration = std::time::Duration::from_secs(10);
/// Go `defaultGRPCKeepaliveTimeout`.
const DEFAULT_GRPC_KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// Go `defaultMetadataDownloadBatchSize`.
const DEFAULT_METADATA_DOWNLOAD_BATCH_SIZE: u32 = 128;
/// Go `vardef.DefChecksumTableConcurrency`.
const DEF_CHECKSUM_TABLE_CONCURRENCY: usize = 4;

impl Config {
    /// Go `Config.adjust`: the defaults of values left zero (BRIE does not
    /// parse the CLI flags).
    pub fn adjust(&mut self) {
        if self.grpc_keepalive_time.is_zero() {
            self.grpc_keepalive_time = DEFAULT_GRPC_KEEPALIVE_TIME;
        }
        if self.grpc_keepalive_timeout.is_zero() {
            self.grpc_keepalive_timeout = DEFAULT_GRPC_KEEPALIVE_TIMEOUT;
        }
        if self.checksum_concurrency == 0 {
            self.checksum_concurrency = DEF_CHECKSUM_TABLE_CONCURRENCY;
        }
        if self.metadata_download_batch_size == 0 {
            self.metadata_download_batch_size = DEFAULT_METADATA_DOWNLOAD_BATCH_SIZE;
        }
    }

    /// Go `TLSConfig.IsEnabled`.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        !self.tls.ca.is_empty()
    }
}

/// Go `GetKeepalive`.
#[must_use]
pub fn get_keepalive(cfg: &Config) -> crate::utils::store_manager::Keepalive {
    crate::utils::store_manager::Keepalive { time: cfg.grpc_keepalive_time, timeout: cfg.grpc_keepalive_timeout }
}

/// Go `NewMgr`: the BR connection over the glue's store, skipping TiFlash.
///
/// # Errors
/// No PD address, or `conn.NewMgr`'s error.
pub fn new_mgr(
    ctx: &tidb_gocontext::Context,
    g: &dyn crate::glue::Glue,
    cfg: &Config,
    need_domain: bool,
    version_checker: crate::conn::VersionCheckerType,
) -> crate::errors::Result<crate::conn::Mgr> {
    if cfg.pd.is_empty() {
        return Err(crate::errors::annotate(
            crate::errors::new(&crate::errors::ERR_INVALID_ARGUMENT),
            "pd address can not be empty",
        ));
    }
    let security = if cfg.tls_enabled() {
        tidb_pd_client::ClusterSecurity::new(cfg.tls.ca.clone(), cfg.tls.cert.clone(), cfg.tls.key.clone(), Vec::new())
    } else {
        tidb_pd_client::ClusterSecurity::plaintext()
    };
    crate::conn::Mgr::new(
        ctx,
        g,
        crate::conn::MgrOptions {
            keyspace_name: &cfg.keyspace_name,
            pd_addrs: &cfg.pd,
            security,
            keepalive: get_keepalive(cfg),
            store_behavior: crate::conn::StoreBehavior::SkipTiFlash,
            check_requirements: cfg.check_requirements,
            need_domain,
            version_checker,
        },
    )
}

/// Go `DefaultConfig`'s SQL-builder fields. Values from `TestDefault`.
pub fn default_config() -> Config {
    Config {
        // Go `S3BackendOptions.ParseFromFlags` always sets ForcePathStyle.
        backend_options: tidb_objstore::BackendOptions {
            s3: tidb_objstore_s3like::S3BackendOptions {
                force_path_style: true,
                ..Default::default()
            },
            ..Default::default()
        },
        storage: String::new(),
        pd: vec!["127.0.0.1:2379".into()],
        tls: TLSConfig::default(),
        rate_limit: 0,
        concurrency: 0,
        checksum: false,
        checksum_concurrency: 4,
        send_creds: true,
        cipher_info: CipherInfo {
            cipher_type: tidb_proto::encryptionpb::EncryptionMethod::Plaintext as i32,
            ..Default::default()
        },
        filter_str: vec!["*.*".into()],
        table_filter: tidb_table_filter::case_insensitive(tidb_table_filter::all()),
        log_progress: false,
        no_creds: false,
        check_requirements: true,
        grpc_keepalive_time: DEFAULT_GRPC_KEEPALIVE_TIME,
        grpc_keepalive_timeout: DEFAULT_GRPC_KEEPALIVE_TIMEOUT,
        keyspace_name: String::new(),
        metadata_download_batch_size: DEFAULT_METADATA_DOWNLOAD_BATCH_SIZE,
    }
}

/// Go `ApplyTiDBRuntimeConfig`: a BRIE statement inside TiDB reaches the
/// cluster through TiDB's own PD addresses (blank entries dropped) and, when
/// any is set, its cluster TLS files.
pub fn apply_tidb_runtime_config(cfg: &mut Config) {
    let tidb_cfg = tidb_config::config_tree::config::get_global_config();
    if !tidb_cfg.path.is_empty() {
        let pds: Vec<String> = tidb_cfg
            .path
            .split(',')
            .map(str::trim)
            .filter(|pd| !pd.is_empty())
            .map(str::to_owned)
            .collect();
        if !pds.is_empty() {
            cfg.pd = pds;
        }
    }
    let security = &tidb_cfg.security;
    if !security.cluster_ssl_ca.is_empty()
        || !security.cluster_ssl_cert.is_empty()
        || !security.cluster_ssl_key.is_empty()
    {
        cfg.tls = TLSConfig {
            ca: security.cluster_ssl_ca.clone(),
            cert: security.cluster_ssl_cert.clone(),
            key: security.cluster_ssl_key.clone(),
        };
    }
}

/// Go `checkCipherKey`.
fn check_cipher_key(key: &str, file: &str) -> Result<(), String> {
    if key.is_empty() == file.is_empty() {
        Err("exactly one of cipher key or keyfile path should be provided: [BR:Common:ErrInvalidArgument]invalid argument".into())
    } else {
        Ok(())
    }
}

/// Go `GetCipherKeyContent`. Decode errors never include the key.
pub fn get_cipher_key_content(key: &str, file: &str) -> Result<Vec<u8>, String> {
    check_cipher_key(key, file)?;
    let data = if key.is_empty() {
        let mut data =
            std::fs::read(file).map_err(|err| format!("failed to read cipher file: {err}"))?;
        if data.last() == Some(&b'\n') {
            data.pop();
        }
        data
    } else {
        key.as_bytes().to_vec()
    };
    hex::decode(data).map_err(|_| "cipher key must be a valid hexadecimal string: [BR:Common:ErrInvalidArgument]invalid argument".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestCheckCipherKey`.
    #[test]
    fn test_check_cipher_key() {
        for (key, file, ok) in [
            ("0123456789abcdef0123456789abcdef", "", true),
            ("0123456789abcdef0123456789abcdef", "/tmp/abc", false),
            ("", "/tmp/abc", true),
            ("", "", false),
        ] {
            assert_eq!(check_cipher_key(key, file).is_ok(), ok);
        }
    }
    /// Go `TestGetCipherKey`.
    #[test]
    fn test_get_cipher_key() {
        assert!(get_cipher_key_content("this is not a hex string", "")
            .unwrap_err()
            .contains("cipher key must be a valid hexadecimal string"));
    }
    /// Go `TestDefault`, fields used by the SQL builder (no CLI flag parser).
    #[test]
    fn test_default() {
        let cfg = default_config();
        let backend = tidb_objstore::BackendOptions {
            s3: tidb_objstore_s3like::S3BackendOptions {
                force_path_style: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(cfg.backend_options == backend);
        assert_eq!(cfg.pd, ["127.0.0.1:2379"]);
        assert!(!cfg.checksum);
        assert!(cfg.send_creds);
        assert_eq!(cfg.concurrency, 0);
        assert_eq!(cfg.checksum_concurrency, 4);
        assert_eq!(cfg.rate_limit, 0);
        assert_eq!(cfg.cipher_info.cipher_type, 1);
        assert_eq!(cfg.filter_str, ["*.*"]);
        assert!(cfg.table_filter.match_table("test", "a"));
    }
}
