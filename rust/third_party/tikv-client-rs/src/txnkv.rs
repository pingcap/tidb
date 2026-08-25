// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Root transactional-client facade corresponding to client-go's `txnkv`
//! package.
//!
//! The implementation owns the source package's construction options and
//! close behavior while re-exporting the completed transaction, snapshot, and
//! lock implementations from their native Rust modules.

use std::future::Future;
use std::ops::Deref;

use crate::config::{get_global_config, Config};
use crate::proto::kvrpcpb;
use crate::tikv::KvStore;
use crate::transaction::close_txn_file_idle_connections;
use crate::{Result, TimestampExt};

pub use crate::kv::ReplicaReadAdjuster;
pub use crate::proto::kvrpcpb::ApiVersion;
pub use crate::proto::kvrpcpb::IsolationLevel as IsoLevel;
pub use crate::transaction::BinlogWriteResult;
pub use crate::transaction::KvFilter;
pub use crate::transaction::Lock;
pub use crate::transaction::LockResolver;
pub use crate::transaction::Priority;
pub use crate::transaction::SchemaLeaseChecker;
pub use crate::transaction::SchemaVersion as SchemaVer;
pub use crate::transaction::Snapshot as KvSnapshot;
pub use crate::transaction::SnapshotIterator as Scanner;
pub use crate::transaction::SnapshotRuntimeStats;
pub use crate::transaction::Transaction as KvTxn;
pub use crate::transaction::TransactionStatus as TxnStatus;
pub use crate::transaction::TransactionStatusKind as TxnStatusKind;
pub use crate::transaction::MAX_TXN_TIME_USE;

pub const SI: IsoLevel = IsoLevel::Si;
pub const RC: IsoLevel = IsoLevel::Rc;
pub const RC_CHECK_TS: IsoLevel = IsoLevel::RcCheckTs;
pub const PRIORITY_HIGH: Priority = Priority::High;
pub const PRIORITY_NORMAL: Priority = Priority::Normal;
pub const PRIORITY_LOW: Priority = Priority::Low;

/// One source-shaped root client option. Later options of the same kind win.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientOption {
    Keyspace(String),
    ApiVersion(ApiVersion),
    SafePointKvPrefix(String),
}

pub fn with_keyspace(keyspace_name: impl Into<String>) -> ClientOption {
    ClientOption::Keyspace(keyspace_name.into())
}

pub const fn with_api_version(api_version: ApiVersion) -> ClientOption {
    ClientOption::ApiVersion(api_version)
}

pub fn with_safe_point_kv_prefix(prefix: impl Into<String>) -> ClientOption {
    ClientOption::SafePointKvPrefix(prefix.into())
}

/// Resolved root `txnkv` construction options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientOptions {
    api_version: ApiVersion,
    keyspace_name: String,
    safe_point_kv_prefix: String,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            api_version: ApiVersion::V1,
            keyspace_name: String::new(),
            safe_point_kv_prefix: String::new(),
        }
    }
}

impl ClientOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_options(options: impl IntoIterator<Item = ClientOption>) -> Self {
        let mut resolved = Self::default();
        for option in options {
            resolved.apply(option);
        }
        resolved
    }

    pub fn apply(&mut self, option: ClientOption) {
        match option {
            ClientOption::Keyspace(keyspace_name) => self.keyspace_name = keyspace_name,
            ClientOption::ApiVersion(api_version) => self.api_version = api_version,
            ClientOption::SafePointKvPrefix(prefix) => self.safe_point_kv_prefix = prefix,
        }
    }

    #[must_use]
    pub fn with_keyspace(mut self, keyspace_name: impl Into<String>) -> Self {
        self.keyspace_name = keyspace_name.into();
        self
    }

    #[must_use]
    pub const fn with_api_version(mut self, api_version: ApiVersion) -> Self {
        self.api_version = api_version;
        self
    }

    #[must_use]
    pub fn with_safe_point_kv_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.safe_point_kv_prefix = prefix.into();
        self
    }

    pub const fn api_version(&self) -> ApiVersion {
        self.api_version
    }

    pub fn keyspace_name(&self) -> &str {
        &self.keyspace_name
    }

    pub fn safe_point_kv_prefix(&self) -> &str {
        &self.safe_point_kv_prefix
    }

    fn resolve_config(&self, base: &Config) -> Result<(Config, String)> {
        let mut config = base.clone();
        config.keyspace = match self.api_version {
            ApiVersion::V1 => None,
            ApiVersion::V2 => Some(self.keyspace_name.clone()),
            unsupported => {
                return Err(crate::Error::StringError(format!(
                    "unknown api version: {}",
                    unsupported as i32
                )));
            }
        };
        Ok((config, self.safe_point_kv_prefix.clone()))
    }
}

/// Transactional client embedding the complete root [`KvStore`] facade.
#[derive(Clone)]
pub struct Client {
    store: KvStore,
}

impl Client {
    /// Creates a V1 transactional client from the process-global source
    /// configuration.
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_client_options(pd_endpoints, ClientOptions::default()).await
    }

    /// Creates a client after applying source-shaped options in order.
    pub async fn new_with_options<S: Into<String>>(
        pd_endpoints: Vec<S>,
        options: impl IntoIterator<Item = ClientOption>,
    ) -> Result<Self> {
        Self::new_with_client_options(pd_endpoints, ClientOptions::from_options(options)).await
    }

    /// Creates a client from an already resolved native option builder.
    pub async fn new_with_client_options<S: Into<String>>(
        pd_endpoints: Vec<S>,
        options: ClientOptions,
    ) -> Result<Self> {
        let global = get_global_config();
        let (config, safe_point_kv_prefix) = options.resolve_config(&global)?;
        let store = KvStore::new_with_config_and_safe_point_prefix(
            pd_endpoints,
            config,
            safe_point_kv_prefix,
        )
        .await?;
        Ok(Self { store })
    }

    /// Returns the current global timestamp as its packed TiKV version.
    pub async fn get_timestamp(&self) -> Result<u64> {
        self.store.current_timestamp().await.map(|ts| ts.version())
    }

    /// Closes root store workers/transports and then clears the shared txn-file
    /// uploader's idle HTTP pool, including when store close returns an error.
    pub async fn close(&self) -> Result<()> {
        close_components(self.store.clone().close(), close_txn_file_idle_connections).await
    }

    pub fn store(&self) -> &KvStore {
        &self.store
    }

    pub fn into_store(self) -> KvStore {
        self.store
    }
}

impl Deref for Client {
    type Target = KvStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl AsRef<KvStore> for Client {
    fn as_ref(&self) -> &KvStore {
        &self.store
    }
}

/// Source-shaped free constructor for callers migrating package-qualified Go
/// code. Native Rust callers may use [`Client::new_with_options`] directly.
pub async fn new_client<S: Into<String>>(
    pd_endpoints: Vec<S>,
    options: impl IntoIterator<Item = ClientOption>,
) -> Result<Client> {
    Client::new_with_options(pd_endpoints, options).await
}

pub fn new_lock(lock: &kvrpcpb::LockInfo) -> Lock {
    Lock::from_lock_info(lock)
}

async fn close_components<F, C>(store_close: F, close_idle: C) -> Result<()>
where
    F: Future<Output = Result<()>>,
    C: FnOnce(),
{
    let result = store_close.await;
    close_idle();
    result
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::proto::kvrpcpb::{Action, CheckTxnStatusResponse, LockInfo, Op};
    use crate::transaction::TransactionStatus;

    use super::*;

    #[test]
    fn source_options_are_ordered_and_map_api_versions_exactly() {
        let base = Config::default().with_keyspace("global-must-not-leak");
        let defaults = ClientOptions::default();
        let (config, prefix) = defaults.resolve_config(&base).unwrap();
        assert_eq!(defaults.api_version(), ApiVersion::V1);
        assert_eq!(config.keyspace, None);
        assert_eq!(prefix, "");

        let options = ClientOptions::from_options([
            with_keyspace("first"),
            with_api_version(ApiVersion::V2),
            with_keyspace("tenant"),
            with_safe_point_kv_prefix("/namespace"),
        ]);
        let (config, prefix) = options.resolve_config(&base).unwrap();
        assert_eq!(config.keyspace.as_deref(), Some("tenant"));
        assert_eq!(prefix, "/namespace");

        let default_v2 = ClientOptions::new().with_api_version(ApiVersion::V2);
        let (config, _) = default_v2.resolve_config(&base).unwrap();
        assert_eq!(config.keyspace.as_deref(), Some(""));

        let error = ClientOptions::new()
            .with_api_version(ApiVersion::V1ttl)
            .resolve_config(&base)
            .unwrap_err();
        assert_eq!(error.to_string(), "unknown api version: 1");
    }

    #[test]
    fn root_aliases_and_compile_only_close_surface_are_available() {
        fn accepts_txn(_: &KvTxn) {}
        fn accepts_snapshot(_: &KvSnapshot) {}
        fn accepts_scanner(_: &Scanner<'_>) {}
        fn accepts_runtime_stats(_: &SnapshotRuntimeStats) {}
        fn accepts_resolver(_: &LockResolver) {}
        fn accepts_adjuster(_: &ReplicaReadAdjuster) {}
        fn accepts_binlog_result(_: &dyn BinlogWriteResult) {}
        fn accepts_filter(_: &dyn KvFilter) {}
        fn accepts_schema_version(_: &dyn SchemaVer) {}
        fn accepts_schema_checker(_: &dyn SchemaLeaseChecker) {}
        fn accepts_store(value: &Client) -> &KvStore {
            value
        }
        async fn close_from_value(value: Client) -> Result<()> {
            value.close().await
        }
        fn close_from_reference(value: &Client) -> impl Future<Output = Result<()>> + '_ {
            value.close()
        }
        let _ = accepts_store;
        let _ = accepts_txn;
        let _ = accepts_snapshot;
        let _ = accepts_scanner;
        let _ = accepts_runtime_stats;
        let _ = accepts_resolver;
        let _ = accepts_adjuster;
        let _ = accepts_binlog_result;
        let _ = accepts_filter;
        let _ = accepts_schema_version;
        let _ = accepts_schema_checker;
        let _ = close_from_value;
        let _ = close_from_reference;

        assert_eq!(SI, IsoLevel::Si);
        assert_eq!(RC, IsoLevel::Rc);
        assert_eq!(RC_CHECK_TS, IsoLevel::RcCheckTs);
        assert_eq!(PRIORITY_HIGH, Priority::High);
        assert_eq!(PRIORITY_NORMAL, Priority::Normal);
        assert_eq!(PRIORITY_LOW, Priority::Low);
        assert_eq!(MAX_TXN_TIME_USE, 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn new_lock_copies_every_source_field_and_classifies_lock_types() {
        let info = LockInfo {
            key: b"key".to_vec(),
            primary_lock: b"primary".to_vec(),
            lock_version: 11,
            lock_ttl: 12,
            txn_size: 13,
            lock_type: Op::SharedPessimisticLock as i32,
            use_async_commit: true,
            lock_for_update_ts: 14,
            min_commit_ts: 15,
            is_txn_file: true,
            ..Default::default()
        };
        let lock = new_lock(&info);
        assert_eq!(lock.key, b"key");
        assert_eq!(lock.primary, b"primary");
        assert_eq!(lock.txn_id, 11);
        assert_eq!(lock.ttl, 12);
        assert_eq!(lock.txn_size, 13);
        assert_eq!(lock.operation(), Some(Op::SharedPessimisticLock));
        assert!(lock.is_pessimistic());
        assert!(!lock.is_shared());
        assert!(lock.use_async_commit);
        assert_eq!(lock.lock_for_update_ts, 14);
        assert_eq!(lock.min_commit_ts, 15);
        assert!(lock.is_txn_file);

        let shared = new_lock(&LockInfo {
            lock_type: Op::SharedLock as i32,
            ..Default::default()
        });
        assert!(shared.is_shared());
        assert!(!shared.is_pessimistic());
    }

    #[test]
    fn exported_transaction_status_methods_match_source_states() {
        let committed = TransactionStatus::from(CheckTxnStatusResponse {
            commit_version: 42,
            action: Action::NoAction as i32,
            ..Default::default()
        });
        assert!(committed.is_committed());
        assert!(!committed.is_rolled_back());
        assert!(committed.is_status_determined());
        assert!(committed.status_cacheable());
        assert_eq!(committed.commit_ts(), 42);
        assert_eq!(committed.ttl(), 0);
        assert_eq!(committed.action(), Action::NoAction);

        let same = TransactionStatus::from(CheckTxnStatusResponse {
            commit_version: 42,
            ..Default::default()
        });
        assert!(committed.has_same_determined_status(&same));

        let rolled_back = TransactionStatus::from(CheckTxnStatusResponse::default());
        assert!(rolled_back.is_rolled_back());
        assert!(rolled_back.has_same_determined_status(&rolled_back));
    }

    #[tokio::test]
    async fn close_always_releases_txn_file_idle_connections_after_store_close() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let store_calls = calls.clone();
        let idle_calls = calls.clone();
        let result = close_components(
            async move {
                store_calls.lock().unwrap().push("store");
                Err(crate::Error::StringError("close failed".to_owned()))
            },
            move || idle_calls.lock().unwrap().push("txn-file"),
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "close failed");
        assert_eq!(*calls.lock().unwrap(), ["store", "txn-file"]);
    }
}
