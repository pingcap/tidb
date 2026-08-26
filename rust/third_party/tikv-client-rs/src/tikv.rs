// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Root `tikv` package compatibility surface.
//!
//! The native Rust owners remain in `pd`, `region_cache`, `request`, `store`,
//! and `transaction`; this module supplies the store-wide lifecycle and
//! safe-point coordination that client-go keeps in its root `tikv` package.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::StreamExt;
use prost::Message;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::Channel;
use tonic::Request as TonicRequest;
use tonic_prost::ProstCodec;

use crate::async_util::Cancellation;
use crate::config::Config;
use crate::oracle;
use crate::pd::{PdClient, PdRpcClient};
use crate::proto::{keyspacepb, kvrpcpb};
use crate::request::Plan;
use crate::request::NULL_KEYSPACE_ID;
use crate::{Error, Result, SecurityManager};
use crate::{Key, Timestamp, TimestampExt};

pub use crate::async_util::Pool;
pub use crate::backoff::{Backoff, DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF};
pub use crate::interceptor::{RpcInterceptor, RpcInterceptorChain, RpcInterceptorHandle};
pub use crate::kv::Getter;
pub use crate::logutil::with_logger as with_log_context;
pub use crate::pd::PdClient as PlacementDriverClient;
pub use crate::pd::{get_store_liveness_timeout, set_store_liveness_timeout};
pub use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
pub use crate::region_cache::{
    change_pd_region_meta_circuit_breaker_settings, set_region_cache_ttl_secs,
    set_region_cache_ttl_with_jitter, PdRegionMetaCircuitBreakerSettings, RegionCache,
    TiFlashLabelFilter, TiFlashRpcContextUnavailableDetail, TiFlashRpcContextUnavailableReason,
    TiFlashSelectionError,
};
pub use crate::request::{
    api_v1_excluded_prefixes as codec_v1_exclude_prefixes, api_v2_prefixes as codec_v2_prefixes,
    decode_api_key, ApiV1Codec, ApiV2Codec, KeyMode, Keyspace, DEFAULT_KEYSPACE_ID,
    DEFAULT_KEYSPACE_NAME,
};
pub use crate::resource_control::{
    disable_resource_control, enable_resource_control, set_resource_control_interceptor,
    unset_resource_control_interceptor,
};
pub use crate::retry::{
    RetryBackoffer as Backoffer, RetryConfig as BackoffConfig, BO_PD_RPC, BO_REGION_MISS,
    BO_TIFLASH_RPC, BO_TIKV_RPC, BO_TXN_LOCK,
};
pub use crate::store::{
    ClientEventListener, EndpointType, KvClient as Client, RegionStore, Request, Store, TikvConnect,
};
pub use crate::transaction::unionstore::{
    KvIterator as Iterator, MemDb as MemDB, MemDbSnapshot as MemBufferSnapshot,
    PipelinedMetrics as Metrics,
};
pub use crate::transaction::Client as TransactionClient;
pub use crate::transaction::SchemaVersion as SchemaVer;
pub use crate::transaction::{
    BinlogWriteResult, KvFilter, LockResolver, PipelinedTxnOptions, SchemaLeaseChecker,
    SchemaVersion, Transaction as KvTxn, TransactionOptions, MAX_TXN_TIME_USE,
};
pub use crate::util::enable_failpoints;
pub type MemBuffer = MemDB;
pub type MemDBCheckpoint = usize;
pub type Mode = KeyMode;
pub type KeyspaceId = u32;
pub use crate::{KeyRange, Variables};

/// Store label used as client-go's transaction/DC scope.
pub const DC_LABEL_KEY: &str = "zone";
pub const GC_SAVED_SAFE_POINT: &str = "/tidb/store/gcworker/saved_safe_point";
pub const GC_STATE_CACHE_INTERVAL: Duration = Duration::from_secs(100);
pub const GC_CPU_TIME_INACCURACY_BOUND: Duration = Duration::from_secs(10);
pub const POLL_TXN_SAFE_POINT_INTERVAL: Duration = Duration::from_secs(10);
pub const POLL_TXN_SAFE_POINT_QUICK_REPEAT_INTERVAL: Duration = Duration::from_secs(1);
pub const SAFE_TS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
pub const READ_TIMEOUT_SHORT: Duration = Duration::from_secs(30);
pub const READ_TIMEOUT_MEDIUM: Duration = Duration::from_secs(60);
pub const MAX_WRITE_EXECUTION_TIME: Duration = Duration::from_secs(20);
pub const GC_SCAN_LOCK_LIMIT: u32 = 2_048;
pub const SPLIT_BATCH_REGION_LIMIT: usize = 2_048;
pub const MODE_RAW: Mode = KeyMode::Raw;
pub const MODE_TXN: Mode = KeyMode::Txn;
pub const NULLSPACE_ID: KeyspaceId = NULL_KEYSPACE_ID;

/// client-go's `WithDefaultPipelinedTxn` values.
pub const fn default_pipelined_txn_options() -> PipelinedTxnOptions {
    PipelinedTxnOptions {
        enable: true,
        flush_concurrency: 128,
        resolve_lock_concurrency: 8,
        write_throttle_ratio: 0.0,
    }
}

/// client-go's parameterized `WithPipelinedTxn` values.
pub const fn pipelined_txn_options(
    flush_concurrency: usize,
    resolve_lock_concurrency: usize,
    write_throttle_ratio: f64,
) -> PipelinedTxnOptions {
    PipelinedTxnOptions {
        enable: true,
        flush_concurrency,
        resolve_lock_concurrency,
        write_throttle_ratio,
    }
}

const UNIFIED_TXN_SAFE_POINT_PATH: &str = GC_SAVED_SAFE_POINT;
const KEYSPACE_LEVEL_TXN_SAFE_POINT_PATH: &str =
    "/keyspaces/tidb/{keyspace_id}/tidb/store/gcworker/saved_safe_point";

pub fn new_backoffer(cancellation: Cancellation, max_sleep_ms: u64) -> Backoffer {
    Backoffer::new(cancellation, max_sleep_ms)
}

pub fn new_backoffer_with_variables(
    cancellation: Cancellation,
    max_sleep_ms: u64,
    variables: Arc<Variables>,
) -> Backoffer {
    Backoffer::with_variables(cancellation, max_sleep_ms, variables)
}

pub fn new_gc_resolve_lock_max_backoffer(cancellation: Cancellation) -> Backoffer {
    Backoffer::new(cancellation, 100_000)
}

pub fn new_noop_backoff(cancellation: Cancellation) -> Backoffer {
    Backoffer::noop(cancellation)
}

pub const fn bo_region_miss() -> BackoffConfig {
    BO_REGION_MISS
}

pub const fn bo_tiflash_rpc() -> BackoffConfig {
    BO_TIFLASH_RPC
}

pub const fn bo_txn_lock() -> BackoffConfig {
    BO_TXN_LOCK
}

pub const fn bo_pd_rpc() -> BackoffConfig {
    BO_PD_RPC
}

pub const fn bo_tikv_rpc() -> BackoffConfig {
    BO_TIKV_RPC
}

pub const fn new_region_ver_id(id: u64, conf_ver: u64, ver: u64) -> RegionVerId {
    RegionVerId { id, conf_ver, ver }
}

pub fn get_store_type_by_meta(store: &crate::proto::metapb::Store) -> EndpointType {
    EndpointType::from_store(store)
}

/// A key/value returned by a prefix safe-point lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafePointKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Pluggable safe-point persistence used by production and deterministic tests.
#[async_trait]
pub trait SafePointKv: Send + Sync {
    async fn put(&self, key: &str, value: &str) -> Result<()>;
    async fn get(&self, key: &str) -> Result<String>;
    async fn get_with_prefix(&self, prefix: &str) -> Result<Vec<SafePointKeyValue>>;
    async fn close(&self) -> Result<()>;
}

/// Thread-safe in-memory safe-point store.
#[derive(Default)]
pub struct MockSafePointKv {
    store: RwLock<BTreeMap<String, String>>,
}

impl MockSafePointKv {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SafePointKv for MockSafePointKv {
    async fn put(&self, key: &str, value: &str) -> Result<()> {
        self.store
            .write()
            .expect("mock safe-point lock poisoned")
            .insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<String> {
        Ok(self
            .store
            .read()
            .expect("mock safe-point lock poisoned")
            .get(key)
            .cloned()
            .unwrap_or_default())
    }

    async fn get_with_prefix(&self, prefix: &str) -> Result<Vec<SafePointKeyValue>> {
        let prefix = prefix.to_owned();
        Ok(self
            .store
            .read()
            .expect("mock safe-point lock poisoned")
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(key, value)| SafePointKeyValue {
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
            })
            .collect())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Minimal etcd-v3 KV client used only for safe-point compatibility.
pub struct EtcdSafePointKv {
    endpoints: Vec<String>,
    security: Arc<SecurityManager>,
    prefix: String,
    channel: AsyncMutex<Option<Channel>>,
    closed: AtomicBool,
}

impl EtcdSafePointKv {
    pub async fn new(
        endpoints: Vec<String>,
        security: Arc<SecurityManager>,
        prefix: impl Into<String>,
    ) -> Result<Self> {
        if endpoints.is_empty() {
            return Err(Error::StringError(
                "etcd endpoints must not be empty".to_owned(),
            ));
        }
        let this = Self {
            endpoints,
            security,
            prefix: prefix.into(),
            channel: AsyncMutex::new(None),
            closed: AtomicBool::new(false),
        };
        this.channel().await?;
        Ok(this)
    }

    fn key(&self, key: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, key).into_bytes()
    }

    async fn channel(&self) -> Result<Channel> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::StringError("safe-point KV is closed".to_owned()));
        }
        let mut channel = self.channel.lock().await;
        if let Some(existing) = channel.as_ref() {
            return Ok(existing.clone());
        }
        let mut last_error = None;
        for endpoint in &self.endpoints {
            match self.security.connect(endpoint, |channel| channel).await {
                Ok(connected) => {
                    *channel = Some(connected.clone());
                    return Ok(connected);
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| Error::StringError("cannot connect to etcd".to_owned())))
    }

    async fn unary<Req, Resp>(
        &self,
        request: Req,
        path: &'static str,
        timeout: Duration,
    ) -> Result<Resp>
    where
        Req: Message + Default + Send + Sync + 'static,
        Resp: Message + Default + Send + Sync + 'static,
    {
        let channel = self.channel().await?;
        let mut grpc = tonic::client::Grpc::new(channel);
        grpc.ready()
            .await
            .map_err(|error| Error::StringError(format!("etcd service unavailable: {error}")))?;
        let mut request = TonicRequest::new(request);
        request.set_timeout(timeout);
        Ok(grpc
            .unary(
                request,
                PathAndQuery::from_static(path),
                ProstCodec::default(),
            )
            .await?
            .into_inner())
    }
}

#[async_trait]
impl SafePointKv for EtcdSafePointKv {
    async fn put(&self, key: &str, value: &str) -> Result<()> {
        let _: EtcdPutResponse = self
            .unary(
                EtcdPutRequest {
                    key: self.key(key),
                    value: value.as_bytes().to_vec(),
                },
                "/etcdserverpb.KV/Put",
                Duration::from_secs(5),
            )
            .await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<String> {
        let response: EtcdRangeResponse = self
            .unary(
                EtcdRangeRequest {
                    key: self.key(key),
                    ..Default::default()
                },
                "/etcdserverpb.KV/Range",
                Duration::from_secs(5),
            )
            .await?;
        response
            .kvs
            .first()
            .map(|kv| String::from_utf8(kv.value.clone()))
            .transpose()
            .map_err(|error| Error::StringError(format!("safe point is not UTF-8: {error}")))
            .map(Option::unwrap_or_default)
    }

    async fn get_with_prefix(&self, prefix: &str) -> Result<Vec<SafePointKeyValue>> {
        let key = self.key(prefix);
        let response: EtcdRangeResponse = self
            .unary(
                EtcdRangeRequest {
                    range_end: prefix_end(&key),
                    key,
                },
                "/etcdserverpb.KV/Range",
                Duration::from_secs(15),
            )
            .await?;
        Ok(response
            .kvs
            .into_iter()
            .map(|kv| SafePointKeyValue {
                key: kv
                    .key
                    .strip_prefix(self.prefix.as_bytes())
                    .unwrap_or(&kv.key)
                    .to_vec(),
                value: kv.value,
            })
            .collect())
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.channel.lock().await.take();
        Ok(())
    }
}

#[derive(Clone, PartialEq, Message)]
struct EtcdRangeRequest {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    range_end: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct EtcdRangeResponse {
    #[prost(message, repeated, tag = "2")]
    kvs: Vec<EtcdKeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct EtcdKeyValue {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct EtcdPutRequest {
    #[prost(bytes = "vec", tag = "1")]
    key: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct EtcdPutResponse {}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] < u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return end;
        }
    }
    vec![0]
}

pub async fn save_safe_point(kv: &dyn SafePointKv, value: u64) -> Result<()> {
    kv.put(GC_SAVED_SAFE_POINT, &value.to_string()).await
}

pub async fn load_safe_point(kv: &dyn SafePointKv) -> Result<u64> {
    let value = kv.get(GC_SAVED_SAFE_POINT).await?;
    if value.is_empty() {
        return Ok(0);
    }
    value
        .parse()
        .map_err(|error| Error::StringError(format!("invalid saved safe point {value:?}: {error}")))
}

/// Cached transaction safe point and its source freshness timestamp.
pub struct TxnSafePointCache {
    state: RwLock<(u64, SystemTime)>,
}

impl TxnSafePointCache {
    pub fn new(safe_point: u64, now: SystemTime) -> Self {
        Self {
            state: RwLock::new((safe_point, now)),
        }
    }

    pub fn update(&self, safe_point: u64, now: SystemTime) {
        *self.state.write().expect("safe-point cache lock poisoned") = (safe_point, now);
    }

    pub fn safe_point(&self) -> u64 {
        self.state.read().expect("safe-point cache lock poisoned").0
    }

    pub fn check_visibility_at(&self, start_timestamp: u64, now: SystemTime) -> Result<()> {
        let (safe_point, updated) = *self.state.read().expect("safe-point cache lock poisoned");
        let elapsed = now.duration_since(updated).unwrap_or_default();
        if elapsed > GC_STATE_CACHE_INTERVAL - GC_CPU_TIME_INACCURACY_BOUND {
            return Err(crate::error::PdServerTimeoutError {
                message: "start timestamp may fall behind safe point".to_owned(),
            }
            .into());
        }
        if start_timestamp < safe_point {
            return Err(crate::error::TransactionAbortedByGcError {
                transaction_start_timestamp: start_timestamp,
                transaction_start_time: oracle::get_time_from_timestamp(start_timestamp),
                transaction_safe_point: safe_point,
                transaction_safe_point_time: oracle::get_time_from_timestamp(safe_point),
            }
            .into());
        }
        Ok(())
    }

    pub fn check_visibility(&self, start_timestamp: u64) -> Result<()> {
        self.check_visibility_at(start_timestamp, SystemTime::now())
    }
}

/// Optional PD-HTTP resolved-TS provider. Invalid values fall back per store.
#[async_trait]
pub trait ResolvedTsProvider: Send + Sync {
    async fn min_resolved_ts(&self, store_ids: &[u64]) -> Result<(u64, HashMap<u64, u64>)>;
}

#[derive(Default)]
struct SafeTsState {
    stores: HashMap<u64, u64>,
    scopes: HashMap<String, u64>,
}

impl SafeTsState {
    fn set_store(&mut self, store_id: u64, safe_ts: u64) {
        if safe_ts == u64::MAX {
            return;
        }
        let previous = self.stores.get(&store_id).copied().unwrap_or(0);
        if safe_ts >= previous {
            self.stores.insert(store_id, safe_ts);
        }
    }

    fn set_scope(&mut self, scope: &str, safe_ts: u64) {
        if safe_ts == u64::MAX {
            return;
        }
        self.scopes.insert(scope.to_owned(), safe_ts);
    }

    fn set_scope_monotonic(&mut self, scope: &str, safe_ts: u64) {
        if safe_ts == u64::MAX {
            return;
        }
        let previous = self.scopes.get(scope).copied().unwrap_or(0);
        if safe_ts >= previous {
            self.scopes.insert(scope.to_owned(), safe_ts);
        }
    }

    fn update_scope_from_stores(&mut self, scope: &str, store_ids: &[u64]) {
        let mut minimum = u64::MAX;
        if store_ids.is_empty() {
            minimum = 0;
        }
        for store_id in store_ids {
            match self.stores.get(store_id).copied() {
                None => minimum = 0,
                Some(0) => {}
                Some(safe_ts) => minimum = minimum.min(safe_ts),
            }
        }
        if minimum == u64::MAX {
            minimum = 0;
        }
        self.set_scope(scope, minimum);
    }
}

pub(crate) struct StoreRuntime {
    uuid: String,
    cluster_id: u64,
    pd: Arc<PdRpcClient>,
    safe_point: Arc<TxnSafePointCache>,
    safe_ts: RwLock<SafeTsState>,
    provider: RwLock<Option<Arc<dyn ResolvedTsProvider>>>,
    compatible_loader: AsyncMutex<Option<Arc<EtcdSafePointKv>>>,
    compatible_mode: AtomicBool,
    cancellation: Cancellation,
    tasks: AsyncMutex<Vec<JoinHandle<()>>>,
    closed: AtomicBool,
    endpoints: Vec<String>,
    security: Arc<SecurityManager>,
    safe_point_kv_prefix: String,
    keyspace_meta: Option<keyspacepb::KeyspaceMeta>,
}

impl StoreRuntime {
    pub(crate) async fn new(
        pd: Arc<PdRpcClient>,
        endpoints: Vec<String>,
        config: &Config,
        safe_point_kv_prefix: String,
        keyspace_meta: Option<keyspacepb::KeyspaceMeta>,
    ) -> Result<Arc<Self>> {
        let cluster_id = pd.cluster_id().await;
        let security = Arc::new(
            config
                .security_manager()
                .map_err(|error| Error::StringError(error.to_string()))?,
        );
        let runtime = Arc::new(Self {
            uuid: format!("tikv-{cluster_id}"),
            cluster_id,
            pd,
            safe_point: Arc::new(TxnSafePointCache::new(0, SystemTime::now())),
            safe_ts: RwLock::new(SafeTsState::default()),
            provider: RwLock::new(None),
            compatible_loader: AsyncMutex::new(None),
            compatible_mode: AtomicBool::new(false),
            cancellation: Cancellation::default(),
            tasks: AsyncMutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            endpoints,
            security,
            safe_point_kv_prefix,
            keyspace_meta,
        });
        let safe_point = runtime.load_txn_safe_point().await?;
        runtime.safe_point.update(safe_point, SystemTime::now());
        runtime.start_workers().await;
        Ok(runtime)
    }

    async fn start_workers(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let cancellation = self.cancellation.clone();
        let safe_point_task = tokio::spawn(async move {
            let mut delay = POLL_TXN_SAFE_POINT_INTERVAL;
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                let Some(runtime) = Weak::upgrade(&weak) else {
                    return;
                };
                match runtime.load_txn_safe_point().await {
                    Ok(value) => {
                        runtime.safe_point.update(value, SystemTime::now());
                        delay = POLL_TXN_SAFE_POINT_INTERVAL;
                    }
                    Err(error) => {
                        log::debug!("transaction safe-point refresh failed: {error}");
                        delay = POLL_TXN_SAFE_POINT_QUICK_REPEAT_INTERVAL;
                    }
                }
            }
        });

        let weak = Arc::downgrade(self);
        let cancellation = self.cancellation.clone();
        let safe_ts_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(SAFE_TS_UPDATE_INTERVAL) => {}
                }
                let Some(runtime) = Weak::upgrade(&weak) else {
                    return;
                };
                if let Err(error) = runtime.refresh_safe_ts().await {
                    log::debug!("safe-TS refresh failed: {error}");
                }
            }
        });
        self.tasks
            .lock()
            .await
            .extend([safe_point_task, safe_ts_task]);
    }

    async fn loader(&self) -> Result<Arc<EtcdSafePointKv>> {
        let mut loader = self.compatible_loader.lock().await;
        if let Some(loader) = loader.as_ref() {
            return Ok(loader.clone());
        }
        let created = Arc::new(
            EtcdSafePointKv::new(
                self.endpoints.clone(),
                self.security.clone(),
                self.safe_point_kv_prefix.clone(),
            )
            .await?,
        );
        *loader = Some(created.clone());
        Ok(created)
    }

    fn compatible_safe_point_path(&self) -> String {
        compatible_safe_point_path(self.keyspace_meta.as_ref())
    }

    async fn load_txn_safe_point(&self) -> Result<u64> {
        load_txn_safe_point_compatibly(
            &self.compatible_mode,
            || async { Ok(self.pd.clone().get_gc_state().await?.txn_safe_point) },
            || async {
                let value = self
                    .loader()
                    .await?
                    .get(&self.compatible_safe_point_path())
                    .await?;
                if value.is_empty() {
                    Ok(0)
                } else {
                    value.parse().map_err(|error| {
                        Error::StringError(format!(
                            "invalid transaction safe point {value:?}: {error}"
                        ))
                    })
                }
            },
        )
        .await
    }

    pub(crate) fn safe_point_cache(&self) -> Arc<TxnSafePointCache> {
        self.safe_point.clone()
    }

    pub(crate) fn uuid(&self) -> &str {
        &self.uuid
    }

    pub(crate) fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn min_safe_ts(&self, scope: &str) -> u64 {
        self.safe_ts
            .read()
            .expect("safe-TS lock poisoned")
            .scopes
            .get(scope)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn store_safe_ts(&self, store_id: u64) -> Option<u64> {
        self.safe_ts
            .read()
            .expect("safe-TS lock poisoned")
            .stores
            .get(&store_id)
            .copied()
    }

    pub(crate) fn set_store_safe_ts(&self, store_id: u64, safe_ts: u64) {
        self.safe_ts
            .write()
            .expect("safe-TS lock poisoned")
            .set_store(store_id, safe_ts);
    }

    pub(crate) fn set_provider(&self, provider: Option<Arc<dyn ResolvedTsProvider>>) {
        *self.provider.write().expect("resolved-TS lock poisoned") = provider;
    }

    pub(crate) async fn refresh_safe_ts(&self) -> Result<()> {
        let stores = self.pd.all_stores().await?;
        let provider = self
            .provider
            .read()
            .expect("resolved-TS lock poisoned")
            .clone();
        refresh_safe_ts_state(&self.safe_ts, &stores, provider).await
    }

    pub(crate) async fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.cancellation.cancel();
        for task in self.tasks.lock().await.drain(..) {
            let _ = task.await;
        }
        if let Some(loader) = self.compatible_loader.lock().await.take() {
            let _ = loader.close().await;
        }
    }
}

fn compatible_safe_point_path(meta: Option<&keyspacepb::KeyspaceMeta>) -> String {
    let Some(meta) = meta else {
        return UNIFIED_TXN_SAFE_POINT_PATH.to_owned();
    };
    let keyspace_level = meta
        .config
        .get("gc_management_type")
        .is_some_and(|value| value == "keyspace_level");
    if !keyspace_level {
        return UNIFIED_TXN_SAFE_POINT_PATH.to_owned();
    }
    let id = match meta.keyspace.as_ref() {
        Some(keyspacepb::keyspace_meta::Keyspace::Id(id)) => *id,
        _ => NULL_KEYSPACE_ID,
    };
    KEYSPACE_LEVEL_TXN_SAFE_POINT_PATH.replace("{keyspace_id}", &id.to_string())
}

async fn load_txn_safe_point_compatibly<P, PF, C, CF>(
    compatible_mode: &AtomicBool,
    pd_load: P,
    compatible_load: C,
) -> Result<u64>
where
    P: FnOnce() -> PF,
    PF: Future<Output = Result<u64>>,
    C: FnOnce() -> CF,
    CF: Future<Output = Result<u64>>,
{
    if !compatible_mode.load(Ordering::Acquire) {
        match pd_load().await {
            Ok(safe_point) => {
                crate::stats::increment_load_txn_safe_point("ok");
                return Ok(safe_point);
            }
            Err(error) if is_unimplemented_gc_state(&error) => {
                compatible_mode.store(true, Ordering::Release);
                log::warn!("PD GetGCState is unavailable; falling back to etcd safe point");
            }
            Err(error) => {
                crate::stats::increment_load_txn_safe_point("fail");
                return Err(error);
            }
        }
    }
    match compatible_load().await {
        Ok(safe_point) => {
            crate::stats::increment_load_txn_safe_point("ok_compatible");
            Ok(safe_point)
        }
        Err(error) => {
            crate::stats::increment_load_txn_safe_point("fail_compatible");
            Err(error)
        }
    }
}

fn is_unimplemented_gc_state(error: &Error) -> bool {
    matches!(error, Error::Unimplemented)
        || matches!(error, Error::GrpcAPI(status) if status.code() == tonic::Code::Unimplemented)
}

async fn refresh_safe_ts_state(
    safe_ts: &RwLock<SafeTsState>,
    stores: &[Store],
    provider: Option<Arc<dyn ResolvedTsProvider>>,
) -> Result<()> {
    if let Some(provider) = provider.as_ref() {
        if let Ok((global, _)) = provider.min_resolved_ts(&[]).await {
            if valid_safe_ts(global) {
                let mut state = safe_ts.write().expect("safe-TS lock poisoned");
                let previous = state
                    .scopes
                    .get(oracle::GLOBAL_TXN_SCOPE)
                    .copied()
                    .unwrap_or(0);
                if previous > global {
                    crate::stats::record_safe_ts_update("skip", "cluster", previous);
                } else {
                    state.set_scope_monotonic(oracle::GLOBAL_TXN_SCOPE, global);
                    crate::stats::record_safe_ts_update("success", "cluster", global);
                }
                return Ok(());
            }
        }
    }

    let ids: Vec<_> = stores.iter().map(|store| store.id).collect();
    let pd_values = match provider.as_ref() {
        Some(provider) => provider
            .min_resolved_ts(&ids)
            .await
            .map(|(_, values)| values)
            .unwrap_or_default(),
        None => HashMap::new(),
    };

    let results = futures::future::join_all(stores.iter().map(|store| async {
        let pd_value = pd_values.get(&store.id).copied().unwrap_or(0);
        let value = if valid_safe_ts(pd_value) {
            Ok(pd_value)
        } else {
            request_store_safe_ts(store).await
        };
        (store.id, value)
    }))
    .await;
    {
        let mut state = safe_ts.write().expect("safe-TS lock poisoned");
        for (store_id, value) in results {
            let store = store_id.to_string();
            match value {
                Ok(value) => {
                    let previous = state.stores.get(&store_id).copied().unwrap_or(0);
                    if previous > value {
                        crate::stats::record_safe_ts_update("skip", &store, previous);
                    } else {
                        state.set_store(store_id, value);
                        crate::stats::record_safe_ts_update("success", &store, value);
                    }
                }
                Err(_) => crate::stats::increment_safe_ts_update_failure(&store),
            }
        }
        let mut scopes: HashMap<String, Vec<u64>> = HashMap::new();
        for store in stores {
            scopes
                .entry(oracle::GLOBAL_TXN_SCOPE.to_owned())
                .or_default()
                .push(store.id);
            if let Some(zone) = store.label_value(DC_LABEL_KEY) {
                scopes.entry(zone.to_owned()).or_default().push(store.id);
            }
        }
        for (scope, ids) in scopes {
            state.update_scope_from_stores(&scope, &ids);
        }
    }
    Ok(())
}

impl Drop for StoreRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn request_store_safe_ts(store: &Store) -> Result<u64> {
    let request = crate::transaction::lowering::new_store_safe_ts_request(Some(
        crate::BoundRange::from((Vec::<u8>::new(), Vec::<u8>::new())),
    ));
    let response = store
        .safe_ts_client
        .dispatch_with_timeout(&request, Some(READ_TIMEOUT_SHORT))
        .await?
        .downcast::<kvrpcpb::StoreSafeTsResponse>()
        .map_err(|_| Error::StringError("invalid StoreSafeTS response type".to_owned()))?;
    Ok(response.safe_ts)
}

pub(crate) fn valid_safe_ts(timestamp: u64) -> bool {
    timestamp != 0 && timestamp != u64::MAX
}

#[async_trait]
trait GcController: Send + Sync {
    async fn advance_transaction_safe_point(&self, target: u64) -> Result<u64>;
    async fn resolve_locks_for_gc(&self, safe_point: u64, concurrency: usize) -> Result<()>;
    async fn advance_gc_safe_point(&self, target: u64) -> Result<u64>;
}

async fn run_gc_controller<C: GcController + ?Sized>(
    controller: &C,
    expected_safe_point: u64,
    concurrency: usize,
) -> Result<u64> {
    if concurrency == 0 {
        return Err(Error::StringError(
            "GC concurrency must be greater than zero".to_owned(),
        ));
    }
    let advanced = controller
        .advance_transaction_safe_point(expected_safe_point)
        .await?;
    let transaction_safe_point = expected_safe_point.min(advanced);
    controller
        .resolve_locks_for_gc(transaction_safe_point, concurrency)
        .await?;
    controller
        .advance_gc_safe_point(transaction_safe_point)
        .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SplitRegionMode {
    Legacy,
    ResolveLocks,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SplitRegionOptions {
    scatter: bool,
    mode: SplitRegionMode,
}

const TXN_FILE_SPLIT_OPTIONS: SplitRegionOptions = SplitRegionOptions {
    scatter: false,
    mode: SplitRegionMode::ResolveLocks,
};

enum SplitResponseAction {
    RetryRegion,
    ResolveLocks(Vec<kvrpcpb::LockInfo>),
    Complete(Vec<u64>),
}

fn classify_split_response(
    mut response: kvrpcpb::SplitRegionResponse,
    mode: SplitRegionMode,
) -> Result<SplitResponseAction> {
    if response.region_error.is_some() {
        return Ok(SplitResponseAction::RetryRegion);
    }
    if mode == SplitRegionMode::ResolveLocks && !response.errors.is_empty() {
        let mut locks = Vec::new();
        for key_error in response.errors {
            locks.extend(crate::transaction::extract_locks_from_key_error(
                &key_error,
            )?);
        }
        return Ok(SplitResponseAction::ResolveLocks(locks));
    }
    if !response.regions.is_empty() {
        response.regions.pop();
    }
    Ok(SplitResponseAction::Complete(
        response
            .regions
            .into_iter()
            .map(|region| region.id)
            .collect(),
    ))
}

struct SplitBatchOutcome {
    region_ids: Vec<u64>,
    retry_keys: Vec<Vec<u8>>,
    error: Option<Error>,
}

#[async_trait]
trait ConstructionOwner: Sized {
    async fn close_after_construction_error(self);
}

async fn finish_store_construction<T, R>(owner: T, result: Result<R>) -> Result<(T, R)>
where
    T: ConstructionOwner,
{
    match result {
        Ok(runtime) => Ok((owner, runtime)),
        Err(error) => {
            owner.close_after_construction_error().await;
            Err(error)
        }
    }
}

#[async_trait]
trait ClosingComponent: Sized {
    async fn close_component(self) -> Result<()>;
}

async fn close_store_components<R, C>(runtime: R, client: C) -> Result<()>
where
    R: ClosingComponent,
    C: ClosingComponent,
{
    runtime.close_component().await?;
    client.close_component().await
}

impl SplitBatchOutcome {
    fn error(error: Error) -> Self {
        Self {
            region_ids: Vec::new(),
            retry_keys: Vec::new(),
            error: Some(error),
        }
    }
}

fn retry_error(error: crate::retry::RetryError) -> Error {
    match error {
        crate::retry::RetryError::Interrupted(error) => error.into(),
        crate::retry::RetryError::KillHandler(error) => error,
        crate::retry::RetryError::Exhausted {
            terminal: Some(crate::retry::RetryTerminal::Static(error)),
            ..
        } => error.into(),
        crate::retry::RetryError::Exhausted {
            terminal: Some(crate::retry::RetryTerminal::PdServerTimeout),
            ..
        } => crate::error::PdServerTimeoutError {
            message: String::new(),
        }
        .into(),
        error => Error::StringError(error.to_string()),
    }
}

/// Source-exact root store facade. It embeds the native transaction client
/// while adding the lifecycle state owned by client-go's `tikv.KVStore`.
#[derive(Clone)]
pub struct KvStore {
    inner: TransactionClient,
    runtime: Arc<StoreRuntime>,
}

impl KvStore {
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, Config::default()).await
    }

    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Self> {
        Self::new_with_config_and_safe_point_prefix(pd_endpoints, config, "").await
    }

    /// Constructs a root store with the compatibility etcd namespace selected
    /// by root `txnkv.WithSafePointKVPrefix`.
    pub(crate) async fn new_with_config_and_safe_point_prefix<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
        safe_point_kv_prefix: impl Into<String>,
    ) -> Result<Self> {
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let safe_point_kv_prefix = safe_point_kv_prefix.into();
        let inner =
            TransactionClient::new_with_config(pd_endpoints.clone(), config.clone()).await?;
        let pd = inner.pd_client();
        let (inner, runtime) = finish_store_construction(
            inner,
            StoreRuntime::new(
                pd.clone(),
                pd_endpoints,
                &config,
                safe_point_kv_prefix,
                pd.keyspace_meta().cloned(),
            )
            .await,
        )
        .await?;
        inner.attach_safe_point_cache(runtime.safe_point_cache());
        Ok(Self { inner, runtime })
    }

    pub fn uuid(&self) -> &str {
        self.runtime.uuid()
    }

    pub fn cluster_id(&self) -> u64 {
        self.runtime.cluster_id()
    }

    /// Returns the production PD/routing client owned by this store.
    pub fn pd_client(&self) -> Arc<PdRpcClient> {
        self.inner.pd_client()
    }

    /// Returns the authoritative live region cache used by this store's
    /// request plans, matching client-go's `KVStore.GetRegionCache` surface.
    pub fn region_cache(
        &self,
    ) -> Arc<RegionCache<crate::CodecPdClient<crate::RetryClient<crate::Cluster>>>> {
        self.inner.pd_client().region_cache()
    }

    pub fn is_closed(&self) -> bool {
        self.runtime.is_closed()
    }

    pub fn check_visibility(&self, start_timestamp: u64) -> Result<()> {
        self.runtime
            .safe_point_cache()
            .check_visibility(start_timestamp)
    }

    #[doc(hidden)]
    pub fn update_txn_safe_point_cache(&self, safe_point: u64, now: SystemTime) {
        self.runtime.safe_point_cache().update(safe_point, now);
    }

    pub fn get_min_safe_ts(&self, transaction_scope: &str) -> u64 {
        self.runtime.min_safe_ts(transaction_scope)
    }

    #[doc(hidden)]
    pub fn get_store_safe_ts(&self, store_id: u64) -> Option<u64> {
        self.runtime.store_safe_ts(store_id)
    }

    #[doc(hidden)]
    pub fn set_store_safe_ts(&self, store_id: u64, safe_ts: u64) {
        self.runtime.set_store_safe_ts(store_id, safe_ts);
    }

    #[doc(hidden)]
    pub fn set_resolved_ts_provider(&self, provider: Option<Arc<dyn ResolvedTsProvider>>) {
        self.runtime.set_provider(provider);
    }

    #[doc(hidden)]
    pub async fn refresh_safe_ts(&self) -> Result<()> {
        self.runtime.refresh_safe_ts().await
    }

    pub async fn current_all_tso_keyspace_group_min_timestamp(&self) -> Result<Timestamp> {
        self.inner.pd_client().get_min_timestamp().await
    }

    /// Modern PD GC sequence: advance transaction safe point, resolve locks,
    /// then advance the GC safe point to the actual transaction safe point.
    pub async fn gc(&self, expected_safe_point: u64) -> Result<u64> {
        self.gc_with_concurrency(expected_safe_point, 8).await
    }

    pub async fn gc_with_concurrency(
        &self,
        expected_safe_point: u64,
        concurrency: usize,
    ) -> Result<u64> {
        run_gc_controller(self, expected_safe_point, concurrency).await
    }

    /// Split through TiKV. The tuple preserves client-go's partial region IDs
    /// when a later batch or scatter operation fails.
    pub async fn split_regions(
        &self,
        split_keys: Vec<Vec<u8>>,
        scatter: bool,
        table_id: Option<i64>,
    ) -> (Vec<u64>, Result<()>) {
        self.split_regions_inner(
            split_keys,
            table_id,
            SplitRegionOptions {
                scatter,
                mode: SplitRegionMode::Legacy,
            },
        )
        .await
    }

    /// Transaction-file split mode resolves lock key errors and never scatters.
    pub async fn split_txn_file_regions(&self, split_keys: Vec<Vec<u8>>) -> Result<()> {
        self.split_regions_inner(split_keys, None, TXN_FILE_SPLIT_OPTIONS)
            .await
            .1
    }

    async fn split_regions_inner(
        &self,
        mut pending: Vec<Vec<u8>>,
        table_id: Option<i64>,
        options: SplitRegionOptions,
    ) -> (Vec<u64>, Result<()>) {
        let pd = self.inner.pd_client();
        let mut region_ids = Vec::with_capacity(pending.len());
        let retry_budget = (pending.len() as u64).saturating_mul(20_000).min(120_000);
        let mut retry = Backoffer::new(Cancellation::default(), retry_budget);
        while !pending.is_empty() {
            pending.sort();
            pending.dedup();
            let mut groups = BTreeMap::<u64, (crate::store::RegionStore, Vec<Vec<u8>>)>::new();
            for key in pending.drain(..) {
                let store = match pd.clone().store_for_key(&Key::from(key.clone())).await {
                    Ok(store) => store,
                    Err(error) => return (region_ids, Err(error)),
                };
                if store.region_with_leader.region.start_key == key {
                    continue;
                }
                groups
                    .entry(store.region_with_leader.id())
                    .or_insert_with(|| (store.clone(), Vec::new()))
                    .1
                    .push(key);
            }
            let mut batches = Vec::new();
            for (_, (store, keys)) in groups {
                for batch in keys.chunks(SPLIT_BATCH_REGION_LIMIT) {
                    batches.push((store.clone(), batch.to_vec()));
                }
            }
            let mut outcomes = Vec::with_capacity(batches.len());
            if batches.len() == 1 {
                let (store, batch) = batches.pop().expect("one split batch");
                outcomes.push(
                    self.send_split_batch(store, batch, table_id, options, &mut retry)
                        .await,
                );
            } else {
                let mut futures = futures::stream::FuturesUnordered::new();
                for (store, batch) in batches {
                    let (mut child, _cancel) = retry.fork();
                    futures.push(async move {
                        let outcome = self
                            .send_split_batch(store, batch, table_id, options, &mut child)
                            .await;
                        (outcome, child)
                    });
                }
                let mut last_finished = None;
                while let Some((outcome, child)) = futures.next().await {
                    outcomes.push(outcome);
                    last_finished = Some(child);
                }
                if let Some(last_finished) = last_finished.as_ref() {
                    retry.update_using_forked(last_finished);
                }
            }
            let mut retry_keys = Vec::new();
            let mut first_error = None;
            for outcome in outcomes {
                region_ids.extend(outcome.region_ids);
                retry_keys.extend(outcome.retry_keys);
                if first_error.is_none() {
                    first_error = outcome.error;
                }
            }
            if let Some(error) = first_error {
                return (region_ids, Err(error));
            }
            if retry_keys.is_empty() {
                return (region_ids, Ok(()));
            }
            pending = retry_keys;
        }
        (region_ids, Ok(()))
    }

    async fn send_split_batch(
        &self,
        store: crate::store::RegionStore,
        batch: Vec<Vec<u8>>,
        table_id: Option<i64>,
        options: SplitRegionOptions,
        retry: &mut Backoffer,
    ) -> SplitBatchOutcome {
        let pd = self.inner.pd_client();
        let request = crate::transaction::lowering::new_split_region_request(
            batch.iter().cloned().map(Key::from),
            false,
        );
        let response =
            match crate::request::PlanBuilder::new(pd.clone(), self.inner.keyspace(), request)
                .keyspace_name_option(self.inner.keyspace_name())
                .single_region_with_store(store.clone())
                .await
            {
                Ok(plan) => match plan.plan().execute().await {
                    Ok(response) => response,
                    Err(error) => return SplitBatchOutcome::error(error),
                },
                Err(error) => return SplitBatchOutcome::error(error),
            };
        match classify_split_response(response, options.mode) {
            Ok(SplitResponseAction::RetryRegion) => {
                pd.invalidate_region_cache(store.region_with_leader.ver_id())
                    .await;
                if let Err(error) = retry
                    .backoff(BO_REGION_MISS, "split region returned a region error")
                    .await
                {
                    return SplitBatchOutcome::error(retry_error(error));
                }
                SplitBatchOutcome {
                    region_ids: Vec::new(),
                    retry_keys: batch,
                    error: None,
                }
            }
            Ok(SplitResponseAction::ResolveLocks(locks)) => match self
                .inner
                .resolve_locks(
                    locks,
                    Timestamp::from_version(u64::MAX),
                    Backoff::equal_jitter_backoff(100, 2_000, 60),
                )
                .await
            {
                Ok(live_locks) if live_locks.is_empty() => SplitBatchOutcome {
                    region_ids: Vec::new(),
                    retry_keys: batch,
                    error: None,
                },
                Ok(live_locks) => SplitBatchOutcome::error(Error::ResolveLockError(live_locks)),
                Err(error) => SplitBatchOutcome::error(error),
            },
            Ok(SplitResponseAction::Complete(region_ids)) => {
                let mut error = None;
                if options.scatter {
                    for region_id in &region_ids {
                        if let Err(scatter_error) =
                            self.scatter_region(*region_id, table_id, retry).await
                        {
                            let timed_out = matches!(&scatter_error, Error::PdServerTimeout(_));
                            if error.is_none() {
                                error = Some(scatter_error);
                            }
                            if timed_out {
                                break;
                            }
                        }
                    }
                }
                SplitBatchOutcome {
                    region_ids,
                    retry_keys: Vec::new(),
                    error,
                }
            }
            Err(error) => SplitBatchOutcome::error(error),
        }
    }

    async fn scatter_region(
        &self,
        region_id: u64,
        table_id: Option<i64>,
        retry: &mut Backoffer,
    ) -> Result<()> {
        loop {
            match self
                .inner
                .pd_client()
                .scatter_regions(vec![region_id], table_id.map(|id| id.to_string()))
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) => {
                    retry
                        .backoff(BO_PD_RPC, error.to_string())
                        .await
                        .map_err(retry_error)?;
                }
            }
        }
    }

    pub async fn wait_scatter_region_finish(
        &self,
        region_id: u64,
        backoff_milliseconds: i64,
    ) -> Result<()> {
        let budget = if backoff_milliseconds <= 0 {
            120_000
        } else {
            backoff_milliseconds as u64
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(budget);
        let mut retry = Backoff::equal_jitter_backoff(100, 2_000, u32::MAX);
        loop {
            match self.inner.pd_client().get_operator(region_id).await {
                Ok(response)
                    if response.desc.as_slice() != b"scatter-region"
                        || response.status
                            != crate::proto::pdpb::OperatorStatus::Running as i32 =>
                {
                    return Ok(())
                }
                Ok(_) | Err(_) => {}
            }
            let delay = retry.next_delay_duration().unwrap_or_default();
            if tokio::time::Instant::now() + delay >= deadline {
                return Err(crate::error::PdServerTimeoutError {
                    message: format!("wait scatter region {region_id} timeout"),
                }
                .into());
            }
            tokio::time::sleep(delay).await;
        }
    }

    pub async fn check_region_in_scattering(&self, region_id: u64) -> Result<bool> {
        let mut retry = Backoffer::new(Cancellation::default(), 20_000);
        loop {
            match self.inner.pd_client().get_operator(region_id).await {
                Ok(response) => {
                    return Ok(response.desc.as_slice() == b"scatter-region"
                        && response.status == crate::proto::pdpb::OperatorStatus::Running as i32)
                }
                Err(error) => {
                    if let Err(error) = retry
                        .backoff(BO_REGION_MISS, error.to_string())
                        .await
                        .map_err(retry_error)
                    {
                        // client-go returns `true` with the terminal retry error.
                        // Rust's `Result<bool>` cannot carry a value and error at
                        // once, so preserve the actionable terminal error.
                        return Err(error);
                    }
                }
            }
        }
    }

    pub async fn close(self) -> Result<()> {
        close_store_components(self.runtime, self.inner).await
    }
}

#[async_trait]
impl ConstructionOwner for TransactionClient {
    async fn close_after_construction_error(self) {
        let _ = self.close().await;
    }
}

#[async_trait]
impl ClosingComponent for Arc<StoreRuntime> {
    async fn close_component(self) -> Result<()> {
        self.close().await;
        Ok(())
    }
}

#[async_trait]
impl ClosingComponent for TransactionClient {
    async fn close_component(self) -> Result<()> {
        self.close().await
    }
}

#[async_trait]
impl GcController for KvStore {
    async fn advance_transaction_safe_point(&self, target: u64) -> Result<u64> {
        Ok(self
            .inner
            .pd_client()
            .advance_transaction_safe_point_for_keyspace(NULL_KEYSPACE_ID, target)
            .await?
            .new_txn_safe_point)
    }

    async fn resolve_locks_for_gc(&self, safe_point: u64, concurrency: usize) -> Result<()> {
        self.inner
            .cleanup_locks_with_concurrency(
                Timestamp::from_version(safe_point),
                concurrency,
                GC_SCAN_LOCK_LIMIT,
            )
            .await
    }

    async fn advance_gc_safe_point(&self, target: u64) -> Result<u64> {
        self.inner.pd_client().update_safepoint_value(target).await
    }
}

impl Deref for KvStore {
    type Target = TransactionClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Native root storage contract used by reusable GC/range helpers.
#[async_trait]
pub trait Storage: Send + Sync {
    fn uuid(&self) -> &str;
    fn cluster_id(&self) -> u64;
    fn check_visibility(&self, start_timestamp: u64) -> Result<()>;
    async fn current_timestamp(&self) -> Result<Timestamp>;
}

#[async_trait]
impl Storage for KvStore {
    fn uuid(&self) -> &str {
        self.uuid()
    }

    fn cluster_id(&self) -> u64 {
        self.cluster_id()
    }

    fn check_visibility(&self, start_timestamp: u64) -> Result<()> {
        self.check_visibility(start_timestamp)
    }

    async fn current_timestamp(&self) -> Result<Timestamp> {
        self.inner.current_timestamp().await
    }
}

/// Bounded Tokio-backed pool used by root-store options.
pub struct Spool {
    semaphore: Arc<tokio::sync::Semaphore>,
    delay: Duration,
    closed: AtomicBool,
}

impl Spool {
    pub fn new(concurrency: usize, delay: Duration) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(concurrency.max(1))),
            delay,
            closed: AtomicBool::new(false),
        }
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.semaphore.close();
    }

    /// Submit a synchronous task. The source wrapper always reports success;
    /// closed pools simply decline further work.
    pub fn run(&self, task: crate::async_util::Task) -> Result<()> {
        self.spawn(task);
        Ok(())
    }
}

impl Pool for Spool {
    fn spawn(&self, task: crate::async_util::Task) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let semaphore = self.semaphore.clone();
        let delay = self.delay;
        tokio::spawn(async move {
            let Ok(_permit) = semaphore.acquire_owned().await else {
                return;
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            task();
        });
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::proto::metapb;

    #[derive(Clone)]
    struct SafeTsMockClient {
        value: u64,
        requests: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Client for SafeTsMockClient {
        async fn dispatch(&self, _request: &dyn Request) -> Result<Box<dyn Any>> {
            self.requests.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Box::new(kvrpcpb::StoreSafeTsResponse {
                safe_ts: self.value,
            }))
        }
    }

    struct MockResolvedTsProvider {
        global: u64,
        stores: HashMap<u64, u64>,
    }

    #[async_trait]
    impl ResolvedTsProvider for MockResolvedTsProvider {
        async fn min_resolved_ts(&self, store_ids: &[u64]) -> Result<(u64, HashMap<u64, u64>)> {
            if store_ids.is_empty() {
                Ok((self.global, HashMap::new()))
            } else {
                Ok((u64::MAX, self.stores.clone()))
            }
        }
    }

    struct SafeTsStoreFixture {
        store: Store,
        ordinary_requests: Arc<AtomicUsize>,
        safe_ts_requests: Arc<AtomicUsize>,
    }

    struct MockGcController {
        calls: StdMutex<Vec<String>>,
        transaction_safe_point: u64,
        gc_safe_point: u64,
        fail_resolution: bool,
    }

    struct MockConstructionOwner(Arc<AtomicBool>);

    #[async_trait]
    impl ConstructionOwner for MockConstructionOwner {
        async fn close_after_construction_error(self) {
            self.0.store(true, AtomicOrdering::Release);
        }
    }

    struct MockClosingComponent {
        name: &'static str,
        calls: Arc<StdMutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl ClosingComponent for MockClosingComponent {
        async fn close_component(self) -> Result<()> {
            self.calls.lock().unwrap().push(self.name);
            Ok(())
        }
    }

    #[async_trait]
    impl GcController for MockGcController {
        async fn advance_transaction_safe_point(&self, target: u64) -> Result<u64> {
            self.calls.lock().unwrap().push(format!("txn:{target}"));
            Ok(self.transaction_safe_point)
        }

        async fn resolve_locks_for_gc(&self, safe_point: u64, concurrency: usize) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("resolve:{safe_point}:{concurrency}"));
            if self.fail_resolution {
                Err(Error::StringError("resolve failed".to_owned()))
            } else {
                Ok(())
            }
        }

        async fn advance_gc_safe_point(&self, target: u64) -> Result<u64> {
            self.calls.lock().unwrap().push(format!("gc:{target}"));
            Ok(self.gc_safe_point)
        }
    }

    fn safe_ts_store(
        id: u64,
        zone: &str,
        endpoint_type: crate::store::EndpointType,
        safe_ts: u64,
    ) -> SafeTsStoreFixture {
        let ordinary_requests = Arc::new(AtomicUsize::new(0));
        let safe_ts_requests = Arc::new(AtomicUsize::new(0));
        let address = format!("store-{id}");
        let peer_address = format!("store-{id}-peer");
        let mut labels = vec![metapb::StoreLabel {
            key: DC_LABEL_KEY.to_owned(),
            value: zone.to_owned(),
        }];
        if endpoint_type == crate::store::EndpointType::TiFlash {
            labels.push(metapb::StoreLabel {
                key: "engine".to_owned(),
                value: "tiflash".to_owned(),
            });
        }
        let metadata = metapb::Store {
            id,
            address: address.clone(),
            peer_address,
            labels,
            ..Default::default()
        };
        let ordinary = Arc::new(SafeTsMockClient {
            value: safe_ts.saturating_add(1_000),
            requests: ordinary_requests.clone(),
        });
        let safe_ts_client = Arc::new(SafeTsMockClient {
            value: safe_ts,
            requests: safe_ts_requests.clone(),
        });
        let mut store = Store::new(ordinary)
            .with_target(address)
            .with_metadata(&metadata);
        if endpoint_type == crate::store::EndpointType::TiFlash {
            store = store.with_safe_ts_client(safe_ts_client);
        } else {
            store.client = safe_ts_client.clone();
            store.safe_ts_client = safe_ts_client;
        }
        SafeTsStoreFixture {
            store,
            ordinary_requests,
            safe_ts_requests,
        }
    }

    async fn refresh_fixture(
        fixtures: &[SafeTsStoreFixture],
        provider: Option<MockResolvedTsProvider>,
    ) -> SafeTsState {
        let stores: Vec<_> = fixtures
            .iter()
            .map(|fixture| fixture.store.clone())
            .collect();
        let state = RwLock::new(SafeTsState::default());
        refresh_safe_ts_state(
            &state,
            &stores,
            provider.map(|provider| Arc::new(provider) as Arc<dyn ResolvedTsProvider>),
        )
        .await
        .unwrap();
        state.into_inner().unwrap()
    }

    #[tokio::test]
    async fn mock_safe_point_kv_close_prefix_and_parser_match_source() {
        let kv = MockSafePointKv::new();
        kv.put("/a/1", "11").await.unwrap();
        kv.put("/a/2", "12").await.unwrap();
        kv.put("/b/1", "21").await.unwrap();
        assert_eq!(kv.get("/a/1").await.unwrap(), "11");
        let values = kv.get_with_prefix("/a/").await.unwrap();
        assert_eq!(values.len(), 2);
        kv.close().await.unwrap();
        assert_eq!(kv.get("/a/1").await.unwrap(), "11");
        save_safe_point(&kv, 42).await.unwrap();
        assert_eq!(load_safe_point(&kv).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn transaction_safe_point_loader_switches_only_on_unimplemented() {
        let compatible_mode = AtomicBool::new(false);
        let fallback_calls = AtomicUsize::new(0);
        let value = load_txn_safe_point_compatibly(
            &compatible_mode,
            || async { Ok(42) },
            || async {
                fallback_calls.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(7)
            },
        )
        .await
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(fallback_calls.load(AtomicOrdering::SeqCst), 0);
        assert!(!compatible_mode.load(AtomicOrdering::Acquire));

        let value = load_txn_safe_point_compatibly(
            &compatible_mode,
            || async { Err(Error::GrpcAPI(tonic::Status::unimplemented("old PD"))) },
            || async {
                fallback_calls.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(7)
            },
        )
        .await
        .unwrap();
        assert_eq!(value, 7);
        assert!(compatible_mode.load(AtomicOrdering::Acquire));

        let value = load_txn_safe_point_compatibly(
            &compatible_mode,
            || async { panic!("sticky compatible mode must skip PD") },
            || async {
                fallback_calls.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(8)
            },
        )
        .await
        .unwrap();
        assert_eq!(value, 8);
        assert_eq!(fallback_calls.load(AtomicOrdering::SeqCst), 2);

        let modern_mode = AtomicBool::new(false);
        let result = load_txn_safe_point_compatibly(
            &modern_mode,
            || async { Err(Error::StringError("PD unavailable".to_owned())) },
            || async { panic!("ordinary PD errors must not enter compatibility mode") },
        )
        .await;
        assert!(result.is_err());
        assert!(!modern_mode.load(AtomicOrdering::Acquire));
    }

    #[tokio::test]
    async fn gc_uses_actual_transaction_safe_point_and_source_call_order() {
        let controller = MockGcController {
            calls: StdMutex::new(Vec::new()),
            transaction_safe_point: 80,
            gc_safe_point: 79,
            fail_resolution: false,
        };
        assert_eq!(run_gc_controller(&controller, 100, 3).await.unwrap(), 79);
        assert_eq!(
            *controller.calls.lock().unwrap(),
            ["txn:100", "resolve:80:3", "gc:80"]
        );
    }

    #[tokio::test]
    async fn gc_does_not_advance_gc_safe_point_after_resolution_failure() {
        let controller = MockGcController {
            calls: StdMutex::new(Vec::new()),
            transaction_safe_point: 120,
            gc_safe_point: 0,
            fail_resolution: true,
        };
        assert!(run_gc_controller(&controller, 100, 8).await.is_err());
        assert_eq!(
            *controller.calls.lock().unwrap(),
            ["txn:100", "resolve:100:8"]
        );
        let empty = MockGcController {
            calls: StdMutex::new(Vec::new()),
            transaction_safe_point: 100,
            gc_safe_point: 100,
            fail_resolution: false,
        };
        assert!(run_gc_controller(&empty, 100, 0).await.is_err());
        assert!(empty.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn constructor_failure_closes_the_partial_client_owner() {
        let closed = Arc::new(AtomicBool::new(false));
        let result = finish_store_construction(
            MockConstructionOwner(closed.clone()),
            Err::<(), _>(Error::StringError(
                "injected constructor failure".to_owned(),
            )),
        )
        .await;
        assert!(result.is_err());
        assert!(closed.load(AtomicOrdering::Acquire));
    }

    #[tokio::test]
    async fn close_stops_runtime_workers_before_the_transport_owner() {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        close_store_components(
            MockClosingComponent {
                name: "runtime",
                calls: calls.clone(),
            },
            MockClosingComponent {
                name: "client",
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["runtime", "client"]);
    }

    #[tokio::test]
    async fn spool_runs_submitted_work_and_declines_work_after_close() {
        let pool = Spool::new(1, Duration::ZERO);
        let completed = Arc::new(AtomicUsize::new(0));
        let task_completed = completed.clone();
        pool.run(Box::new(move || {
            task_completed.fetch_add(1, AtomicOrdering::Release);
        }))
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while completed.load(AtomicOrdering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        pool.close();
        let task_completed = completed.clone();
        pool.run(Box::new(move || {
            task_completed.fetch_add(1, AtomicOrdering::Release);
        }))
        .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(completed.load(AtomicOrdering::Acquire), 1);
    }

    #[test]
    fn split_regions_preserves_legacy_key_error_behavior() {
        let response = kvrpcpb::SplitRegionResponse {
            errors: vec![kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: b"k".to_vec(),
                    lock_version: 1,
                    lock_ttl: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            classify_split_response(response, SplitRegionMode::Legacy).unwrap(),
            SplitResponseAction::Complete(region_ids) if region_ids.is_empty()
        ));
    }

    #[test]
    fn split_txn_file_regions_resolves_locks_and_retries() {
        let response = kvrpcpb::SplitRegionResponse {
            errors: vec![kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: b"k".to_vec(),
                    primary_lock: b"k".to_vec(),
                    lock_version: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            classify_split_response(response, SplitRegionMode::ResolveLocks).unwrap(),
            SplitResponseAction::ResolveLocks(locks)
                if locks.len() == 1 && locks[0].key == b"k"
        ));
    }

    #[test]
    fn split_txn_file_regions_never_scatters() {
        assert!(!std::hint::black_box(TXN_FILE_SPLIT_OPTIONS).scatter);
        assert_eq!(TXN_FILE_SPLIT_OPTIONS.mode, SplitRegionMode::ResolveLocks);
    }

    #[test]
    fn split_key_errors_expand_shared_lock_holders() {
        let response = kvrpcpb::SplitRegionResponse {
            errors: vec![kvrpcpb::KeyError {
                locked: Some(kvrpcpb::LockInfo {
                    key: b"wrapper".to_vec(),
                    lock_type: kvrpcpb::Op::SharedLock as i32,
                    shared_lock_infos: vec![
                        kvrpcpb::LockInfo {
                            key: b"k".to_vec(),
                            lock_version: 1,
                            lock_type: kvrpcpb::Op::PessimisticLock as i32,
                            ..Default::default()
                        },
                        kvrpcpb::LockInfo {
                            key: b"k".to_vec(),
                            lock_version: 1,
                            lock_type: kvrpcpb::Op::Lock as i32,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let SplitResponseAction::ResolveLocks(locks) =
            classify_split_response(response, SplitRegionMode::ResolveLocks).unwrap()
        else {
            panic!("shared holders must be resolved");
        };
        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].lock_type, kvrpcpb::Op::PessimisticLock as i32);
        assert_eq!(locks[1].lock_type, kvrpcpb::Op::Lock as i32);
    }

    #[test]
    fn split_response_retries_region_errors_and_keeps_only_new_left_regions() {
        assert!(matches!(
            classify_split_response(
                kvrpcpb::SplitRegionResponse {
                    region_error: Some(Default::default()),
                    ..Default::default()
                },
                SplitRegionMode::Legacy,
            )
            .unwrap(),
            SplitResponseAction::RetryRegion
        ));
        let action = classify_split_response(
            kvrpcpb::SplitRegionResponse {
                regions: vec![
                    metapb::Region {
                        id: 11,
                        ..Default::default()
                    },
                    metapb::Region {
                        id: 12,
                        ..Default::default()
                    },
                    metapb::Region {
                        id: 13,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            SplitRegionMode::Legacy,
        )
        .unwrap();
        assert!(matches!(
            action,
            SplitResponseAction::Complete(region_ids) if region_ids == [11, 12]
        ));
    }

    #[test]
    fn visibility_rejects_stale_cache_and_old_transactions() {
        let now = SystemTime::now();
        let cache = TxnSafePointCache::new(100, now);
        assert!(cache.check_visibility_at(100, now).is_ok());
        assert!(matches!(
            cache.check_visibility_at(99, now),
            Err(Error::TransactionAbortedByGc(_))
        ));
        let stale = now + GC_STATE_CACHE_INTERVAL;
        assert!(matches!(
            cache.check_visibility_at(100, stale),
            Err(Error::PdServerTimeout(_))
        ));
    }

    #[test]
    fn min_safe_ts_ignores_zero_but_not_missing_stores() {
        let mut state = SafeTsState::default();
        state.set_store(1, 100);
        state.set_store(2, 0);
        state.update_scope_from_stores("mixed", &[1, 2]);
        assert_eq!(state.scopes["mixed"], 100);
        state.update_scope_from_stores("missing", &[1, 3]);
        assert_eq!(state.scopes["missing"], 0);
        state.update_scope_from_stores("zeros", &[2]);
        assert_eq!(state.scopes["zeros"], 0);
        state.set_store(1, 90);
        assert_eq!(state.stores[&1], 100, "safe TS is monotonic");
        state.set_store(1, u64::MAX);
        assert_eq!(state.stores[&1], 100);
        state.set_scope("drop", 100);
        state.set_scope("drop", 0);
        assert_eq!(state.scopes["drop"], 0, "store-derived scope can fall");
        state.set_scope_monotonic("pd", 100);
        state.set_scope_monotonic("pd", 90);
        assert_eq!(state.scopes["pd"], 100, "PD global scope is monotonic");
    }

    #[tokio::test]
    async fn min_safe_ts_from_stores() {
        let tikv = safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 100);
        let tiflash = safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 80);
        let fixtures = [tikv, tiflash];
        let state = refresh_fixture(&fixtures, None).await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 80);
        assert_eq!(state.scopes["z1"], 100);
        assert_eq!(state.scopes["z2"], 80);
        assert_eq!(state.stores[&1], 100);
        assert_eq!(state.stores[&2], 80);
        assert_eq!(fixtures[0].safe_ts_requests.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(fixtures[1].safe_ts_requests.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            fixtures[1].ordinary_requests.load(AtomicOrdering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn min_safe_ts_from_stores_with_all_zeros() {
        let fixtures = [
            safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 0),
            safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 0),
        ];
        let state = refresh_fixture(&fixtures, None).await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 0);
    }

    #[tokio::test]
    async fn min_safe_ts_from_stores_with_some_zeros() {
        let fixtures = [
            safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 100),
            safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 0),
        ];
        let state = refresh_fixture(&fixtures, None).await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 100);
    }

    #[tokio::test]
    async fn min_safe_ts_from_pd() {
        let fixtures = [
            safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 100),
            safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 80),
        ];
        let state = refresh_fixture(
            &fixtures,
            Some(MockResolvedTsProvider {
                global: 90,
                stores: HashMap::new(),
            }),
        )
        .await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 90);
        assert_eq!(fixtures[0].safe_ts_requests.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(fixtures[1].safe_ts_requests.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn min_safe_ts_from_pd_by_stores() {
        let fixtures = [
            safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 100),
            safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 80),
        ];
        let state = refresh_fixture(
            &fixtures,
            Some(MockResolvedTsProvider {
                global: u64::MAX,
                stores: HashMap::from([(1, 101), (2, 102)]),
            }),
        )
        .await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 101);
        assert_eq!(fixtures[0].safe_ts_requests.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(fixtures[1].safe_ts_requests.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn min_safe_ts_from_mixed_sources_uses_store_fallback_for_zero() {
        let fixtures = [
            safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 100),
            safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 80),
        ];
        let state = refresh_fixture(
            &fixtures,
            Some(MockResolvedTsProvider {
                global: u64::MAX,
                stores: HashMap::from([(1, 10), (2, 0)]),
            }),
        )
        .await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 10);
        assert_eq!(state.scopes["z1"], 10);
        assert_eq!(state.scopes["z2"], 80);
        assert_eq!(fixtures[0].safe_ts_requests.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(fixtures[1].safe_ts_requests.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn min_safe_ts_from_mixed_sources_uses_store_fallback_for_max() {
        let fixtures = [
            safe_ts_store(1, "z1", crate::store::EndpointType::TiKv, 100),
            safe_ts_store(2, "z2", crate::store::EndpointType::TiFlash, 80),
        ];
        let state = refresh_fixture(
            &fixtures,
            Some(MockResolvedTsProvider {
                global: u64::MAX,
                stores: HashMap::from([(1, u64::MAX), (2, 10)]),
            }),
        )
        .await;
        assert_eq!(state.scopes[oracle::GLOBAL_TXN_SCOPE], 10);
        assert_eq!(state.scopes["z1"], 100);
        assert_eq!(state.scopes["z2"], 10);
        assert_eq!(fixtures[0].safe_ts_requests.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(fixtures[1].safe_ts_requests.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn compatible_keyspace_path_and_prefix_end_are_exact() {
        assert_eq!(prefix_end(b"abc"), b"abd");
        assert_eq!(prefix_end(&[0xff]), vec![0]);
        assert_eq!(UNIFIED_TXN_SAFE_POINT_PATH, GC_SAVED_SAFE_POINT);
        assert_eq!(
            compatible_safe_point_path(None),
            UNIFIED_TXN_SAFE_POINT_PATH
        );
        let unified = keyspacepb::KeyspaceMeta {
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(7)),
            ..Default::default()
        };
        assert_eq!(
            compatible_safe_point_path(Some(&unified)),
            UNIFIED_TXN_SAFE_POINT_PATH
        );
        let mut keyspace_level = unified;
        keyspace_level
            .config
            .insert("gc_management_type".to_owned(), "keyspace_level".to_owned());
        assert_eq!(
            compatible_safe_point_path(Some(&keyspace_level)),
            "/keyspaces/tidb/7/tidb/store/gcworker/saved_safe_point"
        );
    }

    #[test]
    fn pipelined_option_helpers_match_root_source_defaults() {
        assert_eq!(
            default_pipelined_txn_options(),
            PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 128,
                resolve_lock_concurrency: 8,
                write_throttle_ratio: 0.0,
            }
        );
        assert_eq!(
            pipelined_txn_options(17, 5, 0.25),
            PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 17,
                resolve_lock_concurrency: 5,
                write_throttle_ratio: 0.25,
            }
        );
    }
}
