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


//! Go `streamhelper/advancer_env.go`: what the advancer needs of the
//! cluster, and the real cluster's environment TiDB builds (`TiDBEnv`).

use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::logbackup;
use tonic::transport::Channel;

pub use super::advancer_cliext::StreamMeta;
use super::advancer_cliext::AdvancerExt;
pub use super::regioniter::{RegionWithLeader, Store, TiKVClusterMeta};
use super::advancer::ResolveLockPlan;
use crate::errors::{self, Result};
use crate::pdclient::PdClient;
use crate::utils::store_manager::StoreManager;

/// Go `logBackupServiceID`.
pub const LOG_BACKUP_SERVICE_ID: &str = "log-backup-coordinator";
/// Go `logBackupSafePointTTL`.
pub const LOG_BACKUP_SAFE_POINT_TTL: Duration = Duration::from_secs(24 * 3600);
/// Go `dialTimeOut`: a disconnected TiKV may hang the dial rather than
/// refuse it.
const DIAL_TIMEOUT: Duration = Duration::from_secs(8);

/// A flush event stream (Go `LogBackup_SubscribeFlushEventClient`).
pub trait FlushEventStream: Send {
    /// Go `Recv`: the next message, blocking.
    ///
    /// # Errors
    /// The stream ended or failed.
    fn recv(&mut self) -> Result<logbackup::SubscribeFlushEventResponse>;
}

/// Go `logbackup.LogBackupClient`, the two calls the advancer makes.
pub trait LogBackupClient: Send + Sync {
    /// Go `GetLastFlushTSOfRegion`.
    ///
    /// # Errors
    /// The RPC error.
    fn get_last_flush_ts_of_region(
        &self,
        ctx: &Context,
        req: logbackup::GetLastFlushTsOfRegionRequest,
    ) -> Result<logbackup::GetLastFlushTsOfRegionResponse>;
    /// Go `SubscribeFlushEvent`: the stream lives until `ctx` is done.
    ///
    /// # Errors
    /// The RPC error.
    fn subscribe_flush_event(
        &self,
        ctx: &Context,
        req: logbackup::SubscribeFlushEventRequest,
    ) -> Result<Box<dyn FlushEventStream>>;
}

/// Go `LogBackupService`.
pub trait LogBackupService: Send + Sync {
    /// Go `GetLogBackupClient`.
    ///
    /// # Errors
    /// The connection error.
    fn get_log_backup_client(&self, ctx: &Context, store_id: u64) -> Result<Arc<dyn LogBackupClient>>;
    /// Go `ClearCache`: drops the cached connection.
    ///
    /// # Errors
    /// `ctx`'s error.
    fn clear_cache(&self, ctx: &Context, store_id: u64) -> Result<()>;
}

/// Go `tikv.RegionLockResolver` as the advancer drives it: a range-task
/// runner over one target range, each region range resolved by
/// `resolveLocksForRangeWithMaxVersionRetry`.
pub trait LockResolverEnv: Send + Sync {
    /// Go `rangetask.NewRangeTaskRunner("advancer-resolve-locks-runner",
    /// store, 1, handler).RunOnRange(ctx, start, end)`.
    ///
    /// # Errors
    /// The scan or resolve error.
    fn resolve_locks_on_range(&self, ctx: &Context, start_key: &[u8], end_key: &[u8], plan: ResolveLockPlan) -> Result<()>;
}

/// Go `LogBackupFlushIntervalGetter`.
pub trait LogBackupFlushIntervalGetter: Send + Sync {
    /// Go `GetLogBackupFlushInterval`: TiKV's `log-backup.max-flush-interval`.
    ///
    /// # Errors
    /// The configs cannot be read or hold none.
    fn get_log_backup_flush_interval(&self, ctx: &Context) -> Result<Duration>;
}

/// Go `Env`.
pub trait Env: TiKVClusterMeta + LogBackupService + StreamMeta + LockResolverEnv + LogBackupFlushIntervalGetter {}

impl<T: TiKVClusterMeta + LogBackupService + StreamMeta + LockResolverEnv + LogBackupFlushIntervalGetter> Env for T {}

/// The tonic `LogBackup` client over a store connection.
struct GrpcLogBackupClient {
    conn: Channel,
    runtime: Arc<tokio::runtime::Runtime>,
}

/// A tonic server stream, read by a task on the runtime into a channel;
/// dropping it aborts the task, which drops the RPC.
struct GrpcFlushStream {
    rx: crossbeam_channel::Receiver<std::result::Result<Option<logbackup::SubscribeFlushEventResponse>, tonic::Status>>,
    task: tokio::task::JoinHandle<()>,
    ctx: Context,
}

impl Drop for GrpcFlushStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FlushEventStream for GrpcFlushStream {
    fn recv(&mut self) -> Result<logbackup::SubscribeFlushEventResponse> {
        crossbeam_channel::select! {
            recv(self.ctx.done()) -> _ => Err(errors::grpc(tonic::Status::cancelled("context canceled"))),
            recv(self.rx) -> msg => match msg {
                Ok(Ok(Some(msg))) => Ok(msg),
                Ok(Ok(None)) | Err(_) => Err(errors::errorf("EOF")),
                Ok(Err(status)) => Err(errors::grpc(status)),
            },
        }
    }
}

impl LogBackupClient for GrpcLogBackupClient {
    fn get_last_flush_ts_of_region(
        &self,
        ctx: &Context,
        req: logbackup::GetLastFlushTsOfRegionRequest,
    ) -> Result<logbackup::GetLastFlushTsOfRegionResponse> {
        if let Some(err) = ctx.err() {
            return Err(Arc::new(err));
        }
        let mut cli = logbackup::log_backup_client::LogBackupClient::new(self.conn.clone());
        self.runtime
            .block_on(cli.get_last_flush_ts_of_region(req))
            .map(tonic::Response::into_inner)
            .map_err(errors::grpc)
    }

    fn subscribe_flush_event(
        &self,
        ctx: &Context,
        req: logbackup::SubscribeFlushEventRequest,
    ) -> Result<Box<dyn FlushEventStream>> {
        let mut cli = logbackup::log_backup_client::LogBackupClient::new(self.conn.clone());
        let stream = self
            .runtime
            .block_on(cli.subscribe_flush_event(req))
            .map(tonic::Response::into_inner)
            .map_err(errors::grpc)?;
        let (tx, rx) = crossbeam_channel::unbounded();
        let task = self.runtime.spawn(async move {
            let mut stream = stream;
            loop {
                let msg = stream.message().await;
                let end = !matches!(msg, Ok(Some(_)));
                if tx.send(msg).is_err() || end {
                    return;
                }
            }
        });
        Ok(Box::new(GrpcFlushStream { rx, task, ctx: ctx.clone() }))
    }
}

/// Go `fetchTiKVConfigs`: each TiKV store's config JSON handed to the
/// collector.
pub type FetchTikvConfigs = dyn Fn(&Context, &mut dyn FnMut(Vec<u8>) -> Result<()>) -> Result<()> + Send + Sync;

/// Go `clusterEnv`: TiKV through PD, the store connections and the
/// metadata in etcd.
pub struct ClusterEnv {
    clis: StoreManager,
    ext: AdvancerExt,
    pd: Arc<dyn PdClient>,
    resolver: Arc<dyn LockResolverEnv>,
    fetch_tikv_configs: Box<FetchTikvConfigs>,
}

impl ClusterEnv {
    /// Go `TiDBEnv`: the store manager over the node's PD client with the
    /// TiKV keepalive, the dial bounded at 8s.
    #[must_use]
    pub fn tidb_env(
        pd: Arc<dyn PdClient>,
        meta: super::MetaDataClient,
        resolver: Arc<dyn LockResolverEnv>,
        keepalive: crate::utils::store_manager::Keepalive,
        security: tidb_pd_client::ClusterSecurity,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        let mut clis = StoreManager::new(Arc::clone(&pd), keepalive, security.clone(), runtime);
        clis.dial_timeout = DIAL_TIMEOUT;
        let http = crate::conn::new_http_client(&security).ok();
        let prefix = if security.is_tls_enabled() { "https://" } else { "http://" };
        let cfg_pd = Arc::clone(&pd);
        Self {
            clis,
            ext: AdvancerExt { meta },
            pd,
            resolver,
            fetch_tikv_configs: Box::new(move |ctx, collect| {
                let Some(http) = &http else { return Err(errors::errorf("no HTTP client for TiKV configs")) };
                let stores = crate::conn::get_all_tikv_stores(ctx, cfg_pd.as_ref(), crate::conn::StoreBehavior::SkipTiFlash)?;
                crate::conn::get_config_bytes_from_tikv_stores(ctx, &stores, http, prefix, collect)
            }),
        }
    }
}

impl TiKVClusterMeta for ClusterEnv {
    /// Go `PDRegionScanner.RegionScan`.
    fn region_scan(&self, ctx: &Context, key: &[u8], end_key: &[u8], limit: usize) -> Result<Vec<RegionWithLeader>> {
        Ok(self
            .pd
            .scan_regions(ctx, key, end_key, limit)?
            .into_iter()
            .map(|r| RegionWithLeader { region: r.meta, leader: r.leader.unwrap_or_default() })
            .collect())
    }

    /// Go `PDRegionScanner.Stores`: the non-TiFlash stores, tombstones left out.
    fn stores(&self, ctx: &Context) -> Result<Vec<Store>> {
        Ok(self
            .pd
            .get_all_stores(ctx, true)?
            .into_iter()
            .filter(|s| !crate::version::is_tiflash(s))
            .map(|s| Store { id: s.id, boot_at: u64::try_from(s.start_timestamp).unwrap_or(0) })
            .collect())
    }

    /// Go `PDRegionScanner.BlockGCUntil`.
    fn block_gc_until(&self, ctx: &Context, at: u64) -> Result<u64> {
        let minimal = self
            .pd
            .update_service_gc_safe_point(
                ctx,
                LOG_BACKUP_SERVICE_ID,
                i64::try_from(LOG_BACKUP_SAFE_POINT_TTL.as_secs()).unwrap_or(i64::MAX),
                at,
            )
            .map_err(|e| errors::annotate(e, "failed to block gc until"))?;
        if minimal > at {
            return Err(errors::errorf(format!("minimal safe point {minimal} is greater than the target {at}")));
        }
        Ok(at)
    }

    /// Go `PDRegionScanner.UnblockGC`: a zero TTL removes the safe point.
    fn unblock_gc(&self, ctx: &Context) -> Result<()> {
        self.pd.update_service_gc_safe_point(ctx, LOG_BACKUP_SERVICE_ID, 0, u64::MAX).map(|_| ())
    }

    /// Go `PDRegionScanner.FetchCurrentTS`.
    fn fetch_current_ts(&self, ctx: &Context) -> Result<u64> {
        crate::conn::get_current_ts_from_pd_with_retry(ctx, self.pd.as_ref())
    }
}

impl LogBackupService for ClusterEnv {
    fn get_log_backup_client(&self, ctx: &Context, store_id: u64) -> Result<Arc<dyn LogBackupClient>> {
        let runtime = Arc::clone(self.clis.runtime());
        self.clis.with_conn(ctx, store_id, |conn| Arc::new(GrpcLogBackupClient { conn, runtime }) as Arc<dyn LogBackupClient>)
    }

    fn clear_cache(&self, ctx: &Context, store_id: u64) -> Result<()> {
        self.clis.remove_conn(ctx, store_id)
    }
}

impl StreamMeta for ClusterEnv {
    fn begin(&self, ctx: &Context, ch: crossbeam_channel::Sender<super::TaskEvent>) -> Result<()> {
        self.ext.begin(ctx, ch)
    }
    fn upload_v3_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str, checkpoint: u64) -> Result<()> {
        self.ext.upload_v3_global_checkpoint_for_task(ctx, task_name, checkpoint)
    }
    fn get_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str) -> Result<u64> {
        self.ext.get_global_checkpoint_for_task(ctx, task_name)
    }
    fn clear_v3_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str) -> Result<()> {
        self.ext.clear_v3_global_checkpoint_for_task(ctx, task_name)
    }
    fn pause_task(&self, ctx: &Context, task_name: &str, opts: Vec<super::client::PauseTaskOption>) -> Result<()> {
        self.ext.pause_task(ctx, task_name, opts)
    }
}

impl LockResolverEnv for ClusterEnv {
    fn resolve_locks_on_range(&self, ctx: &Context, start_key: &[u8], end_key: &[u8], plan: ResolveLockPlan) -> Result<()> {
        self.resolver.resolve_locks_on_range(ctx, start_key, end_key, plan)
    }
}

/// Go `AdvancerLockResolver`: scans like the GC resolver but resolves with
/// `ResolveLocks(bo, 0, locks)`, which checks each transaction's status
/// and resolves only expired ones, where GC's `BatchResolveLock` resolves
/// every lock.
pub struct AdvancerLockResolver<PdC: tikv_client::PdClient> {
    base: tikv_client::transaction::gc::BaseRegionLockResolver<PdC>,
}

#[async_trait::async_trait]
impl<PdC: tikv_client::PdClient> tikv_client::transaction::gc::RegionLockResolver for AdvancerLockResolver<PdC> {
    fn identifier(&self) -> String {
        self.base.identifier()
    }

    async fn resolve_locks_in_one_region(
        &self,
        backoff: &mut tikv_client::Backoff,
        locks: Vec<tidb_proto::kvrpcpb::LockInfo>,
        location: Option<tikv_client::transaction::gc::KeyLocation>,
    ) -> tikv_client::Result<Option<tikv_client::transaction::gc::KeyLocation>> {
        use tikv_client::TimestampExt as _;
        self.base.store().resolve_locks(locks, tikv_client::Timestamp::from_version(0), backoff.clone()).await?;
        Ok(location)
    }

    async fn scan_locks_in_one_region(
        &self,
        backoff: &mut tikv_client::Backoff,
        key: &[u8],
        end_key: &[u8],
        max_version: u64,
        scan_limit: u32,
    ) -> tikv_client::Result<(Vec<tidb_proto::kvrpcpb::LockInfo>, Option<tikv_client::transaction::gc::KeyLocation>)> {
        self.base.scan_locks_in_one_region(backoff, key, end_key, max_version, scan_limit).await
    }
}

/// Go `resolveLocksForRangeWithMaxVersionRetry` as a range-task handler.
struct AdvancerResolveLocksHandler<PdC: tikv_client::PdClient> {
    resolver: Arc<AdvancerLockResolver<PdC>>,
    plan: ResolveLockPlan,
    keyspace: tikv_client::request::Keyspace,
}

impl<PdC: tikv_client::PdClient> Clone for AdvancerResolveLocksHandler<PdC> {
    fn clone(&self) -> Self {
        Self { resolver: Arc::clone(&self.resolver), plan: self.plan, keyspace: self.keyspace }
    }
}

#[async_trait::async_trait]
#[allow(clippy::result_large_err)]
impl<PdC: tikv_client::PdClient> tikv_client::transaction::range_task::RangeTaskHandler for AdvancerResolveLocksHandler<PdC> {
    async fn handle(
        &self,
        cancellation: tikv_client::async_util::Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (tikv_client::transaction::range_task::TaskStat, tikv_client::Result<()>) {
        // The runner sees physical region bounds; the resolver takes the
        // logical ones (Go's codec PD client decodes them).
        let range = match self.keyspace {
            tikv_client::request::Keyspace::Enable { keyspace_id } => {
                tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, keyspace_id)
                    .and_then(|codec| codec.decode_range(&range.0, &range.1))
            }
            _ => Ok(range),
        };
        let (start, end) = match range {
            Ok(range) => range,
            Err(error) => return (tikv_client::transaction::range_task::TaskStat::default(), Err(error)),
        };
        super::advancer::resolve_locks_for_range_with_max_version_retry(self.plan, |max_version| {
            tikv_client::transaction::gc::resolve_locks_for_range(
                cancellation.clone(),
                self.resolver.as_ref(),
                max_version,
                start.clone(),
                end.clone(),
                &tikv_client::transaction::gc::new_gc_resolve_lock_max_backoff,
                tikv_client::tikv::GC_SCAN_LOCK_LIMIT,
            )
        })
        .await
    }
}

/// The TiKV half of Go `clusterEnv.AdvancerLockResolver`.
pub struct TikvLockResolver<PdC: tikv_client::PdClient> {
    runtime: Arc<tokio::runtime::Runtime>,
    pd: Arc<PdC>,
    resolver: Arc<AdvancerLockResolver<PdC>>,
    keyspace: tikv_client::request::Keyspace,
}

impl<PdC: tikv_client::PdClient> TikvLockResolver<PdC> {
    /// Go `newAdvancerLockResolver(tikvStore)`.
    #[must_use]
    pub fn new(store: &tidb_txnkv::driver::tikv_store::TikvStore<PdC>) -> Self {
        let kv = store.store();
        Self {
            runtime: Arc::clone(store.runtime()),
            pd: kv.pd_client(),
            resolver: Arc::new(AdvancerLockResolver {
                base: tikv_client::transaction::gc::BaseRegionLockResolver::new("log backup advancer", (**kv).clone()),
            }),
            keyspace: kv.keyspace(),
        }
    }
}

impl<PdC: tikv_client::PdClient> LockResolverEnv for TikvLockResolver<PdC> {
    fn resolve_locks_on_range(&self, _ctx: &Context, start_key: &[u8], end_key: &[u8], plan: ResolveLockPlan) -> Result<()> {
        let (start, end) = match self.keyspace {
            tikv_client::request::Keyspace::Enable { keyspace_id } => {
                tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, keyspace_id)
                    .map_err(crate::pdclient::from_client_error)?
                    .encode_range(start_key, end_key)
            }
            _ => (start_key.to_vec(), end_key.to_vec()),
        };
        let handler = AdvancerResolveLocksHandler { resolver: Arc::clone(&self.resolver), plan, keyspace: self.keyspace };
        let runner = tikv_client::transaction::range_task::Runner::new(
            "advancer-resolve-locks-runner",
            Arc::clone(&self.pd),
            1,
            handler,
        );
        self.runtime.block_on(runner.run_on_range(start, end)).map_err(crate::pdclient::from_client_error)
    }
}

impl LogBackupFlushIntervalGetter for ClusterEnv {
    fn get_log_backup_flush_interval(&self, ctx: &Context) -> Result<Duration> {
        get_log_backup_flush_interval_from_tikv_config(ctx, &*self.fetch_tikv_configs)
    }
}

/// Go `GetLogBackupFlushIntervalFromTiKVConfig`: the largest
/// `log-backup.max-flush-interval` of the TiKV stores.
///
/// # Errors
/// A config is unreadable, or no store answered.
pub fn get_log_backup_flush_interval_from_tikv_config(
    ctx: &Context,
    fetch_tikv_configs: &FetchTikvConfigs,
) -> Result<Duration> {
    let mut max = Duration::ZERO;
    let mut min = Duration::ZERO;
    let mut store_count = 0;
    fetch_tikv_configs(ctx, &mut |resp| {
        let interval = parse_log_backup_flush_interval_from_config(&resp).inspect_err(|err| {
            tidb_log::warn(
                "failed to parse log-backup.max-flush-interval from TiKV config",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        })?;
        if store_count == 0 || interval < min {
            min = interval;
        }
        max = max.max(interval);
        store_count += 1;
        Ok(())
    })?;
    if store_count == 0 {
        return Err(errors::errorf("no TiKV config found for log-backup.max-flush-interval"));
    }
    if min != max {
        tidb_log::warn(
            "TiKV log-backup.max-flush-interval is not consistent; use the max value for resolve lock",
            &[
                Field::new("stores", Value::I64(store_count)),
                Field::new("min-flush-interval", Value::Str(format!("{min:?}"))),
                Field::new("max-flush-interval", Value::Str(format!("{max:?}"))),
            ],
        );
    }
    Ok(max)
}

/// Go `parseLogBackupFlushIntervalFromConfig`.
///
/// # Errors
/// Not JSON, or the interval is missing or not positive.
pub fn parse_log_backup_flush_interval_from_config(resp: &[u8]) -> Result<Duration> {
    let v: serde_json::Value = serde_json::from_slice(resp).map_err(|e| errors::errorf(e.to_string()))?;
    let Some(text) = v.get("log-backup").and_then(|l| l.get("max-flush-interval")) else {
        return Err(errors::errorf("log-backup.max-flush-interval is not found in TiKV config"));
    };
    let text = text.as_str().ok_or_else(|| errors::errorf(format!("invalid log-backup.max-flush-interval {text}")))?;
    let nanos = tidb_config_configtypes::parse_duration(text).map_err(errors::errorf)?;
    let d = Duration::from_nanos(u64::try_from(nanos).unwrap_or(0));
    if d.is_zero() {
        return Err(errors::errorf(format!("invalid log-backup.max-flush-interval {text}")));
    }
    Ok(d)
}
