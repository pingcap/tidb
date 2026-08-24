// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use log::info;

use crate::async_util::Cancellation;
use crate::backoff::{DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF};
use crate::config::Config;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::pdpb::Timestamp;
use crate::request::plan::CleanupLocksResult;
use crate::request::Dispatch;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::KvRequest;
use crate::request::NoTarget;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::request::{build_keyspace_name, keyspace_from_pd_meta, Keyspace};
use crate::retry::RetryBackoffer;
use crate::store::region_stream_for_range;
use crate::timestamp::TimestampExt;
use crate::transaction::latch::LatchesScheduler;
use crate::transaction::lock::ResolveLocksOptions;
use crate::transaction::lowering::new_delete_range_request;
use crate::transaction::lowering::new_scan_lock_request;
use crate::transaction::lowering::new_unsafe_destroy_range_request;
use crate::transaction::range_task::{RangeTaskHandler, Runner, TaskStat};
use crate::transaction::resolve_locks;
use crate::transaction::ResolveLocksContext;
use crate::transaction::Snapshot;
use crate::transaction::Transaction;
use crate::transaction::TransactionOptions;
use crate::Backoff;
use crate::BoundRange;
use crate::Result;

/// Protobuf-generated lock information returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::LockInfo as ProtoLockInfo;

// FIXME: cargo-culted value
const SCAN_LOCK_BATCH_SIZE: u32 = 1024;
const DELETE_RANGE_ONE_REGION_MAX_BACKOFF_MS: u64 = 100_000;
const DEFAULT_DELETE_RANGE_CONCURRENCY: usize = 1;

fn delete_range_retry_backoffer(cancellation: Cancellation) -> RetryBackoffer {
    RetryBackoffer::new(cancellation, DELETE_RANGE_ONE_REGION_MAX_BACKOFF_MS)
}

/// The TiKV transactional `Client` is used to interact with TiKV using transactional requests.
///
/// Transactions support optimistic and pessimistic modes. For more details see the SIG-transaction
/// [docs](https://github.com/tikv/sig-transaction/tree/master/doc/tikv#optimistic-and-pessimistic-transactions).
///
/// Begin a [`Transaction`] by calling [`begin_optimistic`](Client::begin_optimistic) or
/// [`begin_pessimistic`](Client::begin_pessimistic). A transaction must be rolled back or committed.
///
/// Besides transactions, the client provides some further functionality:
/// - `gc`: trigger a GC process which clears stale data in the cluster.
/// - `current_timestamp`: get the current `Timestamp` from PD.
/// - `snapshot`: get a [`Snapshot`] of the database at a specified timestamp.
///   A `Snapshot` is a read-only transaction.
///
/// The returned results of transactional requests are [`Future`](std::future::Future)s that must be
/// awaited to execute.
pub struct Client {
    pd: Arc<PdRpcClient>,
    keyspace: Keyspace,
    /// Canonical API V2 keyspace metadata loaded from PD and sent in each
    /// request context, as client-go's codec does.
    keyspace_name: Option<String>,
    latches: Option<Arc<LatchesScheduler>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            pd: self.pd.clone(),
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
            latches: self.latches.clone(),
        }
    }
}

impl Client {
    /// Create a transactional [`Client`] and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// # });
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Client> {
        Self::new_with_config(pd_endpoints, Config::default()).await
    }

    /// Create a transactional [`Client`] with a custom configuration, and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::time::Duration;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new_with_config(
    ///     vec!["192.168.0.100"],
    ///     Config::default().with_timeout(Duration::from_secs(60)),
    /// )
    /// .await
    /// .unwrap();
    /// # });
    /// ```
    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Client> {
        let latches = transaction_latches(&config)?;
        debug!("creating new transactional client");
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let pd = Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), true).await?);
        let (keyspace, keyspace_name) = match config.keyspace {
            Some(name) => {
                let keyspace = pd.load_keyspace(&build_keyspace_name(name)).await?;
                (keyspace_from_pd_meta(&keyspace)?, Some(keyspace.name))
            }
            None => (Keyspace::Disable, None),
        };
        Ok(Client {
            pd,
            keyspace,
            keyspace_name,
            latches,
        })
    }

    /// Create a transactional [`Client`] that uses API V2 without adding or removing any API V2
    /// keyspace/key-mode prefix, with a custom configuration.
    ///
    /// This is intended for **server-side embedding** use cases. `config.keyspace` must be unset.
    pub async fn new_with_config_api_v2_no_prefix<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Client> {
        if config.keyspace.is_some() {
            return Err(crate::Error::StringError(
                "config.keyspace must be unset when using api-v2-no-prefix mode".to_owned(),
            ));
        }
        let latches = transaction_latches(&config)?;

        debug!("creating new transactional client (api-v2-no-prefix)");
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let pd = Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), true).await?);
        Ok(Client {
            pd,
            keyspace: Keyspace::ApiV2NoPrefix,
            keyspace_name: None,
            latches,
        })
    }

    /// Creates a new optimistic [`Transaction`].
    ///
    /// Use the transaction to issue requests like [`get`](Transaction::get) or
    /// [`put`](Transaction::put).
    ///
    /// Write operations do not lock data in TiKV, thus the commit request may fail due to a write
    /// conflict.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client.begin_optimistic().await.unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_optimistic(&self) -> Result<Transaction> {
        let timestamp = self.current_timestamp().await?;
        debug!(
            "began optimistic transaction, start_ts: {}",
            timestamp.version()
        );
        Ok(self.new_transaction(timestamp, TransactionOptions::new_optimistic()))
    }

    /// Creates a new pessimistic [`Transaction`].
    ///
    /// Write operations will lock the data until committed, thus commit requests should not suffer
    /// from write conflicts.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client.begin_pessimistic().await.unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_pessimistic(&self) -> Result<Transaction> {
        let timestamp = self.current_timestamp().await?;
        debug!(
            "began pessimistic transaction, start_ts: {}",
            timestamp.version()
        );
        Ok(self.new_transaction(timestamp, TransactionOptions::new_pessimistic()))
    }

    /// Create a new customized [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient, TransactionOptions};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client
    ///     .begin_with_options(TransactionOptions::default().use_async_commit())
    ///     .await
    ///     .unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_with_options(&self, options: TransactionOptions) -> Result<Transaction> {
        let timestamp = self.current_timestamp().await?;
        debug!("began transaction, start_ts: {}", timestamp.version());
        Ok(self.new_transaction(timestamp, options))
    }

    /// Create a new [`Snapshot`](Snapshot) at the given [`Timestamp`](Timestamp).
    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> Snapshot {
        debug!("creating new snapshot");
        Snapshot::new(self.new_transaction(timestamp, options.read_only()))
    }

    /// Retrieve the current [`Timestamp`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let timestamp = client.current_timestamp().await.unwrap();
    /// # });
    /// ```
    pub async fn current_timestamp(&self) -> Result<Timestamp> {
        self.pd.clone().get_timestamp().await
    }

    /// Request garbage collection (GC) of the TiKV cluster.
    ///
    /// GC deletes MVCC records whose timestamp is lower than the given `safepoint`. We must guarantee
    ///  that all transactions started before this timestamp had committed. We can keep an active
    /// transaction list in application to decide which is the minimal start timestamp of them.
    ///
    /// For each key, the last mutation record (unless it's a deletion) before `safepoint` is retained.
    ///
    /// GC is performed by:
    /// 1. resolving all locks with timestamp <= `safepoint`
    /// 2. updating PD's known safepoint
    ///
    /// This is a simplified version of [GC in TiDB](https://docs.pingcap.com/tidb/stable/garbage-collection-overview).
    /// We skip the second step "delete ranges" which is an optimization for TiDB.
    pub async fn gc(&self, safepoint: Timestamp) -> Result<bool> {
        debug!("invoking transactional gc request");

        let options = ResolveLocksOptions {
            batch_size: SCAN_LOCK_BATCH_SIZE,
            ..Default::default()
        };
        self.cleanup_locks(.., &safepoint, options).await?;

        // update safepoint to PD
        let res: bool = self
            .pd
            .clone()
            .update_safepoint(safepoint.version())
            .await?;
        if !res {
            info!(
                "GC safepoint not updated: PD already holds a safepoint newer than the requested {}",
                safepoint.version()
            );
        }
        Ok(res)
    }

    pub async fn cleanup_locks(
        &self,
        range: impl Into<BoundRange>,
        safepoint: &Timestamp,
        options: ResolveLocksOptions,
    ) -> Result<CleanupLocksResult> {
        debug!("invoking cleanup async commit locks");
        // scan all locks with ts <= safepoint
        let ctx = ResolveLocksContext::default();
        let backoff = Backoff::equal_jitter_backoff(100, 10000, 50);
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_scan_lock_request(range, safepoint, options.batch_size);
        let plan = self
            .plan(req)
            .cleanup_locks(ctx.clone(), options, backoff, self.keyspace)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    // Note: `batch_size` must be >= expected number of locks.
    pub async fn scan_locks(
        &self,
        safepoint: &Timestamp,
        range: impl Into<BoundRange>,
        batch_size: u32,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_scan_lock_request(range, safepoint, batch_size);
        let plan = self
            .plan(req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Resolves the given locks and returns any that remain live.
    ///
    /// This method retries until either all locks are resolved or the provided
    /// `backoff` is exhausted. The `timestamp` is used as the caller start
    /// timestamp when checking transaction status.
    pub async fn resolve_locks(
        &self,
        locks: Vec<ProtoLockInfo>,
        timestamp: Timestamp,
        mut backoff: Backoff,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let mut live_locks = locks;
        loop {
            let resolved_locks = resolve_locks(
                live_locks.encode_keyspace(self.keyspace, KeyMode::Txn),
                timestamp.clone(),
                self.pd.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                None,
            )
            .await?;
            live_locks = resolved_locks.truncate_keyspace(self.keyspace);
            if live_locks.is_empty() {
                return Ok(live_locks);
            }

            match backoff.next_delay_duration() {
                None => return Ok(live_locks),
                Some(delay_duration) => {
                    tokio::time::sleep(delay_duration).await;
                }
            }
        }
    }

    /// Cleans up all keys in a range and quickly reclaim disk space.
    ///
    /// The range can span over multiple regions.
    ///
    /// Note that the request will directly delete data from RocksDB, and all MVCC will be erased.
    ///
    /// This interface is intended for special scenarios that resemble operations like "drop table" or "drop database" in TiDB.
    pub async fn unsafe_destroy_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_unsafe_destroy_range_request(range);
        let plan = self
            .plan(req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Delete every MVCC version in `range` immediately.
    ///
    /// This is the transactional equivalent of client-go's `KVStore.DeleteRange`.
    /// It is destructive, spans regions as needed, and can put substantial load on
    /// TiKV when invoked frequently. The returned count is the number of regions
    /// that completed successfully.
    pub async fn delete_range(&self, range: impl Into<BoundRange>) -> Result<usize> {
        self.delete_range_with_concurrency(range, DEFAULT_DELETE_RANGE_CONCURRENCY)
            .await
    }

    /// Delete every MVCC version in `range` immediately, using at most
    /// `concurrency` source-compatible range tasks at once.
    ///
    /// This corresponds to client-go's explicit `KVStore.DeleteRange`
    /// concurrency argument. Each task processes its assigned regions in
    /// order, and a failure cancels the remaining tasks.
    pub async fn delete_range_with_concurrency(
        &self,
        range: impl Into<BoundRange>,
        concurrency: usize,
    ) -> Result<usize> {
        self.run_delete_range_task(range, concurrency, false).await
    }

    /// Notify every region in `range` about a pending destructive operation
    /// without deleting its data.
    ///
    /// TiKV replicates this request through Raft with `notify_only` set. This
    /// is client-go's `NewNotifyDeleteRangeTask` behavior and is useful before
    /// a subsequent unsafe range-destruction operation.
    pub async fn notify_delete_range_with_concurrency(
        &self,
        range: impl Into<BoundRange>,
        concurrency: usize,
    ) -> Result<usize> {
        self.run_delete_range_task(range, concurrency, true).await
    }

    async fn run_delete_range_task(
        &self,
        range: impl Into<BoundRange>,
        concurrency: usize,
        notify_only: bool,
    ) -> Result<usize> {
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let (start_key, end_key) = range.into_keys();
        let runner = Runner::new(
            if notify_only {
                "delete-range-notify"
            } else {
                "delete-range"
            },
            self.pd.clone(),
            concurrency,
            DeleteRangeHandler {
                client: self.clone(),
                notify_only,
            },
        );
        runner
            .run_on_range(start_key.into(), end_key.unwrap_or_default().into())
            .await?;
        Ok(runner.completed_regions())
    }

    fn new_transaction(&self, timestamp: Timestamp, options: TransactionOptions) -> Transaction {
        Transaction::new_with_latches_and_keyspace_name(
            timestamp,
            self.pd.clone(),
            options,
            self.keyspace,
            self.keyspace_name.clone(),
            self.latches.clone(),
        )
    }

    fn plan<Req: KvRequest>(
        &self,
        request: Req,
    ) -> PlanBuilder<PdRpcClient, Dispatch<Req>, NoTarget> {
        PlanBuilder::new(self.pd.clone(), self.keyspace, request)
            .keyspace_name_option(self.keyspace_name.as_deref())
    }
}

#[derive(Clone)]
struct DeleteRangeHandler {
    client: Client,
    notify_only: bool,
}

#[async_trait]
impl RangeTaskHandler for DeleteRangeHandler {
    async fn handle(
        &self,
        cancellation: Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (TaskStat, Result<()>) {
        let mut stat = TaskStat::default();
        let regions = region_stream_for_range(range, self.client.pd.clone());
        futures::pin_mut!(regions);

        while let Some(region) = regions.next().await {
            if cancellation.is_cancelled() {
                return (
                    stat,
                    Err(crate::Error::StringError("range task cancelled".to_owned())),
                );
            }
            let ((start_key, end_key), _) = match region {
                Ok(region) => region,
                Err(error) => return (stat, Err(error)),
            };
            let range = BoundRange::from((start_key, end_key));
            let mut request = new_delete_range_request(range);
            request.notify_only = self.notify_only;
            match self
                .client
                .plan(request)
                .retry_multi_region_with_retry_backoffer(delete_range_retry_backoffer(
                    cancellation.clone(),
                ))
                .merge(crate::request::Collect)
                .plan()
                .execute()
                .await
            {
                Ok(completed_regions) => stat.completed_regions += completed_regions,
                Err(error) => return (stat, Err(error)),
            }
        }
        (stat, Ok(()))
    }
}

fn transaction_latches(config: &Config) -> Result<Option<Arc<LatchesScheduler>>> {
    let latches = config.txn_local_latches;
    if !latches.enabled {
        return Ok(None);
    }
    if latches.capacity == 0 {
        return Err(crate::Error::StringError(
            "txn-local-latches.capacity can not be 0".to_owned(),
        ));
    }
    Ok(Some(LatchesScheduler::new(latches.capacity)))
}

#[cfg(test)]
mod latch_config_tests {
    use super::*;

    #[test]
    fn transaction_latches_are_disabled_by_default_and_validate_capacity() {
        assert!(transaction_latches(&Config::default()).unwrap().is_none());

        let invalid = Config::default().with_txn_local_latches(0);
        let error = match transaction_latches(&invalid) {
            Ok(_) => panic!("zero latch capacity must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "txn-local-latches.capacity can not be 0");

        let valid = Config::default().with_txn_local_latches(7);
        assert!(transaction_latches(&valid).unwrap().is_some());
    }

    #[test]
    fn delete_range_uses_client_go_per_region_retry_budget() {
        let backoffer = delete_range_retry_backoffer(Cancellation::default());
        assert_eq!(
            backoffer.max_sleep_ms(),
            DELETE_RANGE_ONE_REGION_MAX_BACKOFF_MS
                * (backoffer.variables().backoff_weight.max(1) as u64)
        );
    }
}
