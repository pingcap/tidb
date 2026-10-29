// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TiDB store facade. The transaction protocol and lock lifecycle belong to client-rust.
use super::call_interceptor::CallSlot;
use super::CommitProtocol;
use tidb_txnkv::transaction::*;
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::gc_state::VisibilityError;
use tidb_txnkv::rpc::UnaryCallContext;
use crate::TikvTransactionDriver;
use tidb_txnkv::MemBufferBackend;
use std::sync::Arc;
use std::time::Duration;
use tikv_client::{PdClient, Timestamp, TimestampExt, TransactionOptions};

/// One TiDB transaction (Go `tikvTxn` over `KVTxn` from the node's store):
/// client-rust's transaction on the node's store, with the store's region
/// cache, sender, lock resolver, oracle and txn safe point. Pessimistic mode
/// is an option on it, as Go's `SetOption(kv.Pessimistic)` is
/// (`KVTxn.SetPessimistic`), not another type.
pub struct ClientTransaction<S: PdClient> {
    pub(super) engine: TikvTransactionDriver<S>,
    store: TikvStore<S>,
    /// Go's `ctx` for this transaction's RPCs.
    calls: CallSlot,

    sql_staging: bool,
    check_existence: bool,
    start_ts: u64,
    _start_ts_guard: tidb_txnkv::inner_txn::StartTsGuard,
    authority_id: u64,
    snapshot_stats: Arc<tikv_client::SnapshotRuntimeStats>,
    /// Go `kv.CollectRuntimeStats`: a point reader's own stats, collecting
    /// the snapshot's RPCs instead of [`Self::snapshot_stats`] while set.
    reader_snapshot_stats: Option<Arc<tikv_client::SnapshotRuntimeStats>>,
    read_ts: u64,
    snapshot_read_options: Option<tidb_txnkv::SnapshotReadOptions>,
    collect_snapshot_stats: bool,
    commit_mode: Arc<std::sync::Mutex<Option<CommittedProtocol>>>,
    schema_error: Arc<std::sync::Mutex<Option<tidb_txnkv::transaction::SchemaLeaseError>>>,
    /// Go `KVTxn.isPessimistic`.
    pessimistic: bool,
    /// The statement timestamp locking reads and locks use (Go
    /// `LockCtx.ForUpdateTS`); the start timestamp until a statement
    /// advances it.
    for_update_ts: u64,
    fair_locking: bool,
    max_conflict_ts: u64,
    lock_expired: Arc<std::sync::atomic::AtomicU32>,
    statement_stage: Option<tidb_txnkv::StagingHandle>,
    /// Go's statement ctx on this transaction's calls: the session's current
    /// statement `RUDetails` and resource-group taggers, read when a call is
    /// made. `None` for a transaction no SQL statement owns.
    kv_context: Option<tidb_txnkv::StatementKvContextCell>,
    /// The statement values last installed on the client transaction.
    applied_kv_context: tidb_txnkv::StatementKvContext,
}

/// One point-read result. Physical diagnostics are supplied by the client engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGetResult {
    /// Timestamp of the transaction that owns the snapshot.
    pub start_ts: u64,
    /// Value returned by the union read, or absence.
    pub value: Option<Vec<u8>>,
    /// Number of physical requests issued by this read.
    pub rpc_count: u64,
}
/// The rows served by one snapshot region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotScanRegion {
    /// Region that served these rows.
    pub region: tikv_client::RegionVerId,
    /// Exclusive region boundary observed by the response.
    pub end_key: Vec<u8>,
    /// Rows from this serving region.
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl<S: PdClient> tidb_txnkv::new_txn::TxnResourceGroup for ClientTransaction<S> {
    fn set_resource_group_name(&mut self, name: &str) {
        self.engine.transaction_mut().set_resource_group_name(name);
        self.snapshot_read_options = None;
    }
}
impl<S: PdClient> ClientTransaction<S> {
    /// Go `tikvStore.Begin` at `start_ts`: the store's own transaction, with
    /// its lock resolver context, read validation and txn safe point.
    pub fn new_opened(
        store: &TikvStore<S>,
        start_ts: u64,
        read_only: bool,
        authority_id: u64,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if start_ts == 0 {
            return Err(OptimisticCoordinatorError::Timestamp(
                "a transaction requires a nonzero start timestamp".into(),
            ));
        }
        let calls = CallSlot::default();
        let mut transaction = Self::begin_on(store, start_ts, read_only, &calls)?;
        let snapshot_stats = Arc::new(tikv_client::SnapshotRuntimeStats::default());
        transaction.set_snapshot_runtime_stats(Some(snapshot_stats.clone()));
        let commit_mode = Arc::new(std::sync::Mutex::new(None));
        let observed_mode = commit_mode.clone();
        transaction.set_commit_callback(move |info, _| {
            if let Ok(info) = serde_json::from_str::<serde_json::Value>(&info) {
                *observed_mode.lock().unwrap_or_else(|e| e.into_inner()) =
                    match info["txn_commit_mode"].as_str() {
                        Some("1pc") => Some(CommittedProtocol::OnePc),
                        Some("async_commit") => Some(CommittedProtocol::AsyncCommit),
                        Some("2pc") => Some(CommittedProtocol::TwoPhase),
                        _ => None,
                    };
            }
        });
        let engine = if read_only {
            TikvTransactionDriver::new_read_only(transaction, Arc::clone(store.runtime()))
        } else {
            TikvTransactionDriver::new(transaction, Arc::clone(store.runtime()))
        };
        Ok(Self {
            engine,
            store: store.clone(),
            calls,
            sql_staging: false,
            check_existence: false,
            start_ts,
            _start_ts_guard: tidb_txnkv::inner_txn::ACTIVE_START_TS.hold(start_ts),
            authority_id,
            snapshot_stats,
            reader_snapshot_stats: None,
            read_ts: start_ts,
            snapshot_read_options: None,
            collect_snapshot_stats: false,
            commit_mode,
            schema_error: Default::default(),
            pessimistic: false,
            for_update_ts: start_ts,
            fair_locking: false,
            max_conflict_ts: 0,
            lock_expired: Default::default(),
            statement_stage: None,
            kv_context: None,
            applied_kv_context: Default::default(),
        })
    }

    /// The store's transaction at `start_ts`, its RPCs bound to `calls`.
    /// Transactions start optimistic; [`Self::set_pessimistic`] switches the
    /// same transaction when Go's statement mode needs row locks.
    fn begin_on(
        store: &TikvStore<S>,
        start_ts: u64,
        read_only: bool,
        calls: &CallSlot,
    ) -> Result<tikv_client::Transaction<S>, OptimisticCoordinatorError> {
        let options = TransactionOptions::new_optimistic()
            .drop_check(tikv_client::CheckLevel::Warn)
            .start_timestamp(Timestamp::from_version(start_ts));
        let options = if read_only {
            options.read_only()
        } else {
            options
        };
        let mut transaction = store
            .block_on(store.store().begin_with_options(options))
            .map_err(Self::client_read_error)?;
        transaction.add_rpc_interceptor(calls.interceptor());
        // Go `newTiKVTxn` -> `txn.SetKVFilter(TiDBKVFilter{})`.
        transaction.set_kv_filter(Arc::new(TidbKvFilter));
        Ok(transaction)
    }
}

/// Go `pkg/store/driver/txn.TiDBKVFilter`: an untouched index key/value --
/// an UPDATE inside a transaction rewrites unchanged index entries into its
/// memory buffer with the untouched flag -- is never committed.
struct TidbKvFilter;

impl tikv_client::KvFilter for TidbKvFilter {
    fn is_unnecessary_key_value(
        &self,
        key: &[u8],
        value: &[u8],
        flags: tikv_client::MutationFlags,
    ) -> tikv_client::Result<bool> {
        let untouched = tidb_codec::table_key::is_untouched_index_kv(key, value);
        if untouched && flags.presumes_key_not_exists() {
            return Err(tikv_client::Error::StringError(
                "unexpected path the untouched key value with PresumeKeyNotExists flag".to_owned(),
            ));
        }
        Ok(untouched)
    }
}
impl<S: PdClient> ClientTransaction<S> {
    /// The immutable transaction start timestamp.
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }
    /// Identity of the process storage authority serving this transaction.
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }
    /// Go `txn.SetOption(kv.RPCInterceptor, ...)`, client-go
    /// `KVTxn.AddRPCInterceptor`: `interceptor` wraps every request this
    /// transaction sends on the caller's behalf.
    pub fn add_rpc_interceptor(&mut self, interceptor: tikv_client::RpcInterceptorHandle) {
        self.engine
            .transaction_mut()
            .inner_mut()
            .add_rpc_interceptor(interceptor);
    }
    /// Binds the session's statement values to this transaction's calls: Go
    /// passes each call the statement ctx carrying `util.RUDetailsCtxKey`,
    /// and sets `kv.ResourceGroupTagger` from `StmtCtx.GetResourceGroupTagger`
    /// (`SetOptionsOnTxnActive`, `SetOptionsBeforeCommit`, the executors'
    /// `setOptionForTopSQL`).
    pub fn set_statement_kv_context(&mut self, context: tidb_txnkv::StatementKvContextCell) {
        self.kv_context = Some(context);
    }
    /// Installs the statement values in force on the client transaction
    /// before a call: client-rust charges its `RuDetails` on every RPC (Go's
    /// resource-control interceptor reading the ctx's `RUDetails`), charges
    /// its `ExecDetails` where client-go charges the ctx's
    /// `util.ExecDetailsKey` (the statement ctx Go passes `KVTxn.Get`,
    /// `BatchGet`, `LockKeys`, `Commit`), and tags every request with the
    /// statement's tagger (Go `KVTxn.SetResourceGroupTagger`).
    fn apply_statement_kv_context(&mut self) {
        let Some(cell) = &self.kv_context else {
            return;
        };
        let context = cell.get();
        if let Some(ru_details) = &context.ru_details {
            let applied = self
                .applied_kv_context
                .ru_details
                .as_ref()
                .is_some_and(|applied| Arc::ptr_eq(applied, ru_details));
            if !applied {
                self.engine
                    .transaction_mut()
                    .set_ru_details(Arc::clone(ru_details));
            }
        }
        if let Some(exec_details) = &context.exec_details {
            let applied = self
                .applied_kv_context
                .exec_details
                .as_ref()
                .is_some_and(|applied| Arc::ptr_eq(applied, exec_details));
            if !applied {
                self.engine
                    .transaction_mut()
                    .set_exec_details(Arc::clone(exec_details));
            }
        }
        if let Some(tagger) = &context.resource_group_tagger {
            let applied = self.applied_kv_context.resource_group_tagger.as_ref();
            // Go: a pipelined transaction may already flush in the
            // background, so its tagger is not touched once it is set.
            let pipelined =
                applied.is_some() && self.engine.transaction_mut().inner_mut().is_pipelined();
            if !pipelined && !applied.is_some_and(|applied| Arc::ptr_eq(applied, tagger)) {
                let tagger = Arc::clone(tagger);
                self.engine
                    .transaction_mut()
                    .inner_mut()
                    .set_resource_group_tagger(Some(Arc::new(
                        move |request: &mut dyn tikv_client::tikv::Request| {
                            tagger().build(Some(&mut tidb_txnkv::ClientRequest(request)));
                        },
                    )));
            }
        }
        self.applied_kv_context = context;
    }
    /// Sets the protocols the client may choose for this transaction.
    pub fn set_commit_protocol(&mut self, protocol: CommitProtocol) {
        let transaction = self.engine.transaction_mut().inner_mut();
        transaction.set_enable_async_commit(protocol.async_commit);
        transaction.set_enable_one_pc(protocol.one_pc);
    }
    /// Sets Go's assertion level at transaction activation, before SQL staging.
    pub fn set_assertion_level(&mut self, level: tidb_proto::KvrpcAssertionLevel) {
        self.set_statement_assertion_level(level);
        use tikv_client::proto::kvrpcpb::AssertionLevel;
        let level = match level {
            tidb_proto::KvrpcAssertionLevel::Off => AssertionLevel::Off,
            tidb_proto::KvrpcAssertionLevel::Fast => AssertionLevel::Fast,
            tidb_proto::KvrpcAssertionLevel::Strict => AssertionLevel::Strict,
        };
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_assertion_level(level);
    }
    /// Go `KVTxn.SetKVFilter`: the commit leaves out every buffered pair the
    /// filter calls unnecessary (Go's driver installs `TiDBKVFilter`).
    pub fn set_kv_filter(&mut self, filter: Arc<dyn tikv_client::KvFilter>) {
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_kv_filter(filter);
    }
    /// Go `newLockCtx` requests existence metadata using the current statement's
    /// assertion setting, independently of the commit-time activation snapshot.
    pub fn set_statement_assertion_level(&mut self, level: tidb_proto::KvrpcAssertionLevel) {
        self.check_existence = level != tidb_proto::KvrpcAssertionLevel::Off;
    }
    /// Go `KVSnapshot.SetRuntimeStats` (`kv.CollectRuntimeStats`): a point
    /// reader's own statistics collect the snapshot's RPCs; `None` returns
    /// collection to the transaction's own statistics.
    pub fn set_snapshot_runtime_stats(
        &mut self,
        stats: Option<Arc<tikv_client::SnapshotRuntimeStats>>,
    ) {
        let unchanged = match (&self.reader_snapshot_stats, &stats) {
            (Some(installed), Some(stats)) => Arc::ptr_eq(installed, stats),
            (None, None) => true,
            _ => false,
        };
        if unchanged {
            return;
        }
        self.collect_snapshot_stats = stats.is_some();
        self.reader_snapshot_stats = stats;
        let active = Arc::clone(self.active_snapshot_stats());
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_snapshot_runtime_stats(Some(active));
    }
    /// The statistics the native snapshot collects into now.
    fn active_snapshot_stats(&self) -> &Arc<tikv_client::SnapshotRuntimeStats> {
        self.reader_snapshot_stats
            .as_ref()
            .unwrap_or(&self.snapshot_stats)
    }
    /// Physical Get and BatchGet counts reported by the native snapshot.
    pub fn snapshot_point_rpc_counts(&self) -> (u64, u64) {
        let stats = self.active_snapshot_stats();
        (
            stats.rpc_count(tikv_client::SnapshotRpcCommand::Get),
            stats.rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
        )
    }
    /// Point-read response details when collection is enabled.
    pub fn snapshot_point_response_stats(&self) -> tikv_client::PointResponseStats {
        let mut stats = self.active_snapshot_stats().point_response_stats();
        if !self.collect_snapshot_stats {
            stats.invalidate();
        }
        stats
    }
    fn read_error(error: impl std::fmt::Display) -> OptimisticCoordinatorError {
        OptimisticCoordinatorError::SnapshotGet(error.to_string())
    }
    fn transaction_error(error: crate::TikvTransactionError) -> OptimisticCoordinatorError {
        match error {
            crate::TikvTransactionError::Buffer(tidb_txnkv::TikvMemBufferError::Kv(error)) => {
                OptimisticCoordinatorError::Storage(error)
            }
            crate::TikvTransactionError::Client(
                error @ tikv_client::Error::TransactionAbortedByGc(_),
            ) => Self::client_read_error(error),
            error => Self::read_error(error),
        }
    }
    fn client_read_error(error: tikv_client::Error) -> OptimisticCoordinatorError {
        // client-go `KVSnapshot`'s `CheckVisibility` failure keeps its own
        // identity, so Go's `ToTiDBErr` gives it `ErrTxnAbortedByGC`.
        if let tikv_client::Error::TransactionAbortedByGc(_) = &error {
            return OptimisticCoordinatorError::Visibility(VisibilityError::from_client_error(
                error,
            ));
        }
        match crate::driver::tikv_transaction::classify_client_cause(&error) {
            TransactionCause::BackoffExhausted { kind, detail } => {
                OptimisticCoordinatorError::SnapshotBackoff { kind, detail }
            }
            _ => Self::read_error(error),
        }
    }
    /// Replace the statement's snapshot settings without changing transaction state.
    pub fn set_snapshot_read_options(&mut self, options: &tidb_txnkv::SnapshotReadOptions) {
        if self.snapshot_read_options.as_ref() == Some(options) {
            return;
        }
        if let Some(context) = &options.kv_context {
            self.set_statement_kv_context(context.clone());
        }
        Self::apply_snapshot_read_options(self.engine.transaction_mut().inner_mut(), options);
        self.set_snapshot_runtime_stats(
            options
                .runtime_stats
                .as_ref()
                .map(|stats| Arc::clone(&stats.0)),
        );
        self.snapshot_read_options = Some(options.clone());
    }

    fn apply_snapshot_read_options(
        transaction: &mut tikv_client::Transaction<S>,
        options: &tidb_txnkv::SnapshotReadOptions,
    ) {
        use tikv_client::{ReplicaReadAdjustment, ReplicaReadSelectorOption, ReplicaReadType};
        let mode = match options.replica_read.raw() {
            1 => ReplicaReadType::Follower,
            2..=4 => ReplicaReadType::Mixed,
            5 => ReplicaReadType::Learner,
            6 => ReplicaReadType::PreferLeader,
            _ => ReplicaReadType::Leader,
        };
        // set_replica_read replaces the entire native replica configuration,
        // clearing the previous statement's labels and stale flag first.
        transaction.set_replica_read(mode);
        transaction.set_stale_read(options.is_staleness);
        let scope = if options.read_replica_scope.is_empty() {
            "global"
        } else {
            &options.read_replica_scope
        };
        transaction.set_read_replica_scope(scope);
        if options.replica_read.is_closest_read() && scope != "global" {
            transaction.set_match_store_labels(vec![tikv_client::proto::metapb::StoreLabel {
                key: "zone".into(),
                value: scope.into(),
            }]);
        }
        transaction.set_snapshot_read_timeout(Duration::from_millis(options.read_timeout_ms));
        transaction.set_snapshot_resource_group_name(options.resource_group_name.clone());
        transaction.set_load_based_replica_read_threshold(Duration::from_nanos(
            options.busy_threshold_ns.max(0) as u64,
        ));
        let adaptive = options.adaptive.clone();
        // Always replace the callback: an explicit transaction can change modes
        // between statements, and the prior adaptive closure must not survive.
        transaction.set_replica_read_adjuster(Arc::new(move |count| {
            let Some((avg, threshold, zone)) = &adaptive else {
                return ReplicaReadAdjustment::new(None, mode);
            };
            let bytes = avg * count as f64;
            let estimate = if !bytes.is_finite() || bytes >= 9_223_372_036_854_775_808.0 {
                i64::MIN
            } else {
                bytes as i64
            };
            if estimate >= *threshold {
                ReplicaReadAdjustment::new(
                    Some(ReplicaReadSelectorOption::MatchLabels(vec![
                        tikv_client::proto::metapb::StoreLabel {
                            key: "zone".into(),
                            value: zone.clone(),
                        },
                    ])),
                    ReplicaReadType::Mixed,
                )
            } else {
                ReplicaReadAdjustment::new(None, ReplicaReadType::Leader)
            }
        }));
    }

    fn prepare_read(&mut self, read_ts: u64, call: &UnaryCallContext) {
        let call = call.clone().with_kv_exec_counter(
            self.snapshot_read_options
                .as_ref()
                .map(|o| o.kv_exec_counter.clone()),
        );
        self.calls.set(&call);
        self.apply_statement_kv_context();
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_enable_async_batch_get(
                tidb_config::config_tree::config::get_global_config()
                    .performance
                    .enable_async_batch_get,
            );
        if self.read_ts != read_ts {
            self.engine
                .transaction_mut()
                .inner_mut()
                .set_snapshot_timestamp(Timestamp::from_version(read_ts));
            self.read_ts = read_ts;
        }
    }
    /// Reads through the native union store at the transaction start timestamp.
    pub fn snapshot_get(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<SnapshotGetResult, OptimisticCoordinatorError> {
        self.snapshot_get_at(key, self.start_ts, call)
    }
    /// Reads at a statement timestamp, invalidating cached reads when it changes.
    pub fn snapshot_get_at(
        &mut self,
        key: &[u8],
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<SnapshotGetResult, OptimisticCoordinatorError> {
        self.prepare_read(read_ts, call);
        let before = self.snapshot_point_rpc_counts().0;
        let value = self
            .engine
            .run(|transaction| transaction.get(key.to_vec()))
            .map_err(Self::client_read_error)?;
        // The store checks the read against its txn safe point after it
        // returns (client-go `KVSnapshot.Get`'s `CheckVisibility`).
        Ok(SnapshotGetResult {
            start_ts: self.start_ts,
            value,
            rpc_count: self.snapshot_point_rpc_counts().0.saturating_sub(before),
        })
    }
    /// Reads keys through the native union store at the start timestamp.
    pub fn snapshot_batch_get(
        &mut self,
        keys: &[Vec<u8>],
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        self.snapshot_batch_get_at(keys, self.start_ts, call)
    }
    /// Batch reads keys at the current statement timestamp.
    pub fn snapshot_batch_get_at(
        &mut self,
        keys: &[Vec<u8>],
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        self.prepare_read(read_ts, call);
        let values = self
            .engine
            .run(|transaction| transaction.batch_get_with_options(keys.iter().cloned(), &[]))
            .map_err(Self::client_read_error)?
            .into_iter()
            .map(|(key, entry)| (Vec::from(key), entry.value))
            .collect();
        Ok(values)
    }
    /// Scans the transaction view in key order.
    pub fn snapshot_scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        self.snapshot_scan_at(start, end, limit, self.start_ts, call)
    }
    /// Scans at a statement timestamp with cancellation shared across pages.
    pub fn snapshot_scan_at(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        // Go Scanner bounds each page RPC independently while retaining the
        // caller's cancellation across the scan.
        let scan_call = UnaryCallContext::with_optional_deadline(None, call.cancellation().clone());
        self.prepare_read(read_ts, &scan_call);
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let range = (
            Included(start.to_vec()),
            if end.is_empty() {
                Unbounded
            } else {
                Excluded(end.to_vec())
            },
        );
        let values = self
            .engine
            .run(|transaction| {
                transaction.scan(
                    range,
                    limit.unwrap_or(u32::MAX as usize).min(u32::MAX as usize) as u32,
                )
            })
            .map_err(Self::client_read_error)?
            .map(|pair| (Vec::from(pair.0), pair.1))
            .collect();
        Ok(values)
    }
    /// Go `KVTxn.GetSnapshot()`: a read-only native transaction over the same
    /// client at `read_ts`, which has none of this transaction's staged
    /// writes (the native `Transaction::get`/`scan`/`batch_get` always merge
    /// their buffer). It shares the transaction's options and ctx.
    fn committed_snapshot_driver(
        &mut self,
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<TikvTransactionDriver<S>, OptimisticCoordinatorError> {
        let scan_call = UnaryCallContext::with_optional_deadline(None, call.cancellation().clone())
            .with_kv_exec_counter(
                self.snapshot_read_options
                    .as_ref()
                    .map(|o| o.kv_exec_counter.clone()),
            );
        self.calls.set(&scan_call);
        self.apply_statement_kv_context();
        let mut transaction = Self::begin_on(&self.store, read_ts, true, &self.calls)?;
        if let Some(options) = &self.snapshot_read_options {
            Self::apply_snapshot_read_options(&mut transaction, options);
        }
        // Go `KVTxn.GetSnapshot` shares the transaction's options and ctx.
        if let Some(ru_details) = &self.applied_kv_context.ru_details {
            transaction.set_ru_details(Arc::clone(ru_details));
        }
        if let Some(exec_details) = &self.applied_kv_context.exec_details {
            transaction.set_exec_details(Arc::clone(exec_details));
        }
        if let Some(tagger) = &self.applied_kv_context.resource_group_tagger {
            let tagger = Arc::clone(tagger);
            transaction.set_resource_group_tagger(Some(Arc::new(
                move |request: &mut dyn tikv_client::tikv::Request| {
                    tagger().build(Some(&mut tidb_txnkv::ClientRequest(request)));
                },
            )));
        }
        Ok(TikvTransactionDriver::new_read_only(
            transaction,
            Arc::clone(self.store.runtime()),
        ))
    }

    /// Go `KVTxn.GetSnapshot().Iter`: scans the committed snapshot at
    /// `read_ts` WITHOUT this transaction's staged writes.
    pub fn snapshot_scan_committed_at(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        let mut committed = self.committed_snapshot_driver(read_ts, call)?;
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let range = (
            Included(start.to_vec()),
            if end.is_empty() {
                Unbounded
            } else {
                Excluded(end.to_vec())
            },
        );
        let values = committed
            .run(|transaction| {
                transaction.scan(
                    range,
                    limit.unwrap_or(u32::MAX as usize).min(u32::MAX as usize) as u32,
                )
            })
            .map_err(Self::client_read_error)?
            .map(|pair| (Vec::from(pair.0), pair.1))
            .collect();
        Ok(values)
    }

    /// Go `KVTxn.GetSnapshot().Get`: one key of the committed snapshot at
    /// `read_ts`, WITHOUT this transaction's staged writes.
    pub fn snapshot_get_committed_at(
        &mut self,
        key: &[u8],
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, OptimisticCoordinatorError> {
        let mut committed = self.committed_snapshot_driver(read_ts, call)?;
        committed
            .run(|transaction| transaction.get(key.to_vec()))
            .map_err(Self::client_read_error)
    }

    /// Go `KVTxn.GetSnapshot().BatchGet`: the keys of the committed snapshot
    /// at `read_ts`, WITHOUT this transaction's staged writes.
    pub fn snapshot_batch_get_committed_at(
        &mut self,
        keys: &[Vec<u8>],
        read_ts: u64,
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError> {
        let mut committed = self.committed_snapshot_driver(read_ts, call)?;
        let values = committed
            .run(|transaction| transaction.batch_get_with_options(keys.iter().cloned(), &[]))
            .map_err(Self::client_read_error)?
            .into_iter()
            .map(|(key, entry)| (Vec::from(key), entry.value))
            .collect();
        Ok(values)
    }
    /// Attaches the SQL schema validity check to the native commit lifecycle.
    pub fn set_schema_lease(&mut self, lease: SchemaLease) {
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_schema_version(Arc::new(DriverSchemaVersion));
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_schema_lease_checker(Arc::new(DriverSchemaChecker {
                lease,
                error: self.schema_error.clone(),
            }));
    }
    /// TiDB's SQL staging handle borrows the exact buffer committed by the client.
    pub fn mem_buffer(&mut self) -> &mut tikv_client::transaction::unionstore::MemDb {
        self.sql_staging = true;
        self.engine.transaction_mut().get_mem_buffer()
    }
    /// Copies staged values and tombstones for SQL planning and diagnostics.
    pub fn staged_entries(&self) -> Vec<(tidb_txnkv::Key, Vec<u8>)> {
        self.engine.staged_entries()
    }
    /// Returns a staged value, including an empty deletion tombstone.
    pub fn staged_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.engine.staged_value(&tidb_txnkv::Key::from(key.to_vec()))
    }
    /// Applies a statement plan atomically to the native staging buffer.
    pub fn stage_mutations(
        &mut self,
        mutations: Vec<tidb_txnkv::transaction::BufferMutation>,
    ) -> Result<(), OptimisticCoordinatorError> {
        let stage = self.engine.staging();
        for mutation in mutations {
            if let Err(error) = self.engine.stage_mutation(&mutation) {
                self.engine.cleanup(stage);
                return Err(Self::transaction_error(error));
            }
        }
        self.engine.release(stage);
        Ok(())
    }
    /// Commits the authoritative buffer through the client protocol engine.
    pub fn commit(
        mut self,
        mutations: Vec<tidb_txnkv::transaction::BufferMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        self.finish_statement(true).map_err(Self::read_error)?;
        self.stage_mutations(mutations)?;
        self.calls.set(call);
        self.apply_statement_kv_context();
        let mut outcome = self
            .engine
            .commit_staged()
            .map_err(Self::transaction_error)?;
        let receipt = match &mut outcome {
            OptimisticCommitOutcome::Committed(value) => &mut value.receipt,
            OptimisticCommitOutcome::RolledBack(value) => {
                if let Some(error) = self
                    .schema_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                {
                    value.cause = TransactionCause::SchemaLease { error: error.error };
                }
                &mut value.receipt
            }
            OptimisticCommitOutcome::Undetermined(value) => &mut value.receipt,
            OptimisticCommitOutcome::CleanupFailed(value) => &mut value.receipt,
        };
        receipt.authority_id = self.authority_id;
        if let Some(mode) = *self.commit_mode.lock().unwrap_or_else(|e| e.into_inner()) {
            receipt.commit_protocol = mode;
        }
        Ok(outcome)
    }
    /// Ends the transaction and releases client-owned locks without publishing writes.
    pub fn finish_without_writes(
        mut self,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError> {
        self.apply_statement_kv_context();
        self.engine
            .finish_without_writes()
            .map_err(Self::transaction_error)?;
        Ok(ReadOnlyTransaction {
            authority_id: self.authority_id,
            start_ts: self.start_ts,
            state: OptimisticTransactionState::ReadOnly,
        })
    }
}
impl<S: PdClient> ClientTransaction<S> {
    /// Groups rows by the regions that actually served the native scan.
    pub fn snapshot_scan_regions(
        &mut self,
        start: &[u8],
        end: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Vec<SnapshotScanRegion>, OptimisticCoordinatorError> {
        self.calls.capture_scan_pages(true);
        let result = self.snapshot_scan(start, end, None, call);
        let pages = self.calls.capture_scan_pages(false);
        let rows = result?;
        // A region split or lock retry can replace a page. Associate each
        // final row with a successful response that actually supplied it.
        let mut served = std::collections::BTreeMap::new();
        for page in pages {
            for (key, value) in page.pairs {
                served.insert(key, (value, page.region.clone(), page.end_key.clone()));
            }
        }
        let mut regions: Vec<SnapshotScanRegion> = Vec::new();
        for pair in rows {
            let (value, region, end_key) = served
                .remove(&pair.0)
                .ok_or_else(|| Self::read_error("scan row has no serving response"))?;
            if value != pair.1 {
                return Err(Self::read_error("scan row disagrees with serving response"));
            }
            if let Some(last) = regions.last_mut().filter(|last| last.region == region) {
                last.pairs.push(pair);
            } else {
                regions.push(SnapshotScanRegion {
                    region,
                    end_key,
                    pairs: vec![pair],
                });
            }
        }
        Ok(regions)
    }
}
struct DriverSchemaVersion;
impl tikv_client::transaction::SchemaVersion for DriverSchemaVersion {
    fn schema_meta_version(&self) -> i64 {
        0
    }
}
struct DriverSchemaChecker {
    lease: SchemaLease,
    error: Arc<std::sync::Mutex<Option<tidb_txnkv::transaction::SchemaLeaseError>>>,
}
impl tikv_client::transaction::SchemaLeaseChecker for DriverSchemaChecker {
    fn check_by_schema_version(
        &self,
        timestamp: u64,
        _version: &dyn tikv_client::transaction::SchemaVersion,
    ) -> tikv_client::Result<tikv_client::transaction::RelatedSchemaChange> {
        self.lease.check(timestamp).map_err(|error| {
            let message = error.error.message().to_owned();
            *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
            tikv_client::Error::StringError(message)
        })?;
        Ok(tikv_client::transaction::RelatedSchemaChange {
            physical_table_ids: Vec::new(),
            action_types: Vec::new(),
            latest_info_schema: Arc::new(DriverSchemaVersion),
        })
    }
}

impl<S: PdClient> ClientTransaction<S> {
    /// Go `KVTxn.SetPessimistic` (`SetOption(kv.Pessimistic)`): locks taken
    /// from now on are pessimistic locks, and commit prewrites them as such.
    pub fn set_pessimistic(&mut self, pessimistic: bool) {
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_pessimistic(pessimistic);
        self.pessimistic = pessimistic;
    }
    /// Go `KVTxn.IsPessimistic`.
    pub fn is_pessimistic(&self) -> bool {
        self.pessimistic
    }
    /// The statement timestamp used by subsequent locking reads.
    pub fn for_update_ts(&self) -> u64 {
        self.for_update_ts
    }
    /// Selects client aggressive locking for subsequent statement scopes.
    pub fn set_fair_locking(&mut self, enabled: bool) {
        self.fair_locking = enabled;
    }
    /// Whether fair locking is enabled for this transaction.
    pub fn is_in_fair_locking_mode(&self) -> bool {
        self.fair_locking
    }
    /// Whether the native heartbeat marked the transaction lock expired.
    pub fn lock_expired(&self) -> bool {
        self.lock_expired.load(std::sync::atomic::Ordering::Acquire) != 0
    }
    /// Greatest conflict timestamp returned by fair locking.
    pub fn max_locked_with_conflict_ts(&self) -> u64 {
        self.max_conflict_ts
    }
    /// Returns the native buffer keys carrying acquired-lock flags.
    pub fn locked_keys(&mut self) -> Vec<Vec<u8>> {
        self.engine
            .locked_keys()
            .into_iter()
            .map(|k| k.as_bytes().to_vec())
            .collect()
    }
    /// Reads at the current pessimistic statement timestamp.
    pub fn for_update_get(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, OptimisticCoordinatorError> {
        self.snapshot_get_at(key, self.for_update_ts, call)
            .map(|r| r.value)
    }
    /// Opens the client statement scope, reusing the SQL staging owner when bound.
    pub fn start_statement(&mut self) {
        if !self.sql_staging && self.statement_stage.is_none() {
            self.statement_stage = Some(self.engine.staging());
        }
        if self.fair_locking && !self.engine.is_statement_locking() {
            self.engine.start_statement_locking();
        }
    }
    /// Completes or cancels the native statement scope while retaining ordinary locks.
    pub fn finish_statement(
        &mut self,
        successful: bool,
    ) -> Result<(), tidb_txnkv::transaction::PessimisticLockFailure> {
        if self.engine.is_statement_locking() {
            if successful {
                self.engine.done_statement_locking()
            } else {
                self.engine.cancel_statement_locking()
            }
            .map_err(lock_error)?;
        }
        if let Some(stage) = self.statement_stage.take() {
            if successful {
                self.engine.release(stage);
            } else {
                self.engine.cleanup(stage);
            }
        }
        Ok(())
    }
    /// Allocates a fresh statement timestamp and forwards the native fair retry hook.
    pub fn advance_for_update_ts(&mut self) -> Result<u64, tidb_txnkv::transaction::PessimisticLockFailure> {
        let store = &self.store;
        let timestamp = store
            .block_on(store.store().current_timestamp())
            .map_err(|error| {
                tidb_txnkv::transaction::PessimisticLockFailure::Transaction(TransactionCause::Timestamp {
                    detail: error.to_string(),
                })
            })?
            .version();
        if timestamp <= self.for_update_ts {
            return Err(tidb_txnkv::transaction::PessimisticLockFailure::Transaction(
                TransactionCause::Timestamp {
                    detail: "for_update_ts did not advance".to_owned(),
                },
            ));
        }
        if self.engine.is_statement_locking() {
            self.engine.retry_statement_locking().map_err(lock_error)?;
        }
        if let Some(stage) = self.statement_stage.take() {
            self.engine.cleanup(stage);
            self.statement_stage = Some(self.engine.staging());
        }
        self.for_update_ts = timestamp;
        Ok(timestamp)
    }
    /// Forwards statement locking and absence checks to the native client.
    pub fn acquire_locks(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &std::collections::BTreeSet<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        call: &UnaryCallContext,
    ) -> Result<tidb_txnkv::transaction::AcquiredLocks, tidb_txnkv::transaction::PessimisticLockFailure> {
        self.acquire(keys, presume_not_exists, wait, call, false, false)
    }
    /// Locks keys and returns values from the statement lock context.
    pub fn acquire_locks_returning_values(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &std::collections::BTreeSet<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        call: &UnaryCallContext,
    ) -> Result<tidb_txnkv::transaction::AcquiredLocks, tidb_txnkv::transaction::PessimisticLockFailure> {
        self.acquire(keys, presume_not_exists, wait, call, true, false)
    }
    /// Go `LockCtx.LockOnlyIfExists` with `InitReturnValues`: locks only the
    /// keys that exist, returning every key's value (read committed's
    /// `lockKeyIfExists`).
    pub fn acquire_locks_returning_values_if_exist(
        &mut self,
        keys: &[Vec<u8>],
        wait: tidb_txnkv::transaction::LockWaitTime,
        call: &UnaryCallContext,
    ) -> Result<tidb_txnkv::transaction::AcquiredLocks, tidb_txnkv::transaction::PessimisticLockFailure> {
        self.acquire(
            keys,
            &std::collections::BTreeSet::new(),
            wait,
            call,
            true,
            true,
        )
    }
    fn acquire(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &std::collections::BTreeSet<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        call: &UnaryCallContext,
        return_values: bool,
        lock_only_if_exists: bool,
    ) -> Result<tidb_txnkv::transaction::AcquiredLocks, tidb_txnkv::transaction::PessimisticLockFailure> {
        self.calls.set(call);
        self.apply_statement_kv_context();
        self.start_statement();
        let before: std::collections::BTreeSet<_> = self.locked_keys().into_iter().collect();
        for key in presume_not_exists {
            self.engine.mem_buffer().backend_mut().update_flags(
                &tidb_txnkv::Key::from(key.clone()),
                &[tidb_txnkv::FlagsOp::SetPresumeKeyNotExists],
            );
        }
        let wait_ms = match wait {
            tidb_txnkv::transaction::LockWaitTime::NoWait => -1,
            tidb_txnkv::transaction::LockWaitTime::AlwaysWait => i64::MAX,
            tidb_txnkv::transaction::LockWaitTime::Timeout(timeout) => {
                i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX)
            }
        };
        let mut context = tikv_client::kv::LockContext::new(
            self.for_update_ts,
            wait_ms,
            std::time::SystemTime::now(),
        );
        context.lock_expired = Some(self.lock_expired.clone());
        // Go `newLockCtx`'s `lockCtx.ResourceGroupTagger`: the first
        // mutation's key; no mutation answers no tag.
        if let Some(tagger) = &self
            .applied_kv_context
            .pessimistic_lock_resource_group_tagger
        {
            let tagger = Arc::clone(tagger);
            context.resource_group_tagger = Some(Arc::new(
                move |request: &tikv_client::proto::kvrpcpb::PessimisticLockRequest| {
                    request
                        .mutations
                        .first()
                        .map_or_else(Vec::new, |mutation| tagger(&mutation.key))
                },
            ));
        }
        if return_values {
            context.init_return_values(keys.len());
        }
        context.lock_only_if_exists = lock_only_if_exists;
        if self.for_update_ts > 0 && self.check_existence {
            context.init_check_existence(keys.len());
        }
        let newly_locked = keys
            .iter()
            .filter(|key| {
                !before.contains(*key)
                    && !self
                        .engine
                        .transaction_mut()
                        .inner_mut()
                        .is_in_aggressive_locking_stage((*key).clone())
            })
            .cloned()
            .collect();
        self.engine
            .run(|transaction| {
                let context = &mut context;
                async move {
                    transaction
                        .lock_keys_with_context(context, keys.iter().cloned())
                        .await
                }
            })
            .map_err(|error| {
                let mut failure = lock_error(crate::TikvTransactionError::Client(error));
                if let [requested] = keys {
                    match &mut failure {
                        tidb_txnkv::transaction::PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key }
                        | tidb_txnkv::transaction::PessimisticLockFailure::LockWaitTimeout { key } => {
                            key.clone_from(requested)
                        }
                        _ => {}
                    }
                }
                failure
            })?;
        self.max_conflict_ts = self
            .max_conflict_ts
            .max(context.max_locked_with_conflict_ts);
        let mut values = std::collections::BTreeMap::new();
        let mut locked_with_conflict = Vec::new();
        for key in keys {
            if let Some(value) = context.returned_value(key) {
                if value.locked_with_conflict_ts > 0 {
                    locked_with_conflict.push((key.clone(), value.locked_with_conflict_ts));
                }
                if return_values && !value.already_locked && value.locked_with_conflict_ts == 0 {
                    values.insert(key.clone(), value.exists.then(|| value.value.clone()));
                }
            }
        }
        Ok(tidb_txnkv::transaction::AcquiredLocks {
            for_update_ts: self.for_update_ts,
            keys: newly_locked,
            locked_with_conflict,
            values,
        })
    }
    /// Cancels the active statement and rolls back the native transaction.
    pub fn rollback(
        mut self,
        call: &UnaryCallContext,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError> {
        self.calls.set(call);
        self.apply_statement_kv_context();
        self.finish_statement(false).map_err(Self::read_error)?;
        self.finish_without_writes()
    }
}
fn lock_error(error: crate::TikvTransactionError) -> tidb_txnkv::transaction::PessimisticLockFailure {
    if let crate::TikvTransactionError::Client(error) = &error {
        if tikv_client::error::is_lock_acquire_fail_and_no_wait_set(error) {
            return tidb_txnkv::transaction::PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key: Vec::new() };
        }
        if tikv_client::error::is_lock_wait_timeout(error) {
            return tidb_txnkv::transaction::PessimisticLockFailure::LockWaitTimeout { key: Vec::new() };
        }
        fn unwrapped(error: &tikv_client::Error) -> &tikv_client::Error {
            match error {
                tikv_client::Error::PessimisticLockError { inner, .. } => unwrapped(inner),
                tikv_client::Error::ExtractedErrors(errors)
                | tikv_client::Error::MultipleKeyErrors(errors)
                    if !errors.is_empty() =>
                {
                    unwrapped(&errors[0])
                }
                _ => error,
            }
        }
        if let tikv_client::Error::Deadlock(deadlock) = unwrapped(error) {
            return tidb_txnkv::transaction::PessimisticLockFailure::Deadlock(deadlock.clone());
        }
    }
    match crate::driver::tikv_transaction::classify_cause(&error) {
        TransactionCause::WriteConflict { detail, .. } => {
            tidb_txnkv::transaction::PessimisticLockFailure::WriteConflict { detail }
        }
        cause => tidb_txnkv::transaction::PessimisticLockFailure::Transaction(cause),
    }
}

impl<S: PdClient> Drop for ClientTransaction<S> {
    fn drop(&mut self) {
        if self.engine.transaction_mut().inner_mut().is_valid() {
            if self.engine.is_statement_locking() {
                let _ = self.engine.cancel_statement_locking();
            }
            let _ = self.engine.finish_without_writes();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_snapshot_errors_reach_the_sql_backoff_conversion() {
        for (error, expected_code) in [
            (tikv_client::error::ERR_TIKV_SERVER_TIMEOUT, 9002),
            (tikv_client::error::ERR_RESOLVE_LOCK_TIMEOUT, 9004),
            (tikv_client::error::ERR_REGION_UNAVAILABLE, 9005),
        ] {
            let error =
                ClientTransaction::<tikv_client::PdRpcClient>::client_read_error(error.into());
            let OptimisticCoordinatorError::SnapshotBackoff { kind, detail } = error else {
                panic!("native backoff was flattened: {error:?}");
            };
            let sql = tidb_txnkv::to_tidb_driver_error(&tidb_txnkv::StorageDriverError::from_backoff(
                kind, &detail,
            ));
            let tidb_txnkv::ConvertedDriverError::Terror(sql) = sql else {
                panic!("expected a TiDB error, got {sql:?}");
            };
            assert_eq!(sql.code().value(), expected_code);
        }
    }

    /// client-go `KVSnapshot`'s `CheckVisibility` failure (`ErrTxnAbortedByGC`)
    /// keeps its identity through a snapshot read, as Go's `ToTiDBErr`
    /// requires, instead of becoming a generic read diagnostic.
    #[test]
    fn a_gc_aborted_snapshot_read_keeps_its_identity() {
        let cache = tikv_client::tikv::TxnSafePointCache::new(450, std::time::SystemTime::now());
        let aborted = cache.check_visibility(449).unwrap_err();
        let expected = OptimisticCoordinatorError::Visibility(VisibilityError::TxnAbortedByGc {
            txn_start_ts: 449,
            txn_safe_point: 450,
        });
        assert_eq!(
            ClientTransaction::<tikv_client::PdRpcClient>::client_read_error(aborted),
            expected
        );
        let aborted = cache.check_visibility(449).unwrap_err();
        assert_eq!(
            ClientTransaction::<tikv_client::PdRpcClient>::transaction_error(
                crate::TikvTransactionError::Client(aborted)
            ),
            expected
        );
    }
}
