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
use super::mutation::{validate_plan, MutationSetError};
use super::state::*;
use super::{CommitProtocol, OptimisticCoordinatorError, SchemaLease, TransactionCommandClient};
use crate::driver::client_bridge::{self, ClientPd};
use crate::gc_state::GcStateCache;
use crate::lock::{LockRecoveryClient, TimestampSource};
use crate::region::{RegionQueryLoader, RegionRecoveryLoader};
use crate::rpc::UnaryCallContext;
use crate::{MemBufferBackend, SharedReadRuntime, TikvTransactionDriver};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tikv_client::{Timestamp, TimestampExt, TransactionOptions};

/// One TiDB transaction. Both optimistic and pessimistic modes use this engine.
pub struct ClientTransaction<C, L, T> {
    pub(super) engine: TikvTransactionDriver<ClientPd>,
    pub(super) client: Arc<ClientPd>,
    marker: std::marker::PhantomData<(C, L, T)>,

    sql_staging: bool,
    start_ts: u64,
    authority_id: u64,
    planned_mutation_count: usize,
    planned_aggregate_bytes: usize,
    gc_state: Arc<GcStateCache>,
    snapshot_stats: Arc<tikv_client::SnapshotRuntimeStats>,
    read_ts: u64,
    collect_snapshot_stats: bool,
    commit_mode: Arc<std::sync::Mutex<Option<CommittedProtocol>>>,
    schema_error: Arc<std::sync::Mutex<Option<super::SchemaLeaseError>>>,
}

/// One point-read result. Physical diagnostics are supplied by the client engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGetResult {
    /// Timestamp of the transaction that owns the snapshot.
    pub start_ts: u64,
    /// Value returned by the union read, or absence.
    pub value: Option<Vec<u8>>,
    /// Region of the last physical Get, absent for a local buffer hit.
    pub region: Option<crate::region::RegionVerId>,
    /// Transport publication evidence for the physical Get.
    pub publication: Option<crate::rpc::TransactionBatchPublication>,
    /// Number of physical requests issued by this read.
    pub rpc_count: u64,
}
/// The rows served by one snapshot region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotScanRegion {
    /// Region that served these rows.
    pub region: crate::region::RegionVerId,
    /// Exclusive region boundary observed by the response.
    pub end_key: Vec<u8>,
    /// Rows from this serving region.
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl<C, L, T> crate::new_txn::TxnResourceGroup for ClientTransaction<C, L, T> {
    fn set_resource_group_name(&mut self, name: &str) {
        self.engine.transaction_mut().set_resource_group_name(name);
    }
}
impl<C, L, T> ClientTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient + Clone + Send + 'static,
    L: RegionRecoveryLoader + RegionQueryLoader + Send + 'static,
    T: TimestampSource + Send + 'static,
{
    /// Opens over injected storage capabilities with an initial visible GC safepoint.
    pub fn new_injected(
        runtime: SharedReadRuntime<C, L>,
        timestamps: T,
        timeout: Duration,
        start_ts: u64,
        opened_at: Instant,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
    ) -> Result<Self, OptimisticCoordinatorError> {
        validate_plan(planned_mutation_count, planned_aggregate_bytes)
            .map_err(OptimisticCoordinatorError::Mutations)?;
        Self::new_opened(
            runtime,
            timestamps,
            timeout,
            start_ts,
            opened_at,
            planned_mutation_count,
            planned_aggregate_bytes,
            Arc::new(GcStateCache::seeded(0, Instant::now())),
        )
    }
    /// Opens the native client at the allocated start timestamp and shared GC state.
    pub fn new_opened(
        runtime: SharedReadRuntime<C, L>,
        timestamps: T,
        _timeout: Duration,
        start_ts: u64,
        _opened_at: Instant,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
        gc_state: Arc<GcStateCache>,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if start_ts == 0 {
            return Err(OptimisticCoordinatorError::Timestamp(
                "a transaction requires a nonzero start timestamp".into(),
            ));
        }
        let authority_id = runtime.authority_id();
        let lock_resolver_context = runtime.native_lock_resolver_context();
        let client = ClientPd::new(runtime, timestamps);
        let mut transaction = tikv_client::Transaction::new(
            Timestamp::from_version(start_ts),
            client.clone(),
            // Transactions start optimistic; the pessimistic wrapper promotes
            // the same native transaction only when Go's statement mode needs
            // row locks. Autocommit DML follows Go's optimistic path.
            TransactionOptions::new_optimistic().drop_check(tikv_client::CheckLevel::Warn),
            tikv_client::request::Keyspace::Disable,
        );
        transaction.set_lock_resolver_context(lock_resolver_context);
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
        let engine = TikvTransactionDriver::new(transaction, client_bridge::runtime());
        Ok(Self {
            engine,
            client,
            sql_staging: false,
            marker: std::marker::PhantomData,
            start_ts,
            authority_id,
            planned_mutation_count,
            planned_aggregate_bytes,
            gc_state,
            snapshot_stats,
            read_ts: start_ts,
            collect_snapshot_stats: false,
            commit_mode,
            schema_error: Default::default(),
        })
    }
}
impl<C, L, T> ClientTransaction<C, L, T> {
    /// The immutable transaction start timestamp.
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }
    /// Identity of the process storage authority serving this transaction.
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }
    /// Subscribes to actual detached commit responses for diagnostics.
    pub fn observe_detached_commits(
        &mut self,
    ) -> std::sync::mpsc::Receiver<super::DetachedCommitCompletion> {
        self.client.observe_detached_commits()
    }
    /// Sets the protocols the client may choose for this transaction.
    pub fn set_commit_protocol(&mut self, protocol: CommitProtocol) {
        let transaction = self.engine.transaction_mut().inner_mut();
        transaction.set_enable_async_commit(protocol.async_commit);
        transaction.set_enable_one_pc(protocol.one_pc);
    }
    /// Installs statement-owned snapshot statistics in the client.
    pub fn set_snapshot_runtime_stats(
        &mut self,
        stats: Option<Arc<tikv_client::SnapshotRuntimeStats>>,
    ) {
        self.collect_snapshot_stats = stats.is_some();
        self.snapshot_stats = stats.unwrap_or_default();
        self.engine
            .transaction_mut()
            .inner_mut()
            .set_snapshot_runtime_stats(Some(self.snapshot_stats.clone()));
    }
    /// Physical Get and BatchGet counts reported by the native snapshot.
    pub fn snapshot_point_rpc_counts(&self) -> (u64, u64) {
        (
            self.snapshot_stats
                .rpc_count(tikv_client::SnapshotRpcCommand::Get),
            self.snapshot_stats
                .rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
        )
    }
    /// Point-read response details when collection is enabled.
    pub fn snapshot_point_response_stats(&self) -> tikv_client::PointResponseStats {
        let mut stats = self.snapshot_stats.point_response_stats();
        if !self.collect_snapshot_stats {
            stats.invalidate();
        }
        stats
    }
    fn read_error(error: impl std::fmt::Display) -> OptimisticCoordinatorError {
        OptimisticCoordinatorError::SnapshotGet(error.to_string())
    }
    fn client_read_error(error: tikv_client::Error) -> OptimisticCoordinatorError {
        match crate::driver::tikv_transaction::classify_client_cause(&error) {
            TransactionCause::BackoffExhausted { kind, detail } => {
                OptimisticCoordinatorError::SnapshotBackoff { kind, detail }
            }
            _ => Self::read_error(error),
        }
    }
    fn prepare_read(&mut self, read_ts: u64, call: &UnaryCallContext) {
        self.client.set_call(call);
        self.client.take_read_trace();
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
            .transaction_mut()
            .get(key.to_vec())
            .map_err(Self::client_read_error)?;
        let trace = self.client.take_read_trace();
        if trace.rpc_count > 0 {
            self.gc_state
                .check_visibility(read_ts)
                .map_err(OptimisticCoordinatorError::Visibility)?;
        }
        let (region, publication) = trace
            .last_get
            .map_or((None, None), |(region, publication)| {
                (Some(region), Some(publication))
            });
        Ok(SnapshotGetResult {
            start_ts: self.start_ts,
            value,
            region,
            publication,
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
            .transaction_mut()
            .block_on(|transaction| transaction.batch_get_with_options(keys.iter().cloned(), &[]))
            .map_err(Self::client_read_error)?
            .into_iter()
            .map(|(key, entry)| (Vec::from(key), entry.value))
            .collect();
        if self.client.take_read_trace().rpc_count > 0 {
            self.gc_state
                .check_visibility(read_ts)
                .map_err(OptimisticCoordinatorError::Visibility)?;
        }
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
            .transaction_mut()
            .scan(
                range,
                limit.unwrap_or(u32::MAX as usize).min(u32::MAX as usize) as u32,
            )
            .map_err(Self::client_read_error)?
            .map(|pair| (Vec::from(pair.0), pair.1))
            .collect();
        if self.client.take_read_trace().rpc_count > 0 {
            self.gc_state
                .check_visibility(read_ts)
                .map_err(OptimisticCoordinatorError::Visibility)?;
        }
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
    pub fn staged_entries(&self) -> Vec<(crate::Key, Vec<u8>)> {
        self.engine.staged_entries()
    }
    /// Returns a staged value, including an empty deletion tombstone.
    pub fn staged_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.engine.staged_value(&crate::Key::from(key.to_vec()))
    }
    /// Applies a statement plan atomically to the native staging buffer.
    pub fn stage_mutations(
        &mut self,
        mutations: Vec<super::OptimisticMutation>,
    ) -> Result<(), OptimisticCoordinatorError> {
        let stage = self.engine.staging();
        for mutation in mutations {
            if let Err(error) = self.engine.stage_mutation(&mutation) {
                self.engine.cleanup(stage);
                return Err(Self::read_error(error));
            }
        }
        self.engine.release(stage);
        Ok(())
    }
    /// Commits the authoritative buffer through the client protocol engine.
    pub fn commit(
        mut self,
        mutations: Vec<super::OptimisticMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        self.stage_mutations(mutations)?;
        let (mutation_count, size) = self.engine.staged_stats();
        if mutation_count > self.planned_mutation_count {
            return Err(OptimisticCoordinatorError::Mutations(
                MutationSetError::TooManyMutations {
                    count: mutation_count,
                    limit: self.planned_mutation_count,
                },
            ));
        }
        if size > self.planned_aggregate_bytes {
            return Err(OptimisticCoordinatorError::Mutations(
                MutationSetError::TransactionTooLarge {
                    size,
                    limit: self.planned_aggregate_bytes,
                },
            ));
        }
        self.client.set_call(call);
        let mut outcome = self.engine.commit_staged().map_err(Self::read_error)?;
        let receipt = match &mut outcome {
            OptimisticCommitOutcome::Committed(value) => &mut value.receipt,
            OptimisticCommitOutcome::RolledBack(value) => {
                if let Some(error) = self
                    .schema_error
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                {
                    value.cause = TransactionCause::SchemaLease {
                        code: error.code,
                        message: error.message,
                    };
                }
                &mut value.receipt
            }
            OptimisticCommitOutcome::Undetermined(value) => &mut value.receipt,
            OptimisticCommitOutcome::CleanupFailed(value) => &mut value.receipt,
        };
        receipt.authority_id = self.authority_id;
        self.client.fill_receipt(receipt);
        if let Some(mode) = *self.commit_mode.lock().unwrap_or_else(|e| e.into_inner()) {
            receipt.commit_protocol = mode;
        }
        Ok(outcome)
    }
    /// Ends the transaction and releases client-owned locks without publishing writes.
    pub fn finish_without_writes(
        mut self,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError> {
        self.engine
            .finish_without_writes()
            .map_err(Self::read_error)?;
        Ok(ReadOnlyTransaction {
            authority_id: self.authority_id,
            start_ts: self.start_ts,
            state: OptimisticTransactionState::ReadOnly,
        })
    }
}
impl<C, L, T> ClientTransaction<C, L, T> {
    /// Groups rows by the regions that actually served the native scan.
    pub fn snapshot_scan_regions(
        &mut self,
        start: &[u8],
        end: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Vec<SnapshotScanRegion>, OptimisticCoordinatorError> {
        self.client.capture_scan_pages(true);
        let result = self.snapshot_scan(start, end, None, call);
        let pages = self.client.capture_scan_pages(false);
        let rows = result?;
        // A region split or lock retry can replace a page. Associate each
        // final row with a successful response that actually supplied it.
        let mut served = std::collections::BTreeMap::new();
        for page in pages {
            for (key, value) in page.pairs {
                served.insert(key, (value, page.region, page.end_key.clone()));
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
    error: Arc<std::sync::Mutex<Option<super::SchemaLeaseError>>>,
}
impl tikv_client::transaction::SchemaLeaseChecker for DriverSchemaChecker {
    fn check_by_schema_version(
        &self,
        timestamp: u64,
        _version: &dyn tikv_client::transaction::SchemaVersion,
    ) -> tikv_client::Result<tikv_client::transaction::RelatedSchemaChange> {
        self.lease.check(timestamp).map_err(|error| {
            let message = error.message.clone();
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

/// TiDB's statement timestamp and options over the same client transaction.
/// Lock ownership, constraints, retries and TTL all remain in client-rust.
pub struct ClientPessimisticTransaction<C, L, T> {
    transaction: ClientTransaction<C, L, T>,
    for_update_ts: u64,
    fair_locking: bool,
    max_conflict_ts: u64,
    lock_expired: Arc<std::sync::atomic::AtomicU32>,
    statement_stage: Option<crate::StagingHandle>,
}
impl<C, L, T> ClientPessimisticTransaction<C, L, T> {
    /// Promotes the same native transaction to pessimistic mode.
    pub fn from_transaction(
        mut transaction: ClientTransaction<C, L, T>,
        _opened_at: Instant,
    ) -> Result<Self, OptimisticCoordinatorError> {
        transaction
            .engine
            .transaction_mut()
            .inner_mut()
            .set_pessimistic(true);
        let for_update_ts = transaction.start_ts();
        Ok(Self {
            transaction,
            for_update_ts,
            fair_locking: false,
            max_conflict_ts: 0,
            lock_expired: Default::default(),
            statement_stage: None,
        })
    }
    /// The immutable transaction start timestamp.
    pub fn start_ts(&self) -> u64 {
        self.transaction.start_ts()
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
    /// Sets the protocols the client may choose for this transaction.
    pub fn set_commit_protocol(&mut self, protocol: CommitProtocol) {
        self.transaction.set_commit_protocol(protocol);
    }
    /// Attaches the SQL schema validity check to the native commit lifecycle.
    pub fn set_schema_lease(&mut self, lease: SchemaLease) {
        self.transaction.set_schema_lease(lease);
    }
    /// Borrows the same transaction for union-store reads or SQL staging.
    pub fn snapshot(&mut self) -> &mut ClientTransaction<C, L, T> {
        &mut self.transaction
    }
    /// Inspects the authoritative transaction without changing it.
    pub fn snapshot_ref(&self) -> &ClientTransaction<C, L, T> {
        &self.transaction
    }
    /// Returns the native buffer keys carrying acquired-lock flags.
    pub fn locked_keys(&mut self) -> Vec<Vec<u8>> {
        self.transaction
            .engine
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
        self.transaction
            .snapshot_get_at(key, self.for_update_ts, call)
            .map(|r| r.value)
    }
    /// Opens the client statement scope, reusing the SQL staging owner when bound.
    pub fn start_statement(&mut self) {
        if !self.transaction.sql_staging && self.statement_stage.is_none() {
            self.statement_stage = Some(self.transaction.engine.staging());
        }
        if self.fair_locking && !self.transaction.engine.is_statement_locking() {
            self.transaction.engine.start_statement_locking();
        }
    }
    /// Completes or cancels the native statement scope while retaining ordinary locks.
    pub fn finish_statement(
        &mut self,
        successful: bool,
    ) -> Result<(), super::PessimisticLockFailure> {
        if self.transaction.engine.is_statement_locking() {
            if successful {
                self.transaction.engine.done_statement_locking()
            } else {
                self.transaction.engine.cancel_statement_locking()
            }
            .map_err(lock_error)?;
        }
        if let Some(stage) = self.statement_stage.take() {
            if successful {
                self.transaction.engine.release(stage);
            } else {
                self.transaction.engine.cleanup(stage);
            }
        }
        Ok(())
    }
    /// Allocates a fresh statement timestamp and forwards the native fair retry hook.
    pub fn advance_for_update_ts(&mut self) -> Result<u64, super::PessimisticLockFailure> {
        use tikv_client::PdClient;
        let timestamp = client_bridge::runtime()
            .block_on(self.transaction.client.clone().get_timestamp())
            .map_err(|error| {
                super::PessimisticLockFailure::Transaction(TransactionCause::Timestamp {
                    detail: error.to_string(),
                })
            })?
            .version();
        if timestamp <= self.for_update_ts {
            return Err(super::PessimisticLockFailure::Transaction(
                TransactionCause::Timestamp {
                    detail: "for_update_ts did not advance".to_owned(),
                },
            ));
        }
        if self.transaction.engine.is_statement_locking() {
            self.transaction
                .engine
                .retry_statement_locking()
                .map_err(lock_error)?;
        }
        if let Some(stage) = self.statement_stage.take() {
            self.transaction.engine.cleanup(stage);
            self.statement_stage = Some(self.transaction.engine.staging());
        }
        self.for_update_ts = timestamp;
        Ok(timestamp)
    }
    /// Forwards statement locking and absence checks to the native client.
    pub fn acquire_locks(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &std::collections::BTreeSet<Vec<u8>>,
        wait: super::LockWaitTime,
        call: &UnaryCallContext,
    ) -> Result<super::AcquiredLocks, super::PessimisticLockFailure> {
        self.acquire(keys, presume_not_exists, wait, call, false)
    }
    /// Locks keys and returns values from the statement lock context.
    pub fn acquire_locks_returning_values(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &std::collections::BTreeSet<Vec<u8>>,
        wait: super::LockWaitTime,
        call: &UnaryCallContext,
    ) -> Result<super::AcquiredLocks, super::PessimisticLockFailure> {
        self.acquire(keys, presume_not_exists, wait, call, true)
    }
    fn acquire(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &std::collections::BTreeSet<Vec<u8>>,
        wait: super::LockWaitTime,
        call: &UnaryCallContext,
        return_values: bool,
    ) -> Result<super::AcquiredLocks, super::PessimisticLockFailure> {
        self.transaction.client.set_call(call);
        self.start_statement();
        let before: std::collections::BTreeSet<_> = self.locked_keys().into_iter().collect();
        for key in presume_not_exists {
            self.transaction
                .engine
                .mem_buffer()
                .backend_mut()
                .update_flags(
                    &crate::Key::from(key.clone()),
                    &[crate::FlagsOp::SetPresumeKeyNotExists],
                );
        }
        let wait_ms = match wait {
            super::LockWaitTime::NoWait => -1,
            super::LockWaitTime::AlwaysWait => i64::MAX,
            super::LockWaitTime::Timeout(timeout) => {
                i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX)
            }
        };
        let mut context = tikv_client::kv::LockContext::new(
            self.for_update_ts,
            wait_ms,
            std::time::SystemTime::now(),
        );
        context.lock_expired = Some(self.lock_expired.clone());
        if return_values {
            context.init_return_values(keys.len());
        }
        let newly_locked = keys
            .iter()
            .filter(|key| {
                !before.contains(*key)
                    && !self
                        .transaction
                        .engine
                        .transaction_mut()
                        .inner_mut()
                        .is_in_aggressive_locking_stage((*key).clone())
            })
            .cloned()
            .collect();
        self.transaction
            .engine
            .transaction_mut()
            .block_on(|transaction| {
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
                        super::PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key }
                        | super::PessimisticLockFailure::LockWaitTimeout { key } => {
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
        Ok(super::AcquiredLocks {
            for_update_ts: self.for_update_ts,
            keys: newly_locked,
            primary_key: self.transaction.client.last_lock_primary(),
            locked_with_conflict,
            values,
        })
    }
    /// Commits the authoritative buffer through the client protocol engine.
    pub fn commit(
        mut self,
        mutations: Vec<super::OptimisticMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        self.finish_statement(true)
            .map_err(ClientTransaction::<C, L, T>::read_error)?;
        self.transaction.commit(mutations, call)
    }
    /// Cancels the active statement and rolls back the native transaction.
    pub fn rollback(
        mut self,
        call: &UnaryCallContext,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError> {
        self.transaction.client.set_call(call);
        self.finish_statement(false)
            .map_err(ClientTransaction::<C, L, T>::read_error)?;
        self.transaction.finish_without_writes()
    }
}
impl<C, L, T> crate::new_txn::TxnResourceGroup for ClientPessimisticTransaction<C, L, T> {
    fn set_resource_group_name(&mut self, name: &str) {
        self.transaction.set_resource_group_name(name);
    }
}
fn lock_error(error: crate::TikvTransactionError) -> super::PessimisticLockFailure {
    if let crate::TikvTransactionError::Client(error) = &error {
        if tikv_client::error::is_lock_acquire_fail_and_no_wait_set(error) {
            return super::PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key: Vec::new() };
        }
        if tikv_client::error::is_lock_wait_timeout(error) {
            return super::PessimisticLockFailure::LockWaitTimeout { key: Vec::new() };
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
            use prost::Message;
            if let Ok(proto) =
                tidb_proto::KvrpcDeadlock::decode(deadlock.deadlock.encode_to_vec().as_slice())
            {
                let mut detail = super::DeadlockDetail::from(&proto);
                detail.is_retryable = deadlock.is_retryable;
                return super::PessimisticLockFailure::Deadlock(detail);
            }
        }
    }
    match crate::driver::tikv_transaction::classify_cause(&error) {
        TransactionCause::WriteConflict { detail } => {
            super::PessimisticLockFailure::WriteConflict { detail }
        }
        cause => super::PessimisticLockFailure::Transaction(cause),
    }
}

impl<C, L, T> Drop for ClientTransaction<C, L, T> {
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
    fn native_transaction_starts_optimistic_until_the_session_promotes_it() {
        use crate::region::*;
        struct NoRegions;
        impl RegionLoader for NoRegions {
            fn cluster_id(&self) -> u64 {
                1
            }
            fn load_region(&mut self, _: &[u8]) -> Result<RegionLocation, RegionLoadError> {
                panic!("opening a transaction must not load regions")
            }
        }
        impl RegionQueryLoader for NoRegions {
            fn query_region(
                &mut self,
                _: RegionQuery<'_>,
                _: RegionQueryOptions,
            ) -> Result<RegionLocation, RegionLoadError> {
                unreachable!()
            }
            fn scan_regions_once(
                &mut self,
                _: &KeyRange,
                _: usize,
                _: RegionQueryOptions,
            ) -> Result<Vec<RegionLocation>, RegionLoadError> {
                unreachable!()
            }
            fn load_store(&mut self, _: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
                unreachable!()
            }
        }
        impl RegionRecoveryLoader for NoRegions {
            fn hydrate_region(
                &mut self,
                _: &RegionMetadata,
                _: u64,
                _: &mut std::collections::BTreeMap<u64, Option<StoreMetadata>>,
            ) -> Result<RegionLocation, RegionLoadError> {
                unreachable!()
            }
        }
        let runtime = SharedReadRuntime::new_injected(
            crate::rpc::TonicCoprocessorClient::new().unwrap(),
            RegionCache::new(NoRegions),
        );
        let mut transaction = ClientTransaction::new_injected(
            runtime,
            crate::lock::FixedTimestampSource::new(200),
            Duration::from_secs(1),
            100,
            Instant::now(),
            1,
            100,
        )
        .unwrap();
        // client-go NewTiKVTxn leaves isPessimistic false. TiDB changes the
        // same KVTxn only when its session transaction mode requires locks.
        assert!(!transaction
            .engine
            .transaction_mut()
            .inner_mut()
            .is_pessimistic());
        let mut pessimistic =
            ClientPessimisticTransaction::from_transaction(transaction, Instant::now()).unwrap();
        assert!(pessimistic
            .transaction
            .engine
            .transaction_mut()
            .inner_mut()
            .is_pessimistic());
    }

    #[test]
    fn native_snapshot_errors_reach_the_sql_backoff_conversion() {
        for (error, expected_code) in [
            (tikv_client::error::ERR_TIKV_SERVER_TIMEOUT, 9002),
            (tikv_client::error::ERR_RESOLVE_LOCK_TIMEOUT, 9004),
            (tikv_client::error::ERR_REGION_UNAVAILABLE, 9005),
        ] {
            let error = ClientTransaction::<(), (), ()>::client_read_error(error.into());
            let OptimisticCoordinatorError::SnapshotBackoff { kind, detail } = error else {
                panic!("native backoff was flattened: {error:?}");
            };
            let sql = crate::to_tidb_driver_error(&crate::StorageDriverError::from_backoff(
                kind, &detail,
            ));
            let crate::ConvertedDriverError::Terror(sql) = sql else {
                panic!("expected a TiDB error, got {sql:?}");
            };
            assert_eq!(sql.code().value(), expected_code);
        }
    }
}
