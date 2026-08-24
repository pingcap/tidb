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

//! The cluster tier's home for one table's auto-increment counter: the meta
//! key Go keeps it in, read and written in a transaction of its OWN.
//!
//! This is the half `tidb_executor`'s [`AutoIdStore`] leaves open. The
//! allocation rules -- which id comes next, which explicit value rebases,
//! when the domain is exhausted -- are the allocator's and are the same on
//! both tiers; what changes here is only WHERE the number lives, which is the
//! whole reason this tier can now serve a table it used to refuse.
//!
//! # Which key, and why it is not the obvious one
//!
//! Go has two auto-id meta keys and picks between them by a rule that is easy
//! to get backwards: `pkg/meta/autoid/autoid.go`'s
//! `NewAllocatorsFromTblInfo` gives an AUTO_INCREMENT column the
//! `AutoIncrementType` allocator -- the `IID:<tableID>` key -- ONLY when
//! `tblInfo.SepAutoInc()`, which is `Version >= TableInfoVersion5 &&
//! AutoIDCache == 1`. Every ordinary table has `AutoIDCache == 0`, so its
//! AUTO_INCREMENT ids come from `RowIDAllocType`: the SAME `TID:<tableID>`
//! key that hands out `_tidb_rowid`. `Allocators::Get` makes that explicit by
//! rewriting a request for `AutoIncrementType` into `RowIDAllocType` whenever
//! `SepAutoInc` is false.
//!
//! Choosing `IID:` because the name matches would put this node's counter in
//! a key no Go `tidb-server` on the same cluster reads, and the two would
//! hand out the same ids from separate counters with nothing to detect it.
//! [`auto_id_key_for`] makes the choice once, from the stored `TableInfo`, so
//! the rule lives in one place.
//!
//! # Why a transaction of its own, and why it retries
//!
//! Go reserves through `kv.RunInNewTxn` (`alloc4Signed`, `rebase4Signed`),
//! not through the statement's transaction. That is what burns an id the
//! moment it is issued: a statement that fails afterwards, or a transaction
//! that rolls back, does not give the id back, and no peer can be handed it
//! either. Staging the bump in the row's own transaction would return ids on
//! rollback and let two nodes commit the same id.
//!
//! `RunInNewTxn` retries on a write conflict, and so does this: two nodes
//! reserving at once means one of them loses the prewrite race, and that node
//! must re-read and try again rather than fail an INSERT. Losing the race is
//! the mechanism that keeps the counters disjoint, so it is a normal event,
//! not an error.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tidb_executor::kv_table::{advance, calc_needed_batch_size, AutoIdStore, AutoIdStoreError};
use tidb_meta::{key, value};
use tidb_model::table_info::TableInfo;
use tidb_pd_client::PdClient;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, RealOptimisticTransactionOpener,
};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};
use tidb_txnkv::PdRegionLoader;

use crate::cluster_catalog::MetaSnapshot;
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// How many times a reservation re-reads after losing a prewrite race.
///
/// Go's `kv.RunInNewTxn` retries `maxRetryCnt` (100) times. The number only
/// has to outlast the nodes contending for one table's counter; a reservation
/// covers [`DEFAULT_AUTO_ID_STEP`] ids, so a node reaches this key rarely
/// enough that even a handful of peers do not queue this deep.
///
/// [`DEFAULT_AUTO_ID_STEP`]: tidb_executor::kv_table::DEFAULT_AUTO_ID_STEP
pub(crate) const MAX_RESERVE_RETRIES: usize = 100;

/// Go `autoid.AutoIDLeaderPath`.
pub const AUTO_ID_LEADER_PATH: &str = "tidb/autoid/leader";
const KEYSPACE_AUTO_ID_LEADER_PATH: &str = "/tidb/autoid/leader";

/// Go `autoid.GetAutoIDServiceLeaderEtcdPath`.
///
/// Nullspace uses the path as-is. A keyspace-scoped etcd client already has
/// the `/keyspaces/tidb/<id>` namespace, so its relative key starts with `/`.
#[must_use]
pub const fn auto_id_service_leader_etcd_path(keyspace_id: u32) -> &'static str {
    if keyspace_id == 0 {
        AUTO_ID_LEADER_PATH
    } else {
        KEYSPACE_AUTO_ID_LEADER_PATH
    }
}

const AUTO_ID_BACKOFF_MIN: Duration = Duration::from_millis(5);
const AUTO_ID_BACKOFF_MAX: Duration = Duration::from_millis(100);

/// One typed request to Go's AutoID service `AllocAutoID` RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoIdServiceAllocRequest {
    /// Database identity in the meta key.
    pub db_id: i64,
    /// Table identity in the meta key.
    pub table_id: i64,
    /// Number of sequence-ladder values requested.
    pub n: u64,
    /// Session `auto_increment_increment`.
    pub increment: i64,
    /// Session `auto_increment_offset`.
    pub offset: i64,
    /// Whether comparisons use the unsigned 64-bit domain.
    pub unsigned: bool,
    /// TiKV API-v2 keyspace identity; zero is Nullspace.
    pub keyspace_id: u32,
}

/// One typed request to Go's AutoID service `Rebase` RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoIdServiceRebaseRequest {
    /// Database identity in the meta key.
    pub db_id: i64,
    /// Table identity in the meta key.
    pub table_id: i64,
    /// New global base requested by the caller.
    pub new_base: i64,
    /// Whether the service must set the base even when it moves backwards.
    pub force: bool,
    /// Whether comparisons use the unsigned 64-bit domain.
    pub unsigned: bool,
    /// TiKV API-v2 keyspace identity; zero is Nullspace.
    pub keyspace_id: u32,
}

/// A transport failure classified before the AutoID retry policy runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoIdServiceRpcError {
    /// A gRPC transport/status failure. Go recognizes this by the brittle
    /// `"rpc error"` substring; the Rust boundary keeps it typed.
    Rpc(String),
    /// A service/discovery failure that retrying a connection cannot repair.
    Other(String),
}

impl std::fmt::Display for AutoIdServiceRpcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(message) | Self::Other(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for AutoIdServiceRpcError {}

/// The transport/discovery seam used by [`AutoIdServiceAllocator`].
///
/// `reset_connection` receives the generation that failed. The allocator
/// calls it at most once for that generation, matching Go `resetConn`'s CAS.
pub trait AutoIdServiceRpc: Send + Sync + 'static {
    /// Calls `AutoIDAlloc.AllocAutoID` once.
    fn alloc_auto_id(
        &self,
        call: &UnaryCallContext,
        request: AutoIdServiceAllocRequest,
    ) -> Result<(i64, i64), AutoIdServiceRpcError>;

    /// Calls `AutoIDAlloc.Rebase` once.
    fn rebase(
        &self,
        call: &UnaryCallContext,
        request: AutoIdServiceRebaseRequest,
    ) -> Result<(), AutoIdServiceRpcError>;

    /// Drops the client for one failed generation so discovery can reconnect.
    fn reset_connection(&self, _generation: u64, _reason: &AutoIdServiceRpcError) {}
}

/// Terminal results of the AutoID service retry loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoIdServiceError {
    /// The statement/query owner canceled the call (Go `context.Canceled`).
    Cancelled,
    /// The caller's absolute RPC deadline elapsed.
    DeadlineExceeded,
    /// `increment` or `offset` was outside Go's inclusive `[1, 65535]` range.
    InvalidIncrementAndOffset {
        /// Rejected increment.
        increment: i64,
        /// Rejected offset.
        offset: i64,
    },
    /// A non-retryable discovery, transport, or service failure.
    Rpc(AutoIdServiceRpcError),
}

impl std::fmt::Display for AutoIdServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("auto ID service call cancelled"),
            Self::DeadlineExceeded => formatter.write_str("auto ID service call deadline exceeded"),
            Self::InvalidIncrementAndOffset { increment, offset } => write!(
                formatter,
                "invalid auto ID increment {increment} or offset {offset}"
            ),
            Self::Rpc(error) => write!(formatter, "auto ID service call failed: {error}"),
        }
    }
}

impl std::error::Error for AutoIdServiceError {}

/// Go `singlePointAlloc`: an AutoID service client with generation-safe
/// reconnect and cancellation-aware retries.
pub struct AutoIdServiceAllocator<C> {
    client: Arc<C>,
    db_id: i64,
    table_id: i64,
    unsigned: bool,
    keyspace_id: u32,
    generation: AtomicU64,
    last_allocated: AtomicI64,
}

impl<C: AutoIdServiceRpc> AutoIdServiceAllocator<C> {
    /// Binds one table to a service transport/discovery implementation.
    #[must_use]
    pub fn new(
        client: Arc<C>,
        db_id: i64,
        table_id: i64,
        unsigned: bool,
        keyspace_id: u32,
    ) -> Self {
        Self {
            client,
            db_id,
            table_id,
            unsigned,
            keyspace_id,
            generation: AtomicU64::new(0),
            last_allocated: AtomicI64::new(0),
        }
    }

    /// Go `singlePointAlloc.Alloc`: retry an RPC generation, but let caller
    /// cancellation/deadline win before connection reset or backoff.
    pub fn alloc(
        &self,
        call: &UnaryCallContext,
        n: u64,
        increment: i64,
        offset: i64,
    ) -> Result<(i64, i64), AutoIdServiceError> {
        if !(1..=65_535).contains(&increment) || !(1..=65_535).contains(&offset) {
            return Err(AutoIdServiceError::InvalidIncrementAndOffset { increment, offset });
        }
        let request = AutoIdServiceAllocRequest {
            db_id: self.db_id,
            table_id: self.table_id,
            n,
            increment,
            offset,
            unsigned: self.unsigned,
            keyspace_id: self.keyspace_id,
        };
        let mut backoff = AutoIdServiceBackoff::default();
        loop {
            let generation = self.generation.load(Ordering::Acquire);
            match self.client.alloc_auto_id(call, request) {
                Ok((min, max)) => {
                    backoff.reset();
                    self.last_allocated.store(min, Ordering::Release);
                    return Ok((min, max));
                }
                Err(error @ AutoIdServiceRpcError::Rpc(_)) => {
                    stopped(call)?;
                    self.reset_generation(generation, &error);
                    backoff.backoff(Some(call))?;
                }
                Err(error) => return Err(AutoIdServiceError::Rpc(error)),
            }
        }
    }

    /// Go `singlePointAlloc.rebase`, with the same terminal cancellation rule
    /// as allocation.
    pub fn rebase(
        &self,
        call: &UnaryCallContext,
        new_base: i64,
        force: bool,
    ) -> Result<(), AutoIdServiceError> {
        let mut backoff = AutoIdServiceBackoff::default();
        let request = AutoIdServiceRebaseRequest {
            db_id: self.db_id,
            table_id: self.table_id,
            new_base,
            force,
            unsigned: self.unsigned,
            keyspace_id: self.keyspace_id,
        };
        loop {
            let generation = self.generation.load(Ordering::Acquire);
            match self.client.rebase(call, request) {
                Ok(()) => {
                    backoff.reset();
                    self.last_allocated.store(new_base, Ordering::Release);
                    return Ok(());
                }
                Err(error @ AutoIdServiceRpcError::Rpc(_)) => {
                    stopped(call)?;
                    self.reset_generation(generation, &error);
                    backoff.backoff(Some(call))?;
                }
                Err(error) => return Err(AutoIdServiceError::Rpc(error)),
            }
        }
    }

    /// Last minimum/base returned by the service, for Go `Transfer` parity.
    #[must_use]
    pub fn last_allocated(&self) -> i64 {
        self.last_allocated.load(Ordering::Acquire)
    }

    fn reset_generation(&self, generation: u64, error: &AutoIdServiceRpcError) {
        if self
            .generation
            .compare_exchange(
                generation,
                generation.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.client.reset_connection(generation, error);
        }
    }
}

#[derive(Debug, Default)]
struct AutoIdServiceBackoff {
    duration: Duration,
}

impl AutoIdServiceBackoff {
    fn reset(&mut self) {
        self.duration = AUTO_ID_BACKOFF_MIN;
    }

    fn backoff(&mut self, call: Option<&UnaryCallContext>) -> Result<(), AutoIdServiceError> {
        if self.duration.is_zero() {
            self.duration = AUTO_ID_BACKOFF_MIN;
        }
        self.duration = self.duration.saturating_mul(2).min(AUTO_ID_BACKOFF_MAX);

        let Some(call) = call else {
            thread::sleep(self.duration);
            return Ok(());
        };
        stopped(call)?;
        let remaining = call.timeout();
        let wait = self.duration.min(remaining);
        if call.cancellation().wait_timeout(wait) {
            return Err(AutoIdServiceError::Cancelled);
        }
        if call.timeout().is_zero() {
            return Err(AutoIdServiceError::DeadlineExceeded);
        }
        Ok(())
    }
}

fn stopped(call: &UnaryCallContext) -> Result<(), AutoIdServiceError> {
    if call.cancellation().is_cancelled() {
        Err(AutoIdServiceError::Cancelled)
    } else if call.timeout().is_zero() {
        Err(AutoIdServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

/// The meta key holding `table`'s auto-increment counter, as Go chooses it.
///
/// See the module doc: `IID:` only for a separate-allocator table
/// (`AUTO_ID_CACHE 1`), `TID:` -- the row-id key -- for every other one.
#[must_use]
pub fn auto_id_key_for(db_id: i64, table: &TableInfo) -> Vec<u8> {
    if table.sep_auto_inc() {
        key::auto_increment_id_kv_key(db_id, table.id)
    } else {
        key::auto_table_id_kv_key(db_id, table.id)
    }
}

/// The meta key holding `table`'s distinct AUTO_RANDOM counter.
#[must_use]
pub fn auto_random_id_key_for(db_id: i64, table: &TableInfo) -> Vec<u8> {
    key::auto_random_table_id_kv_key(db_id, table.id)
}

/// One table's counter, living in the cluster's meta keys.
///
/// Held by the node and SHARED by every session on it, which is what makes a
/// reservation worth its transaction: Go caches a range per `tidb-server`,
/// not per connection, so a hundred sysbench connections inserting into one
/// table read this key as rarely as one connection does. A per-session store
/// would be correct and would burn a whole step per connection.
#[derive(Clone)]
pub struct ClusterAutoIdStore<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    opener: RealOptimisticTransactionOpener<C, L, P>,
    /// Go's `mDBs` hash key the counter field hangs off.
    counter_key: Vec<u8>,
    /// How long each meta read and the commit may take.
    timeout: Duration,
}

impl<C, L, P> std::fmt::Debug for ClusterAutoIdStore<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterAutoIdStore")
            .field("counter_key", &self.counter_key)
            .finish_non_exhaustive()
    }
}

impl<C, L, P> ClusterAutoIdStore<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// The counter for `table` in database `db_id`.
    #[must_use]
    pub fn new(
        opener: RealOptimisticTransactionOpener<C, L, P>,
        db_id: i64,
        table: &TableInfo,
        timeout: Duration,
    ) -> Self {
        Self::over_key(opener, auto_id_key_for(db_id, table), timeout)
    }

    /// The distinct AUTO_RANDOM counter for `table`.
    #[must_use]
    pub fn new_random(
        opener: RealOptimisticTransactionOpener<C, L, P>,
        db_id: i64,
        table: &TableInfo,
        timeout: Duration,
    ) -> Self {
        Self::over_key(opener, auto_random_id_key_for(db_id, table), timeout)
    }

    fn over_key(
        opener: RealOptimisticTransactionOpener<C, L, P>,
        counter_key: Vec<u8>,
        timeout: Duration,
    ) -> Self {
        ClusterAutoIdStore {
            opener,
            counter_key,
            timeout,
        }
    }

    /// As an [`AutoIdStore`] the table can be given.
    #[must_use]
    pub fn shared(self) -> Arc<dyn AutoIdStore> {
        Arc::new(self)
    }

    /// Runs one read-modify-write attempt against the counter key.
    ///
    /// `decide` sees the stored value as its 64-bit pattern -- Go's absent key
    /// reads as 0 (`HGetInt64`) -- and returns what the key should hold plus
    /// what the caller gets back. Returning `None` for the new value commits
    /// nothing, which is Go's "required base satisfied, we don't need to
    /// update KV".
    fn transact<T>(&self, decide: impl Fn(u64) -> (Option<u64>, T)) -> Result<T, AutoIdStoreError> {
        let mut conflicts = 0usize;
        loop {
            let call = UnaryCallContext::with_timeout(self.timeout);
            let mut transaction = self
                .opener
                // One key, whose value is a decimal integer.
                .begin(1, 64)
                .map_err(|error| store_error("open", &error))?;
            let stored = {
                let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
                snapshot
                    .get(&self.counter_key)
                    .map_err(|error| store_error("read", &error))?
            };
            // Go `TxStructure.HGetInt64`: a missing field is zero, and a
            // stored value is a decimal string.
            let current = match stored {
                None => 0i64,
                Some(bytes) => {
                    value::parse_int_value(&bytes).map_err(|error| store_error("decode", &error))?
                }
            };
            let (new_value, outcome) = decide(current as u64);
            let Some(new_value) = new_value else {
                transaction
                    .finish_without_writes()
                    .map_err(|error| store_error("finish", &error))?;
                return Ok(outcome);
            };
            let mutation = OptimisticMutation::meta_put(
                self.counter_key.clone(),
                value::encode_int_value(new_value as i64),
            )
            .map_err(|error| store_error("encode", &error))?;
            match transaction
                .commit(vec![mutation], &call)
                .map_err(|error| store_error("commit", &error))?
            {
                OptimisticCommitOutcome::Committed(_) => return Ok(outcome),
                // A peer reserved from this same key first. Go's
                // `RunInNewTxn` re-reads and tries again, which is the only
                // answer that keeps the two ranges disjoint.
                other => {
                    conflicts += 1;
                    if conflicts >= MAX_RESERVE_RETRIES {
                        return Err(AutoIdStoreError(format!(
                            "the auto-increment counter could not be reserved after \
                             {conflicts} attempts, the last ending {:?}",
                            other.state()
                        )));
                    }
                }
            }
        }
    }
}

impl<C, L, P> AutoIdStore for ClusterAutoIdStore<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn reserve(&self, step: u64, unsigned: bool) -> Result<(u64, u64), AutoIdStoreError> {
        self.transact(|current| {
            let end = advance(current, step, unsigned);
            // No room left: say so by handing back an empty range instead of
            // writing the key, which is Go returning from inside the
            // reservation without ever calling `Inc`.
            if end == current {
                (None, (current, current))
            } else {
                (Some(end), (current, end))
            }
        })
    }

    fn next_global(&self) -> Result<u64, AutoIdStoreError> {
        self.transact(|current| (None, current.wrapping_add(1)))
    }

    fn reserve_batch(
        &self,
        minimum_step: u64,
        n: u64,
        increment: u64,
        offset: u64,
        unsigned: bool,
    ) -> Result<(u64, u64), AutoIdStoreError> {
        self.transact(|current| {
            batch_reservation(current, minimum_step, n, increment, offset, unsigned)
        })
    }

    fn rebase(&self, required: u64, unsigned: bool) -> Result<(), AutoIdStoreError> {
        self.transact(|current| {
            if tidb_executor::kv_table::exceeds(required, current, unsigned) {
                (Some(required), ())
            } else {
                (None, ())
            }
        })
    }

    fn force_rebase(&self, required: u64, _unsigned: bool) -> Result<(), AutoIdStoreError> {
        self.transact(|current| {
            if current == required {
                (None, ())
            } else {
                (Some(required), ())
            }
        })
    }

    fn reset(&self) -> Result<(), AutoIdStoreError> {
        self.transact(|current| {
            if current == 0 {
                (None, ())
            } else {
                (Some(0), ())
            }
        })
    }
}

/// The decision run inside the counter transaction for one batch request.
/// Computing `needed` here, after the current global base was read, is the
/// source invariant exercised by `TestAllocComputationIssue`.
fn batch_reservation(
    current: u64,
    minimum_step: u64,
    n: u64,
    increment: u64,
    offset: u64,
    unsigned: bool,
) -> (Option<u64>, (u64, u64)) {
    let needed = calc_needed_batch_size(current, n, increment, offset, unsigned);
    let end = advance(current, minimum_step.max(needed), unsigned);
    if end == current {
        (None, (current, current))
    } else {
        (Some(end), (current, end))
    }
}

/// One phrase for every way the counter's home can be out of reach, so the
/// statement that surfaces it says which step failed.
fn store_error(step: &str, error: &impl std::fmt::Display) -> AutoIdStoreError {
    AutoIdStoreError(format!(
        "the table's auto-increment counter could not be {step}: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    #[derive(Debug)]
    struct MockAutoIdServiceRpc {
        alloc_calls: AtomicUsize,
        rebase_calls: AtomicUsize,
        reset_calls: AtomicUsize,
        alloc_error: Option<AutoIdServiceRpcError>,
        rebase_error: Option<AutoIdServiceRpcError>,
    }

    impl AutoIdServiceRpc for MockAutoIdServiceRpc {
        fn alloc_auto_id(
            &self,
            _call: &UnaryCallContext,
            _request: AutoIdServiceAllocRequest,
        ) -> Result<(i64, i64), AutoIdServiceRpcError> {
            self.alloc_calls.fetch_add(1, Ordering::Relaxed);
            self.alloc_error.clone().map_or(Ok((0, 1)), Err)
        }

        fn rebase(
            &self,
            _call: &UnaryCallContext,
            _request: AutoIdServiceRebaseRequest,
        ) -> Result<(), AutoIdServiceRpcError> {
            self.rebase_calls.fetch_add(1, Ordering::Relaxed);
            self.rebase_error.clone().map_or(Ok(()), Err)
        }

        fn reset_connection(&self, _generation: u64, _reason: &AutoIdServiceRpcError) {
            self.reset_calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn canceled_call() -> UnaryCallContext {
        let cancellation = tidb_txnkv::rpc::UnaryCancellation::new();
        cancellation.cancel();
        UnaryCallContext::new(Duration::from_secs(1), cancellation)
    }

    /// Source: `pkg/meta/autoid/autoid_test.go::TestAllocComputationIssue`.
    #[test]
    fn test_alloc_computation_issue() {
        // The stale allocator-local bases in the Go regression are 9 and 4.
        // Its transaction reads the actual shared bases 10 and 7, and the
        // next two values on an increment-3 ladder therefore need six slots.
        assert_eq!(
            batch_reservation(10, 3, 2, 3, 1, true),
            (Some(16), (10, 16))
        );
        assert_eq!(batch_reservation(7, 3, 2, 3, 1, false), (Some(13), (7, 13)));

        // The configured reservation step may be larger, but never smaller
        // than the batch recomputed from the transaction's own base.
        assert_eq!(
            batch_reservation(10, 30, 2, 3, 1, false),
            (Some(40), (10, 40))
        );
    }

    /// Source:
    /// `pkg/meta/autoid/autoid_test.go::TestGetAutoIDServiceLeaderEtcdPath`.
    #[test]
    fn test_get_auto_id_service_leader_etcd_path() {
        assert_eq!(auto_id_service_leader_etcd_path(0), AUTO_ID_LEADER_PATH);
        assert_eq!(auto_id_service_leader_etcd_path(1), "/tidb/autoid/leader");
    }

    /// Source:
    /// `pkg/meta/autoid/autoid_service_test.go::TestAllocCanceledRPCReturnsQuickly`.
    #[test]
    fn test_alloc_canceled_rpc_returns_quickly() {
        let client = Arc::new(MockAutoIdServiceRpc {
            alloc_calls: AtomicUsize::new(0),
            rebase_calls: AtomicUsize::new(0),
            reset_calls: AtomicUsize::new(0),
            alloc_error: Some(AutoIdServiceRpcError::Rpc(
                "rpc error: code = Canceled desc = context canceled".to_owned(),
            )),
            rebase_error: None,
        });
        let allocator = AutoIdServiceAllocator::new(Arc::clone(&client), 1, 1, false, 0);
        let start = Instant::now();

        assert_eq!(
            allocator.alloc(&canceled_call(), 1, 1, 1),
            Err(AutoIdServiceError::Cancelled)
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(client.alloc_calls.load(Ordering::Relaxed), 1);
        assert_eq!(client.reset_calls.load(Ordering::Relaxed), 0);
    }

    /// Source:
    /// `pkg/meta/autoid/autoid_service_test.go::TestRebaseCanceledRPCReturnsQuickly`.
    #[test]
    fn test_rebase_canceled_rpc_returns_quickly() {
        let client = Arc::new(MockAutoIdServiceRpc {
            alloc_calls: AtomicUsize::new(0),
            rebase_calls: AtomicUsize::new(0),
            reset_calls: AtomicUsize::new(0),
            alloc_error: None,
            rebase_error: Some(AutoIdServiceRpcError::Rpc(
                "rpc error: code = Canceled desc = context canceled".to_owned(),
            )),
        });
        let allocator = AutoIdServiceAllocator::new(Arc::clone(&client), 1, 1, false, 0);
        let start = Instant::now();

        assert_eq!(
            allocator.rebase(&canceled_call(), 100, false),
            Err(AutoIdServiceError::Cancelled)
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(client.rebase_calls.load(Ordering::Relaxed), 1);
        assert_eq!(client.reset_calls.load(Ordering::Relaxed), 0);
    }

    /// Source: `pkg/meta/autoid/autoid_service_test.go::TestBackoffCtxAware`.
    #[test]
    fn test_backoff_ctx_aware() {
        let mut backoff = AutoIdServiceBackoff::default();

        let start = Instant::now();
        backoff.backoff(None).unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= AUTO_ID_BACKOFF_MIN);
        assert!(elapsed <= AUTO_ID_BACKOFF_MIN + Duration::from_millis(50));

        backoff.reset();
        let start = Instant::now();
        assert_eq!(
            backoff.backoff(Some(&canceled_call())),
            Err(AutoIdServiceError::Cancelled)
        );
        assert!(start.elapsed() < Duration::from_millis(10));

        backoff.reset();
        let cancellation = tidb_txnkv::rpc::UnaryCancellation::new();
        let cancel_from_thread = cancellation.clone();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            cancel_from_thread.cancel();
        });
        let call = UnaryCallContext::new(Duration::from_secs(1), cancellation);
        let start = Instant::now();
        let _ = backoff.backoff(Some(&call));
        assert!(start.elapsed() < Duration::from_millis(50));
        worker.join().unwrap();
    }
}
