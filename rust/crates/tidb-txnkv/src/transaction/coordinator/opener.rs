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

//! Deriving one transaction from the already-running process authorities, and
//! keeping a pessimistic transaction's primary lock alive while it runs.
//!
//! Go boundary: client-go's `txn.go` — `KVStore.Begin` allocates the `start_ts`
//! from PD and hands back a committer bound to the store's region cache and
//! transport — plus the TxnHeartBeat sender behind `txnLockTTLKeepAlive`.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_pd_client::PdClient;

use crate::gc_state::{GcStateCache, TxnSafePointLoader, TxnSafePointRefresher};
use crate::lock::TimestampSource;
use crate::rpc::{TonicCoprocessorClient, UnaryCallContext};
use crate::{PdRegionLoader, SharedReadOpener, SharedReadRuntime};

use super::super::command_client::{PublishedCommand, TransactionCommandClient};
use super::super::mutation::validate_plan;
use super::super::region_batches::point_route;
use super::super::ttl::{HeartBeatFailure, LockKeepAlive, TxnHeartBeatSender, MANAGED_LOCK_TTL_MS};
use super::{
    CommitProtocol, OptimisticCoordinatorError, ProductionOptimisticTransaction,
    ProductionPessimisticTransaction, RealOptimisticTransaction,
};

/// Process-level opener for concrete normal optimistic transactions.
///
/// It holds only a cloneable session opener and the cloneable capability for
/// the already-running PD worker. The unique RegionCache maintenance and TiKV
/// transport lifecycle owners remain with the process that supplied them.
pub struct RealOptimisticTransactionOpener {
    opener: crate::SharedReadOpener<TonicCoprocessorClient, PdRegionLoader>,
    pd: PdClient,
    timeout: Duration,
    /// Keeps the shared txn safe point current for as long as any clone of this
    /// opener — and therefore any transaction it opened — can still read.
    gc_state: Arc<TxnSafePointRefresher>,
    protocol: CommitProtocol,
}

impl Clone for RealOptimisticTransactionOpener {
    fn clone(&self) -> Self {
        Self {
            opener: self.opener.clone(),
            pd: self.pd.clone(),
            timeout: self.timeout,
            gc_state: Arc::clone(&self.gc_state),
            protocol: self.protocol,
        }
    }
}

impl RealOptimisticTransactionOpener {
    /// Derives transaction-opening capability from the already-running shared
    /// read authority. This starts no PD, RegionCache, or transport worker.
    pub fn from_process_capabilities(
        opener: SharedReadOpener<TonicCoprocessorClient, PdRegionLoader>,
        pd: PdClient,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if pd.cluster_id() == 0 {
            return Err(OptimisticCoordinatorError::ZeroClusterId);
        }
        // client-go loads the txn safe point inside `NewKVStore` and fails
        // store construction if it cannot: a reader that does not know the
        // safe point cannot tell a valid snapshot from a collected one.
        let gc_state = TxnSafePointRefresher::start(TxnSafePointLoader::new(
            pd.clone(),
            // The null keyspace: keyspace-level GC is not a scope this client
            // reads under.
            None,
            timeout,
        ))
        .map_err(|error| OptimisticCoordinatorError::GcState(error.to_string()))?;
        Ok(Self {
            opener,
            pd,
            timeout,
            gc_state: Arc::new(gc_state),
            protocol: CommitProtocol::two_phase_only(),
        })
    }

    /// Lets every transaction opened from here attempt `protocol`.
    ///
    /// The node resolves `@@tidb_enable_async_commit` / `@@tidb_enable_1pc`
    /// once, exactly as it resolves `@@tidb_pessimistic_txn_fair_locking`, so
    /// this is a property of the opener rather than an argument threaded
    /// through every call site that begins a transaction.
    #[must_use]
    pub const fn with_commit_protocol(mut self, protocol: CommitProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// The shared txn safe point every transaction from this opener reads
    /// against.
    #[must_use]
    pub fn gc_state_cache(&self) -> Arc<GcStateCache> {
        self.gc_state.cache()
    }

    /// Stable shared process authority identity.
    #[must_use]
    pub fn authority_id(&self) -> u64 {
        self.opener.authority_id()
    }

    /// The PD cluster this opener writes to, as PD itself names it.
    ///
    /// Never zero: [`Self::from_process_capabilities`] refuses a PD client that
    /// has not learned its cluster ID.
    #[must_use]
    pub fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
    }

    /// Opens a worker-local transaction over the existing process authorities.
    pub fn begin(
        &self,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
    ) -> Result<ProductionOptimisticTransaction, OptimisticCoordinatorError> {
        // Reject an invalid plan before opening a session or consuming a real
        // TSO; `new_injected` revalidates for callers that already hold one.
        validate_plan(planned_mutation_count, planned_aggregate_bytes)
            .map_err(OptimisticCoordinatorError::Mutations)?;
        self.open(planned_mutation_count, planned_aggregate_bytes)
    }

    /// Opens a writable transaction at a timestamp that has ALREADY been spent
    /// — the one the statement's own read is at — spending none of its own.
    ///
    /// This is what makes an implicit single-statement transaction a single
    /// transaction. Go allocates one timestamp for an autocommit DML and uses
    /// it for both halves: `pkg/sessiontxn/isolation/optimistic.go:45-46` points
    /// `getStmtReadTSFunc` *and* `getStmtForUpdateTSFunc` at `getTxnStartTS`,
    /// and client-go's `2pc.go` sets the committer's `startTS: txn.StartTS()`,
    /// which `prewrite.go` sends as `StartVersion`. Prewriting at a LATER
    /// timestamp than the read is not a slower version of the same thing: it is
    /// silent lost-update, because TiKV's conflict check compares a key's
    /// latest `commit_ts` against the *prewriting* transaction's `start_ts`, so
    /// a commit landing between the read and a fresh write timestamp is not a
    /// conflict TiKV can see and the stale value overwrites it with no error.
    ///
    /// `u64::MAX` is refused. It is not a timestamp — it is
    /// [`Self::begin_read_only_at_max_ts`]'s marker for "the latest committed
    /// version", correct only for a read that never writes. Refusing it here is
    /// what makes "a max-ts read must not publish" a property of the only
    /// function that can turn a read timestamp into a write one, rather than a
    /// comment somewhere upstream.
    pub fn begin_at(
        &self,
        start_ts: u64,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
    ) -> Result<ProductionOptimisticTransaction, OptimisticCoordinatorError> {
        validate_plan(planned_mutation_count, planned_aggregate_bytes)
            .map_err(OptimisticCoordinatorError::Mutations)?;
        if start_ts == u64::MAX {
            return Err(OptimisticCoordinatorError::Timestamp(
                "refusing to publish at the max-ts read marker: u64::MAX is the latest-committed \
                 read version, not a start timestamp a write may carry"
                    .to_owned(),
            ));
        }
        self.open_at(
            Some(start_ts),
            planned_mutation_count,
            planned_aggregate_bytes,
        )
    }

    /// Opens a transaction that may only read.
    ///
    /// A read has no mutation plan to validate, and a zero plan is not a
    /// loophole: it is the tightest possible write budget, so any later attempt
    /// to publish a mutation on this transaction is rejected.
    pub fn begin_read_only(
        &self,
    ) -> Result<ProductionOptimisticTransaction, OptimisticCoordinatorError> {
        self.open(0, 0)
    }

    /// Opens a read-only transaction at `u64::MAX` — the latest committed
    /// version — without asking PD for a timestamp at all.
    ///
    /// This is Go's `forcePrepareConstStartTS(math.MaxUint64)`
    /// (`pkg/sessiontxn/isolation/optimistic.go`), and it carries Go's whole
    /// soundness condition with it: reading at `MaxUint64` ignores snapshot
    /// isolation, so it is correct ONLY for a statement that reads exactly one
    /// row once and has no second read to stay consistent with. Nothing here
    /// can check that; the caller that DECLARES the shape owns it. This method
    /// is deliberately not `begin_read_only`'s default and takes no `start_ts`
    /// argument, so the only timestamp it can produce is the one Go names.
    ///
    /// Confirmed against a real cluster: TiKV honours `MaxUint64` as "read the
    /// latest committed value", and a row committed between two such reads
    /// becomes visible to the second.
    pub fn begin_read_only_at_max_ts(
        &self,
    ) -> Result<ProductionOptimisticTransaction, OptimisticCoordinatorError> {
        self.open_at(Some(u64::MAX), 0, 0)
    }

    fn open(
        &self,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
    ) -> Result<ProductionOptimisticTransaction, OptimisticCoordinatorError> {
        self.open_at(None, planned_mutation_count, planned_aggregate_bytes)
    }

    /// `start_ts` of `None` spends one PD timestamp; `Some` uses the supplied
    /// one and spends none.
    fn open_at(
        &self,
        start_ts: Option<u64>,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
    ) -> Result<ProductionOptimisticTransaction, OptimisticCoordinatorError> {
        let opened_at = Instant::now();
        let runtime = self
            .opener
            .open_session()
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        if runtime.cluster_id() != self.pd.cluster_id() {
            return Err(OptimisticCoordinatorError::ClusterMismatch {
                pd: self.pd.cluster_id(),
                region_cache: runtime.cluster_id(),
            });
        }
        let start_ts = match start_ts {
            Some(start_ts) => start_ts,
            None => self
                .pd
                .get_timestamp()
                .map_err(|error| OptimisticCoordinatorError::Timestamp(error.to_string()))?,
        };
        if start_ts == 0 {
            return Err(OptimisticCoordinatorError::Timestamp(
                "PD returned zero start timestamp".to_owned(),
            ));
        }
        let mut transaction = RealOptimisticTransaction::new_opened(
            runtime,
            PdLockTimestampSource(self.pd.clone()),
            self.timeout,
            start_ts,
            opened_at,
            planned_mutation_count,
            planned_aggregate_bytes,
            self.gc_state.cache(),
        )?;
        transaction.set_commit_protocol(self.protocol);
        Ok(transaction)
    }

    /// Opens a pessimistic transaction over the same process authorities.
    ///
    /// It shares the optimistic opener because a pessimistic transaction *is*
    /// an optimistic two-phase commit preceded by statement-level locking; only
    /// the conflict-detection point differs.
    pub fn begin_pessimistic(
        &self,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
    ) -> Result<ProductionPessimisticTransaction, OptimisticCoordinatorError> {
        let opened_at = Instant::now();
        let two_pc = self.begin(planned_mutation_count, planned_aggregate_bytes)?;
        super::super::RealPessimisticTransaction::from_transaction(two_pc, opened_at)
    }

    /// Starts refreshing `primary`'s lock TTL until the handle is dropped.
    ///
    /// A pessimistic transaction must call this once its primary key is
    /// locked, because that lock then has to survive every later statement.
    /// The keep-alive runs on its own thread with its own session opened from
    /// these same process authorities — the caller's session is thread-local
    /// and cannot be shared.
    pub fn start_lock_keep_alive(
        &self,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<LockKeepAlive, String> {
        // client-go refreshes at half the managed TTL, so a lock is renewed
        // once before it could expire even if one heartbeat is lost.
        self.start_lock_keep_alive_with_tick(
            primary,
            start_ts,
            Duration::from_millis(MANAGED_LOCK_TTL_MS / 2),
        )
    }

    /// Same as [`Self::start_lock_keep_alive`] with an explicit refresh
    /// interval, so a proof can observe several refreshes without waiting the
    /// production interval.
    pub fn start_lock_keep_alive_with_tick(
        &self,
        primary: Vec<u8>,
        start_ts: u64,
        tick: Duration,
    ) -> Result<LockKeepAlive, String> {
        let opener = self.opener.clone();
        let pd = self.pd.clone();
        let timeout = self.timeout;
        LockKeepAlive::start(primary, start_ts, tick, move || {
            let runtime = opener
                .open_session()
                .map_err(|error| format!("cannot open a keep-alive session: {error}"))?;
            Ok(SessionHeartBeatSender {
                runtime,
                pd,
                timeout,
            })
        })
    }
}

/// Production TxnHeartBeat sender bound to one keep-alive thread's session.
struct SessionHeartBeatSender {
    runtime: SharedReadRuntime<TonicCoprocessorClient, PdRegionLoader>,
    pd: PdClient,
    timeout: Duration,
}

impl TxnHeartBeatSender for SessionHeartBeatSender {
    fn current_ts(&self) -> Result<u64, String> {
        self.pd.get_timestamp().map_err(|error| error.to_string())
    }

    fn send_heart_beat(
        &mut self,
        primary: &[u8],
        start_ts: u64,
        advise_ttl_ms: u64,
    ) -> Result<u64, HeartBeatFailure> {
        let call = UnaryCallContext::with_timeout(self.timeout);
        let route = point_route(&self.runtime, primary)
            .map_err(|error| HeartBeatFailure::Transport(error.to_string()))?;
        let request = tidb_proto::KvrpcTxnHeartBeatRequest {
            primary_lock: primary.to_vec(),
            start_version: start_ts,
            advise_lock_ttl: advise_ttl_ms,
            ..tidb_proto::KvrpcTxnHeartBeatRequest::default()
        };
        let published = self
            .runtime
            .client()
            .try_borrow_mut()
            .map_err(|_| {
                HeartBeatFailure::Transport("keep-alive session is already borrowed".to_owned())
            })?
            .publish_txn_heart_beat(route.address(), &request, route.context(), &call);
        match published {
            PublishedCommand::BeforePublication(error)
            | PublishedCommand::AfterPublication { error, .. } => {
                Err(HeartBeatFailure::Transport(error))
            }
            PublishedCommand::Response(response) => {
                // A region error is transient: the next tick reroutes. A key
                // error is not — it means the lock this heartbeat exists to
                // refresh is gone.
                if let Some(region_error) = response.response.region_error.as_ref() {
                    return Err(HeartBeatFailure::Transport(format!("{region_error:?}")));
                }
                if let Some(error) = response.response.error.as_ref() {
                    return Err(HeartBeatFailure::Rejected(format!("{error:?}")));
                }
                Ok(response.response.lock_ttl)
            }
        }
    }
}

/// Real PD timestamp authority used by the one production transaction.
pub struct PdLockTimestampSource(PdClient);

impl fmt::Debug for PdLockTimestampSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PdLockTimestampSource")
            .finish_non_exhaustive()
    }
}

impl TimestampSource for PdLockTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.0.get_timestamp().map_err(|error| error.to_string())
    }
}
