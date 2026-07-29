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

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_pd_client::PdClient;
use tidb_proto::{
    KvrpcAssertionLevel, KvrpcBatchRollbackRequest, KvrpcCommitRequest, KvrpcCommitRole,
    KvrpcCommitTsExpired, KvrpcForUpdateTsConstraint, KvrpcGetRequest, KvrpcGetResponse,
    KvrpcKeyError, KvrpcPessimisticAction, KvrpcPrewriteRequest, KvrpcPrewriteResponse,
    KvrpcScanRequest, KvrpcScanResponse,
};

use crate::gc_state::{GcStateCache, TxnSafePointLoader, TxnSafePointRefresher, VisibilityError};
use crate::lock::{
    decode_lock_observation, resolve_optimistic_locks, LockRecoveryClient, LockRecoveryResult,
    TimestampSource,
};
use crate::region::{RegionBackoffBudget, RegionErrorDisposition, RegionRecoveryLoader};
use crate::rpc::{
    TonicCoprocessorClient, TransactionBatchPublication, TransactionBatchResponse, UnaryCallContext,
};
use crate::{PdRegionLoader, SharedReadOpener, SharedReadRuntime};

use super::command_client::{PublishedCommand, TransactionCommandClient};
use super::mutation::{validate_and_sort, validate_plan, MutationSetError, OptimisticMutation};
use super::region_batches::{
    group_keys, group_mutations, point_route, RegionKeyBatch, RegionMutationBatch,
};
use super::state::{
    CleanupBatchFailure, CleanupFailedTransaction, CommittedProtocol, CommittedTransaction,
    CoordinatorState, OptimisticCommitOutcome, OptimisticTransactionReceipt,
    OptimisticTransactionState, ReadOnlyTransaction, RolledBackTransaction, SecondaryCommitFailure,
    SnapshotReadReceipt, TransactionAttemptPhase, TransactionAttemptReceipt,
    TransactionAttemptResult, TransactionCause, UndeterminedTransaction,
};
use super::ttl::{HeartBeatFailure, LockKeepAlive, TxnHeartBeatSender, MANAGED_LOCK_TTL_MS};

const DEFAULT_LOCK_TTL_MS: u64 = 3_000;
/// Go `config.DefaultConfig().TiKVClient.AsyncCommit.KeysLimit`.
const ASYNC_COMMIT_KEYS_LIMIT: usize = 256;
/// Go `config.DefaultConfig().TiKVClient.AsyncCommit.TotalKeySizeLimit` (4 KiB).
const ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT: u64 = 4 * 1024;
/// Go `config.DefaultConfig().TiKVClient.AsyncCommit.SafeWindow` (2s).
const ASYNC_COMMIT_SAFE_WINDOW_MS: u64 = 2_000;
pub(super) const MAX_LOCK_ATTEMPTS: usize = 4;
/// Key/value pairs one snapshot scan returned, in key order.
pub type SnapshotScanPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Pairs one Scan page may return. client-go's `scanBatchSize`.
const SCAN_PAGE_LIMIT: u32 = 256;
const TSO_LOGICAL_BITS: u32 = 18;
const MAX_COMMIT_TS_DRIFT_MS: u64 = 60 * 60 * 1_000;

/// Concrete process/session authority errors rejected before a transaction opens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OptimisticCoordinatorError {
    /// PD and RegionCache must describe the same real cluster.
    ClusterMismatch {
        /// Cluster ID reported by the sole PD worker.
        pd: u64,
        /// Cluster ID reported by the shared RegionCache loader.
        region_cache: u64,
    },
    /// Real cluster identity cannot be zero.
    ZeroClusterId,
    /// Real PD timestamp allocation failed.
    Timestamp(String),
    /// The caller supplied an invalid mutation set.
    Mutations(MutationSetError),
    /// A real snapshot Get could not produce a determinate result.
    SnapshotGet(String),
    /// The data this transaction read may already have been garbage-collected.
    ///
    /// This is deliberately its own variant rather than a `SnapshotGet` string:
    /// it is terminal, never retryable at the same `start_ts`, and it maps to
    /// its own SQL error tier. Folding it into the generic bucket is what makes
    /// a GC-overtaken read look like a transport hiccup worth retrying.
    Visibility(VisibilityError),
    /// The txn safe point could not be loaded when the authority was built.
    GcState(String),
}

impl fmt::Display for OptimisticCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterMismatch { pd, region_cache } => write!(
                formatter,
                "PD cluster {pd} does not match RegionCache cluster {region_cache}"
            ),
            Self::ZeroClusterId => {
                formatter.write_str("real optimistic 2PC requires a nonzero cluster ID")
            }
            Self::Timestamp(error) => write!(formatter, "PD timestamp allocation failed: {error}"),
            Self::Mutations(error) => error.fmt(formatter),
            Self::SnapshotGet(error) => write!(formatter, "snapshot Get failed: {error}"),
            Self::Visibility(error) => error.fmt(formatter),
            Self::GcState(error) => write!(formatter, "txn safe point unavailable: {error}"),
        }
    }
}

impl std::error::Error for OptimisticCoordinatorError {}

/// Which faster-than-2PC commit protocols a transaction may attempt.
///
/// These are permissions, not decisions: TiKV can refuse either protocol on any
/// prewrite response, and the coordinator then finishes the transaction as a
/// normal two-phase commit. Both flags come from the session
/// (`@@tidb_enable_async_commit`, `@@tidb_enable_1pc`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitProtocol {
    /// `@@tidb_enable_async_commit`: commit at the completed prewrite, using
    /// `max(min_commit_ts)` instead of a second PD round trip.
    pub async_commit: bool,
    /// `@@tidb_enable_1pc`: let TiKV commit a single-region transaction inside
    /// the prewrite itself, so no Commit command is ever published.
    pub one_pc: bool,
}

impl CommitProtocol {
    /// The protocol set of a transaction that must use normal two-phase commit.
    #[must_use]
    pub const fn two_phase_only() -> Self {
        Self {
            async_commit: false,
            one_pc: false,
        }
    }
}

/// Result of one real transactional point Get at the transaction start timestamp.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGetResult {
    /// Real PD snapshot timestamp shared with later Prewrite.
    pub start_ts: u64,
    /// `None` means TiKV returned `not_found` at exactly `start_ts`.
    pub value: Option<Vec<u8>>,
    /// Exact region epoch that served the successful Get.
    pub region: crate::region::RegionVerId,
    /// Physical BatchCommands publication that produced the value.
    pub publication: TransactionBatchPublication,
}

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

    fn open(
        &self,
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
        let start_ts = self
            .pd
            .get_timestamp()
            .map_err(|error| OptimisticCoordinatorError::Timestamp(error.to_string()))?;
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
        super::RealPessimisticTransaction::from_transaction(two_pc, opened_at)
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

/// The one production pessimistic transaction.
pub type ProductionPessimisticTransaction = super::RealPessimisticTransaction<
    TonicCoprocessorClient,
    PdRegionLoader,
    PdLockTimestampSource,
>;

/// The one production transaction: real TiKV transport, real PD-backed region
/// topology, and real PD timestamps.
pub type ProductionOptimisticTransaction =
    RealOptimisticTransaction<TonicCoprocessorClient, PdRegionLoader, PdLockTimestampSource>;

/// One concrete normal optimistic transaction fixed to a real PD `start_ts`.
///
/// The type parameters name the capabilities this coordinator consumes — the
/// shared TiKV client, the region topology authority, and the timestamp
/// authority. They do not admit a second transaction implementation: the only
/// production instantiation is [`ProductionOptimisticTransaction`], built by
/// [`RealOptimisticTransactionOpener::begin`].
pub struct RealOptimisticTransaction<C, L, T> {
    runtime: SharedReadRuntime<C, L>,
    timestamps: T,
    timeout: Duration,
    start_ts: u64,
    planned_mutation_count: usize,
    planned_aggregate_bytes: usize,
    state: CoordinatorState,
    snapshot_reads: Vec<SnapshotReadReceipt>,
    opened_at: Instant,
    authority_id: u64,
    forward_backoff: RegionBackoffBudget,
    secondary_backoff: RegionBackoffBudget,
    cleanup_backoff: RegionBackoffBudget,
    pessimistic: Option<PessimisticPrewritePlan>,
    /// The store-wide txn safe point every read from this transaction is
    /// validated against once TiKV has answered.
    gc_state: Arc<GcStateCache>,
    protocol: CommitProtocol,
}

/// What a pessimistic transaction already proved before it reached Prewrite.
///
/// Prewrite of a pessimistic transaction is not a second conflict check: for
/// every key whose pessimistic lock this transaction still holds, TiKV must
/// verify the lock instead of re-checking for write conflicts, which is what
/// makes the statement-level `for_update_ts` retry safe. A key that was never
/// locked — a pure insert of a new row, for instance — keeps the optimistic
/// check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PessimisticPrewritePlan {
    /// Latest statement timestamp under which locks were acquired.
    pub(super) for_update_ts: u64,
    /// Exact encoded keys this transaction holds a pessimistic lock on.
    pub(super) locked_keys: std::collections::BTreeSet<Vec<u8>>,
    /// Keys whose lock fair locking granted at a timestamp higher than the
    /// transaction's `for_update_ts`, and that exact timestamp.
    ///
    /// Go `twoPhaseCommitter.forUpdateTSConstraints`: Prewrite carries one
    /// `for_update_ts` for the whole request, so a lock taken with conflict
    /// must name its own, or TiKV would verify a lock that is not there.
    pub(super) for_update_ts_constraints: std::collections::BTreeMap<Vec<u8>, u64>,
}

impl<C, L, T> RealOptimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    /// Builds one transaction over already-owned authorities and an already
    /// allocated `start_ts`.
    ///
    /// This is the single construction path;
    /// [`RealOptimisticTransactionOpener::begin`] is the production caller and
    /// supplies the real PD timestamp plus the instant it began opening, so
    /// the lock TTL keeps charging for session-open and TSO time. Focused
    /// tests use it to reach decoded-response branches that a live cluster
    /// cannot produce on demand; it creates no PD, RegionCache, or transport
    /// worker of its own.
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
            // A transaction built without a store has no GC authority to share.
            // A zero txn safe point is not a bypass — it is what PD reports on
            // a cluster where GC has never advanced, so the same comparison
            // runs and simply admits every timestamp.
            Arc::new(GcStateCache::seeded(0, Instant::now())),
        )
    }

    /// Builds one already-opened transaction whose plan the caller validated.
    ///
    /// A read-only transaction legitimately has a zero mutation plan, which
    /// [`validate_plan`] rejects, so the plan check stays with the callers that
    /// intend to write.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_opened(
        runtime: SharedReadRuntime<C, L>,
        timestamps: T,
        timeout: Duration,
        start_ts: u64,
        opened_at: Instant,
        planned_mutation_count: usize,
        planned_aggregate_bytes: usize,
        gc_state: Arc<GcStateCache>,
    ) -> Result<Self, OptimisticCoordinatorError> {
        if start_ts == 0 {
            return Err(OptimisticCoordinatorError::Timestamp(
                "a transaction requires a real nonzero start timestamp".to_owned(),
            ));
        }
        let authority_id = runtime.authority_id();
        Ok(Self {
            runtime,
            timestamps,
            timeout,
            start_ts,
            planned_mutation_count,
            planned_aggregate_bytes,
            state: CoordinatorState::New,
            snapshot_reads: Vec::new(),
            opened_at,
            authority_id,
            forward_backoff: RegionBackoffBudget::campaign_default(),
            secondary_backoff: RegionBackoffBudget::campaign_default(),
            cleanup_backoff: RegionBackoffBudget::campaign_default(),
            pessimistic: None,
            gc_state,
            protocol: CommitProtocol::two_phase_only(),
        })
    }

    /// Permits this transaction to attempt async commit and/or 1PC.
    pub fn set_commit_protocol(&mut self, protocol: CommitProtocol) {
        self.protocol = protocol;
    }

    /// Rejects a completed read whose `start_ts` GC has already passed.
    ///
    /// Called only after TiKV has answered, mirroring client-go's placement of
    /// `CheckVisibility` at the end of `snapshot.get` and `snapshot.scan`. A
    /// pre-read check would be worthless: GC can advance while the RPC is in
    /// flight, so only a post-read check covers the data actually returned.
    fn check_visibility(&self) -> Result<(), OptimisticCoordinatorError> {
        self.gc_state
            .check_visibility(self.start_ts)
            .map_err(OptimisticCoordinatorError::Visibility)
    }

    /// Binds the pessimistic locks a caller already acquired at `start_ts`.
    pub(super) fn set_pessimistic_prewrite(&mut self, plan: PessimisticPrewritePlan) {
        self.pessimistic = Some(plan);
    }

    pub(super) const fn runtime(&self) -> &SharedReadRuntime<C, L> {
        &self.runtime
    }

    pub(super) const fn timestamps(&self) -> &T {
        &self.timestamps
    }

    pub(super) const fn call_timeout(&self) -> Duration {
        self.timeout
    }

    /// Snapshot timestamp allocated before any read or write.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Shared process authority identity used by reads and writes.
    #[must_use]
    pub const fn authority_id(&self) -> u64 {
        self.authority_id
    }

    /// Reads one encoded key at this transaction's exact start timestamp.
    pub fn snapshot_get(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<SnapshotGetResult, OptimisticCoordinatorError> {
        if key.is_empty() {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "encoded key is empty".to_owned(),
            ));
        }
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut lock_attempts = 0usize;
        loop {
            let route = point_route(&self.runtime, key)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            let request = KvrpcGetRequest {
                key: key.to_vec(),
                version: self.start_ts,
                need_commit_ts: true,
                ..KvrpcGetRequest::default()
            };
            let response = self.begin_get(&route, &request, call)?;
            if let Some(region_error) = response.response.region_error.as_ref() {
                self.recover_region_error(
                    RecoveryPhase::Forward,
                    region_error,
                    route.attempt(),
                    call,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                continue;
            }
            if let Some(key_error) = response.response.error.as_ref() {
                if let Some(lock_info) = key_error.locked.as_ref() {
                    let locks = decode_lock_observation(lock_info).map_err(|error| {
                        OptimisticCoordinatorError::SnapshotGet(error.to_string())
                    })?;
                    match resolve_optimistic_locks(
                        &self.runtime,
                        &locks,
                        self.start_ts,
                        route.context(),
                        call,
                        &self.timestamps,
                    )
                    .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?
                    {
                        LockRecoveryResult::Resolved(_) if lock_attempts < MAX_LOCK_ATTEMPTS => {
                            lock_attempts += 1;
                            continue;
                        }
                        LockRecoveryResult::Resolved(_) => {
                            return Err(OptimisticCoordinatorError::SnapshotGet(
                                "snapshot lock retry budget exhausted".to_owned(),
                            ));
                        }
                        LockRecoveryResult::Alive(wait) => {
                            if lock_attempts >= MAX_LOCK_ATTEMPTS {
                                return Err(OptimisticCoordinatorError::SnapshotGet(
                                    "snapshot lock retry budget exhausted".to_owned(),
                                ));
                            }
                            wait_with_call(call, alive_retry_delay(wait)).map_err(|error| {
                                OptimisticCoordinatorError::SnapshotGet(error.to_string())
                            })?;
                            lock_attempts += 1;
                            continue;
                        }
                    }
                }
                return Err(OptimisticCoordinatorError::SnapshotGet(format!(
                    "TiKV key error: {key_error:?}"
                )));
            }
            self.check_visibility()?;
            let value = if response.response.not_found {
                None
            } else {
                Some(response.response.value)
            };
            let result = SnapshotGetResult {
                start_ts: self.start_ts,
                value,
                region: route.region(),
                publication: response.publication,
            };
            self.snapshot_reads.push(SnapshotReadReceipt {
                key: key.to_vec(),
                region: route.region(),
                publication: result.publication.clone(),
            });
            return Ok(result);
        }
    }

    /// Reads every pair in `[start_key, end_key)` at this transaction's exact
    /// start timestamp.
    ///
    /// One Scan is answered by one region, so this walks the range region by
    /// region and, inside a region, page by page until the region is drained.
    /// Because every page is read at the same `start_ts`, a concurrent DDL
    /// cannot make the caller see half of one schema version and half of
    /// another — that single-snapshot property is what makes this usable as a
    /// catalog read.
    pub fn snapshot_scan(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<SnapshotScanPairs, OptimisticCoordinatorError> {
        if start_key.is_empty() {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "scan start key is empty".to_owned(),
            ));
        }
        if end_key.is_empty() || end_key <= start_key {
            return Err(OptimisticCoordinatorError::SnapshotGet(
                "scan range must be a non-empty [start, end)".to_owned(),
            ));
        }
        self.state
            .transition(CoordinatorState::Reading)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let mut pairs = Vec::new();
        let mut cursor = start_key.to_vec();
        let mut lock_attempts = 0usize;
        while cursor.as_slice() < end_key {
            let route = point_route(&self.runtime, &cursor)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            // TiKV stops at the region boundary anyway; naming it keeps the
            // cursor advance exact when a page ends flush with the region.
            let region_end = route.region_end_key().to_vec();
            let page_end = if region_end.is_empty() || region_end.as_slice() > end_key {
                end_key.to_vec()
            } else {
                region_end.clone()
            };
            let request = KvrpcScanRequest {
                start_key: cursor.clone(),
                end_key: page_end.clone(),
                limit: SCAN_PAGE_LIMIT,
                version: self.start_ts,
                ..KvrpcScanRequest::default()
            };
            let response = self.begin_scan(&route, &request, call)?;
            if let Some(region_error) = response.response.region_error.as_ref() {
                self.recover_region_error(
                    RecoveryPhase::Forward,
                    region_error,
                    route.attempt(),
                    call,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
                continue;
            }
            let mut locked = Vec::new();
            if let Some(key_error) = response.response.error.as_ref() {
                collect_scan_lock(key_error, &mut locked)?;
            }
            for pair in &response.response.pairs {
                if let Some(key_error) = pair.error.as_ref() {
                    collect_scan_lock(key_error, &mut locked)?;
                }
            }
            if !locked.is_empty() {
                if lock_attempts >= MAX_LOCK_ATTEMPTS {
                    return Err(OptimisticCoordinatorError::SnapshotGet(
                        "scan lock retry budget exhausted".to_owned(),
                    ));
                }
                lock_attempts += 1;
                match resolve_optimistic_locks(
                    &self.runtime,
                    &locked,
                    self.start_ts,
                    route.context(),
                    call,
                    &self.timestamps,
                )
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?
                {
                    LockRecoveryResult::Resolved(_) => {}
                    LockRecoveryResult::Alive(wait) => {
                        wait_with_call(call, alive_retry_delay(wait)).map_err(|error| {
                            OptimisticCoordinatorError::SnapshotGet(error.to_string())
                        })?;
                    }
                }
                // Redo this page: a locked scan returns no trustworthy pairs.
                continue;
            }
            // Per page, as client-go checks per `scan.Next` batch: a long scan
            // must not spend its whole range on the strength of one check made
            // before the first page.
            self.check_visibility()?;
            let page_len = response.response.pairs.len();
            let last_key = response
                .response
                .pairs
                .last()
                .map(|pair| pair.key.clone())
                .unwrap_or_default();
            for pair in response.response.pairs {
                pairs.push((pair.key, pair.value));
            }
            self.snapshot_reads.push(SnapshotReadReceipt {
                key: cursor.clone(),
                region: route.region(),
                publication: response.publication,
            });
            if page_len == SCAN_PAGE_LIMIT as usize {
                // The page filled up; the next key after the last one served
                // is the smallest key this page could not have covered.
                cursor = last_key;
                cursor.push(0);
            } else {
                // The region (or the requested range) is drained.
                if page_end.as_slice() >= end_key {
                    break;
                }
                cursor = page_end;
            }
        }
        Ok(pairs)
    }

    fn begin_scan(
        &self,
        route: &RegionKeyBatch,
        request: &KvrpcScanRequest,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchResponse<KvrpcScanResponse>, OptimisticCoordinatorError> {
        let published = self
            .runtime
            .client()
            .try_borrow_mut()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "TiKV client is already borrowed".to_owned(),
                )
            })?
            .publish_transaction_scan(route.address(), request, route.context(), call);
        match published {
            PublishedCommand::Response(response) => Ok(response),
            PublishedCommand::BeforePublication(error)
            | PublishedCommand::AfterPublication { error, .. } => {
                Err(OptimisticCoordinatorError::SnapshotGet(error))
            }
        }
    }

    /// Completes a missing or unchanged UPDATE with no write publication.
    pub fn finish_without_writes(
        mut self,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError> {
        self.state
            .transition(CoordinatorState::ReadOnly)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        Ok(ReadOnlyTransaction {
            authority_id: self.authority_id,
            start_ts: self.start_ts,
            state: OptimisticTransactionState::ReadOnly,
            snapshot_reads: self.snapshot_reads,
        })
    }

    /// Consumes this snapshot into one normal optimistic two-phase commit.
    pub fn commit(
        mut self,
        mutations: Vec<OptimisticMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        let mutations =
            validate_and_sort(mutations).map_err(OptimisticCoordinatorError::Mutations)?;
        let actual_bytes = mutations
            .iter()
            .try_fold(0usize, |size, mutation| {
                size.checked_add(mutation.key().len())?
                    .checked_add(mutation.value().len())
            })
            .unwrap_or(usize::MAX);
        if mutations.len() > self.planned_mutation_count {
            return Err(OptimisticCoordinatorError::Mutations(
                MutationSetError::TooManyMutations {
                    count: mutations.len(),
                    limit: self.planned_mutation_count,
                },
            ));
        }
        if actual_bytes > self.planned_aggregate_bytes {
            return Err(OptimisticCoordinatorError::Mutations(
                MutationSetError::TransactionTooLarge {
                    size: actual_bytes,
                    limit: self.planned_aggregate_bytes,
                },
            ));
        }
        self.state
            .transition(CoordinatorState::Prewriting)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        let primary_key = mutations[0].key().to_vec();
        let mut receipt = OptimisticTransactionReceipt::new(
            self.authority_id,
            self.start_ts,
            primary_key.clone(),
            mutations.len(),
        );
        let lock_ttl_ms = transaction_lock_ttl_ms(self.opened_at, actual_bytes);
        receipt.lock_ttl_ms = lock_ttl_ms;
        let mut possibly_prewrite_keys = Vec::<Vec<u8>>::new();
        let mut min_commit_ts = self.start_ts.saturating_add(1);
        let mut protocol = self.attempted_protocol(&mutations, &primary_key);
        let mut queue = match group_mutations(&self.runtime, &mutations) {
            Ok(batches) => {
                protocol.observe_batch_count(batches.len());
                VecDeque::from(
                    batches
                        .into_iter()
                        .map(|batch| (batch, false))
                        .collect::<Vec<_>>(),
                )
            }
            Err(error) => {
                return Ok(self.rollback_after_failure(
                    receipt,
                    &[],
                    TransactionCause::Region {
                        detail: format!("initial region grouping failed: {error}"),
                    },
                ));
            }
        };
        let mut lock_attempts = 0usize;

        while let Some((batch, is_retry)) = queue.pop_front() {
            receipt.region_attempts.push(batch.region());
            let published_keys = batch.keys();
            match self.prewrite_batch(
                &batch,
                &primary_key,
                mutations.len(),
                lock_ttl_ms,
                is_retry,
                &protocol,
                call,
            ) {
                PublishedCommand::Response(response) => {
                    possibly_prewrite_keys.extend(published_keys.iter().cloned());
                    receipt
                        .prewrite_attempt_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        let region_cause = TransactionCause::Region {
                            detail: format!("Prewrite region retry: {region_error:?}"),
                        };
                        if let Err(cause) = self.recover_region_error(
                            RecoveryPhase::Forward,
                            region_error,
                            batch.attempt(),
                            call,
                        ) {
                            record_attempt(
                                &mut receipt,
                                TransactionAttemptPhase::Prewrite,
                                &published_keys,
                                &batch,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                            );
                            return Ok(self.rollback_after_failure(
                                receipt,
                                &possibly_prewrite_keys,
                                cause,
                            ));
                        }
                        record_attempt(
                            &mut receipt,
                            TransactionAttemptPhase::Prewrite,
                            &published_keys,
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Retry(region_cause),
                        );
                        match group_mutations(&self.runtime, batch.mutations()) {
                            Ok(regrouped) => {
                                protocol.observe_batch_count(regrouped.len());
                                for regrouped_batch in regrouped.into_iter().rev() {
                                    queue.push_front((regrouped_batch, true));
                                }
                                continue;
                            }
                            Err(error) => {
                                return Ok(self.rollback_after_failure(
                                    receipt,
                                    &possibly_prewrite_keys,
                                    TransactionCause::Region {
                                        detail: format!("cannot regroup Prewrite keys: {error}"),
                                    },
                                ));
                            }
                        }
                    }
                    if !response.response.errors.is_empty() {
                        match self.handle_prewrite_key_errors(
                            &response.response.errors,
                            batch.context(),
                            call,
                        ) {
                            Ok(()) if lock_attempts < MAX_LOCK_ATTEMPTS => {
                                record_attempt(
                                    &mut receipt,
                                    TransactionAttemptPhase::Prewrite,
                                    &published_keys,
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::Retry(TransactionCause::Lock {
                                        key: primary_key.clone(),
                                        detail: "Prewrite lock resolved or waited; retrying at the same start_ts".to_owned(),
                                    }),
                                );
                                lock_attempts += 1;
                                queue.push_front((batch, true));
                                continue;
                            }
                            Ok(()) => {
                                let cause = TransactionCause::Lock {
                                    key: primary_key.clone(),
                                    detail: "Prewrite lock retry budget exhausted".to_owned(),
                                };
                                record_attempt(
                                    &mut receipt,
                                    TransactionAttemptPhase::Prewrite,
                                    &published_keys,
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                return Ok(self.rollback_after_failure(
                                    receipt,
                                    &possibly_prewrite_keys,
                                    cause,
                                ));
                            }
                            Err(cause) => {
                                record_attempt(
                                    &mut receipt,
                                    TransactionAttemptPhase::Prewrite,
                                    &published_keys,
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                return Ok(self.rollback_after_failure(
                                    receipt,
                                    &possibly_prewrite_keys,
                                    cause,
                                ));
                            }
                        }
                    }
                    if let Err(cause) = protocol.observe_prewrite_response(&response.response) {
                        record_attempt(
                            &mut receipt,
                            TransactionAttemptPhase::Prewrite,
                            &published_keys,
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        return Ok(self.rollback_after_failure(
                            receipt,
                            &possibly_prewrite_keys,
                            cause,
                        ));
                    }
                    min_commit_ts = min_commit_ts.max(response.response.min_commit_ts);
                    record_attempt(
                        &mut receipt,
                        TransactionAttemptPhase::Prewrite,
                        &published_keys,
                        &batch,
                        Some(response.publication.clone()),
                        TransactionAttemptResult::Confirmed,
                    );
                    receipt.prewrite_publications.push(response.publication);
                }
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("Prewrite failed before publication: {error}"),
                    };
                    record_attempt(
                        &mut receipt,
                        TransactionAttemptPhase::Prewrite,
                        &published_keys,
                        &batch,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    return Ok(self.rollback_after_failure(
                        receipt,
                        &possibly_prewrite_keys,
                        cause,
                    ));
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    possibly_prewrite_keys.extend(published_keys.iter().cloned());
                    receipt
                        .prewrite_attempt_publications
                        .push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!("Prewrite completion failed after publication: {error}"),
                    };
                    record_attempt(
                        &mut receipt,
                        TransactionAttemptPhase::Prewrite,
                        &published_keys,
                        &batch,
                        Some(publication),
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    return Ok(self.rollback_after_failure(
                        receipt,
                        &possibly_prewrite_keys,
                        cause,
                    ));
                }
            }
        }

        self.state
            .transition(CoordinatorState::Prewritten)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;

        let all_mutation_keys = mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect::<Vec<_>>();

        // 1PC: TiKV already committed every key while answering the prewrite,
        // so publishing a Commit would be a second, contradictory decision.
        if protocol.use_one_pc {
            if protocol.one_pc_commit_ts == 0 {
                return Ok(self.rollback_after_failure(
                    receipt,
                    &possibly_prewrite_keys,
                    TransactionCause::InvalidResponse {
                        detail: "1PC prewrite reported success without a commit timestamp"
                            .to_owned(),
                    },
                ));
            }
            receipt.commit_ts = protocol.one_pc_commit_ts;
            receipt.commit_protocol = CommittedProtocol::OnePc;
            self.state
                .transition(CoordinatorState::OnePcCommitted)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            self.state
                .transition(CoordinatorState::Committed)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            return Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
                receipt,
                secondary_failures: Vec::new(),
            }));
        }

        // Async commit: the completed prewrite is the commit point and
        // `max(min_commit_ts)` is the commit timestamp, so no second PD round
        // trip happens. The Commit commands below only make the decision
        // visible without a lock resolution; failing them cannot un-commit the
        // transaction, which is why they are reported as secondary failures.
        if protocol.use_async_commit {
            receipt.commit_ts = min_commit_ts;
            receipt.commit_protocol = CommittedProtocol::AsyncCommit;
            self.state
                .transition(CoordinatorState::AsyncCommitted)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            let failures = self.commit_secondaries(
                &all_mutation_keys,
                &primary_key,
                min_commit_ts,
                true,
                &mut receipt,
            );
            self.state
                .transition(CoordinatorState::Committed)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
            return Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
                receipt,
                secondary_failures: failures,
            }));
        }

        let commit_ts = match self.commit_timestamp(min_commit_ts, call) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                return Ok(self.rollback_after_failure(receipt, &possibly_prewrite_keys, error));
            }
        };
        receipt.commit_ts = commit_ts;

        self.state
            .transition(CoordinatorState::PrimaryCommitting)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;

        let committed_primary_batch_keys = match self.commit_primary(
            &all_mutation_keys,
            &primary_key,
            commit_ts,
            call,
            &mut receipt,
        ) {
            PrimaryResult::Committed(keys) => keys,
            PrimaryResult::DefinitiveFailure(error) => {
                return Ok(self.rollback_after_failure(receipt, &possibly_prewrite_keys, error));
            }
            PrimaryResult::Undetermined(error) => {
                self.state
                    .transition(CoordinatorState::Undetermined)
                    .map_err(|cause| OptimisticCoordinatorError::SnapshotGet(cause.to_string()))?;
                return Ok(OptimisticCommitOutcome::Undetermined(
                    UndeterminedTransaction {
                        receipt,
                        cause: error,
                    },
                ));
            }
        };
        self.state
            .transition(CoordinatorState::PrimaryCommitted)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;

        let secondary_keys = mutations
            .iter()
            .filter(|mutation| {
                !committed_primary_batch_keys
                    .iter()
                    .any(|key| key.as_slice() == mutation.key())
            })
            .map(|mutation| mutation.key().to_vec())
            .collect::<Vec<_>>();
        if !secondary_keys.is_empty() {
            self.state
                .transition(CoordinatorState::SecondariesCommitting)
                .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        }
        let secondary_failures = self.commit_secondaries(
            &secondary_keys,
            &primary_key,
            receipt.commit_ts,
            false,
            &mut receipt,
        );
        self.state
            .transition(CoordinatorState::Committed)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?;
        Ok(OptimisticCommitOutcome::Committed(CommittedTransaction {
            receipt,
            secondary_failures,
        }))
    }

    fn begin_get(
        &self,
        route: &RegionKeyBatch,
        request: &KvrpcGetRequest,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchResponse<KvrpcGetResponse>, OptimisticCoordinatorError> {
        let published = self
            .runtime
            .client()
            .try_borrow_mut()
            .map_err(|_| {
                OptimisticCoordinatorError::SnapshotGet(
                    "TiKV client is already borrowed".to_owned(),
                )
            })?
            .publish_transaction_get(route.address(), request, route.context(), call);
        match published {
            PublishedCommand::Response(response) => Ok(response),
            PublishedCommand::BeforePublication(error)
            | PublishedCommand::AfterPublication { error, .. } => {
                Err(OptimisticCoordinatorError::SnapshotGet(error))
            }
        }
    }

    /// Decides which faster-than-2PC protocols this exact mutation set may
    /// still attempt.
    ///
    /// Go `twoPhaseCommitter.checkAsyncCommit`/`checkOnePC`: the session
    /// permission is necessary but not sufficient, because an async-commit
    /// primary lock has to carry every secondary key and a lock that large
    /// would cost more to write and to recover than the saved round trip.
    fn attempted_protocol(
        &self,
        mutations: &[OptimisticMutation],
        primary_key: &[u8],
    ) -> AttemptedProtocol {
        let total_key_bytes = mutations
            .iter()
            .map(|mutation| mutation.key().len() as u64)
            .sum::<u64>();
        let use_async_commit = self.protocol.async_commit
            && mutations.len() <= ASYNC_COMMIT_KEYS_LIMIT
            && total_key_bytes <= ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT;
        AttemptedProtocol {
            use_async_commit,
            use_one_pc: self.protocol.one_pc,
            max_commit_ts: if use_async_commit {
                self.max_commit_ts()
            } else {
                0
            },
            one_pc_commit_ts: 0,
            secondaries: if use_async_commit {
                mutations
                    .iter()
                    .filter(|mutation| mutation.key() != primary_key)
                    .map(|mutation| mutation.key().to_vec())
                    .collect()
            } else {
                Vec::new()
            },
        }
    }

    /// The latest commit timestamp an async-commit prewrite may be granted.
    ///
    /// Go `calculateMaxCommitTS`: a synthetic "now" is derived from the elapsed
    /// wall time since the transaction opened, and the safe window is added on
    /// top. Bounding the commit timestamp is what keeps a schema version valid
    /// for the whole life of the commit even though no PD timestamp is taken.
    fn max_commit_ts(&self) -> u64 {
        let elapsed_ms = u64::try_from(self.opened_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let current_ts = (elapsed_ms << TSO_LOGICAL_BITS).saturating_add(self.start_ts);
        (ASYNC_COMMIT_SAFE_WINDOW_MS << TSO_LOGICAL_BITS).saturating_add(current_ts)
    }

    #[allow(clippy::too_many_arguments)]
    fn prewrite_batch(
        &self,
        batch: &RegionMutationBatch,
        primary_key: &[u8],
        transaction_size: usize,
        lock_ttl_ms: u64,
        is_retry: bool,
        protocol: &AttemptedProtocol,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPrewriteResponse> {
        let mut request = KvrpcPrewriteRequest {
            mutations: batch
                .mutations()
                .iter()
                .map(OptimisticMutation::to_proto)
                .collect(),
            primary_lock: primary_key.to_vec(),
            start_version: self.start_ts,
            lock_ttl: lock_ttl_ms,
            txn_size: u64::try_from(transaction_size).unwrap_or(u64::MAX),
            min_commit_ts: self.start_ts.saturating_add(1),
            max_commit_ts: protocol.max_commit_ts,
            use_async_commit: protocol.use_async_commit,
            try_one_pc: protocol.use_one_pc,
            assertion_level: KvrpcAssertionLevel::Strict as i32,
            ..KvrpcPrewriteRequest::default()
        };
        // Only the primary lock names the secondaries; that is what makes the
        // primary the single entry point for recovering the transaction.
        if protocol.use_async_commit
            && batch
                .mutations()
                .iter()
                .any(|mutation| mutation.key() == primary_key)
        {
            request.secondaries = protocol.secondaries.clone();
        }
        if let Some(plan) = self.pessimistic.as_ref() {
            request.for_update_ts = plan.for_update_ts;
            request.min_commit_ts = plan.for_update_ts.saturating_add(1);
            request.pessimistic_actions = batch
                .mutations()
                .iter()
                .map(|mutation| {
                    if plan.locked_keys.contains(mutation.key()) {
                        KvrpcPessimisticAction::DoPessimisticCheck as i32
                    } else {
                        KvrpcPessimisticAction::SkipPessimisticCheck as i32
                    }
                })
                .collect();
            request.for_update_ts_constraints = batch
                .mutations()
                .iter()
                .enumerate()
                .filter_map(|(index, mutation)| {
                    let expected = *plan.for_update_ts_constraints.get(mutation.key())?;
                    Some(KvrpcForUpdateTsConstraint {
                        index: u32::try_from(index).unwrap_or(u32::MAX),
                        expected_for_update_ts: expected,
                    })
                })
                .collect();
        }
        request.context = None;
        let mut context = batch.context().clone();
        context.is_retry_request = is_retry;
        match self.runtime.client().try_borrow_mut() {
            Ok(mut client) => client.publish_prewrite(batch.address(), &request, &context, call),
            Err(_) => PublishedCommand::BeforePublication(
                "TiKV client is already borrowed while publishing Prewrite".to_owned(),
            ),
        }
    }

    fn handle_prewrite_key_errors(
        &self,
        errors: &[KvrpcKeyError],
        context: &tidb_proto::KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<(), TransactionCause> {
        let mut eligible_locks = Vec::new();
        for error in errors {
            let Some(lock_info) = error.locked.as_ref() else {
                return Err(classify_key_error(error));
            };
            let locks = decode_lock_observation(lock_info).map_err(|error| {
                TransactionCause::InvalidResponse {
                    detail: format!("invalid Prewrite lock observation: {error}"),
                }
            })?;
            for lock in locks {
                if lock.txn_id > self.start_ts {
                    return Err(TransactionCause::WriteConflict {
                        detail: format!(
                            "Prewrite observed newer optimistic lock txn_id={} start_ts={}",
                            lock.txn_id, self.start_ts
                        ),
                    });
                }
                eligible_locks.push(lock);
            }
        }
        if eligible_locks.is_empty() {
            return Err(TransactionCause::InvalidResponse {
                detail: "Prewrite returned an empty KeyError set".to_owned(),
            });
        }
        match resolve_optimistic_locks(
            &self.runtime,
            &eligible_locks,
            self.start_ts,
            context,
            call,
            &self.timestamps,
        )
        .map_err(|error| TransactionCause::Lock {
            key: eligible_locks[0].key.clone(),
            detail: format!("Prewrite lock recovery failed: {error}"),
        })? {
            LockRecoveryResult::Resolved(_) => Ok(()),
            LockRecoveryResult::Alive(wait) if alive_retry_delay(wait) <= call.timeout() => {
                wait_with_call(call, alive_retry_delay(wait))?;
                Ok(())
            }
            LockRecoveryResult::Alive(wait) => Err(TransactionCause::Lock {
                key: eligible_locks[0].key.clone(),
                detail: format!(
                    "Prewrite lock remains alive for {wait:?}, beyond transaction deadline"
                ),
            }),
        }
    }

    pub(super) fn recover_region_error(
        &mut self,
        phase: RecoveryPhase,
        error: &tidb_proto::RegionError,
        attempt: &crate::region::RegionAttempt,
        call: &UnaryCallContext,
    ) -> Result<(), TransactionCause> {
        let backoff = match phase {
            RecoveryPhase::Forward => &mut self.forward_backoff,
            RecoveryPhase::Secondary => &mut self.secondary_backoff,
            RecoveryPhase::Cleanup => &mut self.cleanup_backoff,
        };
        let disposition = self
            .runtime
            .region_cache_handle()
            .on_region_error(error, attempt.clone(), backoff)
            .map_err(|error| TransactionCause::Region {
                detail: format!("RegionCache recovery lifecycle failed: {error}"),
            })?
            .map_err(|error| TransactionCause::Region {
                detail: format!("RegionCache rejected region error: {error}"),
            })?;
        let delay = match disposition {
            RegionErrorDisposition::RetryRoute { delay, .. }
            | RegionErrorDisposition::RetrySelector { delay, .. }
            | RegionErrorDisposition::RebuildRanges { delay, .. } => delay,
            RegionErrorDisposition::ReturnRegionError => {
                return Err(TransactionCause::Region {
                    detail: format!("TiKV returned non-retryable region error: {error:?}"),
                });
            }
            RegionErrorDisposition::Terminal(terminal) => {
                return Err(TransactionCause::Region {
                    detail: format!("TiKV returned terminal region error: {terminal:?}"),
                });
            }
        };
        wait_with_call(call, delay)
    }

    fn commit_timestamp(
        &self,
        minimum: u64,
        call: &UnaryCallContext,
    ) -> Result<u64, TransactionCause> {
        for _ in 0..MAX_LOCK_ATTEMPTS {
            if call.cancellation().is_cancelled() || call.timeout().is_zero() {
                return Err(TransactionCause::Transport {
                    detail: "commit timestamp allocation was cancelled".to_owned(),
                });
            }
            let timestamp =
                self.timestamps
                    .current_ts()
                    .map_err(|error| TransactionCause::Timestamp {
                        detail: format!("cannot allocate commit timestamp: {error}"),
                    })?;
            if call.cancellation().is_cancelled() || call.timeout().is_zero() {
                return Err(TransactionCause::Transport {
                    detail: "commit timestamp completed after cancellation".to_owned(),
                });
            }
            if timestamp > self.start_ts && timestamp >= minimum {
                return Ok(timestamp);
            }
        }
        Err(TransactionCause::Timestamp {
            detail: format!(
                "PD did not return commit_ts >= {minimum} and > {}",
                self.start_ts
            ),
        })
    }

    fn commit_primary(
        &mut self,
        primary_batch_keys: &[Vec<u8>],
        primary_key: &[u8],
        mut commit_ts: u64,
        call: &UnaryCallContext,
        receipt: &mut OptimisticTransactionReceipt,
    ) -> PrimaryResult {
        let mut attempt = 0usize;
        loop {
            let routes = match group_keys(&self.runtime, primary_batch_keys) {
                Ok(routes) => routes,
                Err(error) => {
                    return PrimaryResult::DefinitiveFailure(TransactionCause::Region {
                        detail: format!("primary Commit regroup failed: {error}"),
                    });
                }
            };
            let Some(route) = routes
                .into_iter()
                .find(|batch| batch.keys().iter().any(|key| key.as_slice() == primary_key))
            else {
                return PrimaryResult::DefinitiveFailure(TransactionCause::InvalidResponse {
                    detail: "primary Commit regroup lost deterministic primary key".to_owned(),
                });
            };
            receipt.region_attempts.push(route.region());
            let request = KvrpcCommitRequest {
                start_version: self.start_ts,
                keys: route.keys().to_vec(),
                commit_version: commit_ts,
                commit_role: KvrpcCommitRole::Primary as i32,
                primary_key: primary_key.to_vec(),
                use_async_commit: false,
                ..KvrpcCommitRequest::default()
            };
            let mut context = route.context().clone();
            context.is_retry_request = attempt > 0;
            let published = match self.runtime.client().try_borrow_mut() {
                Ok(mut client) => client.publish_commit(route.address(), &request, &context, call),
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing primary Commit".to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("primary Commit failed before publication: {error}"),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::PrimaryCommit,
                        route.keys(),
                        &route,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    return PrimaryResult::DefinitiveFailure(cause);
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    receipt.primary_publications.push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!(
                            "primary Commit completion failed after publication: {error}"
                        ),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::PrimaryCommit,
                        route.keys(),
                        &route,
                        Some(publication),
                        TransactionAttemptResult::Ambiguous(cause.clone()),
                    );
                    return PrimaryResult::Undetermined(cause);
                }
                PublishedCommand::Response(response) => {
                    receipt
                        .primary_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        if primary_region_response_is_ambiguous(region_error) {
                            let cause = TransactionCause::Region {
                                detail: format!("primary Commit returned undetermined region error: {region_error:?}"),
                            };
                            record_attempt(
                                receipt,
                                TransactionAttemptPhase::PrimaryCommit,
                                route.keys(),
                                &route,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::Ambiguous(cause.clone()),
                            );
                            return PrimaryResult::Undetermined(cause);
                        }
                        if let Err(cause) = self.recover_region_error(
                            RecoveryPhase::Forward,
                            region_error,
                            route.attempt(),
                            call,
                        ) {
                            // A decoded region error definitively rejected this
                            // publication. Later local recovery failure cannot
                            // turn that rejected attempt into ambiguity.
                            record_attempt(
                                receipt,
                                TransactionAttemptPhase::PrimaryCommit,
                                route.keys(),
                                &route,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                            );
                            return PrimaryResult::DefinitiveFailure(cause);
                        }
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::PrimaryCommit,
                            route.keys(),
                            &route,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Retry(TransactionCause::Region {
                                detail: format!("primary Commit region retry: {region_error:?}"),
                            }),
                        );
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                    if let Some(error) = response.response.error.as_ref() {
                        if let Some(expired) = error.commit_ts_expired.as_ref() {
                            let minimum = match validate_commit_ts_expired(
                                expired,
                                self.start_ts,
                                primary_key,
                                commit_ts,
                            ) {
                                Ok(minimum) => minimum,
                                Err(cause) => {
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::PrimaryCommit,
                                        route.keys(),
                                        &route,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                    );
                                    return PrimaryResult::DefinitiveFailure(cause);
                                }
                            };
                            record_attempt(
                                receipt,
                                TransactionAttemptPhase::PrimaryCommit,
                                route.keys(),
                                &route,
                                Some(response.publication.clone()),
                                TransactionAttemptResult::Retry(TransactionCause::Timestamp {
                                    detail: format!(
                                        "primary Commit retry requires min_commit_ts {minimum}"
                                    ),
                                }),
                            );
                            match self.commit_timestamp(minimum, call) {
                                Ok(new_commit_ts) => {
                                    commit_ts = new_commit_ts;
                                    receipt.commit_ts = new_commit_ts;
                                    attempt = attempt.saturating_add(1);
                                    continue;
                                }
                                Err(cause) => return PrimaryResult::DefinitiveFailure(cause),
                            }
                        }
                        let cause = classify_key_error(error);
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::PrimaryCommit,
                            route.keys(),
                            &route,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        return PrimaryResult::DefinitiveFailure(cause);
                    }
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::PrimaryCommit,
                        route.keys(),
                        &route,
                        Some(response.publication.clone()),
                        TransactionAttemptResult::Confirmed,
                    );
                    return PrimaryResult::Committed(route.keys().to_vec());
                }
            }
        }
    }

    /// Commits keys whose outcome is already decided.
    ///
    /// The batch that happens to hold the primary key commits in the primary
    /// role — which only occurs on the async-commit path, where every key is
    /// passed in at once because the decision was already made at prewrite.
    fn commit_secondaries(
        &mut self,
        secondary_keys: &[Vec<u8>],
        primary_key: &[u8],
        commit_ts: u64,
        use_async_commit: bool,
        receipt: &mut OptimisticTransactionReceipt,
    ) -> Vec<SecondaryCommitFailure> {
        if secondary_keys.is_empty() {
            return Vec::new();
        }
        let cleanup_call = UnaryCallContext::with_timeout(self.timeout);
        let mut queue = match group_keys(&self.runtime, secondary_keys) {
            Ok(batches) => VecDeque::from(batches),
            Err(error) => {
                return vec![SecondaryCommitFailure {
                    keys: secondary_keys.to_vec(),
                    region: None,
                    address: None,
                    publication: None,
                    cause: TransactionCause::Region {
                        detail: format!("secondary grouping failed: {error}"),
                    },
                }];
            }
        };
        let mut failures = Vec::new();
        while let Some(batch) = queue.pop_front() {
            receipt.region_attempts.push(batch.region());
            let holds_primary = batch.keys().iter().any(|key| key.as_slice() == primary_key);
            let request = KvrpcCommitRequest {
                start_version: self.start_ts,
                keys: batch.keys().to_vec(),
                commit_version: commit_ts,
                commit_role: if holds_primary {
                    KvrpcCommitRole::Primary as i32
                } else {
                    KvrpcCommitRole::Secondary as i32
                },
                primary_key: primary_key.to_vec(),
                use_async_commit,
                ..KvrpcCommitRequest::default()
            };
            let published = match self.runtime.client().try_borrow_mut() {
                Ok(mut client) => {
                    client.publish_commit(batch.address(), &request, batch.context(), &cleanup_call)
                }
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing secondary Commit".to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("secondary Commit failed before publication: {error}"),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::SecondaryCommit,
                        batch.keys(),
                        &batch,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    failures.push(SecondaryCommitFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: None,
                        cause,
                    });
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    receipt
                        .secondary_attempt_publications
                        .push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!(
                            "secondary Commit completion failed after publication: {error}"
                        ),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::SecondaryCommit,
                        batch.keys(),
                        &batch,
                        Some(publication.clone()),
                        TransactionAttemptResult::Ambiguous(cause.clone()),
                    );
                    failures.push(SecondaryCommitFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: Some(publication),
                        cause,
                    });
                }
                PublishedCommand::Response(response) => {
                    receipt
                        .secondary_attempt_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        match self.recover_region_error(
                            RecoveryPhase::Secondary,
                            region_error,
                            batch.attempt(),
                            &cleanup_call,
                        ) {
                            Ok(()) => match group_keys(&self.runtime, batch.keys()) {
                                Ok(regrouped) => {
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::SecondaryCommit,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::Retry(TransactionCause::Region {
                                            detail: format!(
                                                "secondary Commit region retry: {region_error:?}"
                                            ),
                                        }),
                                    );
                                    for item in regrouped.into_iter().rev() {
                                        queue.push_front(item);
                                    }
                                    continue;
                                }
                                Err(error) => {
                                    let cause = TransactionCause::Region {
                                        detail: format!("secondary Commit regroup failed: {error}"),
                                    };
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::SecondaryCommit,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                    );
                                    failures.push(SecondaryCommitFailure {
                                        keys: batch.keys().to_vec(),
                                        region: Some(batch.region()),
                                        address: Some(batch.address().to_owned()),
                                        publication: Some(response.publication.clone()),
                                        cause,
                                    });
                                }
                            },
                            Err(cause) => {
                                record_attempt(
                                    receipt,
                                    TransactionAttemptPhase::SecondaryCommit,
                                    batch.keys(),
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                failures.push(SecondaryCommitFailure {
                                    keys: batch.keys().to_vec(),
                                    region: Some(batch.region()),
                                    address: Some(batch.address().to_owned()),
                                    publication: Some(response.publication.clone()),
                                    cause,
                                });
                            }
                        }
                    } else if let Some(error) = response.response.error.as_ref() {
                        let cause = classify_key_error(error);
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::SecondaryCommit,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        failures.push(SecondaryCommitFailure {
                            keys: batch.keys().to_vec(),
                            region: Some(batch.region()),
                            address: Some(batch.address().to_owned()),
                            publication: Some(response.publication.clone()),
                            cause,
                        });
                    } else {
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::SecondaryCommit,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Confirmed,
                        );
                        receipt.secondary_publications.push(response.publication);
                    }
                }
            }
        }
        failures
    }

    fn rollback_after_failure(
        &mut self,
        mut receipt: OptimisticTransactionReceipt,
        keys: &[Vec<u8>],
        cause: TransactionCause,
    ) -> OptimisticCommitOutcome {
        if let Err(error) = self.state.transition(CoordinatorState::RollingBack) {
            return OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
                receipt,
                cause,
                cleanup_failures: vec![CleanupBatchFailure {
                    keys: keys.to_vec(),
                    region: None,
                    address: None,
                    publication: None,
                    cause: error,
                }],
            });
        }
        let cleanup_failures = self.rollback_keys(keys, &mut receipt);
        if cleanup_failures.is_empty() {
            if let Err(error) = self.state.transition(CoordinatorState::RolledBack) {
                return OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
                    receipt,
                    cause,
                    cleanup_failures: vec![CleanupBatchFailure {
                        keys: keys.to_vec(),
                        region: None,
                        address: None,
                        publication: None,
                        cause: error,
                    }],
                });
            }
            OptimisticCommitOutcome::RolledBack(RolledBackTransaction { receipt, cause })
        } else {
            let _ = self.state.transition(CoordinatorState::CleanupFailed);
            OptimisticCommitOutcome::CleanupFailed(CleanupFailedTransaction {
                receipt,
                cause,
                cleanup_failures,
            })
        }
    }

    fn rollback_keys(
        &mut self,
        keys: &[Vec<u8>],
        receipt: &mut OptimisticTransactionReceipt,
    ) -> Vec<CleanupBatchFailure> {
        if keys.is_empty() {
            return Vec::new();
        }
        let cleanup_call = UnaryCallContext::with_timeout(self.timeout);
        let mut queue = match group_keys(&self.runtime, keys) {
            Ok(batches) => VecDeque::from(batches),
            Err(error) => {
                return vec![CleanupBatchFailure {
                    keys: keys.to_vec(),
                    region: None,
                    address: None,
                    publication: None,
                    cause: TransactionCause::Region {
                        detail: format!("rollback grouping failed: {error}"),
                    },
                }];
            }
        };
        let mut failures = Vec::new();
        while let Some(batch) = queue.pop_front() {
            receipt.region_attempts.push(batch.region());
            let request = KvrpcBatchRollbackRequest {
                start_version: self.start_ts,
                keys: batch.keys().to_vec(),
                ..KvrpcBatchRollbackRequest::default()
            };
            let published = match self.runtime.client().try_borrow_mut() {
                Ok(mut client) => client.publish_batch_rollback(
                    batch.address(),
                    &request,
                    batch.context(),
                    &cleanup_call,
                ),
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing BatchRollback".to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error) => {
                    let cause = TransactionCause::Transport {
                        detail: format!("BatchRollback failed before publication: {error}"),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::BatchRollback,
                        batch.keys(),
                        &batch,
                        None,
                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                    );
                    failures.push(CleanupBatchFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: None,
                        cause,
                    });
                }
                PublishedCommand::AfterPublication { publication, error } => {
                    receipt
                        .rollback_attempt_publications
                        .push(publication.clone());
                    let cause = TransactionCause::Transport {
                        detail: format!(
                            "BatchRollback completion failed after publication: {error}"
                        ),
                    };
                    record_attempt(
                        receipt,
                        TransactionAttemptPhase::BatchRollback,
                        batch.keys(),
                        &batch,
                        Some(publication.clone()),
                        TransactionAttemptResult::Ambiguous(cause.clone()),
                    );
                    failures.push(CleanupBatchFailure {
                        keys: batch.keys().to_vec(),
                        region: Some(batch.region()),
                        address: Some(batch.address().to_owned()),
                        publication: Some(publication),
                        cause,
                    });
                }
                PublishedCommand::Response(response) => {
                    receipt
                        .rollback_attempt_publications
                        .push(response.publication.clone());
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        match self.recover_region_error(
                            RecoveryPhase::Cleanup,
                            region_error,
                            batch.attempt(),
                            &cleanup_call,
                        ) {
                            Ok(()) => match group_keys(&self.runtime, batch.keys()) {
                                Ok(regrouped) => {
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::BatchRollback,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::Retry(TransactionCause::Region {
                                            detail: format!(
                                                "BatchRollback region retry: {region_error:?}"
                                            ),
                                        }),
                                    );
                                    for item in regrouped.into_iter().rev() {
                                        queue.push_front(item);
                                    }
                                    continue;
                                }
                                Err(error) => {
                                    let cause = TransactionCause::Region {
                                        detail: format!("BatchRollback regroup failed: {error}"),
                                    };
                                    record_attempt(
                                        receipt,
                                        TransactionAttemptPhase::BatchRollback,
                                        batch.keys(),
                                        &batch,
                                        Some(response.publication.clone()),
                                        TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                    );
                                    failures.push(CleanupBatchFailure {
                                        keys: batch.keys().to_vec(),
                                        region: Some(batch.region()),
                                        address: Some(batch.address().to_owned()),
                                        publication: Some(response.publication.clone()),
                                        cause,
                                    });
                                }
                            },
                            Err(cause) => {
                                record_attempt(
                                    receipt,
                                    TransactionAttemptPhase::BatchRollback,
                                    batch.keys(),
                                    &batch,
                                    Some(response.publication.clone()),
                                    TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                                );
                                failures.push(CleanupBatchFailure {
                                    keys: batch.keys().to_vec(),
                                    region: Some(batch.region()),
                                    address: Some(batch.address().to_owned()),
                                    publication: Some(response.publication.clone()),
                                    cause,
                                });
                            }
                        }
                    } else if let Some(error) = response.response.error.as_ref() {
                        let cause = classify_key_error(error);
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::BatchRollback,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::DefinitiveFailure(cause.clone()),
                        );
                        failures.push(CleanupBatchFailure {
                            keys: batch.keys().to_vec(),
                            region: Some(batch.region()),
                            address: Some(batch.address().to_owned()),
                            publication: Some(response.publication.clone()),
                            cause,
                        });
                    } else {
                        record_attempt(
                            receipt,
                            TransactionAttemptPhase::BatchRollback,
                            batch.keys(),
                            &batch,
                            Some(response.publication.clone()),
                            TransactionAttemptResult::Confirmed,
                        );
                        receipt.rollback_publications.push(response.publication);
                    }
                }
            }
        }
        failures
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

/// One commit's live protocol decision, narrowed as TiKV answers.
///
/// It only ever narrows: every observation can turn a protocol off, none can
/// turn one back on. That is what makes the fallback to normal two-phase commit
/// safe to take at any point during prewrite.
struct AttemptedProtocol {
    use_async_commit: bool,
    use_one_pc: bool,
    max_commit_ts: u64,
    one_pc_commit_ts: u64,
    secondaries: Vec<Vec<u8>>,
}

impl AttemptedProtocol {
    /// Go `checkOnePCFallBack`: 1PC is a single-region protocol, so the moment
    /// the mutations need more than one region it is off — including when a
    /// region split discovers this mid-prewrite.
    fn observe_batch_count(&mut self, batches: usize) {
        if batches > 1 {
            self.use_one_pc = false;
        }
    }

    /// Applies one successful prewrite response to the protocol decision.
    ///
    /// Go `prewrite1BatchReqHandler.handleSingleBatchSucceed`. TiKV signals
    /// refusal by omission: a zeroed `one_pc_commit_ts` under `try_one_pc`, or
    /// a zeroed `min_commit_ts` under `use_async_commit`, each means "finish
    /// this the normal way".
    fn observe_prewrite_response(
        &mut self,
        response: &KvrpcPrewriteResponse,
    ) -> Result<(), TransactionCause> {
        if self.use_one_pc {
            if response.one_pc_commit_ts == 0 {
                if response.min_commit_ts != 0 {
                    return Err(TransactionCause::InvalidResponse {
                        detail: format!(
                            "1PC fallback must zero min_commit_ts, got {}",
                            response.min_commit_ts
                        ),
                    });
                }
                self.use_one_pc = false;
                // A 1PC fallback is TiKV declining to commit in the prewrite at
                // all, so async commit cannot rescue this transaction either.
                self.use_async_commit = false;
            } else if self.one_pc_commit_ts != 0 {
                return Err(TransactionCause::InvalidResponse {
                    detail: "1PC committed more than one prewrite batch".to_owned(),
                });
            } else {
                self.one_pc_commit_ts = response.one_pc_commit_ts;
            }
            return Ok(());
        }
        if response.one_pc_commit_ts != 0 {
            return Err(TransactionCause::InvalidResponse {
                detail: format!(
                    "TiKV committed a non-1PC transaction with 1PC at {}",
                    response.one_pc_commit_ts
                ),
            });
        }
        if self.use_async_commit && response.min_commit_ts == 0 {
            self.use_async_commit = false;
        }
        Ok(())
    }
}

enum PrimaryResult {
    Committed(Vec<Vec<u8>>),
    DefinitiveFailure(TransactionCause),
    Undetermined(TransactionCause),
}

trait AttemptRoute {
    fn evidence_region(&self) -> crate::region::RegionVerId;
    fn evidence_address(&self) -> &str;
}

impl AttemptRoute for RegionMutationBatch {
    fn evidence_region(&self) -> crate::region::RegionVerId {
        self.region()
    }

    fn evidence_address(&self) -> &str {
        self.address()
    }
}

impl AttemptRoute for RegionKeyBatch {
    fn evidence_region(&self) -> crate::region::RegionVerId {
        self.region()
    }

    fn evidence_address(&self) -> &str {
        self.address()
    }
}

fn record_attempt(
    receipt: &mut OptimisticTransactionReceipt,
    phase: TransactionAttemptPhase,
    keys: &[Vec<u8>],
    route: &impl AttemptRoute,
    publication: Option<TransactionBatchPublication>,
    result: TransactionAttemptResult,
) {
    receipt.attempt_history.push(TransactionAttemptReceipt {
        phase,
        keys: keys.to_vec(),
        region: route.evidence_region(),
        address: route.evidence_address().to_owned(),
        publication,
        result,
    });
}

#[derive(Clone, Copy)]
pub(super) enum RecoveryPhase {
    Forward,
    Secondary,
    Cleanup,
}

pub(super) fn classify_key_error(error: &KvrpcKeyError) -> TransactionCause {
    if let Some(already_exists) = error.already_exist.as_ref() {
        return TransactionCause::AlreadyExists {
            key: already_exists.key.clone(),
            detail: format!("key already exists: {already_exists:?}"),
        };
    }
    if let Some(assertion) = error.assertion_failed.as_ref() {
        return TransactionCause::AssertionFailed {
            key: assertion.key.clone(),
            detail: format!("mutation assertion failed: {assertion:?}"),
        };
    }
    if let Some(conflict) = error.conflict.as_ref() {
        return TransactionCause::WriteConflict {
            detail: format!("optimistic write conflict: {conflict:?}"),
        };
    }
    if let Some(lock) = error.locked.as_ref() {
        return TransactionCause::Lock {
            key: lock.key.clone(),
            detail: format!("key is locked: {lock:?}"),
        };
    }
    TransactionCause::InvalidResponse {
        detail: format!("unclassified TiKV key error: {error:?}"),
    }
}

fn primary_region_response_is_ambiguous(error: &tidb_proto::RegionError) -> bool {
    error.undetermined_result.is_some()
}

fn validate_commit_ts_expired(
    expired: &KvrpcCommitTsExpired,
    start_ts: u64,
    primary_key: &[u8],
    attempted_commit_ts: u64,
) -> Result<u64, TransactionCause> {
    let latest_pinned_min_commit_ts = attempted_commit_ts
        .saturating_add(MAX_COMMIT_TS_DRIFT_MS.saturating_mul(1_u64 << TSO_LOGICAL_BITS));
    if expired.start_ts != start_ts
        || expired.attempted_commit_ts != attempted_commit_ts
        || expired.key != primary_key
        || expired.min_commit_ts <= attempted_commit_ts
        || expired.min_commit_ts > latest_pinned_min_commit_ts
    {
        return Err(TransactionCause::InvalidResponse {
            detail: format!(
                "CommitTsExpired violates pinned primary retry contract: {expired:?}; attempted_commit_ts={attempted_commit_ts}, latest_min_commit_ts={latest_pinned_min_commit_ts}"
            ),
        });
    }
    Ok(expired.min_commit_ts)
}

pub(super) fn transaction_lock_ttl_ms(opened_at: Instant, transaction_bytes: usize) -> u64 {
    const BYTES_PER_MIB: f64 = (1024 * 1024) as f64;
    const TTL_FACTOR_MS: f64 = 6_000.0;
    const MANAGED_LOCK_TTL_MS: u64 = 20_000;
    const SIZE_THRESHOLD_BYTES: usize = 16 * 1024;

    let sized_ttl = if transaction_bytes >= SIZE_THRESHOLD_BYTES {
        let size_mib = transaction_bytes as f64 / BYTES_PER_MIB;
        (TTL_FACTOR_MS * size_mib.sqrt()) as u64
    } else {
        DEFAULT_LOCK_TTL_MS
    };
    let base = sized_ttl.clamp(DEFAULT_LOCK_TTL_MS, MANAGED_LOCK_TTL_MS);
    let elapsed_ms = u64::try_from(opened_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    base.saturating_add(elapsed_ms)
}

/// Gathers the locks named by one Scan key error so they can be resolved.
///
/// A key error without lock information is not something a snapshot read can
/// recover from, so it fails the scan instead of being retried forever.
fn collect_scan_lock(
    key_error: &KvrpcKeyError,
    locked: &mut Vec<crate::lock::OptimisticLock>,
) -> Result<(), OptimisticCoordinatorError> {
    let Some(lock_info) = key_error.locked.as_ref() else {
        return Err(OptimisticCoordinatorError::SnapshotGet(format!(
            "TiKV scan key error: {key_error:?}"
        )));
    };
    locked.extend(
        decode_lock_observation(lock_info)
            .map_err(|error| OptimisticCoordinatorError::SnapshotGet(error.to_string()))?,
    );
    Ok(())
}

pub(super) fn wait_with_call(
    call: &UnaryCallContext,
    delay: Duration,
) -> Result<(), TransactionCause> {
    if call.cancellation().is_cancelled() || delay > call.timeout() {
        return Err(TransactionCause::Transport {
            detail: "transaction wait exceeded its deadline or was cancelled".to_owned(),
        });
    }
    if call.cancellation().wait_timeout(delay) {
        return Err(TransactionCause::Transport {
            detail: "transaction wait was cancelled".to_owned(),
        });
    }
    if call.timeout().is_zero() && !delay.is_zero() {
        return Err(TransactionCause::Transport {
            detail: "transaction wait reached its absolute deadline".to_owned(),
        });
    }
    Ok(())
}

pub(super) fn alive_retry_delay(remaining_ttl: Duration) -> Duration {
    remaining_ttl.max(Duration::from_millis(10))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_proto::{
        KvrpcAlreadyExist, KvrpcAssertion, KvrpcAssertionFailed, KvrpcLockInfo, KvrpcWriteConflict,
    };

    #[test]
    fn lock_ttl_adds_read_time_and_scales_large_transactions() {
        let recent = Instant::now();
        assert!(transaction_lock_ttl_ms(recent, 1) >= DEFAULT_LOCK_TTL_MS);
        let old = Instant::now() - Duration::from_millis(25);
        assert!(transaction_lock_ttl_ms(old, 1) >= DEFAULT_LOCK_TTL_MS + 25);
        assert!(transaction_lock_ttl_ms(recent, 4 * 1024 * 1024) >= 12_000);
    }

    #[test]
    fn commit_ts_expired_retry_is_pinned_to_exact_attempt_and_one_hour() {
        let start_ts = 10_u64 << TSO_LOGICAL_BITS;
        let attempted_commit_ts = start_ts + ((2 * MAX_COMMIT_TS_DRIFT_MS) << TSO_LOGICAL_BITS);
        let within_hour = attempted_commit_ts + ((MAX_COMMIT_TS_DRIFT_MS - 1) << TSO_LOGICAL_BITS);
        let valid = KvrpcCommitTsExpired {
            start_ts,
            attempted_commit_ts,
            key: b"primary".to_vec(),
            min_commit_ts: within_hour,
        };
        assert_eq!(
            validate_commit_ts_expired(&valid, start_ts, b"primary", attempted_commit_ts),
            Ok(within_hour)
        );

        let mut wrong_attempt = valid.clone();
        wrong_attempt.attempted_commit_ts += 1;
        assert!(matches!(
            validate_commit_ts_expired(&wrong_attempt, start_ts, b"primary", attempted_commit_ts),
            Err(TransactionCause::InvalidResponse { .. })
        ));

        let mut beyond_pin = valid;
        beyond_pin.min_commit_ts =
            attempted_commit_ts + ((MAX_COMMIT_TS_DRIFT_MS + 1) << TSO_LOGICAL_BITS);
        assert!(matches!(
            validate_commit_ts_expired(&beyond_pin, start_ts, b"primary", attempted_commit_ts),
            Err(TransactionCause::InvalidResponse { .. })
        ));
    }

    #[test]
    fn commit_key_errors_keep_executor_visible_identity() {
        let already_exists = KvrpcKeyError {
            already_exist: Some(KvrpcAlreadyExist { key: b"e".to_vec() }),
            ..KvrpcKeyError::default()
        };
        assert!(matches!(
            classify_key_error(&already_exists),
            TransactionCause::AlreadyExists { key, .. } if key == b"e"
        ));

        let assertion = KvrpcKeyError {
            assertion_failed: Some(KvrpcAssertionFailed {
                start_ts: 7,
                key: b"a".to_vec(),
                assertion: KvrpcAssertion::Exist as i32,
                ..KvrpcAssertionFailed::default()
            }),
            ..KvrpcKeyError::default()
        };
        assert!(matches!(
            classify_key_error(&assertion),
            TransactionCause::AssertionFailed { key, .. } if key == b"a"
        ));

        let conflict = KvrpcKeyError {
            conflict: Some(KvrpcWriteConflict {
                start_ts: 7,
                conflict_ts: 9,
                key: b"c".to_vec(),
                ..KvrpcWriteConflict::default()
            }),
            ..KvrpcKeyError::default()
        };
        assert!(matches!(
            classify_key_error(&conflict),
            TransactionCause::WriteConflict { .. }
        ));

        let lock = KvrpcKeyError {
            locked: Some(KvrpcLockInfo {
                key: b"l".to_vec(),
                ..KvrpcLockInfo::default()
            }),
            ..KvrpcKeyError::default()
        };
        assert!(matches!(
            classify_key_error(&lock),
            TransactionCause::Lock { key, .. } if key == b"l"
        ));
        assert!(matches!(
            classify_key_error(&KvrpcKeyError::default()),
            TransactionCause::InvalidResponse { .. }
        ));
    }

    #[test]
    fn waits_use_the_absolute_call_deadline_and_cancellation() {
        assert_eq!(alive_retry_delay(Duration::ZERO), Duration::from_millis(10));
        let expired = UnaryCallContext::with_timeout(Duration::ZERO);
        assert!(matches!(
            wait_with_call(&expired, Duration::from_millis(1)),
            Err(TransactionCause::Transport { .. })
        ));
        let cancellation = crate::rpc::UnaryCancellation::new();
        cancellation.cancel();
        let cancelled = UnaryCallContext::new(Duration::from_secs(1), cancellation);
        assert!(matches!(
            wait_with_call(&cancelled, Duration::ZERO),
            Err(TransactionCause::Transport { .. })
        ));
    }

    #[test]
    fn only_explicit_undetermined_primary_region_response_is_ambiguous() {
        assert!(!primary_region_response_is_ambiguous(
            &tidb_proto::RegionError::default()
        ));
        let undetermined = tidb_proto::RegionError {
            undetermined_result: Some(tidb_proto::errorpb::UndeterminedResult::default()),
            ..tidb_proto::RegionError::default()
        };
        assert!(primary_region_response_is_ambiguous(&undetermined));
    }
}
