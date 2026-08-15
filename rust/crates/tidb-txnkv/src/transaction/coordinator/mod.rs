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

//! The transaction coordinator: one optimistic two-phase commit over real PD
//! timestamps and real TiKV transport.
//!
//! This mirrors client-go's `txnkv/transaction` package, whose 2PC driver is
//! also split by phase. The subject of each module here is the phase it owns:
//!
//! | module | subject | Go boundary |
//! | --- | --- | --- |
//! | [`opener`] | deriving transactions from process authorities, lock keep-alive | `txn.go` (`KVStore.Begin`), `txn_lock_keepalive` |
//! | [`snapshot_read`] | reads pinned to `start_ts` (Get, Scan) | `snapshot.go` |
//! | [`prewrite`] | Prewrite, and the async-commit/1PC decision it narrows | `prewrite.go` |
//! | [`commit`] | commit timestamp, primary commit, secondary commit | `commit.go`, `2pc.go` |
//! | [`cleanup`] | the two ways a transaction ends without committing | `cleanup.go` |
//!
//! This file keeps what all five phases share: the transaction struct itself,
//! the caller-visible error, the tuning constants, region-error recovery, and
//! the per-attempt receipt evidence.

mod cleanup;
mod commit;
mod opener;
mod prewrite;
mod snapshot_read;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_proto::{KvrpcContext, KvrpcKeyError};

use crate::gc_state::{GcStateCache, VisibilityError};
use crate::lock::{LockRecoveryClient, TimestampSource};
use crate::region::{
    RegionBackoffBudget, RegionErrorDisposition, RegionRecoveryLoader, RegionTerminalError,
};
use crate::rpc::{TonicCoprocessorClient, TransactionBatchPublication, UnaryCallContext};
use crate::{PdRegionLoader, SharedReadRuntime};

use super::command_client::TransactionCommandClient;
use super::mutation::{validate_plan, MutationSetError};
use super::region_batches::{RegionKeyBatch, RegionMutationBatch};
use super::state::{
    CoordinatorState, OptimisticTransactionReceipt, SnapshotReadReceipt, TransactionAttemptPhase,
    TransactionAttemptReceipt, TransactionAttemptResult, TransactionCause,
};

pub use opener::{PdLockTimestampSource, RealOptimisticTransactionOpener};
pub use snapshot_read::SnapshotGetResult;

const DEFAULT_LOCK_TTL_MS: u64 = 3_000;
/// Go `config.DefaultConfig().TiKVClient.AsyncCommit.KeysLimit`.
const ASYNC_COMMIT_KEYS_LIMIT: usize = 256;
/// Go `config.DefaultConfig().TiKVClient.AsyncCommit.TotalKeySizeLimit` (4 KiB).
const ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT: u64 = 4 * 1024;
/// Go `config.DefaultConfig().TiKVClient.AsyncCommit.SafeWindow` (2s).
const ASYNC_COMMIT_SAFE_WINDOW_MS: u64 = 2_000;
pub(super) const MAX_LOCK_ATTEMPTS: usize = 4;

/// Go `client.ReadTimeoutShort` (`internal/client/client.go:79`), the per-RPC
/// deadline every cleanup and commit batch is sent under.
const RPC_READ_TIMEOUT_SHORT: Duration = Duration::from_secs(30);
/// Go `cleanupMaxBackoff` (`2pc.go:1638`).
const CLEANUP_MAX_BACKOFF: Duration = Duration::from_millis(20_000);
/// Go `CommitSecondaryMaxBackoff` (`2pc.go:967`).
const COMMIT_SECONDARY_MAX_BACKOFF: Duration = Duration::from_millis(41_000);

/// The deadline a detached cleanup runs under.
///
/// Go builds the cleanup backoffer on `c.store.Ctx()` (`2pc.go:1660`) — the
/// *store* context, not the statement's — and spawns it detached
/// (`2pc.go:1651`), precisely so a statement that has already spent its own
/// deadline still gets a full 20-second budget to unstick the locks it left
/// behind. Binding cleanup to the statement timeout instead makes
/// [`wait_with_call`] reject the very first backoff delay, so the rollback
/// cannot retry at all and the locks are abandoned to the lock resolver.
///
/// One backoff budget plus one full-length RPC: the budget is sleep-only in Go,
/// and a batch that consumed `ReadTimeoutShort` must still be able to spend it.
pub(super) const fn cleanup_call_budget() -> Duration {
    CLEANUP_MAX_BACKOFF.saturating_add(RPC_READ_TIMEOUT_SHORT)
}

/// The deadline secondary commit runs under, same store-lifetime reasoning as
/// [`cleanup_call_budget`]; Go uses `c.store.Ctx()` at `2pc.go:1054` and
/// `2pc.go:2036` with `CommitSecondaryMaxBackoff`.
pub(super) const fn secondary_commit_call_budget() -> Duration {
    COMMIT_SECONDARY_MAX_BACKOFF.saturating_add(RPC_READ_TIMEOUT_SHORT)
}
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
    /// The primary key this transaction pinned at its first locking statement,
    /// which every pessimistic lock it holds already names as `primary_lock`
    /// and which its TTL heartbeat refreshes.
    ///
    /// Go `twoPhaseCommitter.primary()` (`2pc.go:779-787`) returns the pinned
    /// `c.primaryKey` whenever it is set and only falls back to
    /// `mutations.GetKey(0)` when it is empty; `initKeysAndMutations`
    /// (`2pc.go:697-698`) sets it *only if still empty*, so mutation order can
    /// never override a pessimistically chosen primary. One transaction, one
    /// primary, for its whole life — that invariant is the entire basis of
    /// lock recovery.
    pinned_primary_key: Option<Vec<u8>>,
    /// The store-wide txn safe point every read from this transaction is
    /// validated against once TiKV has answered.
    gc_state: Arc<GcStateCache>,
    protocol: CommitProtocol,
    /// Resource group inherited by Prewrite and Commit request contexts.
    resource_group_name: Option<String>,
    /// Transactions whose locks every later read from this snapshot may step
    /// over, and transactions whose committed value every later read must see
    /// through their lock.
    ///
    /// Go `KVSnapshot.resolvedLocks` / `KVSnapshot.committedLocks`
    /// (`snapshot.go:124-125`), the two `util.TSSet`s that `ClientHelper`
    /// fills from `ResolveLockResult` and replays into `Context` on every
    /// subsequent send. Without them a reader that meets a lock whose
    /// min-commit-ts TiKV pushed meets the very same lock on its retry, which
    /// is the deadloop `client_helper.go`'s own comment warns about.
    resolved_locks: crate::lock::SnapshotLockSet,
}

impl<C, L, T> crate::new_txn::TxnResourceGroup for RealOptimisticTransaction<C, L, T> {
    fn set_resource_group_name(&mut self, name: &str) {
        self.resource_group_name = Some(name.to_owned());
    }
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
            secondary_backoff: RegionBackoffBudget::new(COMMIT_SECONDARY_MAX_BACKOFF),
            cleanup_backoff: RegionBackoffBudget::new(CLEANUP_MAX_BACKOFF),
            pessimistic: None,
            pinned_primary_key: None,
            gc_state,
            protocol: CommitProtocol::two_phase_only(),
            resource_group_name: None,
            resolved_locks: crate::lock::SnapshotLockSet::default(),
        })
    }

    /// Permits this transaction to attempt async commit and/or 1PC.
    pub fn set_commit_protocol(&mut self, protocol: CommitProtocol) {
        self.protocol = protocol;
    }

    /// Rejects a completed read whose timestamp GC has already passed.
    ///
    /// Called only after TiKV has answered, mirroring client-go's placement of
    /// `CheckVisibility` at the end of `snapshot.get` and `snapshot.scan`. A
    /// pre-read check would be worthless: GC can advance while the RPC is in
    /// flight, so only a post-read check covers the data actually returned.
    fn check_visibility_at(&self, read_ts: u64) -> Result<(), OptimisticCoordinatorError> {
        self.gc_state
            .check_visibility(read_ts)
            .map_err(OptimisticCoordinatorError::Visibility)
    }

    /// Rejects a completed read whose transaction snapshot GC has already
    /// passed.
    fn check_visibility(&self) -> Result<(), OptimisticCoordinatorError> {
        self.check_visibility_at(self.start_ts)
    }

    /// Binds the pessimistic locks a caller already acquired at `start_ts`.
    pub(super) fn set_pessimistic_prewrite(&mut self, plan: PessimisticPrewritePlan) {
        self.pessimistic = Some(plan);
    }

    /// Pins this transaction's primary key for the rest of its life.
    ///
    /// The parameter is a plain `Vec<u8>`, not an `Option`: "pin nothing" is
    /// not a thing a caller can express here, because a default primary is
    /// exactly what used to silently override the pinned one. A transaction
    /// that never locked anything simply never calls this, and prewrite falls
    /// back to mutation order — Go's `len(c.primaryKey) == 0` branch.
    pub(super) fn pin_primary_key(&mut self, key: Vec<u8>) {
        self.pinned_primary_key = Some(key);
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

    /// Clones a routed request context and attaches this transaction's resource
    /// group without disturbing route, priority, penalty, or tracing fields.
    pub(super) fn write_context(&self, context: &KvrpcContext) -> KvrpcContext {
        let mut context = context.clone();
        if let Some(resource_group_name) = self.resource_group_name.as_ref() {
            resource_group_name.clone_into(
                &mut context
                    .resource_control_context
                    .get_or_insert_with(Default::default)
                    .resource_group_name,
            );
        }
        context
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
        recover_region_error_with(&self.runtime, backoff, error, attempt, call)
    }
}

/// Applies one region error to the shared cache and one caller-owned backoff
/// budget. Snapshot reads use this both inside a transaction and from the
/// transaction-free MaxTS point path, so recovery semantics cannot drift.
pub(super) fn recover_region_error_with<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    backoff: &mut RegionBackoffBudget,
    error: &tidb_proto::RegionError,
    attempt: &crate::region::RegionAttempt,
    call: &UnaryCallContext,
) -> Result<(), TransactionCause>
where
    L: RegionRecoveryLoader,
{
    let disposition = runtime
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
            return Err(terminal_region_cause(terminal));
        }
    };
    wait_with_call(call, delay)
}

fn terminal_region_cause(terminal: RegionTerminalError) -> TransactionCause {
    match terminal {
        RegionTerminalError::BackoffExhausted { kind, max_sleep } => {
            TransactionCause::BackoffExhausted {
                kind,
                detail: if kind == crate::region::RegionBackoffKind::TikvServerBusy {
                    // client-go excludes Busy sleep from longestSleepCfg. If
                    // the excluded-only cap fires, Backoff returns the current
                    // ordinary error rather than BoTiKVServerBusy's sentinel.
                    "server is busy".to_owned()
                } else {
                    format!("TiKV region backoff exhausted: kind={kind:?}, max_sleep={max_sleep:?}")
                },
            }
        }
        terminal => TransactionCause::Region {
            detail: format!("TiKV returned terminal region error: {terminal:?}"),
        },
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::{RegionBackoffKind, RegionTerminalError};
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
    fn terminal_backoff_retains_its_kind_without_typing_structural_region_errors() {
        let unavailable = terminal_region_cause(RegionTerminalError::BackoffExhausted {
            kind: RegionBackoffKind::RegionScheduling,
            max_sleep: Duration::from_secs(20),
        });
        assert!(matches!(
            unavailable,
            TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::RegionScheduling,
                ..
            }
        ));

        let busy = terminal_region_cause(RegionTerminalError::BackoffExhausted {
            kind: RegionBackoffKind::TikvServerBusy,
            max_sleep: Duration::from_secs(20),
        });
        assert!(matches!(
            busy,
            TransactionCause::BackoffExhausted { kind: RegionBackoffKind::TikvServerBusy, detail }
                if detail == "server is busy"
        ));

        let flashback = terminal_region_cause(RegionTerminalError::FlashbackInProgress {
            region_id: 42,
            flashback_start_ts: 99,
        });
        assert!(matches!(
            flashback,
            TransactionCause::Region { detail }
                if detail.contains("FlashbackInProgress") && detail.contains("42")
        ));
    }
}
