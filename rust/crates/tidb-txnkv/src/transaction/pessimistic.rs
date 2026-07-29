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

//! Pessimistic transactions over the same 2PC engine as optimistic ones.
//!
//! The difference is where conflict is detected. An optimistic transaction
//! discovers conflict at Prewrite, and a conflict there costs the whole
//! transaction. A pessimistic transaction takes a lock as each statement runs,
//! under that statement's own `for_update_ts`, so a conflict costs only the
//! statement: the caller advances `for_update_ts` and retries it. By commit
//! time every conflict is already settled, which is why the final two-phase
//! commit is literally the optimistic one — [`RealOptimisticTransaction`] —
//! with Prewrite told to verify the locks instead of re-checking for conflicts.
//!
//! Fair (aggressive) locking is the second way a conflict can be settled. With
//! `@@tidb_pessimistic_txn_fair_locking` on, a single-key locking statement is
//! sent in `WakeUpModeForceLock`, and TiKV may grant the lock *despite* a
//! newer committed version, reporting `LockResultLockedWithConflict` with that
//! version's commit timestamp. The lock then really exists at that timestamp,
//! not at the requested `for_update_ts` — so this owner records it per key and
//! declares it to Prewrite as a `for_update_ts` constraint, and releases such a
//! lock at the higher timestamp. The statement still has to be retried, because
//! its result was computed from a snapshot the lock has now overtaken; what
//! fair locking buys is that the lock survives the retry, so the retry needs no
//! second PessimisticLock round trip for that key.
//!
//! Out of scope here, and deliberately so: `return_values`/`check_existence`
//! value caching, async commit, and 1PC.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

use tidb_proto::{
    KvrpcAssertion, KvrpcDeadlock, KvrpcKeyError, KvrpcMutation, KvrpcOp,
    KvrpcPessimisticLockKeyResultType, KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse,
    KvrpcPessimisticLockWakeUpMode, KvrpcPessimisticRollbackRequest,
};

use crate::lock::{
    decode_blocking_lock_observation, resolve_blocking_locks, BlockingLock, LockRecoveryClient,
    LockRecoveryResult, TimestampSource,
};
use crate::region::{RegionLoader, RegionRecoveryLoader};
use crate::rpc::UnaryCallContext;

use super::command_client::{PublishedCommand, TransactionCommandClient};
use super::coordinator::{
    classify_key_error, PessimisticPrewritePlan, RealOptimisticTransaction, RecoveryPhase,
};
use super::mutation::OptimisticMutation;
use super::region_batches::{group_keys, RegionKeyBatch};
use super::state::{OptimisticCommitOutcome, TransactionCause};
use super::ttl::MANAGED_LOCK_TTL_MS;
use super::OptimisticCoordinatorError;

/// Wait budget for one locking statement, in client-go's exact encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockWaitTime {
    /// `SELECT ... FOR UPDATE NOWAIT`: fail the statement rather than queue.
    NoWait,
    /// Wait as long as the surrounding call allows.
    AlwaysWait,
    /// Wait at most this long, then fail with a lock-wait timeout.
    Timeout(Duration),
}

impl LockWaitTime {
    /// Encodes the residual budget into `PessimisticLockRequest.wait_timeout`.
    ///
    /// TiKV waits server-side for this many milliseconds before answering, so
    /// the value sent must shrink as the statement's own budget is consumed —
    /// otherwise every retry would restart the full wait.
    fn wait_timeout_ms(self, waited: Duration) -> i64 {
        match self {
            Self::NoWait => LOCK_NO_WAIT,
            Self::AlwaysWait => LOCK_ALWAYS_WAIT,
            Self::Timeout(budget) => {
                let remaining = budget.saturating_sub(waited);
                i64::try_from(remaining.as_millis())
                    .ok()
                    .filter(|remaining| *remaining > 0)
                    .unwrap_or(LOCK_NO_WAIT)
            }
        }
    }

    /// Whether a statement that is still blocked after `waited` must give up.
    const fn is_exhausted(self, waited: Duration) -> bool {
        match self {
            Self::NoWait => true,
            Self::AlwaysWait => false,
            Self::Timeout(budget) => waited.as_millis() >= budget.as_millis(),
        }
    }
}

/// client-go `kv.LockNoWait`.
const LOCK_NO_WAIT: i64 = -1;
/// client-go `kv.LockAlwaysWait`.
const LOCK_ALWAYS_WAIT: i64 = i64::MAX;

/// A deadlock TiKV's detector proved, reported verbatim to the SQL layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadlockDetail {
    /// Start timestamp of the transaction holding the awaited lock.
    pub lock_ts: u64,
    /// Key this statement was blocked on.
    pub lock_key: Vec<u8>,
    /// Hash of the key that closes the cycle.
    pub deadlock_key_hash: u64,
    /// Key this transaction already holds that closes the cycle.
    pub deadlock_key: Vec<u8>,
    /// Every transaction on the detected cycle, as `(txn, wait_for_txn)`.
    pub wait_chain: Vec<(u64, u64)>,
}

impl From<&KvrpcDeadlock> for DeadlockDetail {
    fn from(deadlock: &KvrpcDeadlock) -> Self {
        Self {
            lock_ts: deadlock.lock_ts,
            lock_key: deadlock.lock_key.clone(),
            deadlock_key_hash: deadlock.deadlock_key_hash,
            deadlock_key: deadlock.deadlock_key.clone(),
            wait_chain: deadlock
                .wait_chain
                .iter()
                .map(|entry| (entry.txn, entry.wait_for_txn))
                .collect(),
        }
    }
}

/// Why one locking statement failed.
///
/// The first four variants are statement-scoped in TiDB: the transaction stays
/// usable and the SQL layer decides whether to retry the statement under a
/// newer `for_update_ts`. [`Self::Transaction`] is not — it ends the
/// transaction, exactly like the optimistic path's causes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PessimisticLockFailure {
    /// TiKV's deadlock detector proved a cycle. The statement must be aborted;
    /// retrying it would recreate the same cycle.
    Deadlock(DeadlockDetail),
    /// A newer version of a key was committed after this statement's
    /// `for_update_ts`. Retry the statement under a fresh one.
    WriteConflict {
        /// TiKV conflict diagnostic.
        detail: String,
    },
    /// `NOWAIT` was requested and the key is locked by a live transaction.
    LockAcquireFailAndNoWaitSet {
        /// Exact encoded key that is locked.
        key: Vec<u8>,
    },
    /// The statement's lock-wait budget elapsed while a live owner held the key.
    LockWaitTimeout {
        /// Exact encoded key that is locked.
        key: Vec<u8>,
    },
    /// Anything that ends the transaction rather than the statement.
    Transaction(TransactionCause),
}

impl fmt::Display for PessimisticLockFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deadlock(detail) => write!(
                formatter,
                "deadlock detected on txn {} over key {:?}",
                detail.lock_ts, detail.lock_key
            ),
            Self::WriteConflict { detail } => {
                write!(formatter, "pessimistic write conflict: {detail}")
            }
            Self::LockAcquireFailAndNoWaitSet { .. } => {
                formatter.write_str("lock acquisition failed and NOWAIT is set")
            }
            Self::LockWaitTimeout { .. } => formatter.write_str("lock wait timeout exceeded"),
            Self::Transaction(cause) => cause.fmt(formatter),
        }
    }
}

impl std::error::Error for PessimisticLockFailure {}

impl PessimisticLockFailure {
    /// Whether the transaction survives this failure and the SQL layer may
    /// retry only the statement under a newer `for_update_ts`.
    #[must_use]
    pub const fn is_statement_scoped(&self) -> bool {
        !matches!(self, Self::Transaction(_))
    }
}

/// Locks one statement acquired, and the timestamp they were acquired under.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquiredLocks {
    /// Statement timestamp every lock in this batch carries.
    pub for_update_ts: u64,
    /// Exact encoded keys newly locked, in encoded-key order.
    pub keys: Vec<Vec<u8>>,
    /// Primary key of the transaction, chosen by the first successful lock.
    pub primary_key: Vec<u8>,
    /// Keys fair locking locked *despite* a conflict, with the commit
    /// timestamp of the version that beat this statement.
    ///
    /// Non-empty only under [`RealPessimisticTransaction::set_fair_locking`].
    /// Each entry says "the lock exists, but at this higher timestamp": the
    /// statement's own result is stale and must be recomputed, while the lock
    /// itself carries over to the retry. Go turns the same fact into
    /// `ErrWriteConflict{reason: "LockedWithConflict"}` in
    /// `pkg/store/driver/txn.generateWriteConflictForLockedWithConflict`.
    pub locked_with_conflict: Vec<(Vec<u8>, u64)>,
}

/// One concrete pessimistic transaction over the shared process authorities.
///
/// It owns the optimistic coordinator that will perform the final two-phase
/// commit, so both phases share one `start_ts`, one session, one region cache,
/// and one transport.
pub struct RealPessimisticTransaction<C, L, T> {
    two_pc: RealOptimisticTransaction<C, L, T>,
    for_update_ts: u64,
    opened_at: Instant,
    primary_key: Option<Vec<u8>>,
    locked_keys: BTreeSet<Vec<u8>>,
    fair_locking: bool,
    /// Actual `for_update_ts` of every lock TiKV granted with a conflict, which
    /// is higher than the `for_update_ts` that asked for it.
    locked_with_conflict: BTreeMap<Vec<u8>, u64>,
    /// Highest such timestamp ever seen, kept even after the key is released.
    /// Go's `twoPhaseCommitter.maxLockedWithConflictTS`: PessimisticRollback
    /// must address a lock at the timestamp it really carries.
    max_locked_with_conflict_ts: u64,
}

impl<C, L, T> RealPessimisticTransaction<C, L, T>
where
    C: TransactionCommandClient + LockRecoveryClient,
    L: RegionRecoveryLoader + RegionLoader,
    T: TimestampSource,
{
    /// Wraps an already-opened transaction, fixing its first `for_update_ts`.
    ///
    /// A pessimistic transaction's first statement runs at `start_ts`, exactly
    /// like client-go, which seeds `forUpdateTS` from `startTS`.
    pub fn from_transaction(
        two_pc: RealOptimisticTransaction<C, L, T>,
        opened_at: Instant,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let for_update_ts = two_pc.start_ts();
        Ok(Self {
            two_pc,
            for_update_ts,
            opened_at,
            primary_key: None,
            locked_keys: BTreeSet::new(),
            fair_locking: false,
            locked_with_conflict: BTreeMap::new(),
            max_locked_with_conflict_ts: 0,
        })
    }

    /// Turns fair (aggressive) locking on or off for the statements that follow.
    ///
    /// This is `@@tidb_pessimistic_txn_fair_locking` reaching the transaction.
    /// Go arms it per statement through `StartFairLocking` /
    /// `DoneFairLocking`; here it is a mode the transaction stays in, because
    /// the only state Go's per-statement scope protects — the derived-lock
    /// buffer — is held by the SQL tier that retries the statement.
    pub const fn set_fair_locking(&mut self, enabled: bool) {
        self.fair_locking = enabled;
    }

    /// Whether the next locking statement may use `WakeUpModeForceLock`.
    #[must_use]
    pub const fn is_in_fair_locking_mode(&self) -> bool {
        self.fair_locking
    }

    /// Selects the fast-commit protocol the final 2PC of this pessimistic
    /// transaction may use, from `@@tidb_enable_async_commit` /
    /// `@@tidb_enable_1pc`. The choice is applied by the shared optimistic
    /// coordinator at commit time.
    pub fn set_commit_protocol(&mut self, protocol: super::CommitProtocol) {
        self.two_pc.set_commit_protocol(protocol);
    }

    /// Highest timestamp at which fair locking granted a lock despite conflict.
    #[must_use]
    pub const fn max_locked_with_conflict_ts(&self) -> u64 {
        self.max_locked_with_conflict_ts
    }

    /// Timestamp a PessimisticRollback of this transaction's locks must carry.
    ///
    /// Go `actionPessimisticRollback.handleSingleBatch`: a lock granted with
    /// conflict exists at the conflicting commit timestamp, so releasing it at
    /// the requested `for_update_ts` would leave it behind.
    const fn rollback_for_update_ts(&self) -> u64 {
        if self.max_locked_with_conflict_ts > self.for_update_ts {
            self.max_locked_with_conflict_ts
        } else {
            self.for_update_ts
        }
    }

    /// Snapshot timestamp shared by reads, locks, and the final commit.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.two_pc.start_ts()
    }

    /// Current statement timestamp.
    #[must_use]
    pub const fn for_update_ts(&self) -> u64 {
        self.for_update_ts
    }

    /// Primary key, present once any key has been locked.
    #[must_use]
    pub fn primary_key(&self) -> Option<&[u8]> {
        self.primary_key.as_deref()
    }

    /// Keys this transaction currently holds pessimistic locks on.
    #[must_use]
    pub fn locked_keys(&self) -> Vec<Vec<u8>> {
        self.locked_keys.iter().cloned().collect()
    }

    /// Allocates a fresh statement timestamp before retrying a statement.
    ///
    /// This is what makes a write conflict cost one statement instead of the
    /// transaction: the retry looks at a newer snapshot for its conflict check
    /// while every lock already taken, and `start_ts` itself, stay valid.
    pub fn advance_for_update_ts(&mut self) -> Result<u64, PessimisticLockFailure> {
        let timestamp = self.two_pc.timestamps().current_ts().map_err(|error| {
            PessimisticLockFailure::Transaction(TransactionCause::Timestamp {
                detail: format!("cannot allocate for_update_ts: {error}"),
            })
        })?;
        if timestamp <= self.for_update_ts {
            return Err(PessimisticLockFailure::Transaction(
                TransactionCause::Timestamp {
                    detail: format!(
                        "PD returned for_update_ts {timestamp} that does not advance {}",
                        self.for_update_ts
                    ),
                },
            ));
        }
        self.for_update_ts = timestamp;
        Ok(timestamp)
    }

    /// Acquires pessimistic locks on `keys` at the current `for_update_ts`.
    ///
    /// `presume_not_exists` names the subset that an INSERT expects to be
    /// absent, which TiKV checks while locking so a duplicate key is reported
    /// at statement time rather than at commit.
    pub fn acquire_locks(
        &mut self,
        keys: &[Vec<u8>],
        presume_not_exists: &BTreeSet<Vec<u8>>,
        wait: LockWaitTime,
        call: &UnaryCallContext,
    ) -> Result<AcquiredLocks, PessimisticLockFailure> {
        if keys.is_empty() {
            return Err(PessimisticLockFailure::Transaction(
                TransactionCause::InvalidResponse {
                    detail: "a locking statement requires at least one key".to_owned(),
                },
            ));
        }
        if keys.iter().any(Vec::is_empty) {
            return Err(PessimisticLockFailure::Transaction(
                TransactionCause::InvalidResponse {
                    detail: "an encoded lock key cannot be empty".to_owned(),
                },
            ));
        }
        let mut sorted = keys.to_vec();
        sorted.sort();
        sorted.dedup();
        // The primary is fixed by the first statement that locks anything: it
        // is the key whose lock every other lock, and the whole transaction's
        // recovery, points at.
        let is_first_lock = self.primary_key.is_none();
        let primary_key = match self.primary_key.clone() {
            Some(primary) => primary,
            None => sorted[0].clone(),
        };
        // Go `KVTxn.LockKeys`: ForceLock is requested only while fair locking
        // is armed *and* the statement locks exactly one key. TiKV's ForceLock
        // path answers about a single key, so a multi-key statement stays in
        // Normal mode even under fair locking.
        let wake_up_mode = if self.fair_locking && sorted.len() == 1 {
            KvrpcPessimisticLockWakeUpMode::WakeUpModeForceLock
        } else {
            KvrpcPessimisticLockWakeUpMode::WakeUpModeNormal
        };
        let wait_started_at = Instant::now();
        let mut queue = VecDeque::from(self.group(&sorted)?);
        let mut newly_locked = Vec::new();
        let mut locked_with_conflict: Vec<(Vec<u8>, u64)> = Vec::new();
        while let Some(batch) = queue.pop_front() {
            match self.lock_batch(
                &batch,
                &primary_key,
                is_first_lock,
                presume_not_exists,
                wait,
                wait_started_at,
                wake_up_mode,
                call,
            )? {
                BatchOutcome::Locked { conflicts } => {
                    newly_locked.extend(batch.keys().iter().cloned());
                    locked_with_conflict.extend(conflicts);
                }
                BatchOutcome::Regroup => {
                    for regrouped in self.group(batch.keys())?.into_iter().rev() {
                        queue.push_front(regrouped);
                    }
                }
            }
        }
        self.primary_key = Some(primary_key.clone());
        self.locked_keys.extend(newly_locked.iter().cloned());
        for (key, conflict_ts) in &locked_with_conflict {
            self.locked_with_conflict.insert(key.clone(), *conflict_ts);
            self.max_locked_with_conflict_ts = self.max_locked_with_conflict_ts.max(*conflict_ts);
        }
        Ok(AcquiredLocks {
            for_update_ts: self.for_update_ts,
            keys: newly_locked,
            primary_key,
            locked_with_conflict,
        })
    }

    /// Releases the pessimistic locks on `keys` without ending the transaction.
    ///
    /// A statement that fails after locking part of its key set must not leave
    /// those locks behind: the next statement runs under a newer
    /// `for_update_ts` and would otherwise block on this transaction's own
    /// abandoned locks.
    pub fn pessimistic_rollback(
        &mut self,
        keys: &[Vec<u8>],
        call: &UnaryCallContext,
    ) -> Result<(), TransactionCause> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut queue = VecDeque::from(self.group(keys).map_err(unwrap_transaction_cause)?);
        while let Some(batch) = queue.pop_front() {
            let request = KvrpcPessimisticRollbackRequest {
                start_version: self.start_ts(),
                for_update_ts: self.rollback_for_update_ts(),
                keys: batch.keys().to_vec(),
                ..KvrpcPessimisticRollbackRequest::default()
            };
            let published = match self.two_pc.runtime().client().try_borrow_mut() {
                Ok(mut client) => client.publish_pessimistic_rollback(
                    batch.address(),
                    &request,
                    batch.context(),
                    call,
                ),
                Err(_) => PublishedCommand::BeforePublication(
                    "TiKV client is already borrowed while publishing PessimisticRollback"
                        .to_owned(),
                ),
            };
            match published {
                PublishedCommand::BeforePublication(error)
                | PublishedCommand::AfterPublication { error, .. } => {
                    return Err(TransactionCause::Transport {
                        detail: format!("PessimisticRollback failed: {error}"),
                    });
                }
                PublishedCommand::Response(response) => {
                    if let Some(region_error) = response.response.region_error.as_ref() {
                        self.two_pc.recover_region_error(
                            RecoveryPhase::Cleanup,
                            region_error,
                            batch.attempt(),
                            call,
                        )?;
                        for regrouped in self
                            .group(batch.keys())
                            .map_err(unwrap_transaction_cause)?
                            .into_iter()
                            .rev()
                        {
                            queue.push_front(regrouped);
                        }
                        continue;
                    }
                    if let Some(error) = response.response.errors.first() {
                        return Err(classify_key_error(error));
                    }
                    for key in batch.keys() {
                        self.locked_keys.remove(key);
                        self.locked_with_conflict.remove(key);
                    }
                }
            }
        }
        Ok(())
    }

    /// Commits through the shared two-phase commit engine.
    ///
    /// Every key still locked is declared to Prewrite, so TiKV verifies those
    /// locks instead of re-running an optimistic conflict check that this
    /// transaction already passed statement by statement.
    pub fn commit(
        mut self,
        mutations: Vec<OptimisticMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError> {
        self.two_pc
            .set_pessimistic_prewrite(PessimisticPrewritePlan {
                for_update_ts: self.for_update_ts,
                locked_keys: self.locked_keys.clone(),
                for_update_ts_constraints: self.locked_with_conflict.clone(),
            });
        self.two_pc.commit(mutations, call)
    }

    /// Borrows the underlying two-phase commit transaction for snapshot reads.
    pub fn snapshot(&mut self) -> &mut RealOptimisticTransaction<C, L, T> {
        &mut self.two_pc
    }

    /// Surrenders the two-phase commit transaction without committing.
    ///
    /// A pessimistic transaction that reaches `COMMIT` or `ROLLBACK` having
    /// staged no mutation still has to terminate its coordinator truthfully —
    /// [`RealOptimisticTransaction::finish_without_writes`] is the only state
    /// transition that says "this transaction published nothing". Release any
    /// held locks with [`Self::pessimistic_rollback`] first; nothing here does
    /// it implicitly, because a drop cannot report a failure.
    pub fn into_two_pc(self) -> RealOptimisticTransaction<C, L, T> {
        self.two_pc
    }

    fn group(&self, keys: &[Vec<u8>]) -> Result<Vec<RegionKeyBatch>, PessimisticLockFailure> {
        group_keys(self.two_pc.runtime(), keys).map_err(|error| {
            PessimisticLockFailure::Transaction(TransactionCause::Region {
                detail: format!("pessimistic lock region grouping failed: {error}"),
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_batch(
        &mut self,
        batch: &RegionKeyBatch,
        primary_key: &[u8],
        is_first_lock: bool,
        presume_not_exists: &BTreeSet<Vec<u8>>,
        wait: LockWaitTime,
        wait_started_at: Instant,
        wake_up_mode: KvrpcPessimisticLockWakeUpMode,
        call: &UnaryCallContext,
    ) -> Result<BatchOutcome, PessimisticLockFailure> {
        let mutations = batch
            .keys()
            .iter()
            .map(|key| KvrpcMutation {
                op: KvrpcOp::PessimisticLock as i32,
                key: key.clone(),
                value: Vec::new(),
                assertion: if presume_not_exists.contains(key) {
                    KvrpcAssertion::NotExist as i32
                } else {
                    KvrpcAssertion::None as i32
                },
            })
            .collect::<Vec<_>>();
        loop {
            let waited = wait_started_at.elapsed();
            let request = KvrpcPessimisticLockRequest {
                mutations: mutations.clone(),
                primary_lock: primary_key.to_vec(),
                start_version: self.start_ts(),
                // A pessimistic lock is refreshed by the keep-alive manager, so
                // its TTL only has to cover the time already elapsed plus one
                // managed interval.
                lock_ttl: elapsed_ms(self.opened_at).saturating_add(MANAGED_LOCK_TTL_MS),
                for_update_ts: self.for_update_ts,
                is_first_lock,
                wait_timeout: wait.wait_timeout_ms(waited),
                min_commit_ts: self.for_update_ts.saturating_add(1),
                wake_up_mode: wake_up_mode as i32,
                ..KvrpcPessimisticLockRequest::default()
            };
            let response = self.publish_lock(batch, &request, call)?;
            if let Some(region_error) = response.region_error.as_ref() {
                self.two_pc
                    .recover_region_error(
                        RecoveryPhase::Forward,
                        region_error,
                        batch.attempt(),
                        call,
                    )
                    .map_err(PessimisticLockFailure::Transaction)?;
                return Ok(BatchOutcome::Regroup);
            }
            if matches!(
                wake_up_mode,
                KvrpcPessimisticLockWakeUpMode::WakeUpModeForceLock
            ) {
                match self.read_force_lock_result(batch, &response)? {
                    ForceLockOutcome::Locked { conflicts } => {
                        return Ok(BatchOutcome::Locked { conflicts })
                    }
                    // TiKV refused this key. It reports why in `errors`, and
                    // the same blocker handling as Normal mode decides whether
                    // the statement may try again.
                    ForceLockOutcome::Failed => {
                        self.wait_out_blockers(
                            &response.errors,
                            batch,
                            wait,
                            wait_started_at,
                            call,
                        )?;
                        continue;
                    }
                }
            }
            if !response.results.is_empty() {
                // `results` belongs to `WakeUpModeForceLock`, which this
                // request did not ask for. A server that fills it is answering
                // a protocol this client did not speak.
                return Err(PessimisticLockFailure::Transaction(
                    TransactionCause::InvalidResponse {
                        detail: "PessimisticLock returned ForceLock results for a Normal wake-up"
                            .to_owned(),
                    },
                ));
            }
            if response.errors.is_empty() {
                return Ok(BatchOutcome::Locked {
                    conflicts: Vec::new(),
                });
            }
            self.wait_out_blockers(&response.errors, batch, wait, wait_started_at, call)?;
        }
    }

    /// Reads the per-key `results` a `WakeUpModeForceLock` answer carries.
    ///
    /// Go `actionPessimisticLock.handlePessimisticLockResponseForceLockMode`.
    /// The shape is exact: at most one mutation and one result, because TiKV's
    /// ForceLock path answers about a single key. Everything Go reaches by
    /// `panic("unreachable")` or `errors.New("Pessimistic lock response
    /// corrupted")` is a response this client did not ask for, so it ends the
    /// transaction rather than the statement.
    fn read_force_lock_result(
        &self,
        batch: &RegionKeyBatch,
        response: &KvrpcPessimisticLockResponse,
    ) -> Result<ForceLockOutcome, PessimisticLockFailure> {
        let corrupted = |detail: &str| {
            PessimisticLockFailure::Transaction(TransactionCause::InvalidResponse {
                detail: detail.to_owned(),
            })
        };
        if batch.keys().len() > 1 || response.results.len() > 1 {
            return Err(corrupted(
                "ForceLock addresses exactly one key, and TiKV answered about more",
            ));
        }
        let Some(result) = response.results.first() else {
            // No result at all: TiKV must have reported a region error (already
            // handled) or a terminal key error, so let the key errors speak.
            if response.errors.is_empty() {
                return Err(corrupted(
                    "ForceLock PessimisticLock answered with neither a result nor an error",
                ));
            }
            return Ok(ForceLockOutcome::Failed);
        };
        let key = batch.keys()[0].clone();
        match KvrpcPessimisticLockKeyResultType::try_from(result.r#type) {
            Ok(KvrpcPessimisticLockKeyResultType::LockResultNormal) => {
                Ok(ForceLockOutcome::Locked {
                    conflicts: Vec::new(),
                })
            }
            Ok(KvrpcPessimisticLockKeyResultType::LockResultLockedWithConflict) => {
                // TiKV grants the lock at the conflicting version's commit
                // timestamp, which is by construction newer than the one asked
                // for. Anything else would leave the lock unaddressable.
                if result.locked_with_conflict_ts <= self.for_update_ts {
                    return Err(corrupted(&format!(
                        "LockedWithConflict timestamp {} does not exceed the requested for_update_ts {}",
                        result.locked_with_conflict_ts, self.for_update_ts
                    )));
                }
                Ok(ForceLockOutcome::Locked {
                    conflicts: vec![(key, result.locked_with_conflict_ts)],
                })
            }
            Ok(KvrpcPessimisticLockKeyResultType::LockResultFailed) => Ok(ForceLockOutcome::Failed),
            Err(_) => Err(corrupted(&format!(
                "unknown PessimisticLockKeyResultType {}",
                result.r#type
            ))),
        }
    }

    fn publish_lock(
        &mut self,
        batch: &RegionKeyBatch,
        request: &KvrpcPessimisticLockRequest,
        call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticLockResponse, PessimisticLockFailure> {
        let published = match self.two_pc.runtime().client().try_borrow_mut() {
            Ok(mut client) => {
                client.publish_pessimistic_lock(batch.address(), request, batch.context(), call)
            }
            Err(_) => PublishedCommand::BeforePublication(
                "TiKV client is already borrowed while publishing PessimisticLock".to_owned(),
            ),
        };
        match published {
            PublishedCommand::Response(response) => Ok(response.response),
            // A lost PessimisticLock response is not ambiguous the way a lost
            // Commit is: the lock either exists or it does not, and either way
            // the transaction has not written anything yet.
            PublishedCommand::BeforePublication(error)
            | PublishedCommand::AfterPublication { error, .. } => Err(
                PessimisticLockFailure::Transaction(TransactionCause::Transport {
                    detail: format!("PessimisticLock failed: {error}"),
                }),
            ),
        }
    }

    /// Classifies the blockers TiKV reported and decides whether to retry.
    ///
    /// Returning `Ok` means "retry the same batch"; every other outcome is a
    /// terminal failure for this statement.
    fn wait_out_blockers(
        &mut self,
        errors: &[KvrpcKeyError],
        batch: &RegionKeyBatch,
        wait: LockWaitTime,
        wait_started_at: Instant,
        call: &UnaryCallContext,
    ) -> Result<(), PessimisticLockFailure> {
        let blockers = collect_blocking_locks(errors)?;
        if blockers.is_empty() {
            // TiKV reported only lock errors it wants re-sent, which is how a
            // wait-timeout wake-up looks when nothing is worth resolving.
            return self.check_wait_budget(wait, wait_started_at, batch.keys().first());
        }
        let blocked_key = blockers[0].key().to_vec();
        let recovery = resolve_blocking_locks(
            self.two_pc.runtime(),
            &blockers,
            self.start_ts(),
            batch.context(),
            call,
            self.two_pc.timestamps(),
        )
        .map_err(|error| {
            PessimisticLockFailure::Transaction(TransactionCause::Lock {
                key: blocked_key.clone(),
                detail: format!("pessimistic lock recovery failed: {error}"),
            })
        })?;
        match recovery {
            // The blockers are gone; the immediate retry will take the lock.
            LockRecoveryResult::Resolved(_) => Ok(()),
            // At least one owner is alive. TiKV already queued and woke this
            // request, so client-go does not add its own backoff here; only the
            // statement's own budget decides whether to try again.
            LockRecoveryResult::Alive(_) => {
                self.check_wait_budget(wait, wait_started_at, Some(&blocked_key))
            }
        }
    }

    fn check_wait_budget(
        &self,
        wait: LockWaitTime,
        wait_started_at: Instant,
        blocked_key: Option<&Vec<u8>>,
    ) -> Result<(), PessimisticLockFailure> {
        let key = blocked_key.cloned().unwrap_or_default();
        if matches!(wait, LockWaitTime::NoWait) {
            return Err(PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key });
        }
        if wait.is_exhausted(wait_started_at.elapsed()) {
            return Err(PessimisticLockFailure::LockWaitTimeout { key });
        }
        // `AlwaysWait` still ends at the surrounding call's deadline: without
        // this, a statement waiting on a lock nobody will ever release would
        // spin forever rather than surface the caller's timeout.
        if wait_started_at.elapsed() >= self.two_pc.call_timeout() {
            return Err(PessimisticLockFailure::Transaction(
                TransactionCause::Transport {
                    detail: "pessimistic lock wait reached the call deadline".to_owned(),
                },
            ));
        }
        Ok(())
    }
}

enum BatchOutcome {
    Locked {
        /// Keys fair locking granted at a higher timestamp than requested.
        conflicts: Vec<(Vec<u8>, u64)>,
    },
    Regroup,
}

/// What one `WakeUpModeForceLock` answer said about its single key.
enum ForceLockOutcome {
    /// The lock exists — at the requested `for_update_ts` when `conflicts` is
    /// empty, otherwise at the conflicting version's commit timestamp.
    Locked { conflicts: Vec<(Vec<u8>, u64)> },
    /// TiKV did not grant the lock; `errors` says why.
    Failed,
}

/// Extracts the locks worth resolving, failing on anything terminal.
///
/// Order matters: a deadlock or a duplicate key ends the statement even if the
/// same response also reports resolvable locks, because retrying cannot change
/// either answer.
fn collect_blocking_locks(
    errors: &[KvrpcKeyError],
) -> Result<Vec<BlockingLock>, PessimisticLockFailure> {
    let mut blockers = Vec::new();
    for error in errors {
        if let Some(deadlock) = error.deadlock.as_ref() {
            return Err(PessimisticLockFailure::Deadlock(DeadlockDetail::from(
                deadlock,
            )));
        }
        if let Some(conflict) = error.conflict.as_ref() {
            return Err(PessimisticLockFailure::WriteConflict {
                detail: format!("{conflict:?}"),
            });
        }
        let Some(lock_info) = error.locked.as_ref() else {
            return Err(PessimisticLockFailure::Transaction(classify_key_error(
                error,
            )));
        };
        let observed = decode_blocking_lock_observation(lock_info).map_err(|error| {
            PessimisticLockFailure::Transaction(TransactionCause::InvalidResponse {
                detail: format!("invalid PessimisticLock lock observation: {error}"),
            })
        })?;
        for lock in observed {
            // A lock refreshed moments ago is owned by a demonstrably live
            // transaction; resolving it would cost an RPC to learn nothing.
            if lock.duration_to_last_update_ms() > 0
                && lock.duration_to_last_update_ms() < crate::lock::SKIP_RESOLVE_THRESHOLD_MS
            {
                continue;
            }
            blockers.push(lock);
        }
    }
    Ok(blockers)
}

fn unwrap_transaction_cause(failure: PessimisticLockFailure) -> TransactionCause {
    match failure {
        PessimisticLockFailure::Transaction(cause) => cause,
        other => TransactionCause::InvalidResponse {
            detail: other.to_string(),
        },
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_budget_sent_to_tikv_shrinks_as_the_statement_waits() {
        let wait = LockWaitTime::Timeout(Duration::from_millis(1_000));
        assert_eq!(wait.wait_timeout_ms(Duration::ZERO), 1_000);
        assert_eq!(wait.wait_timeout_ms(Duration::from_millis(600)), 400);
        // A spent budget must not be re-sent as a fresh full wait, and must not
        // be sent as "wait forever" either: it becomes an explicit no-wait.
        assert_eq!(
            wait.wait_timeout_ms(Duration::from_millis(1_000)),
            LOCK_NO_WAIT
        );
        assert_eq!(wait.wait_timeout_ms(Duration::from_secs(60)), LOCK_NO_WAIT);
    }

    #[test]
    fn no_wait_and_always_wait_keep_their_pinned_encodings() {
        assert_eq!(
            LockWaitTime::NoWait.wait_timeout_ms(Duration::ZERO),
            LOCK_NO_WAIT
        );
        assert_eq!(
            LockWaitTime::AlwaysWait.wait_timeout_ms(Duration::from_secs(600)),
            LOCK_ALWAYS_WAIT
        );
    }

    #[test]
    fn only_a_bounded_budget_can_be_exhausted() {
        assert!(LockWaitTime::NoWait.is_exhausted(Duration::ZERO));
        assert!(!LockWaitTime::AlwaysWait.is_exhausted(Duration::from_secs(86_400)));
        let wait = LockWaitTime::Timeout(Duration::from_millis(500));
        assert!(!wait.is_exhausted(Duration::from_millis(499)));
        assert!(wait.is_exhausted(Duration::from_millis(500)));
    }

    #[test]
    fn a_statement_scoped_failure_does_not_end_the_transaction() {
        assert!(PessimisticLockFailure::WriteConflict {
            detail: String::new()
        }
        .is_statement_scoped());
        assert!(PessimisticLockFailure::LockWaitTimeout { key: Vec::new() }.is_statement_scoped());
        assert!(
            !PessimisticLockFailure::Transaction(TransactionCause::Transport {
                detail: String::new()
            })
            .is_statement_scoped()
        );
    }
}
