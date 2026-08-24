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

use std::cell::Cell;
use std::fmt;
use std::time::Duration;

use tidb_proto::{
    KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse, KvrpcCheckTxnStatusRequest,
    KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcPeer, KvrpcPessimisticRollbackRequest,
    KvrpcPessimisticRollbackResponse, KvrpcRegionEpoch, KvrpcResolveLockRequest,
    KvrpcResolveLockResponse, KvrpcTxnAction,
};

use crate::region::{
    ReadPolicy, RegionAttempt, RegionBackoffBudget, RegionBackoffKind, RegionErrorDisposition,
    RegionRecoveryError, RegionRecoveryLoader, RequestSelection,
};
use crate::rpc::TonicCoprocessorClient;
use crate::{DirectUnaryClientError, SharedReadRuntime, UnaryCallContext};

use super::{LockAdmissionError, OptimisticLock};

/// Go `getTxnStatusMaxBackoff` (`txnkv/txnlock/lock_resolver.go:51`).
const GET_TXN_STATUS_MAX_BACKOFF: Duration = Duration::from_millis(20_000);

/// Exact timestamp authority injected by the caller.
pub trait TimestampSource: fmt::Debug {
    /// Returns a fresh real TSO value on every call.
    ///
    /// Wall-clock synthesis and replaying a previously returned TSO are not
    /// admitted. Callers may require a second value after a slow status RPC.
    fn current_ts(&self) -> Result<u64, String>;
}

/// One-shot injected TSO for paths that can prove they need only one value.
#[derive(Debug)]
pub struct FixedTimestampSource {
    timestamp: Cell<Option<u64>>,
}

impl FixedTimestampSource {
    /// Creates a source that returns `timestamp` exactly once.
    #[must_use]
    pub const fn new(timestamp: u64) -> Self {
        Self {
            timestamp: Cell::new(Some(timestamp)),
        }
    }
}

impl TimestampSource for FixedTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        let timestamp = self
            .timestamp
            .take()
            .ok_or_else(|| "one-shot timestamp source is exhausted".to_owned())?;
        if timestamp == 0 {
            return Err("current timestamp must be a real nonzero TSO".to_owned());
        }
        Ok(timestamp)
    }
}

/// Typed commands required from the sole shared TiKV client.
pub trait LockRecoveryClient {
    /// Sends CheckTxnStatus through the client's existing unary core.
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError>;

    /// Sends CheckSecondaryLocks through the client's existing unary core.
    fn check_secondary_locks_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckSecondaryLocksRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError>;

    /// Sends keyed ResolveLock through the client's existing unary core.
    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError>;

    /// Sends keyed PessimisticRollback cleaning one expired pessimistic lock.
    ///
    /// ResolveLock cannot clean a pessimistic lock: there is no commit record
    /// to redo or undo, only a lock entry that must be dropped at its exact
    /// `for_update_ts`.
    fn pessimistic_rollback_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError>;
}

impl LockRecoveryClient for TonicCoprocessorClient {
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        self.check_txn_status(address, request, context, call)
    }

    fn check_secondary_locks_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckSecondaryLocksRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        self.check_secondary_locks(address, request, context, call)
    }

    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        self.resolve_lock(address, request, context, call)
    }

    fn pessimistic_rollback_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        let decoded = self
            .begin_transaction_pessimistic_rollback(address, None, request, context, call)?
            .complete(call)
            .map_err(|error| DirectUnaryClientError::InvalidRequest(error.to_string()))??;
        Ok(decoded.response)
    }
}

/// Determined transaction status returned by CheckTxnStatus.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedTxnStatus {
    /// The primary has committed at this exact commit timestamp.
    Committed(u64),
    /// The primary is rolled back.
    RolledBack,
}

/// Bounded outcome returned to DistSQL's same-task retry owner.
///
/// Go `txnlock.ResolveLockResult` (`lock_resolver.go:405-411`). One batch of
/// locks can end in more than one way at once — one owner still running while
/// another's min-commit-ts was pushed past the reader — so the three answers
/// are fields, not alternatives. Collapsing them into an enum would silently
/// drop the pushed transaction's id and make the reader meet the same lock
/// forever.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LockRecoveryResult {
    /// Shortest wait any still-running owner asked for; zero means none did.
    ///
    /// Go `ResolveLockResult.TTL`, produced by `txnExpireTime`, whose
    /// uninitialised value is likewise `0`.
    pub ttl: Duration,
    /// Determined fates observed while cleaning the expired locks.
    pub statuses: Vec<ResolvedTxnStatus>,
    /// Transactions whose locks the reader may step over.
    ///
    /// Go `ResolveLockResult.IgnoreLocks` -> `Context.resolved_locks`.
    pub ignore_locks: Vec<u64>,
    /// Transactions committed at or before the reader, whose value the reader
    /// must see through the lock.
    ///
    /// Go `ResolveLockResult.AccessLocks` -> `Context.committed_locks`.
    pub access_locks: Vec<u64>,
}

impl LockRecoveryResult {
    /// Whether at least one owner is still running and asked to be waited for.
    ///
    /// Go tests `msBeforeTxnExpired > 0` at every caller.
    #[must_use]
    pub const fn is_alive(&self) -> bool {
        !self.ttl.is_zero()
    }

    /// One owner is still running and nothing was decided.
    #[must_use]
    pub(crate) fn alive(ttl: Duration) -> Self {
        Self {
            ttl,
            ..Self::default()
        }
    }

    /// One lock whose owner's fate is now decided.
    ///
    /// Go `lock_resolver.go:632-651` sorts exactly these three cases, and it
    /// does so on the write path too — the write callers simply discard the
    /// lists. Sorting here as well keeps one rule instead of two.
    #[must_use]
    pub(crate) fn resolved(txn_id: u64, status: ResolvedTxnStatus, caller_start_ts: u64) -> Self {
        let mut result = Self {
            statuses: vec![status],
            ..Self::default()
        };
        result.classify(txn_id, status, caller_start_ts);
        result
    }

    /// Files one decided status under `ignore` or `access`.
    fn classify(&mut self, txn_id: u64, status: ResolvedTxnStatus, caller_start_ts: u64) {
        match status {
            // Go: `status.IsCommitted() && status.CommitTS() <= callerStartTS`
            // — the reader is entitled to the value under the lock.
            ResolvedTxnStatus::Committed(commit_ts) if commit_ts <= caller_start_ts => {
                self.access_locks.push(txn_id);
            }
            // Go: `status.IsRolledBack()`, and committed strictly after the
            // reader — either way there is nothing here for it to see.
            ResolvedTxnStatus::Committed(_) | ResolvedTxnStatus::RolledBack => {
                self.ignore_locks.push(txn_id);
            }
        }
    }
}

/// The two per-reader timestamp sets a read replays into its request context.
///
/// Go `KVSnapshot.resolvedLocks` / `KVSnapshot.committedLocks`
/// (`snapshot.go:124-125`), filled from [`LockRecoveryResult`] by
/// `ClientHelper` (`client_helper.go:97-122`) and stamped onto
/// `Context.resolved_locks` / `Context.committed_locks` before every send
/// (`client_helper.go:148-149`). Go guards them with a mutex because a
/// snapshot is shared across goroutines; a reader here is owned by one caller,
/// so the ordered set alone carries the same meaning.
///
/// This set is what makes a lock the reader has already classified stop
/// blocking it. Without it the retry meets the same lock again — the deadloop
/// `client_helper.go`'s own comment warns about.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotLockSet {
    /// Go `resolvedLocks` -> `Context.resolved_locks`.
    ignore: std::collections::BTreeSet<u64>,
    /// Go `committedLocks` -> `Context.committed_locks`.
    access: std::collections::BTreeSet<u64>,
    /// The read timestamp the classifications were made against. Go scopes
    /// `resolvedLocks`/`committedLocks` to ONE `KVSnapshot`, and a snapshot
    /// has one version -- a pessimistic retry reads through a FRESH snapshot
    /// at `for_update_ts`, so no classification survives a version change.
    /// `None` is a set nothing has classified yet.
    classified_at: Option<u64>,
}

impl SnapshotLockSet {
    /// Go's per-`KVSnapshot` scoping: entering a read at a DIFFERENT
    /// timestamp discards every earlier classification, because ignore/access
    /// verdicts are relative to the version they were decided at. A lock
    /// bypassed as "committed after my snapshot" at `start_ts` may be exactly
    /// the committed value a `for_update_ts` retry exists to observe; a stale
    /// stamp would make TiKV skip that lock forever, a sticky stale read.
    pub fn rescope(&mut self, read_ts: u64) {
        if self.classified_at != Some(read_ts) {
            // Go `KVSnapshot.SetSnapshotTS`
            // (`txnkv/txnsnapshot/snapshot.go:189-202`) clears exactly ONE of
            // the two sets -- `s.resolvedLocks = util.TSSet{}`, "remove the
            // minCommitTS pushed information" -- and deliberately leaves
            // `committedLocks` standing. That asymmetry is right: `ignore`
            // carries this reader's pushed-min-commit-ts decisions, which are
            // relative to the version they were made at, while `access`
            // records that a transaction COMMITTED at or before the reader --
            // and since a snapshot timestamp only ever advances here (a
            // pessimistic retry moves to a newer `for_update_ts`), that fact
            // stays true at the new version.
            self.ignore.clear();
            self.classified_at = Some(read_ts);
        }
    }

    /// Go `ClientHelper.ResolveLocks`: whatever the resolver classified is put
    /// into the reader's sets and stays there for the SNAPSHOT's life (see
    /// [`Self::rescope`] for the version boundary).
    pub fn absorb(&mut self, result: &LockRecoveryResult) {
        self.ignore.extend(result.ignore_locks.iter().copied());
        self.access.extend(result.access_locks.iter().copied());
    }

    /// Go `ClientHelper.SendReqCtx`: both sets are stamped onto the context of
    /// every request this reader sends after the first resolve.
    pub fn stamp(&self, context: &mut KvrpcContext) {
        context.resolved_locks = self.ignore.iter().copied().collect();
        context.committed_locks = self.access.iter().copied().collect();
    }

    /// Whether anything has been classified yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ignore.is_empty() && self.access.is_empty()
    }
}

/// Fail-closed recovery errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockRecoveryError {
    /// LockInfo does not belong to the bounded optimistic protocol.
    Admission(LockAdmissionError),
    /// Injected timestamp authority failed.
    Timestamp(String),
    /// The sole region cache is already borrowed by another owner.
    RegionCacheLifecycle,
    /// The sole client is already borrowed by another owner.
    ClientLifecycle,
    /// Primary or secondary route selection failed.
    Route(String),
    /// The bounded path never retries topology errors internally.
    RegionError(String),
    /// TxnNotFound, primary mismatch, and every other KeyError fail closed.
    KeyError(String),
    /// The CheckTxnStatus response is neither alive, committed, nor rolled back.
    UndeterminedStatus {
        /// Lock action returned by TiKV when transaction status cannot be determined.
        action: i32,
    },
    /// The same client/core failed the typed unary command.
    Rpc(String),
    /// The canonical read cancellation won before further lock recovery.
    CallerCancelled,
    /// CheckSecondaryLocks answered with a lock that is not async-commit.
    ///
    /// Go `nonAsyncCommitLock`: the primary said async commit and a secondary
    /// disagrees, so the two views of the same transaction cannot both be true.
    NonAsyncCommitLock,
    /// An async-commit recovery observed a self-contradictory commit timestamp.
    AsyncCommitConflict(String),
    /// The resolver's local TxnNotFound retry loop spent its whole budget.
    ///
    /// Go's public `GetTxnStatus` owns the same 20-second local bound, but
    /// prewrite passes a shared 40-second backoffer. Callers must therefore not
    /// infer `BoTxnNotFound`'s SQL identity from this local boundary alone.
    StatusBackoffExhausted,
    /// The next TxnNotFound delay would outlive this RPC caller's deadline.
    ///
    /// This is not Go's `BoTxnNotFound` max-sleep exhaustion and therefore
    /// must not acquire its registered resolve-lock-timeout identity.
    StatusRetryDeadlineExceeded,
}

impl fmt::Display for LockRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => write!(formatter, "lock admission failed: {error}"),
            Self::Timestamp(error) => write!(formatter, "timestamp source failed: {error}"),
            Self::RegionCacheLifecycle => {
                formatter.write_str("shared region cache is already borrowed")
            }
            Self::ClientLifecycle => formatter.write_str("shared TiKV client is already borrowed"),
            Self::Route(error) => write!(formatter, "lock route failed: {error}"),
            Self::RegionError(error) => {
                write!(formatter, "lock RPC returned region error: {error}")
            }
            Self::KeyError(error) => write!(formatter, "lock RPC returned key error: {error}"),
            Self::UndeterminedStatus { action } => {
                write!(
                    formatter,
                    "CheckTxnStatus returned undetermined action {action}"
                )
            }
            Self::Rpc(error) => write!(formatter, "lock RPC failed: {error}"),
            Self::CallerCancelled => formatter.write_str("lock recovery cancelled by caller"),
            Self::NonAsyncCommitLock => {
                formatter.write_str("CheckSecondaryLocks returned a non-async-commit lock")
            }
            Self::AsyncCommitConflict(error) => {
                write!(formatter, "async commit recovery is inconsistent: {error}")
            }
            Self::StatusBackoffExhausted => formatter.write_str(
                "CheckTxnStatus kept reporting TxnNotFound until the backoff budget ran out",
            ),
            Self::StatusRetryDeadlineExceeded => {
                formatter.write_str("CheckTxnStatus retry delay would outlive the caller deadline")
            }
        }
    }
}

impl std::error::Error for LockRecoveryError {}

impl From<LockAdmissionError> for LockRecoveryError {
    fn from(error: LockAdmissionError) -> Self {
        Self::Admission(error)
    }
}

/// Resolves admitted locks using only the supplied shared runtime.
///
/// `for_read` is Go's `ResolveLocksOptions.ForRead`
/// (`ResolveLocksForRead` vs `ResolveLocks`). It is not a tuning knob: a
/// reader may step over or read through a live lock and a writer may not, so
/// the same lock legitimately ends the call two different ways.
pub fn resolve_optimistic_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[OptimisticLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
    for_read: bool,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    let mut result = LockRecoveryResult {
        statuses: Vec::with_capacity(locks.len()),
        ..LockRecoveryResult::default()
    };
    let mut minimum_wait = None::<Duration>;
    for lock in locks {
        // Go `resolve(l, forceSyncCommit)` (`lock_resolver.go:577-621`) is a
        // closure that calls itself exactly once more, with forceSyncCommit
        // set, when the async-commit recovery reports a secondary that is not
        // an async-commit lock. Two views of one transaction cannot both be
        // true, and the sync-commit view is the one TiKV can still answer.
        let outcome = match resolve_one_optimistic_lock(
            runtime,
            lock,
            caller_start_ts,
            base_context,
            call,
            timestamp_source,
            false,
        ) {
            Err(LockRecoveryError::NonAsyncCommitLock) => resolve_one_optimistic_lock(
                runtime,
                lock,
                caller_start_ts,
                base_context,
                call,
                timestamp_source,
                true,
            )?,
            other => other?,
        };
        // Go `lock_resolver.go:632-651` classifies every lock, whatever its
        // outcome was, into exactly one of three buckets.
        match outcome {
            // Go `lock_resolver.go:626-632`: a writer never reads through or
            // around a lock, so for it a live owner ends the classification
            // right here whatever `action` said.
            OneLockOutcome::Alive { wait, .. } if !for_read => {
                minimum_wait = Some(minimum_wait.map_or(wait, |current| current.min(wait)));
            }
            // Go: `status.action == kvrpcpb.Action_MinCommitTSPushed`. TiKV
            // moved this still-running transaction's min-commit-ts above the
            // reader's timestamp, so whatever it eventually commits at is
            // invisible here. The lock is stepped over, not waited for.
            OneLockOutcome::Alive {
                min_commit_ts_pushed: true,
                ..
            } => result.ignore_locks.push(lock.txn_id),
            OneLockOutcome::Resolved(status) => {
                result.statuses.push(status);
                result.classify(lock.txn_id, status, caller_start_ts);
            }
            // Go's trailing `else`: the owner is alive and its min-commit-ts
            // was not pushed, so the reader owes it the rest of its TTL.
            OneLockOutcome::Alive { wait, .. } => {
                minimum_wait = Some(minimum_wait.map_or(wait, |current| current.min(wait)));
            }
        }
    }
    result.ttl = minimum_wait.unwrap_or_default();
    Ok(result)
}

/// What one lock's recovery concluded.
///
/// Go carries this as one `TxnStatus`: `ttl != 0` is the alive state and
/// `action` rides along with it, which is why the pushed min-commit-ts is a
/// field of the alive answer rather than an answer of its own.
pub(super) enum OneLockOutcome {
    /// The owner is still running (Go `status.ttl != 0`).
    Alive {
        /// How long is left of the owner's TTL.
        wait: Duration,
        /// Go `status.action == Action_MinCommitTSPushed`: TiKV moved this
        /// owner's min-commit-ts above the caller, so a *reader* may step over
        /// the lock instead of waiting. A writer still waits.
        min_commit_ts_pushed: bool,
    },
    /// The owner's fate is decided and this lock has been cleaned.
    Resolved(ResolvedTxnStatus),
}

fn resolve_one_optimistic_lock<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    lock: &OptimisticLock,
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
    force_sync_commit: bool,
) -> Result<OneLockOutcome, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    let check_response = match query_txn_status(
        runtime,
        &LockStatusQuery {
            primary: &lock.primary,
            txn_id: lock.txn_id,
            ttl_ms: lock.ttl_ms,
            resolving_pessimistic_lock: false,
        },
        caller_start_ts,
        base_context,
        call,
        timestamp_source,
        force_sync_commit,
    )? {
        LockStatus::Answered(response) => response,
        // Both of these are gated on `resolving_pessimistic_lock` inside the
        // query, exactly as Go gates them (`lock_resolver.go:947-968,1069`), so
        // an optimistic lock reaches neither. Go's own non-pessimistic arm at
        // `lock_resolver.go:581-584` raises the error, and so does this.
        LockStatus::AlivePessimistic(ttl_ms) => {
            return Ok(OneLockOutcome::Alive {
                wait: Duration::from_millis(ttl_ms),
                min_commit_ts_pushed: false,
            });
        }
        LockStatus::PrimaryMismatch => {
            return Err(LockRecoveryError::KeyError(
                "CheckTxnStatus reported primary mismatch for an optimistic lock".to_owned(),
            ));
        }
    };
    // Go `getTxnStatus` (`txnkv/txnlock/lock_resolver.go:1080`) stores this
    // field with a bare `status.primaryLock = cmdResp.LockInfo` -- no
    // admission, no type gate -- and only ever asks it
    // `primaryLock.UseAsyncCommit` (`:341,:591`), plus `.Secondaries` and
    // `.MinCommitTs` once that is true (`:1276,:1335`). So the faithful port
    // is to take what TiKV returned and read those fields.
    //
    // Admitting it through the optimistic-only gate instead failed the whole
    // status answer with "lock admission failed: pessimistic lock type 5 is
    // outside bounded recovery" whenever a read met a live pessimistic
    // transaction -- and a pessimistic primary is simply the ordinary state
    // of one still in its locking phase, which answers "not an async-commit
    // primary" and nothing more.
    let primary_lock = check_response
        .lock_info
        .as_ref()
        .map(|info| OptimisticLock {
            key: info.key.clone(),
            primary: info.primary_lock.clone(),
            txn_id: info.lock_version,
            ttl_ms: info.lock_ttl,
            txn_size: info.txn_size,
            lock_type: info.lock_type,
            min_commit_ts: info.min_commit_ts,
            use_async_commit: info.use_async_commit,
            secondaries: info.secondaries.clone(),
        });
    if check_response.lock_ttl > 0 {
        let async_commit_primary = primary_lock
            .as_ref()
            .is_some_and(|primary_lock| primary_lock.use_async_commit);
        // client-go sends the pre-RPC TSO in CheckTxnStatus, then asks the
        // oracle again when converting the returned absolute lock TTL to
        // a remaining wait. A slow status RPC must consume its own time.
        check_cancelled(call)?;
        let post_check_ts = timestamp_source
            .current_ts()
            .map_err(LockRecoveryError::Timestamp)?;
        check_cancelled(call)?;
        let ttl = remaining_lock_ttl(lock.txn_id, check_response.lock_ttl, post_check_ts);
        // Go `expiredAsyncCommitLocks`: an async-commit primary that is
        // still present but expired is not "alive". Its commit point is the
        // completed prewrite, so waiting for a TTL nobody will refresh only
        // stalls the reader; the fate must come from the secondaries.
        // `!forceSyncCommit` is part of Go's condition: the retry exists
        // precisely to stop taking the async-commit path for this lock.
        if async_commit_primary && !force_sync_commit && ttl.is_zero() {
            let primary_lock = primary_lock.expect("an async-commit primary was observed");
            return Ok(OneLockOutcome::Resolved(resolve_async_commit_lock(
                runtime,
                lock,
                &primary_lock,
                base_context,
                call,
            )?));
        }
        return Ok(OneLockOutcome::Alive {
            wait: ttl,
            // Go keeps this on `TxnStatus.action` and reads it back in the
            // classification loop, so the wait and the push are one answer.
            min_commit_ts_pushed: check_response.action == KvrpcTxnAction::MinCommitTsPushed as i32,
        });
    }
    let status = classify_determined_status(&check_response)?;
    if lock.key != lock.primary {
        check_cancelled(call)?;
        resolve_secondary(runtime, lock, status, base_context, call)?;
    }
    Ok(OneLockOutcome::Resolved(status))
}

/// The identity one CheckTxnStatus query needs from the lock it is about.
pub(super) struct LockStatusQuery<'a> {
    /// Primary key named by the lock, which the query is sent to.
    pub(super) primary: &'a [u8],
    /// Owning transaction's start timestamp.
    pub(super) txn_id: u64,
    /// The lock's own TTL as TiKV reported it.
    ///
    /// Two distinct roles, both of them load-bearing: zero selects the
    /// unconditional-resolve protocol below, and any other value is the expiry
    /// input that decides whether a TxnNotFound answer escalates to
    /// `rollback_if_not_exist`.
    pub(super) ttl_ms: u64,
    /// Whether the lock being cleaned is a pessimistic lock, which TiKV needs
    /// in order to answer `primary_mismatch` and which gates the two
    /// pessimistic-only arms of Go's TxnNotFound handling.
    pub(super) resolving_pessimistic_lock: bool,
}

/// How a status query ended, short of an error.
///
/// The decoded response dwarfs the other two variants, and it is the one
/// returned on essentially every call, so boxing it would only add an
/// allocation to the hot answer.
#[allow(clippy::large_enum_variant)]
pub(super) enum LockStatus {
    /// TiKV determined something; the caller interprets the response.
    Answered(KvrpcCheckTxnStatusResponse),
    /// A live pessimistic transaction whose primary record does not exist.
    ///
    /// Go `lock_resolver.go:966-968`: rolling this back would abort a running
    /// transaction that merely rolled back its own primary lock, so the caller
    /// waits out the lock's TTL and lets the owner retry instead.
    AlivePessimistic(u64),
    /// The lock names a key that is not its transaction's primary.
    PrimaryMismatch,
}

/// Queries one transaction's status, looping the way Go's
/// `getTxnStatusFromLock` (`lock_resolver.go:910-980`) loops.
///
/// TxnNotFound is not an error here, it is the canonical orphan: a secondary
/// prewrite landed and the coordinator died before the primary. Treating it as
/// terminal leaves the key unreadable and unwritable forever, because every
/// later reader repeats the same failing query. The escape is TiKV's own:
/// re-ask with `rollback_if_not_exist`, which makes TiKV write the rollback
/// record and unstick the lock.
#[allow(clippy::too_many_arguments)]
pub(super) fn query_txn_status<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    query: &LockStatusQuery<'_>,
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
    force_sync_commit: bool,
) -> Result<LockStatus, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    check_cancelled(call)?;
    // Go `lock_resolver.go:915-926`, comment and all: "NOTE: l.TTL = 0 is a
    // special protocol!!!". When a pessimistic prewrite collides with a lock,
    // TiKV reports TTL 0 to say "resolve this unconditionally". Taking a fresh
    // TSO instead makes the lock look alive for its whole real TTL and
    // livelocks the collision.
    let current_ts = if query.ttl_ms == 0 {
        u64::MAX
    } else {
        timestamp_source
            .current_ts()
            .map_err(LockRecoveryError::Timestamp)?
    };
    let mut rollback_if_not_exist = false;
    let mut backoff = RegionBackoffBudget::new(GET_TXN_STATUS_MAX_BACKOFF);
    loop {
        check_cancelled(call)?;
        let (primary_address, primary_context, primary_attempt) =
            route_key_attempt(runtime, query.primary, base_context)?;
        check_cancelled(call)?;
        let request = KvrpcCheckTxnStatusRequest {
            primary_key: query.primary.to_vec(),
            lock_ts: query.txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock: query.resolving_pessimistic_lock,
            verify_is_primary: true,
            is_txn_file: false,
            ..KvrpcCheckTxnStatusRequest::default()
        };
        let response = runtime
            .client()
            .try_lock()
            .map_err(|_| LockRecoveryError::ClientLifecycle)?
            .check_txn_status_for_lock(&primary_address, &request, &primary_context, call)
            .map_err(map_rpc_error)?;
        // Match client-go's post-RPC ctx.Err precedence before interpreting or
        // acting on a simultaneous CheckTxnStatus result.
        check_cancelled(call)?;
        if let Some(error) = response.region_error.as_ref() {
            // Go `getTxnStatus`: BoRegionMiss and go round again against the
            // refreshed route, rather than failing the caller's statement.
            recover_lock_region_error(runtime, error, &primary_attempt, &mut backoff, call)?;
            continue;
        }
        let Some(key_error) = response.error.as_ref() else {
            return Ok(LockStatus::Answered(response));
        };
        if key_error.txn_not_found.is_some() {
            // Asking again with the same escalation would repeat forever: TiKV
            // cannot both be told to write the rollback record and keep
            // reporting that there is nothing to write. Go has no such guard
            // and spins here without backing off; one terminal answer is the
            // honest end of the loop.
            if rollback_if_not_exist {
                return Err(LockRecoveryError::KeyError(format!("{key_error:?}")));
            }
            // Go re-reads the oracle on every iteration, so a lock that expires
            // mid-loop is noticed. `TTL == 0` is already "expired" by protocol.
            let expired = if current_ts == u64::MAX {
                true
            } else {
                check_cancelled(call)?;
                let now = timestamp_source
                    .current_ts()
                    .map_err(LockRecoveryError::Timestamp)?;
                remaining_lock_ttl(query.txn_id, query.ttl_ms, now).is_zero()
            };
            if expired {
                rollback_if_not_exist = true;
                continue;
            }
            if query.resolving_pessimistic_lock {
                return Ok(LockStatus::AlivePessimistic(query.ttl_ms));
            }
            wait_status_backoff(&mut backoff, call)?;
            continue;
        }
        // Go `lock_resolver.go:1069-1073`: only a pessimistic lock may act on
        // this, by rolling itself back; anything else is an unexpected error.
        if key_error.primary_mismatch.is_some() && query.resolving_pessimistic_lock {
            return Ok(LockStatus::PrimaryMismatch);
        }
        return Err(LockRecoveryError::KeyError(format!("{key_error:?}")));
    }
}

/// Sleeps one `BoTxnNotFound` interval, bounded by the caller's deadline.
fn wait_status_backoff(
    backoff: &mut RegionBackoffBudget,
    call: &UnaryCallContext,
) -> Result<(), LockRecoveryError> {
    check_cancelled(call)?;
    if call.timeout().is_zero() {
        return Err(LockRecoveryError::StatusRetryDeadlineExceeded);
    }
    let delay = match backoff.next_delay(RegionBackoffKind::TxnNotFound) {
        Ok(delay) => delay,
        Err(_) => {
            // client-go checks `ctx.Done()` before selecting the backoff
            // config's max-sleep error. Repeat the caller check at the error
            // boundary so an already expired/cancelled call cannot acquire
            // BoTxnNotFound's registered resolve-lock-timeout identity.
            check_cancelled(call)?;
            if call.timeout().is_zero() {
                return Err(LockRecoveryError::StatusRetryDeadlineExceeded);
            }
            return Err(LockRecoveryError::StatusBackoffExhausted);
        }
    };
    if delay > call.timeout() {
        return Err(LockRecoveryError::StatusRetryDeadlineExceeded);
    }
    if call.cancellation().wait_timeout(delay) {
        return Err(LockRecoveryError::CallerCancelled);
    }
    check_cancelled(call)
}

pub(super) fn remaining_lock_ttl(txn_id: u64, lock_ttl_ms: u64, current_ts: u64) -> Duration {
    const TSO_LOGICAL_BITS: u32 = 18;
    let lock_started_ms = txn_id >> TSO_LOGICAL_BITS;
    let now_ms = current_ts >> TSO_LOGICAL_BITS;
    Duration::from_millis(
        lock_started_ms
            .saturating_add(lock_ttl_ms)
            .saturating_sub(now_ms),
    )
}

pub(super) fn classify_determined_status(
    response: &KvrpcCheckTxnStatusResponse,
) -> Result<ResolvedTxnStatus, LockRecoveryError> {
    if response.commit_version > 0 {
        return Ok(ResolvedTxnStatus::Committed(response.commit_version));
    }
    if matches!(
        response.action,
        action if action == KvrpcTxnAction::NoAction as i32
            || action == KvrpcTxnAction::TtlExpireRollback as i32
            || action == KvrpcTxnAction::LockNotExistRollback as i32
    ) {
        return Ok(ResolvedTxnStatus::RolledBack);
    }
    Err(LockRecoveryError::UndeterminedStatus {
        action: response.action,
    })
}

/// Commit-timestamp evidence assembled from an async-commit transaction's
/// secondary locks.
///
/// Go `asyncResolveData`. The invariant it enforces is that a transaction has
/// exactly one fate: either every lock is still present, in which case the
/// commit timestamp is `max(min_commit_ts)` over all of them, or at least one
/// lock is already gone, in which case TiKV's own commit timestamp for that key
/// is the only admissible answer and every other observation must agree with it.
struct AsyncResolveData {
    commit_ts: u64,
    keys: Vec<Vec<u8>>,
    missing_lock: bool,
}

impl AsyncResolveData {
    fn add_keys(
        &mut self,
        locks: &[tidb_proto::KvrpcLockInfo],
        expected: usize,
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<(), LockRecoveryError> {
        if locks.len() < expected {
            // A lock is missing, so the transaction was already committed or
            // rolled back and TiKV has resolved the remaining keys itself.
            if !self.missing_lock {
                if commit_ts != 0 && commit_ts < self.commit_ts {
                    return Err(LockRecoveryError::AsyncCommitConflict(format!(
                        "commit ts {commit_ts} precedes min commit ts {}",
                        self.commit_ts
                    )));
                }
                self.commit_ts = commit_ts;
            }
            self.missing_lock = true;
            if self.commit_ts != commit_ts {
                return Err(LockRecoveryError::AsyncCommitConflict(format!(
                    "commit ts mismatch: {} and {commit_ts}",
                    self.commit_ts
                )));
            }
            return Ok(());
        }
        for lock in locks {
            if lock.lock_version != start_ts {
                return Err(LockRecoveryError::AsyncCommitConflict(format!(
                    "unexpected lock timestamp, expected {start_ts}, found {}",
                    lock.lock_version
                )));
            }
            if !lock.use_async_commit {
                return Err(LockRecoveryError::NonAsyncCommitLock);
            }
            if !self.missing_lock && lock.min_commit_ts > self.commit_ts {
                self.commit_ts = lock.min_commit_ts;
            }
            self.keys.push(lock.key.clone());
        }
        Ok(())
    }
}

/// Determines and then applies an expired async-commit transaction's fate.
///
/// Go `LockResolver.resolveAsyncCommitLock` for the undetermined case: the
/// primary lock names every secondary, CheckSecondaryLocks reports which of
/// them still hold a lock, and the transaction counts as committed exactly when
/// the assembled commit timestamp is nonzero. Only then is ResolveLock sent, so
/// a partially-prewritten async-commit transaction is never half committed.
fn resolve_async_commit_lock<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    lock: &OptimisticLock,
    primary_lock: &OptimisticLock,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
) -> Result<ResolvedTxnStatus, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
{
    let mut data = AsyncResolveData {
        commit_ts: primary_lock.min_commit_ts,
        keys: Vec::new(),
        missing_lock: false,
    };
    // Go `checkAllSecondaries`: a region error backs off `BoRegionMiss` and
    // re-runs the whole grouping, because a split has changed WHICH region
    // each secondary belongs to -- regrouping is the point, not just
    // re-sending. `data` is rebuilt with it so no batch is counted twice.
    let mut backoff = RegionBackoffBudget::new(GET_TXN_STATUS_MAX_BACKOFF);
    'regroup: loop {
        data = AsyncResolveData {
            commit_ts: primary_lock.min_commit_ts,
            keys: Vec::new(),
            missing_lock: false,
        };
        for group in group_keys_by_region(runtime, &primary_lock.secondaries, base_context)? {
            check_cancelled(call)?;
            let request = KvrpcCheckSecondaryLocksRequest {
                keys: group.keys.clone(),
                start_version: lock.txn_id,
                ..KvrpcCheckSecondaryLocksRequest::default()
            };
            let response = runtime
                .client()
                .try_lock()
                .map_err(|_| LockRecoveryError::ClientLifecycle)?
                .check_secondary_locks_for_lock(&group.address, &request, &group.context, call)
                .map_err(map_rpc_error)?;
            check_cancelled(call)?;
            if let Some(error) = response.region_error.as_ref() {
                recover_lock_region_error(runtime, error, &group.attempt, &mut backoff, call)?;
                continue 'regroup;
            }
            if let Some(error) = response.error.as_ref() {
                return Err(LockRecoveryError::KeyError(format!("{error:?}")));
            }
            data.add_keys(
                &response.locks,
                group.keys.len(),
                lock.txn_id,
                response.commit_ts,
            )?;
        }
        // Every group answered without a split moving the keys.
        break;
    }
    // The primary is resolved with the same fate as every secondary; it is
    // deliberately last, so a failure cannot leave secondaries resolved against
    // a primary that still claims a different outcome.
    data.keys.push(lock.primary.clone());
    let status = if data.commit_ts == 0 {
        ResolvedTxnStatus::RolledBack
    } else {
        ResolvedTxnStatus::Committed(data.commit_ts)
    };
    for key in &data.keys {
        resolve_key(runtime, lock.txn_id, key, status, base_context, call)?;
    }
    Ok(status)
}

/// One region's share of a keyed recovery command.
struct RegionKeyGroup {
    address: String,
    /// The route this group was sent on, kept so a region error the send
    /// comes back with can be applied to the cache before the retry.
    attempt: RegionAttempt,
    context: KvrpcContext,
    keys: Vec<Vec<u8>>,
}

/// Routes `keys` and groups them by the region that serves each, preserving the
/// caller's key order inside every group.
fn group_keys_by_region<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    keys: &[Vec<u8>],
    base_context: &KvrpcContext,
) -> Result<Vec<RegionKeyGroup>, LockRecoveryError>
where
    L: RegionRecoveryLoader,
{
    let mut groups: Vec<RegionKeyGroup> = Vec::new();
    for key in keys {
        let (address, context, attempt) = route_key_attempt(runtime, key, base_context)?;
        match groups
            .iter_mut()
            .find(|group| group.address == address && group.context.region_id == context.region_id)
        {
            Some(group) => group.keys.push(key.clone()),
            None => groups.push(RegionKeyGroup {
                address,
                attempt,
                context,
                keys: vec![key.clone()],
            }),
        }
    }
    Ok(groups)
}

fn resolve_secondary<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    lock: &OptimisticLock,
    status: ResolvedTxnStatus,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
) -> Result<(), LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
{
    resolve_key(runtime, lock.txn_id, &lock.key, status, base_context, call)
}

fn resolve_key<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    txn_id: u64,
    key: &[u8],
    status: ResolvedTxnStatus,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
) -> Result<(), LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
{
    // Go `resolveLock` wraps exactly this send in `for { ... }`, retrying a
    // region error after `BoRegionMiss`; the budget is the same
    // resolve-lock budget its backoffer carries.
    let mut backoff = RegionBackoffBudget::new(GET_TXN_STATUS_MAX_BACKOFF);
    loop {
        check_cancelled(call)?;
        let (address, context, attempt) = route_key_attempt(runtime, key, base_context)?;
        check_cancelled(call)?;
        let request = KvrpcResolveLockRequest {
            start_version: txn_id,
            commit_version: match status {
                ResolvedTxnStatus::Committed(commit_ts) => commit_ts,
                ResolvedTxnStatus::RolledBack => 0,
            },
            keys: vec![key.to_vec()],
            is_async: false,
            is_txn_file: false,
            ..KvrpcResolveLockRequest::default()
        };
        let response = runtime
            .client()
            .try_lock()
            .map_err(|_| LockRecoveryError::ClientLifecycle)?
            .resolve_lock_for_read(&address, &request, &context, call)
            .map_err(map_rpc_error)?;
        // Do not inspect or publish a ResolveLock result after caller cancellation.
        check_cancelled(call)?;
        if let Some(error) = response.region_error.as_ref() {
            recover_lock_region_error(runtime, error, &attempt, &mut backoff, call)?;
            continue;
        }
        if let Some(error) = response.error.as_ref() {
            return Err(LockRecoveryError::KeyError(format!("{error:?}")));
        }
        return Ok(());
    }
}

pub(super) fn map_rpc_error(error: DirectUnaryClientError) -> LockRecoveryError {
    if matches!(error, DirectUnaryClientError::CallerCancelled) {
        LockRecoveryError::CallerCancelled
    } else {
        LockRecoveryError::Rpc(error.to_string())
    }
}

pub(super) fn check_cancelled(call: &UnaryCallContext) -> Result<(), LockRecoveryError> {
    if call.cancellation().is_cancelled() {
        Err(LockRecoveryError::CallerCancelled)
    } else {
        Ok(())
    }
}

pub(super) fn route_key<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    key: &[u8],
    base_context: &KvrpcContext,
) -> Result<(String, KvrpcContext), LockRecoveryError>
where
    L: RegionRecoveryLoader,
{
    let (address, context, _) = route_key_attempt(runtime, key, base_context)?;
    Ok((address, context))
}

/// [`route_key`] keeping the attempt, which is what the region cache needs to
/// recover a region error the routed RPC comes back with.
pub(super) fn route_key_attempt<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    key: &[u8],
    base_context: &KvrpcContext,
) -> Result<(String, KvrpcContext, RegionAttempt), LockRecoveryError>
where
    L: RegionRecoveryLoader,
{
    let region = runtime
        .locate_key(key)
        .map_err(|_| LockRecoveryError::RegionCacheLifecycle)?
        .map_err(|error| LockRecoveryError::Route(error.to_string()))?
        .region;
    let selected = runtime
        .with_region_cache(|cache| {
            let mut selector = cache
                .request_selector(region, ReadPolicy::default())
                .map_err(|error| LockRecoveryError::Route(error.to_string()))?;
            let RequestSelection::Attempt(selected) = cache
                .select_request(&mut selector)
                .map_err(|error| LockRecoveryError::Route(error.to_string()))?
            else {
                return Err(LockRecoveryError::Route(
                    "freshly located lock region unexpectedly requires reload".to_owned(),
                ));
            };
            Ok(selected)
        })
        .map_err(|_| LockRecoveryError::RegionCacheLifecycle)??;
    let mut context = base_context.clone();
    context.region_id = selected.attempt.region.id;
    context.region_epoch = Some(KvrpcRegionEpoch {
        conf_ver: selected.attempt.region.epoch.conf_ver,
        version: selected.attempt.region.epoch.version,
    });
    context.peer = Some(KvrpcPeer {
        id: selected.attempt.peer_id,
        store_id: selected.attempt.store_id,
        role: selected.role.as_i32(),
        is_witness: selected.is_witness,
    });
    context.replica_read = false;
    context.stale_read = false;
    context.cluster_id = runtime.cluster_id();
    Ok((selected.attempt.address.clone(), context, selected.attempt))
}

/// Applies one region error met by a LOCK-path RPC to the cache and reserves
/// the caller's next retry delay -- client-go's
/// `bo.Backoff(retry.BoRegionMiss, ...)` followed by `continue`, which is
/// what `getTxnStatus`, `resolveLock` and `checkSecondaries` each do
/// (`lock_resolver.go`). Their RPCs go through `store.SendReq`, whose
/// `RegionRequestSender` refreshes the cache before the retry re-locates; this
/// port calls the client directly, so the refresh has to be asked for here.
/// Without it a region SPLIT during a locking workload aborted the
/// transaction with 1105 instead of being retried.
pub(super) fn recover_lock_region_error<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    error: &tidb_proto::RegionError,
    attempt: &RegionAttempt,
    backoff: &mut RegionBackoffBudget,
    call: &UnaryCallContext,
) -> Result<(), LockRecoveryError>
where
    L: RegionRecoveryLoader,
{
    let recovered = runtime
        .with_region_cache(|cache| cache.on_region_error(error, attempt.clone(), backoff))
        .map_err(|_| LockRecoveryError::RegionCacheLifecycle)?;
    let delay = match recovered {
        // A stale observation means a concurrent caller already refreshed
        // this route; re-locating is enough. See the coordinator's
        // `disposition_for_recovery_outcome` for the same rule.
        Err(RegionRecoveryError::StaleObservation(_)) => backoff
            .next_delay(RegionBackoffKind::RegionMiss)
            .map_err(|_| LockRecoveryError::RegionError(format!("{error:?}")))?,
        Err(other) => return Err(LockRecoveryError::RegionError(other.to_string())),
        Ok(RegionErrorDisposition::RetryRoute { delay, .. })
        | Ok(RegionErrorDisposition::RetrySelector { delay, .. })
        | Ok(RegionErrorDisposition::RebuildRanges { delay, .. }) => delay,
        // Non-retryable and terminal answers keep their identity: Go returns
        // these to the lock caller rather than spinning on them.
        Ok(RegionErrorDisposition::ReturnRegionError) | Ok(RegionErrorDisposition::Terminal(_)) => {
            return Err(LockRecoveryError::RegionError(format!("{error:?}")))
        }
    };
    if delay > call.timeout() {
        return Err(LockRecoveryError::StatusRetryDeadlineExceeded);
    }
    if call.cancellation().wait_timeout(delay) {
        return Err(LockRecoveryError::CallerCancelled);
    }
    check_cancelled(call)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{wait_status_backoff, LockRecoveryError};
    use crate::region::RegionBackoffBudget;
    use crate::{UnaryCallContext, UnaryCancellation};

    #[test]
    fn txn_not_found_wait_distinguishes_budget_exhaustion_from_call_deadline() {
        let mut exhausted = RegionBackoffBudget::with_jitter_seed(Duration::from_millis(1), 1);
        let live_call = UnaryCallContext::with_timeout(Duration::from_secs(1));
        assert_eq!(wait_status_backoff(&mut exhausted, &live_call), Ok(()));
        assert_eq!(
            wait_status_backoff(&mut exhausted, &live_call),
            Err(LockRecoveryError::StatusBackoffExhausted)
        );

        let mut available = RegionBackoffBudget::with_jitter_seed(Duration::from_secs(20), 1);
        let expired_call = UnaryCallContext::with_timeout(Duration::ZERO);
        assert_eq!(
            wait_status_backoff(&mut available, &expired_call),
            Err(LockRecoveryError::StatusRetryDeadlineExceeded)
        );

        let mut exhausted_at_deadline =
            RegionBackoffBudget::with_jitter_seed(Duration::from_millis(1), 2);
        let live_call = UnaryCallContext::with_timeout(Duration::from_secs(1));
        assert_eq!(
            wait_status_backoff(&mut exhausted_at_deadline, &live_call),
            Ok(())
        );
        let expired_call = UnaryCallContext::with_timeout(Duration::ZERO);
        assert_eq!(
            wait_status_backoff(&mut exhausted_at_deadline, &expired_call),
            Err(LockRecoveryError::StatusRetryDeadlineExceeded)
        );

        let mut exhausted_at_cancellation =
            RegionBackoffBudget::with_jitter_seed(Duration::from_millis(1), 3);
        let live_call = UnaryCallContext::with_timeout(Duration::from_secs(1));
        assert_eq!(
            wait_status_backoff(&mut exhausted_at_cancellation, &live_call),
            Ok(())
        );
        let cancellation = UnaryCancellation::new();
        cancellation.cancel();
        let cancelled_call = UnaryCallContext::new(Duration::from_secs(1), cancellation);
        assert_eq!(
            wait_status_backoff(&mut exhausted_at_cancellation, &cancelled_call),
            Err(LockRecoveryError::CallerCancelled)
        );
    }
}
