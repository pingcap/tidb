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

use crate::region::{ReadPolicy, RegionLoader, RequestSelection};
use crate::rpc::TonicCoprocessorClient;
use crate::{DirectUnaryClientError, SharedReadRuntime, UnaryCallContext};

use super::{LockAdmissionError, OptimisticLock};

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LockRecoveryResult {
    /// All determined locks were cleaned and no wait remains.
    Resolved(Vec<ResolvedTxnStatus>),
    /// At least one transaction is alive; wait the minimum response TTL.
    Alive(Duration),
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
    /// MinCommitTSPushed requires resolved-lock propagation outside this slice.
    MinCommitTsPushed,
    /// CheckSecondaryLocks answered with a lock that is not async-commit.
    ///
    /// Go `nonAsyncCommitLock`: the primary said async commit and a secondary
    /// disagrees, so the two views of the same transaction cannot both be true.
    NonAsyncCommitLock,
    /// An async-commit recovery observed a self-contradictory commit timestamp.
    AsyncCommitConflict(String),
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
            Self::MinCommitTsPushed => formatter
                .write_str("MinCommitTSPushed lock requires deferred resolved-lock propagation"),
            Self::NonAsyncCommitLock => {
                formatter.write_str("CheckSecondaryLocks returned a non-async-commit lock")
            }
            Self::AsyncCommitConflict(error) => {
                write!(formatter, "async commit recovery is inconsistent: {error}")
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
pub fn resolve_optimistic_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[OptimisticLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionLoader,
    T: TimestampSource + ?Sized,
{
    let mut statuses = Vec::with_capacity(locks.len());
    let mut minimum_wait = None::<Duration>;
    for lock in locks {
        check_cancelled(call)?;
        let current_ts = timestamp_source
            .current_ts()
            .map_err(LockRecoveryError::Timestamp)?;
        check_cancelled(call)?;
        let (primary_address, primary_context) = route_key(runtime, &lock.primary, base_context)?;
        check_cancelled(call)?;
        let check_request = KvrpcCheckTxnStatusRequest {
            primary_key: lock.primary.clone(),
            lock_ts: lock.txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist: false,
            force_sync_commit: false,
            resolving_pessimistic_lock: false,
            verify_is_primary: true,
            is_txn_file: false,
            ..KvrpcCheckTxnStatusRequest::default()
        };
        let check_response = runtime
            .client()
            .try_borrow_mut()
            .map_err(|_| LockRecoveryError::ClientLifecycle)?
            .check_txn_status_for_lock(&primary_address, &check_request, &primary_context, call)
            .map_err(map_rpc_error)?;
        // Match client-go's post-RPC ctx.Err precedence before interpreting or
        // acting on a simultaneous CheckTxnStatus result.
        check_cancelled(call)?;
        reject_check_error(&check_response)?;
        let primary_lock = match check_response.lock_info.as_ref() {
            // The returned primary lock uses the same strict protocol gate.
            Some(primary_lock) => super::decode_lock_observation(primary_lock)?
                .into_iter()
                .next(),
            None => None,
        };
        if check_response.lock_ttl > 0 {
            let async_commit_primary = primary_lock
                .as_ref()
                .is_some_and(|primary_lock| primary_lock.use_async_commit);
            if !async_commit_primary
                && check_response.action == KvrpcTxnAction::MinCommitTsPushed as i32
            {
                return Err(LockRecoveryError::MinCommitTsPushed);
            }
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
            if async_commit_primary && ttl.is_zero() {
                let primary_lock = primary_lock.expect("an async-commit primary was observed");
                statuses.push(resolve_async_commit_lock(
                    runtime,
                    lock,
                    &primary_lock,
                    base_context,
                    call,
                )?);
                continue;
            }
            if check_response.action == KvrpcTxnAction::MinCommitTsPushed as i32 {
                return Err(LockRecoveryError::MinCommitTsPushed);
            }
            minimum_wait = Some(minimum_wait.map_or(ttl, |wait| wait.min(ttl)));
            continue;
        }
        let status = classify_determined_status(&check_response)?;
        if lock.key != lock.primary {
            check_cancelled(call)?;
            resolve_secondary(runtime, lock, status, base_context, call)?;
        }
        statuses.push(status);
    }
    Ok(match minimum_wait {
        Some(wait) => LockRecoveryResult::Alive(wait),
        None => LockRecoveryResult::Resolved(statuses),
    })
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

pub(super) fn reject_check_error(
    response: &KvrpcCheckTxnStatusResponse,
) -> Result<(), LockRecoveryError> {
    if let Some(error) = response.region_error.as_ref() {
        return Err(LockRecoveryError::RegionError(format!("{error:?}")));
    }
    if let Some(error) = response.error.as_ref() {
        return Err(LockRecoveryError::KeyError(format!("{error:?}")));
    }
    Ok(())
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
    L: RegionLoader,
{
    let mut data = AsyncResolveData {
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
            .try_borrow_mut()
            .map_err(|_| LockRecoveryError::ClientLifecycle)?
            .check_secondary_locks_for_lock(&group.address, &request, &group.context, call)
            .map_err(map_rpc_error)?;
        check_cancelled(call)?;
        if let Some(error) = response.region_error.as_ref() {
            return Err(LockRecoveryError::RegionError(format!("{error:?}")));
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
    L: RegionLoader,
{
    let mut groups: Vec<RegionKeyGroup> = Vec::new();
    for key in keys {
        let (address, context) = route_key(runtime, key, base_context)?;
        match groups
            .iter_mut()
            .find(|group| group.address == address && group.context.region_id == context.region_id)
        {
            Some(group) => group.keys.push(key.clone()),
            None => groups.push(RegionKeyGroup {
                address,
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
    L: RegionLoader,
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
    L: RegionLoader,
{
    check_cancelled(call)?;
    let (address, context) = route_key(runtime, key, base_context)?;
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
        .try_borrow_mut()
        .map_err(|_| LockRecoveryError::ClientLifecycle)?
        .resolve_lock_for_read(&address, &request, &context, call)
        .map_err(map_rpc_error)?;
    // Do not inspect or publish a ResolveLock result after caller cancellation.
    check_cancelled(call)?;
    if let Some(error) = response.region_error.as_ref() {
        return Err(LockRecoveryError::RegionError(format!("{error:?}")));
    }
    if let Some(error) = response.error.as_ref() {
        return Err(LockRecoveryError::KeyError(format!("{error:?}")));
    }
    Ok(())
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
    L: RegionLoader,
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
    Ok((selected.attempt.address, context))
}
