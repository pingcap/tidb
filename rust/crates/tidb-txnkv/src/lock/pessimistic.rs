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

//! Recovery of the locks that block a pessimistic locking statement.
//!
//! A snapshot read only ever meets prewrite locks, so
//! [`super::resolve_optimistic_locks`] can assume one cleanup protocol. A
//! locking statement additionally meets pessimistic locks, which carry no
//! commit record: an expired one is dropped with PessimisticRollback at its
//! own `for_update_ts` rather than replayed with ResolveLock. Both protocols
//! still start from the same CheckTxnStatus on the owner's primary, so this
//! module reuses that decision and only forks at cleanup.

use std::time::Duration;

use tidb_proto::{
    KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext,
    KvrpcPessimisticRollbackRequest, KvrpcTxnAction,
};

use crate::region::RegionLoader;
use crate::{SharedReadRuntime, UnaryCallContext};

use super::model::{BlockingLock, PessimisticLock};
use super::resolver::{
    check_cancelled, classify_determined_status, map_rpc_error, reject_check_error,
    remaining_lock_ttl, route_key,
};
use super::{
    LockRecoveryClient, LockRecoveryError, LockRecoveryResult, ResolvedTxnStatus, TimestampSource,
};

/// TiKV refreshes a lock's `duration_to_last_update_ms` whenever it wakes a
/// waiter. A lock refreshed this recently is almost certainly owned by a live
/// transaction, so client-go waits again instead of paying for a status RPC
/// that would only report "alive". TiKV's own default wait is one second.
pub const SKIP_RESOLVE_THRESHOLD_MS: u64 = 300;

/// Resolves every lock blocking one pessimistic locking attempt.
///
/// Returns [`LockRecoveryResult::Alive`] when at least one owner is still
/// running, carrying the shortest remaining TTL, exactly like the optimistic
/// path. Locks refreshed within [`SKIP_RESOLVE_THRESHOLD_MS`] are treated as
/// alive without an RPC.
pub fn resolve_blocking_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
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
        if lock.duration_to_last_update_ms() > 0
            && lock.duration_to_last_update_ms() < SKIP_RESOLVE_THRESHOLD_MS
        {
            minimum_wait = Some(
                minimum_wait.map_or(Duration::from_millis(SKIP_RESOLVE_THRESHOLD_MS), |wait| {
                    wait.min(Duration::from_millis(SKIP_RESOLVE_THRESHOLD_MS))
                }),
            );
            continue;
        }
        let outcome = match lock {
            BlockingLock::Optimistic(lock) => super::resolve_optimistic_locks(
                runtime,
                std::slice::from_ref(lock),
                caller_start_ts,
                base_context,
                call,
                timestamp_source,
            )?,
            BlockingLock::Pessimistic(lock) => resolve_one_pessimistic_lock(
                runtime,
                lock,
                caller_start_ts,
                base_context,
                call,
                timestamp_source,
            )?,
        };
        match outcome {
            LockRecoveryResult::Resolved(resolved) => statuses.extend(resolved),
            LockRecoveryResult::Alive(wait) => {
                minimum_wait = Some(minimum_wait.map_or(wait, |current| current.min(wait)));
            }
        }
    }
    Ok(match minimum_wait {
        Some(wait) => LockRecoveryResult::Alive(wait),
        None => LockRecoveryResult::Resolved(statuses),
    })
}

fn resolve_one_pessimistic_lock<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    lock: &PessimisticLock,
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
    let current_ts = timestamp_source
        .current_ts()
        .map_err(LockRecoveryError::Timestamp)?;
    check_cancelled(call)?;
    let (primary_address, primary_context) = route_key(runtime, &lock.primary, base_context)?;
    let request = KvrpcCheckTxnStatusRequest {
        primary_key: lock.primary.clone(),
        lock_ts: lock.txn_id,
        caller_start_ts,
        current_ts,
        rollback_if_not_exist: false,
        force_sync_commit: false,
        resolving_pessimistic_lock: true,
        verify_is_primary: true,
        is_txn_file: false,
        ..KvrpcCheckTxnStatusRequest::default()
    };
    let response = runtime
        .client()
        .try_borrow_mut()
        .map_err(|_| LockRecoveryError::ClientLifecycle)?
        .check_txn_status_for_lock(&primary_address, &request, &primary_context, call)
        .map_err(map_rpc_error)?;
    check_cancelled(call)?;
    reject_check_error(&response)?;
    if response.lock_ttl > 0 {
        let post_check_ts = timestamp_source
            .current_ts()
            .map_err(LockRecoveryError::Timestamp)?;
        check_cancelled(call)?;
        return Ok(LockRecoveryResult::Alive(remaining_lock_ttl(
            lock.txn_id,
            response.lock_ttl,
            post_check_ts,
        )));
    }
    let status = classify_determined_pessimistic_status(&response)?;
    // CheckTxnStatus with `resolving_pessimistic_lock` already dropped the
    // primary's own lock when it decided the transaction was expired, so only a
    // non-primary key still needs its lock entry removed.
    if lock.key != lock.primary {
        check_cancelled(call)?;
        pessimistic_rollback_lock(runtime, lock, base_context, call)?;
    }
    Ok(LockRecoveryResult::Resolved(vec![status]))
}

/// Classifies a status query that announced `resolving_pessimistic_lock`.
///
/// TiKV answers such a query with `TTLExpirePessimisticRollback`, an action the
/// optimistic classifier must keep rejecting: it means "this transaction's
/// pessimistic lock was expired and dropped", which says nothing about a
/// prewrite lock of the same transaction.
fn classify_determined_pessimistic_status(
    response: &KvrpcCheckTxnStatusResponse,
) -> Result<ResolvedTxnStatus, LockRecoveryError> {
    if response.commit_version == 0
        && response.action == KvrpcTxnAction::TtlExpirePessimisticRollback as i32
    {
        return Ok(ResolvedTxnStatus::RolledBack);
    }
    classify_determined_status(response)
}

fn pessimistic_rollback_lock<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    lock: &PessimisticLock,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
) -> Result<(), LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionLoader,
{
    let (address, context) = route_key(runtime, &lock.key, base_context)?;
    check_cancelled(call)?;
    let request = KvrpcPessimisticRollbackRequest {
        start_version: lock.txn_id,
        // A lock whose owner never reported a statement timestamp must still be
        // matched by the cleanup, and TiKV drops locks up to `for_update_ts`.
        for_update_ts: if lock.for_update_ts == 0 {
            u64::MAX
        } else {
            lock.for_update_ts
        },
        keys: vec![lock.key.clone()],
        ..KvrpcPessimisticRollbackRequest::default()
    };
    let response = runtime
        .client()
        .try_borrow_mut()
        .map_err(|_| LockRecoveryError::ClientLifecycle)?
        .pessimistic_rollback_for_lock(&address, &request, &context, call)
        .map_err(map_rpc_error)?;
    check_cancelled(call)?;
    if let Some(error) = response.region_error.as_ref() {
        return Err(LockRecoveryError::RegionError(format!("{error:?}")));
    }
    if let Some(error) = response.errors.first() {
        return Err(LockRecoveryError::KeyError(format!("{error:?}")));
    }
    Ok(())
}
