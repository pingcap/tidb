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
//! module reuses that decision and branches only at cleanup.

use std::time::Duration;

use tidb_proto::{
    KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcPessimisticRollbackRequest, KvrpcTxnAction,
};

use crate::region::{RegionBackoffBudget, RegionRecoveryLoader};
use crate::{ResolvingLock, SharedReadRuntime, UnaryCallContext};

use super::model::{BlockingLock, PessimisticLock};
use super::resolver::{
    check_cancelled, check_lock_call, classify_determined_status, flush_lite_resolve_cleanups,
    map_rpc_error, primary_lock_from_check_response, query_txn_status, recover_lock_region_error,
    remaining_lock_ttl, resolve_async_commit_lock, resolve_optimistic_lock_refs_collecting,
    resolve_optimistic_lock_refs_with_backoff, route_key_attempt, LiteResolveCleanups, LockStatus,
    LockStatusQuery,
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
/// Returns a non-zero [`LockRecoveryResult::ttl`] when at least one owner is
/// still running, carrying the shortest remaining TTL, exactly like the optimistic
/// path. Locks refreshed within [`SKIP_RESOLVE_THRESHOLD_MS`] are treated as
/// alive without an RPC.
pub fn resolve_blocking_locks<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
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
    resolve_blocking_locks_with_backoff(
        runtime,
        locks,
        caller_start_ts,
        base_context,
        call,
        timestamp_source,
        for_read,
        &mut RegionBackoffBudget::new(Duration::from_secs(20)),
    )
}

/// Resolves blocking locks using the caller's existing region retry history.
///
/// TiDB passes one `Backoffer` from the cop task or snapshot into nested lock
/// resolution. Callers that own such a budget must use this entry point so
/// status retries and the subsequent lock wait share one effective limit.
pub fn resolve_blocking_locks_with_backoff<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
    for_read: bool,
    backoff: &mut RegionBackoffBudget,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    let mut record = None;
    record_blocking_locks(runtime, locks, caller_start_ts, &mut record);
    resolve_blocking_locks_recorded(
        runtime,
        locks,
        caller_start_ts,
        base_context,
        call,
        timestamp_source,
        for_read,
        false,
        backoff,
    )
}

pub(crate) fn record_blocking_locks<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
    caller_start_ts: u64,
    record: &mut Option<crate::ResolvingLocksGuard>,
) where
    L: RegionRecoveryLoader,
{
    let locks = locks.iter().map(|lock| match lock {
        BlockingLock::Optimistic(lock) => ResolvingLock {
            txn_id: caller_start_ts,
            lock_txn_id: lock.txn_id,
            key: lock.key.clone(),
            primary: lock.primary.clone(),
        },
        BlockingLock::Pessimistic(lock) => ResolvingLock {
            txn_id: caller_start_ts,
            lock_txn_id: lock.txn_id,
            key: lock.key.clone(),
            primary: lock.primary.clone(),
        },
    });
    if let Some(record) = record {
        record.update(locks);
    } else {
        *record = Some(runtime.record_resolving_locks(caller_start_ts, locks));
    }
}

/// The caller owns the resolving record for the full read or worker lifetime.
pub(crate) fn resolve_blocking_locks_recorded<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    locks: &[BlockingLock],
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
    for_read: bool,
    lite: bool,
    backoff: &mut RegionBackoffBudget,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    // Go collects small or explicitly lite optimistic locks before issuing
    // keyed ResolveLock requests. Reads schedule these batches asynchronously;
    // writers resolve them synchronously. Pessimistic locks keep their rollback path.
    if locks
        .iter()
        .all(|lock| matches!(lock, BlockingLock::Optimistic(_)))
    {
        let optimistic_locks = locks
            .iter()
            .filter_map(|lock| match lock {
                BlockingLock::Optimistic(lock) => Some(lock),
                BlockingLock::Pessimistic(_) => None,
            })
            .collect::<Vec<_>>();
        return resolve_optimistic_lock_refs_with_backoff(
            runtime,
            &optimistic_locks,
            caller_start_ts,
            base_context,
            call,
            timestamp_source,
            for_read,
            lite,
            backoff,
        );
    }

    if !locks.is_empty() {
        crate::client_go_metrics::inc_lock_resolver_resolve();
    }

    let mut lite_cleanups = LiteResolveCleanups::default();
    let mut result = LockRecoveryResult {
        statuses: Vec::with_capacity(locks.len()),
        ..LockRecoveryResult::default()
    };
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
            BlockingLock::Optimistic(lock) => resolve_optimistic_lock_refs_collecting(
                runtime,
                std::slice::from_ref(&lock),
                caller_start_ts,
                base_context,
                call,
                timestamp_source,
                for_read,
                lite,
                backoff,
                &mut lite_cleanups,
            )?,
            BlockingLock::Pessimistic(lock) => resolve_one_pessimistic_lock(
                runtime,
                lock,
                caller_start_ts,
                base_context,
                call,
                timestamp_source,
                for_read,
                backoff,
            )?,
        };
        if outcome.is_alive() {
            minimum_wait =
                Some(minimum_wait.map_or(outcome.ttl, |current| current.min(outcome.ttl)));
        }
        result.statuses.extend(outcome.statuses);
        result.ignore_locks.extend(outcome.ignore_locks);
        result.access_locks.extend(outcome.access_locks);
    }
    flush_lite_resolve_cleanups(
        runtime,
        &mut lite_cleanups,
        base_context,
        call,
        for_read,
        backoff,
    )?;
    result.ttl = minimum_wait.unwrap_or_default();
    if !result.ttl.is_zero() {
        crate::client_go_metrics::inc_lock_resolver_wait_expired();
    }
    Ok(result)
}

fn resolve_one_pessimistic_lock<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    lock: &PessimisticLock,
    caller_start_ts: u64,
    base_context: &KvrpcContext,
    call: &UnaryCallContext,
    timestamp_source: &T,
    for_read: bool,
    backoff: &mut RegionBackoffBudget,
) -> Result<LockRecoveryResult, LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    let (response, cached_status) = match query_txn_status(
        runtime,
        &LockStatusQuery {
            primary: &lock.primary,
            txn_id: lock.txn_id,
            ttl_ms: lock.ttl_ms,
            resolving_pessimistic_lock: true,
        },
        caller_start_ts,
        base_context,
        call,
        timestamp_source,
        false,
        backoff,
    )? {
        LockStatus::Answered {
            response,
            cached_status,
        } => (response, cached_status),
        // Go `lock_resolver.go:966-968`: the owner is alive and merely rolled
        // its own primary lock back, so waiting lets it retry instead of
        // aborting it.
        LockStatus::AlivePessimistic(ttl_ms) => {
            crate::client_go_metrics::inc_lock_resolver_not_expired();
            return Ok(LockRecoveryResult::alive(Duration::from_millis(ttl_ms)));
        }
        // Go `lock_resolver.go:580-586`: this lock points at a key that is not
        // its transaction's primary, so it is stale by construction. It is
        // rolled back without the `key != primary` guard the determined path
        // uses — the mismatch is the proof that this key is not a primary.
        LockStatus::PrimaryMismatch => {
            crate::client_go_metrics::inc_lock_resolver_expired();
            crate::client_go_metrics::inc_lock_resolver_resolve_locks();
            check_cancelled(call)?;
            pessimistic_rollback_lock(runtime, lock, base_context, call, backoff)?;
            return Ok(LockRecoveryResult::resolved(
                lock.txn_id,
                ResolvedTxnStatus::RolledBack,
                caller_start_ts,
            ));
        }
    };
    let ttl = if response.lock_ttl > 0 {
        let post_check_ts = timestamp_source
            .current_ts()
            .map_err(LockRecoveryError::Timestamp)?;
        check_cancelled(call)?;
        remaining_lock_ttl(lock.txn_id, response.lock_ttl, post_check_ts)
    } else {
        Duration::ZERO
    };
    let primary_lock = primary_lock_from_check_response(&response);
    let async_commit_primary = primary_lock
        .as_ref()
        .is_some_and(|primary_lock| primary_lock.use_async_commit);
    if async_commit_primary && ttl.is_zero() {
        crate::client_go_metrics::inc_lock_resolver_expired();
        let primary_lock = primary_lock.expect("an async-commit primary was observed");
        let determined_status = cached_status.or_else(|| {
            (response.lock_ttl == 0)
                .then(|| classify_determined_pessimistic_status(&response).ok())
                .flatten()
        });
        let status = resolve_async_commit_lock(
            runtime,
            lock.txn_id,
            &lock.primary,
            &primary_lock,
            &response,
            determined_status,
            base_context,
            call,
            backoff,
        )?;
        return Ok(LockRecoveryResult::resolved(
            lock.txn_id,
            status,
            caller_start_ts,
        ));
    }
    if response.lock_ttl > 0 {
        if for_read {
            if let Some(status) = cached_status {
                return Ok(LockRecoveryResult::resolved(
                    lock.txn_id,
                    status,
                    caller_start_ts,
                ));
            }
        }
        crate::client_go_metrics::inc_lock_resolver_not_expired();
        return Ok(LockRecoveryResult::alive(ttl));
    }
    crate::client_go_metrics::inc_lock_resolver_expired();
    crate::client_go_metrics::inc_lock_resolver_resolve_locks();
    let status = match cached_status {
        Some(status) => status,
        None => classify_determined_pessimistic_status(&response)?,
    };
    // CheckTxnStatus with `resolving_pessimistic_lock` already dropped the
    // primary's own lock when it decided the transaction was expired, so only a
    // non-primary key still needs its lock entry removed.
    if lock.key != lock.primary {
        check_cancelled(call)?;
        pessimistic_rollback_lock(runtime, lock, base_context, call, backoff)?;
    }
    Ok(LockRecoveryResult::resolved(
        lock.txn_id,
        status,
        caller_start_ts,
    ))
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
    // Go `lock_resolver.go`'s `Action_LockNotExistDoNothing` arm: the owner's
    // primary lock no longer exists and no commit record was found -- the
    // owner already rolled its statement's locks back (a pessimistic
    // statement retry does exactly that), so the blocked key's leftover lock
    // is stale and is simply removed (`resolvePessimisticLock`); nothing is
    // written for the primary, which is what "do nothing" promises. Two
    // pessimistic transactions retrying against each other reach this arm
    // routinely; treating it as undetermined aborted statements Go quietly
    // cleans up after.
    if response.commit_version == 0
        && response.action == KvrpcTxnAction::LockNotExistDoNothing as i32
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
    backoff: &mut RegionBackoffBudget,
) -> Result<(), LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
{
    loop {
        let (address, context, attempt) = route_key_attempt(runtime, &lock.key, base_context)?;
        check_lock_call(call)?;
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
            .try_lock()
            .map_err(|_| LockRecoveryError::ClientLifecycle)?
            .pessimistic_rollback_for_lock(&address, &request, &context, call)
            .map_err(map_rpc_error)?;
        check_lock_call(call)?;
        if let Some(error) = response.region_error.as_ref() {
            recover_lock_region_error(runtime, error, &attempt, backoff, call)?;
            continue;
        }
        if let Some(error) = response.errors.first() {
            return Err(LockRecoveryError::KeyError(format!("{error:?}")));
        }
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(action: KvrpcTxnAction, commit_version: u64) -> KvrpcCheckTxnStatusResponse {
        KvrpcCheckTxnStatusResponse {
            action: action as i32,
            commit_version,
            ..KvrpcCheckTxnStatusResponse::default()
        }
    }

    /// Go `lock_resolver.go`'s `Action_LockNotExistDoNothing` arm: the owner
    /// already rolled its statement's locks back (a pessimistic retry does
    /// exactly that), so the blocked key's leftover lock is stale and the
    /// verdict is a determined rollback -- never "undetermined", which
    /// aborted with 1105 statements Go quietly cleans up after.
    #[test]
    fn lock_not_exist_do_nothing_is_a_determined_rollback() {
        assert_eq!(
            classify_determined_pessimistic_status(&response(
                KvrpcTxnAction::LockNotExistDoNothing,
                0
            ))
            .unwrap(),
            ResolvedTxnStatus::RolledBack
        );
    }

    /// TiKV answers a `resolving_pessimistic_lock` query for an expired owner
    /// with `TTLExpirePessimisticRollback`; that too is a determined rollback
    /// on this path.
    #[test]
    fn ttl_expire_pessimistic_rollback_is_a_determined_rollback() {
        assert_eq!(
            classify_determined_pessimistic_status(&response(
                KvrpcTxnAction::TtlExpirePessimisticRollback,
                0
            ))
            .unwrap(),
            ResolvedTxnStatus::RolledBack
        );
    }

    /// A commit record still wins over either action: the optimistic
    /// classifier's committed arm stays reachable through the delegation.
    #[test]
    fn commit_record_still_classifies_as_committed() {
        assert_eq!(
            classify_determined_pessimistic_status(&response(KvrpcTxnAction::NoAction, 42))
                .unwrap(),
            ResolvedTxnStatus::Committed(42)
        );
    }
}
