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
    KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcPessimisticRollbackRequest, KvrpcTxnAction,
};

use crate::region::RegionRecoveryLoader;
use crate::{SharedReadRuntime, UnaryCallContext};

use super::model::{BlockingLock, PessimisticLock};
use super::resolver::{
    check_cancelled, classify_determined_status, map_rpc_error, query_txn_status,
    remaining_lock_ttl, route_key, LockStatus, LockStatusQuery,
};
use super::{
    LockRecoveryClient, LockRecoveryError, LockRecoveryResult, ResolvedTxnStatus, TimestampSource,
};

/// TiKV refreshes a lock's `duration_to_last_update_ms` whenever it wakes a
/// waiter. A lock refreshed this recently is almost certainly owned by a live
/// transaction, so client-go waits again instead of paying for a status RPC
/// that would only report "alive". TiKV's own default wait is one second.
pub const SKIP_RESOLVE_THRESHOLD_MS: u64 = 300;

/// Whether an optimistic prewrite may clean a pessimistic lock it collides with.
///
/// Off by default, and the default is the whole point. A Go tidb-server sharing
/// the cluster leaves pessimistic locks that client-go resolves and we today
/// refuse, so refusing is a real availability gap — but resolving one
/// incorrectly rolls back another transaction's work, which is strictly worse
/// than refusing. The protocol below is byte-shaped after client-go and reviewed,
/// yet it has never executed against a real TiKV from the prewrite path, so it
/// stays behind `TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY` until a cluster run
/// proves it. Read once per process: a mid-run flip would make two prewrites in
/// the same transaction disagree about the protocol.
#[must_use]
pub fn pessimistic_prewrite_recovery_enabled() -> bool {
    static ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        std::env::var_os("TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY").is_some()
    });
    *ENABLED
}

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
            BlockingLock::Optimistic(lock) => super::resolve_optimistic_locks(
                runtime,
                std::slice::from_ref(lock),
                caller_start_ts,
                base_context,
                call,
                timestamp_source,
                // Go picks `ResolveLocksForRead` or `ResolveLocks` by the
                // CALLER, not by the lock: a reader may step over a lock it
                // has classified, a writer must wait it out.
                for_read,
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
        if outcome.is_alive() {
            minimum_wait =
                Some(minimum_wait.map_or(outcome.ttl, |current| current.min(outcome.ttl)));
        }
        result.statuses.extend(outcome.statuses);
        result.ignore_locks.extend(outcome.ignore_locks);
        result.access_locks.extend(outcome.access_locks);
    }
    result.ttl = minimum_wait.unwrap_or_default();
    Ok(result)
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
    L: RegionRecoveryLoader,
    T: TimestampSource + ?Sized,
{
    let response = match query_txn_status(
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
    )? {
        LockStatus::Answered(response) => response,
        // Go `lock_resolver.go:966-968`: the owner is alive and merely rolled
        // its own primary lock back, so waiting lets it retry instead of
        // aborting it.
        LockStatus::AlivePessimistic(ttl_ms) => {
            return Ok(LockRecoveryResult::alive(Duration::from_millis(ttl_ms)));
        }
        // Go `lock_resolver.go:580-586`: this lock points at a key that is not
        // its transaction's primary, so it is stale by construction. It is
        // rolled back without the `key != primary` guard the determined path
        // uses — the mismatch is the proof that this key is not a primary.
        LockStatus::PrimaryMismatch => {
            check_cancelled(call)?;
            pessimistic_rollback_lock(runtime, lock, base_context, call)?;
            return Ok(LockRecoveryResult::resolved(
                lock.txn_id,
                ResolvedTxnStatus::RolledBack,
                caller_start_ts,
            ));
        }
    };
    if response.lock_ttl > 0 {
        let post_check_ts = timestamp_source
            .current_ts()
            .map_err(LockRecoveryError::Timestamp)?;
        check_cancelled(call)?;
        return Ok(LockRecoveryResult::alive(remaining_lock_ttl(
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
) -> Result<(), LockRecoveryError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
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
