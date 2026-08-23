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

// aggregate-test: standalone

#![allow(missing_docs)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

pub use tidb_txnkv::region;
pub use tidb_txnkv::rpc;
pub use tidb_txnkv::{
    DirectUnaryClientError, DirectUnaryConnectionError, DirectUnaryGrpcCode, SharedReadRuntime,
    UnaryCallContext, UnaryCancellation,
};

#[allow(unused_imports)]
#[path = "../src/lock/mod.rs"]
mod lock;

use lock::{
    resolve_blocking_locks, resolve_optimistic_locks, BlockingLock, FixedTimestampSource,
    LockRecoveryClient, LockRecoveryError, LockRecoveryResult, OptimisticLock, ResolvedTxnStatus,
    SnapshotLockSet, TimestampSource, SKIP_RESOLVE_THRESHOLD_MS,
};
use region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionVerId, Store,
};
use tidb_proto::{
    kvrpcpb, KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse,
    KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcLockInfo,
    KvrpcResolveLockRequest, KvrpcResolveLockResponse, KvrpcTxnAction,
};

#[derive(Clone)]
struct StaticLoader {
    locations: Vec<RegionLocation>,
}

impl RegionLoader for StaticLoader {
    fn cluster_id(&self) -> u64 {
        77
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.locations
            .iter()
            .find(|location| location.contains_key(key))
            .cloned()
            .ok_or_else(|| RegionLoadError::new("test", "missing test region"))
    }
}

#[derive(Debug, Default)]
struct Recorded {
    checks: Vec<(String, KvrpcCheckTxnStatusRequest, KvrpcContext)>,
    resolves: Vec<(String, KvrpcResolveLockRequest, KvrpcContext)>,
    pessimistic_rollbacks: Vec<(String, tidb_proto::KvrpcPessimisticRollbackRequest)>,
    secondary_checks: Vec<(String, KvrpcCheckSecondaryLocksRequest)>,
}

struct MockClient {
    checks: VecDeque<KvrpcCheckTxnStatusResponse>,
    secondary_checks: VecDeque<KvrpcCheckSecondaryLocksResponse>,
    recorded: Rc<RefCell<Recorded>>,
    cancel_after_check: bool,
    cancel_after_resolve: bool,
    check_error: Option<DirectUnaryClientError>,
    resolve_error: Option<DirectUnaryClientError>,
}

impl LockRecoveryClient for MockClient {
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        self.recorded.borrow_mut().checks.push((
            address.to_owned(),
            request.clone(),
            context.clone(),
        ));
        if let Some(error) = self.check_error.take() {
            return Err(error);
        }
        let response = self.checks.pop_front().expect("one queued status");
        if self.cancel_after_check {
            call.cancellation().cancel();
        }
        Ok(response)
    }

    fn check_secondary_locks_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckSecondaryLocksRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        self.recorded
            .borrow_mut()
            .secondary_checks
            .push((address.to_owned(), request.clone()));
        Ok(self
            .secondary_checks
            .pop_front()
            .expect("one queued secondary-lock answer"))
    }

    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        self.recorded.borrow_mut().resolves.push((
            address.to_owned(),
            request.clone(),
            context.clone(),
        ));
        if let Some(error) = self.resolve_error.take() {
            return Err(error);
        }
        if self.cancel_after_resolve {
            call.cancellation().cancel();
        }
        Ok(KvrpcResolveLockResponse::default())
    }

    fn pessimistic_rollback_for_lock(
        &mut self,
        address: &str,
        request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        self.recorded
            .borrow_mut()
            .pessimistic_rollbacks
            .push((address.to_owned(), request.clone()));
        Ok(tidb_proto::KvrpcPessimisticRollbackResponse::default())
    }
}

fn location(id: u64, start: &[u8], end: &[u8], address: &str) -> RegionLocation {
    RegionLocation {
        region: RegionVerId {
            id,
            epoch: RegionEpoch {
                conf_ver: id + 10,
                version: id + 20,
            },
        },
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: vec![Peer {
            id: id + 100,
            store_id: id + 200,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 1,
        }],
        leader_peer_id: Some(id + 100),
        stores: vec![Store {
            id: id + 200,
            address: address.to_owned(),
            epoch: 1,
        }],
        ..RegionLocation::default()
    }
}

fn runtime(
    statuses: Vec<KvrpcCheckTxnStatusResponse>,
) -> (
    SharedReadRuntime<MockClient, StaticLoader>,
    Rc<RefCell<Recorded>>,
) {
    runtime_with_secondary_checks(statuses, Vec::new())
}

fn runtime_with_secondary_checks(
    statuses: Vec<KvrpcCheckTxnStatusResponse>,
    secondary_checks: Vec<KvrpcCheckSecondaryLocksResponse>,
) -> (
    SharedReadRuntime<MockClient, StaticLoader>,
    Rc<RefCell<Recorded>>,
) {
    let recorded = Rc::new(RefCell::new(Recorded::default()));
    let client = MockClient {
        checks: statuses.into(),
        secondary_checks: secondary_checks.into(),
        recorded: Rc::clone(&recorded),
        cancel_after_check: false,
        cancel_after_resolve: false,
        check_error: None,
        resolve_error: None,
    };
    let cache = RegionCache::new(StaticLoader {
        locations: vec![
            location(1, b"a", b"r", "primary:20160"),
            location(2, b"r", b"", "secondary:20160"),
        ],
    });
    (SharedReadRuntime::new_injected(client, cache), recorded)
}

/// Go `ResolveLockResult` whose `IgnoreLocks` names these transactions: their
/// locks are stepped over, and the ids reach TiKV as `Context.resolved_locks`.
fn ignoring(statuses: Vec<ResolvedTxnStatus>, txn_ids: Vec<u64>) -> LockRecoveryResult {
    LockRecoveryResult {
        ttl: Duration::ZERO,
        statuses,
        ignore_locks: txn_ids,
        access_locks: Vec::new(),
    }
}

/// Go `ResolveLockResult` whose `AccessLocks` names these transactions: the
/// caller reads through their locks via `Context.committed_locks`.
fn accessing(statuses: Vec<ResolvedTxnStatus>, txn_ids: Vec<u64>) -> LockRecoveryResult {
    LockRecoveryResult {
        ttl: Duration::ZERO,
        statuses,
        ignore_locks: Vec::new(),
        access_locks: txn_ids,
    }
}

/// Go `ResolveLockResult` that only asked the caller to wait.
fn still_alive(ttl: Duration) -> LockRecoveryResult {
    LockRecoveryResult {
        ttl,
        ..LockRecoveryResult::default()
    }
}

fn secondary() -> OptimisticLock {
    OptimisticLock {
        key: b"secondary".to_vec(),
        primary: b"primary".to_vec(),
        txn_id: 1_000 << 18,
        ttl_ms: 500,
        txn_size: 2,
        lock_type: 0,
        min_commit_ts: 0,
        use_async_commit: false,
        secondaries: Vec::new(),
    }
}

fn call() -> UnaryCallContext {
    UnaryCallContext::new(Duration::from_secs(2), UnaryCancellation::new())
}

#[derive(Debug)]
struct AdvancingTimestampSource {
    timestamps: RefCell<VecDeque<u64>>,
    cancellation: Option<UnaryCancellation>,
    calls: Cell<usize>,
}

impl AdvancingTimestampSource {
    fn new(timestamps: impl IntoIterator<Item = u64>) -> Self {
        Self {
            timestamps: RefCell::new(timestamps.into_iter().collect()),
            cancellation: None,
            calls: Cell::new(0),
        }
    }
}

impl TimestampSource for AdvancingTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        let call = self.calls.get() + 1;
        self.calls.set(call);
        let timestamp = self
            .timestamps
            .borrow_mut()
            .pop_front()
            .ok_or_else(|| "missing scripted timestamp".to_owned())?;
        if call == 2 {
            if let Some(cancellation) = &self.cancellation {
                cancellation.cancel();
            }
        }
        Ok(timestamp)
    }
}

#[test]
fn committed_primary_resolves_exact_secondary_through_same_authorities() {
    let commit_ts = 1_200 << 18;
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        commit_version: commit_ts,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    let result = resolve_optimistic_locks(
        &runtime,
        &[secondary()],
        1_300 << 18,
        &KvrpcContext {
            request_source: "source".to_owned(),
            ..KvrpcContext::default()
        },
        &call(),
        &FixedTimestampSource::new(1_100 << 18),
        true,
    )
    .unwrap();
    assert_eq!(
        result,
        accessing(
            vec![ResolvedTxnStatus::Committed(commit_ts)],
            vec![1_000 << 18]
        )
    );

    let recorded = recorded.borrow();
    assert_eq!(recorded.checks.len(), 1);
    assert_eq!(recorded.resolves.len(), 1);
    let (check_address, check, check_context) = &recorded.checks[0];
    assert_eq!(check_address, "primary:20160");
    assert_eq!(check.primary_key, b"primary");
    assert_eq!(check.lock_ts, 1_000 << 18);
    assert_eq!(check.caller_start_ts, 1_300 << 18);
    assert_eq!(check.current_ts, 1_100 << 18);
    assert_ne!(check.caller_start_ts, check.current_ts);
    assert!(!check.rollback_if_not_exist);
    assert!(check.verify_is_primary);
    assert_eq!(check_context.region_id, 1);
    assert_eq!(check_context.cluster_id, 77);
    assert_eq!(check_context.request_source, "source");

    let (resolve_address, resolve, resolve_context) = &recorded.resolves[0];
    assert_eq!(resolve_address, "secondary:20160");
    assert_eq!(resolve.start_version, 1_000 << 18);
    assert_eq!(resolve.commit_version, commit_ts);
    assert_eq!(resolve.keys, vec![b"secondary".to_vec()]);
    assert!(!resolve.is_async);
    assert!(!resolve.is_txn_file);
    assert_eq!(resolve_context.region_id, 2);
}

#[test]
fn rolled_back_status_uses_zero_commit_version() {
    for action in [
        KvrpcTxnAction::NoAction,
        KvrpcTxnAction::TtlExpireRollback,
        KvrpcTxnAction::LockNotExistRollback,
    ] {
        let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
            action: action as i32,
            ..KvrpcCheckTxnStatusResponse::default()
        }]);
        let result = resolve_optimistic_locks(
            &runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        )
        .unwrap();
        assert_eq!(
            result,
            ignoring(vec![ResolvedTxnStatus::RolledBack], vec![1_000 << 18])
        );
        assert_eq!(recorded.borrow().resolves[0].1.commit_version, 0);
    }
}

#[test]
fn alive_status_returns_only_remaining_absolute_transaction_ttl() {
    for (current_physical_ms, expected_remaining_ms) in [(1_300, 200), (9_000, 0)] {
        let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
            lock_ttl: 500,
            ..KvrpcCheckTxnStatusResponse::default()
        }]);
        let result = resolve_optimistic_locks(
            &runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &AdvancingTimestampSource::new([1_100 << 18, current_physical_ms << 18]),
            true,
        )
        .unwrap();
        assert_eq!(
            still_alive(Duration::from_millis(expected_remaining_ms)),
            result
        );
        assert!(recorded.borrow().resolves.is_empty());
        assert_eq!(recorded.borrow().checks[0].1.current_ts, 1_100 << 18);
    }
}

#[test]
fn alive_status_rejects_an_exhausted_one_shot_timestamp_source() {
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        lock_ttl: 500,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    assert_eq!(
        resolve_optimistic_locks(
            &runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        ),
        Err(LockRecoveryError::Timestamp(
            "one-shot timestamp source is exhausted".to_owned()
        ))
    );
    assert_eq!(recorded.borrow().checks[0].1.current_ts, 1_100 << 18);
    assert!(recorded.borrow().resolves.is_empty());
}

#[test]
fn alive_status_uses_post_check_timestamp_and_rechecks_cancellation() {
    let (first_runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        lock_ttl: 500,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    let timestamps = AdvancingTimestampSource::new([1_100 << 18, 1_300 << 18]);
    assert_eq!(
        resolve_optimistic_locks(
            &first_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &timestamps,
            true,
        )
        .unwrap(),
        still_alive(Duration::from_millis(200))
    );
    assert_eq!(recorded.borrow().checks[0].1.current_ts, 1_100 << 18);
    assert_eq!(timestamps.calls.get(), 2);

    let (second_runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse {
        lock_ttl: 500,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    let cancellation = UnaryCancellation::new();
    let timestamps = AdvancingTimestampSource {
        timestamps: RefCell::new([1_100 << 18, 1_300 << 18].into_iter().collect()),
        cancellation: Some(cancellation.clone()),
        calls: Cell::new(0),
    };
    assert_eq!(
        resolve_optimistic_locks(
            &second_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &UnaryCallContext::new(Duration::from_secs(2), cancellation),
            &timestamps,
            true,
        ),
        Err(LockRecoveryError::CallerCancelled)
    );
}

#[test]
fn cancellation_after_check_or_resolve_wins_before_followup_mutation() {
    for cancel_after_resolve in [false, true] {
        let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
            commit_version: 1_200 << 18,
            ..KvrpcCheckTxnStatusResponse::default()
        }]);
        {
            let mut client = runtime.client().lock().unwrap();
            client.cancel_after_check = !cancel_after_resolve;
            client.cancel_after_resolve = cancel_after_resolve;
        }
        let cancellation = UnaryCancellation::new();
        let result = resolve_optimistic_locks(
            &runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &UnaryCallContext::new(Duration::from_secs(2), cancellation),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        );
        assert_eq!(result, Err(LockRecoveryError::CallerCancelled));
        assert_eq!(
            recorded.borrow().resolves.len(),
            usize::from(cancel_after_resolve)
        );
    }
}

#[test]
fn caller_cancelled_rpc_is_typed_at_both_lock_commands() {
    let (check_runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse::default()]);
    check_runtime.client().lock().unwrap().check_error = Some(DirectUnaryClientError::CallerCancelled);
    assert_eq!(
        resolve_optimistic_locks(
            &check_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        ),
        Err(LockRecoveryError::CallerCancelled)
    );

    let (resolve_runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        commit_version: 1_200 << 18,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    resolve_runtime.client().lock().unwrap().resolve_error =
        Some(DirectUnaryClientError::CallerCancelled);
    assert_eq!(
        resolve_optimistic_locks(
            &resolve_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        ),
        Err(LockRecoveryError::CallerCancelled)
    );
    assert_eq!(recorded.borrow().resolves.len(), 1);

    let (remote_runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse::default()]);
    remote_runtime.client().lock().unwrap().check_error = Some(DirectUnaryClientError::Connection(
        DirectUnaryConnectionError::remote_grpc(
            "primary:20160",
            9,
            DirectUnaryGrpcCode::Canceled,
            "remote canceled".to_owned(),
        ),
    ));
    assert!(matches!(
        resolve_optimistic_locks(
            &remote_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        ),
        Err(LockRecoveryError::Rpc(_))
    ));
}

fn txn_not_found_status() -> KvrpcCheckTxnStatusResponse {
    KvrpcCheckTxnStatusResponse {
        error: Some(kvrpcpb::KeyError {
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                start_ts: 1_000 << 18,
                primary_key: b"primary".to_vec(),
            }),
            ..kvrpcpb::KeyError::default()
        }),
        ..KvrpcCheckTxnStatusResponse::default()
    }
}

/// The canonical orphan lock is recoverable: a secondary prewrite landed and
/// the coordinator died before the primary.
///
/// Go `getTxnStatusFromLock` (`lock_resolver.go:928-980`) is a `for{}`. On
/// txnNotFound with the lock already past its TTL it sets
/// `rollbackIfNotExist = true` and asks again, which makes TiKV write the
/// rollback record and unstick the key. Treating txnNotFound as terminal
/// leaves that key unreadable and unwritable forever — every later reader
/// repeats the identical failing query, including the catalog load.
#[test]
fn an_expired_txn_not_found_lock_escalates_to_rollback_if_not_exist() {
    let (runtime, recorded) = runtime(vec![
        txn_not_found_status(),
        KvrpcCheckTxnStatusResponse {
            action: KvrpcTxnAction::LockNotExistRollback as i32,
            ..KvrpcCheckTxnStatusResponse::default()
        },
    ]);

    let result = resolve_optimistic_locks(
        &runtime,
        &[secondary()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        // The lock started at physical 1_000 with a 500ms TTL; the expiry read
        // at 2_000 is well past it.
        &AdvancingTimestampSource::new([1_100 << 18, 2_000 << 18]),
        true,
    )
    .expect("an expired orphan lock is recoverable, not a permanent error");

    assert_eq!(
        result,
        ignoring(vec![ResolvedTxnStatus::RolledBack], vec![1_000 << 18])
    );
    let recorded = recorded.borrow();
    assert_eq!(recorded.checks.len(), 2);
    assert!(
        !recorded.checks[0].1.rollback_if_not_exist,
        "the first query must not ask TiKV to invent a rollback record"
    );
    assert!(
        recorded.checks[1].1.rollback_if_not_exist,
        "only the escalation may, and without it the lock is uncleanable"
    );
    assert_eq!(recorded.resolves.len(), 1);
    assert_eq!(recorded.resolves[0].1.commit_version, 0);
}

/// Go `lock_resolver.go:712-722` splits a determined commit on one comparison:
/// `status.IsCommitted() && status.CommitTS() > callerStartTS` is `canIgnore`
/// and `<= callerStartTS` is `canAccess`. The equal case is the whole rule. A
/// transaction that committed at exactly the reader's own timestamp is visible
/// to that reader, so its value must be read *through* the lock via
/// `Context.committed_locks`; filing it under `resolved_locks` instead tells
/// TiKV to step over a row the reader is entitled to, and the read comes back
/// short with no error to show for it.
#[test]
fn a_commit_at_the_readers_own_timestamp_is_read_through_not_stepped_over() {
    let caller_start_ts = 1_300 << 18;
    for (commit_ts, expected) in [
        (
            caller_start_ts,
            accessing(
                vec![ResolvedTxnStatus::Committed(caller_start_ts)],
                vec![1_000 << 18],
            ),
        ),
        (
            caller_start_ts + 1,
            ignoring(
                vec![ResolvedTxnStatus::Committed(caller_start_ts + 1)],
                vec![1_000 << 18],
            ),
        ),
    ] {
        let (runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse {
            commit_version: commit_ts,
            ..KvrpcCheckTxnStatusResponse::default()
        }]);
        assert_eq!(
            resolve_optimistic_locks(
                &runtime,
                &[secondary()],
                caller_start_ts,
                &KvrpcContext::default(),
                &call(),
                &FixedTimestampSource::new(1_100 << 18),
                true,
            ),
            Ok(expected),
            "a commit at {commit_ts} seen by a reader at {caller_start_ts}"
        );
    }
}

/// A txnNotFound whose lock has *not* expired is a concurrent prewrite whose
/// primary simply has not landed yet: back off and ask again, never roll it
/// back. Go `bo.Backoff(retry.BoTxnNotFound, err)` (`lock_resolver.go:975`).
#[test]
fn a_live_txn_not_found_lock_backs_off_instead_of_rolling_back() {
    let (runtime, recorded) = runtime(vec![
        txn_not_found_status(),
        KvrpcCheckTxnStatusResponse {
            commit_version: 1_200 << 18,
            ..KvrpcCheckTxnStatusResponse::default()
        },
    ]);

    let result = resolve_optimistic_locks(
        &runtime,
        &[secondary()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        // The expiry read at 1_200 is still inside the lock's 500ms TTL.
        &AdvancingTimestampSource::new([1_100 << 18, 1_200 << 18]),
        true,
    )
    .expect("a concurrent prewrite is waited out, not failed");

    assert_eq!(
        result,
        accessing(
            vec![ResolvedTxnStatus::Committed(1_200 << 18)],
            vec![1_000 << 18]
        )
    );
    let recorded = recorded.borrow();
    assert_eq!(recorded.checks.len(), 2);
    assert!(
        recorded
            .checks
            .iter()
            .all(|(_, request, _)| !request.rollback_if_not_exist),
        "a live transaction's primary must never be rolled back by a reader"
    );
}

/// A lock whose TTL TiKV reported as zero is resolved unconditionally.
///
/// Go `lock_resolver.go:915-926`, comment and all: "NOTE: l.TTL = 0 is a
/// special protocol!!!" — TiKV says zero to tell the client to resolve now,
/// and the client answers by sending `current_ts = MaxUint64`. Taking a fresh
/// TSO instead makes the lock look alive for its whole real TTL and livelocks
/// the pessimistic-prewrite collision.
#[test]
fn a_zero_ttl_lock_is_resolved_unconditionally_with_max_current_ts() {
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        action: KvrpcTxnAction::TtlExpireRollback as i32,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);

    let result = resolve_optimistic_locks(
        &runtime,
        &[OptimisticLock {
            ttl_ms: 0,
            ..secondary()
        }],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        // Exhausted on purpose: the protocol forbids consulting the oracle at
        // all, so any TSO read here is the bug.
        &AdvancingTimestampSource::new([]),
        true,
    )
    .expect("a zero-TTL lock is resolved without asking the oracle");

    assert_eq!(
        result,
        ignoring(vec![ResolvedTxnStatus::RolledBack], vec![1_000 << 18])
    );
    let recorded = recorded.borrow();
    assert_eq!(recorded.checks.len(), 1);
    assert_eq!(recorded.checks[0].1.current_ts, u64::MAX);
}

/// A pessimistic lock naming a key that is not its transaction's primary is
/// stale by construction, so it is pessimistic-rolled-back rather than raised.
///
/// Go `lock_resolver.go:1069-1073` returns `primaryMismatch` only when
/// `resolvingPessimisticLock`, and `lock_resolver.go:580-586` then clears the
/// status and falls into `resolvePessimisticLock`.
#[test]
fn a_primary_mismatch_pessimistic_lock_is_rolled_back_not_raised() {
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        error: Some(kvrpcpb::KeyError {
            primary_mismatch: Some(kvrpcpb::PrimaryMismatch { lock_info: None }),
            ..kvrpcpb::KeyError::default()
        }),
        ..KvrpcCheckTxnStatusResponse::default()
    }]);

    let result = resolve_blocking_locks(
        &runtime,
        &[blocking_pessimistic(b"secondary", 0)],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &FixedTimestampSource::new(1_100 << 18),
    )
    .expect("a stale pessimistic lock is cleanable");

    assert_eq!(
        result,
        ignoring(vec![ResolvedTxnStatus::RolledBack], vec![1_000 << 18])
    );
    let recorded = recorded.borrow();
    assert_eq!(recorded.pessimistic_rollbacks.len(), 1);
    assert_eq!(
        recorded.pessimistic_rollbacks[0].1.keys,
        vec![b"secondary".to_vec()]
    );
    assert!(recorded.resolves.is_empty());
}

#[test]
fn primary_mismatch_and_undetermined_status_fail_closed() {
    // An *optimistic* lock has no primary-mismatch recourse: Go's handler is
    // gated on `resolvingPessimisticLock` and its caller re-raises for any
    // non-pessimistic lock (`lock_resolver.go:581-584`).
    let (mismatch_runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        error: Some(kvrpcpb::KeyError {
            primary_mismatch: Some(kvrpcpb::PrimaryMismatch { lock_info: None }),
            ..kvrpcpb::KeyError::default()
        }),
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    assert!(matches!(
        resolve_optimistic_locks(
            &mismatch_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        ),
        Err(LockRecoveryError::KeyError(_))
    ));
    assert!(recorded.borrow().resolves.is_empty());

    let (undetermined_runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse {
        action: KvrpcTxnAction::MinCommitTsPushed as i32,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    assert!(matches!(
        resolve_optimistic_locks(
            &undetermined_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
            true,
        ),
        Err(LockRecoveryError::UndeterminedStatus { .. })
    ));

    // Go `lock_resolver.go:632-638`: a live lock whose min-commit-ts TiKV
    // pushed above the reader is neither an error nor a wait. The owner will
    // commit after this reader, so the reader steps over the lock and names
    // the owner in `Context.resolved_locks` on its retry.
    let (pushed_runtime, pushed_recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        action: KvrpcTxnAction::MinCommitTsPushed as i32,
        lock_ttl: 500,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    assert_eq!(
        resolve_optimistic_locks(
            &pushed_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &AdvancingTimestampSource::new([1_100 << 18, 1_100 << 18]),
            true,
        ),
        Ok(ignoring(Vec::new(), vec![1_000 << 18]))
    );
    // Nothing was cleaned: the owner is still running.
    assert!(pushed_recorded.borrow().resolves.is_empty());

    // Go `lock_resolver.go:626-632`: the same answer reaches a *writer* through
    // the `!forRead` guard, which returns before the classification. A writer
    // cannot step over a lock, so it waits out the rest of the owner's TTL.
    let (write_runtime, write_recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        action: KvrpcTxnAction::MinCommitTsPushed as i32,
        lock_ttl: 500,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    assert_eq!(
        resolve_optimistic_locks(
            &write_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &AdvancingTimestampSource::new([1_100 << 18, 1_100 << 18]),
            false,
        ),
        Ok(still_alive(Duration::from_millis(400)))
    );
    assert!(write_recorded.borrow().resolves.is_empty());
}

// -----------------------------------------------------------------------------
// Blocking-lock recovery for pessimistic locking statements
// -----------------------------------------------------------------------------

/// Builds a blocker the way a real statement does: from the exact `LockInfo`
/// TiKV puts in the `KeyIsLocked` error, through the admission decoder.
fn blocking_pessimistic(key: &[u8], refreshed_ms: u64) -> BlockingLock {
    let observation = kvrpcpb::LockInfo {
        key: key.to_vec(),
        primary_lock: b"primary".to_vec(),
        lock_version: 1_000 << 18,
        lock_ttl: 500,
        lock_type: kvrpcpb::Op::PessimisticLock as i32,
        lock_for_update_ts: 1_050 << 18,
        duration_to_last_update_ms: refreshed_ms,
        ..kvrpcpb::LockInfo::default()
    };
    let mut admitted = lock::decode_blocking_lock_observation(&observation)
        .expect("TiKV's own pessimistic lock is admissible to a locking statement");
    assert_eq!(admitted.len(), 1);
    let blocker = admitted.remove(0);
    assert_eq!(blocker.key(), key);
    assert_eq!(blocker.txn_id(), 1_000 << 18);
    assert!(matches!(blocker, BlockingLock::Pessimistic(_)));
    assert_eq!(blocker.protocol_name(), "pessimistic");
    blocker
}

/// An expired pessimistic lock is dropped, not replayed.
///
/// Source contract (`LockResolver.resolvePessimisticLock`): a pessimistic lock
/// has no commit record, so cleanup is PessimisticRollback at the lock's own
/// `for_update_ts` — never ResolveLock, which would need a commit version.
#[test]
fn an_expired_pessimistic_blocker_is_rolled_back_at_its_own_for_update_ts() {
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        lock_ttl: 0,
        action: KvrpcTxnAction::TtlExpirePessimisticRollback as i32,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);

    let result = resolve_blocking_locks(
        &runtime,
        &[blocking_pessimistic(b"secondary", 0)],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &FixedTimestampSource::new(1_100 << 18),
    )
    .unwrap();

    assert_eq!(
        result,
        ignoring(vec![ResolvedTxnStatus::RolledBack], vec![1_000 << 18])
    );
    let recorded = recorded.borrow();
    // The status query must announce which protocol it is resolving, or TiKV
    // cannot apply its pessimistic-specific expiry rules.
    assert_eq!(recorded.checks.len(), 1);
    assert!(recorded.checks[0].1.resolving_pessimistic_lock);
    assert_eq!(recorded.checks[0].1.lock_ts, 1_000 << 18);
    assert!(
        recorded.resolves.is_empty(),
        "a pessimistic lock has no commit record to resolve"
    );
    assert_eq!(recorded.pessimistic_rollbacks.len(), 1);
    let (address, rollback) = &recorded.pessimistic_rollbacks[0];
    // Cleanup is routed to the blocked key's own region, not the primary's.
    assert_eq!(address, "secondary:20160");
    assert_eq!(rollback.start_version, 1_000 << 18);
    assert_eq!(rollback.for_update_ts, 1_050 << 18);
    assert_eq!(rollback.keys, vec![b"secondary".to_vec()]);
}

/// A pessimistic lock whose owner is still running is waited on, not cleaned.
#[test]
fn a_live_pessimistic_blocker_reports_its_remaining_ttl() {
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        lock_ttl: 500,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);

    let result = resolve_blocking_locks(
        &runtime,
        &[blocking_pessimistic(b"secondary", 0)],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &AdvancingTimestampSource::new([1_100 << 18, 1_200 << 18]),
    )
    .unwrap();

    // The lock started at 1_000ms with a 500ms TTL and it is now 1_200ms, so
    // 300ms of the owner's lease remain.
    assert_eq!(result, still_alive(Duration::from_millis(300)));
    assert!(recorded.borrow().pessimistic_rollbacks.is_empty());
}

/// A lock TiKV refreshed moments ago is treated as alive without an RPC.
///
/// Source contract (`skipResolveThresholdMs`): TiKV updates this field when it
/// wakes a waiter, so a small value proves the owner is running. Paying for a
/// status RPC could only confirm what is already known.
#[test]
fn a_freshly_refreshed_blocker_is_assumed_alive_without_a_status_rpc() {
    let (runtime, recorded) = runtime(Vec::new());

    let result = resolve_blocking_locks(
        &runtime,
        &[blocking_pessimistic(
            b"secondary",
            SKIP_RESOLVE_THRESHOLD_MS - 1,
        )],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &FixedTimestampSource::new(1_100 << 18),
    )
    .unwrap();

    assert_eq!(
        result,
        still_alive(Duration::from_millis(SKIP_RESOLVE_THRESHOLD_MS))
    );
    let recorded = recorded.borrow();
    assert!(recorded.checks.is_empty(), "no RPC may be spent");
    assert!(recorded.pessimistic_rollbacks.is_empty());
}

/// A statement blocked by both protocols at once cleans each its own way.
#[test]
fn a_mixed_blocker_set_uses_each_locks_own_cleanup_protocol() {
    let (runtime, recorded) = runtime(vec![
        // The optimistic blocker committed.
        KvrpcCheckTxnStatusResponse {
            commit_version: 1_200 << 18,
            ..KvrpcCheckTxnStatusResponse::default()
        },
        // The pessimistic blocker expired.
        KvrpcCheckTxnStatusResponse {
            lock_ttl: 0,
            action: KvrpcTxnAction::TtlExpirePessimisticRollback as i32,
            ..KvrpcCheckTxnStatusResponse::default()
        },
    ]);

    let result = resolve_blocking_locks(
        &runtime,
        &[
            BlockingLock::Optimistic(secondary()),
            blocking_pessimistic(b"secondary", 0),
        ],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &AdvancingTimestampSource::new([1_100 << 18, 1_100 << 18]),
    )
    .unwrap();

    assert_eq!(
        result,
        LockRecoveryResult {
            ttl: Duration::ZERO,
            statuses: vec![
                ResolvedTxnStatus::Committed(1_200 << 18),
                ResolvedTxnStatus::RolledBack,
            ],
            // The optimistic blocker committed at or before this caller, so
            // its value is readable through the lock; the pessimistic blocker
            // left nothing behind, so its lock is merely stepped over.
            ignore_locks: vec![1_000 << 18],
            access_locks: vec![1_000 << 18],
        }
    );
    let recorded = recorded.borrow();
    assert_eq!(recorded.checks.len(), 2);
    assert!(!recorded.checks[0].1.resolving_pessimistic_lock);
    assert!(recorded.checks[1].1.resolving_pessimistic_lock);
    // Exactly one of each cleanup command, never both for the same lock.
    assert_eq!(recorded.resolves.len(), 1);
    assert_eq!(recorded.resolves[0].1.commit_version, 1_200 << 18);
    assert_eq!(recorded.pessimistic_rollbacks.len(), 1);
}

/// A blocker holding its own primary needs no second cleanup command.
///
/// Source contract: CheckTxnStatus with `resolving_pessimistic_lock` already
/// removed the primary's lock when it ruled the transaction expired.
#[test]
fn a_primary_pessimistic_blocker_is_cleaned_by_the_status_query_alone() {
    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        lock_ttl: 0,
        action: KvrpcTxnAction::TtlExpirePessimisticRollback as i32,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);

    resolve_blocking_locks(
        &runtime,
        &[blocking_pessimistic(b"primary", 0)],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &FixedTimestampSource::new(1_100 << 18),
    )
    .unwrap();

    assert!(recorded.borrow().pessimistic_rollbacks.is_empty());
}

// -----------------------------------------------------------------------------
// Async-commit recovery
// -----------------------------------------------------------------------------

const ASYNC_TXN_ID: u64 = 1_000 << 18;
/// Two secondaries the static topology splits across both regions, so recovery
/// has to group them and reconcile two independent answers.
const ASYNC_SECONDARIES: [&[u8]; 2] = [b"alpha", b"secondary"];

fn async_primary_lock(min_commit_ts: u64) -> KvrpcLockInfo {
    KvrpcLockInfo {
        primary_lock: b"primary".to_vec(),
        key: b"primary".to_vec(),
        lock_version: ASYNC_TXN_ID,
        lock_ttl: 500,
        use_async_commit: true,
        min_commit_ts,
        secondaries: ASYNC_SECONDARIES.iter().map(|key| key.to_vec()).collect(),
        ..KvrpcLockInfo::default()
    }
}

/// The CheckTxnStatus answer for a still-present async-commit primary.
fn async_primary_status(min_commit_ts: u64) -> KvrpcCheckTxnStatusResponse {
    KvrpcCheckTxnStatusResponse {
        lock_ttl: 500,
        lock_info: Some(async_primary_lock(min_commit_ts)),
        ..KvrpcCheckTxnStatusResponse::default()
    }
}

fn present_secondary_lock(key: &[u8], min_commit_ts: u64) -> KvrpcLockInfo {
    KvrpcLockInfo {
        primary_lock: b"primary".to_vec(),
        key: key.to_vec(),
        lock_version: ASYNC_TXN_ID,
        lock_ttl: 500,
        use_async_commit: true,
        min_commit_ts,
        ..KvrpcLockInfo::default()
    }
}

fn async_blocker() -> OptimisticLock {
    OptimisticLock {
        key: b"secondary".to_vec(),
        primary: b"primary".to_vec(),
        txn_id: ASYNC_TXN_ID,
        ttl_ms: 500,
        txn_size: 3,
        lock_type: 0,
        min_commit_ts: 0,
        use_async_commit: true,
        secondaries: Vec::new(),
    }
}

/// The pre-RPC timestamp, then a post-RPC one past the lock's 500ms TTL, so the
/// primary counts as expired rather than alive.
fn expired_timestamps() -> AdvancingTimestampSource {
    AdvancingTimestampSource::new([1_100 << 18, 2_000 << 18])
}

/// An expired async-commit transaction whose locks are all still present
/// commits at the largest `min_commit_ts` any of them reports.
///
/// Source contract (`asyncResolveData.addKeys`): with no lock missing, no key
/// has been committed yet, so the commit point is `max(min_commit_ts)` over the
/// primary and every secondary — and every key is then resolved at that one
/// timestamp.
#[test]
fn an_expired_async_commit_txn_commits_at_the_largest_min_commit_ts() {
    let (runtime, recorded) = runtime_with_secondary_checks(
        vec![async_primary_status(1_400 << 18)],
        vec![
            KvrpcCheckSecondaryLocksResponse {
                locks: vec![present_secondary_lock(b"alpha", 1_500 << 18)],
                ..KvrpcCheckSecondaryLocksResponse::default()
            },
            KvrpcCheckSecondaryLocksResponse {
                locks: vec![present_secondary_lock(b"secondary", 1_450 << 18)],
                ..KvrpcCheckSecondaryLocksResponse::default()
            },
        ],
    );

    let result = resolve_optimistic_locks(
        &runtime,
        &[async_blocker()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &expired_timestamps(),
        true,
    )
    .unwrap();

    assert_eq!(
        result,
        ignoring(
            vec![ResolvedTxnStatus::Committed(1_500 << 18)],
            vec![ASYNC_TXN_ID]
        ),
        "the commit timestamp is the maximum min_commit_ts, not the primary's"
    );

    let recorded = recorded.borrow();
    // One CheckSecondaryLocks per region, each carrying only its own keys.
    assert_eq!(recorded.secondary_checks.len(), 2);
    assert_eq!(recorded.secondary_checks[0].0, "primary:20160");
    assert_eq!(recorded.secondary_checks[0].1.keys, vec![b"alpha".to_vec()]);
    assert_eq!(recorded.secondary_checks[1].0, "secondary:20160");
    assert_eq!(
        recorded.secondary_checks[1].1.keys,
        vec![b"secondary".to_vec()]
    );
    assert!(recorded
        .secondary_checks
        .iter()
        .all(|(_, request)| request.start_version == ASYNC_TXN_ID));

    // Every secondary plus the primary is resolved, and the primary is last so
    // it can never contradict an already-resolved secondary.
    let resolved_keys = recorded
        .resolves
        .iter()
        .map(|(_, request, _)| request.keys.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        resolved_keys,
        vec![
            vec![b"alpha".to_vec()],
            vec![b"secondary".to_vec()],
            vec![b"primary".to_vec()],
        ]
    );
    assert!(recorded.resolves.iter().all(|(_, request, _)| {
        request.commit_version == 1_500 << 18 && request.start_version == ASYNC_TXN_ID
    }));
}

/// A key whose lock is already gone fixes the commit timestamp for every other
/// key, overriding the locks that are still present.
///
/// Source contract: TiKV reports the timestamp it actually committed that key
/// at, and a transaction has exactly one commit timestamp, so
/// `max(min_commit_ts)` stops being admissible once a real one exists.
#[test]
fn a_missing_lock_fixes_the_commit_timestamp_for_the_whole_transaction() {
    let (runtime, _recorded) = runtime_with_secondary_checks(
        vec![async_primary_status(1_400 << 18)],
        vec![
            // `alpha` was already committed, so TiKV reports no lock for it.
            KvrpcCheckSecondaryLocksResponse {
                locks: Vec::new(),
                commit_ts: 1_600 << 18,
                ..KvrpcCheckSecondaryLocksResponse::default()
            },
            KvrpcCheckSecondaryLocksResponse {
                locks: vec![present_secondary_lock(b"secondary", 1_450 << 18)],
                ..KvrpcCheckSecondaryLocksResponse::default()
            },
        ],
    );

    let result = resolve_optimistic_locks(
        &runtime,
        &[async_blocker()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &expired_timestamps(),
        true,
    )
    .unwrap();

    assert_eq!(
        result,
        ignoring(
            vec![ResolvedTxnStatus::Committed(1_600 << 18)],
            vec![ASYNC_TXN_ID]
        )
    );
}

/// An expired async-commit transaction with a missing lock and no commit
/// timestamp is rolled back.
#[test]
fn an_async_commit_txn_with_a_rolled_back_key_resolves_as_rolled_back() {
    let (runtime, recorded) = runtime_with_secondary_checks(
        vec![async_primary_status(0)],
        vec![
            KvrpcCheckSecondaryLocksResponse::default(),
            KvrpcCheckSecondaryLocksResponse::default(),
        ],
    );

    let result = resolve_optimistic_locks(
        &runtime,
        &[async_blocker()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        &expired_timestamps(),
        true,
    )
    .unwrap();

    assert_eq!(
        result,
        ignoring(vec![ResolvedTxnStatus::RolledBack], vec![ASYNC_TXN_ID])
    );
    assert!(recorded
        .borrow()
        .resolves
        .iter()
        .all(|(_, request, _)| request.commit_version == 0));
}

/// A secondary that denies the async-commit protocol its primary claims sends
/// the whole recovery back through the sync-commit path.
///
/// Go `resolve(l, true)` (`lock_resolver.go:619-621`): the two views cannot
/// both be true, and the sync-commit view is the one TiKV can still answer, so
/// the status query is re-sent with `force_sync_commit`. Raising instead leaves
/// every key of that transaction permanently blocked.
#[test]
fn a_non_async_commit_secondary_retries_with_force_sync_commit() {
    let (runtime, recorded) = runtime_with_secondary_checks(
        vec![
            async_primary_status(1_400 << 18),
            // The forced retry sees a plain determined status.
            KvrpcCheckTxnStatusResponse {
                commit_version: 1_500 << 18,
                ..KvrpcCheckTxnStatusResponse::default()
            },
        ],
        vec![KvrpcCheckSecondaryLocksResponse {
            locks: vec![KvrpcLockInfo {
                use_async_commit: false,
                ..present_secondary_lock(b"alpha", 1_450 << 18)
            }],
            ..KvrpcCheckSecondaryLocksResponse::default()
        }],
    );

    let result = resolve_optimistic_locks(
        &runtime,
        &[async_blocker()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        // Pre-RPC, post-RPC (expired), then the forced retry's own pre-RPC read.
        &AdvancingTimestampSource::new([1_100 << 18, 2_000 << 18, 2_100 << 18]),
        true,
    )
    .expect("the sync-commit view resolves what the async-commit view could not");

    assert_eq!(
        result,
        ignoring(
            vec![ResolvedTxnStatus::Committed(1_500 << 18)],
            vec![ASYNC_TXN_ID]
        )
    );
    let recorded = recorded.borrow();
    assert_eq!(recorded.checks.len(), 2);
    assert!(!recorded.checks[0].1.force_sync_commit);
    assert!(
        recorded.checks[1].1.force_sync_commit,
        "the retry exists precisely to stop taking the async-commit path"
    );
    assert_eq!(recorded.resolves.len(), 1);
    assert_eq!(recorded.resolves[0].1.commit_version, 1_500 << 18);
}

/// An async-commit primary whose TTL has *not* run out is still alive, so the
/// reader waits instead of forcing a recovery.
#[test]
fn a_live_async_commit_primary_is_waited_for_rather_than_recovered() {
    let (runtime, recorded) =
        runtime_with_secondary_checks(vec![async_primary_status(1_400 << 18)], Vec::new());

    let result = resolve_optimistic_locks(
        &runtime,
        &[async_blocker()],
        1_300 << 18,
        &KvrpcContext::default(),
        &call(),
        // The post-check timestamp is still inside the 500ms TTL.
        &AdvancingTimestampSource::new([1_100 << 18, 1_200 << 18]),
        true,
    )
    .unwrap();

    assert_eq!(result, still_alive(Duration::from_millis(300)));
    assert!(recorded.borrow().secondary_checks.is_empty());
    assert!(recorded.borrow().resolves.is_empty());
}

/// An optimistic prewrite that meets a Go tidb-server's pessimistic lock must
/// keep refusing until the resolve is proven on a real cluster.
///
/// The resolver below is complete and exercised by every test above, but it has
/// never run against TiKV from the prewrite path, and a wrong resolve rolls
/// back another transaction's work. So the default answer stays the refusal,
/// and only `TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY` in the process
/// environment opens the resolving path.
#[test]
fn prewrite_pessimistic_recovery_is_off_unless_the_environment_opts_in() {
    assert_eq!(
        lock::pessimistic_prewrite_recovery_enabled(),
        std::env::var_os("TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY").is_some()
    );
}

/// Go `ClientHelper.ResolveLocks` then `ClientHelper.SendReqCtx`
/// (`client_helper.go:113-122,148-149`): what the resolver classified is put
/// into the reader's two `TSSet`s and replayed on every later request.
///
/// Both the ordering and the de-duplication are Go's: `TSSet` is a map, so a
/// transaction named twice appears once, and `GetAll` returns a set.
#[test]
fn a_readers_lock_sets_accumulate_across_resolves_and_reach_every_request() {
    let mut sets = SnapshotLockSet::default();
    assert!(sets.is_empty());

    let mut context = KvrpcContext::default();
    sets.stamp(&mut context);
    // Go `TSSet.GetAll` returns nil for an empty set, which is the same wire
    // shape as an empty repeated field.
    assert!(context.resolved_locks.is_empty());
    assert!(context.committed_locks.is_empty());

    // Round one: the reader met a pushed transaction and one that had already
    // committed before it.
    sets.absorb(&ignoring(Vec::new(), vec![1_000 << 18]));
    sets.absorb(&accessing(
        vec![ResolvedTxnStatus::Committed(1_200 << 18)],
        vec![900 << 18],
    ));
    // Round two names only what round two met. `LockRecoveryResult` is built
    // fresh per resolve, so these rounds are genuinely disjoint — which is what
    // makes this an accumulator rather than a latch. A set that replaced
    // instead of accumulating would drop round one's ids right here, and the
    // retry would meet those same locks again: the deadloop
    // `client_helper.go:51-56` says `ClientHelper` exists to prevent.
    sets.absorb(&ignoring(Vec::new(), vec![1_100 << 18]));
    sets.absorb(&accessing(
        vec![ResolvedTxnStatus::Committed(1_150 << 18)],
        vec![950 << 18],
    ));
    // A second meeting with the same pushed transaction must not duplicate it:
    // Go `TSSet` is a map, so `Put` of a known id is a no-op.
    sets.absorb(&ignoring(Vec::new(), vec![1_000 << 18]));

    let mut context = KvrpcContext::default();
    sets.stamp(&mut context);
    assert_eq!(
        context.resolved_locks,
        vec![1_000 << 18, 1_100 << 18],
        "IgnoreLocks -> Context.resolved_locks, accumulated over every round"
    );
    assert_eq!(
        context.committed_locks,
        vec![900 << 18, 950 << 18],
        "AccessLocks -> Context.committed_locks, accumulated over every round"
    );
    assert!(!sets.is_empty());
}

/// Go must reset the set by hand, because its snapshot timestamp is mutable:
///
/// ```text
/// // Invalidate cache if the snapshotTS change!
/// s.version = ts
/// ...
/// // And also remove the minCommitTS pushed information.
/// s.resolvedLocks = util.TSSet{}
/// ```
/// (`txnkv/txnsnapshot/snapshot.go:195-201`, `SetSnapshotTS`.)
///
/// The reset is not bookkeeping. "TiKV pushed this owner's min-commit-ts above
/// the reader" is a fact about one exact timestamp; carried to a later, larger
/// one it becomes a lie, and the reader steps over a lock whose value it should
/// now see. Here the timestamp is bound once at construction alongside the set
/// that is classified against it, so there is no moment at which the two can
/// disagree and nothing to reset. This test is what keeps that true: a setter
/// added later would reintroduce Go's hazard without reintroducing Go's reset.
#[test]
fn a_readers_lock_sets_cannot_outlive_the_timestamp_they_were_classified_against() {
    let coordinator_dir =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/transaction/coordinator");
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(&coordinator_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push((path.clone(), std::fs::read_to_string(&path).unwrap()));
        }
    }
    assert!(sources.len() > 1, "coordinator sources were not found");
    for (path, source) in &sources {
        for reassignment in ["self.start_ts =", "self.resolved_locks ="] {
            assert!(
                !source.contains(reassignment),
                "{} reassigns `{reassignment}`: a snapshot timestamp that can move \
                 needs Go's `SetSnapshotTS` reset of the pushed-min-commit-ts set",
                path.display()
            );
        }
    }
    // Born together, in the one constructor, so neither can be refreshed
    // without the other.
    let coordinator = &sources
        .iter()
        .find(|(path, _)| path.ends_with("mod.rs"))
        .expect("coordinator mod.rs")
        .1;
    assert_eq!(
        coordinator
            .matches("resolved_locks: crate::lock::SnapshotLockSet::default()")
            .count(),
        1
    );
}

/// The snapshot read path must fill the sets before it retries and stamp them
/// before it sends, or the retry meets the identical lock forever.
#[test]
fn the_snapshot_read_path_stamps_its_lock_sets_on_every_send() {
    let snapshot = std::fs::read_to_string(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/transaction/coordinator/snapshot_read.rs"),
    )
    .unwrap();
    // One stamp per read command, all taken from the freshly routed context.
    assert_eq!(
        snapshot
            .matches("self.resolved_locks.stamp(&mut context)")
            .count(),
        3
    );
    assert_eq!(
        snapshot
            .matches("self.resolved_locks.absorb(&recovery)")
            .count(),
        3
    );
    assert!(!snapshot.contains("route.context(), call)"));
    // Every read-command call must pass the FRESHLY routed context, never a
    // context read back off the route. `begin_get` is a free function taking
    // the runtime first, `begin_scan` a method, so each call site is checked
    // for the same trailing argument list rather than one fixed prefix.
    for command in ["begin_get(", "begin_scan("] {
        // `let response = ` selects the CALL sites; the `fn` definitions
        // spell the same name and are not argument lists.
        let sites: Vec<&str> = snapshot
            .split(&format!("let response = {command}"))
            .skip(1)
            .chain(
                snapshot
                    .split(&format!("let response = self.{command}"))
                    .skip(1),
            )
            .collect();
        assert!(!sites.is_empty(), "no {command} call site");
        for site in sites {
            let arguments = site.split(')').next().expect("an argument list");
            assert!(
                arguments.ends_with("&route, &context, &request, call"),
                "{command} must take the freshly routed context: {arguments}"
            );
        }
    }
    // BatchGet publishes a whole round of region-routed requests at once; the
    // freshly routed (and stamped) contexts travel inside `requests`.
    assert!(snapshot.contains(".publish_transaction_batch_gets(&requests, call)"));
}
