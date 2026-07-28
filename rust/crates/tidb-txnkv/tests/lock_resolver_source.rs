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
    TimestampSource, SKIP_RESOLVE_THRESHOLD_MS,
};
use region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionVerId, Store,
};
use tidb_proto::{
    kvrpcpb, KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext,
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
}

struct MockClient {
    checks: VecDeque<KvrpcCheckTxnStatusResponse>,
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
    let recorded = Rc::new(RefCell::new(Recorded::default()));
    let client = MockClient {
        checks: statuses.into(),
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

fn secondary() -> OptimisticLock {
    OptimisticLock {
        key: b"secondary".to_vec(),
        primary: b"primary".to_vec(),
        txn_id: 1_000 << 18,
        ttl_ms: 500,
        txn_size: 2,
        lock_type: 0,
        min_commit_ts: 0,
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
    )
    .unwrap();
    assert_eq!(
        result,
        LockRecoveryResult::Resolved(vec![ResolvedTxnStatus::Committed(commit_ts)])
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
        )
        .unwrap();
        assert_eq!(
            result,
            LockRecoveryResult::Resolved(vec![ResolvedTxnStatus::RolledBack])
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
        )
        .unwrap();
        assert_eq!(
            LockRecoveryResult::Alive(Duration::from_millis(expected_remaining_ms)),
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
        )
        .unwrap(),
        LockRecoveryResult::Alive(Duration::from_millis(200))
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
            let mut client = runtime.client().borrow_mut();
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
    check_runtime.client().borrow_mut().check_error = Some(DirectUnaryClientError::CallerCancelled);
    assert_eq!(
        resolve_optimistic_locks(
            &check_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
        ),
        Err(LockRecoveryError::CallerCancelled)
    );

    let (resolve_runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        commit_version: 1_200 << 18,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    resolve_runtime.client().borrow_mut().resolve_error =
        Some(DirectUnaryClientError::CallerCancelled);
    assert_eq!(
        resolve_optimistic_locks(
            &resolve_runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
        ),
        Err(LockRecoveryError::CallerCancelled)
    );
    assert_eq!(recorded.borrow().resolves.len(), 1);

    let (remote_runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse::default()]);
    remote_runtime.client().borrow_mut().check_error = Some(DirectUnaryClientError::Connection(
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
        ),
        Err(LockRecoveryError::Rpc(_))
    ));
}

#[test]
fn txn_not_found_primary_mismatch_and_undetermined_status_fail_closed() {
    for error in [
        kvrpcpb::KeyError {
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                start_ts: 1_000 << 18,
                primary_key: b"primary".to_vec(),
            }),
            ..kvrpcpb::KeyError::default()
        },
        kvrpcpb::KeyError {
            primary_mismatch: Some(kvrpcpb::PrimaryMismatch { lock_info: None }),
            ..kvrpcpb::KeyError::default()
        },
    ] {
        let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
            error: Some(error),
            ..KvrpcCheckTxnStatusResponse::default()
        }]);
        let result = resolve_optimistic_locks(
            &runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource::new(1_100 << 18),
        );
        assert!(matches!(result, Err(LockRecoveryError::KeyError(_))));
        assert!(recorded.borrow().resolves.is_empty());
    }

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
        ),
        Err(LockRecoveryError::UndeterminedStatus { .. })
    ));

    let (runtime, recorded) = runtime(vec![KvrpcCheckTxnStatusResponse {
        action: KvrpcTxnAction::MinCommitTsPushed as i32,
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
        ),
        Err(LockRecoveryError::MinCommitTsPushed)
    );
    assert!(recorded.borrow().resolves.is_empty());
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
        LockRecoveryResult::Resolved(vec![ResolvedTxnStatus::RolledBack])
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
    assert_eq!(
        result,
        LockRecoveryResult::Alive(Duration::from_millis(300))
    );
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
        LockRecoveryResult::Alive(Duration::from_millis(SKIP_RESOLVE_THRESHOLD_MS))
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
        LockRecoveryResult::Resolved(vec![
            ResolvedTxnStatus::Committed(1_200 << 18),
            ResolvedTxnStatus::RolledBack,
        ])
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
