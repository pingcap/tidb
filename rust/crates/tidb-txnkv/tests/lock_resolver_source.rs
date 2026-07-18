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

#![allow(missing_docs)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

pub use tidb_txnkv::region;
pub use tidb_txnkv::rpc;
pub use tidb_txnkv::{
    DirectUnaryClientError, SharedReadRuntime, UnaryCallContext, UnaryCancellation,
};

#[allow(unused_imports)]
#[path = "../src/lock/mod.rs"]
mod lock;

use lock::{
    resolve_optimistic_locks, FixedTimestampSource, LockRecoveryClient, LockRecoveryError,
    LockRecoveryResult, OptimisticLock, ResolvedTxnStatus,
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
}

struct MockClient {
    checks: VecDeque<KvrpcCheckTxnStatusResponse>,
    recorded: Rc<RefCell<Recorded>>,
}

impl LockRecoveryClient for MockClient {
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        self.recorded.borrow_mut().checks.push((
            address.to_owned(),
            request.clone(),
            context.clone(),
        ));
        Ok(self.checks.pop_front().expect("one queued status"))
    }

    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        self.recorded.borrow_mut().resolves.push((
            address.to_owned(),
            request.clone(),
            context.clone(),
        ));
        Ok(KvrpcResolveLockResponse::default())
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
    };
    let cache = RegionCache::new(StaticLoader {
        locations: vec![
            location(1, b"a", b"r", "primary:20160"),
            location(2, b"r", b"", "secondary:20160"),
        ],
    });
    (SharedReadRuntime::new(client, cache), recorded)
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
        &FixedTimestampSource(1_100 << 18),
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
            &FixedTimestampSource(1_100 << 18),
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
fn alive_status_returns_response_ttl_without_local_expiry_arithmetic() {
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
        // This current TS is deliberately far beyond txn_id + response TTL.
        // Any local expiry reconstruction would return zero instead of 500ms.
        &FixedTimestampSource(9_000 << 18),
    )
    .unwrap();
    assert_eq!(
        LockRecoveryResult::Alive(Duration::from_millis(500)),
        result
    );
    assert!(recorded.borrow().resolves.is_empty());
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
            &FixedTimestampSource(1_100 << 18),
        );
        assert!(matches!(result, Err(LockRecoveryError::KeyError(_))));
        assert!(recorded.borrow().resolves.is_empty());
    }

    let (runtime, _) = runtime(vec![KvrpcCheckTxnStatusResponse {
        action: KvrpcTxnAction::MinCommitTsPushed as i32,
        ..KvrpcCheckTxnStatusResponse::default()
    }]);
    assert!(matches!(
        resolve_optimistic_locks(
            &runtime,
            &[secondary()],
            1_300 << 18,
            &KvrpcContext::default(),
            &call(),
            &FixedTimestampSource(1_100 << 18),
        ),
        Err(LockRecoveryError::UndeterminedStatus { .. })
    ));
}
