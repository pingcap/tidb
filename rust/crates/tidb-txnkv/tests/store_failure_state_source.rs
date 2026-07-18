// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use std::collections::VecDeque;
use tidb_txnkv::region::{
    Peer, PeerRole, ReadPolicy, RegionAttempt, RegionCache, RegionLoadError, RegionLoader,
    RegionLocation, RegionRecoveryError, RegionVerId, RequestSelection, Store, StoreFailureOutcome,
    StoreLiveness, StoreResolveState,
};

struct Loader(VecDeque<RegionLocation>);

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.0
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("empty", "no region"))
    }
}

fn location(id: u64, start: &[u8], end: &[u8], address: &str, epoch: u64) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, 1, 1),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: vec![Peer {
            id: id * 10,
            store_id: 101,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: epoch,
        }],
        leader_peer_id: Some(id * 10),
        stores: vec![Store {
            id: 101,
            address: address.to_owned(),
            epoch,
        }],
    }
}

fn attempt(region: RegionVerId, peer_id: u64, address: &str, epoch: u64) -> RegionAttempt {
    RegionAttempt {
        region,
        peer_id,
        store_id: 101,
        address: address.to_owned(),
        store_epoch: epoch,
    }
}

#[test]
fn one_exact_failure_stales_every_region_snapshot_for_the_store() {
    let left = location(1, b"", b"m", "tikv-old", 7);
    let right = location(2, b"m", b"", "tikv-old", 7);
    let mut cache = RegionCache::new(Loader(VecDeque::from([left.clone(), right.clone()])));
    cache.locate_key(b"a").unwrap();
    cache.locate_key(b"z").unwrap();

    assert_eq!(
        cache
            .on_send_failure(
                &attempt(left.region, 10, "tikv-old", 7),
                StoreLiveness::Unreachable,
            )
            .unwrap(),
        StoreFailureOutcome::Invalidated {
            previous_epoch: 7,
            current_epoch: 8,
        }
    );
    let state = cache.store_state(101).unwrap();
    assert_eq!(state.epoch(), 8);
    assert_eq!(state.liveness(), StoreLiveness::Unreachable);
    assert_eq!(state.resolve_state(), StoreResolveState::NeedCheck);

    let mut selector = cache
        .request_selector(right.region, ReadPolicy::default())
        .unwrap();
    assert_eq!(
        cache.select_request(&mut selector).unwrap(),
        RequestSelection::ReloadRegion {
            region: right.region
        }
    );
}

#[test]
fn delayed_failure_cannot_invalidate_a_newer_generation() {
    let first = location(1, b"", b"", "tikv", 7);
    let mut cache = RegionCache::new(Loader(VecDeque::from([first.clone()])));
    cache.locate_key(b"a").unwrap();
    let observed = attempt(first.region, 10, "tikv", 7);
    cache
        .on_send_failure(&observed, StoreLiveness::Unknown)
        .unwrap();

    assert!(matches!(
        cache.on_send_failure(&observed, StoreLiveness::Unreachable),
        Err(RegionRecoveryError::StaleObservation(_))
    ));
    assert_eq!(cache.store_state(101).unwrap().epoch(), 8);
}

#[test]
fn loader_epoch_zero_re_resolves_without_rolling_back_failure_epoch() {
    let first = location(1, b"", b"", "tikv-old", 7);
    let refreshed = location(1, b"", b"", "tikv-new", 0);
    let mut cache = RegionCache::new(Loader(VecDeque::from([first.clone(), refreshed])));
    cache.locate_key(b"a").unwrap();
    cache
        .on_send_failure(
            &attempt(first.region, 10, "tikv-old", 7),
            StoreLiveness::Unreachable,
        )
        .unwrap();
    cache.invalidate(first.region);

    let reloaded = cache.locate_key(b"a").unwrap();
    assert_eq!(reloaded.peers[0].store_epoch, 8);
    assert_eq!(reloaded.stores[0].epoch, 8);
    let state = cache.store_state(101).unwrap();
    assert_eq!(state.epoch(), 8);
    assert_eq!(state.address(), "tikv-new");
    assert_eq!(state.resolve_state(), StoreResolveState::Resolved);
    assert_eq!(
        state.liveness(),
        StoreLiveness::Unreachable,
        "metadata resolution must not invent a successful health observation"
    );
}

#[test]
fn existing_canonical_epoch_ignores_every_loader_supplied_epoch() {
    let left = location(1, b"", b"m", "tikv", 7);
    let supplied_ahead = location(2, b"m", b"", "tikv", 99);
    let mut cache = RegionCache::new(Loader(VecDeque::from([left, supplied_ahead])));
    cache.locate_key(b"a").unwrap();
    let right = cache.locate_key(b"z").unwrap();

    assert_eq!(right.peers[0].store_epoch, 7);
    assert_eq!(right.stores[0].epoch, 7);
    assert_eq!(cache.store_state(101).unwrap().epoch(), 7);
}

#[test]
fn address_change_stales_old_region_peer_but_preserves_healthy_alternate() {
    let mut left = location(1, b"", b"m", "tikv-old", 7);
    left.peers.push(Peer {
        id: 11,
        store_id: 102,
        role: PeerRole::Voter,
        is_witness: false,
        store_epoch: 3,
    });
    left.stores.push(Store {
        id: 102,
        address: "tikv-alternate".to_owned(),
        epoch: 3,
    });
    let right = location(2, b"m", b"", "tikv-new", 0);
    let mut cache = RegionCache::new(Loader(VecDeque::from([left.clone(), right])));
    cache.locate_key(b"a").unwrap();
    cache.locate_key(b"z").unwrap();
    assert_eq!(cache.store_state(101).unwrap().epoch(), 8);

    let mut selector = cache
        .request_selector(left.region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(request) = cache.select_request(&mut selector).unwrap() else {
        panic!("stale leader store generation must fall through to a healthy voter")
    };
    assert_eq!(request.attempt.peer_id, 11);
    assert_eq!(request.attempt.store_id, 102);
    assert_eq!(request.attempt.address, "tikv-alternate");
    assert!(!request.cached_leader);
}

#[test]
fn re_resolved_unknown_cached_leader_remains_a_candidate() {
    let first = location(1, b"", b"", "tikv", 7);
    let refreshed = location(1, b"", b"", "tikv", 0);
    let mut cache = RegionCache::new(Loader(VecDeque::from([first.clone(), refreshed])));
    cache.locate_key(b"a").unwrap();
    cache
        .on_send_failure(
            &attempt(first.region, 10, "tikv", 7),
            StoreLiveness::Unknown,
        )
        .unwrap();
    cache.invalidate(first.region);
    let region = cache.locate_key(b"a").unwrap().region;
    assert_eq!(
        cache.store_state(101).unwrap().liveness(),
        StoreLiveness::Unknown
    );

    let mut selector = cache
        .request_selector(region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(request) = cache.select_request(&mut selector).unwrap() else {
        panic!("unknown liveness is not proof that the re-resolved leader is unreachable")
    };
    assert_eq!(request.attempt.peer_id, 10);
    assert!(request.cached_leader);
}

#[test]
fn reachable_failure_preserves_canonical_generation() {
    let first = location(1, b"", b"", "tikv", 7);
    let mut cache = RegionCache::new(Loader(VecDeque::from([first.clone()])));
    cache.locate_key(b"a").unwrap();
    assert_eq!(
        cache
            .on_send_failure(
                &attempt(first.region, 10, "tikv", 7),
                StoreLiveness::Reachable,
            )
            .unwrap(),
        StoreFailureOutcome::Reachable { epoch: 7 }
    );
    assert_eq!(cache.store_state(101).unwrap().epoch(), 7);
}
