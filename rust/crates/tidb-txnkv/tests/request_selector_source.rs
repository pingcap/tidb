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

use std::time::Duration;

use tidb_txnkv::region::{
    LeaderRequest, Peer, PeerRole, ReadPolicy, RegionCache, RegionLoadError, RegionLoader,
    RegionLocation, RegionRouteError, RegionVerId, ReplicaReadMode, RequestSelection, Store,
    StoreLiveness, MAX_REPLICA_ATTEMPTS,
};

struct Loader(Option<RegionLocation>);

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.0
            .take()
            .ok_or_else(|| RegionLoadError::new("empty", "already loaded"))
    }
}

fn location() -> RegionLocation {
    let specs = [
        (11, 101, PeerRole::Voter, false),
        (12, 102, PeerRole::Voter, false),
        (13, 103, PeerRole::Voter, false),
        (14, 104, PeerRole::Learner, false),
        (15, 105, PeerRole::Voter, true),
        (16, 106, PeerRole::Unknown(99), false),
    ];
    RegionLocation {
        region: RegionVerId::new(9, 2, 3),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: specs
            .iter()
            .map(|(id, store_id, role, witness)| Peer {
                id: *id,
                store_id: *store_id,
                role: *role,
                is_witness: *witness,
                store_epoch: 7,
            })
            .collect(),
        leader_peer_id: Some(11),
        stores: specs
            .iter()
            .map(|(_, store_id, _, _)| Store {
                id: *store_id,
                address: format!("tikv-{store_id}"),
                epoch: 7,
            })
            .collect(),
    }
}

fn cache() -> RegionCache<Loader> {
    let mut cache = RegionCache::new(Loader(Some(location())));
    cache.locate_key(b"key").unwrap();
    cache
}

fn next(cache: &mut RegionCache<Loader>) -> LeaderRequest {
    let mut selector = cache
        .request_selector(location().region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(request) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected request")
    };
    request
}

fn policy(mode: ReplicaReadMode) -> ReadPolicy {
    ReadPolicy {
        mode,
        ..ReadPolicy::default()
    }
}

fn select(
    cache: &mut RegionCache<Loader>,
    selector: &mut tidb_txnkv::region::RequestSelector,
) -> LeaderRequest {
    let RequestSelection::Attempt(request) = cache.select_request(selector).unwrap() else {
        panic!("expected request attempt")
    };
    request
}

fn complete(selector: &mut tidb_txnkv::region::RequestSelector, request: &LeaderRequest) {
    assert!(selector.record_attempt_result(&request.attempt, Duration::from_millis(1)));
}

#[test]
fn follower_policy_visits_nonleaders_before_leader_with_replica_flags() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(region, policy(ReplicaReadMode::Follower))
        .unwrap();

    for (peer_id, role) in [
        (12, PeerRole::Voter),
        (13, PeerRole::Voter),
        (14, PeerRole::Learner),
    ] {
        let request = select(&mut cache, &mut selector);
        assert_eq!(request.attempt.peer_id, peer_id);
        assert_eq!(request.role, role);
        assert!(request.replica_read);
        assert!(!request.stale_read);
        complete(&mut selector, &request);
    }
    let leader = select(&mut cache, &mut selector);
    assert_eq!(leader.attempt.peer_id, 11);
    assert!(leader.cached_leader);
    assert!(!leader.replica_read);
    assert!(!leader.stale_read);

    let mut rotated = cache
        .request_selector(
            region,
            ReadPolicy {
                mode: ReplicaReadMode::Follower,
                selection_seed: 1,
                ..ReadPolicy::default()
            },
        )
        .unwrap();
    assert_eq!(select(&mut cache, &mut rotated).attempt.peer_id, 13);

    complete(&mut selector, &leader);
    assert_eq!(
        cache.select_request(&mut selector).unwrap(),
        RequestSelection::ReloadRegion { region }
    );
}

#[test]
fn mixed_policy_treats_leader_and_replicas_as_equal_seeded_candidates() {
    let mut cache = cache();
    let region = location().region;
    let mut leader_first = cache
        .request_selector(region, policy(ReplicaReadMode::Mixed))
        .unwrap();
    let leader = select(&mut cache, &mut leader_first);
    assert_eq!(leader.attempt.peer_id, 11);
    assert!(!leader.replica_read);
    assert!(!leader.stale_read);

    let mut follower_first = cache
        .request_selector(
            region,
            ReadPolicy {
                mode: ReplicaReadMode::Mixed,
                selection_seed: 1,
                ..ReadPolicy::default()
            },
        )
        .unwrap();
    let follower = select(&mut cache, &mut follower_first);
    assert_eq!(follower.attempt.peer_id, 12);
    assert!(follower.replica_read);
    assert!(!follower.stale_read);
}

#[test]
fn prefer_leader_falls_back_to_replica_after_leader_rejection() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(
            region,
            ReadPolicy {
                mode: ReplicaReadMode::PreferLeader,
                selection_seed: 2,
                ..ReadPolicy::default()
            },
        )
        .unwrap();

    let leader = select(&mut cache, &mut selector);
    assert_eq!(leader.attempt.peer_id, 11);
    assert!(!leader.replica_read);
    complete(&mut selector, &leader);
    selector.reject_peer(leader.attempt.peer_id);

    let fallback = select(&mut cache, &mut selector);
    assert_eq!(fallback.attempt.peer_id, 14);
    assert!(fallback.replica_read);
    assert!(!fallback.stale_read);
}

#[test]
fn learner_policy_prefers_learner_then_falls_back_to_voter() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(region, policy(ReplicaReadMode::Learner))
        .unwrap();

    let learner = select(&mut cache, &mut selector);
    assert_eq!(learner.attempt.peer_id, 14);
    assert_eq!(learner.role, PeerRole::Learner);
    assert!(learner.replica_read);
    complete(&mut selector, &learner);
    selector.reject_peer(learner.attempt.peer_id);

    let fallback = select(&mut cache, &mut selector);
    assert_eq!(fallback.attempt.peer_id, 11);
    assert!(fallback.cached_leader);
    assert!(!fallback.replica_read);
}

#[test]
fn stale_policy_forces_unattempted_leader_then_uses_ordinary_replica_reads() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(
            region,
            ReadPolicy {
                mode: ReplicaReadMode::Mixed,
                stale_read: true,
                selection_seed: 1,
                ..ReadPolicy::default()
            },
        )
        .unwrap();

    let stale = select(&mut cache, &mut selector);
    assert_eq!(stale.attempt.peer_id, 12);
    assert!(!stale.replica_read);
    assert!(stale.stale_read);
    complete(&mut selector, &stale);
    assert!(!cache.promote_successful_request(&stale).unwrap());

    let leader = select(&mut cache, &mut selector);
    assert_eq!(leader.attempt.peer_id, 11);
    assert!(!leader.replica_read);
    assert!(!leader.stale_read);
    complete(&mut selector, &leader);
    assert!(!selector.record_data_not_ready(&leader.attempt));

    let ordinary = select(&mut cache, &mut selector);
    assert_ne!(ordinary.attempt.peer_id, 11);
    assert!(ordinary.replica_read);
    assert!(!ordinary.stale_read);
}

#[test]
fn data_is_not_ready_allows_one_deferred_retry_after_unattempted_peers() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(
            region,
            ReadPolicy {
                mode: ReplicaReadMode::Mixed,
                stale_read: true,
                selection_seed: 1,
                ..ReadPolicy::default()
            },
        )
        .unwrap();

    let stale = select(&mut cache, &mut selector);
    assert_eq!(stale.attempt.peer_id, 12);
    complete(&mut selector, &stale);
    assert!(selector.record_data_not_ready(&stale.attempt));
    assert!(!selector.record_data_not_ready(&stale.attempt));

    let leader = select(&mut cache, &mut selector);
    assert_eq!(leader.attempt.peer_id, 11);
    complete(&mut selector, &leader);

    for _ in 0..2 {
        let unattempted = select(&mut cache, &mut selector);
        assert_ne!(unattempted.attempt.peer_id, 12);
        complete(&mut selector, &unattempted);
    }
    let retry = select(&mut cache, &mut selector);
    assert_eq!(retry.attempt.peer_id, 12);
    assert!(retry.replica_read);
    assert!(!retry.stale_read);
}

#[test]
fn unreachable_replica_is_excluded_from_a_fresh_request_selector() {
    let mut cache = cache();
    let region = location().region;
    let mut first = cache
        .request_selector(region, policy(ReplicaReadMode::Follower))
        .unwrap();
    let failed = select(&mut cache, &mut first);
    assert_eq!(failed.attempt.peer_id, 12);
    complete(&mut first, &failed);
    cache
        .on_send_failure(&failed.attempt, StoreLiveness::Unreachable)
        .unwrap();

    let mut later = cache
        .request_selector(region, policy(ReplicaReadMode::Follower))
        .unwrap();
    let selected = select(&mut cache, &mut later);
    assert_eq!(selected.attempt.peer_id, 13);
    assert_ne!(selected.attempt.store_id, failed.attempt.store_id);
    assert!(selected.replica_read);
}

#[test]
fn dead_leader_rotates_to_voter_without_changing_leader_read_semantics() {
    let mut cache = cache();
    let mut selector = cache
        .request_selector(location().region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(leader) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected leader")
    };
    assert_eq!(leader.attempt.peer_id, 11);
    assert!(selector.record_attempt_result(&leader.attempt, Duration::from_secs(1)));
    cache
        .on_send_failure(&leader.attempt, StoreLiveness::Unreachable)
        .unwrap();

    let RequestSelection::Attempt(alternate) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected alternate voter")
    };
    assert_eq!(alternate.attempt.peer_id, 12);
    assert!(!alternate.cached_leader);
    assert_eq!(alternate.role, PeerRole::Voter);
    assert!(!alternate.is_witness);
    assert!(!alternate.replica_read);
    assert!(!alternate.stale_read);
}

#[test]
fn successful_alternate_is_promoted_to_the_next_request_leader() {
    let mut cache = cache();
    let mut selector = cache
        .request_selector(location().region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(leader) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected leader")
    };
    assert!(selector.record_attempt_result(&leader.attempt, Duration::from_millis(1)));
    cache
        .on_send_failure(&leader.attempt, StoreLiveness::Unknown)
        .unwrap();
    let RequestSelection::Attempt(alternate) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected alternate")
    };

    assert!(selector.record_attempt_result(&alternate.attempt, Duration::from_millis(1)));
    assert!(cache.promote_successful_request(&alternate).unwrap());
    let promoted = next(&mut cache);
    assert_eq!(promoted.attempt.peer_id, alternate.attempt.peer_id);
    assert!(promoted.cached_leader);
}

#[test]
fn missing_not_leader_rejects_observed_peer_and_probes_an_alternate() {
    let mut cache = cache();
    let mut selector = cache
        .request_selector(location().region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(leader) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected leader")
    };
    assert!(selector.record_attempt_result(&leader.attempt, Duration::from_millis(2)));
    selector.reject_peer(leader.attempt.peer_id);
    let RequestSelection::Attempt(alternate) = cache.select_request(&mut selector).unwrap() else {
        panic!("missing leader must probe another voter")
    };
    assert_eq!(alternate.attempt.peer_id, 12);
    assert!(!alternate.replica_read);
    assert!(!alternate.stale_read);
}

#[test]
fn leader_attempt_and_time_limits_deterministically_fall_through() {
    let mut cache = cache();
    let mut selector = cache
        .request_selector(location().region, ReadPolicy::default())
        .unwrap();
    for _ in 0..MAX_REPLICA_ATTEMPTS {
        let RequestSelection::Attempt(request) = cache.select_request(&mut selector).unwrap()
        else {
            panic!("leader has ten attempts")
        };
        assert_eq!(request.attempt.peer_id, 11);
        assert!(selector.record_attempt_result(&request.attempt, Duration::ZERO));
    }
    let RequestSelection::Attempt(alternate) = cache.select_request(&mut selector).unwrap() else {
        panic!("attempt exhaustion must fall through")
    };
    assert_eq!(alternate.attempt.peer_id, 12);

    let mut timed = cache
        .request_selector(location().region, ReadPolicy::default())
        .unwrap();
    for _ in 0..2 {
        let RequestSelection::Attempt(leader) = cache.select_request(&mut timed).unwrap() else {
            panic!("leader must be selected before cumulative timeout")
        };
        assert_eq!(leader.attempt.peer_id, 11);
        assert!(timed.record_attempt_result(&leader.attempt, Duration::from_secs(30)));
    }
    let RequestSelection::Attempt(alternate) = cache.select_request(&mut timed).unwrap() else {
        panic!("time exhaustion must fall through")
    };
    assert_eq!(alternate.attempt.peer_id, 12);
}

#[test]
fn attempt_result_must_exactly_match_the_pending_dispatch() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(request) = cache.select_request(&mut selector).unwrap() else {
        panic!("expected leader")
    };
    assert_eq!(
        cache.select_request(&mut selector),
        Err(RegionRouteError::AttemptStillPending {
            region,
            peer_id: request.attempt.peer_id,
        })
    );

    let mut stale = request.attempt.clone();
    stale.address.push_str("-stale");
    assert!(!selector.record_attempt_result(&stale, Duration::from_secs(60)));
    assert!(selector.record_attempt_result(&request.attempt, Duration::from_secs(1)));
    assert!(!selector.record_attempt_result(&request.attempt, Duration::from_secs(1)));
}

#[test]
fn stale_unreachable_learner_and_witness_peers_exhaust_to_reload() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(region, ReadPolicy::default())
        .unwrap();

    for expected_peer in [11, 12, 13] {
        let RequestSelection::Attempt(request) = cache.select_request(&mut selector).unwrap()
        else {
            panic!("expected voter {expected_peer}")
        };
        assert_eq!(request.attempt.peer_id, expected_peer);
        assert!(selector.record_attempt_result(&request.attempt, Duration::from_millis(1)));
        cache
            .on_send_failure(&request.attempt, StoreLiveness::Unreachable)
            .unwrap();
    }

    assert_eq!(
        cache.select_request(&mut selector).unwrap(),
        RequestSelection::ReloadRegion { region }
    );
    assert!(cache.is_empty());
}

#[test]
fn stale_success_cannot_promote_a_replaced_store_generation() {
    let mut cache = cache();
    let leader = next(&mut cache);
    cache
        .on_send_failure(&leader.attempt, StoreLiveness::Unknown)
        .unwrap();
    assert!(cache.promote_successful_request(&leader).is_err());
}
