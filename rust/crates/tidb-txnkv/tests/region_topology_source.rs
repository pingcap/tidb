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

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use tidb_txnkv::region::{
    LeaderRequest, Peer, PeerRole, ReadPolicy, RegionAttempt, RegionCache, RegionLoadError,
    RegionLoader, RegionLocation, RegionVerId, ReplicaReadMode, RequestSelection, RouteFeedback,
    RouteFeedbackApplication, RouteOutcome, Store, StoreFailureOutcome, StoreLiveness,
};

struct Loader {
    locations: VecDeque<RegionLocation>,
    labels: BTreeMap<u64, Vec<(String, String)>>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.locations
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("exhausted", "no region remains"))
    }

    fn store_labels(&self, store_id: u64) -> &[(String, String)] {
        self.labels.get(&store_id).map(Vec::as_slice).unwrap_or(&[])
    }
}

fn region(id: u64, start: &[u8], end: &[u8], peers: &[(u64, u64, PeerRole)]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, 1, 1),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: peers
            .iter()
            .map(|(peer_id, store_id, role)| Peer {
                id: *peer_id,
                store_id: *store_id,
                role: *role,
                is_witness: false,
                store_epoch: 0,
            })
            .collect(),
        leader_peer_id: peers.first().map(|peer| peer.0),
        stores: peers
            .iter()
            .map(|(_, store_id, _)| Store {
                id: *store_id,
                address: format!("store-{store_id}"),
                epoch: 0,
            })
            .collect(),
        ..RegionLocation::default()
    }
}

fn request(target: RegionAttempt, proxy: Option<RegionAttempt>) -> LeaderRequest {
    LeaderRequest {
        attempt: target,
        proxy,
        role: PeerRole::Voter,
        is_witness: false,
        replica_read: false,
        stale_read: false,
        cached_leader: true,
        forwarding: true,
        read_mode: ReplicaReadMode::Leader,
    }
}

#[test]
fn pd_labels_reach_one_immutable_ordered_route_snapshot() {
    // region_cache.go:362-430 newRegion, 936-1032 GetTiKVRPCContext.
    let location = region(
        7,
        b"",
        b"z",
        &[
            (11, 101, PeerRole::Voter),
            (12, 102, PeerRole::Voter),
            (13, 103, PeerRole::Learner),
        ],
    );
    let mut cache = RegionCache::new(Loader {
        locations: VecDeque::from([location]),
        labels: BTreeMap::from([
            (101, vec![("zone".to_owned(), "shanghai".to_owned())]),
            (
                102,
                vec![
                    ("zone".to_owned(), "beijing".to_owned()),
                    ("disk".to_owned(), "ssd".to_owned()),
                ],
            ),
        ]),
    });

    let version = cache.locate_key(b"m").unwrap().region;
    let snapshot = cache.route_snapshot(version).unwrap();
    assert_eq!(snapshot.region(), version);
    assert_eq!(
        snapshot
            .peers()
            .iter()
            .map(|peer| peer.attempt().peer_id)
            .collect::<Vec<_>>(),
        [11, 12, 13]
    );
    assert!(snapshot.peers()[0].cached_leader());
    assert_eq!(snapshot.peers()[2].role(), PeerRole::Learner);
    assert!(snapshot.peers()[1].labels_match(&[
        ("zone".to_owned(), "beijing".to_owned()),
        ("disk".to_owned(), "ssd".to_owned()),
    ]));
    assert!(!snapshot.peers()[0].labels_match(&[("zone".to_owned(), "beijing".to_owned(),)]));
    assert!(snapshot.peers()[2].labels().is_empty());
}

#[test]
fn preferred_proxy_feedback_is_exact_reusable_and_direct_recovery_clears_it() {
    // region_cache.go:2822-2862 getProxyStore, 3340-3356 proxy publication.
    let location = region(
        8,
        b"",
        b"",
        &[(21, 201, PeerRole::Voter), (22, 202, PeerRole::Voter)],
    );
    let mut cache = RegionCache::new(Loader {
        locations: VecDeque::from([location]),
        labels: BTreeMap::new(),
    });
    let version = cache.locate_key(b"k").unwrap().region;
    let snapshot = cache.route_snapshot(version).unwrap();
    let target = snapshot.peers()[0].attempt().clone();
    let proxy = snapshot.peers()[1].attempt().clone();
    let forwarded = request(target.clone(), Some(proxy.clone()));

    assert_eq!(
        cache
            .apply_route_feedback(&RouteFeedback::from_request(
                &forwarded,
                RouteOutcome::Success,
            ))
            .unwrap(),
        RouteFeedbackApplication::ProxyPublished
    );
    assert_eq!(
        cache.route_snapshot(version).unwrap().preferred_proxy(),
        Some(&proxy)
    );
    assert_eq!(
        cache
            .apply_route_feedback(&RouteFeedback::from_request(
                &forwarded,
                RouteOutcome::Success,
            ))
            .unwrap(),
        RouteFeedbackApplication::Unchanged
    );

    let direct = request(target, None);
    assert_eq!(
        cache
            .apply_route_feedback(&RouteFeedback::from_request(&direct, RouteOutcome::Success,))
            .unwrap(),
        RouteFeedbackApplication::ProxyCleared
    );
    assert_eq!(
        cache.route_snapshot(version).unwrap().preferred_proxy(),
        None
    );
}

#[test]
fn proxy_failure_clears_only_that_generation_and_stale_feedback_cannot_republish() {
    let location = region(
        9,
        b"",
        b"",
        &[(31, 301, PeerRole::Voter), (32, 302, PeerRole::Voter)],
    );
    let mut cache = RegionCache::new(Loader {
        locations: VecDeque::from([location]),
        labels: BTreeMap::new(),
    });
    let version = cache.locate_key(b"k").unwrap().region;
    let snapshot = cache.route_snapshot(version).unwrap();
    let forwarded = request(
        snapshot.peers()[0].attempt().clone(),
        Some(snapshot.peers()[1].attempt().clone()),
    );
    let success = RouteFeedback::from_request(&forwarded, RouteOutcome::Success);
    let failure = RouteFeedback::from_request(&forwarded, RouteOutcome::Failure);
    cache.apply_route_feedback(&success).unwrap();
    assert_eq!(
        cache.apply_route_feedback(&failure).unwrap(),
        RouteFeedbackApplication::ProxyCleared
    );

    cache.apply_route_feedback(&success).unwrap();
    let old_proxy = forwarded.proxy().unwrap().clone();
    cache
        .on_send_failure(&old_proxy, StoreLiveness::Unreachable)
        .unwrap();
    assert!(cache.apply_route_feedback(&success).is_err());
    assert_eq!(cache.store_state(302).unwrap().epoch(), 1);
}

#[test]
fn one_store_failure_invalidates_every_region_snapshot_of_that_generation() {
    // TestRegionCache/TestSendFailInvalidateRegionsInSameStore.
    let left = region(10, b"", b"m", &[(41, 401, PeerRole::Voter)]);
    let right = region(11, b"m", b"", &[(42, 401, PeerRole::Voter)]);
    let mut cache = RegionCache::new(Loader {
        locations: VecDeque::from([left, right]),
        labels: BTreeMap::new(),
    });
    let left_version = cache.locate_key(b"a").unwrap().region;
    let right_version = cache.locate_key(b"x").unwrap().region;
    assert_eq!(cache.len(), 2);
    let failed = cache.route_snapshot(left_version).unwrap().peers()[0]
        .attempt()
        .clone();
    cache
        .on_send_failure(&failed, StoreLiveness::Unreachable)
        .unwrap();

    assert!(cache.route_snapshot(right_version).is_err());
    let mut selector = cache
        .request_selector(right_version, ReadPolicy::default())
        .unwrap();
    assert_eq!(
        cache.select_request(&mut selector).unwrap(),
        RequestSelection::ReloadRegion {
            region: right_version
        }
    );
}

#[test]
fn forwarding_runtime_preserves_target_rotates_proxy_reuses_and_recovers_direct() {
    let location = region(
        12,
        b"",
        b"",
        &[
            (51, 501, PeerRole::Voter),
            (52, 502, PeerRole::Voter),
            (53, 503, PeerRole::Voter),
        ],
    );
    let mut cache = RegionCache::new(Loader {
        locations: VecDeque::from([location]),
        labels: BTreeMap::new(),
    });
    let version = cache.locate_key(b"k").unwrap().region;
    let policy = ReadPolicy {
        forwarding: true,
        ..ReadPolicy::default()
    };
    let mut selector = cache.request_selector(version, policy).unwrap();

    let RequestSelection::Attempt(direct) = cache.select_request(&mut selector).unwrap() else {
        panic!("leader must be tried directly first")
    };
    assert_eq!(direct.target().store_id, 501);
    assert!(direct.proxy().is_none());
    assert!(selector.record_attempt_result(direct.target(), Duration::from_millis(1)));
    assert_eq!(
        cache
            .on_route_send_failure(&direct, StoreLiveness::Unreachable)
            .unwrap(),
        StoreFailureOutcome::ForwardingRequired { epoch: 0 }
    );
    assert_eq!(cache.store_state(501).unwrap().epoch(), 0);

    let RequestSelection::Attempt(first_proxy) = cache.select_request(&mut selector).unwrap()
    else {
        panic!("unreachable leader must retain its identity through a proxy")
    };
    assert_eq!(first_proxy.target(), direct.target());
    assert_eq!(first_proxy.proxy().map(|proxy| proxy.store_id), Some(502));
    assert_eq!(first_proxy.dispatch_address(), "store-502");
    assert_eq!(first_proxy.forwarded_host(), Some("store-501"));
    assert!(selector.abort_unsent_attempt(first_proxy.target(), first_proxy.proxy()));
    let RequestSelection::Attempt(first_proxy_retry) = cache.select_request(&mut selector).unwrap()
    else {
        panic!("unsent forwarding rollback must restore target and proxy budget")
    };
    assert_eq!(first_proxy_retry, first_proxy);
    let first_proxy = first_proxy_retry;
    assert!(selector.record_attempt_result(first_proxy.target(), Duration::from_millis(1)));
    cache
        .on_route_send_failure(&first_proxy, StoreLiveness::Unreachable)
        .unwrap();
    assert_eq!(cache.store_state(501).unwrap().epoch(), 0);
    assert_eq!(cache.store_state(502).unwrap().epoch(), 1);

    let RequestSelection::Attempt(second_proxy) = cache.select_request(&mut selector).unwrap()
    else {
        panic!("failed physical proxy must rotate without changing the target")
    };
    assert_eq!(second_proxy.target(), direct.target());
    assert_eq!(second_proxy.proxy().map(|proxy| proxy.store_id), Some(503));
    assert!(selector.record_attempt_result(second_proxy.target(), Duration::from_millis(1)));
    assert_eq!(
        cache.on_route_success(&second_proxy).unwrap(),
        RouteFeedbackApplication::ProxyPublished
    );

    let mut fresh = cache.request_selector(version, policy).unwrap();
    let RequestSelection::Attempt(reused) = cache.select_request(&mut fresh).unwrap() else {
        panic!("fresh selector must reuse the proven proxy")
    };
    assert_eq!(reused.proxy().map(|proxy| proxy.store_id), Some(503));

    cache
        .on_send_failure(direct.target(), StoreLiveness::Reachable)
        .unwrap();
    let mut recovered = cache.request_selector(version, policy).unwrap();
    let RequestSelection::Attempt(recovered_direct) = cache.select_request(&mut recovered).unwrap()
    else {
        panic!("reachable leader must return to direct dispatch")
    };
    assert!(recovered_direct.proxy().is_none());
    assert_eq!(recovered_direct.dispatch_address(), "store-501");
    assert_eq!(
        cache.on_route_success(&recovered_direct).unwrap(),
        RouteFeedbackApplication::ProxyCleared
    );
    assert!(cache.preferred_proxy(version).is_none());
}

#[test]
fn busy_runtime_executes_the_exact_500_800_150_diversion_sequence() {
    let location = region(
        13,
        b"",
        b"",
        &[
            (61, 601, PeerRole::Voter),
            (62, 602, PeerRole::Voter),
            (63, 603, PeerRole::Voter),
        ],
    );
    let mut cache = RegionCache::new(Loader {
        locations: VecDeque::from([location]),
        labels: BTreeMap::new(),
    });
    let version = cache.locate_key(b"k").unwrap().region;
    let start = Duration::from_secs(1);
    let mut selector = cache
        .request_selector(version, ReadPolicy::default())
        .unwrap();
    selector.set_busy_threshold(Duration::from_millis(50));

    for (expected_peer, estimated_wait_ms) in [(61, 500), (62, 800), (63, 150)] {
        let RequestSelection::Attempt(selected) =
            cache.select_request_at(&mut selector, start).unwrap()
        else {
            panic!("an idle replica must remain before the final busy response")
        };
        assert_eq!(selected.target().peer_id, expected_peer);
        assert!(selector.record_attempt_result(selected.target(), Duration::from_millis(1)));
        cache
            .on_server_busy(&mut selector, selected.target(), estimated_wait_ms, start)
            .unwrap();
        assert!(selector.acknowledge_server_busy(selected.target()));
    }

    let RequestSelection::Attempt(fallback) =
        cache.select_request_at(&mut selector, start).unwrap()
    else {
        panic!("all-busy fallback must retain the region and retry its leader")
    };
    assert_eq!(fallback.target().peer_id, 61);
    assert!(!fallback.replica_read);
    assert_eq!(selector.busy_threshold(), Duration::ZERO);
    assert_eq!(cache.len(), 1);

    let mut fresh = cache
        .request_selector(version, ReadPolicy::default())
        .unwrap();
    fresh.set_busy_threshold(Duration::from_millis(50));
    let RequestSelection::Attempt(least_busy) = cache
        .select_request_at(&mut fresh, start + Duration::from_millis(120))
        .unwrap()
    else {
        panic!("decayed 150ms follower must be selected by the fresh request")
    };
    assert_eq!(least_busy.target().peer_id, 63);
    assert!(least_busy.replica_read);
}
