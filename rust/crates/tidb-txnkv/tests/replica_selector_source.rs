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

//! Source-shaped leader selector policy tests.

use tidb_txnkv::region::{
    Peer, PeerRole, ReadPolicy, RegionCache, RegionLoadError, RegionLoader, RegionLocation,
    RegionRouteError, RegionVerId, ReplicaReadMode, RequestSelection, Store,
};

struct Loader(Option<RegionLocation>);

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.0
            .take()
            .ok_or_else(|| RegionLoadError::new("empty", "region already loaded"))
    }
}

fn location() -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(9, 2, 3),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![
            Peer {
                id: 11,
                store_id: 101,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 4,
            },
            Peer {
                id: 12,
                store_id: 102,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 8,
            },
        ],
        leader_peer_id: Some(12),
        stores: vec![
            Store {
                id: 101,
                address: "follower:20160".to_owned(),
                epoch: 4,
            },
            Store {
                id: 102,
                address: "leader:20160".to_owned(),
                epoch: 8,
            },
        ],
    }
}

fn cache() -> RegionCache<Loader> {
    let mut cache = RegionCache::new(Loader(Some(location())));
    cache.locate_key(b"key").unwrap();
    cache
}

#[test]
fn leader_policy_selects_pd_leader_first_with_leader_flags() {
    let mut cache = cache();
    let region = location().region;
    let mut selector = cache
        .request_selector(region, ReadPolicy::default())
        .unwrap();
    let RequestSelection::Attempt(selected) = cache.select_request(&mut selector).unwrap() else {
        panic!("leader must be selected")
    };
    assert_eq!(selected.attempt.peer_id, 12);
    assert_eq!(selected.attempt.address, "leader:20160");
    assert!(selected.cached_leader);
    assert!(!selected.replica_read);
    assert!(!selected.stale_read);
}

#[test]
fn supported_replica_read_policies_create_request_scoped_selectors() {
    for policy in [
        ReadPolicy {
            mode: ReplicaReadMode::Follower,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::Mixed,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::Learner,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::PreferLeader,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::Mixed,
            stale_read: true,
            ..ReadPolicy::default()
        },
    ] {
        assert!(cache().request_selector(location().region, policy).is_ok());
    }
}

#[test]
fn leader_forwarding_is_admitted_and_invalid_stale_combinations_fail_closed() {
    assert!(cache()
        .request_selector(
            location().region,
            ReadPolicy {
                forwarding: true,
                ..ReadPolicy::default()
            },
        )
        .is_ok());

    for policy in [
        ReadPolicy {
            mode: ReplicaReadMode::Leader,
            stale_read: true,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::Follower,
            stale_read: true,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::Learner,
            stale_read: true,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            mode: ReplicaReadMode::PreferLeader,
            stale_read: true,
            ..ReadPolicy::default()
        },
    ] {
        assert_eq!(
            cache().request_selector(location().region, policy),
            Err(RegionRouteError::UnsupportedReadPolicy)
        );
    }
}
