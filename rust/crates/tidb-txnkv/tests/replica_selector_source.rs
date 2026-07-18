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

//! Source-shaped leader selector tests.

use tidb_txnkv::region::{
    Peer, PeerRole, ReadPolicy, RegionLocation, RegionRouteError, RegionVerId, ReplicaReadMode,
    ReplicaSelector, Store,
};

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
                store_epoch: 4,
            },
            Peer {
                id: 12,
                store_id: 102,
                role: PeerRole::Voter,
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

#[test]
fn leader_policy_selects_only_pd_leader_store() {
    let location = location();
    let selected = ReplicaSelector::select_leader(&location, ReadPolicy::default()).unwrap();
    assert_eq!(selected.peer.id, 12);
    assert_eq!(selected.store.address, "leader:20160");
}

#[test]
fn follower_stale_mixed_and_proxy_modes_fail_closed() {
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
            stale_read: true,
            ..ReadPolicy::default()
        },
        ReadPolicy {
            forwarding: true,
            ..ReadPolicy::default()
        },
    ] {
        assert_eq!(
            ReplicaSelector::select_leader(&location(), policy),
            Err(RegionRouteError::UnsupportedReadPolicy)
        );
    }
}

#[test]
fn missing_metadata_and_stale_store_epoch_are_typed_failures() {
    let mut candidate = location();
    candidate.leader_peer_id = None;
    assert_eq!(
        ReplicaSelector::select_leader(&candidate, ReadPolicy::default()),
        Err(RegionRouteError::MissingLeader)
    );

    candidate = location();
    candidate.stores.retain(|store| store.id != 102);
    assert_eq!(
        ReplicaSelector::select_leader(&candidate, ReadPolicy::default()),
        Err(RegionRouteError::MissingStore(102))
    );

    candidate = location();
    candidate.stores[1].address.clear();
    assert_eq!(
        ReplicaSelector::select_leader(&candidate, ReadPolicy::default()),
        Err(RegionRouteError::MissingAddress(102))
    );

    candidate = location();
    candidate.stores[1].epoch = 9;
    assert_eq!(
        ReplicaSelector::select_leader(&candidate, ReadPolicy::default()),
        Err(RegionRouteError::StaleStoreEpoch {
            store_id: 102,
            expected: 8,
            actual: 9,
        })
    );
}
