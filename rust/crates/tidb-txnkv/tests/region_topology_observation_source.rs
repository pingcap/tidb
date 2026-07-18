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

//! Direct transition of the send-failure peer-vector observation guard.

use std::collections::VecDeque;

use tidb_txnkv::region::{
    Peer, PeerRole, ReadPolicy, RegionCache, RegionLoadError, RegionLoader, RegionLocation,
    RegionRecoveryError, RegionVerId, RequestSelection, Store, StoreLiveness,
};

struct Loader(VecDeque<RegionLocation>);

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.0
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-region", "topology script exhausted"))
    }
}

fn location(peer_one_witness: bool, peer_one_down: bool) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(7, 1, 1),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![
            Peer {
                id: 11,
                store_id: 101,
                role: PeerRole::Voter,
                is_witness: peer_one_witness,
                store_epoch: 3,
            },
            Peer {
                id: 12,
                store_id: 102,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 4,
            },
        ],
        leader_peer_id: Some(12),
        stores: vec![
            Store {
                id: 101,
                address: "tikv-1".to_owned(),
                epoch: 3,
            },
            Store {
                id: 102,
                address: "tikv-2".to_owned(),
                epoch: 4,
            },
        ],
        down_peer_ids: peer_one_down.then_some(11).into_iter().collect(),
        ..RegionLocation::default()
    }
}

#[test]
fn cache_issued_observation_rejects_down_and_witness_transitions() {
    for after in [location(true, false), location(false, true)] {
        let before = location(false, false);
        let mut cache = RegionCache::new(Loader([before.clone(), after].into()));
        cache.locate_key(b"a").unwrap();
        let mut selector = cache
            .request_selector(before.region, ReadPolicy::default())
            .unwrap();
        let RequestSelection::Attempt(selected) = cache.select_request(&mut selector).unwrap()
        else {
            panic!("fresh region must select its leader");
        };
        let observation = cache.observe_attempt(selected.dispatch_attempt()).unwrap();

        cache.invalidate(before.region);
        cache.locate_key(b"a").unwrap();
        assert!(matches!(
            cache.on_route_send_failure_observed(
                &selected,
                &observation,
                StoreLiveness::Unreachable,
            ),
            Err(RegionRecoveryError::StaleObservation(_))
        ));
        assert_eq!(cache.store_state(102).unwrap().epoch(), 4);
        assert_eq!(
            cache.store_state(102).unwrap().liveness(),
            StoreLiveness::Reachable
        );
    }
}
