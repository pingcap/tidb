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

//! Source-shaped single-region cache tests.

use std::collections::VecDeque;

use tidb_txnkv::region::{
    KeyRange, Peer, PeerRole, RegionCache, RegionLoader, RegionLocation, RegionRouteError,
    RegionVerId, Store,
};

struct Loader {
    loads: usize,
    regions: VecDeque<RegionLocation>,
}

impl RegionLoader for Loader {
    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, String> {
        self.loads += 1;
        self.regions
            .pop_front()
            .ok_or_else(|| "no region".to_owned())
    }
}

fn location(id: u64, conf_ver: u64, version: u64, start: &[u8], end: &[u8]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, conf_ver, version),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: vec![Peer {
            id: id * 10,
            store_id: id * 100,
            role: PeerRole::Voter,
            store_epoch: 7,
        }],
        leader_peer_id: Some(id * 10),
        stores: vec![Store {
            id: id * 100,
            address: format!("store-{id}"),
            epoch: 7,
        }],
    }
}

#[test]
fn cache_miss_loads_once_then_ordered_hits_reuse_snapshot() {
    let loader = Loader {
        loads: 0,
        regions: VecDeque::from([location(2, 1, 1, b"m", b""), location(1, 1, 1, b"", b"m")]),
    };
    let mut cache = RegionCache::new(loader);

    assert_eq!(cache.locate_key(b"z").unwrap().region.id, 2);
    assert_eq!(cache.locate_key(b"y").unwrap().region.id, 2);
    assert_eq!(cache.locate_key(b"a").unwrap().region.id, 1);
    assert_eq!(cache.locate_key(b"b").unwrap().region.id, 1);
    assert_eq!(cache.len(), 2);
}

#[test]
fn exact_invalidation_refills_but_wrong_epoch_does_not_remove() {
    let first = location(1, 1, 1, b"", b"");
    let replacement = location(1, 2, 1, b"", b"");
    let loader = Loader {
        loads: 0,
        regions: VecDeque::from([first.clone(), replacement.clone()]),
    };
    let mut cache = RegionCache::new(loader);

    assert_eq!(cache.locate_key(b"a").unwrap(), &first);
    assert!(!cache.invalidate(RegionVerId::new(1, 2, 1)));
    assert_eq!(cache.locate_key(b"b").unwrap(), &first);
    assert!(cache.invalidate(first.region));
    assert_eq!(cache.locate_key(b"b").unwrap(), &replacement);
}

#[test]
fn one_region_range_is_admitted_and_cross_region_range_fails_closed() {
    let loader = Loader {
        loads: 0,
        regions: VecDeque::from([location(1, 1, 1, b"a", b"m")]),
    };
    let mut cache = RegionCache::new(loader);

    let admitted = KeyRange::new(b"b".to_vec(), b"m".to_vec());
    assert_eq!(cache.locate_range(&admitted).unwrap().region.id, 1);
    let crossing = KeyRange::new(b"b".to_vec(), b"z".to_vec());
    assert_eq!(
        cache.locate_range(&crossing),
        Err(RegionRouteError::MultiRegion)
    );
    let unbounded = KeyRange::new(b"b".to_vec(), Vec::new());
    assert_eq!(
        cache.locate_range(&unbounded),
        Err(RegionRouteError::MultiRegion)
    );
}

#[test]
fn loader_must_return_a_containing_region() {
    let loader = Loader {
        loads: 0,
        regions: VecDeque::from([location(1, 1, 1, b"m", b"z")]),
    };
    let mut cache = RegionCache::new(loader);
    assert!(matches!(
        cache.locate_key(b"a"),
        Err(RegionRouteError::Loader(_))
    ));
    assert!(cache.is_empty());
}
