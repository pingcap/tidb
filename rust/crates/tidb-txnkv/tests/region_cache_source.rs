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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use tidb_txnkv::region::{
    KeyRange, Peer, PeerRole, RegionCache, RegionLoadError, RegionLoader, RegionLocation,
    RegionRouteError, RegionVerId, Store,
};

struct Loader {
    cluster_id: u64,
    loads: usize,
    regions: VecDeque<RegionLocation>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.loads += 1;
        self.regions
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("test-loader-empty", "no region"))
    }
}

struct FailingLoader {
    cluster_id: u64,
    error: Option<RegionLoadError>,
}

struct RecordingLoader {
    calls: Rc<RefCell<Vec<Vec<u8>>>>,
    regions: VecDeque<RegionLocation>,
}

impl RegionLoader for RecordingLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.calls.borrow_mut().push(key.to_vec());
        self.regions
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("test-loader-empty", "no region"))
    }
}

impl RegionLoader for FailingLoader {
    fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Err(self.error.take().expect("test loader called once"))
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
            is_witness: false,
            store_epoch: 7,
        }],
        leader_peer_id: Some(id * 10),
        stores: vec![Store {
            id: id * 100,
            address: format!("store-{id}"),
            epoch: 7,
        }],
        ..RegionLocation::default()
    }
}

#[test]
fn cache_miss_loads_once_then_ordered_hits_reuse_snapshot() {
    let loader = Loader {
        cluster_id: 42,
        loads: 0,
        regions: VecDeque::from([location(2, 1, 1, b"m", b""), location(1, 1, 1, b"", b"m")]),
    };
    let mut cache = RegionCache::new(loader);

    assert_eq!(cache.cluster_id(), 42);
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
        cluster_id: 42,
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
        cluster_id: 42,
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
        cluster_id: 42,
        loads: 0,
        regions: VecDeque::from([location(1, 1, 1, b"m", b"z")]),
    };
    let mut cache = RegionCache::new(loader);
    assert!(matches!(
        cache.locate_key(b"a"),
        Err(RegionRouteError::LoadedRegionDoesNotContainKey {
            region: RegionVerId { id: 1, .. }
        })
    ));
    assert!(cache.is_empty());
}

#[test]
fn loader_failure_preserves_concrete_identity_and_message() {
    let failure = RegionLoadError::new("mock-pd::missing-region", "no region for 6162");
    assert_eq!(failure.identity(), "mock-pd::missing-region");
    assert_eq!(failure.message(), "no region for 6162");
    assert_eq!(
        failure.to_string(),
        "mock-pd::missing-region: no region for 6162"
    );

    let loader = FailingLoader {
        cluster_id: 42,
        error: Some(failure.clone()),
    };
    let mut cache = RegionCache::new(loader);
    let route_error = cache.locate_key(b"ab").unwrap_err();
    assert_eq!(route_error, RegionRouteError::Loader(failure));
    assert_eq!(
        route_error.to_string(),
        "region loader failed: mock-pd::missing-region: no region for 6162"
    );
    assert!(cache.is_empty());
}

#[test]
fn stale_merge_parent_does_not_evict_newer_split_child() {
    let child = location(2, 1, 3, b"m", b"z");
    let stale_parent = location(1, 1, 2, b"a", b"z");
    let loader = Loader {
        cluster_id: 42,
        loads: 0,
        regions: VecDeque::from([child.clone(), stale_parent.clone()]),
    };
    let mut cache = RegionCache::new(loader);

    assert_eq!(cache.locate_key(b"n").unwrap(), &child);
    assert_eq!(
        cache.locate_key(b"b"),
        Err(RegionRouteError::StaleRegionEpoch {
            loaded: stale_parent.region,
            cached: child.region,
        })
    );

    assert_eq!(cache.len(), 1);
    assert_eq!(cache.locate_key(b"n").unwrap(), &child);
}

#[test]
fn stale_same_region_loader_result_is_rejected_without_eviction() {
    let current = location(3, 4, 5, b"m", b"z");
    let stale = location(3, 4, 4, b"a", b"z");
    let loader = Loader {
        cluster_id: 42,
        loads: 0,
        regions: VecDeque::from([current.clone(), stale.clone()]),
    };
    let mut cache = RegionCache::new(loader);

    assert_eq!(cache.locate_key(b"n").unwrap(), &current);
    assert_eq!(
        cache.locate_key(b"b"),
        Err(RegionRouteError::StaleRegionEpoch {
            loaded: stale.region,
            cached: current.region,
        })
    );
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.locate_key(b"n").unwrap(), &current);
}

#[test]
fn range_walk_reuses_cache_and_preserves_contiguous_region_order() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader = RecordingLoader {
        calls: Rc::clone(&calls),
        regions: VecDeque::from([location(1, 1, 1, b"a", b"m"), location(2, 1, 1, b"m", b"z")]),
    };
    let mut cache = RegionCache::new(loader);
    let left = KeyRange::new(b"a".to_vec(), b"m".to_vec());
    let right = KeyRange::new(b"m".to_vec(), b"z".to_vec());

    let regions = cache.locate_ranges(&[left, right]).unwrap();
    assert_eq!(
        regions
            .iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(calls.borrow().as_slice(), [b"a".to_vec(), b"m".to_vec()]);

    assert_eq!(
        cache
            .locate_ranges(&[KeyRange::new(b"a".to_vec(), b"z".to_vec())])
            .unwrap(),
        regions
    );
    assert_eq!(calls.borrow().len(), 2, "cache hits must not reload PD");
}

#[test]
fn overlapping_and_duplicate_inputs_do_not_duplicate_region_loads() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader = RecordingLoader {
        calls: Rc::clone(&calls),
        regions: VecDeque::from([location(1, 1, 1, b"a", b"m"), location(2, 1, 1, b"m", b"z")]),
    };
    let mut cache = RegionCache::new(loader);
    let whole = KeyRange::new(b"a".to_vec(), b"z".to_vec());
    let overlap = KeyRange::new(b"b".to_vec(), b"y".to_vec());

    let regions = cache
        .locate_ranges(&[whole.clone(), overlap, whole])
        .unwrap();
    assert_eq!(
        regions
            .iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(calls.borrow().as_slice(), [b"a".to_vec(), b"m".to_vec()]);
}

#[test]
fn unbounded_final_region_terminates_and_end_boundary_is_exclusive() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader = RecordingLoader {
        calls: Rc::clone(&calls),
        regions: VecDeque::from([location(1, 1, 1, b"a", b"m"), location(2, 1, 1, b"m", b"")]),
    };
    let mut cache = RegionCache::new(loader);

    let first_only = cache
        .locate_ranges(&[KeyRange::new(b"a".to_vec(), b"m".to_vec())])
        .unwrap();
    assert_eq!(first_only[0].region.id, 1);
    assert_eq!(
        calls.borrow().len(),
        1,
        "end equality must not load the next region"
    );

    let all = cache
        .locate_ranges(&[KeyRange::new(b"a".to_vec(), Vec::new())])
        .unwrap();
    assert_eq!(
        all.iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(calls.borrow().as_slice(), [b"a".to_vec(), b"m".to_vec()]);
}

#[test]
fn malformed_ranges_and_region_bounds_fail_before_cache_insertion() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let loader = RecordingLoader {
        calls: Rc::clone(&calls),
        regions: VecDeque::from([location(1, 1, 1, b"m", b"m")]),
    };
    let mut cache = RegionCache::new(loader);

    for invalid in [
        KeyRange::new(b"m".to_vec(), b"m".to_vec()),
        KeyRange::new(b"z".to_vec(), b"a".to_vec()),
    ] {
        assert_eq!(
            cache.locate_ranges(&[invalid]),
            Err(RegionRouteError::InvalidRange)
        );
    }
    assert!(calls.borrow().is_empty());

    assert_eq!(
        cache.locate_ranges(&[KeyRange::new(b"m".to_vec(), b"z".to_vec())]),
        Err(RegionRouteError::InvalidRegionBounds {
            region: RegionVerId::new(1, 1, 1),
        })
    );
    assert!(cache.is_empty());
}

#[test]
fn overlapping_or_gapped_successor_boundary_fails_without_replacing_cached_region() {
    for successor_start in [b"a".as_slice(), b"l".as_slice(), b"n".as_slice()] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let first = location(1, 1, 1, b"a", b"m");
        let successor = location(2, 1, 1, successor_start, b"z");
        let loader = RecordingLoader {
            calls,
            regions: VecDeque::from([first.clone(), successor.clone()]),
        };
        let mut cache = RegionCache::new(loader);

        assert_eq!(
            cache.locate_ranges(&[KeyRange::new(b"a".to_vec(), b"z".to_vec())]),
            Err(RegionRouteError::DiscontinuousRegion {
                region: successor.region,
            })
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.locate_key(b"b").unwrap(), &first);
    }
}
