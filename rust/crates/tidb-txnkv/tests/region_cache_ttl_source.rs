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

//! Direct transition of client-go RegionCache TTL and delayed-reload flags.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tidb_txnkv::region::{
    BatchRegionLoader, CacheEntryState, CacheReloadState, KeyRange, Peer, PeerRole, ReadPolicy,
    RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation, RegionVerId,
    ReplicaReadMode, RequestSelection, Store, StoreLiveness,
};

struct Loader {
    loads: usize,
    down: bool,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.loads += 1;
        Ok(RegionLocation {
            region: RegionVerId {
                id: 1,
                epoch: RegionEpoch {
                    conf_ver: self.loads as u64,
                    version: self.loads as u64,
                },
            },
            start_key: Vec::new(),
            end_key: Vec::new(),
            peers: Vec::new(),
            leader_peer_id: None,
            stores: Vec::new(),
            buckets: None,
            down_peer_ids: self.down.then_some(9).into_iter().collect(),
            pending_peer_ids: Vec::new(),
        })
    }
}

#[test]
fn strict_expiry_and_near_boundary_renewal_match_go() {
    let mut state = CacheEntryState::new(102);
    assert!(state.check_and_renew(102, 2, 104));
    assert_eq!(state.expires_at_seconds(), 104);
    assert!(!state.check_and_renew(105, 2, 107));

    let mut fixed = CacheEntryState::new(102);
    fixed.mark(CacheReloadState::ExpireAfterTtl);
    assert!(fixed.check_and_renew(102, 2, 104));
    assert_eq!(fixed.expires_at_seconds(), 102);
    assert!(!fixed.check_and_renew(103, 2, 105));
}

#[test]
fn source_reload_flags_do_not_collapse_into_one_boolean() {
    let mut delayed = CacheEntryState::new(200);
    delayed.mark(CacheReloadState::DelayedReloadPending);
    assert!(delayed.check_and_renew(100, 10, 110));
    assert!(delayed.release_delayed_reload());
    assert!(!delayed.check_and_renew(100, 10, 110));

    let mut immediate = CacheEntryState::new(200);
    immediate.mark(CacheReloadState::ReloadOnAccess);
    assert!(!immediate.check_and_renew(100, 10, 110));
}

#[test]
fn down_peer_entry_expires_even_under_continuous_access() {
    let mut cache = RegionCache::with_ttl(
        Loader {
            loads: 0,
            down: true,
        },
        2,
        0,
    );
    let first = cache.locate_key_at(b"a", 100).unwrap().region;
    assert_eq!(first.epoch.version, 1);
    assert_eq!(cache.locate_key_at(b"a", 102).unwrap().region, first);
    let reloaded = cache.locate_key_at(b"a", 103).unwrap().region;
    assert_eq!(reloaded.epoch.version, 2);
}

struct DisjointLoader {
    loads: Arc<Mutex<Vec<u64>>>,
}

impl RegionLoader for DisjointLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let (id, start, end) = if key < b"m".as_slice() {
            (1, b"a".as_slice(), b"m".as_slice())
        } else {
            (2, b"m".as_slice(), b"z".as_slice())
        };
        let mut loads = self.loads.lock().unwrap();
        loads.push(id);
        let version = loads.iter().filter(|loaded| **loaded == id).count() as u64;
        Ok(region(id, version, start, end))
    }
}

impl BatchRegionLoader for DisjointLoader {
    fn batch_load_regions(
        &mut self,
        _ranges: &[KeyRange],
        _limit: usize,
        _need_buckets: bool,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-batch-load",
            "the requested region must remain cached",
        ))
    }
}

#[test]
fn batch_access_renews_only_regions_traversed_by_requested_ranges() {
    let loads = Arc::new(Mutex::new(Vec::new()));
    let mut cache = RegionCache::with_ttl(
        DisjointLoader {
            loads: Arc::clone(&loads),
        },
        2,
        0,
    );
    let left = cache.locate_key_at(b"a", 100).unwrap().region;
    let right = cache.locate_key_at(b"x", 100).unwrap().region;
    assert_eq!(*loads.lock().unwrap(), [1, 2]);

    let left_range = [KeyRange::new(b"a".to_vec(), b"m".to_vec())];
    assert_eq!(
        cache
            .batch_locate_key_ranges_at(&left_range, false, 102)
            .unwrap()[0]
            .region,
        left
    );
    assert_eq!(
        cache
            .batch_locate_key_ranges_at(&left_range, false, 103)
            .unwrap()[0]
            .region,
        left
    );

    let reloaded = cache.locate_key_at(b"x", 103).unwrap().region;
    assert_eq!(reloaded.id, right.id);
    assert_eq!(reloaded.epoch.version, 2);
    assert_eq!(*loads.lock().unwrap(), [1, 2, 2]);
}

struct StableBatchLoader {
    location: RegionLocation,
    batch_calls: Arc<Mutex<usize>>,
}

impl RegionLoader for StableBatchLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Ok(self.location.clone())
    }
}

impl BatchRegionLoader for StableBatchLoader {
    fn batch_load_regions(
        &mut self,
        _ranges: &[KeyRange],
        _limit: usize,
        _need_buckets: bool,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        *self.batch_calls.lock().unwrap() += 1;
        Ok(vec![self.location.clone()])
    }
}

#[test]
fn expired_batch_reload_with_identical_region_identity_remains_visible() {
    let batch_calls = Arc::new(Mutex::new(0));
    let stable = region(7, 3, b"a", b"z");
    let mut cache = RegionCache::with_ttl(
        StableBatchLoader {
            location: stable.clone(),
            batch_calls: Arc::clone(&batch_calls),
        },
        2,
        0,
    );
    assert_eq!(cache.locate_key_at(b"b", 100).unwrap(), &stable);

    let located = cache
        .batch_locate_key_ranges_at(&[KeyRange::new(b"a".to_vec(), b"z".to_vec())], false, 103)
        .unwrap();
    assert_eq!(located, [stable]);
    assert_eq!(*batch_calls.lock().unwrap(), 1);
}

struct SequenceLoader {
    regions: VecDeque<RegionLocation>,
    loads: Arc<Mutex<usize>>,
}

impl RegionLoader for SequenceLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        *self.loads.lock().unwrap() += 1;
        self.regions.pop_front().ok_or_else(|| {
            RegionLoadError::new("missing-scripted-region", "test loader was exhausted")
        })
    }
}

#[test]
fn maintenance_releases_delayed_reload_into_foreground_split_lookup() {
    let loads = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        SequenceLoader {
            regions: VecDeque::from([region(1, 1, b"a", b"z"), region(2, 2, b"m", b"z")]),
            loads: Arc::clone(&loads),
        },
        10,
        0,
    );
    let parent = cache.locate_key_at(b"x", 100).unwrap().region;
    assert!(cache.mark_delayed_reload(parent));

    assert_eq!(cache.locate_key_at(b"x", 101).unwrap().region, parent);
    assert_eq!(*loads.lock().unwrap(), 1);
    assert_eq!(cache.maintain_entries_at(101), 1);

    let child = cache.locate_key_at(b"x", 101).unwrap().region;
    assert_eq!(child.id, 2);
    assert_eq!(*loads.lock().unwrap(), 2);
}

#[test]
fn replica_selection_marks_stale_candidate_store_for_delayed_reload() {
    let loads = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        SequenceLoader {
            regions: VecDeque::from([routed_region(1, 1), routed_region(2, 2)]),
            loads: Arc::clone(&loads),
        },
        10,
        0,
    );
    let region = cache.locate_key_at(b"x", 100).unwrap().region;
    let policy = ReadPolicy {
        mode: ReplicaReadMode::Mixed,
        ..ReadPolicy::default()
    };
    let mut first_selector = cache.request_selector(region, policy).unwrap();
    let RequestSelection::Attempt(leader) = cache.select_request(&mut first_selector).unwrap()
    else {
        panic!("the first mixed read must select the leader")
    };
    assert!(leader.cached_leader);
    cache
        .on_send_failure(&leader.attempt, StoreLiveness::Unreachable)
        .unwrap();

    let mut retry_selector = cache.request_selector(region, policy).unwrap();
    let RequestSelection::Attempt(follower) = cache.select_request(&mut retry_selector).unwrap()
    else {
        panic!("the healthy follower must remain selectable")
    };
    assert!(!follower.cached_leader);
    assert_eq!(cache.maintain_entries_at(101), 1);

    assert_eq!(cache.locate_key_at(b"x", 101).unwrap().region.id, 2);
    assert_eq!(*loads.lock().unwrap(), 2);
}

fn region(id: u64, version: u64, start: &[u8], end: &[u8]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId {
            id,
            epoch: RegionEpoch {
                conf_ver: version,
                version,
            },
        },
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: Vec::new(),
        leader_peer_id: None,
        stores: Vec::new(),
        buckets: None,
        down_peer_ids: Vec::new(),
        pending_peer_ids: Vec::new(),
    }
}

fn routed_region(id: u64, version: u64) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, version, version),
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        peers: vec![
            Peer {
                id: id * 10 + 1,
                store_id: 101,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 0,
            },
            Peer {
                id: id * 10 + 2,
                store_id: 102,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 0,
            },
        ],
        leader_peer_id: Some(id * 10 + 1),
        stores: vec![
            Store {
                id: 101,
                address: "leader:20160".to_owned(),
                epoch: 0,
            },
            Store {
                id: 102,
                address: "follower:20160".to_owned(),
                epoch: 0,
            },
        ],
        buckets: None,
        down_peer_ids: Vec::new(),
        pending_peer_ids: Vec::new(),
    }
}
