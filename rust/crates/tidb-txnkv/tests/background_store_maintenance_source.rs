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

//! Direct transition of in-place store re-resolution and tombstone expiry.

use std::collections::VecDeque;
use std::time::Duration;

use tidb_txnkv::region::{
    KeyRange, Peer, PeerRole, ReadPolicy, RegionAttempt, RegionCache, RegionLoadError,
    RegionLoader, RegionLocation, RegionQuery, RegionQueryLoader, RegionQueryOptions, RegionVerId,
    RequestSelection, Store, StoreFailureOutcome, StoreLiveness, StoreMetadata,
    StoreRefreshOutcome, StoreResolveState,
};
use tidb_txnkv::SharedReadRuntime;

struct Loader {
    location: Option<RegionLocation>,
    stores: VecDeque<Option<StoreMetadata>>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.location
            .take()
            .ok_or_else(|| RegionLoadError::new("missing-region", "region script exhausted"))
    }
}

impl RegionQueryLoader for Loader {
    fn query_region(
        &mut self,
        _query: RegionQuery<'_>,
        _options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-query",
            "query not expected",
        ))
    }

    fn scan_regions_once(
        &mut self,
        _range: &KeyRange,
        _limit: usize,
        _options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Err(RegionLoadError::new("unexpected-scan", "scan not expected"))
    }

    fn load_store(&mut self, _store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        self.stores
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-store", "store script exhausted"))
    }
}

fn location() -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(7, 1, 1),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![Peer {
            id: 11,
            store_id: 101,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: 7,
        }],
        leader_peer_id: Some(11),
        stores: vec![Store {
            id: 101,
            address: "tikv-old".to_owned(),
            epoch: 7,
        }],
        ..RegionLocation::default()
    }
}

#[test]
fn refresh_mutates_one_store_record_and_tombstone_expires_dependents() {
    let location = location();
    let mut cache = RegionCache::with_ttl(
        Loader {
            location: Some(location.clone()),
            stores: vec![
                Some(StoreMetadata {
                    id: 101,
                    address: "tikv-new".to_owned(),
                    labels: vec![("zone".to_owned(), "z2".to_owned())],
                }),
                None,
            ]
            .into(),
        },
        10,
        0,
    );
    cache.locate_key_at(b"a", 0).unwrap();
    let state_address = cache.store_state(101).unwrap() as *const _ as usize;
    let attempt = RegionAttempt {
        region: location.region,
        peer_id: 11,
        store_id: 101,
        address: "tikv-old".to_owned(),
        store_epoch: 7,
    };
    cache
        .on_send_failure(&attempt, StoreLiveness::Unreachable)
        .unwrap();

    assert_eq!(
        cache.refresh_store(101).unwrap(),
        StoreRefreshOutcome::Refreshed
    );
    let refreshed = cache.store_state(101).unwrap();
    assert_eq!(refreshed as *const _ as usize, state_address);
    assert_eq!(
        refreshed.epoch(),
        9,
        "NeedCheck address change advances generation"
    );
    assert_eq!(refreshed.address(), "tikv-new");
    assert_eq!(refreshed.resolve_state(), StoreResolveState::Resolved);
    assert_eq!(refreshed.liveness(), StoreLiveness::Unreachable);
    assert_eq!(cache.store_label(101, "zone"), Some("z2"));

    assert_eq!(
        cache.refresh_store(101).unwrap(),
        StoreRefreshOutcome::Removed
    );
    assert_eq!(
        cache.store_state(101).unwrap().resolve_state(),
        StoreResolveState::Removed
    );
    assert_eq!(cache.maintain_entries_at(11), 0);
    assert!(
        cache.is_empty(),
        "removed-store dependent expires at its existing TTL"
    );
}

#[test]
fn production_failure_triggers_in_place_maintenance_on_the_shared_cache() {
    let runtime = SharedReadRuntime::new_with_maintenance(
        (),
        RegionCache::with_ttl(
            Loader {
                location: Some(location()),
                stores: [Some(StoreMetadata {
                    id: 101,
                    address: "tikv-new".to_owned(),
                    labels: vec![("zone".to_owned(), "z2".to_owned())],
                })]
                .into(),
            },
            10,
            0,
        ),
    )
    .unwrap();
    let background = runtime.region_cache_handle();
    let outcome = runtime
        .with_region_cache(|cache| {
            let region = cache.locate_key_at(b"a", 0).unwrap().region;
            let mut selector = cache
                .request_selector(region, ReadPolicy::default())
                .unwrap();
            let RequestSelection::Attempt(selected) = cache.select_request(&mut selector).unwrap()
            else {
                panic!("fresh region must select its leader");
            };
            let observation = cache.observe_attempt(selected.dispatch_attempt()).unwrap();
            cache
                .on_route_send_failure_observed(&selected, &observation, StoreLiveness::Unreachable)
                .unwrap()
        })
        .unwrap();
    assert!(matches!(outcome, StoreFailureOutcome::Invalidated { .. }));
    assert!(runtime.trigger_store_check().unwrap());
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(background.completed_rounds().unwrap(), 1);
    runtime
        .with_region_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store.address(), "tikv-new");
            assert_eq!(store.resolve_state(), StoreResolveState::Resolved);
            assert_eq!(cache.store_label(101, "zone"), Some("z2"));
        })
        .unwrap();
    runtime.shutdown().unwrap();
}
