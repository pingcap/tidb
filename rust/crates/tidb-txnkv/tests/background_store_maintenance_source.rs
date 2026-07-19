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

//! Direct transition of unlocked, stale-safe in-place store re-resolution.

use std::collections::VecDeque;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use tidb_txnkv::region::{
    BackgroundRegionCache, KeyRange, Peer, PeerRole, ReadPolicy, RegionAttempt, RegionCache,
    RegionLoadError, RegionLoader, RegionLocation, RegionQuery, RegionQueryLoader,
    RegionQueryOptions, RegionVerId, RequestSelection, Store, StoreFailureOutcome, StoreLiveness,
    StoreLivenessProbe, StoreMetadata, StoreResolveState,
};
use tidb_txnkv::SharedReadAuthority;

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

struct BlockingLoader {
    location: Option<RegionLocation>,
    stores: VecDeque<Option<StoreMetadata>>,
    started: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
    store_loads: usize,
}

struct RestartLoader {
    locations: VecDeque<RegionLocation>,
    stores: VecDeque<Option<StoreMetadata>>,
}

impl RegionLoader for RestartLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.locations
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-region", "region script exhausted"))
    }
}

impl RegionQueryLoader for RestartLoader {
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

struct RestartProbe {
    addresses: Arc<Mutex<Vec<String>>>,
}

impl StoreLivenessProbe for RestartProbe {
    fn probe(&self, address: &str, _timeout: Duration) -> StoreLiveness {
        self.addresses.lock().unwrap().push(address.to_owned());
        StoreLiveness::Reachable
    }
}

struct ConstantProbe(StoreLiveness);

impl StoreLivenessProbe for ConstantProbe {
    fn probe(&self, _address: &str, _timeout: Duration) -> StoreLiveness {
        self.0
    }
}

struct BlockingRestartProbe {
    started: mpsc::Sender<String>,
    release: Arc<(Mutex<bool>, Condvar)>,
    result: StoreLiveness,
}

impl StoreLivenessProbe for BlockingRestartProbe {
    fn probe(&self, address: &str, _timeout: Duration) -> StoreLiveness {
        self.started.send(address.to_owned()).unwrap();
        let (released, wake) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
        self.result
    }
}

impl RegionLoader for BlockingLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.location
            .take()
            .ok_or_else(|| RegionLoadError::new("missing-region", "region script exhausted"))
    }
}

impl RegionQueryLoader for BlockingLoader {
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
        self.store_loads += 1;
        if self.store_loads == 1 {
            self.started.send(()).unwrap();
            let (released, wake) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
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

fn location_at(address: &str) -> RegionLocation {
    let mut location = location();
    location.stores[0].address = address.to_owned();
    location
}

fn release_probe(release: &Arc<(Mutex<bool>, Condvar)>) {
    let (released, wake) = &**release;
    *released.lock().unwrap() = true;
    wake.notify_one();
}

#[test]
fn same_address_restart_becomes_selectable_after_background_liveness_recovers() {
    let location = location();
    let mut cache = RegionCache::with_ttl(
        RestartLoader {
            locations: [location.clone(), location.clone()].into(),
            stores: [Some(StoreMetadata {
                id: 101,
                address: "tikv-old".to_owned(),
                labels: Vec::new(),
            })]
            .into(),
        },
        10,
        0,
    );
    cache.locate_key(b"a").unwrap();
    let attempt = RegionAttempt {
        region: location.region,
        peer_id: 11,
        store_id: 101,
        address: "tikv-old".to_owned(),
        store_epoch: 7,
    };
    let addresses = Arc::new(Mutex::new(Vec::new()));
    let background = BackgroundRegionCache::start_with_liveness(
        cache,
        RestartProbe {
            addresses: Arc::clone(&addresses),
        },
        Duration::from_secs(3600),
        50,
        Duration::from_millis(10),
    )
    .unwrap();

    let outcome = background
        .with_cache(|cache| cache.on_send_failure(&attempt, StoreLiveness::Unreachable))
        .unwrap()
        .unwrap();
    assert!(matches!(outcome, StoreFailureOutcome::Invalidated { .. }));
    assert!(background.trigger_store_check().unwrap());
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    background
        .with_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store.epoch(), 8);
            assert_eq!(store.address(), "tikv-old");
            assert_eq!(store.resolve_state(), StoreResolveState::Resolved);
            assert_eq!(store.liveness(), StoreLiveness::Reachable);
            assert!(cache.invalidate(location.region));
        })
        .unwrap();
    assert_eq!(addresses.lock().unwrap().as_slice(), ["tikv-old"]);

    let reloaded = background.locate_key(b"a").unwrap().unwrap();
    let selected = background
        .with_cache(|cache| {
            let mut selector = cache
                .request_selector(reloaded.region, ReadPolicy::default())
                .unwrap();
            cache.select_request(&mut selector).unwrap()
        })
        .unwrap();
    let RequestSelection::Attempt(selected) = selected else {
        panic!("restarted leader must become selectable after health recovery")
    };
    assert_eq!(selected.attempt.store_id, 101);
    assert_eq!(selected.attempt.store_epoch, 8);

    background.shutdown().unwrap();
}

#[test]
fn delayed_probe_cannot_overwrite_newer_foreground_liveness() {
    let location = location();
    let mut cache = RegionCache::with_ttl(
        RestartLoader {
            locations: [location.clone(), location.clone()].into(),
            stores: [Some(StoreMetadata {
                id: 101,
                address: "tikv-old".to_owned(),
                labels: Vec::new(),
            })]
            .into(),
        },
        10,
        0,
    );
    cache.locate_key(b"a").unwrap();
    let failed = RegionAttempt {
        region: location.region,
        peer_id: 11,
        store_id: 101,
        address: "tikv-old".to_owned(),
        store_epoch: 7,
    };
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let background = BackgroundRegionCache::start_with_liveness(
        cache,
        BlockingRestartProbe {
            started: started_tx,
            release: Arc::clone(&release),
            result: StoreLiveness::Unreachable,
        },
        Duration::from_secs(3600),
        50,
        Duration::from_millis(10),
    )
    .unwrap();
    background
        .with_cache(|cache| cache.on_send_failure(&failed, StoreLiveness::Unreachable))
        .unwrap()
        .unwrap();
    assert!(background.trigger_store_check().unwrap());
    assert_eq!(
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "tikv-old"
    );

    background
        .with_cache(|cache| assert!(cache.invalidate(location.region)))
        .unwrap();
    background.locate_key(b"a").unwrap().unwrap();
    let recovered = RegionAttempt {
        store_epoch: 8,
        ..failed.clone()
    };
    let foreground = background
        .with_cache(|cache| cache.on_send_failure(&recovered, StoreLiveness::Reachable))
        .unwrap()
        .unwrap();
    assert_eq!(foreground, StoreFailureOutcome::Reachable { epoch: 8 });
    release_probe(&release);
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        background
            .last_round()
            .unwrap()
            .unwrap()
            .stores
            .stale_discarded,
        1
    );
    background
        .with_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store.epoch(), 8);
            assert_eq!(store.liveness(), StoreLiveness::Reachable);
        })
        .unwrap();
    background.shutdown().unwrap();
}

#[test]
fn delayed_success_cannot_revive_replaced_store_generation() {
    let old = location_at("tikv-old");
    let new = location_at("tikv-new");
    let mut cache = RegionCache::with_ttl(
        RestartLoader {
            locations: [old.clone(), new].into(),
            stores: [Some(StoreMetadata {
                id: 101,
                address: "tikv-old".to_owned(),
                labels: Vec::new(),
            })]
            .into(),
        },
        10,
        0,
    );
    cache.locate_key(b"a").unwrap();
    let failed = RegionAttempt {
        region: old.region,
        peer_id: 11,
        store_id: 101,
        address: "tikv-old".to_owned(),
        store_epoch: 7,
    };
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let background = BackgroundRegionCache::start_with_liveness(
        cache,
        BlockingRestartProbe {
            started: started_tx,
            release: Arc::clone(&release),
            result: StoreLiveness::Reachable,
        },
        Duration::from_secs(3600),
        50,
        Duration::from_millis(10),
    )
    .unwrap();
    background
        .with_cache(|cache| cache.on_send_failure(&failed, StoreLiveness::Unreachable))
        .unwrap()
        .unwrap();
    assert!(background.trigger_store_check().unwrap());
    assert_eq!(
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "tikv-old"
    );

    background
        .with_cache(|cache| assert!(cache.invalidate(old.region)))
        .unwrap();
    background.locate_key(b"a").unwrap().unwrap();
    release_probe(&release);
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        background
            .last_round()
            .unwrap()
            .unwrap()
            .stores
            .stale_discarded,
        1
    );
    background
        .with_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store.epoch(), 9);
            assert_eq!(store.address(), "tikv-new");
            assert_eq!(store.liveness(), StoreLiveness::Unreachable);
        })
        .unwrap();
    background.shutdown().unwrap();
}

#[test]
fn unknown_probe_does_not_revive_known_unreachable_store() {
    let location = location();
    let mut cache = RegionCache::with_ttl(
        RestartLoader {
            locations: [location.clone()].into(),
            stores: [Some(StoreMetadata {
                id: 101,
                address: "tikv-old".to_owned(),
                labels: Vec::new(),
            })]
            .into(),
        },
        10,
        0,
    );
    cache.locate_key(b"a").unwrap();
    let failed = RegionAttempt {
        region: location.region,
        peer_id: 11,
        store_id: 101,
        address: "tikv-old".to_owned(),
        store_epoch: 7,
    };
    let background = BackgroundRegionCache::start_with_liveness(
        cache,
        ConstantProbe(StoreLiveness::Unknown),
        Duration::from_secs(3600),
        50,
        Duration::from_millis(10),
    )
    .unwrap();
    background
        .with_cache(|cache| cache.on_send_failure(&failed, StoreLiveness::Unreachable))
        .unwrap()
        .unwrap();
    assert!(background.trigger_store_check().unwrap());
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    background
        .with_cache(|cache| {
            assert_eq!(
                cache.store_state(101).unwrap().liveness(),
                StoreLiveness::Unreachable
            );
        })
        .unwrap();
    background.shutdown().unwrap();
}

#[test]
fn zero_liveness_timeout_is_rejected_before_worker_spawn() {
    let result = BackgroundRegionCache::start_with_liveness(
        RegionCache::new(RestartLoader {
            locations: VecDeque::new(),
            stores: VecDeque::new(),
        }),
        ConstantProbe(StoreLiveness::Reachable),
        Duration::from_secs(1),
        50,
        Duration::ZERO,
    );
    assert!(matches!(
        result,
        Err(tidb_txnkv::region::BackgroundRegionCacheError::ZeroLivenessTimeout)
    ));
}

#[test]
fn blocked_store_load_keeps_cache_available_and_discards_stale_publication() {
    let location = location();
    let attempt = RegionAttempt {
        region: location.region,
        peer_id: 11,
        store_id: 101,
        address: "tikv-old".to_owned(),
        store_epoch: 7,
    };
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut cache = RegionCache::with_ttl(
        BlockingLoader {
            location: Some(location.clone()),
            stores: vec![
                Some(StoreMetadata {
                    id: 101,
                    address: "tikv-stale".to_owned(),
                    labels: vec![("zone".to_owned(), "stale".to_owned())],
                }),
                Some(StoreMetadata {
                    id: 101,
                    address: "tikv-current".to_owned(),
                    labels: vec![("zone".to_owned(), "current".to_owned())],
                }),
            ]
            .into(),
            started: started_tx,
            release: Arc::clone(&release),
            store_loads: 0,
        },
        10,
        0,
    );
    cache.locate_key(b"a").unwrap();
    let state_address = cache.store_state(101).unwrap() as *const _ as usize;
    let background = BackgroundRegionCache::start(cache, Duration::from_millis(200), 50).unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let foreground = background.clone();
    let (access_tx, access_rx) = mpsc::channel();
    let foreground_thread = thread::spawn(move || {
        let result = foreground
            .with_cache(|cache| cache.on_send_failure(&attempt, StoreLiveness::Unreachable))
            .unwrap()
            .unwrap();
        access_tx.send(result).unwrap();
    });
    let access = access_rx.recv_timeout(Duration::from_millis(100));
    {
        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_one();
    }
    let outcome = access.expect("foreground cache access must not wait for blocked PD I/O");
    assert!(matches!(outcome, StoreFailureOutcome::Invalidated { .. }));
    foreground_thread.join().unwrap();

    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let stale_round = background.last_round().unwrap().unwrap();
    assert_eq!(stale_round.stores.stale_discarded, 1);
    assert_eq!(stale_round.stores.refreshed, 0);
    background
        .with_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store as *const _ as usize, state_address);
            assert_eq!(store.epoch(), 8);
            assert_eq!(store.address(), "tikv-old");
            assert_eq!(store.resolve_state(), StoreResolveState::NeedCheck);
        })
        .unwrap();

    let completed = background.completed_rounds().unwrap();
    assert!(background.trigger_store_check().unwrap());
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > completed {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let current_round = background.last_round().unwrap().unwrap();
    assert_eq!(current_round.stores.refreshed, 1);
    assert_eq!(current_round.stores.stale_discarded, 0);
    background
        .with_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store as *const _ as usize, state_address);
            assert_eq!(store.epoch(), 9);
            assert_eq!(store.address(), "tikv-current");
            assert_eq!(store.resolve_state(), StoreResolveState::Resolved);
            assert_eq!(store.liveness(), StoreLiveness::Unreachable);
            assert_eq!(cache.store_label(101, "zone"), Some("current"));
        })
        .unwrap();
    background.shutdown().unwrap();
}

#[test]
fn periodic_tombstone_refresh_expires_dependent_regions_without_replacing_store() {
    let mut cache = RegionCache::with_ttl(
        Loader {
            location: Some(location()),
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
    let background = BackgroundRegionCache::start(cache, Duration::from_millis(1), 50).unwrap();
    for _ in 0..100 {
        if background.completed_rounds().unwrap() >= 2 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(background.completed_rounds().unwrap() >= 2);
    background
        .with_cache(|cache| {
            let store = cache.store_state(101).unwrap();
            assert_eq!(store as *const _ as usize, state_address);
            assert_eq!(store.resolve_state(), StoreResolveState::Removed);
            assert_eq!(cache.maintain_entries_at(u64::MAX), 0);
            assert!(cache.is_empty());
        })
        .unwrap();
    background.shutdown().unwrap();
}

#[test]
fn production_failure_triggers_in_place_maintenance_on_the_shared_cache() {
    let authority = SharedReadAuthority::start(
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
    let runtime = authority.open_session().unwrap();
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
    drop(background);
    drop(runtime);
    authority.shutdown().unwrap();
}
