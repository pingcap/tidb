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

//! Blocking-loader proof for optimistic foreground region publication.

use std::collections::VecDeque;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use tidb_proto::{errorpb, metapb};
use tidb_txnkv::region::{
    BackgroundRegionCache, KeyRange, Peer, PeerRole, RegionAttempt, RegionBackoffBudget,
    RegionCache, RegionErrorDisposition, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};

struct BlockingLoader {
    regions: VecDeque<RegionLocation>,
    calls: usize,
    block_at: usize,
    started: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl RegionLoader for BlockingLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.calls += 1;
        let region = self
            .regions
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-region", "region script exhausted"))?;
        if self.calls == self.block_at {
            self.started.send(()).unwrap();
            let (released, wake) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
        Ok(region)
    }
}

impl RegionRecoveryLoader for BlockingLoader {
    fn hydrate_region(
        &mut self,
        _metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-hydration",
            "hydration not expected",
        ))
    }
}

fn location(leader_peer_id: u64) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(7, 1, 1),
        start_key: Vec::new(),
        end_key: Vec::new(),
        peers: vec![
            Peer {
                id: 11,
                store_id: 101,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 7,
            },
            Peer {
                id: 12,
                store_id: 102,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 8,
            },
        ],
        leader_peer_id: Some(leader_peer_id),
        stores: vec![
            Store {
                id: 101,
                address: "tikv-101".to_owned(),
                epoch: 7,
            },
            Store {
                id: 102,
                address: "tikv-102".to_owned(),
                epoch: 8,
            },
        ],
        ..RegionLocation::default()
    }
}

#[test]
fn blocked_region_load_allows_foreground_update_and_cannot_overwrite_it() {
    let initial = location(11);
    let stale = initial.clone();
    let current = location(12);
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut cache = RegionCache::new(BlockingLoader {
        regions: [initial.clone(), stale, current.clone()].into(),
        calls: 0,
        block_at: 2,
        started: started_tx,
        release: Arc::clone(&release),
    });
    let region = cache.locate_key(b"k").unwrap().region;
    assert!(cache.mark_reload_on_access(region));
    let background = BackgroundRegionCache::start_gc(cache, Duration::from_secs(3600), 50).unwrap();

    let lookup = background.clone();
    let lookup_thread = thread::spawn(move || {
        lookup
            .locate_ranges(&[KeyRange::new(b"k".to_vec(), Vec::new())])
            .unwrap()
            .unwrap()
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let foreground = background.clone();
    let (access_tx, access_rx) = mpsc::channel();
    let foreground_thread = thread::spawn(move || {
        let result = foreground
            .with_cache(|cache| {
                cache.on_region_error(
                    &errorpb::Error {
                        not_leader: Some(errorpb::NotLeader {
                            region_id: 7,
                            leader: Some(metapb::Peer {
                                id: 12,
                                store_id: 102,
                                role: 0,
                                is_witness: false,
                            }),
                        }),
                        ..errorpb::Error::default()
                    },
                    RegionAttempt {
                        region,
                        peer_id: 11,
                        store_id: 101,
                        address: "tikv-101".to_owned(),
                        store_epoch: 7,
                    },
                    &mut RegionBackoffBudget::campaign_default(),
                )
            })
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
    access.expect("foreground topology update must not wait for blocked PD I/O");
    foreground_thread.join().unwrap();

    let locations = lookup_thread.join().unwrap();
    assert_eq!(locations, vec![current.clone()]);
    assert_eq!(
        background.locate_key(b"k").unwrap().unwrap(),
        current,
        "the stale leader reply must be discarded before current publication"
    );
    background.shutdown().unwrap();
}

struct BlockingHydrationLoader {
    initial: Option<RegionLocation>,
    replacement: Option<RegionLocation>,
    started: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl RegionLoader for BlockingHydrationLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.initial
            .take()
            .ok_or_else(|| RegionLoadError::new("missing-region", "initial region was consumed"))
    }
}

impl RegionRecoveryLoader for BlockingHydrationLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        let replacement = self.replacement.take().ok_or_else(|| {
            RegionLoadError::new("missing-hydration", "replacement region was consumed")
        })?;
        assert_eq!(metadata.region, replacement.region);
        assert_eq!(leader_store_id, 101);
        self.started.send(()).unwrap();
        let (released, wake) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = wake.wait(released).unwrap();
        }
        Ok(replacement)
    }
}

#[test]
fn blocked_epoch_hydration_allows_topology_update_and_discards_stale_replacement() {
    let initial = location(11);
    let mut replacement = location(11);
    replacement.region = RegionVerId::new(8, 2, 2);
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut cache = RegionCache::new(BlockingHydrationLoader {
        initial: Some(initial),
        replacement: Some(replacement),
        started: started_tx,
        release: Arc::clone(&release),
    });
    let region = cache.locate_key(b"k").unwrap().region;
    let background = BackgroundRegionCache::start_gc(cache, Duration::from_secs(3600), 50).unwrap();

    let recovery = background.clone();
    let recovery_thread = thread::spawn(move || {
        recovery
            .on_region_error(
                &errorpb::Error {
                    epoch_not_match: Some(errorpb::EpochNotMatch {
                        current_regions: vec![metapb::Region {
                            id: 8,
                            region_epoch: Some(metapb::RegionEpoch {
                                conf_ver: 2,
                                version: 2,
                            }),
                            peers: vec![
                                metapb::Peer {
                                    id: 11,
                                    store_id: 101,
                                    role: 0,
                                    is_witness: false,
                                },
                                metapb::Peer {
                                    id: 12,
                                    store_id: 102,
                                    role: 0,
                                    is_witness: false,
                                },
                            ],
                            ..metapb::Region::default()
                        }],
                    }),
                    ..errorpb::Error::default()
                },
                RegionAttempt {
                    region,
                    peer_id: 11,
                    store_id: 101,
                    address: "tikv-101".to_owned(),
                    store_epoch: 7,
                },
                &mut RegionBackoffBudget::campaign_default(),
            )
            .unwrap()
            .unwrap()
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let foreground = background.clone();
    let (access_tx, access_rx) = mpsc::channel();
    let foreground_thread = thread::spawn(move || {
        let disposition = foreground
            .with_cache(|cache| {
                cache.on_region_error(
                    &errorpb::Error {
                        not_leader: Some(errorpb::NotLeader {
                            region_id: 7,
                            leader: Some(metapb::Peer {
                                id: 12,
                                store_id: 102,
                                role: 0,
                                is_witness: false,
                            }),
                        }),
                        ..errorpb::Error::default()
                    },
                    RegionAttempt {
                        region,
                        peer_id: 11,
                        store_id: 101,
                        address: "tikv-101".to_owned(),
                        store_epoch: 7,
                    },
                    &mut RegionBackoffBudget::campaign_default(),
                )
            })
            .unwrap()
            .unwrap();
        access_tx.send(disposition).unwrap();
    });
    let access = access_rx.recv_timeout(Duration::from_millis(100));
    {
        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_one();
    }
    access.expect("topology update must not wait for EpochNotMatch store hydration");
    foreground_thread.join().unwrap();

    assert!(matches!(
        recovery_thread.join().unwrap(),
        RegionErrorDisposition::RebuildRanges { .. }
    ));
    let current = background.locate_key(b"k").unwrap().unwrap();
    assert_eq!(current.region, region);
    assert_eq!(current.leader_peer_id, Some(12));
    background.shutdown().unwrap();
}

fn ranged_location(id: u64, version: u64, start: &[u8], end: &[u8]) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, version, 1),
        start_key: start.to_vec(),
        end_key: end.to_vec(),
        peers: vec![Peer {
            id: id * 10 + 1,
            store_id: id * 100 + 1,
            role: PeerRole::Voter,
            is_witness: false,
            store_epoch: version,
        }],
        leader_peer_id: Some(id * 10 + 1),
        stores: vec![Store {
            id: id * 100 + 1,
            address: format!("tikv-{id}-{version}"),
            epoch: version,
        }],
        ..RegionLocation::default()
    }
}

#[test]
fn range_lookup_restarts_after_concurrent_topology_replacement() {
    let old_left = ranged_location(1, 1, b"", b"m");
    let old_right = ranged_location(2, 1, b"m", b"");
    let new_right = ranged_location(2, 2, b"m", b"");
    let new_left = ranged_location(1, 2, b"", b"m");
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut cache = RegionCache::new(BlockingLoader {
        regions: [
            old_left.clone(),
            old_right.clone(),
            new_right.clone(),
            new_left.clone(),
        ]
        .into(),
        calls: 0,
        block_at: 3,
        started: started_tx,
        release: Arc::clone(&release),
    });
    assert_eq!(cache.locate_key(b"b").unwrap().region, old_left.region);
    assert_eq!(cache.locate_key(b"n").unwrap().region, old_right.region);
    assert!(cache.mark_reload_on_access(old_right.region));
    let background = BackgroundRegionCache::start_gc(cache, Duration::from_secs(3600), 50).unwrap();

    let lookup = background.clone();
    let lookup_thread = thread::spawn(move || {
        lookup
            .locate_ranges(&[KeyRange::new(b"b".to_vec(), b"z".to_vec())])
            .unwrap()
            .unwrap()
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    assert!(background
        .with_cache(|cache| cache.invalidate(old_left.region))
        .unwrap());
    {
        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_one();
    }

    assert_eq!(
        lookup_thread.join().unwrap(),
        vec![new_left, new_right],
        "a range lookup must restart instead of returning old-left/new-right"
    );
    background.shutdown().unwrap();
}
