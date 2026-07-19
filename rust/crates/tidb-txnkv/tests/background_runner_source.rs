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

//! Source-shaped lifecycle proof for one triggerable maintenance task.

use std::time::Duration;

use tidb_txnkv::region::{
    BackgroundRegionCache, KeyRange, RegionCache, RegionLoadError, RegionLoader, RegionLocation,
    RegionQuery, RegionQueryLoader, RegionQueryOptions, StoreMetadata,
};
use tidb_txnkv::SharedReadAuthority;

struct Loader;

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new("unexpected-load", "load not expected"))
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
        Err(RegionLoadError::new(
            "unexpected-store",
            "store not expected",
        ))
    }
}

#[test]
fn trigger_wakes_the_single_driver_and_shutdown_waits_for_close() {
    let background =
        BackgroundRegionCache::start(RegionCache::new(Loader), Duration::from_secs(3600), 50)
            .unwrap();
    assert!(background.trigger_store_check().unwrap());
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(background.completed_rounds().unwrap(), 1);
    let round = background.last_round().unwrap().unwrap();
    assert!(round.triggered);
    assert_eq!(round.stores.attempted, 0);
    assert_eq!(round.stores.stale_discarded, 0);

    background.shutdown().unwrap();
}

#[test]
fn sessions_share_one_maintained_cache_but_cannot_stop_its_authority() {
    let authority = SharedReadAuthority::start((), RegionCache::new(Loader)).unwrap();
    let opener = authority.opener();
    let first = authority.open_session().unwrap();
    let second = authority.open_session().unwrap();
    let background = first.region_cache_handle();
    let runtime_cache = first
        .with_region_cache(|cache| std::ptr::from_mut(cache).addr())
        .unwrap();
    let background_cache = background
        .with_cache(|cache| std::ptr::from_mut(cache).addr())
        .unwrap();
    assert_eq!(runtime_cache, background_cache);

    drop(first);
    drop(second);
    assert!(!background.is_closed().unwrap());
    let completed = background.completed_rounds().unwrap();
    assert!(background.trigger_store_check().unwrap());
    for _ in 0..100 {
        if background.completed_rounds().unwrap() > completed {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(background.completed_rounds().unwrap() > completed);

    drop(background);
    authority.shutdown().unwrap();
    assert_eq!(
        opener.open_session().err(),
        Some(tidb_txnkv::region::BackgroundRegionCacheError::LeaseAdmissionClosed)
    );
}

#[test]
fn active_cache_lease_rejects_shutdown_without_stopping_the_worker() {
    let owner =
        BackgroundRegionCache::start(RegionCache::new(Loader), Duration::from_secs(3600), 50)
            .unwrap();
    let lease = owner.handle().unwrap();

    assert_eq!(
        owner.shutdown(),
        Err(tidb_txnkv::region::BackgroundRegionCacheError::SharedOwners { owners: 1 })
    );
    assert!(!owner.is_closed().unwrap());
    assert!(lease.trigger_store_check().unwrap());

    drop(lease);
    owner.shutdown().unwrap();
    assert!(owner.is_closed().unwrap());
}

#[test]
fn poisoned_cache_is_returned_by_the_fallible_worker_join() {
    let owner =
        BackgroundRegionCache::start(RegionCache::new(Loader), Duration::from_secs(3600), 50)
            .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.with_cache::<()>(|_| panic!("poison canonical cache"));
    }));
    assert!(panic.is_err());
    assert!(owner.trigger_store_check().unwrap());

    for _ in 0..100 {
        if owner.is_closed().unwrap() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(owner.is_closed().unwrap());
    assert_eq!(
        owner.shutdown(),
        Err(tidb_txnkv::region::BackgroundRegionCacheError::CachePoisoned)
    );
}
