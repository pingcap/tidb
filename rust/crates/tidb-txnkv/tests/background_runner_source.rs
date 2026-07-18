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
use tidb_txnkv::SharedReadRuntime;

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
    assert!(background.last_round().unwrap().unwrap().triggered);

    background.shutdown().unwrap();
    assert!(background.is_closed().unwrap());
    assert!(!background.trigger_store_check().unwrap());
}

#[test]
fn shared_runtime_clones_one_maintained_cache_and_joins_once() {
    let runtime = SharedReadRuntime::new_with_maintenance((), RegionCache::new(Loader)).unwrap();
    let clone = runtime.clone();
    let background = runtime.region_cache_handle();
    let runtime_cache = runtime
        .with_region_cache(|cache| std::ptr::from_mut(cache).addr())
        .unwrap();
    let background_cache = background
        .with_cache(|cache| std::ptr::from_mut(cache).addr())
        .unwrap();
    assert_eq!(runtime_cache, background_cache);

    clone.shutdown().unwrap();
    assert!(background.is_closed().unwrap());
    runtime.shutdown().unwrap();
}
