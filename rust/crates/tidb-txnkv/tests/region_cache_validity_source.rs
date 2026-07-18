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

//! Direct transition of client-go's publishing and nonpublishing region-ID
//! lookup boundaries.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tidb_txnkv::region::{
    KeyRange, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionQuery,
    RegionQueryLoader, RegionQueryOptions, RegionRouteError, RegionVerId, StoreMetadata,
};

struct Loader {
    replies: VecDeque<RegionLocation>,
    ids: Arc<Mutex<Vec<u64>>>,
}

impl RegionLoader for Loader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-key",
            "region-ID test requires an ID query",
        ))
    }
}

impl RegionQueryLoader for Loader {
    fn query_region(
        &mut self,
        query: RegionQuery<'_>,
        _options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        let RegionQuery::Id(id) = query else {
            return Err(RegionLoadError::new(
                "unexpected-query",
                "region-ID test requires an ID query",
            ));
        };
        self.ids.lock().unwrap().push(id);
        self.replies
            .pop_front()
            .ok_or_else(|| RegionLoadError::new("missing-reply", "ID script exhausted"))
    }

    fn scan_regions_once(
        &mut self,
        _range: &KeyRange,
        _limit: usize,
        _options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-scan",
            "region-ID test does not scan",
        ))
    }

    fn load_store(&mut self, _store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-store",
            "region-ID test does not resolve stores",
        ))
    }
}

fn region(id: u64) -> RegionLocation {
    RegionLocation {
        region: RegionVerId::new(id, 1, 1),
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        ..RegionLocation::default()
    }
}

#[test]
fn direct_by_id_does_not_publish_but_cache_lookup_does() {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let mut cache = RegionCache::with_ttl(
        Loader {
            replies: vec![region(7), region(7)].into(),
            ids: Arc::clone(&ids),
        },
        600,
        0,
    );

    assert_eq!(
        cache.locate_region_by_id_from_source(7).unwrap().region.id,
        7
    );
    assert!(cache.is_empty());

    assert_eq!(cache.locate_region_by_id_at(7, 10).unwrap().region.id, 7);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.locate_region_by_id_at(7, 11).unwrap().region.id, 7);
    assert_eq!(*ids.lock().unwrap(), [7, 7]);
}

#[test]
fn mismatched_by_id_reply_is_rejected_without_publication() {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let mut cache = RegionCache::new(Loader {
        replies: vec![region(8)].into(),
        ids,
    });
    let error = cache.locate_region_by_id_at(7, 10).unwrap_err();
    assert!(matches!(
        error,
        RegionRouteError::Loader(ref error) if error.identity() == "region-id-mismatch"
    ));
    assert!(cache.is_empty());
}
