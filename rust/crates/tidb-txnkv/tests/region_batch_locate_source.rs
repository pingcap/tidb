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

//! Direct transition of client-go batch merge, range splitting, limits, and
//! coverage checks.

use std::sync::{Arc, Mutex};

use tidb_txnkv::region::{
    merge_loaded_and_cached, ranges_after_key, regions_have_gap, BatchRegionLoader, KeyRange,
    RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation, RegionVerId,
    DEFAULT_REGIONS_PER_BATCH, MAX_RANGES_PER_BATCH,
};

fn region(id: u64, start: &str, end: &str) -> RegionLocation {
    RegionLocation {
        region: RegionVerId {
            id,
            epoch: RegionEpoch {
                conf_ver: 1,
                version: 1,
            },
        },
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
        peers: Vec::new(),
        leader_peer_id: None,
        stores: Vec::new(),
        buckets: None,
        down_peer_ids: Vec::new(),
        pending_peer_ids: Vec::new(),
    }
}

fn ranges(keys: &[(&str, &str)]) -> Vec<KeyRange> {
    keys.iter()
        .map(|(start, end)| KeyRange::new(start.as_bytes(), end.as_bytes()))
        .collect()
}

#[test]
fn split_ranges_ports_original_boundary_table() {
    let cases = [
        (("a", "c"), "a", vec![("a", "c")]),
        (("a", "c"), "b", vec![("b", "c")]),
        (("a", "c"), "c", vec![]),
        (("a", ""), "b", vec![("b", "")]),
    ];
    for ((start, end), split, expected) in cases {
        assert_eq!(
            ranges_after_key(&ranges(&[(start, end)]), split.as_bytes()),
            ranges(&expected)
        );
    }
    assert_eq!(
        ranges_after_key(&ranges(&[("a", "b"), ("c", "f")]), b"b"),
        ranges(&[("c", "f")])
    );
    assert_eq!(MAX_RANGES_PER_BATCH, 16 * DEFAULT_REGIONS_PER_BATCH);
    assert_eq!(DEFAULT_REGIONS_PER_BATCH, 128);
}

#[test]
fn merger_preserves_partial_overlap_but_drops_fully_covered_cache() {
    let cached = vec![region(1, "a", "b"), region(2, "c", "e")];
    let loaded = vec![region(3, "b", "d"), region(4, "d", "f")];
    let merged = merge_loaded_and_cached(&cached, &loaded);
    let bounds: Vec<_> = merged
        .iter()
        .map(|region| (region.start_key.clone(), region.end_key.clone()))
        .collect();
    assert_eq!(
        bounds,
        vec![
            (b"a".to_vec(), b"b".to_vec()),
            (b"b".to_vec(), b"d".to_vec()),
            (b"d".to_vec(), b"f".to_vec()),
        ]
    );
}

#[test]
fn coverage_distinguishes_holes_from_a_reached_limit() {
    let requested = ranges(&[("a", "c")]);
    let partial = vec![region(1, "a", "b")];
    assert!(regions_have_gap(&requested, &partial, 0));
    assert!(!regions_have_gap(&requested, &partial, 1));
    assert!(!regions_have_gap(
        &requested,
        &[region(1, "a", "b"), region(2, "b", "c")],
        0
    ));
}

struct BatchLoader {
    calls: Arc<Mutex<Vec<(usize, usize, bool)>>>,
}

impl RegionLoader for BatchLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-unary",
            "batch path required",
        ))
    }
}

impl BatchRegionLoader for BatchLoader {
    fn batch_load_regions(
        &mut self,
        requested: &[KeyRange],
        limit: usize,
        need_buckets: bool,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((requested.len(), limit, need_buckets));
        let call = calls.len();
        drop(calls);
        Ok(requested
            .iter()
            .enumerate()
            .map(|(index, range)| {
                region(
                    (call * 10_000 + index) as u64,
                    std::str::from_utf8(&range.start).unwrap(),
                    std::str::from_utf8(&range.end).unwrap(),
                )
            })
            .collect())
    }
}

#[test]
fn batch_cache_uses_exact_pd_limit_and_need_bucket_flag() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let loader = BatchLoader {
        calls: Arc::clone(&calls),
    };
    let mut cache = RegionCache::with_ttl(loader, 600, 0);
    let requested = ranges(&[("a", "b"), ("c", "d")]);
    let loaded = cache.batch_locate_key_ranges(&requested, true).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(*calls.lock().unwrap(), vec![(2, 128, true)]);
    // A second lookup is served entirely by the canonical cache.
    assert_eq!(
        cache
            .batch_locate_key_ranges(&requested, true)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(*calls.lock().unwrap(), vec![(2, 128, true)]);
}
