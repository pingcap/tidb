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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tidb_txnkv::region::{
    merge_loaded_and_cached, ranges_after_key, regions_have_gap, BatchLoadOptions,
    BatchRegionLoader, BatchScanBackoff, BatchScanRetryReason, KeyRange, RegionCache, RegionEpoch,
    RegionLoadError, RegionLoader, RegionLocation, RegionRouteError, RegionVerId,
    DEFAULT_REGIONS_PER_BATCH, MAX_RANGES_PER_BATCH,
};

#[derive(Default)]
struct NoRetry;

impl BatchScanBackoff for NoRetry {
    fn backoff(&mut self, reason: BatchScanRetryReason) -> Result<(), RegionRouteError> {
        Err(RegionRouteError::Loader(RegionLoadError::new(
            "batch-backoff-exhausted",
            format!("retry budget exhausted after {reason:?}"),
        )))
    }
}

#[derive(Default)]
struct ScriptedBackoff {
    remaining: usize,
    reasons: Vec<BatchScanRetryReason>,
}

impl BatchScanBackoff for ScriptedBackoff {
    fn backoff(&mut self, reason: BatchScanRetryReason) -> Result<(), RegionRouteError> {
        self.reasons.push(reason);
        if self.remaining == 0 {
            return NoRetry.backoff(reason);
        }
        self.remaining -= 1;
        Ok(())
    }
}

fn options(need_buckets: bool) -> BatchLoadOptions {
    BatchLoadOptions {
        need_buckets,
        need_leader: false,
    }
}

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

fn leaderful_region(id: u64, start: &str, end: &str) -> RegionLocation {
    let mut location = region(id, start, end);
    location.leader_peer_id = Some(id * 10);
    location
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
    assert!(regions_have_gap(&requested, &[region(2, "x", "y")], 1));
    assert!(!regions_have_gap(
        &requested,
        &[region(1, "a", "b"), region(2, "b", "c")],
        0
    ));
}

type BatchLoadCall = (usize, usize, bool, bool);
type SharedBatchLoadCalls = Arc<Mutex<Vec<BatchLoadCall>>>;

struct BatchLoader {
    calls: SharedBatchLoadCalls,
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
        options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((
            requested.len(),
            limit,
            options.need_buckets,
            options.need_leader,
        ));
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
    let loaded = cache
        .batch_locate_key_ranges(&requested, options(true), &mut NoRetry)
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(*calls.lock().unwrap(), vec![(2, 128, true, false)]);
    // A second lookup is served entirely by the canonical cache.
    assert_eq!(
        cache
            .batch_locate_key_ranges(&requested, options(true), &mut NoRetry)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(*calls.lock().unwrap(), vec![(2, 128, true, false)]);
}

type BatchCall = (Vec<KeyRange>, usize, BatchLoadOptions);

struct OrderedLoader {
    regions: Vec<RegionLocation>,
    calls: Arc<Mutex<Vec<BatchCall>>>,
}

impl RegionLoader for OrderedLoader {
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

impl BatchRegionLoader for OrderedLoader {
    fn batch_load_regions(
        &mut self,
        requested: &[KeyRange],
        limit: usize,
        options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        self.calls
            .lock()
            .unwrap()
            .push((requested.to_vec(), limit, options));
        Ok(self
            .regions
            .iter()
            .filter(|region| requested.iter().any(|range| intersects(region, range)))
            .take(limit)
            .cloned()
            .collect())
    }
}

#[test]
fn batch_cache_progresses_after_the_exact_128_region_pd_limit() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let all_regions = (0..130)
        .map(|index| binary_region(index + 1, key(index), key(index + 1)))
        .collect();
    let mut cache = RegionCache::with_ttl(
        OrderedLoader {
            regions: all_regions,
            calls: Arc::clone(&calls),
        },
        600,
        0,
    );
    let requested = [KeyRange::new(key(0), key(130))];

    let located = cache
        .batch_locate_key_ranges_at(&requested, options(false), &mut NoRetry, 100)
        .unwrap();
    assert_eq!(located.len(), 130);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, requested);
    assert_eq!(calls[0].1, DEFAULT_REGIONS_PER_BATCH);
    assert_eq!(calls[1].0, [KeyRange::new(key(128), key(130))]);
    assert_eq!(calls[1].1, DEFAULT_REGIONS_PER_BATCH);
}

#[test]
fn batch_cache_caps_each_pd_request_at_2048_ranges() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let loader = BatchLoader {
        calls: Arc::clone(&calls),
    };
    let mut cache = RegionCache::with_ttl(loader, 600, 0);
    let requested = (0..(MAX_RANGES_PER_BATCH + 2))
        .map(|index| {
            KeyRange::new(
                format!("{index:05}a").into_bytes(),
                format!("{index:05}b").into_bytes(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        cache
            .batch_locate_key_ranges_at(&requested, options(false), &mut NoRetry, 100)
            .unwrap()
            .len(),
        requested.len()
    );
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            (
                MAX_RANGES_PER_BATCH,
                DEFAULT_REGIONS_PER_BATCH,
                false,
                false
            ),
            (2, DEFAULT_REGIONS_PER_BATCH, false, false),
        ]
    );
}

struct ReplyLoader {
    replies: VecDeque<Vec<RegionLocation>>,
    calls: Arc<Mutex<usize>>,
}

impl RegionLoader for ReplyLoader {
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

impl BatchRegionLoader for ReplyLoader {
    fn batch_load_regions(
        &mut self,
        _requested: &[KeyRange],
        _limit: usize,
        _options: BatchLoadOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        *self.calls.lock().unwrap() += 1;
        self.replies.pop_front().ok_or_else(|| {
            RegionLoadError::new("missing-scripted-batch", "test loader was exhausted")
        })
    }
}

#[test]
fn terminal_gap_reply_does_not_pollute_the_cache() {
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            replies: VecDeque::from([vec![region(1, "a", "b"), region(2, "c", "d")]]),
            calls: Arc::new(Mutex::new(0)),
        },
        600,
        0,
    );
    let error = cache
        .batch_locate_key_ranges_at(&ranges(&[("a", "d")]), options(false), &mut NoRetry, 100)
        .unwrap_err();
    assert!(matches!(error, RegionRouteError::Loader(_)));
    assert!(cache.is_empty());
}

#[test]
fn cached_overlap_cannot_mask_an_interior_gap_in_the_exact_pd_reply() {
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            replies: VecDeque::from([
                vec![region(1, "b", "c")],
                vec![region(2, "a", "b"), region(3, "c", "d")],
            ]),
            calls: Arc::new(Mutex::new(0)),
        },
        600,
        0,
    );
    cache
        .batch_locate_key_ranges_at(&ranges(&[("b", "c")]), options(false), &mut NoRetry, 100)
        .unwrap();

    assert!(matches!(
        cache
            .batch_locate_key_ranges_at(&ranges(&[("a", "d")]), options(false), &mut NoRetry, 100,),
        Err(RegionRouteError::Loader(_))
    ));
    assert_eq!(cache.locate_key_at(b"b", 100).unwrap().region.id, 1);
}

#[test]
fn gap_reply_retries_then_publishes_only_the_converged_reply() {
    let calls = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            replies: VecDeque::from([
                vec![region(1, "a", "b"), region(2, "c", "d")],
                vec![region(3, "a", "b"), region(4, "b", "d")],
            ]),
            calls: Arc::clone(&calls),
        },
        600,
        0,
    );
    let mut backoff = ScriptedBackoff {
        remaining: 1,
        reasons: Vec::new(),
    };
    let located = cache
        .batch_locate_key_ranges_at(&ranges(&[("a", "d")]), options(false), &mut backoff, 100)
        .unwrap();
    assert_eq!(
        located
            .iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert_eq!(backoff.reasons, [BatchScanRetryReason::CoverageGap]);
    assert_eq!(*calls.lock().unwrap(), 2);
}

#[test]
fn overlapping_cached_and_loaded_regions_progress_to_complete_coverage() {
    let calls = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            replies: VecDeque::from([
                vec![region(1, "a", "c")],
                vec![region(2, "b", "d")],
                vec![region(3, "d", "e")],
            ]),
            calls: Arc::clone(&calls),
        },
        600,
        0,
    );
    assert_eq!(
        cache
            .batch_locate_key_ranges_at(&ranges(&[("a", "c")]), options(false), &mut NoRetry, 100,)
            .unwrap()
            .len(),
        1
    );

    let located = cache
        .batch_locate_key_ranges_at(&ranges(&[("b", "e")]), options(false), &mut NoRetry, 100)
        .unwrap();
    assert_eq!(
        located
            .iter()
            .map(|region| (region.start_key.clone(), region.end_key.clone()))
            .collect::<Vec<_>>(),
        vec![
            (b"b".to_vec(), b"d".to_vec()),
            (b"d".to_vec(), b"e".to_vec()),
        ]
    );
    assert_eq!(*calls.lock().unwrap(), 3);
}

#[test]
fn live_batch_merger_preserves_a_partial_cached_bridge_for_this_result() {
    let calls = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            replies: VecDeque::from([
                vec![region(1, "e", "f")],
                vec![region(2, "d2", "e1"), region(3, "f", "g")],
            ]),
            calls: Arc::clone(&calls),
        },
        600,
        0,
    );
    cache
        .batch_locate_key_ranges_at(&ranges(&[("e", "f")]), options(false), &mut NoRetry, 100)
        .unwrap();

    let located = cache
        .batch_locate_key_ranges_at(
            &ranges(&[("d2", "e1"), ("f", "g")]),
            options(false),
            &mut NoRetry,
            100,
        )
        .unwrap();
    assert_eq!(
        located
            .iter()
            .map(|region| (region.start_key.clone(), region.end_key.clone()))
            .collect::<Vec<_>>(),
        vec![
            (b"d2".to_vec(), b"e1".to_vec()),
            (b"e".to_vec(), b"f".to_vec()),
            (b"f".to_vec(), b"g".to_vec()),
        ]
    );
    assert_eq!(*calls.lock().unwrap(), 2);
}

#[test]
fn leader_required_batch_retries_all_leaderless_but_accepts_mixed_metadata() {
    let calls = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            replies: VecDeque::from([
                vec![region(1, "a", "c")],
                vec![leaderful_region(2, "a", "b"), region(3, "b", "c")],
                vec![leaderful_region(4, "b", "c")],
            ]),
            calls: Arc::clone(&calls),
        },
        600,
        0,
    );
    let mut backoff = ScriptedBackoff {
        remaining: 1,
        reasons: Vec::new(),
    };
    let located = cache
        .batch_locate_key_ranges_at(
            &ranges(&[("a", "c")]),
            BatchLoadOptions {
                need_buckets: true,
                need_leader: true,
            },
            &mut backoff,
            100,
        )
        .unwrap();
    assert_eq!(
        located
            .iter()
            .map(|region| region.region.id)
            .collect::<Vec<_>>(),
        [2, 4]
    );
    assert_eq!(backoff.reasons, [BatchScanRetryReason::MissingLeader]);
    assert_eq!(*calls.lock().unwrap(), 3);
}

#[test]
fn merger_ports_the_original_overlap_and_hole_table() {
    let cases = [
        (
            vec![("b", "c"), ("c", "d")],
            vec![("a", "b")],
            vec![("a", "b"), ("b", "c"), ("c", "d")],
        ),
        (
            vec![("a", "b"), ("c", "d")],
            vec![("b", "c")],
            vec![("a", "b"), ("b", "c"), ("c", "d")],
        ),
        (vec![("", "")], vec![("a", "b"), ("b", "c")], vec![("", "")]),
        (
            vec![("b", "")],
            vec![("a", "b"), ("c", "d")],
            vec![("a", "b"), ("b", "")],
        ),
        (
            vec![("b", "e")],
            vec![("a", "b"), ("c", "d")],
            vec![("a", "b"), ("b", "e")],
        ),
        (
            vec![("b", "d")],
            vec![("a", "b"), ("c", "e")],
            vec![("a", "b"), ("b", "d"), ("c", "e")],
        ),
        (
            vec![("b", "d"), ("d", "e"), ("f", "h")],
            vec![("a", "b"), ("c", "g")],
            vec![("a", "b"), ("b", "d"), ("d", "e"), ("c", "g"), ("f", "h")],
        ),
    ];
    for (fresh, cached, expected) in cases {
        let expected = expected
            .into_iter()
            .map(|(start, end)| (start.to_owned(), end.to_owned()))
            .collect::<Vec<_>>();
        let fresh = fresh
            .into_iter()
            .enumerate()
            .map(|(index, (start, end))| region(10_000 + index as u64, start, end))
            .collect::<Vec<_>>();
        let cached = cached
            .into_iter()
            .enumerate()
            .map(|(index, (start, end))| region(20_000 + index as u64, start, end))
            .collect::<Vec<_>>();
        let actual = merge_loaded_and_cached(&cached, &fresh)
            .into_iter()
            .map(|region| {
                (
                    String::from_utf8(region.start_key).unwrap(),
                    String::from_utf8(region.end_key).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[test]
fn cache_accepts_loader_owned_legacy_scan_fallback_results() {
    let calls = Arc::new(Mutex::new(0));
    let mut cache = RegionCache::with_ttl(
        ReplyLoader {
            // The BatchRegionLoader boundary owns the exact-Unimplemented
            // fallback; this reply is the ordered legacy ScanRegions result.
            replies: VecDeque::from([vec![region(1, "a", "b"), region(2, "b", "d")]]),
            calls: Arc::clone(&calls),
        },
        600,
        0,
    );
    assert_eq!(
        cache
            .batch_locate_key_ranges_at(&ranges(&[("a", "d")]), options(true), &mut NoRetry, 100,)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(*calls.lock().unwrap(), 1);
}

fn key(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn binary_region(id: u64, start: Vec<u8>, end: Vec<u8>) -> RegionLocation {
    let mut location = region(id, "", "");
    location.start_key = start;
    location.end_key = end;
    location
}

fn intersects(region: &RegionLocation, range: &KeyRange) -> bool {
    let region_before = !region.end_key.is_empty() && region.end_key <= range.start;
    let range_before = !range.end.is_empty() && range.end <= region.start_key;
    !region_before && !range_before
}
