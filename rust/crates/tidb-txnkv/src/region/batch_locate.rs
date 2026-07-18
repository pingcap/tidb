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

//! Source-shaped cached traversal and batch-scan merge rules.

use std::collections::BTreeMap;

use super::{KeyRange, RegionLocation, RegionVerId};

/// Pinned client-go batch region limit.
pub const DEFAULT_REGIONS_PER_BATCH: usize = 128;
/// Pinned client-go maximum of `16 * defaultRegionsPerBatch` input ranges.
pub const MAX_RANGES_PER_BATCH: usize = 16 * DEFAULT_REGIONS_PER_BATCH;

/// Returns the remaining ordered ranges strictly after `split_key`.
#[must_use]
pub fn ranges_after_key(ranges: &[KeyRange], split_key: &[u8]) -> Vec<KeyRange> {
    if ranges.is_empty() || split_key.is_empty() {
        return Vec::new();
    }
    let last = ranges.last().expect("nonempty ranges were validated above");
    if !last.end.is_empty() && split_key >= last.end.as_slice() {
        return Vec::new();
    }
    let index =
        ranges.partition_point(|range| !range.end.is_empty() && range.end.as_slice() <= split_key);
    let mut remaining = ranges[index..].to_vec();
    if let Some(first) = remaining.first_mut() {
        if split_key > first.start.as_slice() {
            first.start = split_key.to_vec();
        }
    }
    remaining
}

/// Merges freshly loaded regions with ordered cached regions. A loaded region
/// replaces every fully covered cache entry, while partial overlaps remain
/// visible for source-compatible retry/error handling.
#[must_use]
pub fn merge_loaded_and_cached(
    cached: &[RegionLocation],
    loaded: &[RegionLocation],
) -> Vec<RegionLocation> {
    let mut merged = Vec::with_capacity(cached.len() + loaded.len());
    let mut cached_index = 0;
    let mut last_loaded_end: Option<&[u8]> = None;

    for fresh in loaded {
        if fresh.start_key.is_empty()
            || last_loaded_end.is_some_and(|end| end >= fresh.start_key.as_slice())
        {
            merged.push(fresh.clone());
        } else {
            while let Some(current) = cached.get(cached_index) {
                if last_loaded_end.is_some_and(|end| {
                    current.end_key.is_empty() || end >= current.end_key.as_slice()
                }) {
                    cached_index += 1;
                    continue;
                }
                if current.start_key >= fresh.start_key {
                    break;
                }
                merged.push(current.clone());
                cached_index += 1;
            }
            merged.push(fresh.clone());
        }
        if fresh.end_key.is_empty() {
            cached_index = cached.len();
            last_loaded_end = None;
        } else {
            last_loaded_end = Some(&fresh.end_key);
        }
    }
    while let Some(current) = cached.get(cached_index) {
        if last_loaded_end
            .is_none_or(|end| !current.end_key.is_empty() && end < current.end_key.as_slice())
        {
            merged.push(current.clone());
        }
        cached_index += 1;
    }
    merged
}

/// Whether returned regions leave a keyspace gap in any requested range.
/// Reaching `limit` makes a missing suffix inconclusive, matching client-go.
#[must_use]
pub fn regions_have_gap(ranges: &[KeyRange], regions: &[RegionLocation], limit: usize) -> bool {
    if ranges.is_empty() {
        return false;
    }
    if regions.is_empty() {
        return true;
    }
    for range in ranges {
        let mut cursor = range.start.as_slice();
        let mut covered = false;
        let mut reached_returned_region = false;
        for region in regions {
            if !region.end_key.is_empty() && region.end_key.as_slice() <= cursor {
                continue;
            }
            if region.start_key.as_slice() > cursor {
                return true;
            }
            reached_returned_region = true;
            if range.end.is_empty() {
                if region.end_key.is_empty() {
                    covered = true;
                    break;
                }
                cursor = &region.end_key;
            } else if region.end_key.is_empty() || region.end_key >= range.end {
                covered = true;
                break;
            } else {
                cursor = &region.end_key;
            }
        }
        // Reaching the response limit makes only a suffix after contiguous
        // returned coverage inconclusive. It never excuses a missing prefix
        // or interior span.
        if !covered && (!reached_returned_region || limit == 0 || regions.len() < limit) {
            return true;
        }
    }
    false
}

/// Sorts and de-duplicates exact region identities selected by caller ranges.
#[must_use]
pub fn regions_intersecting_ranges(
    regions: &[RegionLocation],
    ranges: &[KeyRange],
) -> Vec<RegionLocation> {
    let mut selected = BTreeMap::<RegionVerId, RegionLocation>::new();
    for region in regions {
        if ranges.iter().any(|range| intersects(region, range)) {
            selected
                .entry(region.region)
                .or_insert_with(|| region.clone());
        }
    }
    let mut selected: Vec<_> = selected.into_values().collect();
    selected.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    selected
}

fn intersects(region: &RegionLocation, range: &KeyRange) -> bool {
    let region_before = !region.end_key.is_empty() && region.end_key <= range.start;
    let range_before = !range.end.is_empty() && range.end <= region.start_key;
    !region_before && !range_before
}
