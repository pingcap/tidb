// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Filling the cache in bulk: the ordered PD batch scan behind a multi-range
//! request, and the contiguous scan behind a range load.
//!
//! Go boundary: client-go's `region_cache.go` — `BatchLocateKeyRanges`, which
//! resolves what valid cache entries can answer and asks PD only for the exact
//! remaining ranges, and `scanRegions`, whose contiguous-coverage contract is
//! what makes a gap in PD's answer an error rather than a silent hole.

use std::collections::BTreeMap;

use super::super::{
    DEFAULT_REGIONS_PER_BATCH, KeyRange, MAX_RANGES_PER_BATCH, RegionLocation, RegionRouteError,
    merge_loaded_and_cached, ranges_after_key, regions_have_gap, regions_intersecting_ranges,
};
use super::lookup::{cache_misses, insert_loaded_into, preserve_newer_buckets};
use super::{
    BatchLoadOptions, BatchScanBackoff, BatchScanRetryReason, RegionCache, RegionQueryBackoff,
    RegionQueryLoader, RegionQueryOptions, RegionQueryRetryReason, RegionQueryRoute,
    cache_now_seconds,
};

pub(in crate::region) struct BatchLocatePlan {
    pub(in crate::region) cached: Vec<RegionLocation>,
    pub(in crate::region) misses: Vec<KeyRange>,
}

impl<L> RegionCache<L>
where
    L: super::RegionLoader,
{
    /// Resolves ordered key ranges through valid cache entries first and the
    /// exact PD batch-scan boundary second.
    pub fn batch_locate_key_ranges(
        &mut self,
        ranges: &[KeyRange],
        options: BatchLoadOptions,
        backoff: &mut impl BatchScanBackoff,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        self.batch_locate_key_ranges_at(ranges, options, backoff, cache_now_seconds())
    }

    /// Deterministic-clock form of [`Self::batch_locate_key_ranges`] used by
    /// source-transition tests.
    pub fn batch_locate_key_ranges_at(
        &mut self,
        ranges: &[KeyRange],
        options: BatchLoadOptions,
        backoff: &mut impl BatchScanBackoff,
        now_seconds: u64,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        let plan = self.plan_batch_locate_key_ranges(ranges, now_seconds)?;
        let mut misses = plan.misses.clone();
        let mut fresh = Vec::new();
        while !misses.is_empty() {
            let batch_len = misses.len().min(MAX_RANGES_PER_BATCH);
            let request = &misses[..batch_len];
            let publishable = loop {
                let loaded = self
                    .with_loader(|loader| {
                        loader.batch_load_regions(request, DEFAULT_REGIONS_PER_BATCH, options)
                    })
                    .map_err(RegionRouteError::Loader)?;
                let retry = if loaded.is_empty() {
                    Some(BatchScanRetryReason::EmptyReply)
                } else if regions_have_gap(request, &loaded, DEFAULT_REGIONS_PER_BATCH) {
                    Some(BatchScanRetryReason::CoverageGap)
                } else {
                    None
                };
                if let Some(reason) = retry {
                    backoff.backoff(reason)?;
                    continue;
                }
                let publishable = if options.need_leader {
                    loaded
                        .iter()
                        .filter(|region| region.leader_peer_id.is_some())
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    loaded.clone()
                };
                if publishable.is_empty() {
                    backoff.backoff(BatchScanRetryReason::MissingLeader)?;
                    continue;
                }
                break publishable;
            };
            let split_key = publishable
                .last()
                .expect("validated publishable batch reply is nonempty")
                .end_key
                .clone();
            fresh.extend(publishable);
            if split_key.is_empty() {
                misses.clear();
            } else {
                let remaining_batch = ranges_after_key(request, &split_key);
                let mut remaining = remaining_batch;
                remaining.extend_from_slice(&misses[batch_len..]);
                if remaining == misses {
                    return Err(RegionRouteError::NonProgressingBatchScan { split_key });
                }
                misses = remaining;
            }
        }

        self.publish_batch_locate_key_ranges(ranges, &plan.cached, fresh, now_seconds)
    }
}

impl<L> RegionCache<L>
where
    L: super::RegionLoader,
{
    pub(in crate::region) fn plan_batch_locate_key_ranges(
        &mut self,
        ranges: &[KeyRange],
        now_seconds: u64,
    ) -> Result<BatchLocatePlan, RegionRouteError> {
        if ranges.iter().any(|range| !range.is_valid()) {
            return Err(RegionRouteError::InvalidRange);
        }
        if ranges.is_empty() {
            return Ok(BatchLocatePlan {
                cached: Vec::new(),
                misses: Vec::new(),
            });
        }
        let unavailable = self.refresh_traversed_entries(ranges, now_seconds)?;
        let cached = cached_regions_for_ranges(&self.regions, ranges, &unavailable)?;
        let misses = cache_misses(&self.regions, ranges, &unavailable)?;
        Ok(BatchLocatePlan { cached, misses })
    }

    pub(in crate::region) fn publish_batch_locate_key_ranges(
        &mut self,
        ranges: &[KeyRange],
        cached: &[RegionLocation],
        fresh: Vec<RegionLocation>,
        now_seconds: u64,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        let merged = merge_loaded_and_cached(cached, &fresh);
        let result = regions_intersecting_ranges(&merged, ranges);
        if regions_have_gap(ranges, &result, 0) {
            return Err(RegionRouteError::BatchScanGap);
        }

        // Validate the complete canonical insertion sequence against a clone
        // before publishing the first region. This keeps terminal failures
        // from leaving a partially updated cache.
        let mut preview = self.regions.clone();
        for mut region in fresh.iter().cloned() {
            preserve_newer_buckets(&preview, &mut region);
            let _ = insert_loaded_into(&mut preview, region)?;
        }
        for region in fresh {
            self.insert_loaded_at(region, now_seconds)?;
        }
        Ok(result)
    }
}

impl<L> RegionCache<L>
where
    L: RegionQueryLoader,
{
    /// Executes the pinned contiguous ScanRegions contract without publishing.
    pub fn scan_regions(
        &mut self,
        range: &KeyRange,
        limit: usize,
        backoff: &mut impl RegionQueryBackoff,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        if !range.is_valid() {
            return Err(RegionRouteError::InvalidRange);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut options = RegionQueryOptions {
            need_buckets: false,
            route: RegionQueryRoute::AllowFollowerOrRouter,
        };
        loop {
            let loaded = self
                .with_loader(|loader| loader.scan_regions_once(range, limit, options))
                .map_err(RegionRouteError::Loader)?;
            let retry = if loaded.is_empty() {
                Some(RegionQueryRetryReason::EmptyReply)
            } else if regions_have_gap(std::slice::from_ref(range), &loaded, limit) {
                Some(RegionQueryRetryReason::CoverageGap)
            } else {
                None
            };
            if let Some(reason) = retry {
                backoff.backoff(reason)?;
                options.route = RegionQueryRoute::LeaderOnly;
                continue;
            }
            let leaderful = loaded
                .into_iter()
                .filter(|region| region.leader_peer_id.is_some())
                .collect::<Vec<_>>();
            if leaderful.is_empty() {
                backoff.backoff(RegionQueryRetryReason::MissingLeader)?;
                options.route = RegionQueryRoute::LeaderOnly;
                continue;
            }
            return Ok(leaderful);
        }
    }

    /// Scans and atomically publishes the returned regions.
    pub fn load_regions_with_range(
        &mut self,
        range: &KeyRange,
        limit: usize,
        backoff: &mut impl RegionQueryBackoff,
    ) -> Result<Vec<RegionLocation>, RegionRouteError> {
        let loaded = self.scan_regions(range, limit, backoff)?;
        let mut preview = self.regions.clone();
        for mut region in loaded.iter().cloned() {
            preserve_newer_buckets(&preview, &mut region);
            let _ = insert_loaded_into(&mut preview, region)?;
        }
        let now_seconds = cache_now_seconds();
        for region in loaded.iter().cloned() {
            self.insert_loaded_at(region, now_seconds)?;
        }
        Ok(loaded)
    }
}

fn cached_regions_for_ranges(
    cached: &[RegionLocation],
    ranges: &[KeyRange],
    unavailable: &std::collections::BTreeSet<super::super::RegionVerId>,
) -> Result<Vec<RegionLocation>, RegionRouteError> {
    let Some(first) = ranges.first() else {
        return Ok(Vec::new());
    };
    let end = ranges
        .last()
        .and_then(|range| (!range.end.is_empty()).then_some(range.end.as_slice()));
    let start = first.start.as_slice();
    let start_index = cached
        .binary_search_by(|region| {
            if region.contains_key(start) {
                std::cmp::Ordering::Equal
            } else if region.start_key.as_slice() > start {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        })
        .unwrap_or_else(|index| index);
    let mut selected = BTreeMap::new();
    for region in cached.iter().skip(start_index) {
        if end.is_some_and(|end| region.start_key.as_slice() >= end) {
            break;
        }
        if !unavailable.contains(&region.region) {
            selected
                .entry(region.region)
                .or_insert_with(|| region.clone());
        }
    }
    let mut selected: Vec<_> = selected.into_values().collect();
    selected.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    Ok(selected)
}
