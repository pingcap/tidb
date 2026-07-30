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

use super::super::{
    merge_loaded_and_cached, ranges_after_key, regions_have_gap, regions_intersecting_ranges,
    KeyRange, RegionLocation, RegionRouteError, DEFAULT_REGIONS_PER_BATCH, MAX_RANGES_PER_BATCH,
};
use super::lookup::{cache_misses, insert_loaded_into, preserve_newer_buckets};
use super::{
    cache_now_seconds, BatchLoadOptions, BatchRegionLoader, BatchScanBackoff, BatchScanRetryReason,
    RegionCache, RegionQueryBackoff, RegionQueryLoader, RegionQueryOptions, RegionQueryRetryReason,
    RegionQueryRoute,
};

impl<L> RegionCache<L>
where
    L: BatchRegionLoader,
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
        if ranges.iter().any(|range| !range.is_valid()) {
            return Err(RegionRouteError::InvalidRange);
        }
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let unavailable = self.refresh_traversed_entries(ranges, now_seconds)?;
        let cached_before_load = self
            .regions
            .iter()
            .filter(|region| !unavailable.contains(&region.region))
            .cloned()
            .collect::<Vec<_>>();

        let mut misses = cache_misses(&self.regions, ranges, &unavailable)?;
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

        let merged = merge_loaded_and_cached(&cached_before_load, &fresh);
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
            insert_loaded_into(&mut preview, region)?;
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
            insert_loaded_into(&mut preview, region)?;
        }
        let now_seconds = cache_now_seconds();
        for region in loaded.iter().cloned() {
            self.insert_loaded_at(region, now_seconds)?;
        }
        Ok(loaded)
    }
}
