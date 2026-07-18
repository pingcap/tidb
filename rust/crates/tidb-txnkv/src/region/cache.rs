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

use super::{KeyRange, RegionLocation, RegionRouteError, RegionVerId};

/// Injected PD-shaped region metadata loader.
pub trait RegionLoader {
    /// Loads the region containing `key` without prescribing any network API.
    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, String>;
}

/// Ordered cache for versioned region snapshots.
pub struct RegionCache<L> {
    loader: L,
    regions: Vec<RegionLocation>,
}

impl<L> RegionCache<L> {
    /// Creates an empty cache over an injected loader.
    #[must_use]
    pub const fn new(loader: L) -> Self {
        Self {
            loader,
            regions: Vec::new(),
        }
    }

    /// Returns the number of cached region snapshots.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.regions.len()
    }

    /// Returns whether the cache is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    /// Invalidates only the exact versioned region identity.
    pub fn invalidate(&mut self, region: RegionVerId) -> bool {
        let original_len = self.regions.len();
        self.regions.retain(|cached| cached.region != region);
        self.regions.len() != original_len
    }

    /// Finds one key, loading and inserting on a miss.
    pub fn locate_key(&mut self, key: &[u8]) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        if let Some(index) = self.find_key(key) {
            return Ok(&self.regions[index]);
        }
        let loaded = self
            .loader
            .load_region(key)
            .map_err(RegionRouteError::Loader)?;
        if !loaded.contains_key(key) {
            return Err(RegionRouteError::Loader(
                "loader returned a region that does not contain the key".to_owned(),
            ));
        }
        let index = self.insert_loaded(loaded)?;
        Ok(&self.regions[index])
    }

    /// Locates a range and rejects any cross-region request.
    pub fn locate_range(&mut self, range: &KeyRange) -> Result<&RegionLocation, RegionRouteError>
    where
        L: RegionLoader,
    {
        if !range.is_valid() {
            return Err(RegionRouteError::InvalidRange);
        }
        let location = self.locate_key(&range.start)?;
        if !location.contains_range(range) {
            return Err(RegionRouteError::MultiRegion);
        }
        Ok(location)
    }

    fn find_key(&self, key: &[u8]) -> Option<usize> {
        self.regions
            .binary_search_by(|region| {
                if region.contains_key(key) {
                    std::cmp::Ordering::Equal
                } else if region.start_key.as_slice() > key {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            })
            .ok()
    }

    fn insert_loaded(&mut self, loaded: RegionLocation) -> Result<usize, RegionRouteError> {
        if let Some(current) = self
            .regions
            .iter()
            .find(|region| region.region.id == loaded.region.id)
        {
            if loaded.region.epoch.is_older_than(current.region.epoch) {
                return Err(RegionRouteError::StaleRegionEpoch {
                    loaded: loaded.region,
                    cached: current.region,
                });
            }
        }

        self.regions.retain(|current| {
            current.region.id != loaded.region.id && !ranges_intersect(current, &loaded)
        });
        let index = self
            .regions
            .binary_search_by(|region| region.start_key.cmp(&loaded.start_key))
            .unwrap_or_else(|index| index);
        self.regions.insert(index, loaded);
        Ok(index)
    }
}

fn ranges_intersect(left: &RegionLocation, right: &RegionLocation) -> bool {
    let left_before_right = !left.end_key.is_empty() && left.end_key <= right.start_key;
    let right_before_left = !right.end_key.is_empty() && right.end_key <= left.start_key;
    !left_before_right && !right_before_left
}
