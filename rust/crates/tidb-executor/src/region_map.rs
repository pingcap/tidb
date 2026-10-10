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

//! The in-process store's REGION boundaries: what `kv.SplittableStore`
//! splits and what `TABLESAMPLE REGIONS()` and the region cache read.
//!
//! The keyspace starts as one region `["", "")`. A split key strictly inside
//! a region splits it in two; a key that is already a region's start key
//! splits nothing -- which is why `SPLIT TABLE` over a range split before
//! answers a smaller `TOTAL_SPLIT_REGION` the second time. Region boundaries
//! are store metadata, not transactional data, so every catalog snapshot
//! shares one map.

use std::collections::BTreeSet;

/// The start keys of every region but the first (whose start is `""`).
#[derive(Debug, Default)]
pub struct RegionMap {
    starts: BTreeSet<Vec<u8>>,
}

impl RegionMap {
    /// client-go `SplitRegions` over the mock store: each distinct key that
    /// falls strictly inside a region creates one region. Returns how many.
    pub fn split<I>(&mut self, keys: I) -> usize
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        keys.into_iter()
            .filter(|key| !key.is_empty())
            .filter(|key| self.starts.insert(key.clone()))
            .count()
    }

    /// client-go `RegionCache.LoadRegionsInKeyRange` clipped to
    /// `[start, end)`, as `splitIntoMultiRanges` clips it: one range per
    /// region overlapping the interval, in key order. An empty `end` is the
    /// keyspace's end.
    #[must_use]
    pub fn ranges_in(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut bounds = vec![start.to_vec()];
        bounds.extend(
            self.starts
                .range::<[u8], _>((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
                .take_while(|key| end.is_empty() || key.as_slice() < end)
                .cloned(),
        );
        let mut ranges = Vec::with_capacity(bounds.len());
        for (index, low) in bounds.iter().enumerate() {
            let high = bounds
                .get(index + 1)
                .cloned()
                .unwrap_or_else(|| end.to_vec());
            ranges.push((low.clone(), high));
        }
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_split_key_counts_only_when_it_creates_a_region() {
        let mut regions = RegionMap::default();
        assert_eq!(
            regions.split([b"t1".to_vec(), b"t5".to_vec(), b"t1".to_vec()]),
            2
        );
        assert_eq!(
            regions.split([b"t5".to_vec(), b"t3".to_vec(), Vec::new()]),
            1
        );
        assert_eq!(
            regions.ranges_in(b"t2", b"t6"),
            vec![
                (b"t2".to_vec(), b"t3".to_vec()),
                (b"t3".to_vec(), b"t5".to_vec()),
                (b"t5".to_vec(), b"t6".to_vec()),
            ]
        );
        assert_eq!(
            regions.ranges_in(b"t3", b"t4"),
            vec![(b"t3".to_vec(), b"t4".to_vec())]
        );
    }
}
