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

use std::collections::BTreeMap;

/// One region as the store reports it: client-go `Region.GetMeta()` plus the
/// leader, for `SHOW TABLE ... REGIONS`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionMeta {
    /// `metapb.Region.Id`.
    pub id: u64,
    /// The region's only peer, which is also its leader.
    pub peer_id: u64,
    /// The store holding that peer.
    pub store_id: u64,
    /// `metapb.Region.StartKey`, the region's own (unclipped) bound.
    pub start_key: Vec<u8>,
    /// `metapb.Region.EndKey`; empty for the last region.
    pub end_key: Vec<u8>,
}

/// The mock cluster's bootstrap: one store, one region, one peer
/// (`BootstrapWithSingleStore`), then ids from 4 upward.
const BOOTSTRAP_STORE_ID: u64 = 1;
const BOOTSTRAP_REGION_ID: u64 = 2;
const BOOTSTRAP_PEER_ID: u64 = 3;
const FIRST_SPLIT_ID: u64 = 4;

/// The start keys of every region but the first (whose start is `""`), with
/// the ids each was created under.
#[derive(Debug)]
pub struct RegionMap {
    /// Start key -> (region id, peer id).
    starts: BTreeMap<Vec<u8>, (u64, u64)>,
    /// The next id the cluster hands out: a split spends one for the new
    /// region and one for its peer, as `Cluster.AllocID` does.
    next_id: u64,
}

impl Default for RegionMap {
    fn default() -> Self {
        Self {
            starts: BTreeMap::new(),
            next_id: FIRST_SPLIT_ID,
        }
    }
}

impl RegionMap {
    /// client-go `SplitRegions` over the mock store: each distinct key that
    /// falls strictly inside a region creates one region. Returns how many.
    ///
    /// The store cuts the keys in ascending order whatever order the caller
    /// built them in, so the ids a batch is given follow key order.
    pub fn split<I>(&mut self, keys: I) -> usize
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let mut keys: Vec<Vec<u8>> = keys.into_iter().filter(|key| !key.is_empty()).collect();
        keys.sort();
        keys.dedup();
        let mut created = 0;
        for key in keys {
            if self.starts.contains_key(&key) {
                continue;
            }
            self.starts.insert(key, (self.next_id, self.next_id + 1));
            self.next_id += 2;
            created += 1;
        }
        created
    }

    /// client-go `RegionCache.LoadRegionsInKeyRange(start, end)`: every region
    /// overlapping `[start, end)`, with its own bounds, in key order. An empty
    /// `end` is the keyspace's end.
    #[must_use]
    pub fn regions_in(&self, start: &[u8], end: &[u8]) -> Vec<RegionMeta> {
        let mut bounds: Vec<(&[u8], u64, u64)> = Vec::new();
        bounds.push((&[], BOOTSTRAP_REGION_ID, BOOTSTRAP_PEER_ID));
        bounds.extend(
            self.starts
                .iter()
                .map(|(key, (id, peer))| (key.as_slice(), *id, *peer)),
        );
        let mut regions = Vec::new();
        for (index, (low, id, peer)) in bounds.iter().enumerate() {
            let high = bounds.get(index + 1).map_or(&[][..], |next| next.0);
            // The region ends at or before the interval starts.
            if !high.is_empty() && high <= start {
                continue;
            }
            // The region starts at or after the interval ends.
            if !end.is_empty() && *low >= end {
                break;
            }
            regions.push(RegionMeta {
                id: *id,
                peer_id: *peer,
                store_id: BOOTSTRAP_STORE_ID,
                start_key: low.to_vec(),
                end_key: high.to_vec(),
            });
        }
        regions
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
                .map(|(key, _)| key)
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

    #[test]
    fn regions_keep_their_own_bounds_and_get_ids_in_key_order() {
        let mut regions = RegionMap::default();
        assert_eq!(regions.split([b"t5".to_vec(), b"t1".to_vec()]), 2);
        let all = regions.regions_in(b"", b"");
        let ids: Vec<(u64, u64)> = all.iter().map(|r| (r.id, r.peer_id)).collect();
        assert_eq!(ids, vec![(2, 3), (4, 5), (6, 7)]);
        assert_eq!(all[1].start_key, b"t1");
        assert_eq!(all[1].end_key, b"t5");
        // A region ending exactly where the interval starts is not in it,
        // and one starting exactly where it ends is not either.
        let inside = regions.regions_in(b"t1", b"t5");
        assert_eq!(inside.len(), 1);
        assert_eq!(inside[0].id, 4);
        assert_eq!(regions.regions_in(b"t2", b"t3")[0].end_key, b"t5");
    }
}
