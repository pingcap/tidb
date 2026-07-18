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

//! Histogram-free global TopN aggregation from
//! `pkg/statistics/handle/globalstats/topn.go`.
//!
//! The source routine also has a histogram lookup/removal path.  This leaf
//! owns only the dependency-closed portion exercised when callers provide
//! topN-only partition statistics: aggregate equal encoded values, rank the
//! result, and split the first `n` values from the remainder.  Datum decoding,
//! histogram fallback/removal, SQL-killer checks, concurrency, and storage
//! remain explicit external boundaries.

use std::collections::HashMap;

use crate::cmsketch::{TopN, TopNEntry};

/// The selected global TopN and values that did not fit in its requested size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobalTopNMerge {
    /// The selected values, sorted by encoded bytes like `TopN::sort`.
    pub top_n: TopN,
    /// Remaining values in source ranking order (count descending, then bytes).
    pub remainder: Vec<TopNEntry>,
}

/// Merge partition TopN entries without consulting histograms.
///
/// This mirrors the map aggregation and final ranking in
/// `MergePartTopN2GlobalTopN` for the histogram-free test path.  Equal encoded
/// values are summed with wrapping `u64` arithmetic, empty TopN groups are
/// skipped, and ranking uses count descending followed by encoded-byte
/// ascending order.  The returned selected TopN is byte-sorted, while its
/// remainder retains ranking order as in `GetMergedTopNFromSortedSlice`.
#[must_use]
pub fn merge_histogram_free_topn(partitions: &[TopN], n: usize) -> Option<GlobalTopNMerge> {
    let mut counts: HashMap<Vec<u8>, u64> = HashMap::new();
    for partition in partitions {
        if partition.total_count() == 0 {
            continue;
        }
        for entry in partition.entries() {
            let count = counts.entry(entry.encoded.clone()).or_default();
            *count = count.wrapping_add(entry.count);
        }
    }

    if counts.is_empty() {
        return None;
    }

    let mut ranked: Vec<TopNEntry> = counts
        .into_iter()
        .map(|(encoded, count)| TopNEntry { encoded, count })
        .collect();
    ranked.sort_unstable_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.encoded.cmp(&right.encoded))
    });

    let split = n.min(ranked.len());
    let remainder = ranked.split_off(split);
    let selected = ranked;
    let mut top_n = TopN::new(selected.len());
    for entry in &selected {
        top_n.append(&entry.encoded, entry.count);
    }
    top_n.sort();

    Some(GlobalTopNMerge { top_n, remainder })
}
