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

//! Source-backed tests for histogram-free global TopN aggregation.

use tidb_stats::{merge_histogram_free_topn, TopN, TopNEntry};

#[test]
fn merge_part_topn_without_histograms_matches_go_fixture() {
    let mut partitions = Vec::with_capacity(10);
    for _ in 0..10 {
        let mut top_n = TopN::new(3);
        top_n.append(&[1, 1], 2);
        top_n.append(&[1, 2], 2);
        top_n.append(&[1, 3], 3);
        top_n.sort();
        partitions.push(top_n);
    }

    let merged = merge_histogram_free_topn(&partitions, 2).expect("non-empty topN");
    assert_eq!(merged.top_n.entries().len(), 2);
    assert_eq!(merged.top_n.total_count(), 50);
    assert_eq!(merged.remainder.len(), 1);
    assert_eq!(merged.top_n.entries()[0].encoded, vec![1, 1]);
    assert_eq!(merged.top_n.entries()[1].encoded, vec![1, 3]);
    assert_eq!(merged.remainder[0].encoded, vec![1, 2]);
}

#[test]
fn merge_histogram_free_topn_skips_empty_and_uses_encoded_tie_breaking() {
    let mut empty = TopN::new(0);
    empty.append(&[9], 0);
    let mut first = TopN::new(2);
    first.append(&[2], 3);
    first.append(&[1], 3);
    let mut second = TopN::new(1);
    second.append(&[1], 1);

    let merged = merge_histogram_free_topn(&[empty, first, second], 1).expect("non-empty topN");
    assert_eq!(
        merged.top_n.entries(),
        &[TopNEntry {
            encoded: vec![1],
            count: 4
        }]
    );
    assert_eq!(
        merged.remainder,
        vec![TopNEntry {
            encoded: vec![2],
            count: 3,
        }]
    );
}

#[test]
fn merge_histogram_free_topn_returns_none_for_empty_partitions() {
    let mut empty = TopN::new(1);
    empty.append(&[7], 0);
    assert!(merge_histogram_free_topn(&[empty], 1).is_none());
}
