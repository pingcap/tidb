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

//! Source-backed tests for the global statistics zero layout.

use tidb_stats::{new_global_stats_layout, GlobalStatsLayout};

#[test]
fn source_global_stats_layout_allocates_nil_slots() {
    let layout = new_global_stats_layout(3);
    assert_eq!(layout.num, 3);
    assert_eq!(layout.histogram_slots.len(), 3);
    assert_eq!(layout.cmsketch_slots.len(), 3);
    assert_eq!(layout.topn_slots.len(), 3);
    assert_eq!(layout.fmsketch_slots.len(), 3);
    assert!(layout.histogram_slots.iter().all(Option::is_none));
    assert!(layout.cmsketch_slots.iter().all(Option::is_none));
    assert!(layout.topn_slots.iter().all(Option::is_none));
    assert!(layout.fmsketch_slots.iter().all(Option::is_none));
    assert_eq!(layout.missing_partition_stats, None);
    assert_eq!(layout.count, 0);
    assert_eq!(layout.modify_count, 0);
}

#[test]
fn source_global_stats_layout_zero_histograms_is_empty() {
    assert_eq!(
        new_global_stats_layout(0),
        GlobalStatsLayout {
            num: 0,
            histogram_slots: Vec::new(),
            cmsketch_slots: Vec::new(),
            topn_slots: Vec::new(),
            fmsketch_slots: Vec::new(),
            missing_partition_stats: None,
            count: 0,
            modify_count: 0,
        }
    );
}
