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

//! Source-backed tests for column/index memory-usage value objects.

use tidb_stats::{ColumnMemUsage, IndexMemUsage};

#[test]
fn source_column_memory_usage_preserves_total_and_tracking_boundaries() {
    let usage = ColumnMemUsage {
        column_id: 7,
        histogram_mem_usage: 10,
        cmsketch_mem_usage: 20,
        fmsketch_mem_usage: 30,
        topn_mem_usage: 40,
        total_mem_usage: 99,
    };
    assert_eq!(usage.item_id(), 7);
    assert_eq!(usage.total_memory_usage(), 99);
    assert_eq!(usage.tracking_mem_usage(), 70);
    assert_eq!(usage.hist_mem_usage(), 10);
    assert_eq!(usage.topn_mem_usage(), 40);
    assert_eq!(usage.cms_mem_usage(), 20);
}

#[test]
fn source_index_memory_usage_preserves_component_methods() {
    let usage = IndexMemUsage {
        index_id: 11,
        histogram_mem_usage: 3,
        cmsketch_mem_usage: 5,
        topn_mem_usage: 7,
        total_mem_usage: 22,
    };
    assert_eq!(usage.item_id(), 11);
    assert_eq!(usage.total_memory_usage(), 22);
    assert_eq!(usage.tracking_mem_usage(), 15);
    assert_eq!(usage.hist_mem_usage(), 3);
    assert_eq!(usage.topn_mem_usage(), 7);
    assert_eq!(usage.cms_mem_usage(), 5);
}

#[test]
fn source_memory_usage_defaults_to_zero() {
    assert_eq!(ColumnMemUsage::default().tracking_mem_usage(), 0);
    assert_eq!(ColumnMemUsage::default().total_memory_usage(), 0);
    assert_eq!(IndexMemUsage::default().tracking_mem_usage(), 0);
    assert_eq!(IndexMemUsage::default().total_memory_usage(), 0);
}
