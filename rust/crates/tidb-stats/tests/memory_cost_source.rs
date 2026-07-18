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

//! Source-backed tests for LFU memory-cost transitions.

use tidb_stats::{
    add_memory_cost, adjust_mem_cost, effective_mem_cost, MemoryCostError, TEST_MODE_MEMORY_COST,
};

#[test]
fn source_nonzero_capacity_is_unchanged() {
    assert_eq!(adjust_mem_cost(123, None), Ok(123));
    assert_eq!(adjust_mem_cost(-1, None), Ok(-1));
}

#[test]
fn source_zero_capacity_uses_twenty_percent_or_propagates_error() {
    assert_eq!(adjust_mem_cost(0, Some(10_000)), Ok(2_000));
    assert_eq!(
        adjust_mem_cost(0, None),
        Err(MemoryCostError::SystemMemoryUnavailable)
    );
}

#[test]
fn source_new_lfu_test_override_is_applied_after_adjustment() {
    assert_eq!(
        effective_mem_cost(0, true, Some(10_000)),
        Ok(TEST_MODE_MEMORY_COST)
    );
    assert_eq!(effective_mem_cost(123, true, None), Ok(123));
    assert_eq!(effective_mem_cost(0, false, Some(10_000)), Ok(2_000));
    assert_eq!(
        effective_mem_cost(0, true, None),
        Err(MemoryCostError::SystemMemoryUnavailable)
    );
}

#[test]
fn source_cost_delta_wraps_like_atomic_int64_add() {
    assert_eq!(add_memory_cost(10, -3), 7);
    assert_eq!(add_memory_cost(i64::MAX, 1), i64::MIN);
    assert_eq!(add_memory_cost(i64::MIN, -1), i64::MAX);
}
