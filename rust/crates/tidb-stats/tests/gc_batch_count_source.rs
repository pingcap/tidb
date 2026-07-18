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

//! Source-backed tests for statistics GC batch counting.

use tidb_stats::gc_batch_count;

#[test]
fn source_gc_batch_count_rounds_only_positive_remainders() {
    assert_eq!(gc_batch_count(0, 1_000), 0);
    assert_eq!(gc_batch_count(1, 1_000), 1);
    assert_eq!(gc_batch_count(1_000, 1_000), 1);
    assert_eq!(gc_batch_count(1_001, 1_000), 2);
    assert_eq!(gc_batch_count(2_001, 1_000), 3);
}

#[test]
fn source_gc_batch_count_preserves_go_signed_arithmetic() {
    // Go division truncates toward zero; a negative remainder does not round
    // the result up because the source checks `total%batch > 0`.
    assert_eq!(gc_batch_count(-1, 1_000), 0);
    assert_eq!(gc_batch_count(-1_001, 1_000), -1);
    assert_eq!(gc_batch_count(i64::MIN, -1), i64::MIN);
}
