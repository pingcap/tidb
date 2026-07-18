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

//! Source-backed tests for the TopN merge task range descriptor.

use tidb_stats::TopnStatsMergeTask;

#[test]
fn source_topn_merge_task_preserves_half_open_bounds() {
    let task = TopnStatsMergeTask::new(3, 17);
    assert_eq!(task.start(), 3);
    assert_eq!(task.end(), 17);
}

#[test]
fn source_topn_merge_task_keeps_empty_and_unvalidated_ranges() {
    let empty = TopnStatsMergeTask::new(5, 5);
    assert_eq!((empty.start(), empty.end()), (5, 5));

    // NewTopnStatsMergeTask stores its inputs directly; the worker owns any
    // eventual slice-bound validation.
    let reversed = TopnStatsMergeTask::new(9, 2);
    assert_eq!((reversed.start(), reversed.end()), (9, 2));

    let negative = TopnStatsMergeTask::new(-3, -1);
    assert_eq!((negative.start(), negative.end()), (-3, -1));
}
