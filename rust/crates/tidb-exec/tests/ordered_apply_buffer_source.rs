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

//! Source-backed tests for ordered parallel Apply result state.

use tidb_exec::ordered_apply_buffer::{OrderedApplyBuffer, OrderedApplyResult, ReorderError};

fn flatten(batches: Vec<Vec<i64>>) -> Vec<i64> {
    batches.into_iter().flatten().collect()
}

#[test]
fn ordered_parallel_apply_preserves_outer_sequence() {
    // Source: pkg/executor/parallel_apply.go:483-608 and
    // pkg/executor/parallel_apply_test.go:560-663 (TestOrderedParallelApply).
    let mut buffer = OrderedApplyBuffer::new(3).expect("valid chunk capacity");
    assert!(buffer
        .push(OrderedApplyResult::success(2, vec![20]))
        .unwrap()
        .is_empty());
    assert!(buffer
        .push(OrderedApplyResult::success(0, vec![0]))
        .unwrap()
        .is_empty());
    let batches = buffer
        .push(OrderedApplyResult::success(1, vec![10, 11]))
        .expect("consecutive results drain");
    assert_eq!(batches, vec![vec![0, 10, 11]]);
    assert_eq!(buffer.pending_len(), 0);
    let batches = buffer
        .push(OrderedApplyResult::success(3, vec![30, 31, 32, 33]))
        .expect("next result drains");
    assert_eq!(batches, vec![vec![20, 30, 31]]);
    let batches = buffer
        .push(OrderedApplyResult::success(4, vec![]))
        .expect("empty outer result advances sequence");
    assert!(batches.is_empty());
    assert_eq!(buffer.flush_idle().unwrap(), Some(vec![32, 33]));
    let batches = buffer
        .push(OrderedApplyResult::success(5, vec![50]))
        .expect("later result drains");
    assert!(batches.is_empty());
    assert_eq!(buffer.finish().unwrap(), vec![vec![50]]);
    assert!(buffer.is_finished());
}

#[test]
fn ordered_parallel_apply_edge_cases_keep_empty_and_left_semi_rows() {
    // Source: pkg/executor/parallel_apply_test.go:664-763
    // (TestOrderedParallelApplyEdgeCases).
    let mut buffer = OrderedApplyBuffer::new(4).unwrap();
    let mut emitted = Vec::new();
    for (sequence, rows) in [
        (0, vec![1]),     // matched left-semi row
        (1, vec![]),      // filtered/unselected outer row
        (2, vec![3, 30]), // unmatched path's emitted row
        (3, vec![]),      // empty outer side result
        (4, vec![5]),     // single-row outer
    ] {
        emitted.extend(
            buffer
                .push(OrderedApplyResult::success(sequence, rows))
                .unwrap(),
        );
    }
    emitted.extend(buffer.finish().unwrap());
    assert_eq!(flatten(emitted), vec![1, 3, 30, 5]);
    assert_eq!(buffer.next_sequence(), 5);
}

#[test]
fn ordered_parallel_apply_large_inner_flushes_multiple_batches() {
    // Source: pkg/executor/parallel_apply_test.go:764-822
    // (TestOrderedParallelApplyLargeInner).
    let mut buffer = OrderedApplyBuffer::new(64).unwrap();
    let mut batches = Vec::new();
    for sequence in (0..3).rev() {
        let rows = (0..90).map(|row| (sequence * 1000 + row) as i64).collect();
        batches.extend(
            buffer
                .push(OrderedApplyResult::success(sequence, rows))
                .unwrap(),
        );
    }
    batches.extend(buffer.finish().unwrap());
    assert_eq!(batches.len(), 5);
    assert!(batches.iter().take(4).all(|batch| batch.len() == 64));
    assert_eq!(batches.last().map(Vec::len), Some(14));
    let rows = flatten(batches);
    assert_eq!(rows.len(), 270);
    assert!(rows.windows(2).all(|window| window[0] < window[1]));
}

#[test]
fn ordered_parallel_apply_left_outer_semi_preserves_unmatched_positions() {
    // Source: pkg/executor/parallel_apply_test.go:823-886
    // (TestOrderedParallelApplyLeftOuterSemiJoin).
    let mut buffer = OrderedApplyBuffer::new(2).unwrap();
    let mut batches = Vec::new();
    batches.extend(
        buffer
            .push(OrderedApplyResult::success(2, vec![4]))
            .unwrap(),
    );
    batches.extend(
        buffer
            .push(OrderedApplyResult::success(0, vec![2]))
            .unwrap(),
    );
    batches.extend(buffer.push(OrderedApplyResult::success(1, vec![])).unwrap());
    batches.extend(buffer.finish().unwrap());
    let rows = flatten(batches);
    assert_eq!(rows, vec![2, 4]);
}

#[test]
fn ordered_parallel_apply_nested_layers_are_independent() {
    // Source: pkg/executor/parallel_apply_test.go:969-1059
    // (TestOrderedParallelApplyNested).
    let mut inner = OrderedApplyBuffer::new(2).unwrap();
    let mut inner_batches = Vec::new();
    inner_batches.extend(
        inner
            .push(OrderedApplyResult::success(1, vec![200]))
            .unwrap(),
    );
    inner_batches.extend(
        inner
            .push(OrderedApplyResult::success(0, vec![100]))
            .unwrap(),
    );
    inner_batches.extend(inner.finish().unwrap());
    let inner_rows = flatten(inner_batches);

    let mut outer = OrderedApplyBuffer::new(2).unwrap();
    let mut outer_batches = Vec::new();
    outer_batches.extend(
        outer
            .push(OrderedApplyResult::success(1, vec![300]))
            .unwrap(),
    );
    outer_batches.extend(
        outer
            .push(OrderedApplyResult::success(0, inner_rows))
            .unwrap(),
    );
    outer_batches.extend(outer.finish().unwrap());
    assert_eq!(flatten(outer_batches), vec![100, 200, 300]);
}

#[test]
fn ordered_parallel_apply_error_and_cancel_are_terminal() {
    // Source: pkg/executor/parallel_apply.go:483-608 plus the ordered
    // worker panic/kill obligations in parallel_apply_test.go:887-968.
    let mut errored = OrderedApplyBuffer::new(2).unwrap();
    assert_eq!(
        errored.push(OrderedApplyResult::failure(0, "worker panic")),
        Err(ReorderError::Worker("worker panic".to_owned()))
    );
    assert_eq!(
        errored.push(OrderedApplyResult::success(1, vec![1])),
        Err(ReorderError::Finished)
    );

    let mut cancelled = OrderedApplyBuffer::new(2).unwrap();
    cancelled
        .push(OrderedApplyResult::success(0, vec![1]))
        .unwrap();
    cancelled.cancel();
    assert_eq!(cancelled.finish(), Err(ReorderError::Cancelled));
    assert_eq!(
        cancelled.push(OrderedApplyResult::success(1, vec![2])),
        Err(ReorderError::Cancelled)
    );
}
