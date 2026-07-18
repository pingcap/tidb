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

//! Source-shaped tests for the first real typed residual-condition consumer.
//!
//! The Go owner is `pkg/executor/join/left_outer_join_probe.go`: a joined
//! predicate can be TRUE, FALSE, or NULL, and outer-row status handling must
//! preserve the NULL distinction.  The Rust adapter evaluates only the
//! existing scalar Datum domain and leaves row/status mutation to its caller.

#[path = "typed_join_runtime_source.rs"]
mod typed_join_runtime_source;

use tidb_ast::{BinaryOp, Expr};
use tidb_datatype::Datum;
use tidb_exec::typed_condition_eval::{
    evaluate_typed_condition, evaluate_typed_condition_batch, finalize_outer_row_statuses,
    merge_outer_row_status, select_outer_row_statuses, transition_outer_row_status, OuterRowStatus,
    OuterRowStatusError, PredicateBatchBuffer, PredicateBatchBufferError, PredicateTruth,
    TypedConditionEvalError,
};
use tidb_planner::join_condition::{ColumnSpec, JoinSchema};
use tidb_planner::predicate_partition::partition_predicates;
use tidb_planner::typed_condition::ConditionEvaluationMode;

fn request() -> tidb_planner::typed_condition::TypedConditionRequest {
    let schema = JoinSchema::new(
        [ColumnSpec::new("id", "left")],
        [ColumnSpec::new("id", "right")],
    );
    let expression = Expr::Binary(
        BinaryOp::Eq,
        Box::new(Expr::Column(vec!["left".to_owned(), "id".to_owned()])),
        Box::new(Expr::Column(vec!["right".to_owned(), "id".to_owned()])),
    );
    partition_predicates([expression], &schema)
        .expect("bind equality")
        .predicates()[0]
        .typed_request(ConditionEvaluationMode::OuterMatchStatus)
}

#[test]
fn scalar_join_condition_produces_true_false_and_unknown() {
    let request = request();
    assert_eq!(
        evaluate_typed_condition(&request, &[Datum::Int(3), Datum::Int(3)]),
        Ok(PredicateTruth::True)
    );
    assert_eq!(
        evaluate_typed_condition(&request, &[Datum::Int(3), Datum::Int(4)]),
        Ok(PredicateTruth::False)
    );
    assert_eq!(
        evaluate_typed_condition(&request, &[Datum::Null, Datum::Int(3)]),
        Ok(PredicateTruth::Unknown)
    );
}

#[test]
fn full_schema_width_is_a_hard_boundary() {
    let error = evaluate_typed_condition(&request(), &[Datum::Int(3)]).expect_err("short row");
    assert_eq!(
        error,
        TypedConditionEvalError::RowWidth {
            expected: 2,
            actual: 1,
        }
    );
}

#[test]
fn batch_condition_preserves_true_false_and_unknown_masks() {
    let rows = vec![
        vec![Datum::Int(3), Datum::Int(3)],
        vec![Datum::Null, Datum::Int(3)],
        vec![Datum::Int(3), Datum::Int(4)],
    ];
    let mask = evaluate_typed_condition_batch(&request(), &rows).expect("batch evaluation");
    assert_eq!(mask.selected(), &[true, false, false]);
    assert_eq!(mask.unknown(), &[false, true, false]);
    assert_eq!(mask.len(), rows.len());
    assert!(!mask.is_empty());
}

#[test]
fn batch_condition_reports_the_failing_row() {
    let error =
        evaluate_typed_condition_batch(&request(), &[vec![Datum::Int(3)]]).expect_err("short row");
    assert_eq!(
        error,
        TypedConditionEvalError::Batch {
            index: 0,
            source: Box::new(TypedConditionEvalError::RowWidth {
                expected: 2,
                actual: 1,
            }),
        }
    );
}

#[test]
fn outer_match_status_transition_preserves_true_and_tracks_unknown() {
    let rows = vec![
        vec![Datum::Int(3), Datum::Int(3)],
        vec![Datum::Int(3), Datum::Int(4)],
        vec![Datum::Null, Datum::Int(3)],
    ];
    let mask = evaluate_typed_condition_batch(&request(), &rows).expect("batch evaluation");
    let initial = [OuterRowStatus::Matched; 3];
    let statuses = transition_outer_row_status(&initial, &mask).expect("status transition");
    assert_eq!(
        statuses.as_slice(),
        &[
            OuterRowStatus::Matched,
            OuterRowStatus::Unmatched,
            OuterRowStatus::HasNull,
        ]
    );
    assert_eq!(initial, [OuterRowStatus::Matched; 3]);
}

#[test]
fn outer_match_status_transition_requires_aligned_masks() {
    let mask = evaluate_typed_condition_batch(&request(), &[vec![Datum::Int(3), Datum::Int(3)]])
        .expect("batch evaluation");
    assert_eq!(
        transition_outer_row_status(&[], &mask),
        Err(OuterRowStatusError::LengthMismatch {
            statuses: 0,
            mask: 1,
        })
    );
}

#[test]
fn cumulative_outer_match_status_keeps_true_and_unknown_across_batches() {
    let first = evaluate_typed_condition_batch(
        &request(),
        &[
            vec![Datum::Null, Datum::Int(3)],
            vec![Datum::Int(3), Datum::Int(4)],
            vec![Datum::Int(3), Datum::Int(4)],
        ],
    )
    .expect("first batch");
    let second = evaluate_typed_condition_batch(
        &request(),
        &[
            vec![Datum::Int(3), Datum::Int(4)],
            vec![Datum::Null, Datum::Int(3)],
            vec![Datum::Int(3), Datum::Int(3)],
        ],
    )
    .expect("second batch");
    let third = evaluate_typed_condition_batch(
        &request(),
        &[
            vec![Datum::Int(3), Datum::Int(3)],
            vec![Datum::Int(3), Datum::Int(4)],
            vec![Datum::Null, Datum::Int(3)],
        ],
    )
    .expect("third batch");

    let statuses = vec![OuterRowStatus::Unmatched; 3];
    let statuses = merge_outer_row_status(&statuses, &first).expect("merge first");
    assert_eq!(
        statuses.as_slice(),
        &[
            OuterRowStatus::HasNull,
            OuterRowStatus::Unmatched,
            OuterRowStatus::Unmatched,
        ]
    );
    let statuses = merge_outer_row_status(&statuses, &second).expect("merge second");
    assert_eq!(
        statuses.as_slice(),
        &[
            OuterRowStatus::HasNull,
            OuterRowStatus::HasNull,
            OuterRowStatus::Matched,
        ]
    );
    let statuses = merge_outer_row_status(&statuses, &third).expect("merge third");
    assert_eq!(
        statuses.as_slice(),
        &[
            OuterRowStatus::Matched,
            OuterRowStatus::HasNull,
            OuterRowStatus::Matched,
        ]
    );
}

#[test]
fn cumulative_outer_match_status_requires_aligned_batches() {
    let mask = evaluate_typed_condition_batch(&request(), &[vec![Datum::Int(3), Datum::Int(3)]])
        .expect("batch evaluation");
    assert_eq!(
        merge_outer_row_status(&[OuterRowStatus::Unmatched; 2], &mask),
        Err(OuterRowStatusError::MergeLengthMismatch {
            accumulated: 2,
            batch: 1,
        })
    );
}

#[test]
fn selected_outer_statuses_keep_logical_indexes_aligned() {
    let rows = vec![
        vec![Datum::Int(3), Datum::Int(3)],
        vec![Datum::Int(3), Datum::Int(4)],
        vec![Datum::Null, Datum::Int(3)],
        vec![Datum::Int(3), Datum::Int(3)],
    ];
    let mask = evaluate_typed_condition_batch(&request(), &rows).expect("batch evaluation");
    let initial = [OuterRowStatus::Matched; 4];
    let statuses = transition_outer_row_status(&initial, &mask).expect("status transition");
    let selected = select_outer_row_statuses(&statuses, &mask).expect("selection alignment");
    assert_eq!(selected.indices(), &[0, 3]);
    assert_eq!(selected.statuses(), &[OuterRowStatus::Matched; 2]);
    assert_eq!(selected.len(), 2);
    assert!(!selected.is_empty());
    assert_eq!(statuses[1], OuterRowStatus::Unmatched);
    assert_eq!(statuses[2], OuterRowStatus::HasNull);
    assert_eq!(initial, [OuterRowStatus::Matched; 4]);
}

#[test]
fn finalization_keeps_default_inner_events_in_source_order() {
    let statuses = [
        OuterRowStatus::Matched,
        OuterRowStatus::Unmatched,
        OuterRowStatus::HasNull,
        OuterRowStatus::Unmatched,
    ];
    let finalizations = finalize_outer_row_statuses(&statuses);
    assert_eq!(
        finalizations
            .iter()
            .map(|event| (event.index(), event.status()))
            .collect::<Vec<_>>(),
        vec![
            (1, OuterRowStatus::Unmatched),
            (2, OuterRowStatus::HasNull),
            (3, OuterRowStatus::Unmatched),
        ]
    );
    assert_eq!(finalizations[0].index(), 1);
    assert_eq!(finalizations[0].status(), OuterRowStatus::Unmatched);
    assert!(finalizations[0].needs_default_inner());
    assert!(!finalizations[0].has_null());
    assert!(finalizations[1].needs_default_inner());
    assert!(finalizations[1].has_null());
    assert_eq!(statuses[0], OuterRowStatus::Matched);
}

#[test]
fn predicate_batch_buffer_resets_replaces_and_validates_alignment() {
    let mut buffer = PredicateBatchBuffer::with_capacity(4);
    buffer.reset(4);
    assert_eq!(buffer.selected(), &[true; 4]);
    assert_eq!(buffer.unknown(), &[false; 4]);
    assert_eq!(buffer.len(), 4);
    assert!(!buffer.is_empty());
    let capacity = buffer.capacity();

    let mask = evaluate_typed_condition_batch(
        &request(),
        &[
            vec![Datum::Int(3), Datum::Int(3)],
            vec![Datum::Null, Datum::Int(3)],
            vec![Datum::Int(3), Datum::Int(4)],
        ],
    )
    .expect("batch evaluation");
    buffer.replace(&mask);
    assert_eq!(buffer.selected(), &[true, false, false]);
    assert_eq!(buffer.unknown(), &[false, true, false]);
    assert_eq!(buffer.validate_len(3), Ok(()));
    assert_eq!(buffer.capacity(), capacity);
    assert_eq!(
        buffer.validate_len(4),
        Err(PredicateBatchBufferError::LengthMismatch {
            expected: 4,
            actual: 3,
        })
    );

    buffer.reset(2);
    assert_eq!(buffer.selected(), &[true; 2]);
    assert_eq!(buffer.unknown(), &[false; 2]);
    assert_eq!(buffer.capacity(), capacity);
}
