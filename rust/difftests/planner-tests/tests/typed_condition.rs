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

//! Source-shaped tests for the deferred typed-condition evaluator handoff.
//!
//! The Go owner is `pkg/executor/join/joiner.go`: ordinary vectorized
//! filtering keeps TRUE rows, while `VectorizedFilterConsiderNull` also
//! reports UNKNOWN for outer/semi join status handling.  These tests verify
//! only the request metadata and source `FullSchema` row width.

use tidb_ast::{BinaryOp, Expr};
use tidb_planner::join_condition::{ColumnSpec, JoinSchema};
use tidb_planner::predicate_partition::partition_predicates;
use tidb_planner::typed_condition::{ConditionEvaluationMode, TruthPolicy};

fn schema() -> JoinSchema {
    JoinSchema::new(
        [ColumnSpec::new("id", "left")],
        [ColumnSpec::new("id", "right")],
    )
}

fn col(table: &str) -> Expr {
    Expr::Column(vec![table.to_owned(), "id".to_owned()])
}

fn equality() -> Expr {
    Expr::Binary(BinaryOp::Eq, Box::new(col("left")), Box::new(col("right")))
}

#[test]
fn join_filter_request_keeps_full_schema_and_true_only_policy() {
    let predicates = partition_predicates([equality()], &schema()).expect("bind equality");
    let request = predicates.predicates()[0].typed_request(ConditionEvaluationMode::JoinFilter);

    assert_eq!(request.mode(), ConditionEvaluationMode::JoinFilter);
    assert_eq!(request.truth_policy(), TruthPolicy::KeepTrueOnly);
    assert_eq!(request.full_schema_width(), 2);
    assert_eq!(request.plan().bindings().len(), 2);
}

#[test]
fn outer_match_request_explicitly_tracks_unknown_without_evaluation() {
    let predicates = partition_predicates(
        [Expr::Binary(
            BinaryOp::Gt,
            Box::new(col("left")),
            Box::new(Expr::Int("1".to_owned())),
        )],
        &schema(),
    )
    .expect("bind child condition");
    let request =
        predicates.predicates()[0].typed_request(ConditionEvaluationMode::OuterMatchStatus);

    assert_eq!(request.mode(), ConditionEvaluationMode::OuterMatchStatus);
    assert_eq!(request.truth_policy(), TruthPolicy::TrackUnknown);
    assert_eq!(request.full_schema_width(), 2);
}
