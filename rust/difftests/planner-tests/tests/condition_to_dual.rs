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

//! Dependency-closed vectors for constant-condition TableDual decisions.
//!
//! The Go anchor is `TestAntiSemiJoinConstFalse` at
//! `pkg/planner/core/logical_plans_test.go:241`.

use tidb_planner::condition_to_dual::{conds_to_table_dual, ConditionTruth};

#[test]
fn contradictory_inner_predicate_reduces_to_dual() {
    assert!(conds_to_table_dual(&[ConditionTruth::False], false));
}

#[test]
fn plan_cache_guard_preserves_original_predicate() {
    assert!(!conds_to_table_dual(&[ConditionTruth::False], true));
}

#[test]
fn null_and_cardinality_boundaries_match_source() {
    assert!(conds_to_table_dual(
        &[ConditionTruth::Null, ConditionTruth::True],
        false
    ));
    assert!(!conds_to_table_dual(
        &[ConditionTruth::True, ConditionTruth::False],
        false
    ));
}
