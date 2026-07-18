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

//! Dependency-closed vectors for
//! `pkg/planner/core/rule/rule_max_min_eliminate.go`.
//!
//! The Go regression anchor is `TestMaxMinEliminateSkipsEmptyScalarAgg` at
//! `pkg/planner/core/rule/rule_max_min_eliminate_test.go:26`.  These vectors
//! isolate the source eligibility gates and single/multi aggregate branch;
//! index/ranger checks and replacement-plan construction remain external.

use tidb_planner::max_min_elimination::{
    AggregateKind, MaxMinAggregationShape, MaxMinEliminationDecision, MaxMinEliminator, ValueType,
};

fn aggregation(
    group_by_items: usize,
    aggregate_kinds: Vec<AggregateKind>,
    used_column_types: Vec<ValueType>,
) -> MaxMinAggregationShape {
    MaxMinAggregationShape::new(group_by_items, aggregate_kinds, used_column_types)
}

#[test]
fn empty_scalar_aggregation_is_ineligible_without_panicking() {
    let shape = aggregation(0, Vec::new(), Vec::new());
    assert_eq!(
        MaxMinEliminator.classify(&shape),
        MaxMinEliminationDecision::Ineligible
    );
}

#[test]
fn grouped_or_non_max_min_aggregations_are_ineligible() {
    let grouped = aggregation(1, vec![AggregateKind::Max], vec![ValueType::Ordinary]);
    assert_eq!(
        MaxMinEliminator.classify(&grouped),
        MaxMinEliminationDecision::Ineligible
    );

    let other = aggregation(
        0,
        vec![AggregateKind::Max, AggregateKind::Other],
        vec![ValueType::Ordinary],
    );
    assert_eq!(
        MaxMinEliminator.classify(&other),
        MaxMinEliminationDecision::Ineligible
    );
}

#[test]
fn enum_and_set_ordering_blocks_elimination() {
    for value_type in [ValueType::Enum, ValueType::Set] {
        let shape = aggregation(0, vec![AggregateKind::Min], vec![value_type]);
        assert_eq!(
            MaxMinEliminator.classify(&shape),
            MaxMinEliminationDecision::Ineligible
        );
    }
}

#[test]
fn ordinary_max_min_aggregations_select_single_or_index_checked_branch() {
    let single = aggregation(0, vec![AggregateKind::Max], vec![ValueType::Ordinary]);
    assert_eq!(
        MaxMinEliminator.classify(&single),
        MaxMinEliminationDecision::Single
    );

    let multiple = aggregation(
        0,
        vec![AggregateKind::Max, AggregateKind::Min],
        vec![ValueType::Ordinary, ValueType::Ordinary],
    );
    assert_eq!(
        MaxMinEliminator.classify(&multiple),
        MaxMinEliminationDecision::MultipleNeedsIndex
    );
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(MaxMinEliminator.name(), "max_min_eliminate");
}
