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

//! Dependency-closed vectors for `pkg/planner/cascades/pattern/pattern.go`.
//!
//! The direct Go anchors are `TestGetOperand` at line 24,
//! `TestOperandMatch` at line 41, `TestNewPattern` at line 65, and
//! `TestPatternSetChildren` at line 75 in
//! `pkg/planner/cascades/pattern/pattern_test.go`.

use tidb_planner::pattern::{
    build_pattern, get_operand, new_pattern, LogicalOperatorKind, Operand,
};
use tidb_planner::pattern_engine::{EngineType, EngineTypeSet};

#[test]
fn source_get_operand_maps_logical_operator_kinds() {
    let cases = [
        (LogicalOperatorKind::Apply, Operand::Apply),
        (LogicalOperatorKind::Join, Operand::Join),
        (LogicalOperatorKind::Aggregation, Operand::Aggregation),
        (LogicalOperatorKind::Projection, Operand::Projection),
        (LogicalOperatorKind::Selection, Operand::Selection),
        (LogicalOperatorKind::MaxOneRow, Operand::MaxOneRow),
        (LogicalOperatorKind::TableDual, Operand::TableDual),
        (LogicalOperatorKind::DataSource, Operand::DataSource),
        (LogicalOperatorKind::UnionScan, Operand::UnionScan),
        (LogicalOperatorKind::UnionAll, Operand::UnionAll),
        (LogicalOperatorKind::Sort, Operand::Sort),
        (LogicalOperatorKind::TopN, Operand::TopN),
        (LogicalOperatorKind::Lock, Operand::Lock),
        (LogicalOperatorKind::Limit, Operand::Limit),
    ];
    for (operator, operand) in cases {
        assert_eq!(get_operand(operator), operand);
    }
    assert_eq!(get_operand(LogicalOperatorKind::Window), Operand::Window);
    assert_eq!(
        get_operand(LogicalOperatorKind::Unsupported),
        Operand::Unsupported
    );
}

#[test]
fn source_operand_match_preserves_wildcard_behavior() {
    for operand in [
        Operand::Limit,
        Operand::Selection,
        Operand::Join,
        Operand::MaxOneRow,
    ] {
        assert!(Operand::Any.matches(operand));
        assert!(operand.matches(Operand::Any));
        assert!(operand.matches(operand));
    }
    assert!(!Operand::Limit.matches(Operand::Selection));
    assert!(!Operand::Limit.matches(Operand::Join));
    assert!(!Operand::Limit.matches(Operand::MaxOneRow));
}

#[test]
fn source_new_pattern_starts_without_children() {
    let pattern = new_pattern(Operand::Any, EngineTypeSet::ALL);
    assert_eq!(pattern.operand, Operand::Any);
    assert!(pattern.children.is_empty());
    assert_eq!(
        new_pattern(Operand::Join, EngineTypeSet::ALL).operand,
        Operand::Join
    );
}

#[test]
fn source_children_are_ordered_and_engine_checked() {
    let mut pattern = new_pattern(Operand::Any, EngineTypeSet::ALL);
    pattern.set_children([
        new_pattern(Operand::Limit, EngineTypeSet::ALL),
        new_pattern(Operand::Selection, EngineTypeSet::ALL),
    ]);
    assert_eq!(pattern.children.len(), 2);
    assert_eq!(pattern.children[0].operand, Operand::Limit);
    assert_eq!(pattern.children[1].operand, Operand::Selection);
    assert!(pattern.matches(Operand::Join, EngineType::TiDb));
    assert!(pattern.matches_operand_any(EngineType::TiKv));

    let built = build_pattern(
        Operand::Join,
        EngineTypeSet::TIKV_ONLY,
        [new_pattern(Operand::Projection, EngineTypeSet::ALL)],
    );
    assert!(built.matches(Operand::Join, EngineType::TiKv));
    assert!(!built.matches(Operand::Join, EngineType::TiDb));
}
