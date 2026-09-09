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

//! Port ledger for `pkg/planner/cascades/pattern` (`pkg/planner.part2` items
//! 104-108 on `origin/master`). All five Go tests are REAL functional ports
//! over [`tidb_planner::pattern`] and [`tidb_planner::pattern_engine`], the
//! transcreations of `pkg/planner/cascades/pattern/pattern.go` and
//! `.../engine.go`. Go's `GetOperand` takes concrete `logicalop.LogicalPlan`
//! values and type-switches over them (pattern.go:26-83); the crate keys that
//! dispatch on an explicit `LogicalOperatorKind` classifier (pattern.rs
//! get_operand), so the port drives every source kind through the same
//! mapping instead of constructing plan objects.

use tidb_planner::pattern::{
    build_pattern, get_operand, new_pattern, LogicalOperatorKind, Operand,
};
use tidb_planner::pattern_engine::{EngineType, EngineTypeSet};

/// GO PORT of
/// `pkg/planner/cascades/pattern/engine_test.go:23 TestEngineTypeSet`.
///
/// Re-derived contract: each EngineType is a distinct bit (engine.go:23-29)
/// and the five predefined sets expose exactly their membership table
/// (engine.go:36-49) via bit-intersection `Contains` (engine.go:52-54):
/// EngineAll covers all three engines; each xOnly set admits just its engine;
/// EngineTiKVOrTiFlash rejects TiDB and admits both coprocessor engines.
#[test]
fn engine_type_set_membership_table_matches_predefined_sets() {
    assert!(EngineTypeSet::ALL.contains(EngineType::TiDb));
    assert!(EngineTypeSet::ALL.contains(EngineType::TiKv));
    assert!(EngineTypeSet::ALL.contains(EngineType::TiFlash));

    assert!(EngineTypeSet::TIDB_ONLY.contains(EngineType::TiDb));
    assert!(!EngineTypeSet::TIDB_ONLY.contains(EngineType::TiKv));
    assert!(!EngineTypeSet::TIDB_ONLY.contains(EngineType::TiFlash));

    assert!(!EngineTypeSet::TIKV_ONLY.contains(EngineType::TiDb));
    assert!(EngineTypeSet::TIKV_ONLY.contains(EngineType::TiKv));
    assert!(!EngineTypeSet::TIKV_ONLY.contains(EngineType::TiFlash));

    assert!(!EngineTypeSet::TIFLASH_ONLY.contains(EngineType::TiDb));
    assert!(!EngineTypeSet::TIFLASH_ONLY.contains(EngineType::TiKv));
    assert!(EngineTypeSet::TIFLASH_ONLY.contains(EngineType::TiFlash));

    assert!(!EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TiDb));
    assert!(EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TiKv));
    assert!(EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TiFlash));
}

/// GO PORT of
/// `pkg/planner/cascades/pattern/pattern_test.go:24 TestGetOperand`.
///
/// Re-derived contract: the operand classifier maps all fourteen plan kinds
/// exercised by the Go test to their OperandIdent constants one-to-one
/// (pattern.go type-switch at :127, constants from :30; same table mirrored
/// in pattern.rs get_operand). The crate classifies a typed operator
/// descriptor, so each row drives the corresponding [`LogicalOperatorKind`]
/// through `get_operand`.
#[test]
fn get_operand_classifies_every_source_logical_operator_kind() {
    let rows = [
        (LogicalOperatorKind::Join, Operand::Join),
        (LogicalOperatorKind::Aggregation, Operand::Aggregation),
        (LogicalOperatorKind::Projection, Operand::Projection),
        (LogicalOperatorKind::Selection, Operand::Selection),
        (LogicalOperatorKind::Apply, Operand::Apply),
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
    for (kind, expected) in rows {
        assert_eq!(get_operand(kind), expected);
    }
}

/// GO PORT of
/// `pkg/planner/cascades/pattern/pattern_test.go:41 TestOperandMatch`.
///
/// Re-derived contract: matching is symmetric around the wildcard —
/// `OperandAny.Match(any-op)` true (:43-47), any concrete op matches the Any
/// operand (:50-55) — exact same-kind pairs match (:57-61), and cross-kind
/// pairs never match (:63-66). Production rule: `Operand.Match`
/// returns true iff either side is OperandAny or ids are equal
/// (pattern.go:175-181).
#[test]
fn operand_match_wildcard_symmetry_and_identity_matrix() {
    // OperandAny matches every concrete operand fed from the right.
    assert!(Operand::Any.matches(Operand::Limit));
    assert!(Operand::Any.matches(Operand::Selection));
    assert!(Operand::Any.matches(Operand::Join));
    assert!(Operand::Any.matches(Operand::MaxOneRow));
    assert!(Operand::Any.matches(Operand::Any));

    // Every concrete operand matches a wildcard on the left.
    assert!(Operand::Limit.matches(Operand::Any));
    assert!(Operand::Selection.matches(Operand::Any));
    assert!(Operand::Join.matches(Operand::Any));
    assert!(Operand::MaxOneRow.matches(Operand::Any));

    // Same-kind identity holds.
    assert!(Operand::Limit.matches(Operand::Limit));
    assert!(Operand::Selection.matches(Operand::Selection));
    assert!(Operand::Join.matches(Operand::Join));
    assert!(Operand::MaxOneRow.matches(Operand::MaxOneRow));

    // Cross-kind pairs never match.
    assert!(!Operand::Limit.matches(Operand::Selection));
    assert!(!Operand::Limit.matches(Operand::Join));
    assert!(!Operand::Limit.matches(Operand::MaxOneRow));
}

/// GO PORT of
/// `pkg/planner/cascades/pattern/pattern_test.go:65 TestNewPattern`.
///
/// Re-derived contract: `NewPattern(operand, engine)` records the operand,
/// stores the engine set, and leaves children nil (pattern.go:207-209): both
/// the wildcard-only and the Join-rooted construction carry no children
/// here.
#[test]
fn new_pattern_records_operand_and_starts_without_children() {
    let p = new_pattern(Operand::Any, EngineTypeSet::ALL);
    assert_eq!(p.operand, Operand::Any);
    assert_eq!(p.engine_types, EngineTypeSet::ALL);
    assert!(p.children.is_empty());

    let p = new_pattern(Operand::Join, EngineTypeSet::ALL);
    assert_eq!(p.operand, Operand::Join);
    assert!(p.children.is_empty());
}

/// GO PORT of
/// `pkg/planner/cascades/pattern/pattern_test.go:75 TestPatternSetChildren`.
///
/// Re-derived contract: `SetChildren` replaces the child slice wholesale and
/// in argument order (:77-88 test body; production SetChildren assigns
/// exactly the variadic list, pattern.go:212-215), leaving leaf children nil
/// beneath each new child.
#[test]
fn set_children_replaces_child_patterns_in_argument_order() {
    let mut p = new_pattern(Operand::Any, EngineTypeSet::ALL);
    p.set_children([new_pattern(Operand::Limit, EngineTypeSet::ALL)]);
    assert_eq!(p.children.len(), 1);
    assert_eq!(p.children[0].operand, Operand::Limit);
    assert!(p.children[0].children.is_empty());

    let mut p = new_pattern(Operand::Join, EngineTypeSet::ALL);
    p.set_children([
        new_pattern(Operand::Projection, EngineTypeSet::ALL),
        new_pattern(Operand::Selection, EngineTypeSet::ALL),
    ]);
    assert_eq!(p.children.len(), 2);
    assert_eq!(p.children[0].operand, Operand::Projection);
    assert!(p.children[0].children.is_empty());
    assert_eq!(p.children[1].operand, Operand::Selection);
    assert!(p.children[1].children.is_empty());

    // Guard the replace semantics: set_children overwrites, never appends.
    p.set_children([new_pattern(Operand::Limit, EngineTypeSet::ALL)]);
    assert_eq!(
        p.children,
        vec![build_pattern(Operand::Limit, EngineTypeSet::ALL, [])]
    );
}
