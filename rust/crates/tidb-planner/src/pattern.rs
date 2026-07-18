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

//! Dependency-closed cascades pattern metadata from
//! `pkg/planner/cascades/pattern/pattern.go`.
//!
//! This leaf keeps the source operand numbering, wildcard matching, engine
//! filtering, child-pattern construction, and logical-operator classification
//! over a typed adapter.  The real Go logical-plan objects and cascades memo
//! remain outside this boundary.

use crate::pattern_engine::{EngineType, EngineTypeSet};

/// Logical operator kind presented to the pattern classifier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalOperatorKind {
    /// LogicalApply.
    Apply,
    /// LogicalJoin.
    Join,
    /// LogicalAggregation.
    Aggregation,
    /// LogicalProjection.
    Projection,
    /// LogicalSelection.
    Selection,
    /// LogicalMaxOneRow.
    MaxOneRow,
    /// LogicalTableDual.
    TableDual,
    /// DataSource.
    DataSource,
    /// LogicalUnionScan.
    UnionScan,
    /// LogicalUnionAll.
    UnionAll,
    /// LogicalSort.
    Sort,
    /// LogicalTopN.
    TopN,
    /// LogicalLock.
    Lock,
    /// LogicalLimit.
    Limit,
    /// TiKVSingleGather.
    TiKvSingleGather,
    /// LogicalTableScan.
    TableScan,
    /// LogicalMemTable.
    MemTable,
    /// LogicalIndexScan.
    IndexScan,
    /// LogicalShow.
    Show,
    /// LogicalWindow.
    Window,
    /// Any unsupported logical operator.
    Unsupported,
}

/// Pattern operand corresponding to a logical-plan operator type.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum Operand {
    /// Wildcard matching every operand.
    Any = 0,
    /// LogicalJoin.
    Join,
    /// LogicalAggregation.
    Aggregation,
    /// LogicalProjection.
    Projection,
    /// LogicalSelection.
    Selection,
    /// LogicalApply.
    Apply,
    /// LogicalMaxOneRow.
    MaxOneRow,
    /// LogicalTableDual.
    TableDual,
    /// DataSource.
    DataSource,
    /// LogicalUnionScan.
    UnionScan,
    /// LogicalUnionAll.
    UnionAll,
    /// LogicalSort.
    Sort,
    /// LogicalTopN.
    TopN,
    /// LogicalLock.
    Lock,
    /// LogicalLimit.
    Limit,
    /// TiKVSingleGather.
    TiKvSingleGather,
    /// LogicalMemTable.
    MemTableScan,
    /// LogicalTableScan.
    TableScan,
    /// LogicalIndexScan.
    IndexScan,
    /// LogicalShow.
    Show,
    /// LogicalWindow.
    Window,
    /// Unsupported logical operator.
    Unsupported,
}

impl Operand {
    /// Returns the source diagnostic name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Any => "OperandAny",
            Self::Join => "OperandJoin",
            Self::Aggregation => "OperandAggregation",
            Self::Projection => "OperandProjection",
            Self::Selection => "OperandSelection",
            Self::Apply => "OperandApply",
            Self::MaxOneRow => "OperandMaxOneRow",
            Self::TableDual => "OperandTableDual",
            Self::DataSource => "OperandDataSource",
            Self::UnionScan => "OperandUnionScan",
            Self::UnionAll => "OperandUnionAll",
            Self::Sort => "OperandSort",
            Self::TopN => "OperandTopN",
            Self::Lock => "OperandLock",
            Self::Limit => "OperandLimit",
            Self::TiKvSingleGather => "OperandTiKVSingleGather",
            Self::MemTableScan => "OperandMemTableScan",
            Self::TableScan => "OperandTableScan",
            Self::IndexScan => "OperandIndexScan",
            Self::Show => "OperandShow",
            Self::Window => "OperandWindow",
            Self::Unsupported => "OperandUnsupported",
        }
    }

    /// Reports whether two operands match, including wildcard semantics.
    #[must_use]
    pub const fn matches(self, other: Self) -> bool {
        matches!(self, Self::Any) || matches!(other, Self::Any) || self as u8 == other as u8
    }
}

impl std::fmt::Display for Operand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Maps a typed logical operator to its source pattern operand.
#[must_use]
pub const fn get_operand(operator: LogicalOperatorKind) -> Operand {
    match operator {
        LogicalOperatorKind::Apply => Operand::Apply,
        LogicalOperatorKind::Join => Operand::Join,
        LogicalOperatorKind::Aggregation => Operand::Aggregation,
        LogicalOperatorKind::Projection => Operand::Projection,
        LogicalOperatorKind::Selection => Operand::Selection,
        LogicalOperatorKind::MaxOneRow => Operand::MaxOneRow,
        LogicalOperatorKind::TableDual => Operand::TableDual,
        LogicalOperatorKind::DataSource => Operand::DataSource,
        LogicalOperatorKind::UnionScan => Operand::UnionScan,
        LogicalOperatorKind::UnionAll => Operand::UnionAll,
        LogicalOperatorKind::Sort => Operand::Sort,
        LogicalOperatorKind::TopN => Operand::TopN,
        LogicalOperatorKind::Lock => Operand::Lock,
        LogicalOperatorKind::Limit => Operand::Limit,
        LogicalOperatorKind::TiKvSingleGather => Operand::TiKvSingleGather,
        LogicalOperatorKind::TableScan => Operand::TableScan,
        LogicalOperatorKind::MemTable => Operand::MemTableScan,
        LogicalOperatorKind::IndexScan => Operand::IndexScan,
        LogicalOperatorKind::Show => Operand::Show,
        LogicalOperatorKind::Window => Operand::Window,
        LogicalOperatorKind::Unsupported => Operand::Unsupported,
    }
}

/// A source pattern node with an operand, engine set, and child patterns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pattern {
    /// Operator metadata for this node.
    pub operand: Operand,
    /// Engines on which this node may execute.
    pub engine_types: EngineTypeSet,
    /// Child patterns in source order.
    pub children: Vec<Self>,
}

impl Pattern {
    /// Reports whether both operand and engine metadata match.
    #[must_use]
    pub const fn matches(&self, operand: Operand, engine: EngineType) -> bool {
        self.engine_types.contains(engine) && self.operand.matches(operand)
    }

    /// Reports whether this node is an operand wildcard for the engine.
    #[must_use]
    pub const fn matches_operand_any(&self, engine: EngineType) -> bool {
        self.engine_types.contains(engine) && matches!(self.operand, Operand::Any)
    }

    /// Replaces children in source order.
    pub fn set_children<I>(&mut self, children: I)
    where
        I: IntoIterator<Item = Self>,
    {
        self.children = children.into_iter().collect();
    }
}

/// Constructs a pattern node without children.
#[must_use]
pub const fn new_pattern(operand: Operand, engine_types: EngineTypeSet) -> Pattern {
    Pattern {
        operand,
        engine_types,
        children: Vec::new(),
    }
}

/// Constructs a pattern node with source-ordered children.
#[must_use]
pub fn build_pattern(
    operand: Operand,
    engine_types: EngineTypeSet,
    children: impl IntoIterator<Item = Pattern>,
) -> Pattern {
    Pattern {
        operand,
        engine_types,
        children: children.into_iter().collect(),
    }
}
