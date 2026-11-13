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

//! Go `pkg/planner/cascades/pattern`: operands (`pattern.go`), engine types
//! (`engine.go`) and the patterns rules match against the memo.

use tidb_planner_core_operator_logicalop::LogicalPlan;

/// Go `EngineType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineType(u32);

impl EngineType {
    /// Go `EngineTiDB`.
    pub const TIDB: Self = Self(1);
    /// Go `EngineTiKV`.
    pub const TIKV: Self = Self(1 << 1);
    /// Go `EngineTiFlash`.
    pub const TIFLASH: Self = Self(1 << 2);

    /// Go `EngineType.String()`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self.0 {
            1 => "EngineTiDB",
            2 => "EngineTiKV",
            4 => "EngineTiFlash",
            _ => "UnknownEngineType",
        }
    }
}

/// Go `EngineTypeSet`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineTypeSet(u32);

impl EngineTypeSet {
    /// Go `EngineTiDBOnly`.
    pub const TIDB_ONLY: Self = Self(EngineType::TIDB.0);
    /// Go `EngineTiKVOnly`.
    pub const TIKV_ONLY: Self = Self(EngineType::TIKV.0);
    /// Go `EngineTiFlashOnly`.
    pub const TIFLASH_ONLY: Self = Self(EngineType::TIFLASH.0);
    /// Go `EngineTiKVOrTiFlash`.
    pub const TIKV_OR_TIFLASH: Self = Self(EngineType::TIKV.0 | EngineType::TIFLASH.0);
    /// Go `EngineAll`.
    pub const ALL: Self = Self(EngineType::TIDB.0 | EngineType::TIKV.0 | EngineType::TIFLASH.0);

    /// Go `Contains(tp)`.
    #[must_use]
    pub const fn contains(self, tp: EngineType) -> bool {
        self.0 & tp.0 != 0
    }
}

/// Go `Operand`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Operand {
    /// Go `OperandAny`.
    Any,
    /// Go `OperandJoin`.
    Join,
    /// Go `OperandAggregation`.
    Aggregation,
    /// Go `OperandProjection`.
    Projection,
    /// Go `OperandSelection`.
    Selection,
    /// Go `OperandApply`.
    Apply,
    /// Go `OperandMaxOneRow`.
    MaxOneRow,
    /// Go `OperandTableDual`.
    TableDual,
    /// Go `OperandDataSource`.
    DataSource,
    /// Go `OperandUnionScan`.
    UnionScan,
    /// Go `OperandUnionAll`.
    UnionAll,
    /// Go `OperandSort`.
    Sort,
    /// Go `OperandTopN`.
    TopN,
    /// Go `OperandLock`.
    Lock,
    /// Go `OperandLimit`.
    Limit,
    /// Go `OperandTiKVSingleGather`.
    TiKVSingleGather,
    /// Go `OperandMemTableScan`.
    MemTableScan,
    /// Go `OperandTableScan`.
    TableScan,
    /// Go `OperandIndexScan`.
    IndexScan,
    /// Go `OperandShow`.
    Show,
    /// Go `OperandWindow`.
    Window,
    /// Go `OperandUnsupported`.
    Unsupported,
}

impl Operand {
    /// Go `Operand.String()`.
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
            Self::TiKVSingleGather => "OperandTiKVSingleGather",
            Self::MemTableScan => "OperandMemTableScan",
            Self::TableScan => "OperandTableScan",
            Self::IndexScan => "OperandIndexScan",
            Self::Show => "OperandShow",
            Self::Window => "OperandWindow",
            Self::Unsupported => "OperandUnsupported",
        }
    }

    /// Go `Operand.Match(t)`: `OperandAny` matches everything.
    #[must_use]
    pub fn matches(self, t: Self) -> bool {
        self == Self::Any || t == Self::Any || self == t
    }
}

/// Go `GetOperand(p)`.
#[must_use]
pub const fn get_operand(p: &LogicalPlan) -> Operand {
    match p {
        LogicalPlan::Apply(_) => Operand::Apply,
        LogicalPlan::Join(_) => Operand::Join,
        LogicalPlan::Aggregation(_) => Operand::Aggregation,
        LogicalPlan::Projection(_) => Operand::Projection,
        LogicalPlan::Selection(_) => Operand::Selection,
        LogicalPlan::MaxOneRow(_) => Operand::MaxOneRow,
        LogicalPlan::TableDual(_) => Operand::TableDual,
        LogicalPlan::DataSource(_) => Operand::DataSource,
        LogicalPlan::UnionScan(_) => Operand::UnionScan,
        LogicalPlan::UnionAll(_) => Operand::UnionAll,
        LogicalPlan::Sort(_) => Operand::Sort,
        LogicalPlan::TopN(_) => Operand::TopN,
        LogicalPlan::Lock(_) => Operand::Lock,
        LogicalPlan::Limit(_) => Operand::Limit,
        LogicalPlan::TiKVSingleGather(_) => Operand::TiKVSingleGather,
        LogicalPlan::TableScan(_) => Operand::TableScan,
        LogicalPlan::MemTable(_) => Operand::MemTableScan,
        LogicalPlan::IndexScan(_) => Operand::IndexScan,
        LogicalPlan::Show(_) => Operand::Show,
        LogicalPlan::Window(_) => Operand::Window,
        _ => Operand::Unsupported,
    }
}

/// Go `Pattern`: an operand on some engines, with child patterns.
#[derive(Clone, Debug)]
pub struct Pattern {
    /// Go's embedded `Operand`.
    pub operand: Operand,
    /// Go's embedded `EngineTypeSet`.
    pub engine_type_set: EngineTypeSet,
    /// Go `Children`.
    pub children: Vec<Pattern>,
}

impl Pattern {
    /// Go `NewPattern(operand, engineTypeSet)`.
    #[must_use]
    pub const fn new(operand: Operand, engine_type_set: EngineTypeSet) -> Self {
        Self {
            operand,
            engine_type_set,
            children: Vec::new(),
        }
    }

    /// Go `BuildPattern(operand, engineTypeSet, children...)`.
    #[must_use]
    pub fn build(operand: Operand, engine_type_set: EngineTypeSet, children: Vec<Self>) -> Self {
        Self {
            operand,
            engine_type_set,
            children,
        }
    }

    /// Go `SetChildren(children...)`.
    pub fn set_children(&mut self, children: Vec<Self>) {
        self.children = children;
    }

    /// Go `Pattern.Match(o, e)`.
    #[must_use]
    pub fn matches(&self, o: Operand, e: EngineType) -> bool {
        self.engine_type_set.contains(e) && self.operand.matches(o)
    }

    /// Go `MatchOperandAny(e)`.
    #[must_use]
    pub fn match_operand_any(&self, e: EngineType) -> bool {
        self.engine_type_set.contains(e) && self.operand == Operand::Any
    }
}
