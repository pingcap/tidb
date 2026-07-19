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

//! PhysicalSelection metadata from
//! `pkg/planner/core/operator/physicalop/physical_selection.go`.
//!
//! The Go selection owns typed expressions, session/context, property and
//! statistics propagation, cost, clone, index resolution, protobuf encoding,
//! and task attachment. This leaf preserves Init plan identity/offset, the
//! source ExplainInfo suffix over caller-supplied sorted expression text, and
//! the resolved signed-BIGINT comparison conditions consumed by the bounded
//! TiKV executor-list lowering. Expression protobuf encoding and execution
//! remain external boundaries.

use std::error::Error;
use std::fmt;

/// The source plan-codec type assigned by `PhysicalSelection.Init`.
pub const PLAN_TYPE: &str = "Selection";

/// Bounded comparison operators accepted by the signed-BIGINT Selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOp {
    /// Strictly less than.
    Lt,
    /// Less than or equal to.
    Le,
    /// Strictly greater than.
    Gt,
    /// Greater than or equal to.
    Ge,
    /// Equal to.
    Eq,
    /// Not equal to.
    Ne,
}

/// One already-resolved operand in the Selection scan input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOperand {
    /// Zero-based offset in the ordered table-scan input.
    InputOffset(u32),
    /// A signed integer literal.
    Int(i64),
}

/// One resolved signed-BIGINT column-versus-integer comparison.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BigIntComparison {
    op: ComparisonOp,
    lhs: ComparisonOperand,
    rhs: ComparisonOperand,
}

impl BigIntComparison {
    /// Creates a comparison with exactly one scan input and one integer.
    pub const fn new(
        op: ComparisonOp,
        lhs: ComparisonOperand,
        rhs: ComparisonOperand,
    ) -> Result<Self, PhysicalSelectionError> {
        if !matches!(
            (lhs, rhs),
            (ComparisonOperand::InputOffset(_), ComparisonOperand::Int(_))
                | (ComparisonOperand::Int(_), ComparisonOperand::InputOffset(_))
        ) {
            return Err(PhysicalSelectionError::InvalidComparisonOperands);
        }
        Ok(Self { op, lhs, rhs })
    }

    /// Returns the comparison operator.
    #[must_use]
    pub const fn op(self) -> ComparisonOp {
        self.op
    }

    /// Returns the left operand without canonicalizing operand order.
    #[must_use]
    pub const fn lhs(self) -> ComparisonOperand {
        self.lhs
    }

    /// Returns the right operand without canonicalizing operand order.
    #[must_use]
    pub const fn rhs(self) -> ComparisonOperand {
        self.rhs
    }

    /// Returns the single resolved scan-input offset.
    #[must_use]
    pub const fn input_offset(self) -> u32 {
        match (self.lhs, self.rhs) {
            (ComparisonOperand::InputOffset(offset), ComparisonOperand::Int(_))
            | (ComparisonOperand::Int(_), ComparisonOperand::InputOffset(offset)) => offset,
            _ => unreachable!(),
        }
    }
}

/// Fail-closed construction error for the bounded physical Selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalSelectionError {
    /// A physical Selection must contain at least one condition.
    EmptyConditions,
    /// This boundary accepts exactly one input offset and one signed integer.
    InvalidComparisonOperands,
}

impl fmt::Display for PhysicalSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyConditions => formatter.write_str("physical Selection has no conditions"),
            Self::InvalidComparisonOperands => formatter.write_str(
                "signed-BIGINT comparison requires one input offset and one integer literal",
            ),
        }
    }
}

impl Error for PhysicalSelectionError {}

/// Physical layout used when this Selection is pushed to TiKV.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectionPushdownLayout {
    /// TiKV receives the scan and Selection as sibling entries in DAG order.
    TiKvExecutorList,
}

/// Minimal initialized physical Selection metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalSelectionPlan {
    condition_explain: String,
    query_block_offset: i32,
    stream_count: u64,
    conditions: Vec<BigIntComparison>,
}

impl PhysicalSelectionPlan {
    /// Initializes source-shaped Selection metadata.
    #[must_use]
    pub fn init(
        condition_explain: impl Into<String>,
        query_block_offset: i32,
        stream_count: u64,
    ) -> Self {
        Self {
            condition_explain: condition_explain.into(),
            query_block_offset,
            stream_count,
            conditions: Vec::new(),
        }
    }

    /// Initializes a bounded executable Selection with resolved conditions.
    ///
    /// The metadata-only [`Self::init`] constructor remains available for the
    /// pre-existing plan/explain surface. Runtime lowering must use this
    /// constructor so an empty Selection cannot enter the TiKV executor list.
    pub fn with_conditions(
        condition_explain: impl Into<String>,
        query_block_offset: i32,
        stream_count: u64,
        conditions: Vec<BigIntComparison>,
    ) -> Result<Self, PhysicalSelectionError> {
        if conditions.is_empty() {
            return Err(PhysicalSelectionError::EmptyConditions);
        }
        Ok(Self {
            condition_explain: condition_explain.into(),
            query_block_offset,
            stream_count,
            conditions,
        })
    }

    /// Creates the bounded runtime Selection with canonical metadata defaults.
    ///
    /// The read-only binder owns resolved semantics, not explain rendering or
    /// query-block bookkeeping. Those independent source surfaces therefore
    /// remain empty/zero unless a metadata-aware caller uses
    /// [`Self::with_conditions`].
    pub fn from_bigint_conditions(
        conditions: Vec<BigIntComparison>,
    ) -> Result<Self, PhysicalSelectionError> {
        Self::with_conditions("", 0, 0, conditions)
    }

    /// Returns the source plan-codec type.
    #[must_use]
    pub const fn plan_type(&self) -> &'static str {
        PLAN_TYPE
    }

    /// Returns the caller-owned query-block offset passed to Init.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns the caller-supplied sorted condition explain text.
    #[must_use]
    pub fn condition_explain(&self) -> &str {
        &self.condition_explain
    }

    /// Returns TiFlash's fine-grained shuffle stream count.
    #[must_use]
    pub const fn stream_count(&self) -> u64 {
        self.stream_count
    }

    /// Returns resolved conditions in SQL/flattened-AND order.
    #[must_use]
    pub fn conditions(&self) -> &[BigIntComparison] {
        &self.conditions
    }

    /// Returns one input offset per condition, preserving condition order.
    pub fn condition_input_offsets(&self) -> impl ExactSizeIterator<Item = u32> + '_ {
        self.conditions
            .iter()
            .map(|condition| condition.input_offset())
    }

    /// Returns the bounded source-shaped TiKV executor-list contract.
    #[must_use]
    pub const fn pushdown_layout(&self) -> SelectionPushdownLayout {
        SelectionPushdownLayout::TiKvExecutorList
    }

    /// TiKV list-form Selection does not embed its scan as `Selection.child`.
    #[must_use]
    pub const fn tikv_embeds_child(&self) -> bool {
        false
    }

    /// Returns source ExplainInfo with the optional stream-count suffix.
    #[must_use]
    pub fn explain_info(&self) -> String {
        if self.stream_count == 0 {
            self.condition_explain.clone()
        } else {
            format!(
                "{}, stream_count: {}",
                self.condition_explain, self.stream_count
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PhysicalSelectionPlan, PLAN_TYPE};

    #[test]
    fn init_preserves_selection_kind_offset_and_expression_text() {
        let plan = PhysicalSelectionPlan::init("gt(a, 1)", -2, 0);
        assert_eq!(plan.plan_type(), PLAN_TYPE);
        assert_eq!(plan.plan_type(), "Selection");
        assert_eq!(plan.query_block_offset(), -2);
        assert_eq!(plan.condition_explain(), "gt(a, 1)");
    }

    #[test]
    fn zero_stream_count_returns_expression_text_without_suffix() {
        let plan = PhysicalSelectionPlan::init("eq(a, 1)", 0, 0);
        assert_eq!(plan.stream_count(), 0);
        assert_eq!(plan.explain_info(), "eq(a, 1)");
    }

    #[test]
    fn positive_stream_count_matches_source_suffix() {
        let plan = PhysicalSelectionPlan::init("gt(a, 1)", 0, 10);
        assert_eq!(plan.explain_info(), "gt(a, 1), stream_count: 10");
    }

    #[test]
    fn empty_expression_text_keeps_source_separator_behavior() {
        assert_eq!(
            PhysicalSelectionPlan::init("", 0, 3).explain_info(),
            ", stream_count: 3"
        );
    }
}
