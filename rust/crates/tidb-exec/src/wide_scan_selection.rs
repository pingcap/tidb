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

//! The wide SQL path's lowering of pushed scan predicates onto the bounded
//! physical Selection.
//!
//! The driver splits a `WHERE` into the conjuncts a base-table scan applies
//! itself and the residual above it
//! ([`tidb_executor::scan_pushdown`]). Those pushed conjuncts are described
//! independently of how they are evaluated, which is exactly the description
//! the read-only node's Selection already accepts: this module converts one
//! into the other and hands the result to
//! [`crate::dag_request::construct_read_only_dag_req`], so a coprocessor
//! request carries the predicate rather than the whole table crossing the
//! network.
//!
//! # Why this is a second entry point and not a widened one
//!
//! [`tidb_planner::read_only_scan`] binds its own conditions and refuses
//! everything outside a captured set; that refusal set is a contract its
//! tests pin. This module does not touch it. It starts from an already-split
//! wide-path predicate and applies the *same* narrow acceptance test
//! independently -- signed BIGINT column against a signed integer literal --
//! so a shape the bounded binder refuses cannot enter the DAG through here
//! either.
//!
//! # What a caller must still do
//!
//! A coprocessor Selection filters the **snapshot** only. The session's
//! staged mutation buffer is merged into the read client-side and never
//! passed through TiKV, so a caller that lowers a predicate through here must
//! keep applying the full predicate to the merged staged rows. That is the
//! promise [`tidb_executor::table_access::TableAccess::accept_scan_filter`] governs,
//! and a source that cannot keep it must refuse the push-down instead.

use std::error::Error;
use std::fmt;

use tidb_datatype::{Datum, FieldTypeCode};
use tidb_executor::scan_pushdown::{ScanComparison, ScanComparisonOp};
use tidb_planner::physical_selection::{
    BigIntComparison, ComparisonOp, ComparisonOperand, PhysicalSelectionError,
    PhysicalSelectionPlan,
};

/// Why a pushed conjunct cannot become a coprocessor Selection condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WideScanSelectionError {
    /// Nothing was pushed, so there is no Selection to build.
    NoConditions,
    /// The compared column is not the signed BIGINT this lowering accepts.
    UnsupportedColumnType {
        /// The refused column's scan input offset.
        offset: u32,
    },
    /// The constant is not a signed integer literal.
    UnsupportedLiteral {
        /// The refused condition's scan input offset.
        offset: u32,
    },
    /// The bounded Selection rejected the assembled conditions.
    Selection(PhysicalSelectionError),
}

impl fmt::Display for WideScanSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoConditions => {
                formatter.write_str("no pushed conjunct to lower into a Selection")
            }
            Self::UnsupportedColumnType { offset } => write!(
                formatter,
                "scan input offset {offset} is not a signed BIGINT column"
            ),
            Self::UnsupportedLiteral { offset } => write!(
                formatter,
                "the constant compared with scan input offset {offset} is not a signed integer"
            ),
            Self::Selection(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for WideScanSelectionError {}

impl From<PhysicalSelectionError> for WideScanSelectionError {
    fn from(error: PhysicalSelectionError) -> Self {
        Self::Selection(error)
    }
}

/// Lowers every pushed conjunct onto one bounded physical Selection.
///
/// Operand order is preserved rather than canonicalized, matching what the
/// source protobuf encodes for `5 < a` versus `a > 5`.
pub fn wide_scan_selection_plan(
    comparisons: &[ScanComparison],
) -> Result<PhysicalSelectionPlan, WideScanSelectionError> {
    if comparisons.is_empty() {
        return Err(WideScanSelectionError::NoConditions);
    }
    let conditions = comparisons
        .iter()
        .map(bigint_condition)
        .collect::<Result<Vec<_>, _>>()?;
    PhysicalSelectionPlan::from_bigint_conditions(conditions).map_err(WideScanSelectionError::from)
}

fn bigint_condition(
    comparison: &ScanComparison,
) -> Result<BigIntComparison, WideScanSelectionError> {
    let offset = comparison.column_offset;
    // TiKV evaluates the signed-BIGINT comparison this lowering encodes; an
    // unsigned column or a narrower integer type has different overflow and
    // sign semantics, so it stays out.
    if comparison.column_type.code() != FieldTypeCode::LongLong
        || comparison.column_type.is_unsigned()
    {
        return Err(WideScanSelectionError::UnsupportedColumnType { offset });
    }
    let Datum::Int(literal) = comparison.literal else {
        return Err(WideScanSelectionError::UnsupportedLiteral { offset });
    };
    let column = ComparisonOperand::InputOffset(offset);
    let constant = ComparisonOperand::Int(literal);
    let (lhs, rhs) = if comparison.column_on_left {
        (column, constant)
    } else {
        (constant, column)
    };
    BigIntComparison::new(comparison_op(comparison.op), lhs, rhs)
        .map_err(WideScanSelectionError::from)
}

const fn comparison_op(op: ScanComparisonOp) -> ComparisonOp {
    match op {
        ScanComparisonOp::Eq => ComparisonOp::Eq,
        ScanComparisonOp::Ne => ComparisonOp::Ne,
        ScanComparisonOp::Lt => ComparisonOp::Lt,
        ScanComparisonOp::Le => ComparisonOp::Le,
        ScanComparisonOp::Gt => ComparisonOp::Gt,
        ScanComparisonOp::Ge => ComparisonOp::Ge,
    }
}
