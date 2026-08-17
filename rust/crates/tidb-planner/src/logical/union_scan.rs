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

//! Go `pkg/planner/core/operator/logicalop/logical_union_scan.go`:
//! `LogicalUnionScan`, "used in non read-only txn or for scanning a local
//! temporary table whose snapshot data is located in memory".
//!
//! SEED of `pkg/planner/core`. This operator was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! # Narrowings, by name
//!
//! * `HandleCols util.HandleCols` becomes the handle COLUMNS, the same
//!   narrowing [`crate::logical::DataSource`] makes.
//! * `ExplainInfo` renders the conditions with
//!   `expression.SortedExplainExpressionList(evalCtx, ...)`, which needs an
//!   `EvalContext`; the condition COUNT is reported instead, as
//!   [`crate::logical::LogicalJoin::explain_info`] does.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_columns_from_expressions};

use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;

/// Go `model.ExtraPhysTblID` (`pkg/meta/model/column.go`): the `_tidb_tid`
/// pseudo-column that names the partition a row came from.
///
/// A literal here rather than an import: `pkg/meta/model` is not a dependency
/// of this crate, and this constant is dependency-closed.
pub const EXTRA_PHYS_TBL_ID: i64 = -3;

/// Go `logicalop.LogicalUnionScan` (`logical_union_scan.go:28`).
#[derive(Clone, Debug, Default)]
pub struct LogicalUnionScan {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Conditions`: applied to the rows the TRANSACTION added, not to the
    /// snapshot rows the child already filtered.
    pub conditions: Vec<Expression>,
    /// Go `HandleCols`' columns.
    pub handle_cols: Vec<Column>,
}

/// What [`LogicalUnionScan::predicate_push_down`] split.
#[derive(Clone, Debug)]
pub struct UnionScanPredicateSplit {
    /// The predicates the child may push down. These are ALSO installed as this
    /// operator's own [`LogicalUnionScan::conditions`].
    pub without_virtual_column: Vec<Expression>,
    /// The predicates that must stay above, because they read a virtual column.
    pub with_virtual_column: Vec<Expression>,
}

impl LogicalUnionScan {
    /// Go `plancodec.TypeUnionScan`.
    pub const TYPE: &'static str = "UnionScan";

    /// Go `LogicalUnionScan.Init(ctx, qbOffset)`
    /// (`logical_union_scan.go:36`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, handle_cols: Vec<Column>) -> Self {
        Self {
            base,
            conditions: Vec::new(),
            handle_cols,
        }
    }

    /// Go `LogicalUnionScan.PredicatePushDown(predicates)`'s LOCAL half
    /// (`logical_union_scan.go:58`).
    ///
    /// Go's reason for the split, with its issue number: "predicates with
    /// virtual columns can't be pushed down to TiKV/TiFlash so they'll be put
    /// into a Projection below the UnionScan, but the current UnionScan doesn't
    /// support placing Projection below it, see #53951."
    ///
    /// The driver then pushes `without_virtual_column` into the child, COPIES
    /// that same list into [`Self::conditions`] — Go's comment: "The conditions
    /// in UnionScan is only used for added rows, so parent Selection should not
    /// be removed" — and returns the child's retained predicates PLUS
    /// `with_virtual_column`. A child that became a `LogicalTableDual` replaces
    /// this operator entirely.
    #[must_use]
    pub fn predicate_push_down(predicates: &[Expression]) -> UnionScanPredicateSplit {
        let mut with_virtual_column = Vec::new();
        let mut without_virtual_column = Vec::new();
        for expr in predicates {
            if contains_virtual_column(expr) {
                with_virtual_column.push(expr.clone());
            } else {
                without_virtual_column.push(expr.clone());
            }
        }
        UnionScanPredicateSplit {
            without_virtual_column,
            with_virtual_column,
        }
    }

    /// Go `LogicalUnionScan.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_union_scan.go:88`): the handle columns, the partition-id
    /// column, and every column the conditions read must all survive.
    ///
    /// The handle is needed because the union scan MERGES the transaction's
    /// added rows with the snapshot's by handle; without it the merge has no
    /// key.
    #[must_use]
    pub fn prune_columns_local(&self, parent_used_cols: &[Column], schema: &Schema) -> Vec<Column> {
        let mut used = parent_used_cols.to_vec();
        used.extend(self.handle_cols.iter().cloned());
        used.extend(
            schema
                .columns
                .iter()
                .filter(|col| col.id == EXTRA_PHYS_TBL_ID)
                .cloned(),
        );
        used.extend(extract_columns_from_expressions(&self.conditions, None));
        used
    }

    /// Go `LogicalUnionScan.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_union_scan.go:120`): the child's orders pass through, and
    /// TiFlash is UNCONDITIONALLY false.
    ///
    /// Go's comment on the order pass-through: "ref
    /// exhaustPhysicalPlans4LogicalUnionScan: it will push down the sort prop
    /// directly. in union scan exec, it will feel the underlying tableReader or
    /// indexReader to get the keepOrder." The TiFlash answer is false because a
    /// union scan reads the transaction's local buffer, which TiFlash has no
    /// view of.
    pub fn prepare_possible_properties(
        &mut self,
        child: Option<&PossiblePropertiesInfo>,
    ) -> PossiblePropertiesInfo {
        self.base.set_has_tiflash(false);
        PossiblePropertiesInfo {
            orders: child.map(|info| info.orders.clone()).unwrap_or_default(),
            has_tiflash: false,
        }
    }

    /// Go `LogicalUnionScan.ExplainInfo()` (`logical_union_scan.go:45`); see
    /// this module's header for the condition count.
    #[must_use]
    pub fn explain_info(&self) -> String {
        format!(
            "conds:{} exprs, handle:{} cols",
            self.conditions.len(),
            self.handle_cols.len()
        )
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            conditions: self.conditions.clone(),
            handle_cols: self.handle_cols.clone(),
        }
    }
}

/// Go `expression.ContainVirtualColumn([]Expression{expr})`
/// (`pkg/expression/util.go`): whether any column the expression reads is a
/// GENERATED column, i.e. carries a `VirtualExpr`.
#[must_use]
pub fn contains_virtual_column(expr: &Expression) -> bool {
    extract_columns(expr)
        .iter()
        .any(|col| col.virtual_expr.is_some())
}
