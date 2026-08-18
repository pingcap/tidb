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

//! Go `pkg/planner/core/operator/logicalop/logical_projection.go`:
//! `LogicalProjection`, one expression per output column.
//!
//! SEED of `pkg/planner/core`. This file MERGES the crate's former
//! `logical_projection` identity leaf: the `Hash64`/`Equals` framing that
//! module modelled over a normalised column adapter now runs on the real
//! [`Expression`]/[`Schema`], as [`LogicalProjection::hash64`] and
//! [`LogicalProjection::equals`], and the adapter module is gone.

use tidb_expr::column::Column;
use tidb_expr::expr_util::substitute::{column_substitute_impl, SubstituteOptions};
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{
    extract_columns, extract_columns_from_expressions, extract_cor_columns,
};

use crate::hash_equaler::{new_hash_equaler, Hasher};
use crate::logical::schema_producer;
use crate::logical::BaseLogicalPlan;
use crate::stats_info::StatsInfo;

/// Go `logicalop.LogicalProjection` (`logical_projection.go:33`).
#[derive(Clone, Debug, Default)]
pub struct LogicalProjection {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `Exprs`: one expression per output column, in schema order.
    pub exprs: Vec<Expression>,
    /// Go `CalculateNoDelay`: set for the `select @@autocommit` style
    /// statement whose value must be computed before the response is sent.
    pub calculate_no_delay: bool,
    /// Go `Proj4Expand`: this projection re-allocates column ids for an
    /// `Expand`, so its column references are NOT real references and it must
    /// not be eliminated.
    pub proj4_expand: bool,
}

impl LogicalProjection {
    /// Go `tryTransformSortItems` (`logical_projection.go:553`): map each
    /// required-order column through this projection's exprs — a bare
    /// `Column` maps to the child column it projects, a `ScalarFunction`
    /// cannot preserve order and fails the whole transform. Go's switch has
    /// NO arm for `Constant`/`CorrelatedColumn`: such an item is silently
    /// DROPPED from the transformed list. That is a quirk visible through
    /// the API and is reproduced, not fixed.
    ///
    /// Go indexes `p.Exprs[idx]` with `Schema().ColumnIndex(col)` and would
    /// panic on a column absent from the schema; this returns failure for
    /// that impossible input instead.
    #[must_use]
    pub fn try_transform_sort_items(
        &self,
        items: &[crate::physical_property::SortItem],
    ) -> Option<Vec<crate::physical_property::SortItem>> {
        let schema = self.base.base.schema()?;
        let mut new_items = Vec::with_capacity(items.len());
        for item in items {
            let idx = schema
                .columns
                .iter()
                .position(|c| c.unique_id == item.col)?;
            match self.exprs.get(idx)? {
                Expression::Column(col) => {
                    new_items.push(crate::physical_property::SortItem::new(
                        col.unique_id,
                        item.desc,
                    ));
                }
                Expression::ScalarFunction(_) => return None,
                Expression::Constant(_) | Expression::CorrelatedColumn(_) => {}
            }
        }
        Some(new_items)
    }

    /// Go `TryToGetChildProp` (`logical_projection.go:524`): the parent's
    /// property expressed over this projection's child, or `None` when a
    /// required order runs through a computed expression. The
    /// `PartialOrderInfo` and `AdvisorySortItems` passes narrow with those
    /// unported fields.
    #[must_use]
    pub fn try_to_get_child_prop(
        &self,
        prop: &crate::physical_property::PhysicalProperty,
    ) -> Option<crate::physical_property::PhysicalProperty> {
        let mut new_prop = prop.clone_essential_fields();
        if !prop.sort_items.is_empty() {
            new_prop.sort_items = self.try_transform_sort_items(&prop.sort_items)?;
        }
        Some(new_prop)
    }

    /// Go `plancodec.TypeProj`.
    pub const TYPE: &'static str = "Projection";

    /// Go `LogicalProjection.Init(ctx, qbOffset)`
    /// (`logical_projection.go:50`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, exprs: Vec<Expression>) -> Self {
        Self {
            base,
            exprs,
            calculate_no_delay: false,
            proj4_expand: false,
        }
    }

    /// Go `LogicalProjection.GetUsedCols()` (`logical_projection.go:496`):
    /// every column any projected expression reads, WITHOUT deduplication
    /// across expressions.
    #[must_use]
    pub fn get_used_cols(&self) -> Vec<Column> {
        let mut used = Vec::new();
        for expr in &self.exprs {
            used.extend(extract_columns(expr));
        }
        used
    }

    /// Go `LogicalProjection.ExtractCorrelatedCols()`
    /// (`logical_projection.go:367`).
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            cor_cols.extend(extract_cor_columns(expr));
        }
        cor_cols
    }

    /// Go `canProjectionBeEliminatedLoose(p)` (`logical_projection.go:663`):
    /// every expression is a bare column reference and this is not an
    /// `Expand` projection.
    #[must_use]
    pub fn can_be_eliminated_loose(&self) -> bool {
        !self.proj4_expand
            && self
                .exprs
                .iter()
                .all(|expr| matches!(expr, Expression::Column(_)))
    }

    /// Go `breakDownPredicates(p, predicates)`
    /// (`logical_projection.go:647`): rewrite each predicate through this
    /// projection's expressions, splitting into those that survived the
    /// rewrite and those that did not.
    ///
    /// A predicate is pushable only when it was actually substituted, the
    /// substitution did not fail, and the result contains no `GET_VAR`/
    /// `SET_VAR`.
    #[must_use]
    pub fn break_down_predicates(
        &self,
        predicates: &[Expression],
        schema: &Schema,
        opts: &SubstituteOptions<'_>,
    ) -> (Vec<Expression>, Vec<Expression>) {
        let mut can_be_pushed = Vec::with_capacity(predicates.len());
        let mut cannot_be_pushed = Vec::with_capacity(predicates.len());
        for cond in predicates {
            let outcome = column_substitute_impl(cond, schema, &self.exprs, true, opts);
            if outcome.substituted
                && !outcome.has_fail
                && !tidb_expr::evaluator::has_get_set_var_func(&outcome.expr)
            {
                can_be_pushed.push(outcome.expr);
            } else {
                cannot_be_pushed.push(cond.clone());
            }
        }
        (can_be_pushed, cannot_be_pushed)
    }

    /// Go `LogicalProjection.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_projection.go:105`): drop every output the parent does not
    /// use, then report the columns the child must still produce.
    ///
    /// Returns `(child_used_cols, self_became_empty)`. Go's two escapes from
    /// this body are decided by the enum-level driver, which owns the child:
    /// * an all-pruned projection over a `LogicalTableDual` is rewritten to a
    ///   single constant column, and
    /// * a projection left with no columns is replaced by its child.
    ///
    /// An expression with `SET_VAR`/`SLEEP` is never pruned, because dropping
    /// it would drop its side effect.
    pub fn prune_columns_local(
        &mut self,
        parent_used_cols: &[Column],
        schema: &mut Schema,
    ) -> (Vec<Column>, bool) {
        let mut used = schema_producer::get_used_list(parent_used_cols, schema);
        let mut all_pruned = true;
        for (i, keep) in used.iter_mut().enumerate() {
            if *keep || tidb_expr::expr_util::predicates::expr_has_set_var_or_sleep(&self.exprs[i])
            {
                *keep = true;
                all_pruned = false;
                break;
            }
        }
        if !all_pruned {
            for i in (0..used.len()).rev() {
                if !used[i]
                    && !tidb_expr::expr_util::predicates::expr_has_set_var_or_sleep(&self.exprs[i])
                {
                    schema.columns.remove(i);
                    self.exprs.remove(i);
                }
            }
        }
        let child_used = extract_columns_from_expressions(&self.exprs, None);
        (child_used, schema.columns.is_empty())
    }

    /// Go `LogicalProjection.buildSchemaByExprs(selfSchema)`
    /// (`logical_projection.go:505`): the schema formed by the projected
    /// expressions themselves, so a child key can be matched positionally.
    ///
    /// Go allocates a fresh plan column id for every non-column expression;
    /// there is no id allocator on this operator, so the placeholder is a
    /// column with `unique_id` set to the SENTINEL `i64::MIN + position`.
    /// A sentinel can never collide with a real allocation and can never
    /// match a child key, which is exactly the role Go's fresh id plays.
    #[must_use]
    pub fn build_schema_by_exprs(&self) -> Schema {
        let columns = self
            .exprs
            .iter()
            .enumerate()
            .map(|(i, expr)| match expr {
                Expression::Column(column) => column.clone(),
                _ => {
                    let mut placeholder = Column::default();
                    placeholder.unique_id = i64::MIN + i as i64;
                    placeholder
                }
            })
            .collect();
        Schema::new(columns)
    }

    /// Go `LogicalProjection.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_projection.go:163`): carry a child key forward when every one
    /// of its columns is projected, mapping through
    /// [`Self::build_schema_by_exprs`] rather than through `selfSchema`,
    /// because the projection renames.
    pub fn build_key_info(&self, self_schema: &mut Schema, child_schema: &[Schema]) {
        self_schema.pk_or_uk.clear();
        let Some(child) = child_schema.first() else {
            return;
        };
        let by_exprs = self.build_schema_by_exprs();
        let mut carried = Vec::new();
        for key in &child.pk_or_uk {
            let Some(indices) = by_exprs.columns_indices(key) else {
                continue;
            };
            if indices.iter().any(|i| *i >= self_schema.columns.len()) {
                continue;
            }
            carried.push(
                indices
                    .into_iter()
                    .map(|i| self_schema.columns[i].clone())
                    .collect::<Vec<_>>(),
            );
        }
        self_schema.pk_or_uk = carried;
    }

    /// Go `LogicalProjection.DeriveStats(childStats, selfSchema, childSchema,
    /// reloads)` (`logical_projection.go:278`).
    ///
    /// The row count passes through unchanged; each output column's NDV is the
    /// NDV of the columns its expression reads.
    ///
    /// # Blocked
    ///
    /// Go calls `cardinality.EstimateColsNDVWithMatchedLen(sctx, cols,
    /// childSchema[0], childProfile)` per expression. That estimator needs the
    /// session and the child histogram collection; without it, a column whose
    /// expression reads exactly ONE child column adopts that column's NDV —
    /// which is what the Go estimator returns in that case — and every other
    /// column is left out of the map rather than guessed.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let child = child_stats.first()?;
        let mut col_ndvs = Vec::new();
        for (i, expr) in self.exprs.iter().enumerate() {
            let Some(output) = self_schema.columns.get(i) else {
                break;
            };
            let read = extract_columns(expr);
            if read.len() == 1 {
                if let Some(ndv) = child.col_ndvs().get(&read[0].unique_id) {
                    col_ndvs.push((output.unique_id, *ndv));
                }
            }
        }
        let stats = StatsInfo::new(child.row_count(), col_ndvs);
        self.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// Go `LogicalProjection.Hash64(h)`
    /// (`logicalop/hash64_equals_generated.go`): the embedded schema producer,
    /// then the expression count and each expression, then the two flags.
    ///
    /// This is the merged form of the crate's former
    /// `logical_projection::LogicalProjectionIdentity`.
    #[must_use]
    pub fn hash64(&self, schema: Option<&Schema>) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_uint64(schema_producer::schema_hash64(schema));
        hasher.hash_int(self.exprs.len() as i64);
        for expr in &self.exprs {
            let mut expr = expr.clone();
            hasher.hash_bytes(expr.hash_code());
        }
        hasher.hash_bool(self.calculate_no_delay);
        hasher.hash_bool(self.proj4_expand);
        hasher.sum64()
    }

    /// Go `LogicalProjection.Equals(other)`: schema identity, then the
    /// expression list in order, then the two flags.
    #[must_use]
    pub fn equals(
        &self,
        self_schema: Option<&Schema>,
        other: &Self,
        other_schema: Option<&Schema>,
    ) -> bool {
        schema_producer::schema_equals(self_schema, other_schema)
            && schema_producer::expression_lists_equal(&self.exprs, &other.exprs)
            && self.calculate_no_delay == other.calculate_no_delay
            && self.proj4_expand == other.proj4_expand
    }
}

impl LogicalProjection {
    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            exprs: self.exprs.clone(),
            calculate_no_delay: self.calculate_no_delay,
            proj4_expand: self.proj4_expand,
        }
    }
}
