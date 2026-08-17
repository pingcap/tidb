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

//! Go `pkg/planner/core/operator/logicalop/logical_aggregation.go`:
//! `LogicalAggregation`, a `GROUP BY` over aggregate functions.
//!
//! SEED of `pkg/planner/core`. The aggregate descriptors are
//! [`tidb_expr::aggregation::AggFuncDesc`] — the workspace's canonical
//! `aggregation.AggFuncDesc`, reused here rather than restated.
//!
//! This file MERGES the crate's former `logical_aggregation` identity leaf:
//! its `Hash64`/`Equals` framing over normalised adapters now runs on the real
//! descriptors and expressions, as [`LogicalAggregation::hash64`] /
//! [`LogicalAggregation::equals`], and the adapter module is gone.

use tidb_expr::aggregation::AggFuncDesc;
use tidb_expr::aggregation::AggFunctionMode;
use tidb_expr::column::Column;
use tidb_expr::expression::{ConstLevel, CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};

use crate::hash_equaler::{new_hash_equaler, Hasher};
use crate::logical::schema_producer;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `ast.AggFuncFirstRow`.
pub const AGG_FUNC_FIRST_ROW: &str = "firstrow";
/// Go `ast.AggFuncCount`.
pub const AGG_FUNC_COUNT: &str = "count";
/// Go `ast.AggFuncMax`.
pub const AGG_FUNC_MAX: &str = "max";
/// Go `ast.AggFuncMin`.
pub const AGG_FUNC_MIN: &str = "min";

/// Go `logicalop.LogicalAggregation` (`logical_aggregation.go:38`).
#[derive(Clone, Debug, Default)]
pub struct LogicalAggregation {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `AggFuncs`: one descriptor per output column, in schema order.
    pub agg_funcs: Vec<AggFuncDesc>,
    /// Go `GroupByItems`.
    pub group_by_items: Vec<Expression>,
    /// Go `PreferAggType`: the `HASH_AGG` / `STREAM_AGG` hint bit set.
    pub prefer_agg_type: u32,
    /// Go `PreferAggToCop`: the `AGG_TO_COP` hint.
    pub prefer_agg_to_cop: bool,
    /// Go `PossibleProperties`, filled by `PreparePossibleProperties`.
    pub possible_properties: PossiblePropertiesInfo,
    /// Go `InputCount`: the child's row count, recorded by `DeriveStats`.
    pub input_count: f64,
    /// Go `NoCopPushDown`: this aggregate must not be pushed to a coprocessor.
    pub no_cop_push_down: bool,
}

impl LogicalAggregation {
    /// Go `plancodec.TypeAgg`.
    pub const TYPE: &'static str = "Aggregation";

    /// Go `LogicalAggregation.Init(ctx, offset)`
    /// (`logical_aggregation.go:58`).
    #[must_use]
    pub fn new(
        base: BaseLogicalPlan,
        agg_funcs: Vec<AggFuncDesc>,
        group_by_items: Vec<Expression>,
    ) -> Self {
        Self {
            base,
            agg_funcs,
            group_by_items,
            ..Self::default()
        }
    }

    /// Go `HasDistinct()` (`logical_aggregation.go:464`).
    #[must_use]
    pub fn has_distinct(&self) -> bool {
        self.agg_funcs.iter().any(|func| func.has_distinct)
    }

    /// Go `HasOrderBy()` (`logical_aggregation.go:474`).
    #[must_use]
    pub fn has_order_by(&self) -> bool {
        self.agg_funcs
            .iter()
            .any(|func| !func.order_by_items.is_empty())
    }

    /// Go `CopyAggHints(agg)` (`logical_aggregation.go:484`).
    pub fn copy_agg_hints(&mut self, other: &Self) {
        self.prefer_agg_type = other.prefer_agg_type;
        self.prefer_agg_to_cop = other.prefer_agg_to_cop;
    }

    /// Go `IsPartialModeAgg()` (`logical_aggregation.go:495`).
    ///
    /// Go indexes `AggFuncs[0]` unguarded and panics on an empty aggregate;
    /// an empty one answers `false` here, which is the arm every caller takes
    /// after Go's own `len(la.AggFuncs) != 0` guard.
    #[must_use]
    pub fn is_partial_mode_agg(&self) -> bool {
        self.agg_funcs
            .first()
            .is_some_and(|func| func.mode == AggFunctionMode::Partial1)
    }

    /// Go `IsCompleteModeAgg()` (`logical_aggregation.go:501`).
    #[must_use]
    pub fn is_complete_mode_agg(&self) -> bool {
        self.agg_funcs
            .first()
            .is_some_and(|func| func.mode == AggFunctionMode::Complete)
    }

    /// Go `GetGroupByCols()` (`logical_aggregation.go:508`): the group-by
    /// items that ARE bare columns. `group by a, b, c+d` yields `[a, b]`.
    #[must_use]
    pub fn get_group_by_cols(&self) -> Vec<Column> {
        self.group_by_items
            .iter()
            .filter_map(|item| match item {
                Expression::Column(column) => Some(column.clone()),
                _ => None,
            })
            .collect()
    }

    /// Go `GetUsedCols()` (`logical_aggregation.go:533`): every column read by
    /// a group-by item, an aggregate argument, or an aggregate `ORDER BY`.
    #[must_use]
    pub fn get_used_cols(&self) -> Vec<Column> {
        let mut used = Vec::new();
        for item in &self.group_by_items {
            used.extend(extract_columns(item));
        }
        for desc in &self.agg_funcs {
            for arg in desc.args() {
                used.extend(extract_columns(arg));
            }
            for order in &desc.order_by_items {
                used.extend(extract_columns(&order.expr));
            }
        }
        used
    }

    /// Go `ExtractCorrelatedCols()` (`logical_aggregation.go:303`).
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.group_by_items.len() + self.agg_funcs.len());
        for expr in &self.group_by_items {
            cor_cols.extend(extract_cor_columns(expr));
        }
        for func in &self.agg_funcs {
            for arg in func.args() {
                cor_cols.extend(extract_cor_columns(arg));
            }
            for order in &func.order_by_items {
                cor_cols.extend(extract_cor_columns(&order.expr));
            }
        }
        cor_cols
    }

    /// Go `hasOnlyConstGroupByItems()` (`logical_aggregation.go:670`).
    #[must_use]
    pub fn has_only_const_group_by_items(&self) -> bool {
        self.group_by_items
            .iter()
            .all(|item| item.const_level() >= ConstLevel::ONLY_IN_CONTEXT)
    }

    /// Go `getAggFuncsColsForFirstRow()` (`logical_aggregation.go:716`): the
    /// output columns of single-column `firstrow()` aggregates.
    ///
    /// Empty when every group-by item is constant: such a group picks an
    /// ARBITRARY input row, so pushing a `HAVING` on a `firstrow()` output back
    /// to base rows would be unsound.
    #[must_use]
    pub fn agg_funcs_cols_for_first_row(&self, self_schema: &Schema) -> Vec<Column> {
        if self.has_only_const_group_by_items() {
            return Vec::new();
        }
        let mut cols = Vec::with_capacity(self.agg_funcs.len());
        for (idx, column) in self_schema.columns.iter().enumerate() {
            let Some(func) = self.agg_funcs.get(idx) else {
                break;
            };
            if func.name() == AGG_FUNC_FIRST_ROW
                && func
                    .args()
                    .first()
                    .is_some_and(|arg| extract_columns(arg).len() == 1)
            {
                cols.push(column.clone());
            }
        }
        cols
    }

    /// Go `getAggFuncsColsForConstResult()` (`logical_aggregation.go:684`):
    /// the outputs whose value equals their single row-independent argument in
    /// every non-empty group, paired with that argument.
    #[must_use]
    pub fn agg_funcs_cols_for_const_result(
        &self,
        self_schema: &Schema,
    ) -> (Vec<Column>, Vec<Expression>) {
        if self.group_by_items.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let mut cols = Vec::with_capacity(self.agg_funcs.len());
        let mut exprs = Vec::with_capacity(self.agg_funcs.len());
        for (idx, column) in self_schema.columns.iter().enumerate() {
            let Some(func) = self.agg_funcs.get(idx) else {
                break;
            };
            if agg_func_result_matches_arg_for_non_empty_group(func) {
                cols.push(column.clone());
                exprs.push(func.args()[0].clone());
            }
        }
        (cols, exprs)
    }

    /// Go `BuildSelfKeyInfo(selfSchema)` (`logical_aggregation.go:797`): the
    /// group-by columns are a key of the output, and a group-less aggregate is
    /// exactly one row.
    pub fn build_self_key_info(&mut self, self_schema: &mut Schema) {
        let group_by_cols = self.get_group_by_cols();
        if !self.group_by_items.is_empty() && group_by_cols.len() == self.group_by_items.len() {
            if let Some(indices) = self_schema.columns_indices(&group_by_cols) {
                let new_key = indices
                    .into_iter()
                    .map(|i| self_schema.columns[i].clone())
                    .collect();
                self_schema.pk_or_uk.push(new_key);
            }
        }
        if self.group_by_items.is_empty() {
            self.base.set_max_one_row(true);
        }
    }

    /// Go `LogicalAggregation.BuildKeyInfo(selfSchema, childSchema)`
    /// (`logical_aggregation.go:194`): a PARTIAL-mode aggregate with functions
    /// contributes nothing, otherwise the schema producer's key propagation
    /// runs and then [`Self::build_self_key_info`].
    pub fn build_key_info(&mut self, self_schema: &mut Schema, child_schema: &[Schema]) {
        if !self.agg_funcs.is_empty() && self.is_partial_mode_agg() {
            return;
        }
        schema_producer::propagate_child_keys(self_schema, child_schema);
        self.build_self_key_info(self_schema);
    }

    /// Go `LogicalAggregation.ExtractColGroups(_)`
    /// (`logical_aggregation.go:250`): the parent's groups are DISCARDED, and
    /// the group-by columns are asked for as one group when there is more than
    /// one of them.
    #[must_use]
    pub fn extract_col_groups(&self) -> Vec<Vec<Column>> {
        let mut gby_cols = Vec::new();
        for item in &self.group_by_items {
            gby_cols.extend(extract_columns(item));
        }
        if gby_cols.len() > 1 {
            gby_cols.sort_by_key(|column| column.unique_id);
            return vec![gby_cols];
        }
        Vec::new()
    }

    /// Go `LogicalAggregation.PreparePossibleProperties(_, childrenProperties)`
    /// (`logical_aggregation.go:268`).
    ///
    /// A group-less aggregate needs no order, so it offers ONE empty order. A
    /// grouped aggregate keeps a child order only when the group-by columns
    /// are a prefix of it.
    pub fn prepare_possible_properties(
        &mut self,
        child_props: Option<&PossiblePropertiesInfo>,
    ) -> PossiblePropertiesInfo {
        let has_tiflash = child_props.is_some_and(|props| props.has_tiflash);
        self.base.set_has_tiflash(has_tiflash);
        if self.group_by_items.is_empty() {
            self.possible_properties = PossiblePropertiesInfo {
                orders: vec![Vec::new()],
                has_tiflash,
            };
            return self.possible_properties.clone();
        }
        let Some(child_props) = child_props else {
            self.possible_properties = PossiblePropertiesInfo {
                orders: Vec::new(),
                has_tiflash,
            };
            return self.possible_properties.clone();
        };
        let group_by_cols = self.get_group_by_cols();
        let mut orders = Vec::with_capacity(child_props.orders.len());
        for candidate in &child_props.orders {
            if max_sort_prefix_len(candidate, &group_by_cols) == group_by_cols.len() {
                orders.push(candidate[..group_by_cols.len()].to_vec());
            }
        }
        self.possible_properties = PossiblePropertiesInfo {
            orders,
            has_tiflash,
        };
        self.possible_properties.clone()
    }

    /// Go `LogicalAggregation.DeriveStats(childStats, selfSchema, childSchema,
    /// reloads)` (`logical_aggregation.go:219`): the output row count is the
    /// NDV of the group-by columns, and every output column takes that NDV.
    ///
    /// # Blocked
    ///
    /// Go's row count is `cardinality.EstimateColsNDVWithMatchedLen(sctx,
    /// gbyCols, childSchema[0], childProfile)`, which needs the session and the
    /// child histograms. The dependency-closed part of that estimator — the
    /// product of the per-column NDVs, capped at the child row count — is what
    /// runs here; a group-by column absent from the child profile falls back to
    /// the child row count, as Go's `EstimateColsNDVWithMatchedLen` does for an
    /// unmatched column.
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
        let mut gby_cols = Vec::new();
        for item in &self.group_by_items {
            gby_cols.extend(extract_columns(item));
        }
        let mut ndv = 1.0_f64;
        for column in &gby_cols {
            ndv *= child
                .col_ndvs()
                .get(&column.unique_id)
                .copied()
                .unwrap_or(child.row_count());
        }
        if gby_cols.is_empty() {
            ndv = 1.0;
        }
        let ndv = ndv.min(child.row_count());
        let stats = StatsInfo::new(
            ndv,
            self_schema
                .columns
                .iter()
                .map(|column| (column.unique_id, ndv)),
        );
        self.input_count = child.row_count();
        self.base.base.set_stats(Some(stats.clone()));
        Some((stats, true))
    }

    /// Go `LogicalAggregation.Hash64(h)`
    /// (`logicalop/hash64_equals_generated.go`): the schema producer, then the
    /// aggregate descriptors, the group-by items, and the possible properties.
    ///
    /// This is the merged form of the crate's former
    /// `logical_aggregation::LogicalAggregationIdentity`.
    #[must_use]
    pub fn hash64(&self, schema: Option<&Schema>) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_uint64(schema_producer::schema_hash64(schema));
        hasher.hash_int(self.agg_funcs.len() as i64);
        for func in &self.agg_funcs {
            hasher.hash_string(func.name());
            hasher.hash_bool(func.has_distinct);
            hasher.hash_int(func.args().len() as i64);
            for arg in func.args() {
                let mut arg = arg.clone();
                hasher.hash_bytes(arg.hash_code());
            }
        }
        hasher.hash_int(self.group_by_items.len() as i64);
        for item in &self.group_by_items {
            let mut item = item.clone();
            hasher.hash_bytes(item.hash_code());
        }
        hasher.hash_bool(self.possible_properties.has_tiflash);
        hasher.sum64()
    }

    /// Go `LogicalAggregation.Equals(other)`.
    #[must_use]
    pub fn equals(
        &self,
        self_schema: Option<&Schema>,
        other: &Self,
        other_schema: Option<&Schema>,
    ) -> bool {
        schema_producer::schema_equals(self_schema, other_schema)
            && self.agg_funcs.len() == other.agg_funcs.len()
            && self
                .agg_funcs
                .iter()
                .zip(&other.agg_funcs)
                .all(|(left, right)| left.equals(right))
            && schema_producer::expression_lists_equal(&self.group_by_items, &other.group_by_items)
    }
}

/// Go `aggFuncResultMatchesArgForNonEmptyGroup(aggFunc)`
/// (`logical_aggregation.go:703`): `MAX`/`MIN` of a single row-independent
/// argument, without `DISTINCT` and without an `ORDER BY`.
#[must_use]
pub fn agg_func_result_matches_arg_for_non_empty_group(func: &AggFuncDesc) -> bool {
    if func.has_distinct || func.args().len() != 1 || !func.order_by_items.is_empty() {
        return false;
    }
    if !matches!(func.name(), AGG_FUNC_MAX | AGG_FUNC_MIN) {
        return false;
    }
    func.args()[0].const_level() >= ConstLevel::ONLY_IN_CONTEXT
}

/// Go `util.GetMaxSortPrefix(sortCols, allCols)`
/// (`pkg/planner/util/misc.go`): how many leading `sort_cols` are also in
/// `all_cols`, positionally.
#[must_use]
pub fn max_sort_prefix_len(sort_cols: &[Column], all_cols: &[Column]) -> usize {
    let limit = sort_cols.len().min(all_cols.len());
    let mut length = 0;
    while length < limit {
        if !all_cols
            .iter()
            .any(|column| column.unique_id == sort_cols[length].unique_id)
        {
            break;
        }
        length += 1;
    }
    length
}

impl LogicalAggregation {
    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            agg_funcs: self.agg_funcs.clone(),
            group_by_items: self.group_by_items.clone(),
            prefer_agg_type: self.prefer_agg_type,
            prefer_agg_to_cop: self.prefer_agg_to_cop,
            possible_properties: self.possible_properties.clone(),
            input_count: self.input_count,
            no_cop_push_down: self.no_cop_push_down,
        }
    }
}
