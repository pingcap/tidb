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
//! [`tidb_expression_aggregation::AggFuncDesc`] — the workspace's canonical
//! `aggregation.AggFuncDesc`, reused here rather than restated.
//!
//! This file MERGES the crate's former `logical_aggregation` identity leaf:
//! its `Hash64`/`Equals` framing over normalised adapters now runs on the real
//! descriptors and expressions, as [`LogicalAggregation::hash64`] /
//! [`LogicalAggregation::equals`], and the adapter module is gone.

use tidb_expression_aggregation::AggFuncDesc;
use tidb_expression_aggregation::AggFunctionMode;
use tidb_expr::column::Column;
use tidb_expr::expr_util::substitute::{evaluate_expr_with_null, SubstituteOptions};
use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expression::{ConstLevel, CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns, extract_cor_columns};
use tidb_exprctx::NoColumns;

use tidb_planner_cascades_base::{new_hash_equaler, Hasher};
use crate::logical::schema_producer;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::{
    hash_possible_properties, possible_properties_equal, PossiblePropertiesInfo,
};
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

    /// Go `GetPotentialPartitionKeys` (`logical_aggregation.go:519`).
    ///
    /// TiFlash can hash-partition an aggregation only by bare group-by
    /// columns.  Keep the complete column, including its type and collation,
    /// because exchange matching uses the collation-aware partition key.
    #[must_use]
    pub fn get_potential_partition_keys(
        &self,
    ) -> Vec<tidb_planner_property::MppPartitionColumn> {
        self.group_by_items
            .iter()
            .filter_map(|item| match item {
                Expression::Column(column) => Some(tidb_planner_property::MppPartitionColumn {
                    col: column.clone(),
                    collate_id: column
                        .get_static_type()
                        .map(|field_type| {
                            tidb_planner_property::collate_id_for_partition(
                                field_type.collation_name(),
                            )
                        })
                        .unwrap_or(-1),
                }),
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
            // Go indexes `la.AggFuncs[idx]` and `Args[0]` unguarded
            // (`logical_aggregation.go:724-725`).
            let func = &self.agg_funcs[idx];
            if func.name() == AGG_FUNC_FIRST_ROW && extract_columns(&func.args()[0]).len() == 1 {
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

    /// Go `LogicalAggregation.splitCondForAggregation(predicates)`
    /// (`logical_aggregation.go:631`): the predicates an aggregate may pass to
    /// its child, and those that must stay above it.
    ///
    /// Returns `(conds_to_push, ret)` in Go's order. `self_schema` is Go's
    /// `la.Schema()`: a pushed predicate over an aggregate OUTPUT column is
    /// rewritten onto the aggregate's argument (`ColumnSubstitute`), so what
    /// reaches the child reads child columns.
    ///
    /// Each CNF item first has its constant-result outputs (`max`/`min` of a
    /// row-independent argument) folded to that argument, then goes through
    /// `pushDownDNFPredicates` against the GROUP BY columns; the part that
    /// found nothing to push is tried again against the single-column
    /// `first_row` outputs. A DNF whose every branch has a pushable part
    /// pushes the relaxed disjunction of those parts and keeps the original
    /// above (`(a > 1 and avg(b) > 1) or (a < 3)` pushes `a > 1 or a < 3`).
    #[must_use]
    pub fn split_cond_for_aggregation(
        &self,
        self_schema: &Schema,
        predicates: &[Expression],
        opts: &SubstituteOptions<'_>,
    ) -> (Vec<Expression>, Vec<Expression>) {
        use tidb_expr::expr_util::fold::fold_constant;
        use tidb_expr::expr_util::normal_form::split_cnf_items;
        use tidb_expr::expr_util::substitute::column_substitute;

        let exprs_original: Vec<Expression> = self
            .agg_funcs
            .iter()
            .map(|func| {
                func.args().first().cloned().unwrap_or_else(|| {
                    Expression::Constant(tidb_expr::constant::Constant::new_one())
                })
            })
            .collect();
        let group_by_columns = Schema::new(self.get_group_by_cols());
        let agg_first_row_columns = Schema::new(self.agg_funcs_cols_for_first_row(self_schema));
        let (const_result_columns, const_result_exprs) =
            self.agg_funcs_cols_for_const_result(self_schema);
        let const_result_schema =
            (!const_result_columns.is_empty()).then(|| Schema::new(const_result_columns));
        let pusher = AggPredicatePusher {
            self_schema,
            exprs_original: &exprs_original,
            opts,
        };

        let mut conds_to_push = Vec::new();
        let mut ret = Vec::new();
        for cond in predicates {
            for mut cnf_item in split_cnf_items(cond) {
                if let Some(schema) = &const_result_schema {
                    cnf_item = fold_constant(
                        &column_substitute(&cnf_item, schema, &const_result_exprs, opts),
                        &NoColumns,
                        opts,
                    );
                }
                let (sub_conds_to_push, sub_ret) = pusher.push_down_dnf_predicates(
                    &cnf_item,
                    &group_by_columns,
                    AggPushKind::GroupBy,
                );
                conds_to_push.extend(sub_conds_to_push);
                // "If we cannot find columns that can be pushed down in the
                // GROUP BY clause, we will then look for columns that can be
                // pushed down in the aggregate functions. Currently, only the
                // first row is supported."
                for item in sub_ret {
                    let (sub_conds_to_push, sub_ret) = pusher.push_down_dnf_predicates(
                        &item,
                        &agg_first_row_columns,
                        AggPushKind::FirstRow,
                    );
                    conds_to_push.extend(sub_conds_to_push);
                    ret.extend(sub_ret);
                }
            }
        }
        (conds_to_push, ret)
    }

    /// Go `LogicalAggregation.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_aggregation.go:113`): drop the unused aggregate outputs and
    /// the constant group-by items, repair the two degenerate cases, and
    /// report what the child must still produce.
    ///
    /// The two repairs are Go's, and both are about ROW COUNT rather than
    /// values:
    /// * an aggregate left with no functions, or whose only survivors are
    ///   `first_row` when they were not all `first_row` to begin with, gains a
    ///   `count(1)` — because `select agg(a) from t` returns one row over an
    ///   empty table while `first_row` returns none, so the choice of repair
    ///   function changes the result;
    /// * an aggregate whose every GROUP BY item was a constant gains
    ///   `group by 1`, because `select count(*) from t` and
    ///   `select count(*) from t group by 1` differ on an empty table.
    ///
    /// # Narrowing
    ///
    /// The repair column Go appends carries a FRESH plan-column id from
    /// `AllocPlanColumnID()`. There is no allocator on this operator, so the
    /// column is given the sentinel id `i64::MIN`, the same device
    /// [`super::LogicalProjection::build_schema_by_exprs`] uses: it can never
    /// collide with a real allocation and can never match a child key.
    /// The whole-tree pruning caller replaces it with a statement allocation
    /// before publishing the schema to the rest of the optimizer.
    /// `aggregation.NewAggFuncDesc`'s return-type inference is likewise not
    /// reachable here, so the repair descriptor inherits the pruned schema's
    /// type when there is one.
    pub fn prune_columns_local(
        &mut self,
        parent_used_cols: &[Column],
        self_schema: &mut Schema,
    ) -> Vec<Column> {
        let used = schema_producer::get_used_list(parent_used_cols, self_schema);
        let mut all_first_row = true;
        let mut all_remain_first_row = true;
        for i in (0..used.len()).rev() {
            // Go indexes `la.AggFuncs[i]` directly
            // (`logical_aggregation.go:123`).
            let func = &self.agg_funcs[i];
            if func.name() != AGG_FUNC_FIRST_ROW {
                all_first_row = false;
            }
            if !used[i] && !tidb_expr::expr_util::predicates::exprs_has_side_effects(func.args()) {
                self_schema.columns.remove(i);
                self.agg_funcs.remove(i);
            } else if func.name() != AGG_FUNC_FIRST_ROW {
                all_remain_first_row = false;
            }
        }

        let mut self_used_cols: Vec<Column> = Vec::new();
        for func in &mut self.agg_funcs {
            for arg in func.args() {
                self_used_cols.extend(extract_columns(arg));
            }
            let (items, columns) = super::sort::prune_by_items(&func.order_by_items);
            func.order_by_items = items;
            self_used_cols.extend(columns);
        }

        if self.agg_funcs.is_empty() || (!all_first_row && all_remain_first_row) {
            let name = if all_first_row {
                AGG_FUNC_FIRST_ROW
            } else {
                AGG_FUNC_COUNT
            };
            let one = tidb_expr::constant::Constant::new_one();
            let Some(ret_type) = one.ret_type.clone() else {
                return self_used_cols;
            };
            self.agg_funcs.push(AggFuncDesc {
                base: tidb_expression_aggregation::BaseFuncDesc {
                    name: name.to_owned(),
                    args: vec![Expression::Constant(one)],
                    ret_type: ret_type.clone(),
                },
                mode: AggFunctionMode::Complete,
                has_distinct: false,
                order_by_items: Vec::new(),
                grouping_id: 0,
            });
            let mut column = Column::default();
            column.unique_id = i64::MIN;
            column.ret_type = Some(ret_type);
            self_schema.columns.push(column);
        }

        if !self.group_by_items.is_empty() {
            for i in (0..self.group_by_items.len()).rev() {
                let cols = extract_columns(&self.group_by_items[i]);
                if cols.is_empty()
                    && !tidb_expr::expr_util::predicates::expr_has_set_var_or_sleep(
                        &self.group_by_items[i],
                    )
                {
                    self.group_by_items.remove(i);
                } else {
                    self_used_cols.extend(cols);
                }
            }
            if self.group_by_items.is_empty() {
                self.group_by_items = vec![Expression::Constant(
                    tidb_expr::constant::Constant::new_one(),
                )];
            }
        }
        self_used_cols
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

    /// Go `LogicalAggregation.CanPullUp()` (`logical_aggregation.go:815`):
    /// only an UNGROUPED aggregation whose every argument becomes NULL over a
    /// NULL child row may be pulled above an apply. That is exactly what makes
    /// the pull-up sound: an empty input contributes no value, so the outer
    /// join's NULL extension is the same answer.
    ///
    /// # Narrowing
    ///
    /// Go evaluates through the statement's `ExprContext`; this port has no
    /// `Columns` value here, so the substitution/evaluation uses
    /// [`NoColumns`]. An argument that needs the session context therefore
    /// answers `false`, which only keeps the apply correlated.
    #[must_use]
    pub fn can_pull_up(&self, child_schema: &Schema) -> bool {
        if !self.group_by_items.is_empty() {
            return false;
        }
        let builder = RealFunctionBuilder::new(&NoColumns);
        let options = SubstituteOptions::new(&builder);
        for func in &self.agg_funcs {
            for arg in func.args() {
                let Ok(result) =
                    evaluate_expr_with_null(arg, child_schema, true, &NoColumns, &options)
                else {
                    return false;
                };
                match result {
                    Expression::Constant(constant) if constant.value.is_null() => {}
                    _ => return false,
                }
            }
        }
        true
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
    /// The default `RiskGroupNDVSkewRatio == 0` uses Go's conservative
    /// estimate: an exact `GroupNDV` when present, otherwise the largest
    /// group-column NDV.
    pub fn derive_stats(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        reloads: &[bool],
    ) -> Option<(StatsInfo, bool)> {
        self.derive_stats_with_group_ndv_skew_ratio(
            child_stats,
            self_schema,
            reloads,
            tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO,
        )
    }

    /// Go `LogicalAggregation.DeriveStats` with its session skew ratio.
    pub fn derive_stats_with_group_ndv_skew_ratio(
        &mut self,
        child_stats: &[StatsInfo],
        self_schema: &Schema,
        reloads: &[bool],
        group_ndv_skew_ratio: f64,
    ) -> Option<(StatsInfo, bool)> {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return Some((existing.clone(), false));
            }
        }
        let child = &child_stats[0];
        let mut gby_cols = Vec::new();
        for item in &self.group_by_items {
            gby_cols.extend(extract_columns(item));
        }
        let group_ids = gby_cols
            .iter()
            .map(|column| column.unique_id)
            .collect::<Vec<_>>();
        let (ndv, _) = crate::cardinality::derive_stats::
            estimate_cols_ndv_with_matched_len_and_skew_ratio(
                &group_ids,
                child,
                group_ndv_skew_ratio,
            );
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
        hash_possible_properties(&mut hasher, &self.possible_properties);
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
            && possible_properties_equal(&self.possible_properties, &other.possible_properties)
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

/// Go's `pushDownPredicatesFunc` choice: which columns a pushed predicate may
/// read, `pushDownPredicatesByGroupby` or `pushDownPredicatesByAggFuncs`.
#[derive(Clone, Copy)]
enum AggPushKind {
    GroupBy,
    FirstRow,
}

/// The state Go's `pushDownCNFPredicatesForAggregation` /
/// `pushDownDNFPredicates` / `pushDownPredicates` share for one aggregation.
struct AggPredicatePusher<'a> {
    /// Go `la.Schema()`.
    self_schema: &'a Schema,
    /// Go `exprsOriginal`: the first argument of each aggregate function.
    exprs_original: &'a [Expression],
    opts: &'a SubstituteOptions<'a>,
}

impl AggPredicatePusher<'_> {
    /// Go `pushDownPredicates`: a constant is pushed AND retained (`having
    /// 1=0` must still empty the one-row result), a scalar function goes to
    /// the by-group-by / by-first-row split, anything else stays above.
    fn push_down_predicates(
        &self,
        cond: &Expression,
        columns: &Schema,
        kind: AggPushKind,
    ) -> (Vec<Expression>, Vec<Expression>) {
        match cond {
            Expression::Constant(_) => (vec![cond.clone()], vec![cond.clone()]),
            Expression::ScalarFunction(_) => match kind {
                AggPushKind::GroupBy => self.push_down_by_group_by(cond, columns),
                AggPushKind::FirstRow => self.push_down_by_first_row(cond, columns),
            },
            _ => (Vec::new(), vec![cond.clone()]),
        }
    }

    fn substituted(&self, cond: &Expression) -> Expression {
        tidb_expr::expr_util::substitute::column_substitute(
            cond,
            self.self_schema,
            self.exprs_original,
            self.opts,
        )
    }

    /// Go `pushDownPredicatesByGroupby`.
    fn push_down_by_group_by(
        &self,
        cond: &Expression,
        group_by_columns: &Schema,
    ) -> (Vec<Expression>, Vec<Expression>) {
        if extract_columns(cond)
            .iter()
            .all(|column| group_by_columns.contains(column))
        {
            (vec![self.substituted(cond)], Vec::new())
        } else {
            (Vec::new(), vec![cond.clone()])
        }
    }

    /// Go `pushDownPredicatesByAggFuncs`: only for `first_row`.
    fn push_down_by_first_row(
        &self,
        cond: &Expression,
        agg_first_row_columns: &Schema,
    ) -> (Vec<Expression>, Vec<Expression>) {
        let extracted = extract_columns(cond);
        if extracted.len() != 1 || agg_first_row_columns.columns.is_empty() {
            return (Vec::new(), vec![cond.clone()]);
        }
        // `aggFirstRowColumns` are the output columns of the first row
        // function; the predicate's column must be one of them.
        if agg_first_row_columns
            .retrieve_column(&extracted[0])
            .is_some()
        {
            (vec![self.substituted(cond)], Vec::new())
        } else {
            (Vec::new(), vec![cond.clone()])
        }
    }

    /// Go `pushDownCNFPredicatesForAggregation`: `(a > 1 or avg(b) > 1) and
    /// (a < 3)` pushes `a < 3` and keeps the disjunction.
    fn push_down_cnf_predicates(
        &self,
        cond: &Expression,
        columns: &Schema,
        kind: AggPushKind,
    ) -> (Vec<Expression>, Vec<Expression>) {
        use tidb_expr::expr_util::normal_form::split_cnf_items;
        use tidb_expr::simple_expr::compose_dnf_condition;

        let items = split_cnf_items(cond);
        if let [only] = items.as_slice() {
            return self.push_down_predicates(only, columns, kind);
        }
        let mut conds_to_push = Vec::new();
        let mut ret = Vec::new();
        for item in &items {
            let (pushed, kept) = self.push_down_dnf_predicates(item, columns, kind);
            if let Some(composed) = compose_dnf_condition(pushed) {
                conds_to_push.push(composed);
            }
            if let Some(composed) = compose_dnf_condition(kept) {
                ret.push(composed);
            }
        }
        (conds_to_push, ret)
    }

    /// Go `pushDownDNFPredicates`: `(a > 1 and avg(b) > 1) or (a < 3)` pushes
    /// `(a > 1) or (a < 3)` and keeps the original above.
    fn push_down_dnf_predicates(
        &self,
        cond: &Expression,
        columns: &Schema,
        kind: AggPushKind,
    ) -> (Vec<Expression>, Vec<Expression>) {
        use tidb_expr::expr_util::normal_form::split_dnf_items;
        use tidb_expr::simple_expr::{compose_cnf_condition, compose_dnf_condition};

        let items = split_dnf_items(cond);
        if let [only] = items.as_slice() {
            return self.push_down_predicates(only, columns, kind);
        }
        let mut conds_to_push = Vec::with_capacity(items.len());
        let mut any_kept = false;
        for item in &items {
            let (pushed, kept) = self.push_down_cnf_predicates(item, columns, kind);
            let Some(composed) = compose_cnf_condition(pushed) else {
                return (Vec::new(), vec![cond.clone()]);
            };
            conds_to_push.push(composed);
            any_kept |= !kept.is_empty();
        }
        if !any_kept {
            // All the condition can be pushed down.
            return (vec![cond.clone()], Vec::new());
        }
        // Some condition can't be pushed down, we need to keep all the
        // condition.
        let relaxed = compose_dnf_condition(conds_to_push).into_iter().collect();
        (relaxed, vec![cond.clone()])
    }
}
