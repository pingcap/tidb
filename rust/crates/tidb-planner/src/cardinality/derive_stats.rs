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

//! Pre-build row-count derivation, Go's `RecursiveDeriveStats`.
//!
//! The join-reorder DP solver costs a candidate tree purely from
//! `StatsInfo().RowCount` values: `baseNodeCumCost` sums the row counts over a
//! node's whole subtree (`rule_join_reorder.go:651-657`) and `calcJoinCumCost`
//! adds the join's own row count to both children's cumulative costs
//! (`rule_join_reorder.go:978-980`). Reproducing the DP's choice therefore
//! needs nothing more than a faithful per-node row count, which is what this
//! module derives.
//!
//! Node kinds are the ones a `t1, t5, (select ... from t2 join t3) dt` shape
//! reaches: `DataSource`, `Selection`, `Projection`, `Aggregation` and inner
//! `Join`. Each rule below is the Go body, not a re-derivation:
//!
//! * `DataSource` -- `deriveStats4DataSource` (`core/stats.go:110-168`) sets
//!   `ds.stats = ds.TableStats.Scale(vars, Selectivity(pushedDownConds))`,
//!   where `TableStats` is built by `initStats` (`core/stats.go:538-574`) with
//!   `RowCount = RealtimeCount` and one `EstimateColumnNDV` per schema column.
//! * `Selection` -- `LogicalSelection.DeriveStats`
//!   (`logicalop/logical_selection.go:227-240`) is a *flat*
//!   `Scale(vars, SelectionFactor)`. It does **not** consult the per-conjunct
//!   selectivity machinery; only the `DataSource` does, for the conditions
//!   that were pushed into it.
//! * `Projection` -- `LogicalProjection.DeriveStats`
//!   (`logicalop/logical_projection.go:278-305`) passes the child's row count
//!   through unchanged and re-derives one NDV per output expression.
//! * `Aggregation` -- `LogicalAggregation.DeriveStats`
//!   (`logicalop/logical_aggregation.go:219-246`) estimates the group-key NDV
//!   and uses that number as both its row count and every output column's NDV.
//! * `Join` -- `LogicalJoin.DeriveStats` (`logicalop/logical_join.go:560-616`)
//!   takes `EstimateFullJoinRowCount` for an inner join and clamps every
//!   inherited column NDV to the join's own row count.

use std::collections::{BTreeMap, BTreeSet};

use crate::cardinality::join::{
    estimate_full_join_row_count, FullJoinRowCountInput, JoinKeyEstimate,
};
use crate::cardinality::ndv::GroupNdv;
use crate::cost_factors::SELECTION_FACTOR;

/// Go `cardinality.distinctFactor` (`cardinality/ndv.go:35`), the NDV a column
/// with no loaded histogram is assumed to have, as a fraction of the table's
/// realtime row count (`EstimateColumnNDV`, `cardinality/ndv.go:39-53`).
pub const DISTINCT_FACTOR: f64 = 0.8;

/// Go `vardef.DefOptRiskScaleNDVSkewRatio` (`vardef/tidb_vars.go:1471`).
///
/// The default is `1.0`, so `ScaleNDV` returns the *skewed* estimate --
/// `originalNDV * selectedRows / originalRows` -- and the uniform branch never
/// contributes in a default session. Both branches are still ported, because
/// the variable is settable.
pub const DEF_SCALE_NDV_SKEW_RATIO: f64 = 1.0;

/// A column identity. Go uses `expression.Column.UniqueID`.
///
/// **This alias disagrees with the rest of the crate and is the wrong one.**
/// Go's `expression.Column.UniqueID` is an `int64`, and this port spells it
/// `i64` everywhere else: [`tidb_expr::column::Column::unique_id`],
/// [`crate::stats_info::StatsInfo`]'s `col_ndvs` key, and
/// [`crate::cardinality::ndv::GroupNdv::columns`]. The mismatch is already
/// being paid for inside this module, which casts `*id as i64` at three
/// points purely to compare against `GroupNdv`.
///
/// It is left as `u64` here only because
/// `crates/tidb-executor/src/driver/join_reorder.rs` imports this alias and
/// builds its column maps against it; narrowing it is part of the two-crate
/// change described on [`LogicalNode`], not a local edit.
pub type ColumnId = u64;

/// Go `property.StatsInfo`, reduced to the fields the DP cost and NDV rules
/// read.
///
/// # This is the SECOND port of `pkg/planner/property/stats_info.go`
///
/// [`crate::stats_info::StatsInfo`] is the other, and it is the one that
/// actually travels in a plan: `BasePlan.stats` holds it, and the logical
/// operators and the plan builder all name it. This one is local to this
/// module and no other module imports it.
///
/// They are not interchangeable, and the difference is not only which fields
/// they carry. **This one keys `col_ndvs` by [`ColumnId`] (`u64`); the plan's
/// keys it by `i64`.** This one is also the richer of the two — it has
/// `group_ndvs`, public fields and a `Default`, none of which the plan's has.
///
/// So when a cardinality rule eventually needs to write derived statistics
/// back onto a plan node, the two must be reconciled: the key types converted,
/// and a home found for `group_ndvs`.
///
/// # The verdict, so the next attempt does not re-derive it
///
/// **Keep [`crate::stats_info::StatsInfo`] and key it by `i64`.** It is the
/// one that travels in `BasePlan.stats`, it already has ~10 producers across
/// the logical operators (`join.rs`, `window.rs`, `max_one_row.rs`,
/// `table_dual.rs`, `show.rs`, `union_all.rs`, `sequence.rs`, `cte.rs`,
/// `mem_table.rs` and the base body in `logical/mod.rs`), and `i64` is what
/// Go's `expression.Column.UniqueID` is and what
/// [`tidb_expr::column::Column::unique_id`] and
/// [`crate::cardinality::ndv::GroupNdv::columns`] already use. The `u64` here
/// is the outlier; see [`ColumnId`].
///
/// The merge is then: add `group_ndvs: Vec<GroupNdv>` and [`StatsInfo::scale`]
/// to `crate::stats_info::StatsInfo`, and delete this struct.
///
/// It was NOT done in the same pass as the `LogicalNode` retarget because it
/// is blocked on the same out-of-crate consumer:
/// `crates/tidb-executor/src/driver/join_reorder.rs` reads
/// `DerivedNode.stats.row_count` as a PUBLIC FIELD at six sites (lines 2884,
/// 3137-3138, 3160, 3198 and 3254 at this branch's pin), while
/// `crate::stats_info::StatsInfo` has private fields and a `row_count()`
/// accessor. Swapping the type in [`DerivedNode`] breaks that crate. Do the
/// merge together with the retarget, under one owner for both crates — a
/// half-merge that leaves two types with one set of fields moved is worse
/// than either end state.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatsInfo {
    /// `StatsInfo.RowCount`.
    pub row_count: f64,
    /// `StatsInfo.ColNDVs`, keyed by column unique ID.
    pub col_ndvs: BTreeMap<ColumnId, f64>,
    /// Exact NDVs of composite column groups supplied by indexes.
    pub group_ndvs: Vec<GroupNdv>,
}

/// Go `cardinality.ScaleNDV` (`cardinality/ndv.go:215-262`).
///
/// `skew_ratio` blends the skewed estimate with the uniform one; the session
/// default is [`DEF_SCALE_NDV_SKEW_RATIO`].
#[must_use]
pub fn scale_ndv(
    original_ndv: f64,
    original_rows: f64,
    selected_rows: f64,
    skew_ratio: f64,
) -> f64 {
    let uniform = estimate_uniform_ndv(original_ndv, original_rows, selected_rows);
    let skewed = estimate_skewed_ndv(original_ndv, original_rows, selected_rows);
    skewed * skew_ratio + uniform * (1.0 - skew_ratio)
}

/// Go `estimateUniformNDV` (`cardinality/ndv.go:234-254`).
fn estimate_uniform_ndv(original_ndv: f64, original_rows: f64, selected_rows: f64) -> f64 {
    if original_rows <= 0.0 || selected_rows <= 0.0 || original_ndv <= 0.0 {
        return 0.0;
    }
    if selected_rows >= original_rows {
        return original_ndv;
    }
    let selectivity = selected_rows / original_rows;
    let rows_per_value = original_rows / original_ndv;
    let not_selected_poss_per_value = (1.0 - selectivity).powf(rows_per_value);
    let new_ndv = original_ndv * (1.0 - not_selected_poss_per_value);
    new_ndv.max(1.0).min(selected_rows)
}

/// Go `estimateSkewedNDV` (`cardinality/ndv.go:257-262`).
fn estimate_skewed_ndv(original_ndv: f64, original_rows: f64, selected_rows: f64) -> f64 {
    if original_rows <= 0.0 {
        return 0.0;
    }
    original_ndv * selected_rows / original_rows
}

impl StatsInfo {
    /// Go `StatsInfo.Scale` (`property/stats_info.go:69-86`).
    ///
    /// Every column NDV is re-scaled through [`scale_ndv`] against the row
    /// count *before* the factor was applied -- not multiplied by the factor.
    ///
    /// At the default skew ratio of `1.0` the two happen to coincide, because
    /// the skewed branch is `ndv * selectedRows / originalRows` and
    /// `selectedRows` is exactly `originalRows * factor`. That equivalence is
    /// a property of the default, not of the rule: at any other ratio the
    /// uniform branch contributes and the two diverge, which is why the source
    /// expression is kept rather than folded into a multiplication.
    #[must_use]
    pub fn scale(&self, factor: f64, skew_ratio: f64) -> Self {
        let scaled_row_count = self.row_count * factor;
        let col_ndvs = self
            .col_ndvs
            .iter()
            .map(|(id, ndv)| {
                (
                    *id,
                    scale_ndv(*ndv, self.row_count, scaled_row_count, skew_ratio),
                )
            })
            .collect();
        let group_ndvs = self
            .group_ndvs
            .iter()
            .map(|group| GroupNdv {
                columns: group.columns.clone(),
                ndv: scale_ndv(group.ndv, self.row_count, scaled_row_count, skew_ratio),
            })
            .collect();
        Self {
            row_count: scaled_row_count,
            col_ndvs,
            group_ndvs,
        }
    }
}

/// Go `EstimateColsNDVWithMatchedLen` (`cardinality/ndv.go:87-123`), production
/// path.
///
/// Returns `(ndv, matched_len)`. An empty column list is Go's early
/// `return 1.0, 1`. For one column, conservative and exponential agree, so the
/// source returns the naive estimate directly. For several columns the
/// production default applies: `DefOptRiskGroupNDVSkewRatio` is `0.0`
/// (`vardef/tidb_vars.go:1472`), so the `skewRatio > 0` branch is not taken and
/// the *conservative* (naive max) estimate is returned with `matched_len = 1`.
///
/// The exponential-backoff blend behind a non-zero group-NDV skew ratio is
/// deliberately out of scope here. The backoff itself is already ported as
/// [`apply_exponential_backoff`](crate::cardinality::apply_exponential_backoff),
/// but nothing calls it from here because the production ratio is zero.
#[must_use]
pub fn estimate_cols_ndv_with_matched_len(cols: &[ColumnId], profile: &StatsInfo) -> (f64, usize) {
    if cols.is_empty() {
        return (1.0, 1);
    }
    let mut sorted_cols = cols.iter().map(|id| *id as i64).collect::<Vec<_>>();
    sorted_cols.sort_unstable();
    if let Some(group) = profile.group_ndvs.iter().find(|group| {
        let mut group_cols = group.columns.clone();
        group_cols.sort_unstable();
        group_cols == sorted_cols
    }) {
        return (group.ndv.max(1.0), group.columns.len());
    }

    let mut max_ndv = 1.0_f64;
    for col in cols {
        if let Some(ndv) = profile.col_ndvs.get(col) {
            if *ndv > 0.0 {
                max_ndv = max_ndv.max(*ndv);
            }
        }
    }
    (max_ndv, 1)
}

/// One output expression of a [`LogicalNode::Projection`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionExpr {
    /// The unique ID this expression takes in the projection's own schema.
    pub output: ColumnId,
    /// Every column the expression reads, Go's
    /// `ExtractAllColumnsFromExpressionsInUsedSlices`.
    pub inputs: Vec<ColumnId>,
    /// The child column when this expression is a direct column reference.
    pub direct_input: Option<ColumnId>,
}

/// The logical node kinds a derived-table join group is built from.
///
/// # This is a SECOND logical tree, and deleting it is a two-crate change
///
/// [`crate::logical::LogicalPlan`] is the crate's one source of truth for a
/// logical plan: a closed enum of 27 operators that [`crate::plan_builder`]
/// produces and [`crate::logical::rule`] rewrites. This type is a reduced,
/// five-variant invention of this port; Go has one `base.LogicalPlan` and
/// `DeriveStats` is a method on it. It SHOULD go away, and this pass should
/// consume `&LogicalPlan`.
///
/// That retarget was attempted and stopped at a blocker outside this crate.
/// `crates/tidb-executor/src/driver/join_reorder.rs` imports [`derive_stats`],
/// `LogicalNode`, [`ProjectionExpr`], [`JoinKind`], [`ColumnId`] and
/// [`DISTINCT_FACTOR`] from here, naming `LogicalNode` at 29 points of which
/// 14 BUILD one out of its own `Rel`/`RowSource` catalog model — `emit` (its
/// `LogicalNode::DataSource` builder), `emit_tree` (its `Join` spine), and the
/// test models below them. That driver holds no `LogicalPlan` and has no
/// `expression::Expression` values with which to build one, so it cannot be
/// repointed at `LogicalPlan` from inside `tidb-planner`.
///
/// What a real retarget needs, in order:
///
/// 1. An owner for `tidb-executor`, because its 14 construction sites move or
///    die with this type.
/// 2. A decision on which way the dependency runs: either that driver learns
///    to build `LogicalPlan` values (it needs a `PlanIdAllocator`, a `Schema`
///    per relation, and equi-join conditions as `Expression`s, none of which
///    it currently has), or `LogicalNode` moves DOWN into `tidb-executor` as
///    that driver's private cost model and leaves this crate entirely.
/// 3. A caller-supplied statistics source keyed by `table_id` /
///    `physical_table_id`. `logical::DataSource` carries `table_stats:
///    Option<StatsInfo>` (Go `DataSource.TableStats`) but NOT `selectivity`
///    (Go's `Selectivity(ds.PushedDownConds)`) and not `group_ndvs`, and this
///    crate has no selectivity implementation — `selectivity` has always been
///    a precomputed input here and must stay one.
/// 4. The 27-vs-5 decision, with no `_ =>` arm: only operators whose Go
///    `DeriveStats` provably leaves the row count unchanged may pass through,
///    everything else must REFUSE. Silently treating a `Limit` or `TopN` as
///    its child inflates the estimate, which is a silent wrong-answer bug in
///    cost-based planning.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalNode {
    /// A base table after predicate pushdown.
    DataSource {
        /// `StatisticTable.RealtimeCount`; `10000` for a pseudo table.
        realtime_count: f64,
        /// Go `DataSource.TableStats.ColNDVs`, keyed by column unique ID.
        column_ndvs: BTreeMap<ColumnId, f64>,
        /// Exact composite-index NDVs from `DataSource.TableStats.GroupNDVs`.
        group_ndvs: Vec<GroupNdv>,
        /// `Selectivity(ds.PushedDownConds)`, already computed.
        selectivity: f64,
    },
    /// A `LogicalSelection`: a flat [`SELECTION_FACTOR`], whatever it filters.
    Selection {
        /// The single child.
        child: Box<LogicalNode>,
    },
    /// A `LogicalProjection`: row count passes through.
    Projection {
        /// The single child.
        child: Box<LogicalNode>,
        /// One entry per output column.
        exprs: Vec<ProjectionExpr>,
    },
    /// A `LogicalAggregation`.
    Aggregation {
        /// The single child.
        child: Box<LogicalNode>,
        /// Every input column extracted from the `GROUP BY` expressions.
        group_by: Vec<ColumnId>,
        /// The aggregation's output schema columns.
        columns: Vec<ColumnId>,
    },
    /// A `LogicalJoin`.
    Join {
        /// The build/left child.
        left: Box<LogicalNode>,
        /// The probe/right child.
        right: Box<LogicalNode>,
        /// Left-hand equi-join key columns.
        left_keys: Vec<ColumnId>,
        /// Right-hand equi-join key columns.
        right_keys: Vec<ColumnId>,
        /// Which side the join PRESERVES.
        kind: JoinKind,
    },
}

/// Go `base.JoinType`, narrowed to the three kinds a `FROM` clause spells.
///
/// It reaches the row count through one line of `LogicalJoin.DeriveStats`
/// (`logical_join.go:598-603`):
///
/// ```go
/// count := p.EqualCondOutCnt
/// if p.JoinType == base.LeftOuterJoin {
///     count = math.Max(count, leftProfile.RowCount)
/// } else if p.JoinType == base.RightOuterJoin {
///     count = math.Max(count, rightProfile.RowCount)
/// }
/// ```
///
/// -- an outer join emits at least one row per preserved-side row, however
/// selective its equality is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JoinKind {
    /// `base.InnerJoin`.
    #[default]
    Inner,
    /// `base.LeftOuterJoin`: every LEFT row survives.
    LeftOuter,
    /// `base.RightOuterJoin`: every RIGHT row survives.
    RightOuter,
}

/// A node's derived stats together with its children's, so a caller can read
/// every per-node row count the way an `EXPLAIN` prints them.
#[derive(Clone, Debug, PartialEq)]
pub struct DerivedNode {
    /// This node's `StatsInfo`.
    pub stats: StatsInfo,
    /// Children in source order.
    pub children: Vec<DerivedNode>,
}

impl DerivedNode {
    /// Go `baseNodeCumCost` (`rule_join_reorder.go:651-657`): this node's row
    /// count plus every descendant's.
    #[must_use]
    pub fn cum_cost(&self) -> f64 {
        self.children
            .iter()
            .fold(self.stats.row_count, |cost, child| cost + child.cum_cost())
    }

    /// Every node's row count, parent before children, depth first.
    #[must_use]
    pub fn row_counts(&self) -> Vec<f64> {
        let mut out = vec![self.stats.row_count];
        for child in &self.children {
            out.extend(child.row_counts());
        }
        out
    }
}

/// Session inputs the derivation reads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeriveStatsContext {
    /// `TiDBOptJoinReorderThreshold`; non-positive disables the `0.9`
    /// correlation adjustment in `EstimateFullJoinRowCount`.
    pub join_reorder_threshold: i32,
    /// `RiskScaleNDVSkewRatio`; see [`DEF_SCALE_NDV_SKEW_RATIO`].
    pub scale_ndv_skew_ratio: f64,
    /// `SelectivityFactor`, Go `cost.SelectionFactor`.
    pub selection_factor: f64,
}

impl Default for DeriveStatsContext {
    fn default() -> Self {
        Self {
            join_reorder_threshold: 0,
            scale_ndv_skew_ratio: DEF_SCALE_NDV_SKEW_RATIO,
            selection_factor: SELECTION_FACTOR,
        }
    }
}

impl DeriveStatsContext {
    /// A default context with the join-reorder threshold set, as the DP solver
    /// requires (the solver only runs when the threshold is positive).
    #[must_use]
    pub fn with_join_reorder_threshold(threshold: i32) -> Self {
        Self {
            join_reorder_threshold: threshold,
            ..Self::default()
        }
    }
}

/// Derives stats for a subtree, Go `RecursiveDeriveStats`: children first, then
/// the node's own rule.
#[must_use]
pub fn derive_stats(node: &LogicalNode, ctx: &DeriveStatsContext) -> DerivedNode {
    derive_stats_with_groups(node, ctx, &[])
}

fn derive_stats_with_groups(
    node: &LogicalNode,
    ctx: &DeriveStatsContext,
    requested_groups: &[Vec<ColumnId>],
) -> DerivedNode {
    match node {
        LogicalNode::DataSource {
            realtime_count,
            column_ndvs,
            group_ndvs,
            selectivity,
        } => {
            let group_ndvs = group_ndvs
                .iter()
                .filter(|group| {
                    let mut group_columns = group.columns.clone();
                    group_columns.sort_unstable();
                    for requested in requested_groups {
                        if requested.len() != group.columns.len() {
                            break;
                        }
                        let mut requested = requested.clone();
                        requested.sort_unstable();
                        if requested
                            .iter()
                            .map(|column| *column as i64)
                            .eq(group_columns.iter().copied())
                        {
                            return true;
                        }
                    }
                    false
                })
                .cloned()
                .collect();
            let table_stats = StatsInfo {
                row_count: *realtime_count,
                col_ndvs: column_ndvs.clone(),
                group_ndvs,
            };
            DerivedNode {
                stats: table_stats.scale(*selectivity, ctx.scale_ndv_skew_ratio),
                children: Vec::new(),
            }
        }
        LogicalNode::Selection { child } => {
            let child = derive_stats_with_groups(child, ctx, requested_groups);
            let mut stats = child
                .stats
                .scale(ctx.selection_factor, ctx.scale_ndv_skew_ratio);
            // LogicalSelection does not preserve GroupNDVs in Go.
            stats.group_ndvs.clear();
            DerivedNode {
                stats,
                children: vec![child],
            }
        }
        LogicalNode::Projection { child, exprs } => {
            let child_groups = requested_groups
                .iter()
                .filter_map(|group| {
                    group
                        .iter()
                        .map(|column| {
                            exprs
                                .iter()
                                .find(|expr| expr.output == *column)
                                .and_then(|expr| expr.direct_input)
                        })
                        .collect::<Option<Vec<_>>>()
                })
                .collect::<Vec<_>>();
            let child = derive_stats_with_groups(child, ctx, &child_groups);
            let group_ndvs = child
                .stats
                .group_ndvs
                .iter()
                .filter_map(|group| {
                    let mut columns = group
                        .columns
                        .iter()
                        .map(|column| {
                            exprs
                                .iter()
                                .find(|expr| expr.direct_input == Some(*column as ColumnId))
                                .map(|expr| expr.output as i64)
                        })
                        .collect::<Option<Vec<_>>>()?;
                    columns.sort_unstable();
                    Some(GroupNdv {
                        columns,
                        ndv: group.ndv,
                    })
                })
                .collect();
            let stats = StatsInfo {
                row_count: child.stats.row_count,
                col_ndvs: exprs
                    .iter()
                    .map(|expr| {
                        let (ndv, _) =
                            estimate_cols_ndv_with_matched_len(&expr.inputs, &child.stats);
                        (expr.output, ndv)
                    })
                    .collect(),
                group_ndvs,
            };
            DerivedNode {
                stats,
                children: vec![child],
            }
        }
        LogicalNode::Aggregation {
            child,
            group_by,
            columns,
        } => {
            let child_groups = (group_by.len() > 1).then(|| group_by.clone());
            let child = derive_stats_with_groups(
                child,
                ctx,
                child_groups.as_ref().map_or(&[], std::slice::from_ref),
            );
            let (ndv, _) = estimate_cols_ndv_with_matched_len(group_by, &child.stats);
            let stats = StatsInfo {
                row_count: ndv,
                // Go deliberately uses the conservative group NDV for every
                // aggregate output, including FIRST_ROW carriers.
                col_ndvs: columns.iter().map(|id| (*id, ndv)).collect(),
                group_ndvs: child
                    .stats
                    .group_ndvs
                    .iter()
                    .filter(|group| {
                        let mut columns = group.columns.clone();
                        columns.sort_unstable();
                        let mut grouped = group_by.iter().map(|id| *id as i64).collect::<Vec<_>>();
                        grouped.sort_unstable();
                        columns == grouped
                    })
                    .cloned()
                    .collect(),
            };
            DerivedNode {
                stats,
                children: vec![child],
            }
        }
        LogicalNode::Join {
            left,
            right,
            left_keys,
            right_keys,
            kind,
        } => {
            let mut child_groups = Vec::new();
            if left_keys.len() > 1 {
                child_groups.push(left_keys.clone());
                child_groups.push(right_keys.clone());
            }
            let outer_columns = match kind {
                JoinKind::LeftOuter => Some(logical_columns(left)),
                JoinKind::RightOuter => Some(logical_columns(right)),
                JoinKind::Inner => None,
            };
            if let Some(outer_columns) = outer_columns {
                child_groups.extend(
                    requested_groups
                        .iter()
                        .filter(|group| group.iter().all(|column| outer_columns.contains(column)))
                        .cloned(),
                );
            }
            let left = derive_stats_with_groups(left, ctx, &child_groups);
            let right = derive_stats_with_groups(right, ctx, &child_groups);
            let (left_ndv, left_matched) =
                estimate_cols_ndv_with_matched_len(left_keys, &left.stats);
            let (right_ndv, right_matched) =
                estimate_cols_ndv_with_matched_len(right_keys, &right.stats);
            let count = estimate_full_join_row_count(&FullJoinRowCountInput {
                left_row_count: left.stats.row_count,
                right_row_count: right.stats.row_count,
                is_cartesian: left_keys.is_empty() && right_keys.is_empty(),
                left_join_keys: JoinKeyEstimate::new(left_ndv, left_matched, left_keys.len()),
                right_join_keys: JoinKeyEstimate::new(right_ndv, right_matched, right_keys.len()),
                left_non_equi_keys: JoinKeyEstimate::empty(),
                right_non_equi_keys: JoinKeyEstimate::empty(),
                join_reorder_threshold: ctx.join_reorder_threshold,
            });
            // An outer join emits at least one row per preserved-side row
            // (`logical_join.go:598-603`), so the equality's own estimate is
            // only a FLOOR on the count, never a ceiling.
            let count = match kind {
                JoinKind::Inner => count,
                JoinKind::LeftOuter => count.max(left.stats.row_count),
                JoinKind::RightOuter => count.max(right.stats.row_count),
            };
            // `LogicalJoin.DeriveStats` clamps every inherited NDV to the
            // join's own row count (`logical_join.go:604-610`).
            let col_ndvs = left
                .stats
                .col_ndvs
                .iter()
                .chain(right.stats.col_ndvs.iter())
                .map(|(id, ndv)| (*id, ndv.min(count)))
                .collect();
            let group_ndvs = match kind {
                JoinKind::LeftOuter => left.stats.group_ndvs.clone(),
                JoinKind::RightOuter => right.stats.group_ndvs.clone(),
                JoinKind::Inner => Vec::new(),
            };
            DerivedNode {
                stats: StatsInfo {
                    row_count: count,
                    col_ndvs,
                    group_ndvs,
                },
                children: vec![left, right],
            }
        }
    }
}

fn logical_columns(node: &LogicalNode) -> BTreeSet<ColumnId> {
    match node {
        LogicalNode::DataSource { column_ndvs, .. } => column_ndvs.keys().copied().collect(),
        LogicalNode::Selection { child } => logical_columns(child),
        LogicalNode::Projection { exprs, .. } => exprs.iter().map(|expr| expr.output).collect(),
        LogicalNode::Aggregation { columns, .. } => columns.iter().copied().collect(),
        LogicalNode::Join { left, right, .. } => logical_columns(left)
            .into_iter()
            .chain(logical_columns(right))
            .collect(),
    }
}

/// Go `calcJoinCumCost` (`rule_join_reorder.go:978-980`): the join's own row
/// count plus both children's cumulative costs.
#[must_use]
pub fn calc_join_cum_cost(join: &DerivedNode, left_cum: f64, right_cum: f64) -> f64 {
    join.stats.row_count + left_cum + right_cum
}
