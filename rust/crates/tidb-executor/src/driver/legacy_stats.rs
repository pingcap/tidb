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

//! The DP join-reorder driver's PRIVATE statistics model — moved down from
//! `tidb-planner`, where it was the third of three logical representations.
//!
//! # This module dies with the driver — do not grow it
//!
//! [`LogicalNode`] here is NOT a plan: it is the reduced input this driver
//! builds from its own `Rel`/`RowSource` catalog to price join orders, the
//! way Go's `rule_join_reorder.go` DP prices candidates from per-node
//! `StatsInfo().RowCount`. The plan path's statistics are
//! `tidb_planner::logical::LogicalPlan::recursive_derive_stats`, derived on
//! the real tree; when this driver's planning path is replaced by the plan
//! layer (the executor-builder milestone), this module is deleted with it.
//! New code must not construct these types.
//!
//! The per-rule arithmetic — `scale_ndv`, `estimate_cols_ndv_with_matched_len`,
//! the profile's own `scale` — stays in `tidb_planner::cardinality`, shared
//! with the live pass; the Go citations on each rule below are the same ones
//! that pass cites, because both were read off the same Go bodies.

use std::collections::{BTreeMap, BTreeSet};

pub use tidb_planner::cardinality::derive_stats::ColumnId;
use tidb_planner::cardinality::derive_stats::{
    estimate_cols_ndv_with_matched_len, scale_ndv, StatsInfo, DEF_SCALE_NDV_SKEW_RATIO,
};
use tidb_planner::cardinality::join::{
    estimate_full_join_row_count, FullJoinRowCountInput, JoinKeyEstimate,
};
use tidb_planner::cardinality::ndv::GroupNdv;
use tidb_planner::cost_factors::SELECTION_FACTOR;

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
            .fold(self.stats.row_count(), |cost, child| {
                cost + child.cum_cost()
            })
    }

    /// Every node's row count, parent before children, depth first.
    #[must_use]
    pub fn row_counts(&self) -> Vec<f64> {
        let mut out = vec![self.stats.row_count()];
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
                            .map(|column| *column)
                            .eq(group_columns.iter().copied())
                        {
                            return true;
                        }
                    }
                    false
                })
                .cloned()
                .collect();
            let table_stats = StatsInfo::new(
                *realtime_count,
                column_ndvs.iter().map(|(id, ndv)| (*id, *ndv)),
            )
            .with_group_ndvs(group_ndvs);
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
            stats.set_group_ndvs(Vec::new());
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
                .group_ndvs()
                .iter()
                .filter_map(|group| {
                    let mut columns = group
                        .columns
                        .iter()
                        .map(|column| {
                            exprs
                                .iter()
                                .find(|expr| expr.direct_input == Some(*column as ColumnId))
                                .map(|expr| expr.output)
                        })
                        .collect::<Option<Vec<_>>>()?;
                    columns.sort_unstable();
                    Some(GroupNdv {
                        columns,
                        ndv: group.ndv,
                    })
                })
                .collect();
            let stats = StatsInfo::new(
                child.stats.row_count(),
                exprs.iter().map(|expr| {
                    let (ndv, _) = estimate_cols_ndv_with_matched_len(&expr.inputs, &child.stats);
                    (expr.output, ndv)
                }),
            )
            .with_group_ndvs(group_ndvs);
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
            let stats = StatsInfo::new(
                ndv,
                // Go deliberately uses the conservative group NDV for every
                // aggregate output, including FIRST_ROW carriers.
                columns.iter().map(|id| (*id, ndv)),
            )
            .with_group_ndvs(
                child
                    .stats
                    .group_ndvs()
                    .iter()
                    .filter(|group| {
                        let mut columns = group.columns.clone();
                        columns.sort_unstable();
                        let mut grouped = group_by.to_vec();
                        grouped.sort_unstable();
                        columns == grouped
                    })
                    .cloned()
                    .collect(),
            );
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
                left_row_count: left.stats.row_count(),
                right_row_count: right.stats.row_count(),
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
                JoinKind::LeftOuter => count.max(left.stats.row_count()),
                JoinKind::RightOuter => count.max(right.stats.row_count()),
            };
            // `LogicalJoin.DeriveStats` clamps every inherited NDV to the
            // join's own row count (`logical_join.go:604-610`).
            let col_ndvs: Vec<(ColumnId, f64)> = left
                .stats
                .col_ndvs()
                .iter()
                .chain(right.stats.col_ndvs().iter())
                .map(|(id, ndv)| (*id, ndv.min(count)))
                .collect();
            let group_ndvs = match kind {
                JoinKind::LeftOuter => left.stats.group_ndvs().to_vec(),
                JoinKind::RightOuter => right.stats.group_ndvs().to_vec(),
                JoinKind::Inner => Vec::new(),
            };
            DerivedNode {
                stats: StatsInfo::new(count, col_ndvs).with_group_ndvs(group_ndvs),
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
    join.stats.row_count() + left_cum + right_cum
}
