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

//! The live executor seam to the ported Go logical planner.
//!
//! The executor still owns physical operator construction, but it must not
//! independently rediscover logical properties from the SQL AST. This module
//! builds and optimizes one logical tree, runs Go's possible-property pass,
//! and translates the aggregation property back to stable relation-qualified
//! column identities understood by the executor's physical source builder.

use tidb_expr::expr_util::RealFunctionBuilder;
use tidb_expr::expression::Expression;
use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::find_best_task::coster::Ver2Coster;
use tidb_planner::find_best_task::dispatch::{find_best_task, DispatchContext};
use tidb_planner::logical::fold::{fold_owned, Descend, OwnedRewrite};
use tidb_planner::logical::rule::{flags, logical_optimize, DisabledLogicalRules, RuleContext};
use tidb_planner::logical::{prepare_possible_properties, LogicalPlan};
use tidb_planner::physical::PhysicalPlan;
use tidb_planner::physical_property::PhysicalProperty;
use tidb_planner::plan_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner::stats_info::StatsInfo;

use super::catalog::{Catalog, TableEntry};
use super::merge_decision::{AggregationOrder, RelColumn};
use crate::plan_trace::PlanTrace;

/// The physical aggregation family selected by Go's `findBestTask` search.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregationFamily {
    Hash,
    Stream,
}

/// The access path selected for one physical `DataSource` by the shared
/// planner. Executor lowering resolves this receipt to its AST leaf and only
/// builds the named path; it does not run another access-path cost race.
#[derive(Clone, Debug)]
pub(crate) enum AccessPath {
    Table {
        ranges: tidb_planner::ranger::types::Ranges,
        keep_order: bool,
        desc: bool,
    },
    Index {
        index_id: i64,
        ranges: tidb_planner::ranger::types::Ranges,
        keep_order: bool,
        desc: bool,
    },
}

/// The root reader wrapped around the selected scan. Go distinguishes a
/// covering `IndexReader` from an `IndexLookUpReader`; executor lowering must
/// preserve that decision instead of re-proving covering from a second name
/// walk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AccessReader {
    Table,
    Index,
    IndexLookup,
    Point,
    BatchPoint,
}

/// Which reader-local TopN shape the selected physical tree contains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReaderTopN {
    /// A TopN inside a table or covering-index reader's cop plan.
    Cop,
    /// A TopN on the build side of an index lookup.
    IndexLookup,
}

/// One physical `Selection` owned by a reader, together with the stable
/// source identity of every column it references.  The shared planner has
/// already decided whether the conditions belong to the index side or the
/// table side; executor lowering only remaps those source columns onto the
/// runtime row layout.
#[derive(Clone, Debug, Default)]
pub(crate) struct AccessFilter {
    pub(crate) conditions: Vec<Expression>,
    pub(crate) columns: Vec<(i64, RelColumn)>,
}

#[derive(Clone, Debug)]
pub(crate) struct AccessDecision {
    pub(crate) table_id: i64,
    pub(crate) relation: Option<String>,
    pub(crate) path: AccessPath,
    pub(crate) reader: AccessReader,
    pub(crate) estimated_rows: Option<f64>,
    /// Go's cop-side `Selection` over the index plan.
    pub(crate) index_filter: AccessFilter,
    /// Go's cop-side `Selection` over the table plan.
    pub(crate) table_filter: AccessFilter,
    /// Direct-column `PhysicalProjection` retained inside the selected
    /// reader's cop task, in output order. Stable source identities let
    /// executor lowering remap the cached tree after column pruning.
    pub(crate) cop_projection: Option<Vec<RelColumn>>,
    /// The partial Limit inside the selected reader, as `(offset, count)`.
    pub(crate) pushed_limit: Option<(u64, u64)>,
    /// The selected reader-local TopN family, if any.
    pub(crate) pushed_topn: Option<ReaderTopN>,
    /// Go `PhysicalIndexLookUpReader.PushedLimit`.
    pub(crate) lookup_limit: Option<(u64, u64)>,
    /// Go `PhysicalIndexLookUpReader.Paging`: the first lookup-task window.
    pub(crate) lookup_batch_size: Option<u64>,
    /// Every predicate recorded by Go's logical `DataSource.AllConds` reached
    /// `PushedDownConds`. When this is false, a root `Selection` still owns
    /// part of the written leaf predicate and executor lowering must not mark
    /// that whole predicate as consumed merely because a reader was selected.
    pub(crate) consumes_leaf_filter: bool,
}

impl AccessDecision {
    /// Resolve a physical scan receipt to one executor leaf. Go retains
    /// `TableAsName` on both physical scan families, so aliases distinguish
    /// repeated uses of the same table after join reorder.
    pub(crate) fn for_leaf<'a>(
        decisions: &'a [Self],
        table_id: i64,
        visible: &str,
    ) -> Option<&'a Self> {
        let mut matches = decisions.iter().filter(|decision| {
            decision.table_id == table_id
                && decision
                    .relation
                    .as_ref()
                    .is_none_or(|relation| relation.eq_ignore_ascii_case(visible))
        });
        let selected = matches.next()?;
        matches.next().is_none().then_some(selected)
    }

    pub(crate) fn keep_order(&self) -> bool {
        match &self.path {
            AccessPath::Table { keep_order, .. } | AccessPath::Index { keep_order, .. } => {
                *keep_order
            }
        }
    }
}

/// The root join selected below the outer aggregation by the shared physical
/// search.  Execution consumes this receipt instead of independently costing
/// the same join families a second time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JoinDecision {
    Merge {
        keys: Vec<(RelColumn, RelColumn)>,
        desc: bool,
        requirements: JoinChildRequirements,
        children: JoinChildren,
    },
    Hash {
        build_child_idx: usize,
        requirements: JoinChildRequirements,
        children: JoinChildren,
    },
    Index {
        kind: tidb_planner::plan_cost_ver2::IndexJoinKind,
        inner_child_idx: usize,
        /// Go `PhysicalIndexHashJoin.KeepOuterOrder`. Plain index join and
        /// index merge join preserve their outer stream by construction.
        keep_outer_order: bool,
        /// A stable identity from the selected physical inner subtree. Logical
        /// join reorder can swap child ordinals, so the executor maps this id
        /// back to the written side instead of treating `inner_child_idx` as
        /// an AST-side index.
        inner_table_id: Option<i64>,
        /// Go's physical scan retains `TableAsName`. Together with the table
        /// id this identifies the exact written leaf in a self-join, without
        /// another executor-side operator-admission walk.
        inner_relation: Option<String>,
        /// The selected secondary index when the inner access is an index
        /// scan. `None` denotes a table/common-handle path or an inner
        /// composite whose first physical leaf is not the dynamic probe.
        inner_index_id: Option<i64>,
        /// The root reader already selected on that dynamic inner access.
        /// Go carries this in the completed inner task; executor lowering
        /// must not recompute covering from its own column demand.
        inner_reader: Option<AccessReader>,
        /// The aggregation family already selected on the rebuilt inner
        /// subtree, when the admitted index-join child retains aggregation.
        inner_aggregation: Option<AggregationFamily>,
        requirements: JoinChildRequirements,
        children: JoinChildren,
    },
}

/// The exact property one physical join required from one child. Columns use
/// stable source identities because executor column pruning changes offsets
/// after the physical search has completed.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct JoinChildRequirement {
    pub(crate) columns: Vec<RelColumn>,
    pub(crate) desc: bool,
    /// The selected child starts with Go's physical Sort enforcer. In that
    /// case the access subtree is deliberately unordered and executor
    /// lowering must build this Sort instead of asking the scan to satisfy
    /// the parent join property directly.
    pub(crate) enforced_sort: bool,
}

/// Both child properties retained on the selected physical join.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct JoinChildRequirements {
    pub(crate) left: JoinChildRequirement,
    pub(crate) right: JoinChildRequirement,
}

/// Planner-selected join receipts below the two physical children. Unary
/// projection/aggregation/reader nodes are transparent: a child receipt is
/// present when that physical subtree contains another join.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct JoinChildren {
    left: Option<Box<JoinDecision>>,
    right: Option<Box<JoinDecision>>,
}

impl JoinDecision {
    pub(crate) fn child(&self, index: usize) -> Option<&JoinDecision> {
        let children = match self {
            Self::Merge { children, .. }
            | Self::Hash { children, .. }
            | Self::Index { children, .. } => children,
        };
        match index {
            0 => children.left.as_deref(),
            1 => children.right.as_deref(),
            _ => None,
        }
    }

    pub(crate) fn child_requirement(&self, index: usize) -> Option<&JoinChildRequirement> {
        let requirements = match self {
            Self::Merge { requirements, .. }
            | Self::Hash { requirements, .. }
            | Self::Index { requirements, .. } => requirements,
        };
        match index {
            0 => Some(&requirements.left),
            1 => Some(&requirements.right),
            _ => None,
        }
    }
}

/// The aggregation facts the live executor consumes from one logical-plan
/// build and one physical enumeration.
#[derive(Clone, Debug, Default)]
pub(crate) struct AggregationDecision {
    /// Go's `AggregationEliminator` replaced the logical aggregation with a
    /// projection. The executor still has to lower that projection, but it
    /// must not independently re-prove whether elimination is legal.
    pub(crate) eliminated: bool,
    pub(crate) order: Option<AggregationOrder>,
    pub(crate) family: Option<AggregationFamily>,
    pub(crate) cop_family: Option<AggregationFamily>,
    pub(crate) join: Option<JoinDecision>,
    pub(crate) access: Vec<AccessDecision>,
    /// The selected physical tree discharged this SELECT's ORDER BY through
    /// a NominalSort, so executor lowering must preserve the chosen child
    /// order instead of installing and costing another Sort/TopN itself.
    pub(crate) order_satisfied: bool,
}

fn scan_access(plan: &PhysicalPlan, reader: AccessReader) -> Option<AccessDecision> {
    match plan {
        PhysicalPlan::TableScan(scan) => Some(AccessDecision {
            table_id: scan.table_id,
            relation: scan.table_as_name.clone(),
            path: AccessPath::Table {
                ranges: scan.ranges.clone(),
                keep_order: scan.keep_order,
                desc: scan.desc,
            },
            reader,
            estimated_rows: scan.base.base.stats_info().map(StatsInfo::row_count),
            index_filter: AccessFilter::default(),
            table_filter: AccessFilter::default(),
            cop_projection: None,
            pushed_limit: None,
            pushed_topn: None,
            lookup_limit: None,
            lookup_batch_size: None,
            consumes_leaf_filter: false,
        }),
        PhysicalPlan::IndexScan(scan) => Some(AccessDecision {
            table_id: scan.table_id,
            relation: scan.table_as_name.clone(),
            path: AccessPath::Index {
                index_id: scan.index_id,
                ranges: scan.ranges.clone(),
                keep_order: scan.keep_order,
                desc: scan.desc,
            },
            reader,
            estimated_rows: scan.base.base.stats_info().map(StatsInfo::row_count),
            index_filter: AccessFilter::default(),
            table_filter: AccessFilter::default(),
            cop_projection: None,
            pushed_limit: None,
            pushed_topn: None,
            lookup_limit: None,
            lookup_batch_size: None,
            consumes_leaf_filter: false,
        }),
        PhysicalPlan::PointGet(get) => Some(AccessDecision {
            table_id: get.table_id,
            relation: None,
            path: get.index_id.map_or_else(
                || AccessPath::Table {
                    ranges: get.ranges.clone(),
                    keep_order: true,
                    desc: false,
                },
                |index_id| AccessPath::Index {
                    index_id,
                    ranges: get.ranges.clone(),
                    keep_order: true,
                    desc: false,
                },
            ),
            reader,
            estimated_rows: get.base.base.stats_info().map(StatsInfo::row_count),
            index_filter: AccessFilter::default(),
            table_filter: AccessFilter::default(),
            cop_projection: None,
            pushed_limit: None,
            pushed_topn: None,
            lookup_limit: None,
            lookup_batch_size: None,
            consumes_leaf_filter: false,
        }),
        PhysicalPlan::BatchPointGet(get) => Some(AccessDecision {
            table_id: get.table_id,
            relation: None,
            path: get.index_id.map_or_else(
                || AccessPath::Table {
                    ranges: get.ranges.clone(),
                    keep_order: false,
                    desc: false,
                },
                |index_id| AccessPath::Index {
                    index_id,
                    ranges: get.ranges.clone(),
                    keep_order: false,
                    desc: false,
                },
            ),
            reader,
            estimated_rows: get.base.base.stats_info().map(StatsInfo::row_count),
            index_filter: AccessFilter::default(),
            table_filter: AccessFilter::default(),
            cop_projection: None,
            pushed_limit: None,
            pushed_topn: None,
            lookup_limit: None,
            lookup_batch_size: None,
            consumes_leaf_filter: false,
        }),
        _ => plan
            .children()
            .iter()
            .find_map(|child| scan_access(child, reader)),
    }
}

/// Finds the one access scan inside a reader-owned cop plan. Go's reader
/// stores pushed Selection/TopN/Agg nodes above the scan, so inspecting only
/// the reader plan's root loses the access identity as soon as any of those
/// operators is present.
fn scan_access_in_plan(plan: &PhysicalPlan, reader: AccessReader) -> Option<AccessDecision> {
    if let Some(access) = scan_access(plan, reader) {
        return Some(access);
    }
    let mut matches = plan
        .children()
        .iter()
        .filter_map(|child| scan_access_in_plan(child, reader));
    let access = matches.next()?;
    matches.next().is_none().then_some(access)
}

fn reader_access(plan: &PhysicalPlan) -> Option<AccessDecision> {
    match plan {
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref()
            .and_then(|plan| scan_access_in_plan(plan, AccessReader::Table)),
        PhysicalPlan::IndexReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(|plan| scan_access_in_plan(plan, AccessReader::Index)),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .index_plan
            .as_deref()
            .and_then(|plan| scan_access_in_plan(plan, AccessReader::IndexLookup)),
        PhysicalPlan::PointGet(_) => scan_access(plan, AccessReader::Point),
        PhysicalPlan::BatchPointGet(_) => scan_access(plan, AccessReader::BatchPoint),
        _ => None,
    }
}

/// The reader family already selected for one dynamic index-join access.
/// Matching is exact and unique within the received inner subtree; an
/// ambiguous self-join receipt fails closed instead of re-proving covering.
fn selected_access(
    plan: &PhysicalPlan,
    table_id: i64,
    index_id: Option<i64>,
) -> Option<AccessDecision> {
    fn matches(access: &AccessDecision, table_id: i64, index_id: Option<i64>) -> bool {
        if access.table_id != table_id {
            return false;
        }
        match (&access.path, index_id) {
            (AccessPath::Table { .. }, None) => true,
            (
                AccessPath::Index {
                    index_id: selected, ..
                },
                Some(wanted),
            ) => *selected == wanted,
            _ => false,
        }
    }

    fn collect(
        plan: &PhysicalPlan,
        table_id: i64,
        index_id: Option<i64>,
        found: &mut Option<AccessDecision>,
        ambiguous: &mut bool,
    ) {
        if let Some(access) = reader_access(plan) {
            if matches(&access, table_id, index_id) {
                if found.replace(access).is_some() {
                    *ambiguous = true;
                }
            }
            return;
        }
        for child in plan.children() {
            collect(child, table_id, index_id, found, ambiguous);
        }
    }

    let mut found = None;
    let mut ambiguous = false;
    collect(plan, table_id, index_id, &mut found, &mut ambiguous);
    (!ambiguous).then_some(found).flatten()
}

fn reader_limit(plan: &PhysicalPlan) -> Option<(u64, u64)> {
    match plan {
        PhysicalPlan::Limit(limit) => Some((limit.offset, limit.count)),
        _ => plan.children().iter().find_map(|child| reader_limit(child)),
    }
}

fn reader_has_topn(plan: &PhysicalPlan) -> bool {
    matches!(plan, PhysicalPlan::TopN(_))
        || plan.children().iter().any(|child| reader_has_topn(child))
}

fn reader_filter(plan: &PhysicalPlan, logical: &LogicalPlan) -> Option<AccessFilter> {
    fn collect_conditions(plan: &PhysicalPlan, conditions: &mut Vec<Expression>) {
        if let PhysicalPlan::Selection(selection) = plan {
            if selection.from_data_source {
                conditions.extend(selection.conditions.iter().cloned());
            }
        }
        for child in plan.children() {
            collect_conditions(child, conditions);
        }
    }

    let mut conditions = Vec::new();
    collect_conditions(plan, &mut conditions);
    access_filter(&conditions, logical)
}

/// Finds the direct-column projection inside one reader-owned cop plan. A
/// computed projection cannot be represented by `DAGRequest.output_offsets`,
/// so it deliberately produces no lowering receipt and stays as an executor
/// projection.
fn reader_projection(plan: &PhysicalPlan, logical: &LogicalPlan) -> Option<Vec<RelColumn>> {
    if let PhysicalPlan::Projection(projection) = plan {
        return projection
            .exprs
            .iter()
            .map(|expression| {
                expression
                    .as_column()
                    .and_then(|column| physical_column_name(column, logical))
            })
            .collect();
    }
    let mut projections = plan
        .children()
        .iter()
        .filter_map(|child| reader_projection(child, logical));
    let projection = projections.next()?;
    projections.next().is_none().then_some(projection)
}

fn access_filter(conditions: &[Expression], logical: &LogicalPlan) -> Option<AccessFilter> {
    let mut columns = Vec::new();
    for condition in conditions {
        for column in tidb_expr::simple_expr::extract_columns(condition) {
            if columns
                .iter()
                .any(|(unique_id, _): &(i64, RelColumn)| *unique_id == column.unique_id)
            {
                continue;
            }
            let name = physical_column_name(&column, logical)?;
            columns.push((column.unique_id, name));
        }
    }
    let mut conditions = conditions.to_vec();
    for condition in &mut conditions {
        materialize_cached_expression(condition);
    }
    Some(AccessFilter {
        conditions,
        columns,
    })
}

/// The retained physical tree keeps parameter/deferred markers for its next
/// in-place rebuild. The per-execution lowering receipt is private, so detach
/// those markers from its cloned expressions after their current values have
/// been materialized.
pub(super) fn materialize_cached_expression(expression: &mut Expression) {
    match expression {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => {}
        Expression::Constant(constant) => {
            constant.param_marker = None;
            constant.deferred_expr = None;
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                materialize_cached_expression(argument);
            }
        }
    }
}

fn merge_filter(target: &mut AccessFilter, extra: AccessFilter) {
    target.conditions.extend(extra.conditions);
    for binding in extra.columns {
        if !target.columns.iter().any(|present| present.0 == binding.0) {
            target.columns.push(binding);
        }
    }
}

fn logical_source_consumes_all_conditions(
    plan: &LogicalPlan,
    table_id: i64,
    relation: Option<&str>,
) -> Option<bool> {
    fn collect<'a>(
        plan: &'a LogicalPlan,
        table_id: i64,
        relation: Option<&str>,
        found: &mut Vec<&'a tidb_planner::logical::DataSource>,
    ) {
        if let LogicalPlan::DataSource(source) = plan {
            let visible = source
                .table_as_name
                .as_deref()
                .unwrap_or(&source.table_name);
            if (source.table_id == table_id || source.physical_table_id == table_id)
                && relation.is_none_or(|relation| relation.eq_ignore_ascii_case(visible))
            {
                found.push(source);
            }
        }
        for child in plan.children() {
            collect(child, table_id, relation, found);
        }
    }

    let mut found = Vec::new();
    collect(plan, table_id, relation, &mut found);
    let source = match found.as_slice() {
        [source] => *source,
        _ => return None,
    };
    if source.all_conds.len() != source.pushed_down_conds.len() {
        return Some(false);
    }
    // Conditions form a multiset, not merely a set: two copies in AllConds
    // must not both match the same single pushed condition. Go moves the
    // expression nodes themselves, so matching each pushed occurrence at
    // most once is the receipt-equivalent check here.
    let mut matched = vec![false; source.pushed_down_conds.len()];
    Some(source.all_conds.iter().all(|condition| {
        source
            .pushed_down_conds
            .iter()
            .enumerate()
            .find(|(index, pushed)| !matched[*index] && condition.equal(pushed))
            .is_some_and(|(index, _)| {
                matched[index] = true;
                true
            })
    }))
}

fn access_decisions(plan: &PhysicalPlan, logical: &LogicalPlan) -> Option<Vec<AccessDecision>> {
    fn collect(
        plan: &PhysicalPlan,
        logical: &LogicalPlan,
        inherited_conditions: &[Expression],
        decisions: &mut Vec<AccessDecision>,
    ) -> Option<()> {
        let mut inherited = inherited_conditions.to_vec();
        if let PhysicalPlan::Selection(selection) = plan {
            if selection.from_data_source {
                inherited.extend(selection.conditions.iter().cloned());
            }
        }
        let mut access = match plan {
            PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref().and_then(|plan| {
                let mut access = scan_access_in_plan(plan, AccessReader::Table)?;
                access.table_filter = reader_filter(plan, logical)?;
                access.cop_projection = reader_projection(plan, logical);
                access.pushed_limit = reader_limit(plan);
                access.pushed_topn = reader_has_topn(plan).then_some(ReaderTopN::Cop);
                Some(access)
            }),
            PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref().and_then(|plan| {
                let mut access = scan_access_in_plan(plan, AccessReader::Index)?;
                access.index_filter = reader_filter(plan, logical)?;
                access.cop_projection = reader_projection(plan, logical);
                access.pushed_limit = reader_limit(plan);
                access.pushed_topn = reader_has_topn(plan).then_some(ReaderTopN::Cop);
                Some(access)
            }),
            // The index half names the access path; the table half is the
            // handle lookup belonging to that same path, not a second leaf.
            PhysicalPlan::IndexLookUpReader(reader) => {
                reader.index_plan.as_deref().and_then(|plan| {
                    let mut access = scan_access_in_plan(plan, AccessReader::IndexLookup)?;
                    access.index_filter = reader_filter(plan, logical)?;
                    access.table_filter = match reader.table_plan.as_deref() {
                        Some(plan) => reader_filter(plan, logical)?,
                        None => AccessFilter::default(),
                    };
                    access.cop_projection = reader
                        .table_plan
                        .as_deref()
                        .and_then(|plan| reader_projection(plan, logical))
                        .or_else(|| reader_projection(plan, logical));
                    access.pushed_limit = reader_limit(plan);
                    access.pushed_topn = reader_has_topn(plan).then_some(ReaderTopN::IndexLookup);
                    access.lookup_limit =
                        reader.pushed_limit.map(|limit| (limit.offset, limit.count));
                    access.lookup_batch_size = reader.paging.then_some(reader.expect_cnt);
                    Some(access)
                })
            }
            PhysicalPlan::PointGet(_) => scan_access(plan, AccessReader::Point),
            PhysicalPlan::BatchPointGet(_) => scan_access(plan, AccessReader::BatchPoint),
            _ => None,
        };
        if let Some(access) = access.as_mut() {
            access.consumes_leaf_filter = logical_source_consumes_all_conditions(
                logical,
                access.table_id,
                access.relation.as_deref(),
            )?;
            let root_filter = access_filter(&inherited, logical)?;
            // `CopTask.RootTaskConds` becomes a Selection above the reader.
            // It is still part of this DataSource's exact physical receipt;
            // preserve it as the table-side filter for mechanical lowering.
            merge_filter(&mut access.table_filter, root_filter);
        }
        if let Some(access) = access {
            decisions.push(access);
            return Some(());
        }
        for child in plan.children() {
            collect(child, logical, &inherited, decisions)?;
        }
        Some(())
    }

    let mut decisions = Vec::new();
    collect(plan, logical, &[], &mut decisions)?;
    Some(decisions)
}

struct InitStats<'a> {
    catalog: &'a Catalog,
    select: Option<&'a tidb_ast::SelectStmt>,
    default_string_match_selectivity: f64,
    zone: &'a tidb_datatype::SessionTimeZone,
}

impl OwnedRewrite for InitStats<'_> {
    type Down = ();
    type Up = ();

    fn descend(&mut self, node: &mut LogicalPlan, (): Self::Down) -> Descend<Self::Down, Self::Up> {
        let LogicalPlan::DataSource(source) = node else {
            return Descend::Children(vec![(); node.children().len()]);
        };
        let statistics = self.catalog.table_statistics(source.table_id);
        let row_count = statistics.map_or(crate::plan_trace::PSEUDO_ROW_COUNT, |statistics| {
            if statistics.pseudo {
                crate::plan_trace::PSEUDO_ROW_COUNT
            } else {
                statistics.row_count.max(0) as f64
            }
        });
        let loaded_columns = statistics
            .map(|statistics| statistics.columns.keys().copied().collect())
            .unwrap_or_default();
        let loaded_indexes = statistics
            .map(|statistics| statistics.indexes.keys().copied().collect())
            .unwrap_or_default();
        let ndvs = source
            .columns
            .iter()
            .zip(
                source
                    .base
                    .base
                    .schema()
                    .into_iter()
                    .flat_map(|schema| &schema.columns),
            )
            .map(|(metadata, column)| {
                // Go `cardinality.EstimateColumnNDV`: a pseudo or missing
                // histogram uses `RealtimeCount * distinctFactor` (0.8),
                // while an analyzed histogram scales its NDV from analyze
                // time to the current realtime row count.
                let ndv = statistics.map_or(row_count * 0.8, |statistics| {
                    if statistics.pseudo {
                        row_count * 0.8
                    } else {
                        statistics
                            .estimate_column_ndv(metadata.id, &loaded_columns, &loaded_indexes)
                            .unwrap_or(row_count * 0.8)
                    }
                });
                (column.unique_id, ndv)
            })
            .collect::<Vec<_>>();
        source.table_stats = Some(StatsInfo::new(row_count, ndvs));
        if let (Some(_), Some(predicate), Some(TableEntry::Kv(table)), Some(table_stats)) = (
            self.select,
            self.select.and_then(|select| select.where_clause.as_ref()),
            self.catalog.get_in(&source.db_name, &source.table_name),
            source.table_stats.clone(),
        ) {
            let statistics = statistics.map(AsRef::as_ref);
            source.table_path_count_after_access =
                crate::handle_range::build_handle_ranges(table, predicate, self.zone)
                    .map(|built| {
                        crate::handle_range::handle_range_row_count(
                            table,
                            &built.ranges,
                            statistics,
                        )
                    })
                    .or(Some(row_count));
            for index in table.plan_indexes() {
                let columns = index
                    .column_offsets
                    .iter()
                    .enumerate()
                    .map(|(position, offset)| {
                        let column = table.columns.get(*offset)?;
                        Some(crate::index_range::RangeColumn {
                            name: column.name.clone(),
                            field_type: column.field_type.clone(),
                            prefix_len: index.prefix_length(position),
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(columns) = columns else {
                    continue;
                };
                let Some(built) = crate::index_range::detach_cond_and_build_range_for_index(
                    &columns, predicate, self.zone,
                ) else {
                    continue;
                };
                source.index_path_count_after_access.insert(
                    index.id,
                    crate::access_cost::index_range_row_count(
                        index,
                        table,
                        &built.ranges,
                        statistics,
                        row_count,
                    ),
                );
            }
            let visible = source
                .table_as_name
                .as_deref()
                .unwrap_or(&source.table_name);
            let scope = PlanTrace::single_table_scope(
                visible,
                Some(source.db_name.clone()),
                table
                    .visible_columns()
                    .iter()
                    .map(|column| (column.name.clone(), column.field_type.clone()))
                    .collect(),
            );
            let selectivity = crate::access_cost::selectivity_with_default_string_match_selectivity(
                predicate,
                table,
                &crate::driver::from::scope_resolver(&scope),
                statistics,
                self.default_string_match_selectivity,
            );
            source.base.base.set_stats(Some(table_stats.scale(
                selectivity,
                tidb_planner::cardinality::derive_stats::DEF_SCALE_NDV_SKEW_RATIO,
            )));
        }
        source.table_scan_penalty = tidb_planner::plan_cost_ver2::TableScanPenaltyInput {
            has_range_info: false,
            // Go's `tidb_opt_prefer_range_scan` default is ON. This executor
            // does not expose a session override yet.
            allow_prefer_range_scan: true,
            pseudo_stats: statistics.is_none_or(|statistics| statistics.pseudo),
            analyze_row_count: statistics
                .map_or(-1, |statistics| statistics.analyze_row_count() as i64),
            modify_count: statistics.map_or(0, |statistics| statistics.modify_count),
            has_partition_scan: false,
            has_index_force: false,
        };
        Descend::Stop(())
    }

    fn ascend(&mut self, node: LogicalPlan, _children: Vec<Self::Up>) -> (LogicalPlan, Self::Up) {
        (node, ())
    }
}

fn reader_aggregation_family(plan: &PhysicalPlan) -> Option<AggregationFamily> {
    let pushed = match plan {
        PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref(),
        PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref(),
        PhysicalPlan::IndexLookUpReader(reader) => reader.table_plan.as_deref(),
        _ => None,
    }?;
    match pushed {
        PhysicalPlan::HashAgg(_) => Some(AggregationFamily::Hash),
        PhysicalPlan::StreamAgg(_) => Some(AggregationFamily::Stream),
        _ => None,
    }
}

fn outer_physical_aggregation(mut plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
    loop {
        match plan {
            PhysicalPlan::HashAgg(_) | PhysicalPlan::StreamAgg(_) => return Some(plan),
            _ => {
                let [child] = plan.children() else {
                    return None;
                };
                plan = child;
            }
        }
    }
}

pub(super) fn physical_aggregation_families(
    plan: &PhysicalPlan,
) -> (Option<AggregationFamily>, Option<AggregationFamily>) {
    let Some(aggregation) = outer_physical_aggregation(plan) else {
        return (None, None);
    };
    let root = match aggregation {
        PhysicalPlan::HashAgg(_) => Some(AggregationFamily::Hash),
        PhysicalPlan::StreamAgg(_) => Some(AggregationFamily::Stream),
        _ => None,
    };
    let cop = aggregation
        .children()
        .first()
        .and_then(|child| reader_aggregation_family(child));
    (root, cop)
}

/// Whether the SELECT-level order still needs an executable enforcer. Merge
/// join child sorts and aggregation input sorts are below the first physical
/// aggregation/join/reader, so only the outer unary spine belongs to the
/// SELECT's LogicalSort/LogicalTopN.
fn outer_order_is_enforced(mut plan: &PhysicalPlan) -> bool {
    loop {
        match plan {
            PhysicalPlan::Sort(_) | PhysicalPlan::TopN(_) => return true,
            PhysicalPlan::HashAgg(_)
            | PhysicalPlan::StreamAgg(_)
            | PhysicalPlan::HashJoin(_)
            | PhysicalPlan::MergeJoin(_)
            | PhysicalPlan::IndexJoin(_)
            | PhysicalPlan::TableReader(_)
            | PhysicalPlan::IndexReader(_)
            | PhysicalPlan::IndexLookUpReader(_)
            | PhysicalPlan::IndexMergeReader(_)
            | PhysicalPlan::PointGet(_)
            | PhysicalPlan::BatchPointGet(_)
            | PhysicalPlan::TableScan(_)
            | PhysicalPlan::IndexScan(_) => return false,
            _ => {
                let [child] = plan.children() else {
                    return false;
                };
                plan = child;
            }
        }
    }
}

fn named_column_in_tree(plan: &LogicalPlan, unique_id: i64) -> Option<RelColumn> {
    if let Some(column) = named_column(plan, unique_id) {
        return Some(column);
    }
    plan.children()
        .iter()
        .find_map(|child| named_column_in_tree(child, unique_id))
}

/// Finds the relation/column identity at the owning data source before a
/// projection or derived-table alias renames it. Executor lowering can then
/// resolve that stable origin through the syntax-side projection instead of
/// mistaking an alias (`key_a`) for a physical column of `t2`.
fn source_column_in_tree(plan: &LogicalPlan, unique_id: i64) -> Option<RelColumn> {
    if matches!(plan, LogicalPlan::DataSource(_)) {
        return named_column(plan, unique_id);
    }
    plan.children()
        .iter()
        .find_map(|child| source_column_in_tree(child, unique_id))
}

fn physical_column_name(
    column: &tidb_expr::column::Column,
    logical: &LogicalPlan,
) -> Option<RelColumn> {
    if let Some(column) = source_column_in_tree(logical, column.unique_id) {
        return Some(column);
    }
    let parts = column.orig_name.split('.').collect::<Vec<_>>();
    if let [.., relation, name] = parts.as_slice() {
        if !relation.is_empty() && !name.is_empty() {
            return Some(RelColumn {
                relation: (*relation).to_owned(),
                column: (*name).to_owned(),
            });
        }
    }
    named_column_in_tree(logical, column.unique_id)
}

fn join_child_requirement(
    plan: &PhysicalPlan,
    child_index: usize,
    logical: &LogicalPlan,
) -> Option<JoinChildRequirement> {
    let property = plan.base().child_req_prop(child_index)?;
    let (all_same_order, desc) = property.all_same_order();
    if !all_same_order {
        return None;
    }
    let columns = property
        .sort_items
        .iter()
        .map(|item| {
            source_column_in_tree(logical, item.col)
                .or_else(|| named_column_in_tree(logical, item.col))
        })
        .collect::<Option<Vec<_>>>()?;
    let enforced_sort = match plan.children().get(child_index) {
        Some(PhysicalPlan::Sort(sort)) => {
            let sort_columns = sort
                .by_items
                .iter()
                .map(|item| {
                    source_column_in_tree(logical, item.col)
                        .or_else(|| named_column_in_tree(logical, item.col))
                        .map(|column| (column, item.desc))
                })
                .collect::<Option<Vec<_>>>()?;
            if sort_columns.len() != columns.len()
                || sort_columns.iter().zip(&columns).any(
                    |((sort_column, sort_desc), required_column)| {
                        sort_column != required_column || *sort_desc != desc
                    },
                )
            {
                return None;
            }
            true
        }
        _ => false,
    };
    Some(JoinChildRequirement {
        columns,
        desc,
        enforced_sort,
    })
}

fn join_child_requirements(
    plan: &PhysicalPlan,
    logical: &LogicalPlan,
) -> Option<JoinChildRequirements> {
    Some(JoinChildRequirements {
        left: join_child_requirement(plan, 0, logical)?,
        right: join_child_requirement(plan, 1, logical)?,
    })
}

fn join_decision_tree(mut plan: &PhysicalPlan, logical: &LogicalPlan) -> Option<JoinDecision> {
    loop {
        match plan {
            PhysicalPlan::MergeJoin(join) => {
                let keys = join
                    .left_join_keys
                    .iter()
                    .zip(&join.right_join_keys)
                    .map(|(left, right)| {
                        Some((
                            physical_column_name(left, logical)?,
                            physical_column_name(right, logical)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?;
                let children = join_children(plan, logical);
                return Some(JoinDecision::Merge {
                    keys,
                    desc: join.desc,
                    requirements: join_child_requirements(plan, logical)?,
                    children,
                });
            }
            PhysicalPlan::HashJoin(join) => {
                let build_child_idx = if join.use_outer_to_build {
                    1usize.checked_sub(join.inner_child_idx)?
                } else {
                    join.inner_child_idx
                };
                return Some(JoinDecision::Hash {
                    build_child_idx,
                    requirements: join_child_requirements(plan, logical)?,
                    children: join_children(plan, logical),
                });
            }
            PhysicalPlan::IndexJoin(join) => {
                let inner_child = plan.children().get(join.inner_child_idx);
                let inner_access = join.inner_access_table_id.and_then(|table_id| {
                    selected_access(inner_child?, table_id, join.inner_access_index_id)
                });
                let inner_aggregation = inner_child
                    .and_then(|child| outer_physical_aggregation(child))
                    .and_then(|aggregation| match aggregation {
                        PhysicalPlan::HashAgg(_) => Some(AggregationFamily::Hash),
                        PhysicalPlan::StreamAgg(_) => Some(AggregationFamily::Stream),
                        _ => None,
                    });
                return Some(JoinDecision::Index {
                    kind: join.kind,
                    inner_child_idx: join.inner_child_idx,
                    keep_outer_order: join.keep_outer_order,
                    inner_table_id: join.inner_access_table_id,
                    inner_relation: inner_access
                        .as_ref()
                        .and_then(|access| access.relation.clone()),
                    inner_index_id: join.inner_access_index_id,
                    inner_reader: inner_access.map(|access| access.reader),
                    inner_aggregation,
                    requirements: join_child_requirements(plan, logical)?,
                    children: join_children(plan, logical),
                });
            }
            _ => {
                let [child] = plan.children() else {
                    return None;
                };
                plan = child;
            }
        }
    }
}

fn join_children(plan: &PhysicalPlan, logical: &LogicalPlan) -> JoinChildren {
    let children = plan.children();
    JoinChildren {
        left: children
            .first()
            .and_then(|child| join_decision_tree(child, logical))
            .map(Box::new),
        right: children
            .get(1)
            .and_then(|child| join_decision_tree(child, logical))
            .map(Box::new),
    }
}

fn planner_logical_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Option<(LogicalPlan, PhysicalPlan)> {
    let (logical, plan_ids, column_ids) =
        planner_optimized_select(select, catalog, current_database, ctx, false)?;
    let physical = {
        let coster = Ver2Coster::from_env(ctx.optimizer_cost_env());
        let mut dispatch = DispatchContext::new(&plan_ids, &coster, 1.0)
            .with_ordering_index_selectivity_ratio(ctx.ordering_index_selectivity_ratio())
            .with_projection_push_down(ctx.allow_projection_push_down())
            .with_limit_push_down_threshold(ctx.limit_push_down_threshold())
            .with_paging(ctx.optimizer_cost_env().session.enable_paging)
            .with_hash_join_concurrency(
                ctx.optimizer_cost_env()
                    .session
                    .hash_join_concurrency
                    .max(1.0) as usize,
            )
            .with_column_ids(&column_ids);
        let task = find_best_task(&logical, &PhysicalProperty::default(), &mut dispatch).ok()?;
        task.plan()?.clone()
    };
    Some((logical, physical))
}

fn decision_from_plans(
    select: &tidb_ast::SelectStmt,
    logical: &LogicalPlan,
    physical: &PhysicalPlan,
) -> Option<AggregationDecision> {
    let aggregate_select = is_aggregate_select(select);
    let aggregation = outer_aggregation_plan(logical);
    let join = join_decision_tree(&physical, &logical);
    let access = access_decisions(&physical, &logical)?;
    let order_satisfied = !select.order_by.is_empty() && !outer_order_is_enforced(&physical);
    let family = aggregation.and_then(|_| {
        let physical_aggregation = outer_physical_aggregation(&physical)?;
        let cop_family = physical_aggregation
            .children()
            .first()
            .and_then(|child| reader_aggregation_family(child));
        match physical_aggregation {
            PhysicalPlan::HashAgg(_) => Some((AggregationFamily::Hash, cop_family)),
            PhysicalPlan::StreamAgg(_) => Some((AggregationFamily::Stream, cop_family)),
            _ => None,
        }
    });
    // A surviving logical aggregation must have one selected physical
    // aggregation.  Returning an otherwise-valid receipt with `family=None`
    // used to make executor lowering silently choose HashAgg, reintroducing
    // a second planner after `findBestTask`.  Treat a malformed physical tree
    // as no receipt so the authoritative caller fails closed.
    if aggregation.is_some() && family.is_none() {
        return None;
    }
    let aggregation_order = aggregation.and_then(|aggregation_plan| {
        let LogicalPlan::Aggregation(aggregation) = aggregation_plan else {
            return None;
        };
        let child = aggregation.base.children().first()?;
        let physical_aggregation = outer_physical_aggregation(&physical)?;
        let required = physical_aggregation.base().child_req_prop(0)?;
        if required.sort_items.is_empty() {
            return None;
        }
        let required_columns = required
            .sort_items
            .iter()
            .map(|item| named_column(child, item.col))
            .collect::<Option<Vec<_>>>()?;
        let physical_group_columns = aggregation
            .group_by_items
            .iter()
            .map(|item| match item {
                Expression::Column(column) => named_column(child, column.unique_id),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let fixed_columns = equality_fixed_columns(child);
        Some(AggregationOrder::from_planner_columns(
            required_columns,
            fixed_columns,
            physical_group_columns,
            required.all_same_order().1,
            matches!(
                physical_aggregation.children().first(),
                Some(PhysicalPlan::Sort(_))
            ),
        ))
    });
    if aggregation.is_none() {
        return Some(AggregationDecision {
            eliminated: aggregate_select,
            order: None,
            family: None,
            cop_family: None,
            join,
            access,
            order_satisfied,
        });
    }
    if select.group_by.is_empty() && !select.distinct {
        return Some(AggregationDecision {
            eliminated: false,
            order: None,
            family: family.map(|(root, _)| root),
            cop_family: family.and_then(|(_, cop)| cop),
            join,
            access,
            order_satisfied,
        });
    }
    Some(AggregationDecision {
        eliminated: false,
        order: aggregation_order,
        family: family.map(|(root, _)| root),
        cop_family: family.and_then(|(_, cop)| cop),
        join,
        access,
        order_satisfied,
    })
}

fn planner_optimized_select(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
    use_plan_cache: bool,
) -> Option<(LogicalPlan, PlanIdAllocator, ColumnIdAllocator)> {
    let source = catalog.planner_catalog(current_database);
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let session_zone = ctx.session_zone();
    let mut builder = PlanBuilder::new(&source, ctx, &plan_ids, &column_ids, session_zone.clone());
    // Go `PlanBuilder.Build` enables column pruning before dispatching to
    // `buildSelect`. This bridge enters at the narrower Rust `build_select`
    // seam, so carry the wrapper's mandatory flag explicitly.
    builder.add_opt_flag(flags::PRUNE_COLUMNS);
    let (plan, flags) = builder.build_select(select).ok()?;
    let function_builder = RealFunctionBuilder::new(ctx);
    let rule_context = RuleContext {
        allocator: &plan_ids,
        builder: &function_builder,
        use_plan_cache,
        allow_derive_topn: true,
        disabled_rules: DisabledLogicalRules::default(),
    };
    let optimized = logical_optimize(&rule_context, flags, plan).ok()?.plan;
    let mut source_count = 0;
    optimized.walk_preorder(&mut |plan| {
        source_count += usize::from(matches!(plan, LogicalPlan::DataSource(_)));
    });
    let (mut optimized, ()) = fold_owned(
        &mut InitStats {
            catalog,
            select: (source_count == 1).then_some(select),
            default_string_match_selectivity: ctx.default_string_match_selectivity(),
            zone: &session_zone,
        },
        optimized,
        (),
    );
    optimized.recursive_derive_stats(&[]).ok()?;
    let logical = prepare_possible_properties(optimized).0;
    Some((logical, plan_ids, column_ids))
}

fn outer_aggregation_plan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    let mut current = plan;
    loop {
        if matches!(current, LogicalPlan::Aggregation(_)) {
            return Some(current);
        }
        let [child] = current.children() else {
            return None;
        };
        current = child;
    }
}

fn outer_aggregation(plan: &LogicalPlan) -> Option<&tidb_planner::logical::LogicalAggregation> {
    match outer_aggregation_plan(plan)? {
        LogicalPlan::Aggregation(aggregation) => Some(aggregation),
        _ => unreachable!("outer_aggregation_plan only returns Aggregation"),
    }
}

fn named_column(plan: &LogicalPlan, unique_id: i64) -> Option<RelColumn> {
    let schema = plan.schema()?;
    let position = schema
        .columns
        .iter()
        .position(|column| column.unique_id == unique_id)?;
    let origin = &schema.columns[position].orig_name;
    let parts = origin.split('.').collect::<Vec<_>>();
    if let [.., relation, column] = parts.as_slice() {
        if !relation.is_empty() && !column.is_empty() {
            return Some(RelColumn {
                relation: (*relation).to_owned(),
                column: (*column).to_owned(),
            });
        }
    }
    let name = plan.output_names().get(position)?;
    let relation = &name.names.table.lower;
    let column = &name.names.column.lower;
    if relation.is_empty() || column.is_empty() {
        return None;
    }
    Some(RelColumn {
        relation: relation.clone(),
        column: column.clone(),
    })
}

fn equality_fixed_columns(plan: &LogicalPlan) -> Vec<RelColumn> {
    let mut fixed = Vec::new();
    let mut work = vec![plan];
    while let Some(node) = work.pop() {
        let (conditions, owner) = match node {
            LogicalPlan::Selection(selection) => {
                (selection.conditions.as_slice(), node.children().first())
            }
            LogicalPlan::DataSource(source) => (source.pushed_down_conds.as_slice(), Some(node)),
            _ => (&[][..], None),
        };
        if let Some(owner) = owner {
            for condition in conditions {
                let Expression::ScalarFunction(function) = condition else {
                    continue;
                };
                let fixed_id = match function.get_args() {
                    [Expression::Column(column), Expression::Constant(_)]
                    | [Expression::Constant(_), Expression::Column(column)]
                        if function.func_name.lowercase() == "eq" =>
                    {
                        Some(column.unique_id)
                    }
                    _ => None,
                };
                if let Some(column) = fixed_id.and_then(|id| named_column(owner, id)) {
                    if !fixed.contains(&column) {
                        fixed.push(column);
                    }
                }
            }
        }
        work.extend(node.children());
    }
    fixed
}

/// Builds and optimizes one logical tree, then returns the physical receipts
/// consumed by executor lowering. Aggregation fields remain empty for a
/// non-aggregate SELECT, while its complete join receipt tree is still
/// available; this keeps the shared physical search authoritative for every
/// SELECT rather than only for statements whose root happens to aggregate.
pub(crate) fn select_decision(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Option<AggregationDecision> {
    if select.rollup {
        return None;
    }
    let (logical, physical) = planner_logical_select(select, catalog, current_database, ctx)?;
    decision_from_plans(select, &logical, &physical)
}

/// The complete physical tree retained by the prepared-plan cache. A hit
/// recursively rebuilds every parameter-dependent range in place and derives
/// the executor receipt from that rebuilt tree. No access,
/// aggregation, join, sort, or reader policy is re-run in the executor.
#[derive(Debug)]
pub(crate) struct CachedSelectPlan {
    select: tidb_ast::SelectStmt,
    physical: PhysicalPlan,
    generation: u64,
}

impl CachedSelectPlan {
    pub(crate) fn bind(&mut self, values: &[tidb_datatype::Datum]) -> Option<u64> {
        super::bind_prepared_select_in_place(&mut self.select, values).ok()?;
        self.physical
            .rebuild_plan_for_cache_in_place(
                &tidb_planner::physical_plan_cache::CachedPlanRebuildContext::new(values),
            )
            .ok()?;
        self.generation = self.generation.wrapping_add(1);
        Some(self.generation)
    }

    pub(crate) fn execution(
        &self,
        generation: u64,
    ) -> Option<(&tidb_ast::SelectStmt, &PhysicalPlan)> {
        (self.generation == generation).then_some((&self.select, &self.physical))
    }
}

/// Builds the same logical and physical plan as ordinary execution, with
/// plan-cache-safe logical rewrites enabled so parameter markers are not
/// folded into a value-specific shape.
pub(crate) fn cached_select_plan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Option<CachedSelectPlan> {
    if select.rollup {
        return None;
    }
    let (logical, plan_ids, column_ids) =
        planner_optimized_select(select, catalog, current_database, ctx, true)?;
    let physical = {
        let coster = Ver2Coster::from_env(ctx.optimizer_cost_env());
        let mut dispatch = DispatchContext::new(&plan_ids, &coster, 1.0)
            .with_ordering_index_selectivity_ratio(ctx.ordering_index_selectivity_ratio())
            .with_projection_push_down(ctx.allow_projection_push_down())
            .with_limit_push_down_threshold(ctx.limit_push_down_threshold())
            .with_paging(ctx.optimizer_cost_env().session.enable_paging)
            .with_hash_join_concurrency(
                ctx.optimizer_cost_env()
                    .session
                    .hash_join_concurrency
                    .max(1.0) as usize,
            )
            .with_column_ids(&column_ids);
        let task = find_best_task(&logical, &PhysicalProperty::default(), &mut dispatch).ok()?;
        task.plan()?.clone()
    };
    Some(CachedSelectPlan {
        select: select.clone(),
        physical,
        generation: 0,
    })
}

fn is_aggregate_select(select: &tidb_ast::SelectStmt) -> bool {
    select.distinct
        || !select.group_by.is_empty()
        || select.fields.fields().iter().any(|field| {
            matches!(field, tidb_ast::SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
        })
        || select
            .having
            .as_ref()
            .is_some_and(tidb_ast::Expr::has_aggregate_flag)
        || select
            .order_by
            .iter()
            .any(|item| item.expr.has_aggregate_flag())
}

/// Returns the logical aggregation-elimination receipt without running a
/// second physical enumeration. Derived-table cardinality modeling only needs
/// this logical fact; asking [`aggregation_decision`] used to cost every
/// physical family and then discard that work.
pub(crate) fn aggregation_eliminated(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_database: &str,
    ctx: &crate::StmtContext,
) -> Option<bool> {
    if select.rollup || !is_aggregate_select(select) {
        return None;
    }
    let (logical, _, _) = planner_optimized_select(select, catalog, current_database, ctx, false)?;
    Some(outer_aggregation(&logical).is_none())
}

#[cfg(test)]
mod tests {
    use super::{
        cached_select_plan, outer_aggregation, planner_optimized_select, select_decision,
        AggregationFamily,
    };
    use crate::Catalog;

    #[test]
    fn global_sum_family_comes_from_the_shared_cost_search() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE revenue (\
                id INT PRIMARY KEY, price DECIMAL(10,2), discount DECIMAL(4,2), k INT)",
            &mut catalog,
        )
        .unwrap();
        let ctx = crate::StmtContext::for_query();
        let stmt =
            tidb_parser::parse("SELECT SUM(price * discount) FROM revenue WHERE k >= 1 AND k <= 3")
                .unwrap();
        let tidb_ast::Stmt::Query(query) = &stmt else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };

        let (logical, _, _) = planner_optimized_select(select, &catalog, "test", &ctx, false)
            .expect("the shared logical builder accepts global SUM");
        let aggregation = outer_aggregation(&logical).expect("the logical aggregation survives");
        assert!(
            aggregation.possible_properties.orders.len() == 1
                && aggregation.possible_properties.orders[0].is_empty(),
            "a global aggregate offers Go's one empty child order"
        );
        let decision = select_decision(select, &catalog, "test", &ctx)
            .expect("the shared physical search answers");
        assert_eq!(decision.family, Some(AggregationFamily::Stream));
        assert_eq!(decision.cop_family, Some(AggregationFamily::Stream));
    }

    #[test]
    fn cached_sysbench_sum_uses_gos_stream_aggregation() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE sbtest1 (\
                id INTEGER NOT NULL AUTO_INCREMENT, \
                k INTEGER DEFAULT 0 NOT NULL, \
                c CHAR(120) DEFAULT '' NOT NULL, \
                pad CHAR(60) DEFAULT '' NOT NULL, \
                PRIMARY KEY (id), INDEX k_1(k))",
            &mut catalog,
        )
        .unwrap();
        let ctx = crate::StmtContext::for_query();
        let stmt =
            tidb_parser::parse("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 1 AND 100").unwrap();
        let tidb_ast::Stmt::Query(query) = &stmt else {
            panic!("not a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };

        let mut cached = cached_select_plan(select, &catalog, "test", &ctx)
            .expect("the prepared-plan cache accepts stock sysbench SUM");
        let generation = cached.bind(&[]).expect("the cached physical tree rebuilds");
        let (_, physical) = cached
            .execution(generation)
            .expect("the rebuilt generation is executable");
        assert_eq!(
            super::physical_aggregation_families(physical),
            (
                Some(AggregationFamily::Stream),
                Some(AggregationFamily::Stream)
            )
        );
    }
}
