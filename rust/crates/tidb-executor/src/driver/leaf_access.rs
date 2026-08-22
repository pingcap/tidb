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

//! HOW ONE LEAF OF A MULTI-TABLE `FROM` IS READ: the access path a table
//! under a join commits to, and the order that walk delivers.
//!
//! # Why this is not in [`super::access`]
//!
//! That file is the SINGLE-TABLE pipeline: `TryFastPlan`, the point gets, the
//! range path, and the prune/pushdown/limit negotiation that follows a
//! committed source. It is offered the statement's own `WHERE`.
//!
//! This one is Go's SAME `findBestTask` recursion arriving at a `DataSource`
//! that sits under a `LogicalJoin`, and it is offered no condition at all --
//! see [`leaf_index_path`]'s own doc for why, and for what closing that would
//! take. The two answer different questions with the same costing, so they
//! share [`crate::access_cost`] and nothing else.
//!
//! The second question is the one this module grew for: WHICH ORDER the walk
//! produces. A leaf whose parent requires an order is handed Go's non-empty
//! `prop` here, and reports back only what the branch that RAN delivers --
//! the verify half of the contract in [`super::merge_decision`]. The same
//! chooser also receives predicates that reference only this leaf, so a
//! clustered-handle or index range can be selected before the join runs.

use super::access::{index_key_part_name, source_schema_columns, PointPlanStmt};
use super::*;

/// The covering index a LEAF of a multi-table `FROM` reads instead of its
/// table, when Go's chooser prefers one; `None` leaves the whole-table scan
/// the leaf builder already installed.
///
/// # Why a leaf gets a choice at all
///
/// Go's `findBestTask` recurses through a `LogicalJoin` into EVERY
/// `DataSource` below it, and each one enumerates and costs its own access
/// paths. There is no separate rule for a table under a join: the join asks
/// its children for a plan and a child answers with the cheapest path it
/// has. This is that recursion, expressed where this tier builds the leaf.
///
/// # Why no `WHERE` reaches it, and why that is the safe half
///
/// The conditions of a leaf under a join arrive from three places -- the
/// statement's `WHERE`, the join's own `ON`, and an equality another leaf
/// supplies -- and which of them may narrow a given leaf depends on which
/// side of which outer join that leaf sits on. None of them is passed here.
/// The consequence is exactly the restriction that makes this safe: with no
/// condition, the only index path the enumeration can produce is the WHOLE
/// index, which reads every row of the table the table scan would have read.
/// So the choice changes which physical structure the rows arrive through
/// and nothing else -- Go's `IndexFullScan` over a covering index, the read
/// it prints wherever an index is narrower than the row.
///
/// The row ORDER does change (index order, not handle order), which is why
/// [`crate::driver::from::build_from`] declines the choice for a leaf whose
/// parent demanded an order of it.
///
/// # What it would take to offer a condition here, MEASURED
///
/// Two statements in `tests/integrationtest`'s recording turn on this, and
/// they need DIFFERENT Go rules -- neither of them this function's:
///
/// * `explain_easy`: `select * from t1 left join t2 on t1.c2 = t2.c1 where
///   t1.c1 > 1` reads `TableRangeScan table:t1 range:(1,+inf]` in TiDB and
///   `TableFullScan` here. The rule is `LogicalJoin.PredicatePushDown`'s
///   `LeftOuterJoin` arm: `extractOnCondition(predicates, true, false)` sends
///   a `WHERE` conjunct on the PRESERVED side down to that child and leaves
///   the inner side's above the join. The routing is safe on that side for a
///   reason that needs no null reasoning -- the `WHERE` rejects the same row
///   whether or not it was null-extended -- but the SAFETY ARGUMENT is about
///   the leaf's position in the join tree, which this call site is not told.
///   The single-table pipeline already builds the range
///   ([`choose_index_range_path`] returning [`ChosenPath::HandleRange`], which
///   `accept_handle_ranges` offers to the scan already installed), so the
///   missing pieces are the conjunct reaching here and the preserved-side
///   proof travelling with it.
///
/// * `executor/jointest/join`: `select /*+ TIDB_SMJ(t2) */ * from t1 left
///   outer join t2 on t1.a = t2.a and t1.a != 3` reads `TableRangeScan
///   table:t2 range:[-inf,3), (3,+inf]` in TiDB. That range is on the INNER
///   side and comes from no `WHERE` at all: `ne(t1.a, 3)` is an ON-condition
///   on the preserved side (it stays as the printed `left cond`), and
///   `expression.PropConstForOuterJoin`'s `propagateColumnEQ` derives
///   `ne(t2.a, 3)` from it through the join key, then `extractOnCondition`
///   sorts the derived conjunct to the right child. The same pass is what
///   emits the `not(isnull(t2.c1))` selection the first statement's recording
///   also carries. It is a union-find solver over the join's conditions
///   (`pkg/expression/constant_propagation.go`), not a routing rule.
///
/// Both are left as measured debt rather than attempted: the first needs an
/// ancestry fact this tier does not thread to a leaf, and the second needs a
/// solver that does not exist here yet.
///
/// # The ORDER request
///
/// `wanted` is Go's `prop.SortItems` for this `DataSource`, as offsets into
/// the leaf's own row. `None` is the EMPTY property -- `findBestTask` with
/// nothing to satisfy, which is the call this function has always been.
/// `Some` is `convertToIndexScan` under a non-empty one, where a candidate
/// survives only when `matchProperty` says its walk already produces the
/// order:
///
/// ```text
/// // matchProperty, the non-int-handle branch
/// if len(idxCols) < len(prop.SortItems) { return property.PropNotMatched }
/// // ... prop.SortItems must be a PREFIX of idxCols
/// ```
///
/// The table path is never returned by this function, so the caller reads a
/// `None` the same way under both: keep the whole-table scan already
/// installed, and report the order THAT walk delivers. Under an order request
/// that is the fail-closed answer -- the handle order either satisfies the
/// request, in which case the caller's own delivery report says so, or it does
/// not and the parent's merge join is dropped by the verify half of the
/// contract in [`crate::driver::merge_decision`].
// Eight, and the eighth is the ORDER the caller must satisfy: Go reaches it as
// `findBestTask`'s own `prop` argument, a second parameter beside the plan
// rather than a field of any of the others. Grouping the catalog inputs into a
// wrapper would name the table twice without changing what travels; the
// sibling choosers in this module carry the same allow.
#[allow(clippy::too_many_arguments)]
pub(crate) fn leaf_index_path(
    table: &KvTable,
    visible: &str,
    columns: &[(String, FieldType)],
    demand: &crate::driver::leaf_demand::LeafDemand,
    where_clause: Option<&tidb_ast::Expr>,
    hints: &crate::index_hints::AvailablePaths,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    wanted: Option<&[usize]>,
) -> Option<LeafAccessPath> {
    // A partitioned leaf would need the pruning this call site has no
    // conditions to run, and Go's `hasPartitionScan` penalty with it.
    if table.partition().is_some() {
        return None;
    }
    // Go's `isCoveringIndex(path.IdxCols, ds.schema.Columns)`: the columns
    // this leaf's parents still read after `rule_column_pruning`.
    let needed = demand.needed(visible, columns);
    let resolver = TableResolver {
        table_name: visible,
        columns,
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
    };
    let stats = catalog.table_statistics(table.stats_physical_id());
    let stats = stats.as_ref().map(AsRef::as_ref);
    let row_size = crate::access_cost::data_source_avg_row_size(table, &needed, stats);
    // Go's TryFastPlan runs before ordinary path costing. A join leaf can use
    // the same direct lookup when its local predicates pin a handle or a
    // complete unique/common key; a one-row source satisfies any requested
    // order trivially.
    if hints.allows_table() {
        if let Some(where_clause) = where_clause {
            let stmt = PointPlanStmt::of_write(Some(where_clause), &[], None);
            if let Ok(Some(handle)) =
                super::access::try_point_get(&stmt, table, columns, &ctx.session_zone())
            {
                return Some(LeafAccessPath::Point {
                    handle,
                    order: wanted.map(|order| order.to_vec()).unwrap_or_default(),
                    candidate: tidb_planner::candidate_cost::Candidate::Fixed {
                        rows: 1.0,
                        row_size,
                        cost: tidb_planner::plan_cost_ver2::point_get_cost(
                            None,
                            1.0,
                            row_size,
                            &tidb_planner::plan_cost_ver2::Ver2Factors::default().tidb_to_kv_net,
                            true,
                        )
                        .value(),
                        num_ranges: 1,
                    },
                });
            }
        }
    }
    let mut paths = crate::access_cost::enumerate_paths(
        table,
        columns,
        where_clause,
        &needed,
        &resolver,
        None,
        stats,
        hints,
        false,
        false,
        demand.statement_forces_an_index(),
        None,
    );
    if let Some(wanted) = wanted {
        // `matchProperty` as a FILTER over the enumeration.
        //
        // Go keeps every path in the skyline and refuses the non-matching ones
        // one layer later -- `convertToIndexScan`/`convertToTableScan` both
        // open with `if !prop.IsSortItemEmpty() && !candidate.matchPropResult
        // .Matched() { return invalidTask }` -- and stops a non-matching path
        // DOMINATING a matching one with the `matchResult` dimension,
        // `compareBool(lhs.matchPropResult.Matched(), rhs...)`. Removing them
        // up front reaches the same answer with one fewer moving part: a
        // matching path is still never pruned by a non-matching one, pruning
        // BETWEEN two matching paths is unchanged (their `matchResult` is 0),
        // and a non-matching path could only ever have produced `invalidTask`.
        // It is also what keeps [`crate::skyline`]'s `match_result = 0` EXACT
        // rather than an approximation -- see that module's own doc.
        //
        // There is no `Sort` enforcer below a `FROM` in this tier, so an
        // enumeration that empties here declines the order rather than paying
        // for one.
        paths.retain(|candidate| match &candidate.path.index {
            Some((index_id, _)) => table
                .indexes()
                .iter()
                .find(|index| index.id == *index_id)
                .is_some_and(|index| leaf_index_order(table, index, columns).starts_with(wanted)),
            // `matchProperty`'s int-handle branch: `if len(prop.SortItems) !=
            // 1 || pkCol == nil { return PropNotMatched }` and then the column
            // itself. The table path stays a COSTED candidate when it matches,
            // which is how an ordered index that is dearer than the ordered
            // table read still loses -- `best.index?` below then reads the
            // table path as "keep the whole-table scan already installed".
            None => leaf_handle_order(table, columns).starts_with(wanted),
        });
    }
    let best = crate::access_cost::choose_access_path(paths, stats, false)?;
    let Some((index_id, ranges)) = best.index else {
        let residual_filters = where_clause.map_or_else(Vec::new, |predicate| {
            crate::handle_range::build_handle_ranges(table, predicate, &ctx.session_zone())
                .map(|built| built.residual.into_iter().cloned().collect())
                .unwrap_or_else(|| {
                    let mut all = Vec::new();
                    crate::plan_trace::collect_and(predicate, &mut all);
                    all.into_iter().cloned().collect()
                })
        });
        return Some(LeafAccessPath::Table {
            ranges: best.table_ranges,
            estimate: best.estimate,
            residual_filters,
            // Keep the scan -> reader boundary intact. A parent aggregation
            // inserts its cop stage below that reader before comparing Go's
            // HashAgg and StreamAgg alternatives.
            candidate: best.planner_candidate,
        });
    };
    // The order the BUILT source will deliver, which is the index walk's own
    // only when this path was chosen to satisfy a property: without that, the
    // source reorders its lookup batches by handle and answers in neither
    // order. An empty answer here is a leaf that promises nothing.
    let order = match wanted {
        Some(_) => table
            .indexes()
            .iter()
            .find(|index| index.id == index_id)
            .map(|index| leaf_index_order(table, index, columns))
            .unwrap_or_default(),
        None => Vec::new(),
    };
    let index_filter =
        crate::access_cost::index_filter_for_path(table, index_id, where_clause, &resolver);
    let residual_filters = crate::access_cost::index_residual_filters_for_path(
        table,
        index_id,
        where_clause,
        &resolver,
    );
    let num_ranges = ranges.len();
    Some(LeafAccessPath::Index(LeafIndexPath {
        index_id,
        ranges,
        estimate: best.estimate,
        order,
        keep_order: wanted.is_some(),
        index_filter,
        residual_filters,
        candidate: tidb_planner::candidate_cost::Candidate::Fixed {
            rows: best.estimate.rows,
            row_size,
            cost: best.cost,
            num_ranges,
        },
    }))
}

/// The narrowed path a join leaf committed to, or the full-table path when no
/// candidate beat it. A table range keeps the already-built `TableScanExec`;
/// an index range replaces it with the streaming index source.
pub(crate) enum LeafAccessPath {
    /// A direct point lookup over a complete handle or unique key.
    Point {
        /// The handle, or `None` for a guaranteed miss.
        handle: Option<TableHandle>,
        /// The requested order, which a one-row source satisfies.
        order: Vec<usize>,
        /// The complete point-read task costed by Go's network formula.
        candidate: tidb_planner::candidate_cost::Candidate,
    },
    /// A clustered-handle range over the existing table scan.
    Table {
        /// The ranges to offer to the table source. `None` is the full range.
        ranges: Option<Vec<IndexRange>>,
        /// The estimate printed for the narrowed scan.
        estimate: crate::access_cost::ScanEstimate,
        /// Predicates that did not become handle access conditions.
        residual_filters: Vec<tidb_ast::Expr>,
        /// The complete reader task, already costed by `access_cost`.
        candidate: tidb_planner::candidate_cost::Candidate,
    },
    /// A secondary-index range source.
    Index(LeafIndexPath),
}

/// The order a WHOLE-TABLE walk of `table` delivers, in the leaf's own row
/// offsets -- the clustered integer handle, and nothing for a table without
/// one.
///
/// The same answer [`crate::merge_join_plan::table_scan_order`] gives, read
/// through the leaf's column list so it can be compared with
/// [`leaf_index_order`] on one numbering.
fn leaf_handle_order(table: &KvTable, columns: &[(String, FieldType)]) -> Vec<usize> {
    crate::merge_join_plan::table_scan_order(table)
        .into_iter()
        .next()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|offset| {
            let column = table.columns.get(offset)?;
            columns
                .iter()
                .position(|(name, _)| name.eq_ignore_ascii_case(&column.name))
        })
        .collect()
}

/// The order an index walk of `index` delivers, as offsets into the LEAF's
/// row (the layout `columns` describes), or the empty order when a key part
/// names a column that row does not carry.
///
/// [`crate::kv_table::KvIndex::ordered_column_offsets`] is the cut at the
/// first PREFIX key part, which is Go's `idxColLens[colIdx] ==
/// types.UnspecifiedLength` test made unrepresentable. The name lookup is the
/// same one [`crate::driver::merge_decision`] does: an expression index's
/// hidden column is a column of the TABLE that no query row carries, so it
/// truncates the order rather than pointing at the wrong offset.
fn leaf_index_order(
    table: &KvTable,
    index: &crate::kv_table::KvIndex,
    columns: &[(String, FieldType)],
) -> Vec<usize> {
    let mut order = Vec::with_capacity(index.ordered_column_offsets().len());
    for offset in index.ordered_column_offsets() {
        let Some(column) = table.columns.get(*offset) else {
            break;
        };
        let Some(at) = columns
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(&column.name))
        else {
            break;
        };
        order.push(at);
    }
    order
}

/// The whole-index path a join leaf committed to: what
/// [`leaf_index_path`] decided and [`leaf_index_source`] then builds.
pub(crate) struct LeafIndexPath {
    index_id: i64,
    ranges: Vec<IndexRange>,
    estimate: crate::access_cost::ScanEstimate,
    /// The order this walk delivers, in the leaf's own row offsets. Read back
    /// by [`crate::driver::from::build_from`] as the leaf's DELIVERY report;
    /// see [`leaf_index_order`].
    order: Vec<usize>,
    /// Go's `PhysicalIndexScan.KeepOrder`: whether this path was chosen to
    /// SATISFY a property, which is what makes the source answer in index
    /// order rather than reordering its handle batches
    /// ([`IndexRangeSourceExec::answer_in_index_order`]).
    keep_order: bool,
    /// Residual conditions evaluated from columns stored in this index.
    index_filter: Option<tidb_ast::Expr>,
    /// Every residual condition, including conditions that require the table
    /// row after a non-covering index lookup.
    residual_filters: Vec<tidb_ast::Expr>,
    /// The complete reader task, already costed by `access_cost`.
    candidate: tidb_planner::candidate_cost::Candidate,
}

impl LeafIndexPath {
    /// The order the walk this path describes delivers.
    pub(crate) fn order(&self) -> &[usize] {
        &self.order
    }

    /// Go's cop-side `Selection` above this index scan, when present.
    pub(crate) fn index_filter(&self) -> Option<&tidb_ast::Expr> {
        self.index_filter.as_ref()
    }

    /// Conditions that remain after the index's access ranges are built.
    pub(crate) fn residual_filters(&self) -> &[tidb_ast::Expr] {
        &self.residual_filters
    }

    pub(crate) fn candidate(&self) -> &tidb_planner::candidate_cost::Candidate {
        &self.candidate
    }
}

/// The streaming source and the `EXPLAIN` node for an index path a leaf
/// committed to, replacing the whole-table scan
/// [`crate::driver::from::build_from`] installed for it.
///
/// This is [`commit_index_range_source`] for the leaf position. It records
/// no `IndexAccessOrder`: that answer belongs to the single-table pipeline,
/// where a `LIMIT` under a matching `ORDER BY` reads it, and a join leaf has
/// no such caller.
pub(crate) fn leaf_index_source(
    table: &KvTable,
    visible: &str,
    columns: &[(String, FieldType)],
    path: LeafIndexPath,
    trace: Option<&mut PlanTrace>,
    ctx: &crate::StmtContext,
) -> Box<dyn Executor> {
    let LeafIndexPath {
        index_id,
        ranges,
        estimate,
        order: _,
        keep_order,
        index_filter: _,
        residual_filters: _,
        candidate: _,
    } = path;
    let mut trace = trace;
    if let Some(trace) = trace.as_deref_mut() {
        let index = table
            .indexes()
            .iter()
            .find(|index| index.id == index_id)
            .expect("the chosen path names an index of this table");
        let index_columns: Vec<String> = index
            .column_offsets
            .iter()
            .map(|offset| index_key_part_name(table, *offset))
            .collect();
        let index_columns: Vec<&str> = index_columns.iter().map(String::as_str).collect();
        if ranges.is_empty() {
            trace.empty_range_table_dual();
        } else if ranges.len() == 1 && ranges[0].is_full() {
            trace.index_full_scan(visible, &index.name, &index_columns, estimate, keep_order);
        } else {
            trace.index_range_scan(visible, &index.name, &index_columns, &ranges, estimate);
            if keep_order {
                trace.keep_order(false);
            }
        }
    }
    let mut exec = IndexRangeSourceExec::new_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        index_id,
        ranges,
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    // The schema above may already be NARROWER than the table (the leaf
    // demand prunes before the access path replaces the source), so the
    // reader is told which stored column each slot is rather than assuming
    // the first n.
    if let Some(offsets) = crate::access_path::stored_column_offsets(table, columns) {
        exec.read_table_columns(offsets);
    }
    crate::table_access::TableAccess::accept_scan_estimate(&mut exec, estimate.rows);
    if keep_order {
        // Go's `keep order:true` index read: `canReorderHandles` is false, so
        // the lookup batches are sorted BACK into index order and the rows
        // leave in the order the walk produced them. Without this the source
        // answers in handle order, which is the exact promise-without-delivery
        // a parent merge join must never be given.
        //
        // A DIRTY table is NOT gated here, though Go wraps a join leaf's
        // reader in `UnionScanExec` exactly as it wraps a single-table read's
        // (`buildUnionScanFromReader` is reached per `DataSource`). MEASURED:
        // adding `|| table.has_dirty_content()` changes no row order this tier
        // can produce -- a leaf that reaches this builder is already answering
        // in index order for both the clean and the dirty read of the same
        // join, with either hash side forced -- and it would leave
        // `LeafIndexPath::order` reporting an order the source now delivers
        // but does not promise. The single-table gate in
        // `super::access::commit_index_range_source` is the one that moves
        // rows.
        exec.answer_in_index_order();
    }
    if let Some(trace) = trace {
        trace.set_scan_act_rows(exec.produced_rows());
    }
    Box::new(exec)
}
