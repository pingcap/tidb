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

//! How a single base table is read: which access path is committed, and what
//! work the driver then hands down to it.
//!
//! This is the whole decision, in one file, in the order it happens:
//!
//! 1. [`commit_fast_path_source`] -- Go's `TryFastPlan`. A single-table
//!    `SELECT` whose `WHERE` pins the handle or a whole unique index reads
//!    those rows directly ([`try_batch_point_get`], [`try_point_get`]);
//!    otherwise the cheapest access path [`crate::access_cost`] enumerates
//!    supplies ranges ([`choose_index_range_path`]), which may be the full
//!    scan itself. Each fast path installs a *streaming*
//!    source over the narrowed path. A batch point get consumes its exact
//!    key-only `IN`, as Go's fast plan does; range and single-point paths keep
//!    their residual `WHERE` above the source.
//! 2. [`prune_scan_columns`] -- the kept-column offer.
//! 3. [`negotiate_scan_filter`] -- the pushed-conjunct offer, and the residual
//!    `WHERE` left above.
//! 4. [`offer_scan_limit`] -- the row-cap offer, gated by [`scan_limit_cap`].
//!
//! Steps 2-4 are offers, not commands: [`crate::table_access`] holds the
//! contract, every method of it is fail-closed, and the source alone decides.
//! The order matters and is fixed here -- pruning runs before the predicate
//! split so a pushed conjunct's `column_offset` is already in narrow space,
//! and the cap is offered last, after the residual `WHERE` is known, because
//! a residual filter above the source forbids one.
//!
//! # Why this is its own file
//!
//! The path choice is Go's cost-based one ([`crate::access_cost`] holds the
//! enumeration, the estimates and the cost formula); this file is where that
//! choice meets the executor it commits to and the negotiation that follows
//! it. Keeping the two together is what keeps a costed path and a runnable
//! path from drifting apart.

use super::point_get_key::point_get_value;
use super::*;

/// Commits the narrowed access path a single-table `SELECT` qualifies for,
/// replacing `from_source`, and reports the row order the committed path
/// produces (`None` when it establishes none).
///
/// Go's `TryFastPlan` runs before the ordinary plan and this mirrors its
/// order: the batch point get is tried first, then an index range when no
/// point get applies, and finally the single point get -- which supersedes an
/// index range already committed, and its ordering claim with it.
///
/// The `WHERE` stays in the pipeline above, so an unsatisfied extra condition
/// still filters the row out. Each fast path installs a streaming source over
/// the narrowed path (see [`crate::access_path`]), not a `Vec` of rows it
/// already read, so an index range over a huge table costs one chunk of
/// memory and a pushed `LIMIT` never reads past its cap.
pub(crate) fn commit_fast_path_source(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
    from_source: &mut Option<Box<dyn Executor>>,
    mut trace: Option<&mut PlanTrace>,
    ctx: &crate::StmtContext,
) -> Result<(Option<IndexAccessOrder>, bool), DriverError> {
    // Go's `PlanBuilder` reads the zone off the same `sessionctx` every other
    // decision here reads; taking it from `ctx` keeps the two from being
    // separately supplied and separately wrong.
    let zone = &ctx.session_zone();
    let disable_point_get = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
    let mut index_order: Option<IndexAccessOrder> = None;
    let mut consumed_where = false;
    let Some(table_ref) = sole_table_ref(&select.from) else {
        return Ok((None, false));
    };
    let Some(table) = sole_kv_table(&select.from, catalog, current_db) else {
        return Ok((None, false));
    };
    let table_name = table_ref
        .name
        .last()
        .map(String::as_str)
        .unwrap_or(table.name.as_str());
    let table = super::from::restricted_to_partitions(&table, &table_ref.partitions, table_name)?;
    let columns = scope.column_list();
    // Go's `getPossibleAccessPaths`: the statement's own `USE`/`FORCE`/
    // `IGNORE INDEX` decide which paths exist before any of them is costed.
    // The names were already validated for every table of the `FROM`
    // (`index_hints::validate_join_index_hints`), so this cannot be the site
    // that raises 1176.
    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        &table,
        current_db,
        ctx,
    )?;
    // Go's `PredicateSimplification` plans a `TableDual rows:0` before any path
    // is costed when the `WHERE` is provably contradictory on some column
    // (`b = 1 AND b = 2`), which is index-independent: it reads no row whether
    // or not `b` is indexed, and holds for a partition key over a partitioned
    // table just the same. Committing the dual here supersedes every access
    // path below, exactly as Go's whole-`DataSource`-to-dual replacement does.
    if let Some(where_clause) = select.where_clause.as_ref() {
        if crate::index_range::where_is_unsatisfiable(&columns, where_clause, zone) {
            install_contradiction_dual(&columns, from_source, trace.as_deref_mut());
            return Ok((None, false));
        }
    }
    // Go's `PartitionProcessor` prunes before any access path is costed, and
    // so does this: an offer refused leaves the source reading every
    // partition, which is a superset and still every row the statement
    // admits.
    // Go `getTableScanPenalty`'s `hasPartitionScan` reads
    // `PlanPartInfo.PruningConds`, which the `PartitionProcessor` leaves
    // behind whenever it had conditions to prune WITH -- exactly when
    // `pruned_partition_ids` answers.
    let partition_scan = pruned_partition_ids(select, &table, zone).is_some();
    if let Some(ids) = pruned_partition_ids(select, &table, zone) {
        if let Some(access) = from_source
            .as_mut()
            .and_then(|source| source.table_access())
        {
            access.accept_partition_pruning(&ids);
        }
    }
    // Go tries the batch point get before the single one.
    let fast_batch_partition_supported = table.partition().is_none_or(|partition| {
        partition.dependencies.len() == 1
            && matches!(
                partition.kind,
                crate::PartitionKind::Hash | crate::PartitionKind::Key
            )
    });
    if let Some(decision) = (!disable_point_get && fast_batch_partition_supported)
        .then(|| try_batch_point_get(select, &table, &columns, zone, &hints, ctx))
        .transpose()?
        .flatten()
        .filter(|decision| {
            table.partition().is_none()
                || ctx.static_partition_prune()
                || decision.handles.len() <= 1
        })
    {
        let BatchPointGetDecision {
            handles,
            index,
            routed_partitions,
        } = decision;
        // This fast plan only accepts a WHERE that is exactly the IN whose
        // keys it just converted. Go replaces the whole pipeline with the
        // BatchPointGetPlan, so retaining the row expression above this
        // source would duplicate a predicate already proved by the key.
        consumed_where = true;
        let exec = HandleSourceExec::new_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handles.clone(),
            crate::kv_table::RowDecodeContext::for_query(ctx),
        );
        if let Some(trace) = trace.as_deref_mut() {
            let index = index.as_ref().map(BatchPointGetIndex::access_object);
            let partitions = routed_partitions
                .unwrap_or_else(|| table.handle_partition_names(&handles, zone, ctx));
            trace.batch_point_get(
                source_table_name(scope, &table.name),
                &handles,
                &partitions,
                index.as_deref(),
                ctx.static_partition_prune(),
            );
            // The rows are read lazily, so the count is the source's live one
            // rather than a `Vec`'s length.
            trace.set_scan_act_rows(exec.produced_rows());
        }
        *from_source = Some(Box::new(exec));
    } else if let Some(decision) = (!disable_point_get
        && hints.allows_index(crate::index_hints::COMMON_PRIMARY_INDEX_ID))
    .then(|| {
        super::common_handle_access::point_ranges(
            select,
            &table,
            &columns,
            zone,
            ctx.static_partition_prune(),
        )
    })
    .flatten()
    {
        let exec = HandleSourceExec::new_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            decision.handles.clone(),
            crate::kv_table::RowDecodeContext::for_query(ctx),
        );
        if let Some(trace) = trace.as_deref_mut() {
            let partitions = table.handle_partition_names(&decision.handles, zone, ctx);
            let index = format!("clustered index:PRIMARY({})", decision.columns.join(", "));
            if decision.handles.len() == 1 {
                trace.index_point_get(source_table_name(scope, &table.name), &partitions, &index);
            } else {
                trace.index_batch_point_get(
                    source_table_name(scope, &table.name),
                    decision.handles.len(),
                    &partitions,
                    &index,
                    ctx.static_partition_prune(),
                );
            }
            trace.set_scan_act_rows(exec.produced_rows());
        }
        *from_source = Some(Box::new(exec));
    } else
    // An index range scan, when no point get applies: the ranges replace the
    // full scan with the rows the index covers, and the WHERE stays above to
    // apply the conditions the ranges did not consume.
    //
    // A point get over the handle IS the table path taken to its limit, so a
    // hint that deleted the table path deletes it too -- Go gates it on the
    // same `indexIsAvailableByHints` (`point_get_plan.go:571`), which is why
    // `FORCE INDEX(idx_b) WHERE a = 2` reads idx_b instead of the row. The
    // The integer-handle BATCH point path is deliberately not gated: Go's
    // `newBatchPointGetPlan` returns before consulting index hints in that
    // arm. Secondary and clustered-common indexes were already checked
    // against `AvailablePaths` while constructing their batch decision.
    if disable_point_get
        || !hints.allows_table()
        || try_point_get(&PointPlanStmt::of_select(select), &table, &columns, zone)?.is_none()
    {
        if !crate::index_hints::no_index_merge(select) {
            if let Some(plan) = choose_index_merge_union(
                select,
                catalog,
                scope,
                &table,
                &columns,
                partition_scan,
                current_db,
            ) {
                commit_index_merge_source(
                    &table,
                    scope,
                    &columns,
                    plan,
                    from_source,
                    trace.as_deref_mut(),
                    ctx,
                );
                return Ok((None, false));
            }
            if let Some(plan) = choose_index_merge_intersection(
                select,
                catalog,
                scope,
                &table,
                &columns,
                partition_scan,
                current_db,
            ) {
                commit_index_merge_source(
                    &table,
                    scope,
                    &columns,
                    plan,
                    from_source,
                    trace.as_deref_mut(),
                    ctx,
                );
                return Ok((None, false));
            }
            if ctx.index_merge() {
                let automatic = AutomaticIndexMergeContext {
                    catalog,
                    scope,
                    table: &table,
                    columns: &columns,
                    partition_scan,
                    hints: &hints,
                    current_db,
                };
                if let Some(plan) = choose_automatic_index_merge_union(select, &automatic) {
                    commit_index_merge_source(
                        &table,
                        scope,
                        &columns,
                        plan,
                        from_source,
                        trace.as_deref_mut(),
                        ctx,
                    );
                    return Ok((None, false));
                }
            }
        }
        match choose_index_range_path(
            select,
            catalog,
            scope,
            &table,
            &columns,
            &hints,
            partition_scan,
        ) {
            // A table path the ranger narrowed. The source already installed
            // by `build_from` IS the right executor -- a `TableRangeScan` is
            // Go's same `PhysicalTableScan` with ranges -- so this offers it
            // the ranges rather than replacing it, and only renames the
            // traced node once the source has taken them. A source that
            // refuses keeps reading the whole table, which is still every row
            // the statement admits.
            Some(ChosenPath::HandleRange(ranges, estimate)) => {
                let accepted = from_source
                    .as_mut()
                    .and_then(|source| source.table_access())
                    .is_some_and(|access| access.accept_handle_ranges(&ranges));
                if accepted {
                    if let Some(trace) = trace.as_deref_mut() {
                        // Go's `findBestTask` returns a `PhysicalTableDual`
                        // the moment a chosen path has NO ranges
                        // (`find_best_task.go`: `if len(path.Ranges) == 0`),
                        // the same short-circuit the index arm below takes.
                        // On the TABLE path this is what `id IS NULL` over an
                        // integer handle reaches: `points2TableRanges` drops
                        // the NULL-ended interval, leaving nothing to read.
                        if ranges.is_empty() {
                            trace.empty_range_table_dual();
                        } else if let Some(handle) = (!disable_point_get)
                            .then(|| single_point_handle(&ranges))
                            .flatten()
                        {
                            // Go's `isPointGetPath` converts a table path whose
                            // one range is a single non-null point on the
                            // integer handle to a `Point_Get`
                            // (`find_best_task.go`: `convertToPointGet`), even
                            // when an extra conjunct stays a filter above --
                            // `c1 = 1 AND c2 > 1` reads `Point_Get`, not a
                            // `TableRangeScan` over `[1,1]`.
                            trace.point_get(source_table_name(scope, &table.name), Some(&handle));
                        } else {
                            trace.table_range_scan(
                                source_table_name(scope, &table.name),
                                &ranges,
                                estimate,
                            );
                        }
                    }
                }
            }
            Some(ChosenPath::Index(index_id, ranges, estimate, covering)) => {
                commit_index_range_source(
                    &table,
                    scope,
                    &columns,
                    index_id,
                    ranges,
                    estimate,
                    covering,
                    from_source,
                    trace.as_deref_mut(),
                    &mut index_order,
                    ctx,
                );
            }
            None => {}
        }
    }
    if let Some(handle) = (!disable_point_get && hints.allows_table())
        .then(|| try_point_get(&PointPlanStmt::of_select(select), &table, &columns, zone))
        .transpose()?
        .flatten()
    {
        // A `None` handle is a WHERE that pins a handle no row can have: the
        // plan is a point get over an empty handle list.
        let exec = HandleSourceExec::new_with_context(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handle.clone().into_iter().collect(),
            crate::kv_table::RowDecodeContext::for_query(ctx),
        );
        if let Some(trace) = trace {
            trace.point_get(source_table_name(scope, &table.name), handle.as_ref());
            trace.set_scan_act_rows(exec.produced_rows());
        }
        // The index-range path above may have already committed a source; a
        // point get supersedes it, and so does its ordering claim.
        index_order = None;
        *from_source = Some(Box::new(exec));
    }
    Ok((index_order, consumed_where))
}

struct IndexMergePlan {
    kind: IndexMergeKind,
    partials: Vec<(i64, Vec<IndexRange>)>,
}

fn choose_index_merge_union(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    partition_scan: bool,
    current_db: &str,
) -> Option<IndexMergePlan> {
    if table.partition().is_some() {
        return None;
    }
    let index_ids = crate::index_hints::single_table_index_merge_indexes(
        select,
        single_table_ref(&select.from),
        table,
        current_db,
    );
    if index_ids.len() < 2 {
        return None;
    }
    let where_clause = select.where_clause.as_ref()?;
    let mut branches = Vec::new();
    collect_index_merge_disjuncts(where_clause, &mut branches);
    if branches.len() < 2 {
        return None;
    }
    let demand = crate::driver::leaf_demand::LeafDemand::of_select(select);
    let needed = demand.needed(&scope.tables[0].name, columns);
    let resolver = ScopeResolver { scope };
    let stats = catalog.table_statistics(table.table_id);
    let stats = stats.as_ref().map(AsRef::as_ref);
    let hints = crate::index_hints::AvailablePaths::index_merge_only(index_ids);
    let mut partials = Vec::with_capacity(branches.len());
    for branch in branches {
        let candidates = crate::access_cost::enumerate_paths(
            table,
            columns,
            Some(branch),
            &needed,
            &resolver,
            None,
            stats,
            &hints,
            false,
            partition_scan,
            true,
        )
        .into_iter()
        .filter(|candidate| !candidate.access_columns.is_empty())
        .collect();
        let path = crate::access_cost::choose_access_path(candidates, stats, false)?;
        let (index_id, ranges) = path.index?;
        partials.push((index_id, ranges));
    }
    Some(IndexMergePlan {
        kind: IndexMergeKind::Union,
        partials,
    })
}

fn choose_index_merge_intersection(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    partition_scan: bool,
    current_db: &str,
) -> Option<IndexMergePlan> {
    if table.partition().is_some() {
        return None;
    }
    let index_ids = crate::index_hints::single_table_index_merge_indexes(
        select,
        single_table_ref(&select.from),
        table,
        current_db,
    );
    if index_ids.len() < 2 {
        return None;
    }
    let where_clause = select.where_clause.as_ref()?;
    let demand = crate::driver::leaf_demand::LeafDemand::of_select(select);
    let needed = demand.needed(&scope.tables[0].name, columns);
    let resolver = ScopeResolver { scope };
    let stats = catalog.table_statistics(table.table_id);
    let stats = stats.as_ref().map(AsRef::as_ref);
    let mut partials = Vec::new();
    for index_id in index_ids {
        let hints = crate::index_hints::AvailablePaths::index_merge_only(vec![index_id]);
        let candidates = crate::access_cost::enumerate_paths(
            table,
            columns,
            Some(where_clause),
            &needed,
            &resolver,
            None,
            stats,
            &hints,
            false,
            partition_scan,
            true,
        )
        .into_iter()
        .filter(|candidate| !candidate.access_columns.is_empty())
        .collect();
        let Some(path) = crate::access_cost::choose_access_path(candidates, stats, false) else {
            continue;
        };
        let Some((index_id, ranges)) = path.index else {
            continue;
        };
        partials.push((index_id, ranges));
    }
    (partials.len() >= 2).then_some(IndexMergePlan {
        kind: IndexMergeKind::Intersection,
        partials,
    })
}

/// The non-MV OR shape `generateORIndexMerge` can lower without a hint: a
/// top-level AND-list containing one DNF whose every arm has a real
/// secondary-index range. Go builds every arm together with the other CNF
/// predicates from its regular access paths, then lets the physical
/// IndexMerge reader compete with the normal table/index reader.
///
/// This intentionally stops before Go's nested-DNF, MV-index, and
/// parameter-plan-cache variants. Those require the unfinished-path builder
/// rather than a second range algebra. The ordinary DNF path below is one
/// complete source shape: its merged cardinality is the complete predicate's
/// selectivity, and every partial has that arm plus the shared CNF access
/// conditions.
struct AutomaticIndexMergeContext<'a> {
    catalog: &'a Catalog,
    scope: &'a FromScope,
    table: &'a KvTable,
    columns: &'a [(String, FieldType)],
    partition_scan: bool,
    hints: &'a crate::index_hints::AvailablePaths,
    current_db: &'a str,
}

fn choose_automatic_index_merge_union(
    select: &tidb_ast::SelectStmt,
    input: &AutomaticIndexMergeContext<'_>,
) -> Option<IndexMergePlan> {
    // A common handle is not carried by every secondary index in this
    // executor's range cursor, so it cannot yet be the handle-only partial
    // reader Go's IndexMerge lowers to.
    let handle = input.table.pk_handle_offset()?;
    if input.table.partition().is_some() {
        return None;
    }
    // An explicit USE_INDEX_MERGE gets the hint path above. This automatic
    // path must not turn an inapplicable hint into a different plan.
    if crate::index_hints::has_single_table_index_merge_hint(
        select,
        single_table_ref(&select.from),
        input.current_db,
    ) {
        return None;
    }
    let where_clause = select.where_clause.as_ref()?;
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let dnf = conjuncts
        .iter()
        .enumerate()
        .find_map(|(index, candidate)| {
            let mut branches = Vec::new();
            collect_index_merge_disjuncts(candidate, &mut branches);
            (branches.len() >= 2).then_some((index, branches))
        })?;
    // Go's generateORIndexMerge considers every DNF member of the top-level
    // CNF list. This first automatic tranche accepts exactly one: selecting
    // between several independently valid merge paths requires carrying all
    // candidates into the common skyline pruning rather than returning the
    // first one.
    if conjuncts.iter().enumerate().any(|(index, candidate)| {
        index != dnf.0 && {
            let mut branches = Vec::new();
            collect_index_merge_disjuncts(candidate, &mut branches);
            branches.len() >= 2
        }
    }) {
        return None;
    }
    let common = conjuncts
        .iter()
        .enumerate()
        .filter_map(|(index, condition)| (index != dnf.0).then_some(*condition))
        .collect::<Vec<_>>();
    let resolver = ScopeResolver { scope: input.scope };
    let stats = input.catalog.table_statistics(input.table.table_id);
    let stats = stats.as_ref().map(AsRef::as_ref);
    let mut paths = Vec::with_capacity(dnf.1.len());
    for branch in dnf.1 {
        let branch = combine_index_merge_conjuncts(&common, branch);
        let candidates = crate::access_cost::enumerate_paths(
            input.table,
            input.columns,
            Some(&branch),
            &[handle],
            &resolver,
            None,
            stats,
            input.hints,
            false,
            input.partition_scan,
            input.hints.has_forced_path(),
        )
        .into_iter()
        .filter(|candidate| {
            candidate.path.index.is_some()
                && !candidate.access_columns.is_empty()
                && candidate.path.index.as_ref().is_some_and(|(index_id, _)| {
                    crate::access_cost::index_is_covering(input.table, *index_id, &[handle])
                })
        })
        .collect();
        paths.push(crate::access_cost::choose_access_path(
            candidates, stats, false,
        )?);
    }
    let index_ids = paths
        .iter()
        .filter_map(|path| path.index.as_ref().map(|(index_id, _)| *index_id))
        .collect::<std::collections::BTreeSet<_>>();
    if index_ids.len() < 2 {
        return None;
    }
    let (regular, needed) = best_single_table_access_path(
        select,
        input.catalog,
        input.scope,
        input.table,
        input.columns,
        input.hints,
        input.partition_scan,
    )?;
    let rows = crate::access_cost::realtime_row_count(stats)
        * crate::access_cost::selectivity(where_clause, input.table, &resolver, stats);
    if crate::access_cost::index_merge_cost(input.table, &needed, stats, rows, &paths)
        >= regular.cost
    {
        return None;
    }
    let partials = paths.into_iter().filter_map(|path| path.index).collect();
    Some(IndexMergePlan {
        kind: IndexMergeKind::Union,
        partials,
    })
}

fn collect_index_merge_disjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_index_merge_disjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, left, right) => {
            collect_index_merge_disjuncts(left, out);
            collect_index_merge_disjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// One DNF arm plus its sibling CNF conditions, preserving the ordinary
/// source predicate shape that Go gives accessPathsForConds. The final
/// selection still evaluates the original expression, so an unsupported
/// shared condition cannot widen the SQL result; it only remains above the
/// reader rather than becoming a range.
fn combine_index_merge_conjuncts(
    common: &[&tidb_ast::Expr],
    branch: &tidb_ast::Expr,
) -> tidb_ast::Expr {
    common.iter().rev().fold(branch.clone(), |right, left| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new((*left).clone()),
            Box::new(right),
        )
    })
}

fn commit_index_merge_source(
    table: &KvTable,
    scope: &FromScope,
    columns: &[(String, FieldType)],
    plan: IndexMergePlan,
    from_source: &mut Option<Box<dyn Executor>>,
    trace: Option<&mut PlanTrace>,
    ctx: &crate::StmtContext,
) {
    let exec = IndexMergeSourceExec::new_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        plan.kind,
        plan.partials.clone(),
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    if let Some(trace) = trace {
        let indexes = plan
            .partials
            .iter()
            .filter_map(|(id, _)| table.indexes().iter().find(|index| index.id == *id))
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        trace.index_merge(
            source_table_name(scope, &table.name),
            &indexes,
            matches!(plan.kind, IndexMergeKind::Intersection),
        );
        trace.set_scan_act_rows(exec.produced_rows());
    }
    *from_source = Some(Box::new(exec));
}

/// The partitions a single-table `SELECT` still reads, named as declared and
/// in definition order -- Go's `PartitionProcessor` output, which is the list
/// `EXPLAIN` fans a static-mode plan out over.
///
/// Two narrowings compose, in either order and both cumulative, exactly as
/// [`crate::KvTable::restrict_read_to_partitions`] composes them for the read
/// itself: the statement's own `PARTITION (p, ...)` list, and whatever the
/// `WHERE` pruned. Empty for an unpartitioned table.
///
/// An unresolvable `PARTITION (p)` name answers the FULL list rather than
/// failing here: the read has already raised 1735 for it, and this is only
/// ever asked for a plan that got built.
pub(crate) fn surviving_partition_names(
    select: &tidb_ast::SelectStmt,
    table_ref: Option<&tidb_ast::TableRef>,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<String> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let selected = table_ref
        .map(|table_ref| table_ref.partitions.as_slice())
        .filter(|names| !names.is_empty())
        .and_then(|names| {
            crate::partition_pruning::ids_for_selected_partitions(partition, names).ok()
        });
    let pruned = pruned_partition_ids(select, table, zone);
    partition
        .definitions
        .iter()
        .filter(|def| selected.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .filter(|def| pruned.as_ref().is_none_or(|ids| ids.contains(&def.id)))
        .map(|def| def.name.clone())
        .collect()
}

/// The static plan branches and the physical-table statistics each branch
/// owns. Go analyzes and plans partition IDs independently in static mode;
/// the logical table's optional global statistics are not a substitute.
pub(crate) fn partition_scan_estimates(
    catalog: &Catalog,
    table: &KvTable,
    names: &[String],
) -> Vec<(String, crate::access_cost::ScanEstimate)> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    names
        .iter()
        .filter_map(|name| {
            let definition = partition
                .definitions
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(name))?;
            Some((
                definition.name.clone(),
                scan_estimate(catalog.table_statistics(definition.id)),
            ))
        })
        .collect()
}

/// Installs a zero-row [`TableDualExec`] for a contradictory `WHERE` and
/// records the `TableDual rows:0` node in place of the scan `build_from` traced.
///
/// The `WHERE` stays in the pipeline above (as every fast path leaves it), so
/// the `Selection` over this source is fed no rows and produces none -- the
/// same answer the full scan gave, reached without reading the table.
fn install_contradiction_dual(
    columns: &[(String, FieldType)],
    from_source: &mut Option<Box<dyn Executor>>,
    trace: Option<&mut PlanTrace>,
) {
    let exec = crate::table_dual::TableDualExec::new(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        0,
    );
    if let Some(trace) = trace {
        trace.empty_range_table_dual();
    }
    *from_source = Some(Box::new(exec));
}

/// The clustered integer handle a single-point table range names, when the
/// range list is exactly one non-null point -- Go's `IsPointNonNullable` over
/// an `IsIntHandlePath` in `isPointGetPath`.
///
/// `None` for anything else: several ranges, an open bound, a NULL endpoint, or
/// a non-integer bound. A common (multi-column) handle never reaches here,
/// because this tier only builds handle ranges over the integer handle.
fn single_point_handle(ranges: &[IndexRange]) -> Option<TableHandle> {
    let [range] = ranges else {
        return None;
    };
    if range.low_exclusive || range.high_exclusive {
        return None;
    }
    match (range.low.as_slice(), range.high.as_slice()) {
        ([Datum::Int(low)], [Datum::Int(high)]) if low == high => Some(TableHandle::Int(*low)),
        ([Datum::UInt(low)], [Datum::UInt(high)]) if low == high => {
            Some(TableHandle::Int(*low as i64))
        }
        _ => None,
    }
}

/// Installs the streaming index-range source for a committed index path, and
/// records the node `EXPLAIN` prints for it.
#[allow(clippy::too_many_arguments)]
fn commit_index_range_source(
    table: &KvTable,
    scope: &FromScope,
    columns: &[(String, FieldType)],
    index_id: i64,
    ranges: Vec<IndexRange>,
    estimate: crate::access_cost::ScanEstimate,
    // Go's `path.IsSingleScan`; see [`ChosenPath::Index`].
    covering: bool,
    from_source: &mut Option<Box<dyn Executor>>,
    trace: Option<&mut PlanTrace>,
    index_order: &mut Option<IndexAccessOrder>,
    ctx: &crate::StmtContext,
) {
    let mut exec = IndexRangeSourceExec::new_with_context(
        ExecutorMeta::new(
            Schema::new(source_schema_columns(columns)),
            0,
            INIT_CAP,
            MAX_CHUNK_SIZE,
        ),
        table.clone(),
        index_id,
        ranges.clone(),
        crate::kv_table::RowDecodeContext::for_query(ctx),
    );
    // A covering path is Go's `PhysicalIndexReader`: the index answers on its
    // own, no handle batch is ever built, and the rows leave in INDEX order.
    // This tier reads the row either way (it has no index-only reader), so the
    // difference has to be declared here rather than shown by the executor's
    // shape.
    //
    // A DIRTY table reaches the same answer by the other door: Go's
    // `tableHasDirtyContent` (`pkg/planner/core/logical_plan_builder.go:5316`)
    // puts a `UnionScanExec` above the reader, and its `compare()` orders on
    // the index's own columns before the handle -- so a double read inside a
    // transaction that has written this table answers in index order too.
    if covering {
        exec.answer_in_index_order();
    }
    if table.has_dirty_content() {
        exec.merge_partition_index_order();
    }
    let index = table
        .indexes()
        .iter()
        .find(|index| index.id == index_id)
        .expect("the chosen path names an index of this table");
    *index_order = Some(IndexAccessOrder::from_ranges(
        index.ordered_column_offsets(),
        &ranges,
    ));
    if let Some(trace) = trace {
        let index_columns: Vec<String> = index
            .column_offsets
            .iter()
            .map(|offset| index_key_part_name(table, *offset))
            .collect();
        let index_columns: Vec<&str> = index_columns.iter().map(String::as_str).collect();
        let point_ranges = index.unique
            && !index.has_prefix()
            && !ranges.is_empty()
            && ranges.iter().all(|range| {
                range.low.len() == index.column_offsets.len() && range.is_point(false)
            });
        let point_partitions = if point_ranges {
            index_range_partition_names(table, index, &ranges, ctx)
        } else {
            Vec::new()
        };
        let clustered = index.name.eq_ignore_ascii_case("PRIMARY")
            && index.column_offsets == table.common_handle_offsets();
        // Go's `findBestTask` returns a `PhysicalTableDual` the moment a
        // chosen path has NO ranges (`find_best_task.go`: `if
        // len(path.Ranges) == 0`), so a contradictory `WHERE` prints no scan
        // at all. Reached only through the `USE INDEX` cases in
        // `tests/integrationtest/t/util/ranger.test`, where an UNSIGNED key
        // part meets a negative bound.
        if ranges.is_empty() {
            trace.empty_range_table_dual();
            trace.set_scan_act_rows(exec.produced_rows());
            *from_source = Some(Box::new(exec));
            return;
        }
        // A path the ranger narrowed nothing on reads the whole index, which
        // Go names `IndexFullScan` and prints without a `range:`.
        if point_ranges && ranges.len() == 1 {
            trace.index_point_get(
                source_table_name(scope, &table.name),
                &point_partitions,
                &format!(
                    "{}index:{}({})",
                    if clustered { "clustered " } else { "" },
                    index.name,
                    index_columns.join(", ")
                ),
            );
        } else if point_ranges && ctx.static_partition_prune() {
            trace.index_batch_point_get(
                source_table_name(scope, &table.name),
                ranges.len(),
                &point_partitions,
                &format!(
                    "{}index:{}({})",
                    if clustered { "clustered " } else { "" },
                    index.name,
                    index_columns.join(", ")
                ),
                true,
            );
        } else if ranges.len() == 1 && ranges[0].is_full() {
            trace.index_full_scan(
                source_table_name(scope, &table.name),
                &index.name,
                &index_columns,
                estimate,
                false,
            );
        } else {
            trace.index_range_scan(
                source_table_name(scope, &table.name),
                &index.name,
                &index_columns,
                &ranges,
                estimate,
            );
        }
        trace.set_scan_act_rows(exec.produced_rows());
    }
    *from_source = Some(Box::new(exec));
}

fn index_range_partition_names(
    table: &KvTable,
    index: &crate::kv_table::KvIndex,
    ranges: &[IndexRange],
    ctx: &impl tidb_expr::Columns,
) -> Vec<String> {
    let Some(partition) = table.partition() else {
        return Vec::new();
    };
    let mut ordinals = Vec::new();
    for range in ranges {
        let mut row = vec![Datum::Null; table.columns.len()];
        for (offset, value) in index.column_offsets.iter().zip(&range.low) {
            row[*offset] = value.clone();
        }
        if let Ok(ordinal) = partition.locate_ordinal(&row, &table.columns, ctx) {
            if !ordinals.contains(&ordinal) {
                ordinals.push(ordinal);
            }
        }
    }
    ordinals.sort_unstable();
    ordinals
        .into_iter()
        .filter_map(|ordinal| partition.definitions.get(ordinal))
        .map(|definition| definition.name.clone())
        .collect()
}

/// The schema a fast-path source emits: the scope's columns in scope order,
/// each carrying the unique id the driver's resolver hands expressions.
pub(crate) fn source_schema_columns(columns: &[(String, FieldType)]) -> Vec<Column> {
    columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect()
}

/// Offers the source only the columns the statement reads, narrowing `scope`
/// with it when the source takes the offer (Go's `rule_column_pruning.go`).
///
/// This runs BEFORE any expression is built, which is the whole point: every
/// offset below is resolved against the narrowed scope from the start, so no
/// already-built index has to be renumbered. It also runs before the predicate
/// push-down, so a pushed conjunct's `column_offset` is already in narrow
/// space -- and the kept set contains the `WHERE`'s columns because the gate
/// collected them.
///
/// No "was the source replaced?" flag is needed: `accept_column_prune`
/// defaults to refusing, so a fast-path source that cannot project simply says
/// no and the full-width path stands. Each source answers for itself,
/// fail-closed -- the same rule the pushed filter and row cap follow.
pub(crate) fn prune_scan_columns(
    select: &tidb_ast::SelectStmt,
    scope: &mut FromScope,
    from_source: &mut Option<Box<dyn Executor>>,
) {
    let Some(source) = from_source.as_mut() else {
        return;
    };
    let Some(keep) = crate::column_prune::prunable_columns(select, scope) else {
        return;
    };
    if keep.len() < scope.width()
        && source
            .table_access()
            .is_some_and(|access| access.accept_column_prune(&keep))
    {
        *scope = crate::column_prune::pruned_scope(scope, &keep);
    }
}

/// Offers the source the conjuncts it can apply itself, and reports both the
/// `WHERE` that must still run above it (`None`: the source took all of it)
/// and the built expressions the source accepted. The latter are the same
/// values execution and EXPLAIN consume; rebuilding them would repeat
/// build-time conversions and their warnings.
///
/// Over a single base table every source below is a real streaming scan, so
/// each answers for itself whether it can keep the promise
/// [`crate::table_access`] describes -- an index range can (it tests every row
/// it emits), a point get's handle source refuses. Only the residual then
/// needs a `Selection`; when the scan takes the whole `WHERE` there is no
/// `Selection` executor left, but the recorded plan is unchanged either way --
/// Go prints one `Selection` over the scan for both halves (captured,
/// `pkg/executor/zz_dump_pushdown_test.go`), and this tier prints no
/// `TableReader`/`cop[tikv]` task to distinguish them.
pub(crate) fn negotiate_scan_filter(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    source: &mut Box<dyn Executor>,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
) -> (Option<tidb_ast::Expr>, Vec<Expression>) {
    match (&select.where_clause, scope.tables.len()) {
        (Some(predicate), 1) => {
            let (pushed, residual) = split_scan_predicates(predicate, &scope_resolver(scope), ctx);
            let accepted = !pushed.is_empty()
                && source
                    .table_access()
                    .is_some_and(|access| access.accept_scan_filter(&pushed, ctx));
            if accepted {
                // `TableFullScan`'s `actRows` counts rows read, not rows kept,
                // so it is taken from the scan itself rather than from the
                // (now filtered) chunks leaving it.
                if let (Some(trace), Some(scanned)) = (
                    trace,
                    source
                        .table_access()
                        .and_then(|access| access.scanned_rows_counter()),
                ) {
                    trace.set_scan_act_rows(scanned);
                }
                (residual, pushed.filters().to_vec())
            } else {
                (Some(predicate.clone()), Vec::new())
            }
        }
        (where_clause, _) => (where_clause.clone(), Vec::new()),
    }
}

/// Offers the source the `LIMIT`'s row cap, when [`scan_limit_cap`] finds one
/// is sound.
///
/// This must run before any wrapper goes over the source, because the cap is a
/// promise only the source itself can keep.
pub(crate) fn offer_scan_limit(
    select: &tidb_ast::SelectStmt,
    residual_where: Option<&tidb_ast::Expr>,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
    source: &mut Box<dyn Executor>,
) {
    let Some(cap) = scan_limit_cap(select, residual_where, index_order, resolver) else {
        return;
    };
    if let Some(access) = source.table_access() {
        access.accept_scan_limit(cap);
    }
}

/// Tells the source whether the order it walks in is the order the statement
/// asked for -- Go's `keep order:true`, which for an `IndexLookUp` decides
/// whether the handle batch is answered in index order or in handle order
/// (see [`crate::table_access::TableAccess::accept_keep_order`]).
///
/// The condition is the SAME [`order_is_index_order`] the limit push-down
/// asks, because it is the same question: Go derives both from one required
/// physical property. It is asked here without the limit, since `keep order`
/// is a property of the read and not of any cap on it.
///
/// Like `offer_scan_limit` this must run before any wrapper goes over the
/// source, and unlike it, nothing above depends on the answer -- a source
/// that refuses is still correct.
pub(crate) fn offer_keep_order(
    select: &tidb_ast::SelectStmt,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
    source: &mut Box<dyn Executor>,
) {
    let Some(order) = index_order else {
        return;
    };
    if select.order_by.is_empty() || !order_is_index_order(select, order, resolver) {
        return;
    }
    if let Some(access) = source.table_access() {
        access.accept_keep_order();
    }
}
/// The index access path a `WHERE` should be read through, when an index
/// beats the full table scan.
///
/// Go's `DetachCondAndBuildRangeForIndex` splits a predicate into access
/// conditions, which become index ranges, and filter conditions, which stay
/// above the read; `findBestTask` then costs every path that split produced
/// and keeps the cheapest. This does the same through
/// [`crate::access_cost`], and returns `None` when the winner is the full
/// scan -- so a filter too broad to pay for an index simply leaves the scan
/// in place, which is the case a "first index that fits" rule always got
/// wrong.
///
/// The whole `WHERE` stays in the pipeline either way, so the filter half of
/// the split is applied by the selection rather than dropped.
/// The narrowed source [`choose_access_path`] committed to, when it narrowed
/// one at all.
///
/// Go's `findBestTask` returns ONE path over a data source and the reader it
/// lowers to follows from which; splitting the two here keeps the driver from
/// having to ask an `Option<index>` what kind of scan it is holding.
pub(crate) enum ChosenPath {
    /// An index path: the index's id, the ranges of it to read, its
    /// estimate, and Go's `path.IsSingleScan` -- whether the index alone
    /// answers the statement (`PhysicalIndexReader`) or a row lookup follows
    /// it (`PhysicalIndexLookUpReader`), which is what decides the row ORDER
    /// (see [`crate::access_path::IndexRangeSourceExec`]).
    Index(i64, Vec<IndexRange>, crate::access_cost::ScanEstimate, bool),
    /// A table path the ranger narrowed, over the clustered integer handle.
    /// An EMPTY range list is the contradictory `WHERE` that reads nothing.
    HandleRange(Vec<IndexRange>, crate::access_cost::ScanEstimate),
}

pub(crate) fn choose_index_range_path(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    hints: &crate::index_hints::AvailablePaths,
    // Go `getTableScanPenalty`'s `hasPartitionScan`, decided by the caller
    // because it is the caller that ran the pruning.
    partition_scan: bool,
) -> Option<ChosenPath> {
    let (best, needed) = best_single_table_access_path(
        select,
        catalog,
        scope,
        table,
        columns,
        hints,
        partition_scan,
    )?;
    let estimate = best.estimate;
    match (best.index, best.table_ranges) {
        (Some((index_id, ranges)), _) => {
            let covering = crate::access_cost::index_is_covering(table, index_id, &needed);
            Some(ChosenPath::Index(index_id, ranges, estimate, covering))
        }
        (None, Some(ranges)) => Some(ChosenPath::HandleRange(ranges, estimate)),
        // The table path the ranger narrowed nothing on: the whole-table read
        // `build_from` already installed is the answer, unchanged.
        (None, None) => None,
    }
}

/// The regular access-path candidate and exact projected columns a
/// single-table `SELECT` costs. Keeping this beneath both normal range
/// selection and automatic IndexMerge makes the two compete in the same cost
/// unit and under the same scan hints, limit, order, and skyline rules.
fn best_single_table_access_path(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
    hints: &crate::index_hints::AvailablePaths,
    partition_scan: bool,
) -> Option<(crate::access_cost::AccessPath, Vec<usize>)> {
    // No `WHERE` at all is not a reason to stop: a covering index is still a
    // candidate, and reading the whole of a narrow index beats reading the
    // whole table (Go's `path.IsSingleScan` arm of `keepIndex`).
    let where_clause = select.where_clause.as_ref();
    // The columns the statement reads, which decides whether an index path
    // covers (Go `isCoveringIndex`) and therefore whether it pays for a
    // double read.
    //
    // This is the SAME analysis a leaf of a multi-table `FROM` uses
    // ([`crate::driver::leaf_demand`]), and deliberately not
    // [`crate::column_prune::prunable_columns`]. The two answer different
    // questions: the pruner NARROWS the source's output, so it must be exact
    // in both directions and refuses every shape it cannot prove -- any
    // subquery above all -- and a refusal reads as "every column". Go has no
    // such refusal: `rule_column_pruning` walks the correlated subquery like
    // any other expression and hands the `DataSource` the columns its parents
    // still need, which is what `isCoveringIndex` then reads. Answering the
    // COST question with the pruner's refusal made `select c2 = (select ...)
    // from t1` declare that it needs `c1, c2, c3`, so `KEY c2(c2)` -- which
    // covers `c1, c2` on an integer-handle table -- was never even a
    // candidate. Captured TiDB reads `IndexFullScan` there.
    //
    // Over-approximating is the safe direction here for the same reason it is
    // at a join leaf: the source still emits the whole row, so a demand that
    // is too wide costs a covering index as a double read and falls back to
    // the scan that would have run anyway.
    let demand = crate::driver::leaf_demand::LeafDemand::of_select(select);
    let needed: Vec<usize> = demand.needed(&scope.tables[0].name, columns);
    let resolver = ScopeResolver { scope };
    // The `LIMIT` an index path may be costed under. `scan_limit_cap`'s own
    // refusals for things between the source and the LIMIT apply here too;
    // the residual `WHERE` is the one it cannot know yet, because which
    // conjuncts the source accepts is settled after the path is chosen. Go
    // has the same ordering and resolves it through the physical property.
    let cap = costing_limit_cap(select);
    let satisfied_by = |offsets: &[usize], ranges: &[IndexRange]| {
        select.order_by.is_empty()
            || order_is_index_order(
                select,
                &IndexAccessOrder::from_ranges(offsets, ranges),
                &resolver,
            )
    };
    let limit = cap.map(|cap| crate::access_cost::PushedLimit {
        cap,
        satisfied_by: &satisfied_by,
    });
    let stats = catalog.table_statistics(table.table_id);
    let stats = stats.as_ref().map(AsRef::as_ref);
    let paths = crate::access_cost::enumerate_paths(
        table,
        columns,
        where_clause,
        &needed,
        &resolver,
        limit.as_ref(),
        stats,
        hints,
        !select.order_by.is_empty(),
        partition_scan,
        demand.statement_forces_an_index(),
    );
    // Go's `prop.ExpectedCnt != math.MaxFloat64`: a row cap on the required
    // property is what disables Fix45132's row-ratio rule inside pruning.
    crate::access_cost::choose_access_path(paths, stats, cap.is_some()).map(|best| (best, needed))
}

/// The partitions a single-table `SELECT`'s `WHERE` proves it has to read,
/// or `None` when nothing narrows them.
///
/// The ranges come from the crate's ONE range builder
/// ([`crate::index_range::detach_cond_and_build_range_for_index`]), asked for
/// the partition expression's column exactly as it would be asked for a
/// single-column index on it. That reuse is the point: Go prunes with the
/// same `ranger` machinery it builds index ranges with, and a second range
/// implementation here would be a second answer to disagree with.
///
/// Pruning is declined -- reading everything -- in two cases, each a
/// SUPERSET and so never a wrong answer:
///
/// * a table with no partitioning;
/// * a partition expression that is not a bare COLUMN. Go prunes `year(a)`
///   through `MakePartitionByFnCol`'s monotonicity analysis, which this tier
///   does not port; a monotonicity claim that is wrong drops a partition
///   holding matching rows;
/// * a `SELECT` with no `WHERE`, which constrains nothing.
fn pruned_partition_ids(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Vec<i64>> {
    let partition = table.partition()?;
    let where_clause = select.where_clause.as_ref()?;
    let tuple_partitioning = matches!(
        partition.kind,
        crate::PartitionKind::Key
            | crate::PartitionKind::ListColumns { .. }
            | crate::PartitionKind::RangeColumns { .. }
    );
    // A bare column is the one scalar partition expression whose own value a
    // range over a column is. Tuple partitioning owns its named tuple.
    let mut range_columns = Vec::with_capacity(partition.dependencies.len());
    for dependency in &partition.dependencies {
        let column = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(dependency))?;
        if !tuple_partitioning && partition.expr_text != format!("`{}`", column.name) {
            return None;
        }
        range_columns.push(crate::index_range::RangeColumn::whole(
            column.name.clone(),
            column.field_type.clone(),
        ));
    }
    if range_columns.is_empty() {
        return None;
    }
    let built = crate::index_range::detach_cond_and_build_range_for_index(
        &range_columns,
        where_clause,
        zone,
    )?;
    crate::partition_pruning::pruned_ids(partition, &built.ranges)
}

/// How `EXPLAIN` names one key part of an index.
///
/// An ordinary key part is the column's name. An expression index's key part
/// is the EXPRESSION, not the hidden column the DDL rewrote it into: Go
/// prints `` index:k1(`a` + 1, b) ``, and the hidden column's generated name
/// appears in no user-visible output at all. The text is the one the column
/// already stores, so the plan and `SHOW CREATE TABLE` cannot disagree.
pub(crate) fn index_key_part_name(table: &KvTable, offset: usize) -> String {
    let Some(column) = table.columns.get(offset) else {
        return String::new();
    };
    match &column.generated {
        Some(generated) if table.is_hidden(offset) => generated.expr_text.clone(),
        _ => column.name.clone(),
    }
}

/// The `offset + count` an index path may be costed under, when nothing
/// between the source and the `LIMIT` can drop or add a row.
///
/// This is [`scan_limit_cap`]'s rule minus the two halves that are not known
/// until a path is committed: the residual `WHERE`, and which index supplies
/// the order (the caller supplies that as `satisfied_by`).
fn costing_limit_cap(select: &tidb_ast::SelectStmt) -> Option<f64> {
    let limit = select.limit.as_ref()?;
    let count = eval_limit_bound(&limit.count).ok()?;
    let offset = match &limit.offset {
        Some(expr) => eval_limit_bound(expr).ok()?,
        None => 0,
    };
    if select.distinct
        || select.having.is_some()
        || !select.group_by.is_empty()
        || crate::window::select_has_window(select)
    {
        return None;
    }
    Some(offset.checked_add(count)? as f64)
}

/// The estimate `EXPLAIN` prints for a table read that stayed a full scan.
///
/// This is the same [`crate::access_cost`] answer the path choice used, so
/// the printed plan and the costed plan cannot disagree. A table with no
/// loaded statistics is Go's `PseudoTable`, and the estimate says so.
pub(crate) fn full_scan_estimate(
    catalog: &Catalog,
    entry: &TableEntry,
) -> crate::access_cost::ScanEstimate {
    let stats = match entry {
        TableEntry::Kv(table) => catalog.table_statistics(table.table_id),
        // A memory table's rows are computed at query time and an
        // INFORMATION_SCHEMA view has no `mysql.stats_*` row, so there is
        // nothing to have analyzed; Go prints the pseudo constant for these
        // too.
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    };
    // The row count is real whenever a `mysql.stats_meta` row carries one,
    // even when no histogram was ever analyzed -- and in that state Go prints
    // the real count AND `stats:pseudo`. `realtime_row_count` owns the rule,
    // so this row and the cost that chose it agree by construction.
    scan_estimate(stats)
}

fn scan_estimate(
    stats: Option<&std::sync::Arc<crate::access_cost::TableStatistics>>,
) -> crate::access_cost::ScanEstimate {
    crate::access_cost::ScanEstimate {
        rows: crate::access_cost::realtime_row_count(stats.map(AsRef::as_ref)),
        pseudo: stats.is_none_or(|stats| stats.pseudo),
    }
}

/// `cardinality.Selectivity` for a single base table's `WHERE`.
///
/// This is what makes a `Selection` over a full scan print the estRows Go
/// prints. `None` means there is no `WHERE` to estimate, and nothing else:
/// a table with no analyzed histograms is Go's `PseudoTable`, which
/// `Selectivity` estimates through the SAME body using pseudo histograms
/// (`pkg/statistics/table.go:1034-1061` fills one per column), so routing it
/// anywhere else is what made `a = 1 and b = 2` print 10.00 against TiDB's
/// 1.00. [`crate::access_cost::selectivity`] owns both arms, and the
/// `stats:pseudo` flag stays where it was decided
/// ([`full_scan_estimate`]) -- which statistics exist is unchanged here, only
/// what is computed from them.
pub(crate) fn stats_selectivity(
    catalog: &Catalog,
    table: &KvTable,
    scope: &FromScope,
    where_clause: Option<&tidb_ast::Expr>,
) -> Option<f64> {
    let predicate = where_clause?;
    let stats = catalog.table_statistics(table.table_id);
    Some(crate::access_cost::selectivity(
        predicate,
        table,
        &scope_resolver(scope),
        stats.as_ref().map(AsRef::as_ref),
    ))
}

/// `cardinality.Selectivity` for a `SELECT`'s `WHERE` over a single base
/// table, when that table has loaded statistics.
pub(crate) fn select_stats_selectivity(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    scope: &FromScope,
) -> Option<f64> {
    let table = single_kv_table(&select.from, catalog, current_db)?;
    stats_selectivity(catalog, &table, scope, select.where_clause.as_ref())
}

/// The full-scan estimate and stats-backed selectivity a single-table write's
/// recorded read plan prints, resolved from the catalog by name.
pub(crate) fn single_table_trace_estimate(
    catalog: &Catalog,
    database: &str,
    name: &str,
    visible: &str,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
) -> (crate::access_cost::ScanEstimate, Option<f64>) {
    let Some(entry) = catalog.get_in(database, name) else {
        return (
            crate::access_cost::ScanEstimate::pseudo(crate::plan_trace::PSEUDO_ROW_COUNT),
            None,
        );
    };
    let estimate = full_scan_estimate(catalog, entry);
    let TableEntry::Kv(table) = entry else {
        return (estimate, None);
    };
    let scope = PlanTrace::single_table_scope(visible, None, columns.to_vec());
    (
        estimate,
        stats_selectivity(catalog, table, &scope, where_clause),
    )
}

/// How a single-table `UPDATE`/`DELETE` FETCHES the records it then filters.
///
/// Both arms narrow only which records are fetched. The write's own per-row
/// `WHERE` evaluation is unchanged and still decides which rows the statement
/// acts on, so the affected row set is the full scan's either way -- see
/// [`write_read_path`].
pub(crate) enum WriteReadPath {
    /// Go's `Point_Get`: one record, read by key. `None` is a key no row can
    /// carry, which Go also plans as a `Point_Get` that reads nothing.
    Point(Option<TableHandle>),
    /// Go's `TableRangeScan`: the handle intervals the `WHERE` implies, and
    /// the estimate `EXPLAIN` prints for them.
    Ranges(Vec<IndexRange>, crate::access_cost::ScanEstimate),
    /// Go's `IndexRangeScan`: the id of the index the chooser preferred, the
    /// ranges of it the `WHERE` implies, and the estimate `EXPLAIN` prints. A
    /// write fetches the candidate records through the index and still filters
    /// per row above, so the ranges are a superset of the affected rows.
    IndexRanges(i64, Vec<IndexRange>, crate::access_cost::ScanEstimate),
}

/// The read a single-table `UPDATE`/`DELETE` performs to find its target
/// rows; `None` when nothing narrows it and the write reads the whole table.
///
/// Go plans a write's read from the same predicate, with the same functions,
/// as a read's. `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) synthesize an `ast.SelectStmt` out
/// of the write's `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan` -- the SAME function a `SELECT` reaches through
/// `TryFastPlan` -- and only when that declines does the ordinary path plan a
/// `DataSource` whose table path gets its ranges from `deriveTablePathStats`
/// exactly as a `SELECT`'s does. This function is that order, and it calls
/// the same two builders the read side calls: [`try_point_get`] and
/// [`crate::handle_range`], the crate's single range algebra.
///
/// The point arm is what makes `WHERE id = 500` one key lookup instead of a
/// scan over the degenerate range `[500,500]`. A single-key range still costs
/// a range scan against storage; a key lookup does not, and that difference
/// is the whole reason Go replaces the read rather than narrowing it.
///
/// Neither arm may change the answer. A point plan is decided ONLY from
/// equalities that pin a whole key ([`try_point_get`] is Go's
/// `getNameValuePairs` rule: `AND` of `column = constant`, nothing else), the
/// key's constant is moved into the column's domain first or the plan is
/// abandoned ([`super::point_get_key`]), and the `WHERE` is still evaluated
/// per row above the fetch -- so an extra conjunct the key did not pin still
/// filters, and a key naming a row that does not exist simply reads nothing.
pub(crate) fn write_read_path(
    catalog: &Catalog,
    database: &str,
    name: &str,
    stmt: &PointPlanStmt<'_>,
    ctx: &crate::StmtContext,
) -> Result<Option<WriteReadPath>, DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return Ok(None);
    };
    // Go's order: the fast plan first, the table path only when it declines.
    // The column list is the table's own, because `try_point_get` reads it at
    // the offsets `pk_handle_offset`/`KvIndex::column_offsets` name, and those
    // are offsets into `KvTable::columns`.
    let columns: Vec<(String, FieldType)> = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect();
    let zone = &ctx.session_zone();
    let disable_point_get = ctx
        .optimizer_fix_control()
        .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false);
    if let Some(handle) = (!disable_point_get)
        .then(|| try_point_get(stmt, table, &columns, zone))
        .transpose()?
        .flatten()
    {
        return Ok(Some(WriteReadPath::Point(handle)));
    }
    // Go's write plan costs the index paths beside the table path, through the
    // same chooser a `SELECT` reaches (`tryUpdatePointPlan` falls through to
    // the ordinary `DataSource`). When the winner is an index, read through it;
    // otherwise fall back to the clustered-handle table path below, unchanged.
    if let Some(index_path) = write_index_range_path(table, &columns, stmt.where_clause, name, ctx)
    {
        return Ok(Some(index_path));
    }
    Ok(
        write_handle_ranges(catalog, database, name, stmt.where_clause, zone)
            .map(|(ranges, estimate)| WriteReadPath::Ranges(ranges, estimate)),
    )
}

/// The index range a single-table `UPDATE`/`DELETE` should read through, when
/// the cost chooser prefers an index over the table path -- Go's write plan
/// reusing the read side's `findBestTask`.
///
/// Returns `Some` only when the winner is an INDEX; a table-path winner (the
/// clustered handle, or nothing) is left to [`write_handle_ranges`] so that
/// path's estimate and its `skipNull` handling are unchanged. Every column is
/// declared needed, because a write reads the whole row to act on it -- Go's
/// write is always a double read -- so the index never covers and the chooser
/// prices it honestly.
fn write_index_range_path(
    table: &KvTable,
    columns: &[(String, FieldType)],
    where_clause: Option<&tidb_ast::Expr>,
    table_name: &str,
    ctx: &crate::StmtContext,
) -> Option<WriteReadPath> {
    let where_clause = where_clause?;
    let resolver = TableResolver {
        table_name,
        columns,
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
    };
    let needed: Vec<usize> = (0..columns.len()).collect();
    let hints = crate::index_hints::AvailablePaths::unrestricted();
    let paths = crate::access_cost::enumerate_paths(
        table,
        columns,
        Some(where_clause),
        &needed,
        &resolver,
        None,
        None,
        &hints,
        false,
        false,
        // An `UPDATE`/`DELETE` carries no `FROM`-clause index hint in the
        // grammar this tier accepts, so no path of it is `path.Forced`.
        false,
    );
    let best = crate::access_cost::choose_access_path(paths, None, false)?;
    match best.index {
        Some((index_id, ranges)) => {
            Some(WriteReadPath::IndexRanges(index_id, ranges, best.estimate))
        }
        None => None,
    }
}

/// The handle ranges a single-table `UPDATE`/`DELETE` reads through, and the
/// estimate `EXPLAIN` prints for that read; `None` when the `WHERE` narrows
/// the clustered integer handle by nothing and the write reads the whole
/// table.
///
/// This is the table-path half of [`write_read_path`]; see its doc for the
/// order the two halves run in and why neither can change the answer.
fn write_handle_ranges(
    catalog: &Catalog,
    database: &str,
    name: &str,
    where_clause: Option<&tidb_ast::Expr>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<(Vec<IndexRange>, crate::access_cost::ScanEstimate)> {
    let where_clause = where_clause?;
    let Some(TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return None;
    };
    let ranges = crate::handle_range::build_handle_ranges(table, where_clause, zone)?.ranges;
    let stats = catalog.table_statistics(table.table_id);
    let stats = stats.as_ref().map(AsRef::as_ref);
    let estimate = crate::access_cost::ScanEstimate {
        rows: crate::handle_range::handle_range_row_count(table, &ranges, stats),
        pseudo: stats.is_none_or(|stats| stats.pseudo),
    };
    Some((ranges, estimate))
}

/// Splits a `WHERE` over one base table into the conjuncts the scan can apply
/// itself and the predicate that must stay above it.
///
/// This is Go's `rule_predicate_push_down` split narrowed to the shape the
/// bounded TiKV Selection lowering already speaks -- see
/// [`crate::predicate_pushdown`] for the rule and for why the pushed half may be
/// removed from the `Selection` only when the source promises to apply it to
/// every row, staged writes included.
///
/// The residual is the remaining conjuncts re-joined with `AND` in their
/// original order, so what runs above the scan is the `WHERE` minus exactly
/// what moved into it. `None` means every conjunct was pushed.
pub(crate) fn split_scan_predicates(
    where_clause: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
    ctx: &crate::StmtContext,
) -> (PushedScanFilter, Option<tidb_ast::Expr>) {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let mut predicates = Vec::new();
    let mut filters = Vec::new();
    let mut residual: Vec<&tidb_ast::Expr> = Vec::new();
    for conjunct in conjuncts {
        match scan_predicate(conjunct, resolver).and_then(|predicate| {
            let mut filter = rewrite_expr_resolved(conjunct, resolver).ok()?;
            // Go `refineArgs`: `int column <cmp> non-int constant` folds the
            // constant into the column's type ONCE here, so the filter this
            // scan runs on every row compares int to int. Without it the
            // string is re-coerced per row -- the same work, and the same
            // 1292 truncation, once for each row scanned.
            tidb_expr::builtin_compare::refine_comparisons(&mut filter, ctx).ok()?;
            Some((predicate, filter))
        }) {
            Some((predicate, filter)) => {
                predicates.push(predicate);
                filters.push(filter);
            }
            None => residual.push(conjunct),
        }
    }
    let residual = residual.into_iter().cloned().reduce(|left, right| {
        tidb_ast::Expr::Binary(
            tidb_ast::BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )
    });
    (PushedScanFilter::new(predicates, filters), residual)
}

/// One conjunct as a coprocessor-describable predicate, when it is one.
///
/// The describable shapes are a column-versus-constant comparison,
/// `IS [NOT] NULL`, `[NOT] IN` over constants, and the `OR`/`NOT` composition
/// of those -- exactly the set TiKV's whitelist admits unconditionally
/// (`infer_pushdown.go`'s `scalarExprSupportedByTiKV`). `AND` is absent
/// because the caller already flattened the top-level `AND` into separate
/// conjuncts, and a nested one inside an `OR` is described by recursing into
/// the branch as its own conjunct list would not be.
fn scan_predicate(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanPredicate> {
    match conjunct {
        tidb_ast::Expr::Paren(inner) => scan_predicate(inner, resolver),
        // `NOT x` and `!x`; the arithmetic unary operators are not predicates.
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Not | tidb_ast::UnaryOp::NotKeyword, inner) => {
            Some(ScanPredicate::Not(Box::new(scan_predicate(
                inner, resolver,
            )?)))
        }
        // `x OR y`, flattened: the chain is left-associative, so flattening
        // and re-folding preserves the same disjunction.
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, ..) => {
            let mut branches = Vec::new();
            collect_disjuncts(conjunct, &mut branches);
            Some(ScanPredicate::Or(
                branches
                    .into_iter()
                    .map(|branch| scan_predicate(branch, resolver))
                    .collect::<Option<Vec<_>>>()?,
            ))
        }
        // Only `IS [NOT] NULL`. `IS TRUE`/`IS FALSE`/`IS UNKNOWN` are separate
        // Go functions with their own signatures and their own NULL handling.
        tidb_ast::Expr::Is {
            expr,
            target: tidb_ast::IsTarget::Null,
            not,
        } => {
            let (offset, column_type) = resolve_column(expr, resolver)?;
            Some(ScanPredicate::IsNull {
                column_offset: offset,
                column_type,
                negated: *not,
            })
        }
        tidb_ast::Expr::In { expr, list, not } => {
            let (offset, column_type) = resolve_column(expr, resolver)?;
            if list.is_empty() {
                return None;
            }
            let mut literals = Vec::with_capacity(list.len());
            for element in list {
                let literal = constant_value(element, &resolver.time_zone())?;
                // A NULL member makes `IN` UNKNOWN rather than false for a
                // non-matching row, and `NOT IN` UNKNOWN for every row; that
                // is not the membership test this description promises.
                if literal == Datum::Null {
                    return None;
                }
                literals.push(literal);
            }
            Some(ScanPredicate::In {
                column_offset: offset,
                column_type,
                literals,
                negated: *not,
            })
        }
        // A builtin call, when the push-down catalog resolves a signature TiKV
        // evaluates for it. The whole `WHERE sin(a)` conjunct is then the
        // Selection condition, evaluated for truth exactly as a `Selection`
        // above the scan would evaluate it.
        _ => scan_comparison(conjunct, resolver)
            .map(ScanPredicate::Compare)
            .or_else(|| scan_operand_call(conjunct, resolver).map(ScanPredicate::Builtin)),
    }
}

/// One argument of a described builtin call: a column of the scanned table, an
/// already-folded integer constant, or a nested call the catalog also resolves.
///
/// Anything else -- a non-integer constant, a subquery, a call whose signature
/// TiKV does not evaluate -- makes the whole conjunct residual, which is Go's
/// own rule: `scalarFuncToPBExpr` returns nil as soon as one child does.
fn scan_operand(
    argument: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
    use tidb_expr::pushdown_catalog::PbScalar;
    if let tidb_ast::Expr::Paren(inner) = argument {
        return scan_operand(inner, resolver);
    }
    if let tidb_ast::Expr::Column(_) = argument {
        let (offset, field_type) = resolve_column(argument, resolver)?;
        return Some(PbScalar::Column { offset, field_type });
    }
    // A constant subtree first, so a folded literal argument (`MOD(a, 3 + 1)`)
    // is the constant Go would have folded rather than a `plus` call. Only an
    // integer is describable: every other constant family needs the TiPB
    // literal encoding this tier does not build.
    if let Some(Datum::Int(value)) = constant_value(argument, &resolver.time_zone()) {
        return Some(PbScalar::IntLiteral(value));
    }
    scan_operand_call(argument, resolver)
}

/// A builtin call as an operand, in either of the two spellings the parser
/// produces for one: an explicit `Expr::Func`, and the operator form real TiDB
/// also desugars to a named scalar function -- `MOD(a, b)` parses as the `%`
/// binary operator, and Go's `ScalarFunction` for it is named `mod` either way.
fn scan_operand_call(
    argument: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<tidb_expr::pushdown_catalog::PbScalar> {
    let (name, args): (String, Vec<&tidb_ast::Expr>) = match argument {
        tidb_ast::Expr::Func { name, args, .. } => {
            (name.to_ascii_lowercase(), args.iter().collect())
        }
        tidb_ast::Expr::Binary(op, lhs, rhs) => (
            tidb_expr::scalar_function::binary_op_name(*op).to_owned(),
            vec![lhs, rhs],
        ),
        _ => return None,
    };
    let operands = args
        .into_iter()
        .map(|nested| scan_operand(nested, resolver))
        .collect::<Option<Vec<_>>>()?;
    tidb_expr::pushdown_catalog::build_call(&name, operands)
}

/// Flattens an `OR` chain into its branches, in source order.
fn collect_disjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_disjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicOr, lhs, rhs) => {
            collect_disjuncts(lhs, out);
            collect_disjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The scan-input offset and declared type of `expr`, when it is a plain
/// reference to a column of the scanned table.
fn resolve_column(
    expr: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<(u32, FieldType)> {
    match expr {
        tidb_ast::Expr::Paren(inner) => resolve_column(inner, resolver),
        tidb_ast::Expr::Column(path) => {
            let (offset, column_type, _) = resolver.resolve(path)?;
            Some((u32::try_from(offset).ok()?, column_type))
        }
        _ => None,
    }
}

/// The already-evaluated value of `expr`, when it is a constant.
///
/// A negated integer literal is folded here rather than left as the unary
/// minus the parser produced, because Go's expression rewriter folds it too
/// (`foldConstant` over a deterministic function of constants) and the
/// coprocessor is therefore sent the negative constant, not a `UnaryMinus`
/// node. Without this, `WHERE a > -1` describes nothing at all.
fn constant_value(expr: &tidb_ast::Expr, zone: &tidb_datatype::SessionTimeZone) -> Option<Datum> {
    match expr {
        tidb_ast::Expr::Paren(inner) => constant_value(inner, zone),
        tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Minus, inner) => {
            match constant_value(inner, zone)? {
                Datum::Int(value) => value.checked_neg().map(Datum::Int),
                // Any other negated constant keeps whatever type MySQL's unary
                // minus gives it, which is not this narrow fold's business.
                _ => None,
            }
        }
        _ => {
            // The zone is the STATEMENT's: a fold here must agree byte for
            // byte with the residual predicate's own fold, or a conjunct
            // consumed into a scan key would probe a different instant than
            // the filter it replaced would have accepted.
            let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
                expr,
                &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
            ) else {
                return None;
            };
            constant.eval().ok()
        }
    }
}

/// One conjunct as a column-versus-constant comparison, when it is one.
fn scan_comparison(
    conjunct: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> Option<ScanComparison> {
    let tidb_ast::Expr::Binary(op, lhs, rhs) = conjunct else {
        return None;
    };
    let op = ScanComparisonOp::from_ast(*op)?;
    // Go accepts the constant on either side and the protobuf preserves the
    // operand order it was written in, so the side is recorded rather than
    // normalized away.
    let (column, value, column_on_left) = match (&**lhs, &**rhs) {
        (tidb_ast::Expr::Column(path), other) => (path, other, true),
        (other, tidb_ast::Expr::Column(path)) => (path, other, false),
        _ => return None,
    };
    // A second column reference on the "constant" side leaves the shape.
    let (offset, column_type, _) = resolver.resolve(column)?;
    let literal = constant_value(value, &resolver.time_zone())?;
    // A NULL constant makes the comparison unknown for every row; that is a
    // whole-predicate property Go handles in the ranger, not a filter shape.
    if literal == Datum::Null {
        return None;
    }
    Some(ScanComparison {
        column_offset: u32::try_from(offset).ok()?,
        column_type,
        op,
        literal,
        column_on_left,
    })
}

/// Flattens an `AND` chain into its conjuncts.
fn collect_conjuncts<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    match expr {
        tidb_ast::Expr::Paren(inner) => collect_conjuncts(inner, out),
        tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) => {
            collect_conjuncts(lhs, out);
            collect_conjuncts(rhs, out);
        }
        other => out.push(other),
    }
}

/// The single TiKV-backed table a `FROM` names, when it names exactly one.
/// A point get applies only to that shape (Go `getSingleTableNameAndAlias`).
/// The one plain table a `FROM` names, when it names exactly one.
///
/// Split out of [`single_kv_table`] because the access-path decision needs the
/// REFERENCE, not just the table it resolves to: the `USE`/`FORCE`/`IGNORE
/// INDEX` hints that decide which paths exist live on the reference.
pub(crate) fn single_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let table_ref = sole_table_ref(from)?;
    // A `PARTITION (...)` restriction is refused by `build_from`; declining
    // the fast path here too keeps a point get from answering a statement the
    // scan would have rejected.
    if !table_ref.partitions.is_empty() {
        return None;
    }
    Some(table_ref)
}

/// [`single_table_ref`] WITHOUT its fast-path refusal: the one table a `FROM`
/// names, whether or not the statement narrowed it with `PARTITION (...)`.
///
/// The refusal above is about which ACCESS PATHS may be chosen. Callers that
/// only want to know which table -- and which partitions of it -- the
/// statement reads want this one, so that a `PARTITION (p)` narrowing is
/// reported rather than silently read as "no single table".
pub(crate) fn sole_table_ref(from: &Option<tidb_ast::Join>) -> Option<&tidb_ast::TableRef> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    Some(table_ref)
}

/// [`single_kv_table`] over [`sole_table_ref`]: the stored table a `FROM`
/// names even when a `PARTITION (...)` list narrowed it.
pub(crate) fn sole_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let table_ref = sole_table_ref(from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    }
}

pub(crate) fn single_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let table_ref = single_table_ref(from)?;
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        // A view stores no rows, so there is no point get to try.
        TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            None
        }
    }
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` or a row-valued `IN` covering one whole unique
/// key
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. A scalar integer primary key becomes an integer handle;
/// a row covering a clustered primary key becomes a common handle; otherwise
/// the row must cover a visible, non-prefix unique secondary index.
pub(crate) struct BatchPointGetDecision {
    /// The distinct record handles the point source will read.
    pub(crate) handles: Vec<TableHandle>,
    /// The index authority, absent only for an integer handle primary key.
    index: Option<BatchPointGetIndex>,
    routed_partitions: Option<Vec<String>>,
}

struct BatchPointGetIndex {
    name: String,
    columns: Vec<String>,
    clustered: bool,
}

impl BatchPointGetIndex {
    fn access_object(&self) -> String {
        format!(
            "{}index:{}({})",
            if self.clustered { "clustered " } else { "" },
            self.name,
            self.columns.join(", ")
        )
    }
}

pub(crate) fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
    hints: &crate::index_hints::AvailablePaths,
    ctx: &impl tidb_expr::Columns,
) -> Result<Option<BatchPointGetDecision>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
        || !select.windows.is_empty()
    {
        return Ok(None);
    }
    let Some(table_ref) = sole_table_ref(&select.from) else {
        return Ok(None);
    };
    let Some(table_name) = table_ref.name.last() else {
        return Ok(None);
    };
    let qualifier = table_ref.alias.as_deref().unwrap_or(table_name);
    // Go's syntax-level fast-plan helper refuses a table containing any
    // generated column, but its ordinary access-path optimizer immediately
    // recovers the same exact-IN query as BatchPointGet. This driver has one
    // access-path decision point, so it implements the final observable plan:
    // an indexed generated column is eligible and the row decoder still
    // materializes virtual values on the returned row.
    let Some(where_clause) = &select.where_clause else {
        return Ok(None);
    };
    // The WHERE must be exactly the IN, as Go requires a PatternInExpr.
    let tidb_ast::Expr::In { expr, list, not } = where_clause else {
        return Ok(None);
    };
    if *not || list.is_empty() {
        return Ok(None);
    }
    fn without_parens(mut expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
        while let tidb_ast::Expr::Paren(inner) = expr {
            expr = inner;
        }
        expr
    }
    fn column_name<'a>(expr: &'a tidb_ast::Expr, qualifier: &str) -> Option<&'a str> {
        let tidb_ast::Expr::Column(path) = without_parens(expr) else {
            return None;
        };
        let name = path.last()?;
        if path.len() >= 2 && !path[path.len() - 2].eq_ignore_ascii_case(qualifier) {
            return None;
        }
        Some(name)
    }

    let left = without_parens(expr);
    let mut names = Vec::new();
    match left {
        tidb_ast::Expr::Column(_) => {
            let Some(name) = column_name(left, qualifier) else {
                return Ok(None);
            };
            names.push(name);
        }
        tidb_ast::Expr::Row(items) => {
            for item in items {
                let Some(name) = column_name(item, qualifier) else {
                    return Ok(None);
                };
                names.push(name);
            }
        }
        _ => return Ok(None),
    }
    let mut offsets = Vec::with_capacity(names.len());
    for name in &names {
        let Some(offset) = columns
            .iter()
            .position(|(column, _)| column.eq_ignore_ascii_case(name))
        else {
            return Ok(None);
        };
        offsets.push(offset);
    }

    // Every right-hand element is either the scalar value for a scalar left
    // side or a row of exactly the same arity. Each value is converted in the
    // domain of the column it was written against BEFORE any index-order
    // permutation, exactly as Go's `getPointGetValue` loop does.
    let mut rows = Vec::with_capacity(list.len());
    for item in list {
        let item = without_parens(item);
        let value_exprs: Vec<&tidb_ast::Expr> = if offsets.len() == 1 {
            vec![item]
        } else {
            let tidb_ast::Expr::Row(items) = item else {
                return Ok(None);
            };
            if items.len() != offsets.len() {
                return Ok(None);
            }
            items.iter().collect()
        };
        let mut values = Vec::with_capacity(value_exprs.len());
        for (value_expr, offset) in value_exprs.into_iter().zip(&offsets) {
            let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(
                value_expr,
                &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
            ) else {
                return Ok(None);
            };
            let Ok(value) = constant.eval() else {
                return Ok(None);
            };
            // Go's secondary/common-index path carries NULL through plan
            // construction and filters that key before lookup. The integer
            // handle arm below still abandons the whole fast plan when it
            // sees NULL, as `newBatchPointGetPlan` does in its handle loop.
            if value.is_null() {
                values.push(Datum::Null);
                continue;
            }
            let Some(value) = point_get_value(&columns[*offset].1, &value) else {
                return Ok(None);
            };
            values.push(value);
        }
        rows.push(values);
    }

    let index_order = |key_offsets: &[usize]| -> Option<Vec<usize>> {
        if key_offsets.len() != offsets.len() {
            return None;
        }
        let positions = key_offsets
            .iter()
            .map(|key_offset| offsets.iter().position(|offset| offset == key_offset))
            .collect::<Option<Vec<_>>>()?;
        let mut unique = positions.clone();
        unique.sort_unstable();
        unique.dedup();
        (unique.len() == offsets.len()).then_some(positions)
    };
    let ordered_values = |row: &[Datum], order: &[usize]| {
        order
            .iter()
            .map(|position| row[*position].clone())
            .collect::<Vec<_>>()
    };
    let push_unique = |handles: &mut Vec<TableHandle>, handle: TableHandle| {
        if !handles.contains(&handle) {
            handles.push(handle);
        }
    };

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if offsets == [offset] {
            // Go `newBatchPointGetPlan` runs every list element through
            // `getPointGetValue` and returns `nil` -- no batch plan at all --
            // as soon as one of them is not exactly representable, so a list
            // mixing `1.0` with `1.5` still answers from a scan rather than
            // silently dropping the element it cannot key.
            let mut handles = Vec::with_capacity(rows.len());
            for row in &rows {
                let handle = match &row[0] {
                    Datum::Int(v) => TableHandle::Int(*v),
                    Datum::UInt(v) => TableHandle::Int(*v as i64),
                    _ => return Ok(None),
                };
                push_unique(&mut handles, handle);
            }
            return Ok(Some(BatchPointGetDecision {
                handles,
                index: None,
                routed_partitions: None,
            }));
        }
    }

    // A clustered composite/string primary key is itself the unique index.
    // Rust deliberately stores no duplicate `KvIndex` metadata for it because
    // its encoding is the record handle, so recognize that authority here.
    let common_handle_order = if hints.allows_index(crate::index_hints::COMMON_PRIMARY_INDEX_ID) {
        index_order(table.common_handle_offsets())
    } else {
        None
    };
    if let Some(order) = common_handle_order {
        let mut handles = Vec::with_capacity(rows.len());
        for row in &rows {
            let values = ordered_values(row, &order);
            if values.contains(&Datum::Null) {
                continue;
            }
            let handle = table
                .common_handle_from_values(&values, zone)
                .map_err(|e| DriverError::Parse(format!("common handle encoding failed: {e:?}")))?;
            push_unique(&mut handles, handle);
        }
        return Ok(Some(BatchPointGetDecision {
            handles,
            index: Some(BatchPointGetIndex {
                name: "PRIMARY".to_owned(),
                columns: table
                    .common_handle_offsets()
                    .iter()
                    .map(|offset| columns[*offset].0.clone())
                    .collect(),
                clustered: true,
            }),
            routed_partitions: None,
        }));
    }

    // The unique-index path.
    let mut table = table.clone();
    for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
        if !index.unique || !hints.allows_index(index.id) {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        // Resolved through visible offsets for the same reason the single
        // point get does: an EXPRESSION key part's hidden generated column
        // sits past the end of the scope, and no `IN` list names it.
        if index
            .column_offsets
            .iter()
            .any(|offset| columns.get(*offset).is_none())
        {
            continue;
        }
        let Some(order) = index_order(&index.column_offsets) else {
            continue;
        };
        let mut handles = Vec::new();
        let mut routed_partitions = Vec::new();
        for row in &rows {
            let values = ordered_values(row, &order);
            if let Some(handle) = table
                .lookup_unique(index.id, &values, zone)
                .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
            {
                if !handles.contains(&handle) {
                    handles.push(handle);
                    if let Some(partition) = table.partition() {
                        let mut full_row = vec![Datum::Null; table.columns.len()];
                        for (offset, value) in offsets.iter().zip(row) {
                            full_row[*offset] = value.clone();
                        }
                        if let Ok(ordinal) =
                            partition.locate_ordinal(&full_row, &table.columns, ctx)
                        {
                            if let Some(definition) = partition.definitions.get(ordinal) {
                                if !routed_partitions.contains(&definition.name) {
                                    routed_partitions.push(definition.name.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(partition) = table.partition() {
            routed_partitions.sort_by_key(|name| {
                partition
                    .definitions
                    .iter()
                    .position(|definition| definition.name.eq_ignore_ascii_case(name))
                    .unwrap_or(usize::MAX)
            });
        }
        return Ok(Some(BatchPointGetDecision {
            handles,
            index: Some(BatchPointGetIndex {
                name: index.name,
                columns: index
                    .column_offsets
                    .iter()
                    .map(|offset| columns[*offset].0.clone())
                    .collect(),
                clustered: false,
            }),
            routed_partitions: Some(routed_partitions),
        }));
    }
    Ok(None)
}

/// Moves every pair's constant into its column's domain, in place.
///
/// Returns false when any pair names an unknown column or holds a constant
/// the column cannot represent exactly, which is Go's "no point plan; let the
/// scan decide" answer.
pub(crate) fn convert_pairs_to_column_domain(
    pairs: &mut [NameValuePair],
    columns: &[(String, FieldType)],
) -> bool {
    for pair in pairs {
        let Some((_, field_type)) = columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&pair.column))
        else {
            return false;
        };
        let Some(value) = point_get_value(field_type, &pair.value) else {
            return false;
        };
        pair.value = value;
    }
    true
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
pub(crate) struct NameValuePair {
    column: String,
    value: Datum,
}

impl NameValuePair {
    /// The column this equality pinned.
    pub(crate) fn column(&self) -> &str {
        &self.column
    }

    /// The value it pinned the column to, already moved into that column's
    /// domain by [`convert_pairs_to_column_domain`].
    pub(crate) const fn value(&self) -> &Datum {
        &self.value
    }
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
pub(crate) fn name_value_pairs(
    expr: &tidb_ast::Expr,
    pairs: &mut Vec<NameValuePair>,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    match expr {
        Expr::Paren(inner) => name_value_pairs(inner, pairs, zone),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            name_value_pairs(lhs, pairs, zone) && name_value_pairs(rhs, pairs, zone)
        }
        Expr::Binary(BinaryOp::Eq, lhs, rhs) => {
            let (column, value) = match (&**lhs, &**rhs) {
                (Expr::Column(path), other) => (path, other),
                (other, Expr::Column(path)) => (path, other),
                _ => return false,
            };
            let Some(name) = column.last() else {
                return false;
            };
            // Only a literal qualifies; anything needing evaluation against a
            // row is not a point-get key.
            let Ok(value) = rewrite_expr_resolved(
                value,
                &tidb_expr::rewriter::ZonedNoResolver::new(zone.clone()),
            ) else {
                return false;
            };
            let Expression::Constant(constant) = value else {
                return false;
            };
            let Ok(value) = constant.eval() else {
                return false;
            };
            pairs.push(NameValuePair {
                column: name.clone(),
                value,
            });
            true
        }
        _ => false,
    }
}

/// The clauses [`try_point_get`] decides a point plan from.
///
/// This exists because Go decides a WRITE's point plan from the SAME
/// function as a read's: `tryUpdatePointPlan`/`tryDeletePointPlan`
/// (`pkg/planner/core/point_get_plan.go`) build an `ast.SelectStmt` out of
/// the write's own `TableRefs`/`Where`/`Order`/`Limit` and hand it to
/// `tryPointGetPlan`. This struct IS that synthesis, expressed as the field
/// copy Go performs rather than as a second point-plan builder -- there is
/// one rule here and one implementation of it, and a write cannot drift from
/// a read about which statements are point plans.
pub(crate) struct PointPlanStmt<'a> {
    where_clause: Option<&'a tidb_ast::Expr>,
    order_by: &'a [tidb_ast::OrderItem],
    limit: Option<&'a tidb_ast::Limit>,
    /// Go's synthesized statement carries no select list, so it has neither
    /// of these; only a real `SELECT` can.
    having: Option<&'a tidb_ast::Expr>,
    group_by: &'a [tidb_ast::GroupByItem],
}

impl<'a> PointPlanStmt<'a> {
    /// A `SELECT`'s own clauses.
    pub(crate) fn of_select(select: &'a tidb_ast::SelectStmt) -> Self {
        PointPlanStmt {
            where_clause: select.where_clause.as_ref(),
            order_by: &select.order_by,
            limit: select.limit.as_ref(),
            having: select.having.as_ref(),
            group_by: &select.group_by,
        }
    }

    /// Go's synthesized `ast.SelectStmt` for a single-table write: the three
    /// clauses `tryUpdatePointPlan`/`tryDeletePointPlan` copy across, and
    /// nothing else.
    pub(crate) fn of_write(
        where_clause: Option<&'a tidb_ast::Expr>,
        order_by: &'a [tidb_ast::OrderItem],
        limit: Option<&'a tidb_ast::Limit>,
    ) -> Self {
        PointPlanStmt {
            where_clause,
            order_by,
            limit,
            having: None,
            group_by: &[],
        }
    }
}

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table statement with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
pub(crate) fn try_point_get(
    select: &PointPlanStmt<'_>,
    table: &KvTable,
    columns: &[(String, FieldType)],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<Option<TableHandle>>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs, zone) || pairs.is_empty() {
        return Ok(None);
    }
    // Go `getNameValuePairs` moves every constant into its column's domain
    // before the pair is usable as a key, and abandons the whole point plan
    // when one of them will not survive the round trip. Doing it here, once
    // for every pair, is what keeps the handle arm below dealing only in
    // integers and the unique-index arm dealing only in column-typed values.
    if !convert_pairs_to_column_domain(&mut pairs, columns) {
        return Ok(None);
    }

    // The handle path: the primary key pinned by exactly one equality, which
    // is Go's `len(pairs) == 1` condition on the handle pair.
    if let Some(handle_offset) = table.pk_handle_offset() {
        let handle_column = &columns[handle_offset].0;
        if pairs.len() == 1 && pairs[0].column.eq_ignore_ascii_case(handle_column) {
            return Ok(Some(match &pairs[0].value {
                Datum::Int(value) => Some(TableHandle::Int(*value)),
                Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                // Unreachable: the conversion above has already put the value
                // in the handle column's integer domain or refused the plan.
                _ => return Ok(None),
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    let mut table = table.clone();
    for index in table.plan_indexes().cloned().collect::<Vec<_>>() {
        if !index.unique {
            continue;
        }
        // Go `point_get_plan.go` declines an index with `HasPrefixIndex()`:
        // an entry found by a CUT value does not prove the row matches, and a
        // point get has no residual predicate to catch that. Skipping the
        // index here is load-bearing, not defensive -- `lookup_unique` fails
        // closed with `None`, which this loop would otherwise read as "no
        // such row" and answer zero rows for a row that exists.
        if index.has_prefix() {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            // Go `getIndexValues` resolves each key part by NAME against the
            // `WHERE`'s pairs, so a key part the statement cannot name
            // declines the whole index. The hidden generated column an
            // EXPRESSION key part was rewritten into is exactly such a part:
            // it lives past the end of the scope's visible column list, and
            // `tidb_shard(a)` is not a name any `WHERE` writes. Resolving the
            // name through `get` makes "no visible column at that offset" and
            // "not pinned by the WHERE" the same answer -- without it the
            // offset indexes past the end and panics, which is what
            // `explain_shard_index`'s `where a=100` reached.
            let Some(pair) = columns.get(*offset).and_then(|(name, _)| {
                pairs
                    .iter()
                    .find(|pair| pair.column.eq_ignore_ascii_case(name))
            }) else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        let handle = table
            .lookup_unique(index.id, &values, zone)
            .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?;
        return Ok(Some(handle));
    }
    Ok(None)
}

/// The row order a committed index access path produces, for the `ORDER BY`
/// half of the `LIMIT` push-down rule.
pub(crate) struct IndexAccessOrder {
    /// The index's columns as offsets into the source row, in index order.
    column_offsets: Vec<usize>,
    /// Whether one range covers the access path. Several ranges are each
    /// internally in index order but are walked one after another, and their
    /// concatenation is not index order, so only a single range establishes
    /// the total order an `ORDER BY` can be discharged against.
    single_range: bool,
    /// Key positions fixed to one value by the single range. Go carries the
    /// equivalent fact in `AccessPath.ConstCols` and skips those positions in
    /// `matchProperty` Case 2.
    constant_positions: Vec<bool>,
}

impl IndexAccessOrder {
    fn from_ranges(column_offsets: &[usize], ranges: &[IndexRange]) -> Self {
        // Go `matchProperty` Case 2: a key part fixed to one value does not
        // participate in the varying row order, so ORDER BY may start after
        // it. Case 3 (different point values in several ranges) requires a
        // merge-sort operator this tier does not have and remains excluded by
        // `single_range`.
        let constant_positions = if let [range] = ranges {
            column_offsets
                .iter()
                .enumerate()
                .map(|(position, _)| {
                    range
                        .low
                        .get(position)
                        .zip(range.high.get(position))
                        .is_some_and(|(low, high)| low == high)
                })
                .collect()
        } else {
            Vec::new()
        };
        Self {
            column_offsets: column_offsets.to_vec(),
            single_range: ranges.len() == 1,
            constant_positions,
        }
    }
}

/// The row cap a `LIMIT` may push into the source, or `None` to leave all the
/// work to the `LimitExec`.
///
/// Go pushes a `Limit` into the cop task below the scan, and a `TopN` when an
/// order has to be established first. Captured from TiDB (mock store,
/// `pkg/executor/zz_dump_limit_test.go`):
///
/// ```text
/// select a, b from t where b > 4 limit 3
///   Limit_8            root       offset:0, count:3
///   └─IndexReader_13   root       index:Limit_12
///     └─Limit_12       cop[tikv]  offset:0, count:3
///       └─IndexRangeScan_11  cop[tikv]  range:(4,+inf], keep order:false
///
/// select a, b from t where b > 4 order by b limit 2, 3
///   Limit_13           root       offset:2, count:3
///     └─Limit_22       cop[tikv]  offset:0, count:5      <- cap is offset+count
///       └─IndexRangeScan_21  cop[tikv]  range:(4,+inf], keep order:true
///
/// select a, b from t order by c limit 3                  <- NOT pushed
///   TopN_8             root       test.t.c, offset:0, count:3
///     └─TopN_17        cop[tikv]  test.t.c, offset:0, count:3
///       └─TableFullScan_16  cop[tikv]  keep order:false  <- reads all 20 rows
///
/// select a, b from t where c > 4 order by b limit 3      <- NOT pushed
///   TopN_8             root       test.t.b, offset:0, count:3
///     └─TopN_18        cop[tikv]  test.t.b, offset:0, count:3
///       └─Selection_17 cop[tikv]  gt(test.t.c, 4)        <- filter below the TopN
/// ```
///
/// # The rule
///
/// The cap is `offset + count`, because the offset rows are dropped above and
/// must still be produced -- Go's cop-side `Limit` carries exactly that
/// (`limit 2, 3` lowers to `offset:0, count:5`).
///
/// A cap is only sound when every row the source emits reaches the `LIMIT`,
/// in the order the `LIMIT` selects from. So it is refused when anything
/// between them can drop rows (a residual `Selection`, `DISTINCT`, `HAVING`),
/// or add them (a window function's materialize-and-append), and when the
/// query has an `ORDER BY` the access path does not already satisfy -- a sort
/// must see every row before it can name the first one, which is why Go turns
/// that case into a `TopN` and leaves the scan reading everything.
///
/// An `ORDER BY` is satisfied when the source is a single index range and the
/// by-items are a prefix of that index's columns, all ascending: the storage
/// iterator walks encoded index keys in ascending order, and the codec's
/// order is the collation order the sort would have used (NULLs lowest, as
/// `ORDER BY ... ASC` puts them first).
///
/// # Divergence from Go
///
/// Go decides this in the planner and *prints* it (`Limit` inside `cop[tikv]`,
/// or `keep order:true` on the scan). This tier has no cop task or
/// `TableReader` in its plan text, so the push-down changes only what runs:
/// the printed plan keeps the `Limit`-over-scan shape either way, and the
/// truncation shows up in `EXPLAIN ANALYZE`'s `actRows` instead.
fn scan_limit_cap(
    select: &tidb_ast::SelectStmt,
    residual_where: Option<&tidb_ast::Expr>,
    index_order: Option<&IndexAccessOrder>,
    resolver: &ScopeResolver<'_>,
) -> Option<u64> {
    let limit = select.limit.as_ref()?;
    let count = eval_limit_bound(&limit.count).ok()?;
    let offset = match &limit.offset {
        Some(expr) => eval_limit_bound(expr).ok()?,
        None => 0,
    };
    let cap = offset.checked_add(count)?;
    // Anything that can drop or add a row between the source and the LIMIT.
    if residual_where.is_some()
        || select.distinct
        || select.having.is_some()
        || crate::window::select_has_window(select)
    {
        return None;
    }
    if select.order_by.is_empty() {
        return Some(cap);
    }
    // An ORDER BY the access path already produces.
    let order = index_order?;
    order_is_index_order(select, order, resolver).then_some(cap)
}

/// Whether an index access path over `column_offsets` already produces the
/// order the `ORDER BY` asks for.
///
/// The by-items must be a prefix of the index's columns, all ascending, over
/// a single range: the storage iterator walks encoded index keys in ascending
/// order, several ranges are walked one after another (so their concatenation
/// is not index order), and the codec's order is the collation order the sort
/// would have used.
fn order_is_index_order(
    select: &tidb_ast::SelectStmt,
    order: &IndexAccessOrder,
    resolver: &ScopeResolver<'_>,
) -> bool {
    if !order.single_range || select.order_by.len() > order.column_offsets.len() {
        return false;
    }
    let mut key_position = 0;
    for item in &select.order_by {
        // The cursor is forward-only, so a descending key is not this order.
        if item.desc {
            return false;
        }
        let Ok(Expression::Column(column)) = rewrite_expr_resolved(&item.expr, resolver) else {
            return false;
        };
        let Some(wanted_offset) = usize::try_from(column.index).ok() else {
            return false;
        };
        loop {
            let Some(offset) = order.column_offsets.get(key_position) else {
                return false;
            };
            // Case 1 precedes Case 2 in Go: ORDER BY the fixed column itself
            // still matches that key part. Only a different requested column
            // causes the fixed key part to be skipped.
            if *offset == wanted_offset {
                key_position += 1;
                break;
            }
            if order
                .constant_positions
                .get(key_position)
                .copied()
                .unwrap_or(false)
            {
                key_position += 1;
                continue;
            }
            return false;
        }
    }
    true
}

#[cfg(test)]
mod find_best_task_property_tests {
    use super::*;

    fn select(sql: &str) -> tidb_ast::SelectStmt {
        let statement = tidb_parser::parse(sql).expect("query parses");
        let Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("expected select")
        };
        (**select).clone()
    }

    #[test]
    fn match_property_skips_a_single_constant_index_prefix_only() {
        let columns = vec![
            ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
        ];
        let scope = PlanTrace::single_table_scope("t", None, columns);
        let resolver = ScopeResolver { scope: &scope };
        let query = select("SELECT b FROM t WHERE a = 1 ORDER BY b");
        let order_by_fixed = select("SELECT a FROM t WHERE a = 1 ORDER BY a");
        let fixed_prefix = IndexAccessOrder {
            column_offsets: vec![0, 1],
            single_range: true,
            constant_positions: vec![true, false],
        };
        assert!(order_is_index_order(&query, &fixed_prefix, &resolver));
        assert!(order_is_index_order(
            &order_by_fixed,
            &fixed_prefix,
            &resolver
        ));

        let varying_prefix = IndexAccessOrder {
            constant_positions: vec![false, false],
            ..fixed_prefix
        };
        assert!(!order_is_index_order(&query, &varying_prefix, &resolver));

        let several_ranges = IndexAccessOrder {
            single_range: false,
            constant_positions: vec![true, false],
            ..varying_prefix
        };
        assert!(!order_is_index_order(&query, &several_ranges, &resolver));
    }

    #[test]
    fn point_get_rejects_either_open_endpoint() {
        let closed = IndexRange {
            low: vec![Datum::Int(7)],
            high: vec![Datum::Int(7)],
            low_exclusive: false,
            high_exclusive: false,
        };
        assert_eq!(
            single_point_handle(std::slice::from_ref(&closed)),
            Some(TableHandle::Int(7))
        );

        let mut low_open = closed.clone();
        low_open.low_exclusive = true;
        assert_eq!(single_point_handle(&[low_open]), None);

        let mut high_open = closed;
        high_open.high_exclusive = true;
        assert_eq!(single_point_handle(&[high_open]), None);
    }
}
