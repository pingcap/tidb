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
//!    source over the narrowed path, and the `WHERE` stays in the pipeline
//!    above: these narrow the source, they never replace the filter.
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
) -> Result<Option<IndexAccessOrder>, DriverError> {
    let mut index_order: Option<IndexAccessOrder> = None;
    let Some(table) = single_kv_table(&select.from, catalog, current_db) else {
        return Ok(None);
    };
    let columns = scope.column_list();
    // Go tries the batch point get before the single one.
    if let Some(handles) = try_batch_point_get(select, &table, &columns)? {
        let exec = HandleSourceExec::new(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handles.clone(),
        );
        if let Some(trace) = trace.as_deref_mut() {
            trace.batch_point_get(source_table_name(scope, &table.name), &handles);
            // The rows are read lazily, so the count is the source's live one
            // rather than a `Vec`'s length.
            trace.set_scan_act_rows(exec.produced_rows());
        }
        *from_source = Some(Box::new(exec));
    } else
    // An index range scan, when no point get applies: the ranges replace the
    // full scan with the rows the index covers, and the WHERE stays above to
    // apply the conditions the ranges did not consume.
    if try_point_get(select, &table, &columns)?.is_none() {
        if let Some((index_id, ranges, estimate)) =
            choose_index_range_path(select, catalog, scope, &table, &columns)
        {
            let exec = IndexRangeSourceExec::new(
                ExecutorMeta::new(
                    Schema::new(source_schema_columns(&columns)),
                    0,
                    INIT_CAP,
                    MAX_CHUNK_SIZE,
                ),
                table.clone(),
                index_id,
                ranges.clone(),
            );
            index_order = Some(IndexAccessOrder {
                column_offsets: table
                    .indexes()
                    .iter()
                    .find(|index| index.id == index_id)
                    .expect("the chosen path names an index of this table")
                    .column_offsets
                    .clone(),
                single_range: ranges.len() == 1,
            });
            if let Some(trace) = trace.as_deref_mut() {
                let index = table
                    .indexes()
                    .iter()
                    .find(|index| index.id == index_id)
                    .expect("the chosen path names an index of this table");
                let index_columns: Vec<&str> = index
                    .column_offsets
                    .iter()
                    .map(|offset| columns[*offset].0.as_str())
                    .collect();
                trace.index_range_scan(
                    source_table_name(scope, &table.name),
                    &index.name,
                    &index_columns,
                    &ranges,
                    estimate,
                );
                trace.set_scan_act_rows(exec.produced_rows());
            }
            *from_source = Some(Box::new(exec));
        }
    }
    if let Some(handle) = try_point_get(select, &table, &columns)? {
        // A `None` handle is a WHERE that pins a handle no row can have: the
        // plan is a point get over an empty handle list.
        let exec = HandleSourceExec::new(
            ExecutorMeta::new(
                Schema::new(source_schema_columns(&columns)),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            table.clone(),
            handle.clone().into_iter().collect(),
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
    Ok(index_order)
}

/// The schema a fast-path source emits: the scope's columns in scope order,
/// each carrying the unique id the driver's resolver hands expressions.
fn source_schema_columns(columns: &[(String, FieldType)]) -> Vec<Column> {
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

/// Offers the source the conjuncts it can apply itself, and reports the
/// `WHERE` that must still run above it (`None`: the source took all of it).
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
) -> Option<tidb_ast::Expr> {
    match (&select.where_clause, scope.tables.len()) {
        (Some(predicate), 1) => {
            let (pushed, residual) = split_scan_predicates(predicate, &scope_resolver(scope));
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
                residual
            } else {
                Some(predicate.clone())
            }
        }
        (where_clause, _) => where_clause.clone(),
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
pub(crate) fn choose_index_range_path(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    scope: &FromScope,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Option<(i64, Vec<IndexRange>, crate::access_cost::ScanEstimate)> {
    let where_clause = select.where_clause.as_ref()?;
    // The columns the statement reads, which decides whether an index path
    // covers (Go `isCoveringIndex`) and therefore whether it pays for a
    // double read. A statement outside the pruner's slice reads everything.
    let needed: Vec<usize> = crate::column_prune::prunable_columns(select, scope)
        .unwrap_or_else(|| (0..columns.len()).collect());
    let resolver = ScopeResolver { scope };
    // The `LIMIT` an index path may be costed under. `scan_limit_cap`'s own
    // refusals for things between the source and the LIMIT apply here too;
    // the residual `WHERE` is the one it cannot know yet, because which
    // conjuncts the source accepts is settled after the path is chosen. Go
    // has the same ordering and resolves it through the physical property.
    let cap = costing_limit_cap(select);
    let satisfied_by = |offsets: &[usize], single_range: bool| {
        select.order_by.is_empty() || order_is_index_order(select, offsets, single_range, &resolver)
    };
    let limit = cap.map(|cap| crate::access_cost::PushedLimit {
        cap,
        satisfied_by: &satisfied_by,
    });
    let paths = crate::access_cost::enumerate_paths(
        table,
        columns,
        Some(where_clause),
        &needed,
        &resolver,
        limit.as_ref(),
        catalog.table_statistics(table.table_id).map(AsRef::as_ref),
    );
    let best = crate::access_cost::choose_access_path(paths)?;
    let (index_id, ranges) = best.index?;
    Some((index_id, ranges, best.estimate))
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
        TableEntry::Mem(_) | TableEntry::View(_) => None,
    };
    match stats {
        // The row count is real whenever a `mysql.stats_meta` row exists,
        // even when no histogram was ever analyzed -- and in that state Go
        // prints the real count AND `stats:pseudo`.
        Some(stats) => crate::access_cost::ScanEstimate {
            rows: stats.row_count.max(0) as f64,
            pseudo: stats.pseudo,
        },
        None => crate::access_cost::ScanEstimate::pseudo(crate::plan_trace::PSEUDO_ROW_COUNT),
    }
}

/// `cardinality.Selectivity` for a single base table's `WHERE`, when that
/// table has loaded statistics; `None` leaves the stats-less rates in force.
///
/// This is what makes a `Selection` over a full scan print the estRows Go
/// prints, instead of the pseudo `0.8`/`1/3`/`1/1000` rates.
pub(crate) fn stats_selectivity(
    catalog: &Catalog,
    table: &KvTable,
    scope: &FromScope,
    where_clause: Option<&tidb_ast::Expr>,
) -> Option<f64> {
    let predicate = where_clause?;
    let stats = catalog.table_statistics(table.table_id)?;
    if stats.pseudo {
        // A table with a row count but no histograms falls back to the same
        // pseudo rates an unanalyzed one uses; saying so with `None` keeps
        // that in one place.
        return None;
    }
    Some(crate::access_cost::selectivity(
        predicate,
        table,
        &scope_resolver(scope),
        Some(stats.as_ref()),
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

/// Splits a `WHERE` over one base table into the conjuncts the scan can apply
/// itself and the predicate that must stay above it.
///
/// This is Go's `rule_predicate_push_down` split narrowed to the shape the
/// bounded TiKV Selection lowering already speaks -- see
/// [`crate::scan_pushdown`] for the rule and for why the pushed half may be
/// removed from the `Selection` only when the source promises to apply it to
/// every row, staged writes included.
///
/// The residual is the remaining conjuncts re-joined with `AND` in their
/// original order, so what runs above the scan is the `WHERE` minus exactly
/// what moved into it. `None` means every conjunct was pushed.
pub(crate) fn split_scan_predicates(
    where_clause: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
) -> (PushedScanFilter, Option<tidb_ast::Expr>) {
    let mut conjuncts = Vec::new();
    collect_conjuncts(where_clause, &mut conjuncts);
    let mut comparisons = Vec::new();
    let mut filters = Vec::new();
    let mut residual: Vec<&tidb_ast::Expr> = Vec::new();
    for conjunct in conjuncts {
        match scan_comparison(conjunct, resolver).and_then(|comparison| {
            Some((comparison, rewrite_expr_resolved(conjunct, resolver).ok()?))
        }) {
            Some((comparison, filter)) => {
                comparisons.push(comparison);
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
    (PushedScanFilter::new(comparisons, filters), residual)
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
    let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(value, &NoResolver) else {
        return None;
    };
    let literal = constant.eval().ok()?;
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
pub(crate) fn single_kv_table(
    from: &Option<tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let join = from.as_ref()?;
    if join.right.is_some() {
        return None;
    }
    let JoinNode::Table(table_ref) = &join.left else {
        return None;
    };
    let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
    match catalog.get_in(database, name)? {
        TableEntry::Kv(kv) => Some(kv.clone()),
        // A view stores no rows, so there is no point get to try.
        TableEntry::Mem(_) | TableEntry::View(_) => None,
    }
}

/// Go `tryWhereIn2BatchPointGet`: a single-table `SELECT` whose whole `WHERE`
/// is `column IN (constants)` over the handle or a single-column unique index
/// reads those rows directly instead of scanning.
///
/// Go rejects the fast plan when `ORDER BY`, `GROUP BY`, `LIMIT`, `HAVING`,
/// `DISTINCT` or a window spec is present, when the `IN` is negated, and when
/// its list is empty. The handle path applies when the table's primary key IS
/// the handle and the column names it; otherwise a unique index whose only
/// column it is.
///
/// DEFERRED (documented): Go's row form, `(a, b) IN ((1, 2), (3, 4))`, which
/// needs multi-column key lookup.
pub(crate) fn try_batch_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Result<Option<Vec<TableHandle>>, DriverError> {
    if select.having.is_some()
        || !select.order_by.is_empty()
        || !select.group_by.is_empty()
        || select.limit.is_some()
        || select.distinct
    {
        return Ok(None);
    }
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
    let tidb_ast::Expr::Column(path) = &**expr else {
        return Ok(None);
    };
    let Some(name) = path.last() else {
        return Ok(None);
    };

    // Every list element must be a constant, or this is not a point plan.
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let Ok(Expression::Constant(constant)) = rewrite_expr_resolved(item, &NoResolver) else {
            return Ok(None);
        };
        let Ok(value) = constant.eval() else {
            return Ok(None);
        };
        values.push(value);
    }

    // The handle path.
    if let Some(offset) = table.pk_handle_offset() {
        if columns[offset].0.eq_ignore_ascii_case(name) {
            let mut handles = Vec::with_capacity(values.len());
            for value in &values {
                match value {
                    Datum::Int(v) => handles.push(TableHandle::Int(*v)),
                    Datum::UInt(v) => handles.push(TableHandle::Int(*v as i64)),
                    // A non-integer constant names no integer handle, so it
                    // simply matches nothing.
                    _ => {}
                }
            }
            return Ok(Some(handles));
        }
    }

    // The unique-index path.
    let mut table = table.clone();
    for index in table.indexes().to_vec() {
        if !index.unique || index.column_offsets.len() != 1 {
            continue;
        }
        if !columns[index.column_offsets[0]]
            .0
            .eq_ignore_ascii_case(name)
        {
            continue;
        }
        let mut handles = Vec::new();
        for value in &values {
            if let Some(handle) = table
                .lookup_unique(index.id, std::slice::from_ref(value))
                .map_err(|e| DriverError::Parse(format!("index lookup failed: {e:?}")))?
            {
                handles.push(handle);
            }
        }
        return Ok(Some(handles));
    }
    Ok(None)
}

/// One `column = constant` equality from a `WHERE`, Go's `nameValuePair`.
struct NameValuePair {
    column: String,
    value: Datum,
}

/// Go `getNameValuePairs`: flattens a `WHERE` that is a conjunction of
/// `column = constant` equalities into pairs, returning `None` for any other
/// shape.
///
/// Go accepts the constant on either side of the `=`, and recurses only
/// through `AND`; anything else (an `OR`, a comparison, a function call)
/// makes the statement ineligible for a point get, which is what returning
/// `None` means here.
fn name_value_pairs(expr: &tidb_ast::Expr, pairs: &mut Vec<NameValuePair>) -> bool {
    use tidb_ast::{BinaryOp, Expr};
    match expr {
        Expr::Paren(inner) => name_value_pairs(inner, pairs),
        Expr::Binary(BinaryOp::LogicAnd, lhs, rhs) => {
            name_value_pairs(lhs, pairs) && name_value_pairs(rhs, pairs)
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
            let Ok(value) = rewrite_expr_resolved(value, &NoResolver) else {
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

/// The row a point get reads, when the statement qualifies for one.
///
/// Go `TryFastPlan`/`tryPointGetPlan`: a single-table `SELECT` with no
/// `HAVING` and no `ORDER BY`, whose `WHERE` is a conjunction of equalities
/// that pins either the handle or every column of a unique index, reads one
/// row directly instead of scanning. `LIMIT` is allowed only when it cannot
/// remove the row (`count > 0` and `offset == 0`), matching Go's check.
///
/// Returns `Ok(None)` when the statement does not qualify, so the caller
/// falls back to the ordinary scan.
pub(crate) fn try_point_get(
    select: &tidb_ast::SelectStmt,
    table: &KvTable,
    columns: &[(String, FieldType)],
) -> Result<Option<Option<TableHandle>>, DriverError> {
    if select.having.is_some() || !select.order_by.is_empty() || !select.group_by.is_empty() {
        return Ok(None);
    }
    if let Some(limit) = &select.limit {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        if count == 0 || offset > 0 {
            return Ok(None);
        }
    }
    let Some(where_clause) = &select.where_clause else {
        return Ok(None);
    };
    let mut pairs = Vec::new();
    if !name_value_pairs(where_clause, &mut pairs) || pairs.is_empty() {
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
                // A non-integer constant cannot name an integer handle, so no
                // row matches rather than the plan being wrong.
                _ => None,
            }));
        }
    }

    // The unique-index path: every column of some unique index is pinned.
    let mut table = table.clone();
    for index in table.indexes().to_vec() {
        if !index.unique {
            continue;
        }
        let mut values = Vec::with_capacity(index.column_offsets.len());
        for offset in &index.column_offsets {
            let name = &columns[*offset].0;
            let Some(pair) = pairs
                .iter()
                .find(|pair| pair.column.eq_ignore_ascii_case(name))
            else {
                values.clear();
                break;
            };
            values.push(pair.value.clone());
        }
        if values.len() != index.column_offsets.len() {
            continue;
        }
        let handle = table
            .lookup_unique(index.id, &values)
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
    order_is_index_order(select, &order.column_offsets, order.single_range, resolver).then_some(cap)
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
    column_offsets: &[usize],
    single_range: bool,
    resolver: &ScopeResolver<'_>,
) -> bool {
    if !single_range || select.order_by.len() > column_offsets.len() {
        return false;
    }
    select
        .order_by
        .iter()
        .zip(column_offsets)
        .all(|(item, offset)| {
            // The cursor is forward-only, so a descending key is not this order.
            !item.desc
                && matches!(
                    rewrite_expr_resolved(&item.expr, resolver),
                    Ok(Expression::Column(column))
                        if usize::try_from(column.index).ok() == Some(*offset)
                )
        })
}
