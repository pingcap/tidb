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

//! `FROM`-clause plan construction: base tables, joins, derived tables,
//! lateral joins and views.
//!
//! Mirrors Go's `PlanBuilder.buildResultSetNode` family (`planner/core/
//! logical_plan_builder.go`): each `ResultSetNode` shape becomes an executor
//! plus the [`FromScope`] that names its output columns for the rewriter.

use super::*;
use crate::cte_storage::CteTableSourceExec;

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
///
/// `NATURAL`/`USING` coalescing does not change that row layout at all -- it
/// is expressed here, as the two pieces of naming a coalesced join adds on
/// top of it (see [`coalesce_common_columns`]).
#[derive(Clone, Debug)]
pub(crate) struct FromScope {
    pub(crate) tables: Vec<FromTable>,
    /// The statement's session `time_zone`, which [`ScopeResolver`] publishes
    /// to the expression rewriter as [`ColumnResolver::time_zone`] -- Go's
    /// `ctx.Location()`, reached while BUILDING an expression (the
    /// `TIMESTAMP 'lit'` fold rounds and offset-normalizes in it). It rides
    /// on the scope because the scope is the one value every rewrite over
    /// this `FROM` already receives; the statement build points set it from
    /// `StmtContext::session_zone` and every derived scope clones it along.
    pub(crate) zone: tidb_expr::SessionTimeZone,
    /// Result width of this statement's configured `TIDB_VERSION()` value.
    pub(crate) tidb_info_len: usize,
    /// The row offsets a `NATURAL`/`USING` join coalesced AWAY: the inner
    /// side's copy of each common column, which stays reachable through its
    /// own table's qualifier (`SELECT u2.id`) but is invisible to `*` and to
    /// an unqualified name -- Go's `FullNames[...].Redundant`. Empty for
    /// every other `FROM` shape, which is what makes this a no-op there.
    pub(crate) coalesced: Vec<usize>,
    /// The row offsets `*` expands to, in order, when that is NOT plain row
    /// order: a coalesced join puts the common columns first and a RIGHT
    /// join reports its two sides right-then-left. Empty means row order.
    pub(crate) star: Vec<usize>,
    /// Whether `t.*` must expand against the VISIBLE columns rather than the
    /// table's own -- Go's `unfoldWildStar` falling through to
    /// `p.OutputNames()` because its type switch found no `LogicalJoin`.
    ///
    /// `buildJoin` returns a `LogicalSelection` WRAPPING the join whenever an
    /// INNER join carries an `ON` clause:
    ///
    /// ```go
    /// // Keep these expressions as a LogicalSelection upon the inner join, in
    /// // order to apply possible decorrelate optimizations. The ON clause is
    /// // actually treated as a WHERE clause now.
    /// if joinPlan.JoinType == base.InnerJoin {
    ///     sel := logicalop.LogicalSelection{Conditions: onCondition}.Init(...)
    ///     sel.SetChildren(joinPlan)
    ///     return sel, nil
    /// }
    /// ```
    ///
    /// so the `FullSchema` that holds a coalesced join's REDUNDANT copies is
    /// not reachable from the top of the `FROM` plan, and `t.*` sees only the
    /// coalesced output names. The observable effect is a column that
    /// disappears from the result, measured through `gorun`:
    ///
    /// ```text
    /// select s2.* from s1 join s2 using(a) join s3 on(s2.a=s3.a);  -- 20
    /// select s2.* from s1 join s2 using(a) left join s3 on(...);   -- 1|20
    /// select s2.* from s1 join s2 using(a), s3;                    -- 1|20
    /// select s2.* from s1 join s2 using(a);                        -- 1|20
    /// select s2.a from s1 join s2 using(a) join s3 on(s2.a=s3.a);  -- 1
    /// ```
    ///
    /// Only the OUTERMOST join decides it, which is why `build_join` sets it
    /// unconditionally rather than merging a child's answer in: an inner join
    /// with `ON` above a `USING` join hides the copies, and a `USING` join
    /// above an inner join with `ON` does not.
    ///
    /// The flag is inert wherever nothing was coalesced -- with no redundant
    /// copy the two expansions are the same list -- which is every `FROM`
    /// shape but this one.
    pub(crate) qualified_star_is_output_only: bool,
}

impl Default for FromScope {
    /// An empty scope in UTC -- the same zone a fresh session's
    /// `StmtContext` answers before any `SET time_zone`. Statement build
    /// points that HAVE a context overwrite the zone from it; only tests and
    /// scopes for statements that never fold a temporal literal rely on this
    /// default.
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            coalesced: Vec::new(),
            star: Vec::new(),
            qualified_star_is_output_only: false,
            zone: tidb_expr::SessionTimeZone::utc(),
            tidb_info_len: tidb_util::printer::get_tidb_info(
                &tidb_util::versioninfo::VersionInfo::build_default(),
            )
            .len(),
        }
    }
}

impl FromScope {
    /// Every column of the scope in row order.
    pub(crate) fn column_list(&self) -> Vec<(String, FieldType)> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().cloned())
            .collect()
    }

    pub(crate) fn width(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }

    /// The column at a row offset, with the name it answers to.
    pub(crate) fn column_at(&self, offset: usize) -> Option<&(String, FieldType)> {
        self.tables
            .iter()
            .find(|t| (t.offset..t.offset + t.columns.len()).contains(&offset))
            .and_then(|t| t.columns.get(offset - t.offset))
    }

    /// How a row offset is written when it must be named unambiguously:
    /// `table.column`, the form a coalesced join's synthesized equality uses
    /// to reach the side it means.
    pub(crate) fn qualified_path(&self, offset: usize) -> Option<Vec<String>> {
        let table = self
            .tables
            .iter()
            .find(|t| (t.offset..t.offset + t.columns.len()).contains(&offset))?;
        let (name, _) = table.columns.get(offset - table.offset)?;
        Some(vec![table.name.clone(), name.clone()])
    }

    /// Every column an unqualified `*` expands to, in display order (Go's
    /// `unfoldWildStar` over the join's own output names).
    pub(crate) fn star_columns(&self) -> Vec<(usize, String, FieldType)> {
        let offsets: Vec<usize> = if self.star.is_empty() {
            (0..self.width()).collect()
        } else {
            self.star.clone()
        };
        offsets
            .into_iter()
            .filter_map(|offset| {
                self.column_at(offset)
                    .map(|(name, ft)| (offset, name.clone(), ft.clone()))
            })
            .collect()
    }
}

/// Resolves a column reference against the joined `FROM` scope.
///
/// A qualified `t.a` binds to table `t`'s column; an unqualified `a` binds to
/// the one table that has such a column, and is rejected as ambiguous when
/// several do -- MySQL's `ERROR 1052 (23000): Column 'a' in field list is
/// ambiguous`, which Go raises from `expression.buildColumn`.
///
/// A column a `NATURAL`/`USING` join coalesced away ([`FromScope::coalesced`])
/// is skipped by the unqualified lookup and only by it: that is exactly what
/// makes `id` unambiguous after `u1 JOIN u2 USING (id)` while `u2.id` still
/// names the right side's own value (captured from Go, which reports the pair
/// as two distinct columns for `SELECT u1.id, u2.id`).
pub(crate) struct ScopeResolver<'a> {
    pub(crate) scope: &'a FromScope,
}

impl ScopeResolver<'_> {
    /// Go `expression.ColumnFullName(db, table, column)` -- the `OrigName` a
    /// resolved column carries and the only text the 1260 `GROUP_CONCAT`
    /// truncation message renders. The rewritten `Expression` keeps only an
    /// index and a unique id, so the name has to be read here, where the
    /// scope still knows which table the reference bound to.
    pub(crate) fn orig_name(&self, path: &[String]) -> Option<String> {
        let (index, _, _) = self.resolve(path)?;
        let table =
            self.scope.tables.iter().find(|table| {
                (table.offset..table.offset + table.columns.len()).contains(&index)
            })?;
        let column = table.columns.get(index - table.offset)?;
        let database = table.database.as_deref()?;
        Some(format!("{database}.{}.{}", table.name, column.0))
    }
}

/// A resolver over `scope`, for the modules that build their own expressions.
pub(crate) fn scope_resolver(scope: &FromScope) -> impl ColumnResolver + '_ {
    ScopeResolver { scope }
}

impl ColumnResolver for ScopeResolver<'_> {
    /// The zone the scope's build point took from the statement's
    /// `StmtContext` -- see [`FromScope::zone`].
    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
        self.scope.zone.clone()
    }

    fn tidb_info_len(&self) -> usize {
        self.scope.tidb_info_len
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (schema, qualifier, name) = match path {
            [name] => (None, None, name),
            [table, name] => (None, Some(table), name),
            // `db.t.a` is how a view's stored definition names its columns.
            [schema, table, name] => (Some(schema), Some(table), name),
            _ => return None,
        };
        let mut found: Option<(usize, FieldType)> = None;
        for table in &self.scope.tables {
            if let Some(q) = qualifier {
                if !q.eq_ignore_ascii_case(&table.name) {
                    continue;
                }
            }
            if let Some(schema) = schema {
                // An aliased or synthetic source carries no schema, so a
                // schema-qualified reference cannot name it.
                match &table.database {
                    Some(db) if db.eq_ignore_ascii_case(schema) => {}
                    _ => continue,
                }
            }
            for (i, (candidate, ft)) in table.columns.iter().enumerate() {
                if candidate.eq_ignore_ascii_case(name) {
                    if qualifier.is_none() && self.scope.coalesced.contains(&(table.offset + i)) {
                        continue;
                    }
                    if found.is_some() {
                        // Ambiguous across tables: MySQL errors rather than
                        // picking one.
                        return None;
                    }
                    found = Some((table.offset + i, ft.clone()));
                }
            }
        }
        let (index, ft) = found?;
        Some((index, ft, (index + 1) as i64))
    }
}

/// Go `DataSource.ExtractFD`'s per-table facts, for
/// [`FromTable::func_deps`].
///
/// The primary key -- in either clustered shape, since a `PKIsHandle` table
/// keeps its single integer key as the row handle and a common handle keeps
/// the composite one there -- and every UNIQUE index, split by strength: one
/// with a nullable column is a LAX key rather than no key at all, which a
/// `WHERE` proving its nullable members non-null promotes.
/// Generated columns are added as Go adds them -- one level each, letting the
/// graph chain `c AS (a+2)`, `d AS (c+2)` into `{a} --> {d}`.
fn table_func_deps(
    entry: &TableEntry,
    columns: &[(String, FieldType)],
) -> crate::driver::funcdep::TableFuncDeps {
    const NOT_NULL_FLAG: u32 = 1;
    let mut deps = crate::driver::funcdep::TableFuncDeps::default();
    let TableEntry::Kv(kv) = entry else {
        return deps;
    };
    let not_null = |offsets: &[usize]| {
        offsets.iter().all(|&offset| {
            columns
                .get(offset)
                .is_some_and(|(_, ft)| ft.flags() & NOT_NULL_FLAG != 0)
        })
    };
    let in_scope = |offsets: &[usize]| offsets.iter().all(|&offset| offset < columns.len());

    let handle_key = kv
        .pk_handle_offset()
        .map(|offset| vec![offset])
        .unwrap_or_else(|| kv.common_handle_offsets().to_vec());
    for key in std::iter::once(handle_key).chain(
        kv.indexes()
            .iter()
            .filter(|index| index.unique)
            .map(|index| index.column_offsets.clone()),
    ) {
        if key.is_empty() || !in_scope(&key) {
            continue;
        }
        if not_null(&key) {
            deps.strict_keys.push(key);
        } else {
            deps.lax_keys.push(key);
        }
    }
    for (offset, column) in kv.columns.iter().enumerate() {
        if offset >= columns.len() {
            break;
        }
        if let Some(generation) = crate::generated_column::GeneratedColumnSlot::generation(column) {
            // Unresolvable here means a catalog bug (DDL refuses to drop or
            // rename a column a generated expression reads), and the fallback
            // -- no functional dependency for this column -- would only cost
            // an optimization, so it stays. It is asserted rather than
            // ignored so the bug surfaces in the tier that CAN see it.
            let resolved =
                crate::generated_column::dependency_offsets(&kv.columns, &generation.dependencies);
            debug_assert!(
                resolved.is_ok(),
                "generated column `{}` reads `{}`, which the table does not define",
                kv.columns[offset].name,
                resolved.as_ref().unwrap_err(),
            );
            let dependencies = resolved.unwrap_or_default();
            if !dependencies.is_empty() && in_scope(&dependencies) {
                deps.generated.push((dependencies, offset));
            }
        }
        if column.field_type.flags() & NOT_NULL_FLAG != 0 {
            deps.not_null.push(offset);
        }
    }
    deps
}

/// `enumerateIndexJoinByOuterIdx`'s property split, decided from the required
/// columns alone: the side that holds every one of them takes `prop`, the
/// other takes the empty property.
///
/// `left_width` is where the right child's columns start in the joined row,
/// or `None` when this tier could not describe the two sides -- in which case
/// neither can be named the outer one and both are asked for nothing.
fn index_join_child_props(
    required: &tidb_planner::physical_property::PhysicalProperty,
    left_width: Option<usize>,
) -> (
    tidb_planner::physical_property::PhysicalProperty,
    tidb_planner::physical_property::PhysicalProperty,
) {
    let empty = tidb_planner::physical_property::PhysicalProperty::default;
    let Some(left_width) = left_width.filter(|_| !required.is_sort_item_empty()) else {
        return (empty(), empty());
    };
    let (all_same, desc) = required.all_same_order();
    if !all_same {
        return (empty(), empty());
    }
    let cols: Vec<usize> = required
        .sort_items
        .iter()
        .map(|item| item.col as usize)
        .collect();
    if cols.iter().all(|col| *col < left_width) {
        (
            crate::driver::merge_decision::child_required_prop(cols.into_iter(), desc),
            empty(),
        )
    } else if cols.iter().all(|col| *col >= left_width) {
        (
            empty(),
            crate::driver::merge_decision::child_required_prop(
                cols.into_iter().map(|col| col - left_width),
                desc,
            ),
        )
    } else {
        // The order straddles the join: no side can provide it alone, which
        // is exactly what `AllColsFromSchema` refuses for both candidates.
        (empty(), empty())
    }
}

/// The column orders an executor a `FROM` builder just built ACTUALLY
/// produces, as offsets into its own row -- Go's `PossiblePropertiesInfo
/// .Orders` read off the PHYSICAL plan instead of the logical one.
///
/// This is the VERIFY half of the promise/verify contract; the PROMISE half is
/// [`crate::driver::merge_decision::possible_properties`]. See that module's
/// doc for why the two exist and what the narrowing they replaced cost.
pub(crate) type Delivered = Vec<Vec<usize>>;

/// Whether a base-table leaf can answer `required` with the order it reads in
/// anyway, which is the only way this tier produces an order at all -- there
/// is no `Sort` enforcer below a `FROM`.
fn leaf_can_keep_order(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> bool {
    let JoinNode::Table(table_ref) = node else {
        // Only the table arm reads the answer; a join re-decides for itself
        // and a derived table is materialized.
        return true;
    };
    if !table_ref.partitions.is_empty() || table_ref.as_of.is_some() {
        return false;
    }
    // A forward scan walks ascending. Go reaches a DESCENDING order by setting
    // `PhysicalTableScan.Desc` and reading the range backwards, which this
    // tier's scan does not do, so a descending demand is declined rather than
    // answered with the ascending stream.
    let (all_same, desc) = required.all_same_order();
    if !all_same || desc {
        return false;
    }
    let Ok((database, name)) = split_table_path(&table_ref.name, current_db) else {
        return false;
    };
    let Some(entry) = catalog.get_in(database, name) else {
        return false;
    };
    let columns: Vec<String> = entry.column_list().into_iter().map(|(c, _)| c).collect();
    let wanted: Vec<usize> = required
        .sort_items
        .iter()
        .map(|item| item.col as usize)
        .collect();
    crate::driver::merge_decision::delivers(
        &crate::driver::merge_decision::table_orders(entry, &columns),
        &wanted,
    )
}

/// Builds the `FROM` scope and the executor that produces its rows.
///
/// Go's `buildJoin` builds a left-deep tree of `LogicalJoin`s over the
/// `FROM` list; this walks the same tree, so `a JOIN b JOIN c` nests as
/// `(a JOIN b) JOIN c` and the row layout is `a`'s columns, then `b`'s, then
/// `c`'s.
///
/// A `LATERAL` derived table on the right of a join is not a join at all but
/// an Apply, so it leaves this path early -- see `build_lateral_join`.
///
/// DEFERRED (documented): `USING`, `NATURAL`, and `STRAIGHT_JOIN`'s ordering
/// guarantee.
///
/// `trace` records the operator each branch commits to, so `EXPLAIN` prints
/// the FROM clause the driver actually built rather than a second guess at
/// it. A shape the recorder has never printed (a derived table, a lateral
/// join) marks the trace refused instead of inventing a node for it.
pub(crate) fn build_from(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    // Go's `findBestTask(prop)` asks a child for a plan that SATISFIES the
    // property and lets the child answer with the path that does. This tier
    // cannot re-plan a built child, so it answers the same question from the
    // other end: the property arrives as a REQUEST, the builder takes the path
    // that would satisfy it when one exists, and the third return value says
    // what the executor it built actually delivers. `build_join` reads that
    // answer back before it commits to a merge join -- the VERIFY half of the
    // contract in `merge_decision`'s module doc.
    //
    // `keep_order` is therefore a request the leaf may DECLINE: a demand this
    // table cannot produce (no clustered integer handle, a partitioned read, a
    // descending order no forward scan walks) leaves the scan free to take its
    // cheapest path, and `EXPLAIN` prints `keep order:false` because that is
    // what it does.
    let keep_order =
        required.need_keep_order() && leaf_can_keep_order(node, catalog, current_db, required);
    match node {
        JoinNode::Table(table_ref) => {
            // A `db.t` reference resolves in that schema; a bare `t` resolves
            // in the session's current one (Go's name resolution).
            let (database, name) = split_table_path(&table_ref.name, current_db)?;
            // `t AS OF TIMESTAMP <expr>` pins a HISTORICAL read. Go resolves
            // the expression to a timestamp and reads the MVCC version at it
            // (`CalculateAsOfTsExpr` -> `StalenessTxnContextProvider`); this
            // tier's store keeps no history, so answering from the present
            // under a historical name would be undetectable. Refuse it, the
            // same way `tidb-planner`'s bounded scan already does
            // (`UnsupportedReadOnlyFeature::StaleRead`).
            if table_ref.as_of.is_some() {
                return Err(DriverError::unsupported(
                    "AS OF TIMESTAMP is not supported yet",
                ));
            }
            // Go reaches `convertToSampleTable` whenever `ds.SampleInfo` is
            // present. This tier has no TiKV region model and therefore
            // cannot reproduce that operator. The clause is parsed, so
            // ignoring it would execute an ordinary scan and silently return
            // the whole table under a sampling contract. Refuse at the
            // construction boundary instead.
            if table_ref.sample.is_some() {
                return Err(DriverError::unsupported("TABLESAMPLE is not supported yet"));
            }
            let entry = catalog
                .get_in(database, name)
                .ok_or(DriverError::unsupported("table not found in catalog"))?;
            // A table alias replaces the name for qualification, as in Go.
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            // Every base-table read starts as a whole-table scan; the fast
            // paths in `run_select_stmt` REPLACE this node at the same
            // moment they replace the executor it describes.
            if let Some(trace) = trace.as_deref_mut() {
                // Go plans a table in a memory schema as a `PhysicalMemTable`
                // rather than costing any access path over it
                // (`find_best_task.go`'s `metadef.IsMemDB` check), and names
                // it by the DECLARED name rather than by the written one.
                if crate::infoschema_meta::is_information_schema(database) {
                    trace.mem_table_scan(&declared_table_name(entry, name));
                } else {
                    trace.table_full_scan(&visible, full_scan_estimate(catalog, entry), keep_order);
                }
            }
            if let TableEntry::View(view) = entry {
                let (exec, scope) = build_view_source(
                    view,
                    database,
                    name,
                    visible,
                    table_ref.alias.is_none(),
                    catalog,
                    ctx,
                )?;
                // A view's body is a whole statement, whose row order this
                // tier does not describe.
                return Ok((meter(exec, trace), scope, Delivered::new()));
            }
            // A sequence is in the table namespace but is not a row source.
            // Go refuses it in the planner -- captured: `select * from s1` is
            // `[planner:1051] Unknown table ''`, with the name genuinely
            // empty in TiDB's own message. That exact wording is not
            // reproduced here (an empty name would say less than the truth);
            // what matters is that the read fails rather than reporting a
            // zero-column row source.
            if entry.is_sequence() {
                return Err(DriverError::unsupported(
                    "a sequence is not a row source; read it with NEXTVAL",
                ));
            }
            let columns = entry.column_list();
            // The leaf's row layout by name, which is the identity
            // `merge_decision`'s orders are written in.
            let column_names: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let schema = Schema::new(schema_columns);
            // Which of the two base-table branches below actually ran, read
            // by the DELIVERY report at the end of this arm. It is set where
            // the index source is built rather than predicted from
            // `keep_order`, so the report cannot drift from the build.
            // The order the branch that RAN delivers, in this leaf's own row
            // offsets. Empty until an index branch fills it in; the table
            // branch's answer is read off the catalog at the end of the arm.
            let mut walked_index: Option<Vec<usize>> = None;
            let exec: Box<dyn Executor> = match entry {
                TableEntry::Mem(mem) => Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    mem.rows.clone(),
                )),
                TableEntry::Cte(cte) => Box::new(CteTableSourceExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    cte.clone(),
                )),
                // Go's `findBestTask` costs an access path for EVERY
                // `DataSource` of the tree, this leaf included, and answers
                // its parent with the cheapest. `leaf_index_path` is that
                // costing. It is offered no condition, so the only path it
                // can return is a WHOLE covering index -- the same rows the
                // scan below reads, through a narrower structure.
                //
                // `keep_order` no longer DELETES that costing. It narrows it:
                // Go's `findBestTask` under a NON-EMPTY property enumerates
                // the same paths and drops the ones `matchProperty` says do
                // not already walk in the required order, so an ordered
                // parent gets the cheapest ORDERED index instead of no index
                // at all. `wanted_order` below is that property.
                //
                // One refusal is left, and it is not about order:
                // `demand.columns == None` is a caller with no statement
                // above the `FROM`, and a single base table -- which
                // `driver::access::commit_fast_path_source` costs WITH its
                // `WHERE`, so a second, condition-blind choice here would
                // only be the worse of the two.
                TableEntry::Kv(kv) => {
                    let wanted_order: Option<Vec<usize>> = keep_order.then(|| {
                        required
                            .sort_items
                            .iter()
                            .map(|item| item.col as usize)
                            .collect()
                    });
                    match demand.columns.and_then(|wanted| {
                        let hints = crate::index_hints::table_ref_hints(table_ref, kv).ok()?;
                        crate::driver::leaf_access::leaf_index_path(
                            kv,
                            &visible,
                            &columns,
                            wanted,
                            &hints,
                            catalog,
                            &ctx.session_zone(),
                            wanted_order.as_deref(),
                        )
                    }) {
                        Some(path) => {
                            walked_index = Some(path.order().to_vec());
                            crate::driver::leaf_access::leaf_index_source(
                                kv,
                                &visible,
                                &columns,
                                path,
                                trace.as_deref_mut(),
                                ctx,
                            )
                        }
                        None => Box::new(TableScanExec::new_with_context(
                            ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                            restricted_to_partitions(kv, &table_ref.partitions, name)?,
                            crate::kv_table::RowDecodeContext::for_query(ctx),
                            // The one production build site: the statement's own
                            // `DAGRequest.flags` and its warning sink, taken
                            // together from the context that decided both.
                            crate::remote_scan::PushdownStatementContext::from_stmt(ctx),
                        )),
                    }
                }
                // Handled above, before the columns were taken.
                TableEntry::View(_) | TableEntry::Sequence(_) => {
                    unreachable!("views and sequences take the branches above")
                }
            };
            let scope = FromScope {
                tables: vec![FromTable {
                    name: visible,
                    // An alias replaces the whole path, so `db.t.col` no
                    // longer names the table once it is aliased.
                    database: table_ref.alias.is_none().then(|| database.to_owned()),
                    func_deps: table_func_deps(entry, &columns),
                    columns,
                    offset: 0,
                }],
                zone: ctx.session_zone(),
                tidb_info_len: ctx.tidb_info_len(),
                ..FromScope::default()
            };
            // What this leaf DELIVERS -- read off the branch that RAN, which
            // is the verify half of `merge_decision`'s promise/verify
            // contract.
            //
            // A leaf that took the index branch answers in the order that
            // WALK produces, which is the index's own key order -- and only
            // because it was built with `keep order:true`, which stops the
            // source from reordering its lookup batches by handle. The order
            // is carried out of the path the chooser returned, not recomputed
            // here, so it cannot name an index the leaf did not walk.
            //
            // `keep_order` is the second gate on the table branch and not
            // `required`: a leaf that DECLINED the request took its cheapest
            // path, and reporting an order there would be the exact
            // promise-without-delivery this contract exists to refuse.
            //
            // `table_scan_orders` and NOT `table_orders`: the promise NOW
            // carries Go's index branch of `PreparePossibleProperties`, and a
            // verify side that re-read the promise would agree with itself
            // rather than checking anything. That coincidence hid a silent
            // row drop -- see `crate::merge_join_plan::table_scan_order`.
            let delivered = match &walked_index {
                Some(order) if !order.is_empty() => vec![order.clone()],
                Some(_) => Delivered::new(),
                None if keep_order => {
                    crate::driver::merge_decision::table_scan_orders(entry, &column_names)
                }
                None => Delivered::new(),
            };
            Ok((meter(exec, trace), scope, delivered))
        }
        JoinNode::Join(join) => {
            // A nested join builds full width: see `build_join`'s `prune`.
            build_join(
                join, catalog, current_db, ctx, trace, None, demand, required,
            )
        }
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            // A `LATERAL` in the LEFTMOST position has no preceding table to
            // correlate with, so it is an ordinary derived table -- the same
            // reading Go's `buildLateralJoin` reaches with an empty outer
            // schema (captured: `SELECT * FROM LATERAL (SELECT 1) x` runs).
            // Its alias column list still renames positionally.
            let _ = lateral;
            // The order a join above this derived table requires OF it is an
            // order the subquery's own leaf has to be asked for: the rows are
            // materialized in arrival order, so asking the leaf is the whole
            // of delivering it. Go's `PhysicalProjection.exhaustPhysicalPlans`
            // maps the property through the select list; see
            // `merge_decision::from_required_prop`.
            let (exec, mut scope) = build_derived_source(
                subquery,
                alias.as_deref(),
                catalog,
                current_db,
                ctx,
                trace,
                required,
            )?;
            rename_derived_columns(&mut scope.tables[0].columns, column_names)?;
            // A derived table is MATERIALIZED here -- `build_derived_source`
            // drains its subquery into a `MemTableSourceExec`, which replays
            // the rows in the order they arrived -- so what it delivers is
            // what its inner `FROM` delivered, projected through its select
            // list. `Phase::Delivered` is that walk, and it is a conservative
            // LOWER bound on the inner build (see its doc): the inner join
            // forms its candidates from the PROMISE and verifies them the same
            // way this one does, so it can only deliver more.
            let delivered = crate::driver::merge_decision::delivered_properties(
                node,
                catalog,
                current_db,
                demand.offered,
            )
            .map(|properties| properties.orders)
            .unwrap_or_default();
            Ok((exec, scope, delivered))
        }
    }
}

/// Runs a derived table's subquery and presents its rows as a `FROM` source.
///
/// Go plans the subquery and wraps it in a `LogicalProjection` the outer query
/// reads by alias; the rows are materialized here instead, which is the same
/// result for a reader. The derived table's column names are the subquery's
/// own result-field names, and only the alias qualifies them -- `db.alias.col`
/// and a base table's name are both Go's `ErrUnknownColumn` once the subquery
/// is behind an alias.
///
/// A `trace` descends INTO the subquery rather than stopping at it. That is
/// what Go's plan text does too: a derived table is not an operator in Go's
/// output, it is the subquery's own plan subtree standing where the `FROM`
/// entry was (captured: `explain select * from (select * from t) x` prints
/// exactly `TableReader_6 -> TableFullScan_5` over `table:t`, with no node
/// naming `x`). Recording the subtree is therefore the faithful description,
/// and the derived table stops being a shape the recorder has to know about.
///
/// A plan-only trace also means the subquery is PLANNED and not run, so an
/// `EXPLAIN` over a derived table executes nothing -- the empty row set it
/// hands back is never drained (see `run_select_traced`'s plan-only return).
pub(crate) fn build_derived_source(
    subquery: &QueryStmt,
    alias: Option<&str>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let (alias, columns, rows) =
        derived_source_relation(subquery, alias, catalog, current_db, ctx, trace, required)?;
    let schema_columns: Vec<Column> = columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let exec: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
        rows,
    ));
    let scope = FromScope {
        tables: vec![FromTable {
            name: alias.to_owned(),
            // An alias is the only qualifier a derived table answers to.
            database: None,
            columns,
            offset: 0,
            func_deps: Default::default(),
        }],
        zone: ctx.session_zone(),
        tidb_info_len: ctx.tidb_info_len(),
        ..FromScope::default()
    };
    Ok((exec, scope))
}

/// A derived table's MATERIALIZED relation: the alias it answers to, its
/// column list and its rows.
///
/// Split out of [`build_derived_source`] because a multi-table write's `FROM`
/// needs the rows themselves rather than an executor over them (see
/// `multi_dml`, whose joined row carries a per-table row identity beside the
/// values). Both callers therefore apply ONE reading of the two rules a
/// derived table's NAME and SHAPE must satisfy -- Go's
/// `ErrDerivedMustHaveAlias` (1248) and `ErrDupFieldName` (1060) -- instead of
/// two that can drift.
type DerivedSourceRelation<'a> = (&'a str, Vec<(String, FieldType)>, Vec<Vec<Datum>>);

pub(crate) fn derived_source_relation<'a>(
    subquery: &QueryStmt,
    alias: Option<&'a str>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> Result<DerivedSourceRelation<'a>, DriverError> {
    // Captured from Go: an alias-less derived table is ErrDerivedMustHaveAlias
    // in a plain SELECT and in a view body alike.
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };
    let (columns, rows) = match subquery {
        QueryStmt::Select(select) => {
            run_select_traced(select, catalog, current_db, ctx, trace, required)?
        }
        QueryStmt::SetOpr(set_opr) => {
            // A set operation has no traced builder: `run_set_opr_stmt` runs
            // its arms and concatenates them without recording an operator, so
            // there is no subtree to stand here. The refusal names the arm
            // shape rather than the derived table, which IS described now.
            if let Some(trace) = trace {
                trace.refuse("a set-operation derived table's plan is not recorded yet");
            }
            run_set_opr_stmt(set_opr, catalog, current_db, ctx)?
        }
    };
    // A derived table is a named relation, so its columns must be uniquely
    // named: Go's ErrDupFieldName, which `(SELECT * FROM t JOIN s ...)` hits
    // whenever the joined tables share a column name.
    for (index, (name, _)) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateColumnName(name.clone()));
        }
    }
    Ok((alias, columns, rows))
}

/// Applies a derived table's `(c1, c2, ...)` alias column list.
///
/// The list renames the subquery's own output columns positionally, and a
/// length disagreement is Go's `ErrViewWrongList` (1353) -- captured, the same
/// error a `CREATE VIEW v (a, b) AS SELECT 1` mismatch reports.
pub(crate) fn rename_derived_columns(
    columns: &mut [(String, FieldType)],
    names: &[String],
) -> Result<(), DriverError> {
    if names.is_empty() {
        return Ok(());
    }
    if names.len() != columns.len() {
        return Err(DriverError::ViewWrongList);
    }
    for (column, name) in columns.iter_mut().zip(names) {
        column.0 = name.clone();
    }
    Ok(())
}

/// The names a `SELECT`'s fields give the relation they produce, when they can
/// be read off the statement alone.
///
/// The lateral path needs the names BEFORE it can run anything, and the run it
/// uses to settle the column TYPES has the correlated columns replaced by
/// literals -- which would rename `SELECT t.a` to the literal's own text. This
/// applies the same naming rule the plain select path uses (an alias, else a
/// column reference's bare name, else the restored expression), and gives up
/// (`None`) on a `*` field, whose width is not known from the statement.
pub(crate) fn derived_field_names(select: &tidb_ast::SelectStmt) -> Option<Vec<String>> {
    select
        .fields
        .fields()
        .iter()
        .enumerate()
        .map(|(field_index, field)| match field {
            SelectField::Expr { expr, alias } => Some(alias.clone().unwrap_or_else(|| {
                crate::driver::default_field_display_name(&select.fields, field_index, expr)
            })),
            _ => None,
        })
        .collect()
}

/// [`derived_field_names`], widened to a `QueryStmt`: a set operation's output
/// is named after its LEFTMOST `SELECT` term, the same rule Go's `buildSetOpr`
/// uses when it derives the result schema from the first child.
pub(crate) fn derived_field_names_query(query: &QueryStmt) -> Option<Vec<String>> {
    match query {
        QueryStmt::Select(select) => derived_field_names(select),
        QueryStmt::SetOpr(set_opr) => {
            let first = set_opr.terms.first()?;
            match &first.body {
                tidb_ast::SetOprTermBody::Select(select) => derived_field_names(select),
                tidb_ast::SetOprTermBody::Nested(nested) => {
                    derived_field_names_query(&QueryStmt::SetOpr(nested.clone()))
                }
            }
        }
    }
}

/// A type-carrying stand-in value for a column of type `ft`.
///
/// Type inference over the probe run needs a datum of the right KIND, and
/// nothing more -- so this is the neutral value of that kind, never a bound
/// that arithmetic in the subquery could overflow. Types with no obvious
/// neutral value fall back to NULL, which is what the probe used before types
/// were carried at all.
pub(crate) fn probe_datum(ft: &FieldType) -> Datum {
    match tidb_datatype::get_min_value(ft) {
        Datum::Int(_) => Datum::Int(0),
        Datum::UInt(_) => Datum::UInt(0),
        Datum::Real(_) => Datum::Real(0.0),
        Datum::Float32(_) => Datum::Float32(0.0),
        Datum::Decimal(_) => Datum::new_decimal(tidb_datatype::Decimal::from_signed_literal("0")),
        other => other,
    }
}

/// Builds a `LATERAL` derived table as an Apply over the tables preceding it.
///
/// Go's `buildLateralJoin` makes this a `LogicalApply` with `InnerJoin`: the
/// left side's columns are the outer schema the subquery's correlated columns
/// bind against, and the subquery is re-run per outer row. That is exactly
/// this crate's correlated-subquery machinery, with the one difference that a
/// derived table yields a RELATION rather than a scalar, so
/// [`crate::apply::LateralApplyExec`] concatenates every inner row onto the
/// outer row instead of appending one value.
///
/// A `LATERAL` over a set operation (`UNION`/`EXCEPT`/`INTERSECT`) walks the
/// same path: [`collect_correlated_columns_query`] and
/// [`bind_subquery_columns_query`] widen the collector and the binder to a
/// `QueryStmt`, so every term of the set operation is re-run per outer row
/// exactly like a lone `SELECT`'s clauses are, and the result's column names
/// come from the set operation's leftmost term (`derived_field_names_query`),
/// matching Go's `buildSetOpr`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_lateral_join(
    join: &tidb_ast::Join,
    left_exec: Box<dyn Executor>,
    left_scope: FromScope,
    subquery: &QueryStmt,
    alias: Option<&str>,
    column_names: &[String],
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    // Go's own rejections, in `buildLateralJoin`'s order.
    if join.natural {
        return Err(DriverError::InvalidLateralJoin(
            "NATURAL JOIN is not supported with LATERAL",
        ));
    }
    if !join.using.is_empty() {
        return Err(DriverError::InvalidLateralJoin(
            "USING clause is not supported with LATERAL",
        ));
    }
    match join.tp {
        tidb_ast::JoinType::Left => {
            return Err(DriverError::InvalidLateralJoin(
                "LEFT JOIN is not supported with LATERAL",
            ))
        }
        tidb_ast::JoinType::Right => {
            return Err(DriverError::InvalidLateralJoin(
                "RIGHT JOIN is not supported with LATERAL",
            ))
        }
        // Comma syntax (which the parser spells `CrossJoin`) and an explicit
        // INNER JOIN are the shapes Go plans.
        tidb_ast::JoinType::Cross => {}
    }
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };
    // The columns the subquery's correlated references name in the left scope.
    // A set-operation subquery's terms are walked the same as a lone
    // `SELECT`'s clauses -- each term is re-run per outer row exactly like a
    // plain `SELECT` would be.
    let mut correlated = Vec::new();
    collect_correlated_columns_query(
        subquery,
        &left_scope,
        catalog,
        current_db,
        &mut correlated,
        ctx,
    );

    // The inner relation's shape must be fixed before the first outer row, so
    // it is settled by one probe run with every correlated column bound to a
    // stand-in value -- the same trick `subquery_result_type` uses for a
    // scalar Apply, except the stand-in must carry the outer column's OWN
    // type: a bare NULL would make `SELECT t.a + u.z` infer the type of
    // `NULL + NULL` rather than of two BIGINTs. The probe's VALUES are
    // discarded; only the field types (and, when the statement does not state
    // them, the names) survive.
    let probe_resolver = ScopeResolver { scope: &left_scope };
    let probes: Vec<(Vec<String>, Datum)> = correlated
        .iter()
        .map(|path| {
            let datum = probe_resolver
                .resolve(path)
                .map_or(Datum::Null, |(_, ft, _)| probe_datum(&ft));
            (path.clone(), datum)
        })
        .collect();
    let typed = bind_subquery_columns_query(subquery, &probes)?;
    let (probe_columns, _) = run_query_stmt(&typed, catalog, current_db, ctx)?;
    let mut columns: Vec<(String, FieldType)> = match derived_field_names_query(subquery) {
        Some(names) if names.len() == probe_columns.len() => names
            .into_iter()
            .zip(&probe_columns)
            .map(|(name, (_, ft))| (name, ft.clone()))
            .collect(),
        _ => probe_columns,
    };
    // A derived table is a named relation, so duplicate column names are Go's
    // ErrDupFieldName -- the same check `build_derived_source` makes.
    for (index, (name, _)) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateColumnName(name.clone()));
        }
    }
    rename_derived_columns(&mut columns, column_names)?;

    let left_width = left_scope.width();
    let mut scope = left_scope.clone();
    scope.tables.push(FromTable {
        name: alias.to_owned(),
        // An alias is the only qualifier a derived table answers to.
        database: None,
        columns: columns.clone(),
        offset: left_width,
        func_deps: Default::default(),
    });

    let inner_width = columns.len();
    let subquery = subquery.clone();
    let correlated_indices = correlated_path_indices(&correlated, &left_scope)?;
    let cache_columns = correlated_indices.clone();
    let correlated_paths = correlated;
    // The callback outlives this borrow of the catalog, so it owns a snapshot
    // (see ApplyExec::new).
    let inner_catalog = catalog.clone();
    let inner_db = current_db.to_owned();
    let inner_ctx = ctx.clone();
    let runner: crate::apply::LateralRunner = Box::new(move |values: &[Datum]| {
        let mut bindings = Vec::with_capacity(correlated_paths.len());
        for (path, index) in correlated_paths.iter().zip(&correlated_indices) {
            let value = values
                .get(*index)
                .cloned()
                .ok_or(ExecError::unsupported("correlated column out of range"))?;
            bindings.push((path.clone(), value));
        }
        let bound = bind_subquery_columns_query(&subquery, &bindings)
            .map_err(|e| ExecError::unsupported(driver_error_text(&e)))?;
        let (_, rows) =
            run_query_stmt(&bound, &inner_catalog, &inner_db, &inner_ctx).map_err(|e| match e {
                DriverError::Exec(exec) => exec,
                other => ExecError::unsupported(driver_error_text(&other)),
            })?;
        Ok(rows)
    });

    let schema_columns: Vec<Column> = scope
        .column_list()
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    debug_assert_eq!(schema_columns.len(), left_width + inner_width);
    let schema = Schema::new(schema_columns);
    let mut exec: Box<dyn Executor> = Box::new(
        crate::apply::LateralApplyExec::new(
            ExecutorMeta::new(schema.clone(), 6, INIT_CAP, MAX_CHUNK_SIZE),
            left_exec,
            runner,
            ctx.statement_memory(),
        )
        .with_cache(
            ctx.apply_cache_capacity(),
            cache_columns,
            ctx.session_zone(),
        ),
    );
    // `JOIN LATERAL (...) x ON <cond>`: the inner join is already produced by
    // the Apply, so the ON condition is simply a filter over its rows.
    if let Some(on) = &join.on {
        let resolver = ScopeResolver { scope: &scope };
        let predicate = rewrite_expr_resolved(on, &resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        exec = Box::new(SelectionExec::new(
            ExecutorMeta::new(schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![predicate],
            exec,
            ctx.clone(),
            ctx.statement_memory(),
        ));
    }
    // An Apply's row order follows its OUTER side, which this tier does not
    // describe here.
    Ok((exec, scope, Delivered::new()))
}

/// How deep a view may nest before the reference is called invalid. A view
/// whose body reads itself (which `CREATE OR REPLACE` can build) would
/// otherwise recurse forever.
///
/// DIVERGENCE (documented): MySQL caps nesting at 61 and reports
/// `ER_VIEW_RECURSIVE` (1462); this reports `ErrViewInvalid` (1356), the same
/// error the other broken-view cases report.
pub(crate) const MAX_VIEW_DEPTH: usize = 32;

thread_local! {
    /// How many view bodies the current statement is inside.
    static VIEW_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Decrements the view-nesting depth however the body's evaluation ends.
pub(crate) struct ViewDepthGuard;

impl ViewDepthGuard {
    /// Enters one view body, refusing to go past [`MAX_VIEW_DEPTH`].
    fn enter(qualified: &str) -> Result<ViewDepthGuard, DriverError> {
        VIEW_DEPTH.with(|depth| {
            if depth.get() >= MAX_VIEW_DEPTH {
                return Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(
                    qualified.to_owned(),
                )));
            }
            depth.set(depth.get() + 1);
            Ok(ViewDepthGuard)
        })
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

/// Runs a view's stored `SELECT` and presents its rows as a `FROM` source.
///
/// Go rewrites the reference into a derived table over the view's plan; the
/// rows here are materialized instead, which is the same result for a reader
/// (the outer `WHERE`, joins and `ORDER BY` all apply to the view's output
/// either way) and differs only in that nothing is pushed into the view.
///
/// The body's own failure is Go's `ErrViewInvalid`: the definition ran once
/// already, when the view was created, so anything that stops it running now
/// is a schema change underneath it.
pub(crate) fn build_view_source(
    view: &ViewDef,
    database: &str,
    name: &str,
    visible: String,
    alias_free: bool,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let qualified = format!("{database}.{name}");
    let _guard = ViewDepthGuard::enter(&qualified)?;
    let invalid = || DriverError::Schema(SchemaErrorKind::ViewInvalid(qualified.clone()));
    // The definition is stored schema-qualified, so it resolves in the view's
    // own schema rather than the reader's.
    let (body_columns, rows) =
        run_select_meta_in(&view.select_sql, catalog, database, ctx).map_err(|_| invalid())?;
    if body_columns.len() != view.columns.len() {
        return Err(invalid());
    }
    // The view's own column names win over the body's, which is what a
    // `CREATE VIEW v (a2) AS SELECT a ...` column list means.
    let columns: Vec<(String, FieldType)> = view
        .columns
        .iter()
        .zip(&body_columns)
        .map(|((name, _), (_, ft))| (name.clone(), ft.clone()))
        .collect();
    let schema_columns: Vec<Column> = columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let exec: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
        rows,
    ));
    let scope = FromScope {
        tables: vec![FromTable {
            name: visible,
            database: alias_free.then(|| database.to_owned()),
            columns,
            offset: 0,
            func_deps: Default::default(),
        }],
        zone: ctx.session_zone(),
        tidb_info_len: ctx.tidb_info_len(),
        ..FromScope::default()
    };
    Ok((exec, scope))
}

/// One common column of a `NATURAL`/`USING` join: the row offset that stays
/// visible under the shared name, and the one that is coalesced away.
struct CommonColumn {
    visible: usize,
    redundant: usize,
}

/// Go `PlanBuilder.coalesceCommonColumns`, as the naming half of a join.
///
/// The whole of `NATURAL JOIN` and `JOIN ... USING` is this: an ordinary join
/// whose `ON` is `l.c = r.c` for every common column `c`, plus a rule about
/// which names the result answers to. Nothing about the ROW changes -- it is
/// still the left side's columns followed by the right side's -- so this
/// returns the common pairs and rewrites only `scope`'s [`FromScope::star`]
/// and [`FromScope::coalesced`], and every consumer downstream (`*`, name
/// resolution, `ONLY_FULL_GROUP_BY`, pruning) reads the scope it always did.
///
/// Captured from Go, and the reason the two orders below are separate:
///
/// * the common columns come FIRST, ordered by the LEFT side's own column
///   order -- not by the order the `USING` list writes them (`m1 JOIN m2
///   USING (b, a)` and `USING (a, b)` both report `a, b, ...`);
/// * a RIGHT join reports right-then-left throughout, so its common columns
///   take the RIGHT side's order and the surviving copy is the RIGHT side's
///   column. That is Go's `leftPlan, rightPlan = rightPlan, leftPlan` swap,
///   and it is what makes the survivor always the OUTER (row-preserving)
///   side, whose value is never the NULL-padded one.
///
/// `using` empty means `NATURAL`: every common name participates.
fn coalesce_common_columns(
    scope: &mut FromScope,
    left_visible: Vec<(usize, String, FieldType)>,
    right_visible: Vec<(usize, String, FieldType)>,
    join_tp: tidb_ast::JoinType,
    using: &[String],
) -> Result<Vec<CommonColumn>, DriverError> {
    // The RIGHT-join mirror: from here on "left" means the outer side.
    let (outer, inner) = match join_tp {
        tidb_ast::JoinType::Right => (right_visible, left_visible),
        _ => (left_visible, right_visible),
    };
    let lower = |name: &str| name.to_ascii_lowercase();
    let filter: Vec<String> = using.iter().map(|name| lower(name)).collect();
    let named = |name: &str| filter.is_empty() || filter.iter().any(|f| f == &lower(name));

    // Go checks ambiguity BEFORE matching: a name that a side offers twice
    // (which only a join can produce) has no single column to coalesce.
    // Without a `USING` filter the check applies to the common names only,
    // with one it applies to every name the filter mentions.
    let ambiguous = |side: &[(usize, String, FieldType)], name: &str| {
        side.iter()
            .filter(|(_, candidate, _)| candidate.eq_ignore_ascii_case(name))
            .count()
            > 1
    };
    let in_both = |name: &str| {
        outer.iter().any(|(_, n, _)| n.eq_ignore_ascii_case(name))
            && inner.iter().any(|(_, n, _)| n.eq_ignore_ascii_case(name))
    };
    for (_, name, _) in outer.iter().chain(inner.iter()) {
        // `NATURAL` has only the common names to coalesce, so only those can
        // be ambiguous; `USING` answers for every name it lists.
        if !named(name) || (filter.is_empty() && !in_both(name)) {
            continue;
        }
        if ambiguous(&outer, name) || ambiguous(&inner, name) {
            return Err(DriverError::AmbiguousColumnInClause {
                column: lower(name),
                clause: "from clause".to_owned(),
            });
        }
    }

    let mut common: Vec<CommonColumn> = Vec::new();
    let mut taken: Vec<String> = Vec::new();
    for (offset, name, _) in &outer {
        // `USING (a, a)`: Go's filter is a set, so a name coalesces once.
        if !named(name) || taken.contains(&lower(name)) {
            continue;
        }
        let Some((inner_offset, ..)) = inner.iter().find(|(_, n, _)| n.eq_ignore_ascii_case(name))
        else {
            continue;
        };
        taken.push(lower(name));
        common.push(CommonColumn {
            visible: *offset,
            redundant: *inner_offset,
        });
    }

    // A `USING` name neither side pairs up is Go's ErrUnknownColumn against
    // the `from clause` -- the same 1054 a missing column anywhere reports.
    if let Some(missing) = filter.iter().find(|name| !taken.contains(name)) {
        return Err(DriverError::UnknownColumnInClause {
            column: missing.clone(),
            clause: "from clause".to_owned(),
        });
    }

    // Display order: the common columns, then the outer side's remaining
    // columns, then the inner side's.
    let remaining = |side: &[(usize, String, FieldType)], common: &[CommonColumn]| -> Vec<usize> {
        side.iter()
            .map(|(offset, ..)| *offset)
            .filter(|offset| {
                !common
                    .iter()
                    .any(|c| c.visible == *offset || c.redundant == *offset)
            })
            .collect()
    };
    scope.star = common
        .iter()
        .map(|c| c.visible)
        .chain(remaining(&outer, &common))
        .chain(remaining(&inner, &common))
        .collect();
    scope.coalesced.extend(common.iter().map(|c| c.redundant));
    Ok(common)
}

/// Builds one join node (or passes through the single-table wrapper).
///
/// `prune` carries the statement whose column needs may narrow this join's
/// two sides, and is `Some` only for a query's OUTERMOST `FROM` node. That
/// restriction is load-bearing rather than tidiness: a nested join's sides
/// must never be narrowed, because the enclosing join's `ON` can name their
/// columns and is not visible from inside. `None` therefore means "build this
/// relation full width", which is what every recursive and non-`SELECT`
/// caller passes. See [`crate::column_prune`].
// Two of the eight are what an enclosing node ASKS of this join -- the
// offered `WHERE` conjuncts and the property it must produce -- and both must
// reach it before its children are built. Grouping them into a wrapper would
// name the demand without changing what travels; the sibling builders in this
// module carry the same allow.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_join(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
    prune: Option<&tidb_ast::SelectStmt>,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    // `FROM a, b` parses as the single-relation wrapper AROUND the real join,
    // while `FROM a JOIN b ON ...` is the join node itself. Unwrapping here
    // keeps the two spellings one shape for the prune request below --
    // otherwise the comma form would drop it and silently never prune.
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        if let JoinNode::Join(inner) = &join.left {
            return build_join(
                inner, catalog, current_db, ctx, trace, prune, demand, required,
            );
        }
    }
    // Go's `GetMergeJoin` reads its children's PROVIDED orders and then hands
    // each child a required property over its own join keys
    // (`tryToGetChildReqProp`). Both halves happen here, BEFORE the children
    // exist, because this tier's children are executors rather than logical
    // plans and cannot be re-planned once built. Everything the decision reads
    // -- the two tables' primary keys and the `ON` clause -- is catalog and
    // syntax, so it needs no child.
    //
    // The required property this join is itself asked for is the empty one:
    // no caller demands an order from a join yet, and the empty property's
    // `AllSameOrder` is Go's ascending answer.
    let merge = crate::driver::merge_decision::merge_join_decision(
        join,
        catalog,
        current_db,
        required,
        demand.offered,
    )
    // `GetMergeJoin` succeeding structurally is NECESSARY but not SUFFICIENT.
    // Before any cost is compared, `exhaustPhysicalPlans4LogicalJoin` reads
    // the statement's join-method hints and three of its arms settle the whole
    // candidate list: a forced hash join returns before a merge candidate is
    // built, a forced index join returns the index candidates alone, and a
    // `NO_MERGE_JOIN` deletes the family outright. That gate is Go's, and it
    // is what separates the FIVE `topn_push_down` statements that differ only
    // by their hint. See `driver::join_method_hints`.
    .filter(|_| {
        demand.join_hints.is_none_or(|hints| {
            let (left, right) = crate::driver::join_method_hints::side_aliases(join);
            hints.merge_join_allowed(
                (left.as_deref(), right.as_deref()),
                required.is_sort_item_empty(),
            )
        })
    });
    // The same walk's other answer: the orders each side's output already
    // carries, which is Go's `p.LeftProperties` / `p.RightProperties` and the
    // input the join enumeration reads to decide whether a merge join is a
    // candidate at all (`driver::join_search`). Computed here for the same
    // reason the merge decision is -- before the children exist.
    let sides = join.right.as_ref().and_then(|right| {
        let left = crate::driver::merge_decision::possible_properties(
            &join.left,
            catalog,
            current_db,
            demand.offered,
        )?;
        let right = crate::driver::merge_decision::possible_properties(
            right,
            catalog,
            current_db,
            demand.offered,
        )?;
        Some((left, right))
    });
    let search_orders = sides
        .as_ref()
        .map(|(left, right)| (left.orders.clone(), right.orders.clone()));
    let (left_required, right_required) = match &merge {
        // `tryToGetChildReqProp`: each child is required to produce ITS OWN
        // join keys' order, in the direction the parent asked for.
        Some(decision) => (
            crate::driver::merge_decision::child_required_prop(
                decision.plan.keys.iter().map(|key| key.left),
                decision.plan.desc,
            ),
            crate::driver::merge_decision::child_required_prop(
                decision.plan.keys.iter().map(|key| key.right),
                decision.plan.desc,
            ),
        ),
        // The parser's single-relation wrapper is not a join at all: it
        // passes its one child the property it was itself asked for, which is
        // how a `FROM a, b` node reaches the table under it.
        None if join.right.is_none() => (
            required.clone(),
            tidb_planner::physical_property::PhysicalProperty::default(),
        ),
        // No merge join here -- but an INDEX join may still be the plan, and
        // `enumerateIndexJoinByOuterIdx` re-plans its OUTER side under the
        // SAME property this join was asked for:
        //
        // ```text
        // if !prop.AllColsFromSchema(outerSchema) { continue }
        // ...
        // chReqProps[outerIdx] = &property.PhysicalProperty{
        //     TaskTp: property.RootTaskType, ExpectedCnt: math.MaxFloat64,
        //     SortItems: prop.SortItems,
        // }
        // ```
        //
        // That `AllColsFromSchema` guard is also what NAMES the outer side
        // here, before either child exists: an index join cannot promise an
        // order over a column its inner side owns, so the side holding every
        // required column is the only side that can be outer. Which side the
        // decision finally looks up is settled after the children are built;
        // if it turns out not to be an index join at all, the request is
        // unsaid again (`PlanTrace::retract_child_keep_order`).
        None => index_join_child_props(required, sides.as_ref().map(|(left, _)| left.width)),
    };
    let (mut left_exec, left_scope, left_delivered) = build_from(
        &join.left,
        catalog,
        current_db,
        ctx,
        trace.as_deref_mut(),
        demand,
        &left_required,
    )?;
    let Some(right_node) = &join.right else {
        // The single-table wrapper the parser always produces: it delivers
        // exactly what its one child does.
        return Ok((left_exec, left_scope, left_delivered));
    };
    if let JoinNode::Derived {
        subquery,
        alias,
        lateral: true,
        column_names,
    } = right_node
    {
        // An Apply, not a join: a shape the plan recorder does not print.
        if let Some(trace) = trace.as_deref_mut() {
            trace.refuse("LATERAL derived tables are not supported yet");
        }
        return build_lateral_join(
            join,
            left_exec,
            left_scope,
            subquery,
            alias.as_deref(),
            column_names,
            catalog,
            current_db,
            ctx,
        );
    }
    let coalescing = join.natural || !join.using.is_empty();
    let (mut right_exec, right_scope, right_delivered) = build_from(
        right_node,
        catalog,
        current_db,
        ctx,
        trace.as_deref_mut(),
        demand,
        &right_required,
    )?;

    // VERIFY -- the second half of the promise/verify contract (see
    // `merge_decision`'s module doc). `merge` was formed from the PROMISE:
    // Go's `PreparePossibleProperties` union, which says which orders a
    // child's output COULD be produced in, not which one it was built in.
    // Both children have now been built, under exactly the properties that
    // decision asked of them, and each reported what it ACTUALLY delivers.
    //
    // A merge join whose child did not deliver would compare groups its input
    // never separated and silently DROP rows. That hazard is why this tier
    // once narrowed the promise itself; it is removed here instead, and
    // removed by READING the built plan rather than by predicting it, so the
    // check cannot drift from what runs. A promise verification cannot
    // deliver falls back to the hash join, which needs no order at all.
    //
    // The key offsets are the pre-prune ones on both sides, which is the same
    // identity `build_from` reported its delivery in; the pruning below
    // renumbers the joined row and happens after this.
    let merge = merge.filter(|decision| {
        let left_keys: Vec<usize> = decision.plan.keys.iter().map(|key| key.left).collect();
        let right_keys: Vec<usize> = decision.plan.keys.iter().map(|key| key.right).collect();
        crate::driver::merge_decision::delivers(&left_delivered, &left_keys)
            && crate::driver::merge_decision::delivers(&right_delivered, &right_keys)
    });

    // The joined scope: the right tables' columns follow the left's.
    let mut left_width = left_scope.width();
    // The two sides' DISPLAY columns, which is what a coalesced join matches
    // on: a nested `a NATURAL JOIN b NATURAL JOIN c` sees the inner join's
    // already-coalesced output, never its hidden duplicates.
    let left_visible = left_scope.star_columns();
    let right_visible: Vec<(usize, String, FieldType)> = right_scope
        .star_columns()
        .into_iter()
        .map(|(offset, name, ft)| (offset + left_width, name, ft))
        .collect();
    let child_coalesced = !left_scope.star.is_empty() || !right_scope.star.is_empty();
    let mut scope = left_scope;
    scope
        .coalesced
        .extend(right_scope.coalesced.iter().map(|o| o + left_width));
    if !coalescing && child_coalesced {
        // Go's `buildJoin` gives a plain join the output names of its two
        // CHILDREN concatenated (`copy(joinPlan.OutputNames(), leftPlan
        // .OutputNames())` and the same for the right at `leftPlan.Schema()
        // .Len()`), and a coalesced child's own names are already the
        // coalesced ones. Row order is therefore the right display order only
        // when NEITHER side coalesced: `t1 JOIN t2 USING (a) RIGHT JOIN t3 ON
        // ...` reports `a, c, d` then t3's `a` (four columns, not the inner
        // join's three), and `FROM t1, t2 NATURAL LEFT JOIN t3` reports t1's
        // `i` then the natural join's single `i` (two, not three). Both were
        // captured from TiDB. A RIGHT join does NOT swap the two sides here --
        // only `coalesceCommonColumns` swaps, and only for the join that
        // coalesces.
        scope.star = left_visible
            .iter()
            .chain(right_visible.iter())
            .map(|(offset, ..)| *offset)
            .collect();
    }
    for table in right_scope.tables {
        scope.tables.push(FromTable {
            name: table.name,
            database: table.database,
            columns: table.columns,
            offset: table.offset + left_width,
            func_deps: table.func_deps,
        });
    }

    // `NATURAL` / `USING`: the common columns' equalities become this join's
    // conditions, and the scope records which name now reaches which column.
    // The ROW is untouched, so everything below runs unchanged.
    let mut coalesced_conditions = Vec::new();
    if coalescing {
        let common = coalesce_common_columns(
            &mut scope,
            left_visible,
            right_visible,
            join.tp,
            &join.using,
        )?;
        let resolver = ScopeResolver { scope: &scope };
        for pair in &common {
            let (Some(visible), Some(redundant)) = (
                scope.qualified_path(pair.visible),
                scope.qualified_path(pair.redundant),
            ) else {
                return Err(DriverError::unsupported(
                    "a coalesced join column has no table to name it",
                ));
            };
            let equality = tidb_ast::Expr::Binary(
                tidb_ast::BinaryOp::Eq,
                Box::new(tidb_ast::Expr::Column(visible)),
                Box::new(tidb_ast::Expr::Column(redundant)),
            );
            coalesced_conditions.push(
                rewrite_expr_resolved(&equality, &resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            );
        }
    }

    // Column pruning across the join, BEFORE the `ON` below is rewritten:
    // each side is offered only the columns the statement (its `ON`
    // included) reads from it. The two sides answer independently, so the
    // widths below are read back off the narrowed scope rather than assumed.
    // A coalesced join is exempt: its scope addresses columns by row offset
    // in `star`/`coalesced`, which renumbering would invalidate.
    // Whether the joined row was RENUMBERED below. A pruned join is the top
    // of its `FROM` plan (only the outermost one is offered a `prune`), so it
    // has no parent to report a delivery to -- and the offsets its children
    // reported are no longer the offsets of its row.
    let mut pruned = false;
    if let Some(select) = prune.filter(|_| !coalescing) {
        if let Some((left_columns, right_columns)) = crate::column_prune::prune_join_sides(
            select,
            join,
            &scope,
            &mut left_exec,
            &mut right_exec,
        ) {
            pruned = true;
            left_width = left_columns.len();
            scope.tables[0].columns = left_columns;
            scope.tables[0].offset = 0;
            scope.tables[1].columns = right_columns;
            scope.tables[1].offset = left_width;
        }
    }

    let column_list = scope.column_list();
    let schema_columns: Vec<Column> = column_list
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let meta = ExecutorMeta::new(Schema::new(schema_columns), 6, INIT_CAP, MAX_CHUNK_SIZE);

    let mut conditions = match &join.on {
        Some(expr) => {
            let resolver = ScopeResolver { scope: &scope };
            vec![rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?]
        }
        None => Vec::new(),
    };
    conditions.append(&mut coalesced_conditions);
    // The `WHERE` equalities this join is the lowest node able to evaluate.
    // Inner joins only, and never a coalesced one, whose scope addresses
    // columns by row offset rather than by name. Each pushed conjunct STAYS
    // in `WHERE`, so it can only narrow the pairs the filter above would
    // have narrowed anyway -- see `driver::predicate_push_down`.
    // A conjunct this join's `ON` ALREADY spells is not pushed a second time.
    // The two spellings meet whenever a `WHERE` equality became an `ON` --
    // which is every edge the join reorder rebuilt a tree from
    // (`driver::join_reorder`) -- and a repeated equality is not merely noise:
    // it doubles the hash join's key list and the `equal:[...]` the plan
    // prints.
    let written_on: Vec<&tidb_ast::Expr> = join
        .on
        .iter()
        .flat_map(|on| {
            let mut conjuncts = Vec::new();
            crate::plan_trace::collect_and(on, &mut conjuncts);
            conjuncts
        })
        .collect();
    let pushed: Vec<&tidb_ast::Expr> = if join.tp == tidb_ast::JoinType::Cross && !coalescing {
        crate::driver::predicate_push_down::spanning_conjuncts(demand.offered, &scope, left_width)
            .into_iter()
            .filter(|conjunct| !written_on.contains(conjunct))
            .collect()
    } else {
        Vec::new()
    };
    for conjunct in &pushed {
        let resolver = ScopeResolver { scope: &scope };
        conditions.push(
            rewrite_expr_resolved(conjunct, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }
    let kind = match join.tp {
        tidb_ast::JoinType::Cross => JoinKind::Inner,
        tidb_ast::JoinType::Left => JoinKind::Left,
        tidb_ast::JoinType::Right => JoinKind::Right,
    };
    // The condition split the executor will run on, so EXPLAIN's
    // `equal:[...]`/`other cond:` and the hash table's own keys are one
    // decision rather than two that can drift.
    let split = crate::hash_join::split_equi(&conditions, left_width);
    // Go's stats-less build side: the inner (non-preserved) child, which is
    // the left one only for a RIGHT join. See `join.rs`'s module doc.
    let build_is_left = kind == JoinKind::Right;
    // Read before the children move into the join: the index-join decision
    // needs each side's OUTPUT types (post-pruning), which only the built
    // executors know.
    let left_types = left_exec.ret_field_types().to_vec();
    let right_types = right_exec.ret_field_types().to_vec();
    let mut join_exec = JoinExec::new(
        meta,
        kind,
        conditions,
        left_exec,
        right_exec,
        ctx.clone(),
        ctx.statement_memory(),
    );
    // The key offsets the decision computed are pre-pruning; the scope is
    // post-pruning. Re-resolving the key NAMES against it is what keeps the
    // executor's merge keys and the columns it actually holds one answer.
    // A name that no longer resolves drops the merge -- fail-closed, and the
    // `keep order:true` already recorded below stays TRUE either way, because
    // the scan streams record keys in key order whether or not this join
    // relies on it.
    let merged = merge.and_then(|decision| {
        let keys: Option<Vec<_>> = decision
            .names
            .iter()
            .map(|(left, right)| {
                // The joined-row offsets, re-read off the FINAL scope, then
                // split back into the per-child offsets the merge executor
                // reads its keys out of. A key that lands on the wrong side
                // of the split names a column this join does not hold, and
                // drops the merge.
                let left = scope_offset_of(&scope, left)?;
                let right = scope_offset_of(&scope, right)?;
                (left < left_width && right >= left_width).then_some(
                    crate::merge_join_plan::MergeJoinKey {
                        left,
                        right: right - left_width,
                    },
                )
            })
            .collect();
        keys.map(|keys| crate::merge_join_plan::MergeJoinPlan {
            keys,
            ..decision.plan
        })
    });
    // A merge join pairs only rows whose keys are EQUAL, so every key it
    // merges on must also be an equality the join would have applied anyway.
    // The decision reads the `ON` clause and the offered `WHERE` conjuncts
    // before the children exist; `split_equi` reads the rewritten conditions
    // the executor actually holds. Requiring the second to contain the first
    // is what keeps a key the pushdown declined from silently narrowing the
    // result.
    let merged = merged.filter(|plan| {
        plan.keys.iter().all(|key| {
            split
                .keys
                .iter()
                .any(|equi| equi.left == key.left && equi.right == key.right)
        })
    });
    if let Some(plan) = merged.clone() {
        join_exec.set_merge_plan(plan);
    }
    // The index strategy: one side read once per outer key rather than whole.
    // It is asked only where the merge decision declined, and it refuses a
    // coalesced join for the same reason the merge one is dropped there --
    // that scope addresses columns by row offset, not by name.
    //
    // WHICH strategy this site may use is asked of Go's own enumeration --
    // `exhaustPhysicalPlans4LogicalJoin` under the property this join was
    // required to produce -- and not of the structural rule underneath. See
    // `driver::join_search`.
    let chosen = crate::driver::join_search::choose(&crate::driver::join_search::SearchInput {
        join,
        join_type: match kind {
            JoinKind::Inner => tidb_planner::find_best_task::LogicalJoinType::Inner,
            JoinKind::Left => tidb_planner::find_best_task::LogicalJoinType::LeftOuter,
            JoinKind::Right => tidb_planner::find_best_task::LogicalJoinType::RightOuter,
        },
        keys: &split.keys,
        left_width,
        width: scope.width(),
        orders: search_orders
            .as_ref()
            .map(|(left, right)| (left.as_slice(), right.as_slice())),
        required,
        rows: demand.rows,
    });
    let index_join = (!coalescing && chosen == crate::driver::join_search::Chosen::Index)
        .then(|| {
            let (left_side, right_side) = crate::driver::index_join_decision::join_sides(
                join,
                &scope,
                current_db,
                left_width,
                catalog,
                &left_types,
                &right_types,
            );
            crate::driver::index_join_decision::index_join_decision(
                kind,
                &split.keys,
                &left_side,
                &right_side,
                merged.is_some(),
            )
        })
        .flatten();
    let index_text = index_join.as_ref().map(|decision| {
        let source = crate::access_path::IndexJoinLookupExec::new_with_context(
            ExecutorMeta::new(
                Schema::new(
                    decision
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, (_, ft))| {
                            let mut column = Column::new((i + 1) as i64, ft.clone());
                            column.index = i as i64;
                            column
                        })
                        .collect(),
                ),
                0,
                INIT_CAP,
                MAX_CHUNK_SIZE,
            ),
            decision.table.clone(),
            decision.object.clone(),
            crate::kv_table::RowDecodeContext::for_query(ctx),
        );
        let keys: Vec<(String, String)> = decision
            .probe_keys
            .iter()
            .map(|at| {
                let key = split.keys[*at];
                let (outer, inner) = if decision.lookup_is_left {
                    (left_width + key.right, key.left)
                } else {
                    (key.left, left_width + key.right)
                };
                (
                    qualified_scope_column(&scope, current_db, outer),
                    qualified_scope_column(&scope, current_db, inner),
                )
            })
            .collect();
        let index = match &decision.object {
            crate::access_path::LookupObject::Index(_) => true,
            crate::access_path::LookupObject::Handle => false,
        };
        let access = match &decision.object {
            crate::access_path::LookupObject::Index(id) => {
                let index = decision
                    .table
                    .indexes()
                    .iter()
                    .find(|index| index.id == *id)
                    .expect("the decision named an index of this table");
                let columns: Vec<&str> = index
                    .column_offsets
                    .iter()
                    .map(|offset| decision.columns[*offset].0.as_str())
                    .collect();
                format!(
                    "table:{}, index:{}({})",
                    decision.visible,
                    index.name,
                    columns.join(", ")
                )
            }
            crate::access_path::LookupObject::Handle => format!("table:{}", decision.visible),
        };
        join_exec.set_index_lookup_plan(crate::join::IndexLookupPlan {
            lookup_is_left: decision.lookup_is_left,
            probe_keys: decision.probe_keys.clone(),
            source,
        });
        (
            crate::plan_trace::IndexJoinText {
                reader: if index { "IndexReader" } else { "TableReader" },
                keys,
                lookup_is_left: decision.lookup_is_left,
                // `getAvgRowSize(.., Schema().Columns)` for each side. Go
                // reaches its `HistColl` branch when the child is a
                // `DataSource`; this tier has no `HistColl` at a join's child
                // and so takes the same function's OTHER branch, the static
                // type width, which is what Go itself falls back to.
                outer_row_size: crate::access_cost::schema_avg_row_size(
                    if decision.lookup_is_left {
                        &right_types
                    } else {
                        &left_types
                    },
                ),
                inner_row_size: crate::access_cost::schema_avg_row_size(
                    if decision.lookup_is_left {
                        &left_types
                    } else {
                        &right_types
                    },
                ),
            },
            decision.lookup_is_left,
            access,
            decision.range_info.clone(),
            index,
        )
    });
    // The plan row and the executor are one decision: if the recorder cannot
    // rewrite the inner side's scan into the range it now reads, the trace is
    // refused rather than printed with a whole-table read under an index
    // join. The EXECUTOR is unaffected -- it still answers correctly.
    let index_text = match (index_text, trace.as_deref_mut()) {
        (Some((text, lookup_is_left, access, range_info, index)), Some(trace)) => {
            if trace
                .index_join_inner_scan(lookup_is_left, access, &range_info, index)
                .is_ok()
            {
                Some(text)
            } else {
                trace.refuse("this index join's inner side is not printable yet");
                None
            }
        }
        (Some((text, ..)), None) => Some(text),
        (None, _) => None,
    };
    let exec: Box<dyn Executor> = Box::new(join_exec);
    let strategy = crate::plan_trace::JoinStrategy {
        equal_mask: split.equal_mask.clone(),
        build_is_left,
        index_lookup: index_text,
        merge_keys: merged.as_ref().map(|plan| {
            plan.keys
                .iter()
                .map(|key| {
                    (
                        qualified_scope_column(&scope, current_db, key.left),
                        qualified_scope_column(&scope, current_db, left_width + key.right),
                    )
                })
                .collect()
        }),
    };
    // What this join DELIVERS to its own parent, read off the plan just
    // committed to rather than promised for it.
    //
    //  * A MERGE join emits its rows in key order, and its two key lists are
    //    equal in every row it emits, so both describe the output.
    //  * An INDEX join streams its OUTER side and emits each outer row's
    //    matches together, in outer order (`JoinExec::next_index_lookup`
    //    walks `state.outer` by cursor). That is Go's
    //    `PhysicalIndexHashJoin.KeepOuterOrder`, and it is the line that keeps
    //    a parent merge join alive above an index join.
    //  * A HASH join promises no order -- `getHashJoins`'s own first
    //    sentence -- and neither does a nested-loop one.
    //
    // Either way the side an outer join NULL-EXTENDS is dropped, which is
    // `union_orders`' whole job.
    let delivered = if pruned {
        Delivered::new()
    } else if let Some(plan) = &merged {
        crate::driver::merge_decision::union_orders(
            join.tp,
            vec![plan.keys.iter().map(|key| key.left).collect()],
            vec![plan.keys.iter().map(|key| key.right + left_width).collect()],
        )
    } else if let Some(decision) = &index_join {
        let outer = if decision.lookup_is_left {
            right_delivered
                .iter()
                .map(|order| order.iter().map(|at| at + left_width).collect())
                .collect()
        } else {
            left_delivered.clone()
        };
        if decision.lookup_is_left {
            crate::driver::merge_decision::union_orders(join.tp, Vec::new(), outer)
        } else {
            crate::driver::merge_decision::union_orders(join.tp, outer, Vec::new())
        }
    } else {
        Delivered::new()
    };
    if let Some(trace) = trace.as_deref_mut() {
        if merged.is_none() && index_join.is_none() {
            // This join asked its children to keep order and then HASHED --
            // it relies on neither child's order. (A merge join relies on
            // both; an index join streams its outer side in order, and its
            // inner side's row was already rewritten to `keep order:false` by
            // `index_join_inner_scan`.) Nothing relies on what this join
            // asked for, so the plan must stop saying it does. See
            // `PlanTrace::retract_child_keep_order`.
            trace.retract_child_keep_order([
                !left_required.is_sort_item_empty(),
                !right_required.is_sort_item_empty(),
            ]);
        }
        if coalescing {
            // The recorder prints the `ON` as written, and a coalesced join
            // has none -- its equalities are synthesized here.
            trace.refuse("NATURAL and USING joins are not printed yet");
        } else if trace
            .join(join, &scope, current_db, &pushed, &strategy)
            .is_err()
        {
            trace.refuse("this join's plan is not supported yet");
        }
    }
    // Set, never merged: this is a property of the node at the TOP of the
    // `FROM` plan, and the top is whichever `build_join` returns last. See
    // [`FromScope::qualified_star_is_output_only`].
    scope.qualified_star_is_output_only = join.tp == tidb_ast::JoinType::Cross && join.on.is_some();
    Ok((meter(exec, trace), scope, delivered))
}

/// `kv` with any `PARTITION (p, ...)` restriction on the reference applied,
/// or `kv` unchanged when the reference wrote none.
///
/// Go resolves the clause in `PartitionProcessor` and keeps only the named
/// partitions' `DataSource`s; here the same narrowing is one call on the
/// table handle (see `KvTable::restrict_read_to_partitions`), so every read
/// path honours it without a partition branch of its own.
///
/// # Errors
///
/// 1735 for a name the table does not have -- captured:
/// `Unknown partition 'nosuch' in table 'ok1'`. A `PARTITION (...)` on an
/// UNPARTITIONED table takes the same error in Go, and does here too, because
/// no name can resolve against a table with no partitions.
pub(crate) fn restricted_to_partitions(
    kv: &crate::KvTable,
    partitions: &[String],
    table: &str,
) -> Result<crate::KvTable, DriverError> {
    if partitions.is_empty() {
        return Ok(kv.clone());
    }
    let unknown = |partition: &str| DriverError::UnknownPartition {
        partition: partition.to_owned(),
        table: table.to_owned(),
    };
    let Some(spec) = kv.partition() else {
        return Err(unknown(&partitions[0]));
    };
    let ids = crate::partition_pruning::ids_for_selected_partitions(spec, partitions)
        .map_err(|name| unknown(&name))?;
    let mut restricted = kv.clone();
    restricted.restrict_read_to_partitions(&ids);
    Ok(restricted)
}

/// Meters `exec` for the node the trace just recorded, when there is one.
fn meter(exec: Box<dyn Executor>, trace: Option<&mut PlanTrace>) -> Box<dyn Executor> {
    match trace {
        Some(trace) => trace.meter(exec),
        None => exec,
    }
}

/// The table a single-table `UPDATE`/`DELETE` targets.
pub(crate) fn single_table_name(
    table_ref: &tidb_ast::TableRef,
    current_db: &str,
) -> Result<(String, String), DriverError> {
    // A READ restricted to partitions is honoured (`restricted_to_partitions`),
    // but a WRITE's is a different narrowing: it decides which rows are
    // MODIFIED, and this path hands the caller a table NAME rather than the
    // handle the restriction would live on. Ignoring it would update or
    // delete rows the statement excluded, so it is refused.
    if !table_ref.partitions.is_empty() {
        return Err(DriverError::unsupported(
            "UPDATE/DELETE ... PARTITION (...) is not supported yet",
        ));
    }
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    Ok((database.to_owned(), name.to_owned()))
}

/// The name a catalog entry stores for itself, which is what Go prints as a
/// memory table's access object -- `table:COLUMNS`, not the lower-case
/// `columns` the statement wrote.
fn declared_table_name(entry: &TableEntry, written: &str) -> String {
    match entry {
        TableEntry::Kv(table) if !table.name.is_empty() => table.name.clone(),
        _ => written.to_owned(),
    }
}

/// The offset within the joined row of the column `key` names, when the
/// scope still holds it.
///
/// The merge decision names its keys by RELATION rather than by offset
/// because column pruning renumbers both sides after the children are built
/// (see [`crate::column_prune`]). A relation keeps its name across a prune,
/// so re-reading the key here is what keeps the executor's merge keys and the
/// columns it actually holds one answer.
fn scope_offset_of(
    scope: &FromScope,
    key: &crate::driver::merge_decision::RelColumn,
) -> Option<usize> {
    let table = scope
        .tables
        .iter()
        .find(|table| table.name.eq_ignore_ascii_case(&key.relation))?;
    let at = table
        .columns
        .iter()
        .position(|(column, _)| column.eq_ignore_ascii_case(&key.column))?;
    Some(table.offset + at)
}

/// A joined-row offset rendered as `db.table.column`, the form
/// `ExplainColumnList` prints a merge join's keys in.
pub(crate) fn qualified_scope_column(scope: &FromScope, current_db: &str, offset: usize) -> String {
    for table in &scope.tables {
        if offset >= table.offset && offset - table.offset < table.columns.len() {
            let database = table.database.as_deref().unwrap_or(current_db);
            return format!(
                "{database}.{}.{}",
                table.name,
                table.columns[offset - table.offset].0
            );
        }
    }
    String::new()
}
