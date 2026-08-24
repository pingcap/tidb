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

pub(crate) type MaterializedRelation = (Vec<(String, FieldType)>, Vec<Vec<Datum>>);

/// An internal qualifier that parsed SQL can never produce. Plain EXPLAIN
/// uses it for Go's `ScalarSubQueryExpr`: a typed, non-constant planner value
/// that is visible to expression rewriting but is not part of an executor row.
pub(crate) const SCALAR_QUERY_SCOPE: &str = "\0scalar_subquery";

#[derive(Clone)]
pub(crate) struct PlanColumn {
    pub(crate) name: String,
    pub(crate) field_type: FieldType,
    pub(crate) unique_id: i64,
    /// The default plain-EXPLAIN branch evaluates scalar subqueries once.
    pub(crate) value: Option<Datum>,
}

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
///
/// `NATURAL`/`USING` coalescing does not change that row layout at all -- it
/// is expressed here, as the two pieces of naming a coalesced join adds on
/// top of it (see [`coalesce_common_columns`]).
#[derive(Clone)]
pub(crate) struct FromScope {
    pub(crate) tables: Vec<FromTable>,
    /// Planner-only values registered by plain EXPLAIN. Their synthetic row
    /// offsets follow the physical source width, but they deliberately do not
    /// contribute to [`FromScope::width`] or wildcard expansion because no
    /// executor ever produces them.
    pub(crate) plan_columns: Vec<PlanColumn>,
    pub(crate) constant_context: Option<crate::StmtContext>,
    /// The statement's session `time_zone`, which [`ScopeResolver`] publishes
    /// to the expression rewriter as [`ColumnResolver::time_zone`] -- Go's
    /// `ctx.Location()`, reached while BUILDING an expression (the
    /// `TIMESTAMP 'lit'` fold rounds and offset-normalizes in it). It rides
    /// on the scope because the scope is the one value every rewrite over
    /// this `FROM` already receives; the statement build points set it from
    /// `StmtContext::session_zone` and every derived scope clones it along.
    pub(crate) zone: tidb_expr::SessionTimeZone,
    pub(crate) tidb_info_len: usize,
    pub(crate) like_default_escape: u8,
    pub(crate) no_unsigned_subtraction: bool,
    pub(crate) div_precision_increment: u32,
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
            plan_columns: Vec::new(),
            constant_context: None,
            coalesced: Vec::new(),
            star: Vec::new(),
            qualified_star_is_output_only: false,
            zone: tidb_expr::SessionTimeZone::utc(),
            tidb_info_len: tidb_util::printer::get_tidb_info(
                &tidb_util::versioninfo::VersionInfo::build_default(),
            )
            .len(),
            like_default_escape: b'\\',
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
        }
    }
}

impl FromScope {
    pub(crate) fn for_statement(ctx: &crate::StmtContext) -> Self {
        Self {
            constant_context: Some(ctx.clone()),
            zone: ctx.session_zone(),
            tidb_info_len: ctx.tidb_info_len(),
            like_default_escape: ctx.like_default_escape(),
            no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
            div_precision_increment: ctx.div_precision_increment(),
            ..Self::default()
        }
    }

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
        Some(format!(
            "{}.{}.{}",
            database.to_lowercase(),
            table.name.to_lowercase(),
            column.0.to_lowercase()
        ))
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

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.scope
            .constant_context
            .as_ref()
            .map(tidb_expr::Columns::date_modes)
            .unwrap_or(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        match &self.scope.constant_context {
            Some(ctx) => ctx.connection_charset_info(),
            None => tidb_expr::collation_derive::connection_charset_info(),
        }
    }

    fn tidb_info_len(&self) -> usize {
        self.scope.tidb_info_len
    }

    fn like_default_escape(&self) -> u8 {
        self.scope.like_default_escape
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.scope.no_unsigned_subtraction
    }

    fn div_precision_increment(&self) -> u32 {
        self.scope.div_precision_increment
    }

    fn current_database(&self) -> Option<String> {
        self.scope
            .constant_context
            .as_ref()
            .and_then(tidb_expr::Columns::current_database)
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        match &self.scope.constant_context {
            Some(ctx) => tidb_expr::fold_constant_in_mode(expression, ctx, mode),
            None if mode != tidb_expr::ConstantFoldMode::Disabled => {
                tidb_expr::derive_constant_null_flag(expression);
            }
            None => {}
        }
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        if let [scope, name] = path {
            if scope == SCALAR_QUERY_SCOPE {
                let physical_width = self.scope.width();
                return self
                    .scope
                    .plan_columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == *name)
                    .map(|(offset, column)| {
                        (
                            physical_width + offset,
                            column.field_type.clone(),
                            column.unique_id,
                        )
                    });
            }
        }
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

    fn orig_name(&self, path: &[String]) -> Option<String> {
        Self::orig_name(self, path)
    }

    fn resolve_constant(&self, path: &[String]) -> Option<Expression> {
        let [scope, name] = path else {
            return None;
        };
        if scope != SCALAR_QUERY_SCOPE {
            return None;
        }
        let column = self
            .scope
            .plan_columns
            .iter()
            .find(|column| column.name == *name)?;
        let mut constant =
            tidb_expr::constant::Constant::new(column.value.clone()?, column.field_type.clone());
        constant.subquery_ref_id = -column.unique_id;
        Some(Expression::Constant(constant))
    }

    fn has_resolved_constants(&self) -> bool {
        self.scope
            .plan_columns
            .iter()
            .any(|column| column.value.is_some())
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
    // Column pruning narrows `columns`, while the catalog keeps physical
    // offsets. Rebind every catalog fact by column name before exposing it to
    // the compact scope, as Go's ColumnPruner remaps UniqueID-based FDs.
    let compact_offset = |physical: usize| {
        let name = &kv.columns.get(physical)?.name;
        columns
            .iter()
            .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
    };
    let remap_key = |physical: &[usize]| {
        physical
            .iter()
            .copied()
            .map(compact_offset)
            .collect::<Option<Vec<_>>>()
    };
    let not_null = |offsets: &[usize]| {
        offsets.iter().all(|&offset| {
            kv.columns
                .get(offset)
                .is_some_and(|column| column.field_type.flags() & NOT_NULL_FLAG != 0)
        })
    };

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
        if key.is_empty() {
            continue;
        }
        let Some(compact_key) = remap_key(&key) else {
            continue;
        };
        if not_null(&key) {
            deps.strict_keys.push(compact_key);
        } else {
            deps.lax_keys.push(compact_key);
        }
    }
    for (offset, column) in kv.columns.iter().enumerate() {
        let compact_generated = compact_offset(offset);
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
            if !dependencies.is_empty() {
                if let (Some(compact_dependencies), Some(compact_generated)) =
                    (remap_key(&dependencies), compact_generated)
                {
                    deps.generated
                        .push((compact_dependencies, compact_generated));
                }
            }
        }
        if column.field_type.flags() & NOT_NULL_FLAG != 0 {
            if let Some(compact) = compact_generated {
                deps.not_null.push(compact);
            }
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

/// Go's `prop.AllColsFromSchema(outerSchema)` gate for IndexJoin candidates.
/// A non-empty parent order can be preserved only by streaming the side that
/// owns every required column as the outer input.
fn index_join_satisfies_required_order(
    lookup_is_left: bool,
    required: &tidb_planner::physical_property::PhysicalProperty,
    left_width: Option<usize>,
) -> bool {
    if required.is_sort_item_empty() {
        return true;
    }
    let Some(left_width) = left_width else {
        return false;
    };
    let (all_same, _) = required.all_same_order();
    if !all_same {
        return false;
    }
    let all_left = required
        .sort_items
        .iter()
        .all(|item| item.col >= 0 && (item.col as usize) < left_width);
    let all_right = required
        .sort_items
        .iter()
        .all(|item| item.col >= left_width as i64);
    (all_left && !lookup_is_left) || (all_right && lookup_is_left)
}

/// Captures a property's stable column identities before recursive pruning
/// changes the positional row layout.
fn required_property_names(
    required: &tidb_planner::physical_property::PhysicalProperty,
    sides: Option<&(
        crate::driver::merge_decision::SideProperties,
        crate::driver::merge_decision::SideProperties,
    )>,
) -> Option<Vec<crate::driver::merge_decision::RelColumn>> {
    let (left, right) = sides?;
    required
        .sort_items
        .iter()
        .map(|item| {
            let offset = usize::try_from(item.col).ok()?;
            if offset < left.width {
                left.column_at(offset)
            } else {
                right.column_at(offset - left.width)
            }
        })
        .collect()
}

/// Re-resolves a parent property against the compact row this join actually
/// built. This is the row-offset equivalent of Go retaining Column UniqueIDs
/// across its second column-pruning pass.
fn remap_required_property(
    required: &tidb_planner::physical_property::PhysicalProperty,
    names: Option<&[crate::driver::merge_decision::RelColumn]>,
    scope: &FromScope,
) -> tidb_planner::physical_property::PhysicalProperty {
    let Some(names) = names else {
        return required.clone();
    };
    let Some(offsets) = names
        .iter()
        .map(|name| scope_offset_of(scope, name))
        .collect::<Option<Vec<_>>>()
    else {
        return required.clone();
    };
    let mut remapped = required.clone();
    for (item, offset) in remapped.sort_items.iter_mut().zip(offsets) {
        item.col = offset as i64;
    }
    remapped
}

/// Maps Go's possible-order promise from the original side schema into the
/// final compact child row. A missing column ends the usable ordered prefix.
fn remap_search_orders(
    properties: &crate::driver::merge_decision::SideProperties,
    scope: &FromScope,
    base: usize,
    width: usize,
) -> Vec<Vec<usize>> {
    properties
        .orders
        .iter()
        .filter_map(|order| {
            let remapped = order
                .iter()
                .map_while(|offset| {
                    let name = properties.column_at(*offset)?;
                    let joined = scope_offset_of(scope, &name)?;
                    (base..base + width)
                        .contains(&joined)
                        .then(|| joined - base)
                })
                .collect::<Vec<_>>();
            (!remapped.is_empty()).then_some(remapped)
        })
        .collect()
}

/// The column orders an executor a `FROM` builder just built ACTUALLY
/// produces, as offsets into its own row -- Go's `PossiblePropertiesInfo
/// .Orders` read off the PHYSICAL plan instead of the logical one.
///
/// This is the VERIFY half of the promise/verify contract; the PROMISE half is
/// [`crate::driver::merge_decision::possible_properties`]. See that module's
/// doc for why the two exist and what the narrowing they replaced cost.
#[derive(Clone, Debug, Default)]
pub(crate) struct Delivered {
    orders: Vec<Vec<usize>>,
    /// The complete physical task built for this subtree, when every operator
    /// on the path can be represented by the ver2 candidate tree.
    pub(crate) candidate: Option<tidb_planner::candidate_cost::Candidate>,
    /// The subtree root is a decorrelated semi or anti-semi join. Aggregation
    /// uses this physical boundary when restoring its functions-first state
    /// layout; it cannot infer the join kind from a stats-dependent candidate.
    pub(crate) semi_join: bool,
}

impl Delivered {
    pub(crate) const fn new() -> Self {
        Self {
            orders: Vec::new(),
            candidate: None,
            semi_join: false,
        }
    }

    pub(crate) fn from_orders(orders: Vec<Vec<usize>>) -> Self {
        Self {
            orders,
            candidate: None,
            semi_join: false,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.orders.clear();
        self.candidate = None;
        self.semi_join = false;
    }
}

impl std::ops::Deref for Delivered {
    type Target = Vec<Vec<usize>>;

    fn deref(&self) -> &Self::Target {
        &self.orders
    }
}

impl std::ops::DerefMut for Delivered {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.orders
    }
}

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
    trace: Option<&mut PlanTrace>,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    // A multi-table FROM recurses build_from -> build_join -> build_from per
    // JOIN NODE without passing `run_select_traced`'s per-SELECT checkpoint,
    // and a debug build's frames here run to hundreds of KB -- TPC-H q2's
    // five-table FROM overflowed the default 8 MB thread stack between
    // checkpoints (SIGABRT in `driver::tests::subqueries`). Go's goroutine
    // stack grows at every frame; this is that semantics per join level,
    // with the red zone sized for one build_from + build_join_with_choice +
    // build_join round.
    stacker::maybe_grow(2 * 1024 * 1024, 16 * 1024 * 1024, move || {
        build_from_inner(node, catalog, current_db, ctx, trace, demand, required)
    })
}

fn build_from_inner(
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
            let entry = catalog.get_in(database, name).ok_or_else(|| {
                DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
                    "{database}.{name}"
                )))
            })?;
            // A table alias replaces the name for qualification, as in Go.
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            if let TableEntry::View(view) = entry {
                let required_columns = demand
                    .columns
                    .map(|columns| columns.needed(&visible, &view.columns));
                let (exec, scope, delivered) = build_view_source(
                    view,
                    database,
                    name,
                    visible,
                    table_ref.alias.is_none(),
                    catalog,
                    ctx,
                    trace.as_deref_mut(),
                    required,
                    required_columns.as_deref(),
                )?;
                // A view's body is a whole statement. Plain EXPLAIN records
                // that statement's deferred subtree; ordinary execution
                // keeps the existing materialized relation.
                return Ok((meter(exec, trace), scope, delivered));
            }
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
            let mut columns = entry.column_list();
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
            // The leaf offset carrying `_tidb_rowid`, so `*` can skip it.
            let mut extra_handle_offset: Option<usize> = None;
            let mut cost_candidate = None;
            // A handle range consumes only its access conjuncts. The
            // remainder is still executed by the scan as a cop Selection and
            // must remain visible in the physical plan.
            let mut access_residual_filter = None;
            let mut access_consumed_filter = false;
            let mut path_residual_filters = None;
            let mut path_traced_residual_filters = Vec::new();
            // `RowSource` has already classified predicates that reference
            // only this leaf. Keep them as one local WHERE for range costing;
            // the written predicates still remain above the join.
            //
            // Go's `DataSource.pushedDownConds` holds BOTH families this
            // tier keeps apart: the `WHERE` conjuncts `rule_predicate_push_
            // down` sent to this child (RowSource's filters) and what
            // `expression.PropConstForOuterJoin` derived through the join
            // keys. The ranger builds access ranges from one merged list, so
            // the derived family must reach the path chooser too: offering
            // only the first left a derived `ne(t2.a, 3)` as a cop Selection
            // where TiDB's recorded plan reads `TableRangeScan
            // range:[-inf,3), (3,+inf]` (`executor/jointest/join`). Only the
            // DERIVED family joins the offer -- see [`Plan::derived`] for
            // why the routed family must not re-price inner-join leaves.
            // The receipt recorded below covers the merged list, and
            // [`apply_pushed_leaf_filters`] re-applies only what the
            // committed path reports as residual.
            let mut leaf_filters: Vec<tidb_ast::Expr> = demand
                .rows
                .and_then(|rows| rows.filters_for(&visible))
                .map(<[tidb_ast::Expr]>::to_vec)
                .unwrap_or_default();
            if demand.rows.is_some() {
                if let Some(pushdown) = demand.pushdown {
                    for filter in pushdown.derived_for(table_ref) {
                        if !leaf_filters.contains(filter) {
                            leaf_filters.push(filter.clone());
                        }
                    }
                }
            }
            let leaf_where = leaf_filters.iter().cloned().reduce(|left, right| {
                tidb_ast::Expr::Binary(
                    tidb_ast::BinaryOp::LogicAnd,
                    Box::new(left),
                    Box::new(right),
                )
            });
            let mut exec: Box<dyn Executor> = match entry {
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
                // its parent with the cheapest. The local predicates are
                // offered to the same chooser, so a bounded table/index path
                // can replace the full scan without removing the filter from
                // the join's upper pipeline.
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
                    // Go records this table at the physical-reader build site;
                    // the statement context uses the same fact for post-kill
                    // expression behavior.
                    ctx.mark_physical_table_reader();
                    if let Some(runtime) = demand
                        .runtime_lookup
                        .filter(|runtime| runtime.table_id == kv.table_id)
                    {
                        // This leaf is the one DataSource selected by the
                        // enclosing composite IndexHashJoin. It keeps the
                        // normal physical row layout while replacing the
                        // whole-table source with a shared-probe lookup.
                        let mut source = crate::access_path::IndexJoinLookupExec::new_with_context(
                            ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                            kv.clone(),
                            runtime.object.clone(),
                            crate::kv_table::RowDecodeContext::for_query(ctx),
                        );
                        source.set_probe_parts(runtime.probe_parts.clone());
                        source.set_shared_probes(runtime.probes.clone());
                        source.set_filters(runtime.filter_exprs.clone(), ctx.clone());
                        source.set_column_projection(
                            Some((0..columns.len()).collect()),
                            std::iter::empty(),
                        );
                        walked_index = Some(Vec::new());
                        cost_candidate = Some(runtime.probe_candidate.clone());
                        Box::new(source)
                    } else {
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
                                leaf_where.as_ref(),
                                &hints,
                                catalog,
                                ctx,
                                wanted_order.as_deref(),
                            )
                        }) {
                            Some(crate::driver::leaf_access::LeafAccessPath::Point {
                                handle,
                                order,
                                candidate,
                            }) => {
                                walked_index = Some(order);
                                cost_candidate = Some(candidate);
                                access_consumed_filter =
                                    crate::driver::access::point_get_predicate_is_consumed(
                                        leaf_where.as_ref(),
                                        kv,
                                        &columns,
                                        &ctx.session_zone(),
                                        // A join leaf carries the `PARTITION
                                        // (p)` list of its OWN table ref; the
                                        // `_tidb_rowid` exception this feeds
                                        // is a single-table plan's, so a leaf
                                        // never claims it.
                                        &[],
                                    );
                                if access_consumed_filter {
                                    path_residual_filters = Some(Vec::new());
                                }
                                let handles = handle.iter().cloned().collect::<Vec<_>>();
                                let source = HandleSourceExec::new_with_context(
                                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                                    kv.clone(),
                                    handles,
                                    crate::kv_table::RowDecodeContext::for_query(ctx),
                                );
                                if let Some(trace) = trace.as_deref_mut() {
                                    trace.point_get(&visible, kv, handle.as_ref(), None);
                                }
                                Box::new(source)
                            }
                            Some(crate::driver::leaf_access::LeafAccessPath::Index(path)) => {
                                walked_index = Some(path.order().to_vec());
                                cost_candidate = Some(path.candidate().clone());
                                access_residual_filter = path.index_filter().cloned();
                                if let Some(filter) = access_residual_filter.as_ref() {
                                    let mut traced = Vec::new();
                                    crate::plan_trace::collect_and(filter, &mut traced);
                                    path_traced_residual_filters =
                                        traced.into_iter().cloned().collect();
                                }
                                path_residual_filters = Some(path.residual_filters().to_vec());
                                let source = crate::driver::leaf_access::leaf_index_source(
                                    kv,
                                    &visible,
                                    &columns,
                                    path,
                                    trace.as_deref_mut(),
                                    ctx,
                                );
                                source
                            }
                            Some(crate::driver::leaf_access::LeafAccessPath::Table {
                                ranges,
                                estimate,
                                residual_filters,
                                candidate,
                            }) => {
                                cost_candidate = Some(candidate);
                                path_residual_filters = Some(residual_filters.clone());
                                let mut source = TableScanExec::new_with_context(
                                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                                    restricted_to_partitions(kv, &table_ref.partitions, name)?,
                                    crate::kv_table::RowDecodeContext::for_query(ctx),
                                    crate::remote_scan::PushdownStatementContext::from_stmt(ctx),
                                );
                                if keep_order
                                    && !source.table_access().is_some_and(|access| {
                                        access
                                            .accept_keep_order(required.sort_desc_for_keep_order())
                                    })
                                {
                                    return Err(DriverError::unsupported(
                                        "table scan cannot satisfy the required order",
                                    ));
                                }
                                if let Some(access) = source.table_access() {
                                    access.accept_scan_estimate(estimate.rows);
                                }
                                let accepted = ranges.as_ref().is_some_and(|ranges| {
                                    source
                                        .table_access()
                                        .is_some_and(|access| access.accept_handle_ranges(ranges))
                                });
                                if accepted {
                                    path_traced_residual_filters = residual_filters;
                                    access_residual_filter =
                                        leaf_where.as_ref().and_then(|predicate| {
                                            crate::handle_range::build_handle_ranges(
                                                kv,
                                                predicate,
                                                &ctx.session_zone(),
                                            )?
                                            .residual
                                            .into_iter()
                                            .cloned()
                                            .reduce(
                                                |left, right| {
                                                    tidb_ast::Expr::Binary(
                                                        tidb_ast::BinaryOp::LogicAnd,
                                                        Box::new(left),
                                                        Box::new(right),
                                                    )
                                                },
                                            )
                                        });
                                    if let (Some(trace), Some(ranges)) =
                                        (trace.as_deref_mut(), ranges.as_ref())
                                    {
                                        if ranges.is_empty() {
                                            trace.empty_range_table_dual();
                                        } else {
                                            // Go `find_best_task.go:2194`: fix
                                            // control 52592 sets
                                            // `canConvertPointGet = false`, so
                                            // a single-point table range stays
                                            // a TableRangeScan instead of
                                            // converting to a Point_Get.
                                            let allow_point_get = !ctx
                                                .optimizer_fix_control()
                                                .get_bool_with_default(
                                                    tidb_planner::fix_control::FIX_52592,
                                                    false,
                                                );
                                            let point_handle = if allow_point_get {
                                                kv.pk_handle_offset().and_then(|_| {
                                                    crate::driver::access::single_point_handle(
                                                        &ranges,
                                                    )
                                                })
                                            } else {
                                                None
                                            };
                                            match point_handle {
                                                Some(handle) => {
                                                    trace.point_get(
                                                        &visible,
                                                        kv,
                                                        Some(&handle),
                                                        None,
                                                    );
                                                }
                                                None => {
                                                    trace.table_range_scan(
                                                        &visible,
                                                        &ranges,
                                                        estimate,
                                                    );
                                                    if keep_order {
                                                        trace.keep_order(false);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Box::new(source)
                            }
                            None => {
                                let mut source = TableScanExec::new_with_context(
                                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                                    restricted_to_partitions(kv, &table_ref.partitions, name)?,
                                    crate::kv_table::RowDecodeContext::for_query(ctx),
                                    // The one production build site: the statement's own
                                    // `DAGRequest.flags` and its warning sink, taken
                                    // together from the context that decided both.
                                    crate::remote_scan::PushdownStatementContext::from_stmt(ctx),
                                );
                                if keep_order
                                    && !source.table_access().is_some_and(|access| {
                                        access
                                            .accept_keep_order(required.sort_desc_for_keep_order())
                                    })
                                {
                                    return Err(DriverError::unsupported(
                                        "table scan cannot satisfy the required order",
                                    ));
                                }
                                crate::table_access::TableAccess::accept_scan_estimate(
                                    &mut source,
                                    crate::driver::access::full_scan_estimate(catalog, entry).rows,
                                );
                                Box::new(source)
                            }
                        }
                    }
                }
                // Handled above, before the columns were taken.
                TableEntry::View(_) | TableEntry::Sequence(_) => {
                    unreachable!("views and sequences take the branches above")
                }
            };
            // Go's `PartitionProcessor` walks the WHOLE logical plan
            // (`rewriteDataSource` recurses through every operator) and
            // replaces EVERY partitioned `DataSource` with one per surviving
            // partition, so a partitioned table read as a join leaf fans out
            // exactly as a single-table `SELECT`'s source does. Doing it only
            // for the single-table shape (`run_select_stmt`'s own call) is
            // what printed one partition-less `TableFullScan table:tx2` where
            // TiDB prints `partition:p1` and `partition:p2` under a
            // `PartitionUnion(Probe)`. Like that call, this fires only under
            // `@@tidb_partition_prune_mode = 'static'`; dynamic pruning keeps
            // the one scan.
            if ctx.static_partition_prune() && demand.partition_fan_out {
                if let (Some(trace), TableEntry::Kv(kv)) = (trace.as_deref_mut(), entry) {
                    let read = crate::driver::access::leaf_read_partitions(
                        kv,
                        &table_ref.partitions,
                    );
                    let estimates =
                        crate::driver::access::surviving_partition_estimates(catalog, &read);
                    let names: Vec<String> =
                        read.iter().map(|(name, _)| name.clone()).collect();
                    trace.partition_union(&names, &estimates);
                }
            }
            // Record predicate consumption only from the physical path that
            // was actually built. A point path consumes its exact key; a
            // streaming source may accept the complete pushed filter. Any
            // declined residue remains in the Selection above the join.
            let scan_consumed_filter =
                offer_leaf_filter(exec.as_mut(), leaf_where.as_ref(), &visible, &columns, ctx);
            let scan_residual_filter = (scan_consumed_filter && access_residual_filter.is_none())
                .then(|| {
                    path_residual_filters.as_ref().and_then(|residuals| {
                        let mut unique = Vec::with_capacity(residuals.len());
                        for residual in residuals {
                            if !unique.contains(residual) {
                                unique.push(residual.clone());
                            }
                        }
                        unique.into_iter().reduce(|left, right| {
                            tidb_ast::Expr::Binary(
                                tidb_ast::BinaryOp::LogicAnd,
                                Box::new(left),
                                Box::new(right),
                            )
                        })
                    })
                })
                .flatten();
            // Go's second ColumnPruner pass reaches every DataSource after join
            // reorder. LeafDemand is a conservative, statement-wide name walk,
            // so offering its result here is safe for nested joins too: every
            // expression is still AST-only and will resolve against the final
            // compact scopes built by the parents.
            if let Some(keep) = demand
                .columns
                .map(|wanted| wanted.needed(&visible, &columns))
                .filter(|keep| !keep.is_empty() && keep.len() < columns.len())
            {
                let accepted = exec
                    .table_access()
                    .is_some_and(|access| access.accept_column_prune(&keep));
                if accepted {
                    if let Some(order) = walked_index.as_mut() {
                        *order = order
                            .iter()
                            .map_while(|offset| keep.iter().position(|kept| kept == offset))
                            .collect();
                    }
                    columns = keep.iter().map(|offset| columns[*offset].clone()).collect();
                }
            }
            // Go appends the extra handle column to a heap table's
            // `DataSource` schema (`buildDataSource`: no `PKIsHandle` column
            // and not `IsCommonHandle` -> `NewExtraHandleSchemaCol`), where
            // it names the record HANDLE rather than any stored column. It
            // goes on AFTER pruning for the same reason Go's survives it:
            // the pruner works in stored offsets and this slot has none.
            let extra_handle = demand
                .all_names
                .is_some_and(|wanted| wanted.names_extra_handle(&visible))
                .then(|| extra_handle_column(entry))
                .flatten();
            if let Some(handle_column) = extra_handle {
                let slot = columns.len();
                if exec
                    .table_access()
                    .is_some_and(|access| access.accept_extra_handle(slot))
                {
                    columns.push(handle_column);
                    extra_handle_offset = Some(slot);
                }
            }
            if leaf_where.is_some() {
                if let Some(rows) = demand.rows {
                    if access_consumed_filter || scan_consumed_filter {
                        rows.mark_leaf_filters_consumed(&visible);
                    } else if let Some(residuals) = path_residual_filters {
                        rows.record_leaf_filter_residuals(
                            &visible,
                            residuals,
                            path_traced_residual_filters,
                        );
                    }
                }
            }
            // The leaf's final row layout by name, after logical pruning.
            let column_names: Vec<String> = columns.iter().map(|(name, _)| name.clone()).collect();
            // `unfoldWildStar` skips `model.ExtraHandleID`, so `*` expands to
            // the stored columns alone even while `_tidb_rowid` sits beside
            // them. Naming the surviving offsets is how this scope says that.
            let star = match extra_handle_offset {
                Some(handle) => (0..columns.len()).filter(|at| *at != handle).collect(),
                None => Vec::new(),
            };
            let scope = FromScope {
                tables: vec![FromTable {
                    name: visible.clone(),
                    // An alias replaces the whole path, so `db.t.col` no
                    // longer names the table once it is aliased.
                    database: table_ref.alias.is_none().then(|| database.to_owned()),
                    // ... but Go still PRINTS the base table, so keep it.
                    physical: table_ref.alias.is_some().then(|| name.to_owned()),
                    func_deps: table_func_deps(entry, &columns),
                    columns,
                    offset: 0,
                }],
                star,
                ..FromScope::for_statement(ctx)
            };
            let derived_trace_filter = demand
                .pushdown
                .is_none()
                .then(|| {
                    demand.rows.and_then(|rows| {
                        rows.trace_filters_for(&visible)?
                            .iter()
                            .cloned()
                            .reduce(|left, right| {
                                tidb_ast::Expr::Binary(
                                    tidb_ast::BinaryOp::LogicAnd,
                                    Box::new(left),
                                    Box::new(right),
                                )
                            })
                    })
                })
                .flatten();
            if access_residual_filter.is_none() && derived_trace_filter.is_none() {
                access_residual_filter = scan_residual_filter;
            }
            let has_access_residual = access_residual_filter.is_some();
            let trace_filter = match (access_residual_filter, derived_trace_filter) {
                (Some(left), Some(right)) if left == right => Some(left),
                (Some(left), Some(right)) => Some(tidb_ast::Expr::Binary(
                    tidb_ast::BinaryOp::LogicAnd,
                    Box::new(left),
                    Box::new(right),
                )),
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            };
            let built_trace_filter = trace_filter.as_ref().and_then(|predicate| {
                let resolver = ScopeResolver { scope: &scope };
                let mut expression = rewrite_expr_resolved(predicate, &resolver).ok()?;
                tidb_expr::builtin_compare::refine_comparisons(&mut expression, ctx).ok()?;
                Some(vec![expression])
            });
            let physical_column_names = (0..scope.width())
                .map(|offset| {
                    let path = scope.qualified_path(offset)?;
                    let [.., relation_name, column_name] = path.as_slice() else {
                        return None;
                    };
                    crate::driver::merge_decision::physical_column_trace_name(
                        node,
                        &crate::driver::merge_decision::RelColumn {
                            relation: relation_name.clone(),
                            column: column_name.clone(),
                        },
                        catalog,
                        current_db,
                    )
                })
                .collect::<Vec<_>>();
            let trace_selectivity = trace_filter.as_ref().and_then(|predicate| match entry {
                TableEntry::Kv(table) if catalog.table_statistics(table.stats_physical_id()).is_some() => {
                    crate::driver::access::stats_selectivity_with_default_string_match_selectivity(
                        catalog,
                        table,
                        &scope,
                        Some(predicate),
                        ctx.default_string_match_selectivity(),
                    )
                }
                TableEntry::Kv(_) => None,
                TableEntry::Mem(_)
                | TableEntry::Cte(_)
                | TableEntry::View(_)
                | TableEntry::Sequence(_) => Some(1.0),
            });
            if let (
                Some(predicate),
                Some(tidb_planner::candidate_cost::Candidate::Fixed { rows, .. }),
            ) = (trace_filter.as_ref(), cost_candidate.as_mut())
            {
                let rate = trace_selectivity
                    .unwrap_or_else(|| crate::plan_trace::pseudo_selectivity(predicate));
                *rows = if has_access_residual {
                    (*rows * rate).max(1.0)
                } else {
                    *rows * rate
                };
            }
            if let Some(predicate) = trace_filter.as_ref() {
                if let Some(trace) = trace.as_deref_mut() {
                    let qualify = crate::plan_trace::Qualifier {
                        db: current_db,
                        scope: &scope,
                        catalog: Some(catalog),
                    };
                    if has_access_residual {
                        trace.residual_selection_with_columns(
                            &predicate,
                            built_trace_filter.as_deref(),
                            &qualify,
                            None,
                            trace_selectivity,
                            &physical_column_names,
                        );
                    } else if let Some([built]) = built_trace_filter.as_deref() {
                        if !trace.physical_selection_with_columns(
                            built,
                            predicate,
                            trace_selectivity,
                            &physical_column_names,
                        ) {
                            trace.selection(
                                &predicate,
                                built_trace_filter.as_deref(),
                                &qualify,
                                trace_selectivity,
                            );
                        }
                    } else {
                        trace.selection(
                            &predicate,
                            built_trace_filter.as_deref(),
                            &qualify,
                            trace_selectivity,
                        );
                    }
                }
            }
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
            let mut delivered = match &walked_index {
                Some(order) if !order.is_empty() => Delivered::from_orders(vec![order.clone()]),
                Some(_) => Delivered::new(),
                None if keep_order => Delivered::from_orders(
                    crate::driver::merge_decision::table_scan_orders(entry, &column_names),
                ),
                None => Delivered::new(),
            };
            delivered.candidate = cost_candidate;
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
            let leaf_filter = alias.as_deref().and_then(|visible| {
                demand.rows.and_then(|rows| {
                    rows.filters_for(visible)?
                        .iter()
                        .cloned()
                        .reduce(|left, right| {
                            tidb_ast::Expr::Binary(
                                tidb_ast::BinaryOp::LogicAnd,
                                Box::new(left),
                                Box::new(right),
                            )
                        })
                })
            });
            let rewritten_subquery = alias.as_deref().and_then(|visible| {
                super::derived_projection_pushdown::push_filters_into_derived(
                    subquery,
                    visible,
                    column_names,
                    leaf_filter.as_ref()?,
                )
            });
            let planned_subquery = rewritten_subquery.as_ref().unwrap_or(subquery);
            let (mut exec, mut scope, mut actual_delivered) = build_derived_source(
                planned_subquery,
                alias.as_deref(),
                catalog,
                current_db,
                ctx,
                trace.as_deref_mut(),
                required,
            )?;
            if rewritten_subquery.is_some() {
                if let (Some(rows), Some(visible)) = (demand.rows, alias.as_deref()) {
                    rows.mark_leaf_filters_consumed(visible);
                }
            }
            rename_derived_columns(&mut scope.tables[0].columns, column_names)?;
            let physical_column_names = (0..scope.width())
                .map(|offset| {
                    let path = scope.qualified_path(offset)?;
                    let [.., relation_name, column_name] = path.as_slice() else {
                        return None;
                    };
                    crate::driver::merge_decision::physical_column_trace_name(
                        node,
                        &crate::driver::merge_decision::RelColumn {
                            relation: relation_name.clone(),
                            column: column_name.clone(),
                        },
                        catalog,
                        current_db,
                    )
                })
                .collect::<Vec<_>>();
            // A join key can leave a derived-table `not(isnull(output))`
            // demand behind even when the derived output is statically
            // NOT NULL (for example a grouped non-null key). Go's
            // `PredicatePushDown` removes that constant predicate before
            // physical planning; keeping it here would add a redundant root
            // Selection and charge its selectivity twice.
            let redundant_not_null_filter = leaf_filter.as_ref().is_some_and(|predicate| {
                let tidb_ast::Expr::Is {
                    expr,
                    target: tidb_ast::IsTarget::Null,
                    not: true,
                } = predicate
                else {
                    return false;
                };
                let tidb_ast::Expr::Column(path) = expr.as_ref() else {
                    return false;
                };
                ScopeResolver { scope: &scope }
                    .resolve(path)
                    .is_some_and(|(_, field_type, _)| {
                        field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
                    })
            });
            if rewritten_subquery.is_none() && !redundant_not_null_filter {
                if let Some(predicate) = leaf_filter.as_ref() {
                    let resolver = ScopeResolver { scope: &scope };
                    let mut built = rewrite_expr_resolved(predicate, &resolver)
                        .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
                    tidb_expr::builtin_compare::refine_comparisons(&mut built, ctx)
                        .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
                    let input_rows = actual_delivered.candidate.as_ref().map(|candidate| {
                        tidb_planner::candidate_cost::evaluate(
                            candidate,
                            &tidb_planner::candidate_cost::CostEnv::default(),
                            tidb_planner::task_type::TaskType::Root,
                        )
                        .rows
                    });
                    if let (Some(child), Some(input_rows)) =
                        (actual_delivered.candidate.take(), input_rows)
                    {
                        actual_delivered.candidate =
                            Some(tidb_planner::candidate_cost::Candidate::Selection {
                                child: Box::new(child),
                                input_rows,
                                conditions: vec![matches!(built, Expression::ScalarFunction(_))],
                            });
                    }
                    exec = Box::new(SelectionExec::new(
                        ExecutorMeta::new(exec.schema().clone(), 1, INIT_CAP, MAX_CHUNK_SIZE),
                        vec![built.clone()],
                        exec,
                        ctx.clone(),
                        ctx.statement_memory(),
                    ));
                    if let (Some(rows), Some(visible)) = (demand.rows, alias.as_deref()) {
                        rows.mark_leaf_filters_consumed(visible);
                    }
                    if let Some(trace) = trace.as_deref_mut() {
                        if !trace.physical_selection_with_columns(
                            &built,
                            predicate,
                            Some(crate::plan_trace::SELECTIVITY_FACTOR),
                            &physical_column_names,
                        ) {
                            trace.selection(
                                predicate,
                                Some(std::slice::from_ref(&built)),
                                &crate::plan_trace::Qualifier {
                                    db: current_db,
                                    scope: &scope,
                                    catalog: Some(catalog),
                                },
                                Some(crate::plan_trace::SELECTIVITY_FACTOR),
                            );
                        }
                        exec = trace.meter(exec);
                    }
                }
            }
            // A derived table is MATERIALIZED here -- `build_derived_source`
            // drains its subquery into a `MemTableSourceExec`, which replays
            // the rows in the order they arrived -- so what it delivers is
            // what its inner `FROM` delivered, projected through its select
            // list. `Phase::Delivered` is that walk, and it is a conservative
            // LOWER bound on the inner build (see its doc): the inner join
            // forms its candidates from the PROMISE and verifies them the same
            // way this one does, so it can only deliver more.
            let delivered = if actual_delivered.is_empty() {
                let mut delivered = Delivered::from_orders(
                    crate::driver::merge_decision::delivered_properties(
                        node,
                        catalog,
                        current_db,
                        demand.offered,
                    )
                    .map(|properties| properties.orders)
                    .unwrap_or_default(),
                );
                delivered.candidate = actual_delivered.candidate;
                delivered
            } else {
                actual_delivered
            };
            Ok((exec, scope, delivered))
        }
    }
}

/// Offers a join leaf's local predicates to the storage source. Returns true
/// only when that source accepted the complete predicate.
fn offer_leaf_filter(
    source: &mut dyn Executor,
    where_clause: Option<&tidb_ast::Expr>,
    visible: &str,
    columns: &[(String, FieldType)],
    ctx: &crate::StmtContext,
) -> bool {
    let Some(where_clause) = where_clause else {
        return false;
    };
    let resolver = TableResolver {
        table_name: visible,
        columns,
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
    };
    let (pushed, residual) =
        crate::driver::access::split_scan_predicates(where_clause, &resolver, ctx);
    !pushed.is_empty()
        && residual.is_none()
        && source
            .table_access()
            .is_some_and(|access| access.accept_scan_filter(&pushed, ctx))
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
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    let mut delivered = Delivered::new();
    let mut deferred_exec = None;
    let (alias, columns, rows) = derived_source_relation_with_delivery(
        subquery,
        alias,
        catalog,
        current_db,
        ctx,
        trace,
        required,
        Some(&mut delivered),
        Some(&mut deferred_exec),
    )?;
    let schema_columns: Vec<Column> = columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let exec: Box<dyn Executor> = deferred_exec.unwrap_or_else(|| {
        Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
            rows,
        ))
    });
    let scope = FromScope {
        tables: vec![FromTable {
            name: alias.to_owned(),
            // An alias is the only qualifier a derived table answers to.
            database: None,
            columns,
            offset: 0,
            func_deps: Default::default(),
            physical: None,
        }],
        ..FromScope::for_statement(ctx)
    };
    Ok((exec, scope, delivered))
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
    derived_source_relation_with_delivery(
        subquery, alias, catalog, current_db, ctx, trace, required, None, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn derived_source_relation_with_delivery<'a>(
    subquery: &QueryStmt,
    alias: Option<&'a str>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    delivered: Option<&mut Delivered>,
    deferred_exec: Option<&mut Option<Box<dyn Executor>>>,
) -> Result<DerivedSourceRelation<'a>, DriverError> {
    // Captured from Go: an alias-less derived table is ErrDerivedMustHaveAlias
    // in a plain SELECT and in a view body alike.
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };
    let (columns, rows) = match subquery {
        QueryStmt::Select(select) => super::run_select_traced_with_delivery(
            select,
            catalog,
            current_db,
            ctx,
            trace.as_deref_mut(),
            required,
            delivered,
            deferred_exec,
            false,
        )?,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CostedJoinChoice {
    Merge,
    Index {
        kind: tidb_planner::plan_cost_ver2::IndexJoinKind,
        lookup_is_left: bool,
        decision_index: usize,
    },
    Hash {
        build_is_left: bool,
    },
}

fn index_probe_candidate(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    catalog: &Catalog,
    output_types: &[FieldType],
    logical_output_rows: f64,
    source_rows: f64,
) -> tidb_planner::candidate_cost::Candidate {
    // Go threads AvgInnerRowCnt through a retained aggregation. The aggregate
    // scales that logical output expectation back to its filtered child rows;
    // the data-source builders then remove only residual path selectivity.
    let after_filter = source_rows.max(0.0);
    let stats = catalog.table_statistics(decision.table.stats_physical_id());
    let access_rows_floor = decision.probe_access_rows_floor(stats.map(AsRef::as_ref));
    let physical_output_rows = if decision.aggregation.is_some() && access_rows_floor > 0.0 {
        source_rows
    } else {
        logical_output_rows
    };
    let mut base_rows = if decision.filter_selectivity > 0.0 {
        after_filter / decision.filter_selectivity
    } else {
        after_filter
    };
    base_rows = base_rows.max(access_rows_floor);
    if decision.max_one_row() {
        base_rows = base_rows.min(1.0);
    }
    let needed_columns = decision.aggregation.as_ref().map_or_else(
        || (0..decision.columns.len()).collect::<Vec<_>>(),
        |aggregation| {
            let mut offsets = aggregation.group_offsets.clone();
            offsets.extend(aggregation.input_offsets.iter().copied());
            offsets.sort_unstable();
            offsets.dedup();
            offsets
        },
    );
    let source_output_types = decision.aggregation.as_ref().map_or_else(
        || output_types.to_vec(),
        |_| {
            needed_columns
                .iter()
                .filter_map(|offset| decision.columns.get(*offset))
                .map(|column| column.1.clone())
                .collect::<Vec<_>>()
        },
    );
    let source_row_size = crate::access_cost::schema_avg_row_size(&source_output_types);
    // A synthetic pruned COUNT keeps Go's table-range candidate on the data
    // source's broad TblCols, while secondary-index coverage still uses the
    // narrow logical schema. Carry the pruning decision explicitly: output
    // order is SELECT-list order and cannot identify this branch.
    let table_scan_columns = if decision
        .aggregation
        .as_ref()
        .is_some_and(|aggregation| aggregation.pruned_row_count)
    {
        (0..decision.columns.len()).collect::<Vec<_>>()
    } else {
        needed_columns.clone()
    };
    let cost = crate::access_cost::index_join_probe_cost(
        &decision.table,
        &decision.object,
        base_rows,
        after_filter,
        &needed_columns,
        &table_scan_columns,
        source_row_size,
        decision.filters.len(),
        stats.as_deref().map(AsRef::as_ref),
    );
    // A retained aggregation has its own physical schema. In particular,
    // LogicalAggregation.PruneColumns appends COUNT(1) when pruning would
    // otherwise leave only FIRST_ROW carriers, so the inner hash-table width
    // cannot be reduced to the join keys requested by the parent.
    let aggregation_output_types = decision.aggregation.as_ref().and_then(|aggregation| {
        aggregation
            .outputs
            .iter()
            .map(|output| match output {
                crate::join::IndexLookupAggregateOutput::Column(offset)
                | crate::join::IndexLookupAggregateOutput::Max { offset, .. }
                | crate::join::IndexLookupAggregateOutput::DecimalSum(offset) => {
                    decision.columns.get(*offset).map(|column| column.1.clone())
                }
                crate::join::IndexLookupAggregateOutput::Count(_) => {
                    Some(FieldType::new(tidb_datatype::FieldTypeCode::LongLong))
                }
            })
            .collect::<Option<Vec<_>>>()
    });
    let output_types = aggregation_output_types.as_deref().unwrap_or(output_types);
    let source = tidb_planner::candidate_cost::Candidate::Fixed {
        rows: after_filter,
        // The scan cost above uses its storage width. The reader has already
        // column-pruned the physical child exposed to a retained aggregation.
        row_size: source_row_size,
        cost,
        num_ranges: 1,
    };
    let Some(aggregation) = &decision.aggregation else {
        return source;
    };
    // LogicalAggregation.DeriveStats creates a new StatsInfo without a
    // HistColl. Its physical StreamAgg/HashAgg therefore reaches Go's static
    // type-width branch in getAvgRowSize, including when an IndexJoin runtime
    // property is threaded through the aggregation to the data source below.
    let row_size = crate::access_cost::schema_avg_row_size(output_types);
    if decision.aggregation_stream_ordered() {
        tidb_planner::candidate_cost::Candidate::StreamAgg {
            child: Box::new(source),
            input_rows: after_filter,
            output_rows: physical_output_rows,
            row_size: tidb_planner::candidate_cost::RowSize::Fixed(row_size),
            num_agg_funcs: aggregation.outputs.len(),
            group_items: vec![false; aggregation.group_offsets.len()],
        }
    } else {
        tidb_planner::candidate_cost::Candidate::HashAgg {
            child: Box::new(source),
            input: tidb_planner::plan_cost_ver2::HashAggInput {
                input_rows: after_filter,
                output_rows: physical_output_rows,
                output_row_size: row_size,
                num_agg_funcs: aggregation.outputs.len(),
                child_can_provide_order: false,
            },
            group_items: vec![false; aggregation.group_offsets.len()],
        }
    }
}

fn fixed_join_receipt(
    candidate: tidb_planner::candidate_cost::Candidate,
    rows: f64,
    row_size: f64,
) -> tidb_planner::candidate_cost::Candidate {
    let num_ranges = tidb_planner::candidate_cost::number_of_ranges(&candidate);
    let cost = tidb_planner::candidate_cost::evaluate(
        &candidate,
        &tidb_planner::candidate_cost::CostEnv::default(),
        tidb_planner::task_type::TaskType::Root,
    )
    .est_cost();
    tidb_planner::candidate_cost::Candidate::Fixed {
        rows,
        row_size,
        cost,
        num_ranges,
    }
}

#[derive(Clone)]
struct PushedLeafSelection {
    conditions: Vec<bool>,
    unmodeled_selectivity: Option<f64>,
}

fn attach_pushed_leaf_selection(
    candidate: Option<tidb_planner::candidate_cost::Candidate>,
    selection: Option<&PushedLeafSelection>,
) -> Option<tidb_planner::candidate_cost::Candidate> {
    let Some(selection) = selection else {
        return candidate;
    };
    candidate.map(|candidate| {
        let input_rows = tidb_planner::candidate_cost::evaluate(
            &candidate,
            &tidb_planner::candidate_cost::CostEnv::default(),
            tidb_planner::task_type::TaskType::Root,
        )
        .rows;
        tidb_planner::candidate_cost::Candidate::Selection {
            child: Box::new(candidate),
            input_rows,
            conditions: selection.conditions.clone(),
        }
    })
}

/// The logical schema width Go leaves after `PruneColumns` for one contiguous
/// side of a join. The executor keeps its original row layout, so this helper
/// returns only the types used by cost model receipts.
fn logical_cost_types(
    scope: &FromScope,
    demand: Option<&crate::driver::leaf_demand::LeafDemand>,
    range: std::ops::Range<usize>,
) -> Vec<FieldType> {
    let mut all = Vec::new();
    let mut needed = Vec::new();
    for table in &scope.tables {
        for (offset, (_, field_type)) in table.columns.iter().enumerate() {
            let absolute = table.offset + offset;
            if range.contains(&absolute) {
                all.push(field_type.clone());
            }
        }
        let Some(demand) = demand else {
            continue;
        };
        for offset in demand.needed(&table.name, &table.columns) {
            let absolute = table.offset + offset;
            if range.contains(&absolute) {
                needed.push(table.columns[offset].1.clone());
            }
        }
    }
    let Some(_) = demand else {
        return all;
    };
    if !needed.is_empty() || all.is_empty() {
        return needed;
    }
    // LogicalSchemaProducer.InlineProjection keeps the first column with the
    // smallest declared length when a parent needs no output column.
    all.into_iter()
        .min_by_key(|field_type| field_type.flen())
        .into_iter()
        .collect()
}

fn candidate_row_size(
    candidate: Option<&tidb_planner::candidate_cost::Candidate>,
    fallback_types: &[FieldType],
) -> f64 {
    candidate.map_or_else(
        || crate::access_cost::schema_avg_row_size(fallback_types),
        |candidate| {
            tidb_planner::candidate_cost::evaluate(
                candidate,
                &tidb_planner::candidate_cost::CostEnv::default(),
                tidb_planner::task_type::TaskType::Root,
            )
            .row_size
        },
    )
}

fn merge_join_candidate(
    left: tidb_planner::candidate_cost::Candidate,
    right: tidb_planner::candidate_cost::Candidate,
    rows: crate::driver::join_reorder::JoinRows,
    num_join_keys: usize,
    num_other_conditions: usize,
) -> tidb_planner::candidate_cost::Candidate {
    tidb_planner::candidate_cost::Candidate::MergeJoin {
        left: Box::new(left),
        right: Box::new(right),
        child_rows: (rows.left, rows.right),
        left_conditions: Vec::new(),
        right_conditions: Vec::new(),
        other_conditions: vec![true; num_other_conditions],
        num_join_keys: (num_join_keys, num_join_keys),
    }
}

fn hash_join_candidate(
    left: tidb_planner::candidate_cost::Candidate,
    right: tidb_planner::candidate_cost::Candidate,
    num_join_keys: usize,
    build_is_left: bool,
    concurrency: f64,
) -> tidb_planner::candidate_cost::Candidate {
    // Go prices a PhysicalHashJoin from getCardinality(build/probe), not from
    // the logical join-reorder groups that produced the parent estimate. A
    // projection, aggregation, or pushed Selection may have changed either
    // physical child's rows by the time this candidate is attached.
    let env = tidb_planner::candidate_cost::CostEnv::default();
    let left_costed = tidb_planner::candidate_cost::evaluate(
        &left,
        &env,
        tidb_planner::task_type::TaskType::Root,
    );
    let right_costed = tidb_planner::candidate_cost::evaluate(
        &right,
        &env,
        tidb_planner::task_type::TaskType::Root,
    );
    let (build, probe, build_rows, probe_rows, build_row_size) = if build_is_left {
        (
            left,
            right,
            left_costed.rows,
            right_costed.rows,
            left_costed.row_size,
        )
    } else {
        (
            right,
            left,
            right_costed.rows,
            left_costed.rows,
            right_costed.row_size,
        )
    };
    tidb_planner::candidate_cost::Candidate::HashJoin {
        build: Box::new(build),
        probe: Box::new(probe),
        input: tidb_planner::plan_cost_ver2::HashJoinInput {
            build_rows,
            probe_rows,
            build_row_size,
            num_build_keys: num_join_keys,
            num_probe_keys: num_join_keys,
            // Go's `getHashJoins` stamps the candidate with
            // `sctx.GetSessionVars().HashJoinConcurrency()`, and
            // `getPlanCostVer24PhysicalHashJoin` divides the probe filter and
            // probe hash by it. mysql-tester's DSN pins it to 1 in every
            // connection the recordings were made from (a plain session
            // resolves 5), so hardcoding either value prices a DIFFERENT
            // session than the one being replayed: at 5 a hash join is
            // charged what five workers share, and the recorded
            // IndexHashJoin/MergeJoin picks lose to it.
            tidb_concurrency: concurrency,
        },
        build_filters: Vec::new(),
        probe_filters: Vec::new(),
    }
}

fn index_join_candidate(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    outer: tidb_planner::candidate_cost::Candidate,
    inner: Option<&tidb_planner::candidate_cost::Candidate>,
    catalog: &Catalog,
    rows: crate::driver::join_reorder::JoinRows,
    matched_rows: crate::driver::join_reorder::JoinRows,
    left_types: &[FieldType],
    right_types: &[FieldType],
    num_join_keys: usize,
    kind: tidb_planner::plan_cost_ver2::IndexJoinKind,
    is_semi_join: bool,
) -> tidb_planner::candidate_cost::Candidate {
    let inner_types = if decision.lookup_is_left {
        left_types
    } else {
        right_types
    };
    let probe_rows = if is_semi_join { matched_rows } else { rows };
    let logical_probe_rows_one = index_join_probe_rows_one(decision, probe_rows);
    let source_rows_one = index_join_physical_probe_rows_one(decision, catalog, probe_rows);
    let probe = index_probe_candidate(
        decision,
        catalog,
        inner_types,
        logical_probe_rows_one,
        source_rows_one,
    );
    let probe_costed = tidb_planner::candidate_cost::evaluate(
        &probe,
        &tidb_planner::candidate_cost::CostEnv::default(),
        tidb_planner::task_type::TaskType::Root,
    );
    let outer_costed = tidb_planner::candidate_cost::evaluate(
        &outer,
        &tidb_planner::candidate_cost::CostEnv::default(),
        tidb_planner::task_type::TaskType::Root,
    );
    // Go rebuilds a composite inner child under IndexJoinProp. The lookup
    // leaf contributes one dynamic range, while every other operator in the
    // subtree is executed for each outer batch. Carry that complete child
    // receipt into the candidate so its cost and per-probe cardinality are
    // not reduced to the lookup leaf alone.
    let probe = if decision.composite {
        inner.map_or(probe, |inner| {
            let inner_costed = tidb_planner::candidate_cost::evaluate(
                inner,
                &tidb_planner::candidate_cost::CostEnv::default(),
                tidb_planner::task_type::TaskType::Root,
            );
            tidb_planner::candidate_cost::Candidate::Fixed {
                rows: inner_costed.rows * outer_costed.rows,
                row_size: inner_costed.row_size,
                cost: probe_costed.est_cost() + inner_costed.est_cost(),
                num_ranges: tidb_planner::candidate_cost::number_of_ranges(inner),
            }
        })
    } else {
        probe
    };
    let probe_costed = tidb_planner::candidate_cost::evaluate(
        &probe,
        &tidb_planner::candidate_cost::CostEnv::default(),
        tidb_planner::task_type::TaskType::Root,
    );
    tidb_planner::candidate_cost::Candidate::IndexJoin {
        build: Box::new(outer),
        probe: Box::new(probe),
        input: tidb_planner::plan_cost_ver2::IndexJoinInput {
            build_rows: outer_costed.rows,
            build_row_size: outer_costed.row_size,
            probe_rows_one: probe_costed.rows,
            probe_row_size: probe_costed.row_size,
            num_right_join_keys: num_join_keys,
            num_left_join_keys: num_join_keys,
            num_ranges: 0.0,
            is_semi_join,
            kind,
        },
        output_rows: rows.joined,
        build_filters: Vec::new(),
        probe_filters: Vec::new(),
    }
}

fn runtime_probe_candidate(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    catalog: &Catalog,
    rows: crate::driver::join_reorder::JoinRows,
    inner_types: &[FieldType],
) -> tidb_planner::candidate_cost::Candidate {
    let logical_probe_rows_one = index_join_probe_rows_one(decision, rows);
    let source_rows_one = index_join_physical_probe_rows_one(decision, catalog, rows);
    index_probe_candidate(
        decision,
        catalog,
        inner_types,
        logical_probe_rows_one,
        source_rows_one,
    )
}

/// Go's `AvgInnerRowCnt`: the rows returned by one dynamically rebuilt inner
/// task, not the cardinality of the complete logical inner relation. A unique
/// object lookup has at most one pre-filter row, so its residual selectivity
/// is the complete per-probe estimate.
fn index_join_probe_rows_one(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    rows: crate::driver::join_reorder::JoinRows,
) -> f64 {
    if decision.max_one_row() {
        return decision.filter_selectivity.clamp(0.0, 1.0);
    }
    let outer_rows = if decision.lookup_is_left {
        rows.right
    } else {
        rows.left
    };
    if outer_rows > 0.0 {
        rows.joined / outer_rows
    } else {
        0.0
    }
}

/// Rows from the filtered base-table source consumed by one rebuilt probe.
/// For a retained aggregation this is Go's
/// `AvgInnerRowCnt * childStats.RowCount / aggStats.RowCount`; a bare lookup
/// has no logical/physical boundary and keeps AvgInnerRowCnt unchanged.
fn index_join_source_rows_one(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    catalog: &Catalog,
    rows: crate::driver::join_reorder::JoinRows,
) -> f64 {
    let logical_output_rows = index_join_probe_rows_one(decision, rows);
    if decision.aggregation.is_none() {
        return logical_output_rows;
    }
    let grouped_rows = if decision.lookup_is_left {
        rows.left
    } else {
        rows.right
    };
    let Some(stats) = catalog
        .table_statistics(decision.table.stats_physical_id())
        .filter(|stats| !stats.pseudo && stats.row_count > 0)
    else {
        return logical_output_rows;
    };
    if grouped_rows <= 0.0 {
        return logical_output_rows;
    }
    let source_rows =
        logical_output_rows * stats.row_count as f64 * decision.source_filter_selectivity
            / grouped_rows;
    if decision.probe_access_rows_floor(Some(stats.as_ref())) > 0.0 {
        source_rows * decision.probe_analyzed_scale(Some(stats.as_ref()))
    } else {
        source_rows
    }
}

/// The row count exposed by the rebuilt physical probe root. When the chosen
/// access path cannot encode every equality key, Go keeps the expanded
/// aggregation/source estimate above that path; a complete-key lookup exposes
/// the logical AvgInnerRowCnt instead.
fn index_join_physical_probe_rows_one(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    catalog: &Catalog,
    rows: crate::driver::join_reorder::JoinRows,
) -> f64 {
    let logical = index_join_probe_rows_one(decision, rows);
    let stats = catalog.table_statistics(decision.table.stats_physical_id());
    if decision.aggregation.is_some()
        && decision.probe_access_rows_floor(stats.as_deref().map(AsRef::as_ref)) > 0.0
    {
        index_join_source_rows_one(decision, catalog, rows)
    } else {
        logical
    }
}

fn index_join_kind_name(kind: tidb_planner::plan_cost_ver2::IndexJoinKind) -> &'static str {
    match kind {
        tidb_planner::plan_cost_ver2::IndexJoinKind::IndexJoin => "IndexJoin",
        tidb_planner::plan_cost_ver2::IndexJoinKind::IndexHashJoin => "IndexHashJoin",
        tidb_planner::plan_cost_ver2::IndexJoinKind::IndexMergeJoin => "IndexMergeJoin",
    }
}

fn fallback_index_join_kind(
    decision: &crate::driver::index_join_decision::IndexJoinDecision,
    catalog: &Catalog,
    rows: crate::driver::join_reorder::JoinRows,
    matched_rows: crate::driver::join_reorder::JoinRows,
    left_row_size: f64,
    right_row_size: f64,
    num_join_keys: usize,
) -> tidb_planner::plan_cost_ver2::IndexJoinKind {
    use tidb_planner::plan_cost_ver2::{hash_build_cost, IndexJoinKind, Ver2Factors};
    use tidb_planner::task_type::TaskType;

    let (outer_rows, outer_row_size, inner_row_size) = if decision.lookup_is_left {
        (rows.right, right_row_size, left_row_size)
    } else {
        (rows.left, left_row_size, right_row_size)
    };
    let probe_rows_one = index_join_physical_probe_rows_one(decision, catalog, matched_rows);
    let factors = Ver2Factors::default();
    let cpu = factors.task_cpu(TaskType::Root);
    let memory = factors.task_mem(TaskType::Root);
    let keys = num_join_keys as f64;
    let index_hash = hash_build_cost(None, outer_rows, outer_row_size, keys, cpu, memory);
    let index = hash_build_cost(
        None,
        probe_rows_one * outer_rows,
        inner_row_size,
        keys,
        cpu,
        memory,
    );
    if index_hash.value() < index.value() {
        IndexJoinKind::IndexHashJoin
    } else {
        IndexJoinKind::IndexJoin
    }
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
            ));
        }
        tidb_ast::JoinType::Right => {
            return Err(DriverError::InvalidLateralJoin(
                "RIGHT JOIN is not supported with LATERAL",
            ));
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
        physical: None,
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
    pub(crate) fn enter(qualified: &str) -> Result<ViewDepthGuard, DriverError> {
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
    mut trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    required_columns: Option<&[usize]>,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    let qualified = format!("{database}.{name}");
    let _guard = ViewDepthGuard::enter(&qualified)?;
    let invalid = || DriverError::Schema(SchemaErrorKind::ViewInvalid(qualified.clone()));
    let statement = tidb_parser::parse(&view.select_sql).map_err(|_| invalid())?;
    let tidb_ast::Stmt::Query(mut query) = statement else {
        return Err(invalid());
    };
    if let Some(trace) = trace.as_deref() {
        trace.activate_pre_reserved_query_source(&query);
    }
    let mut visible_columns = view.columns.clone();
    if let Some(required_columns) =
        required_columns.filter(|columns| !columns.is_empty() && columns.len() < view.columns.len())
    {
        if let tidb_ast::QueryStmt::Select(select) = &mut *query {
            let can_prune = !select.distinct
                && select.with.is_none()
                && select.having.is_none()
                && select.order_by.is_empty()
                && select.limit.is_none()
                && select.windows.is_empty()
                && select
                    .fields
                    .fields()
                    .iter()
                    .all(|field| matches!(field, tidb_ast::SelectField::Expr { .. }));
            if can_prune {
                let mut fields = tidb_ast::SelectFieldList::default();
                let mut projected_columns = Vec::with_capacity(required_columns.len());
                for &offset in required_columns {
                    let Some(field) = select.fields.fields().get(offset).cloned() else {
                        return Err(invalid());
                    };
                    let Some(column) = view.columns.get(offset).cloned() else {
                        return Err(invalid());
                    };
                    fields.push_with_text_and_projection_offset(
                        field,
                        select
                            .fields
                            .original_text(offset)
                            .unwrap_or_default()
                            .to_vec(),
                        select.fields.projection_offset(offset),
                    );
                    projected_columns.push(column);
                }
                select.fields = fields;
                visible_columns = projected_columns;
            }
        }
    }
    // A view is Go's derived logical plan with a catalog-owned output schema.
    // Reusing the derived builder for execution as well as plain EXPLAIN keeps
    // its physical candidate receipt available to a surrounding join search.
    let (exec, mut scope, delivered) = build_derived_source(
        &query,
        Some(&visible),
        catalog,
        database,
        ctx,
        trace.as_deref_mut(),
        required,
    )
    .map_err(|_| invalid())?;
    let [table] = scope.tables.as_mut_slice() else {
        return Err(invalid());
    };
    if table.columns.len() != visible_columns.len() {
        return Err(invalid());
    }
    for ((actual_name, _), (view_name, _)) in table.columns.iter_mut().zip(&visible_columns) {
        *actual_name = view_name.clone();
    }
    table.database = alias_free.then(|| database.to_owned());
    Ok((exec, scope, delivered))
}

pub(crate) fn view_source_relation(
    view: &ViewDef,
    database: &str,
    name: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<MaterializedRelation, DriverError> {
    let qualified = format!("{database}.{name}");
    let _guard = ViewDepthGuard::enter(&qualified)?;
    let invalid = || DriverError::Schema(SchemaErrorKind::ViewInvalid(qualified.clone()));
    let (body_columns, rows) =
        run_select_meta_in(&view.select_sql, catalog, database, ctx).map_err(|_| invalid())?;
    if body_columns.len() != view.columns.len() {
        return Err(invalid());
    }
    let columns = view
        .columns
        .iter()
        .zip(&body_columns)
        .map(|((name, _), (_, ft))| (name.clone(), ft.clone()))
        .collect();
    Ok((columns, rows))
}

/// One common column of a `NATURAL`/`USING` join: the row offset that stays
/// visible under the shared name, and the one that is coalesced away.
pub(crate) struct CommonColumn {
    pub(crate) visible: usize,
    pub(crate) redundant: usize,
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
pub(crate) fn coalesce_common_columns(
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
fn apply_pushed_leaf_filters(
    node: &JoinNode,
    mut exec: Box<dyn Executor>,
    scope: &FromScope,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    prebuilt_pending_filters: Option<&[tidb_ast::Expr]>,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    trace_from_top: usize,
    current_db: &str,
    catalog: &Catalog,
) -> Result<(Box<dyn Executor>, Option<PushedLeafSelection>), DriverError> {
    let mut relation = node;
    while let JoinNode::Join(join) = relation {
        if join.right.is_some() || join.on.is_some() || !join.using.is_empty() || join.natural {
            return Ok((exec, None));
        }
        relation = &join.left;
    }
    let JoinNode::Table(table) = relation else {
        return Ok((exec, None));
    };
    let visible = table
        .alias
        .as_ref()
        .or_else(|| table.name.last())
        .map(String::as_str)
        .unwrap_or_default();
    let Some(all_filters) = demand
        .pushdown
        .map(|pushdown| pushdown.filters_for(table))
        .filter(|filters| !filters.is_empty())
    else {
        return Ok((exec, None));
    };
    let path_receipt = demand
        .rows
        .and_then(|rows| rows.leaf_filter_receipt(visible));
    let original_filters = demand
        .rows
        .and_then(|rows| rows.filters_for(visible))
        .unwrap_or_default();
    // The outer-join-derived family the leaf build also offered to its path
    // chooser; consumed members must not become a second Selection here.
    let derived_filters = demand
        .pushdown
        .map(|pushdown| pushdown.derived_for(table))
        .unwrap_or_default();
    let filters = all_filters
        .iter()
        .filter(|filter| {
            // A decorrelated scalar predicate may already be recorded as
            // consumed by the logical row source, while the prebuilt outer
            // child still needs to execute it below the sibling semi join.
            // Keep such pending filters in the physical Selection instead of
            // treating the logical consumption receipt as execution.
            //
            // The receipt covers the OFFERED list -- RowSource's filters
            // plus the outer-join-derived family (the leaf build unions the
            // two into `leaf_where`) -- so for an offered filter the
            // residuals alone say whether a Selection is still needed.
            // Keeping a derived filter unconditionally would re-run a
            // condition the committed path already turned into ranges; a
            // filter that was never offered at all still keeps its
            // Selection, receipt or no receipt.
            prebuilt_pending_filters.is_some_and(|pending| pending.contains(filter))
                || (!original_filters.contains(filter) && !derived_filters.contains(filter))
                || path_receipt
                    .as_ref()
                    .is_none_or(|(residuals, _)| residuals.contains(filter))
        })
        .cloned()
        .collect::<Vec<_>>();
    if filters.is_empty() {
        return Ok((exec, None));
    }
    let resolver = ScopeResolver { scope };
    let mut built = Vec::with_capacity(filters.len());
    for filter in &filters {
        let mut expression = rewrite_expr_resolved(filter, &resolver)
            .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
        tidb_expr::builtin_compare::refine_comparisons(&mut expression, ctx)
            .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
        // Go's rewriter runs `foldConstant` while building every pushed
        // condition (`expression.rewrite` -> `foldConstant`), so a
        // DATE_ADD over constants lands as a literal even when THIS
        // scope lost its statement context during the join rebuild --
        // the caller's `ctx` is the same session context Go would use.
        tidb_expr::fold_constant_in_mode(&mut expression, ctx, tidb_expr::ConstantFoldMode::Normal);
        built.push(expression);
    }
    let (trace_filters, trace_built): (Vec<_>, Vec<_>) = filters
        .iter()
        .zip(&built)
        .filter(|(filter, _)| {
            path_receipt
                .as_ref()
                .is_none_or(|(_, traced)| !traced.contains(filter))
        })
        .map(|(filter, expression)| (filter.clone(), expression.clone()))
        .unzip();
    // A path may already have priced some residuals in its own candidate.
    // `trace_filters` are exactly the remaining physical Selection, while
    // only predicates absent from `RowSource` still need to narrow logical
    // join cardinality.
    let unmodeled_filters = if prebuilt_pending_filters.is_none() {
        trace_filters
            .iter()
            .filter(|filter| !original_filters.contains(*filter))
            .cloned()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let execution_filters = filters
        .iter()
        .zip(&built)
        .filter(|(filter, _)| {
            prebuilt_pending_filters.is_none_or(|pending| pending.contains(filter))
        })
        .map(|(_, expression)| expression.clone())
        .collect::<Vec<_>>();

    // Go's `LogicalSelection.PredicatePushDown` offers leaf-local filters to
    // the DataSource even when the leaf sits below a reordered join. The
    // earlier build site only saw `RowSource` filters, so reordered join
    // inputs silently kept their Selection at the root and shipped the whole
    // table over the wire. The source may accept only the pushable subset;
    // the original predicate remains in the parent plan for any residual.
    if prebuilt_pending_filters.is_none() && scope.tables.len() == 1 {
        if let Some(written) = crate::driver::predicate_push_down::combined(&filters) {
            let resolver = ScopeResolver { scope };
            let (pushed, _) =
                crate::driver::access::split_scan_predicates(&written, &resolver, ctx);
            if !pushed.is_empty() {
                let _ = exec
                    .table_access()
                    .is_some_and(|access| access.accept_scan_filter(&pushed, ctx));
            }
        }
    }

    if !execution_filters.is_empty() {
        let schema = Schema::new(
            exec.ret_field_types()
                .iter()
                .enumerate()
                .map(|(index, field_type)| {
                    let mut column = Column::new((index + 1) as i64, field_type.clone());
                    column.index = index as i64;
                    column
                })
                .collect(),
        );
        exec = Box::new(SelectionExec::new(
            ExecutorMeta::new(schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            execution_filters,
            exec,
            ctx.clone(),
            ctx.statement_memory(),
        ));
    }
    if !original_filters.is_empty()
        && original_filters
            .iter()
            .all(|filter| all_filters.contains(filter))
    {
        if let Some(rows) = demand.rows {
            rows.mark_leaf_filters_consumed(visible);
        }
    }
    let selection_rate = |filters: &[tidb_ast::Expr]| {
        let written = crate::driver::predicate_push_down::combined(filters)?;
        let stats_selectivity = split_table_path(&table.name, current_db)
            .ok()
            .and_then(|(database, name)| catalog.get_in(database, name))
            .and_then(|entry| match entry {
                TableEntry::Kv(table) => {
                    crate::driver::access::stats_selectivity_with_default_string_match_selectivity(
                        catalog,
                        table,
                        scope,
                        Some(&written),
                        ctx.default_string_match_selectivity(),
                    )
                }
                TableEntry::View(_) => Some(crate::plan_trace::SELECTIVITY_FACTOR),
                TableEntry::Mem(_) | TableEntry::Cte(_) | TableEntry::Sequence(_) => None,
            });
        Some(
            stats_selectivity
                .or_else(|| crate::driver::predicate_push_down::derived_not_null_rate(filters))
                .unwrap_or_else(|| crate::plan_trace::pseudo_selectivity(&written)),
        )
    };
    let unmodeled_selectivity = selection_rate(&unmodeled_filters);
    let pushed_selection = (!trace_built.is_empty()).then(|| PushedLeafSelection {
        conditions: trace_built
            .iter()
            .map(|expression| matches!(expression, Expression::ScalarFunction(_)))
            .collect(),
        unmodeled_selectivity,
    });
    if let (Some(trace), Some(written)) = (
        trace,
        crate::driver::predicate_push_down::combined(&trace_filters),
    ) {
        let physical_column_names = (0..scope.width())
            .map(|offset| {
                let path = scope.qualified_path(offset)?;
                let [.., relation_name, column_name] = path.as_slice() else {
                    return None;
                };
                crate::driver::merge_decision::physical_column_trace_name(
                    relation,
                    &crate::driver::merge_decision::RelColumn {
                        relation: relation_name.clone(),
                        column: column_name.clone(),
                    },
                    catalog,
                    current_db,
                )
            })
            .collect::<Vec<_>>();
        trace.pushed_selection(
            trace_from_top,
            &written,
            &trace_built,
            &crate::plan_trace::Qualifier {
                db: current_db,
                scope,
                catalog: Some(catalog),
            },
            &physical_column_names,
            selection_rate(&trace_filters),
        );
        exec = trace.meter_child(trace_from_top, exec);
    }
    Ok((exec, pushed_selection))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_join(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    prune: Option<&tidb_ast::SelectStmt>,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    required: &tidb_planner::physical_property::PhysicalProperty,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    build_join_with_choice(
        join, current_db, catalog, ctx, trace, prune, demand, required, None, None, None,
    )
}

struct PrebuiltJoinLeft {
    exec: Box<dyn Executor>,
    scope: FromScope,
    delivered: Delivered,
    pending_filters: Vec<tidb_ast::Expr>,
    logical_rows: Option<f64>,
    matching_rows: Option<f64>,
    projection: Option<super::agg_select::JoinOutputProjection>,
}

/// Plans a decorrelated `EXISTS` / `NOT EXISTS` through the same physical
/// join search as an ordinary `FROM` join. The outer child has already been
/// built by the enclosing SELECT; the inner child and both sides' pushed
/// predicates are still planned here so a dynamic index range is executable,
/// not merely an EXPLAIN rendering.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_semi_join(
    join: &tidb_ast::Join,
    outer_exec: Box<dyn Executor>,
    outer_scope: FromScope,
    outer_delivered: Delivered,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    prune: Option<&tidb_ast::SelectStmt>,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    anti: bool,
    pending_outer_filters: Vec<tidb_ast::Expr>,
    outer_projection: Option<&super::agg_select::JoinOutputProjection>,
    logical_outer_rows: Option<f64>,
    matching_outer_rows: Option<f64>,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    build_join_with_choice(
        join,
        current_db,
        catalog,
        ctx,
        trace,
        prune,
        demand,
        &tidb_planner::physical_property::PhysicalProperty::default(),
        None,
        Some(if anti {
            JoinKind::AntiSemi
        } else {
            JoinKind::Semi
        }),
        Some(PrebuiltJoinLeft {
            exec: outer_exec,
            scope: outer_scope,
            delivered: outer_delivered,
            pending_filters: pending_outer_filters,
            logical_rows: logical_outer_rows,
            matching_rows: matching_outer_rows,
            projection: outer_projection.cloned(),
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn build_join_with_choice(
    join: &tidb_ast::Join,
    current_db: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
    prune: Option<&tidb_ast::SelectStmt>,
    demand: crate::driver::leaf_demand::FromDemand<'_>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    committed_choice: Option<CostedJoinChoice>,
    kind_override: Option<JoinKind>,
    mut prebuilt_left: Option<PrebuiltJoinLeft>,
) -> Result<(Box<dyn Executor>, FromScope, Delivered), DriverError> {
    let source_join = join;
    let kind = kind_override.unwrap_or(match join.tp {
        tidb_ast::JoinType::Cross => JoinKind::Inner,
        tidb_ast::JoinType::Left => JoinKind::Left,
        tidb_ast::JoinType::Right => JoinKind::Right,
    });
    let semi_join = matches!(kind, JoinKind::Semi | JoinKind::AntiSemi);
    let consumption_before = demand
        .rows
        .map(crate::driver::join_reorder::RowSource::filter_consumption_checkpoint);
    // `FROM a, b` parses as the single-relation wrapper AROUND the real join,
    // while `FROM a JOIN b ON ...` is the join node itself. Unwrapping here
    // keeps the two spellings one shape for the prune request below --
    // otherwise the comma form would drop it and silently never prune.
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        if let JoinNode::Join(inner) = &join.left {
            return build_join_with_choice(
                inner,
                current_db,
                catalog,
                ctx,
                trace,
                prune,
                demand,
                required,
                committed_choice,
                kind_override,
                prebuilt_left,
            );
        }
    }
    let plan_only = trace.as_deref().is_some_and(PlanTrace::is_plan_only);
    let mut child_output_columns = demand.output_columns.cloned();
    if let Some(columns) = &mut child_output_columns {
        columns.add_current_join(join);
    }
    let child_demand = crate::driver::leaf_demand::FromDemand {
        output_columns: child_output_columns.as_ref(),
        ..demand
    };
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
    let hinted_sides = crate::driver::join_method_hints::side_aliases(join);
    let forced_merge = !semi_join
        && demand.join_hints.is_some_and(|hints| {
            hints.forces_merge((hinted_sides.0.as_deref(), hinted_sides.1.as_deref()))
        });
    let ordinary_merge = (!semi_join)
        .then(|| {
            crate::driver::merge_decision::merge_join_decision(
                join,
                catalog,
                current_db,
                required,
                demand.offered,
                demand.rows,
            )
        })
        .flatten()
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
                hints.merge_join_allowed(
                    (hinted_sides.0.as_deref(), hinted_sides.1.as_deref()),
                    required.is_sort_item_empty(),
                )
            })
        });
    let merge = ordinary_merge.or_else(|| {
        forced_merge.then(|| {
            crate::driver::merge_decision::enforced_merge_join_decision(
                join,
                catalog,
                current_db,
                required,
                demand.offered,
            )
        })?
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
    let required_names = required_property_names(required, sides.as_ref());
    let empty_property = tidb_planner::physical_property::PhysicalProperty::default;
    let (left_required, right_required) = match committed_choice {
        Some(CostedJoinChoice::Hash { .. }) => (empty_property(), empty_property()),
        Some(CostedJoinChoice::Index { .. }) => {
            index_join_child_props(required, sides.as_ref().map(|(left, _)| left.width))
        }
        Some(CostedJoinChoice::Merge) | None => match &merge {
            // `tryToGetChildReqProp`: each child is required to produce ITS OWN
            // join keys' order, including any index prefix fixed by an equality
            // predicate, in the direction the parent asked for.
            Some(decision) => (
                crate::driver::merge_decision::child_required_prop(
                    decision.left_required.iter().copied(),
                    decision.plan.desc,
                ),
                crate::driver::merge_decision::child_required_prop(
                    decision.right_required.iter().copied(),
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
        },
    };
    // A merge alternative may lose to an unordered candidate. Its first pass
    // is therefore a cost-only build; recording starts only when the winning
    // properties are known and the subtree is rebuilt once.
    let initial_children_use_merge_property =
        merge.is_some() && matches!(committed_choice, None | Some(CostedJoinChoice::Merge));
    let suppress_initial_trace =
        committed_choice.is_none() && initial_children_use_merge_property && trace.is_some();
    let mut left_plan_only_trace = plan_only.then(PlanTrace::planning);
    let left_trace = if suppress_initial_trace {
        left_plan_only_trace.as_mut()
    } else {
        trace.as_deref_mut()
    };
    let (
        mut left_exec,
        mut left_scope,
        mut left_delivered,
        prebuilt_left_pending_filters,
        prebuilt_left_logical_rows,
        prebuilt_left_matching_rows,
        prebuilt_left_projection,
    ) = match prebuilt_left.take() {
        Some(prebuilt) => (
            prebuilt.exec,
            prebuilt.scope,
            prebuilt.delivered,
            Some(prebuilt.pending_filters),
            prebuilt.logical_rows,
            prebuilt.matching_rows,
            prebuilt.projection,
        ),
        None => {
            let (exec, scope, delivered) = build_from(
                &join.left,
                catalog,
                current_db,
                ctx,
                left_trace,
                child_demand,
                &left_required,
            )?;
            (exec, scope, delivered, None, None, None, None)
        }
    };
    if !demand.plan_columns.is_empty() {
        left_scope.plan_columns = demand.plan_columns.to_vec();
    }
    // Go's LogicalJoin.PruneColumns runs after a filter subquery has become a
    // semi join and before physical search. The preserved child is already an
    // executor in this driver, so reproduce that boundary with an executable
    // projection over a multi-table child. Single-table children keep using
    // DataSource column pruning, which has no visible root Projection.
    if prebuilt_left_pending_filters.is_some() && left_scope.tables.len() > 1 {
        if let Some(mut columns) = child_output_columns.clone() {
            if let Some(pending) = prebuilt_left_pending_filters.as_deref() {
                columns.add_predicates(pending);
            }
            let needed = columns.needed_scope(&left_scope);
            let mut keep = prebuilt_left_projection
                .as_ref()
                .map(|projection| {
                    projection
                        .sources
                        .iter()
                        .copied()
                        .filter(|offset| needed.contains(offset))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let remaining = needed
                .iter()
                .copied()
                .filter(|offset| !keep.contains(offset))
                .collect::<Vec<_>>();
            keep.extend(remaining);
            // Pure column pruning does not survive as a visible Projection in
            // Go. Keep the operator only when join reorder's written-schema
            // restoration changes the columns' relative order (q16); an
            // already matching order is absorbed by pruning (q21).
            let restores_written_order =
                prebuilt_left_projection.is_some() && keep.as_slice() != needed.as_slice();
            if !keep.is_empty() && restores_written_order {
                let input_schema = left_exec.schema().clone();
                let expressions = keep
                    .iter()
                    .map(|offset| {
                        let mut column = input_schema.columns[*offset].clone();
                        column.index = *offset as i64;
                        Expression::Column(column)
                    })
                    .collect::<Vec<_>>();
                let output_columns = keep
                    .iter()
                    .enumerate()
                    .map(|(output, input)| {
                        let mut column = input_schema.columns[*input].clone();
                        column.index = output as i64;
                        column
                    })
                    .collect::<Vec<_>>();
                left_exec = Box::new(ProjectionExec::new(
                    ExecutorMeta::new(Schema::new(output_columns), 1, INIT_CAP, MAX_CHUNK_SIZE),
                    expressions,
                    left_exec,
                    ctx.clone(),
                ));
                let projection_fields = keep
                    .iter()
                    .map(|offset| {
                        prebuilt_left_projection
                            .as_ref()
                            .and_then(|projection| {
                                projection
                                    .sources
                                    .iter()
                                    .position(|source| source == offset)
                                    .and_then(|position| projection.fields.get(position))
                            })
                            .cloned()
                            .unwrap_or_else(|| {
                                qualified_scope_column(&left_scope, current_db, *offset)
                            })
                    })
                    .collect::<Vec<_>>();
                left_scope =
                    crate::column_prune::projected_scope(&left_scope, &keep).ok_or_else(|| {
                        DriverError::unsupported(
                            "a semi-join projection interleaves one table's output columns",
                        )
                    })?;
                if let Some(child) = left_delivered.candidate.take() {
                    let input_rows = tidb_planner::candidate_cost::evaluate(
                        &child,
                        &tidb_planner::candidate_cost::CostEnv::default(),
                        tidb_planner::task_type::TaskType::Root,
                    )
                    .rows;
                    left_delivered.candidate =
                        Some(tidb_planner::candidate_cost::Candidate::Projection {
                            child: Box::new(child),
                            input_rows,
                            exprs: vec![false; keep.len()],
                        });
                }
                if let Some(trace) = trace.as_deref_mut() {
                    trace.join_reorder_projection(&projection_fields);
                    left_exec = trace.meter(left_exec);
                }
            }
        }
    }
    if forced_merge {
        if let Some(decision) = &merge {
            // Resolved off the CHILD's own scope by name rather than read out
            // of `plan.keys`: those offsets were fixed before column pruning
            // decided what this child would actually produce. It is the same
            // reason `merged` below re-reads its keys off the final scope.
            // `t right join t t1 on t.a = t1.b` projecting only `t1.b` leaves
            // that child ONE column wide while its recorded key offset is
            // still 1, and the enforced sort indexed off the end of the
            // child's field types.
            //
            // A name that no longer resolves drops the enforced sort instead
            // of guessing an offset -- fail-closed, exactly as `merged` does.
            let keys: Option<Vec<usize>> = decision
                .names
                .iter()
                .map(|(left, _)| scope_offset_of(&left_scope, left))
                .collect();
            if let Some(keys) =
                keys.filter(|keys| !crate::driver::merge_decision::delivers(&left_delivered, keys))
            {
                let names = decision
                    .names
                    .iter()
                    .map(|(left, _)| enforced_merge_key_name(left, current_db))
                    .collect::<Vec<_>>();
                // During the cost-only first pass this tier plans the children
                // into a discarded plan-only trace; the enforced sort belongs
                // THERE, not on the real trace. Recording it on the real one
                // left two orphan root Sorts above the finished MergeJoin once
                // the traced rebuild re-recorded them inside their subtrees.
                let sort_trace = if suppress_initial_trace {
                    left_plan_only_trace.as_mut()
                } else {
                    trace.as_deref_mut()
                };
                left_exec = enforced_merge_sort(
                    left_exec,
                    &keys,
                    decision.plan.desc,
                    &names,
                    &mut left_delivered,
                    ctx,
                    sort_trace,
                );
            }
        }
    }
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
    let mut right_plan_only_trace = plan_only.then(PlanTrace::planning);
    let right_trace = if suppress_initial_trace {
        right_plan_only_trace.as_mut()
    } else {
        trace.as_deref_mut()
    };
    let (mut right_exec, mut right_scope, mut right_delivered) = build_from(
        right_node,
        catalog,
        current_db,
        ctx,
        right_trace,
        child_demand,
        &right_required,
    )?;
    if !demand.plan_columns.is_empty() {
        right_scope.plan_columns = demand.plan_columns.to_vec();
    }
    if forced_merge {
        if let Some(decision) = &merge {
            // Resolved off this child's own scope, for the reason spelled out
            // on the left side above.
            let keys: Option<Vec<usize>> = decision
                .names
                .iter()
                .map(|(_, right)| scope_offset_of(&right_scope, right))
                .collect();
            if let Some(keys) =
                keys.filter(|keys| !crate::driver::merge_decision::delivers(&right_delivered, keys))
            {
                let names = decision
                    .names
                    .iter()
                    .map(|(_, right)| enforced_merge_key_name(right, current_db))
                    .collect::<Vec<_>>();
                // Same suppression as the left side above.
                let sort_trace = if suppress_initial_trace {
                    right_plan_only_trace.as_mut()
                } else {
                    trace.as_deref_mut()
                };
                right_exec = enforced_merge_sort(
                    right_exec,
                    &keys,
                    decision.plan.desc,
                    &names,
                    &mut right_delivered,
                    ctx,
                    sort_trace,
                );
            }
        }
    }
    let mut left_filter_scope = left_scope.clone();
    let mut right_filter_scope = right_scope.clone();
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
    // The decision was formed before recursive column pruning. Resolve its
    // stable relation-qualified names against the compact child scopes before
    // comparing them with the delivery receipts those children just returned.
    let merge = merge.filter(|decision| {
        let left_required = decision
            .left_required_names
            .iter()
            .map(|name| scope_offset_of(&left_scope, name))
            .collect::<Option<Vec<_>>>();
        let right_required = decision
            .right_required_names
            .iter()
            .map(|name| scope_offset_of(&right_scope, name))
            .collect::<Option<Vec<_>>>();
        left_required.is_some_and(|required| {
            crate::driver::merge_decision::delivers(&left_delivered, &required)
        }) && right_required.is_some_and(|required| {
            crate::driver::merge_decision::delivers(&right_delivered, &required)
        })
    });
    let merge = match committed_choice {
        Some(CostedJoinChoice::Index { .. } | CostedJoinChoice::Hash { .. }) => None,
        Some(CostedJoinChoice::Merge) | None => merge,
    };
    // Keep the complete child orders by NAME before the decision is consumed
    // while re-resolving its executor keys. They include any fixed leading
    // index columns and become this join's truthful delivery receipt below.
    let merge_required_names = merge.as_ref().map(|decision| {
        (
            decision.left_required_names.clone(),
            decision.right_required_names.clone(),
        )
    });
    let merge_trace_names = merge.as_ref().map(|decision| decision.names.clone());

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
    let debug_left_relations = left_scope
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    let debug_right_relations = right_scope
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    let child_coalesced = !left_scope.star.is_empty() || !right_scope.star.is_empty();
    // Record which child contains the runtime lookup target before the scopes
    // are folded into the joined scope below. Composite index joins need this
    // only to constrain hash-build orientation during candidate enumeration.
    let runtime_target_side = demand.runtime_lookup.and_then(|runtime| {
        let contains_target = |child_scope: &FromScope| {
            child_scope.tables.iter().any(|table| {
                let database = table.database.as_deref().unwrap_or(current_db);
                matches!(
                    catalog.get_in(database, table.name.as_str()),
                    Some(crate::driver::catalog::TableEntry::Kv(table))
                        if table.table_id == runtime.table_id
                )
            })
        };
        if contains_target(&left_scope) {
            Some(true)
        } else if contains_target(&right_scope) {
            Some(false)
        } else {
            None
        }
    });
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
            physical: table.physical,
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
    // The join may be renumbered below. Its committed merge plan is resolved
    // again against the narrowed scope, so the delivery report later uses
    // those final offsets rather than the children's pre-prune offsets.
    if let Some(select) = prune.filter(|_| !coalescing) {
        if let Some(pruned_sides) = crate::column_prune::prune_join_sides(
            select,
            join,
            &scope,
            &mut left_exec,
            &mut right_exec,
        ) {
            let crate::column_prune::PrunedJoinSides {
                left_columns,
                right_columns,
                left_func_deps,
                right_func_deps,
            } = pruned_sides;
            if left_filter_scope.tables.len() == 1 {
                left_filter_scope.tables[0].columns = left_columns.clone();
            }
            if right_filter_scope.tables.len() == 1 {
                right_filter_scope.tables[0].columns = right_columns.clone();
            }
            left_width = left_columns.len();
            scope.tables[0].columns = left_columns;
            scope.tables[0].offset = 0;
            scope.tables[0].func_deps = left_func_deps;
            scope.tables[1].columns = right_columns;
            scope.tables[1].offset = left_width;
            scope.tables[1].func_deps = right_func_deps;
        }
    }

    let left_filter_trace = if suppress_initial_trace {
        None
    } else {
        trace.as_deref_mut()
    };
    let (filtered_left, left_pushed_selection) = apply_pushed_leaf_filters(
        &source_join.left,
        left_exec,
        &left_filter_scope,
        demand,
        prebuilt_left_pending_filters.as_deref(),
        ctx,
        left_filter_trace,
        1,
        current_db,
        catalog,
    )?;
    left_exec = filtered_left;
    let right_filter_trace = if suppress_initial_trace {
        None
    } else {
        trace.as_deref_mut()
    };
    let (filtered_right, right_pushed_selection) = apply_pushed_leaf_filters(
        right_node,
        right_exec,
        &right_filter_scope,
        demand,
        None,
        ctx,
        right_filter_trace,
        0,
        current_db,
        catalog,
    )?;
    right_exec = filtered_right;

    // Remove a leaf-local ON conjunct only after the post-pruning Selection
    // that owns it has been installed. Join equalities and preserved-side
    // outer-join predicates remain at the join.
    let pushed_join;
    let join = match demand.rows {
        Some(rows) => {
            pushed_join = {
                let mut join = join.clone();
                join.on = rows.residual_on(join.on.as_ref());
                join
            };
            &pushed_join
        }
        None => join,
    };

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
    let mut pushed: Vec<&tidb_ast::Expr> = if join.tp == tidb_ast::JoinType::Cross && !coalescing {
        crate::driver::predicate_push_down::spanning_conjuncts(demand.offered, &scope, left_width)
            .into_iter()
            .filter(|conjunct| !written_on.contains(conjunct))
            .collect()
    } else {
        Vec::new()
    };
    if join.tp == tidb_ast::JoinType::Cross && !coalescing {
        if let Some(rows) = demand.rows {
            pushed.extend(
                rows.join_other_conditions(&source_join.left, right_node)
                    .into_iter()
                    .filter(|conjunct| !written_on.contains(conjunct)),
            );
        }
    }
    for conjunct in &pushed {
        let resolver = ScopeResolver { scope: &scope };
        conditions.push(
            rewrite_expr_resolved(conjunct, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        );
    }
    // Go builds EqualConditions separately from OtherConditions. Comparison
    // refinement belongs to the latter here: mutating an already-typed
    // column-to-column hash key can change the dynamic range estimate even
    // though the key and result rows are unchanged.
    let logical_split = crate::hash_join::split_equi(&conditions, left_width);
    refine_other_join_conditions(&mut conditions, &logical_split.equal_mask, ctx)?;
    let mut join_input_columns = child_output_columns.clone();
    if let Some(columns) = &mut join_input_columns {
        columns.add_predicates(pushed.iter().copied());
    }
    // The condition split the executor will run on, so EXPLAIN's
    // `equal:[...]`/`other cond:` and the hash table's own keys are one
    // decision rather than two that can drift.
    let split = crate::hash_join::split_equi(&conditions, left_width);
    // Go's join-key cast chain over the same conditions: the mismatched
    // int-vs-string equalities `updateEQCond` would materialize (their
    // plan-column ids advance the statement's stream below), and the subset
    // `rule_join_key_type_cast.go` rewrites to an integer key, which is what
    // makes an INL_JOIN on the int side's handle possible at all.
    let mut coercions = crate::driver::join_key_cast::analyze(&conditions, left_width, ctx);
    let coercion_double_cast_pairs = coercions.double_cast_pairs();
    let coercion_rewritten = coercions.rewritten();
    let physical_conditions = {
        let mut flattened = Vec::new();
        for condition in &conditions {
            collect_physical_join_conjuncts(condition, &mut flattened);
        }
        let columns = (0..scope.width())
            .map(|offset| {
                let path = scope.qualified_path(offset)?;
                let [.., relation, column] = path.as_slice() else {
                    return None;
                };
                let column = crate::driver::merge_decision::RelColumn {
                    relation: relation.clone(),
                    column: column.clone(),
                };
                crate::driver::merge_decision::physical_column_trace_name(
                    if offset < left_width {
                        &join.left
                    } else {
                        right_node
                    },
                    &column,
                    catalog,
                    current_db,
                )
            })
            .collect::<Vec<_>>();
        (flattened.len() == split.equal_mask.len()).then_some((flattened, columns))
    };
    // Go's stats-less build side: the inner (non-preserved) child, which is
    // the left one only for a RIGHT join. See `join.rs`'s module doc.
    let build_is_left = kind == JoinKind::Right;
    // Read before the children move into the join: the index-join decision
    // needs each side's OUTPUT types (post-pruning), which only the built
    // executors know.
    let left_schema = left_exec.schema().clone();
    let right_schema = right_exec.schema().clone();
    let left_types = left_exec.ret_field_types().to_vec();
    let right_types = right_exec.ret_field_types().to_vec();
    let left_cost_types = logical_cost_types(&scope, join_input_columns.as_ref(), 0..left_width);
    let right_cost_types = logical_cost_types(
        &scope,
        join_input_columns.as_ref(),
        left_width..scope.width(),
    );
    let output_cost_types = logical_cost_types(
        &scope,
        demand.output_columns,
        0..if semi_join { left_width } else { scope.width() },
    );
    let mut estimated_matched_rows = crate::driver::join_search::estimated_rows(join, demand.rows);
    if let (Some(rows), Some(matching_outer_rows)) =
        (&mut estimated_matched_rows, prebuilt_left_matching_rows)
    {
        let scale = if rows.left > 0.0 {
            matching_outer_rows / rows.left
        } else {
            1.0
        };
        rows.left = matching_outer_rows;
        rows.joined *= scale;
    }
    if let Some(rows) = &mut estimated_matched_rows {
        if let Some(selectivity) = left_pushed_selection
            .as_ref()
            .and_then(|selection| selection.unmodeled_selectivity)
        {
            rows.left *= selectivity;
            rows.joined *= selectivity;
        }
        if let Some(selectivity) = right_pushed_selection
            .as_ref()
            .and_then(|selection| selection.unmodeled_selectivity)
        {
            rows.right *= selectivity;
            rows.joined *= selectivity;
        }
    }
    let mut estimated_join_rows = estimated_matched_rows;
    if let Some(rows) = &mut estimated_join_rows {
        if let Some(logical_outer_rows) = prebuilt_left_logical_rows {
            rows.left = logical_outer_rows;
        }
        if semi_join {
            rows.joined = rows.left * crate::plan_trace::SELECTIVITY_FACTOR;
        }
    }
    if prebuilt_left_pending_filters.is_some() {
        if let (Some(rows), Some(candidate)) =
            (estimated_join_rows, left_delivered.candidate.take())
        {
            let costed = tidb_planner::candidate_cost::evaluate(
                &candidate,
                &tidb_planner::candidate_cost::CostEnv::default(),
                tidb_planner::task_type::TaskType::Root,
            );
            let filter_count = sole_relation_name(&join.left)
                .and_then(|visible| demand.rows?.filters_for(visible))
                .map_or(0, <[_]>::len);
            let candidate = if filter_count == 0 {
                candidate
            } else {
                tidb_planner::candidate_cost::Candidate::Selection {
                    child: Box::new(candidate),
                    input_rows: costed.rows,
                    conditions: vec![true; filter_count],
                }
            };
            left_delivered.candidate =
                Some(fixed_join_receipt(candidate, rows.left, costed.row_size));
        }
    }
    let left_candidate_row_size =
        candidate_row_size(left_delivered.candidate.as_ref(), &left_cost_types);
    let right_candidate_row_size =
        candidate_row_size(right_delivered.candidate.as_ref(), &right_cost_types);
    let mut comparison_not_null = Vec::new();
    for condition in &conditions {
        collect_comparison_not_null_columns(condition, &mut comparison_not_null);
    }
    comparison_not_null.sort_unstable();
    comparison_not_null.dedup();
    comparison_not_null.retain(|offset| {
        let output_is_nullable = left_types
            .iter()
            .chain(&right_types)
            .nth(*offset)
            .is_some_and(|field_type| {
                !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
            });
        if !output_is_nullable {
            return false;
        }
        let Some(path) = scope.qualified_path(*offset) else {
            return true;
        };
        let [.., relation, column] = path.as_slice() else {
            return true;
        };
        let source = if *offset < left_width {
            &join.left
        } else {
            right_node
        };
        crate::driver::merge_decision::physical_column_is_nullable(
            source,
            &crate::driver::merge_decision::RelColumn {
                relation: relation.clone(),
                column: column.clone(),
            },
            catalog,
            current_db,
        )
        .unwrap_or(true)
    });
    let meta_schema = join_executor_schema(semi_join, &left_schema, &right_schema);
    let meta = ExecutorMeta::new(meta_schema, 6, INIT_CAP, MAX_CHUNK_SIZE);
    let mut join_exec = JoinExec::new(
        meta,
        kind,
        conditions,
        left_exec,
        right_exec,
        ctx.clone(),
        ctx.statement_memory(),
    );
    // When every stable WHERE conjunct belongs to this inner join, the join
    // executor now evaluates the complete predicate (equal keys and `other
    // cond` alike). Report that receipt so the equivalent Selection above it
    // is removed. A partial push remains fail-closed and keeps the Selection.
    let pushed_consumes_where = !demand.offered.is_empty()
        && demand
            .offered
            .iter()
            .all(|conjunct| pushed.contains(&conjunct))
        && join.tp == tidb_ast::JoinType::Cross;
    join_exec.set_consumes_where(pushed_consumes_where);
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
    let compact_required = remap_required_property(required, required_names.as_deref(), &scope);
    let search_orders = sides.as_ref().map(|(left, right)| {
        (
            remap_search_orders(left, &scope, 0, left_width),
            remap_search_orders(
                right,
                &scope,
                left_width,
                scope.width().saturating_sub(left_width),
            ),
        )
    });
    // The index strategy: one side read once per outer key rather than whole.
    // The search may choose it over an available merge plan for a one-row
    // outer side. It refuses a coalesced join for the same reason the merge
    // one is dropped there -- that scope addresses columns by row offset, not
    // by name.
    //
    // WHICH strategy this site may use is asked of Go's own enumeration --
    // `exhaustPhysicalPlans4LogicalJoin` under the property this join was
    // required to produce -- and not of the structural rule underneath. See
    // `driver::join_search`.
    let (hint_left, hint_right) = crate::driver::join_method_hints::side_aliases(join);
    let forced_index_name = demand.join_hints.and_then(|hints| {
        hints.forced_index_join_name(
            (hint_left.as_deref(), hint_right.as_deref()),
            required.is_sort_item_empty(),
        )
    });
    let forced_root_join_name = demand.join_hints.and_then(|hints| {
        hints.forced_root_join_name(
            (hint_left.as_deref(), hint_right.as_deref()),
            required.is_sort_item_empty(),
        )
    });
    let chosen = match committed_choice {
        Some(CostedJoinChoice::Index { .. }) => crate::driver::join_search::Chosen::Index,
        Some(CostedJoinChoice::Merge | CostedJoinChoice::Hash { .. }) => {
            crate::driver::join_search::Chosen::Refused(
                crate::driver::join_search::Refusal::NoIndexCandidate,
            )
        }
        None if forced_index_name.is_some() => crate::driver::join_search::Chosen::Index,
        None => crate::driver::join_search::choose(&crate::driver::join_search::SearchInput {
            join,
            join_type: match kind {
                JoinKind::Inner => tidb_planner::find_best_task::LogicalJoinType::Inner,
                JoinKind::Left => tidb_planner::find_best_task::LogicalJoinType::LeftOuter,
                JoinKind::Right => tidb_planner::find_best_task::LogicalJoinType::RightOuter,
                JoinKind::Semi => tidb_planner::find_best_task::LogicalJoinType::Semi,
                JoinKind::AntiSemi => tidb_planner::find_best_task::LogicalJoinType::AntiSemi,
            },
            keys: &split.keys,
            left_width,
            width: scope.width(),
            orders: search_orders
                .as_ref()
                .map(|(left, right)| (left.as_slice(), right.as_slice())),
            required: &compact_required,
            merge_available: merged.is_some(),
            rows: demand.rows,
        }),
    };
    // Go enumerates index joins independently of PreparePossibleProperties.
    // Missing child orders prevent inventing a merge candidate, but an index
    // path that the concrete sides can build must still enter cost comparison.
    let index_enumerated = matches!(
        chosen,
        crate::driver::join_search::Chosen::Index
            | crate::driver::join_search::Chosen::IndexForSingleOuterRow
            | crate::driver::join_search::Chosen::Refused(
                crate::driver::join_search::Refusal::HashAlsoEnumerated
                    | crate::driver::join_search::Refusal::MergeAlsoEnumerated
                    | crate::driver::join_search::Refusal::NoChildOrders
            )
    );
    let mut cast_probe_ordinal = None;
    let index_joins = (demand.runtime_lookup.is_none() && !coalescing && index_enumerated)
        .then(|| {
            let (left_side, right_side) = crate::driver::index_join_decision::join_sides(
                join,
                &split.keys,
                &scope,
                current_db,
                left_width,
                catalog,
                &left_types,
                &right_types,
            );
            let mut decisions = crate::driver::index_join_decision::index_join_decisions_with_context(
                kind,
                &split.keys,
                &left_side,
                &right_side,
                // A merge candidate being present does not prevent Go from
                // enumerating and costing the index family beside it.
                false,
                demand.rows,
                Some(catalog),
                ctx,
            )
            .into_iter()
            .filter(|decision| {
                index_join_satisfies_required_order(
                    decision.lookup_is_left,
                    &compact_required,
                    Some(left_width),
                )
            })
            .collect::<Vec<crate::driver::index_join_decision::IndexJoinDecision>>();
            // Go's FORCE preference (`PreferLeftAsINLJInner` /
            // `PreferRightAsINLJInner`, resolved in `findBestTask` once the
            // inner side has physicalized — `exhaust_physical_plans.go`'s
            // enumeration note): `TIDB_INLJ(t)` names the INNER side, so
            // when the index-family hint names a side's alias, only the
            // decisions probing THAT side survive. An empty result falls
            // back to every decision, which is Go's give-up-and-warn arm.
            if let Some(hints) = demand.join_hints {
                let names_inner = |decision: &crate::driver::index_join_decision::IndexJoinDecision| {
                    let alias = if decision.lookup_is_left {
                        hint_left.as_deref()
                    } else {
                        hint_right.as_deref()
                    };
                    alias.is_some_and(|alias| hints.index_family_names_alias(alias))
                };
                if decisions.iter().any(&names_inner) {
                    decisions.retain(&names_inner);
                }
            }
            // Go's `rule_join_key_type_cast` rewrite makes the INT side of a
            // mismatched equality probeable by `cast(str AS SIGNED)`. The
            // split keys hold no such equality, so it arrives here as its
            // own candidate -- and only under an index-family hint naming
            // the int side, the one surface the recordings pin (every
            // unhinted recording of this shape is a hash join).
            if decisions.is_empty() && forced_index_name.is_some() {
                for (ordinal, &pair_at) in coercion_rewritten.iter().enumerate() {
                    let pair = &mut coercions.mismatched[pair_at];
                    let lookup_is_left = pair.int_offset < left_width;
                    let lookup_alias = if lookup_is_left {
                        hint_left.as_deref()
                    } else {
                        hint_right.as_deref()
                    };
                    let hinted = lookup_alias.zip(demand.join_hints).is_some_and(
                        |(alias, hints)| hints.index_family_names_alias(alias),
                    );
                    if !hinted {
                        continue;
                    }
                    let Some(rewrite) = pair.rewrite.take() else {
                        continue;
                    };
                    let (inner_side, inner_offset, outer_offset) = if lookup_is_left {
                        (&left_side, pair.int_offset, pair.str_offset - left_width)
                    } else {
                        (&right_side, pair.int_offset - left_width, pair.str_offset)
                    };
                    let decision = crate::driver::index_join_decision::cast_lookup_decision(
                        kind,
                        lookup_is_left,
                        crate::driver::index_join_decision::CastLookupKey {
                            inner_offset,
                            outer_offset,
                            rewrite,
                        },
                        inner_side,
                        demand.rows,
                    );
                    if let Some(decision) = decision.filter(|decision| {
                        index_join_satisfies_required_order(
                            decision.lookup_is_left,
                            &compact_required,
                            Some(left_width),
                        )
                    }) {
                        cast_probe_ordinal = Some(ordinal);
                        decisions.push(decision);
                        break;
                    }
                }
            }
            decisions
        })
        .unwrap_or_default();
    for decision in &index_joins {
        decision.record_stats_access(
            catalog
                .table_statistics(decision.table.stats_physical_id())
                .map(AsRef::as_ref),
        );
    }
    let consumption_after_initial = demand
        .rows
        .map(crate::driver::join_reorder::RowSource::filter_consumption_checkpoint);

    // Go's composite IndexJoin asks the inner child for a fresh plan under
    // IndexJoinProp. That rebuild replaces the target leaf with a dynamic
    // lookup and scales every surrounding join to the expected outer rows;
    // the ordinary child candidate above is therefore not a valid probe-cost
    // receipt. Build those receipts once, before comparing join families.
    let mut composite_inner_candidates = vec![None; index_joins.len()];
    for (decision_index, decision) in index_joins.iter().enumerate() {
        if !decision.composite {
            continue;
        }
        let probes = std::rc::Rc::new(std::cell::RefCell::new(
            crate::access_path::SharedIndexJoinProbes::default(),
        ));
        let probe_rows = estimated_matched_rows.or(estimated_join_rows).unwrap_or(
            crate::driver::join_reorder::JoinRows {
                left: 1.0,
                right: 1.0,
                joined: 1.0,
            },
        );
        let probe_candidate = runtime_probe_candidate(
            decision,
            catalog,
            probe_rows,
            if decision.lookup_is_left {
                &left_cost_types
            } else {
                &right_cost_types
            },
        );
        let runtime = crate::driver::leaf_demand::RuntimeLookupDemand {
            table_id: decision.table.table_id,
            object: decision.object.clone(),
            probe_parts: decision.probe_parts.clone(),
            probes,
            filter_exprs: decision.filter_exprs.clone(),
            probe_candidate,
        };
        let runtime_demand = crate::driver::leaf_demand::FromDemand {
            runtime_lookup: Some(&runtime),
            // A cost-only rebuild of one inner leaf, not a plan being printed.
            partition_fan_out: false,
            ..child_demand
        };
        let target = if decision.lookup_is_left {
            &join.left
        } else {
            join.right
                .as_ref()
                .expect("an index join decision requires a right child")
        };
        let checkpoint = demand
            .rows
            .map(crate::driver::join_reorder::RowSource::filter_consumption_checkpoint);
        let (_, _, delivered) = build_from(
            target,
            catalog,
            current_db,
            ctx,
            None,
            runtime_demand,
            &tidb_planner::physical_property::PhysicalProperty::default(),
        )?;
        composite_inner_candidates[decision_index] = delivered.candidate;
        if let (Some(rows), Some(checkpoint)) = (demand.rows, checkpoint) {
            rows.restore_filter_consumption(checkpoint);
        }
    }

    // A merge pass built its children under ordered properties. Price the
    // unordered/index alternatives from a second lazy build, then restore
    // predicate receipts so this speculative pass commits no physical path.
    let needs_alternative_children = committed_choice.is_none()
        && initial_children_use_merge_property
        && (!index_joins.is_empty() || required.is_sort_item_empty());
    let (mut alternative_left_candidate, mut alternative_right_candidate) =
        if needs_alternative_children {
            let (alternative_left_required, alternative_right_required) =
                if required.is_sort_item_empty() {
                    (
                        tidb_planner::physical_property::PhysicalProperty::default(),
                        tidb_planner::physical_property::PhysicalProperty::default(),
                    )
                } else {
                    index_join_child_props(required, sides.as_ref().map(|(left, _)| left.width))
                };
            let mut alternative_left_trace = plan_only.then(PlanTrace::planning);
            let (_, _, left) = build_from(
                &source_join.left,
                catalog,
                current_db,
                ctx,
                alternative_left_trace.as_mut(),
                child_demand,
                &alternative_left_required,
            )?;
            let mut alternative_right_trace = plan_only.then(PlanTrace::planning);
            let (_, _, right) = build_from(
                source_join
                    .right
                    .as_ref()
                    .expect("a costed join has a right child"),
                catalog,
                current_db,
                ctx,
                alternative_right_trace.as_mut(),
                child_demand,
                &alternative_right_required,
            )?;
            if let (Some(rows), Some(checkpoint)) = (demand.rows, consumption_after_initial.clone())
            {
                rows.restore_filter_consumption(checkpoint);
            }
            (left.candidate, right.candidate)
        } else {
            (
                left_delivered.candidate.clone(),
                right_delivered.candidate.clone(),
            )
        };
    left_delivered.candidate = attach_pushed_leaf_selection(
        left_delivered.candidate.take(),
        left_pushed_selection.as_ref(),
    );
    right_delivered.candidate = attach_pushed_leaf_selection(
        right_delivered.candidate.take(),
        right_pushed_selection.as_ref(),
    );
    alternative_left_candidate =
        attach_pushed_leaf_selection(alternative_left_candidate, left_pushed_selection.as_ref());
    alternative_right_candidate =
        attach_pushed_leaf_selection(alternative_right_candidate, right_pushed_selection.as_ref());
    // Go attaches each pushed Selection's StatsInfo to the physical child
    // before join-family costs are compared. Access candidates here still
    // carry the pre-filter scan cardinality, while RowSource already owns the
    // exact left/right rows after those predicates. Keep the access cost and
    // replace only the logical receipt used by Merge/Hash/Index costing.
    //
    // The INDEX family keeps the child's own receipt instead: go prices an
    // IndexJoin's BUILD side at the physical OUTER plan's row count -- an
    // `IndexMerge` partial carries its real CountAfterAccess (`stats.go:250`
    // skips `adjustCountAfterAccess` for partials), so a member-of-driven
    // outer costs thousands of lookups, not the logical source's default-
    // selectivity millions. The normalized receipt below would erase exactly
    // that fact, so the raw children are captured before it runs.
    let raw_alternative_left_candidate = alternative_left_candidate.clone();
    let raw_alternative_right_candidate = alternative_right_candidate.clone();
    let mut ordered_left_candidate = left_delivered.candidate.clone();
    let mut ordered_right_candidate = right_delivered.candidate.clone();
    if !semi_join {
        if let Some(rows) = estimated_matched_rows {
            let normalize = |candidate: Option<tidb_planner::candidate_cost::Candidate>,
                             rows: f64,
                             types: &[FieldType]| {
                candidate.map(|candidate| {
                    let row_size = candidate_row_size(Some(&candidate), types);
                    fixed_join_receipt(candidate, rows, row_size)
                })
            };
            ordered_left_candidate = normalize(ordered_left_candidate, rows.left, &left_cost_types);
            ordered_right_candidate =
                normalize(ordered_right_candidate, rows.right, &right_cost_types);
            alternative_left_candidate =
                normalize(alternative_left_candidate, rows.left, &left_cost_types);
            alternative_right_candidate =
                normalize(alternative_right_candidate, rows.right, &right_cost_types);
        }
    }

    let forced_index_kind = forced_index_name.map(|name| match name {
        "IndexHashJoin" => tidb_planner::plan_cost_ver2::IndexJoinKind::IndexHashJoin,
        "IndexMergeJoin" => tidb_planner::plan_cost_ver2::IndexJoinKind::IndexMergeJoin,
        _ => tidb_planner::plan_cost_ver2::IndexJoinKind::IndexJoin,
    });
    // A valid merge plan whose ordered child has no complete candidate tree
    // cannot be compared with the costable unordered families. Treating the
    // missing receipt as an infinite merge cost changed Go's chosen plan.
    let unpriced_merge = merged.is_some()
        && (left_delivered.candidate.is_none() || right_delivered.candidate.is_none());
    let mut alternatives = Vec::new();
    if committed_choice.is_none() {
        let only_merge = forced_root_join_name == Some("MergeJoin");
        let only_hash = forced_root_join_name == Some("HashJoin");
        let only_index = forced_index_kind.is_some();
        if !only_hash && !only_index {
            if let (Some(rows), Some(left), Some(right)) = (
                estimated_join_rows,
                ordered_left_candidate.clone(),
                ordered_right_candidate.clone(),
            ) {
                if merged.is_some() {
                    alternatives.push((
                        CostedJoinChoice::Merge,
                        merge_join_candidate(
                            left,
                            right,
                            rows,
                            split.keys.len(),
                            split.equal_mask.iter().filter(|equal| !**equal).count(),
                        ),
                    ));
                }
            }
        }
        if !only_merge && !only_hash {
            if let Some(rows) = estimated_join_rows {
                for (decision_index, decision) in index_joins.iter().enumerate() {
                    let outer = if decision.lookup_is_left {
                        raw_alternative_right_candidate.clone()
                    } else {
                        raw_alternative_left_candidate.clone()
                    };
                    if let Some(outer) = outer {
                        let kinds: &[tidb_planner::plan_cost_ver2::IndexJoinKind] =
                            match forced_index_kind {
                                Some(ref kind) => std::slice::from_ref(kind),
                                None => &[
                                    tidb_planner::plan_cost_ver2::IndexJoinKind::IndexJoin,
                                    tidb_planner::plan_cost_ver2::IndexJoinKind::IndexHashJoin,
                                ],
                            };
                        for kind in kinds {
                            let candidate = index_join_candidate(
                                decision,
                                outer.clone(),
                                if decision.lookup_is_left {
                                    composite_inner_candidates[decision_index]
                                        .as_ref()
                                        .or(left_delivered.candidate.as_ref())
                                } else {
                                    composite_inner_candidates[decision_index]
                                        .as_ref()
                                        .or(right_delivered.candidate.as_ref())
                                },
                                catalog,
                                rows,
                                estimated_matched_rows.unwrap_or(rows),
                                &left_cost_types,
                                &right_cost_types,
                                split.keys.len(),
                                *kind,
                                semi_join,
                            );
                            alternatives.push((
                                CostedJoinChoice::Index {
                                    kind: *kind,
                                    lookup_is_left: decision.lookup_is_left,
                                    decision_index,
                                },
                                candidate,
                            ));
                        }
                    }
                }
            }
        }
        if !only_merge && !only_index && required.is_sort_item_empty() {
            if let (Some(_), Some(left), Some(right)) = (
                estimated_join_rows,
                alternative_left_candidate.clone(),
                alternative_right_candidate.clone(),
            ) {
                // Go `getHashJoins` enumerates both build orientations for
                // inner and outer joins. Right outer join tries its preserved
                // right side first; inner and left outer try right first.
                let build_orientations: &[bool] = match kind {
                    JoinKind::Right => &[true, false],
                    JoinKind::Inner | JoinKind::Left => &[false, true],
                    // Go's semi/anti-semi physical search also costs both
                    // hash orientations. The preserved (left) side is the
                    // build when it is cheaper, which is material for
                    // TPC-H q22's filtered customer/orders join.
                    JoinKind::Semi | JoinKind::AntiSemi => &[false, true],
                };
                let build_orientations = match runtime_target_side {
                    // The runtime target is the physical INNER child. Its
                    // child scopes are presented in probe-first order by the
                    // IndexJoinProp rebuild, so the target side maps to the
                    // opposite hash-build flag at this join site.
                    Some(true) => vec![false],
                    Some(false) => vec![true],
                    None => build_orientations.to_vec(),
                };
                for build_is_left in build_orientations {
                    alternatives.push((
                        CostedJoinChoice::Hash { build_is_left },
                        hash_join_candidate(
                            left.clone(),
                            right.clone(),
                            split.keys.len(),
                            build_is_left,
                            ctx.hash_join_concurrency(),
                        ),
                    ));
                }
            }
        }
    }

    let mut best: Option<(CostedJoinChoice, tidb_planner::candidate_cost::CostedNode)> = None;
    for (choice, candidate) in &alternatives {
        let costed = tidb_planner::candidate_cost::evaluate(
            candidate,
            &tidb_planner::candidate_cost::CostEnv::default(),
            tidb_planner::task_type::TaskType::Root,
        );
        if best
            .as_ref()
            .is_none_or(|(_, incumbent)| tidb_planner::candidate_cost::prefer(&costed, incumbent))
        {
            best = Some((*choice, costed));
        }
    }
    let fallback_index = index_joins.iter().enumerate().find(|(_, decision)| {
        if matches!(
            chosen,
            crate::driver::join_search::Chosen::Index
                | crate::driver::join_search::Chosen::IndexForSingleOuterRow
        ) {
            return true;
        }
        decision.aggregation.is_some()
            && decision.constant_constrained_probe
            && matches!(
                chosen,
                crate::driver::join_search::Chosen::Refused(
                    crate::driver::join_search::Refusal::HashAlsoEnumerated
                )
            )
            && estimated_join_rows.is_some_and(|rows| {
                let (outer, inner) = if decision.lookup_is_left {
                    (rows.right, rows.left)
                } else {
                    (rows.left, rows.right)
                };
                outer <= inner && rows.joined <= inner
            })
    });
    let fallback_hash_build_is_left = estimated_join_rows.map_or(kind == JoinKind::Right, |rows| {
        kind == JoinKind::Right
            || matches!(kind, JoinKind::Inner | JoinKind::Semi | JoinKind::AntiSemi)
                && rows.left < rows.right
    });
    let winning_choice = committed_choice
        .or_else(|| {
            (unpriced_merge
                && forced_root_join_name != Some("HashJoin")
                && forced_index_kind.is_none())
            .then_some(CostedJoinChoice::Merge)
        })
        .or_else(|| best.as_ref().map(|(choice, _)| *choice))
        .unwrap_or_else(|| {
            if let (Some(kind), Some(decision)) = (forced_index_kind, index_joins.first()) {
                CostedJoinChoice::Index {
                    kind,
                    lookup_is_left: decision.lookup_is_left,
                    decision_index: 0,
                }
            } else if let (Some((decision_index, decision)), Some(rows)) =
                (fallback_index, estimated_join_rows)
            {
                CostedJoinChoice::Index {
                    kind: fallback_index_join_kind(
                        decision,
                        catalog,
                        rows,
                        estimated_matched_rows.unwrap_or(rows),
                        left_candidate_row_size,
                        right_candidate_row_size,
                        split.keys.len(),
                    ),
                    lookup_is_left: decision.lookup_is_left,
                    decision_index,
                }
            } else if merged.is_some() {
                CostedJoinChoice::Merge
            } else {
                CostedJoinChoice::Hash {
                    build_is_left: fallback_hash_build_is_left,
                }
            }
        });
    if std::env::var_os("TIDB_DEBUG_JOIN_CANDIDATES").is_some() && !alternatives.is_empty() {
        for (choice, candidate) in &alternatives {
            let costed = tidb_planner::candidate_cost::evaluate(
                candidate,
                &tidb_planner::candidate_cost::CostEnv::default(),
                tidb_planner::task_type::TaskType::Root,
            );
            eprintln!(
                "JOIN_CANDIDATE {choice:?} cost={:?} rows={:.2}\n{candidate:#?}",
                costed.cost.value(), costed.rows
            );
        }
    }
    if std::env::var_os("TIDB_DEBUG_JOIN_CHOICE").is_some() {
        let mode = if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
            "plan-only"
        } else if trace.is_some() {
            "trace"
        } else {
            "runtime"
        };
        let alternatives_text = alternatives
            .iter()
            .map(|(choice, candidate)| {
                let costed = tidb_planner::candidate_cost::evaluate(
                    candidate,
                    &tidb_planner::candidate_cost::CostEnv::default(),
                    tidb_planner::task_type::TaskType::Root,
                );
                format!("{choice:?}:cost={:?},rows={:.3}", costed.cost, costed.rows)
            })
            .collect::<Vec<_>>()
            .join("; ");
        let left_relations = debug_left_relations.join(",");
        let right_relations = debug_right_relations.join(",");
        eprintln!(
            "JOIN_CHOICE mode={mode} kind={kind:?} left=[{left_relations}] right=[{right_relations}] committed={committed_choice:?} chosen={chosen:?} winning={winning_choice:?} merge={} indexes={} estimated={estimated_join_rows:?} best={:?} alternatives=[{alternatives_text}]",
            merged.is_some(),
            index_joins.len(),
            best.as_ref().map(|(_, costed)| (&costed.cost, costed.rows)),
        );
    }
    // Rebuild once with the winning child properties. This also supplies the
    // real trace after the cost-only merge pass suppressed it.
    if committed_choice.is_none()
        && (suppress_initial_trace
            || (initial_children_use_merge_property
                && !matches!(winning_choice, CostedJoinChoice::Merge)))
    {
        if let (Some(rows), Some(checkpoint)) = (demand.rows, consumption_before.clone()) {
            rows.restore_filter_consumption(checkpoint);
        }
        return build_join_with_choice(
            source_join,
            current_db,
            catalog,
            ctx,
            trace,
            prune,
            demand,
            required,
            Some(winning_choice),
            kind_override,
            None,
        );
    }

    let merged = matches!(winning_choice, CostedJoinChoice::Merge)
        .then_some(merged)
        .flatten();
    let mut index_join = match winning_choice {
        CostedJoinChoice::Index { decision_index, .. } => {
            index_joins.into_iter().nth(decision_index)
        }
        CostedJoinChoice::Merge | CostedJoinChoice::Hash { .. } => None,
    };
    // Go's join-key cast chain consumes plan-column ids during logical
    // optimization -- after the sources and the SELECT projection's ids,
    // before any physical numbering -- whether or not any strategy uses the
    // rewrite. This point runs exactly once per finally-built join: the
    // cost-only initial pass above returns into `build_join_with_choice`
    // BEFORE reaching it. The chosen cast probe's range text is numbered
    // from the same stream: Go's `indexJoinIntPKRangeInfo` prints the
    // rule's injected cast column, which has no `OrigName` -- `Column#N`.
    if let Some(trace) = trace.as_deref() {
        let cast_stream_ids =
            trace.join_key_cast_stream(coercion_double_cast_pairs, coercion_rewritten.len());
        if let Some(decision) = index_join
            .as_mut()
            .filter(|decision| decision.probe_cast.is_some())
        {
            if let Some(id) = cast_probe_ordinal.and_then(|at| cast_stream_ids.get(at)) {
                decision.range_info = format!("[Column#{id}]");
            }
        }
    }
    let selected_index_name = match winning_choice {
        CostedJoinChoice::Index { kind, .. } => Some(index_join_kind_name(kind)),
        CostedJoinChoice::Merge | CostedJoinChoice::Hash { .. } => forced_index_name,
    };
    if let CostedJoinChoice::Hash { build_is_left } = winning_choice {
        join_exec.set_hash_build_is_left(build_is_left);
    }
    if let Some(plan) = merged.clone() {
        join_exec.set_merge_plan(plan);
    }
    let mut composite_lookup_source = None;
    if let Some(decision) = index_join.as_ref().filter(|decision| decision.composite) {
        let probes = std::rc::Rc::new(std::cell::RefCell::new(
            crate::access_path::SharedIndexJoinProbes::default(),
        ));
        let probe_rows = estimated_matched_rows.or(estimated_join_rows).unwrap_or(
            crate::driver::join_reorder::JoinRows {
                left: 1.0,
                right: 1.0,
                joined: 1.0,
            },
        );
        let probe_candidate = runtime_probe_candidate(
            decision,
            catalog,
            probe_rows,
            if decision.lookup_is_left {
                &left_cost_types
            } else {
                &right_cost_types
            },
        );
        let runtime = crate::driver::leaf_demand::RuntimeLookupDemand {
            table_id: decision.table.table_id,
            object: decision.object.clone(),
            probe_parts: decision.probe_parts.clone(),
            probes: probes.clone(),
            filter_exprs: decision.filter_exprs.clone(),
            probe_candidate,
        };
        let runtime_demand = crate::driver::leaf_demand::FromDemand {
            runtime_lookup: Some(&runtime),
            // A cost-only rebuild of one inner leaf, not a plan being printed.
            partition_fan_out: false,
            ..child_demand
        };
        let target = if decision.lookup_is_left {
            &join.left
        } else {
            right_node
        };
        let checkpoint = demand
            .rows
            .map(crate::driver::join_reorder::RowSource::filter_consumption_checkpoint);
        let (exec, runtime_scope, _) = build_from(
            target,
            catalog,
            current_db,
            ctx,
            None,
            runtime_demand,
            &tidb_planner::physical_property::PhysicalProperty::default(),
        )?;
        if let (Some(rows), Some(checkpoint)) = (demand.rows, checkpoint) {
            rows.restore_filter_consumption(checkpoint);
        }
        let (expected_offsets, expected_schema) = if decision.lookup_is_left {
            (0..left_width, &left_schema)
        } else {
            (left_width..scope.width(), &right_schema)
        };
        let exec = project_composite_lookup_source(
            exec,
            &runtime_scope,
            &scope,
            expected_offsets,
            expected_schema,
            ctx,
        )?;
        composite_lookup_source = Some(crate::join::IndexLookupSource::Composite { exec, probes });
    }
    let index_text = index_join.as_ref().map(|decision| {
        let source_columns: Vec<(String, FieldType)> = if decision.aggregation.is_some() {
            decision.columns.clone()
        } else {
            decision
                .output_offsets
                .iter()
                .filter_map(|offset| decision.columns.get(*offset).cloned())
                .collect()
        };
        let source = composite_lookup_source.take().unwrap_or_else(|| {
            let mut source = crate::access_path::IndexJoinLookupExec::new_with_context(
                ExecutorMeta::new(
                    Schema::new(
                        source_columns
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
            source.set_probe_parts(decision.probe_parts.clone());
            source.set_filters(decision.filter_exprs.clone(), ctx.clone());
            let aggregation_offsets =
                decision
                    .aggregation
                    .as_ref()
                    .map_or_else(Vec::new, |aggregation| {
                        aggregation
                            .group_offsets
                            .iter()
                            .chain(&aggregation.input_offsets)
                            .copied()
                            .collect()
                    });
            source.set_column_projection(
                decision
                    .aggregation
                    .is_none()
                    .then(|| decision.output_offsets.clone()),
                aggregation_offsets,
            );
            crate::join::IndexLookupSource::Leaf(source)
        });
        let physical_name = |offset: usize| {
            let fallback = qualified_scope_column(&scope, current_db, offset);
            let Some(path) = scope.qualified_path(offset) else {
                return fallback;
            };
            let [.., relation, column] = path.as_slice() else {
                return fallback;
            };
            let column = crate::driver::merge_decision::RelColumn {
                relation: relation.clone(),
                column: column.clone(),
            };
            crate::driver::merge_decision::physical_column_trace_name(
                if offset < left_width {
                    &join.left
                } else {
                    right_node
                },
                &column,
                catalog,
                current_db,
            )
            .unwrap_or(fallback)
        };
        // Go's `completePhysicalIndexJoin` removes equality keys that the
        // chosen access path did not admit. `PhysicalIndexJoin.ExplainInfo`
        // prints those physical key slices in their retained join-key order,
        // while sorting the reconstructed equality expressions separately.
        // The executor still uses `decision.probe_keys` in object-key order.
        let outer_node = if decision.lookup_is_left {
            right_node
        } else {
            &join.left
        };
        let display_keys = split
            .keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let (outer, inner) = if decision.lookup_is_left {
                    (left_width + key.right, key.left)
                } else {
                    (key.left, left_width + key.right)
                };
                let crosses_grouping = scope.qualified_path(outer).is_some_and(|path| {
                    let [.., relation, column] = path.as_slice() else {
                        return false;
                    };
                    crate::driver::merge_decision::physical_column_crosses_grouping(
                        outer_node,
                        &crate::driver::merge_decision::RelColumn {
                            relation: relation.clone(),
                            column: column.clone(),
                        },
                        catalog,
                        current_db,
                    )
                });
                let outer = physical_name(outer);
                let inner = physical_name(inner);
                (
                    index,
                    format!("eq({outer}, {inner})"),
                    (outer, inner),
                    crosses_grouping,
                )
            })
            .collect::<Vec<_>>();
        let mut equal_conditions = display_keys
            .iter()
            .map(|(_, condition, _, _)| condition.clone())
            .collect::<Vec<_>>();
        equal_conditions.sort_unstable();
        let mut keys = display_keys
            .into_iter()
            .filter(|(index, _, _, _)| decision.probe_keys.contains(index))
            .collect::<Vec<_>>();
        if demand.physical_source_names
            || keys
                .iter()
                .all(|(_, _, _, crosses_grouping)| *crosses_grouping)
        {
            keys.sort_by(|(_, _, left, _), (_, _, right, _)| left.0.cmp(&right.0));
        }
        let mut keys = keys
            .into_iter()
            .map(|(_, _, key, _)| key)
            .collect::<Vec<_>>();
        // The cast probe's equality is not among the split keys; its outer
        // key is the rule's injected cast column, already numbered into
        // `range_info` (`[Column#N]`), and its inner key is the int side's
        // re-published bare column, which this tier does not number -- Go's
        // unnamed-column fallback. The scan row the harness compares does
        // not read these; they keep the operator info from printing empty
        // key lists.
        if let Some(cast) = &decision.probe_cast {
            let outer = decision
                .range_info
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            let inner = decision
                .output_offsets
                .get(cast.inner_offset)
                .and_then(|offset| decision.columns.get(*offset))
                .map_or_else(
                    || "Column".to_owned(),
                    |(name, _)| format!("{}.{}.{name}", decision.database, decision.visible),
                );
            equal_conditions = vec![format!("eq({outer}, {inner})")];
            keys = vec![(outer, inner)];
        }
        let index = match &decision.object {
            crate::access_path::LookupObject::Index(_) => true,
            crate::access_path::LookupObject::Handle
            | crate::access_path::LookupObject::CommonHandle => false,
        };
        let mut needed_columns = decision.output_offsets.clone();
        if let Some(aggregation) = &decision.aggregation {
            needed_columns.extend(aggregation.group_offsets.iter().copied());
            needed_columns.extend(aggregation.input_offsets.iter().copied());
        }
        needed_columns.extend(crate::access_path::expression_column_offsets(
            &decision.filter_exprs,
        ));
        needed_columns.sort_unstable();
        needed_columns.dedup();
        let index_lookup = match &decision.object {
            crate::access_path::LookupObject::Index(id) => {
                !crate::access_cost::index_is_covering(&decision.table, *id, &needed_columns)
            }
            crate::access_path::LookupObject::Handle
            | crate::access_path::LookupObject::CommonHandle => false,
        };
        let unique = decision.max_one_row();
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
            crate::access_path::LookupObject::CommonHandle => {
                format!("table:{}", decision.visible)
            }
        };
        let (estimated_outer_rows, estimated_source_rows_one) =
            estimated_matched_rows.map_or((None, None), |rows| {
                let outer = if decision.lookup_is_left {
                    rows.right
                } else {
                    rows.left
                };
                (
                    Some(outer),
                    Some(index_join_physical_probe_rows_one(decision, catalog, rows)),
                )
            });
        let (outer_not_null, inner_not_null) = comparison_not_null.iter().copied().fold(
            (Vec::new(), Vec::new()),
            |mut offsets, offset| {
                if decision.lookup_is_left {
                    if offset < left_width {
                        offsets.1.push(offset);
                    } else {
                        offsets.0.push(offset - left_width);
                    }
                } else if offset < left_width {
                    offsets.0.push(offset);
                } else {
                    offsets.1.push(offset - left_width);
                }
                offsets
            },
        );
        // Go rebuilds the physical source once per outer row. A retained
        // aggregation first expands AvgInnerRowCnt back to filtered source
        // rows; #70176 then floors rows-after-access independently. The outer
        // not-null predicate pushed below an eliminated aggregation reduces
        // both physical totals.
        let outer_selectivity = if outer_not_null.is_empty() {
            1.0
        } else {
            crate::plan_trace::SELECTIVITY_FACTOR
        };
        let estimated_lookup_rows = estimated_outer_rows
            .zip(estimated_source_rows_one)
            .map(|(outer, source)| outer * source);
        // A residual leaf Selection can be attached after the index decision
        // has been made (q4's column-to-column date predicate is the notable
        // case). Include its Go pseudo selectivity when expanding the access
        // rows; otherwise the range scan is charged with post-filter rows and
        // the Selection is charged a second time.
        //
        // The columns the rebuilt range already fixes to a constant are
        // excluded here for the same reason `decision.filter_selectivity`
        // excludes them: Go's range CONSUMES those equalities, so its
        // `countAfterAccess` never divides by their selectivity. See
        // `IndexJoinDecision::static_key_columns`.
        let effective_filter_selectivity = if decision.filter_selectivity < 1.0 {
            decision.filter_selectivity
        } else {
            demand
                .rows
                .and_then(|rows| rows.filters_for(&decision.visible))
                .map(|filters| {
                    crate::driver::index_join_decision::residual_filter_selectivity(
                        filters,
                        &decision.static_key_columns(),
                        &decision.columns,
                        &decision.table,
                        &decision.visible,
                        catalog
                            .table_statistics(decision.table.stats_physical_id())
                            .map(AsRef::as_ref),
                        &ctx.session_zone(),
                    )
                })
                .unwrap_or(decision.filter_selectivity)
        };
        // Go's inner TABLE-scan task (`constructDS2TableScanTask`,
        // `exhaust_physical_plans.go:857-874`) prices one outer row as
        // `AvgInnerRowCnt`, divided by the residual selectivity when the scan
        // carries filters, and caps it at 1.0 only for a max-one-row object.
        // There is no LOWER bound on it. Its inner INDEX-scan task has an
        // upper one -- `rowCountUpperBound = TableStats.RowCount /
        // joinKeyNDV`, applied as `math.Min` (`:1144`) -- and even that is
        // gated behind `fixcontrol.Fix44855`, which defaults to false.
        //
        // `probe_access_rows_floor` computes that same `RowCount / NDV`
        // quantity and applied it with `.max()`, as a FLOOR. Same formula,
        // opposite direction, on the path Go bounds least: for TPCC's
        // `orders` probe (2 of 3 clustered-key columns, so not max-one-row)
        // it raised a per-outer-row estimate to `300000/1`, the whole table.
        let estimated_access_rows = estimated_outer_rows
            .zip(estimated_source_rows_one)
            .map(|(outer, source)| {
                let before_filter = if effective_filter_selectivity > 0.0 {
                    source / effective_filter_selectivity.clamp(0.0, 1.0)
                } else {
                    source
                };
                outer * before_filter * outer_selectivity
            });
        let estimated_index_join_rows = estimated_join_rows.map(|rows| {
            if outer_not_null.is_empty() {
                rows.joined
            } else {
                rows.joined * crate::plan_trace::SELECTIVITY_FACTOR
            }
        });
        let grouped_reader = if decision.aggregation.is_some() {
            if decision.aggregation_stream_ordered() {
                "StreamAgg"
            } else {
                "HashAgg"
            }
        } else {
            ""
        };
        join_exec.set_index_lookup_plan(crate::join::IndexLookupPlan {
            lookup_is_left: decision.lookup_is_left,
            probe_keys: decision.probe_keys.clone(),
            source,
            aggregation: decision.aggregation.clone(),
            aggregation_stream_ordered: decision.aggregation_stream_ordered(),
            outer_not_null: outer_not_null.clone(),
            inner_not_null: inner_not_null.clone(),
            probe_cast: decision.probe_cast.clone(),
        });
        join_exec.set_consumes_where(pushed_consumes_where || decision.consumes_where);
        (
            crate::plan_trace::IndexJoinText {
                reader: if decision.composite {
                    "HashJoin"
                } else if decision.aggregation.is_some() && !inner_not_null.is_empty() {
                    "Selection"
                } else if decision.aggregation.is_some() {
                    grouped_reader
                } else if index_lookup {
                    "IndexLookUp"
                } else if index {
                    "IndexReader"
                } else {
                    "TableReader"
                },
                keys,
                equal_conditions,
                lookup_is_left: decision.lookup_is_left,
                unique,
                forced_name: selected_index_name,
                // `getAvgRowSize(.., Schema().Columns)` for each side. Go
                // reaches its `HistColl` branch when the child is a
                // `DataSource`; this tier has no `HistColl` at a join's child
                // and so takes the same function's OTHER branch, the static
                // type width, which is what Go itself falls back to.
                outer_row_size: if decision.lookup_is_left {
                    right_candidate_row_size
                } else {
                    left_candidate_row_size
                },
                inner_row_size: if decision.lookup_is_left {
                    left_candidate_row_size
                } else {
                    right_candidate_row_size
                },
                estimated_outer_rows,
                estimated_probe_rows_one: estimated_source_rows_one,
                estimated_join_rows: estimated_index_join_rows,
            },
            decision.lookup_is_left,
            access,
            decision.range_info.clone(),
            index,
            index_lookup,
            estimated_lookup_rows,
            estimated_access_rows,
            outer_not_null,
            inner_not_null,
        )
    });
    // The plan row and the executor are one decision: if the recorder cannot
    // rewrite the inner side's scan into the range it now reads, the trace is
    // refused rather than printed with a whole-table read under an index
    // join. The EXECUTOR is unaffected -- it still answers correctly.
    let index_text = match (index_text, trace.as_deref_mut()) {
        (
            Some((
                text,
                lookup_is_left,
                access,
                range_info,
                index,
                index_lookup,
                estimated_lookup_rows,
                estimated_access_rows,
                outer_not_null,
                inner_not_null,
            )),
            Some(trace),
        ) => {
            let decision = index_join
                .as_ref()
                .expect("printable index text has an index decision");
            let qualify = crate::plan_trace::Qualifier {
                db: current_db,
                scope: &scope,
                catalog: Some(catalog),
            };
            let filter_scope = crate::plan_trace::PlanTrace::single_table_scope(
                &decision.table.name,
                Some(decision.database.clone()),
                decision.columns.clone(),
            );
            let filter_qualify = crate::plan_trace::Qualifier {
                db: &decision.database,
                scope: &filter_scope,
                catalog: Some(catalog),
            };
            let filters = filter_qualify
                .expressions(&decision.filter_exprs)
                .map(|filters| vec![filters])
                .unwrap_or_else(|| {
                    let mut filters: Vec<String> = decision
                        .filters
                        .iter()
                        .map(|filter| qualify.expr(filter))
                        .collect();
                    filters.sort_unstable();
                    filters
                });
            if trace
                .index_join_inner_scan(
                    lookup_is_left,
                    crate::plan_trace::IndexJoinInnerPathText {
                        access,
                        range_info: &range_info,
                        index,
                        index_lookup,
                        visible: &decision.visible,
                        estimated_rows: estimated_lookup_rows,
                        estimated_access_rows,
                        estimated_outer_rows: text.estimated_outer_rows,
                        unique: text.unique,
                        keep_outer_order: !required.is_sort_item_empty(),
                        grouped_derived: decision.aggregation.is_some(),
                        composite: decision.composite,
                        stream_aggregation: decision.aggregation_stream_ordered(),
                        aggregation_info: decision.aggregation_info.as_deref(),
                        aggregation_final_info: decision.aggregation_final_info.as_deref(),
                        aggregation_partial_info: decision.aggregation_partial_info.as_deref(),
                        outer_not_null: &outer_not_null,
                        inner_not_null: &inner_not_null,
                    },
                    &filters,
                    decision.filter_selectivity,
                )
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
    let build_is_left = match winning_choice {
        CostedJoinChoice::Hash { build_is_left } => build_is_left,
        CostedJoinChoice::Merge | CostedJoinChoice::Index { .. } => build_is_left,
    };
    let strategy = crate::plan_trace::JoinStrategy {
        equal_mask: split.equal_mask.clone(),
        build_is_left: index_join
            .as_ref()
            .map_or(build_is_left, |decision| !decision.lookup_is_left),
        left_width,
        index_lookup: index_text,
        physical_conditions,
        estimated_join_rows: estimated_join_rows.map(|rows| rows.joined),
        merge_keys: merged.as_ref().map(|plan| {
            plan.keys
                .iter()
                .zip(
                    merge_trace_names
                        .as_ref()
                        .expect("a committed merge keeps its logical key names"),
                )
                .map(|(key, (left_name, right_name))| {
                    let left = crate::driver::merge_decision::physical_column_trace_name(
                        &join.left, left_name, catalog, current_db,
                    )
                    .unwrap_or_else(|| qualified_scope_column(&scope, current_db, key.left));
                    let right = crate::driver::merge_decision::physical_column_trace_name(
                        right_node, right_name, catalog, current_db,
                    )
                    .unwrap_or_else(|| {
                        qualified_scope_column(&scope, current_db, left_width + key.right)
                    });
                    (left, right)
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
    let merged_orders = merged.as_ref().and_then(|_| {
        let (left_names, right_names) = merge_required_names.as_ref()?;
        let left: Option<Vec<usize>> = left_names
            .iter()
            .map(|name| {
                let offset = scope_offset_of(&scope, name)?;
                (offset < left_width).then_some(offset)
            })
            .collect();
        let right: Option<Vec<usize>> = right_names
            .iter()
            .map(|name| {
                let offset = scope_offset_of(&scope, name)?;
                (offset >= left_width).then_some(offset)
            })
            .collect();
        Some((left?, right?))
    });
    let committed_candidate = estimated_join_rows.and_then(|rows| {
        let candidate = match winning_choice {
            CostedJoinChoice::Merge => merge_join_candidate(
                left_delivered.candidate.clone()?,
                right_delivered.candidate.clone()?,
                rows,
                split.keys.len(),
                split.equal_mask.iter().filter(|equal| !**equal).count(),
            ),
            CostedJoinChoice::Index { kind, .. } => {
                let decision = index_join.as_ref()?;
                let outer = if decision.lookup_is_left {
                    right_delivered.candidate.clone()?
                } else {
                    left_delivered.candidate.clone()?
                };
                index_join_candidate(
                    decision,
                    outer,
                    if decision.lookup_is_left {
                        left_delivered.candidate.as_ref()
                    } else {
                        right_delivered.candidate.as_ref()
                    },
                    catalog,
                    rows,
                    estimated_matched_rows.unwrap_or(rows),
                    &left_cost_types,
                    &right_cost_types,
                    split.keys.len(),
                    kind,
                    semi_join,
                )
            }
            CostedJoinChoice::Hash { build_is_left } => hash_join_candidate(
                left_delivered.candidate.clone()?,
                right_delivered.candidate.clone()?,
                split.keys.len(),
                build_is_left,
                ctx.hash_join_concurrency(),
            ),
        };
        Some(fixed_join_receipt(
            candidate,
            rows.joined,
            crate::access_cost::schema_avg_row_size(&output_cost_types),
        ))
    });
    let mut delivered = if let Some((left, right)) = merged_orders {
        Delivered::from_orders(crate::driver::merge_decision::union_orders(
            join.tp,
            vec![left],
            vec![right],
        ))
    } else if let Some(decision) = &index_join {
        let outer = if decision.lookup_is_left {
            right_delivered
                .iter()
                .map(|order| order.iter().map(|at| at + left_width).collect())
                .collect()
        } else {
            left_delivered.orders.clone()
        };
        if decision.lookup_is_left {
            Delivered::from_orders(crate::driver::merge_decision::union_orders(
                join.tp,
                Vec::new(),
                outer,
            ))
        } else {
            Delivered::from_orders(crate::driver::merge_decision::union_orders(
                join.tp,
                outer,
                Vec::new(),
            ))
        }
    } else {
        Delivered::new()
    };
    delivered.candidate = committed_candidate;
    delivered.semi_join = semi_join;
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
        } else {
            if index_join.is_none() {
                trace.join_scan_readers();
            }
            if trace
                .join(join, kind, &scope, current_db, &pushed, &strategy)
                .is_err()
            {
                trace.refuse("this join's plan is not supported yet");
            }
        }
    }
    // Set, never merged: this is a property of the node at the TOP of the
    // `FROM` plan, and the top is whichever `build_join` returns last. See
    // [`FromScope::qualified_star_is_output_only`].
    scope.qualified_star_is_output_only = join.tp == tidb_ast::JoinType::Cross && join.on.is_some();
    if semi_join {
        scope.tables.retain(|table| table.offset < left_width);
        scope.coalesced.retain(|offset| *offset < left_width);
        scope.star.retain(|offset| *offset < left_width);
    }
    Ok((meter(exec, trace), scope, delivered))
}

/// Go's LogicalJoin exposes the left child schema followed by the right child
/// schema. The logical FromScope is not suitable for executor chunks: child
/// column pruning can make it wider or differently ordered than the schemas
/// the children actually write.
fn join_executor_schema(semi_join: bool, left: &Schema, right: &Schema) -> Schema {
    if semi_join {
        left.clone()
    } else {
        tidb_expr::schema::merge_schema(Some(left), Some(right))
            .expect("a non-semi join has both child schemas")
    }
}

/// Restores the output contract of a composite index-lookup child after its
/// runtime subtree is rebuilt with a dynamic probe source. Go's parent
/// executor keeps consuming the original child's pruned `Schema`; rebuilding
/// the target is allowed to change access paths, but not its visible columns.
fn project_composite_lookup_source(
    exec: Box<dyn Executor>,
    runtime_scope: &FromScope,
    expected_scope: &FromScope,
    expected_offsets: std::ops::Range<usize>,
    expected_schema: &Schema,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let expected_offsets = expected_offsets.collect::<Vec<_>>();
    if expected_offsets.len() != expected_schema.len()
        || runtime_scope.width() != exec.schema().len()
    {
        return Err(ExecError::internal(
            "composite index lookup rebuilt an incompatible output schema",
        )
        .into());
    }

    let runtime_schema = exec.schema().clone();
    let resolver = ScopeResolver {
        scope: runtime_scope,
    };
    let mut input_offsets = Vec::with_capacity(expected_offsets.len());
    let mut expressions = Vec::with_capacity(expected_offsets.len());
    for (output_offset, expected_offset) in expected_offsets.into_iter().enumerate() {
        let path = expected_scope
            .qualified_path(expected_offset)
            .ok_or_else(|| {
                ExecError::internal("composite index lookup expected an unnameable output column")
            })?;
        let (input_offset, _, _) = resolver.resolve(&path).ok_or_else(|| {
            ExecError::internal("composite index lookup could not restore a pruned output column")
        })?;
        let input_column = runtime_schema.columns.get(input_offset).ok_or_else(|| {
            ExecError::internal("composite index lookup resolved a column outside its schema")
        })?;
        let expected_column = expected_schema.columns.get(output_offset).ok_or_else(|| {
            ExecError::internal("composite index lookup expected schema is incomplete")
        })?;
        if input_column.ret_type != expected_column.ret_type {
            return Err(ExecError::internal(
                "composite index lookup restored a column with an incompatible type",
            )
            .into());
        }
        let mut column = input_column.clone();
        column.index = input_offset as i64;
        input_offsets.push(input_offset);
        expressions.push(Expression::Column(column));
    }

    let identity = runtime_schema.len() == expected_schema.len()
        && input_offsets.iter().copied().eq(0..runtime_schema.len());
    if identity {
        return Ok(exec);
    }

    let init_cap = exec.init_cap();
    let max_chunk_size = exec.max_chunk_size();
    Ok(Box::new(ProjectionExec::new(
        ExecutorMeta::new(expected_schema.clone(), 2, init_cap, max_chunk_size),
        expressions,
        exec,
        ctx.clone(),
    )))
}

fn sole_relation_name(node: &JoinNode) -> Option<&str> {
    match node {
        JoinNode::Table(table) => table
            .alias
            .as_deref()
            .or_else(|| table.name.last().map(String::as_str)),
        JoinNode::Join(join)
            if join.right.is_none()
                && join.on.is_none()
                && join.using.is_empty()
                && !join.natural =>
        {
            sole_relation_name(&join.left)
        }
        JoinNode::Derived { alias, .. } => alias.as_deref(),
        JoinNode::Join(_) => None,
    }
}

fn enforced_merge_key_name(
    key: &crate::driver::merge_decision::RelColumn,
    current_db: &str,
) -> String {
    if key.column.starts_with("_inject_") {
        "Column".to_owned()
    } else {
        format!("{current_db}.{}.{}", key.relation, key.column)
    }
}

fn enforced_merge_sort(
    exec: Box<dyn Executor>,
    keys: &[usize],
    desc: bool,
    names: &[String],
    delivered: &mut Delivered,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
) -> Box<dyn Executor> {
    let types = exec.ret_field_types();
    let by_items = keys
        .iter()
        .map(|at| {
            let mut column = Column::new((*at + 1) as i64, types[*at].clone());
            column.index = *at as i64;
            SortByItem {
                expr: Expression::Column(column),
                desc,
            }
        })
        .collect::<Vec<_>>();
    let schema = exec.schema().clone();
    let mut sorted: Box<dyn Executor> = Box::new(SortExec::new(
        ExecutorMeta::new(schema, 3, INIT_CAP, MAX_CHUNK_SIZE),
        by_items,
        exec,
        ctx.clone(),
        ctx.statement_memory(),
    ));
    if let Some(child) = delivered.candidate.take() {
        let costed = tidb_planner::candidate_cost::evaluate(
            &child,
            &tidb_planner::candidate_cost::CostEnv::default(),
            tidb_planner::task_type::TaskType::Root,
        );
        delivered.candidate = Some(tidb_planner::candidate_cost::Candidate::Sort {
            child: Box::new(child),
            rows: costed.rows,
            row_size: tidb_planner::candidate_cost::RowSize::Fixed(costed.row_size),
            by_items: vec![false; keys.len()],
        });
    }
    delivered.orders = vec![keys.to_vec()];
    if let Some(trace) = trace {
        trace.enforced_merge_sort(names, desc);
        sorted = trace.meter(sorted);
    }
    sorted
}

fn collect_physical_join_conjuncts(expression: &Expression, out: &mut Vec<Expression>) {
    if let Expression::ScalarFunction(function) = expression {
        if function.func_name.lowercase() == "and" {
            for argument in &function.args {
                collect_physical_join_conjuncts(argument, out);
            }
            return;
        }
    }
    out.push(expression.clone());
}

fn refine_other_join_conditions(
    conditions: &mut [Expression],
    equal_mask: &[bool],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    fn refine(
        expression: &mut Expression,
        equal_mask: &[bool],
        at: &mut usize,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        if let Expression::ScalarFunction(function) = expression {
            if function.func_name.lowercase() == "and" {
                for argument in &mut function.args {
                    refine(argument, equal_mask, at, ctx)?;
                }
                return Ok(());
            }
        }
        let is_equal = equal_mask.get(*at).copied().unwrap_or(false);
        *at += 1;
        if !is_equal {
            tidb_expr::builtin_compare::refine_comparisons(expression, ctx)
                .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
        }
        Ok(())
    }

    let mut at = 0;
    for condition in conditions {
        refine(condition, equal_mask, &mut at, ctx)?;
    }
    debug_assert_eq!(at, equal_mask.len());
    Ok(())
}

/// Direct column arguments a NULL-propagating comparison proves non-NULL.
/// `NullEQ` is deliberately absent: it is true for two NULL operands.
fn collect_comparison_not_null_columns(expression: &Expression, out: &mut Vec<usize>) {
    let Expression::ScalarFunction(function) = expression else {
        return;
    };
    let name = function.func_name.lowercase();
    if name == "and" {
        for argument in &function.args {
            collect_comparison_not_null_columns(argument, out);
        }
        return;
    }
    if !matches!(name, "eq" | "ne" | "lt" | "le" | "gt" | "ge") {
        return;
    }
    out.extend(function.args.iter().filter_map(|argument| {
        let Expression::Column(column) = argument else {
            return None;
        };
        usize::try_from(column.index).ok()
    }));
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
pub(super) fn scope_offset_of(
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
                "{}.{}.{}",
                database.to_lowercase(),
                table.name.to_lowercase(),
                table.columns[offset - table.offset].0.to_lowercase()
            );
        }
    }
    String::new()
}

#[cfg(test)]
mod join_schema_tests {
    use super::{join_executor_schema, project_composite_lookup_source, FromScope};
    use crate::driver::FromTable;
    use crate::executor::ExecutorMeta;
    use crate::mem_table::MemTableSourceExec;
    use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode, StringDatum};
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    fn schema(types: &[FieldTypeCode]) -> Schema {
        Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(index, code)| Column::new(index as i64 + 1, FieldType::new(*code)))
                .collect(),
        )
    }

    fn scope(tables: &[(&str, &[(&str, FieldTypeCode)])]) -> FromScope {
        let mut offset = 0;
        let mut scope = FromScope::default();
        for (name, columns) in tables {
            scope.tables.push(FromTable {
                name: (*name).to_owned(),
                database: Some("test".to_owned()),
                columns: columns
                    .iter()
                    .map(|(name, code)| ((*name).to_owned(), FieldType::new(*code)))
                    .collect(),
                offset,
                func_deps: crate::driver::funcdep::TableFuncDeps::default(),
                physical: None,
            });
            offset += columns.len();
        }
        scope
    }

    #[test]
    fn join_output_schema_follows_pruned_child_order() {
        let left = schema(&[FieldTypeCode::LongLong, FieldTypeCode::Varchar]);
        let right = schema(&[FieldTypeCode::Varchar, FieldTypeCode::LongLong]);
        let output = join_executor_schema(false, &left, &right);
        let types = output
            .columns
            .iter()
            .map(|column| column.ret_type.clone().expect("column type"))
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                FieldType::new(FieldTypeCode::LongLong),
                FieldType::new(FieldTypeCode::Varchar),
                FieldType::new(FieldTypeCode::Varchar),
                FieldType::new(FieldTypeCode::LongLong),
            ],
            "executor output must be Go MergeSchema(left child, right child)",
        );
    }

    #[test]
    fn semi_join_output_schema_keeps_only_the_left_child() {
        let left = schema(&[FieldTypeCode::LongLong]);
        let right = schema(&[FieldTypeCode::Varchar]);
        let output = join_executor_schema(true, &left, &right);
        assert_eq!(output.columns.len(), 1);
        assert_eq!(
            output.columns[0].ret_type,
            Some(FieldType::new(FieldTypeCode::LongLong))
        );
    }

    #[test]
    fn composite_lookup_restores_the_pruned_child_schema() {
        let runtime_scope = scope(&[
            (
                "supplier",
                &[
                    ("s_suppkey", FieldTypeCode::LongLong),
                    ("s_name", FieldTypeCode::Varchar),
                    ("s_nationkey", FieldTypeCode::LongLong),
                ],
            ),
            (
                "nation",
                &[
                    ("n_nationkey", FieldTypeCode::LongLong),
                    ("n_name", FieldTypeCode::Varchar),
                    ("n_regionkey", FieldTypeCode::LongLong),
                ],
            ),
            (
                "part",
                &[
                    ("p_partkey", FieldTypeCode::LongLong),
                    ("p_name", FieldTypeCode::Varchar),
                    ("p_size", FieldTypeCode::LongLong),
                ],
            ),
        ]);
        let expected_scope = scope(&[
            (
                "outer",
                &[
                    ("c0", FieldTypeCode::LongLong),
                    ("c1", FieldTypeCode::LongLong),
                    ("c2", FieldTypeCode::LongLong),
                    ("c3", FieldTypeCode::LongLong),
                    ("c4", FieldTypeCode::LongLong),
                    ("c5", FieldTypeCode::LongLong),
                    ("c6", FieldTypeCode::LongLong),
                    ("c7", FieldTypeCode::LongLong),
                ],
            ),
            (
                "supplier",
                &[
                    ("s_name", FieldTypeCode::Varchar),
                    ("s_nationkey", FieldTypeCode::LongLong),
                ],
            ),
            (
                "nation",
                &[
                    ("n_name", FieldTypeCode::Varchar),
                    ("n_nationkey", FieldTypeCode::LongLong),
                ],
            ),
        ]);
        let runtime_schema = schema(&[
            FieldTypeCode::LongLong,
            FieldTypeCode::Varchar,
            FieldTypeCode::LongLong,
            FieldTypeCode::LongLong,
            FieldTypeCode::Varchar,
            FieldTypeCode::LongLong,
            FieldTypeCode::LongLong,
            FieldTypeCode::Varchar,
            FieldTypeCode::LongLong,
        ]);
        let expected_schema = schema(&[
            FieldTypeCode::Varchar,
            FieldTypeCode::LongLong,
            FieldTypeCode::Varchar,
            FieldTypeCode::LongLong,
        ]);
        let source = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(runtime_schema, 0, 1, 32),
            vec![vec![
                Datum::Int(1),
                Datum::Bytes(b"Supplier#1".to_vec()),
                Datum::Int(10),
                Datum::Int(10),
                Datum::Bytes(b"ALGERIA".to_vec()),
                Datum::Int(0),
                Datum::Int(99),
                Datum::Bytes(b"green part".to_vec()),
                Datum::Int(7),
            ]],
        ));
        let mut projected = project_composite_lookup_source(
            source,
            &runtime_scope,
            &expected_scope,
            8..12,
            &expected_schema,
            &crate::StmtContext::for_query(),
        )
        .expect("all pruned child columns exist in the rebuilt subtree");

        projected.open().unwrap();
        let types = projected.ret_field_types().to_vec();
        let mut chunk = projected.new_chunk();
        projected.next(&mut chunk).unwrap();
        assert_eq!(chunk.num_cols(), 4);
        assert_eq!(
            chunk.get_row(0).get_datum_row(&types),
            vec![
                Datum::String(StringDatum::new(
                    b"Supplier#1".to_vec(),
                    Collation::Utf8Mb4Bin,
                )),
                Datum::Int(10),
                Datum::String(StringDatum::new(b"ALGERIA".to_vec(), Collation::Utf8Mb4Bin,)),
                Datum::Int(10),
            ],
        );
        projected.close().unwrap();
    }
}

/// Go `buildDataSource`'s extra handle column, for a table that has one.
///
/// A table whose primary key IS the handle reports that column instead, and a
/// clustered common handle builds `HandleCols` from the primary index -- in
/// both cases `_tidb_rowid` names nothing, which is why TiDB answers
/// "Unknown column" for it there. Only a HEAP table gets the extra column.
pub(crate) fn extra_handle_column(entry: &TableEntry) -> Option<(String, FieldType)> {
    let TableEntry::Kv(kv) = entry else {
        return None;
    };
    if kv.pk_handle_offset().is_some() || !kv.common_handle_offsets().is_empty() {
        return None;
    }
    Some((
        crate::driver::leaf_demand::EXTRA_HANDLE_NAME.to_owned(),
        FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
            .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
    ))
}
