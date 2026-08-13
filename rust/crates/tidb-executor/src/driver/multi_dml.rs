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

//! Multi-table `UPDATE` and `DELETE`.
//!
//! Go `executor.UpdateExec` (its `tblColPosInfos` path) and
//! `executor.DeleteExec.deleteMultiTablesByChunk` both read ONE joined row
//! stream and write back to several base tables, so each output row has to
//! carry the identity of the base row every table contributed. Go gets that
//! from `TblColPosInfo.HandleCols`, a handle column the planner adds to the
//! join's schema per target table. The read path here does the same thing by
//! keeping the handle BESIDE the values instead of inside them
//! ([`SourceRow`]), which is why a multi-table write cannot reuse
//! `build_from`'s executor tree -- that tree emits values only.
//!
//! The rules below were captured from a real TiDB session (`mockstore`
//! `session.Execute`, reading affected rows off `StmtCtx`), not inferred:
//!
//! * **UPDATE-ONCE.** A base row reachable through several join paths is
//!   written once: `UPDATE a JOIN b ON a.id = b.aid SET a.x = a.x + 1` with
//!   two `b` rows per `a` row leaves `a.x` at `x+1`, not `x+2`. Go keys
//!   `updatedRowKeys` by (target position, handle) and skips a repeat --
//!   but only when the FIRST visit actually CHANGED the row, which is why
//!   [`UpdateOnce`] remembers a bool rather than mere presence.
//! * **Assignments read the ORIGINAL row, across tables.**
//!   `UPDATE s1, s2 SET s1.x = s2.y, s2.y = s1.x` swaps the two values: both
//!   right-hand sides see the joined row as the statement found it. This is
//!   the single-table `SET` rule (`compute_updated_row`) widened to the whole
//!   join, so it is one reading, not two.
//! * **Affected rows are CHANGED rows, summed over target tables.**
//!   `SET q1.v = q1.v, q2.v = 5` reports 1, not 2. A join matching nothing
//!   reports 0.
//! * **An outer join's NULL-padded side is not written.** Go's
//!   `unmatchedOuterRow` reads the handle column and skips a NULL one;
//!   `UPDATE y1 LEFT JOIN y2 ON ... SET y2.v = 9` touches only the rows `y2`
//!   really had.
//! * **The same physical table joined under two aliases is two targets.**
//!   `UPDATE z1 AS p JOIN z1 AS q ON p.id = q.id SET p.v = p.v + 1,
//!   q.v = q.v + 10` reports 4 for a two-row table and stores `q`'s value:
//!   the update-once key is the target POSITION, so each alias writes the
//!   row once and the later write wins.
//! * **DELETE dedups by physical TABLE, not by position.** Go's `tblRowMap`
//!   is keyed by `TblID`, so `DELETE t1, t1 FROM ...` removes each row once
//!   and reports 1. A target row reachable through several join paths is
//!   likewise deleted once.
//! * **A DELETE target is named by its ALIAS when it has one.**
//!   `DELETE x FROM f1 AS x JOIN f2` works; `DELETE f1 FROM f1 AS x JOIN f2`
//!   is Go's `ERROR 1109 Unknown table 'f1' in MULTI DELETE`, as is naming a
//!   table the `FROM` never mentions. A schema-qualified `DELETE test.t1
//!   FROM t1 ...` resolves when the table is not aliased. `DELETE a FROM ...`
//!   and `DELETE FROM a USING ...` are the same statement.
//! * **`ORDER BY`/`LIMIT`.** The parser already carries this: a multi-table
//!   `DELETE` rejects both with a syntax error (Go errno 1064), and so does
//!   a COMMA-joined `UPDATE` (Go errno 1221, "Incorrect usage of UPDATE and
//!   LIMIT"). An explicitly `JOIN`ed `UPDATE` accepts them, and the `LIMIT`
//!   caps the JOINED ROWS the statement reaches -- the same "rows reached,
//!   not rows changed" reading the single-table path already has.
//!
//! * **A derived table is a READ source, never a target.** Go builds the
//!   whole `FROM` (`buildResultSetNode`) before it decides what is writable,
//!   and decides that separately: `updatableTableListResolver.Leave` adds a
//!   `TableSource` to the updatable list only when
//!   `v.Source.(*ast.TableName)` succeeds, so a subquery source is simply
//!   absent from it. `buildUpdateLists` then turns that absence into an
//!   error at the `SET` column -- "1: update (select * from t1) t1 set b =
//!   1111111 ----- (no updatable table here) ... subQuery is not counted as
//!   updatable table", `ErrNonUpdatableTable` (1288). `DELETE` reaches the
//!   same place through `collectTableName`, whose `canUpdate` is likewise
//!   the `*ast.TableName` type assertion. So `UPDATE t t1, (SELECT ...) t2
//!   SET t1.b = t2.b` WRITES, and only a `SET`/`DELETE` target naming `t2`
//!   is refused.
//!
//! A `NATURAL`/`USING` join uses the same equality and naming rules as the
//! query path, while retaining both physical columns and their row identities
//! for the write phase. Go likewise restores the full child schema for
//! `UPDATE`/`DELETE` after building the coalesced join condition.
//!
//! `LATERAL` derived sources are an Apply: their query is rebound and run for
//! every left-row snapshot before the ordinary join/write logic consumes the
//! resulting rows. The derived side remains read-only because it has no base
//! row identity.

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::kv_table::TableHandle;

/// The identity of a base-table row, so a joined row can be written back to
/// the row it came from. This is Go's `HandleCols`-derived `kv.Handle` for a
/// stored table, and the row's position for a matrix-backed one (which has
/// no handles; the position is stable because every write of one statement
/// is applied to a snapshot taken before the first of them).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RowId {
    Kv(TableHandle),
    Mem(usize),
}

/// Where a `FROM` source's rows live, and therefore whether a write may name
/// it. This is Go's `updatableTableListResolver`/`collectTableName` decision,
/// which both make by the same test -- `x.Source.(*ast.TableName)` -- and
/// which is settled once here rather than at each write site.
#[derive(Clone)]
enum SourceOrigin {
    /// A base table, identified for writing back by schema and stored name.
    Base {
        /// The schema the table really lives in.
        database: String,
        /// The stored table name.
        name: String,
    },
    /// A derived table: rows materialized from a subquery, with no base-table
    /// identity behind them. A read source only.
    Derived,
}

/// One source participating in a multi-table DML statement's `FROM`.
#[derive(Clone)]
struct SourceTable {
    /// The name the statement qualifies it with: the alias when it has one.
    visible: String,
    /// The schema, when a `db.t` reference may still name it -- `None` once
    /// an alias has replaced the whole path, exactly as in [`FromTable`].
    qualifiable_db: Option<String>,
    /// Whether a write may name this source, and where it writes if so.
    origin: SourceOrigin,
    columns: Vec<(String, FieldType)>,
    /// Default/generation metadata aligned with `columns`.
    default_meta: Vec<super::dml::ColumnDefaultMeta>,
    /// Where this table's columns start in the joined row.
    offset: usize,
}

impl SourceTable {
    fn end(&self) -> usize {
        self.offset + self.columns.len()
    }

    /// Go's `ErrNonUpdatableTable` for this source, so the two write sites
    /// that can reach a non-updatable source raise ONE error.
    fn not_updatable(&self, statement: &'static str) -> DriverError {
        DriverError::NonUpdatableTable {
            table: self.visible.clone(),
            statement,
        }
    }
}

/// A joined row: one row identity per participating table (`None` where an
/// outer join NULL-padded that side), then the concatenated column values.
type SourceRow = (Vec<Option<RowId>>, Vec<Datum>);

/// The joined row source a multi-table write reads.
struct MultiSource {
    tables: Vec<SourceTable>,
    rows: Vec<SourceRow>,
    /// The statement's session `time_zone`, carried into [`Self::scope`] so
    /// the `WHERE`/`ON`/`SET` rewrites over this source fold temporal
    /// literals in the session's zone (see [`FromScope::zone`]).
    zone: tidb_expr::SessionTimeZone,
    tidb_info_len: usize,
    /// The output naming state of a child `NATURAL`/`USING` join. The row
    /// remains full-width for writes, just as Go resets the join schema for
    /// DML after using the coalesced names to construct its equality.
    coalesced: Vec<usize>,
    star: Vec<usize>,
}

impl MultiSource {
    fn width(&self) -> usize {
        self.tables.last().map_or(0, SourceTable::end)
    }

    /// The name scope, so `WHERE`/`ON`/`SET` resolve through the very same
    /// [`ScopeResolver`] a `SELECT` over this `FROM` would use.
    fn scope(&self) -> FromScope {
        FromScope {
            tables: self
                .tables
                .iter()
                .map(|table| FromTable {
                    name: table.visible.clone(),
                    database: table.qualifiable_db.clone(),
                    columns: table.columns.clone(),
                    offset: table.offset,
                    func_deps: Default::default(),
                })
                .collect(),
            zone: self.zone.clone(),
            tidb_info_len: self.tidb_info_len,
            coalesced: self.coalesced.clone(),
            star: self.star.clone(),
            ..FromScope::default()
        }
    }

    fn field_types(&self) -> Vec<FieldType> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().map(|(_, ft)| ft.clone()))
            .collect()
    }

    fn column_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().map(|(name, _)| name.clone()))
            .collect()
    }

    /// The table whose columns cover `offset` in the joined row.
    fn table_of_column(&self, offset: usize) -> Option<usize> {
        self.tables
            .iter()
            .position(|t| offset >= t.offset && offset < t.end())
    }
}

/// Reads every base table of `join` into one joined row set, each row
/// carrying its per-table row identity.
fn build_multi_source(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<MultiSource, DriverError> {
    let left = build_multi_node(&join.left, catalog, current_db, ctx)?;
    let Some(right_node) = &join.right else {
        // The single-relation wrapper the parser always produces.
        return Ok(left);
    };
    if let tidb_ast::JoinNode::Derived {
        subquery,
        alias,
        lateral: true,
        column_names,
    } = right_node
    {
        return join_lateral_source(
            left,
            join,
            subquery,
            alias.as_deref(),
            column_names,
            catalog,
            current_db,
            ctx,
        );
    }
    let right = build_multi_node(right_node, catalog, current_db, ctx)?;
    join_sources(left, right, join, ctx)
}

fn build_multi_node(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<MultiSource, DriverError> {
    match node {
        tidb_ast::JoinNode::Table(table_ref) => {
            scan_base_table(table_ref, catalog, current_db, ctx)
        }
        tidb_ast::JoinNode::Join(join) => build_multi_source(join, catalog, current_db, ctx),
        tidb_ast::JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => scan_derived_table(
            subquery,
            alias.as_deref(),
            *lateral,
            column_names,
            catalog,
            current_db,
            ctx,
        ),
    }
}

/// Materializes a derived table as a READ-ONLY source of the join.
///
/// It goes through `from::derived_source_relation`, so the alias rule
/// (`ErrDerivedMustHaveAlias`) and the duplicate-column rule
/// (`ErrDupFieldName`) are the SELECT path's own, not a second reading of
/// them. Its rows carry no [`RowId`]: Go's updatable list never contains a
/// subquery source, so no write can name it and there is no identity to
/// invent.
fn scan_derived_table(
    subquery: &tidb_ast::QueryStmt,
    alias: Option<&str>,
    lateral: bool,
    column_names: &[String],
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<MultiSource, DriverError> {
    if lateral {
        // A LATERAL source is re-evaluated per outer row, which is a
        // different read than the one this join performs.
        return Err(DriverError::unsupported(
            "a LATERAL derived table is not supported in multi-table DML",
        ));
    }
    let (alias, mut columns, rows) = super::from::derived_source_relation(
        subquery,
        alias,
        catalog,
        current_db,
        ctx,
        None,
        &tidb_planner::physical_property::PhysicalProperty::default(),
    )?;
    // The parser refuses `(SELECT ...) t (x, y)` in an UPDATE's `FROM`
    // (Go errno 1064, both the comma and the JOIN spelling), so this list is
    // empty on every statement that reaches here. It is applied anyway
    // because the ALTERNATIVE to applying it is reading the subquery's own
    // column names under the statement's names -- silently wrong values --
    // the moment a parser admits the form.
    super::from::rename_derived_columns(&mut columns, column_names)?;
    let default_meta = columns
        .iter()
        .map(|(name, field_type)| super::dml::ColumnDefaultMeta {
            default_value: None,
            not_null: false,
            no_default_value: false,
            name: name.clone(),
            field_type: field_type.clone(),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            generated: false,
        })
        .collect();
    Ok(MultiSource {
        zone: ctx.session_zone(),
        tidb_info_len: ctx.tidb_info_len(),
        coalesced: Vec::new(),
        star: Vec::new(),
        tables: vec![SourceTable {
            visible: alias.to_owned(),
            // An alias is the only qualifier a derived table answers to.
            qualifiable_db: None,
            origin: SourceOrigin::Derived,
            columns,
            default_meta,
            offset: 0,
        }],
        rows: rows.into_iter().map(|row| (vec![None], row)).collect(),
    })
}

/// Applies a `LATERAL` derived table to every row of `left` before joining it
/// into the full DML row. This is the value/identity analogue of
/// [`super::from::build_lateral_join`]: the same correlation collector and
/// binder are used, while [`join_sources`] keeps the outer base-row identity
/// beside the values for a later UPDATE/DELETE.
#[allow(clippy::too_many_arguments)]
fn join_lateral_source(
    left: MultiSource,
    join: &tidb_ast::Join,
    subquery: &tidb_ast::QueryStmt,
    alias: Option<&str>,
    column_names: &[String],
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<MultiSource, DriverError> {
    // Keep Go `buildLateralJoin`'s accepted/rejected join shapes aligned with
    // the SELECT path. A lateral source is an Apply, not an outer join.
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
        tidb_ast::JoinType::Cross => {}
    }
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };

    let left_scope = left.scope();
    let mut correlated = Vec::new();
    collect_correlated_columns_query(
        subquery,
        &left_scope,
        catalog,
        current_db,
        &mut correlated,
        ctx,
    );
    let probe_resolver = ScopeResolver { scope: &left_scope };
    let probes: Vec<(Vec<String>, Datum)> = correlated
        .iter()
        .map(|path| {
            let datum = probe_resolver
                .resolve(path)
                .map_or(Datum::Null, |(_, field_type, _)| {
                    super::from::probe_datum(&field_type)
                });
            (path.clone(), datum)
        })
        .collect();
    let typed = bind_subquery_columns_query(subquery, &probes)?;
    let (probe_columns, _) = run_query_stmt(&typed, catalog, current_db, ctx)?;
    let mut columns = match super::from::derived_field_names_query(subquery) {
        Some(names) if names.len() == probe_columns.len() => names
            .into_iter()
            .zip(&probe_columns)
            .map(|(name, (_, field_type))| (name, field_type.clone()))
            .collect(),
        _ => probe_columns,
    };
    for (index, (name, _)) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateColumnName(name.clone()));
        }
    }
    super::from::rename_derived_columns(&mut columns, column_names)?;
    let correlated_indices = correlated_path_indices(&correlated, &left_scope)?;

    let derived = SourceTable {
        visible: alias.to_owned(),
        qualifiable_db: None,
        origin: SourceOrigin::Derived,
        columns,
        default_meta: Vec::new(),
        offset: 0,
    };
    let MultiSource {
        tables: left_tables,
        rows: left_rows,
        zone,
        tidb_info_len,
        coalesced,
        star,
    } = left;
    let new_left = |rows| MultiSource {
        tables: left_tables.clone(),
        rows,
        zone: zone.clone(),
        tidb_info_len,
        coalesced: coalesced.clone(),
        star: star.clone(),
    };
    let new_right = |rows| MultiSource {
        tables: vec![derived.clone()],
        rows,
        zone: zone.clone(),
        tidb_info_len,
        coalesced: Vec::new(),
        star: Vec::new(),
    };

    // Build the joined scope once even if the outer relation is empty. Each
    // row below uses the same physical layout and conditions, but receives a
    // freshly bound inner relation as Go's Apply does.
    let mut result = join_sources(new_left(Vec::new()), new_right(Vec::new()), join, ctx)?;
    for (left_ids, left_values) in left_rows {
        let mut bindings = Vec::with_capacity(correlated.len());
        for (path, index) in correlated.iter().zip(&correlated_indices) {
            let value = left_values
                .get(*index)
                .cloned()
                .ok_or(DriverError::unsupported("correlated column out of range"))?;
            bindings.push((path.clone(), value));
        }
        let bound = bind_subquery_columns_query(subquery, &bindings)?;
        let (_, rows) = run_query_stmt(&bound, catalog, current_db, ctx)?;
        let right_rows = rows.into_iter().map(|row| (vec![None], row)).collect();
        let joined = join_sources(
            new_left(vec![(left_ids, left_values)]),
            new_right(right_rows),
            join,
            ctx,
        )?;
        result.rows.extend(joined.rows);
    }
    Ok(result)
}

/// Reads one base table's rows together with their handles.
fn scan_base_table(
    table_ref: &tidb_ast::TableRef,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<MultiSource, DriverError> {
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    let entry = catalog
        .get_in(database, name)
        .ok_or(DriverError::unsupported("table not found in catalog"))?;
    let columns = entry.column_list();
    let default_meta = super::dml::column_metadata(entry);
    let rows: Vec<SourceRow> = match entry {
        TableEntry::Mem(mem) => mem
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| (vec![Some(RowId::Mem(index))], row.clone()))
            .collect(),
        TableEntry::Kv(kv) => kv
            .clone()
            .scan_rows_with_handles(&ctx.session_zone())
            .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?
            .into_iter()
            .map(|(handle, row)| (vec![Some(RowId::Kv(handle))], row))
            .collect(),
        TableEntry::Cte(cte) => cte
            .to_rows()
            .map_err(DriverError::from)?
            .into_iter()
            .map(|row| (vec![None], row))
            .collect(),
        // Go expands a view into its stored SELECT before deciding which
        // sources are writable. Its rows therefore participate in a join,
        // but carry no base-table identity and cannot be an UPDATE/DELETE
        // target themselves.
        TableEntry::View(view) => {
            let (columns, rows) =
                super::from::view_source_relation(view, database, name, catalog, ctx)?;
            let default_meta = columns
                .iter()
                .map(|(name, field_type)| super::dml::ColumnDefaultMeta {
                    default_value: None,
                    not_null: false,
                    no_default_value: false,
                    name: name.clone(),
                    field_type: field_type.clone(),
                    column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                    generated: false,
                })
                .collect();
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            return Ok(MultiSource {
                zone: ctx.session_zone(),
                tidb_info_len: ctx.tidb_info_len(),
                coalesced: Vec::new(),
                star: Vec::new(),
                tables: vec![SourceTable {
                    visible,
                    qualifiable_db: table_ref.alias.is_none().then(|| database.to_owned()),
                    origin: SourceOrigin::Derived,
                    columns,
                    default_meta,
                    offset: 0,
                }],
                rows: rows.into_iter().map(|row| (vec![None], row)).collect(),
            });
        }
        // A sequence has no rows to identify either.
        TableEntry::Sequence(_) => {
            return Err(DriverError::unsupported(
                "a sequence is not supported in multi-table DML",
            ))
        }
    };
    let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
    Ok(MultiSource {
        zone: ctx.session_zone(),
        tidb_info_len: ctx.tidb_info_len(),
        coalesced: Vec::new(),
        star: Vec::new(),
        tables: vec![SourceTable {
            visible,
            qualifiable_db: table_ref.alias.is_none().then(|| database.to_owned()),
            origin: SourceOrigin::Base {
                database: database.to_owned(),
                name: name.to_owned(),
            },
            columns,
            default_meta,
            offset: 0,
        }],
        rows,
    })
}

/// Nested-loop joins two sources, keeping both sides' row identities and
/// NULL-padding the non-preserved side of an outer join.
fn join_sources(
    left: MultiSource,
    right: MultiSource,
    join: &tidb_ast::Join,
    ctx: &crate::StmtContext,
) -> Result<MultiSource, DriverError> {
    let left_width = left.width();
    let right_width = right.width();
    let left_tables = left.tables.len();
    let right_tables = right.tables.len();
    // Capture the child naming state before moving their physical table
    // slots into the full DML row below.
    let left_scope = left.scope();
    let right_scope = right.scope();
    let mut tables = left.tables;
    for table in right.tables {
        tables.push(SourceTable {
            offset: table.offset + left_width,
            ..table
        });
    }
    // Build the same full physical row that `UPDATE`/`DELETE` use in Go.
    // The scope carries the separate NATURAL/USING display state: it affects
    // name resolution and supplies the synthesized equality, never the row
    // identities that the write phase needs.
    let left_visible = left_scope.star_columns();
    let right_visible: Vec<(usize, String, FieldType)> = right_scope
        .star_columns()
        .into_iter()
        .map(|(offset, name, field_type)| (offset + left_width, name, field_type))
        .collect();
    let child_coalesced = !left_scope.star.is_empty() || !right_scope.star.is_empty();
    let mut scope = left_scope;
    scope.coalesced.extend(
        right_scope
            .coalesced
            .iter()
            .map(|offset| offset + left_width),
    );
    if !join.natural && join.using.is_empty() && child_coalesced {
        scope.star = left_visible
            .iter()
            .chain(&right_visible)
            .map(|(offset, ..)| *offset)
            .collect();
    }

    let joined = MultiSource {
        tables,
        rows: Vec::new(),
        zone: ctx.session_zone(),
        tidb_info_len: ctx.tidb_info_len(),
        coalesced: Vec::new(),
        star: Vec::new(),
    };
    for table in &joined.tables[left_tables..] {
        // `scope` still has the left tables at their original offsets. The
        // right tables are appended here exactly once under their full-row
        // offsets; their identity stays independent of display coalescing.
        scope.tables.push(FromTable {
            name: table.visible.clone(),
            database: table.qualifiable_db.clone(),
            columns: table.columns.clone(),
            offset: table.offset,
            func_deps: Default::default(),
        });
    }
    let mut coalesced_conditions = Vec::new();
    if join.natural || !join.using.is_empty() {
        let common = super::from::coalesce_common_columns(
            &mut scope,
            left_visible,
            right_visible,
            join.tp,
            &join.using,
        )?;
        let resolver = ScopeResolver { scope: &scope };
        for pair in common {
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
    let field_types = joined.field_types();
    let mut conditions = match &join.on {
        Some(expr) => {
            vec![
                rewrite_expr_resolved(expr, &ScopeResolver { scope: &scope })
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            ]
        }
        None => Vec::new(),
    };
    conditions.append(&mut coalesced_conditions);

    let mut rows = Vec::new();
    let mut right_matched = vec![false; right.rows.len()];
    for (left_ids, left_values) in &left.rows {
        let mut matched = false;
        for (right_index, (right_ids, right_values)) in right.rows.iter().enumerate() {
            let mut values = left_values.clone();
            values.extend_from_slice(right_values);
            let mut joins = true;
            for condition in &conditions {
                let chunk = row_chunk(&values, &field_types)?;
                let selected = condition
                    .eval(ctx, chunk.get_row(0))
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                if !datum_is_true(&selected) {
                    joins = false;
                    break;
                }
            }
            if !joins {
                continue;
            }
            matched = true;
            right_matched[right_index] = true;
            let mut ids = left_ids.clone();
            ids.extend(right_ids.iter().cloned());
            rows.push((ids, values));
        }
        if !matched && join.tp == tidb_ast::JoinType::Left {
            let mut values = left_values.clone();
            values.extend(std::iter::repeat_n(Datum::Null, right_width));
            let mut ids = left_ids.clone();
            ids.extend(std::iter::repeat_n(None, right_tables));
            rows.push((ids, values));
        }
    }
    if join.tp == tidb_ast::JoinType::Right {
        for (right_index, (right_ids, right_values)) in right.rows.iter().enumerate() {
            if right_matched[right_index] {
                continue;
            }
            let mut values = vec![Datum::Null; left_width];
            values.extend_from_slice(right_values);
            let mut ids = vec![None; left_tables];
            ids.extend(right_ids.iter().cloned());
            rows.push((ids, values));
        }
    }
    Ok(MultiSource {
        rows,
        coalesced: scope.coalesced,
        star: scope.star,
        ..joined
    })
}

/// The joined rows a multi-table write acts on: the `FROM` joined, the
/// `WHERE` applied, then `ORDER BY`/`LIMIT` (which only an explicitly
/// `JOIN`ed `UPDATE` may carry -- the parser rejects the other spellings).
fn selected_rows(
    source: &mut MultiSource,
    where_clause: &Option<tidb_ast::Expr>,
    order_by: &[tidb_ast::OrderItem],
    limit: &Option<tidb_ast::Limit>,
    ctx: &crate::StmtContext,
) -> Result<Vec<SourceRow>, DriverError> {
    let scope = source.scope();
    let resolver = ScopeResolver { scope: &scope };
    let field_types = source.field_types();
    let column_names = source.column_names();
    let predicate = match where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let mut rows = Vec::new();
    for row in std::mem::take(&mut source.rows) {
        if row_is_selected(&row.1, &field_types, &predicate, ctx)? {
            rows.push(row);
        }
    }
    order_rows_for_dml(
        &mut rows,
        order_by,
        &field_types,
        &resolver,
        &column_names,
        ctx,
    )?;
    // Go's `LIMIT` is a plan operator above the join, so it caps the joined
    // rows the write REACHES -- never the subset whose value ended up
    // different.
    if let Some(cap) = dml_row_limit(limit)? {
        rows.truncate(usize::try_from(cap).unwrap_or(usize::MAX));
    }
    Ok(rows)
}

/// Go's `updatedRowKeys`: per (target position, row identity), whether the
/// write that reached it CHANGED the row. A repeat visit is skipped only
/// when the first one changed something -- a no-op first visit leaves the
/// row eligible, which is Go's `changed && skipMultipleChangesOnSameRow`.
type UpdateOnce = BTreeMap<(usize, RowId), bool>;

/// Runs a multi-table `UPDATE`, returning MySQL's affected-row count.
/// Accounts the JOINED rows a multi-table write holds, against
/// `tidb_mem_quota_query`.
///
/// Go `DeleteExec.composeTblRowMap` prices exactly this row -- the joined one,
/// `types.EstimatedMemUsage(joinedRow, 1)` -- because a multi-table write's
/// working set is the join output, not any one table's rows.
/// `deleteMultiTablesByChunk`/`updateRows` consume it as the chunks arrive;
/// here the join is already materialized, so it is one pass over what is
/// held, and it runs before any table is touched.
fn account_joined_rows(
    rows: &[SourceRow],
    label: i64,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let accountant = ctx.statement_memory().write_accountant(label);
    for (_, values) in rows {
        accountant.account_row(values).map_err(DriverError::from)?;
    }
    Ok(())
}

pub(crate) fn run_multi_update(
    update: &tidb_ast::UpdateStmt,
    from: &tidb_ast::Join,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let mut source = build_multi_source(from, catalog, current_db, ctx)?;
    let scope = source.scope();
    let assignments = resolve_assignments(&update.assignments, &source, &scope, ctx)?;
    let field_types = source.field_types();
    let rows = selected_rows(
        &mut source,
        &update.where_clause,
        &update.order_by,
        &update.limit,
        ctx,
    )?;

    account_joined_rows(&rows, crate::mem_quota::label::UPDATE, ctx)?;

    let mut once: UpdateOnce = BTreeMap::new();
    let mut changed_rows = 0u64;
    for (ids, values) in &rows {
        let chunk = row_chunk(values, &field_types)?;
        for (slot, table) in source.tables.iter().enumerate() {
            // Nothing assigned to this table, or an outer join NULL-padded
            // it: Go's `tableUpdatable` is false either way.
            let Some(id) = &ids[slot] else { continue };
            if !assignments.iter().any(|a| a.slot == slot) {
                continue;
            }
            if once.get(&(slot, id.clone())) == Some(&true) {
                continue;
            }
            let old = &values[table.offset..table.end()];
            let mut new_row = old.to_vec();
            for assignment in assignments.iter().filter(|a| a.slot == slot) {
                let value = assignment
                    .value
                    .eval(ctx, chunk.get_row(0))
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                new_row[assignment.column] = cast_value_for_update_assignment(
                    value,
                    &table.columns[assignment.column].1,
                    &table.columns[assignment.column].0,
                    0,
                    ctx,
                )?;
            }
            // Go `updateRecord` step 5, the same rule the single-table path
            // follows: every column of the new row is NULL-checked before the
            // changed comparison.
            let level = crate::bad_null::NullLevel::from_is_error(ctx.strict());
            for (value, (name, field_type)) in new_row.iter_mut().zip(table.columns.iter()) {
                crate::bad_null::handle_bad_null(value, field_type, name, level, ctx)?;
            }
            let changed = new_row != old;
            if changed {
                write_row(catalog, table, id, &new_row, ctx)?;
                changed_rows += 1;
            }
            once.insert((slot, id.clone()), changed);
        }
    }
    Ok(changed_rows)
}

/// One resolved `SET` assignment: which target table it writes, that table's
/// own column offset, and the value expression over the WHOLE joined row.
struct MultiAssignment {
    slot: usize,
    column: usize,
    value: Expression,
}

fn resolve_assignments(
    assignments: &[tidb_ast::Assignment],
    source: &MultiSource,
    scope: &FromScope,
    ctx: &crate::StmtContext,
) -> Result<Vec<MultiAssignment>, DriverError> {
    let resolver = ScopeResolver { scope };
    let default_row = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let mut resolved = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        // Go reports a `SET` column it cannot bind -- including one
        // qualified by a table the join never mentions -- as
        // `ERROR 1054 Unknown column '<col>' in 'field list'`.
        let (offset, _, _) = resolver.resolve(&assignment.col).ok_or_else(|| {
            DriverError::UnknownColumnInClause {
                column: assignment.col.last().cloned().unwrap_or_default(),
                clause: "field list".to_owned(),
            }
        })?;
        let slot = source
            .table_of_column(offset)
            .ok_or(DriverError::unsupported("SET column outside the join"))?;
        // Go `buildUpdateLists`, the `!foundListItem` branch: the column
        // resolved, but against a source the updatable list does not hold --
        // "subQuery is not counted as updatable table".
        if matches!(source.tables[slot].origin, SourceOrigin::Derived) {
            return Err(source.tables[slot].not_updatable("UPDATE"));
        }
        let column = offset - source.tables[slot].offset;
        let target_meta = &source.tables[slot].default_meta[column];
        if target_meta.generated {
            let own_default = match &assignment.value {
                tidb_ast::Expr::Default(None) => true,
                tidb_ast::Expr::Default(Some(path)) => resolver
                    .resolve(path)
                    .is_some_and(|(default_offset, _, _)| default_offset == offset),
                _ => false,
            };
            if own_default {
                continue;
            }
            let table_name = match &source.tables[slot].origin {
                SourceOrigin::Base { name, .. } => name.clone(),
                SourceOrigin::Derived => source.tables[slot].visible.clone(),
            };
            return Err(DriverError::BadGeneratedColumn {
                column: target_meta.name.clone(),
                table: table_name,
            });
        }

        let value = match &assignment.value {
            tidb_ast::Expr::Default(None) => {
                let datum = super::dml::materialize_column_default(
                    target_meta,
                    super::dml::DefaultUse::Expression,
                    ctx,
                    default_row.get_row(0),
                )?;
                Expression::Constant(tidb_expr::constant::Constant::new(
                    datum,
                    target_meta.field_type.clone(),
                ))
            }
            value => {
                let defaults = super::dml::prepare_named_defaults(
                    value,
                    ctx,
                    default_row.get_row(0),
                    super::dml::DefaultUse::Expression,
                    |path| {
                        let (default_offset, _, _) = resolver.resolve(path).ok_or_else(|| {
                            DriverError::UnknownColumnInClause {
                                column: path.last().cloned().unwrap_or_default(),
                                clause: "field list".to_owned(),
                            }
                        })?;
                        let default_slot = source
                            .table_of_column(default_offset)
                            .ok_or(DriverError::unsupported("DEFAULT column outside the join"))?;
                        let default_column = default_offset - source.tables[default_slot].offset;
                        if matches!(source.tables[default_slot].origin, SourceOrigin::Derived) {
                            return Err(DriverError::NoDefaultForField(
                                source.tables[default_slot].columns[default_column]
                                    .0
                                    .clone(),
                            ));
                        }
                        Ok(super::dml::ResolvedDefaultColumn {
                            identity: super::dml::DefaultColumnIdentity {
                                table: default_slot,
                                column: default_column,
                            },
                            meta: source.tables[default_slot].default_meta[default_column].clone(),
                        })
                    },
                )?;
                super::dml::rewrite_with_prepared_defaults(value, &resolver, &defaults)?
            }
        };
        resolved.push(MultiAssignment {
            slot,
            column,
            value,
        });
    }
    Ok(resolved)
}

fn write_row(
    catalog: &mut Catalog,
    table: &SourceTable,
    id: &RowId,
    row: &[Datum],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // Only a `SourceOrigin::Base` reaches here: `resolve_assignments` refused
    // every other kind before a single row was read.
    let SourceOrigin::Base { database, name } = &table.origin else {
        return Err(table.not_updatable("UPDATE"));
    };
    let entry = catalog
        .get_mut_in(database, name)
        .ok_or(DriverError::unsupported("unknown table"))?;
    match (entry, id) {
        (TableEntry::Mem(mem), RowId::Mem(index)) => {
            mem.rows[*index] = row.to_vec();
            Ok(())
        }
        (TableEntry::Kv(kv), RowId::Kv(handle)) => {
            kv.update_row(handle, row, ctx).map_err(kv_write_error)
        }
        // The identity was read off this very entry a moment ago.
        _ => Err(DriverError::unsupported(
            "table storage changed during a multi-table write",
        )),
    }
}

/// Runs a multi-table `DELETE`, returning the number of removed rows.
pub(crate) fn run_multi_delete(
    delete: &tidb_ast::DeleteStmt,
    targets: &[Vec<String>],
    from: &tidb_ast::Join,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let mut source = build_multi_source(from, catalog, current_db, ctx)?;
    let target_slots = resolve_delete_targets(targets, &source)?;
    let rows = selected_rows(&mut source, &delete.where_clause, &[], &None, ctx)?;
    account_joined_rows(&rows, crate::mem_quota::label::DELETE, ctx)?;

    // Go's `tblRowMap` is keyed by TABLE ID, so a row reachable through
    // several join paths -- or named twice in the target list -- is removed
    // once, and two aliases of one table are still one table here (unlike
    // UPDATE, whose key is the target position).
    let mut doomed: BTreeSet<(String, String, RowId)> = BTreeSet::new();
    for (ids, _) in &rows {
        for &slot in &target_slots {
            let Some(id) = &ids[slot] else { continue };
            let table = &source.tables[slot];
            // `resolve_delete_targets` admitted only base sources.
            let SourceOrigin::Base { database, name } = &table.origin else {
                return Err(table.not_updatable("DELETE"));
            };
            doomed.insert((database.clone(), name.clone(), id.clone()));
        }
    }

    let deleted = doomed.len() as u64;
    // A matrix-backed table identifies rows by position, so its removals are
    // applied from the back; a stored table's handle is position-independent.
    for (database, name, id) in doomed.into_iter().rev() {
        let entry = catalog
            .get_mut_in(&database, &name)
            .ok_or(DriverError::unsupported("unknown table"))?;
        match (entry, &id) {
            (TableEntry::Mem(mem), RowId::Mem(index)) => {
                mem.rows.remove(*index);
            }
            (TableEntry::Kv(kv), RowId::Kv(handle)) => {
                kv.delete_row(handle, &ctx.session_zone())
                    .map_err(|e| super::dml::kv_read_error("row delete failed", e))?
            }
            _ => {
                return Err(DriverError::unsupported(
                    "table storage changed during a multi-table write",
                ))
            }
        }
    }
    Ok(deleted)
}

/// Binds each written target name to the `FROM` sources it names.
///
/// A source is named by its ALIAS once it has one (`DELETE f1 FROM f1 AS x`
/// is Go's `ErrUnknownTable`), and a schema-qualified target resolves only
/// against an unaliased source -- the same rule [`ScopeResolver`] applies to
/// a column's qualifier, so the two cannot drift.
fn resolve_delete_targets(
    targets: &[Vec<String>],
    source: &MultiSource,
) -> Result<Vec<usize>, DriverError> {
    let mut slots = Vec::new();
    for target in targets {
        let (schema, name) = match target.as_slice() {
            [name] => (None, name),
            [schema, name] => (Some(schema), name),
            _ => return Err(DriverError::UnknownTableInMultiDelete(target.join("."))),
        };
        let mut found = false;
        for (slot, table) in source.tables.iter().enumerate() {
            if !table.visible.eq_ignore_ascii_case(name) {
                continue;
            }
            if let Some(schema) = schema {
                match &table.qualifiable_db {
                    Some(db) if db.eq_ignore_ascii_case(schema) => {}
                    _ => continue,
                }
            }
            // Go's `collectTableName` records the source under this very
            // name whether or not it is updatable, and the caller splits the
            // two outcomes: a name the `FROM` never provides is 1109
            // ("check sql like: `delete b from (select * from t) as a, t`"),
            // while a name it provides NON-updatably is 1288 ("check sql
            // like: `delete a from (select * from t) as a, t`").
            if matches!(table.origin, SourceOrigin::Derived) {
                return Err(table.not_updatable("DELETE"));
            }
            found = true;
            if !slots.contains(&slot) {
                slots.push(slot);
            }
        }
        if !found {
            return Err(DriverError::UnknownTableInMultiDelete(name.clone()));
        }
    }
    Ok(slots)
}
