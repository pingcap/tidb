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
//! join's schema per target table. The shared physical child carries those
//! identities through every read operator. At the write boundary [`SourceRow`]
//! separates them from values. Read-only virtual inputs contribute no handle.
//!
//! The rules below were captured from a real TiDB session (`mockstore`
//! `session.Execute`, reading affected rows off `StmtCtx`), not inferred:
//!
//! * **UPDATE-ONCE.** A base row reachable through several join paths is
//!   written once: `UPDATE a JOIN b ON a.id = b.aid SET a.x = a.x + 1` with
//!   two `b` rows per `a` row leaves `a.x` at `x+1`, not `x+2`. Go keys
//!   `updatedRowKeys` by (target position, handle) and skips a repeat --
//!   but only when the FIRST visit actually CHANGED the row, which is why
//!   [`UpdateOnce`] remembers a bool rather than mere presence. Current Go
//!   disables this skip for partitioned heaps, where physical rows can share
//!   the same bare handle; `UpdateExec.prepare` owns that exception.
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
//!   row once. Their assigned columns merge by base-table/row identity;
//!   a later assignment to the same column wins.
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
//! `LATERAL` derived sources use the shared physical Apply executor. The derived side remains read-only because it has no base
//! row identity.

use std::collections::BTreeMap;

use super::dml::update_record::UpdateRecords;
use super::*;
use crate::kv_table::TableHandle;

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
    /// The SQL scope ends in a heap handle, outside visible/writable metadata.
    extra_handle: bool,
    /// Go UpdateExec.prepare keeps partitioned heap rows eligible even when
    /// another physical row has the same bare handle.
    skip_repeated_updates: bool,
    /// Default/generation metadata for visible stored columns.
    default_meta: Vec<super::dml::ColumnDefaultMeta>,
    /// Generated offsets in the complete writable row, including hidden columns.
    generated_columns: Vec<usize>,
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
struct SourceRow {
    ids: Vec<Option<TableHandle>>,
    /// Visible SQL scope used to evaluate assignments.
    values: Vec<Datum>,
    /// Complete writable preimages retained from the physical child.
    stored: Vec<Vec<Datum>>,
}

/// Metadata for the joined values a multi-table write reads.
struct MultiLayout {
    tables: Vec<SourceTable>,
    constant_context: crate::StmtContext,
    /// The output naming state of a child `NATURAL`/`USING` join. The row
    /// remains full-width for writes, just as Go resets the join schema for
    /// DML after using the coalesced names to construct its equality.
    coalesced: Vec<usize>,
    star: Vec<usize>,
}

impl MultiLayout {
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
                })
                .collect(),
            coalesced: self.coalesced.clone(),
            star: self.star.clone(),
            ..FromScope::for_statement(&self.constant_context)
        }
    }

    fn field_types(&self) -> Vec<FieldType> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().map(|(_, ft)| ft.clone()))
            .collect()
    }

    /// The table whose columns cover `offset` in the joined row.
    fn table_of_column(&self, offset: usize) -> Option<usize> {
        self.tables
            .iter()
            .position(|t| offset >= t.offset && offset < t.end())
    }
}

/// Resolve source shape through the logical planner without opening any row
/// source. Derived queries and lateral Apply inputs participate in name/type
/// resolution, but execute only when the retained physical child is opened.
fn build_multi_layout(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<MultiLayout, DriverError> {
    let logical = super::planner_bridge::logical_from_plan(join, catalog, current_db, ctx, true)
        .map_err(super::planner_error_to_driver)?;
    let schema = logical
        .schema()
        .ok_or_else(|| DriverError::unsupported("DML source has no schema"))?;
    let names = logical.output_names();
    if schema.columns.len() != names.len() {
        return Err(DriverError::unsupported(
            "DML source names and schema differ",
        ));
    }

    fn node_layout(
        node: &tidb_ast::JoinNode,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
        columns_for_alias: &impl Fn(&str) -> Result<Vec<(String, FieldType)>, DriverError>,
    ) -> Result<MultiLayout, DriverError> {
        let (visible, qualifiable_db, origin, mut columns, default_meta) = match node {
            tidb_ast::JoinNode::Join(join) => {
                return join_layout(join, catalog, current_db, ctx, columns_for_alias)
            }
            tidb_ast::JoinNode::Table(table) => {
                let (database, name) = split_table_path(&table.name, current_db)?;
                let entry = catalog.get_in(database, name).ok_or_else(|| {
                    DriverError::Schema(crate::SchemaErrorKind::UnknownTable(format!(
                        "{database}.{name}"
                    )))
                })?;
                let origin = match entry {
                    TableEntry::Mem(_) | TableEntry::Kv(_) => SourceOrigin::Base {
                        database: database.to_owned(),
                        name: name.to_owned(),
                    },
                    TableEntry::View(_) => SourceOrigin::Derived,
                    TableEntry::Sequence(_) => {
                        return Err(DriverError::unsupported(
                            "a sequence is not supported in multi-table DML",
                        ))
                    }
                };
                (
                    table.alias.clone().unwrap_or_else(|| name.to_owned()),
                    table.alias.is_none().then(|| database.to_owned()),
                    origin,
                    entry.column_list(),
                    super::dml::column_metadata(entry),
                )
            }
            tidb_ast::JoinNode::Derived { alias, .. } => {
                let alias = alias
                    .as_deref()
                    .filter(|alias| !alias.is_empty())
                    .ok_or(DriverError::DerivedMustHaveAlias)?;
                (
                    alias.to_owned(),
                    None,
                    SourceOrigin::Derived,
                    columns_for_alias(alias)?,
                    Vec::new(),
                )
            }
        };
        let extra = match &origin {
            SourceOrigin::Base { database, name } => catalog
                .get_in(database, name)
                .and_then(super::from::extra_handle_column),
            SourceOrigin::Derived => None,
        };
        let extra_handle = extra.is_some();
        let skip_repeated_updates = match &origin {
            SourceOrigin::Base { database, name } => match catalog.get_in(database, name) {
                Some(TableEntry::Kv(kv)) => !extra_handle || kv.partition().is_none(),
                _ => true,
            },
            SourceOrigin::Derived => true,
        };
        let star = (0..columns.len()).collect();
        if let Some(column) = extra {
            columns.push(column);
        }
        let generated_columns = match &origin {
            SourceOrigin::Base { database, name } => match catalog.get_in(database, name) {
                Some(TableEntry::Kv(kv)) => kv
                    .columns()
                    .iter()
                    .enumerate()
                    .filter_map(|(offset, column)| column.generated.is_some().then_some(offset))
                    .collect(),
                _ => Vec::new(),
            },
            SourceOrigin::Derived => Vec::new(),
        };
        Ok(MultiLayout {
            tables: vec![SourceTable {
                visible,
                qualifiable_db,
                origin,
                columns,
                extra_handle,
                skip_repeated_updates,
                default_meta,
                generated_columns,
                offset: 0,
            }],
            constant_context: ctx.clone(),
            coalesced: Vec::new(),
            star,
        })
    }

    fn join_layout(
        join: &tidb_ast::Join,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
        columns_for_alias: &impl Fn(&str) -> Result<Vec<(String, FieldType)>, DriverError>,
    ) -> Result<MultiLayout, DriverError> {
        let left = node_layout(&join.left, catalog, current_db, ctx, columns_for_alias)?;
        match &join.right {
            Some(right) => merge_source_layout(
                left,
                node_layout(right, catalog, current_db, ctx, columns_for_alias)?,
                join,
                ctx,
            ),
            None => Ok(left),
        }
    }

    let columns_for_alias = |alias: &str| {
        schema
            .columns
            .iter()
            .zip(names)
            .filter(|(_, name)| name.names.table.original.eq_ignore_ascii_case(alias))
            .map(|(column, name)| {
                Ok((
                    name.names.column.original.clone(),
                    column.ret_type.clone().ok_or_else(|| {
                        DriverError::unsupported("derived DML column has no type")
                    })?,
                ))
            })
            .collect::<Result<Vec<_>, DriverError>>()
    };
    join_layout(join, catalog, current_db, ctx, &columns_for_alias)
}

/// DELETE permissions use the same resolved outer targets as execution and
/// FK planning. Names that occur only inside derived queries cannot name a
/// writable outer target.
pub fn delete_privilege_tables(
    delete: &tidb_ast::DeleteStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, String)>, DriverError> {
    let tidb_ast::DeleteKind::Multi { targets, from, .. } = &delete.kind else {
        let tidb_ast::DeleteKind::Single(table) = &delete.kind else {
            unreachable!()
        };
        let (database, name) = split_table_path(&table.name, current_db)?;
        return Ok(vec![(database.to_owned(), name.to_owned())]);
    };
    let source = build_multi_layout(from, catalog, current_db, ctx)?;
    let mut resolved = Vec::new();
    for slot in resolve_delete_targets(targets, &source)? {
        let SourceOrigin::Base { database, name } = &source.tables[slot].origin else {
            unreachable!()
        };
        let table = (database.clone(), name.clone());
        if !resolved.contains(&table) {
            resolved.push(table);
        }
    }
    Ok(resolved)
}

/// Merge naming metadata while retaining the full DML value layout.
/// Row execution, including outer padding and conditions, belongs to the
/// shared physical child.
fn merge_source_layout(
    left: MultiLayout,
    right: MultiLayout,
    join: &tidb_ast::Join,
    ctx: &crate::StmtContext,
) -> Result<MultiLayout, DriverError> {
    let left_width = left.width();
    let left_tables = left.tables.len();
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
    // name resolution, never the row
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

    let joined = MultiLayout {
        tables,
        constant_context: ctx.clone(),
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
        });
    }
    if join.natural || !join.using.is_empty() {
        super::from::coalesce_common_columns(
            &mut scope,
            left_visible,
            right_visible,
            join.tp,
            &join.using,
        )?;
    }
    Ok(MultiLayout {
        coalesced: scope.coalesced,
        star: scope.star,
        ..joined
    })
}

/// Go's `updatedRowKeys`: per (target position, row identity), whether the
/// write that reached it CHANGED the row. A repeat visit is skipped only
/// when the first one changed something -- a no-op first visit leaves the
/// row eligible. Go also disables skipping for partitioned heap tables.
type UpdateOnce = BTreeMap<(usize, TableHandle), bool>;

/// Runs a multi-table `UPDATE`, returning MySQL's affected-row count.
pub(crate) fn run_multi_update(
    update: &tidb_ast::UpdateStmt,
    from: &tidb_ast::Join,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    fk_triggers: &[tidb_planner::physical::FkTriggerNode],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    let source = build_multi_layout(from, catalog, current_db, ctx)?;
    let scope = source.scope();
    let assignments = resolve_assignments(&update.assignments, &source, &scope, ctx)?;
    let on_update_now: Vec<super::dml::PreparedOnUpdateNow> = source
        .tables
        .iter()
        .enumerate()
        .map(|(slot, table)| {
            super::dml::PreparedOnUpdateNow::new(
                &table.default_meta,
                assignments
                    .iter()
                    .filter(move |assignment| assignment.slot == slot)
                    .map(|assignment| assignment.column),
            )
        })
        .collect::<Result<_, _>>()?;
    let field_types = source.field_types();
    let rows = {
        let plan = physical_plan
            .ok_or_else(|| DriverError::unsupported("a multi-table write has no read plan"))?;
        planned_source_rows(
            &source,
            plan,
            catalog,
            ctx,
            runtime,
            crate::mem_quota::label::UPDATE,
        )?
    };

    // Go keeps updatedRowKeys per target position, but mergedRowData per
    // base table and handle. Neither map can stand in for the other.
    let targets: Vec<_> = source
        .tables
        .iter()
        .enumerate()
        .map(|(slot, table)| {
            if !assignments.iter().any(|assignment| {
                assignment.slot == slot && assignment.column < table.default_meta.len()
            }) {
                return None;
            }
            match &table.origin {
                SourceOrigin::Base { database, name } => {
                    Some((database.to_lowercase(), name.to_lowercase()))
                }
                SourceOrigin::Derived => None,
            }
        })
        .collect();
    let multiple: Vec<_> = targets
        .iter()
        .map(|target| {
            target.as_ref().is_some_and(|target| {
                targets
                    .iter()
                    .flatten()
                    .filter(|other| *other == target)
                    .count()
                    > 1
            })
        })
        .collect();
    let mut merged: BTreeMap<((String, String), TableHandle), Vec<Datum>> = BTreeMap::new();
    let accountant = ctx
        .statement_memory()
        .write_accountant(crate::mem_quota::label::UPDATE);
    let mut records = UpdateRecords::new(fk_triggers);
    let mut once: UpdateOnce = BTreeMap::new();
    let mut matched_rows = 0u64;
    let mut touched_rows = 0u64;
    let mut changed_rows = 0u64;
    for (row_index, row) in rows.iter().enumerate() {
        let (ids, values) = (&row.ids, &row.values);
        let chunk = row_chunk(values, &field_types)?;
        let evaluated = assignments
            .iter()
            .map(|assignment| {
                let table = &source.tables[assignment.slot];
                let extra = table.extra_handle && assignment.column == table.default_meta.len();
                if !extra && ids[assignment.slot].is_none() {
                    return Ok(None);
                }
                let value = assignment
                    .value
                    .eval(ctx, chunk.get_row(0))
                    .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
                if extra {
                    return Ok(None);
                }
                cast_value_for_update_assignment(
                    value,
                    &table.columns[assignment.column].1,
                    &table.columns[assignment.column].0,
                    row_index,
                    ctx,
                )
                .map(Some)
            })
            .collect::<Result<Vec<_>, DriverError>>()?;
        let mut prepared = Vec::with_capacity(source.tables.len());
        // Compose every target from the same input row before merging aliases.
        for (slot, table) in source.tables.iter().enumerate() {
            let Some(id) = ids[slot].as_ref().filter(|_| targets[slot].is_some()) else {
                prepared.push(None);
                continue;
            };
            if table.skip_repeated_updates && once.get(&(slot, id.clone())) == Some(&true) {
                prepared.push(None);
                continue;
            }
            let mut old = row.stored[slot].clone();
            let mut new = old.clone();
            for (assignment, value) in assignments.iter().zip(&evaluated) {
                if assignment.slot == slot {
                    if let Some(value) = value {
                        new[assignment.column] = value.clone();
                    }
                }
            }
            if multiple[slot] {
                let key = (
                    targets[slot].clone().expect("an assigned base table"),
                    id.clone(),
                );
                if let Some(previous) = merged.get_mut(&key) {
                    for (column, meta) in table.default_meta.iter().enumerate() {
                        if meta.generated {
                            continue;
                        }
                        old[column] = previous[column].clone();
                        if assignments
                            .iter()
                            .any(|a| a.slot == slot && a.column == column)
                        {
                            previous[column] = new[column].clone();
                        } else {
                            new[column] = previous[column].clone();
                        }
                    }
                } else {
                    merged.insert(key, new.clone());
                }
                accountant.account_row(&new).map_err(DriverError::from)?;
            }
            prepared.push(Some((old, new)));
        }
        for (slot, prepared) in prepared.iter_mut().enumerate() {
            let Some((old, new)) = prepared else { continue };
            let table = &source.tables[slot];
            let id = ids[slot].as_ref().expect("prepared target has an identity");
            let (database, name) = targets[slot]
                .as_ref()
                .expect("prepared target is a base table");
            let merge_key = ((database.clone(), name.clone()), id.clone());
            if multiple[slot] {
                let previous = &merged[&merge_key];
                for &column in &table.generated_columns {
                    old[column] = previous[column].clone();
                    new[column] = previous[column].clone();
                }
            }
            if !once.contains_key(&(slot, id.clone())) {
                matched_rows += 1;
            }
            on_update_now[slot].apply(old, new, ctx, chunk.get_row(0))?;
            let outcome = records.write(
                catalog,
                database,
                name,
                id,
                old,
                new,
                None,
                update.ignore,
                super::GeneratedWrite::Update { row_index },
                ctx,
            )?;
            if multiple[slot] {
                let previous = merged
                    .get_mut(&merge_key)
                    .expect("non-generated merge ran first");
                for &column in &table.generated_columns {
                    previous[column] = new[column].clone();
                }
            }
            changed_rows += u64::from(outcome.changed());
            touched_rows += u64::from(outcome.touched());
            if !outcome.ignored() {
                once.insert((slot, id.clone()), outcome.changed());
            }
        }
    }
    records.finish(catalog, ctx)?;
    ctx.set_message(format!(
        "Rows matched: {matched_rows}  Changed: {changed_rows}  Warnings: {}",
        ctx.warning_count()
    ));
    Ok(if ctx.client_found_rows() {
        touched_rows
    } else {
        changed_rows
    })
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
    source: &MultiLayout,
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
        let target_meta = source.tables[slot].default_meta.get(column);
        if target_meta.is_none() && !ctx.allow_write_row_id() {
            return Err(DriverError::unsupported(super::dml::WRITE_ROW_ID_REFUSED));
        }
        if let Some(target_meta) = target_meta.filter(|meta| meta.generated) {
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
                    target_meta.ok_or_else(|| DriverError::UnknownColumnInClause {
                        column: source.tables[slot].columns[column].0.clone(),
                        clause: "field_list".to_owned(),
                    })?,
                    super::dml::DefaultUse::Expression,
                    ctx,
                    default_row.get_row(0),
                )?;
                Expression::Constant(tidb_expr::constant::Constant::new(
                    datum,
                    source.tables[slot].columns[column].1.clone(),
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
                            meta: source.tables[default_slot]
                                .default_meta
                                .get(default_column)
                                .cloned()
                                .ok_or_else(|| DriverError::UnknownColumnInClause {
                                    column: source.tables[default_slot].columns[default_column]
                                        .0
                                        .clone(),
                                    clause: "field_list".to_owned(),
                                })?,
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

/// Runs a multi-table `DELETE`, returning the number of removed rows.
pub(crate) fn run_multi_delete(
    delete: &tidb_ast::DeleteStmt,
    targets: &[Vec<String>],
    from: &tidb_ast::Join,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    fk_triggers: &[tidb_planner::physical::FkTriggerNode],
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    let source = build_multi_layout(from, catalog, current_db, ctx)?;
    let target_slots = resolve_delete_targets(targets, &source)?;
    let rows = {
        let plan = physical_plan
            .ok_or_else(|| DriverError::unsupported("a multi-table write has no read plan"))?;
        planned_source_rows(
            &source,
            plan,
            catalog,
            ctx,
            runtime,
            crate::mem_quota::label::DELETE,
        )?
    };

    // Go's `tblRowMap` is keyed by TABLE ID, so a row reachable through
    // several join paths -- or named twice in the target list -- is removed
    // once, and two aliases of one table are still one table here (unlike
    // UPDATE, whose key is the target position).
    let mut doomed: BTreeMap<(String, String, TableHandle), (usize, usize)> = BTreeMap::new();
    for (row_index, row) in rows.iter().enumerate() {
        let ids = &row.ids;
        for &slot in &target_slots {
            let Some(id) = &ids[slot] else { continue };
            let table = &source.tables[slot];
            // `resolve_delete_targets` admitted only base sources.
            let SourceOrigin::Base { database, name } = &table.origin else {
                return Err(table.not_updatable("DELETE"));
            };
            doomed
                .entry((database.clone(), name.clone(), id.clone()))
                .or_insert((row_index, slot));
        }
    }

    let mut records = super::dml::delete_record::DeleteRecords::new(fk_triggers);
    let mut deleted = 0;
    for ((database, name, id), location) in doomed {
        let old = &rows[location.0].stored[location.1];
        deleted +=
            u64::from(records.write(catalog, &database, &name, &id, old, delete.ignore, ctx)?);
    }
    records.finish(catalog, ctx)?;

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
    source: &MultiLayout,
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

/// Go `buildUpdate`/`buildDelete`'s read: the `FROM` join with the `WHERE`,
/// `ORDER BY` and `LIMIT` above it, as one SELECT for the planner, carrying
/// the statement's hints (`pushTableHints(stmt.TableHints, 0)`).
pub(crate) fn multi_dml_select(
    hints: &[tidb_ast::Hint],
    from: &tidb_ast::Join,
    where_clause: Option<&tidb_ast::Expr>,
    order_by: &[tidb_ast::OrderItem],
    limit: Option<&tidb_ast::Limit>,
) -> tidb_ast::QueryStmt {
    let mut fields = tidb_ast::SelectFieldList::default();
    fields.push(tidb_ast::SelectField::Wildcard(Vec::new()));
    tidb_ast::QueryStmt::Select(Box::new(tidb_ast::SelectStmt {
        kind: Default::default(),
        is_in_braces: false,
        with: None,
        hints: hints.to_vec(),
        priority: Default::default(),
        sql_small_result: false,
        sql_big_result: false,
        sql_buffer_result: false,
        sql_no_cache: false,
        straight_join: false,
        calc_found_rows: false,
        distinct: false,
        all: false,
        fields,
        values: Vec::new(),
        from: Some(from.clone()),
        where_clause: where_clause.cloned(),
        group_by: Vec::new(),
        rollup: false,
        having: None,
        windows: Vec::new(),
        order_by: order_by.to_vec(),
        limit: limit.cloned(),
        lock: None,
        into_outfile: None,
        into_vars: Vec::new(),
    }))
}

/// Plan all writable targets before executing any source. Aliases of one
/// physical table contribute to one FK policy, as Go's tblID2UpdateColumns
/// and per-table FK maps do.
pub(crate) fn multi_dml_physical_plan(
    dml: MultiDmlRef<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner::physical::PhysicalPlan, DriverError> {
    use super::fk_trigger_plan::FkPlanSpec;
    let mut specs: BTreeMap<(i64, String, String), FkPlanSpec> = BTreeMap::new();
    let mut add_target = |database: String, table: String, column: Option<String>| {
        let identity = match catalog.get_in(&database, &table) {
            Some(TableEntry::Kv(table)) => (table.table_id, String::new(), String::new()),
            _ => (0, database.to_ascii_lowercase(), table.to_ascii_lowercase()),
        };
        let spec = specs.entry(identity).or_insert_with(|| FkPlanSpec {
            database,
            table,
            ..FkPlanSpec::default()
        });
        if let Some(column) = column {
            if !spec
                .updated_cols
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(&column))
            {
                spec.updated_cols.push(column);
            }
        }
    };
    let (operator, select) = match dml {
        MultiDmlRef::Update(update) => {
            let tidb_ast::UpdateKind::Multi { from, .. } = &update.kind else {
                return Err(DriverError::unsupported(
                    "multi-table UPDATE plan requires a join",
                ));
            };
            for (database, table, column) in
                super::planner_bridge::update_target_columns(update, catalog, current_db, ctx)?
            {
                add_target(database, table, Some(column));
            }
            (
                "Update",
                multi_dml_select(
                    &update.hints,
                    from,
                    update.where_clause.as_ref(),
                    &update.order_by,
                    update.limit.as_ref(),
                ),
            )
        }
        MultiDmlRef::Delete(delete) => {
            let tidb_ast::DeleteKind::Multi { from, .. } = &delete.kind else {
                return Err(DriverError::unsupported(
                    "multi-table DELETE plan requires a join",
                ));
            };
            for (database, table) in delete_privilege_tables(delete, catalog, current_db, ctx)? {
                add_target(database, table, None);
            }
            (
                "Delete",
                multi_dml_select(&delete.hints, from, delete.where_clause.as_ref(), &[], None),
            )
        }
    };
    let specs: Vec<_> = specs.into_values().collect();
    super::dml::physical_multi_dml_plan(operator, &select, catalog, current_db, ctx, &specs)
}

/// Runs the planned read (the `SelectPlan` under the DML root) of a
/// multi-table write and splits each output row
/// into the per-table row identities the write needs.
///
/// Go reads these identities from `tblID2Handle`'s handle columns in the
/// optimized `SelectPlan`'s schema (`logical_plan_builder.go:6200`): each
/// base table contributes its stored columns followed by `_tidb_rowid` when
/// its handle is not the primary key, the DML build keeps the merged
/// (uncoalesced) join schema, and a NULL handle is an outer join's padded
/// side (`unmatchedOuterRow`).
fn planned_source_rows(
    source: &MultiLayout,
    plan: &mut tidb_planner::physical::PhysicalPlan,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
    memory_label: i64,
) -> Result<Vec<SourceRow>, DriverError> {
    struct Slot<'a> {
        width: usize,
        kv: Option<&'a crate::kv_table::KvTable>,
        stored: usize,
        visible: usize,
        extra: bool,
    }
    let mut slots = Vec::with_capacity(source.tables.len());
    for table in &source.tables {
        let entry = match &table.origin {
            SourceOrigin::Base { database, name } => catalog.get_in(database, name),
            SourceOrigin::Derived => None,
        };
        let kv = match entry {
            Some(TableEntry::Kv(kv)) => Some(&**kv),
            _ => None,
        };
        let extra = kv.is_some_and(|kv| {
            kv.pk_handle_offset().is_none() && kv.common_handle_offsets().is_empty()
        });
        let stored = kv.map_or(table.columns.len(), |kv| kv.columns().len());
        slots.push(Slot {
            width: stored + usize::from(extra),
            kv,
            stored,
            visible: table.columns.len() - usize::from(table.extra_handle),
            extra: table.extra_handle,
        });
    }
    let expected: usize = slots.iter().map(|slot| slot.width).sum();
    let zone = ctx.session_zone();
    let accountant = ctx.statement_memory().write_accountant(memory_label);
    let mut out = Vec::new();
    let collected = super::physical_builder::execute_dml_source(
        plan,
        catalog,
        ctx,
        runtime.is_some(),
        |row| {
            if row.len() != expected {
                return Err(DriverError::unsupported(format!(
                    "a multi-table read returned {} columns, expected {expected}",
                    row.len()
                )));
            }
            let mut ids = Vec::with_capacity(slots.len());
            let mut values = Vec::with_capacity(source.width());
            let mut stored = Vec::with_capacity(slots.len());
            let mut start = 0;
            for slot in &slots {
                let part = &row[start..start + slot.width];
                start += slot.width;
                let id = match slot.kv {
                    None => None,
                    Some(kv) => {
                        let handle = if let Some(offset) = kv.pk_handle_offset() {
                            match &part[offset] {
                                Datum::Int(value) => Some(TableHandle::Int(*value)),
                                Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                                _ => None,
                            }
                        } else if !kv.common_handle_offsets().is_empty() {
                            let handle_values: Vec<Datum> = kv
                                .common_handle_offsets()
                                .iter()
                                .map(|offset| part[*offset].clone())
                                .collect();
                            if handle_values
                                .iter()
                                .any(|value| matches!(value, Datum::Null))
                            {
                                None
                            } else {
                                Some(
                                    kv.common_handle_of_values(&handle_values, &zone)
                                        .map_err(kv_write_error)?,
                                )
                            }
                        } else {
                            match &part[slot.stored] {
                                Datum::Int(value) => Some(TableHandle::Int(*value)),
                                Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
                                _ => None,
                            }
                        };
                        handle
                    }
                };
                ids.push(id);
                values.extend_from_slice(&part[..slot.visible]);
                if slot.extra {
                    values.push(part[slot.stored].clone());
                }
                accountant
                    .account_row(&part[..slot.stored])
                    .map_err(DriverError::from)?;
                stored.push(part[..slot.stored].to_vec());
            }
            // Charge as rows arrive, before retaining them or pulling another chunk.
            accountant.account_row(&values).map_err(DriverError::from)?;
            out.push(SourceRow {
                ids,
                values,
                stored,
            });
            Ok(())
        },
    )?;
    if let Some(runtime) = runtime {
        runtime.extend(collected);
    }
    Ok(out)
}

/// The EXPLAIN plan of a multi-table `UPDATE`/`DELETE`: Go's `Update`/
/// `Delete` root over the optimized join `SelectPlan`. `None` for a
/// single-table statement, which keeps its own builder.
pub(crate) fn multi_dml_explain_plan(
    dml: MultiDmlRef<'_>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_planner::physical::PhysicalPlan>, DriverError> {
    match dml {
        MultiDmlRef::Update(update) => {
            let tidb_ast::UpdateKind::Multi { .. } = &update.kind else {
                return Ok(None);
            };
            multi_dml_physical_plan(MultiDmlRef::Update(update), catalog, current_db, ctx).map(Some)
        }
        MultiDmlRef::Delete(delete) => {
            let tidb_ast::DeleteKind::Multi { .. } = &delete.kind else {
                return Ok(None);
            };
            multi_dml_physical_plan(MultiDmlRef::Delete(delete), catalog, current_db, ctx).map(Some)
        }
    }
}

/// Which multi-table write [`multi_dml_explain_plan`] plans.
#[derive(Clone, Copy)]
pub(crate) enum MultiDmlRef<'a> {
    Update(&'a tidb_ast::UpdateStmt),
    Delete(&'a tidb_ast::DeleteStmt),
}
