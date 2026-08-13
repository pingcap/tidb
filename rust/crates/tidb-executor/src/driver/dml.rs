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

//! The write paths: `INSERT` / `UPDATE` / `DELETE`, plus the column-default,
//! cast, row-ordering and `ON DUPLICATE KEY UPDATE` machinery they share.
//!
//! Mirrors Go's `PlanBuilder.buildInsert` / `buildUpdate` / `buildDelete` and
//! the `executor` package's `InsertExec` / `UpdateExec` / `DeleteExec`.

use super::*;
use crate::kv_table::{AutoIdError, AutoIncrement, AutoRandom, AutoRandomError};

mod correlated;
mod defaults;

use correlated::{dml_table_scope, DmlExpression, UpdateExpression};

pub(crate) use defaults::{
    column_default, column_metadata, materialize_column_default, prepare_named_defaults,
    rewrite_with_prepared_defaults, ColumnDefaultMeta, DefaultColumnIdentity, DefaultUse,
    PreparedNamedDefault, ResolvedDefaultColumn,
};

/// Parses and runs a plain `INSERT INTO t [(cols)] VALUES (...), ...` against
/// `catalog`, returning the number of inserted rows.
///
/// The write half of the in-memory gateway. It supports the normal insert
/// forms, including `REPLACE`, `IGNORE`, `ON DUPLICATE KEY UPDATE`, `SET`
/// syntax, query sources, and `PARTITION (...)` destination validation.
/// A `RETURNING` clause is parsed and silently ignored: Go's hand-written
/// parser stores it on the AST but the planner and executor never read it, so
/// the write runs normally and answers with a plain OK packet. Columns not
/// listed in an explicit column list are filled with NULL (column defaults
/// wait on ColumnInfo default-value wiring).
pub fn run_insert_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_insert_on`] resolving unqualified names in `current_db`.
pub fn run_insert_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_insert_reporting(sql, catalog, current_db, ctx).map(|outcome| outcome.0)
}

/// [`run_insert_in`], also reporting the first auto-increment id the statement
/// allocated, which is what MySQL answers with as `LAST_INSERT_ID`.
///
/// `None` when the statement allocated nothing: an explicit auto value or a
/// table with no auto column leaves the session's value untouched, which is
/// the behavior captured from TiDB.
pub fn run_insert_reporting(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(u64, Option<u64>), DriverError> {
    let stmt = ctx.parse(sql)?;

    let insert = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => insert,
            _ => return Err(DriverError::unsupported("only INSERT is supported here")),
        },
        _ => return Err(DriverError::unsupported("only INSERT is supported here")),
    };
    run_insert_stmt(insert, catalog, current_db, ctx)
}

/// [`run_insert_reporting`], starting from an already-parsed `InsertStmt`
/// rather than re-parsing a SQL string -- what `EXPLAIN ANALYZE INSERT`
/// needs (it already holds the parsed statement the `EXPLAIN` wraps, and
/// real `EXPLAIN ANALYZE` executes the wrapped statement, captured via
/// `pkg/executor`: an `EXPLAIN ANALYZE INSERT` really inserts the row).
pub(crate) fn run_insert_stmt(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(u64, Option<u64>), DriverError> {
    run_insert_traced(insert, catalog, current_db, ctx, None)
}

struct InsertTargetLayout {
    database: String,
    table_name: String,
    column_list: Vec<(String, FieldType)>,
    target_offsets: Vec<usize>,
    generated_targets: Vec<bool>,
    column_meta: Vec<ColumnDefaultMeta>,
}

/// Resolves everything owned by the INSERT target before an INSERT SELECT
/// source is executed. Go plans the target and rejects a view, sequence,
/// unknown field or generated target before opening the source executor; a
/// source with user-variable or sequence side effects must not run first.
fn resolve_insert_target(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<InsertTargetLayout, DriverError> {
    let (database, table_name) = split_table_path(&insert.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let table = catalog
        .get_in(&database, &table_name)
        .ok_or(DriverError::unsupported("table not found in catalog"))?;
    if table.is_view() {
        return Err(DriverError::InsertIntoViewUnsupported(table_name));
    }
    if table.is_sequence() {
        return Err(DriverError::InsertIntoSequenceUnsupported(table_name));
    }
    if matches!(table, TableEntry::Cte(_)) {
        return Err(DriverError::unsupported("a CTE is not an INSERT target"));
    }
    let column_list = table.column_list();
    let named_columns: Vec<String> = if insert.set_syntax {
        insert
            .set_columns
            .iter()
            .map(|path| path.last().cloned().unwrap_or_default())
            .collect()
    } else {
        insert.columns.clone()
    };
    let target_offsets = if insert.set_syntax || insert.columns_specified {
        named_columns
            .iter()
            .map(|name| {
                column_list
                    .iter()
                    .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                    .ok_or_else(|| DriverError::UnknownColumnInClause {
                        column: name.clone(),
                        clause: "field list".to_owned(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        (0..column_list.len()).collect()
    };
    let generated_targets: Vec<bool> = match table {
        TableEntry::Kv(kv) => kv
            .visible_columns()
            .iter()
            .map(|column| column.generated.is_some())
            .collect(),
        TableEntry::Mem(_) => vec![false; column_list.len()],
        TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            unreachable!("read-only targets were refused above")
        }
    };
    let column_meta = column_metadata(table);
    if insert.source.is_some() {
        for &offset in &target_offsets {
            if generated_targets[offset] {
                return Err(DriverError::BadGeneratedColumn {
                    column: column_list[offset].0.clone(),
                    table: table_name,
                });
            }
        }
    }
    Ok(InsertTargetLayout {
        database,
        table_name,
        column_list,
        target_offsets,
        generated_targets,
        column_meta,
    })
}

/// [`run_insert_stmt`], recording the plan it builds into `trace`.
///
/// An `INSERT ... SELECT`'s source is traced by the very run that feeds the
/// insert, so its `actRows` are the rows this statement really read -- there
/// is no second, mirrored execution of the source to count them, and so no
/// way for a source reading the target table to be counted twice.
pub(crate) fn run_insert_traced(
    insert: &tidb_ast::InsertStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<(u64, Option<u64>), DriverError> {
    if insert.replace && !insert.on_duplicate.is_empty() {
        return Err(DriverError::unsupported("partitions are not supported yet"));
    }

    let target_layout = resolve_insert_target(insert, catalog, current_db)?;
    let eval_chunk = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let select_on_duplicate = if insert.source.is_some() {
        Some(prepare_on_duplicate_assignments(
            &insert.on_duplicate,
            &target_layout.column_list,
            &target_layout.column_meta,
            &target_layout.table_name,
            ctx,
            eval_chunk.get_row(0),
        )?)
    } else {
        None
    };

    // Once the target is valid, `INSERT ... SELECT` runs its source over the
    // pre-insert catalog and materializes those rows before the table is
    // borrowed mutably.
    let source_rows: Option<Vec<Vec<Datum>>> = match &insert.source {
        Some(query) => Some(match &**query {
            tidb_ast::QueryStmt::Select(select) => {
                run_select_traced(
                    select,
                    catalog,
                    current_db,
                    ctx,
                    trace.as_deref_mut(),
                    &tidb_planner::physical_property::PhysicalProperty::default(),
                    false,
                )?
                .1
            }
            tidb_ast::QueryStmt::SetOpr(set_opr) => {
                // EXPLAIN has never described a set-operation source.
                if let Some(trace) = trace.as_deref_mut() {
                    trace.refuse("EXPLAIN of a set-operation INSERT source is not supported yet");
                }
                run_set_opr_stmt(set_opr, catalog, current_db, ctx)?.1
            }
        }),
        None => None,
    };
    if let Some(trace) = trace {
        trace.write("Insert", insert.source.is_some());
        // Plain `EXPLAIN INSERT` plans the write without performing it, as
        // Go's does (captured: the row is not there afterward).
        if trace.is_plan_only() {
            return Ok((0, None));
        }
    }

    let InsertTargetLayout {
        database,
        table_name,
        column_list,
        target_offsets,
        generated_targets,
        column_meta,
    } = target_layout;
    let table = catalog
        .get_mut_in(&database, &table_name)
        .ok_or(DriverError::unsupported("table not found in catalog"))?;

    // Go's planner wraps a partitioned INSERT target in
    // `partitionTableWithGivenSets`: names are resolved once, then every
    // completed row is routed normally and checked against this id set at the
    // table write boundary.  This is deliberately not a read restriction.
    let insert_partition_ids = if insert.partitions.is_empty() {
        None
    } else {
        let TableEntry::Kv(kv) = &*table else {
            return Err(DriverError::UnknownPartition {
                partition: insert.partitions[0].clone(),
                table: table_name.clone(),
            });
        };
        let Some(spec) = kv.partition() else {
            return Err(DriverError::UnknownPartition {
                partition: insert.partitions[0].clone(),
                table: table_name.clone(),
            });
        };
        Some(
            crate::partition_pruning::ids_for_selected_partitions(spec, &insert.partitions)
                .map_err(|partition| DriverError::UnknownPartition {
                    partition,
                    table: table_name.clone(),
                })?,
        )
    };

    let auto_increment_offset = match table {
        TableEntry::Kv(kv) => kv.auto_increment_offset(),
        TableEntry::Mem(_) => None,
        TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            unreachable!("INSERT through a read-only relation is refused above")
        }
    };
    let auto_random_offset = match table {
        TableEntry::Kv(kv) => kv.auto_random().map(|spec| spec.offset),
        TableEntry::Mem(_) => None,
        TableEntry::Cte(_) | TableEntry::View(_) | TableEntry::Sequence(_) => {
            unreachable!("INSERT through a read-only relation is refused above")
        }
    };
    // One marker per staged row: after casting, a supplied zero under
    // NO_AUTO_VALUE_ON_ZERO remains zero while NULL and omitted values still
    // take an id. Keeping the supplied-ness beside the row distinguishes the
    // two zero values that the later allocator otherwise cannot tell apart.
    let mut auto_rows: Vec<(usize, bool)> = Vec::new();
    let mut auto_random_rows: Vec<(usize, bool)> = Vec::new();
    let mut first_allocated: Option<u64> = None;

    let mut inserted = 0u64;
    // A source query supplies already-evaluated values; a VALUES list
    // supplies expressions. Both fill the same target offsets.
    let value_rows: Vec<Vec<Datum>> = match &source_rows {
        Some(rows) => rows.clone(),
        None => Vec::new(),
    };
    let row_count = source_rows.as_ref().map_or(insert.rows.len(), Vec::len);
    // Go `ResetContextOfStmt`'s `ast.InsertStmt` arm:
    //   ErrGroupBadNull  = error when !IgnoreErr && (strict || len(stmt.Lists) == 1)
    //   ErrGroupNoDefault = error when strict
    // `stmt.Lists` is the VALUES lists, so an `INSERT ... SELECT` is never
    // "single" no matter how many rows the query returns -- which is why the
    // count below reads the AST and not `row_count`.
    //
    // This is the ONE value-level rule `IGNORE` does not reach through
    // `strict` alone: the single-row promotion holds in every SQL mode, so an
    // `IGNORE` statement has to override it separately. Captured from TiDB,
    // `INSERT IGNORE INTO t(a INT NOT NULL) VALUES (NULL)` under the default
    // strict mode warns 1048 and stores `0`.
    let bad_null_level = crate::bad_null::NullLevel::from_is_error(
        !ctx.ignore_err() && (ctx.strict() || insert.rows.len() == 1),
    );

    enum PreparedInsertValue {
        Generated,
        Expression(Box<Expression>),
    }

    // Go lowers every explicit VALUES default while building the insert plan,
    // row-major and before an ordinary VALUES expression is evaluated. This
    // ordering is observable for computed defaults such as RAND(), and is
    // deliberately distinct from omitted columns, whose defaults are filled
    // later while each runtime row is built.
    let mut prepared_value_rows: Vec<Vec<PreparedInsertValue>> = Vec::new();
    let names_a_column = insert.set_syntax || insert.columns_specified;
    let mut previous_width = target_offsets.len();
    if source_rows.is_none() {
        let resolver = TableResolver {
            table_name: &table_name,
            columns: &column_list,
            zone: ctx.session_zone(),
        };
        for (index, values) in insert.rows.iter().enumerate() {
            let width = values.len();
            let expected = if index == 0 {
                target_offsets.len()
            } else {
                previous_width
            };
            let arity_is_checked = index > 0 || names_a_column || width > 0;
            if arity_is_checked && width != expected {
                return Err(DriverError::unsupported(
                    "VALUES arity does not match the column list",
                ));
            }
            previous_width = width;

            let mut prepared = Vec::with_capacity(width);
            for (position, value) in values.iter().enumerate() {
                let target_offset = target_offsets[position];
                let target = ResolvedDefaultColumn {
                    identity: DefaultColumnIdentity {
                        table: 0,
                        column: target_offset,
                    },
                    meta: column_meta[target_offset].clone(),
                };
                let resolve_root_default =
                    |path: &[String]| -> Result<ResolvedDefaultColumn, DriverError> {
                        let name = path
                            .last()
                            .ok_or(DriverError::unsupported("DEFAULT() needs a column"))?;
                        let column = column_meta
                            .iter()
                            .position(|meta| meta.name.eq_ignore_ascii_case(name))
                            .ok_or_else(|| DriverError::UnknownColumnInClause {
                                column: name.clone(),
                                clause: "field list".to_owned(),
                            })?;
                        Ok(ResolvedDefaultColumn {
                            identity: DefaultColumnIdentity { table: 0, column },
                            meta: column_meta[column].clone(),
                        })
                    };

                let expression = match value {
                    tidb_ast::Expr::Default(None) if target.meta.generated => {
                        prepared.push(PreparedInsertValue::Generated);
                        continue;
                    }
                    tidb_ast::Expr::Default(None) => {
                        let datum = materialize_column_default(
                            &target.meta,
                            DefaultUse::Insert,
                            ctx,
                            eval_chunk.get_row(0),
                        )?;
                        Expression::Constant(tidb_expr::constant::Constant::new(
                            datum,
                            target.meta.field_type,
                        ))
                    }
                    tidb_ast::Expr::Default(Some(path)) => {
                        let source = resolve_root_default(path)?;
                        if target.identity != source.identity
                            && (target.meta.generated || source.meta.generated)
                        {
                            return Err(DriverError::BadGeneratedColumn {
                                column: target.meta.name,
                                table: table_name.clone(),
                            });
                        }
                        if target.meta.generated {
                            prepared.push(PreparedInsertValue::Generated);
                            continue;
                        }
                        let datum = materialize_column_default(
                            &source.meta,
                            DefaultUse::Insert,
                            ctx,
                            eval_chunk.get_row(0),
                        )?;
                        Expression::Constant(tidb_expr::constant::Constant::new(
                            datum,
                            source.meta.field_type,
                        ))
                    }
                    _ if target.meta.generated => {
                        return Err(DriverError::BadGeneratedColumn {
                            column: target.meta.name,
                            table: table_name.clone(),
                        });
                    }
                    _ => {
                        let defaults = prepare_named_defaults(
                            value,
                            ctx,
                            eval_chunk.get_row(0),
                            DefaultUse::Expression,
                            |path| {
                                let (column, _, _) = resolver.resolve(path).ok_or_else(|| {
                                    DriverError::UnknownColumnInClause {
                                        column: path.last().cloned().unwrap_or_default(),
                                        clause: "field list".to_owned(),
                                    }
                                })?;
                                Ok(ResolvedDefaultColumn {
                                    identity: DefaultColumnIdentity { table: 0, column },
                                    meta: column_meta[column].clone(),
                                })
                            },
                        )?;
                        rewrite_with_prepared_defaults(value, &resolver, &defaults)?
                    }
                };
                prepared.push(PreparedInsertValue::Expression(Box::new(expression)));
            }
            prepared_value_rows.push(prepared);
        }
    } else {
        // INSERT SELECT still performs the same arity check, but has no AST
        // values to lower.
        for (index, values) in value_rows.iter().enumerate() {
            let width = values.len();
            let expected = if index == 0 {
                target_offsets.len()
            } else {
                previous_width
            };
            if width != expected {
                return Err(DriverError::unsupported(
                    "VALUES arity does not match the column list",
                ));
            }
            previous_width = width;
        }
    }

    // Go resolves ON DUPLICATE after the VALUES lists and before execution.
    // Keeping that placement makes generated-column and DEFAULT errors visible
    // even when no candidate row ends up conflicting.
    let prepared_on_duplicate = match select_on_duplicate {
        Some(prepared) => prepared,
        None => prepare_on_duplicate_assignments(
            &insert.on_duplicate,
            &column_list,
            &column_meta,
            &table_name,
            ctx,
            eval_chunk.get_row(0),
        )?,
    };

    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(row_count);
    // Go `buildValuesListOfInsert` checks arity in two steps: the FIRST row
    // against the target columns, and every later row against the one before
    // it. The first check is skipped when both the column list and the first
    // value list are empty, which is what makes `INSERT t VALUES ()` -- a row
    // of nothing but defaults -- legal; chaining the later rows to their
    // predecessor is then what still rejects `VALUES (), (1)`. An empty row
    // assigns nothing, so the default, auto-increment and NOT NULL rules
    // below fill the whole row.
    for index in 0..row_count {
        let width = match source_rows.as_ref() {
            Some(_) => value_rows.get(index).map_or(0, Vec::len),
            None => insert.rows[index].len(),
        };
        let mut row = vec![Datum::Null; column_list.len()];
        let mut assigned = vec![false; column_list.len()];
        for (position, &offset) in target_offsets.iter().enumerate().take(width) {
            let value = match source_rows.as_ref() {
                Some(_) => value_rows[index][position].clone(),
                None => match &prepared_value_rows[index][position] {
                    PreparedInsertValue::Generated => continue,
                    PreparedInsertValue::Expression(expression) => expression
                        .eval(ctx, eval_chunk.get_row(0))
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
                },
            };
            row[offset] = value;
            assigned[offset] = true;
        }
        // Go fills the auto-increment column before the default and NOT NULL
        // rules run, so an omitted auto column never looks like a missing
        // value (`adjustAutoIncrementDatum` runs inside the row build).
        if let Some(offset) = auto_increment_offset {
            // An omitted or explicitly NULL auto column becomes the zero
            // marker, which allocation replaces; Go does this before the
            // NOT NULL check, so a NULL here is never a bad-null error.
            let supplied = assigned[offset] && row[offset] != Datum::Null;
            if !supplied {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_rows.push((new_rows.len(), supplied));
        }
        // AUTO_RANDOM uses the same NULL/omitted/zero value boundary as Go's
        // `adjustAutoRandomDatum`, but draws from its own allocator below.
        if let Some(offset) = auto_random_offset {
            let supplied = assigned[offset] && row[offset] != Datum::Null;
            if !supplied {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_random_rows.push((new_rows.len(), supplied));
        }
        // A generated column has a value source of its own, so it is neither
        // defaulted nor NULL-checked here: it counts as supplied, and the
        // expression fills it below. Without this a `NOT NULL` generated
        // column would raise ErrNoDefaultForField for a row Go accepts.
        for (offset, generated) in generated_targets.iter().enumerate() {
            if *generated {
                assigned[offset] = true;
                row[offset] = Datum::Null;
            }
        }
        // Only a column the statement omits takes its default, and only such
        // a column can raise ErrNoDefaultForField (Go `fillColValue`).
        for offset in 0..column_list.len() {
            if !assigned[offset] {
                row[offset] = column_default(&column_meta, offset, ctx, eval_chunk.get_row(0))?;
            }
        }
        // Go `Column.HandleBadNull`: an explicit NULL in a NOT NULL column is
        // ErrColumnCantNull, which is a different error from omitting a
        // column that has no default. Whether it FAILS the statement is
        // `bad_null_level` above, not the SQL mode alone.
        for (offset, value) in row.iter_mut().enumerate() {
            // A generated column's value is not built yet at this point, so
            // the NULL standing in for it is not the user's NULL.
            if assigned[offset] && !generated_targets[offset] {
                crate::bad_null::handle_bad_null(
                    value,
                    &column_meta[offset].field_type,
                    &column_list[offset].0,
                    bad_null_level,
                    ctx,
                )?;
            }
        }
        // Go casts each value to its column's type before the row is
        // written, which is what rounds a decimal to the column's scale and
        // parses a numeric string.
        if let TableEntry::Kv(kv) = &*table {
            for (offset, value) in row.iter_mut().enumerate() {
                let column = &kv.columns[offset];
                *value = cast_value_for_column(
                    std::mem::replace(value, Datum::Null),
                    &column.field_type,
                    &column.name,
                    new_rows.len(),
                    ctx,
                )?;
            }
            // The generated columns are computed from the finished row, so
            // the conflict lookup and the foreign-key check below see the
            // same values the write will store.
            kv.materialize_generated(&mut row, ctx)
                .map_err(kv_write_error)?;
        }
        new_rows.push(row);
        inserted += 1;
    }
    // Go `InsertValues.insertRows`/`insertRowsFromSelect`, the consume that
    // sits immediately before `base.exec(ctx, rows)`:
    // `types.EstimatedMemUsage(rows[0], len(rows))` over the staged rows.
    // Accounting HERE, before a single row is written, is what makes a
    // cancelled INSERT leave the table exactly as it found it.
    ctx.statement_memory()
        .write_accountant(mem_quota::label::INSERT)
        .account_rows(&new_rows)
        .map_err(DriverError::from)?;
    // A matrix table has neither an allocator nor constraints, so it is
    // finished here; everything below is the byte-backed write path.
    if let TableEntry::Mem(mem) = table {
        mem.rows.extend(new_rows);
        return Ok((inserted, first_allocated));
    }
    {
        let TableEntry::Kv(kv) = table else {
            unreachable!("INSERT through a view is refused above")
        };
        if let Some(auto_offset) = auto_random_offset {
            for (index, supplied) in &auto_random_rows {
                if *supplied
                    && ctx.auto_increment_zero_is_explicit()
                    && matches!(
                        new_rows[*index][auto_offset],
                        Datum::Int(0) | Datum::UInt(0)
                    )
                {
                    continue;
                }
                let outcome = kv
                    .apply_auto_random(
                        &mut new_rows[*index],
                        ctx.auto_increment_step(),
                        ctx.allow_auto_random_explicit_insert(),
                        || ctx.reuse_auto_random_id(),
                        ctx.next_auto_random_shard(1),
                    )
                    .map_err(|error| match error {
                        AutoRandomError::ExplicitInsertDisabled => DriverError::InvalidAutoRandom(
                            "Explicit insertion on auto_random column is disabled. Try to set @@allow_auto_random_explicit_insert = true."
                                .to_owned(),
                        ),
                        AutoRandomError::AutoId(AutoIdError::Exhausted) => {
                            DriverError::AutoRandReadFailed
                        }
                        AutoRandomError::AutoId(AutoIdError::OutOfRange {
                            value,
                            type_name,
                        }) => DriverError::ConstantOverflows { value, type_name },
                        AutoRandomError::AutoId(AutoIdError::Store(detail)) => {
                            DriverError::AutoIdUnavailable(detail.0)
                        }
                        AutoRandomError::NotApplicable
                        | AutoRandomError::RebaseOverflow { .. } => {
                            unreachable!("row allocation does not rebase a table option")
                        }
                    })?;
                if let Some(placed) = outcome.placed() {
                    ctx.record_auto_random_id(placed);
                }
                match outcome {
                    AutoRandom::Given(given) => ctx.record_given_insert_id(given),
                    AutoRandom::Allocated(id) if first_allocated.is_none() => {
                        first_allocated = Some(id);
                    }
                    AutoRandom::Absent | AutoRandom::Reused(_) | AutoRandom::Allocated(_) => {}
                }
            }
        }
        if let Some(auto_offset) = auto_increment_offset {
            // The allocator lives on the table, so the ids are handed out here
            // rather than while the rows were being built.
            for (index, supplied) in &auto_rows {
                if *supplied
                    && ctx.auto_increment_zero_is_explicit()
                    && matches!(
                        new_rows[*index][auto_offset],
                        Datum::Int(0) | Datum::UInt(0)
                    )
                {
                    continue;
                }
                // A full domain is Go's 1467; a counter whose home could not
                // be reached is NOT that, and saying 1467 for it would report
                // a table that has run out of ids when the ids are all still
                // there.
                // Go's replay hands the row back the id its losing attempt
                // gave it (`RetryInfo`); outside a replay there is nothing to
                // hand back and the counter is drawn from as usual. The cursor
                // is read lazily so that a row carrying its OWN id does not
                // consume from it -- see `apply_auto_increment`.
                let outcome = kv
                    .apply_auto_increment(&mut new_rows[*index], ctx.auto_increment_step(), || {
                        ctx.reuse_auto_increment_id()
                    })
                    .map_err(|error| match error {
                        AutoIdError::Exhausted => DriverError::AutoincReadFailed,
                        // An id that does not fit the COLUMN is not a full
                        // domain: Go casts the allocated id and reports the
                        // cast's own 1690, which names the value and type.
                        AutoIdError::OutOfRange { value, type_name } => {
                            DriverError::ConstantOverflows { value, type_name }
                        }
                        AutoIdError::Store(detail) => DriverError::AutoIdUnavailable(detail.0),
                    })?;
                // Recorded whether it was drawn, handed back, or supplied by
                // the row, so the NEXT attempt replays this attempt's
                // assignment exactly. Go records in all three arms
                // (`insert_common.go:902`, `:946`), and an explicit id left
                // out of the list is what desynchronised the cursor.
                if let Some(placed) = outcome.placed() {
                    ctx.record_auto_increment_id(placed);
                }
                match outcome {
                    AutoIncrement::Given(given) => {
                        // Go records the explicit value as `StmtCtx.InsertID`,
                        // and the LAST row's wins; the OK packet falls back to
                        // it when the statement published nothing.
                        ctx.record_given_insert_id(given);
                    }
                    // Go keeps the FIRST id the statement ALLOCATED. A reused
                    // one is deliberately not it: the replay's consume loop
                    // returns before `lastInsertID` is ever assigned, so the
                    // value a client read after the losing attempt is the one
                    // that survives.
                    AutoIncrement::Allocated(id) if first_allocated.is_none() => {
                        first_allocated = Some(id as u64);
                    }
                    AutoIncrement::Absent
                    | AutoIncrement::Reused(_)
                    | AutoIncrement::Allocated(_) => {}
                }
            }
        }
    }
    // Go's `FKCheckExec` sits between the row build and the write, which is
    // why the table's own borrow is released for it: the check reads the
    // PARENT tables, which the statement never named.
    let fk_verdicts = if ctx.foreign_key_checks() {
        crate::foreign_key::check_child_rows(
            catalog,
            &database,
            &table_name,
            &new_rows,
            &ctx.session_zone(),
        )?
    } else {
        vec![None; new_rows.len()]
    };
    if !insert.ignore {
        if let Some(error) = fk_verdicts.iter().flatten().next() {
            return Err(error.clone());
        }
    }
    // The table is re-borrowed per use rather than held across the loop,
    // because REPLACE's row removal runs the PARENT-side referential
    // operators, and those write the DEPENDENT tables the statement never
    // named -- reachable only from the catalog, not from this table.
    fn target<'a>(
        catalog: &'a mut Catalog,
        database: &str,
        table_name: &str,
    ) -> &'a mut crate::kv_table::KvTable {
        match catalog.get_mut_in(database, table_name) {
            Some(TableEntry::Kv(kv)) => kv,
            _ => unreachable!("INSERT through a view is refused above"),
        }
    }
    // Go resolves a conflict per row, before the row is written: REPLACE
    // deletes every row it collides with, ON DUPLICATE KEY UPDATE applies
    // its assignments to the first one, and IGNORE skips the row with the
    // duplicate reported as a warning.
    inserted = 0;
    for (position, row) in new_rows.iter().enumerate() {
        // Go's `FKCheckExec` runs per row, before the row is added, and
        // under `INSERT IGNORE` its violation is a warning and a skip rather
        // than a statement error.
        if let Some(error) = &fk_verdicts[position] {
            let warning = error.clone().to_mysql_error();
            ctx.append_warning_parts(warning.code, &warning.message);
            continue;
        }
        let conflicts = target(catalog, &database, &table_name)
            .conflicting_handles(row, &ctx.session_zone())
            .map_err(|e| kv_read_error("conflict lookup failed", e))?;
        if !conflicts.is_empty() {
            if insert.replace {
                // Go `InsertValues.removeRow` (`insert_common.go`): a
                // conflicting row IDENTICAL to the one being written is left
                // in place -- not deleted and not rewritten -- and counts
                // ONE, not the two a delete-plus-insert would. This is also
                // the site `tidb_lock_unchanged_keys` governs, which is why
                // Go's `TestInsertLockUnchangedKeys` drives it with
                // `replace into t values (1)` over the same row.
                let mut unchanged = false;
                for handle in &conflicts {
                    let existing = target(catalog, &database, &table_name)
                        .get_row_by_handle(handle, &ctx.session_zone())
                        .map_err(|e| kv_read_error("row read failed", e))?;
                    if existing.as_deref() == Some(row) {
                        inserted += 1;
                        unchanged = true;
                        break;
                    }
                    // Go `removeRow` ends in `onRemoveRowForFK`, so the row
                    // REPLACE withdraws is a PARENT-side change exactly like
                    // a DELETE's: a dependent that restricts makes the whole
                    // statement 1451, and one that cascades follows it. Run
                    // BEFORE the removal, so a restricted REPLACE leaves the
                    // parent where it was rather than half-applied.
                    if let (true, Some(existing)) = (ctx.foreign_key_checks(), &existing) {
                        let changes = [crate::foreign_key::ParentChange::Delete(existing)];
                        crate::foreign_key::cascade_parent_changes(
                            catalog,
                            &database,
                            &table_name,
                            &changes,
                            ctx,
                        )?;
                    }
                    // Otherwise the conflicting row goes, and the affected
                    // count is one per deleted row plus one for the inserted
                    // row.
                    target(catalog, &database, &table_name)
                        .delete_row(handle, &ctx.session_zone())
                        .map_err(|e| kv_read_error("row delete failed", e))?;
                    inserted += 1;
                }
                if unchanged {
                    continue;
                }
            } else if !insert.on_duplicate.is_empty() {
                inserted += apply_on_duplicate(
                    target(catalog, &database, &table_name),
                    &conflicts[0],
                    row,
                    &prepared_on_duplicate,
                    &column_list,
                    position,
                    ctx,
                )?;
                continue;
            } else if insert.ignore {
                let reported = target(catalog, &database, &table_name)
                    .duplicate_entry_error(row, &ctx.session_zone())
                    .map_err(|e| kv_read_error("conflict lookup failed", e))?;
                if let crate::kv_table::KvTableError::DuplicateEntry { value, key } = reported {
                    let warning = DriverError::DuplicateEntry { value, key }.to_mysql_error();
                    ctx.append_warning_parts(warning.code, &warning.message);
                }
                continue;
            }
        }
        // Go publishes the statement's first allocated id the moment a row is
        // ACCEPTED for insertion (`addRecord` -> `SetLastInsertID`), which is
        // why a hard duplicate publishes -- its deferred unique-key check
        // fails the statement only afterwards -- while an IGNORE-skipped row
        // and a row redirected into ON DUPLICATE KEY UPDATE never reach here
        // and so publish nothing.
        if let Some(allocated) = first_allocated {
            ctx.publish_last_insert_id(allocated);
        }
        if let Some(partitions) = &insert_partition_ids {
            target(catalog, &database, &table_name)
                .validate_insert_partitions(row, partitions, ctx)
                .map_err(kv_write_error)?;
        }
        target(catalog, &database, &table_name)
            .insert_row(row, ctx)
            .map_err(kv_write_error)?;
        inserted += 1;
    }
    Ok((inserted, first_allocated))
}

/// The one rendering of a byte-backed write failure, so every write path
/// reports the same statement error for the same cause.
///
/// The generation arm is the reason this is shared rather than repeated: a
/// generated column's expression fails with an evaluation error that already
/// carries its own MySQL code (1365 for a zero divisor under
/// `ERROR_FOR_DIVISION_BY_ZERO`), and rendering it as a generic parse failure
/// would replace the code an application branches on.
pub(crate) fn kv_write_error(error: crate::kv_table::KvTableError) -> DriverError {
    match error {
        crate::kv_table::KvTableError::Storage(message) if message.starts_with("Retryable(") => {
            DriverError::Txn(crate::TxnErrorKind::RegionUnavailable)
        }
        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
            DriverError::DuplicateEntry { value, key }
        }
        crate::kv_table::KvTableError::Generation {
            eval: Some(eval), ..
        } => DriverError::Exec(crate::ExecError::Eval(eval)),
        // A RANGE table with no `MAXVALUE` partition rejects the row rather
        // than storing it somewhere; 1526 is the code an application sees.
        crate::kv_table::KvTableError::NoPartitionForValue(value) => {
            DriverError::NoPartitionForValue(value)
        }
        crate::kv_table::KvTableError::RowDoesNotMatchGivenPartitionSet => {
            DriverError::RowDoesNotMatchGivenPartitionSet
        }
        // A HASH partition value with no signed reading is Go's own
        // `ConvertTo` error surfacing out of `locateHashPartition`: 1690,
        // naming the value and `bigint`, the type it did not fit.
        crate::kv_table::KvTableError::PartitionValueOverflowsBigint(value) => {
            DriverError::ConstantOverflows {
                value,
                type_name: "bigint".to_owned(),
            }
        }
        // The `_tidb_rowid` a non-clustered row needs comes off the same
        // counter the AUTO_INCREMENT column does, so its exhaustion is the
        // same 1467 an allocated column value would have reported.
        crate::kv_table::KvTableError::AutoIdExhausted => DriverError::AutoincReadFailed,
        other => DriverError::Parse(format!("row encode failed: {other:?}")),
    }
}

/// Renders a table read failure while preserving the storage layer's
/// retryable-region signal as a transaction error instead of a parse error.
pub(crate) fn kv_read_error(operation: &str, error: crate::kv_table::KvTableError) -> DriverError {
    match error {
        crate::kv_table::KvTableError::Storage(message) if message.starts_with("Retryable(") => {
            DriverError::Txn(crate::TxnErrorKind::RegionUnavailable)
        }
        other => DriverError::Parse(format!("{operation}: {other:?}")),
    }
}

/// Orders candidate rows the way a DML statement's own `ORDER BY` does, and
/// reports the row cap its `LIMIT` sets.
///
/// Go plans `UPDATE`/`DELETE ... ORDER BY ... LIMIT n` as a sort and a limit
/// over the rows to modify, so the cap counts rows actually MODIFIED, not
/// rows examined -- which is why the limit is applied by the caller as it
/// modifies rather than by truncating this list.
pub(crate) fn order_rows_for_dml<H>(
    rows: &mut [(H, Vec<Datum>)],
    order_by: &[tidb_ast::OrderItem],
    field_types: &[FieldType],
    resolver: &impl tidb_expr::rewriter::ColumnResolver,
    column_names: &[String],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let mut items = Vec::with_capacity(order_by.len());
    for item in order_by {
        // A bare positive integer literal is a positional reference to the
        // table's own column at that 1-based position, NOT a constant —
        // confirmed via `zz_dump_parity_test.go`
        // (`TestZZDumpParityDMLPositionalOrderBy`): `UPDATE t SET a = a +
        // 100 ORDER BY 2 LIMIT 1` on `t(a, b)` picked the row with the
        // smallest `b`, i.e. `2` resolved to column `b`, exactly like
        // `SELECT`'s positional `ORDER BY`/`GROUP BY` (see
        // `tidb_exec::order::positional`). There is no select list here, so
        // the position indexes the table's declared columns instead.
        let resolved_expr = match dml_order_by_position(&item.expr)? {
            Some(pos) => {
                let name = column_names
                    .get(pos)
                    .ok_or(DriverError::unsupported("ORDER BY position out of range"))?;
                tidb_ast::Expr::Column(vec![name.clone()])
            }
            None => item.expr.clone(),
        };
        let expr = rewrite_expr_resolved(&resolved_expr, resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        items.push((expr, item.desc));
    }
    // Each row's sort key is computed once, so the comparison itself cannot
    // fail partway through and leave a partial order.
    let mut keyed = Vec::with_capacity(rows.len());
    for (index, (_, row)) in rows.iter().enumerate() {
        let chunk = row_chunk(row, field_types)?;
        let mut key = Vec::with_capacity(items.len());
        for (expr, _) in &items {
            key.push(
                expr.eval(ctx, chunk.get_row(0))
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
            );
        }
        keyed.push((index, key));
    }
    let mut failure = None;
    keyed.sort_by(|left, right| {
        for (position, (_, desc)) in items.iter().enumerate() {
            let ordering = match tidb_expr::compare_datums(&left.1[position], &right.1[position]) {
                Ok(ordering) => ordering,
                Err(error) => {
                    failure = Some(error);
                    std::cmp::Ordering::Equal
                }
            };
            if ordering != std::cmp::Ordering::Equal {
                return if *desc { ordering.reverse() } else { ordering };
            }
        }
        std::cmp::Ordering::Equal
    });
    if let Some(error) = failure {
        return Err(DriverError::Exec(ExecError::Eval(error)));
    }
    let order: Vec<usize> = keyed.into_iter().map(|(index, _)| index).collect();
    apply_permutation(rows, &order);
    Ok(())
}

/// If `expr` is a positive integer literal `N` (or the boolean literals
/// `TRUE`/`FALSE`, treated as `1`/`0`), returns its 0-based column index
/// (`N-1`); any other expression returns `None`; position `0` is an error.
/// Mirrors `tidb_exec::order::positional`'s SELECT-list version, but for
/// `UPDATE`/`DELETE ... ORDER BY`, which has no select list to index — see
/// `order_rows_for_dml`.
pub(crate) fn dml_order_by_position(expr: &tidb_ast::Expr) -> Result<Option<usize>, DriverError> {
    if let Some((_, index)) = positional_field_index(expr) {
        return index.map(Some).map_err(|why| match why {
            PositionalError::Malformed => DriverError::unsupported("ORDER BY position"),
            PositionalError::Zero => DriverError::unsupported("ORDER BY position 0"),
        });
    }
    // `ORDER BY TRUE` reaches the DML tier as a boolean literal rather than
    // digits, and MySQL still reads it as position 1.
    match expr {
        tidb_ast::Expr::Bool(b) => usize::from(*b)
            .checked_sub(1)
            .ok_or(DriverError::unsupported("ORDER BY position 0"))
            .map(Some),
        _ => Ok(None),
    }
}

/// Reorders `rows` so that position `i` holds what was at `order[i]`.
pub(crate) fn apply_permutation<T>(rows: &mut [T], order: &[usize]) {
    let mut done = vec![false; rows.len()];
    for start in 0..rows.len() {
        if done[start] || order[start] == start {
            done[start] = true;
            continue;
        }
        let mut current = start;
        loop {
            let next = order[current];
            done[current] = true;
            if next == start {
                break;
            }
            rows.swap(current, next);
            current = next;
        }
    }
}

/// The row cap a DML `LIMIT` sets, which Go requires to be a constant.
pub(crate) fn dml_row_limit(limit: &Option<tidb_ast::Limit>) -> Result<Option<u64>, DriverError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    if limit.offset.is_some() {
        return Err(DriverError::unsupported(
            "an UPDATE/DELETE LIMIT takes no offset",
        ));
    }
    Ok(Some(eval_limit_bound(&limit.count)?))
}

#[derive(Clone, Debug)]
enum PreparedOnDuplicateValue {
    Constant(Expression),
    Ast {
        value: tidb_ast::Expr,
        defaults: Vec<PreparedNamedDefault>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedOnDuplicateAssignment {
    offset: usize,
    value: PreparedOnDuplicateValue,
}

/// Resolves ON DUPLICATE assignments once, whether or not an inserted row
/// eventually conflicts. `VALUES(col)` remains in the AST until a candidate
/// row exists, but every DEFAULT leaf is already a typed statement constant.
fn prepare_on_duplicate_assignments(
    assignments: &[tidb_ast::Assignment],
    column_list: &[(String, FieldType)],
    column_meta: &[ColumnDefaultMeta],
    table_name: &str,
    ctx: &crate::StmtContext,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Vec<PreparedOnDuplicateAssignment>, DriverError> {
    let resolver = TableResolver {
        table_name,
        columns: column_list,
        zone: ctx.session_zone(),
    };
    let mut prepared = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let (offset, _, _) = resolver.resolve(&assignment.col).ok_or_else(|| {
            DriverError::UnknownColumnInClause {
                column: assignment.col.last().cloned().unwrap_or_default(),
                clause: "field list".to_owned(),
            }
        })?;
        let target_identity = DefaultColumnIdentity {
            table: 0,
            column: offset,
        };
        let target_meta = &column_meta[offset];

        if target_meta.generated {
            let is_own_default = match &assignment.value {
                tidb_ast::Expr::Default(None) => true,
                tidb_ast::Expr::Default(Some(path)) => {
                    resolver.resolve(path).is_some_and(|(column, _, _)| {
                        target_identity == (DefaultColumnIdentity { table: 0, column })
                    })
                }
                _ => false,
            };
            if is_own_default {
                continue;
            }
            return Err(DriverError::BadGeneratedColumn {
                column: target_meta.name.clone(),
                table: table_name.to_owned(),
            });
        }

        let value = match &assignment.value {
            tidb_ast::Expr::Default(None) => {
                let datum =
                    materialize_column_default(target_meta, DefaultUse::Expression, ctx, row)?;
                PreparedOnDuplicateValue::Constant(Expression::Constant(
                    tidb_expr::constant::Constant::new(datum, target_meta.field_type.clone()),
                ))
            }
            value => {
                let defaults =
                    prepare_named_defaults(value, ctx, row, DefaultUse::Expression, |path| {
                        let (column, _, _) = resolver.resolve(path).ok_or_else(|| {
                            DriverError::UnknownColumnInClause {
                                column: path.last().cloned().unwrap_or_default(),
                                clause: "field list".to_owned(),
                            }
                        })?;
                        Ok(ResolvedDefaultColumn {
                            identity: DefaultColumnIdentity { table: 0, column },
                            meta: column_meta[column].clone(),
                        })
                    })?;
                PreparedOnDuplicateValue::Ast {
                    value: value.clone(),
                    defaults,
                }
            }
        };
        prepared.push(PreparedOnDuplicateAssignment { offset, value });
    }
    Ok(prepared)
}

/// Go `ON DUPLICATE KEY UPDATE`: applies the assignments to the row already
/// stored, and reports what the statement counts as affected.
///
/// Captured from TiDB: the assignments read the EXISTING row (`c = c + 1` on
/// a stored 10 gives 11, not the rejected value plus one), `VALUES(col)`
/// reads the row that would have been inserted, an update that changes
/// nothing counts 0, and one that changes something counts 2.
pub(crate) fn apply_on_duplicate(
    table: &mut crate::KvTable,
    handle: &crate::kv_table::TableHandle,
    candidate: &[Datum],
    assignments: &[PreparedOnDuplicateAssignment],
    column_list: &[(String, FieldType)],
    row_index: usize,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let Some(existing) = table
        .get_row_by_handle(handle, &ctx.session_zone())
        .map_err(|e| kv_read_error("row read failed", e))?
    else {
        return Ok(0);
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let resolver = TableResolver {
        table_name: "",
        columns: column_list,
        zone: ctx.session_zone(),
    };
    let mut updated = existing.clone();
    for assignment in assignments {
        let expr = match &assignment.value {
            PreparedOnDuplicateValue::Constant(expression) => expression.clone(),
            PreparedOnDuplicateValue::Ast { value, defaults } => {
                // `VALUES(col)` is the value the insert would have written,
                // resolved only after this candidate exists. DEFAULT leaves
                // remain bound to the statement constants prepared earlier.
                let bound = substitute_values_references(value, candidate, column_list)?;
                rewrite_with_prepared_defaults(&bound, &resolver, defaults)?
            }
        };
        let chunk = row_chunk(&updated, &field_types)?;
        let value = expr
            .eval(ctx, chunk.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        updated[assignment.offset] = cast_value_for_assignment(
            value,
            &field_types[assignment.offset],
            &column_list[assignment.offset].0,
            row_index,
            ctx,
        )?;
    }
    if updated == existing {
        // Captured: an update that changes nothing affects no rows.
        return Ok(0);
    }
    table
        .update_row(handle, &updated, ctx)
        .map_err(kv_write_error)?;
    Ok(2)
}

/// Replaces every `VALUES(col)` in an `ON DUPLICATE KEY UPDATE` assignment
/// with the literal the insert would have written for that column.
///
/// Go does not substitute at all: its expression rewriter handles
/// `*ast.ValuesExpr` in `Enter` (`expression_rewriter.go:623`) by pushing a
/// `ScalarFunction` closed over the column OFFSET onto the expression stack,
/// which reads `SessionVars.CurrInsertValues` at eval time. Because that is a
/// stack rewrite driven by the generic AST walk, the position of `VALUES()`
/// inside the assignment is irrelevant -- `a = IFNULL(VALUES(a), 0)` and
/// `a = CASE WHEN VALUES(a) > 0 THEN VALUES(a) ELSE b END` are handled by the
/// same line as `a = VALUES(a)`.
///
/// So this substitution has to be TOTAL over the expression tree, not a
/// hand-listed set of container variants: a per-variant recursion silently
/// left `VALUES()` alive inside every function call, `CASE`, `IN`, `BETWEEN`
/// and subquery, where it then resolved as an unknown function. Riding the
/// package-wide [`tidb_ast::Visitable`] walk -- the same traversal Go's
/// `Node.Accept` gives its rewriter -- removes the variant list entirely.
pub(crate) fn substitute_values_references(
    expr: &tidb_ast::Expr,
    candidate: &[Datum],
    column_list: &[(String, FieldType)],
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Visitable;

    struct Substitute<'a> {
        candidate: &'a [Datum],
        column_list: &'a [(String, FieldType)],
        error: Option<DriverError>,
    }

    impl Substitute<'_> {
        fn value_of(&self, args: &[tidb_ast::Expr]) -> Result<tidb_ast::Expr, DriverError> {
            let Some(tidb_ast::Expr::Column(path)) = args.first() else {
                return Err(DriverError::unsupported("VALUES() takes a column name"));
            };
            let name = path
                .last()
                .ok_or(DriverError::unsupported("VALUES() takes a column name"))?;
            let offset = self
                .column_list
                .iter()
                .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                // Go scopes the same failure to the insert's field list:
                // `plannererrors.ErrUnknownColumn.GenWithStackByArgs(
                // v.Column.Name.OrigColName(), "field list")`.
                .ok_or_else(|| DriverError::UnknownColumnInClause {
                    column: name.clone(),
                    clause: "field list".to_owned(),
                })?;
            datum_to_literal(&self.candidate[offset])
        }
    }

    impl tidb_ast::Visitor for Substitute<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            let tidb_ast::Expr::Func { name, args, .. } = expr else {
                return false;
            };
            if !name.eq_ignore_ascii_case("values") {
                return false;
            }
            match self.value_of(args) {
                Ok(literal) => *expr = literal,
                Err(error) => self.error = Some(error),
            }
            // The arguments of a substituted `VALUES()` are gone with it, and
            // its replacement is a literal: nothing below is left to visit.
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            // Go's `Enter` returns `ok == false` on a rewrite failure, which
            // aborts the whole walk; stopping here is the same abort.
            self.error.is_none()
        }
    }

    let mut rewritten = expr.clone();
    let mut visitor = Substitute {
        candidate,
        column_list,
        error: None,
    };
    rewritten.accept(&mut visitor);
    match visitor.error {
        Some(error) => Err(error),
        None => Ok(rewritten),
    }
}

/// Runs a single-table `UPDATE`, returning MySQL's affected-row count.
///
/// Go `executor.UpdateExec` + `updateRecord`: each row the `WHERE` selects is
/// re-evaluated with the `SET` assignments applied, and a row is written back
/// only when a column actually changed. The affected-row count is the number
/// of CHANGED rows, not the number matched -- an unchanged row is "touched"
/// instead, and only a client that negotiated `CLIENT_FOUND_ROWS` sees it
/// counted (that capability is not modelled here, so the count is always the
/// changed-row count).
///
/// Assignments are evaluated against the row's ORIGINAL values. Go constructs
/// the complete replacement row before it writes any assignment, so one `SET`
/// item cannot observe a previous item's new value.
///
/// Multi-table `UPDATE` lives in `multi_dml`, which reads a joined row
/// source carrying each target's row identity.
///
/// DEFERRED (documented): generated and `ON UPDATE CURRENT_TIMESTAMP`
/// columns. A changed primary-key handle moves the row and rewrites its
/// secondary-index entries. Single-table
/// `ORDER BY`/`LIMIT` is supported (see `order_rows_for_dml`,
/// `dml_row_limit`).
pub fn run_update_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_update_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_update_on`] resolving unqualified names in `current_db`.
pub fn run_update_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = ctx.parse(sql)?;
    let update = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(update) => update,
            _ => return Err(DriverError::unsupported("only UPDATE is supported here")),
        },
        _ => return Err(DriverError::unsupported("only UPDATE is supported here")),
    };
    run_update_stmt(update, catalog, current_db, ctx)
}

/// [`run_update_in`]'s body, taking the already-parsed AST directly so
/// `explain::explain_analyze_update_stmt` (which already holds a parsed
/// `UpdateStmt` from the `EXPLAIN ANALYZE` wrapper) can execute the SAME
/// write path real `EXPLAIN ANALYZE UPDATE` runs, rather than re-deriving
/// it or re-parsing the statement text (which `explain`'s callers do not
/// keep around).
pub(crate) fn run_update_stmt(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_update_traced(update, catalog, current_db, ctx, None)
}

/// [`run_update_stmt`], recording the plan it builds into `trace`.
///
/// The read plan is the one this function performs -- the `Point_Get`,
/// `TableRangeScan` or `TableFullScan` [`super::access::write_read_path`]
/// chose, with a `Selection` above it for the `WHERE`. Its `actRows` are
/// counted off the very read and predicate the update runs. The one access
/// path a write is still never offered is a non-unique INDEX; see `explain`'s
/// divergence 8.
pub(crate) fn run_update_traced(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<u64, DriverError> {
    let zone = ctx.session_zone();
    // A `RETURNING` clause is parsed and silently ignored, matching Go: the
    // planner and executor never read `UpdateStmt.Returning`. `StmtContext`
    // already carries `IgnoreErr`, so casts, bad NULLs, and other value-level
    // write checks use the same warning-and-coercion path as INSERT IGNORE.
    let table_ref = match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => table_ref,
        // A multi-table write reads a joined row source that carries every
        // target's row identity, which is a different read path -- see
        // `multi_dml`'s module doc. `EXPLAIN` has never described it.
        tidb_ast::UpdateKind::Multi { from, .. } => {
            if let Some(trace) = trace.as_deref_mut() {
                trace.refuse("multi-table UPDATE plans are not supported yet");
                if trace.is_plan_only() {
                    return Ok(0);
                }
            }
            return super::multi_dml::run_multi_update(update, from, catalog, current_db, ctx);
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let table = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::unsupported("unknown table"))?;
    if !table_ref.partitions.is_empty() && !matches!(table, TableEntry::Kv(_)) {
        return Err(DriverError::UnknownPartition {
            partition: table_ref.partitions[0].clone(),
            table: name.clone(),
        });
    }
    let column_list = table.column_list();
    let column_meta = column_metadata(table);

    // An alias REPLACES the table name as the only usable qualifier, in the
    // SET list as much as the WHERE: `UPDATE u AS x SET x.v = 1` resolves and
    // `SET u.v = 1` is Go's unknown-column error. Resolving both sides through
    // the one resolver is what makes those two cases the same case.
    let resolver = TableResolver {
        table_name: table_ref.alias.as_deref().unwrap_or(&name),
        columns: &column_list,
        zone: ctx.session_zone(),
    };
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let (offset, _, _) = resolver
            .resolve(&assignment.col)
            .ok_or(DriverError::unsupported("unknown column in SET"))?;
        // Go `IsDefaultExprSameColumn`: a generated target accepts only bare
        // DEFAULT or DEFAULT(the same resolved column). DEFAULT(other), even
        // though it has the same AST variant, is error 3105.
        if column_meta[offset].generated {
            let own_default = match &assignment.value {
                tidb_ast::Expr::Default(None) => true,
                tidb_ast::Expr::Default(Some(path)) => resolver
                    .resolve(path)
                    .is_some_and(|(source, _, _)| source == offset),
                _ => false,
            };
            if !own_default {
                return Err(DriverError::BadGeneratedColumn {
                    column: column_meta[offset].name.clone(),
                    table: name.clone(),
                });
            }
            // The generation expression remains the value source.
            continue;
        }
        assignments.push((offset, assignment.value.clone()));
    }
    let dml_scope = dml_table_scope(table_ref, &database, &name, column_list.clone(), ctx);
    let predicate = match &update.where_clause {
        Some(expr) => Some(DmlExpression::build(
            expr,
            dml_scope.clone(),
            catalog,
            current_db,
            ctx,
        )?),
        None => None,
    };
    let default_row = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let mut set_exprs = Vec::with_capacity(assignments.len());
    for (offset, value) in &assignments {
        let expression = match value {
            // Go fills a bare update DEFAULT with the target column name and
            // resolves it through GetColDefaultValue while building the
            // assignment. Do the same once here, before row iteration, so a
            // computed default and any warning are statement-scoped.
            tidb_ast::Expr::Default(None) => {
                let value = materialize_column_default(
                    &column_meta[*offset],
                    DefaultUse::Expression,
                    ctx,
                    default_row.get_row(0),
                )?;
                UpdateExpression::scalar(Expression::Constant(tidb_expr::constant::Constant::new(
                    value,
                    column_meta[*offset].field_type.clone(),
                )))
            }
            _ => {
                let defaults = prepare_named_defaults(
                    value,
                    ctx,
                    default_row.get_row(0),
                    DefaultUse::Expression,
                    |path| {
                        let (column, _, _) = resolver.resolve(path).ok_or_else(|| {
                            DriverError::UnknownColumnInClause {
                                column: path.last().cloned().unwrap_or_default(),
                                clause: "field list".to_owned(),
                            }
                        })?;
                        Ok(ResolvedDefaultColumn {
                            identity: DefaultColumnIdentity { table: 0, column },
                            meta: column_meta[column].clone(),
                        })
                    },
                )?;
                if expr_has_subquery(value) {
                    UpdateExpression::applied(DmlExpression::build_with_prepared_defaults(
                        value,
                        dml_scope.clone(),
                        catalog,
                        current_db,
                        ctx,
                        &defaults,
                    )?)
                } else {
                    UpdateExpression::scalar(rewrite_with_prepared_defaults(
                        value, &resolver, &defaults,
                    )?)
                }
            }
        };
        set_exprs.push((*offset, expression));
    }

    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
    let row_limit = dml_row_limit(&update.limit)?;
    // The records this write FETCHES: one key when the `WHERE` pins a whole
    // key, otherwise the handle intervals it implies. See
    // `access::write_read_path` for the Go functions this mirrors and for why
    // neither narrowing can change which rows the statement acts on.
    let read_path = super::access::write_read_path(
        catalog,
        &database,
        &name,
        &super::access::PointPlanStmt::of_write(
            update.where_clause.as_ref(),
            &update.order_by,
            update.limit.as_ref(),
        ),
        ctx,
    )?;
    if let Some(trace) = trace.as_deref_mut() {
        trace_dml_source(
            trace,
            catalog,
            DmlTarget {
                table_ref,
                database: &database,
                name: &name,
            },
            &column_list,
            &update.where_clause,
            read_path.as_ref(),
            current_db,
        );
        trace.write("Update", true);
        if trace.is_plan_only() {
            return Ok(0);
        }
    }
    enum SourceRows {
        Mem(Vec<Vec<Datum>>),
        Kv {
            rows: Vec<(crate::kv_table::TableHandle, Vec<Datum>)>,
            partition_ids: Option<Vec<i64>>,
        },
    }

    // Finish the physical read before evaluating the predicate. The Apply's
    // inner query needs an immutable view of the complete statement snapshot,
    // including the target table itself, and no write is applied until every
    // replacement has been staged.
    let source_rows = {
        let entry = catalog
            .get_mut_in(&database, &name)
            .ok_or(DriverError::unsupported("unknown table"))?;
        match entry {
            TableEntry::Cte(_) | TableEntry::View(_) => {
                return Err(DriverError::TableNotUpdatable(name.clone()))
            }
            TableEntry::Sequence(_) => {
                return Err(DriverError::unsupported(
                    "UPDATE of a sequence is not a statement TiDB accepts",
                ))
            }
            TableEntry::Mem(mem) => SourceRows::Mem(mem.rows.clone()),
            TableEntry::Kv(kv) => {
                let partition_ids = if table_ref.partitions.is_empty() {
                    None
                } else {
                    let Some(spec) = kv.partition() else {
                        return Err(DriverError::UnknownPartition {
                            partition: table_ref.partitions[0].clone(),
                            table: name.clone(),
                        });
                    };
                    Some(
                        crate::partition_pruning::ids_for_selected_partitions(
                            spec,
                            &table_ref.partitions,
                        )
                        .map_err(|partition| {
                            DriverError::UnknownPartition {
                                partition,
                                table: name.clone(),
                            }
                        })?,
                    )
                };
                let mut source = restricted_to_partitions(kv, &table_ref.partitions, &name)?;
                let mut rows = fetch_write_rows(&mut source, read_path.as_ref(), &zone)?;
                order_rows_for_dml(
                    &mut rows,
                    &update.order_by,
                    &field_types,
                    &resolver,
                    &column_names,
                    ctx,
                )?;
                SourceRows::Kv {
                    rows,
                    partition_ids,
                }
            }
        }
    };

    let scanned = match &source_rows {
        SourceRows::Mem(rows) => rows.len() as u64,
        SourceRows::Kv { rows, .. } => rows.len() as u64,
    };
    let mut matched = 0u64;
    let mut changed = 0u64;
    let mut rewrites: Vec<(crate::kv_table::TableHandle, Vec<Datum>, Vec<Datum>)> = Vec::new();
    let row_evaluator = UpdateRowEvaluator {
        field_types: &field_types,
        column_names: &column_names,
        predicate: &predicate,
        set_exprs: &set_exprs,
        catalog,
        current_db,
        ctx,
    };
    match source_rows {
        SourceRows::Mem(rows) => {
            let mut updates = Vec::new();
            for (index, row) in rows.iter().enumerate() {
                if row_limit.is_some_and(|cap| matched >= cap) {
                    break;
                }
                if let Some(new_row) = row_evaluator.compute(row, &mut matched)? {
                    updates.push((index, new_row));
                }
            }
            changed = updates.len() as u64;
            let Some(TableEntry::Mem(mem)) = catalog.get_mut_in(&database, &name) else {
                unreachable!("the update source kind cannot change within one statement")
            };
            for (index, new_row) in updates {
                mem.rows[index] = new_row;
            }
        }
        SourceRows::Kv {
            rows,
            partition_ids,
        } => {
            let accountant = ctx
                .statement_memory()
                .write_accountant(mem_quota::label::UPDATE);
            let Some(TableEntry::Kv(kv)) = catalog.get_in(&database, &name) else {
                unreachable!("the update source kind cannot change within one statement")
            };
            for (handle, row) in rows {
                accountant.account_row(&row).map_err(DriverError::from)?;
                if row_limit.is_some_and(|cap| matched >= cap) {
                    break;
                }
                if let Some(mut new_row) = row_evaluator.compute(&row, &mut matched)? {
                    kv.materialize_generated(&mut new_row, ctx)
                        .map_err(kv_write_error)?;
                    if let Some(partitions) = &partition_ids {
                        kv.validate_update_partitions(&row, &new_row, partitions, ctx)
                            .map_err(kv_write_error)?;
                    }
                    accountant
                        .account_row(&new_row)
                        .map_err(DriverError::from)?;
                    rewrites.push((handle, row, new_row));
                }
            }
        }
    }
    if !rewrites.is_empty() {
        if update.ignore {
            // Go's IGNORE path resolves each write in statement order. A key
            // moved by an earlier row is consequently free for a later row,
            // while any row that still conflicts becomes one warning and no
            // write. Checking all staged replacements against the original
            // table would reject that valid ordered move.
            for (handle, old_row, new_row) in rewrites {
                let duplicate = {
                    let Some(TableEntry::Kv(kv)) = catalog.get_mut_in(&database, &name) else {
                        unreachable!("only a byte-backed table stages rewrites")
                    };
                    kv.conflicting_handles(&new_row, &zone)
                        .map_err(|error| kv_read_error("conflict lookup failed", error))?
                        .iter()
                        .any(|conflict| conflict != &handle)
                };
                if duplicate {
                    let Some(TableEntry::Kv(kv)) = catalog.get_mut_in(&database, &name) else {
                        unreachable!("only a byte-backed table stages rewrites")
                    };
                    let error = kv
                        .duplicate_entry_error(&new_row, &zone)
                        .map_err(|error| kv_read_error("conflict lookup failed", error))?;
                    let warning = kv_write_error(error).to_mysql_error();
                    ctx.append_warning_parts(warning.code, &warning.message);
                    continue;
                }
                if ctx.foreign_key_checks() {
                    let rows = [new_row.clone()];
                    if let Err(error) = crate::foreign_key::require_child_rows(
                        catalog, &database, &name, &rows, &zone,
                    ) {
                        let warning = error.to_mysql_error();
                        ctx.append_warning_parts(warning.code, &warning.message);
                        continue;
                    }
                    let changes = [crate::foreign_key::ParentChange::Update {
                        old: &old_row,
                        new: &new_row,
                    }];
                    if let Err(error) = crate::foreign_key::cascade_parent_changes(
                        catalog, &database, &name, &changes, ctx,
                    ) {
                        let warning = error.to_mysql_error();
                        ctx.append_warning_parts(warning.code, &warning.message);
                        continue;
                    }
                }
                let Some(TableEntry::Kv(kv)) = catalog.get_mut_in(&database, &name) else {
                    unreachable!("only a byte-backed table stages rewrites")
                };
                match kv.update_row(&handle, &new_row, ctx) {
                    Ok(()) => changed += 1,
                    Err(crate::kv_table::KvTableError::DuplicateEntry { value, key }) => {
                        let warning = DriverError::DuplicateEntry { value, key }.to_mysql_error();
                        ctx.append_warning_parts(warning.code, &warning.message);
                    }
                    Err(error) => return Err(kv_write_error(error)),
                }
            }
        } else {
            if ctx.foreign_key_checks() {
                let new_rows: Vec<Vec<Datum>> = rewrites
                    .iter()
                    .map(|(_, _, new_row)| new_row.clone())
                    .collect();
                crate::foreign_key::require_child_rows(
                    catalog, &database, &name, &new_rows, &zone,
                )?;
                let changes: Vec<crate::foreign_key::ParentChange<'_>> = rewrites
                    .iter()
                    .map(
                        |(_, old, new_row)| crate::foreign_key::ParentChange::Update {
                            old,
                            new: new_row,
                        },
                    )
                    .collect();
                crate::foreign_key::cascade_parent_changes(
                    catalog, &database, &name, &changes, ctx,
                )?;
            }
            let Some(TableEntry::Kv(kv)) = catalog.get_mut_in(&database, &name) else {
                unreachable!("only a byte-backed table stages rewrites")
            };
            for (handle, _, new_row) in &rewrites {
                kv.update_row(handle, new_row, ctx)
                    .map_err(kv_write_error)?;
                changed += 1;
            }
        }
    }
    if let Some(trace) = trace {
        trace.set_dml_source_act_rows(scanned, matched, update.where_clause.is_some());
    }
    Ok(changed)
}

struct UpdateRowEvaluator<'a> {
    field_types: &'a [FieldType],
    column_names: &'a [String],
    predicate: &'a Option<DmlExpression>,
    set_exprs: &'a [(usize, UpdateExpression)],
    catalog: &'a Catalog,
    current_db: &'a str,
    ctx: &'a crate::StmtContext,
}

impl UpdateRowEvaluator<'_> {
    /// Applies the `SET` assignments to one row, returning the new row only
    /// when the `WHERE` selected it AND a column actually changed (Go's
    /// `changed` flag).
    fn compute(&self, row: &[Datum], matched: &mut u64) -> Result<Option<Vec<Datum>>, DriverError> {
        let chunk = row_chunk(row, self.field_types)?;
        if let Some(predicate) = self.predicate {
            let selected = predicate.eval(row, self.catalog, self.current_db, self.ctx)?;
            if !datum_is_true(&selected) {
                return Ok(None);
            }
        }
        // The `WHERE` selected this row. That is what a `Selection`'s `actRows`
        // counts -- not the narrower `changed` below, which Go reports as
        // `affected` rather than as rows the filter passed.
        *matched += 1;
        // Every assignment reads the row as the statement found it, so
        // `SET a = 100, b = a` stores the ORIGINAL `a` in `b`, and
        // `SET c = a, a = b, b = c` rotates the three original values in one step.
        // Go builds the whole new row from the old one before writing any of it
        // (`executor.UpdateExec` composes `newRowData` off the fetched row), which
        // is why an earlier assignment is invisible to a later one. Evaluating
        // against the single unmodified `chunk` makes that the only reading there
        // is, rather than a rule the loop has to re-establish per assignment.
        let mut new_row = row.to_vec();
        for (offset, expr) in self.set_exprs {
            let value = expr.eval(
                row,
                chunk.get_row(0),
                self.catalog,
                self.current_db,
                self.ctx,
            )?;
            // Go casts an assigned value to its column's type here too, which is
            // what stores `SET d = 9.87654` in a DECIMAL(10,3) column as 9.877.
            new_row[*offset] = cast_value_for_update_assignment(
                value,
                &self.field_types[*offset],
                &self.column_names[*offset],
                0,
                self.ctx,
            )?;
        }
        // Go `updateRecord`'s step 5 runs `HandleBadNull` over EVERY column of
        // the new row, before the row is compared with the old one -- which is
        // why a row whose NULL is replaced by the same zero it already held is
        // still counted as unchanged AND still warns. `ErrGroupBadNull` for an
        // UPDATE is an error exactly under strict mode (`ResetUpdateStmtCtx`).
        // Zipped rather than indexed because the row can be WIDER than the column
        // list: an expression index appends a hidden generated column to the
        // stored row that `column_list` does not name. Go loops `t.Cols()`, which
        // is the visible columns, so stopping where the names stop IS the rule
        // and not a bounds guard.
        let level = crate::bad_null::NullLevel::from_is_error(self.ctx.strict());
        for ((value, field_type), name) in new_row
            .iter_mut()
            .zip(self.field_types.iter())
            .zip(self.column_names.iter())
        {
            crate::bad_null::handle_bad_null(value, field_type, name, level, self.ctx)?;
        }
        if new_row == row {
            // Go counts this row as touched, not affected.
            return Ok(None);
        }
        Ok(Some(new_row))
    }
}

/// Runs a single-table `DELETE`, returning the number of removed rows.
///
/// Go `executor.DeleteExec`: every row the `WHERE` selects is removed, and the
/// affected-row count is simply that count.
///
/// `DELETE IGNORE` runs as a plain `DELETE`: Go's `IGNORE` downgrades a
/// per-row failure to a skipped row plus a warning, and the only per-row
/// failure a `DELETE` can raise is a foreign-key restriction, which this
/// engine does not model at all -- so with nothing to downgrade the two
/// spellings really are one statement here. Captured from Go: without a
/// referencing child row, `DELETE IGNORE` and `DELETE` remove the same rows
/// and report the same count. Multi-table `DELETE` lives in `multi_dml`.
///
/// DEFERRED (documented): `QUICK`. Single-table
/// `ORDER BY`/`LIMIT` IS supported (see `order_rows_for_dml`,
/// `dml_row_limit`). A `RETURNING` clause is parsed and silently ignored,
/// matching Go, where the planner and executor never read
/// `DeleteStmt.Returning`.
pub fn run_delete_on(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_delete_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_delete_on`] resolving unqualified names in `current_db`.
pub fn run_delete_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let stmt = ctx.parse(sql)?;
    let delete = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Delete(delete) => delete,
            _ => return Err(DriverError::unsupported("only DELETE is supported here")),
        },
        _ => return Err(DriverError::unsupported("only DELETE is supported here")),
    };
    run_delete_stmt(delete, catalog, current_db, ctx)
}

/// [`run_delete_in`]'s body, taking the already-parsed AST directly -- see
/// [`run_update_stmt`]'s doc for why `explain::explain_analyze_delete_stmt`
/// needs this split.
pub(crate) fn run_delete_stmt(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    run_delete_traced(delete, catalog, current_db, ctx, None)
}

/// [`run_delete_stmt`], recording the plan it builds into `trace` -- see
/// [`run_update_traced`] for the read plan's shape and where its `actRows`
/// come from.
pub(crate) fn run_delete_traced(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<u64, DriverError> {
    let zone = ctx.session_zone();
    // `DELETE IGNORE` differs from a plain `DELETE` only in what it does with
    // a referential violation: Go downgrades it from a statement error to a
    // per-row skip with a warning. `QUICK` is an index-maintenance hint with
    // no visible behaviour, and is still refused rather than ignored.
    if delete.quick {
        return Err(DriverError::unsupported(
            "DELETE QUICK is not supported yet",
        ));
    }
    let table_ref = match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => table_ref,
        // See `multi_dml`'s module doc; `EXPLAIN` has never described this.
        tidb_ast::DeleteKind::Multi { targets, from, .. } => {
            if let Some(trace) = trace.as_deref_mut() {
                trace.refuse("multi-table DELETE plans are not supported yet");
                if trace.is_plan_only() {
                    return Ok(0);
                }
            }
            return super::multi_dml::run_multi_delete(
                delete, targets, from, catalog, current_db, ctx,
            );
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::unsupported("unknown table"))?
        .column_list();
    if !table_ref.partitions.is_empty()
        && !matches!(catalog.get_in(&database, &name), Some(TableEntry::Kv(_)))
    {
        return Err(DriverError::UnknownPartition {
            partition: table_ref.partitions[0].clone(),
            table: name.clone(),
        });
    }
    // As in UPDATE: `DELETE FROM u AS y WHERE y.id = 1` resolves and
    // `WHERE u.id = 1` does not.
    let resolver = TableResolver {
        table_name: table_ref.alias.as_deref().unwrap_or(&name),
        columns: &column_list,
        zone: ctx.session_zone(),
    };
    let predicate = match &delete.where_clause {
        Some(expr) => Some(DmlExpression::build(
            expr,
            dml_table_scope(table_ref, &database, &name, column_list.clone(), ctx),
            catalog,
            current_db,
            ctx,
        )?),
        None => None,
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
    let row_limit = dml_row_limit(&delete.limit)?;
    // As in UPDATE: the key or the handle intervals the `WHERE` implies are
    // the records this write fetches.
    let read_path = super::access::write_read_path(
        catalog,
        &database,
        &name,
        &super::access::PointPlanStmt::of_write(
            delete.where_clause.as_ref(),
            &delete.order_by,
            delete.limit.as_ref(),
        ),
        ctx,
    )?;
    if let Some(trace) = trace.as_deref_mut() {
        trace_dml_source(
            trace,
            catalog,
            DmlTarget {
                table_ref,
                database: &database,
                name: &name,
            },
            &column_list,
            &delete.where_clause,
            read_path.as_ref(),
            current_db,
        );
        trace.write("Delete", true);
        if trace.is_plan_only() {
            return Ok(0);
        }
    }
    enum SourceRows {
        Mem(Vec<Vec<Datum>>),
        Kv(Vec<(crate::kv_table::TableHandle, Vec<Datum>)>),
    }
    let source_rows = {
        let entry = catalog
            .get_mut_in(&database, &name)
            .ok_or(DriverError::unsupported("unknown table"))?;
        match entry {
            TableEntry::Cte(_) | TableEntry::View(_) => {
                return Err(DriverError::DeleteViewUnsupported(name.clone()))
            }
            TableEntry::Sequence(_) => {
                return Err(DriverError::DeleteSequenceUnsupported(name.clone()))
            }
            TableEntry::Mem(mem) => SourceRows::Mem(mem.rows.clone()),
            TableEntry::Kv(kv) => {
                let mut source = restricted_to_partitions(kv, &table_ref.partitions, &name)?;
                let mut rows = fetch_write_rows(&mut source, read_path.as_ref(), &zone)?;
                order_rows_for_dml(
                    &mut rows,
                    &delete.order_by,
                    &field_types,
                    &resolver,
                    &column_names,
                    ctx,
                )?;
                SourceRows::Kv(rows)
            }
        }
    };
    let scanned = match &source_rows {
        SourceRows::Mem(rows) => rows.len() as u64,
        SourceRows::Kv(rows) => rows.len() as u64,
    };
    let mut deleted = 0u64;
    let mut doomed: Vec<(crate::kv_table::TableHandle, Vec<Datum>)> = Vec::new();
    match source_rows {
        SourceRows::Mem(rows) => {
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows {
                if dml_row_is_selected(&row, &predicate, catalog, current_db, ctx)? {
                    deleted += 1;
                } else {
                    kept.push(row);
                }
            }
            let Some(TableEntry::Mem(mem)) = catalog.get_mut_in(&database, &name) else {
                unreachable!("the delete source kind cannot change within one statement")
            };
            mem.rows = kept;
        }
        SourceRows::Kv(rows) => {
            // Go `DeleteExec.deleteSingleTableByChunk`: the child's chunk is
            // consumed as it arrives, which is why a `DELETE` over a table
            // too large for the quota is cancelled by the DELETE and not by
            // the read below it. Here the rows are already materialized, so
            // the equivalent is per row, inside the loop.
            let accountant = ctx
                .statement_memory()
                .write_accountant(mem_quota::label::DELETE);
            // Selected first, deleted after: the parent-side cascade below
            // needs the table released, because it writes the DEPENDENT
            // tables the statement never named.
            for (handle, row) in rows {
                accountant.account_row(&row).map_err(DriverError::from)?;
                // Go's LIMIT caps the rows DELETED, not the rows examined.
                if row_limit.is_some_and(|cap| doomed.len() as u64 >= cap) {
                    break;
                }
                if dml_row_is_selected(&row, &predicate, catalog, current_db, ctx)? {
                    doomed.push((handle, row));
                }
            }
        }
    }
    if !doomed.is_empty() {
        if ctx.foreign_key_checks() {
            // Under IGNORE each row stands or falls alone, so the cascade
            // runs per row and a restricted row is dropped from the
            // statement with a warning instead of failing it.
            if delete.ignore {
                let mut surviving = Vec::with_capacity(doomed.len());
                for (handle, row) in doomed {
                    let changes = [crate::foreign_key::ParentChange::Delete(&row)];
                    match crate::foreign_key::cascade_parent_changes(
                        catalog, &database, &name, &changes, ctx,
                    ) {
                        Ok(()) => surviving.push((handle, row.clone())),
                        Err(error) => {
                            let warning = error.to_mysql_error();
                            ctx.append_warning_parts(warning.code, &warning.message);
                        }
                    }
                }
                doomed = surviving;
            } else {
                let changes: Vec<crate::foreign_key::ParentChange<'_>> = doomed
                    .iter()
                    .map(|(_, row)| crate::foreign_key::ParentChange::Delete(row))
                    .collect();
                crate::foreign_key::cascade_parent_changes(
                    catalog, &database, &name, &changes, ctx,
                )?;
            }
        }
        let Some(TableEntry::Kv(kv)) = catalog.get_mut_in(&database, &name) else {
            unreachable!("only a byte-backed table stages deletions")
        };
        for (handle, _) in &doomed {
            kv.delete_row(handle, &zone)
                .map_err(|e| kv_read_error("row delete failed", e))?;
            deleted += 1;
        }
    }
    if let Some(trace) = trace {
        // Every selected row IS deleted, so the delete count is also the
        // number of rows the `WHERE` passed.
        trace.set_dml_source_act_rows(scanned, deleted, delete.where_clause.is_some());
    }
    Ok(deleted)
}

/// Fetches the records a single-table write will filter, through the read
/// path chosen for it.
///
/// One function for `UPDATE` and `DELETE` both, because the two statements
/// differ in what they do with a record and not in how they find one -- and
/// because a second copy of this dispatch is exactly how a write path comes
/// to read a different record set than the plan it printed.
///
/// A point get reads ONE key. `get_row_by_handle` is the same read
/// `HandleSourceExec` performs for a `SELECT`'s `Point_Get`, and it answers
/// `None` for a key no record carries -- Go's point get that finds nothing.
fn fetch_write_rows(
    kv: &mut crate::kv_table::KvTable,
    read_path: Option<&super::access::WriteReadPath>,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Vec<(crate::kv_table::TableHandle, Vec<Datum>)>, DriverError> {
    let decode_failed = |e| DriverError::Parse(format!("row decode failed: {e:?}"));
    match read_path {
        Some(super::access::WriteReadPath::Point(handle)) => {
            let Some(handle) = handle else {
                return Ok(Vec::new());
            };
            Ok(kv
                .get_row_by_handle(handle, zone)
                .map_err(decode_failed)?
                .map(|row| vec![(handle.clone(), row)])
                .unwrap_or_default())
        }
        Some(super::access::WriteReadPath::Ranges(ranges, _)) => kv
            .scan_rows_with_handles_in(Some(ranges), zone)
            .map_err(decode_failed),
        Some(super::access::WriteReadPath::IndexRanges(index_id, ranges, _)) => {
            // The index range narrows WHICH records are fetched, in index
            // order; the row is then read by its handle, and the `WHERE` above
            // still filters. Ranges over one index cover disjoint key intervals,
            // so a handle is read at most once.
            let mut rows = Vec::new();
            for range in ranges {
                for handle in kv
                    .scan_index_range(*index_id, range, zone)
                    .map_err(decode_failed)?
                {
                    if let Some(row) = kv.get_row_by_handle(&handle, zone).map_err(decode_failed)? {
                        rows.push((handle, row));
                    }
                }
            }
            Ok(rows)
        }
        None => kv
            .scan_rows_with_handles_in(None, zone)
            .map_err(decode_failed),
    }
}

/// Records the read plan a single-table write performs to find its target
/// rows: the read `access::write_read_path` chose -- a `Point_Get`, a
/// `TableRangeScan`, or the full scan neither narrowed -- with a `Selection`
/// above it for the `WHERE` (`explain`'s divergences 7 and 8).
///
/// The table a single-table write reads, as the statement names it.
struct DmlTarget<'a> {
    /// The `FROM`-side reference, which carries the alias `EXPLAIN` prints.
    table_ref: &'a tidb_ast::TableRef,
    /// The schema the name resolved in.
    database: &'a str,
    /// The stored table name.
    name: &'a str,
}

/// Renames the scan `trace_dml_source` just recorded to the `IndexRangeScan`
/// (or `IndexFullScan`) the write reads through, matching what the read side's
/// `commit_index_range_source` prints for the same index and ranges.
#[allow(clippy::too_many_arguments)]
fn trace_write_index_scan(
    trace: &mut PlanTrace,
    catalog: &Catalog,
    database: &str,
    name: &str,
    visible: &str,
    index_id: i64,
    ranges: &[crate::kv_table::IndexRange],
    estimate: crate::access_cost::ScanEstimate,
) {
    let Some(super::catalog::TableEntry::Kv(table)) = catalog.get_in(database, name) else {
        return;
    };
    let Some(index) = table.indexes().iter().find(|index| index.id == index_id) else {
        return;
    };
    let index_columns: Vec<String> = index
        .column_offsets
        .iter()
        .map(|offset| super::access::index_key_part_name(table, *offset))
        .collect();
    let index_columns: Vec<&str> = index_columns.iter().map(String::as_str).collect();
    if ranges.len() == 1 && ranges[0].is_full() {
        trace.index_full_scan(visible, &index.name, &index_columns, estimate, false);
    } else {
        trace.index_range_scan(visible, &index.name, &index_columns, ranges, estimate);
    }
}

fn trace_dml_source(
    trace: &mut PlanTrace,
    catalog: &Catalog,
    target: DmlTarget<'_>,
    columns: &[(String, FieldType)],
    where_clause: &Option<tidb_ast::Expr>,
    read_path: Option<&super::access::WriteReadPath>,
    current_db: &str,
) {
    let DmlTarget {
        table_ref,
        database,
        name,
    } = target;
    let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
    let (estimate, selectivity) = single_table_trace_estimate(
        catalog,
        database,
        name,
        &visible,
        columns,
        where_clause.as_ref(),
    );
    trace.table_full_scan(&visible, estimate, false);
    // The same two rewrites the read side performs, from the same chooser: a
    // range scan RENAMES the scan just recorded, because the write really
    // does run that scan over only those ranges; a point get REPLACES it,
    // because the write reads by key and runs no scan at all.
    match read_path {
        Some(super::access::WriteReadPath::Ranges(ranges, range_estimate)) => {
            trace.table_range_scan(&visible, ranges, *range_estimate);
        }
        Some(super::access::WriteReadPath::IndexRanges(index_id, ranges, range_estimate)) => {
            trace_write_index_scan(
                trace,
                catalog,
                database,
                name,
                &visible,
                *index_id,
                ranges,
                *range_estimate,
            );
        }
        Some(super::access::WriteReadPath::Point(handle)) => {
            trace.point_get(&visible, handle.as_ref());
        }
        None => {}
    }
    let Some(predicate) = where_clause else {
        return;
    };
    let scope = PlanTrace::single_table_scope(
        &visible,
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns.to_vec(),
    );
    trace.selection(
        predicate,
        None,
        &Qualifier {
            db: current_db,
            scope: &scope,
        },
        selectivity,
    );
}

/// Whether the `WHERE` predicate (absent = every row) selects this row.
pub(crate) fn row_is_selected(
    row: &[Datum],
    field_types: &[FieldType],
    predicate: &Option<Expression>,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    let chunk = row_chunk(row, field_types)?;
    let selected = predicate
        .eval(ctx, chunk.get_row(0))
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    Ok(datum_is_true(&selected))
}

fn dml_row_is_selected(
    row: &[Datum],
    predicate: &Option<DmlExpression>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    Ok(datum_is_true(
        &predicate.eval(row, catalog, current_db, ctx)?,
    ))
}

/// A one-row chunk holding `row`, so an expression can be evaluated over it.
///
/// `field_types` is the SCHEMA the expression was built against, and it is
/// what decides the chunk's width. A row read straight from storage is wider
/// than that when the table has hidden expression-index columns; those are
/// the TAIL (see `crate::expression_index`), and no expression a statement
/// can write is able to name one, so the visible prefix is exactly the row
/// the expression means.
pub(crate) fn row_chunk(
    row: &[Datum],
    field_types: &[FieldType],
) -> Result<tidb_chunk::chunk::Chunk, DriverError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(field_types, 1);
    for (i, value) in row.iter().take(field_types.len()).enumerate() {
        chunk.append_datum(i, value);
    }
    // A row SHORTER than the schema (a partially built one) still has to
    // present every column, or a reference to a trailing one reads off the
    // end.
    for i in row.len()..field_types.len() {
        chunk.append_datum(i, &Datum::Null);
    }
    Ok(chunk)
}

/// Go's `WHERE` truth test: NULL and zero are false.
pub(crate) fn datum_is_true(value: &Datum) -> bool {
    match value {
        Datum::Null => false,
        Datum::Int(v) => *v != 0,
        Datum::UInt(v) => *v != 0,
        Datum::Real(v) => *v != 0.0,
        other => !matches!(other, Datum::Null),
    }
}
