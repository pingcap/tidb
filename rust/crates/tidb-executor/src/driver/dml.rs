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

/// Parses and runs a plain `INSERT INTO t [(cols)] VALUES (...), ...` against
/// `catalog`, returning the number of inserted rows.
///
/// The write half of the in-memory gateway (the storage-backed `InsertExec`
/// with autoid/defaults/constraints lands with real tables). Unsupported here
/// (rejected, documented): `REPLACE`, `IGNORE`, `ON DUPLICATE KEY UPDATE`,
/// `SET` syntax, `INSERT ... SELECT`, and partitions. A `RETURNING` clause is
/// parsed and silently ignored: Go's hand-written parser stores it on the AST
/// but the planner and executor never read it, so the write runs normally and
/// answers with a plain OK packet (verified against Go with a testkit probe).
/// Columns not
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
) -> Result<(u64, Option<i64>), DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;

    let insert = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => insert,
            _ => return Err(DriverError::Unsupported("only INSERT is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only INSERT is supported here")),
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
) -> Result<(u64, Option<i64>), DriverError> {
    if !insert.partitions.is_empty() || (insert.replace && !insert.on_duplicate.is_empty()) {
        return Err(DriverError::Unsupported("partitions are not supported yet"));
    }

    // `INSERT ... SELECT` runs its source query first, over the catalog as it
    // stands: Go materializes the SelectExec's rows and feeds them to the
    // insert, so a source that reads the target table sees the pre-insert
    // rows. The query runs before the table is borrowed mutably, which is the
    // same ordering.
    let source_rows: Option<Vec<Vec<Datum>>> = match &insert.source {
        Some(query) => Some(match &**query {
            tidb_ast::QueryStmt::Select(select) => {
                run_select_stmt(select, catalog, current_db, ctx)?.1
            }
            tidb_ast::QueryStmt::SetOpr(set_opr) => {
                run_set_opr_stmt(set_opr, catalog, current_db, ctx)?.1
            }
        }),
        None => None,
    };

    let (database, table_name) = split_table_path(&insert.table, current_db)?;
    let (database, table_name) = (database.to_owned(), table_name.to_owned());
    let table = catalog
        .get_mut_in(&database, &table_name)
        .ok_or(DriverError::Unsupported("table not found in catalog"))?;
    // Go refuses a write through a view before planning anything.
    if table.is_view() {
        return Err(DriverError::InsertIntoViewUnsupported(table_name.clone()));
    }
    let column_list = table.column_list();

    // Map an explicit column list to table offsets; without one, values map to
    // every column in order.
    //
    // `INSERT ... SET a = 1, b = 2` is the same statement as
    // `INSERT (a, b) VALUES (1, 2)` -- Go normalizes its `Setlist` into
    // `Columns` + one `Lists` entry, and the parser here does the same, so
    // the assignment columns are simply another way to name the targets.
    let named_columns: Vec<String> = if insert.set_syntax {
        insert
            .set_columns
            .iter()
            .map(|path| path.last().cloned().unwrap_or_default())
            .collect()
    } else {
        insert.columns.clone()
    };
    let target_offsets: Vec<usize> = if insert.set_syntax || insert.columns_specified {
        named_columns
            .iter()
            .map(|name| {
                column_list
                    .iter()
                    .position(|(n, _)| n.eq_ignore_ascii_case(name))
                    .ok_or_else(|| DriverError::UnknownColumnInClause {
                        column: name.clone(),
                        clause: "field list".to_owned(),
                    })
            })
            .collect::<Result<_, _>>()?
    } else {
        (0..column_list.len()).collect()
    };

    // Evaluate each VALUES row (constant expressions over the dual row).
    let eval_chunk = {
        let mut c = tidb_chunk::chunk::Chunk::new_empty(&[]);
        c.set_num_virtual_rows(1);
        c
    };
    // The per-column metadata the default and NOT NULL rules read.
    let column_meta: Vec<(Option<Datum>, bool, String)> = match table {
        TableEntry::Kv(kv) => kv
            .columns
            .iter()
            .map(|c| {
                (
                    c.default_value.clone(),
                    c.field_type.flags() & 1 != 0,
                    c.name.clone(),
                )
            })
            .collect(),
        // A matrix-backed table carries no column metadata, so every column
        // is nullable with no default -- the original mock behavior.
        TableEntry::Mem(mem) => mem
            .columns
            .iter()
            .map(|(name, _)| (None, false, name.clone()))
            .collect(),
        TableEntry::View(_) => unreachable!("INSERT through a view is refused above"),
    };

    let auto_increment_offset = match table {
        TableEntry::Kv(kv) => kv.auto_increment_offset(),
        TableEntry::Mem(_) => None,
        TableEntry::View(_) => unreachable!("INSERT through a view is refused above"),
    };
    let mut auto_rows: Vec<usize> = Vec::new();
    let mut first_allocated: Option<i64> = None;

    let mut inserted = 0u64;
    // A source query supplies already-evaluated values; a VALUES list
    // supplies expressions. Both fill the same target offsets.
    let value_rows: Vec<Vec<Datum>> = match &source_rows {
        Some(rows) => rows.clone(),
        None => Vec::new(),
    };
    let row_count = source_rows.as_ref().map_or(insert.rows.len(), Vec::len);
    let mut new_rows: Vec<Vec<Datum>> = Vec::with_capacity(row_count);
    for index in 0..row_count {
        let width = match source_rows.as_ref() {
            Some(_) => value_rows.get(index).map_or(0, Vec::len),
            None => insert.rows[index].len(),
        };
        if width != target_offsets.len() {
            return Err(DriverError::Unsupported(
                "VALUES arity does not match the column list",
            ));
        }
        let mut row = vec![Datum::Null; column_list.len()];
        let mut assigned = vec![false; column_list.len()];
        for (position, &offset) in target_offsets.iter().enumerate() {
            let value = match source_rows.as_ref() {
                Some(_) => value_rows[index][position].clone(),
                None => {
                    let rewritten =
                        rewrite_expr_resolved(&insert.rows[index][position], &NoResolver)
                            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                    rewritten
                        .eval(ctx, eval_chunk.get_row(0))
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?
                }
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
            if !assigned[offset] || row[offset] == Datum::Null {
                row[offset] = Datum::Int(0);
            }
            assigned[offset] = true;
            auto_rows.push(new_rows.len());
        }
        // Only a column the statement omits takes its default, and only such
        // a column can raise ErrNoDefaultForField (Go `fillColValue`).
        for offset in 0..column_list.len() {
            if !assigned[offset] {
                row[offset] = column_default(&column_meta, offset)?;
            }
        }
        // Go `Column.CheckNotNull`: an explicit NULL in a NOT NULL column is
        // ErrColumnCantNull, which is a different error from omitting a
        // column that has no default.
        for (offset, value) in row.iter().enumerate() {
            if *value == Datum::Null && column_is_not_null(&column_meta, offset) && assigned[offset]
            {
                return Err(DriverError::ColumnCannotBeNull(
                    column_list[offset].0.clone(),
                ));
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
        }
        new_rows.push(row);
        inserted += 1;
    }
    match table {
        TableEntry::View(_) => unreachable!("INSERT through a view is refused above"),
        TableEntry::Mem(mem) => mem.rows.extend(new_rows),
        TableEntry::Kv(kv) => {
            // The allocator lives on the table, so the ids are handed out here
            // rather than while the rows were being built.
            for index in &auto_rows {
                if let Some(allocated) = kv.apply_auto_increment(&mut new_rows[*index]) {
                    // Go keeps the FIRST allocated id of the statement.
                    if first_allocated.is_none() {
                        first_allocated = Some(allocated);
                    }
                }
            }
            // Go resolves a conflict per row, before the row is written:
            // REPLACE deletes every row it collides with, ON DUPLICATE KEY
            // UPDATE applies its assignments to the first one, and IGNORE
            // skips the row with the duplicate reported as a warning.
            inserted = 0;
            for row in &new_rows {
                let conflicts = kv
                    .conflicting_handles(row)
                    .map_err(|e| DriverError::Parse(format!("conflict lookup failed: {e:?}")))?;
                if !conflicts.is_empty() {
                    if insert.replace {
                        // Captured: the affected count is one per deleted row
                        // plus one for the inserted row.
                        for handle in &conflicts {
                            kv.delete_row(handle).map_err(|e| {
                                DriverError::Parse(format!("row delete failed: {e:?}"))
                            })?;
                            inserted += 1;
                        }
                    } else if !insert.on_duplicate.is_empty() {
                        inserted += apply_on_duplicate(
                            kv,
                            &conflicts[0],
                            row,
                            &insert.on_duplicate,
                            &column_list,
                            ctx,
                        )?;
                        continue;
                    } else if insert.ignore {
                        let reported = kv.duplicate_entry_error(row).map_err(|e| {
                            DriverError::Parse(format!("conflict lookup failed: {e:?}"))
                        })?;
                        if let crate::kv_table::KvTableError::DuplicateEntry { value, key } =
                            reported
                        {
                            let warning =
                                DriverError::DuplicateEntry { value, key }.to_mysql_error();
                            ctx.append_warning_parts(warning.code, &warning.message);
                        }
                        continue;
                    }
                }
                kv.insert_row(row).map_err(|e| match e {
                    crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                        DriverError::DuplicateEntry { value, key }
                    }
                    other => DriverError::Parse(format!("row encode failed: {other:?}")),
                })?;
                inserted += 1;
            }
        }
    }
    Ok((inserted, first_allocated))
}

/// Whether a conversion event is one TiDB reports nothing for.
///
/// Rounding a NUMBER into a narrower decimal is the case: captured, both
/// `INSERT INTO t(d DECIMAL(10,3)) VALUES (1.23456)` and
/// `ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159` are accepted in
/// silence, storing 1.235 and 3.14. Go reaches that through
/// `ProduceDecWithSpecifiedTp`, whose rounding notice never becomes a
/// statement error. A STRING source is a different case -- it may not be a
/// number at all -- so it is never silent.
pub(crate) fn conversion_event_is_silent(
    value: &Datum,
    field_type: &FieldType,
    event: &tidb_datatype::ScalarConversionEvent,
) -> bool {
    let numeric_source = matches!(
        value,
        Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Float32(_) | Datum::Decimal(_)
    );
    numeric_source
        && matches!(field_type.eval_type(), tidb_datatype::EvalType::Decimal)
        && matches!(event, tidb_datatype::ScalarConversionEvent::Truncated)
}

/// Go `table.CastValue` + `completeInsertErr`: converts one written value into
/// the column's own type, and names the failure the way the insert path does.
///
/// The strict SQL mode makes a bad value fail the statement; without it the
/// converted (clamped or truncated) value is stored and the same message is a
/// warning, which is what `sql_mode = ''` produces in TiDB.
pub(crate) fn cast_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    if value.is_null() {
        return Ok(value);
    }
    let converted = value
        .convert_to(field_type, ctx.conversion_flags())
        .map_err(|error| {
            json_write_error(&error).unwrap_or(DriverError::IncorrectValue {
                type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                value: datum_error_text(&value),
                column: column.to_owned(),
                row: row_index + 1,
            })
        })?;
    let Some(event) = converted.event else {
        return Ok(converted.value);
    };
    if conversion_event_is_silent(&value, field_type, &event) {
        return Ok(converted.value);
    }
    // Go picks the message from the conversion's own error kind: a string
    // that does not fit is ErrDataTooLong, a number outside the column's
    // range is ErrWarnDataOutOfRange, and anything else is the
    // "Incorrect <type> value" form.
    let error = match event {
        tidb_datatype::ScalarConversionEvent::Overflow(_) => DriverError::DataOutOfRange {
            column: column.to_owned(),
            row: row_index + 1,
        },
        tidb_datatype::ScalarConversionEvent::Truncated
            if matches!(field_type.eval_type(), tidb_datatype::EvalType::String) =>
        {
            DriverError::DataTooLong {
                column: column.to_owned(),
                row: row_index + 1,
            }
        }
        tidb_datatype::ScalarConversionEvent::Truncated => DriverError::IncorrectValue {
            type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
            value: datum_error_text(&value),
            column: column.to_owned(),
            row: row_index + 1,
        },
    };
    if ctx.strict() {
        return Err(error);
    }
    let reported = error.to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(converted.value)
}

/// The `json`-class error a write into a JSON column reports as its own.
///
/// Go's `table.CastValue` returns the error `ParseBinaryJSONFromString`
/// produced unchanged, so a malformed document written into a JSON column is
/// 3140 with the parser's message -- NOT the generic 1366 "Incorrect json
/// value" that every other failed column cast reports. That distinction is
/// SQL-visible: it survives `sql_mode = ''` as an ERROR, because it is the
/// document that cannot exist, not a value that can be clamped.
pub(crate) fn json_write_error(error: &tidb_datatype::DatumValueError) -> Option<DriverError> {
    let tidb_datatype::DatumValueError::Json(error) = error else {
        return None;
    };
    let json = match error {
        tidb_datatype::BinaryJSONError::EmptyDocument => tidb_expr::JsonError::EmptyText,
        _ => tidb_expr::JsonError::InvalidText,
    };
    Some(DriverError::Exec(crate::ExecError::Eval(
        tidb_expr::EvalError::Json(json),
    )))
}

/// A value as MySQL prints it inside a conversion error message.
pub(crate) fn datum_error_text(value: &Datum) -> String {
    match value {
        Datum::Int(v) => v.to_string(),
        Datum::UInt(v) => v.to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Decimal(v) => v.to_string(),
        Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
        other => format!("{other:?}"),
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
                    .ok_or(DriverError::Unsupported("ORDER BY position out of range"))?;
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
            PositionalError::Malformed => DriverError::Unsupported("ORDER BY position"),
            PositionalError::Zero => DriverError::Unsupported("ORDER BY position 0"),
        });
    }
    // `ORDER BY TRUE` reaches the DML tier as a boolean literal rather than
    // digits, and MySQL still reads it as position 1.
    match expr {
        tidb_ast::Expr::Bool(b) => usize::from(*b)
            .checked_sub(1)
            .ok_or(DriverError::Unsupported("ORDER BY position 0"))
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
        return Err(DriverError::Unsupported(
            "an UPDATE/DELETE LIMIT takes no offset",
        ));
    }
    Ok(Some(eval_limit_bound(&limit.count)?))
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
    assignments: &[tidb_ast::Assignment],
    column_list: &[(String, FieldType)],
    ctx: &crate::StmtContext,
) -> Result<u64, DriverError> {
    let Some(existing) = table
        .get_row_by_handle(handle)
        .map_err(|e| DriverError::Parse(format!("row read failed: {e:?}")))?
    else {
        return Ok(0);
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let mut updated = existing.clone();
    for assignment in assignments {
        let name = assignment
            .col
            .last()
            .ok_or(DriverError::Unsupported("empty assignment column"))?;
        let offset = column_list
            .iter()
            .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| DriverError::UnknownColumnInClause {
                column: name.clone(),
                clause: "field list".to_owned(),
            })?;
        // `VALUES(col)` is the value the insert would have written, which Go
        // resolves before evaluating the assignment.
        let bound = substitute_values_references(&assignment.value, candidate, column_list)?;
        let resolver = TableResolver {
            table_name: "",
            columns: column_list,
        };
        let expr = rewrite_expr_resolved(&bound, &resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        let chunk = row_chunk(&updated, &field_types)?;
        let value = expr
            .eval(ctx, chunk.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        updated[offset] =
            cast_value_for_column(value, &field_types[offset], &column_list[offset].0, 0, ctx)?;
    }
    if updated == existing {
        // Captured: an update that changes nothing affects no rows.
        return Ok(0);
    }
    table.update_row(handle, &updated).map_err(|e| match e {
        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
            DriverError::DuplicateEntry { value, key }
        }
        other => DriverError::Parse(format!("row encode failed: {other:?}")),
    })?;
    Ok(2)
}

/// Replaces every `VALUES(col)` in an `ON DUPLICATE KEY UPDATE` assignment
/// with the literal the insert would have written for that column.
pub(crate) fn substitute_values_references(
    expr: &tidb_ast::Expr,
    candidate: &[Datum],
    column_list: &[(String, FieldType)],
) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match expr {
        Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("values") => {
            let Some(Expr::Column(path)) = args.first() else {
                return Err(DriverError::Unsupported("VALUES() takes a column name"));
            };
            let name = path
                .last()
                .ok_or(DriverError::Unsupported("VALUES() takes a column name"))?;
            let offset = column_list
                .iter()
                .position(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .ok_or_else(|| DriverError::UnknownColumnInClause {
                    column: name.clone(),
                    clause: "field list".to_owned(),
                })?;
            datum_to_literal(&candidate[offset])?
        }
        Expr::Paren(inner) => Expr::Paren(Box::new(substitute_values_references(
            inner,
            candidate,
            column_list,
        )?)),
        Expr::Unary(op, inner) => Expr::Unary(
            *op,
            Box::new(substitute_values_references(inner, candidate, column_list)?),
        ),
        Expr::Binary(op, left, right) => Expr::Binary(
            *op,
            Box::new(substitute_values_references(left, candidate, column_list)?),
            Box::new(substitute_values_references(right, candidate, column_list)?),
        ),
        other => other.clone(),
    })
}

/// The value an omitted column takes, following Go `GetColDefaultValue` and
/// `getColDefaultValueFromNil`: the stored `DEFAULT` when one was written, or
/// NULL for a nullable column; a NOT NULL column with no default is Go's
/// `ErrNoDefaultForField` under strict mode.
///
/// DEFERRED (documented): non-strict mode, where Go warns and writes the
/// type's zero value instead of failing. This seed always behaves as strict
/// mode, which is TiDB's default sql_mode.
pub(crate) fn column_default(
    meta: &[(Option<Datum>, bool, String)],
    offset: usize,
) -> Result<Datum, DriverError> {
    let (default_value, not_null, name) = &meta[offset];
    match default_value {
        Some(value) => Ok(value.clone()),
        None if *not_null => Err(DriverError::NoDefaultForField(name.clone())),
        None => Ok(Datum::Null),
    }
}

/// Whether the column at `offset` carries Go's `NotNullFlag`.
pub(crate) fn column_is_not_null(meta: &[(Option<Datum>, bool, String)], offset: usize) -> bool {
    meta[offset].1
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
/// Assignments are evaluated against the row's ORIGINAL values, left to right,
/// with each assignment seeing the effects of the previous ones -- Go's
/// `composeNewRow` order.
///
/// DEFERRED (documented): multi-table UPDATE, `IGNORE`, generated and
/// `ON UPDATE CURRENT_TIMESTAMP` columns, and the handle-changed path (a row
/// whose primary-key handle column is assigned is deleted and re-inserted in
/// Go; this seed rejects it). Single-table `ORDER BY`/`LIMIT` IS supported
/// (see `order_rows_for_dml`, `dml_row_limit`).
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
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let update = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(update) => update,
            _ => return Err(DriverError::Unsupported("only UPDATE is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only UPDATE is supported here")),
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
    // A `RETURNING` clause is parsed and silently ignored, matching Go: the
    // planner and executor never read `UpdateStmt.Returning`.
    if update.ignore {
        return Err(DriverError::Unsupported(
            "UPDATE IGNORE is not supported yet",
        ));
    }
    let table_ref = match &update.kind {
        tidb_ast::UpdateKind::Single(table_ref) => table_ref,
        tidb_ast::UpdateKind::Multi { .. } => {
            return Err(DriverError::Unsupported(
                "multi-table UPDATE is not supported yet",
            ))
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?
        .column_list();

    // SET targets, as offsets into the row.
    let mut assignments = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let column = assignment
            .col
            .last()
            .ok_or(DriverError::Unsupported("empty assignment target"))?;
        let offset = column_list
            .iter()
            .position(|(candidate, _)| candidate.eq_ignore_ascii_case(column))
            .ok_or(DriverError::Unsupported("unknown column in SET"))?;
        assignments.push((offset, assignment.value.clone()));
    }

    let resolver = TableResolver {
        table_name: &name,
        columns: &column_list,
    };
    let predicate = match &update.where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let mut set_exprs = Vec::with_capacity(assignments.len());
    for (offset, value) in &assignments {
        set_exprs.push((
            *offset,
            rewrite_expr_resolved(value, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ));
    }

    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
    let row_limit = dml_row_limit(&update.limit)?;
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut changed = 0u64;
    match entry {
        // Go's planner rejects an UPDATE whose target is a view.
        TableEntry::View(_) => return Err(DriverError::TableNotUpdatable(name.clone())),
        TableEntry::Mem(mem) => {
            let mut updates = Vec::new();
            for (index, row) in mem.rows.iter().enumerate() {
                if let Some(new_row) = compute_updated_row(
                    row,
                    &field_types,
                    &column_names,
                    &predicate,
                    &set_exprs,
                    ctx,
                )? {
                    updates.push((index, new_row));
                }
            }
            changed = updates.len() as u64;
            for (index, new_row) in updates {
                mem.rows[index] = new_row;
            }
        }
        TableEntry::Kv(kv) => {
            let mut rows = kv
                .scan_rows_with_handles()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            order_rows_for_dml(
                &mut rows,
                &update.order_by,
                &field_types,
                &resolver,
                &column_names,
                ctx,
            )?;
            for (handle, row) in rows {
                if row_limit.is_some_and(|cap| changed >= cap) {
                    break;
                }
                if let Some(new_row) = compute_updated_row(
                    &row,
                    &field_types,
                    &column_names,
                    &predicate,
                    &set_exprs,
                    ctx,
                )? {
                    kv.update_row(&handle, &new_row).map_err(|e| match e {
                        crate::kv_table::KvTableError::DuplicateEntry { value, key } => {
                            DriverError::DuplicateEntry { value, key }
                        }
                        other => DriverError::Parse(format!("row encode failed: {other:?}")),
                    })?;
                    changed += 1;
                }
            }
        }
    }
    Ok(changed)
}

/// Applies the `SET` assignments to one row, returning the new row only when
/// the `WHERE` selected it AND a column actually changed (Go's `changed` flag).
pub(crate) fn compute_updated_row(
    row: &[Datum],
    field_types: &[FieldType],
    column_names: &[String],
    predicate: &Option<Expression>,
    set_exprs: &[(usize, Expression)],
    ctx: &crate::StmtContext,
) -> Result<Option<Vec<Datum>>, DriverError> {
    let chunk = row_chunk(row, field_types)?;
    if let Some(predicate) = predicate {
        let selected = predicate
            .eval(ctx, chunk.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        if !datum_is_true(&selected) {
            return Ok(None);
        }
    }
    let mut new_row = row.to_vec();
    for (offset, expr) in set_exprs {
        // Go evaluates each assignment over the row as the previous
        // assignments left it, so `SET a = 1, b = a` sees the new `a`.
        let source = row_chunk(&new_row, field_types)?;
        let value = expr
            .eval(ctx, source.get_row(0))
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        // Go casts an assigned value to its column's type here too, which is
        // what stores `SET d = 9.87654` in a DECIMAL(10,3) column as 9.877.
        new_row[*offset] =
            cast_value_for_column(value, &field_types[*offset], &column_names[*offset], 0, ctx)?;
    }
    if new_row == row {
        // Go counts this row as touched, not affected.
        return Ok(None);
    }
    Ok(Some(new_row))
}

/// Runs a single-table `DELETE`, returning the number of removed rows.
///
/// Go `executor.DeleteExec`: every row the `WHERE` selects is removed, and the
/// affected-row count is simply that count.
///
/// DEFERRED (documented): multi-table DELETE, `IGNORE`. Single-table
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
    let stmt = tidb_parser::parse(sql).map_err(|e| DriverError::Parse(format!("{e:?}")))?;
    let delete = match &stmt {
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Delete(delete) => delete,
            _ => return Err(DriverError::Unsupported("only DELETE is supported here")),
        },
        _ => return Err(DriverError::Unsupported("only DELETE is supported here")),
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
    if delete.ignore || delete.quick {
        return Err(DriverError::Unsupported(
            "only plain DELETE FROM t [WHERE ...] is supported",
        ));
    }
    let table_ref = match &delete.kind {
        tidb_ast::DeleteKind::Single(table_ref) => table_ref,
        tidb_ast::DeleteKind::Multi { .. } => {
            return Err(DriverError::Unsupported(
                "multi-table DELETE is not supported yet",
            ))
        }
    };
    let (database, name) = single_table_name(table_ref, current_db)?;
    let column_list = catalog
        .get_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?
        .column_list();
    let resolver = TableResolver {
        table_name: &name,
        columns: &column_list,
    };
    let predicate = match &delete.where_clause {
        Some(expr) => Some(
            rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?,
        ),
        None => None,
    };
    let field_types: Vec<FieldType> = column_list.iter().map(|(_, ft)| ft.clone()).collect();
    let column_names: Vec<String> = column_list.iter().map(|(name, _)| name.clone()).collect();
    let row_limit = dml_row_limit(&delete.limit)?;
    let entry = catalog
        .get_mut_in(&database, &name)
        .ok_or(DriverError::Unsupported("unknown table"))?;

    let mut deleted = 0u64;
    match entry {
        TableEntry::View(_) => return Err(DriverError::DeleteViewUnsupported(name.clone())),
        TableEntry::Mem(mem) => {
            let mut kept = Vec::with_capacity(mem.rows.len());
            for row in std::mem::take(&mut mem.rows) {
                if row_is_selected(&row, &field_types, &predicate, ctx)? {
                    deleted += 1;
                } else {
                    kept.push(row);
                }
            }
            mem.rows = kept;
        }
        TableEntry::Kv(kv) => {
            let mut rows = kv
                .scan_rows_with_handles()
                .map_err(|e| DriverError::Parse(format!("row decode failed: {e:?}")))?;
            order_rows_for_dml(
                &mut rows,
                &delete.order_by,
                &field_types,
                &resolver,
                &column_names,
                ctx,
            )?;
            for (handle, row) in rows {
                // Go's LIMIT caps the rows DELETED, not the rows examined.
                if row_limit.is_some_and(|cap| deleted >= cap) {
                    break;
                }
                if row_is_selected(&row, &field_types, &predicate, ctx)? {
                    kv.delete_row(&handle)
                        .map_err(|e| DriverError::Parse(format!("row delete failed: {e:?}")))?;
                    deleted += 1;
                }
            }
        }
    }
    Ok(deleted)
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

/// A one-row chunk holding `row`, so an expression can be evaluated over it.
pub(crate) fn row_chunk(
    row: &[Datum],
    field_types: &[FieldType],
) -> Result<tidb_chunk::chunk::Chunk, DriverError> {
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(field_types, 1);
    for (i, value) in row.iter().enumerate() {
        chunk.append_datum(i, value);
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
