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

//! Go `LOAD DATA`: the executor builder's checks and `NewLoadDataWorker`
//! (`pkg/executor/load_data.go`), the parts of
//! `pkg/executor/importer/import.go` it uses (`NewPlanFromLoadDataPlan`,
//! `checkFieldParams`, the field mappings and insert columns,
//! `generateCSVConfig`, `HandleSkipNRows`), and the encode/commit loop.
//!
//! [`prepare_load_data`] is everything Go decides before `LoadDataExec.Open`
//! asks the client for its file; [`run_load_data`] is `Next` over the
//! file's bytes. Rows are written by the INSERT executor
//! ([`crate::driver::run_load_data_insert`]), in Go's batches of 1000, inside
//! the statement's own transaction.

pub mod csv_parser;

use tidb_ast::{ColumnOrUserVar, LoadDataOnDuplicate, LoadDataStmt};
use tidb_datatype::{Collation, Datum, FieldTypeCode};

use crate::{Catalog, DriverError, SchemaErrorKind, TableEntry};
use csv_parser::{CharsetConvertor, CsvConfig, CsvError, CsvParser};
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// Go `maxRowsInBatch` for LOAD DATA's `InsertValues`.
const MAX_ROWS_IN_BATCH: usize = 1000;

/// Session state Go's LOAD DATA reads.
#[derive(Clone, Debug)]
pub struct LoadDataEnv {
    /// `character_set_database`, the default `CHARACTER SET`.
    pub charset_database: String,
    /// Whether the SQL mode is strict.
    pub strict_sql_mode: bool,
    /// Go `SessionVars.AllowWriteRowID`.
    pub allow_write_row_id: bool,
}

/// One input field's destination (Go `importer.FieldMapping`).
#[derive(Clone, Debug)]
enum FieldMapping {
    Column {
        /// The column's offset in the table's columns.
        offset: usize,
    },
    UserVar(String),
}

/// What Go's planner and executor builder settled for one `LOAD DATA`.
#[derive(Clone, Debug)]
pub struct PreparedLoadData {
    database: String,
    table: String,
    path: String,
    local: bool,
    on_duplicate: LoadDataOnDuplicate,
    restrictive: bool,
    charset: Option<String>,
    csv: CsvConfig,
    ignore_lines: u64,
    field_mappings: Vec<FieldMapping>,
    insert_columns: Vec<String>,
    assignments: Vec<tidb_ast::Assignment>,
}

impl PreparedLoadData {
    /// The `INFILE` path the client is asked for.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

fn wrong_format_config_by_args(detail: &str) -> DriverError {
    // `ErrLoadDataWrongFormatConfig`'s message is empty, so Go's
    // `GenWithStackByArgs` renders its argument as an extra.
    DriverError::DdlCoded {
        errno: 8162,
        message: format!("%!(EXTRA string={detail})"),
    }
}

fn wrong_format_config(detail: String) -> DriverError {
    DriverError::DdlCoded {
        errno: 8162,
        message: detail,
    }
}

fn plain_error(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(crate::MysqlError::new(1105, message.into()))
}

/// Go `buildLoadData`, `executorBuilder.buildLoadData` and
/// `NewLoadDataWorker`, up to the point `LoadDataExec.Open` requests the
/// file. `check_privileges` is the planner's visit-info check, run after the
/// table resolves and before the executor builder's own checks.
pub fn prepare_load_data(
    load: &LoadDataStmt,
    catalog: &Catalog,
    current_db: &str,
    env: &LoadDataEnv,
    check_privileges: impl FnOnce(&str, &str) -> Result<(), DriverError>,
) -> Result<PreparedLoadData, DriverError> {
    let (database, name) = match load.table.as_slice() {
        [database, name] => (database.clone(), name.clone()),
        [_] if current_db.is_empty() => {
            return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
        }
        [name] => (current_db.to_owned(), name.clone()),
        _ => return Err(DriverError::unsupported("LOAD DATA names a table path")),
    };
    let Some(entry) = catalog.table_in(&database, &name) else {
        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
            "{database}.{name}"
        ))));
    };
    check_privileges(&database, &name)?;
    let TableEntry::Kv(table) = entry else {
        return Err(DriverError::NonUpdatableTable {
            table: name,
            statement: "LOAD",
        });
    };

    // Go `NewPlanFromLoadDataPlan`.
    let charset = load
        .charset
        .clone()
        .or_else(|| Some(env.charset_database.clone()));
    let restrictive = env.strict_sql_mode && load.on_duplicate != LoadDataOnDuplicate::Ignore;
    // Go `NewLineFieldsInfo`.
    let fields_terminated_by = load
        .fields
        .terminated
        .clone()
        .unwrap_or_else(|| "\t".to_owned());
    let fields_enclosed_by = load.fields.enclosed.clone().unwrap_or_default();
    let fields_escaped_by = load
        .fields
        .escaped
        .clone()
        .unwrap_or_else(|| "\\".to_owned());
    let lines_starting_by = load.lines.starting.clone().unwrap_or_default();
    let lines_terminated_by = load
        .lines
        .terminated
        .clone()
        .unwrap_or_else(|| "\n".to_owned());
    let mut null_defs: Vec<Vec<u8>> = Vec::new();
    let mut null_value_opt_enclosed = false;
    if let Some(defined) = &load.fields.defined_null_by {
        null_defs.push(defined.as_bytes().to_vec());
        null_value_opt_enclosed = load.fields.null_optionally_enclosed;
    } else if !fields_enclosed_by.is_empty() {
        null_defs.push(b"NULL".to_vec());
    }
    if !fields_escaped_by.is_empty() {
        null_defs.push(vec![fields_escaped_by.as_bytes()[0], b'N']);
    }

    // Go `LoadDataController.checkFieldParams`.
    if load.path.is_empty() {
        return Err(DriverError::DdlCoded {
            errno: 8156,
            message: "The value of INFILE must not be empty when LOAD DATA from LOCAL".to_owned(),
        });
    }
    if null_value_opt_enclosed && fields_enclosed_by.is_empty() {
        return Err(wrong_format_config_by_args(
            "must specify FIELDS [OPTIONALLY] ENCLOSED BY when use NULL DEFINED BY OPTIONALLY ENCLOSED",
        ));
    }
    if lines_terminated_by.is_empty() {
        return Err(wrong_format_config_by_args("LINES TERMINATED BY is empty"));
    }
    if fields_terminated_by.is_empty() {
        return Err(wrong_format_config_by_args(
            "load data with empty field terminator",
        ));
    }
    if !fields_enclosed_by.is_empty()
        && (fields_enclosed_by.starts_with(&fields_terminated_by)
            || fields_terminated_by.starts_with(&fields_enclosed_by))
    {
        return Err(wrong_format_config_by_args(
            "FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other",
        ));
    }

    // Go `buildFieldMappings`: an unknown column maps to nothing here and
    // fails in `buildInsertColumns`.
    let visible = table.visible_columns();
    let find_visible = |wanted: &str| {
        visible
            .iter()
            .position(|column| column.name.go_equal_fold(wanted))
    };
    let mut field_mappings = Vec::new();
    let mut column_names: Vec<String> = Vec::new();
    if load.columns_and_user_vars.is_empty() {
        for (offset, column) in visible.iter().enumerate() {
            field_mappings.push(FieldMapping::Column { offset });
            column_names.push(column.name.clone());
        }
    } else {
        for item in &load.columns_and_user_vars {
            match item {
                ColumnOrUserVar::Column(name) => {
                    if let Some(offset) = find_visible(name) {
                        field_mappings.push(FieldMapping::Column { offset });
                    } else {
                        field_mappings.push(FieldMapping::UserVar(String::new()));
                    }
                    column_names.push(name.clone());
                }
                ColumnOrUserVar::UserVar(name) => {
                    field_mappings.push(FieldMapping::UserVar(name.clone()));
                }
            }
        }
    }
    // Go `buildInsertColumns`: the SET columns join only when the mapped
    // columns are not already the whole visible table.
    if column_names.len() != visible.len() {
        for assignment in &load.column_assignments {
            column_names.push(assignment.col.last().cloned().unwrap_or_default());
        }
    }
    let pk_is_handle = table.pk_handle_offset().is_some();
    let mut seen: Vec<String> = Vec::with_capacity(column_names.len());
    for name in &column_names {
        // Go `table.FindCols`: `_tidb_rowid` is the extra handle column of a
        // table without an integer primary-key handle.
        let found = find_visible(name)
            .map(|offset| visible[offset].name.clone())
            .or_else(|| {
                (!pk_is_handle && name.eq_ignore_ascii_case("_tidb_rowid"))
                    .then(|| "_tidb_rowid".to_owned())
            });
        let Some(found) = found else {
            return Err(DriverError::UnknownColumnInClause {
                column: name.clone(),
                clause: "field list".to_owned(),
            });
        };
        // Go `table.CheckOnce`.
        if seen.iter().any(|earlier| earlier.go_lower_eq(&found)) {
            return Err(DriverError::DdlCoded {
                errno: 1110,
                message: format!("Column '{found}' specified twice"),
            });
        }
        seen.push(found);
    }

    // Go `generateCSVConfig` for LOAD DATA.
    let csv = CsvConfig {
        fields_terminated_by: fields_terminated_by.into_bytes(),
        fields_enclosed_by: fields_enclosed_by.into_bytes(),
        lines_terminated_by: lines_terminated_by.into_bytes(),
        not_null: false,
        field_null_defined_by: null_defs,
        trim_last_empty_field: false,
        fields_escaped_by: fields_escaped_by.into_bytes(),
        lines_starting_by: lines_starting_by.into_bytes(),
        allow_empty_line: true,
        quoted_null_is_text: !null_value_opt_enclosed,
        unescaped_quote: true,
    };
    Ok(PreparedLoadData {
        database,
        table: name,
        path: load.path.clone(),
        local: load.local,
        on_duplicate: load.on_duplicate,
        restrictive,
        charset,
        csv,
        ignore_lines: load.ignore_lines.unwrap_or(0),
        field_mappings,
        insert_columns: seen,
        assignments: load.column_assignments.clone(),
    })
}

/// Go `mydump.Compression` in declaration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Compression {
    None,
    Gz,
    Lz4,
    Zstd,
    Xz,
    Lzo,
    Snappy,
}

/// Go `ParseCompressionOnFileExtension`.
fn compression_of(path: &str) -> Compression {
    let extension = std::path::Path::new(path)
        .extension()
        .map(|extension| extension.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    match extension.trim() {
        "gz" | "gzip" => Compression::Gz,
        "lz4" => Compression::Lz4,
        "zstd" | "zst" => Compression::Zstd,
        "xz" => Compression::Xz,
        "lzo" => Compression::Lzo,
        "snappy" => Compression::Snappy,
        _ => Compression::None,
    }
}

/// Go `LoadLocal`'s reader: `ToStorageCompressType` and the decompressing
/// reader over the client's bytes.
fn decompress(path: &str, data: &[u8]) -> Result<Vec<u8>, DriverError> {
    use std::io::Read;
    let compression = compression_of(path);
    let read_error = |error: std::io::Error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            plain_error("unexpected EOF")
        } else {
            plain_error(error.to_string())
        }
    };
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Gz => {
            let mut out = Vec::new();
            flate2::read::MultiGzDecoder::new(data)
                .read_to_end(&mut out)
                .map_err(read_error)?;
            Ok(out)
        }
        Compression::Zstd => {
            let mut out = Vec::new();
            zstd::stream::read::Decoder::new(data)
                .and_then(|mut decoder| decoder.read_to_end(&mut out))
                .map_err(read_error)?;
            Ok(out)
        }
        Compression::Snappy => {
            let mut out = Vec::new();
            snap::read::FrameDecoder::new(data)
                .read_to_end(&mut out)
                .map_err(read_error)?;
            Ok(out)
        }
        Compression::Lz4 | Compression::Xz | Compression::Lzo => Err(plain_error(format!(
            "compression {} doesn't have related storage compressType",
            compression as u8
        ))),
    }
}

fn read_failure(error: &CsvError) -> DriverError {
    DriverError::DdlCoded {
        errno: 8160,
        message: format!(
            "Failed to read source files. Reason: {error}. Only the following formats delimited text file (csv, tsv), parquet, sql are supported. Please provide the valid source file(s)"
        ),
    }
}

/// Go `LoadDataExec.Next` for a client file: decodes `data`, encodes every
/// row and writes it through the INSERT executor, then sets Go's
/// `Records: ...  Deleted: ...  Skipped: ...  Warnings: ...` message.
/// Answers the affected rows and the first allocated id.
pub fn run_load_data(
    prepared: &PreparedLoadData,
    data: &[u8],
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    env: &LoadDataEnv,
) -> Result<(u64, Option<u64>), DriverError> {
    let _ = current_db;
    // Go `createInsertValues`.
    if prepared
        .insert_columns
        .iter()
        .any(|column| column.go_lower_eq("_tidb_rowid"))
        && !env.allow_write_row_id
    {
        return Err(plain_error(
            "load data statement for _tidb_rowid are not supported",
        ));
    }
    let TableEntry::Kv(table) = catalog
        .table_in(&prepared.database, &prepared.table)
        .ok_or_else(|| {
            DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                "{}.{}",
                prepared.database, prepared.table
            )))
        })?
    else {
        return Err(DriverError::NonUpdatableTable {
            table: prepared.table.clone(),
            statement: "LOAD",
        });
    };
    let columns = table.columns().to_vec();

    // Go `createColAssignExprs`: built once against no schema.
    let resolver = crate::driver::TableResolver {
        database: None,
        table_name: "",
        columns: &[],
        constant_context: ctx.clone(),
        zone: ctx.session_zone(),
        no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
        div_precision_increment: ctx.div_precision_increment(),
        clause_message: "expression",
    };
    let mut assignment_exprs = Vec::with_capacity(prepared.assignments.len());
    for assignment in &prepared.assignments {
        let expr = tidb_expr::rewriter::rewrite_expr_resolved(&assignment.value, &resolver)
            .map_err(|error| match error {
                tidb_expr::EvalError::UnknownColumn(column) => DriverError::UnknownColumnInClause {
                    column: column.go_to_lower(),
                    clause: "expression".to_owned(),
                },
                other => DriverError::Exec(crate::ExecError::Eval(other)),
            })?;
        assignment_exprs.push(expr);
    }

    // Go `GetParser` / `newLoadDataParser`.
    let source = decompress(&prepared.path, data)?;
    let convertor = match &prepared.charset {
        Some(charset) => {
            Some(CharsetConvertor::new(charset, "\u{FFFD}").map_err(wrong_format_config)?)
        }
        None => None,
    };
    let mut parser = CsvParser::new(&prepared.csv, source, convertor)
        .map_err(|error| wrong_format_config(error.to_string()))?;
    // Go `HandleSkipNRows`.
    for _ in 0..prepared.ignore_lines {
        match parser.read_until_terminator() {
            Ok(()) => {}
            Err(CsvError::Eof) => break,
            Err(error) => return Err(plain_error(error.to_string())),
        }
    }

    let eval_chunk = {
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        chunk
    };
    let user_vars = ctx.user_vars().cloned();
    let field_count = prepared.field_mappings.len();
    let restrictive = prepared.restrictive;
    let mut row_count: usize = 0;
    let mut at_eof = false;
    // Go `encodeWorker.parserData2TableData` for the next file row.
    let mut encode_next = |row_count: &mut usize| -> Result<Option<Vec<Datum>>, DriverError> {
        let parsed = match parser.read_row() {
            Ok(parsed) => parsed,
            Err(CsvError::Eof) => return Ok(None),
            Err(error) => return Err(read_failure(&error)),
        };
        *row_count += 1;
        let row_number = *row_count;
        let mismatch = if parsed.len() < field_count {
            Some((
                1261,
                format!("Row {row_number} doesn't contain data for all columns"),
            ))
        } else if parsed.len() > field_count {
            Some((
                1262,
                format!(
                    "Row {row_number} was truncated; it contained more data than there were input columns"
                ),
            ))
        } else {
            None
        };
        if let Some((errno, message)) = mismatch {
            if restrictive {
                return Err(DriverError::DdlCoded { errno, message });
            }
            ctx.append_warning_parts(errno, &message);
        }
        let set_var = |name: &str, value: Option<&Datum>| {
            if let Some(vars) = &user_vars {
                let name = name.to_lowercase();
                match value {
                    Some(value) if !value.is_null() => vars.set_user_var_val(&name, value.clone()),
                    _ => vars.unset_user_var(&name),
                }
            }
        };
        let mut row = Vec::with_capacity(field_count + assignment_exprs.len());
        for (index, mapping) in prepared.field_mappings.iter().enumerate() {
            let value = parsed.get(index);
            match mapping {
                FieldMapping::UserVar(name) => set_var(name, value),
                FieldMapping::Column { offset } => {
                    let column = &columns[*offset];
                    match value {
                        // A missing NOT NULL time column is the current time.
                        None if matches!(
                            column.field_type.code(),
                            FieldTypeCode::Date
                                | FieldTypeCode::Datetime
                                | FieldTypeCode::Timestamp
                        ) && column
                            .field_type
                            .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) =>
                        {
                            row.push(Datum::new_collation_string(
                                chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                Collation::Utf8Mb4Bin,
                            ));
                        }
                        None => row.push(Datum::Null),
                        // A generated column's value is never the file's.
                        Some(_) if column.generated.is_some() => row.push(Datum::Null),
                        Some(value) => row.push(value.clone()),
                    }
                }
            }
        }
        for expr in &assignment_exprs {
            match expr.eval(ctx, eval_chunk.get_row(0)) {
                Ok(value) => row.push(value),
                Err(error) => {
                    let error = DriverError::Exec(crate::ExecError::Eval(error));
                    if restrictive {
                        return Err(error);
                    }
                    let rendered = error.to_mysql_error();
                    ctx.append_warning_parts(rendered.code, &rendered.message);
                    row.push(Datum::Null);
                }
            }
        }
        // `getRow` reads only the insert columns.
        row.truncate(prepared.insert_columns.len());
        Ok(Some(row))
    };

    let insert = tidb_ast::InsertStmt {
        hints: Vec::new(),
        priority: tidb_ast::StatementPriority::None,
        ignore: prepared.on_duplicate == LoadDataOnDuplicate::Ignore,
        table: vec![prepared.database.clone(), prepared.table.clone()],
        partitions: Vec::new(),
        columns: prepared.insert_columns.clone(),
        columns_specified: true,
        set_columns: Vec::new(),
        rows: Vec::new(),
        source: None,
        source_parenthesized: false,
        on_duplicate: Vec::new(),
        row_alias: None,
        column_aliases: Vec::new(),
        returning: Default::default(),
        set_syntax: false,
        replace: prepared.on_duplicate == LoadDataOnDuplicate::Replace,
    };
    let mut totals = crate::driver::LoadDataCounts::default();
    let mut first_allocated = None;
    // Go `processOneStream` / `commitWork`: batches of `maxRowsInBatch`.
    while !at_eof {
        let first_row = row_count;
        let mut in_batch = 0;
        let mut next_row = || -> Result<Option<Vec<Datum>>, DriverError> {
            if in_batch == MAX_ROWS_IN_BATCH {
                return Ok(None);
            }
            match encode_next(&mut row_count)? {
                Some(row) => {
                    in_batch += 1;
                    Ok(Some(row))
                }
                None => {
                    at_eof = true;
                    Ok(None)
                }
            }
        };
        let (counts, allocated) = crate::driver::run_load_data_insert(
            &insert,
            crate::driver::LoadDataRows {
                next_row: &mut next_row,
                restrictive,
                first_row,
            },
            catalog,
            &prepared.database,
            ctx,
        )?;
        totals.records += counts.records;
        totals.deleted += counts.deleted;
        totals.copied += counts.copied;
        totals.affected += counts.affected;
        first_allocated = first_allocated.or(allocated);
        if counts.records == 0 {
            break;
        }
    }

    // Go `LoadDataWorker.setResult`.
    let warnings = (ctx.warning_count() as u64).min(u64::from(u16::MAX));
    ctx.set_message(format!(
        "Records: {}  Deleted: {}  Skipped: {}  Warnings: {}",
        totals.records,
        totals.deleted,
        totals.records.saturating_sub(totals.copied),
        warnings
    ));
    let _ = prepared.local;
    Ok((totals.affected, first_allocated))
}
