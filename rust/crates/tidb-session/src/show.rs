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

//! The `SHOW` family: every `AdminStmt::Show*` (and `DESCRIBE`/`KILL`-adjacent
//! `ShowCreate`/`ShowColumns`/`ShowTables`) arm of
//! [`crate::Session::apply_schema_statement`], reached through
//! [`Session::dispatch_admin_stmt`], plus the `SHOW CREATE TABLE`/`VIEW` text
//! builders and column-description rows those arms share with
//! `run_information_schema_select` in `lib.rs`.
//!
//! `EXPLAIN`, account management (`CREATE`/`ALTER`/`DROP`/`RENAME USER`,
//! `GRANT`/`REVOKE`/`SHOW GRANTS`), and `KILL`/processlist live in their own
//! modules (`explain_arm`, `account`, `process_arm`) and are only delegated
//! to from [`Session::dispatch_admin_stmt`] here.

use crate::show_index::{show_index_rows, SHOW_INDEX_COLUMNS};
use crate::*;
use tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH;
use tidb_util::stringutil::go_to_lower;

/// The `Type` cell of a `SHOW COLUMNS`/`DESCRIBE` row: Go `NewColDesc`'s
/// `col.GetTypeDesc()`.
fn type_desc_cell(field_type: &tidb_datatype::FieldType) -> Datum {
    Datum::Bytes(
        field_type
            .type_desc(STRICT_INTEGER_DISPLAY_WIDTH)
            .into_bytes(),
    )
}

/// The text of a column's SETTLED `DEFAULT`, as every surface that prints one
/// renders it.
///
/// Go stores the default as a string and each printer -- `pkg/executor/show.go`
/// for both `SHOW CREATE TABLE` and `SHOW COLUMNS`, and
/// `pkg/infoschema`'s `COLUMN_DEFAULT` -- carries the SAME `TypeBit` branch:
/// a `BIT` column's stored bytes print through
/// `BinaryLiteral.ToBitLiteralString(true)`, so `DEFAULT 250` and
/// `DEFAULT b'11111010'` both read back as `b'11111010'`. Every other type
/// prints its stored text.
pub(crate) fn column_default_text(
    value: &Datum,
    field_type: &tidb_datatype::FieldType,
) -> Option<String> {
    if field_type.code() == tidb_datatype::FieldTypeCode::Bit {
        return match value {
            Datum::Null => None,
            Datum::String(_)
            | Datum::Bytes(_)
            | Datum::Raw(_)
            | Datum::BinaryLiteral(_)
            | Datum::Bit(_) => Some(
                tidb_datatype::BinaryLiteral::from(value.go_bytes()).to_bit_literal_string(true),
            ),
            // Anything else never settled into the column's own domain;
            // rendering it as bits would invent a value it does not hold.
            other => datum_text(other),
        };
    }
    datum_text(value)
}

/// The SQL-visible text of one LITERAL column default.
///
/// Go stores a version-1-and-later `TIMESTAMP` default as a UTC wall clock,
/// then `GetColDefaultValue` projects it into the reading session before any
/// metadata surface prints it. Version 0 uses the system zone as the source;
/// [`tidb_executor::column_default::materialize_stored_literal`] owns that
/// version boundary. Other types do not pass through this cast here: their
/// stored metadata spelling is the spelling SHOW and INFORMATION_SCHEMA
/// report.
pub(crate) fn literal_column_default_text(
    value: &Datum,
    column: &tidb_executor::KvColumn,
    flags: tidb_datatype::ConversionFlags,
    session_zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<String>, tidb_datatype::DatumValueError> {
    if column.field_type.code() != tidb_datatype::FieldTypeCode::Timestamp {
        return Ok(column_default_text(value, &column.field_type));
    }
    let converted = tidb_executor::column_default::materialize_stored_literal(
        value,
        &column.field_type,
        column.column_info_version,
        flags,
        session_zone,
    )?;
    Ok(match converted.value {
        Datum::Time(time) => Some(time.to_string()),
        other => column_default_text(&other, &column.field_type),
    })
}

#[path = "show_create.rs"]
mod create;
use create::{show_create_table_text, show_create_view_text, table_type_of};

#[path = "show_statistics.rs"]
mod statistics;

/// Go `NewColDesc`'s `Collation` cell: the column's own collation name, and
/// NULL for anything with no character set -- a numeric or temporal column,
/// and a binary-charset string column alike (captured: `varbinary`, `binary`,
/// `blob` and `longblob` all report NULL).
fn column_collation_cell(field_type: &tidb_datatype::FieldType) -> Datum {
    if field_type.has_charset() {
        Datum::Bytes(field_type.collation_name().as_bytes().to_vec())
    } else {
        Datum::Null
    }
}

/// Go `table.ColDescFieldNames(false)`: the columns `SHOW COLUMNS` and
/// `DESCRIBE` produce.
const COL_DESC_FIELD_NAMES: &[&str] = &["Field", "Type", "Null", "Key", "Default", "Extra"];

/// Go `table.ColDescFieldNames(true)`: the extra columns `SHOW FULL COLUMNS`
/// inserts between `Type` and `Null`, plus the trailing `Privileges` and
/// `Comment` columns.
const FULL_COL_DESC_FIELD_NAMES: &[&str] = &[
    "Field",
    "Type",
    "Collation",
    "Null",
    "Key",
    "Default",
    "Extra",
    "Privileges",
    "Comment",
];

/// Go's mock session's fixed grant string for every column of every table
/// (`fetchShowColumns`): this tier grants no per-column privileges of its
/// own, so it reports the same static capture MySQL/TiDB print for a column
/// the current user can select, insert, update, and reference.
const FULL_COL_DESC_PRIVILEGES: &str = "select,insert,update,references";

/// Go `table.NewColDesc`, restricted to the facts this seed's metadata holds.
///
/// `Null` is NO when the column carries `NotNullFlag`; `Key` is PRI for a
/// primary-key column, UNI for a column that is the whole of a unique index,
/// and MUL for one that leads a non-unique index -- Go reads those from the
/// column's key flags, which the DDL sets from the same index definitions.
///
/// `Default` is the column's stored `DEFAULT`, or NULL when none was written.
///
/// `Extra` follows Go `NewColDesc`'s ordered precedence: auto increment,
/// ON UPDATE CURRENT_TIMESTAMP, generated-column kind, then expression
/// default.
fn column_description(
    column: &tidb_executor::KvColumn,
    offset: usize,
    table: &tidb_executor::KvTable,
    full: bool,
    ctx: &tidb_executor::StmtContext,
) -> Result<Vec<Datum>, DriverError> {
    let null_flag = if column.field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    let extra = column_extra(
        &column.field_type,
        table.auto_increment_offset() == Some(offset),
        column.generated.as_ref().map(|generated| generated.stored),
        column
            .default_value
            .as_ref()
            .is_some_and(tidb_executor::column_default::ColumnDefault::is_default_generated),
    );
    let key_flag = column_key_flag(table, offset);
    let default = match &column.default_value {
        Some(tidb_executor::column_default::ColumnDefault::Value(value)) => {
            match literal_column_default_text(
                value,
                column,
                ctx.show_default_conversion_flags(),
                &ctx.session_zone(),
            )
            .map_err(|_| DriverError::FieldGetDefaultFailed(column.name.clone()))?
            {
                Some(text) => Datum::Bytes(text.into_bytes()),
                None => Datum::Null,
            }
        }
        // Go `NewColDesc` reports a computed default's STORED string here,
        // which is not the parenthesised form `SHOW CREATE TABLE` prints.
        Some(computed) => match computed.column_desc_text(&column.field_type) {
            Some(text) => Datum::Bytes(text.into_bytes()),
            None => Datum::Null,
        },
        None => Datum::Null,
    };
    if !full {
        return Ok(vec![
            Datum::Bytes(column.name.clone().into_bytes()),
            type_desc_cell(&column.field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.into_bytes()),
            default,
            Datum::Bytes(extra.clone().into_bytes()),
        ]);
    }
    let collation = column_collation_cell(&column.field_type);
    Ok(vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        type_desc_cell(&column.field_type),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.into_bytes()),
        default,
        Datum::Bytes(extra.into_bytes()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        // Go `table.NewColDesc`'s `Comment`, from `ColumnInfo.Comment`.
        Datum::Bytes(column.comment.clone().into_bytes()),
    ])
}

/// Go `table.NewColDesc`'s `Extra` field, kept as a pure formatter so every
/// metadata producer supplies the same four source facts in the same order.
pub(crate) fn column_extra(
    field_type: &tidb_datatype::FieldType,
    auto_increment: bool,
    generated_stored: Option<bool>,
    default_is_expr: bool,
) -> String {
    if auto_increment {
        return "auto_increment".to_owned();
    }
    if field_type.has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW) {
        let fsp = field_type.decimal();
        return if fsp > 0 {
            format!("DEFAULT_GENERATED on update CURRENT_TIMESTAMP({fsp})")
        } else {
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP".to_owned()
        };
    }
    if let Some(stored) = generated_stored {
        return if stored {
            "STORED GENERATED".to_owned()
        } else {
            "VIRTUAL GENERATED".to_owned()
        };
    }
    if default_is_expr {
        return "DEFAULT_GENERATED".to_owned();
    }
    String::new()
}

/// A view column's `SHOW COLUMNS` row.
///
/// A view carries no storage metadata, so Go reports no key, no default and
/// no extra for every one of its columns; only the name, the type the body
/// produced, and nullability come from the definition.
///
/// # Why this disagrees with `information_schema.columns`, on purpose
///
/// Go's `tryFillViewColumnType` (`pkg/executor/show.go`) OVERWRITES the stored
/// column's `FieldType` with the re-planned one and then rewrites `VarString`
/// to `Varchar` in place, so every cell this row prints -- type text, charset,
/// collation, nullability -- is read off the PLAN's type.
/// `dataForColumnsInTable` does not: it keeps the re-planned type for
/// `COLUMN_TYPE`/`DATA_TYPE` only and builds the rest from the STORED column,
/// and it does the `VarString` remap for `DATA_TYPE` alone.
///
/// One captured view makes both halves visible at once:
///
/// ```text
/// desc v                            ->  event_id | varchar(32)    | NO
/// information_schema.columns for v  ->  event_id | var_string(32) | YES
/// ```
///
/// Making the two surfaces agree would be the regression, not the fix.
fn view_column_description(
    name: &str,
    field_type: &tidb_datatype::FieldType,
    full: bool,
) -> Vec<Datum> {
    let field_type = &show_columns_view_type(field_type);
    let null_flag = if field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    if !full {
        return vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            type_desc_cell(field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(Vec::new()),
            Datum::Null,
            Datum::Bytes(Vec::new()),
        ];
    }
    let collation = column_collation_cell(field_type);
    vec![
        Datum::Bytes(name.as_bytes().to_vec()),
        type_desc_cell(field_type),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
        Datum::Null,
        Datum::Bytes(Vec::new()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
    ]
}

/// Go `tryFillViewColumnType`'s closing rewrite: a re-planned view column
/// whose type came back `VarString` is reported as a `VARCHAR`.
///
/// `CAST(... AS CHAR(32))` yields a `VarString` in the plan, so without this a
/// view over one describes itself with a type name no `CREATE TABLE` can
/// spell. The rewrite is confined to the `SHOW` surface; see
/// [`view_column_description`] for the surface that deliberately does not.
fn show_columns_view_type(field_type: &tidb_datatype::FieldType) -> tidb_datatype::FieldType {
    let mut field_type = field_type.clone();
    if field_type.code() == tidb_datatype::FieldTypeCode::VarString {
        field_type.set_code(tidb_datatype::FieldTypeCode::Varchar);
    }
    field_type
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

/// Go `NewColDesc`'s key flag, shared by `SHOW COLUMNS` and
/// `information_schema.COLUMNS`: PRI for a primary key, UNI for a column that
/// is the whole of a unique index, MUL for one that leads a non-unique index.
pub(crate) fn column_key_flag(table: &tidb_executor::KvTable, offset: usize) -> String {
    if table.columns[offset]
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    {
        "PRI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
    {
        "UNI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.first() == Some(&offset))
    {
        "MUL".to_owned()
    } else {
        String::new()
    }
}

/// A one-column result set of strings, the shape SHOW DATABASES and SHOW
/// TABLES produce.
/// A result set with the named VARCHAR columns and no rows, which is what
/// several Go `SHOW` arms answer when the feature they report on is absent.
pub(crate) fn text_columns_output(columns: &[&str]) -> StmtOutput {
    StmtOutput::Rows {
        columns: columns
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    FieldType::new(tidb_datatype::FieldTypeCode::Varchar),
                )
            })
            .collect(),
        rows: Vec::new(),
    }
}

pub(crate) fn string_column_output(column: &str, values: Vec<String>) -> StmtOutput {
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    StmtOutput::Rows {
        columns: vec![(column.to_owned(), field_type)],
        rows: values
            .into_iter()
            .map(|value| vec![Datum::Bytes(value.into_bytes())])
            .collect(),
    }
}

/// The collations `SHOW COLLATION` reports, in mock TiDB's own capture order
/// (alphabetical by collation name). `Utf8Mb4ZhPinyinTiDbAsCs` is
/// deliberately excluded: it is a reserved stub collation, and Go's own
/// `SHOW COLLATION` capture omits it too.
const SHOW_COLLATION_ROWS: &[tidb_datatype::Collation] = &[
    tidb_datatype::Collation::AsciiBin,
    tidb_datatype::Collation::Binary,
    tidb_datatype::Collation::Gb18030Bin,
    tidb_datatype::Collation::Gb18030ChineseCi,
    tidb_datatype::Collation::GbkBin,
    tidb_datatype::Collation::GbkChineseCi,
    tidb_datatype::Collation::Latin1Bin,
    tidb_datatype::Collation::Utf8Bin,
    tidb_datatype::Collation::Utf8GeneralCi,
    tidb_datatype::Collation::Utf8UnicodeCi,
    tidb_datatype::Collation::Utf8Mb40900AiCi,
    tidb_datatype::Collation::Utf8Mb40900Bin,
    tidb_datatype::Collation::Utf8Mb4Bin,
    tidb_datatype::Collation::Utf8Mb4GeneralCi,
    tidb_datatype::Collation::Utf8Mb4UnicodeCi,
];

/// Whether `collation` is the one `SHOW COLLATION` marks `Default`.
///
/// Go reads `Collation.IsDefault`, which `switchDefaultCollation` keeps in
/// step with the owning charset's default collation, so this is exactly
/// "`collation` is its charset's default" and is derived rather than listed.
/// An explicit list here was a second place for the `gbk`/`gb18030` default to
/// be spelled, and it carried a doc claim -- that
/// [`tidb_datatype::Charset::default_collation`] returns the `_bin`
/// collations for those charsets -- that was already untrue when read.
fn is_default_show_collation(collation: tidb_datatype::Collation) -> bool {
    collation.charset().default_collation() == collation
}

/// The column names a `SHOW VARIABLES` row carries, which its `WHERE` filter
/// resolves against.
const SHOW_VARIABLE_COLUMNS: &[&str; 2] = &["Variable_name", "Value"];

/// The virtual column names available to `SHOW CHARSET WHERE`.
const SHOW_CHARSET_COLUMNS: &[&str; 4] = &["Charset", "Description", "Default collation", "Maxlen"];

/// The virtual column names available to `SHOW ENGINES WHERE`.
const SHOW_ENGINES_COLUMNS: &[&str; 6] = &[
    "Engine",
    "Support",
    "Comment",
    "Transactions",
    "XA",
    "Savepoints",
];

/// The virtual column names available to `SHOW COLLATION WHERE`.
const SHOW_COLLATION_COLUMNS: &[&str; 7] = &[
    "Collation",
    "Charset",
    "Id",
    "Default",
    "Compiled",
    "Sortlen",
    "Pad_attribute",
];

/// The status variables this tier truthfully reports for `SHOW STATUS`, as
/// `(name, value, session_only)`, in row order.
///
/// The values are Go's captured defaults for a plain (no-TLS, no-compression)
/// connection, which is exactly what this tier is: no wire compression, so
/// `Compression` is `OFF`, and no TLS, so the `Ssl_*` family is empty/`0`.
/// The `session_only` flag mirrors Go's `vardef.ScopeSession`, which
/// `fetchShowStatus` uses to drop rows from `SHOW GLOBAL STATUS`.
///
/// NOT modelled (this tier has no metrics/server tier to read them from):
/// the `Performance_schema_session_connect_attrs_*` counters,
/// `ddl_schema_version`, `server_id`, `last_plan_binding_update_time`, and
/// `tidb_keys_examined`.
const SHOW_STATUS_VARS: &[(&str, &str, bool)] = &[
    ("Compression", "OFF", true),
    ("Compression_algorithm", "", true),
    ("Compression_level", "0", true),
    ("Ssl_cipher", "", false),
    ("Ssl_cipher_list", "", false),
    // The SERVER provider's static rows (`pkg/server/stat.go:34`): both
    // empty without TLS certificates, GLOBAL|SESSION scope.
    ("Ssl_server_not_after", "", false),
    ("Ssl_server_not_before", "", false),
    ("Ssl_verify_mode", "0", false),
    ("Ssl_version", "", false),
];

/// A resolver over one row of a virtual `SHOW` result, so the statement's own
/// `WHERE` can be evaluated against it.
///
/// Go builds the same thing as a real selection over the show output; this
/// tier evaluates the predicate per row instead, which is the same filter
/// without a plan to carry it.
struct ShowRowResolver<'a> {
    columns: &'a [&'a str],
    row: &'a [Datum],
}

impl tidb_executor::Columns for ShowRowResolver<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        let name = path.last()?;
        let index = self
            .columns
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))?;
        self.row.get(index).cloned()
    }
}

/// Whether one virtual `SHOW` row satisfies the statement's `WHERE`.
fn show_row_matches(
    predicate: &tidb_ast::Expr,
    columns: &[&str],
    row: &[Datum],
) -> Result<bool, DriverError> {
    let resolver = ShowRowResolver { columns, row };
    let value = tidb_executor::eval_in(predicate, &resolver)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    let truthy = tidb_executor::truthy_of(&value)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    Ok(truthy.unwrap_or(false))
}

/// An evaluated SHOW LIKE operand, including whether Go's predicate extractor
/// lower-cases both the metadata name and a literal pattern.
struct ShowLikePattern {
    value: Option<String>,
    fold_lowercase: bool,
    literal_name: Option<String>,
}

impl ShowLikePattern {
    fn from_expr(expr: &tidb_ast::Expr, value: Option<String>, has_extractor: bool) -> Self {
        let extracted_literal = has_extractor
            && matches!(
                expr,
                tidb_ast::Expr::Null
                    | tidb_ast::Expr::Int(_)
                    | tidb_ast::Expr::Decimal(_)
                    | tidb_ast::Expr::Float(_)
                    | tidb_ast::Expr::Hex(_)
                    | tidb_ast::Expr::Bit(_)
                    | tidb_ast::Expr::String(_)
                    | tidb_ast::Expr::RawString(_)
                    | tidb_ast::Expr::Bool(_)
            );
        let literal_name = if extracted_literal {
            value.clone().filter(|name| !name.is_empty())
        } else {
            None
        };
        Self {
            value: if extracted_literal {
                value.map(go_to_lower)
            } else {
                value
            },
            fold_lowercase: extracted_literal,
            literal_name,
        }
    }

    fn column_name(&self, base: &str) -> String {
        self.literal_name
            .as_ref()
            .map_or_else(|| base.to_owned(), |pattern| format!("{base} ({pattern})"))
    }

    fn matches(&self, text: &str) -> bool {
        let Some(pattern) = &self.value else {
            return false;
        };
        if self.fold_lowercase {
            tidb_executor::like_match_with_collation(
                go_to_lower(text),
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        } else {
            tidb_executor::like_match_with_collation(
                text,
                pattern,
                None,
                tidb_datatype::Collation::Utf8Mb4Bin,
            )
        }
    }
}

/// Applies the `LIKE`/`WHERE` layer Go builds over a virtual SHOW result.
fn filter_show_output(
    output: StmtOutput,
    like_pattern: Option<ShowLikePattern>,
    where_clause: Option<&tidb_ast::Expr>,
) -> Result<StmtOutput, DriverError> {
    let StmtOutput::Rows { columns, rows } = output else {
        return Ok(output);
    };
    let column_names: Vec<&str> = columns.iter().map(|(name, _)| name.as_str()).collect();
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows {
        let matches_like = match &like_pattern {
            None => true,
            Some(pattern) => row
                .first()
                .and_then(datum_text)
                .is_some_and(|text| pattern.matches(&text)),
        };
        if !matches_like {
            continue;
        }
        if let Some(predicate) = where_clause {
            if !show_row_matches(predicate, &column_names, &row)? {
                continue;
            }
        }
        filtered.push(row);
    }
    Ok(StmtOutput::Rows {
        columns,
        rows: filtered,
    })
}

impl Session {
    /// The `SHOW COLUMNS` / `DESCRIBE` result for one table, optionally
    /// narrowed to a single column as Go's `DESCRIBE tbl col` narrows it.
    fn show_columns(
        &mut self,
        database: &str,
        table_path: &[String],
        column: Option<&str>,
        full: bool,
    ) -> Result<StmtOutput, DriverError> {
        // A `db.tbl` path names its own schema, as everywhere else.
        let (database, table_name) = match table_path {
            [name] => (database.to_owned(), name.clone()),
            [db, name] => (db.clone(), name.clone()),
            _ => return Err(DriverError::unsupported("empty table name")),
        };
        let ctx = self.statement_context(false);
        let rows = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.table_in(&database, &table_name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{database}.{table_name}"
                ))));
            };
            if let tidb_executor::TableEntry::View(view) = entry {
                // Go re-plans the body here (`tryFillViewColumnType`), so the
                // types reported are the ones the base tables have now, and a
                // body that no longer resolves fails the statement with its
                // own error rather than with ErrViewInvalid.
                let view = view.clone();
                let columns = tidb_executor::view_column_list(&view, &database, catalog, &ctx)?;
                return Ok(columns
                    .iter()
                    .filter(|(candidate, _)| {
                        column.is_none_or(|name| candidate.eq_ignore_ascii_case(name))
                    })
                    .map(|(name, field_type)| view_column_description(name, field_type, full))
                    .collect::<Vec<_>>());
            }
            let tidb_executor::TableEntry::Kv(table) = entry else {
                return Err(DriverError::unsupported(
                    "SHOW COLUMNS needs a storage-backed table",
                ));
            };
            table
                .visible_columns()
                .iter()
                .enumerate()
                .filter(|(_, candidate)| {
                    column.is_none_or(|name| candidate.name.eq_ignore_ascii_case(name))
                })
                .map(|(offset, candidate)| column_description(candidate, offset, table, full, &ctx))
                .collect::<Result<Vec<_>, _>>()
        })?;
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let field_names = if full {
            FULL_COL_DESC_FIELD_NAMES
        } else {
            COL_DESC_FIELD_NAMES
        };
        Ok(StmtOutput::Rows {
            columns: field_names
                .iter()
                .map(|name| ((*name).to_owned(), field_type.clone()))
                .collect(),
            rows,
        })
    }

    /// The `AdminStmt` dispatch [`Session::apply_schema_statement`] reaches
    /// for the whole admin/inspection surface: the `SHOW` family here, plus
    /// `EXPLAIN`, `GRANT`/`REVOKE`/`SHOW GRANTS`, and `KILL` delegated to
    /// their own modules.
    pub(crate) fn dispatch_admin_stmt(
        &mut self,
        admin: &tidb_ast::AdminStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        match admin {
            // `EXPLAIN <select>`: plan the statement and report the plan,
            // running nothing. Go's EXPLAIN plans without executing (an
            // `EXPLAIN INSERT` inserts no row, captured), and so does
            // this: `tidb_executor::explain_select_stmt` re-runs the
            // driver's own read-path decisions without touching storage.
            //
            // See `tidb_executor::explain`'s module doc for every place
            // this tier's plan text diverges from Go's and why.
            tidb_ast::AdminStmt::Explain(explain) => self.explain_stmt(explain),
            // SQL bindings. See `crate::binding_arm`, and `crate::binding`
            // for the normalization and hint-transfer they are built on.
            tidb_ast::AdminStmt::CreateBinding(create) => {
                Ok(Some(self.create_binding_stmt(create)?))
            }
            tidb_ast::AdminStmt::DropBinding(drop) => Ok(Some(self.drop_binding_stmt(drop)?)),
            tidb_ast::AdminStmt::SetBinding(set) => Ok(Some(self.set_binding_stmt(set)?)),
            tidb_ast::AdminStmt::ShowBindings(show) => {
                let output = self.show_bindings_stmt(show)?;
                // Measured on real TiDB: `LIKE` filters on `Original_sql`
                // CASE-SENSITIVELY (`like '%KB%'` over a lowercase binding
                // answers nothing), so the literal is NOT folded the way
                // `SHOW DATABASES LIKE` folds -- `has_extractor: false`.
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowBindingsFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, false)), None)
                    }
                    Some(tidb_ast::ShowBindingsFilter::Where(expr)) => (None, Some(expr)),
                };
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // `ANALYZE TABLE`, over this session's own catalog. See
            // `crate::analyze_arm` for why an in-process session runs it here
            // rather than routing it at a cluster node that can write
            // `mysql.stats_*`.
            tidb_ast::AdminStmt::AnalyzeTable(_) | tidb_ast::AdminStmt::AnalyzeIncremental(_) => {
                self.analyze_stmt(admin)
            }
            // Go receives LOAD STATS bytes through the MySQL client-local
            // transfer handler and persists them through the statistics
            // handle. The standalone session has neither boundary; the
            // cluster connection owns the complete path.
            tidb_ast::AdminStmt::LoadStats(_) => Err(DriverError::unsupported(
                "LOAD STATS requires client-local file transfer",
            )),
            tidb_ast::AdminStmt::LockStats(lock) => self.stats_lock_stmt(lock, true),
            tidb_ast::AdminStmt::UnlockStats(unlock) => self.stats_lock_stmt(unlock, false),
            tidb_ast::AdminStmt::CreateWorkloadSnapshot => {
                if !self.has_scoped_privilege("", "", privilege::GlobalPriv::Super) {
                    return Err(DriverError::SpecificAccessDenied("SUPER".to_owned()));
                }
                let worker = self.workload_repository.as_ref().ok_or_else(|| {
                    DriverError::NotSupportedYet("Workload repository is not enabled".into())
                })?;
                worker.take_snapshot().map_err(|error| {
                    if error == "Workload repository is not enabled" {
                        DriverError::NotSupportedYet(error.into())
                    } else {
                        DriverError::unsupported(error)
                    }
                })?;
                Ok(Some(StmtOutput::Done(true)))
            }
            // `ADMIN RELOAD <blacklist>`: Go's `ReloadExprPushdownBlacklist`
            // and `ReloadOptRuleBlacklist` executors, each of which reads its
            // `mysql.*` table and publishes what the optimizer consults. Both
            // answer no rows, as every ADMIN maintenance statement does.
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::ExprPushdownBlacklist) => {
                self.reload_expr_pushdown_blacklist()?;
                Ok(Some(StmtOutput::Done(true)))
            }
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::OptRuleBlacklist) => {
                self.reload_opt_rule_blacklist()?;
                Ok(Some(StmtOutput::Done(true)))
            }
            tidb_ast::AdminStmt::Grant(grant) => Ok(Some(self.grant_stmt(grant)?)),
            tidb_ast::AdminStmt::Revoke(revoke) => Ok(Some(self.revoke_stmt(revoke)?)),
            tidb_ast::AdminStmt::ShowGrants(show) => Ok(Some(self.show_grants_stmt(show)?)),
            tidb_ast::AdminStmt::ShowCreateUser(spec) => {
                Ok(Some(self.show_create_user_stmt(spec)?))
            }
            tidb_ast::AdminStmt::GrantRole(grant) => Ok(Some(self.grant_role_stmt(grant)?)),
            tidb_ast::AdminStmt::RevokeRole(revoke) => Ok(Some(self.revoke_role_stmt(revoke)?)),
            tidb_ast::AdminStmt::ShowDatabases(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowDatabasesFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowDatabasesFilter::Where(expr)) => (None, Some(expr)),
                };
                let names = self.with_catalog_mut(|catalog| Ok(catalog.database_names()))?;
                // Go `fetchShowDatabases` (`executor/show.go` around line
                // 462): one `DBIsVisible` per schema, so an account sees
                // only what it holds some evidence for -- plus
                // `information_schema`, which is visible to everyone and is
                // already first in `database_names`.
                let names = names
                    .into_iter()
                    .filter(|name| self.database_is_visible(name))
                    .collect();
                let column_name = like_pattern.as_ref().map_or_else(
                    || "Database".to_owned(),
                    |pattern| pattern.column_name("Database"),
                );
                let output = string_column_output(&column_name, names);
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go `fetchShowTableStatus`: one row per table in the
            // schema, with the columns MySQL's own SHOW TABLE STATUS
            // reports.
            //
            // NOT MODELLED, and each reported the way Go reports an
            // absent value rather than invented: every size and count
            // (Rows, Data_length, Index_length and friends) is 0, which
            // is also what TiDB itself answers without a statistics tier;
            // Create_time is NULL because this tier stores no per-table
            // creation timestamp; Update_time, Check_time and Checksum
            // are NULL or empty for the same reason.
            tidb_ast::AdminStmt::ShowTableStatus(show) => {
                let database = match &show.database {
                    Some(database) => database.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                // Go `fetchShowTableStatus` (`executor/show.go` around line
                // 639) applies the same pre-lookup 1044 gate `SHOW TABLES`
                // does.
                self.require_visible_database(&database)?;
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowTableStatusFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowTableStatusFilter::Where(expr)) => (None, Some(expr)),
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let mut rows = Vec::new();
                    let names = catalog.table_names(&database).ok_or_else(|| {
                        DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                    })?;
                    for name in names {
                        let entry = catalog.table_in(&database, &name);
                        let (auto_increment, table_charset, comment, create_options) = match entry {
                            Some(tidb_executor::TableEntry::Kv(table)) => (
                                table.next_auto_increment(),
                                table.charset(),
                                table.comment(),
                                // The same rule `information_schema.tables`
                                // reports, which is where Go's SHOW TABLE
                                // STATUS reads this from.
                                if table.partition().is_some() {
                                    "partitioned"
                                } else if table.is_cached() {
                                    "cached=on"
                                } else {
                                    ""
                                },
                            ),
                            _ => (None, tidb_executor::TableCharset::default(), "", ""),
                        };
                        let row = if entry.is_some_and(tidb_executor::TableEntry::is_view) {
                            crate::show_admin::show_table_status_view_row(&name)
                        } else {
                            crate::show_admin::show_table_status_row(
                                &name,
                                auto_increment,
                                table_charset,
                                comment,
                                create_options,
                            )
                        };
                        rows.push(row);
                    }
                    Ok(rows)
                })?;
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let columns = crate::show_admin::SHOW_TABLE_STATUS_COLUMNS
                    .iter()
                    .map(|(name, numeric)| {
                        ((*name).to_owned(), if *numeric { number() } else { text() })
                    })
                    .collect();
                let output = StmtOutput::Rows { columns, rows };
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go `fetchShowIndex`: one row per index COLUMN, ordered
            // with the clustered primary key first, then the table's own
            // indexes in definition order.
            //
            // NOT MODELLED, and each reported the way Go reports an
            // absent value rather than invented: Cardinality is 0 (no
            // statistics tier), Sub_part and Packed are NULL (no prefix
            // or packed indexes here), Comment/Index_comment are empty,
            // Expression is NULL when no expression key part exists; Global
            // is the stored `IndexInfo.Global` flag.
            tidb_ast::AdminStmt::ShowIndex(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowIndexFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, false)), None)
                    }
                    Some(tidb_ast::ShowIndexFilter::Where(expr)) => (None, Some(expr)),
                };
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match show.table.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    let tidb_executor::TableEntry::Kv(table) = entry else {
                        return Err(DriverError::unsupported(
                            "SHOW INDEX needs a storage-backed table",
                        ));
                    };
                    Ok(show_index_rows(&table_name, table))
                })?;
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let columns = SHOW_INDEX_COLUMNS
                    .iter()
                    .map(|(name, numeric)| {
                        ((*name).to_owned(), if *numeric { number() } else { text() })
                    })
                    .collect();
                let output = StmtOutput::Rows { columns, rows };
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go `ShowExec` with `ShowVariables`: one row per variable,
            // as `Variable_name` and `Value`, filtered by LIKE.
            //
            // `GLOBAL` reads the shared table live (a variable with no
            // GLOBAL scope at all falls back to its registry default, as Go
            // reports SOMETHING for every name `SHOW GLOBAL VARIABLES`
            // lists rather than erroring); `SESSION`/unqualified reads this
            // session's own copy, same as a plain `@@x`.
            tidb_ast::AdminStmt::ShowVariables(show) => {
                let pattern = match &show.like {
                    Some(tidb_ast::Expr::String(text)) => Some(text.clone()),
                    Some(_) => {
                        return Err(DriverError::unsupported(
                            "SHOW VARIABLES LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let mut rows = Vec::new();
                for definition in sysvar::SYS_VARS {
                    if self.sem_hides_sysvar(definition.name) {
                        continue;
                    }
                    let matches = match &pattern {
                        Some(pattern) => tidb_executor::like_match_with_collation(
                            definition.name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ),
                        None => true,
                    };
                    if !matches {
                        continue;
                    }
                    let value = if show.global {
                        self.vars
                            .get_global(definition.name)
                            .unwrap_or_else(|_| sysvar::effective_default(definition))
                    } else {
                        self.vars
                            .get_system(definition.name)
                            .unwrap_or_else(|_| sysvar::effective_default(definition))
                    };
                    let row = vec![
                        Datum::Bytes(definition.name.as_bytes().to_vec()),
                        Datum::Bytes(value.into_bytes()),
                    ];
                    // Go plans the WHERE as a selection over the same
                    // virtual rows, which is what this filter is.
                    if let Some(predicate) = &show.where_clause {
                        if !show_row_matches(predicate, SHOW_VARIABLE_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                        (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowStatus`: one `Variable_name | Value` row per
            // status variable that `variable.GetStatusVars` collects from
            // the registered `Statistics` providers, with `GLOBAL` scope
            // skipping session-only variables.
            //
            // This tier serves only `SHOW_STATUS_VARS` (see its doc
            // comment for what is not modelled). As with the
            // `ShowVariables` arm above, GLOBAL and SESSION read the same
            // values here because this tier keeps no persisted global
            // tier; GLOBAL still drops session-only rows, which the Go
            // capture confirms (`SHOW GLOBAL STATUS` omits the
            // `Compression*` family).
            tidb_ast::AdminStmt::ShowStatus(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowStatusFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowStatusFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW STATUS LIKE takes a string pattern",
                        ))
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowStatusFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let mut rows = Vec::new();
                let (tls_cipher, tls_version) = self.tls_status();
                let (tls_cipher, tls_version) = (tls_cipher.to_owned(), tls_version.to_owned());
                // The SERVER provider's dynamic row (`stat.go:87`):
                // `Uptime` is the seconds since the hosting server started,
                // GLOBAL scope (shown by both SESSION and GLOBAL views).
                let uptime = self.server_start_timestamp.map(|start| {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |since| since.as_secs() as i64);
                    (now - start).max(0).to_string()
                });
                let dynamic = uptime
                    .as_ref()
                    .map(|value| ("Uptime", value.as_str(), false));
                for &(name, value, session_only) in SHOW_STATUS_VARS.iter().chain(dynamic.iter()) {
                    // Go fills these two per connection from the negotiated
                    // TLS state (`server.go:1329`); a plaintext connection
                    // keeps the table's empty strings.
                    let value = match name {
                        "Ssl_cipher" => tls_cipher.as_str(),
                        "Ssl_version" => tls_version.as_str(),
                        _ => value,
                    };
                    if self.sem_hides_status_var(name) {
                        continue;
                    }
                    if show.global && session_only {
                        continue;
                    }
                    if let Some(pattern) = &pattern {
                        if !tidb_executor::like_match_with_collation(
                            name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ) {
                            continue;
                        }
                    }
                    let row = vec![
                        Datum::Bytes(name.as_bytes().to_vec()),
                        Datum::Bytes(value.as_bytes().to_vec()),
                    ];
                    // Go plans the WHERE as a selection over the same
                    // virtual rows, which is what this filter is.
                    if let Some(predicate) = predicate {
                        if !show_row_matches(predicate, SHOW_VARIABLE_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                        (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowCharset`: one row per charset in the parser's
            // registry, captured from mock TiDB (`Charset | Description |
            // Default collation | Maxlen`).
            tidb_ast::AdminStmt::ShowCharset(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowCharsetFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW CHARSET LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                    Some(tidb_ast::ShowCharsetFilter::Where(_)) => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                // Go's `SHOW CHARSET` is `charset.GetSupportedCharsets`, whose
                // rows carry whatever default collation
                // `switchDefaultCollation` last wrote. Reading the registry
                // here rather than a table copied out of it is what keeps the
                // `gbk`/`gb18030` default from having a second spelling.
                let mut rows = Vec::new();
                for info in tidb_datatype::get_supported_charsets() {
                    if let Some(pattern) = &pattern {
                        if !tidb_executor::like_match_with_collation(
                            &info.name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ) {
                            continue;
                        }
                    }
                    let row = vec![
                        Datum::Bytes(info.name.into_bytes()),
                        Datum::Bytes(info.description.into_bytes()),
                        Datum::Bytes(info.default_collation.into_bytes()),
                        Datum::Int(info.maxlen as i64),
                    ];
                    if let Some(predicate) = predicate {
                        if !show_row_matches(predicate, SHOW_CHARSET_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Charset".to_owned(), text()),
                        ("Description".to_owned(), text()),
                        ("Default collation".to_owned(), text()),
                        ("Maxlen".to_owned(), number()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowEngines`: this tier is the mock/embedded
            // single-engine server, so the table is always the single
            // `InnoDB` row Go's mock session reports.
            tidb_ast::AdminStmt::ShowEngines(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.as_str())
                    }
                    Some(tidb_ast::ShowEnginesFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW ENGINES LIKE takes a string pattern",
                        ));
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let row = vec![
                    Datum::Bytes(b"InnoDB".to_vec()),
                    Datum::Bytes(b"DEFAULT".to_vec()),
                    Datum::Bytes(
                        b"Supports transactions, row-level locking, and foreign keys".to_vec(),
                    ),
                    Datum::Bytes(b"YES".to_vec()),
                    Datum::Bytes(b"YES".to_vec()),
                    Datum::Bytes(b"YES".to_vec()),
                ];
                let included = pattern.is_none_or(|pattern| {
                    tidb_executor::like_match_with_collation(
                        "InnoDB",
                        pattern,
                        None,
                        tidb_datatype::Collation::Utf8Mb4Bin,
                    )
                }) && predicate
                    .map(|predicate| show_row_matches(predicate, SHOW_ENGINES_COLUMNS, &row))
                    .transpose()?
                    .unwrap_or(true);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Engine".to_owned(), text()),
                        ("Support".to_owned(), text()),
                        ("Comment".to_owned(), text()),
                        ("Transactions".to_owned(), text()),
                        ("XA".to_owned(), text()),
                        ("Savepoints".to_owned(), text()),
                    ],
                    rows: included.then_some(row).into_iter().collect(),
                }))
            }
            tidb_ast::AdminStmt::ShowStatsTopN(show) => self.stats_topn_stmt(show).map(Some),
            tidb_ast::AdminStmt::ShowStatsBuckets(show) => self.stats_buckets_stmt(show).map(Some),
            tidb_ast::AdminStmt::ShowStatsHistograms(show) => {
                self.stats_histograms_stmt(show).map(Some)
            }
            tidb_ast::AdminStmt::ShowStatsLocked(show) => self.stats_locked_stmt(show).map(Some),
            // Go `fetchShowCollation`: one row per collation in the
            // parser's registry (`Collation | Charset | Id | Default |
            // Compiled | Sortlen | Pad_attribute`).
            //
            // NOT MODELLED (documented): `Utf8Mb4ZhPinyinTiDbAsCs`, TiDB's
            // reserved pinyin collation stub -- mock TiDB's own `SHOW
            // COLLATION` capture omits it too, so this table matches the
            // 15 collations Go actually lists rather than this crate's
            // full 16-variant registry.
            tidb_ast::AdminStmt::ShowCollation(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowCollationFilter::Like(_)) => {
                        return Err(DriverError::unsupported(
                            "SHOW COLLATION LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                    Some(tidb_ast::ShowCollationFilter::Where(_)) => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let mut rows = Vec::new();
                for &collation in SHOW_COLLATION_ROWS {
                    let name = collation.name();
                    if let Some(pattern) = &pattern {
                        if !tidb_executor::like_match_with_collation(
                            name,
                            pattern,
                            None,
                            tidb_datatype::Collation::Utf8Mb4Bin,
                        ) {
                            continue;
                        }
                    }
                    let (sortlen, pad_attribute): (i64, &str) = match collation {
                        tidb_datatype::Collation::Utf8UnicodeCi
                        | tidb_datatype::Collation::Utf8Mb4UnicodeCi => (8, "PAD SPACE"),
                        tidb_datatype::Collation::Utf8Mb40900AiCi => (0, "NO PAD"),
                        tidb_datatype::Collation::Binary
                        | tidb_datatype::Collation::Utf8Mb40900Bin => (1, "NO PAD"),
                        _ => (1, "PAD SPACE"),
                    };
                    let row = vec![
                        Datum::Bytes(name.as_bytes().to_vec()),
                        Datum::Bytes(collation.charset().name().as_bytes().to_vec()),
                        Datum::Int(i64::from(collation.id())),
                        Datum::Bytes(if is_default_show_collation(collation) {
                            b"Yes".to_vec()
                        } else {
                            Vec::new()
                        }),
                        Datum::Bytes(b"Yes".to_vec()),
                        Datum::Int(sortlen),
                        Datum::Bytes(pad_attribute.as_bytes().to_vec()),
                    ];
                    if let Some(predicate) = predicate {
                        if !show_row_matches(predicate, SHOW_COLLATION_COLUMNS, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Collation".to_owned(), text()),
                        ("Charset".to_owned(), text()),
                        ("Id".to_owned(), number()),
                        ("Default".to_owned(), text()),
                        ("Compiled".to_owned(), text()),
                        ("Sortlen".to_owned(), number()),
                        ("Pad_attribute".to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `ShowExec` with `ShowWarnings`/`ShowErrors`: the rows are
            // the statement-context warnings, whose `Level` column is
            // `Warning` or `Error`.
            //
            // DEFERRED (documented, and refused rather than ignored): the
            // optional filter Go's shared SHOW grammar accepts here.
            // Go `SimpleExec.executeFlush`, `ShowDDLExec` and
            // `fetchShowMasterStatus` live in `crate::show_admin`: they
            // answer without touching a user table, and keeping them here
            // pushed this file past the repository's 2200-line ceiling.
            tidb_ast::AdminStmt::Flush(flush) => {
                return crate::show_admin::flush_stmt(flush).map(Some);
            }
            tidb_ast::AdminStmt::ShowDdl => {
                return Ok(Some(crate::show_admin::show_ddl_output(
                    &self.show_ddl_rows(),
                )));
            }
            tidb_ast::AdminStmt::ShowMasterStatus => {
                return Ok(Some(crate::show_admin::master_status_output(
                    self.current_tso().value(),
                )));
            }
            tidb_ast::AdminStmt::ShowWarnings(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::unsupported(
                        "SHOW WARNINGS filters are not supported yet",
                    ));
                }
                Ok(Some(self.warning_output(show.count_only, false)))
            }
            tidb_ast::AdminStmt::ShowErrors(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::unsupported(
                        "SHOW ERRORS filters are not supported yet",
                    ));
                }
                Ok(Some(self.warning_output(show.count_only, true)))
            }
            // Go `ShowExec.fetchShowProcessList`: one row per live
            // connection of this server, read from the session manager.
            tidb_ast::AdminStmt::ShowInspection(show) => {
                // Pinned Go `ShowExec.fetchAll` keeps the grammar entry but
                // rejects execution because extended statistics was removed.
                if show.kind == tidb_ast::ShowInspectionKind::StatsExtended {
                    return Err(DriverError::unsupported(
                        "Extended statistics feature has been removed",
                    ));
                }
                // Go `ShowExec.fetchShowBindingCacheStatus`; see
                // `crate::binding_arm`.
                if show.kind == tidb_ast::ShowInspectionKind::BindingCacheStatus {
                    return Ok(Some(self.binding_cache_status_stmt()?));
                }
                // Go `ShowExec.fetchShowPlugins` over `plugin.GetAll()`. No
                // plugin framework runs here, so the answer is the empty set
                // -- which is also what a stock `tidb-server` reports.
                // Go answers these two without a source: see
                // `crate::show_admin`.
                if let Some(output) = crate::show_admin::inspection_output(show.kind) {
                    return Ok(Some(output));
                }
                // Go `ShowExec.fetchShowStatsMeta` (`executor/show_stats.go:36`).
                if show.kind == tidb_ast::ShowInspectionKind::StatsMeta {
                    return self.stats_meta_stmt(show.filter.as_ref()).map(Some);
                }
                if show.kind == tidb_ast::ShowInspectionKind::StatsHealthy {
                    return self.stats_healthy_stmt(show.filter.as_ref()).map(Some);
                }
                if show.kind == tidb_ast::ShowInspectionKind::ColumnStatsUsage {
                    return self.column_stats_usage_stmt(show.filter.as_ref()).map(Some);
                }
                if show.kind == tidb_ast::ShowInspectionKind::HistogramsInFlight {
                    let (like_pattern, where_clause) = match show.filter.as_ref() {
                        None => (None, None),
                        Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                            let value = datum_text(&self.eval_value(expr)?);
                            (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                        }
                        Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
                    };
                    let count = self
                        .with_catalog_mut(|catalog| Ok(catalog.clean_needed_statistics_items()))?;
                    let output = StmtOutput::Rows {
                        columns: vec![(
                            "HistogramsInFlight".to_owned(),
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        )],
                        rows: vec![tidb_executor::show_stats::histograms_in_flight_row(count)],
                    };
                    return filter_show_output(output, like_pattern, where_clause).map(Some);
                }
                if show.kind == tidb_ast::ShowInspectionKind::AnalyzeStatus {
                    return self.analyze_status_stmt(show.filter.as_ref()).map(Some);
                }
                if show.kind != tidb_ast::ShowInspectionKind::ProcessList {
                    return Ok(None);
                }
                if show.filter.is_some() || show.database.is_some() {
                    return Err(DriverError::unsupported(
                        "SHOW PROCESSLIST filters are not supported yet",
                    ));
                }
                Ok(Some(self.process_list_output(show.full)))
            }
            // Go `ShowExec.fetchShowBuiltins` (`show.go:2459`) over
            // `expression.GetBuiltinList`, under the single column
            // `planbuilder.go:6084` names.
            tidb_ast::AdminStmt::ShowBuiltins => {
                let text = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![("Supported_builtin_functions".to_owned(), text)],
                    rows: tidb_executor::builtin_list()
                        .into_iter()
                        .map(|name| vec![Datum::new_string(name)])
                        .collect(),
                }))
            }
            // Go `SimpleExec.executeKillStmt`.
            tidb_ast::AdminStmt::Kill(kill) => self.kill_stmt(kill),
            // Go `CheckTableExec` / `CheckIndexRangeExec`. See
            // [`crate::admin_check_arm`] for the shapes and the refusals.
            tidb_ast::AdminStmt::AdminCheck(check) => {
                self.admin_check_stmt(check.as_ref()).map(Some)
            }
            tidb_ast::AdminStmt::ShowCreate {
                kind,
                name,
                if_not_exists,
            } => {
                if *kind == tidb_ast::ShowCreateKind::Database {
                    let [database] = name.as_slice() else {
                        return Err(DriverError::unsupported("empty database name"));
                    };
                    let (reported, charset) = self.with_catalog_mut(|catalog| {
                        catalog.database_definition(database).ok_or_else(|| {
                            DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                        })
                    })?;
                    return Ok(Some(crate::show_create_database::output(
                        reported,
                        charset,
                        *if_not_exists,
                    )));
                }
                // Go `fetchShowCreatePlacementPolicy` (`executor/show.go:1774`)
                // looks the policy up and reports `ErrPlacementPolicyNotExists`
                // (8239) for a name that is not there. The row is the policy's
                // ORIGINAL-cased name beside
                // `ConstructResultOfShowCreatePlacementPolicy` (`:1742`).
                if *kind == tidb_ast::ShowCreateKind::PlacementPolicy {
                    let [policy_name] = name.as_slice() else {
                        return Err(DriverError::unsupported("empty policy name"));
                    };
                    let (reported, clause) = self.with_catalog_mut(|catalog| {
                        catalog
                            .policy(policy_name)
                            .map(|policy| {
                                let settings = policy
                                    .placement_settings
                                    .as_ref()
                                    .map(|settings| settings.read().to_clause())
                                    .unwrap_or_default();
                                (policy.name.original().to_owned(), settings)
                            })
                            .ok_or_else(|| {
                                DriverError::PlacementPolicyNotExists(policy_name.clone())
                            })
                    })?;
                    return Ok(Some(crate::show_create_placement_policy::output(
                        &reported, &clause,
                    )));
                }
                // `SHOW CREATE SEQUENCE` and `SHOW CREATE TABLE` take the
                // SAME path: Go's `buildShow` picks the column names from
                // whether the object IS a sequence, not from the keyword
                // written (captured: `show create table s1` over a sequence
                // answers `Sequence | Create Sequence` with the
                // `CREATE SEQUENCE` text).
                let want_view = match kind {
                    tidb_ast::ShowCreateKind::Table | tidb_ast::ShowCreateKind::Sequence => false,
                    tidb_ast::ShowCreateKind::View => true,
                    _ => return Ok(None),
                };
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match name.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let ctx = self.statement_context(false);
                // A view answers either spelling with the same row, which
                // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
                // table is refused.
                let shown = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    match entry {
                        tidb_executor::TableEntry::View(view) => Ok((
                            show_create_view_text(view),
                            table_name.clone(),
                            Some((
                                view.character_set_client.clone(),
                                view.collation_connection.clone(),
                            )),
                            false,
                        )),
                        _ if want_view => Err(DriverError::Schema(SchemaErrorKind::WrongObject {
                            name: format!("{database}.{table_name}"),
                            expected: "VIEW",
                        })),
                        tidb_executor::TableEntry::Sequence(sequence) => Ok((
                            tidb_executor::show_create_sequence(sequence),
                            sequence.name.clone(),
                            None,
                            true,
                        )),
                        tidb_executor::TableEntry::Kv(table) => Ok((
                            show_create_table_text(&database, &table_name, table, &ctx)?,
                            table_name.clone(),
                            None,
                            false,
                        )),
                        tidb_executor::TableEntry::Mem(_) => Err(DriverError::unsupported(
                            "SHOW CREATE TABLE needs a storage-backed table",
                        )),
                    }
                });
                let (text, reported, view_charset, is_sequence) = shown?;
                let field_type =
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                // Go's view form carries its own header and the session's
                // character set and collation.
                if let Some((character_set_client, collation_connection)) = view_charset {
                    return Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("View".to_owned(), field_type.clone()),
                            ("Create View".to_owned(), field_type.clone()),
                            ("character_set_client".to_owned(), field_type.clone()),
                            ("collation_connection".to_owned(), field_type),
                        ],
                        rows: vec![vec![
                            Datum::Bytes(reported.into_bytes()),
                            Datum::Bytes(text.into_bytes()),
                            Datum::Bytes(character_set_client.into_bytes()),
                            Datum::Bytes(collation_connection.into_bytes()),
                        ]],
                    }));
                }
                // Go `buildShow` names these columns from `isSequence`, so a
                // sequence reports `Sequence | Create Sequence` whichever
                // keyword was written.
                let (name_column, text_column) = if is_sequence {
                    ("Sequence", "Create Sequence")
                } else {
                    ("Table", "Create Table")
                };
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (name_column.to_owned(), field_type.clone()),
                        (text_column.to_owned(), field_type),
                    ],
                    rows: vec![vec![
                        Datum::Bytes(reported.into_bytes()),
                        Datum::Bytes(text.into_bytes()),
                    ]],
                }))
            }
            tidb_ast::AdminStmt::ShowTableNextRowId(show) => {
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match show.table.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let ids = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    let tidb_executor::TableEntry::Kv(table) = entry else {
                        return Err(DriverError::unsupported(
                            "SHOW TABLE NEXT_ROW_ID needs a storage-backed table",
                        ));
                    };
                    table
                        .next_global_row_ids()
                        .map_err(|error| DriverError::AutoIdUnavailable(error.0))
                })?;
                let rows = ids
                    .into_iter()
                    .map(|(column, next, id_type)| {
                        vec![
                            Datum::Bytes(database.as_bytes().to_vec()),
                            Datum::Bytes(table_name.as_bytes().to_vec()),
                            Datum::Bytes(column.into_bytes()),
                            Datum::Int(next),
                            Datum::Bytes(id_type.as_bytes().to_vec()),
                        ]
                    })
                    .collect();
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("DB_NAME".to_owned(), text()),
                        ("TABLE_NAME".to_owned(), text()),
                        ("COLUMN_NAME".to_owned(), text()),
                        ("NEXT_GLOBAL_ROW_ID".to_owned(), number()),
                        ("ID_TYPE".to_owned(), text()),
                    ],
                    rows,
                }))
            }
            // Go `fetchShowColumns`.
            tidb_ast::AdminStmt::ShowColumns(show) => {
                if show.extended {
                    return Err(DriverError::unsupported(
                        "SHOW EXTENDED COLUMNS is not supported yet",
                    ));
                }
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowColumnsFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowColumnsFilter::Where(expr)) => (None, Some(expr)),
                };
                let database = match &show.database {
                    Some(name) => name.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                let output = self.show_columns(&database, &show.table, None, show.full)?;
                filter_show_output(output, like_pattern, where_clause).map(Some)
            }
            // Go's parser rewrites `DESCRIBE tbl [col]` into a SHOW
            // COLUMNS statement; this parser keeps a node of its own, so
            // the same output is produced from it here.
            tidb_ast::AdminStmt::DescribeTable(describe) => {
                let database = self.require_current_database()?.to_owned();
                let column = describe.column.as_ref().and_then(|path| path.last());
                self.show_columns(
                    &database,
                    &describe.table,
                    column.map(String::as_str),
                    false,
                )
                .map(Some)
            }
            tidb_ast::AdminStmt::ShowTables(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowTablesFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowTablesFilter::Where(expr)) => (None, Some(expr)),
                };
                let database = match &show.database {
                    Some(name) => name.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                // Go `fetchShowTables` (`executor/show.go` around line 576)
                // asks `DBIsVisible` BEFORE `SchemaExists`, so a schema this
                // account could not see reports 1044 whether or not it
                // exists.
                self.require_visible_database(&database)?;
                let full = show.full;
                let listed = self.with_catalog_mut(|catalog| {
                    Ok(catalog.table_names(&database).map(|names| {
                        names
                            .into_iter()
                            .map(|name| {
                                let is_view = catalog.is_view_in(&database, &name);
                                let is_sequence = catalog.is_sequence_in(&database, &name);
                                (name, is_view, is_sequence)
                            })
                            .collect::<Vec<_>>()
                    }))
                })?;
                let listed = listed.ok_or_else(|| {
                    DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                })?;
                // Go filters each listed table by "any privilege at all"
                // (`show.go` around line 613), with `CREATE TEMPORARY
                // TABLES` excluded from the mask. Column-scope grants are
                // deliberately not consulted, which is Go's own standing
                // TODO there and is measured: a `SELECT(a)` grant makes the
                // SCHEMA visible but lists no table.
                let listed: Vec<_> = listed
                    .into_iter()
                    .filter(|(name, ..)| {
                        self.has_any_scoped_privilege(
                            &database,
                            name,
                            privilege::show_tables_priv_mask(),
                        )
                    })
                    .filter(|(name, ..)| {
                        like_pattern
                            .as_ref()
                            .is_none_or(|pattern| pattern.matches(name))
                    })
                    .collect();
                // Go names the column after the schema being listed.
                let base_name = format!("Tables_in_{database}");
                let name_column = match &like_pattern {
                    Some(pattern) => pattern.column_name(&base_name),
                    None => base_name,
                };
                let field_type =
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let column_names: Vec<&str> = if full {
                    vec![name_column.as_str(), "Table_type"]
                } else {
                    vec![name_column.as_str()]
                };
                let mut rows = Vec::with_capacity(listed.len());
                for (name, is_view, is_sequence) in listed {
                    let mut row = vec![Datum::Bytes(name.into_bytes())];
                    if full {
                        row.push(Datum::Bytes(
                            table_type_of(is_view, is_sequence).as_bytes().to_vec(),
                        ));
                    }
                    if let Some(predicate) = where_clause {
                        if !show_row_matches(predicate, &column_names, &row)? {
                            continue;
                        }
                    }
                    rows.push(row);
                }
                let columns = if full {
                    vec![
                        (name_column, field_type.clone()),
                        ("Table_type".to_owned(), field_type),
                    ]
                } else {
                    vec![(name_column, field_type)]
                };
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod column_description_source_tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

    #[test]
    fn test_desc() {
        // Direct port of pkg/table/column_test.go::TestDesc. The Go test
        // drives NewColDesc by replacing the column flags, then toggles the
        // generated-column storage bit and asks for both result schemas.
        let auto = FieldType::new(FieldTypeCode::Long).with_flags(
            FieldTypeFlags::AUTO_INCREMENT | FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY,
        );
        assert_eq!(column_extra(&auto, true, None, false), "auto_increment");

        let multiple = FieldType::new(FieldTypeCode::Long).with_flags(FieldTypeFlags::MULTIPLE_KEY);
        assert_eq!(column_extra(&multiple, false, None, false), "");

        let on_update = FieldType::new(FieldTypeCode::Timestamp)
            .with_flags(FieldTypeFlags::UNIQUE_KEY | FieldTypeFlags::ON_UPDATE_NOW);
        assert_eq!(
            column_extra(&on_update, false, None, false),
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
        );

        let ordinary = FieldType::new(FieldTypeCode::Long);
        assert_eq!(
            column_extra(&ordinary, false, Some(true), false),
            "STORED GENERATED"
        );
        assert_eq!(
            column_extra(&ordinary, false, Some(false), false),
            "VIRTUAL GENERATED"
        );
        assert_eq!(
            column_extra(&ordinary, false, None, true),
            "DEFAULT_GENERATED"
        );

        assert_eq!(
            COL_DESC_FIELD_NAMES,
            ["Field", "Type", "Null", "Key", "Default", "Extra"]
        );
        assert_eq!(
            FULL_COL_DESC_FIELD_NAMES,
            [
                "Field",
                "Type",
                "Collation",
                "Null",
                "Key",
                "Default",
                "Extra",
                "Privileges",
                "Comment",
            ]
        );
    }
}
