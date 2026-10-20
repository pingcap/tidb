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
//! `open_information_schema_query` in `dispatch.rs`.
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
    // Go `NewColDesc`: a column carrying `NoDefaultValueFlag` reports no
    // default, whatever value its metadata still stores (a NOT NULL TEXT/BLOB
    // given `DEFAULT ''` under a non-strict sql_mode).
    let no_default = column
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE);
    let default = match &column.default_value {
        _ if no_default => Datum::Null,
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
    // Go `NewColDesc`: the key column reads the type's own flags, which a
    // view column inherits from the base column it projects.
    let key_flag = if field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY) {
        "PRI"
    } else if field_type.has_flag(tidb_datatype::FieldTypeFlags::UNIQUE_KEY) {
        "UNI"
    } else if field_type.has_flag(tidb_datatype::FieldTypeFlags::MULTIPLE_KEY) {
        "MUL"
    } else {
        ""
    };
    if !full {
        return vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            type_desc_cell(field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.as_bytes().to_vec()),
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
        Datum::Bytes(key_flag.as_bytes().to_vec()),
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
                    (*name).into(),
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
fn is_default_show_collation(collation: tidb_datatype::Collation, utf8mb4_default: &str) -> bool {
    // Go `isUTF8MB4AndDefaultCollation`: utf8mb4's default is the session's
    // `default_collation_for_utf8mb4`.
    if collation.charset() == tidb_datatype::Charset::Utf8Mb4 {
        return collation.name() == utf8mb4_default;
    }
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
/// `Uptime`, `tidb_keys_examined`, `ddl_schema_version`, `server_id`,
/// `last_plan_binding_update_time` and the connect-attrs counters are
/// appended per statement.
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
    // go's SHOW WHERE resolves names against the statement's OWN output
    // columns; a name outside that list is ErrBadField with the 'where
    // clause' clause -- even when it names a DATA column of the inspected
    // table (oracle: `SHOW COLUMNS FROM sy WHERE b IS NOT NULL` fails
    // `Unknown column 'b' in 'where clause'`).
    if let Some(missing) = first_unknown_output_column(predicate, columns) {
        return Err(DriverError::UnknownColumnInClause {
            column: missing,
            clause: "where clause".into(),
        });
    }
    let resolver = ShowRowResolver { columns, row };
    let value = tidb_executor::eval_in(predicate, &resolver)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    let truthy = tidb_executor::truthy_of(&value)
        .map_err(|e| DriverError::Exec(tidb_executor::ExecError::Eval(e)))?;
    Ok(truthy.unwrap_or(false))
}

/// The first referenced column name that is not one of the SHOW output's own
/// columns, in evaluation order.
fn first_unknown_output_column(predicate: &tidb_ast::Expr, columns: &[&str]) -> Option<String> {
    struct Collector<'a> {
        columns: &'a [&'a str],
        missing: Option<String>,
    }
    impl tidb_ast::Visitor for Collector<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(name) = node
                .downcast_ref::<tidb_ast::Expr>()
                .and_then(|expr| match expr {
                    tidb_ast::Expr::Column(path) => path.last().cloned(),
                    _ => None,
                })
            {
                if !self
                    .columns
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(&name))
                {
                    self.missing = Some(name);
                }
                // A column leaf has no children; false keeps the walk alive
                // for sibling keys (`WHERE a = 1 AND b = 2`).
                return false;
            }
            // Descend into compound expressions: `enter` returning true
            // SKIPS the subtree, which would never reach the operands.
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut probe = predicate.clone();
    let mut collector = Collector {
        columns,
        missing: None,
    };
    tidb_ast::Visitable::accept(&mut probe, &mut collector);
    collector.missing
}

/// Whether `expr` names any column.
fn references_column(expr: &tidb_ast::Expr) -> bool {
    first_unknown_output_column(expr, &[]).is_some()
}

/// Go `buildShow`: `LIKE <pattern>` is `<first output column> LIKE
/// <pattern>`, planned as a Selection over the SHOW's own rows, so an
/// identifier in the pattern names one of those columns. One the output does
/// not have is 1054 when the statement is built, whatever the rows, spelled
/// in lower case as Go reports it.
fn show_like_predicate(
    pattern: &tidb_ast::Expr,
    columns: &[&str],
) -> Result<tidb_ast::Expr, DriverError> {
    if let Some(missing) = first_unknown_output_column(pattern, columns) {
        return Err(DriverError::UnknownColumnInClause {
            column: go_to_lower(&missing),
            clause: "where clause".into(),
        });
    }
    let first = columns.first().copied().unwrap_or_default();
    Ok(tidb_ast::Expr::Like {
        expr: Box::new(tidb_ast::Expr::Column(vec![first.to_owned()])),
        pattern: Box::new(pattern.clone()),
        not: false,
        ilike: false,
        escape: None,
    })
}

/// An evaluated SHOW LIKE operand, including whether Go's predicate extractor
/// lower-cases both the metadata name and a literal pattern.
pub(crate) struct ShowLikePattern {
    value: Option<String>,
    fold_lowercase: bool,
    literal_name: Option<String>,
    /// A pattern that names columns: Go plans it as a predicate over each
    /// row ([`show_like_predicate`]) instead of a literal match.
    row_pattern: Option<tidb_ast::Expr>,
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
        if references_column(expr) {
            return Self {
                value: None,
                fold_lowercase: false,
                literal_name: None,
                row_pattern: Some(expr.clone()),
            };
        }
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
            row_pattern: None,
        }
    }

    /// The per-row predicate for a pattern that names columns, after Go's
    /// build-time 1054 for a column `columns` does not have; `None` for a
    /// literal pattern.
    fn row_predicate(&self, columns: &[&str]) -> Result<Option<tidb_ast::Expr>, DriverError> {
        self.row_pattern
            .as_ref()
            .map(|pattern| show_like_predicate(pattern, columns))
            .transpose()
    }

    fn column_name(&self, base: &str) -> String {
        self.literal_name
            .as_ref()
            .map_or_else(|| base.to_owned(), |pattern| format!("{base} ({pattern})"))
    }

    fn matches(&self, text: &str) -> bool {
        // A row predicate is applied where the whole row is known.
        if self.row_pattern.is_some() {
            return true;
        }
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

/// The SHOW result Go's `buildShow` hands its `LogicalSelection` when a
/// `WHERE` reads a subquery: the rows as a table the planner reads, under a
/// schema name no statement can write.
const SHOW_SELECTION_SCHEMA: &str = "__tidb_show_selection";
const SHOW_SELECTION_TABLE: &str = "show_rows";

impl Session {
    /// Go `buildShow`'s selections over a virtual SHOW result: `LIKE`
    /// matches the first column and `WHERE` is a `LogicalSelection` over the
    /// result's own columns. A `WHERE` that reads a subquery is planned as
    /// that selection, which builds the subquery the way any SELECT's does
    /// (`field IN (SELECT ...)`, `field < ALL (SELECT ...)`); any other
    /// predicate evaluates row by row against the same columns.
    /// Go `ShowExec.fetchShowAffinity`: one row per table (`table` level) or
    /// partition (`partition` level) carrying an AFFINITY, with its PD group's
    /// state from `affinity.GetAllGroupStates` -- NULL where PD holds no such
    /// group. The rows follow `ListTablesWithSpecialAttribute` over infoschema
    /// v2: schema name, then table id, both descending. A literal LIKE is Go's
    /// extractor, matched against the TABLE name.
    fn show_affinity_stmt(
        &mut self,
        filter: Option<&tidb_ast::ShowInspectionFilter>,
    ) -> Result<StmtOutput, DriverError> {
        let (like_pattern, where_clause) = match filter {
            None => (None, None),
            Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                let value = datum_text(&self.eval_value(expr)?);
                (Some(ShowLikePattern::from_expr(expr, value, true)), None)
            }
            Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
        };
        struct AffinityRow {
            db_lower: String,
            table_id: i64,
            db: String,
            table: String,
            partition: Option<String>,
            group_id: String,
        }
        let mut infos = Vec::new();
        {
            let catalog = self.lock_catalog()?;
            for database in catalog.database_names() {
                for name in catalog.table_names(&database).unwrap_or_default() {
                    let Some(tidb_executor::TableEntry::Kv(table)) =
                        catalog.table_in(&database, &name)
                    else {
                        continue;
                    };
                    let Some(affinity) = table.affinity() else {
                        continue;
                    };
                    if like_pattern.as_ref().is_some_and(|pattern| {
                        pattern.row_pattern.is_none() && !pattern.matches(&go_to_lower(&table.name))
                    }) {
                        continue;
                    }
                    let row = |partition: Option<String>, group_id: String| AffinityRow {
                        db_lower: go_to_lower(&database),
                        table_id: table.table_id,
                        db: database.clone(),
                        table: table.name.clone(),
                        partition,
                        group_id,
                    };
                    match affinity.level.as_str() {
                        "table" => infos.push(row(
                            None,
                            tidb_exec::ddl_affinity::get_table_affinity_group_id(table.table_id),
                        )),
                        "partition" => {
                            for definition in
                                table.partition().map_or(&[][..], |spec| &spec.definitions)
                            {
                                infos.push(row(
                                    Some(definition.name.clone()),
                                    tidb_exec::ddl_affinity::get_partition_affinity_group_id(
                                        table.table_id,
                                        definition.id,
                                    ),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        // A stable sort keeps a table's partitions in definition order.
        infos.sort_by(|left, right| {
            right
                .db_lower
                .cmp(&left.db_lower)
                .then(right.table_id.cmp(&left.table_id))
        });
        let states = tidb_domain::affinity::get_all_group_states().map_err(|error| {
            DriverError::Mysql(tidb_executor::MysqlError::new(1105, error.to_string()))
        })?;
        let rows = infos
            .into_iter()
            .map(|info| {
                let state = states.get(&info.group_id);
                let text = |text: &str| Datum::Bytes(text.as_bytes().to_vec());
                vec![
                    text(&info.db),
                    text(&info.table),
                    info.partition.as_deref().map_or(Datum::Null, text),
                    state
                        .filter(|state| state.group.leader_store_id != 0)
                        .map_or(Datum::Null, |state| {
                            Datum::UInt(state.group.leader_store_id)
                        }),
                    state
                        .filter(|state| !state.group.voter_store_ids.is_empty())
                        .map_or(Datum::Null, |state| {
                            text(
                                &state
                                    .group
                                    .voter_store_ids
                                    .iter()
                                    .map(u64::to_string)
                                    .collect::<Vec<_>>()
                                    .join(","),
                            )
                        }),
                    state.map_or(Datum::Null, |state| {
                        text(match state.phase.as_str() {
                            "pending" => "Pending",
                            "preparing" => "Preparing",
                            "stable" => "Stable",
                            other => other,
                        })
                    }),
                    state.map_or(Datum::Null, |state| Datum::Int(state.region_count)),
                    state.map_or(Datum::Null, |state| Datum::Int(state.affinity_region_count)),
                ]
            })
            .collect();
        let varchar = || FieldType::new(tidb_datatype::FieldTypeCode::Varchar);
        let longlong = || FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let columns = vec![
            ("Db_name".to_owned(), varchar()),
            ("Table_name".to_owned(), varchar()),
            ("Partition_name".to_owned(), varchar()),
            ("Leader_store_id".to_owned(), longlong()),
            ("Voter_store_ids".to_owned(), varchar()),
            ("Status".to_owned(), varchar()),
            ("Region_count".to_owned(), longlong()),
            ("Affinity_region_count".to_owned(), longlong()),
        ];
        let row_like = like_pattern.filter(|pattern| pattern.row_pattern.is_some());
        self.filter_show_output(StmtOutput::Rows { columns, rows }, row_like, where_clause)
    }

    pub(crate) fn filter_show_output(
        &mut self,
        output: StmtOutput,
        like_pattern: Option<ShowLikePattern>,
        where_clause: Option<&tidb_ast::Expr>,
    ) -> Result<StmtOutput, DriverError> {
        let Some(predicate) =
            where_clause.filter(|predicate| predicate.flags() & tidb_ast::FLAG_HAS_SUBQUERY != 0)
        else {
            return filter_show_rows(output, like_pattern, where_clause);
        };
        let StmtOutput::Rows { columns, rows } = filter_show_rows(output, like_pattern, None)?
        else {
            unreachable!("filtering keeps a row result a row result")
        };
        let mut query = match tidb_parser::parse(&format!(
            "SELECT * FROM `{SHOW_SELECTION_SCHEMA}`.`{SHOW_SELECTION_TABLE}`"
        )) {
            Ok(Stmt::Query(query)) => query,
            _ => unreachable!("the selection template parses as a query"),
        };
        let tidb_ast::QueryStmt::Select(select) = &mut *query else {
            unreachable!("the selection template is a SELECT")
        };
        select.where_clause = Some(predicate.clone());
        let current_db = self.current_db.clone();
        let ctx = self.statement_context(false);
        let table = tidb_executor::MemTable {
            columns: columns.clone(),
            rows,
        };
        let (_, rows) = self.with_catalog_mut(|catalog| {
            let mut scratch = catalog.clone();
            scratch.register_mem_in(SHOW_SELECTION_SCHEMA, SHOW_SELECTION_TABLE, table);
            tidb_executor::driver::open_query_meta_stmt_with_physical(
                &query,
                None,
                &scratch,
                &current_db,
                &ctx,
            )?
            .collect()
        })?;
        Ok(StmtOutput::Rows { columns, rows })
    }
}

/// Applies the `LIKE`/`WHERE` layer Go builds over a virtual SHOW result,
/// evaluating the predicate row by row against the result's columns.
pub(crate) fn filter_show_rows(
    output: StmtOutput,
    like_pattern: Option<ShowLikePattern>,
    where_clause: Option<&tidb_ast::Expr>,
) -> Result<StmtOutput, DriverError> {
    let StmtOutput::Rows { columns, rows } = output else {
        return Ok(output);
    };
    let column_names: Vec<&str> = columns.iter().map(|(name, _)| name.as_str()).collect();
    let like_predicate = match &like_pattern {
        Some(pattern) => pattern.row_predicate(&column_names)?,
        None => None,
    };
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows {
        let matches_like = match (&like_predicate, &like_pattern) {
            (Some(predicate), _) => show_row_matches(predicate, &column_names, &row)?,
            (None, None) => true,
            (None, Some(pattern)) => row
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
    /// Go `buildShow`'s selections over `SHOW WARNINGS` / `SHOW ERRORS`
    /// rows: `LIKE` matches the `Level` column, `WHERE` any of the three.
    fn filter_warning_output(
        &mut self,
        output: StmtOutput,
        like: Option<&tidb_ast::Expr>,
        where_clause: Option<&tidb_ast::Expr>,
    ) -> Result<StmtOutput, DriverError> {
        let like_pattern = match like {
            Some(expr) => {
                let value = datum_text(&self.eval_value(expr)?);
                Some(ShowLikePattern::from_expr(expr, value, false))
            }
            None => None,
        };
        self.filter_show_output(output, like_pattern, where_clause)
    }

    /// The `SHOW COLUMNS` / `DESCRIBE` result for one table, optionally
    /// narrowed to a single column as Go's `DESCRIBE tbl col` narrows it.
    /// The schema `SHOW COLUMNS` / `DESCRIBE` default to: the `FROM db` clause,
    /// else the current database -- `ErrNoDB` only for an unqualified table, as
    /// a `db.tbl` path names its own schema and needs no current one.
    fn show_columns_database(
        &self,
        explicit: Option<&str>,
        table_path: &[String],
    ) -> Result<String, DriverError> {
        match (explicit, table_path) {
            (Some(database), _) => Ok(database.to_owned()),
            (None, [_, _]) => Ok(String::new()),
            (None, _) => self.require_current_database().map(str::to_owned),
        }
    }

    fn show_columns(
        &mut self,
        database: &str,
        table_path: &[String],
        column: Option<&str>,
        full: bool,
        extended: bool,
    ) -> Result<StmtOutput, DriverError> {
        // A `db.tbl` path names its own schema, as everywhere else.
        let (database, table_name) = match table_path {
            [name] => (database.to_owned(), name.clone()),
            [db, name] => (db.clone(), name.clone()),
            _ => return Err(DriverError::unsupported("empty table name")),
        };
        // Go `fetchShowColumns` asks for any column privilege once the table
        // resolves, and refuses as a denied SELECT on the table as stored.
        let stored_name = self
            .lock_catalog()?
            .table_in(&database, &table_name)
            .map(|entry| match entry {
                tidb_executor::TableEntry::Kv(table) if !table.name().is_empty() => {
                    table.name().to_owned()
                }
                tidb_executor::TableEntry::View(view) => view.name.clone(),
                _ => table_name.clone(),
            });
        if let (Some(stored_name), Some((_, user, host))) = (stored_name, self.privilege_context())
        {
            if !self.has_any_scoped_privilege(
                &database,
                &table_name,
                crate::privilege::column_privs_mask(),
            ) {
                return Err(DriverError::TableAccessDenied {
                    privilege: "SELECT",
                    user: user.to_owned(),
                    host: host.to_owned(),
                    table: stored_name,
                });
            }
        }
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
            // `EXTENDED` adds the hidden columns MySQL keeps internally
            // (`tb.Cols()` rather than `tb.VisibleCols()`).
            let listed = if extended {
                table.columns()
            } else {
                table.visible_columns()
            };
            listed
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

    /// Go `buildShow` for SHOW CREATE TABLE/SEQUENCE/VIEW: a view needs SHOW
    /// VIEW, anything else any privilege but CREATE TEMPORARY TABLES (a
    /// local temporary table any at all), each refused as its own command
    /// denied on the lower-cased name. A missing table is checked as a table.
    pub(crate) fn require_show_create_privilege(
        &self,
        database: &str,
        table_name: &str,
    ) -> Result<(), DriverError> {
        let Some((_, user, host)) = self.privilege_context() else {
            return Ok(());
        };
        let is_view = self.lock_catalog()?.is_view_in(database, table_name);
        let (granted, command) = if is_view {
            (
                self.has_scoped_privilege(
                    database,
                    table_name,
                    crate::privilege::GlobalPriv::ShowView,
                ),
                "SHOW VIEW",
            )
        } else {
            let mut mask = crate::privilege::any_priv_mask();
            if !self.is_local_temporary_table(database, table_name) {
                mask &= !crate::privilege::GlobalPriv::CreateTemporaryTables.bit();
            }
            (
                self.has_any_scoped_privilege(database, table_name, mask),
                "SHOW",
            )
        };
        if granted {
            return Ok(());
        }
        Err(DriverError::TableAccessDenied {
            privilege: command,
            user: user.to_owned(),
            host: host.to_owned(),
            table: tidb_util::stringutil::go_to_lower(table_name),
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
        // Go `buildAdmin` appends a SUPER visit for every ADMIN statement it
        // plans past its early returns, and `CheckPrivilege` reports a denial
        // as `ErrPrivilegeCheckFail`.
        if admin_needs_super(admin)
            && !self.has_scoped_privilege("", "", privilege::GlobalPriv::Super)
        {
            return Err(DriverError::PrivilegeCheckFail(
                privilege::GlobalPriv::Super.check_fail_name().to_owned(),
            ));
        }
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
            // Go `executeAdminSetBDRRole` / `executeAdminUnsetBDRRole` write
            // the meta key `BDRRole`; `AdminShowBDRRoleExec` reads it.
            tidb_ast::AdminStmt::SetBdrRole(_)
            | tidb_ast::AdminStmt::UnsetBdrRole
            | tidb_ast::AdminStmt::ShowBdrRole => {
                let role = match admin {
                    tidb_ast::AdminStmt::SetBdrRole(tidb_ast::BdrRole::Primary) => "primary",
                    tidb_ast::AdminStmt::SetBdrRole(tidb_ast::BdrRole::Secondary) => "secondary",
                    tidb_ast::AdminStmt::UnsetBdrRole => "",
                    _ => {
                        let role = match self.bdr_role_store.clone() {
                            Some(store) => store.get().map_err(DriverError::unsupported)?,
                            None => self.with_catalog_mut(|catalog| Ok(catalog.bdr_role()))?,
                        };
                        let mut field_type =
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::String);
                        field_type.set_flen(1);
                        return Ok(Some(StmtOutput::Rows {
                            columns: vec![("BDR_ROLE".to_owned(), field_type)],
                            rows: vec![vec![Datum::Bytes(role.into_bytes())]],
                        }));
                    }
                };
                match self.bdr_role_store.clone() {
                    Some(store) => store.set(role).map_err(DriverError::unsupported)?,
                    None => self.with_catalog_mut(|catalog| {
                        catalog.set_bdr_role(role);
                        Ok(())
                    })?,
                }
                Ok(Some(StmtOutput::Done(true)))
            }
            tidb_ast::AdminStmt::FlushPlanCache(scope) => {
                self.flush_session_plan_cache(*scope)?;
                Ok(Some(StmtOutput::Done(true)))
            }
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
                self.filter_show_output(output, like_pattern, where_clause)
                    .map(Some)
            }
            // `ANALYZE TABLE`, over this session's own catalog. See
            // `crate::analyze_arm` for why an in-process session runs it here
            // rather than routing it at a cluster node that can write
            // `mysql.stats_*`.
            tidb_ast::AdminStmt::AnalyzeTable(_) | tidb_ast::AdminStmt::AnalyzeIncremental(_) => {
                self.analyze_stmt(admin)
            }
            tidb_ast::AdminStmt::LoadStats(load) => self.load_stats_stmt(&load.path).map(Some),
            tidb_ast::AdminStmt::DropStats(drop) => self.drop_stats_stmt(drop).map(Some),
            tidb_ast::AdminStmt::LockStats(lock) => self.stats_lock_stmt(lock, true),
            tidb_ast::AdminStmt::UnlockStats(unlock) => self.stats_lock_stmt(unlock, false),
            tidb_ast::AdminStmt::CreateWorkloadSnapshot => {
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
            // go `SimpleExec.executeReloadBindings` (`pkg/executor/simple.go`):
            // the reload re-syncs the binding cache from storage and answers
            // no rows, like every maintenance statement.
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::Bindings) => {
                Ok(Some(StmtOutput::Done(true)))
            }
            // Go `executeAdminReloadStatistics`.
            tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::Statistics) => Err(
                DriverError::Mysql(tidb_executor::MysqlError::unknown(
                    "Extended statistics feature has been removed",
                )),
            ),
            // Go `plugin.ChangeDisableFlagAndFlush`: no plugin is loaded
            // here, so the first named plugin is unknown.
            tidb_ast::AdminStmt::Plugins { plugins, .. } => match plugins.first() {
                Some(name) => Err(DriverError::Mysql(tidb_executor::MysqlError::unknown(
                    format!("plugin '{name}' not found"),
                ))),
                None => Ok(Some(StmtOutput::Done(true))),
            },
            // go `SimpleExec.executeAdminBindingControl` (`pkg/executor/simple.go`):
            // `ADMIN FLUSH BINDINGS` publishes the cache and answers OK;
            // `CAPTURE`/`EVOLVE` refuse with the two messages Go's own
            // feature gates produce (`Auto Capture is not supported` when
            // `tidb_capture_plan_baselines` is off, and the baseline-evolution
            // GA refusal).
            tidb_ast::AdminStmt::BindingControl(kind) => match kind {
                tidb_ast::AdminBindingControlKind::Flush => Ok(Some(StmtOutput::Done(true))),
                tidb_ast::AdminBindingControlKind::Capture => Err(DriverError::unsupported(
                    "Auto Capture is not supported",
                )),
                tidb_ast::AdminBindingControlKind::Evolve => Err(DriverError::unsupported(
                    "Cannot enable baseline evolution feature, it is not generally available now",
                )),
            },
            // go `execCommandOnDDLJobs` (`pkg/executor/admin.go`): one
            // `(JOB_ID, RESULT)` row per requested id, not an error.
            tidb_ast::AdminStmt::DdlJobControl(control) => {
                Ok(Some(self.ddl_job_control_stmt(control)?))
            }
            tidb_ast::AdminStmt::SplitRegion(split) => Ok(Some(self.split_region_stmt(split)?)),
            // go `ChecksumTableExec` (`pkg/executor/checksum.go`).
            tidb_ast::AdminStmt::AdminChecksum(checksum) => {
                Ok(Some(self.admin_checksum_stmt(&checksum.tables)?))
            }
            // go `adminShowSlowExec` over the domain's slow-query memory.
            tidb_ast::AdminStmt::ShowSlow(show) => Ok(Some(self.admin_show_slow_stmt(show)?)),
            // go `fetchShowDDLJobs` over the finished-job history.
            tidb_ast::AdminStmt::ShowDdlJobs(show) => {
                Ok(Some(self.admin_show_ddl_jobs_stmt(show)?))
            }
            // go `ShowDDLJobQueriesExec` / `...WithRangeExec`.
            tidb_ast::AdminStmt::ShowDdlJobQueries(show) => {
                Ok(Some(self.admin_show_ddl_job_queries_stmt(show)?))
            }
            // go `SimpleExec.executeExplainFor` (`pkg/executor/simple.go`):
            // the target connection must exist on this server
            // (`ErrNoSuchThread` 1094, `Unknown thread id: %d`); its running
            // statement then explains through the ordinary machinery.
            tidb_ast::AdminStmt::ExplainFor(explain_for) => {
                let target = explain_for.connection_id;
                let Some(guard) = self.process.as_ref() else {
                    return Err(DriverError::UnknownThreadId(target));
                };
                let registry = guard.registry();
                let Some(target_row) = registry.snapshot().into_iter().find(|row| row.id == target)
                else {
                    return Err(DriverError::UnknownThreadId(target));
                };
                let plan = registry.plan_of(target);
                // Go `buildExplainFor`: another user's connection needs SUPER,
                // refused with `ErrAccessDenied` over the two names alone.
                if let Some((_, user, host)) = self.privilege_context() {
                    if user != target_row.user
                        && !self.has_scoped_privilege("", "", crate::privilege::GlobalPriv::Super)
                    {
                        return Err(DriverError::Mysql(tidb_executor::MysqlError::new(
                            1045,
                            format!(
                                "Access denied for user '{user}'@'{host}' (using password: %!s(MISSING))"
                            ),
                        )));
                    }
                }
                let format = explain_for.format.to_lowercase();
                let text = |name: &str| {
                    (
                        name.to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString),
                    )
                };
                // Go `buildExplainFor`: no plan at all is the statement's own
                // empty result; any other plan needs one of the three formats.
                if matches!(plan, crate::process::ProcessPlan::None) {
                    return Ok(Some(StmtOutput::Affected(0)));
                }
                let explain_format = match format.as_str() {
                    "" | "row" => tidb_executor::ExplainFormat::Row,
                    "brief" => tidb_executor::ExplainFormat::Brief,
                    "verbose" => tidb_executor::ExplainFormat::Verbose,
                    other => {
                        return Err(DriverError::Mysql(tidb_executor::MysqlError::unknown(
                            format!("explain format '{other}' for connection is not supported now"),
                        )));
                    }
                };
                let plan = match plan {
                    crate::process::ProcessPlan::Plan(plan) => plan,
                    // An `Explain` plan renders nothing, under the analyze
                    // header of the format.
                    _ => {
                        let names: &[&str] = if explain_format == tidb_executor::ExplainFormat::Verbose
                        {
                            &[
                                "id",
                                "estRows",
                                "estCost",
                                "actRows",
                                "task",
                                "access object",
                                "execution info",
                                "operator info",
                                "memory",
                                "disk",
                            ]
                        } else {
                            &[
                                "id",
                                "estRows",
                                "actRows",
                                "task",
                                "access object",
                                "execution info",
                                "operator info",
                                "memory",
                                "disk",
                            ]
                        };
                        return Ok(Some(StmtOutput::Rows {
                            columns: names.iter().map(|name| text(name)).collect(),
                            rows: Vec::new(),
                        }));
                    }
                };
                let ctx = self.statement_context(false);
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    tidb_executor::explain_connection_plan(&plan.0, catalog, &ctx, explain_format)
                })?;
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            // go `fetchShowPrivileges` (`pkg/executor/show.go:2036`): the
            // static table then every dynamic privilege.
            tidb_ast::AdminStmt::ShowPrivileges => {
                Ok(Some(crate::show_admin::show_privileges_output()))
            }
            // Go `fetchShowOpenTables` answers the empty set; the schema is
            // `buildShowSchema`'s four columns.
            tidb_ast::AdminStmt::ShowOpenTables(_) => Ok(Some(StmtOutput::Rows {
                columns: vec![
                    (
                        "Database".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar),
                    ),
                    (
                        "Table".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar),
                    ),
                    (
                        "In_use".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
                    ),
                    (
                        "Name_locked".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
                    ),
                ],
                rows: Vec::new(),
            })),
            // go `fetchShowImportJobs` over the dist-task framework: this
            // tier runs no IMPORT jobs, so the answer is the empty set (the
            // oracle's own answer on the battery's table).
            tidb_ast::AdminStmt::ShowImportJobs(_) => {
                use tidb_datatype::{FieldType, FieldTypeCode};
                // Go `importIntoSchemaNames` / `ImportIntoSchemaFTypes`.
                let string = || FieldType::new(FieldTypeCode::String);
                let long = || FieldType::new(FieldTypeCode::LongLong);
                let timestamp = || FieldType::new(FieldTypeCode::Timestamp);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Job_ID".to_owned(), long()),
                        ("Group_Key".to_owned(), string()),
                        ("Data_Source".to_owned(), string()),
                        ("Target_Table".to_owned(), string()),
                        ("Table_ID".to_owned(), long()),
                        ("Phase".to_owned(), string()),
                        ("Status".to_owned(), string()),
                        ("Source_File_Size".to_owned(), string()),
                        ("Imported_Rows".to_owned(), long()),
                        ("Result_Message".to_owned(), string()),
                        ("Create_Time".to_owned(), timestamp()),
                        ("Start_Time".to_owned(), timestamp()),
                        ("End_Time".to_owned(), timestamp()),
                        ("Created_By".to_owned(), string()),
                        ("Last_Update_Time".to_owned(), timestamp()),
                        ("Cur_Step".to_owned(), string()),
                        ("Cur_Step_Processed_Size".to_owned(), string()),
                        ("Cur_Step_Total_Size".to_owned(), string()),
                        ("Cur_Step_Progress_Pct".to_owned(), string()),
                        ("Cur_Step_Speed".to_owned(), string()),
                        ("Cur_Step_ETA".to_owned(), string()),
                    ],
                    rows: Vec::new(),
                }))
            }
            // Go `buildShowSchema` for `ShowImportGroups`: the jobs are
            // grouped by key; this tier runs no IMPORT jobs.
            tidb_ast::AdminStmt::ShowImportGroups(_) => {
                use tidb_datatype::{FieldType, FieldTypeCode};
                let long = || FieldType::new(FieldTypeCode::LongLong);
                let timestamp = || FieldType::new(FieldTypeCode::Timestamp);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Group_Key".to_owned(), FieldType::new(FieldTypeCode::String)),
                        ("Total_Jobs".to_owned(), long()),
                        ("Pending".to_owned(), long()),
                        ("Running".to_owned(), long()),
                        ("Completed".to_owned(), long()),
                        ("Failed".to_owned(), long()),
                        ("Cancelled".to_owned(), long()),
                        ("First_Job_Create_Time".to_owned(), timestamp()),
                        ("Last_Job_Update_Time".to_owned(), timestamp()),
                    ],
                    rows: Vec::new(),
                }))
            }
            // Go `TraceExec`: runs the statement, then reports the spans.
            tidb_ast::AdminStmt::Trace(trace) => {
                crate::trace::check_options(trace)
                    .map_err(|message| DriverError::Mysql(tidb_executor::MysqlError::unknown(message)))?;
                let inner_sql = crate::trace::statement_text(&trace.statement);
                let started = std::time::SystemTime::now();
                let clock = std::time::Instant::now();
                let inner = (*trace.statement).clone();
                let outcome = self.execute_statement_owned(inner, &inner_sql);
                let elapsed = clock.elapsed();
                let (rows, affected, error) = match outcome {
                    Ok(StmtOutput::Rows { rows, .. }) => (rows.len() as u64, 0, None),
                    Ok(StmtOutput::Affected(count)) => (0, count, None),
                    Ok(_) => (0, 0, None),
                    Err(error) => {
                        let mysql = error.to_mysql_error();
                        (0, 0, Some((mysql.code, mysql.message)))
                    }
                };
                let plan = self.published_plan();
                let kind = crate::trace::kind_of(
                    &trace.statement,
                    plan,
                    rows,
                    self.in_transaction(),
                );
                let spans = crate::trace::build_spans(&crate::trace::TraceInput {
                    kind,
                    sql: inner_sql,
                    started,
                    elapsed,
                    affected_rows: affected,
                    error,
                    chunk_sizes: self.chunk_sizes(),
                });
                let (columns, rows) = crate::trace::render(&trace.format, &spans, started)
                    .map_err(|message| DriverError::Mysql(tidb_executor::MysqlError::unknown(message)))?;
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            // Go `getTiProxyAddrs`: every TRAFFIC statement talks to a
            // TiProxy, and this deployment has none.
            tidb_ast::AdminStmt::Traffic(_) => Err(DriverError::Mysql(tidb_executor::MysqlError::unknown(
                "no tiproxy server found",
            ))),
            // Go `fetchShowPlacement` and its FOR DATABASE/TABLE/PARTITION and
            // LABELS forms (`executor/show_placement.go`).
            tidb_ast::AdminStmt::ShowPlacement(show) => {
                let columns = if show.target == tidb_ast::ShowPlacementTarget::Labels {
                    vec![
                        (
                            "Key".to_owned(),
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar),
                        ),
                        (
                            "Values".to_owned(),
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Json),
                        ),
                    ]
                } else {
                    ["Target", "Placement", "Scheduling_State"]
                        .iter()
                        .map(|name| {
                            (
                                (*name).to_owned(),
                                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar),
                            )
                        })
                        .collect()
                };
                let rows = self.placement_rows(&show.target)?;
                let (like_pattern, where_clause) = match show.filter.as_ref() {
                    None => (None, None),
                    Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
                };
                self.filter_show_output(
                    StmtOutput::Rows { columns, rows },
                    like_pattern,
                    where_clause,
                )
                .map(Some)
            }
            // Go `RecommendIndexExec` (`executor/recommend_index.go`).
            tidb_ast::AdminStmt::RecommendIndex(recommend) => {
                self.recommend_index_stmt(recommend).map(Some)
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
                    || "Database".into(),
                    |pattern| pattern.column_name("Database"),
                );
                let output = string_column_output(&column_name, names);
                self.filter_show_output(output, like_pattern, where_clause)
                    .map(Some)
            }
            // Go `fetchShowTableStatus`: its restricted SQL over
            // `information_schema.tables` for the schema, ordered by table
            // name, so every size and count is the TABLES reader's.
            tidb_ast::AdminStmt::ShowTableStatus(show) => {
                let database = match &show.database {
                    Some(database) => database.clone(),
                    None => self.require_current_database()?.into(),
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
                let zone = self.session_time_zone();
                let visibility = self.schema_visibility();
                let catalog = self.table_storage_statistics_catalog(true)?;
                if catalog.table_names(&database).is_none() {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                        database.clone(),
                    )));
                }
                let rows = crate::infoschema::show_table_status_rows(
                    &catalog,
                    &visibility,
                    &zone,
                    &database,
                );
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let columns = crate::show_admin::SHOW_TABLE_STATUS_COLUMNS
                    .iter()
                    .map(|(name, numeric)| {
                        // Go declares the three timestamps DATETIME.
                        let field_type = if matches!(*name, "Create_time" | "Update_time" | "Check_time")
                        {
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Datetime)
                        } else if *numeric {
                            number()
                        } else {
                            text()
                        };
                        ((*name).to_owned(), field_type)
                    })
                    .collect();
                let output = StmtOutput::Rows { columns, rows };
                self.filter_show_output(output, like_pattern, where_clause)
                    .map(Some)
            }
            // Go `fetchShowIndex`: one row per index COLUMN, ordered
            // with the clustered primary key first, then the table's own
            // indexes in definition order.
            //
            // Cardinality is each column's NDV from the table's stats and
            // Sub_part the declared prefix length; Packed is NULL and
            // Comment empty, as Go writes them; Expression is NULL when no
            // expression key part exists; Global is the stored
            // `IndexInfo.Global` flag.
            tidb_ast::AdminStmt::ShowIndex(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowIndexFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, false)), None)
                    }
                    Some(tidb_ast::ShowIndexFilter::Where(expr)) => (None, Some(expr)),
                };
                let (database, table_name) = match show.table.as_slice() {
                    // go resolves a QUALIFIED name without consulting the
                    // session database: `SHOW INDEX FROM d3p.t2` answers with
                    // no `USE` in sight. Only the bare form needs one, and go
                    // answers 1046 (`ErrNoDB`) for it.
                    [table] => (self.require_current_database()?.to_owned(), table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    let table = match entry {
                        tidb_executor::TableEntry::Kv(table) => table,
                        // A view's `TableInfo` carries no indexes (Go answers
                        // the header and no rows).
                        tidb_executor::TableEntry::View(_)
                        | tidb_executor::TableEntry::Sequence(_) => return Ok(Vec::new()),
                        _ => {
                            return Err(DriverError::unsupported(
                                "SHOW INDEX needs a storage-backed table",
                            ));
                        }
                    };
                    // Go `GetPhysicalTableStats(tb.Meta().ID, ...)`.
                    let statistics = catalog.table_statistics(table.table_id);
                    Ok(show_index_rows(&table_name, table, |column_id| {
                        statistics
                            .as_ref()
                            .and_then(|statistics| statistics.columns.get(&column_id))
                            .map_or(0, |column| column.histogram.ndv)
                    }))
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
                self.filter_show_output(output, like_pattern, where_clause)
                    .map(Some)
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
                // Go `ShowBaseExtractor` lowercases the pattern.
                let pattern = match &show.like {
                    Some(tidb_ast::Expr::String(text)) => {
                        Some(tidb_util::stringutil::go_to_lower(text))
                    }
                    _ => None,
                };
                let like_predicate = match &show.like {
                    Some(expr) if !matches!(expr, tidb_ast::Expr::String(_)) => {
                        Some(show_like_predicate(expr, SHOW_VARIABLE_COLUMNS)?)
                    }
                    _ => None,
                };
                let predicate = show.where_clause.as_ref().or(like_predicate.as_ref());
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let mut rows = Vec::new();
                let noop_enabled = self.vars.noop_variables_enabled();
                for definition in sysvar::SYS_VARS {
                    // Go `fetchShowVariables`: GLOBAL lists no SESSION-only
                    // variable, and neither form lists a noop variable while
                    // `tidb_enable_noop_variables` is OFF.
                    if (show.global && definition.scope == sysvar::SCOPE_SESSION)
                        || (definition.is_noop() && !noop_enabled)
                    {
                        continue;
                    }
                    // Go `fetchShowVariables` also skips `InternalSessionVariable`.
                    if self.sem_hides_sysvar(definition.name)
                        || definition.is_internal_session_variable()
                    {
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
                    // The read-only session getters Go computes from live
                    // session state (`GetSession`), not from a stored value.
                    let live = if show.global {
                        None
                    } else {
                        match definition.name {
                            "tidb_last_ddl_info" => Some(self.last_ddl_info_value()),
                            "tidb_last_query_info" => Some(self.last_query_info_value()),
                            "tidb_last_txn_info" => Some(self.last_txn_info_value()),
                            _ => None,
                        }
                    };
                    let value = if let Some(live) = live {
                        live
                    } else if show.global {
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
                    rows.push(row);
                }
                self.filter_show_output(
                    StmtOutput::Rows {
                        columns: vec![
                            (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                            (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                        ],
                        rows,
                    },
                    None,
                    predicate,
                )
                .map(Some)
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
                    _ => None,
                };
                let like_predicate = match &show.filter {
                    Some(tidb_ast::ShowStatusFilter::Like(expr))
                        if !matches!(expr, tidb_ast::Expr::String(_)) =>
                    {
                        Some(show_like_predicate(expr, SHOW_VARIABLE_COLUMNS)?)
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowStatusFilter::Where(expr)) => Some(expr),
                    _ => like_predicate.as_ref(),
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
                // `tidb_keys_examined` (`statusvar.go`), session scope.
                let keys_examined = self.keys_examined().to_string();
                // The DDL provider's rows (`pkg/ddl/stat.go`): the schema
                // version and this node's DDL id, GLOBAL|SESSION scope.
                let ddl_schema_version = self
                    .cluster_schema_version
                    .as_ref()
                    .map_or_else(|| self.lock_catalog().map_or(0, |c| c.schema_version() as i64), |source| source())
                    .to_string();
                let server_id = self
                    .server_info_syncer
                    .as_ref()
                    .map(|syncer| syncer.local_server_info().static_info.id.clone())
                    .unwrap_or_default();
                // The binding handle's row (`binding_handle.go`), session
                // scope: `Time.String()` of the last update, zero until a
                // binding was loaded.
                let binding_update_time = self
                    .global_binding_cache
                    .as_ref()
                    .map(|cache| cache.load().last_update_time())
                    .filter(|time| !time.is_empty())
                    .unwrap_or_else(|| "0000-00-00 00:00:00".to_owned());
                let mut dynamic = vec![
                    ("Performance_schema_session_connect_attrs_longest_seen", "0", false),
                    ("Performance_schema_session_connect_attrs_lost", "0", false),
                    ("ddl_schema_version", ddl_schema_version.as_str(), false),
                    ("last_plan_binding_update_time", binding_update_time.as_str(), true),
                    ("server_id", server_id.as_str(), false),
                    ("tidb_keys_examined", keys_examined.as_str(), true),
                ];
                if let Some(value) = uptime.as_ref() {
                    dynamic.push(("Uptime", value.as_str(), false));
                }
                // Go `variable.GetStatusVars`: the registered providers'
                // rows (the GC worker's `tidb_gc_*`).
                let registered = crate::statusvar::get_status_vars().map_err(|error| {
                    DriverError::Mysql(tidb_executor::MysqlError::unknown(error))
                })?;
                dynamic.extend(registered.iter().map(|(name, status)| {
                    (
                        name.as_str(),
                        status.value.as_str(),
                        status.scope == crate::sysvar::SCOPE_SESSION,
                    )
                }));
                // Go lists the collected statuses sorted by name.
                let mut listed = SHOW_STATUS_VARS
                    .iter()
                    .copied()
                    .chain(dynamic)
                    .collect::<Vec<_>>();
                listed.sort_by(|left, right| left.0.cmp(right.0));
                for (name, value, session_only) in listed {
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
                    rows.push(row);
                }
                self.filter_show_output(
                    StmtOutput::Rows {
                        columns: vec![
                            (SHOW_VARIABLE_COLUMNS[0].to_owned(), text()),
                            (SHOW_VARIABLE_COLUMNS[1].to_owned(), text()),
                        ],
                        rows,
                    },
                    None,
                    predicate,
                )
                .map(Some)
            }
            // Go `fetchShowCharset`: one row per charset in the parser's
            // registry, captured from mock TiDB (`Charset | Description |
            // Default collation | Maxlen`).
            tidb_ast::AdminStmt::ShowCharset(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    _ => None,
                };
                let like_predicate = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Like(expr))
                        if !matches!(expr, tidb_ast::Expr::String(_)) =>
                    {
                        Some(show_like_predicate(expr, SHOW_CHARSET_COLUMNS)?)
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Where(expr)) => Some(expr),
                    _ => like_predicate.as_ref(),
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
                // Go reads utf8mb4's default from the session's
                // `default_collation_for_utf8mb4`.
                let utf8mb4_default = self
                    .vars
                    .get_system("default_collation_for_utf8mb4")
                    .unwrap_or_else(|_| "utf8mb4_bin".to_owned());
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
                    let default_collation = if info.name == "utf8mb4" {
                        utf8mb4_default.clone()
                    } else {
                        info.default_collation
                    };
                    let row = vec![
                        Datum::Bytes(info.name.into_bytes()),
                        Datum::Bytes(info.description.into_bytes()),
                        Datum::Bytes(default_collation.into_bytes()),
                        Datum::Int(info.maxlen as i64),
                    ];
                    rows.push(row);
                }
                self.filter_show_output(
                    StmtOutput::Rows {
                        columns: vec![
                            ("Charset".to_owned(), text()),
                            ("Description".to_owned(), text()),
                            ("Default collation".to_owned(), text()),
                            ("Maxlen".to_owned(), number()),
                        ],
                        rows,
                    },
                    None,
                    predicate,
                )
                .map(Some)
            }
            // Go `fetchShowEngines`: this tier is the mock/embedded
            // single-engine server, so the table is always the single
            // `InnoDB` row Go's mock session reports.
            tidb_ast::AdminStmt::ShowEngines(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.as_str())
                    }
                    _ => None,
                };
                let like_predicate = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Like(expr))
                        if !matches!(expr, tidb_ast::Expr::String(_)) =>
                    {
                        Some(show_like_predicate(expr, SHOW_ENGINES_COLUMNS)?)
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowEnginesFilter::Where(expr)) => Some(expr),
                    _ => like_predicate.as_ref(),
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
                });
                self.filter_show_output(
                    StmtOutput::Rows {
                        columns: vec![
                            ("Engine".to_owned(), text()),
                            ("Support".to_owned(), text()),
                            ("Comment".to_owned(), text()),
                            ("Transactions".to_owned(), text()),
                            ("XA".to_owned(), text()),
                            ("Savepoints".to_owned(), text()),
                        ],
                        rows: included.then_some(row).into_iter().collect(),
                    },
                    None,
                    predicate,
                )
                .map(Some)
            }
            tidb_ast::AdminStmt::ShowStatsTopN(show) => self.stats_topn_stmt(show).map(Some),
            tidb_ast::AdminStmt::ShowStatsBuckets(show) => {
                self.require_show_stats_privilege("stats_buckets")?;
                self.stats_buckets_stmt(show).map(Some)
            }
            tidb_ast::AdminStmt::ShowStatsHistograms(show) => {
                self.require_show_stats_privilege("stats_histograms")?;
                self.stats_histograms_stmt(show).map(Some)
            }
            tidb_ast::AdminStmt::ShowStatsLocked(show) => {
                self.require_show_stats_privilege("stats_table_locked")?;
                self.stats_locked_stmt(show).map(Some)
            }
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
                // Go `ShowBaseExtractor` lowercases the pattern
                // (`strings.ToLower(ptn)`) before matching it against the
                // lowercase collation names.
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(tidb_util::stringutil::go_to_lower(text))
                    }
                    _ => None,
                };
                let like_predicate = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Like(expr))
                        if !matches!(expr, tidb_ast::Expr::String(_)) =>
                    {
                        Some(show_like_predicate(expr, SHOW_COLLATION_COLUMNS)?)
                    }
                    _ => None,
                };
                let predicate = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Where(expr)) => Some(expr),
                    _ => like_predicate.as_ref(),
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let mut rows = Vec::new();
                let utf8mb4_default = self
                    .vars
                    .get_system("default_collation_for_utf8mb4")
                    .unwrap_or_else(|_| "utf8mb4_bin".to_owned());
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
                        Datum::Bytes(if is_default_show_collation(collation, &utf8mb4_default) {
                            b"Yes".to_vec()
                        } else {
                            Vec::new()
                        }),
                        Datum::Bytes(b"Yes".to_vec()),
                        Datum::Int(sortlen),
                        Datum::Bytes(pad_attribute.as_bytes().to_vec()),
                    ];
                    rows.push(row);
                }
                self.filter_show_output(
                    StmtOutput::Rows {
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
                    },
                    None,
                    predicate,
                )
                .map(Some)
            }
            // Go `SimpleExec.executeFlush`, `ShowDDLExec` and
            // `fetchShowMasterStatus` live in `crate::show_admin`: they
            // answer without touching a user table, and keeping them here
            // pushed this file past the repository's 2200-line ceiling.
            tidb_ast::AdminStmt::Flush(flush) => {
                if matches!(flush.target, tidb_ast::FlushTarget::Status) {
                    self.flush_keys_examined();
                }
                if matches!(flush.target, tidb_ast::FlushTarget::Privileges) {
                    self.flush_privileges()?;
                }
                let stats_delta_targets = match &flush.target {
                    tidb_ast::FlushTarget::StatsDelta { objects, .. } => {
                        Some(self.flush_stats_delta_objects(objects))
                    }
                    _ => None,
                };
                let shared = self.shared_catalog();
                let mut catalog = shared
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                return crate::show_admin::flush_stmt(flush, &mut catalog, stats_delta_targets)
                    .map(Some);
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
            // Go `buildShow` plans a `LIKE` as a selection on the first
            // output column (`Level`) and a `WHERE` as a selection over the
            // rows; there is no extractor, so a literal pattern is not
            // folded. The rows are read before the operand is evaluated, so
            // the filter cannot disturb the warnings it filters.
            tidb_ast::AdminStmt::ShowWarnings(show) => {
                let output = self.warning_output(show.count_only, false);
                let (like, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowWarningsFilter::Like(expr)) => (Some(expr), None),
                    Some(tidb_ast::ShowWarningsFilter::Where(expr)) => (None, Some(expr)),
                };
                self.filter_warning_output(output, like, where_clause)
                    .map(Some)
            }
            tidb_ast::AdminStmt::ShowErrors(show) => {
                let output = self.warning_output(show.count_only, true);
                let (like, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowErrorsFilter::Like(expr)) => (Some(expr), None),
                    Some(tidb_ast::ShowErrorsFilter::Where(expr)) => (None, Some(expr)),
                };
                self.filter_warning_output(output, like, where_clause)
                    .map(Some)
            }
            // Go `ShowExec.fetchShowProcessList`: one row per live
            // connection of this server, read from the session manager.
            tidb_ast::AdminStmt::ShowInspection(show) => {
                if show.kind == tidb_ast::ShowInspectionKind::SessionStates {
                    return self.show_session_states().map(Some);
                }
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
                // `SHOW BINARY LOG STATUS` is go's renamed `SHOW MASTER
                // STATUS` (`fetchShowMasterStatus`): the same
                // `('tidb-binlog', position, '', '', '')` row at the
                // session's current TSO.
                if show.kind == tidb_ast::ShowInspectionKind::BinaryLogStatus {
                    return Ok(Some(crate::show_admin::master_status_output(
                        self.current_tso().value(),
                    )));
                }
                // Go `ShowExec.fetchShowReplicaStatus` refuses outright:
                // `This version of TiDB doesn't yet support 'SHOW {REPLICA |
                // SLAVE} STATUS'` (1235).
                if matches!(
                    show.kind,
                    tidb_ast::ShowInspectionKind::ReplicaStatus
                ) {
                    return Err(DriverError::NotSupportedYet(
                        "SHOW {REPLICA | SLAVE} STATUS".into(),
                    ));
                }
                if show.kind == tidb_ast::ShowInspectionKind::Config {
                    let rows = self.cluster_config_table_rows(&[Default::default()])?;
                    let StmtOutput::Rows { columns, .. } = text_columns_output(&[
                        "Type", "Instance", "Name", "Value",
                    ]) else { unreachable!() };
                    let (like_pattern, where_clause) = match show.filter.as_ref() {
                        None => (None, None),
                        Some(tidb_ast::ShowInspectionFilter::Like(expr)) => {
                            let value = datum_text(&self.eval_value(expr)?);
                            (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                        }
                        Some(tidb_ast::ShowInspectionFilter::Where(expr)) => (None, Some(expr)),
                    };
                    return self
                        .filter_show_output(
                            StmtOutput::Rows { columns, rows },
                            like_pattern,
                            where_clause,
                        )
                        .map(Some);
                }
                if show.kind == tidb_ast::ShowInspectionKind::Affinity {
                    return self.show_affinity_stmt(show.filter.as_ref()).map(Some);
                }
                if let Some(output) = crate::show_admin::inspection_output(show.kind) {
                    return Ok(Some(output));
                }
                // Go `ShowExec.fetchShowStatsMeta` (`executor/show_stats.go:36`).
                if show.kind == tidb_ast::ShowInspectionKind::StatsMeta {
                    self.require_show_stats_privilege("stats_meta")?;
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
                            "HistogramsInFlight".into(),
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        )],
                        rows: vec![tidb_executor::show_stats::histograms_in_flight_row(count)],
                    };
                    return self
                        .filter_show_output(output, like_pattern, where_clause)
                        .map(Some);
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
            // Go `RecoverIndexExec` / `CleanupIndexExec`.
            tidb_ast::AdminStmt::AdminRecoverIndex(recover) => {
                self.admin_recover_index_stmt(recover.as_ref()).map(Some)
            }
            tidb_ast::AdminStmt::AdminCleanupIndex(cleanup) => {
                self.admin_cleanup_index_stmt(cleanup.as_ref()).map(Some)
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
                    let (reported, charset, placement) = self.with_catalog_mut(|catalog| {
                        let (reported, charset) =
                            catalog.database_definition(database).ok_or_else(|| {
                                DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                                    database.clone(),
                                ))
                            })?;
                        let placement = catalog
                            .database_placement_policy(database)
                            .map(|reference| reference.name.original().to_owned());
                        Ok((reported, charset, placement))
                    })?;
                    return Ok(Some(crate::show_create_database::output(
                        reported,
                        charset,
                        placement.as_deref(),
                        *if_not_exists,
                        self.vars.sql_mode().has_ansi_quotes_mode(),
                    )));
                }
                // Go `fetchShowCreateResourceGroup`: the group by name
                // (`ErrResourceGroupNotExists` when absent), reported under
                // the name as written beside `CREATE RESOURCE GROUP`
                // followed by its settings.
                if *kind == tidb_ast::ShowCreateKind::ResourceGroup {
                    let [group_name] = name.as_slice() else {
                        return Err(DriverError::unsupported("empty resource group name"));
                    };
                    let text = self.with_catalog_mut(|catalog| {
                        let group = catalog.resource_group(group_name).ok_or_else(|| {
                            crate::resource_group::resource_group_not_exists(group_name)
                        })?;
                        let settings = group
                            .settings
                            .as_ref()
                            .map(|settings| settings.to_string())
                            .unwrap_or_default();
                        Ok(format!(
                            "CREATE RESOURCE GROUP `{}` {settings}",
                            group.name.original()
                        ))
                    })?;
                    let field_type =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar);
                    return Ok(Some(StmtOutput::Rows {
                        columns: vec![
                            ("Resource_Group".to_owned(), field_type.clone()),
                            ("Create Resource Group".to_owned(), field_type),
                        ],
                        rows: vec![vec![
                            Datum::Bytes(group_name.clone().into_bytes()),
                            Datum::Bytes(text.into_bytes()),
                        ]],
                    }));
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
                // whether the session-visible object IS a sequence, not from
                // the keyword written, and `fetchShowCreateTable` prints a
                // sequence's own DDL (`constructResultOfShowCreateTable`'s
                // `IsSequence` arm). `SHOW CREATE SEQUENCE` reads the
                // persistent object, which a local temporary table of the
                // same name does not shadow, and refuses anything that is not
                // a sequence (`fetchShowCreateSequence`'s ErrWrongObject).
                let want_view = match kind {
                    tidb_ast::ShowCreateKind::Table | tidb_ast::ShowCreateKind::Sequence => false,
                    tidb_ast::ShowCreateKind::View => true,
                    _ => return Ok(None),
                };
                let want_sequence = matches!(kind, tidb_ast::ShowCreateKind::Sequence);
                let (database, table_name) = match name.as_slice() {
                    // go resolves a QUALIFIED name without consulting the
                    // session database: `SHOW INDEX FROM d3p.t2` answers with
                    // no `USE` in sight. Only the bare form needs one, and go
                    // answers 1046 (`ErrNoDB`) for it.
                    [table] => (self.require_current_database()?.to_owned(), table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                self.require_show_create_privilege(&database, &table_name)?;
                let ctx = self.statement_context(false);
                // A view answers either spelling with the same row, which
                // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
                // table is refused.
                let ansi_quotes = self.vars.sql_mode().has_ansi_quotes_mode();
                // The header follows the object the session sees.
                let visible_is_sequence = self.with_catalog_mut(|catalog| {
                    Ok(matches!(
                        catalog.table_in(&database, &table_name),
                        Some(tidb_executor::TableEntry::Sequence(_))
                    ))
                })?;
                // Go `preprocess.go` detaches the LOCAL temporary tables for
                // `SHOW CREATE VIEW` as for `SHOW CREATE SEQUENCE`: a
                // temporary table of the same name does not shadow either.
                let include_local = !want_sequence && !want_view;
                let shown = self.with_catalog_scope(include_local, |catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    match entry {
                        // Go prints the object's stored name
                        // (`TableInfo.Name.O`), not the name as written.
                        tidb_executor::TableEntry::View(view) => Ok((
                            show_create_view_text(view, ansi_quotes),
                            view.name.clone(),
                            Some((
                                view.character_set_client.clone(),
                                view.collation_connection.clone(),
                            )),
                        )),
                        _ if want_view => Err(DriverError::Schema(SchemaErrorKind::WrongObject {
                            name: format!("{database}.{table_name}"),
                            expected: "VIEW",
                        })),
                        tidb_executor::TableEntry::Sequence(sequence) => Ok((
                            tidb_executor::show_create_sequence(sequence),
                            sequence.name.clone(),
                            None,
                        )),
                        _ if want_sequence => {
                            Err(DriverError::Schema(SchemaErrorKind::WrongObject {
                                name: format!("{database}.{table_name}"),
                                expected: "SEQUENCE",
                            }))
                        }
                        tidb_executor::TableEntry::Kv(table) => {
                            let stored = if table.name().is_empty() {
                                table_name.as_str()
                            } else {
                                table.name()
                            };
                            Ok((
                                show_create_table_text(&database, stored, table, &ctx)?,
                                stored.to_owned(),
                                None,
                            ))
                        }
                        tidb_executor::TableEntry::Mem(_) => Err(DriverError::unsupported(
                            "SHOW CREATE TABLE needs a storage-backed table",
                        )),
                    }
                });
                let (text, reported, view_charset) = shown?;
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
                let (name_column, text_column) = if visible_is_sequence {
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
                let (database, table_name) = match show.table.as_slice() {
                    // go resolves a QUALIFIED name without consulting the
                    // session database: `SHOW INDEX FROM d3p.t2` answers with
                    // no `USE` in sight. Only the bare form needs one, and go
                    // answers 1046 (`ErrNoDB`) for it.
                    [table] => (self.require_current_database()?.to_owned(), table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                let ids = self.with_catalog_mut(|catalog| {
                    // Go `ShowNextRowIDExec` reads the domain's infoschema,
                    // which holds no local temporary table.
                    let Some(entry) = catalog.persistent_table_in(&database, &table_name) else {
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
            // Go `fetchShowTableRegions` / `fetchShowDistributions`.
            tidb_ast::AdminStmt::ShowTablePlacement(show) => {
                let (database, table_name) = match show.table.as_slice() {
                    [table] => (self.require_current_database()?.to_owned(), table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::unsupported("empty table name")),
                };
                if show.kind == tidb_ast::ShowTablePlacementKind::Distributions {
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::show_regions::show_table_distributions(
                            catalog,
                            &database,
                            &table_name,
                            &show.partitions,
                        )
                    })?;
                    return Ok(None);
                }
                let rows = self.with_catalog_mut(|catalog| {
                    tidb_executor::show_regions::show_table_regions(
                        catalog,
                        &database,
                        &table_name,
                        &show.partitions,
                        show.index.as_deref(),
                    )
                })?;
                let output = StmtOutput::Rows {
                    columns: tidb_executor::show_regions::table_region_columns(),
                    rows,
                };
                let where_clause = match &show.filter {
                    Some(tidb_ast::ShowInspectionFilter::Where(expr)) => Some(expr),
                    _ => None,
                };
                self.filter_show_output(output, None, where_clause).map(Some)
            }
            // Go `fetchShowColumns`.
            tidb_ast::AdminStmt::ShowColumns(show) => {
                let (like_pattern, where_clause) = match &show.filter {
                    None => (None, None),
                    Some(tidb_ast::ShowColumnsFilter::Like(expr)) => {
                        let value = datum_text(&self.eval_value(expr)?);
                        (Some(ShowLikePattern::from_expr(expr, value, true)), None)
                    }
                    Some(tidb_ast::ShowColumnsFilter::Where(expr)) => (None, Some(expr)),
                };
                let database = self.show_columns_database(show.database.as_deref(), &show.table)?;
                let output = self.show_columns(&database, &show.table, None, show.full, show.extended)?;
                self.filter_show_output(output, like_pattern, where_clause)
                    .map(Some)
            }
            // Go's parser rewrites `DESCRIBE tbl [col]` into a SHOW
            // COLUMNS statement; this parser keeps a node of its own, so
            // the same output is produced from it here.
            tidb_ast::AdminStmt::DescribeTable(describe) => {
                let database = self.show_columns_database(None, &describe.table)?;
                let column = describe.column.as_ref().and_then(|path| path.last());
                self.show_columns(
                    &database,
                    &describe.table,
                    column.map(String::as_str),
                    false,
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
                    None => self.require_current_database()?.into(),
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
                let like_predicate = match &like_pattern {
                    Some(pattern) => pattern.row_predicate(&column_names)?,
                    None => None,
                };
                let is_virtual_schema = [
                    "information_schema",
                    "performance_schema",
                    "metrics_schema",
                ]
                .iter()
                .any(|schema| database.eq_ignore_ascii_case(schema));
                let mut rows = Vec::with_capacity(listed.len());
                for (name, is_view, is_sequence) in listed {
                    let mut row = vec![Datum::Bytes(name.into_bytes())];
                    if full {
                        row.push(Datum::Bytes(
                            table_type_of(is_virtual_schema, is_view, is_sequence)
                                .as_bytes()
                                .to_vec(),
                        ));
                    }
                    if let Some(predicate) = &like_predicate {
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
                self.filter_show_output(StmtOutput::Rows { columns, rows }, None, where_clause)
                    .map(Some)
            }
            // Go `SimpleExec.executeShutdown` + `asyncDelayShutdown`
            // (pkg/executor/simple.go:3584-3611): the statement answers
            // success immediately, and a 1-second-delayed task SIGTERMs this
            // very process, whose installed handler performs the graceful
            // stop. `RESTART`/`HELP` keep their unsupported report.
            tidb_ast::AdminStmt::ServerControl(control) => match &**control {
                tidb_ast::ServerControlStmt::Shutdown => {
                    std::thread::spawn(|| {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        signal_hook::low_level::raise(signal_hook::consts::SIGTERM);
                    });
                    Ok(Some(StmtOutput::Affected(0)))
                }
                // Go's executor builder names the AST node of the statements
                // it has no executor for (`Unsupported type *ast.HelpStmt`,
                // 8108).
                tidb_ast::ServerControlStmt::Restart => Err(DriverError::DdlCoded {
                    errno: 8108,
                    message: "Unsupported type *ast.RestartStmt".to_owned(),
                }),
                tidb_ast::ServerControlStmt::Help(_) => Err(DriverError::DdlCoded {
                    errno: 8108,
                    message: "Unsupported type *ast.HelpStmt".to_owned(),
                }),
            },
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

/// Go `buildAdmin`'s statements that reach its trailing SUPER visit: every
/// ADMIN statement but the reloads, plugins, binding operations and
/// FLUSH PLAN CACHE it returns before that line.
fn admin_needs_super(admin: &tidb_ast::AdminStmt) -> bool {
    matches!(
        admin,
        tidb_ast::AdminStmt::AdminCheck(_)
            | tidb_ast::AdminStmt::AdminChecksum(_)
            | tidb_ast::AdminStmt::AdminRecoverIndex(_)
            | tidb_ast::AdminStmt::AdminCleanupIndex(_)
            | tidb_ast::AdminStmt::ShowNextRowId(_)
            | tidb_ast::AdminStmt::ShowDdl
            | tidb_ast::AdminStmt::ShowDdlJobs(_)
            | tidb_ast::AdminStmt::ShowDdlJobQueries(_)
            | tidb_ast::AdminStmt::DdlJobControl(_)
            | tidb_ast::AdminStmt::AlterDdlJobs(_)
            | tidb_ast::AdminStmt::ShowSlow(_)
            | tidb_ast::AdminStmt::SetBdrRole(_)
            | tidb_ast::AdminStmt::UnsetBdrRole
            | tidb_ast::AdminStmt::ShowBdrRole
            | tidb_ast::AdminStmt::CreateWorkloadSnapshot
    )
}
