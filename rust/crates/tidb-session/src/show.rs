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

use crate::*;

/// Go `stringutil.Escape` with a non-ANSI_QUOTES sql_mode: backtick-quoted,
/// with an embedded backtick doubled.
fn escape_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go's `TABLE_TYPE` / `Table_type` value for an object.
fn table_type_of(is_view: bool) -> &'static str {
    if is_view {
        "VIEW"
    } else {
        "BASE TABLE"
    }
}

/// Go `ConstructResultOfShowCreateView`.
///
/// Go always prints the full preamble, including the defaults the statement
/// never wrote, and always prints an explicit column list even when the
/// `CREATE VIEW` had none -- the names come from the stored definition.
///
/// DIVERGENCE (documented): the definer is whatever the statement recorded,
/// which in this tier is the empty identity, printed as ``@``. A TiDB with
/// authentication prints the connected user there.
fn show_create_view_text(view: &tidb_executor::ViewDef) -> String {
    let mut out = format!(
        "CREATE ALGORITHM={} DEFINER={}@{} SQL SECURITY {} VIEW {} (",
        view.algorithm,
        escape_name(&view.definer_user),
        escape_name(&view.definer_host),
        view.security,
        escape_name(&view.name),
    );
    for (index, (name, _)) in view.columns.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&escape_name(name));
    }
    out.push_str(") AS ");
    out.push_str(&view.select_sql);
    out
}

/// Go `constructResultOfShowCreateTable`, over the metadata this seed keeps.
///
/// The shape is Go's line for line: the header, two-space-indented column
/// clauses separated by ",\n", the clustered primary key when the handle is
/// one, then the indexes, then the closing paren with the engine and charset.
///
/// NOT MODELLED (documented, and each one rejected at DDL time so no table can
/// carry it): generated columns, AUTO_INCREMENT, AUTO_RANDOM, ON UPDATE
/// CURRENT_TIMESTAMP, column and index comments, foreign keys, check
/// constraints, partitioning, temporary tables, views and sequences.
/// Per-column CHARACTER SET / COLLATE clauses are omitted because this seed
/// stores no per-column charset, so every column takes the table's.
fn show_create_table_text(name: &str, table: &tidb_executor::KvTable) -> String {
    let mut out = format!("CREATE TABLE {} (\n", escape_name(name));
    let mut clauses: Vec<String> = Vec::with_capacity(table.columns.len() + 1);

    for (offset, column) in table.columns.iter().enumerate() {
        let mut clause = format!(
            "  {} {}",
            escape_name(&column.name),
            column.field_type.compact_str(false)
        );
        let not_null = column.field_type.flags() & NOT_NULL_FLAG != 0;
        if table.auto_increment_offset() == Some(offset) {
            // Go writes the pair together for an auto column and prints no
            // default for it.
            clause.push_str(" NOT NULL AUTO_INCREMENT");
            clauses.push(clause);
            continue;
        }
        if not_null {
            clause.push_str(" NOT NULL");
        }
        // Go prints nothing for a column carrying NoDefaultValueFlag, which is
        // a NOT NULL column with no DEFAULT clause; a nullable column with no
        // DEFAULT reports DEFAULT NULL, as MySQL does.
        match &column.default_value {
            Some(Datum::Null) => clause.push_str(" DEFAULT NULL"),
            Some(value) => {
                // Go quotes every non-bit default, integers included.
                let text = datum_text(value).unwrap_or_default();
                clause.push_str(&format!(" DEFAULT '{text}'"));
            }
            None if !not_null => clause.push_str(" DEFAULT NULL"),
            None => {}
        }
        clauses.push(clause);
    }

    // Go emits a clustered primary key here, because a clustered key -- an
    // int handle or a common handle -- is not in the index list.
    let clustered: Vec<usize> = match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None => table.common_handle_offsets().to_vec(),
    };
    if !clustered.is_empty() {
        let columns = clustered
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        clauses.push(format!(
            "  PRIMARY KEY ({columns}) /*T![clustered_index] CLUSTERED */"
        ));
    }

    for index in table.indexes() {
        let columns = index
            .column_offsets
            .iter()
            .map(|offset| escape_name(&table.columns[*offset].name))
            .collect::<Vec<_>>()
            .join(",");
        if index.name.eq_ignore_ascii_case("PRIMARY") {
            // A primary key that is not the handle is non-clustered here,
            // since this seed builds no clustered common handle.
            clauses.push(format!(
                "  PRIMARY KEY ({columns}) /*T![clustered_index] NONCLUSTERED */"
            ));
        } else if index.unique {
            clauses.push(format!(
                "  UNIQUE KEY {} ({columns})",
                escape_name(&index.name)
            ));
        } else {
            clauses.push(format!("  KEY {} ({columns})", escape_name(&index.name)));
        }
    }

    out.push_str(&clauses.join(",\n"));
    out.push_str(&format!(
        "\n) ENGINE=InnoDB DEFAULT CHARSET={TABLE_CHARSET} COLLATE={TABLE_COLLATE}"
    ));
    out
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
/// `Extra` reports `auto_increment` for the auto column.
///
/// NOT MODELLED (documented): the other `Extra` values -- ON UPDATE
/// CURRENT_TIMESTAMP and the generated-column markers -- because those column
/// kinds are rejected at DDL time, so no column can carry them.
fn column_description(
    column: &tidb_executor::KvColumn,
    offset: usize,
    table: &tidb_executor::KvTable,
    full: bool,
) -> Vec<Datum> {
    let null_flag = if column.field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    // Go `NewColDesc`: an auto-increment column reports auto_increment.
    let extra = if table.auto_increment_offset() == Some(offset) {
        "auto_increment"
    } else {
        ""
    };
    let key_flag = column_key_flag(table, offset);
    let default = match &column.default_value {
        Some(value) => match datum_text(value) {
            Some(text) => Datum::Bytes(text.into_bytes()),
            None => Datum::Null,
        },
        None => Datum::Null,
    };
    if !full {
        return vec![
            Datum::Bytes(column.name.clone().into_bytes()),
            Datum::Bytes(column.field_type.compact_str(false).into_bytes()),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.into_bytes()),
            default,
            Datum::Bytes(extra.as_bytes().to_vec()),
        ];
    }
    // Go `NewColDesc`: `Collation` is NULL for a non-string type (numerics,
    // temporals, ...), and the column's own collation name otherwise.
    //
    // NOT MODELLED (documented): a per-column charset/collation override.
    // This tier's DDL does not track one, so every string column reports the
    // schema default (`utf8mb4_bin`), which is what a plain `VARCHAR` column
    // with no explicit `CHARACTER SET`/`COLLATE` actually gets in Go too.
    let collation = if column.field_type.is_string() {
        Datum::Bytes(tidb_datatype::Collation::DEFAULT.name().as_bytes().to_vec())
    } else {
        Datum::Null
    };
    vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        Datum::Bytes(column.field_type.compact_str(false).into_bytes()),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.into_bytes()),
        default,
        Datum::Bytes(extra.as_bytes().to_vec()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()), // Comment: no per-column comments modelled.
    ]
}

/// A view column's `SHOW COLUMNS` row.
///
/// A view carries no storage metadata, so Go reports no key, no default and
/// no extra for every one of its columns; only the name, the type the body
/// produced, and nullability come from the definition. The body's columns are
/// nullable here because nothing propagates a base column's NOT NULL through
/// the view's stored types, which is what Go reports for these views too.
fn view_column_description(
    name: &str,
    field_type: &tidb_datatype::FieldType,
    full: bool,
) -> Vec<Datum> {
    let null_flag = if field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    if !full {
        return vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            Datum::Bytes(field_type.compact_str(false).into_bytes()),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(Vec::new()),
            Datum::Null,
            Datum::Bytes(Vec::new()),
        ];
    }
    let collation = if field_type.is_string() {
        Datum::Bytes(tidb_datatype::Collation::DEFAULT.name().as_bytes().to_vec())
    } else {
        Datum::Null
    };
    vec![
        Datum::Bytes(name.as_bytes().to_vec()),
        Datum::Bytes(field_type.compact_str(false).into_bytes()),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
        Datum::Null,
        Datum::Bytes(Vec::new()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
    ]
}

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;

/// Go `NewColDesc`'s key flag, shared by `SHOW COLUMNS` and
/// `information_schema.COLUMNS`: PRI for a primary key, UNI for a column that
/// is the whole of a unique index, MUL for one that leads a non-unique index.
pub(crate) fn column_key_flag(table: &tidb_executor::KvTable, offset: usize) -> String {
    let is_handle =
        table.pk_handle_offset() == Some(offset) || table.common_handle_offsets().contains(&offset);
    if is_handle
        || table.indexes().iter().any(|index| {
            index.name.eq_ignore_ascii_case("PRIMARY") && index.column_offsets == [offset]
        })
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

/// The `SHOW TABLE STATUS` header, with the columns Go reports as numbers
/// marked.
const SHOW_TABLE_STATUS_COLUMNS: &[(&str, bool)] = &[
    ("Name", false),
    ("Engine", false),
    ("Version", true),
    ("Row_format", false),
    ("Rows", true),
    ("Avg_row_length", true),
    ("Data_length", true),
    ("Max_data_length", true),
    ("Index_length", true),
    ("Data_free", true),
    ("Auto_increment", true),
    ("Create_time", false),
    ("Update_time", false),
    ("Check_time", false),
    ("Collation", false),
    ("Checksum", false),
    ("Create_options", false),
    ("Comment", false),
];

fn show_table_status_row(name: &str, auto_increment: Option<i64>) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    vec![
        text(name),
        text("InnoDB"),
        Datum::Int(10),
        text("Compact"),
        Datum::Int(0), // Rows
        Datum::Int(0), // Avg_row_length
        Datum::Int(0), // Data_length
        Datum::Int(0), // Max_data_length
        Datum::Int(0), // Index_length
        Datum::Int(0), // Data_free
        match auto_increment {
            Some(next) => Datum::Int(next),
            None => Datum::Null,
        },
        Datum::Null, // Create_time: no per-table creation timestamp here.
        Datum::Null, // Update_time
        Datum::Null, // Check_time
        text(TABLE_COLLATE),
        text(""), // Checksum
        text(""), // Create_options
        text(""), // Comment
    ]
}

/// One `SHOW TABLE STATUS` row for a view. Captured from Go: a view answers
/// its name, NULL for every storage cell -- engine, version, row format,
/// counts, sizes, collation and create options alike -- an empty `Checksum`,
/// and the literal `VIEW` as its comment, which is how the two kinds of
/// object are told apart in this output.
fn show_table_status_view_row(name: &str) -> Vec<Datum> {
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut row = vec![text(name)];
    // Engine through Auto_increment: ten cells a view has no value for.
    row.extend(std::iter::repeat_n(Datum::Null, 10));
    // Create_time, which Go fills and this tier has no source for, then
    // Update_time, Check_time and Collation, which are NULL for a view in Go
    // too.
    row.extend(std::iter::repeat_n(Datum::Null, 4));
    row.push(text("")); // Checksum
    row.push(Datum::Null); // Create_options
    row.push(text("VIEW")); // Comment
    row
}

/// `SHOW CHARSET` rows: `(Charset, Description, Default collation, Maxlen)`,
/// captured verbatim from mock TiDB's `charset.GetSupportedCharsets`. Order
/// matches the capture (alphabetical by charset name).
const SHOW_CHARSET_ROWS: &[(&str, &str, &str, i64)] = &[
    ("ascii", "US ASCII", "ascii_bin", 1),
    ("binary", "binary", "binary", 1),
    (
        "gb18030",
        "China National Standard GB18030",
        "gb18030_chinese_ci",
        4,
    ),
    (
        "gbk",
        "Chinese Internal Code Specification",
        "gbk_chinese_ci",
        2,
    ),
    ("latin1", "Latin1", "latin1_bin", 1),
    ("utf8", "UTF-8 Unicode", "utf8_bin", 3),
    ("utf8mb4", "UTF-8 Unicode", "utf8mb4_bin", 4),
];

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
/// This is NOT the same as [`tidb_datatype::Charset::default_collation`]:
/// mock TiDB's capture shows `gbk_chinese_ci`/`gb18030_chinese_ci` as the
/// default for their charsets, not the `_bin` collations that method
/// returns, so the SHOW COLLATION default is listed explicitly here rather
/// than derived from it.
fn is_default_show_collation(collation: tidb_datatype::Collation) -> bool {
    matches!(
        collation,
        tidb_datatype::Collation::AsciiBin
            | tidb_datatype::Collation::Binary
            | tidb_datatype::Collation::Gb18030ChineseCi
            | tidb_datatype::Collation::GbkChineseCi
            | tidb_datatype::Collation::Latin1Bin
            | tidb_datatype::Collation::Utf8Bin
            | tidb_datatype::Collation::Utf8Mb4Bin
    )
}

/// The `SHOW INDEX` header, with the columns Go reports as numbers marked.
const SHOW_INDEX_COLUMNS: &[(&str, bool)] = &[
    ("Table", false),
    ("Non_unique", true),
    ("Key_name", false),
    ("Seq_in_index", true),
    ("Column_name", false),
    ("Collation", false),
    ("Cardinality", true),
    ("Sub_part", true),
    ("Packed", false),
    ("Null", false),
    ("Index_type", false),
    ("Comment", false),
    ("Index_comment", false),
    ("Visible", false),
    ("Expression", false),
    ("Clustered", false),
    ("Global", false),
];

/// One `SHOW INDEX` row per index column, in Go's own order: the clustered
/// primary key first, then each index in definition order.
fn show_index_rows(table_name: &str, table: &tidb_executor::KvTable) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    let text = |value: &str| Datum::Bytes(value.as_bytes().to_vec());
    let mut push = |key_name: &str,
                    unique: bool,
                    clustered: bool,
                    sequence: usize,
                    column: &str,
                    nullable: bool| {
        rows.push(vec![
            text(table_name),
            Datum::Int(i64::from(!unique)),
            text(key_name),
            Datum::Int(sequence as i64),
            text(column),
            text("A"),
            // No statistics tier, so Go's estimate is simply absent.
            Datum::Int(0),
            Datum::Null,
            Datum::Null,
            text(if nullable { "YES" } else { "" }),
            text("BTREE"),
            text(""),
            text(""),
            text("YES"),
            Datum::Null,
            text(if clustered { "YES" } else { "NO" }),
            text("NO"),
        ]);
    };
    // The clustered primary key is not in the index list, the same way
    // SHOW CREATE TABLE prints it separately.
    if let Some(offset) = table.pk_handle_offset() {
        push("PRIMARY", true, true, 1, &table.columns[offset].name, false);
    }
    for index in table.indexes() {
        let clustered =
            index.name.eq_ignore_ascii_case("PRIMARY") && !table.common_handle_offsets().is_empty();
        for (position, offset) in index.column_offsets.iter().enumerate() {
            let column = &table.columns[*offset];
            let nullable = column.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0;
            push(
                &index.name,
                index.unique,
                clustered,
                position + 1,
                &column.name,
                nullable,
            );
        }
    }
    rows
}

/// The column names a `SHOW VARIABLES` row carries, which its `WHERE` filter
/// resolves against.
const SHOW_VARIABLE_COLUMNS: &[&str; 2] = &["Variable_name", "Value"];

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
            _ => return Err(DriverError::Unsupported("empty table name")),
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
                return Err(DriverError::Unsupported(
                    "SHOW COLUMNS needs a storage-backed table",
                ));
            };
            Ok(table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, candidate)| {
                    column.is_none_or(|name| candidate.name.eq_ignore_ascii_case(name))
                })
                .map(|(offset, candidate)| column_description(candidate, offset, table, full))
                .collect::<Vec<_>>())
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
            tidb_ast::AdminStmt::Grant(grant) => Ok(Some(self.grant_stmt(grant)?)),
            tidb_ast::AdminStmt::Revoke(revoke) => Ok(Some(self.revoke_stmt(revoke)?)),
            tidb_ast::AdminStmt::ShowGrants(show) => Ok(Some(self.show_grants_stmt(show)?)),
            tidb_ast::AdminStmt::GrantRole(grant) => Ok(Some(self.grant_role_stmt(grant)?)),
            tidb_ast::AdminStmt::RevokeRole(revoke) => Ok(Some(self.revoke_role_stmt(revoke)?)),
            tidb_ast::AdminStmt::ShowDatabases(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW DATABASES filters are not supported yet",
                    ));
                }
                let names = self.with_catalog_mut(|catalog| Ok(catalog.database_names()))?;
                Ok(Some(string_column_output("Database", names)))
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
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowTableStatusFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowTableStatusFilter::Like(_)) => {
                        return Err(DriverError::Unsupported(
                            "SHOW TABLE STATUS LIKE takes a string pattern",
                        ))
                    }
                    Some(tidb_ast::ShowTableStatusFilter::Where(_)) | None => None,
                };
                let where_clause = match &show.filter {
                    Some(tidb_ast::ShowTableStatusFilter::Where(expr)) => Some(expr.clone()),
                    _ => None,
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let mut rows = Vec::new();
                    let names = catalog.table_names(&database).ok_or_else(|| {
                        DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                    })?;
                    for name in names {
                        if let Some(pattern) = &pattern {
                            if !tidb_executor::like_match_with_collation(
                                &name,
                                pattern,
                                None,
                                tidb_datatype::Collation::Utf8Mb4Bin,
                            ) {
                                continue;
                            }
                        }
                        let entry = catalog.table_in(&database, &name);
                        let auto_increment = match entry {
                            Some(tidb_executor::TableEntry::Kv(table)) => {
                                table.next_auto_increment()
                            }
                            _ => None,
                        };
                        let row = if entry.is_some_and(tidb_executor::TableEntry::is_view) {
                            show_table_status_view_row(&name)
                        } else {
                            show_table_status_row(&name, auto_increment)
                        };
                        if let Some(predicate) = &where_clause {
                            if !show_row_matches(
                                predicate,
                                &SHOW_TABLE_STATUS_COLUMNS
                                    .iter()
                                    .map(|(name, _)| *name)
                                    .collect::<Vec<_>>(),
                                &row,
                            )? {
                                continue;
                            }
                        }
                        rows.push(row);
                    }
                    Ok(rows)
                })?;
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let columns = SHOW_TABLE_STATUS_COLUMNS
                    .iter()
                    .map(|(name, numeric)| {
                        ((*name).to_owned(), if *numeric { number() } else { text() })
                    })
                    .collect();
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            // Go `fetchShowIndex`: one row per index COLUMN, ordered
            // with the clustered primary key first, then the table's own
            // indexes in definition order.
            //
            // NOT MODELLED, and each reported the way Go reports an
            // absent value rather than invented: Cardinality is 0 (no
            // statistics tier), Sub_part and Packed are NULL (no prefix
            // or packed indexes here), Comment/Index_comment are empty,
            // Expression is NULL (no expression indexes), and Global is
            // NO (no partitioned global indexes).
            tidb_ast::AdminStmt::ShowIndex(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW INDEX filters are not supported yet",
                    ));
                }
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match show.table.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::Unsupported("empty table name")),
                };
                let rows = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    let tidb_executor::TableEntry::Kv(table) = entry else {
                        return Err(DriverError::Unsupported(
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
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            // Go `ShowExec` with `ShowVariables`: one row per variable,
            // as `Variable_name` and `Value`, filtered by LIKE.
            //
            // DEFERRED (documented): the GLOBAL/SESSION distinction,
            // which reads the same value here because this tier keeps no
            // persisted global tier (`SET GLOBAL` already documents it).
            tidb_ast::AdminStmt::ShowVariables(show) => {
                let pattern = match &show.like {
                    Some(tidb_ast::Expr::String(text)) => Some(text.clone()),
                    Some(_) => {
                        return Err(DriverError::Unsupported(
                            "SHOW VARIABLES LIKE takes a string pattern",
                        ))
                    }
                    None => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let mut rows = Vec::new();
                for definition in sysvar::SYS_VARS {
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
                    let value = self
                        .vars
                        .get_system(definition.name)
                        .unwrap_or_else(|_| definition.value.to_owned());
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
                        return Err(DriverError::Unsupported(
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
                for &(name, value, session_only) in SHOW_STATUS_VARS {
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
            //
            // DEFERRED (documented, and refused rather than ignored):
            // `WHERE`, because honoring it needs the same virtual-row
            // selection machinery `SHOW STATUS` uses and this table is
            // static rather than session state.
            tidb_ast::AdminStmt::ShowCharset(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCharsetFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowCharsetFilter::Like(_)) => {
                        return Err(DriverError::Unsupported(
                            "SHOW CHARSET LIKE takes a string pattern",
                        ))
                    }
                    Some(tidb_ast::ShowCharsetFilter::Where(_)) => {
                        return Err(DriverError::Unsupported(
                            "SHOW CHARSET WHERE is not supported yet",
                        ))
                    }
                    None => None,
                };
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                let number =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                let mut rows = Vec::new();
                for &(name, description, default_collation, maxlen) in SHOW_CHARSET_ROWS {
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
                    rows.push(vec![
                        Datum::Bytes(name.as_bytes().to_vec()),
                        Datum::Bytes(description.as_bytes().to_vec()),
                        Datum::Bytes(default_collation.as_bytes().to_vec()),
                        Datum::Int(maxlen),
                    ]);
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
            //
            // DEFERRED (documented, refused rather than ignored): `WHERE`
            // /`LIKE`, for the same reason as `SHOW CHARSET` above --
            // there is exactly one row and no virtual-row selection path
            // wired up for it yet.
            tidb_ast::AdminStmt::ShowEngines(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW ENGINES filters are not supported yet",
                    ));
                }
                let text =
                    || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Engine".to_owned(), text()),
                        ("Support".to_owned(), text()),
                        ("Comment".to_owned(), text()),
                        ("Transactions".to_owned(), text()),
                        ("XA".to_owned(), text()),
                        ("Savepoints".to_owned(), text()),
                    ],
                    rows: vec![vec![
                        Datum::Bytes(b"InnoDB".to_vec()),
                        Datum::Bytes(b"DEFAULT".to_vec()),
                        Datum::Bytes(
                            b"Supports transactions, row-level locking, and foreign keys".to_vec(),
                        ),
                        Datum::Bytes(b"YES".to_vec()),
                        Datum::Bytes(b"YES".to_vec()),
                        Datum::Bytes(b"YES".to_vec()),
                    ]],
                }))
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
            //
            // DEFERRED (documented, and refused rather than ignored):
            // `WHERE`, for the same reason as `SHOW CHARSET` above.
            tidb_ast::AdminStmt::ShowCollation(show) => {
                let pattern = match &show.filter {
                    Some(tidb_ast::ShowCollationFilter::Like(tidb_ast::Expr::String(text))) => {
                        Some(text.clone())
                    }
                    Some(tidb_ast::ShowCollationFilter::Like(_)) => {
                        return Err(DriverError::Unsupported(
                            "SHOW COLLATION LIKE takes a string pattern",
                        ))
                    }
                    Some(tidb_ast::ShowCollationFilter::Where(_)) => {
                        return Err(DriverError::Unsupported(
                            "SHOW COLLATION WHERE is not supported yet",
                        ))
                    }
                    None => None,
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
                    rows.push(vec![
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
                    ]);
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
            tidb_ast::AdminStmt::ShowWarnings(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW WARNINGS filters are not supported yet",
                    ));
                }
                Ok(Some(self.warning_output(show.count_only, false)))
            }
            tidb_ast::AdminStmt::ShowErrors(show) => {
                if show.filter.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW ERRORS filters are not supported yet",
                    ));
                }
                Ok(Some(self.warning_output(show.count_only, true)))
            }
            // Go `ShowExec.fetchShowProcessList`: one row per live
            // connection of this server, read from the session manager.
            tidb_ast::AdminStmt::ShowInspection(show) => {
                if show.kind != tidb_ast::ShowInspectionKind::ProcessList {
                    return Ok(None);
                }
                if show.filter.is_some() || show.database.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW PROCESSLIST filters are not supported yet",
                    ));
                }
                Ok(Some(self.process_list_output(show.full)))
            }
            // Go `SimpleExec.executeKillStmt`.
            tidb_ast::AdminStmt::Kill(kill) => self.kill_stmt(kill),
            // Go `fetchShowCreateTable`.
            tidb_ast::AdminStmt::ShowCreate { kind, name, .. } => {
                let want_view = match kind {
                    tidb_ast::ShowCreateKind::Table => false,
                    tidb_ast::ShowCreateKind::View => true,
                    _ => return Ok(None),
                };
                let current = self.require_current_database()?.to_owned();
                let (database, table_name) = match name.as_slice() {
                    [table] => (current, table.clone()),
                    [database, table] => (database.clone(), table.clone()),
                    _ => return Err(DriverError::Unsupported("empty table name")),
                };
                // A view answers either spelling with the same row, which
                // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
                // table is refused.
                let (text, reported, is_view) = self.with_catalog_mut(|catalog| {
                    let Some(entry) = catalog.table_in(&database, &table_name) else {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                            "{database}.{table_name}"
                        ))));
                    };
                    match entry {
                        tidb_executor::TableEntry::View(view) => {
                            Ok((show_create_view_text(view), table_name.clone(), true))
                        }
                        _ if want_view => Err(DriverError::Schema(SchemaErrorKind::NotView(
                            format!("{database}.{table_name}"),
                        ))),
                        tidb_executor::TableEntry::Kv(table) => Ok((
                            show_create_table_text(&table_name, table),
                            table_name.clone(),
                            false,
                        )),
                        tidb_executor::TableEntry::Mem(_) => Err(DriverError::Unsupported(
                            "SHOW CREATE TABLE needs a storage-backed table",
                        )),
                    }
                })?;
                let field_type =
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                // Go's view form carries its own header and the session's
                // character set and collation.
                if is_view {
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
                            Datum::Bytes(b"utf8mb4".to_vec()),
                            Datum::Bytes(b"utf8mb4_bin".to_vec()),
                        ]],
                    }));
                }
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        ("Table".to_owned(), field_type.clone()),
                        ("Create Table".to_owned(), field_type),
                    ],
                    rows: vec![vec![
                        Datum::Bytes(reported.into_bytes()),
                        Datum::Bytes(text.into_bytes()),
                    ]],
                }))
            }
            // Go `fetchShowColumns`.
            tidb_ast::AdminStmt::ShowColumns(show) => {
                if show.filter.is_some() || show.extended {
                    return Err(DriverError::Unsupported(
                        "SHOW EXTENDED COLUMNS and column filters are not supported yet",
                    ));
                }
                let database = match &show.database {
                    Some(name) => name.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                self.show_columns(&database, &show.table, None, show.full)
                    .map(Some)
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
                if show.filter.is_some() {
                    return Err(DriverError::Unsupported(
                        "SHOW TABLES filters are not supported yet",
                    ));
                }
                let database = match &show.database {
                    Some(name) => name.clone(),
                    None => self.require_current_database()?.to_owned(),
                };
                let full = show.full;
                let listed = self.with_catalog_mut(|catalog| {
                    Ok(catalog.table_names(&database).map(|names| {
                        names
                            .into_iter()
                            .map(|name| {
                                let is_view = catalog.is_view_in(&database, &name);
                                (name, is_view)
                            })
                            .collect::<Vec<_>>()
                    }))
                })?;
                let listed = listed.ok_or_else(|| {
                    DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
                })?;
                // Go names the column after the schema being listed.
                let name_column = format!("Tables_in_{database}");
                if !full {
                    return Ok(Some(string_column_output(
                        &name_column,
                        listed.into_iter().map(|(name, _)| name).collect(),
                    )));
                }
                // Go's `SHOW FULL TABLES` adds the object kind.
                let field_type =
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                Ok(Some(StmtOutput::Rows {
                    columns: vec![
                        (name_column, field_type.clone()),
                        ("Table_type".to_owned(), field_type),
                    ],
                    rows: listed
                        .into_iter()
                        .map(|(name, is_view)| {
                            vec![
                                Datum::Bytes(name.into_bytes()),
                                Datum::Bytes(table_type_of(is_view).as_bytes().to_vec()),
                            ]
                        })
                        .collect(),
                }))
            }
            _ => Ok(None),
        }
    }
}
