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

//! The `information_schema` virtual tables.
//!
//! Go builds these in `pkg/infoschema/tables.go` as memory tables whose rows
//! are computed from the schema state at query time; this does the same over
//! the catalog. Clients introspect through these rather than through `SHOW`,
//! so the column lists must match exactly -- a `SELECT *` that returns the
//! wrong arity breaks a client that reads by position.
//!
//! Every column list and value below was CAPTURED from a running TiDB by
//! querying the table and printing the rows, not transcribed from the Go
//! source: several values (`NOT_SHARDED(PK_IS_HANDLE)`, the per-type
//! `CHARACTER_OCTET_LENGTH`, `NUMERIC_PRECISION` 19 for bigint) are computed
//! in ways that reading the table definitions would not reveal.
//!
//! NOT MODELLED (documented): the statistics columns are reported as TiDB
//! reports them for a table it has not analyzed -- `TABLE_ROWS`,
//! `DATA_LENGTH` and friends are 0 and `CREATE_TIME` is NULL rather than a
//! fabricated timestamp; the other `information_schema` tables; and the
//! `mysql`, `performance_schema`, `sys` and `metrics_schema` databases, whose
//! contents are separate tiers.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::{Catalog, KvTable, TableEntry};

/// Go's schema name for the virtual database.
pub const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";

/// Go `infoschema.tableSchemataCols`.
const SCHEMATA_COLUMNS: &[(&str, bool)] = &[
    ("CATALOG_NAME", false),
    ("SCHEMA_NAME", false),
    ("DEFAULT_CHARACTER_SET_NAME", false),
    ("DEFAULT_COLLATION_NAME", false),
    ("SQL_PATH", false),
    ("TIDB_PLACEMENT_POLICY_NAME", false),
];

/// Go `infoschema.tableTablesCols`.
const TABLES_COLUMNS: &[(&str, bool)] = &[
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("TABLE_TYPE", false),
    ("ENGINE", false),
    ("VERSION", true),
    ("ROW_FORMAT", false),
    ("TABLE_ROWS", true),
    ("AVG_ROW_LENGTH", true),
    ("DATA_LENGTH", true),
    ("MAX_DATA_LENGTH", true),
    ("INDEX_LENGTH", true),
    ("DATA_FREE", true),
    ("AUTO_INCREMENT", true),
    ("CREATE_TIME", false),
    ("UPDATE_TIME", false),
    ("CHECK_TIME", false),
    ("TABLE_COLLATION", false),
    ("CHECKSUM", true),
    ("CREATE_OPTIONS", false),
    ("TABLE_COMMENT", false),
    ("TIDB_TABLE_ID", true),
    ("TIDB_ROW_ID_SHARDING_INFO", false),
    ("TIDB_PK_TYPE", false),
    ("TIDB_PLACEMENT_POLICY_NAME", false),
    ("TIDB_TABLE_MODE", false),
    ("TIDB_AFFINITY", false),
    ("TIDB_STORAGE_CLASS", false),
];

/// Go `infoschema.tableColumnsCols`.
const COLUMNS_COLUMNS: &[(&str, bool)] = &[
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("COLUMN_NAME", false),
    ("ORDINAL_POSITION", true),
    ("COLUMN_DEFAULT", false),
    ("IS_NULLABLE", false),
    ("DATA_TYPE", false),
    ("CHARACTER_MAXIMUM_LENGTH", true),
    ("CHARACTER_OCTET_LENGTH", true),
    ("NUMERIC_PRECISION", true),
    ("NUMERIC_SCALE", true),
    ("DATETIME_PRECISION", true),
    ("CHARACTER_SET_NAME", false),
    ("COLLATION_NAME", false),
    ("COLUMN_TYPE", false),
    ("COLUMN_KEY", false),
    ("EXTRA", false),
    ("PRIVILEGES", false),
    ("COLUMN_COMMENT", false),
    ("GENERATION_EXPRESSION", false),
    ("SRS_ID", true),
];

/// Go's catalog name, which is always `def`.
const CATALOG: &str = "def";
/// Go `mysql.DefaultCharset` / its default collation, reported for every
/// schema and character column.
const CHARSET: &str = "utf8mb4";
const COLLATION: &str = "utf8mb4_bin";
/// Go's fixed per-column privilege list.
const PRIVILEGES: &str = "select,insert,update,references";

/// Whether `name` is the virtual schema, which is matched case-insensitively
/// as every schema name is.
#[must_use]
pub fn is_information_schema(name: &str) -> bool {
    name.eq_ignore_ascii_case(INFORMATION_SCHEMA)
}

/// The column names of one `information_schema` table, or `None` when the
/// table is not one this tier implements.
#[must_use]
pub fn table_columns(name: &str) -> Option<&'static [(&'static str, bool)]> {
    if name.eq_ignore_ascii_case("SCHEMATA") {
        Some(SCHEMATA_COLUMNS)
    } else if name.eq_ignore_ascii_case("TABLES") {
        Some(TABLES_COLUMNS)
    } else if name.eq_ignore_ascii_case("COLUMNS") {
        Some(COLUMNS_COLUMNS)
    } else {
        None
    }
}

/// A string cell.
fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

/// The rows of one `information_schema` table, computed from `catalog`.
#[must_use]
pub fn table_rows(name: &str, catalog: &Catalog) -> Option<Vec<Vec<Datum>>> {
    if name.eq_ignore_ascii_case("SCHEMATA") {
        return Some(schemata_rows(catalog));
    }
    if name.eq_ignore_ascii_case("TABLES") {
        return Some(tables_rows(catalog));
    }
    if name.eq_ignore_ascii_case("COLUMNS") {
        return Some(columns_rows(catalog));
    }
    None
}

/// Every schema, including the virtual one, which is why `SHOW DATABASES`
/// lists it too.
fn schemata_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    catalog
        .database_names()
        .into_iter()
        .map(|name| {
            vec![
                text(CATALOG),
                text(&name),
                text(CHARSET),
                text(COLLATION),
                // Go reports SQL_PATH and the placement policy as NULL.
                Datum::Null,
                Datum::Null,
            ]
        })
        .collect()
}

/// One row per table, in schema then table order.
fn tables_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            rows.push(vec![
                text(CATALOG),
                text(&schema),
                text(&table_name),
                text("BASE TABLE"),
                text("InnoDB"),
                Datum::Int(10),
                text("Compact"),
                // Statistics TiDB reports as 0 until the table is analyzed.
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                // CREATE_TIME is NULL rather than a fabricated timestamp.
                Datum::Null,
                Datum::Null,
                Datum::Null,
                text(COLLATION),
                Datum::Null,
                text(""),
                text(""),
                Datum::Int(table.table_id),
                text(&sharding_info(table)),
                text(&pk_type(table)),
                Datum::Null,
                text("Normal"),
                Datum::Null,
                text(""),
            ]);
        }
    }
    rows
}

/// Go `TIDB_ROW_ID_SHARDING_INFO`, whose value states why the table is not
/// sharded.
fn sharding_info(table: &KvTable) -> String {
    if table.pk_handle_offset().is_some() {
        "NOT_SHARDED(PK_IS_HANDLE)".to_owned()
    } else {
        "NOT_SHARDED".to_owned()
    }
}

/// Go `TIDB_PK_TYPE`: how the primary key is stored.
fn pk_type(table: &KvTable) -> String {
    if table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty() {
        "CLUSTERED".to_owned()
    } else {
        "NONCLUSTERED".to_owned()
    }
}

/// One row per column of every table.
fn columns_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            for (offset, column) in table.columns.iter().enumerate() {
                rows.push(column_row(&schema, &table_name, table, offset, column));
            }
        }
    }
    rows
}

/// One `COLUMNS` row.
fn column_row(
    schema: &str,
    table_name: &str,
    table: &KvTable,
    offset: usize,
    column: &tidb_executor::KvColumn,
) -> Vec<Datum> {
    let field_type = &column.field_type;
    let is_string = matches!(
        field_type.code(),
        FieldTypeCode::VarString | FieldTypeCode::String | FieldTypeCode::Varchar
    );
    let not_null = field_type.flags() & 1 != 0;

    // A character column reports its length and octet length; a numeric one
    // reports precision and scale. Captured from TiDB: varchar(8) gives 8 and
    // 32, bigint gives 19 and 0.
    let (char_max, char_octet, numeric_precision, numeric_scale) = if is_string {
        let flen = field_type.flen();
        (
            Datum::Int(flen),
            Datum::Int(flen * 4),
            Datum::Null,
            Datum::Null,
        )
    } else {
        (
            Datum::Null,
            Datum::Null,
            Datum::Int(numeric_precision_of(field_type)),
            Datum::Int(0),
        )
    };
    let (charset_name, collation_name) = if is_string {
        (text(CHARSET), text(COLLATION))
    } else {
        (Datum::Null, Datum::Null)
    };

    vec![
        text(CATALOG),
        text(schema),
        text(table_name),
        text(&column.name),
        Datum::Int((offset + 1) as i64),
        match &column.default_value {
            Some(Datum::Null) | None => Datum::Null,
            Some(value) => text(&crate::datum_text(value).unwrap_or_default()),
        },
        text(if not_null { "NO" } else { "YES" }),
        text(&data_type_of(field_type)),
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        // DATETIME_PRECISION, for the temporal types this tier does not report.
        Datum::Null,
        charset_name,
        collation_name,
        text(&field_type.compact_str(false)),
        text(&crate::column_key_flag(table, offset)),
        text(if table.auto_increment_offset() == Some(offset) {
            "auto_increment"
        } else {
            ""
        }),
        text(PRIVILEGES),
        text(""),
        text(""),
        Datum::Null,
    ]
}

/// Go `DATA_TYPE`: the type name without its display width, which is the
/// `COLUMN_TYPE` text cut at the first parenthesis.
fn data_type_of(field_type: &FieldType) -> String {
    let column_type = field_type.compact_str(false);
    match column_type.find('(') {
        Some(index) => column_type[..index].to_owned(),
        None => column_type,
    }
}

/// Go's `NUMERIC_PRECISION` for the integer types, captured as 19 for bigint.
fn numeric_precision_of(field_type: &FieldType) -> i64 {
    match field_type.code() {
        FieldTypeCode::Tiny => 3,
        FieldTypeCode::Short => 5,
        FieldTypeCode::Int24 => 7,
        FieldTypeCode::Long => 10,
        FieldTypeCode::LongLong => 19,
        _ => 0,
    }
}

/// The `(name, type)` pairs a virtual table's result carries.
#[must_use]
pub fn table_schema(name: &str) -> Option<Vec<(String, FieldType)>> {
    let columns = table_columns(name)?;
    Some(
        columns
            .iter()
            .map(|(column, numeric)| {
                // A numeric column must be typed as one, or its cells cannot
                // be written into the chunk.
                let code = if *numeric {
                    FieldTypeCode::LongLong
                } else {
                    FieldTypeCode::VarString
                };
                ((*column).to_owned(), FieldType::new(code))
            })
            .collect(),
    )
}
