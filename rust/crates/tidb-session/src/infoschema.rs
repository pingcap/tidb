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
//! fabricated timestamp; `REFERENTIAL_CONSTRAINTS` always has zero rows,
//! since this tier has no foreign keys; the other `information_schema`
//! tables; and the `mysql`, `performance_schema`, `sys` and `metrics_schema`
//! databases, whose contents are separate tiers.

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

/// Go `infoschema.tableViewsCols`.
const VIEWS_COLUMNS: &[(&str, bool)] = &[
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("VIEW_DEFINITION", false),
    ("CHECK_OPTION", false),
    ("IS_UPDATABLE", false),
    ("DEFINER", false),
    ("SECURITY_TYPE", false),
    ("CHARACTER_SET_CLIENT", false),
    ("COLLATION_CONNECTION", false),
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

/// Go `infoschema.tableKeyColumnUsageCols`, captured from a running TiDB:
/// one row per column of a `PRIMARY KEY` or `UNIQUE` index (a plain, non-unique
/// `KEY` does not appear here -- it only shows up in `STATISTICS`).
const KEY_COLUMN_USAGE_COLUMNS: &[(&str, bool)] = &[
    ("CONSTRAINT_CATALOG", false),
    ("CONSTRAINT_SCHEMA", false),
    ("CONSTRAINT_NAME", false),
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("COLUMN_NAME", false),
    ("ORDINAL_POSITION", true),
    ("POSITION_IN_UNIQUE_CONSTRAINT", true),
    ("REFERENCED_TABLE_SCHEMA", false),
    ("REFERENCED_TABLE_NAME", false),
    ("REFERENCED_COLUMN_NAME", false),
];

/// Go `infoschema.tableTableConstraintsCols`, captured: one row per
/// `PRIMARY KEY` or `UNIQUE` constraint (not per column).
const TABLE_CONSTRAINTS_COLUMNS: &[(&str, bool)] = &[
    ("CONSTRAINT_CATALOG", false),
    ("CONSTRAINT_SCHEMA", false),
    ("CONSTRAINT_NAME", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("CONSTRAINT_TYPE", false),
];

/// Go `infoschema.tableStatisticsCols`, captured: one row per indexed
/// column, the same population `SHOW INDEX` reports (see
/// `show_index_rows` in `lib.rs`), but under this table's own column set
/// -- no `Clustered`/`Global` columns, and `TABLE_CATALOG`/`TABLE_SCHEMA`/
/// `INDEX_SCHEMA` in their place.
const STATISTICS_COLUMNS: &[(&str, bool)] = &[
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("NON_UNIQUE", true),
    ("INDEX_SCHEMA", false),
    ("INDEX_NAME", false),
    ("SEQ_IN_INDEX", true),
    ("COLUMN_NAME", false),
    ("COLLATION", false),
    ("CARDINALITY", true),
    ("SUB_PART", true),
    ("PACKED", false),
    ("NULLABLE", false),
    ("INDEX_TYPE", false),
    ("COMMENT", false),
    ("INDEX_COMMENT", false),
    ("IS_VISIBLE", false),
    ("Expression", false),
];

/// Go `infoschema.tableReferentialConstraintsCols`. This tier has no foreign
/// keys, so the table always has zero rows; only the header is captured.
const REFERENTIAL_CONSTRAINTS_COLUMNS: &[(&str, bool)] = &[
    ("CONSTRAINT_CATALOG", false),
    ("CONSTRAINT_SCHEMA", false),
    ("CONSTRAINT_NAME", false),
    ("UNIQUE_CONSTRAINT_CATALOG", false),
    ("UNIQUE_CONSTRAINT_SCHEMA", false),
    ("UNIQUE_CONSTRAINT_NAME", false),
    ("MATCH_OPTION", false),
    ("UPDATE_RULE", false),
    ("DELETE_RULE", false),
    ("TABLE_NAME", false),
    ("REFERENCED_TABLE_NAME", false),
];

/// Go `infoschema.tableProcesslistCols`, CAPTURED from `pkg/infoschema/tables.go`
/// (`ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO` are the `SHOW
/// PROCESSLIST` columns; the rest are this table's own extras). Rows come
/// from session/registry state, not the catalog -- see
/// `Session::process_list_table_rows`, which is why this table has no
/// `*_rows` function alongside it in this module.
const PROCESSLIST_COLUMNS: &[(&str, bool)] = &[
    ("ID", true),
    ("USER", false),
    ("HOST", false),
    ("DB", false),
    ("COMMAND", false),
    ("TIME", true),
    ("STATE", false),
    ("INFO", false),
    ("DIGEST", false),
    ("MEM", true),
    ("MEM_ARBITRATION", true),
    ("MEM_WAIT_ARBITRATE_START", false),
    ("MEM_WAIT_ARBITRATE_BYTES", true),
    ("DISK", true),
    ("TxnStart", false),
    ("RESOURCE_GROUP", false),
    ("SESSION_ALIAS", false),
    ("ROWS_AFFECTED", true),
    ("TIDB_CPU", true),
    ("TIKV_CPU", true),
];

/// Go `infoschema.tableUserPrivilegesCols`.
///
/// CAPTURED: unlike its `SCHEMA_PRIVILEGES`/`TABLE_PRIVILEGES`/
/// `COLUMN_PRIVILEGES` siblings below, this one DOES have a retriever
/// (`MySQLPrivilege.UserPrivilegesTable`) and DOES serve rows -- including
/// one per DYNAMIC privilege, whose `IS_GRANTABLE` comes from that
/// privilege's own `with_grant_option` rather than from the account's
/// `GRANT OPTION`. Rows are built session-side (see
/// `Session::user_privileges_table_rows`), not from the catalog.
const USER_PRIVILEGES_COLUMNS: &[(&str, bool)] = &[
    ("GRANTEE", false),
    ("TABLE_CATALOG", false),
    ("PRIVILEGE_TYPE", false),
    ("IS_GRANTABLE", false),
];

/// Go `infoschema.tableSchemaPrivilegesCols`.
///
/// CAPTURED: this table -- and its `TABLE_PRIVILEGES` / `COLUMN_PRIVILEGES`
/// siblings below -- is DECLARED in `pkg/infoschema/tables.go` but has NO
/// retriever anywhere in `pkg/executor`, so real TiDB serves the header and
/// never a row. Verified against `testkit.CreateMockStore` with grants
/// actually present (`GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'`,
/// `GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'`, plus table-scope
/// grants): `SELECT COUNT(*)` returns `0`. Populating these from the
/// privilege registry would be a DIVERGENCE from Go, not a completion --
/// so the emptiness is the behavior being transcreated.
const SCHEMA_PRIVILEGES_COLUMNS: &[(&str, bool)] = &[
    ("GRANTEE", false),
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("PRIVILEGE_TYPE", false),
    ("IS_GRANTABLE", false),
];

/// Go `infoschema.tableTablePrivilegesCols`. Always empty -- see
/// `SCHEMA_PRIVILEGES_COLUMNS`.
const TABLE_PRIVILEGES_COLUMNS: &[(&str, bool)] = &[
    ("GRANTEE", false),
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("PRIVILEGE_TYPE", false),
    ("IS_GRANTABLE", false),
];

/// Go `infoschema.tableColumnPrivilegesCols`. Always empty -- see
/// `SCHEMA_PRIVILEGES_COLUMNS`.
const COLUMN_PRIVILEGES_COLUMNS: &[(&str, bool)] = &[
    ("GRANTEE", false),
    ("TABLE_CATALOG", false),
    ("TABLE_SCHEMA", false),
    ("TABLE_NAME", false),
    ("COLUMN_NAME", false),
    ("PRIVILEGE_TYPE", false),
    ("IS_GRANTABLE", false),
];

/// The column names of one `information_schema` table, or `None` when the
/// table is not one this tier implements.
#[must_use]
pub fn table_columns(name: &str) -> Option<&'static [(&'static str, bool)]> {
    if name.eq_ignore_ascii_case("SCHEMATA") {
        Some(SCHEMATA_COLUMNS)
    } else if name.eq_ignore_ascii_case("TABLES") {
        Some(TABLES_COLUMNS)
    } else if name.eq_ignore_ascii_case("VIEWS") {
        Some(VIEWS_COLUMNS)
    } else if name.eq_ignore_ascii_case("COLUMNS") {
        Some(COLUMNS_COLUMNS)
    } else if name.eq_ignore_ascii_case("KEY_COLUMN_USAGE") {
        Some(KEY_COLUMN_USAGE_COLUMNS)
    } else if name.eq_ignore_ascii_case("STATISTICS") {
        Some(STATISTICS_COLUMNS)
    } else if name.eq_ignore_ascii_case("TABLE_CONSTRAINTS") {
        Some(TABLE_CONSTRAINTS_COLUMNS)
    } else if name.eq_ignore_ascii_case("REFERENTIAL_CONSTRAINTS") {
        Some(REFERENTIAL_CONSTRAINTS_COLUMNS)
    } else if name.eq_ignore_ascii_case("PROCESSLIST") {
        Some(PROCESSLIST_COLUMNS)
    } else if name.eq_ignore_ascii_case("USER_PRIVILEGES") {
        Some(USER_PRIVILEGES_COLUMNS)
    } else if name.eq_ignore_ascii_case("SCHEMA_PRIVILEGES") {
        Some(SCHEMA_PRIVILEGES_COLUMNS)
    } else if name.eq_ignore_ascii_case("TABLE_PRIVILEGES") {
        Some(TABLE_PRIVILEGES_COLUMNS)
    } else if name.eq_ignore_ascii_case("COLUMN_PRIVILEGES") {
        Some(COLUMN_PRIVILEGES_COLUMNS)
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
    if name.eq_ignore_ascii_case("VIEWS") {
        return Some(views_rows(catalog));
    }
    if name.eq_ignore_ascii_case("COLUMNS") {
        return Some(columns_rows(catalog));
    }
    if name.eq_ignore_ascii_case("KEY_COLUMN_USAGE") {
        return Some(key_column_usage_rows(catalog));
    }
    if name.eq_ignore_ascii_case("STATISTICS") {
        return Some(statistics_rows(catalog));
    }
    if name.eq_ignore_ascii_case("TABLE_CONSTRAINTS") {
        return Some(table_constraints_rows(catalog));
    }
    if name.eq_ignore_ascii_case("REFERENTIAL_CONSTRAINTS") {
        // No foreign keys in this tier: the header exists, the body never
        // does.
        return Some(Vec::new());
    }
    if name.eq_ignore_ascii_case("SCHEMA_PRIVILEGES")
        || name.eq_ignore_ascii_case("TABLE_PRIVILEGES")
        || name.eq_ignore_ascii_case("COLUMN_PRIVILEGES")
    {
        // Declared but never retrieved in Go, even with grants present --
        // the header exists, the body never does.
        return Some(Vec::new());
    }
    None
}

/// One row per column of every `PRIMARY KEY` or `UNIQUE` index.
fn key_column_usage_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            // The clustered handle, reported as a one-column PRIMARY KEY not
            // present in `table.indexes()`.
            if let Some(offset) = table.pk_handle_offset() {
                push_key_column_usage_row(
                    &mut rows,
                    &schema,
                    &table_name,
                    "PRIMARY",
                    true,
                    1,
                    &table.columns[offset].name,
                );
            }
            for index in table.indexes() {
                if !index.unique {
                    continue;
                }
                let is_primary = index.name.eq_ignore_ascii_case("PRIMARY");
                for (position, offset) in index.column_offsets.iter().enumerate() {
                    push_key_column_usage_row(
                        &mut rows,
                        &schema,
                        &table_name,
                        &index.name,
                        is_primary,
                        (position + 1) as i64,
                        &table.columns[*offset].name,
                    );
                }
            }
        }
    }
    rows
}

/// One `KEY_COLUMN_USAGE` row.
///
/// `POSITION_IN_UNIQUE_CONSTRAINT` was captured as the column's own ordinal
/// for a `PRIMARY KEY` and `NULL` for every other `UNIQUE` key -- an
/// asymmetry this reproduces rather than smooths over, since Go's own value
/// is what a client reads.
fn push_key_column_usage_row(
    rows: &mut Vec<Vec<Datum>>,
    schema: &str,
    table_name: &str,
    constraint_name: &str,
    is_primary: bool,
    ordinal_position: i64,
    column_name: &str,
) {
    rows.push(vec![
        text(CATALOG),
        text(schema),
        text(constraint_name),
        text(CATALOG),
        text(schema),
        text(table_name),
        text(column_name),
        Datum::Int(ordinal_position),
        if is_primary {
            Datum::Int(ordinal_position)
        } else {
            Datum::Null
        },
        Datum::Null,
        Datum::Null,
        Datum::Null,
    ]);
}

/// One row per indexed column, the `STATISTICS` table's own column set over
/// the same population `SHOW INDEX` reports.
fn statistics_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            if let Some(offset) = table.pk_handle_offset() {
                rows.push(statistics_row(
                    &schema,
                    &table_name,
                    "PRIMARY",
                    true,
                    1,
                    &table.columns[offset].name,
                    false,
                ));
            }
            for index in table.indexes() {
                for (position, offset) in index.column_offsets.iter().enumerate() {
                    let column = &table.columns[*offset];
                    let nullable =
                        column.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0;
                    rows.push(statistics_row(
                        &schema,
                        &table_name,
                        &index.name,
                        index.unique,
                        position + 1,
                        &column.name,
                        nullable,
                    ));
                }
            }
        }
    }
    rows
}

/// One `STATISTICS` row.
fn statistics_row(
    schema: &str,
    table_name: &str,
    index_name: &str,
    unique: bool,
    sequence: usize,
    column_name: &str,
    nullable: bool,
) -> Vec<Datum> {
    vec![
        text(CATALOG),
        text(schema),
        text(table_name),
        Datum::Int(i64::from(!unique)),
        text(schema),
        text(index_name),
        Datum::Int(sequence as i64),
        text(column_name),
        text("A"),
        // No statistics tier, so Go's cardinality estimate is simply absent.
        Datum::Int(0),
        Datum::Null,
        Datum::Null,
        text(if nullable { "YES" } else { "" }),
        text("BTREE"),
        text(""),
        text(""),
        text("YES"),
        Datum::Null,
    ]
}

/// One row per `PRIMARY KEY` or `UNIQUE` constraint (not per column).
fn table_constraints_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            if table.pk_handle_offset().is_some() {
                rows.push(table_constraint_row(
                    &schema,
                    &table_name,
                    "PRIMARY",
                    "PRIMARY KEY",
                ));
            }
            for index in table.indexes() {
                if !index.unique {
                    continue;
                }
                let constraint_type = if index.name.eq_ignore_ascii_case("PRIMARY") {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                };
                rows.push(table_constraint_row(
                    &schema,
                    &table_name,
                    &index.name,
                    constraint_type,
                ));
            }
        }
    }
    rows
}

/// One `TABLE_CONSTRAINTS` row.
fn table_constraint_row(
    schema: &str,
    table_name: &str,
    constraint_name: &str,
    constraint_type: &str,
) -> Vec<Datum> {
    vec![
        text(CATALOG),
        text(schema),
        text(constraint_name),
        text(schema),
        text(table_name),
        text(constraint_type),
    ]
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
            let table = match catalog.table_in(&schema, &table_name) {
                Some(TableEntry::Kv(table)) => table,
                // A view has no storage, so every storage column is NULL and
                // the comment states the kind -- Go's own captured row.
                Some(TableEntry::View(_)) => {
                    rows.push(view_tables_row(&schema, &table_name));
                    continue;
                }
                _ => continue,
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

/// The `information_schema.tables` row of a view: everything a base table
/// reports about its storage is NULL, `TABLE_TYPE` and `TABLE_COMMENT` both
/// say `VIEW`, and `TIDB_PK_TYPE` still reports `NONCLUSTERED`.
///
/// DIVERGENCE (documented): `TIDB_TABLE_ID` is NULL here because this tier
/// allocates no id for a view; Go reports the view's own table id.
/// `CREATE_TIME` is NULL for the same reason it is NULL for a base table --
/// nothing records one.
fn view_tables_row(schema: &str, table_name: &str) -> Vec<Datum> {
    let mut row = vec![text(CATALOG), text(schema), text(table_name), text("VIEW")];
    // ENGINE through CREATE_OPTIONS: sixteen columns a view has no value for.
    row.extend(std::iter::repeat_n(Datum::Null, 16));
    // TABLE_COMMENT, TIDB_TABLE_ID, TIDB_ROW_ID_SHARDING_INFO, TIDB_PK_TYPE.
    row.push(text("VIEW"));
    row.push(Datum::Null);
    row.push(Datum::Null);
    row.push(text("NONCLUSTERED"));
    // The four trailing TiDB placement/mode columns.
    row.extend(std::iter::repeat_n(Datum::Null, 4));
    row
}

/// One row per view, in schema then view order.
///
/// `CHECK_OPTION` is the view's stored mode, `CASCADED` unless `WITH LOCAL
/// CHECK OPTION` was written -- Go records one on every view, written or not
/// (captured).
///
/// DIVERGENCE (documented): `IS_UPDATABLE` is always `NO`, which is what Go
/// reports for every view this tier can create -- no view here is updatable.
fn views_rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            let Some(TableEntry::View(view)) = catalog.table_in(&schema, &table_name) else {
                continue;
            };
            rows.push(vec![
                text(CATALOG),
                text(&schema),
                text(&table_name),
                text(&view.select_sql),
                text(&view.check_option),
                text("NO"),
                text(&format!("{}@{}", view.definer_user, view.definer_host)),
                text(&view.security),
                text(CHARSET),
                text(COLLATION),
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
            match catalog.table_in(&schema, &table_name) {
                Some(TableEntry::Kv(table)) => {
                    for (offset, column) in table.columns.iter().enumerate() {
                        rows.push(column_row(&schema, &table_name, table, offset, column));
                    }
                }
                // A view's columns are its body's, resolved now rather than
                // at CREATE (Go fills them the same way here as DESCRIBE
                // does). A body that no longer resolves drops out of this
                // table entirely, which is what Go answers (captured: a view
                // over a dropped column reports no COLUMNS rows at all).
                Some(TableEntry::View(view)) => {
                    let ctx = tidb_executor::StmtContext::for_query();
                    let Ok(columns) = tidb_executor::view_column_list(view, &schema, catalog, &ctx)
                    else {
                        continue;
                    };
                    for (offset, (name, field_type)) in columns.iter().enumerate() {
                        rows.push(view_column_row(
                            &schema,
                            &table_name,
                            name,
                            field_type,
                            offset,
                        ));
                    }
                }
                _ => continue,
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
    let not_null = field_type.flags() & 1 != 0;
    let TypeCells {
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        charset_name,
        collation_name,
    } = type_cells(field_type);

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
        text(&crate::show::column_key_flag(table, offset)),
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

/// The `COLUMNS` cells a column's type alone decides.
struct TypeCells {
    char_max: Datum,
    char_octet: Datum,
    numeric_precision: Datum,
    numeric_scale: Datum,
    charset_name: Datum,
    collation_name: Datum,
}

/// A character column reports its length and octet length; a numeric one
/// reports precision and scale. Captured from TiDB: varchar(8) gives 8 and
/// 32, bigint gives 19 and 0.
fn type_cells(field_type: &FieldType) -> TypeCells {
    let is_string = matches!(
        field_type.code(),
        FieldTypeCode::VarString | FieldTypeCode::String | FieldTypeCode::Varchar
    );
    if is_string {
        let flen = field_type.flen();
        TypeCells {
            char_max: Datum::Int(flen),
            char_octet: Datum::Int(flen * 4),
            numeric_precision: Datum::Null,
            numeric_scale: Datum::Null,
            charset_name: text(CHARSET),
            collation_name: text(COLLATION),
        }
    } else {
        TypeCells {
            char_max: Datum::Null,
            char_octet: Datum::Null,
            numeric_precision: Datum::Int(numeric_precision_of(field_type)),
            numeric_scale: Datum::Int(0),
            charset_name: Datum::Null,
            collation_name: Datum::Null,
        }
    }
}

/// One `COLUMNS` row for a view's column.
///
/// A view has no storage metadata, so the key, default, extra and comment
/// cells are all the empty answers Go gives (captured: `COLUMN_KEY` and
/// `EXTRA` empty, `COLUMN_DEFAULT` NULL, `IS_NULLABLE` YES, and the same
/// `PRIVILEGES` string a base table's column carries).
fn view_column_row(
    schema: &str,
    table_name: &str,
    name: &str,
    field_type: &FieldType,
    offset: usize,
) -> Vec<Datum> {
    let TypeCells {
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        charset_name,
        collation_name,
    } = type_cells(field_type);
    vec![
        text(CATALOG),
        text(schema),
        text(table_name),
        text(name),
        Datum::Int((offset + 1) as i64),
        Datum::Null,
        text("YES"),
        text(&data_type_of(field_type)),
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        Datum::Null,
        charset_name,
        collation_name,
        text(&field_type.compact_str(false)),
        text(""),
        text(""),
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
