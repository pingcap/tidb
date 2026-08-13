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
//! tables; and the contents of `mysql`, which is a real schema OBJECT in the
//! catalog (see `Catalog::default`) holding none of its 61 bootstrap tables,
//! so `SCHEMATA` lists it as TiDB does while `TABLES` reports it empty and
//! naming one of its tables refuses with 1146. The `performance_schema`,
//! `sys` and `metrics_schema` databases are absent entirely; their contents
//! are separate tiers.

use tidb_datatype::{
    Datum, FieldType, FieldTypeCode, STRICT_INTEGER_DISPLAY_WIDTH, UNSPECIFIED_LENGTH,
};
use tidb_executor::{Catalog, KvTable, TableEntry};

pub use tidb_executor::infoschema_meta::{
    is_information_schema, served_table_names, table_schema, INFORMATION_SCHEMA,
};

/// Go's catalog name, which is always `def`.
const CATALOG: &str = "def";
/// Go `mysql.DefaultCharset` / its default collation, reported for every
/// schema and character column.
const CHARSET: &str = "utf8mb4";
const COLLATION: &str = "utf8mb4_bin";
/// Go's fixed per-column privilege list.
const PRIVILEGES: &str = "select,insert,update,references";

/// A string cell.
fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

/// Go's `mysql.AllPrivMask`, the mask almost every `information_schema`
/// retriever tests a row against ("does this account hold ANY privilege on
/// this object").
const ANY_PRIV: PrivMask = PrivMask::Any;

/// The per-row privilege filter Go's `information_schema` retrievers apply,
/// as an owned snapshot of the asking session -- the catalog is borrowed
/// under a lock while these rows are built, so the decision cannot call back
/// into the session.
///
/// A `SchemaVisibility::unrestricted()` shows everything, which is Go's own
/// `checker == nil` arm (`infoschema_reader.go`'s `hasPriv` explains it: a
/// missing privilege manager is the signature of an internal statement).
#[derive(Clone, Default)]
pub struct SchemaVisibility {
    context: Option<VisibilityContext>,
}

#[derive(Clone)]
struct VisibilityContext {
    registry: crate::privilege::PrivilegeRegistry,
    user: String,
    host: String,
    active_roles: Vec<crate::privilege::Account>,
}

/// Which of Go's two masks a retriever filters with.
#[derive(Clone, Copy)]
pub enum PrivMask {
    /// `mysql.AllPrivMask`.
    Any,
    /// `mysql.AllColumnPrivs`, which only `COLUMNS` uses.
    Column,
}

impl SchemaVisibility {
    /// Every object visible -- Go's `checker == nil`.
    #[must_use]
    pub fn unrestricted() -> Self {
        Self::default()
    }

    /// The filter for one authenticated session.
    #[must_use]
    pub fn for_session(
        registry: crate::privilege::PrivilegeRegistry,
        user: &str,
        host: &str,
        active_roles: &[crate::privilege::Account],
    ) -> Self {
        Self {
            context: Some(VisibilityContext {
                registry,
                user: user.to_owned(),
                host: host.to_owned(),
                active_roles: active_roles.to_vec(),
            }),
        }
    }

    /// Go `RequestVerification(activeRoles, database, table, "", mask)`.
    #[must_use]
    fn allows(&self, database: &str, table: &str, mask: PrivMask) -> bool {
        let Some(context) = &self.context else {
            return true;
        };
        let mask = match mask {
            PrivMask::Any => crate::privilege::any_priv_mask(),
            PrivMask::Column => crate::privilege::column_privs_mask(),
        };
        let has_restricted_tables_admin = context.registry.has_dynamic_priv_with_roles(
            &context.user,
            &context.host,
            &context.active_roles,
            "RESTRICTED_TABLES_ADMIN",
            false,
        );
        if let Some(verdict) = crate::table_privilege::sem_verdict_mask(
            database,
            table,
            mask,
            has_restricted_tables_admin,
        ) {
            return verdict;
        }
        if let Some(verdict) = crate::table_privilege::mem_db_verdict_mask(database, mask) {
            return verdict;
        }
        context.registry.has_priv_mask_with_roles(
            &context.user,
            &context.host,
            &context.active_roles,
            database,
            table,
            mask,
        )
    }
}

/// Every `(schema, table)` pair the asking session may see, in catalog order.
///
/// This is the ONE place Go's per-retriever
/// `RequestVerification(schema, table, "", mask)` lands: every retriever that
/// walks tables walks this instead of the catalog, so a new one cannot be
/// written that forgets the check.
fn visible_tables(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    mask: PrivMask,
) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for schema in catalog.database_names() {
        let Some(tables) = catalog.table_names(&schema) else {
            continue;
        };
        for table_name in tables {
            if visibility.allows(&schema, &table_name, mask) {
                pairs.push((schema.clone(), table_name));
            }
        }
    }
    pairs
}

/// The rows of one `information_schema` table, computed from `catalog` and
/// filtered by what `visibility` may see.
#[must_use]
pub fn table_rows(
    name: &str,
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    ctx: &tidb_executor::StmtContext,
) -> Option<Vec<Vec<Datum>>> {
    if name.eq_ignore_ascii_case("SCHEMATA") {
        return Some(schemata_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("TABLES") {
        return Some(tables_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("VIEWS") {
        return Some(views_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("COLUMNS") {
        return Some(columns_rows(catalog, visibility, ctx));
    }
    if name.eq_ignore_ascii_case("KEY_COLUMN_USAGE") {
        return Some(key_column_usage_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("STATISTICS") {
        return Some(statistics_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("TABLE_CONSTRAINTS") {
        return Some(table_constraints_rows(catalog, visibility));
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
fn key_column_usage_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
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
fn statistics_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
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
        // Go `setDataForStatisticsInTable` writes the STRING "1"/"0" here,
        // which is why the declared type is `varchar(1)` and not an integer.
        text(if unique { "0" } else { "1" }),
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
fn table_constraints_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
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

/// Every schema, including the virtual one.
///
/// The filter here is Go's `setDataFromSchemata`
/// (`infoschema_reader.go` around line 439):
/// `RequestVerification(schema, "", "", AllPrivMask)` -- a GLOBAL or
/// `mysql.db` privilege, NOT the wider `DBIsVisible` that `SHOW DATABASES`
/// uses. The two answers really do differ and the difference is measured: an
/// account holding only `GRANT SELECT(a) ON d1.t` (or only
/// `GRANT SELECT ON d1.t`) sees `d1` in `SHOW DATABASES` and does NOT see it
/// here.
fn schemata_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    catalog
        .database_names()
        .into_iter()
        .filter(|name| visibility.allows(name, "", ANY_PRIV))
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
fn tables_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
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
            text(table.comment()),
            Datum::Int(table.table_id),
            text(&sharding_info(table)),
            text(&pk_type(table)),
            Datum::Null,
            text("Normal"),
            Datum::Null,
            text(""),
        ]);
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
fn views_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
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
///
/// The only retriever that does NOT filter with `AllPrivMask`: Go's
/// `setDataForColumnsWithOneTable` (`infoschema_reader.go` around line 1095)
/// walks `mysql.AllColumnPrivs` and admits the table when ANY of
/// `SELECT`/`INSERT`/`UPDATE`/`REFERENCES` is held, so a table reachable only
/// through, say, a `DROP` grant lists no columns.
fn columns_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    ctx: &tidb_executor::StmtContext,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, PrivMask::Column) {
        match catalog.table_in(&schema, &table_name) {
            Some(TableEntry::Kv(table)) => {
                // Hidden columns are absent here, and ORDINAL_POSITION
                // counts only the visible ones -- which needs no separate
                // counter, because a visible column's offset IS its
                // physical offset (see `tidb_executor::expression_index`).
                // Captured: a table with an expression index and columns
                // `a`, `z` reports exactly a|1, z|2.
                for (offset, column) in table.visible_columns().iter().enumerate() {
                    rows.push(column_row(&schema, &table_name, table, offset, column, ctx));
                }
            }
            // A view's columns are its body's, resolved now rather than
            // at CREATE (Go fills them the same way here as DESCRIBE
            // does). A body that no longer resolves drops out of this
            // table entirely, which is what Go answers (captured: a view
            // over a dropped column reports no COLUMNS rows at all).
            Some(TableEntry::View(view)) => {
                let Ok(columns) = tidb_executor::view_column_list(view, &schema, catalog, ctx)
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
    rows
}

/// One `COLUMNS` row.
fn column_row(
    schema: &str,
    table_name: &str,
    table: &KvTable,
    offset: usize,
    column: &tidb_executor::KvColumn,
    ctx: &tidb_executor::StmtContext,
) -> Vec<Datum> {
    let field_type = &column.field_type;
    let not_null = field_type.flags() & 1 != 0;
    let TypeCells {
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        datetime_precision,
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
            Some(tidb_executor::column_default::ColumnDefault::Value(Datum::Null)) | None => {
                Datum::Null
            }
            // Go `infoschema_reader.go` fills COLUMN_DEFAULT from
            // `ColDesc.DefaultValue`, the same string `SHOW COLUMNS` reports,
            // so a computed default reports its stored text unparenthesised.
            Some(tidb_executor::column_default::ColumnDefault::Value(value)) => {
                // Go's INFORMATION_SCHEMA retriever deliberately keeps the
                // stored metadata text when GetColDefaultValue cannot
                // materialize it; SHOW propagates the same conversion error.
                let visible = crate::show::literal_column_default_text(
                    value,
                    column,
                    ctx.query_default_conversion_flags(),
                    &ctx.session_zone(),
                )
                .ok()
                .flatten()
                .or_else(|| crate::show::column_default_text(value, field_type))
                .unwrap_or_default();
                text(&visible)
            }
            Some(computed) => match computed.column_desc_text(field_type) {
                Some(stored) => text(&stored),
                None => Datum::Null,
            },
        },
        text(if not_null { "NO" } else { "YES" }),
        text(&data_type_of(field_type)),
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        charset_name,
        collation_name,
        text(&field_type.info_schema_str(STRICT_INTEGER_DISPLAY_WIDTH)),
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
    datetime_precision: Datum,
    charset_name: Datum,
    collation_name: Datum,
}

/// A character column reports its length and octet length; a numeric one
/// reports precision and scale. Captured from TiDB: varchar(8) gives 8 and
/// 32, bigint gives 19 and 0.
fn type_cells(field_type: &FieldType) -> TypeCells {
    // Every type with a character length: the string types, plus ENUM/SET,
    // which Go's `IsString` excludes but which do report one.
    if field_type.code().is_string() || field_type.has_charset() {
        // One rule for every string type, character or binary: the character
        // length is the field length and the octet length scales it by the
        // charset's bytes-per-character. Captured: `varchar(10)` utf8mb4 gives
        // 10/40, the same column in latin1 gives 10/10, `varbinary(10)` gives
        // 10/10, `text` gives 65535/262140, `enum('a','B')` gives 1/4.
        //
        // A binary-charset column reports no charset and no collation at all,
        // which is exactly `HasCharset` being false for it.
        let flen = field_type.flen();
        let charset = field_type.charset();
        let (charset_name, collation_name) = if field_type.has_charset() {
            (
                text(field_type.charset_name()),
                text(field_type.collation_name()),
            )
        } else {
            (Datum::Null, Datum::Null)
        };
        TypeCells {
            char_max: Datum::Int(flen),
            char_octet: Datum::Int(flen.saturating_mul(charset.maxlen())),
            numeric_precision: Datum::Null,
            numeric_scale: Datum::Null,
            datetime_precision: Datum::Null,
            charset_name,
            collation_name,
        }
    } else {
        // Go `dataForColumnsInTable` substitutes the type's DEFAULT length and
        // decimal whenever the column left them unspecified, and only then
        // splits into the temporal and the numeric arm. Both cells are absent,
        // not zero, for every type outside those two arms -- `YEAR` and `DATE`
        // are neither fractionable nor numeric, so they report NULL twice.
        let code = field_type.code();
        let (default_flen, default_decimal) = code.default_length_and_decimal();
        let flen = if field_type.flen() == UNSPECIFIED_LENGTH {
            default_flen
        } else {
            field_type.flen()
        };
        let decimal = if field_type.decimal() == UNSPECIFIED_LENGTH {
            default_decimal
        } else {
            field_type.decimal()
        };
        let (numeric_precision, numeric_scale, datetime_precision) = if code.is_type_fractionable()
        {
            (Datum::Null, Datum::Null, Datum::Int(decimal))
        } else if code.is_type_numeric() {
            // FLOAT and DOUBLE report no scale when none was written -- their
            // default decimal is -1, which Go tests for rather than storing.
            let scale = if !matches!(code, FieldTypeCode::Float | FieldTypeCode::Double)
                || decimal != UNSPECIFIED_LENGTH
            {
                Datum::Int(decimal)
            } else {
                Datum::Null
            };
            (
                Datum::Int(numeric_precision_of(field_type, flen)),
                scale,
                Datum::Null,
            )
        } else {
            (Datum::Null, Datum::Null, Datum::Null)
        };
        TypeCells {
            char_max: Datum::Null,
            char_octet: Datum::Null,
            numeric_precision,
            numeric_scale,
            datetime_precision,
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
        datetime_precision,
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
        datetime_precision,
        charset_name,
        collation_name,
        text(&field_type.info_schema_str(STRICT_INTEGER_DISPLAY_WIDTH)),
        text(""),
        text(""),
        text(PRIVILEGES),
        text(""),
        text(""),
        Datum::Null,
    ]
}

/// Go `DATA_TYPE`: the bare type name, `types.TypeToStr` of the column's code
/// and charset -- no display width, and no `(`-truncation of the printed type
/// to fake one.
///
/// Go remaps `TypeVarString` to `TypeVarchar` for THIS cell alone; the
/// `COLUMN_TYPE` cell beside it keeps the un-remapped spelling, which is why a
/// view column can read `varchar` here and `var_string(32)` there.
fn data_type_of(field_type: &FieldType) -> String {
    let code = match field_type.code() {
        FieldTypeCode::VarString => FieldTypeCode::Varchar,
        code => code,
    };
    tidb_datatype::type_to_str(code, field_type.charset_name()).to_owned()
}

/// Go `getNumericPrecision`. `flen` is the caller's length with the type's
/// default already substituted, which is what makes an unwritten `DECIMAL`
/// report 10 and a `DECIMAL(20,4)` report 20.
///
/// MEDIUMINT and BIGINT report a WIDER precision when unsigned; the MEDIUMINT
/// pair is MySQL bug 69042, which TiDB reproduces deliberately.
fn numeric_precision_of(field_type: &FieldType, flen: i64) -> i64 {
    match field_type.code() {
        FieldTypeCode::Tiny => 3,
        FieldTypeCode::Short => 5,
        FieldTypeCode::Int24 => {
            if field_type.is_unsigned() {
                8
            } else {
                7
            }
        }
        FieldTypeCode::Long => 10,
        FieldTypeCode::LongLong => {
            if field_type.is_unsigned() {
                20
            } else {
                19
            }
        }
        FieldTypeCode::Bit
        | FieldTypeCode::Float
        | FieldTypeCode::Double
        | FieldTypeCode::NewDecimal => flen,
        _ => 0,
    }
}
