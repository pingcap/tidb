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
//! NOT MODELLED (documented): `CREATE_TIME` is NULL rather than a fabricated
//! timestamp; the other `information_schema` tables; and the
//! contents of `mysql`, which is a real schema OBJECT in the
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
    // go's `infoschema.AllSchemas` iteration is alphabetical per schema and
    // the schemas sort case-insensitively (`d10` precedes
    // `INFORMATION_SCHEMA`), so every I_S table listing follows that order
    // rather than the catalog's own insertion order.
    pairs.sort_by(|(left_schema, left_table), (right_schema, right_table)| {
        let schema_order = left_schema
            .to_ascii_lowercase()
            .cmp(&right_schema.to_ascii_lowercase());
        schema_order.then_with(|| {
            left_table
                .to_ascii_lowercase()
                .cmp(&right_table.to_ascii_lowercase())
        })
    });
    pairs
}

/// Go `memtableRetriever.setDataFromIndexUsage`: one row for every index of
/// every table visible to the current account, joined with the Domain's
/// node-global usage counters. Integer primary-key handles use synthetic
/// index ID zero because Go does not keep them in `TableInfo.Indices`.
pub(crate) fn tidb_index_usage_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    collector: &tidb_stats_handle_usage_indexusage::Collector,
) -> Vec<Vec<Datum>> {
    use chrono::{DateTime, Local};
    use tidb_datatype::{core_time_from_datetime, Time, TimeType};

    fn usage_row(
        schema: &str,
        table: &str,
        index: &str,
        usage: tidb_stats_handle_usage_indexusage::Sample,
    ) -> Vec<Datum> {
        let last_access = if usage.last_used_at
            == tidb_stats_handle_usage_indexusage::Sample::default().last_used_at
        {
            Datum::Null
        } else {
            let local: DateTime<Local> = usage.last_used_at.with_timezone(&Local);
            let core = core_time_from_datetime(local);
            Datum::new_time(
                Time::new(core, TimeType::Timestamp, 0)
                    .expect("fsp 0 is valid for TIDB_INDEX_USAGE timestamps"),
            )
        };
        let mut row = vec![
            text(schema),
            text(table),
            text(index),
            Datum::Int(usage.query_total as i64),
            Datum::Int(usage.kv_req_total as i64),
            Datum::Int(usage.row_access_total as i64),
        ];
        row.extend(
            usage
                .percentage_access
                .into_iter()
                .map(|value| Datum::Int(value as i64)),
        );
        row.push(last_access);
        row
    }

    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        if table.pk_handle_offset().is_some() {
            rows.push(usage_row(
                &schema,
                &table.name,
                "primary",
                collector.get_index_usage(table.table_id, 0),
            ));
        }
        for index in table.indexes() {
            rows.push(usage_row(
                &schema,
                &table.name,
                &index.name.to_ascii_lowercase(),
                collector.get_index_usage(table.table_id, index.id),
            ));
        }
    }
    rows
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
    if name.eq_ignore_ascii_case("PARTITIONS") {
        return Some(partitions_rows(catalog, visibility));
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
    if name.eq_ignore_ascii_case("TIFLASH_REPLICA") {
        return Some(tiflash_replica_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("REFERENTIAL_CONSTRAINTS") {
        return Some(referential_constraints_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("CHARACTER_SETS") {
        return Some(character_sets_rows());
    }
    if name.eq_ignore_ascii_case("ENGINES") {
        return Some(engines_rows());
    }
    if name.eq_ignore_ascii_case("TRIGGERS") {
        return Some(triggers_rows());
    }
    if name.eq_ignore_ascii_case("ROUTINES") {
        return Some(routines_rows());
    }
    if name.eq_ignore_ascii_case("EVENTS") {
        return Some(events_rows());
    }
    if name.eq_ignore_ascii_case("INSPECTION_RESULT") {
        return Some(inspection_result_rows());
    }
    if name.eq_ignore_ascii_case("TIDB_INDEXES") {
        return Some(tidb_indexes_rows(catalog, visibility));
    }
    if name.eq_ignore_ascii_case("METRICS_TABLES") {
        return Some(metrics_tables_rows());
    }
    if name.eq_ignore_ascii_case("CLUSTER_CONFIG") {
        return Some(cluster_config_rows());
    }
    if name.eq_ignore_ascii_case("PARAMETERS") {
        return Some(parameters_rows());
    }
    if name.eq_ignore_ascii_case("PLUGINS") {
        return Some(plugins_rows());
    }
    if name.eq_ignore_ascii_case("SEQUENCES") {
        return Some(sequences_rows());
    }
    if name.eq_ignore_ascii_case("RUNAWAY_WATCHES") {
        return Some(runaway_watches_rows());
    }
    if name.eq_ignore_ascii_case("TIFLASH_TABLES") {
        return Some(tiflash_tables_rows());
    }
    if name.eq_ignore_ascii_case("TIFLASH_SEGMENTS") {
        return Some(tiflash_segments_rows());
    }
    if name.eq_ignore_ascii_case("RESOURCE_GROUPS") {
        return Some(resource_groups_rows());
    }
    if name.eq_ignore_ascii_case("COLLATIONS") {
        return Some(collations_rows());
    }
    if name.eq_ignore_ascii_case("COLLATION_CHARACTER_SET_APPLICABILITY") {
        return Some(
            SUPPORTED_COLLATIONS
                .iter()
                .map(|c| vec![text(c.name), text(c.charset)])
                .collect(),
        );
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

/// Go `dataForTableTiFlashReplica` (pkg/executor/infoschema_reader.go:2838):
/// one row per table carrying a `TiFlashReplica`, reporting the persisted
/// replica metadata. PROGRESS mirrors AVAILABLE (1.0 once the replica is
/// readable, 0.0 while the learners are being placed): the live fraction
/// lives in the replica manager's progress cache, which this seam does not
/// see yet.
fn tiflash_replica_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        let Some(replica) = table.tiflash_replica() else {
            continue;
        };
        let progress = f64::from(replica.available);
        rows.push(vec![
            text(&schema),
            text(&table_name),
            Datum::Int(table.table_id),
            Datum::Int(replica.count as i64),
            text(
                &replica
                    .location_labels
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            Datum::Int(i64::from(replica.available)),
            Datum::Real(progress),
        ]);
    }
    rows
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
                KeyColumnConstraint {
                    name: "PRIMARY",
                    is_primary: true,
                    reference: None,
                },
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
                    KeyColumnConstraint {
                        name: &index.name,
                        is_primary,
                        reference: None,
                    },
                    (position + 1) as i64,
                    &table.columns[*offset].name,
                );
            }
        }
        for foreign_key in table.foreign_keys() {
            for (position, column_name) in foreign_key.cols.iter().enumerate() {
                push_key_column_usage_row(
                    &mut rows,
                    &schema,
                    &table_name,
                    KeyColumnConstraint {
                        name: &foreign_key.name,
                        is_primary: false,
                        reference: Some(KeyColumnReference {
                            schema: &foreign_key.ref_schema,
                            table: &foreign_key.ref_table,
                            column: foreign_key
                                .ref_cols
                                .get(position)
                                .map_or("", String::as_str),
                        }),
                    },
                    (position + 1) as i64,
                    column_name,
                );
            }
        }
    }
    rows
}

struct KeyColumnReference<'a> {
    schema: &'a str,
    table: &'a str,
    column: &'a str,
}

struct KeyColumnConstraint<'a> {
    name: &'a str,
    is_primary: bool,
    reference: Option<KeyColumnReference<'a>>,
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
    constraint: KeyColumnConstraint<'_>,
    ordinal_position: i64,
    column_name: &str,
) {
    let position_in_unique = if constraint.is_primary || constraint.reference.is_some() {
        Datum::Int(if constraint.is_primary {
            ordinal_position
        } else {
            1
        })
    } else {
        Datum::Null
    };
    let (referenced_schema, referenced_table, referenced_column) =
        constraint
            .reference
            .map_or((Datum::Null, Datum::Null, Datum::Null), |reference| {
                (
                    text(reference.schema),
                    text(reference.table),
                    text(reference.column),
                )
            });
    rows.push(vec![
        text(CATALOG),
        text(schema),
        text(constraint.name),
        text(CATALOG),
        text(schema),
        text(table_name),
        text(column_name),
        Datum::Int(ordinal_position),
        position_in_unique,
        referenced_schema,
        referenced_table,
        referenced_column,
    ]);
}

/// One row per indexed column, the `STATISTICS` table's own column set over
/// the same population `SHOW INDEX` reports.
fn statistics_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    let mut indexed_rows: Vec<(i64, Vec<tidb_datatype::Datum>)> = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        if let Some(offset) = table.pk_handle_offset() {
            rows.push(statistics_row(
                &schema,
                &table_name,
                StatisticsIndex {
                    name: "PRIMARY",
                    unique: true,
                    comment: "",
                    visible: true,
                },
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
                    StatisticsIndex {
                        name: &index.name,
                        unique: index.unique,
                        comment: &index.comment,
                        visible: index.visible,
                    },
                    position + 1,
                    &column.name,
                    nullable,
                ));
            }
        }
    }
    rows
}

struct StatisticsIndex<'a> {
    name: &'a str,
    unique: bool,
    comment: &'a str,
    visible: bool,
}

/// One `STATISTICS` row.
fn statistics_row(
    schema: &str,
    table_name: &str,
    index: StatisticsIndex<'_>,
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
        text(if index.unique { "0" } else { "1" }),
        text(schema),
        text(index.name),
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
        text(index.comment),
        text(if index.visible { "YES" } else { "NO" }),
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
        for foreign_key in table.foreign_keys() {
            rows.push(table_constraint_row(
                &schema,
                &table_name,
                &foreign_key.name,
                "FOREIGN KEY",
            ));
        }
    }
    rows
}

fn referential_constraints_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        for foreign_key in table.foreign_keys() {
            rows.push(vec![
                text(CATALOG),
                text(&schema),
                text(&foreign_key.name),
                text(CATALOG),
                text(&schema),
                text("PRIMARY"),
                text("NONE"),
                text(referential_rule(foreign_key.on_update)),
                text(referential_rule(foreign_key.on_delete)),
                text(&table_name),
                text(&foreign_key.ref_table),
            ]);
        }
    }
    rows
}

fn referential_rule(action: tidb_executor::FkAction) -> &'static str {
    match action {
        tidb_executor::FkAction::NoOption | tidb_executor::FkAction::NoAction => "NO ACTION",
        tidb_executor::FkAction::Restrict => "RESTRICT",
        tidb_executor::FkAction::Cascade => "CASCADE",
        tidb_executor::FkAction::SetNull => "SET NULL",
        tidb_executor::FkAction::SetDefault => "SET DEFAULT",
    }
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
    // go seeds metrics_schema/performance_schema/sys at bootstrap; the
    // catalog here doesn't carry them as objects, so synthesize their rows
    // (the utf8mb4/utf8mb4_bin defaults go's bootstrap settles).
    let synthesized: Vec<Vec<Datum>> = [
        ("METRICS_SCHEMA", "utf8mb4"),
        ("PERFORMANCE_SCHEMA", "utf8mb4"),
        ("sys", "utf8mb4"),
    ]
    .iter()
    .map(|(display, charset)| {
        vec![
            text(CATALOG),
            text(display),
            text(charset),
            text("utf8mb4_bin"),
            Datum::Null,
            Datum::Null,
        ]
    })
    .collect();
    catalog
        .database_names()
        .into_iter()
        .filter(|name| visibility.allows(name, "", ANY_PRIV))
        .filter(|name| {
            !matches!(
                name.to_ascii_lowercase().as_str(),
                "metrics_schema" | "performance_schema" | "sys"
            )
        })
        .map(|name| {
            // Go reads `DBInfo.Charset`/`Collate`, which `CREATE DATABASE`
            // settles and `ALTER DATABASE ... CHARACTER SET` moves; reporting
            // the server default here made that ALTER invisible.
            let charset = catalog
                .database_definition(&name)
                .map_or_else(tidb_executor::TableCharset::default, |(_, charset)| charset);
            vec![
                text(CATALOG),
                text(&name),
                text(charset.charset.name()),
                text(charset.collation.name()),
                // Go reports SQL_PATH and the placement policy as NULL.
                Datum::Null,
                Datum::Null,
            ]
        })
        .chain(synthesized)
        .collect()
}

/// One row per table, in schema then table order.
/// Go `setDataFromPartitions` (`executor/infoschema_reader.go`).
///
/// EVERY visible table produces at least one row. An unpartitioned one gets a
/// single row whose partition columns are all NULL -- not zero rows -- so a
/// client joining against this table still sees it. Reporting nothing for
/// unpartitioned tables would make them look absent rather than unpartitioned.
fn partitions_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        // Go's partition reader lists VIEWS as one all-NULL partition row of
        // zero stats (oracle: d17.v beside d17.base) -- every partition cell
        // NULL, the four stat cells at their zero/NULL defaults.
        if matches!(
            catalog.table_in(&schema, &table_name),
            Some(TableEntry::View(_))
        ) {
            rows.push(vec![
                Datum::Bytes(b"def".to_vec()),
                Datum::Bytes(schema.clone().into_bytes()),
                Datum::Bytes(table_name.clone().into_bytes()),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::UInt(0),
                Datum::Null,
                Datum::UInt(0),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]);
            continue;
        }
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        let catalog_value = || Datum::Bytes(b"def".to_vec());
        let Some(partition) = table.partition() else {
            let (row_count, average_row_length, data_length, index_length) =
                table.storage_statistics();
            rows.push(vec![
                catalog_value(),
                Datum::Bytes(schema.clone().into_bytes()),
                Datum::Bytes(table_name.clone().into_bytes()),
                // Every partition column is NULL for an unpartitioned table.
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::UInt(row_count),
                Datum::UInt(average_row_length),
                Datum::UInt(data_length),
                Datum::Null,
                Datum::UInt(index_length),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ]);
            continue;
        };
        // Go names the COLUMNS forms differently from the expression forms and
        // prints the column list in place of the expression
        // (`infoschema_reader.go`): `RANGE COLUMNS`, `LIST COLUMNS`, `KEY`.
        let (method, expression) = match &partition.kind {
            tidb_executor::PartitionKind::RangeColumns { .. } => {
                ("RANGE COLUMNS".to_owned(), partition.expr_text.clone())
            }
            tidb_executor::PartitionKind::ListColumns { .. } => {
                ("LIST COLUMNS".to_owned(), partition.expr_text.clone())
            }
            tidb_executor::PartitionKind::Key => ("KEY".to_owned(), partition.expr_text.clone()),
            other => (other.sql().to_owned(), partition.expr_text.clone()),
        };
        for (ordinal, definition) in partition.definitions.iter().enumerate() {
            let (row_count, average_row_length, data_length, index_length) =
                table.partition_storage_statistics(definition.id);
            // Go's `PARTITION_DESCRIPTION`: the RANGE bounds joined by commas,
            // or the LIST tuples with multi-column ones parenthesised.
            let description = match &partition.kind {
                tidb_executor::PartitionKind::Range { .. }
                | tidb_executor::PartitionKind::RangeColumns { .. } => {
                    definition.less_than.join(",")
                }
                tidb_executor::PartitionKind::List { .. }
                | tidb_executor::PartitionKind::ListColumns { .. } => definition
                    .in_values
                    .iter()
                    .map(|tuple| {
                        if tuple.len() == 1 {
                            tuple[0].clone()
                        } else {
                            format!("({})", tuple.join(","))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(","),
                _ => String::new(),
            };
            let description = if description.is_empty() {
                Datum::Null
            } else {
                Datum::Bytes(description.into_bytes())
            };
            let policy = definition
                .placement_policy
                .as_ref()
                .map_or(Datum::Null, |reference| {
                    Datum::Bytes(reference.name.original().as_bytes().to_vec())
                });
            let comment = if definition.comment.is_empty() {
                Datum::Null
            } else {
                Datum::Bytes(definition.comment.clone().into_bytes())
            };
            rows.push(vec![
                catalog_value(),
                Datum::Bytes(schema.clone().into_bytes()),
                Datum::Bytes(table_name.clone().into_bytes()),
                Datum::Bytes(definition.name.clone().into_bytes()),
                Datum::Null,
                // Go's ordinal is ONE-based.
                Datum::Int(ordinal as i64 + 1),
                Datum::Null,
                Datum::Bytes(method.clone().into_bytes()),
                Datum::Null,
                Datum::Bytes(expression.clone().into_bytes()),
                Datum::Null,
                description,
                Datum::UInt(row_count),
                Datum::UInt(average_row_length),
                Datum::UInt(data_length),
                Datum::Null,
                Datum::UInt(index_length),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::Null,
                comment,
                Datum::Null,
                Datum::Null,
                Datum::Int(definition.id),
                policy,
                Datum::Null,
                Datum::Null,
            ]);
        }
    }
    rows
}

/// Go `tableIDMap` (`pkg/infoschema/tables.go:253`): every
/// `INFORMATION_SCHEMA` memory table's id offset from
/// `autoid.InformationSchemaDBID` (`SystemSchemaIDFlag | 1`, `1<<62 | 1`).
///
/// Transcribed whole, including the gaps Go's own comments explain (14,
/// 27-29, 66 removed in issues 9154/28890) and EXCLUDING the one entry that
/// is not a table at all -- Go maps `CatalogVal` (`"def"`) to offset 9, a
/// stray in its own map that never registers as a memory table.
///
/// This is what `information_schema.tables` reports as `TIDB_TABLE_ID` for
/// the schema's own tables, and the ids are OBSERVABLE: `infoschema/v2`
/// filters `where TIDB_TABLE_ID = 4611686018427387967` and expects
/// `CLUSTER_STATEMENTS_SUMMARY_HISTORY` -- `(1<<62|1) + 62`.
const INFORMATION_SCHEMA_TABLE_IDS: &[(i64, &str)] = &[
    (1, "SCHEMATA"),
    (2, "TABLES"),
    (3, "COLUMNS"),
    (4, "COLUMN_STATISTICS"),
    (5, "STATISTICS"),
    (6, "CHARACTER_SETS"),
    (7, "COLLATIONS"),
    (8, "FILES"),
    (10, "PROFILING"),
    (11, "PARTITIONS"),
    (12, "KEY_COLUMN_USAGE"),
    (13, "REFERENTIAL_CONSTRAINTS"),
    (15, "PLUGINS"),
    (16, "TABLE_CONSTRAINTS"),
    (17, "TRIGGERS"),
    (18, "USER_PRIVILEGES"),
    (19, "SCHEMA_PRIVILEGES"),
    (20, "TABLE_PRIVILEGES"),
    (21, "COLUMN_PRIVILEGES"),
    (22, "ENGINES"),
    (23, "VIEWS"),
    (24, "ROUTINES"),
    (25, "PARAMETERS"),
    (26, "EVENTS"),
    (30, "OPTIMIZER_TRACE"),
    (31, "TABLESPACES"),
    (32, "COLLATION_CHARACTER_SET_APPLICABILITY"),
    (33, "PROCESSLIST"),
    (34, "TIDB_INDEXES"),
    (35, "SLOW_QUERY"),
    (36, "TIDB_HOT_REGIONS"),
    (37, "TIKV_STORE_STATUS"),
    (38, "ANALYZE_STATUS"),
    (39, "TIKV_REGION_STATUS"),
    (40, "TIKV_REGION_PEERS"),
    (41, "TIDB_SERVERS_INFO"),
    (42, "CLUSTER_INFO"),
    (43, "CLUSTER_CONFIG"),
    (44, "CLUSTER_LOAD"),
    (45, "TIFLASH_REPLICA"),
    (46, "CLUSTER_SLOW_QUERY"),
    (47, "CLUSTER_PROCESSLIST"),
    (48, "CLUSTER_LOG"),
    (49, "CLUSTER_HARDWARE"),
    (50, "CLUSTER_SYSTEMINFO"),
    (51, "INSPECTION_RESULT"),
    (52, "METRICS_SUMMARY"),
    (53, "METRICS_SUMMARY_BY_LABEL"),
    (54, "METRICS_TABLES"),
    (55, "INSPECTION_SUMMARY"),
    (56, "INSPECTION_RULES"),
    (57, "DDL_JOBS"),
    (58, "SEQUENCES"),
    (59, "STATEMENTS_SUMMARY"),
    (60, "STATEMENTS_SUMMARY_HISTORY"),
    (61, "CLUSTER_STATEMENTS_SUMMARY"),
    (62, "CLUSTER_STATEMENTS_SUMMARY_HISTORY"),
    (63, "TABLE_STORAGE_STATS"),
    (64, "TIFLASH_TABLES"),
    (65, "TIFLASH_SEGMENTS"),
    (67, "CLIENT_ERRORS_SUMMARY_GLOBAL"),
    (68, "CLIENT_ERRORS_SUMMARY_BY_USER"),
    (69, "CLIENT_ERRORS_SUMMARY_BY_HOST"),
    (70, "TIDB_TRX"),
    (71, "CLUSTER_TIDB_TRX"),
    (72, "DEADLOCKS"),
    (73, "CLUSTER_DEADLOCKS"),
    (74, "DATA_LOCK_WAITS"),
    (75, "STATEMENTS_SUMMARY_EVICTED"),
    (76, "CLUSTER_STATEMENTS_SUMMARY_EVICTED"),
    (77, "ATTRIBUTES"),
    (78, "TIDB_HOT_REGIONS_HISTORY"),
    (79, "PLACEMENT_POLICIES"),
    (80, "TRX_SUMMARY"),
    (81, "CLUSTER_TRX_SUMMARY"),
    (82, "VARIABLES_INFO"),
    (83, "USER_ATTRIBUTES"),
    (84, "MEMORY_USAGE"),
    (85, "MEMORY_USAGE_OPS_HISTORY"),
    (86, "CLUSTER_MEMORY_USAGE"),
    (87, "CLUSTER_MEMORY_USAGE_OPS_HISTORY"),
    (88, "RESOURCE_GROUPS"),
    (89, "RUNAWAY_WATCHES"),
    (90, "CHECK_CONSTRAINTS"),
    (91, "TIDB_CHECK_CONSTRAINTS"),
    (92, "KEYWORDS"),
    (93, "TIDB_INDEX_USAGE"),
    (94, "CLUSTER_TIDB_INDEX_USAGE"),
    (95, "TIFLASH_INDEXES"),
    (96, "TIDB_PLAN_CACHE"),
    (97, "CLUSTER_TIDB_PLAN_CACHE"),
    (98, "TIDB_STATEMENTS_STATS"),
    (99, "CLUSTER_TIDB_STATEMENTS_STATS"),
    (100, "KEYSPACE_META"),
    (101, "SCHEMATA_EXTENSIONS"),
];

/// Go `autoid.InformationSchemaDBID`.
const INFORMATION_SCHEMA_DB_ID: i64 = (1 << 62) | 1;

/// The `information_schema.tables` rows for the schema's OWN tables: Go
/// `infoschema_reader.go`'s `setDataFromOneTable` with `metadef.IsMemDB`
/// true, so `TABLE_TYPE` is `SYSTEM VIEW`, the storage numbers are zero, and
/// `TIDB_TABLE_ID` comes from `tableIDMap`. Emitted for EVERY table Go
/// registers, served here or not -- the id and the name are the table's
/// METADATA, from Go's own source; only QUERYING an unserved one refuses.
fn information_schema_tables_rows() -> Vec<Vec<Datum>> {
    INFORMATION_SCHEMA_TABLE_IDS
        .iter()
        .map(|(offset, name)| {
            let mut row = vec![
                text(CATALOG),
                text(INFORMATION_SCHEMA),
                text(name),
                text("SYSTEM VIEW"),
                text("InnoDB"),
                Datum::Int(10),
                text("Compact"),
            ];
            // TABLE_ROWS through AUTO_INCREMENT: zero, as Go's
            // `EstimateDataLength` answers for a memory table -- the same
            // seven cells the base-table row above carries.
            row.extend(std::iter::repeat_n(Datum::Int(0), 7));
            // CREATE_TIME, UPDATE_TIME, CHECK_TIME.
            row.extend(std::iter::repeat_n(Datum::Null, 3));
            row.push(text("utf8mb4_bin"));
            row.push(Datum::Null);
            row.push(text(""));
            row.push(text(""));
            row.push(Datum::Int(INFORMATION_SCHEMA_DB_ID + offset));
            row.push(Datum::Null);
            row.push(text("NONCLUSTERED"));
            row.push(Datum::Null);
            row.push(text("Normal"));
            row.push(Datum::Null);
            row.push(text(""));
            row
        })
        .collect()
}

// Generated from the go oracle: `SHOW TABLES` + TIDB_TABLE_ID per system schema.

pub const METRICS_SCHEMA_TABLES: &[(&str, i64)] = &[
    ("abnormal_stores", 4611686018427407905),
    ("etcd_disk_wal_fsync_rate", 4611686018427407906),
    ("etcd_wal_fsync_duration", 4611686018427407907),
    ("etcd_wal_fsync_total_count", 4611686018427407908),
    ("etcd_wal_fsync_total_time", 4611686018427407909),
    ("go_gc_count", 4611686018427407910),
    ("go_gc_cpu_usage", 4611686018427407911),
    ("go_gc_duration", 4611686018427407912),
    ("go_heap_mem_usage", 4611686018427407913),
    ("go_threads", 4611686018427407914),
    ("goroutines_count", 4611686018427407915),
    ("node_cpu_usage", 4611686018427407916),
    ("node_disk_available_size", 4611686018427407917),
    ("node_disk_io_util", 4611686018427407918),
    ("node_disk_iops", 4611686018427407919),
    ("node_disk_read_latency", 4611686018427407920),
    ("node_disk_size", 4611686018427407921),
    ("node_disk_state", 4611686018427407922),
    ("node_disk_throughput", 4611686018427407923),
    ("node_disk_usage", 4611686018427407924),
    ("node_disk_write_latency", 4611686018427407925),
    ("node_file_descriptor_allocated", 4611686018427407926),
    ("node_kernel_context_switches", 4611686018427407927),
    ("node_kernel_forks", 4611686018427407928),
    ("node_kernel_interrupts", 4611686018427407929),
    ("node_load1", 4611686018427407930),
    ("node_load15", 4611686018427407931),
    ("node_load5", 4611686018427407932),
    ("node_memory_active", 4611686018427407933),
    ("node_memory_available", 4611686018427407934),
    ("node_memory_buffers", 4611686018427407935),
    ("node_memory_cached", 4611686018427407936),
    ("node_memory_dirty", 4611686018427407937),
    ("node_memory_free", 4611686018427407938),
    ("node_memory_inactive", 4611686018427407939),
    ("node_memory_mapped", 4611686018427407940),
    ("node_memory_shared", 4611686018427407941),
    ("node_memory_swap_used", 4611686018427407942),
    ("node_memory_usage", 4611686018427407943),
    ("node_memory_writeback", 4611686018427407944),
    ("node_memory_writeback_tmp", 4611686018427407945),
    ("node_network_in_drops", 4611686018427407946),
    ("node_network_in_errors", 4611686018427407947),
    ("node_network_in_errors_total_count", 4611686018427407948),
    ("node_network_in_packets", 4611686018427407949),
    ("node_network_in_traffic", 4611686018427407950),
    ("node_network_interface_speed", 4611686018427407951),
    ("node_network_out_drops", 4611686018427407952),
    ("node_network_out_errors", 4611686018427407953),
    ("node_network_out_errors_total_count", 4611686018427407954),
    ("node_network_out_packets", 4611686018427407955),
    ("node_network_out_traffic", 4611686018427407956),
    ("node_network_utilization_in_hourly", 4611686018427407957),
    ("node_network_utilization_out_hourly", 4611686018427407958),
    ("node_process_open_fd_count", 4611686018427407959),
    ("node_processes_blocked", 4611686018427407960),
    ("node_processes_running", 4611686018427407961),
    ("node_tcp_connections", 4611686018427407962),
    ("node_tcp_in_use", 4611686018427407963),
    ("node_tcp_segments_retransmitted", 4611686018427407964),
    ("node_total_memory", 4611686018427407965),
    ("node_uptime", 4611686018427407966),
    ("node_virtual_cpus", 4611686018427407967),
    ("normal_stores", 4611686018427407968),
    ("pd_balance_scheduler_status", 4611686018427407969),
    ("pd_checker_event_count", 4611686018427407970),
    ("pd_client_cmd_duration", 4611686018427407971),
    ("pd_client_cmd_ops", 4611686018427407972),
    ("pd_client_cmd_total_count", 4611686018427407973),
    ("pd_client_cmd_total_time", 4611686018427407974),
    ("pd_cluster_metadata", 4611686018427407975),
    ("pd_cluster_status", 4611686018427407976),
    ("pd_cmd_fail_ops", 4611686018427407977),
    ("pd_cmd_fail_total_count", 4611686018427407978),
    ("pd_grpc_completed_commands_duration", 4611686018427407979),
    ("pd_grpc_completed_commands_rate", 4611686018427407980),
    (
        "pd_grpc_completed_commands_total_count",
        4611686018427407981,
    ),
    ("pd_grpc_completed_commands_total_time", 4611686018427407982),
    ("pd_handle_transactions_duration", 4611686018427407983),
    ("pd_handle_transactions_rate", 4611686018427407984),
    ("pd_handle_transactions_total_count", 4611686018427407985),
    ("pd_handle_transactions_total_time", 4611686018427407986),
    ("pd_hotspot_status", 4611686018427407987),
    ("pd_label_distribution", 4611686018427407988),
    ("pd_operator_finish_duration", 4611686018427407989),
    ("pd_operator_finish_total_count", 4611686018427407990),
    ("pd_operator_finish_total_time", 4611686018427407991),
    ("pd_operator_step_finish_duration", 4611686018427407992),
    ("pd_operator_step_finish_total_count", 4611686018427407993),
    ("pd_operator_step_finish_total_time", 4611686018427407994),
    ("pd_peer_round_trip_duration", 4611686018427407995),
    ("pd_peer_round_trip_total_count", 4611686018427407996),
    ("pd_peer_round_trip_total_time", 4611686018427407997),
    ("pd_region_health", 4611686018427407998),
    ("pd_region_heartbeat_duration", 4611686018427407999),
    ("pd_region_heartbeat_total_count", 4611686018427408000),
    ("pd_region_heartbeat_total_time", 4611686018427408001),
    ("pd_region_label_isolation_level", 4611686018427408002),
    ("pd_region_syncer_status", 4611686018427408003),
    ("pd_request_rpc_duration", 4611686018427408004),
    ("pd_request_rpc_duration_avg", 4611686018427408005),
    ("pd_request_rpc_ops", 4611686018427408006),
    ("pd_request_rpc_total_count", 4611686018427408007),
    ("pd_request_rpc_total_time", 4611686018427408008),
    ("pd_role", 4611686018427408009),
    ("pd_schedule_filter", 4611686018427408010),
    ("pd_schedule_operator", 4611686018427408011),
    ("pd_schedule_operator_total_num", 4611686018427408012),
    ("pd_schedule_store_limit", 4611686018427408013),
    ("pd_scheduler_balance_direction", 4611686018427408014),
    ("pd_scheduler_balance_leader", 4611686018427408015),
    ("pd_scheduler_balance_region", 4611686018427408016),
    ("pd_scheduler_config", 4611686018427408017),
    ("pd_scheduler_op_influence", 4611686018427408018),
    ("pd_scheduler_region_heartbeat", 4611686018427408019),
    ("pd_scheduler_status", 4611686018427408020),
    ("pd_scheduler_store_status", 4611686018427408021),
    ("pd_scheduler_tolerant_resource", 4611686018427408022),
    ("pd_server_etcd_state", 4611686018427408023),
    ("pd_start_tso_wait_duration", 4611686018427408024),
    ("pd_start_tso_wait_total_count", 4611686018427408025),
    ("pd_start_tso_wait_total_time", 4611686018427408026),
    ("pd_tso_rpc_duration", 4611686018427408027),
    ("pd_tso_rpc_total_count", 4611686018427408028),
    ("pd_tso_rpc_total_time", 4611686018427408029),
    ("pd_tso_wait_duration", 4611686018427408030),
    ("pd_tso_wait_total_count", 4611686018427408031),
    ("pd_tso_wait_total_time", 4611686018427408032),
    ("process_cpu_usage", 4611686018427408033),
    ("resource_manager_resource_unit", 4611686018427408034),
    ("store_available_ratio", 4611686018427408035),
    ("store_size_amplification", 4611686018427408036),
    ("tidb_auto_id_qps", 4611686018427408037),
    ("tidb_auto_id_request_duration", 4611686018427408038),
    ("tidb_auto_id_request_total_count", 4611686018427408039),
    ("tidb_auto_id_request_total_time", 4611686018427408040),
    ("tidb_batch_client_pending_req_count", 4611686018427408041),
    (
        "tidb_batch_client_unavailable_duration",
        4611686018427408042,
    ),
    (
        "tidb_batch_client_unavailable_total_count",
        4611686018427408043,
    ),
    (
        "tidb_batch_client_unavailable_total_time",
        4611686018427408044,
    ),
    ("tidb_batch_client_wait_conn_duration", 4611686018427408045),
    (
        "tidb_batch_client_wait_conn_total_count",
        4611686018427408046,
    ),
    (
        "tidb_batch_client_wait_conn_total_time",
        4611686018427408047,
    ),
    ("tidb_batch_client_wait_duration", 4611686018427408048),
    ("tidb_batch_client_wait_total_count", 4611686018427408049),
    ("tidb_batch_client_wait_total_time", 4611686018427408050),
    ("tidb_binlog_error_count", 4611686018427408051),
    ("tidb_binlog_error_total_count", 4611686018427408052),
    ("tidb_compile_duration", 4611686018427408053),
    ("tidb_compile_total_count", 4611686018427408054),
    ("tidb_compile_total_time", 4611686018427408055),
    ("tidb_connection_count", 4611686018427408056),
    ("tidb_connection_idle_duration", 4611686018427408057),
    ("tidb_connection_idle_total_count", 4611686018427408058),
    ("tidb_connection_idle_total_time", 4611686018427408059),
    ("tidb_cop_duration", 4611686018427408060),
    ("tidb_cop_total_count", 4611686018427408061),
    ("tidb_cop_total_time", 4611686018427408062),
    ("tidb_ddl_add_index_speed", 4611686018427408063),
    ("tidb_ddl_batch_add_index_duration", 4611686018427408064),
    ("tidb_ddl_batch_add_index_total_count", 4611686018427408065),
    ("tidb_ddl_batch_add_index_total_time", 4611686018427408066),
    ("tidb_ddl_deploy_syncer_duration", 4611686018427408067),
    ("tidb_ddl_deploy_syncer_total_count", 4611686018427408068),
    ("tidb_ddl_deploy_syncer_total_time", 4611686018427408069),
    ("tidb_ddl_duration", 4611686018427408070),
    ("tidb_ddl_meta_opm", 4611686018427408071),
    ("tidb_ddl_opm", 4611686018427408072),
    ("tidb_ddl_total_count", 4611686018427408073),
    ("tidb_ddl_total_time", 4611686018427408074),
    ("tidb_ddl_update_self_version_duration", 4611686018427408075),
    (
        "tidb_ddl_update_self_version_total_count",
        4611686018427408076,
    ),
    (
        "tidb_ddl_update_self_version_total_time",
        4611686018427408077,
    ),
    ("tidb_ddl_waiting_jobs_num", 4611686018427408078),
    ("tidb_ddl_worker_duration", 4611686018427408079),
    ("tidb_ddl_worker_total_count", 4611686018427408080),
    ("tidb_ddl_worker_total_time", 4611686018427408081),
    ("tidb_distsql_copr_cache", 4611686018427408082),
    ("tidb_distsql_execution_duration", 4611686018427408083),
    ("tidb_distsql_execution_total_count", 4611686018427408084),
    ("tidb_distsql_execution_total_time", 4611686018427408085),
    ("tidb_distsql_partial_num", 4611686018427408086),
    ("tidb_distsql_partial_num_total_count", 4611686018427408087),
    ("tidb_distsql_partial_qps", 4611686018427408088),
    ("tidb_distsql_partial_scan_key_num", 4611686018427408089),
    (
        "tidb_distsql_partial_scan_key_num_total_count",
        4611686018427408090,
    ),
    (
        "tidb_distsql_partial_scan_key_total_num",
        4611686018427408091,
    ),
    ("tidb_distsql_partial_total_num", 4611686018427408092),
    ("tidb_distsql_qps", 4611686018427408093),
    ("tidb_distsql_scan_key_num", 4611686018427408094),
    ("tidb_distsql_scan_key_num_total_count", 4611686018427408095),
    ("tidb_distsql_scan_key_total_num", 4611686018427408096),
    ("tidb_event_opm", 4611686018427408097),
    ("tidb_execute_duration", 4611686018427408098),
    ("tidb_execute_total_count", 4611686018427408099),
    ("tidb_execute_total_time", 4611686018427408100),
    ("tidb_expensive_executors_ops", 4611686018427408101),
    ("tidb_failed_query_opm", 4611686018427408102),
    ("tidb_gc_action_result_opm", 4611686018427408103),
    ("tidb_gc_config", 4611686018427408104),
    ("tidb_gc_delete_range_fail_opm", 4611686018427408105),
    ("tidb_gc_delete_range_task_status", 4611686018427408106),
    ("tidb_gc_duration", 4611686018427408107),
    ("tidb_gc_fail_opm", 4611686018427408108),
    ("tidb_gc_push_task_duration", 4611686018427408109),
    ("tidb_gc_push_task_total_count", 4611686018427408110),
    ("tidb_gc_push_task_total_time", 4611686018427408111),
    ("tidb_gc_too_many_locks_opm", 4611686018427408112),
    ("tidb_gc_total_count", 4611686018427408113),
    ("tidb_gc_total_time", 4611686018427408114),
    ("tidb_gc_worker_action_opm", 4611686018427408115),
    ("tidb_get_token_duration", 4611686018427408116),
    ("tidb_get_token_total_count", 4611686018427408117),
    ("tidb_get_token_total_time", 4611686018427408118),
    ("tidb_handshake_error_opm", 4611686018427408119),
    ("tidb_handshake_error_total_count", 4611686018427408120),
    ("tidb_ia_remote_read_segment_count", 4611686018427408121),
    ("tidb_ia_remote_read_segment_size", 4611686018427408122),
    (
        "tidb_ia_remote_read_segment_wait_time_histogram",
        4611686018427408123,
    ),
    ("tidb_keep_alive_opm", 4611686018427408124),
    ("tidb_kv_backoff_duration", 4611686018427408125),
    ("tidb_kv_backoff_ops", 4611686018427408126),
    ("tidb_kv_backoff_total_count", 4611686018427408127),
    ("tidb_kv_backoff_total_time", 4611686018427408128),
    ("tidb_kv_region_error_ops", 4611686018427408129),
    ("tidb_kv_region_error_total_count", 4611686018427408130),
    ("tidb_kv_request_duration", 4611686018427408131),
    ("tidb_kv_request_ops", 4611686018427408132),
    ("tidb_kv_request_total_count", 4611686018427408133),
    ("tidb_kv_request_total_time", 4611686018427408134),
    ("tidb_kv_snapshot_ops", 4611686018427408135),
    ("tidb_kv_txn_ops", 4611686018427408136),
    ("tidb_kv_write_num", 4611686018427408137),
    ("tidb_kv_write_num_total_count", 4611686018427408138),
    ("tidb_kv_write_size", 4611686018427408139),
    ("tidb_kv_write_size_total_count", 4611686018427408140),
    ("tidb_kv_write_total_num", 4611686018427408141),
    ("tidb_kv_write_total_size", 4611686018427408142),
    ("tidb_load_privilege_ops", 4611686018427408143),
    ("tidb_load_safepoint_fail_ops", 4611686018427408144),
    ("tidb_load_safepoint_ops", 4611686018427408145),
    ("tidb_load_safepoint_total_num", 4611686018427408146),
    ("tidb_load_schema_duration", 4611686018427408147),
    ("tidb_load_schema_ops", 4611686018427408148),
    ("tidb_load_schema_total_count", 4611686018427408149),
    ("tidb_load_schema_total_time", 4611686018427408150),
    ("tidb_lock_cleanup_fail_ops", 4611686018427408151),
    ("tidb_lock_resolver_ops", 4611686018427408152),
    ("tidb_lock_resolver_total_num", 4611686018427408153),
    ("tidb_meta_operation_duration", 4611686018427408154),
    ("tidb_meta_operation_total_count", 4611686018427408155),
    ("tidb_meta_operation_total_time", 4611686018427408156),
    ("tidb_new_etcd_session_duration", 4611686018427408157),
    ("tidb_new_etcd_session_total_count", 4611686018427408158),
    ("tidb_new_etcd_session_total_time", 4611686018427408159),
    ("tidb_ops_internal", 4611686018427408160),
    ("tidb_ops_statement", 4611686018427408161),
    ("tidb_owner_handle_syncer_duration", 4611686018427408162),
    ("tidb_owner_handle_syncer_total_count", 4611686018427408163),
    ("tidb_owner_handle_syncer_total_time", 4611686018427408164),
    ("tidb_owner_watcher_ops", 4611686018427408165),
    ("tidb_panic_count", 4611686018427408166),
    ("tidb_panic_count_total_count", 4611686018427408167),
    ("tidb_parse_duration", 4611686018427408168),
    ("tidb_parse_total_count", 4611686018427408169),
    ("tidb_parse_total_time", 4611686018427408170),
    ("tidb_prepared_statement_count", 4611686018427408171),
    ("tidb_process_mem_usage", 4611686018427408172),
    ("tidb_qps", 4611686018427408173),
    ("tidb_qps_ideal", 4611686018427408174),
    ("tidb_query_duration", 4611686018427408175),
    ("tidb_query_total_count", 4611686018427408176),
    ("tidb_query_total_time", 4611686018427408177),
    ("tidb_query_using_plan_cache_ops", 4611686018427408178),
    ("tidb_region_cache_ops", 4611686018427408179),
    ("tidb_schema_lease_error_opm", 4611686018427408180),
    ("tidb_schema_lease_error_total_count", 4611686018427408181),
    ("tidb_server_maxprocs", 4611686018427408182),
    ("tidb_slow_query_cop_process_duration", 4611686018427408183),
    (
        "tidb_slow_query_cop_process_total_count",
        4611686018427408184,
    ),
    (
        "tidb_slow_query_cop_process_total_time",
        4611686018427408185,
    ),
    ("tidb_slow_query_cop_wait_duration", 4611686018427408186),
    ("tidb_slow_query_cop_wait_total_count", 4611686018427408187),
    ("tidb_slow_query_cop_wait_total_time", 4611686018427408188),
    ("tidb_slow_query_duration", 4611686018427408189),
    ("tidb_slow_query_qps", 4611686018427408190),
    ("tidb_slow_query_total_count", 4611686018427408191),
    ("tidb_slow_query_total_time", 4611686018427408192),
    ("tidb_statistics_auto_analyze_duration", 4611686018427408193),
    ("tidb_statistics_auto_analyze_ops", 4611686018427408194),
    (
        "tidb_statistics_auto_analyze_total_count",
        4611686018427408195,
    ),
    (
        "tidb_statistics_auto_analyze_total_time",
        4611686018427408196,
    ),
    ("tidb_statistics_manual_analyze_ops", 4611686018427408197),
    ("tidb_statistics_pseudo_estimation_ops", 4611686018427408198),
    (
        "tidb_statistics_pseudo_estimation_total_count",
        4611686018427408199,
    ),
    ("tidb_statistics_stats_inaccuracy_rate", 4611686018427408200),
    (
        "tidb_statistics_stats_inaccuracy_rate_total_count",
        4611686018427408201,
    ),
    (
        "tidb_statistics_stats_inaccuracy_total_rate",
        4611686018427408202,
    ),
    ("tidb_statistics_update_stats_ops", 4611686018427408203),
    (
        "tidb_statistics_update_stats_total_count",
        4611686018427408204,
    ),
    ("tidb_time_jump_back_ops", 4611686018427408205),
    ("tidb_transaction_duration", 4611686018427408206),
    (
        "tidb_transaction_local_latch_wait_duration",
        4611686018427408207,
    ),
    (
        "tidb_transaction_local_latch_wait_total_count",
        4611686018427408208,
    ),
    (
        "tidb_transaction_local_latch_wait_total_time",
        4611686018427408209,
    ),
    ("tidb_transaction_ops", 4611686018427408210),
    ("tidb_transaction_retry_error_ops", 4611686018427408211),
    (
        "tidb_transaction_retry_error_total_count",
        4611686018427408212,
    ),
    ("tidb_transaction_retry_num", 4611686018427408213),
    (
        "tidb_transaction_retry_num_total_count",
        4611686018427408214,
    ),
    ("tidb_transaction_retry_total_num", 4611686018427408215),
    ("tidb_transaction_statement_num", 4611686018427408216),
    (
        "tidb_transaction_statement_num_total_count",
        4611686018427408217,
    ),
    ("tidb_transaction_statement_total_num", 4611686018427408218),
    ("tidb_transaction_total_count", 4611686018427408219),
    ("tidb_transaction_total_time", 4611686018427408220),
    ("tidb_txn_cmd_duration", 4611686018427408221),
    ("tidb_txn_cmd_total_count", 4611686018427408222),
    ("tidb_txn_cmd_total_time", 4611686018427408223),
    ("tidb_txn_region_num", 4611686018427408224),
    ("tidb_txn_region_num_total_count", 4611686018427408225),
    ("tidb_txn_region_total_num", 4611686018427408226),
    ("tiflash_cpu_quota", 4611686018427408227),
    ("tiflash_process_cpu_usage", 4611686018427408228),
    (
        "tiflash_resource_manager_resource_unit",
        4611686018427408229,
    ),
    ("tikv_active_written_leaders", 4611686018427408230),
    ("tikv_admin_apply", 4611686018427408231),
    ("tikv_allocator_stats", 4611686018427408232),
    ("tikv_apply_avg_wait_duration", 4611686018427408233),
    ("tikv_approximate_avg_region_size", 4611686018427408234),
    ("tikv_approximate_region_size", 4611686018427408235),
    (
        "tikv_approximate_region_size_histogram",
        4611686018427408236,
    ),
    (
        "tikv_approximate_region_size_total_count",
        4611686018427408237,
    ),
    ("tikv_approximate_region_total_size", 4611686018427408238),
    ("tikv_auto_gc_progress", 4611686018427408239),
    ("tikv_auto_gc_safepoint", 4611686018427408240),
    ("tikv_auto_gc_working", 4611686018427408241),
    ("tikv_average_grpc_messge_duration", 4611686018427408242),
    ("tikv_backup_avg_duration", 4611686018427408243),
    ("tikv_backup_duration", 4611686018427408244),
    ("tikv_backup_errors", 4611686018427408245),
    ("tikv_backup_errors_total_count", 4611686018427408246),
    ("tikv_backup_flow", 4611686018427408247),
    ("tikv_backup_range_avg_duration", 4611686018427408248),
    ("tikv_backup_range_duration", 4611686018427408249),
    ("tikv_backup_range_size", 4611686018427408250),
    ("tikv_backup_range_size_total_count", 4611686018427408251),
    ("tikv_backup_range_total_count", 4611686018427408252),
    ("tikv_backup_range_total_size", 4611686018427408253),
    ("tikv_backup_range_total_time", 4611686018427408254),
    ("tikv_backup_total_count", 4611686018427408255),
    ("tikv_backup_total_time", 4611686018427408256),
    ("tikv_block_all_cache_hit", 4611686018427408257),
    ("tikv_block_bloom_prefix_cache_hit", 4611686018427408258),
    ("tikv_block_cache_size", 4611686018427408259),
    ("tikv_block_data_cache_hit", 4611686018427408260),
    ("tikv_block_filter_cache_hit", 4611686018427408261),
    ("tikv_block_index_cache_hit", 4611686018427408262),
    ("tikv_channel_full", 4611686018427408263),
    ("tikv_channel_full_total_count", 4611686018427408264),
    ("tikv_check_split", 4611686018427408265),
    ("tikv_check_split_duration", 4611686018427408266),
    ("tikv_check_split_total_count", 4611686018427408267),
    ("tikv_check_split_total_time", 4611686018427408268),
    ("tikv_client_task_progress", 4611686018427408269),
    ("tikv_compaction_duration", 4611686018427408270),
    ("tikv_compaction_max_duration", 4611686018427408271),
    ("tikv_compaction_operations", 4611686018427408272),
    ("tikv_compaction_pending_bytes", 4611686018427408273),
    ("tikv_compaction_reason", 4611686018427408274),
    ("tikv_compression_ratio", 4611686018427408275),
    ("tikv_config_raftstore", 4611686018427408276),
    ("tikv_config_rocksdb", 4611686018427408277),
    ("tikv_cop_dag_executors_ops", 4611686018427408278),
    ("tikv_cop_dag_requests_ops", 4611686018427408279),
    ("tikv_cop_handle_duration", 4611686018427408280),
    ("tikv_cop_handle_total_count", 4611686018427408281),
    ("tikv_cop_handle_total_time", 4611686018427408282),
    ("tikv_cop_kv_cursor_operations", 4611686018427408283),
    (
        "tikv_cop_kv_cursor_operations_total_count",
        4611686018427408284,
    ),
    ("tikv_cop_request_duration", 4611686018427408285),
    ("tikv_cop_request_durations", 4611686018427408286),
    ("tikv_cop_request_total_count", 4611686018427408287),
    ("tikv_cop_request_total_time", 4611686018427408288),
    ("tikv_cop_requests_ops", 4611686018427408289),
    ("tikv_cop_scan_details", 4611686018427408290),
    ("tikv_cop_scan_details_total", 4611686018427408291),
    ("tikv_cop_scan_keys_num", 4611686018427408292),
    ("tikv_cop_scan_keys_total_num", 4611686018427408293),
    (
        "tikv_cop_total_response_size_per_seconds",
        4611686018427408294,
    ),
    ("tikv_cop_total_response_total_size", 4611686018427408295),
    (
        "tikv_cop_total_rocksdb_perf_statistics",
        4611686018427408296,
    ),
    ("tikv_cop_wait_duration", 4611686018427408297),
    ("tikv_cop_wait_total_count", 4611686018427408298),
    ("tikv_cop_wait_total_time", 4611686018427408299),
    ("tikv_coprocessor_is_busy", 4611686018427408300),
    ("tikv_coprocessor_is_busy_total_count", 4611686018427408301),
    ("tikv_coprocessor_request_error", 4611686018427408302),
    (
        "tikv_coprocessor_request_error_total_count",
        4611686018427408303,
    ),
    ("tikv_corrrput_keys_flow", 4611686018427408304),
    ("tikv_cpu_quota", 4611686018427408305),
    ("tikv_critical_error", 4611686018427408306),
    ("tikv_critical_error_total_count", 4611686018427408307),
    ("tikv_disk_read_bytes", 4611686018427408308),
    ("tikv_disk_write_bytes", 4611686018427408309),
    ("tikv_engine_avg_get_duration", 4611686018427408310),
    ("tikv_engine_avg_seek_duration", 4611686018427408311),
    ("tikv_engine_blob_bytes_flow", 4611686018427408312),
    ("tikv_engine_blob_file_count", 4611686018427408313),
    ("tikv_engine_blob_file_read_duration", 4611686018427408314),
    ("tikv_engine_blob_file_size", 4611686018427408315),
    ("tikv_engine_blob_file_sync_duration", 4611686018427408316),
    ("tikv_engine_blob_file_sync_operations", 4611686018427408317),
    ("tikv_engine_blob_file_write_duration", 4611686018427408318),
    ("tikv_engine_blob_gc_bytes_flow", 4611686018427408319),
    ("tikv_engine_blob_gc_duration", 4611686018427408320),
    ("tikv_engine_blob_gc_file", 4611686018427408321),
    ("tikv_engine_blob_gc_keys_flow", 4611686018427408322),
    ("tikv_engine_blob_get_duration", 4611686018427408323),
    ("tikv_engine_blob_key_avg_size", 4611686018427408324),
    ("tikv_engine_blob_key_max_size", 4611686018427408325),
    ("tikv_engine_blob_seek_duration", 4611686018427408326),
    ("tikv_engine_blob_seek_operations", 4611686018427408327),
    ("tikv_engine_blob_value_avg_size", 4611686018427408328),
    ("tikv_engine_blob_value_max_size", 4611686018427408329),
    ("tikv_engine_compaction_flow_bytes", 4611686018427408330),
    (
        "tikv_engine_get_block_cache_operations",
        4611686018427408331,
    ),
    ("tikv_engine_get_cpu_cache_operations", 4611686018427408332),
    ("tikv_engine_get_memtable_operations", 4611686018427408333),
    ("tikv_engine_live_blob_size", 4611686018427408334),
    ("tikv_engine_max_get_duration", 4611686018427408335),
    ("tikv_engine_max_seek_duration", 4611686018427408336),
    ("tikv_engine_seek_operations", 4611686018427408337),
    ("tikv_engine_size", 4611686018427408338),
    ("tikv_engine_wal_sync_operations", 4611686018427408339),
    ("tikv_engine_write_duration", 4611686018427408340),
    ("tikv_engine_write_max_duration", 4611686018427408341),
    ("tikv_engine_write_operations", 4611686018427408342),
    ("tikv_engine_write_stall", 4611686018427408343),
    ("tikv_flow_mbps", 4611686018427408344),
    ("tikv_flush_messages", 4611686018427408345),
    ("tikv_flush_messages_total_num", 4611686018427408346),
    ("tikv_futurepool_handled_tasks", 4611686018427408347),
    (
        "tikv_futurepool_handled_tasks_total_num",
        4611686018427408348,
    ),
    ("tikv_futurepool_pending_tasks", 4611686018427408349),
    (
        "tikv_futurepool_pending_tasks_total_num",
        4611686018427408350,
    ),
    ("tikv_gc_fail_tasks", 4611686018427408351),
    ("tikv_gc_keys", 4611686018427408352),
    ("tikv_gc_keys_total_num", 4611686018427408353),
    ("tikv_gc_skipped_tasks", 4611686018427408354),
    ("tikv_gc_speed", 4611686018427408355),
    ("tikv_gc_tasks_avg_duration", 4611686018427408356),
    ("tikv_gc_tasks_duration", 4611686018427408357),
    ("tikv_gc_tasks_ops", 4611686018427408358),
    ("tikv_gc_tasks_total_count", 4611686018427408359),
    ("tikv_gc_tasks_total_time", 4611686018427408360),
    ("tikv_gc_too_busy", 4611686018427408361),
    ("tikv_grpc_avg_req_batch_size", 4611686018427408362),
    ("tikv_grpc_avg_resp_batch_size", 4611686018427408363),
    ("tikv_grpc_error_total_count", 4611686018427408364),
    ("tikv_grpc_errors", 4611686018427408365),
    ("tikv_grpc_message_duration", 4611686018427408366),
    ("tikv_grpc_message_total_count", 4611686018427408367),
    ("tikv_grpc_message_total_time", 4611686018427408368),
    ("tikv_grpc_qps", 4611686018427408369),
    ("tikv_grpc_req_batch_size", 4611686018427408370),
    ("tikv_grpc_req_batch_size_total_count", 4611686018427408371),
    ("tikv_grpc_req_batch_total_size", 4611686018427408372),
    ("tikv_grpc_resp_batch_size", 4611686018427408373),
    ("tikv_grpc_resp_batch_size_total_count", 4611686018427408374),
    ("tikv_grpc_resp_batch_total_size", 4611686018427408375),
    ("tikv_handle_snapshot_duration", 4611686018427408376),
    ("tikv_handle_snapshot_total_count", 4611686018427408377),
    ("tikv_handle_snapshot_total_time", 4611686018427408378),
    ("tikv_ingest_sst_avg_duration", 4611686018427408379),
    ("tikv_ingest_sst_duration", 4611686018427408380),
    ("tikv_ingest_sst_total_count", 4611686018427408381),
    ("tikv_ingest_sst_total_time", 4611686018427408382),
    ("tikv_io_utilization", 4611686018427408383),
    ("tikv_leader_missing", 4611686018427408384),
    ("tikv_local_reader_execute_requests", 4611686018427408385),
    ("tikv_local_reader_reject_requests", 4611686018427408386),
    (
        "tikv_lock_manager_deadlock_detect_avg_duration",
        4611686018427408387,
    ),
    (
        "tikv_lock_manager_deadlock_detect_duration",
        4611686018427408388,
    ),
    (
        "tikv_lock_manager_deadlock_detect_total_count",
        4611686018427408389,
    ),
    (
        "tikv_lock_manager_deadlock_detect_total_time",
        4611686018427408390,
    ),
    (
        "tikv_lock_manager_deadlock_detector_leader",
        4611686018427408391,
    ),
    ("tikv_lock_manager_detect_error", 4611686018427408392),
    (
        "tikv_lock_manager_detect_error_total_count",
        4611686018427408393,
    ),
    ("tikv_lock_manager_handled_tasks", 4611686018427408394),
    ("tikv_lock_manager_wait_table", 4611686018427408395),
    (
        "tikv_lock_manager_waiter_lifetime_avg_duration",
        4611686018427408396,
    ),
    (
        "tikv_lock_manager_waiter_lifetime_duration",
        4611686018427408397,
    ),
    (
        "tikv_lock_manager_waiter_lifetime_total_count",
        4611686018427408398,
    ),
    (
        "tikv_lock_manager_waiter_lifetime_total_time",
        4611686018427408399,
    ),
    ("tikv_memory", 4611686018427408400),
    ("tikv_memtable_hit", 4611686018427408401),
    ("tikv_memtable_size", 4611686018427408402),
    ("tikv_mvcc_delete_versions", 4611686018427408403),
    ("tikv_mvcc_versions", 4611686018427408404),
    ("tikv_number_files_at_each_level", 4611686018427408405),
    ("tikv_number_of_snapshots", 4611686018427408406),
    ("tikv_oldest_snapshots_duration", 4611686018427408407),
    ("tikv_pd_heartbeat", 4611686018427408408),
    ("tikv_pd_heartbeats", 4611686018427408409),
    ("tikv_pd_request_avg_duration", 4611686018427408410),
    ("tikv_pd_request_duration", 4611686018427408411),
    ("tikv_pd_request_ops", 4611686018427408412),
    ("tikv_pd_request_total_count", 4611686018427408413),
    ("tikv_pd_request_total_time", 4611686018427408414),
    ("tikv_pd_validate_peers", 4611686018427408415),
    ("tikv_per_read_avg_bytes", 4611686018427408416),
    ("tikv_per_read_max_bytes", 4611686018427408417),
    ("tikv_per_write_avg_bytes", 4611686018427408418),
    ("tikv_per_write_max_bytes", 4611686018427408419),
    ("tikv_propose_avg_wait_duration", 4611686018427408420),
    ("tikv_raft_dropped_messages", 4611686018427408421),
    ("tikv_raft_dropped_messages_total", 4611686018427408422),
    ("tikv_raft_log_speed", 4611686018427408423),
    ("tikv_raft_message_avg_batch_size", 4611686018427408424),
    ("tikv_raft_message_batch_size", 4611686018427408425),
    (
        "tikv_raft_message_batch_size_total_count",
        4611686018427408426,
    ),
    ("tikv_raft_message_batch_total_size", 4611686018427408427),
    ("tikv_raft_proposals", 4611686018427408428),
    ("tikv_raft_proposals_per_ready", 4611686018427408429),
    (
        "tikv_raft_proposals_per_ready_total_count",
        4611686018427408430,
    ),
    ("tikv_raft_proposals_per_total_ready", 4611686018427408431),
    ("tikv_raft_proposals_total_num", 4611686018427408432),
    ("tikv_raft_sent_messages", 4611686018427408433),
    ("tikv_raft_sent_messages_total_num", 4611686018427408434),
    ("tikv_raft_store_events_duration", 4611686018427408435),
    ("tikv_raft_store_events_total_count", 4611686018427408436),
    ("tikv_raft_store_events_total_time", 4611686018427408437),
    (
        "tikv_raftstore_append_log_avg_duration",
        4611686018427408438,
    ),
    ("tikv_raftstore_append_log_duration", 4611686018427408439),
    ("tikv_raftstore_append_log_total_count", 4611686018427408440),
    ("tikv_raftstore_append_log_total_time", 4611686018427408441),
    ("tikv_raftstore_apply_log_avg_duration", 4611686018427408442),
    ("tikv_raftstore_apply_log_duration", 4611686018427408443),
    ("tikv_raftstore_apply_log_total_count", 4611686018427408444),
    ("tikv_raftstore_apply_log_total_time", 4611686018427408445),
    ("tikv_raftstore_apply_wait_duration", 4611686018427408446),
    ("tikv_raftstore_apply_wait_total_count", 4611686018427408447),
    ("tikv_raftstore_apply_wait_total_time", 4611686018427408448),
    (
        "tikv_raftstore_commit_log_avg_duration",
        4611686018427408449,
    ),
    ("tikv_raftstore_commit_log_duration", 4611686018427408450),
    ("tikv_raftstore_commit_log_total_count", 4611686018427408451),
    ("tikv_raftstore_commit_log_total_time", 4611686018427408452),
    ("tikv_raftstore_process_duration", 4611686018427408453),
    ("tikv_raftstore_process_handled", 4611686018427408454),
    ("tikv_raftstore_process_total_count", 4611686018427408455),
    ("tikv_raftstore_process_total_time", 4611686018427408456),
    ("tikv_raftstore_propose_wait_duration", 4611686018427408457),
    (
        "tikv_raftstore_propose_wait_total_count",
        4611686018427408458,
    ),
    (
        "tikv_raftstore_propose_wait_total_time",
        4611686018427408459,
    ),
    ("tikv_read_amplication", 4611686018427408460),
    ("tikv_ready_handled", 4611686018427408461),
    ("tikv_receive_messages", 4611686018427408462),
    ("tikv_receive_messages_total_num", 4611686018427408463),
    ("tikv_region_average_written_bytes", 4611686018427408464),
    ("tikv_region_average_written_keys", 4611686018427408465),
    ("tikv_region_change", 4611686018427408466),
    ("tikv_region_count", 4611686018427408467),
    ("tikv_region_written_bytes", 4611686018427408468),
    ("tikv_region_written_keys", 4611686018427408469),
    ("tikv_request_batch_avg", 4611686018427408470),
    ("tikv_request_batch_ratio", 4611686018427408471),
    ("tikv_request_batch_ratio_total_count", 4611686018427408472),
    ("tikv_request_batch_size", 4611686018427408473),
    ("tikv_request_batch_size_avg", 4611686018427408474),
    ("tikv_request_batch_size_total_count", 4611686018427408475),
    ("tikv_request_batch_total_ratio", 4611686018427408476),
    ("tikv_request_batch_total_size", 4611686018427408477),
    ("tikv_scheduler_command_avg_duration", 4611686018427408478),
    ("tikv_scheduler_command_duration", 4611686018427408479),
    ("tikv_scheduler_command_total_count", 4611686018427408480),
    ("tikv_scheduler_command_total_time", 4611686018427408481),
    ("tikv_scheduler_is_busy", 4611686018427408482),
    ("tikv_scheduler_is_busy_total_count", 4611686018427408483),
    ("tikv_scheduler_keys_read", 4611686018427408484),
    ("tikv_scheduler_keys_read_avg", 4611686018427408485),
    ("tikv_scheduler_keys_read_total_count", 4611686018427408486),
    ("tikv_scheduler_keys_total_read", 4611686018427408487),
    ("tikv_scheduler_keys_total_written", 4611686018427408488),
    ("tikv_scheduler_keys_written", 4611686018427408489),
    ("tikv_scheduler_keys_written_avg", 4611686018427408490),
    (
        "tikv_scheduler_keys_written_total_count",
        4611686018427408491,
    ),
    (
        "tikv_scheduler_latch_wait_avg_duration",
        4611686018427408492,
    ),
    ("tikv_scheduler_latch_wait_duration", 4611686018427408493),
    ("tikv_scheduler_latch_wait_total_count", 4611686018427408494),
    ("tikv_scheduler_latch_wait_total_time", 4611686018427408495),
    ("tikv_scheduler_pending_commands", 4611686018427408496),
    ("tikv_scheduler_priority_commands", 4611686018427408497),
    (
        "tikv_scheduler_processing_read_duration",
        4611686018427408498,
    ),
    (
        "tikv_scheduler_processing_read_total_count",
        4611686018427408499,
    ),
    (
        "tikv_scheduler_processing_read_total_time",
        4611686018427408500,
    ),
    ("tikv_scheduler_scan_details", 4611686018427408501),
    ("tikv_scheduler_scan_details_total_num", 4611686018427408502),
    ("tikv_scheduler_stage", 4611686018427408503),
    ("tikv_scheduler_stage_total_num", 4611686018427408504),
    ("tikv_scheduler_writing_bytes", 4611686018427408505),
    ("tikv_send_snapshot_duration", 4611686018427408506),
    ("tikv_send_snapshot_total_count", 4611686018427408507),
    ("tikv_send_snapshot_total_time", 4611686018427408508),
    ("tikv_server_report_failures", 4611686018427408509),
    (
        "tikv_server_report_failures_total_count",
        4611686018427408510,
    ),
    ("tikv_snapshot_kv_count", 4611686018427408511),
    ("tikv_snapshot_kv_count_total_count", 4611686018427408512),
    ("tikv_snapshot_kv_total_count", 4611686018427408513),
    ("tikv_snapshot_size", 4611686018427408514),
    ("tikv_snapshot_size_total_count", 4611686018427408515),
    ("tikv_snapshot_state_count", 4611686018427408516),
    ("tikv_snapshot_state_total_count", 4611686018427408517),
    ("tikv_snapshot_total_size", 4611686018427408518),
    ("tikv_sst_read_duration", 4611686018427408519),
    ("tikv_sst_read_max_duration", 4611686018427408520),
    (
        "tikv_stall_conditions_changed_of_each_cf",
        4611686018427408521,
    ),
    (
        "tikv_storage_async_request_avg_duration",
        4611686018427408522,
    ),
    ("tikv_storage_async_request_duration", 4611686018427408523),
    (
        "tikv_storage_async_request_total_count",
        4611686018427408524,
    ),
    ("tikv_storage_async_request_total_time", 4611686018427408525),
    ("tikv_storage_async_requests", 4611686018427408526),
    (
        "tikv_storage_async_requests_total_count",
        4611686018427408527,
    ),
    ("tikv_storage_command_ops", 4611686018427408528),
    ("tikv_store_size", 4611686018427408529),
    ("tikv_thread_cpu", 4611686018427408530),
    (
        "tikv_thread_nonvoluntary_context_switches",
        4611686018427408531,
    ),
    (
        "tikv_thread_voluntary_context_switches",
        4611686018427408532,
    ),
    ("tikv_threads_io", 4611686018427408533),
    ("tikv_threads_state", 4611686018427408534),
    ("tikv_total_keys", 4611686018427408535),
    ("tikv_wal_sync_duration", 4611686018427408536),
    ("tikv_wal_sync_max_duration", 4611686018427408537),
    ("tikv_worker_handled_tasks", 4611686018427408538),
    ("tikv_worker_handled_tasks_total_num", 4611686018427408539),
    ("tikv_worker_pending_tasks", 4611686018427408540),
    ("tikv_worker_pending_tasks_total_num", 4611686018427408541),
    ("tikv_write_stall_avg_duration", 4611686018427408542),
    ("tikv_write_stall_max_duration", 4611686018427408543),
    ("tikv_write_stall_reason", 4611686018427408544),
    ("up", 4611686018427408545),
    ("uptime", 4611686018427408546),
];

pub const PERFORMANCE_SCHEMA_TABLES: &[(&str, i64)] = &[
    ("events_stages_current", 4611686018427397904),
    ("events_stages_history", 4611686018427397905),
    ("events_stages_history_long", 4611686018427397906),
    ("events_statements_current", 4611686018427397907),
    ("events_statements_history", 4611686018427397908),
    ("events_statements_history_long", 4611686018427397909),
    ("events_statements_summary_by_digest", 4611686018427397910),
    ("events_transactions_current", 4611686018427397911),
    ("events_transactions_history", 4611686018427397912),
    ("events_transactions_history_long", 4611686018427397913),
    ("global_status", 4611686018427397914),
    ("global_variables", 4611686018427397915),
    ("pd_profile_allocs", 4611686018427397916),
    ("pd_profile_block", 4611686018427397917),
    ("pd_profile_cpu", 4611686018427397918),
    ("pd_profile_goroutines", 4611686018427397919),
    ("pd_profile_memory", 4611686018427397920),
    ("pd_profile_mutex", 4611686018427397921),
    ("prepared_statements_instances", 4611686018427397922),
    ("session_account_connect_attrs", 4611686018427397923),
    ("session_connect_attrs", 4611686018427397924),
    ("session_status", 4611686018427397925),
    ("session_variables", 4611686018427397926),
    ("setup_actors", 4611686018427397927),
    ("setup_consumers", 4611686018427397928),
    ("setup_instruments", 4611686018427397929),
    ("setup_objects", 4611686018427397930),
    ("status_by_connection", 4611686018427397931),
    ("tidb_profile_allocs", 4611686018427397932),
    ("tidb_profile_block", 4611686018427397933),
    ("tidb_profile_cpu", 4611686018427397934),
    ("tidb_profile_goroutines", 4611686018427397935),
    ("tidb_profile_memory", 4611686018427397936),
    ("tidb_profile_mutex", 4611686018427397937),
    ("tikv_profile_cpu", 4611686018427397938),
];

pub const SYS_TABLES: &[(&str, i64)] = &[("schema_unused_indexes", 122)];

include!("metrics_tables_rows.rs");
include!("cluster_config_rows.rs");

fn tables_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    // go's TABLES output lists the USER schemas first (the ci-alphabetical
    // order), then information_schema's own tables, then the
    // metrics/performance/sys views -- the oracle's first row is `d10.e1`,
    // not an information_schema row.
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        // go seeds metrics_schema/performance_schema/sys with SYSTEM VIEW
        // rows (InnoDB/Compact, every storage cell zero, ids from its
        // bootstrap block) -- the seeded placeholder tables render the same
        // shape instead of BASE TABLE rows.
        if matches!(
            schema.to_ascii_lowercase().as_str(),
            "metrics_schema" | "performance_schema" | "sys"
        ) {
            let table = match catalog.table_in(&schema, &table_name) {
                Some(TableEntry::Kv(table)) => table,
                _ => continue,
            };
            rows.push(vec![
                text(CATALOG),
                text(&schema),
                text(&table_name),
                text("SYSTEM VIEW"),
                text("InnoDB"),
                Datum::Int(10),
                text("Compact"),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Int(0),
                Datum::Null,
                crate::datetime_datum(0),
                Datum::Null,
                Datum::Null,
                text("utf8mb4_bin"),
                Datum::Null,
                text(""),
                text(""),
                Datum::Int(table.table_id),
                Datum::Null,
                text("NONCLUSTERED"),
                Datum::Null,
                text("Normal"),
                Datum::Null,
                text(""),
            ]);
            continue;
        }
        let table = match catalog.table_in(&schema, &table_name) {
            Some(TableEntry::Kv(table)) => table,
            Some(TableEntry::View(_)) => {
                rows.push(view_tables_row(&schema, &table_name));
                continue;
            }
            _ => continue,
        };
        let (row_count, average_row_length, data_length, index_length) = table.storage_statistics();
        rows.push(vec![
            text(CATALOG),
            text(&schema),
            text(&table_name),
            text("BASE TABLE"),
            text("InnoDB"),
            Datum::Int(10),
            text("Compact"),
            Datum::UInt(row_count),
            Datum::UInt(average_row_length),
            Datum::UInt(data_length),
            Datum::Int(0),
            Datum::UInt(index_length),
            Datum::Int(0),
            // go's AUTO_INCREMENT cell reads the STORED `AutoIncID` (the
            // allocator high-water), not the id the next insert would take:
            // a freshly created table reads 0, and a table without an
            // auto-increment column reads NULL.
            match table.next_auto_increment() {
                Some(next) => Datum::Int(next.saturating_sub(1)),
                None => Datum::Null,
            },
            // CREATE_TIME is NULL rather than a fabricated timestamp.
            Datum::Null,
            Datum::Null,
            Datum::Null,
            // Go reads `TableInfo.Collate`, which the table was created with;
            // the server default here made a latin1 table report utf8mb4_bin.
            text(table.charset().collation.name()),
            Datum::Null,
            text(table_create_options(table)),
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
    // go lists information_schema's own tables after the user schemas.
    rows.extend(information_schema_tables_rows());
    // go seeds metrics_schema/performance_schema/sys with their own SYSTEM
    // VIEW / VIEW rows (the oracle cells: the epoch CREATE_TIME for the
    // metrics/performance machinery; the view's own creation time for sys;
    // the TIDB_TABLE_ID blocks the bootstrap assigned per schema).
    for (schema, schema_upper, tables, created, is_view) in [
        (
            "metrics_schema",
            "METRICS_SCHEMA",
            METRICS_SCHEMA_TABLES,
            "1970-01-01 08:00:00",
            false,
        ),
        (
            "performance_schema",
            "PERFORMANCE_SCHEMA",
            PERFORMANCE_SCHEMA_TABLES,
            "1970-01-01 08:00:00",
            false,
        ),
        ("sys", "sys", SYS_TABLES, "2026-09-24 18:05:56", true),
    ] {
        let created = tidb_datatype::parse_time(
            created,
            tidb_datatype::TimeType::DateTime,
            0,
            false,
            true,
            false,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .map(|parsed| tidb_datatype::Datum::Time(parsed.time))
        .unwrap_or(tidb_datatype::Datum::Null);
        for (table_name, table_id) in tables {
            let mut row = vec![
                text(CATALOG),
                text(schema_upper),
                text(table_name),
                if is_view {
                    text("VIEW")
                } else {
                    text("SYSTEM VIEW")
                },
                if is_view { Datum::Null } else { text("InnoDB") },
                if is_view { Datum::Null } else { Datum::Int(10) },
                if is_view {
                    Datum::Null
                } else {
                    text("Compact")
                },
            ];
            if is_view {
                row.extend(std::iter::repeat_n(Datum::Null, 13));
                row.push(created.clone());
                row.extend(std::iter::repeat_n(Datum::Null, 7));
                row.push(text("VIEW"));
            } else {
                row.extend(std::iter::repeat_n(Datum::Int(0), 6));
                row.push(Datum::Null);
                row.push(created.clone());
                row.extend(std::iter::repeat_n(Datum::Null, 2));
                row.push(text("utf8mb4_bin"));
                row.push(Datum::Null);
                row.push(text(""));
                row.push(text(""));
            }
            row.push(Datum::Int(*table_id));
            row.push(Datum::Null);
            row.push(text("NONCLUSTERED"));
            row.push(Datum::Null);
            if is_view {
                row.push(Datum::Null);
                row.push(Datum::Null);
                row.push(Datum::Null);
            } else {
                row.push(text("Normal"));
                row.push(Datum::Null);
                row.push(text(""));
            }
            rows.push(row);
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

/// Go `GetShardingInfo` (`pkg/infoschema/tables.go`): the AUTO_RANDOM branch
/// prints the shard bit count (with a RANGE BITS suffix only when the range
/// bits are non-default), then SHARD_BITS, then the two NOT_SHARDED forms.
fn sharding_info(table: &KvTable) -> String {
    if let Some(spec) = table.auto_random() {
        let mut info = format!("PK_AUTO_RANDOM_BITS={}", spec.shard_bits);
        if spec.range_bits != 0 && spec.range_bits != 64 {
            info = format!("{info}, RANGE BITS={}", spec.range_bits);
        }
        info
    } else if table.shard_row_id_bits() > 0 {
        format!("SHARD_BITS={}", table.shard_row_id_bits())
    } else if table.pk_handle_offset().is_some() {
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

/// Go `information_schema.tables.CREATE_OPTIONS`: partitioning takes
/// precedence, otherwise an enabled table cache is reported as `cached=on`.
fn table_create_options(table: &KvTable) -> &'static str {
    if table.partition().is_some() {
        "partitioned"
    } else if table.is_cached() {
        "cached=on"
    } else {
        ""
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
        text(&crate::show::column_extra(
            field_type,
            table.auto_increment_offset() == Some(offset),
            column.generated.as_ref().map(|generated| generated.stored),
            column
                .default_value
                .as_ref()
                .is_some_and(tidb_executor::column_default::ColumnDefault::is_default_generated),
        )),
        text(PRIVILEGES),
        // Go `COLUMN_COMMENT`, from `ColumnInfo.Comment`.
        text(&column.comment),
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

/// One row of Go's collation registry (`parser/charset/charset.go`'s
/// `collations` table), restricted to the names `collate.newCollatorMap`
/// registers minus the hidden `utf8mb4_zh_pinyin_tidb_as_cs` — exactly Go's
/// `GetSupportedCollations()` under new collation, already name-sorted.
struct SupportedCollation {
    name: &'static str,
    charset: &'static str,
    id: i64,
    sortlen: i64,
    pad_space: bool,
}

impl SupportedCollation {
    /// Whether this collation is its charset's default — DERIVED from the
    /// datatype registry, which applies Go's `collate.switchDefaultCollation`
    /// at startup (under new collation, `gbk`/`gb18030` default to their
    /// `_chinese_ci` collations, NOT the literal `_bin` ones in
    /// `CharacterSetInfos`). A literal here was a THIRD spelling of that
    /// switch, and it was wrong the first time it was read — probe 23
    /// caught `SHOW CHARACTER SET` and this memtable disagreeing.
    fn is_default(&self) -> bool {
        tidb_datatype::Collation::from_name(self.name)
            .is_some_and(|collation| collation.charset().default_collation() == collation)
    }
}

const SUPPORTED_COLLATIONS: &[SupportedCollation] = &[
    SupportedCollation {
        name: "ascii_bin",
        charset: "ascii",
        id: 65,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "binary",
        charset: "binary",
        id: 63,
        sortlen: 1,
        pad_space: false,
    },
    SupportedCollation {
        name: "gb18030_bin",
        charset: "gb18030",
        id: 249,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "gb18030_chinese_ci",
        charset: "gb18030",
        id: 248,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "gbk_bin",
        charset: "gbk",
        id: 87,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "gbk_chinese_ci",
        charset: "gbk",
        id: 28,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "latin1_bin",
        charset: "latin1",
        id: 47,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "utf8_bin",
        charset: "utf8",
        id: 83,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "utf8_general_ci",
        charset: "utf8",
        id: 33,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "utf8_unicode_ci",
        charset: "utf8",
        id: 192,
        sortlen: 8,
        pad_space: true,
    },
    SupportedCollation {
        name: "utf8mb4_0900_ai_ci",
        charset: "utf8mb4",
        id: 255,
        sortlen: 0,
        pad_space: false,
    },
    SupportedCollation {
        name: "utf8mb4_0900_bin",
        charset: "utf8mb4",
        id: 309,
        sortlen: 1,
        pad_space: false,
    },
    SupportedCollation {
        name: "utf8mb4_bin",
        charset: "utf8mb4",
        id: 46,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "utf8mb4_general_ci",
        charset: "utf8mb4",
        id: 45,
        sortlen: 1,
        pad_space: true,
    },
    SupportedCollation {
        name: "utf8mb4_unicode_ci",
        charset: "utf8mb4",
        id: 224,
        sortlen: 8,
        pad_space: true,
    },
];

/// Go `setDataFromCharacterSets` (`infoschema_reader.go:1804`) over
/// `charset.CharacterSetInfos`, name-sorted as `GetSupportedCharsets` sorts.
fn character_sets_rows() -> Vec<Vec<Datum>> {
    const CHARSETS: &[(&str, &str, i64)] = &[
        ("ascii", "US ASCII", 1),
        ("binary", "binary", 1),
        ("gb18030", "China National Standard GB18030", 4),
        ("gbk", "Chinese Internal Code Specification", 2),
        ("latin1", "Latin1", 1),
        ("utf8", "UTF-8 Unicode", 3),
        ("utf8mb4", "UTF-8 Unicode", 4),
    ];
    CHARSETS
        .iter()
        .map(|&(name, desc, maxlen)| {
            // The default collation comes from the registry, which carries
            // Go's `switchDefaultCollation` state — the same source `SHOW
            // CHARACTER SET` reads, so the two can never disagree again.
            let collation = tidb_datatype::Charset::from_name(name)
                .map(|charset| charset.default_collation().name())
                .unwrap_or_default();
            vec![text(name), text(collation), text(desc), Datum::Int(maxlen)]
        })
        .collect()
}

fn engines_rows() -> Vec<Vec<Datum>> {
    // go `setDataForEngines`: the single InnoDB engine row this tier serves.
    vec![vec![
        text("InnoDB"),
        text("DEFAULT"),
        text("Supports transactions, row-level locking, and foreign keys"),
        text("YES"),
        text("YES"),
        text("YES"),
    ]]
}

/// No triggers exist in this tier: the table serves its schema with zero
/// rows, exactly as go's empty `infoschema.readerBuilder` result does.
fn triggers_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

/// No stored routines exist in this tier.
fn routines_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

/// No events exist in this tier.
fn events_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn tidb_indexes_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    // go `setDataFromIndexes`: `ListSchemasAndTables` walks schemas in
    // ci-alphabetical order (`ListSchemas` sorts `AllSchemaNames`), and the
    // tables of each schema in the meta KV's KEY BYTE ORDER —
    // `meta.ListTables` scans the raw `t<id>` rows, so `t1063` sorts before
    // `t428` and id length matters, not magnitude. Each visible table then
    // contributes one row per key part, with go's own column spellings (the
    // expression parts carry COLUMN_NAME 'NULL' beside the expression text;
    // the clustered table's primary reads CLUSTERED 'YES').
    let mut entries: Vec<(String, String, Vec<Datum>)> = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        let meta_key = format!("t{}", table.table_id);
        if let Some(offset) = table.pk_handle_offset() {
            entries.push((
                schema.to_lowercase(),
                meta_key.clone(),
                vec![
                    text(&schema),
                    text(&table_name),
                    Datum::Int(0),
                    text("PRIMARY"),
                    Datum::Int(1),
                    text(&table.columns[offset].name),
                    Datum::Null,
                    text(""),
                    Datum::Null,
                    Datum::Int(0),
                    text("YES"),
                    text("YES"),
                    Datum::Int(0),
                    Datum::Null,
                ],
            ));
        }
        for index in table.indexes() {
            for (position, offset) in index.column_offsets.iter().enumerate() {
                let column = &table.columns[*offset];
                let prefix = index.prefix_length(position);
                // go renders an expression index part with COLUMN_NAME 'NULL'
                // beside the expression text (`builtinRegexpSig`-shaped
                // `Null` for SUB_PART); the plain columns keep their names.
                let generated_expr = column
                    .generated
                    .as_ref()
                    .map(|generated| generated.expr_text.clone());
                entries.push((
                    schema.to_lowercase(),
                    meta_key.clone(),
                    vec![
                        text(&schema),
                        text(&table_name),
                        Datum::Int(i64::from(!index.unique)),
                        text(&index.name),
                        Datum::Int(position as i64 + 1),
                        match &generated_expr {
                            Some(_) => text("NULL"),
                            None => text(&column.name),
                        },
                        if prefix > 0 {
                            Datum::Int(prefix)
                        } else {
                            Datum::Null
                        },
                        text(&index.comment),
                        match &generated_expr {
                            Some(expr) => text(expr),
                            None => Datum::Null,
                        },
                        Datum::Int(index.id),
                        text(if index.visible { "YES" } else { "NO" }),
                        text("NO"),
                        Datum::Int(i64::from(index.global)),
                        Datum::Null,
                    ],
                ));
            }
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    entries.into_iter().map(|(_, _, row)| row).collect()
}

fn cluster_config_rows() -> Vec<Vec<Datum>> {
    CLUSTER_CONFIG_ROWS
        .iter()
        .map(|cells| {
            vec![
                tidb_datatype::Datum::Bytes(cells[0].as_bytes().to_vec()),
                tidb_datatype::Datum::Bytes(cells[1].as_bytes().to_vec()),
                tidb_datatype::Datum::Bytes(cells[2].as_bytes().to_vec()),
                tidb_datatype::Datum::Bytes(cells[3].as_bytes().to_vec()),
            ]
        })
        .collect()
}

fn metrics_tables_rows() -> Vec<Vec<Datum>> {
    METRICS_TABLES_ROWS
        .iter()
        .map(|cells| {
            let quantile = cells[3].parse::<f64>().ok();
            vec![
                tidb_datatype::Datum::Bytes(cells[0].as_bytes().to_vec()),
                tidb_datatype::Datum::Bytes(cells[1].as_bytes().to_vec()),
                tidb_datatype::Datum::Bytes(cells[2].as_bytes().to_vec()),
                quantile.map_or(tidb_datatype::Datum::Null, |value| {
                    tidb_datatype::Datum::Real(value)
                }),
                tidb_datatype::Datum::Bytes(cells[4].as_bytes().to_vec()),
            ]
        })
        .collect()
}

fn inspection_result_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn parameters_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn plugins_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn sequences_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn runaway_watches_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn tiflash_tables_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

fn tiflash_segments_rows() -> Vec<Vec<Datum>> {
    Vec::new()
}

/// go `SetResourceGroups`: the single `default` resource group row this tier
/// serves (RU_PER_SEC/QUERY_LIMIT render as the UNLIMITED spellings).
fn resource_groups_rows() -> Vec<Vec<Datum>> {
    vec![vec![
        text("default"),
        text("UNLIMITED"),
        text("MEDIUM"),
        text("UNLIMITED"),
        Datum::Null,
        Datum::Null,
    ]]
}

/// Go `setDataFromCollations` (`infoschema_reader.go:1815`): IS_COMPILED is
/// the fixed "Yes"; IS_DEFAULT is empty rather than "No" for a non-default
/// collation — Go's own spelling.
fn collations_rows() -> Vec<Vec<Datum>> {
    SUPPORTED_COLLATIONS
        .iter()
        .map(|c| {
            vec![
                text(c.name),
                text(c.charset),
                Datum::Int(c.id),
                text(if c.is_default() { "Yes" } else { "" }),
                text("Yes"),
                Datum::Int(c.sortlen),
                text(if c.pad_space { "PAD SPACE" } else { "NO PAD" }),
            ]
        })
        .collect()
}
