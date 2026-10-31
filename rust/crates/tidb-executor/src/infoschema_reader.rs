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
//! Column definitions live in `tidb_infoschema::infoschema_meta`. Catalog-backed
//! readers below share the statement's schema and privilege snapshot. Session
//! dispatch composes process-owned providers for runtime tables. A declared
//! virtual table does not imply that its live provider is implemented; remaining
//! provider gaps are tracked in the structural parity register.

use tidb_datatype::{
    Datum, FieldType, FieldTypeCode, STRICT_INTEGER_DISPLAY_WIDTH, UNSPECIFIED_LENGTH,
};
use tidb_privilege::PrivilegeTypeExt;
use crate::{Catalog, KvTable, TableEntry};

use tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA;

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
    registry: tidb_privilege::PrivilegeRegistry,
    user: String,
    host: String,
    active_roles: Vec<tidb_privilege::Account>,
}

/// Which of Go's two masks a retriever filters with.
#[derive(Clone, Copy)]
pub enum PrivMask {
    /// `mysql.AllPrivMask`.
    Any,
    /// `mysql.AllColumnPrivs`, which only `COLUMNS` uses.
    Column,
    /// `mysql.SelectPriv`, which the check-constraint tables ask for.
    Select,
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
        registry: tidb_privilege::PrivilegeRegistry,
        user: &str,
        host: &str,
        active_roles: &[tidb_privilege::Account],
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
    pub fn allows(&self, database: &str, table: &str, mask: PrivMask) -> bool {
        self.allows_bits(
            database,
            table,
            match mask {
                PrivMask::Any => tidb_privilege::any_priv_mask(),
                PrivMask::Column => tidb_privilege::all_column_privs_mask(),
                PrivMask::Select => tidb_mysql::SelectPriv.bit(),
            },
        )
    }

    /// Go `dataForColumnsInTable`'s `priv`: each of `mysql.AllColumnPrivs`
    /// the session holds on the table, joined lower-case as `PrivToString`
    /// prints them. A session with no privilege checker sees all four.
    fn column_privileges(&self, database: &str, table: &str) -> String {
        if self.context.is_none() {
            return PRIVILEGES.to_owned();
        }
        [
            (tidb_mysql::SelectPriv, "select"),
            (tidb_mysql::InsertPriv, "insert"),
            (tidb_mysql::UpdatePriv, "update"),
            (tidb_mysql::ReferencesPriv, "references"),
        ]
        .iter()
        .filter(|(privilege, _)| self.allows_bits(database, table, privilege.bit()))
        .map(|(_, name)| *name)
        .collect::<Vec<_>>()
        .join(",")
    }

    fn allows_bits(&self, database: &str, table: &str, mask: u64) -> bool {
        let Some(context) = &self.context else {
            return true;
        };
        let has_restricted_tables_admin = context.registry.has_dynamic_priv_with_roles(
            &context.user,
            &context.host,
            &context.active_roles,
            "RESTRICTED_TABLES_ADMIN",
            false,
        );
        if let Some(verdict) = tidb_privilege::verification::sem_verdict_mask(
            database,
            table,
            mask,
            has_restricted_tables_admin,
        ) {
            return verdict;
        }
        if let Some(verdict) = tidb_privilege::verification::mem_db_verdict_mask(database, mask) {
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
        let schema_order = left_schema.go_to_lower().cmp(&right_schema.go_to_lower());
        schema_order.then_with(|| left_table.go_to_lower().cmp(&right_table.go_to_lower()))
    });
    pairs
}

/// Go `memtableRetriever.setDataFromIndexUsage`: one row for every index of
/// every table visible to the current account, joined with the Domain's
/// node-global usage counters. Integer primary-key handles use synthetic
/// index ID zero because Go does not keep them in `TableInfo.Indices`.
pub fn tidb_index_usage_rows(
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
                &table.name(),
                "primary",
                collector.get_index_usage(table.table_id(), 0),
            ));
        }
        for index in table.indexes() {
            rows.push(usage_row(
                &schema,
                &table.name(),
                &index.name.go_to_lower(),
                collector.get_index_usage(table.table_id(), index.id),
            ));
        }
    }
    rows
}

/// The tables [`table_rows`] answers, which a scan retrieves itself.
pub const SERVED_TABLES: &[&str] = &[
    "SCHEMATA",
    "TABLES",
    "PARTITIONS",
    "VIEWS",
    "COLUMNS",
    "KEY_COLUMN_USAGE",
    "STATISTICS",
    "TABLE_CONSTRAINTS",
    "CHECK_CONSTRAINTS",
    "TIDB_CHECK_CONSTRAINTS",
    "KEYWORDS",
    "TIFLASH_REPLICA",
    "REFERENTIAL_CONSTRAINTS",
    "CHARACTER_SETS",
    "ENGINES",
    "TRIGGERS",
    "ROUTINES",
    "EVENTS",
    "INSPECTION_RESULT",
    "TIDB_INDEXES",
    "METRICS_TABLES",
    "PARAMETERS",
    "PLUGINS",
    "SEQUENCES",
    "RUNAWAY_WATCHES",
    "TIFLASH_TABLES",
    "TIFLASH_SEGMENTS",
    "RESOURCE_GROUPS",
    "COLLATIONS",
    "COLLATION_CHARACTER_SET_APPLICABILITY",
    "SCHEMA_PRIVILEGES",
    "TABLE_PRIVILEGES",
    "COLUMN_PRIVILEGES",
];

/// Whether [`table_rows`] answers `name`.
#[must_use]
pub fn serves_table(name: &str) -> bool {
    SERVED_TABLES.iter().any(|table| name.go_lower_eq(table))
}

/// The rows of one `information_schema` table, computed from `catalog` and
/// filtered by what `visibility` may see.
#[must_use]
pub fn table_rows(
    name: &str,
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    ctx: &crate::StmtContext,
) -> Option<Vec<Vec<Datum>>> {
    if name.go_lower_eq("SCHEMATA") {
        return Some(schemata_rows(catalog, visibility));
    }
    if name.go_lower_eq("TABLES") {
        return Some(tables_rows(catalog, visibility, &ctx.session_zone()));
    }
    if name.go_lower_eq("PARTITIONS") {
        return Some(partitions_rows(catalog, visibility, &ctx.session_zone()));
    }
    if name.go_lower_eq("VIEWS") {
        return Some(views_rows(catalog, visibility));
    }
    if name.go_lower_eq("COLUMNS") {
        return Some(columns_rows(catalog, visibility, ctx));
    }
    if name.go_lower_eq("KEY_COLUMN_USAGE") {
        return Some(key_column_usage_rows(catalog, visibility));
    }
    if name.go_lower_eq("STATISTICS") {
        return Some(statistics_rows(catalog, visibility));
    }
    if name.go_lower_eq("TABLE_CONSTRAINTS") {
        return Some(table_constraints_rows(catalog, visibility));
    }
    if name.go_lower_eq("CHECK_CONSTRAINTS") {
        return Some(check_constraints_rows(catalog, visibility, false));
    }
    if name.go_lower_eq("TIDB_CHECK_CONSTRAINTS") {
        return Some(check_constraints_rows(catalog, visibility, true));
    }
    if name.go_lower_eq("KEYWORDS") {
        return Some(keywords_rows());
    }
    if name.go_lower_eq("TIFLASH_REPLICA") {
        return Some(tiflash_replica_rows(catalog, visibility));
    }
    if name.go_lower_eq("REFERENTIAL_CONSTRAINTS") {
        return Some(referential_constraints_rows(catalog, visibility));
    }
    if name.go_lower_eq("CHARACTER_SETS") {
        return Some(character_sets_rows());
    }
    if name.go_lower_eq("ENGINES") {
        return Some(engines_rows());
    }
    if name.go_lower_eq("TRIGGERS") {
        return Some(triggers_rows());
    }
    if name.go_lower_eq("ROUTINES") {
        return Some(routines_rows());
    }
    if name.go_lower_eq("EVENTS") {
        return Some(events_rows());
    }
    if name.go_lower_eq("INSPECTION_RESULT") {
        return Some(inspection_result_rows());
    }
    if name.go_lower_eq("TIDB_INDEXES") {
        return Some(tidb_indexes_rows(catalog, visibility));
    }
    if name.go_lower_eq("METRICS_TABLES") {
        return Some(metrics_tables_rows());
    }
    if name.go_lower_eq("PARAMETERS") {
        return Some(parameters_rows());
    }
    if name.go_lower_eq("PLUGINS") {
        return Some(plugins_rows());
    }
    if name.go_lower_eq("SEQUENCES") {
        return Some(sequences_rows(catalog, visibility));
    }
    if name.go_lower_eq("RUNAWAY_WATCHES") {
        return Some(runaway_watches_rows());
    }
    if name.go_lower_eq("TIFLASH_TABLES") {
        return Some(tiflash_tables_rows());
    }
    if name.go_lower_eq("TIFLASH_SEGMENTS") {
        return Some(tiflash_segments_rows());
    }
    if name.go_lower_eq("RESOURCE_GROUPS") {
        return Some(resource_groups_rows(catalog));
    }
    if name.go_lower_eq("COLLATIONS") {
        return Some(collations_rows());
    }
    if name.go_lower_eq("COLLATION_CHARACTER_SET_APPLICABILITY") {
        return Some(
            SUPPORTED_COLLATIONS
                .iter()
                .map(|c| vec![text(c.name), text(c.charset)])
                .collect(),
        );
    }
    if name.go_lower_eq("SCHEMA_PRIVILEGES")
        || name.go_lower_eq("TABLE_PRIVILEGES")
        || name.go_lower_eq("COLUMN_PRIVILEGES")
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
        let fallback = f64::from(replica.available);
        let progress = if let Some(partition) = table.partition() {
            let ids: Vec<_> = partition.definitions.iter().map(|p| p.id).collect();
            if ids.is_empty() {
                fallback
            } else {
                ids.iter()
                    .map(|id| {
                        tidb_ddl::tiflash_replica_manager::cached_replica_progress(*id)
                            .unwrap_or(fallback)
                    })
                    .sum::<f64>()
                    / ids.len() as f64
            }
        } else {
            tidb_ddl::tiflash_replica_manager::cached_replica_progress(table.table_id())
                .unwrap_or(fallback)
        };
        let progress = tidb_datatype::truncate(progress, 2);
        rows.push(vec![
            text(&schema),
            text(&table_name),
            Datum::Int(table.table_id()),
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
/// Go `keyColumnUsageInTable`: the integer handle (`POSITION_IN_UNIQUE_
/// CONSTRAINT` 1), then every PRIMARY or UNIQUE index's visible key parts
/// (`NULL` position, the primary named `PRIMARY`), then each foreign key's
/// columns with their referenced column.
fn key_column_usage_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        if let Some(offset) = table.pk_handle_offset() {
            rows.push(key_column_usage_row(
                &schema,
                &table_name,
                "PRIMARY",
                &table.columns()[offset].name,
                1,
                Datum::Int(1),
                None,
            ));
        }
        for index in table.indexes() {
            let constraint_name = if index.primary {
                "PRIMARY"
            } else if index.unique {
                index.name.as_str()
            } else {
                continue;
            };
            for (position, offset) in index.column_offsets.iter().enumerate() {
                if table.is_hidden(*offset) {
                    continue;
                }
                rows.push(key_column_usage_row(
                    &schema,
                    &table_name,
                    constraint_name,
                    &table.columns()[*offset].name,
                    (position + 1) as i64,
                    Datum::Null,
                    None,
                ));
            }
        }
        for foreign_key in table.foreign_keys() {
            for (position, column_name) in foreign_key.cols.iter().enumerate() {
                rows.push(key_column_usage_row(
                    &schema,
                    &table_name,
                    &foreign_key.name,
                    column_name,
                    (position + 1) as i64,
                    Datum::Int(1),
                    Some((
                        foreign_key.ref_schema.as_str(),
                        foreign_key.ref_table.as_str(),
                        foreign_key
                            .ref_cols
                            .get(position)
                            .map_or("", String::as_str),
                    )),
                ));
            }
        }
    }
    rows
}

/// One `KEY_COLUMN_USAGE` row; `reference` is a foreign key's referenced
/// `(schema, table, column)`.
fn key_column_usage_row(
    schema: &str,
    table_name: &str,
    constraint_name: &str,
    column_name: &str,
    ordinal_position: i64,
    position_in_unique: Datum,
    reference: Option<(&str, &str, &str)>,
) -> Vec<Datum> {
    let (referenced_schema, referenced_table, referenced_column) = reference.map_or(
        (Datum::Null, Datum::Null, Datum::Null),
        |(schema, table, column)| (text(schema), text(table), text(column)),
    );
    vec![
        text(CATALOG),
        text(schema),
        text(constraint_name),
        text(CATALOG),
        text(schema),
        text(table_name),
        text(column_name),
        Datum::Int(ordinal_position),
        position_in_unique,
        referenced_schema,
        referenced_table,
        referenced_column,
    ]
}

/// Go `setDataForStatisticsInTable`: the integer handle as `PRIMARY`, then
/// one row per key part of every index. An expression key part reads
/// `COLUMN_NAME` `NULL` beside its expression, and a prefix part reports its
/// length in `SUB_PART`.
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
                StatisticsIndex {
                    name: "PRIMARY",
                    unique: true,
                    comment: "",
                    visible: true,
                },
                StatisticsKeyPart {
                    sequence: 1,
                    column_name: &table.columns()[offset].name,
                    sub_part: Datum::Null,
                    nullable: false,
                    expression: Datum::Null,
                },
            ));
        }
        for index in table.indexes() {
            for (position, offset) in index.column_offsets.iter().enumerate() {
                let column = &table.columns()[*offset];
                let nullable =
                    column.field_type.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL == 0;
                let (column_name, expression) = match &column.generated {
                    Some(generated) if table.is_hidden(*offset) => {
                        ("NULL", text(&generated.expr_text))
                    }
                    _ => (column.name.as_str(), Datum::Null),
                };
                let prefix = index.prefix_length(position);
                rows.push(statistics_row(
                    &schema,
                    &table_name,
                    StatisticsIndex {
                        name: &index.name,
                        unique: index.unique,
                        comment: &index.comment,
                        visible: index.visible,
                    },
                    StatisticsKeyPart {
                        sequence: position + 1,
                        column_name,
                        sub_part: if prefix == UNSPECIFIED_LENGTH {
                            Datum::Null
                        } else {
                            Datum::Int(prefix)
                        },
                        nullable,
                        expression,
                    },
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

struct StatisticsKeyPart<'a> {
    sequence: usize,
    column_name: &'a str,
    sub_part: Datum,
    nullable: bool,
    expression: Datum,
}

/// One `STATISTICS` row.
fn statistics_row(
    schema: &str,
    table_name: &str,
    index: StatisticsIndex<'_>,
    part: StatisticsKeyPart<'_>,
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
        Datum::Int(part.sequence as i64),
        text(part.column_name),
        text("A"),
        // Go reports no cardinality estimate here.
        Datum::Int(0),
        part.sub_part,
        Datum::Null,
        text(if part.nullable { "YES" } else { "" }),
        text("BTREE"),
        text(""),
        text(index.comment),
        text(if index.visible { "YES" } else { "NO" }),
        part.expression,
    ]
}

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
            let constraint_type = if index.primary {
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

fn referential_rule(action: crate::FkAction) -> &'static str {
    match action {
        crate::FkAction::NoOption | crate::FkAction::NoAction => "NO ACTION",
        crate::FkAction::Restrict => "RESTRICT",
        crate::FkAction::Cascade => "CASCADE",
        crate::FkAction::SetNull => "SET NULL",
        crate::FkAction::SetDefault => "SET DEFAULT",
    }
}

/// One `TABLE_CONSTRAINTS` row.
/// Go `setDataFromCheckConstraints` / `setDataFromTiDBCheckConstraints`
/// (`infoschema_reader.go:937`/`:982`): each PUBLIC check constraint of a
/// table the session may SELECT from, its clause parenthesized; the TiDB form
/// adds the table's name and id.
fn check_constraints_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    tidb_form: bool,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, PrivMask::Select) {
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        for constraint in table.check_constraint_infos() {
            if constraint.state != tidb_model::SchemaState::PUBLIC {
                continue;
            }
            let mut row = vec![
                text(CATALOG),
                text(&schema),
                text(constraint.name.original()),
                text(&format!("({})", constraint.expr_string)),
            ];
            if tidb_form {
                row.push(text(&table_name));
                row.push(Datum::Int(table.table_id()));
            }
            rows.push(row);
        }
    }
    rows
}

/// Go `setDataFromKeywords` (`infoschema_reader.go:4055`): `parser.Keywords`
/// in its own order, RESERVED as 1 or 0.
fn keywords_rows() -> Vec<Vec<Datum>> {
    tidb_lexer::KEYWORDS
        .iter()
        .map(|keyword| vec![text(keyword.word), Datum::Int(i64::from(keyword.reserved))])
        .collect()
}

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
    .filter(|(display, _)| visibility.allows(display, "", ANY_PRIV))
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
                name.go_to_lower().as_str(),
                "metrics_schema" | "performance_schema" | "sys"
            )
        })
        .map(|name| {
            // Go reads `DBInfo.Charset`/`Collate`, which `CREATE DATABASE`
            // settles and `ALTER DATABASE ... CHARACTER SET` moves; reporting
            // the server default here made that ALTER invisible.
            let charset = catalog
                .database_definition(&name)
                .map_or_else(crate::TableCharset::default, |(_, charset)| charset);
            vec![
                text(CATALOG),
                text(&name),
                text(charset.charset.name()),
                text(charset.collation.name()),
                // Go reports SQL_PATH as NULL, and the placement policy's
                // name when the schema has one.
                Datum::Null,
                catalog
                    .database_placement_policy(&name)
                    .map_or(Datum::Null, |reference| text(reference.name.original())),
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
fn partitions_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    // One row of an object with no partitions: every partition cell NULL, the
    // storage cells at the counts given, CREATE_TIME the creating TSO.
    let unpartitioned_row = |schema: &str,
                             table_name: &str,
                             stats: (u64, u64, u64, u64),
                             update_ts: u64,
                             affinity: Datum| {
        let (row_count, average_row_length, data_length, index_length) = stats;
        vec![
            Datum::Bytes(b"def".to_vec()),
            Datum::Bytes(schema.as_bytes().to_vec()),
            Datum::Bytes(table_name.as_bytes().to_vec()),
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
            update_time_datum(update_ts, zone),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            affinity,
            Datum::Null,
        ]
    };
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        // Go's partition reader lists views and sequences like any
        // unpartitioned table: every partition cell NULL, zero storage
        // statistics, the creating TSO as CREATE_TIME.
        match catalog.table_in(&schema, &table_name) {
            Some(TableEntry::View(view)) => {
                rows.push(unpartitioned_row(
                    &schema,
                    &table_name,
                    (0, 0, 0, 0),
                    view.update_ts,
                    Datum::Null,
                ));
                continue;
            }
            Some(TableEntry::Sequence(sequence)) => {
                rows.push(unpartitioned_row(
                    &schema,
                    &table_name,
                    (0, 0, 0, 0),
                    sequence.update_ts,
                    Datum::Null,
                ));
                continue;
            }
            _ => {}
        }
        let Some(TableEntry::Kv(table)) = catalog.table_in(&schema, &table_name) else {
            continue;
        };
        let catalog_value = || Datum::Bytes(b"def".to_vec());
        // Go reports the table's AFFINITY level on every row it emits.
        let affinity = || {
            table.affinity().map_or(Datum::Null, |affinity| {
                Datum::Bytes(affinity.level.clone().into_bytes())
            })
        };
        let Some(partition) = table.partition() else {
            rows.push(unpartitioned_row(
                &schema,
                &table_name,
                table.storage_statistics(),
                table.update_ts(),
                affinity(),
            ));
            continue;
        };
        // Go names the COLUMNS forms differently from the expression forms and
        // prints the column list in place of the expression
        // (`infoschema_reader.go`): `RANGE COLUMNS`, `LIST COLUMNS`, `KEY`.
        let (method, expression) = match &partition.kind {
            crate::PartitionKind::RangeColumns { .. } => {
                ("RANGE COLUMNS".to_owned(), partition.expr_text.clone())
            }
            crate::PartitionKind::ListColumns { .. } => {
                ("LIST COLUMNS".to_owned(), partition.expr_text.clone())
            }
            crate::PartitionKind::Key => ("KEY".to_owned(), partition.expr_text.clone()),
            other => (other.sql().to_owned(), partition.expr_text.clone()),
        };
        for (ordinal, definition) in partition.definitions.iter().enumerate() {
            let (row_count, average_row_length, data_length, index_length) =
                table.partition_storage_statistics(definition.id);
            // Go's `PARTITION_DESCRIPTION`: the RANGE bounds joined by commas,
            // or the LIST tuples with multi-column ones parenthesised.
            let description = match &partition.kind {
                crate::PartitionKind::Range { .. }
                | crate::PartitionKind::RangeColumns { .. } => {
                    definition.less_than.join(",")
                }
                crate::PartitionKind::List { .. }
                | crate::PartitionKind::ListColumns { .. } => definition
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
            // Go keeps both as strings, so a HASH/KEY description and an
            // absent comment read back as '' rather than NULL.
            let description = Datum::Bytes(description.into_bytes());
            let policy = definition
                .placement_policy
                .as_ref()
                .map_or(Datum::Null, |reference| {
                    Datum::Bytes(reference.name.original().as_bytes().to_vec())
                });
            let comment = Datum::Bytes(definition.comment.clone().into_bytes());
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
                // Go: `uint64(0)` for MAX_DATA_LENGTH and DATA_FREE of a
                // partition definition (NULL for an unpartitioned table).
                Datum::UInt(0),
                Datum::UInt(index_length),
                Datum::UInt(0),
                update_time_datum(table.update_ts(), zone),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                comment,
                Datum::Null,
                Datum::Null,
                Datum::Int(definition.id),
                policy,
                affinity(),
                Datum::Bytes(definition.storage_class_string().into_bytes()),
            ]);
        }
    }
    rows
}

/// Go `autoid.InformationSchemaDBID`.
const INFORMATION_SCHEMA_DB_ID: i64 = (1 << 62) | 1;

use tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA_TABLE_IDS;
use tidb_hack::{GoCaseCompare, GoToLower};

/// The `information_schema.tables` rows for the schema's OWN tables: Go
/// `infoschema_reader.go`'s `setDataFromOneTable` with `metadef.IsMemDB`
/// true, so `TABLE_TYPE` is `SYSTEM VIEW`, the storage numbers are zero, and
/// `TIDB_TABLE_ID` comes from `tableIDMap`. Emitted for EVERY table Go
/// registers, served here or not -- the id and the name are the table's
/// METADATA, from Go's own source; only QUERYING an unserved one refuses.
fn information_schema_tables_rows(zone: &tidb_datatype::SessionTimeZone) -> Vec<Vec<Datum>> {
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
            // TABLE_ROWS through DATA_FREE: zero, as Go's `EstimateDataLength`
            // answers for a memory table. AUTO_INCREMENT is NULL (no
            // auto-increment column), and CREATE_TIME is `GetUpdateTime()` of
            // a table that never recorded a TSO: the epoch in the session
            // zone. UPDATE_TIME and CHECK_TIME are NULL.
            row.extend(std::iter::repeat_n(Datum::Int(0), 6));
            row.push(Datum::Null);
            row.push(update_time_datum(0, zone));
            row.extend(std::iter::repeat_n(Datum::Null, 2));
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

/// Go `memtableRetriever.setDataForAttributes`: each label rule naming a
/// table or partition that still exists, and whose table the session may
/// SELECT from. A malformed rule is skipped (Go logs it).
pub fn attributes_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
) -> Result<Vec<Vec<Datum>>, crate::DriverError> {
    use tidb_ddl_label as ddl_label;
    // Go `infosync.GetAllLabelRules`: the node's label-rule manager.
    let rules = tidb_domain_infosync::label_manager::label_rule_manager()
        .get_all_label_rules(&ddl_label::CodecV1)
        .map_err(|error| {
            crate::DriverError::unsupported(format!("get the label rules failed: {error}"))
        })?;
    let mut rows = Vec::with_capacity(rules.len());
    for rule in rules {
        let Ok((db_name, table_name, partition_name)) = check_label_rule(&rule) else {
            continue;
        };
        let Some(table_id) = decode_table_id_from_rule(&rule) else {
            continue;
        };
        if table_or_partition_not_exist(catalog, &db_name, &table_name, &partition_name, table_id) {
            continue;
        }
        if table_name.is_empty()
            || db_name.is_empty()
            || !visibility.allows(&db_name, &table_name, PrivMask::Select)
        {
            continue;
        }
        let labels = ddl_label::restore_region_labels(&rule.labels.snapshot());
        let ranges = rule
            .data
            .as_ref()
            .map(|data| match &*data.read() {
                serde_json::Value::Array(items) => items
                    .iter()
                    .filter_map(serde_json::Value::as_object)
                    .map(|range| {
                        format!(
                            "[{}, {}]",
                            go_sprint_any(range.get("start_key")),
                            go_sprint_any(range.get("end_key"))
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                _ => String::new(),
            })
            .unwrap_or_default();
        rows.push(vec![
            text(&ddl_label::restore_rule_id(&rule.id)),
            text(&rule.rule_type),
            text(&labels),
            text(&ranges),
        ]);
    }
    Ok(rows)
}

/// Go `DDLJobsReaderExec`: the history newest-first, each job rendered by
/// `appendJobToChunk` and kept when the session holds any privilege on the
/// table it names.
pub fn ddl_jobs_rows(
    jobs: &[tidb_model::Job],
    visibility: &SchemaVisibility,
    catalog: &crate::Catalog,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for job in jobs {
        let job_rows = crate::ddl_jobs::ddl_job_rows(job, false, catalog);
        let names = |cell: &Datum| match cell {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).go_to_lower(),
            _ => String::new(),
        };
        let (schema, table) = (names(&job_rows[0][1]), names(&job_rows[0][2]));
        if !visibility.allows(&schema, &table, PrivMask::Any) {
            continue;
        }
        rows.extend(job_rows);
    }
    rows
}

/// Go's `%s` of a decoded JSON value: a string prints as itself, a missing
/// key as `%!s(<nil>)`.
fn go_sprint_any(value: Option<&serde_json::Value>) -> String {
    match value {
        None | Some(serde_json::Value::Null) => "%!s(<nil>)".to_owned(),
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(other) => format!("%!s({other})"),
    }
}

/// Go `checkRule`: the schema, table and (possibly empty) partition a rule
/// ID names, or why the rule is malformed.
fn check_label_rule(
    rule: &tidb_ddl_label::Rule,
) -> Result<(String, String, String), String> {
    use tidb_ddl_label::{ID_PREFIX, KEYSPACE_PREFIX};
    let parts = rule.id.split('/').collect::<Vec<_>>();
    if parts.len() < 3 {
        return Err(format!("invalid label rule ID: {}", rule.id));
    }
    if rule.rule_type.is_empty() {
        return Err("empty label rule type".to_owned());
    }
    if rule.labels.with_visible(<[_]>::is_empty) {
        return Err("the label rule has no label".to_owned());
    }
    if rule.data.is_none() {
        return Err("the label rule has no data".to_owned());
    }
    let mut id_offset = 0;
    if tidb_config_kerneltype::is_next_gen() && parts[0] == KEYSPACE_PREFIX {
        if parts.len() < 5 {
            return Err(format!("invalid keyspace label rule ID: {}", rule.id));
        }
        id_offset = 2;
    }
    if parts[id_offset] != ID_PREFIX || parts.len() < id_offset + 3 {
        return Err(format!("invalid label rule ID: {}", rule.id));
    }
    Ok((
        parts[id_offset + 1].to_owned(),
        parts[id_offset + 2].to_owned(),
        parts
            .get(id_offset + 3)
            .copied()
            .unwrap_or_default()
            .to_owned(),
    ))
}

/// Go `decodeTableIDFromRule`: the table ID the first key range's start key
/// encodes, or `None` (Go's error) when there is none.
fn decode_table_id_from_rule(rule: &tidb_ddl_label::Rule) -> Option<i64> {
    let data = rule.data.as_ref()?;
    let data = data.read();
    let first = data.as_array()?.first()?.as_object()?;
    let mut key = go_hex_decode_string(&go_sprint_any(first.get("start_key")))?;
    if let Ok((_, decoded)) = tidb_codec::decode_bytes(&key) {
        key = decoded;
    }
    match tidb_tablecodec::table_key::decode_table_id(&key) {
        0 => None,
        table_id => Some(table_id),
    }
}

/// Go `hex.DecodeString`, `None` for its errors (an odd length or a byte
/// that is not a hex digit).
fn go_hex_decode_string(text: &str) -> Option<Vec<u8>> {
    let digits = text.as_bytes();
    if digits.len() % 2 != 0 {
        return None;
    }
    let nibble = |digit: u8| char::from(digit).to_digit(16).map(|value| value as u8);
    digits
        .chunks_exact(2)
        .map(|pair| Some(nibble(pair[0])? << 4 | nibble(pair[1])?))
        .collect()
}

/// Go `tableOrPartitionNotExist`.
fn table_or_partition_not_exist(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    partition_name: &str,
    table_id: i64,
) -> bool {
    if partition_name.is_empty() {
        !matches!(
            catalog.table_in(db_name, table_name),
            Some(TableEntry::Kv(table)) if table.table_id() == table_id
        )
    } else {
        catalog.find_table_by_partition_id(table_id).is_none()
    }
}

/// `sys`' own tables with the id Go's bootstrap draws for them: the view is
/// the 112th id under the classic kernel (53 tables, `mysql.tidb_mdl_view`
/// before it) and the third under the next-gen one (`bootstrapSchemas` draws
/// none, the views are its first two objects). Go's own counters, not a
/// reading of the store: a node over a store the other kernel bootstrapped
/// still says its own kernel's id here.
pub fn sys_tables() -> [(&'static str, i64); 1] {
    let id = if tidb_config_kerneltype::is_next_gen() {
        3
    } else {
        112
    };
    [("schema_unused_indexes", id)]
}

/// A `DATETIME` for a table's `UpdateTS` TSO in the session zone (Go
/// `types.FromGoTime(table.GetUpdateTime().In(loc))`), NULL for a table whose
/// creating tier recorded no TSO.
pub fn update_time_datum(update_ts: u64, zone: &tidb_datatype::SessionTimeZone) -> Datum {
    use chrono::{Datelike, TimeZone, Timelike};
    // Go `TSConvert2Time`: a table that never recorded a TSO (the seeded
    // `metrics_schema` ones) reads as the epoch in the session zone.
    let Some(moment) = zone
        .timestamp_millis_opt((update_ts >> 18) as i64)
        .single()
        .map(|moment| moment.naive_local())
    else {
        return Datum::Null;
    };
    tidb_datatype::Time::from_date_checked(
        moment.year(),
        moment.month() as i32,
        moment.day() as i32,
        moment.hour() as i32,
        moment.minute() as i32,
        moment.second() as i32,
        0,
        tidb_datatype::TimeType::DateTime,
        0,
    )
    .map_or(Datum::Null, Datum::Time)
}

/// Go `fetchShowTableStatus`'s restricted SQL over `information_schema.tables`:
/// `SELECT table_name, engine, version, row_format, table_rows,
/// avg_row_length, data_length, max_data_length, index_length, data_free,
/// auto_increment, create_time, update_time, check_time, table_collation,
/// IFNULL(checksum,''), create_options, table_comment ... WHERE
/// lower(table_schema)=? ORDER BY table_name`, over `catalog`'s TABLES rows.
pub fn show_table_status_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    zone: &tidb_datatype::SessionTimeZone,
    database: &str,
) -> Vec<Vec<Datum>> {
    const TABLE_SCHEMA: usize = 1;
    const TABLE_NAME: usize = 2;
    const CHECKSUM: usize = 18;
    let schema = database.go_to_lower();
    let mut rows = tables_rows(catalog, visibility, zone)
        .into_iter()
        .filter(|row| {
            tidb_datatype::datum_text::sysvar_datum_text(&row[TABLE_SCHEMA]).is_some_and(|name| name.go_to_lower() == schema)
        })
        .map(|row| {
            let mut status = Vec::with_capacity(18);
            status.push(row[TABLE_NAME].clone());
            status.extend(row[4..CHECKSUM].iter().cloned());
            status.push(match &row[CHECKSUM] {
                Datum::Null => Datum::Bytes(Vec::new()),
                checksum => checksum.clone(),
            });
            status.extend(row[CHECKSUM + 1..=CHECKSUM + 2].iter().cloned());
            status
        })
        .collect::<Vec<_>>();
    // `table_name` is utf8mb4_bin: the ORDER BY compares bytes.
    rows.sort_by(|left, right| match (&left[0], &right[0]) {
        (Datum::Bytes(left), Datum::Bytes(right)) => left.cmp(right),
        _ => std::cmp::Ordering::Equal,
    });
    rows
}

fn tables_rows(
    catalog: &Catalog,
    visibility: &SchemaVisibility,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Vec<Datum>> {
    // go's TABLES output lists the USER schemas first (the ci-alphabetical
    // order), then information_schema's own tables, then the
    // metrics/performance/sys views -- the oracle's first row is `d10.e1`,
    // not an information_schema row.
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, ANY_PRIV) {
        // information_schema's own tables are listed once, as the SYSTEM
        // VIEW rows below; their catalog entries are not base tables.
        if schema.go_lower_eq(INFORMATION_SCHEMA) {
            continue;
        }
        // go seeds metrics_schema/performance_schema/sys with SYSTEM VIEW
        // rows (InnoDB/Compact, every storage cell zero, ids from its
        // bootstrap block) -- the seeded placeholder tables render the same
        // shape instead of BASE TABLE rows.
        if matches!(
            schema.go_to_lower().as_str(),
            "metrics_schema" | "performance_schema" | "sys"
        ) {
            // The metric tables are listed once, from the generated block
            // below, in the order the oracle shows them.
            if schema.go_lower_eq("metrics_schema") {
                continue;
            }
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
                datetime_datum(0),
                Datum::Null,
                Datum::Null,
                text("utf8mb4_bin"),
                Datum::Null,
                text(""),
                text(""),
                Datum::Int(table.table_id()),
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
            Some(TableEntry::View(view)) => {
                rows.push(view_tables_row(&schema, &table_name, view, zone));
                continue;
            }
            Some(TableEntry::Sequence(sequence)) => {
                rows.push(sequence_tables_row(&schema, sequence, zone));
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
            // Go `getAutoIncrementID`: the allocator's `Base() + 1`, 0 while
            // it has drawn nothing; NULL without an auto-increment column.
            table.allocated_auto_increment().map_or(Datum::Null, Datum::Int),
            // Go `table.GetUpdateTime().In(loc)`: the creating job's TSO in
            // the session's zone; NULL where the creating tier recorded none.
            update_time_datum(table.update_ts(), zone),
            Datum::Null,
            Datum::Null,
            // Go reads `TableInfo.Collate`, which the table was created with;
            // the server default here made a latin1 table report utf8mb4_bin.
            text(table.charset().collation.name()),
            Datum::Null,
            text(table_create_options(table)),
            text(&table.comment()),
            Datum::Int(table.table_id()),
            text(&sharding_info(table)),
            text(&pk_type(table)),
            table
                .placement_policy()
                .map_or(Datum::Null, |policy| text(policy.name.original())),
            text("Normal"),
            table
                .affinity()
                .map_or(Datum::Null, |affinity| text(&affinity.level)),
            text(&table.storage_class_string()),
        ]);
    }
    // go lists information_schema's own tables after the user schemas.
    rows.extend(information_schema_tables_rows(zone));
    // go seeds metrics_schema and sys with their own SYSTEM VIEW / VIEW rows
    // (the oracle cells: the epoch CREATE_TIME for the metrics tables; the
    // view's own creation time for sys; the TIDB_TABLE_ID blocks the
    // bootstrap assigned per schema). performance_schema's tables are
    // catalog entries and render through the loop above.
    let sys_tables = sys_tables();
    for (schema_upper, tables, created, is_view) in [
        (
            "METRICS_SCHEMA",
            tidb_infoschema::metricsschema::table_ids(),
            "1970-01-01 08:00:00",
            false,
        ),
        ("sys", &sys_tables[..], "2026-09-24 18:05:56", true),
    ] {
        let created = if schema_upper == "METRICS_SCHEMA" {
            // These tables never recorded a TSO: the epoch, in the session
            // zone, exactly as the memory tables above.
            update_time_datum(0, zone)
        } else {
            tidb_datatype::parse_time(
                created,
                tidb_datatype::TimeType::DateTime,
                0,
                false,
                true,
                false,
                &tidb_datatype::SessionTimeZone::utc(),
            )
            .map(|parsed| tidb_datatype::Datum::Time(parsed.time))
            .unwrap_or(tidb_datatype::Datum::Null)
        };
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
                // TABLE_ROWS through AUTO_INCREMENT, then CREATE_TIME, then
                // UPDATE_TIME through CREATE_OPTIONS, then TABLE_COMMENT.
                row.extend(std::iter::repeat_n(Datum::Null, 7));
                row.push(created.clone());
                row.extend(std::iter::repeat_n(Datum::Null, 5));
                row.push(text("VIEW"));
            } else {
                row.extend(std::iter::repeat_n(Datum::Int(0), 6));
                row.push(Datum::Null);
                row.push(created.clone());
                row.extend(std::iter::repeat_n(Datum::Null, 2));
                row.push(text("utf8mb4_bin"));
                row.push(Datum::Null);
                row.push(text(""));
                // A metric table's comment is its definition's (`tableInfo.Comment`).
                let comment = tidb_infoschema::metric_table_def::get_metric_table_def(table_name)
                    .filter(|_| schema_upper == "METRICS_SCHEMA")
                    .map_or("", |def| def.comment);
                row.push(text(comment));
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
fn view_tables_row(
    schema: &str,
    table_name: &str,
    view: &crate::ViewDef,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Datum> {
    let mut row = vec![text(CATALOG), text(schema), text(table_name), text("VIEW")];
    // ENGINE through CREATE_OPTIONS: sixteen columns a view has no value for.
    row.extend(std::iter::repeat_n(Datum::Null, 16));
    // CREATE_TIME is the creating job's TSO.
    row[14] = update_time_datum(view.update_ts, zone);
    // TABLE_COMMENT, TIDB_TABLE_ID, TIDB_ROW_ID_SHARDING_INFO, TIDB_PK_TYPE.
    row.push(text("VIEW"));
    row.push(if view.id == 0 {
        Datum::Null
    } else {
        Datum::Int(view.id)
    });
    row.push(Datum::Null);
    row.push(text("NONCLUSTERED"));
    // The four trailing TiDB placement/mode columns.
    row.extend(std::iter::repeat_n(Datum::Null, 4));
    row
}

/// Go `setDataFromOneTable` for a sequence: Go stores it as a table with no
/// columns and no collation (`CreateSequence` calls `BuildTableInfo` with
/// neither), so `EstimateDataLength` reports one row and no data, and the
/// collation falls back to the server default.
fn sequence_tables_row(
    schema: &str,
    sequence: &tidb_table::sequence::SequenceDef,
    zone: &tidb_datatype::SessionTimeZone,
) -> Vec<Datum> {
    vec![
        text(CATALOG),
        text(schema),
        text(&sequence.name),
        text("SEQUENCE"),
        text("InnoDB"),
        Datum::Int(10),
        text("Compact"),
        Datum::UInt(1),
        Datum::UInt(0),
        Datum::UInt(0),
        Datum::Int(0),
        Datum::UInt(0),
        Datum::Int(0),
        Datum::Null,
        update_time_datum(sequence.update_ts, zone),
        Datum::Null,
        Datum::Null,
        text(tidb_mysql::DefaultCollationName),
        Datum::Null,
        text(""),
        text(""),
        Datum::Int(sequence.id),
        text("NOT_SHARDED"),
        text("NONCLUSTERED"),
        Datum::Null,
        text("Normal"),
        Datum::Null,
        text(""),
    ]
}

/// One row per view, in schema then view order.
///
/// `CHECK_OPTION` is the view's stored mode, `CASCADED` unless `WITH LOCAL
/// CHECK OPTION` was written -- Go records one on every view, written or not
/// (captured).
///
/// DIVERGENCE (documented): `IS_UPDATABLE` is always `NO`, which is what Go
/// reports for every view this tier can create -- no view here is updatable.
fn non_empty_or<'a>(value: &'a str, default: &'a str) -> &'a str {
    if value.is_empty() {
        default
    } else {
        value
    }
}

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
            // Go reads the view's own charset and collation (the session's
            // connection ones at CREATE VIEW), falling back to the defaults.
            text(non_empty_or(&view.character_set_client, CHARSET)),
            text(non_empty_or(&view.collation_connection, COLLATION)),
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
    ctx: &crate::StmtContext,
) -> Vec<Vec<Datum>> {
    let mut rows = Vec::new();
    for (schema, table_name) in visible_tables(catalog, visibility, PrivMask::Column) {
        let privileges = visibility.column_privileges(&schema, &table_name);
        match catalog.table_in(&schema, &table_name) {
            Some(TableEntry::Kv(table)) => {
                // Hidden columns are absent here, and ORDINAL_POSITION
                // counts only the visible ones (Go `ListColumns`' `ord`).
                // Captured: a table with an expression index and columns
                // `a`, `z` reports exactly a|1, z|2.
                for (ordinal, (offset, column)) in table.visible_columns().into_iter().enumerate() {
                    rows.push(column_row(
                        &schema,
                        &table_name,
                        table,
                        ordinal + 1,
                        offset,
                        column,
                        &privileges,
                        ctx,
                    ));
                }
            }
            // A view's columns are its body's, resolved now rather than
            // at CREATE (Go fills them the same way here as DESCRIBE
            // does). A body that no longer resolves drops out of this
            // table entirely, which is what Go answers (captured: a view
            // over a dropped column reports no COLUMNS rows at all).
            Some(TableEntry::View(view)) => {
                let Ok(columns) = crate::view_column_list(view, &schema, catalog, ctx)
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
                        &privileges,
                    ));
                }
            }
            _ => continue,
        }
    }
    rows
}

/// One `COLUMNS` row: `ordinal` is its `ORDINAL_POSITION`, `offset` its
/// place in the stored row.
fn column_row(
    schema: &str,
    table_name: &str,
    table: &KvTable,
    ordinal: usize,
    offset: usize,
    column: &crate::KvColumn,
    privileges: &str,
    ctx: &crate::StmtContext,
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
        Datum::Int(ordinal as i64),
        match &column.default_value {
            // Go `NewColDesc`: `NoDefaultValueFlag` reports no default.
            _ if field_type.has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE) => {
                Datum::Null
            }
            Some(tidb_table::column_default::ColumnDefault::Value(Datum::Null)) | None => {
                Datum::Null
            }
            // Go `infoschema_reader.go` fills COLUMN_DEFAULT from
            // `ColDesc.DefaultValue`, the same string `SHOW COLUMNS` reports,
            // so a computed default reports its stored text unparenthesised.
            Some(tidb_table::column_default::ColumnDefault::Value(value)) => {
                // Go's INFORMATION_SCHEMA retriever deliberately keeps the
                // stored metadata text when GetColDefaultValue cannot
                // materialize it; SHOW propagates the same conversion error.
                let visible = crate::show::columns::literal_column_default_text(
                    value,
                    column,
                    ctx.query_default_conversion_flags(),
                    &ctx.session_zone(),
                )
                .ok()
                .flatten()
                .or_else(|| crate::show::columns::column_default_text(value, field_type))
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
        text(&crate::show::columns::column_key_flag(table, offset)),
        text(&crate::show::columns::column_extra(
            field_type,
            table.auto_increment_offset() == Some(offset),
            column.generated.as_ref().map(|generated| generated.stored),
            column
                .default_value
                .as_ref()
                .is_some_and(tidb_table::column_default::ColumnDefault::is_default_generated),
        )),
        text(privileges),
        // Go `COLUMN_COMMENT`, from `ColumnInfo.Comment`.
        text(&column.comment),
        // Go `GENERATION_EXPRESSION`, `ColumnInfo.GeneratedExprString`.
        text(
            column
                .generated
                .as_ref()
                .map_or("", |generated| generated.expr_text.as_str()),
        ),
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

/// Go `dataForColumnsInTable`'s type cells: SET reports the summed element
/// lengths plus the separating commas, ENUM its longest element, a string
/// type its length, a fractionable type its precision, a numeric type its
/// precision and scale, and the NULL type zero lengths. The octet length is
/// `calcCharOctLength`, the charset's bytes per character times the length.
/// A column without a charset reports no charset or collation
/// (`table.NewColDesc`).
fn type_cells(field_type: &FieldType) -> TypeCells {
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
    let (charset_name, collation_name) = if field_type.has_charset() {
        (
            text(field_type.charset_name()),
            text(field_type.collation_name()),
        )
    } else {
        (Datum::Null, Datum::Null)
    };
    let mut cells = TypeCells {
        char_max: Datum::Null,
        char_octet: Datum::Null,
        numeric_precision: Datum::Null,
        numeric_scale: Datum::Null,
        datetime_precision: Datum::Null,
        charset_name,
        collation_name,
    };
    let character_lengths = |length: i64| {
        (
            Datum::Int(length),
            Datum::Int(length.saturating_mul(field_type.charset().maxlen())),
        )
    };
    match code {
        FieldTypeCode::Set => {
            let elems = field_type.elems().snapshot();
            let mut length = elems.iter().map(|elem| elem.len() as i64).sum::<i64>();
            if !elems.is_empty() {
                length += elems.len() as i64 - 1;
            }
            (cells.char_max, cells.char_octet) = character_lengths(length);
        }
        FieldTypeCode::Enum => {
            let length = field_type
                .elems()
                .snapshot()
                .iter()
                .map(|elem| elem.len() as i64)
                .max()
                .unwrap_or(0);
            (cells.char_max, cells.char_octet) = character_lengths(length);
        }
        _ if code.is_string() => {
            (cells.char_max, cells.char_octet) = character_lengths(flen);
        }
        _ if code.is_type_fractionable() => cells.datetime_precision = Datum::Int(decimal),
        _ if code.is_type_numeric() => {
            cells.numeric_precision = Datum::Int(numeric_precision_of(field_type, flen));
            // FLOAT and DOUBLE report no scale when none was written.
            if !matches!(code, FieldTypeCode::Float | FieldTypeCode::Double)
                || decimal != UNSPECIFIED_LENGTH
            {
                cells.numeric_scale = Datum::Int(decimal);
            }
        }
        FieldTypeCode::Null => {
            cells.char_max = Datum::Int(0);
            cells.char_octet = Datum::Int(0);
        }
        _ => {}
    }
    cells
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
    privileges: &str,
) -> Vec<Datum> {
    let TypeCells {
        char_max,
        char_octet,
        numeric_precision,
        numeric_scale,
        datetime_precision,
        ..
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
        // Go `NewColDesc(table.ToColumn(col))` reads the view's STORED
        // column, which carries no charset or collation.
        Datum::Null,
        Datum::Null,
        text(&field_type.info_schema_str(STRICT_INTEGER_DISPLAY_WIDTH)),
        text(""),
        text(""),
        text(privileges),
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
        tidb_charset::Collation::from_name(self.name)
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
            let collation = tidb_charset::Charset::from_name(name)
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
        let meta_key = format!("t{}", table.table_id());
        if let Some(offset) = table.pk_handle_offset() {
            entries.push((
                schema.go_to_lower(),
                meta_key.clone(),
                vec![
                    text(&schema),
                    text(&table_name),
                    Datum::Int(0),
                    text("PRIMARY"),
                    Datum::Int(1),
                    text(&table.columns()[offset].name),
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
            // Go `setDataFromIndex`: a common handle's primary key is the
            // clustered index; `ConditionExprString` is a partial index's
            // predicate.
            let clustered = if index.clustered_primary { "YES" } else { "NO" };
            let predicate = table
                .partial_index_condition_string(index.id)
                .map_or(Datum::Null, |condition| text(&condition));
            for (position, offset) in index.column_offsets.iter().enumerate() {
                let column = &table.columns()[*offset];
                let prefix = index.prefix_length(position);
                // An expression key part is a hidden column: Go reports
                // COLUMN_NAME 'NULL' beside its expression text.
                let generated_expr = column
                    .generated
                    .as_ref()
                    .filter(|_| table.is_hidden(*offset))
                    .map(|generated| generated.expr_text.clone());
                entries.push((
                    schema.go_to_lower(),
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
                        if prefix == UNSPECIFIED_LENGTH {
                            Datum::Null
                        } else {
                            Datum::Int(prefix)
                        },
                        text(&index.comment),
                        match &generated_expr {
                            Some(expr) => text(expr),
                            None => Datum::Null,
                        },
                        Datum::Int(index.id),
                        text(if index.visible { "YES" } else { "NO" }),
                        text(clustered),
                        Datum::Int(i64::from(index.global)),
                        predicate.clone(),
                    ],
                ));
            }
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    entries.into_iter().map(|(_, _, row)| row).collect()
}

/// Go `memtableRetriever.setDataForMetricTables`: one row per metric table,
/// sorted by name, with the labels comma-joined.
fn metrics_tables_rows() -> Vec<Vec<Datum>> {
    tidb_infoschema::metric_table_def::METRIC_TABLE_MAP
        .iter()
        .map(|(name, def)| {
            vec![
                Datum::Bytes(name.as_bytes().to_vec()),
                Datum::Bytes(def.prom_ql.as_bytes().to_vec()),
                Datum::Bytes(def.labels.join(",").into_bytes()),
                Datum::Real(def.quantile),
                Datum::Bytes(def.comment.as_bytes().to_vec()),
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

/// Go setDataFromSequences reads the schema definition, never allocator state.
fn sequences_rows(catalog: &Catalog, visibility: &SchemaVisibility) -> Vec<Vec<Datum>> {
    visible_tables(catalog, visibility, ANY_PRIV)
        .into_iter()
        .filter_map(|(schema, name)| {
            let sequence = catalog.sequence_in(&schema, &name)?;
            let info = sequence.allocator.info();
            Some(vec![
                text(CATALOG),
                text(&schema),
                text(&sequence.name),
                Datum::Int(i64::from(info.cache)),
                Datum::Int(info.cache_value),
                Datum::Int(i64::from(info.cycle)),
                Datum::Int(info.increment),
                Datum::Int(info.max_value),
                Datum::Int(info.min_value),
                Datum::Int(info.start),
                text(&sequence.comment),
            ])
        })
        .collect()
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

/// Go `memtableRetriever.setDataFromResourceGroups` over the groups the
/// resource manager holds -- for this tier, the catalog's, as `onCreate`/
/// `onAlterResourceGroup` register them (`NewGroupFromOptions`' shape).
fn resource_groups_rows(catalog: &crate::Catalog) -> Vec<Vec<Datum>> {
    let mut groups: Vec<_> = catalog.resource_groups().collect();
    groups.sort_by(|left, right| left.name.lowercase().cmp(right.name.lowercase()));
    groups.into_iter().map(resource_group_row).collect()
}

fn resource_group_row(group: &tidb_model::ResourceGroupInfo) -> Vec<Datum> {
    let name = text(group.name.original());
    let Some(settings) = group
        .settings
        .as_deref()
        .filter(|settings| settings.ru_rate > 0)
    else {
        // Not RU mode.
        return vec![
            name,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ];
    };
    let duration = |ms: i64| tidb_config_configtypes::format_go_duration(ms * 1_000_000);
    let mut query_limit = String::new();
    if let Some(runaway) = &settings.runaway {
        let runaway = runaway.read();
        let mut rules = Vec::new();
        if runaway.exec_elapsed_time_ms > 0 {
            rules.push(format!(
                "EXEC_ELAPSED='{}'",
                duration(runaway.exec_elapsed_time_ms as i64)
            ));
        }
        if runaway.processed_keys > 0 {
            rules.push(format!("PROCESSED_KEYS={}", runaway.processed_keys));
        }
        if runaway.request_unit > 0 {
            rules.push(format!("RU={}", runaway.request_unit));
        }
        query_limit = rules.join(", ");
        use tidb_model::ResourceGroupRunawayAction as Action;
        match runaway.action {
            Action::DRY_RUN | Action::COOLDOWN | Action::KILL => {
                query_limit.push_str(&format!(", ACTION={}", runaway.action.sql()));
            }
            Action::SWITCH_GROUP => query_limit.push_str(&format!(
                ", ACTION={}({})",
                runaway.action.sql(),
                runaway.switch_group_name
            )),
            _ => {}
        }
        // `NewGroupFromOptions` keeps a watch only for a watch type.
        if runaway.watch_type != tidb_model::ResourceGroupRunawayWatch::NONE {
            if runaway.watch_duration_ms > 0 {
                query_limit.push_str(&format!(
                    ", WATCH={} DURATION='{}'",
                    runaway.watch_type.sql(),
                    duration(runaway.watch_duration_ms)
                ));
            } else {
                query_limit.push_str(&format!(
                    ", WATCH={} DURATION=UNLIMITED",
                    runaway.watch_type.sql()
                ));
            }
        }
    }
    let mut background = Vec::new();
    if let Some(settings) = &settings.background {
        let settings = settings.read();
        if let Some(job_types) = settings
            .job_types
            .as_ref()
            .filter(|types| !types.is_empty())
        {
            background.push(format!("TASK_TYPES='{}'", job_types.join(",")));
        }
        if settings.resource_util_limit > 0 {
            background.push(format!(
                "UTILIZATION_LIMIT={}",
                settings.resource_util_limit
            ));
        }
    }
    let background = background.join(", ");
    let optional = |value: String| {
        if value.is_empty() {
            Datum::Null
        } else {
            text(&value)
        }
    };
    vec![
        name,
        text(&if settings.ru_rate == i64::from(i32::MAX) as u64 {
            "UNLIMITED".to_owned()
        } else {
            settings.ru_rate.to_string()
        }),
        text(tidb_ast::priority_value_to_name(settings.priority)),
        text(match settings.burst_limit {
            -1 => "UNLIMITED",
            -2 => "MODERATED",
            _ => "OFF",
        }),
        optional(query_limit),
        optional(background),
    ]
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

/// A `DATETIME` cell for a unix timestamp, in the node's own clock -- Go's
/// `types.NewTime(types.FromGoTime(time.Unix(ts, 0)), mysql.TypeDatetime, 0)`.
pub fn datetime_datum(unix_seconds: i64) -> tidb_datatype::Datum {
    use chrono::{Datelike, TimeZone, Timelike};
    // go renders these datetimes through the node's own clock (`FromGoTime`),
    // i.e. the system's local zone -- the UTC read answered 13:07 where the
    // oracle shows 21:07 for the same boot instant.
    let Some(local) = chrono::Local
        .timestamp_opt(unix_seconds, 0)
        .single()
        .map(|moment| moment.naive_local())
    else {
        return tidb_datatype::Datum::Null;
    };
    tidb_datatype::Time::from_date_checked(
        local.year(),
        local.month() as i32,
        local.day() as i32,
        local.hour() as i32,
        local.minute() as i32,
        local.second() as i32,
        0,
        tidb_datatype::TimeType::DateTime,
        0,
    )
    .map_or(tidb_datatype::Datum::Null, tidb_datatype::Datum::Time)
}

