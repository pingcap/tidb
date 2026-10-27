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
//! `ShowCreate`/`ShowColumns`/`ShowTables`) arm,
//! reached through [`Session::dispatch_admin_stmt`], plus the `SHOW CREATE TABLE`/`VIEW` text
//! builders and column-description rows those arms share with
//! `Session::retrieve_memory_table` in `dispatch.rs`.
//!
//! `EXPLAIN`, account management (`CREATE`/`ALTER`/`DROP`/`RENAME USER`,
//! `GRANT`/`REVOKE`/`SHOW GRANTS`), and `KILL`/processlist live in their own
//! modules (`explain_arm`, `account`, `process_arm`) and are only delegated
//! to from [`Session::dispatch_admin_stmt`] here.

use crate::privilege::PrivilegeTypeExt;
use crate::show_index::show_index_rows;
use tidb_executor::simple::account::AccountExec;
use tidb_executor::simple::account_password::AccountPasswordExec;
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
use create::{show_create_table_text, show_create_view_text};

#[path = "show_statistics.rs"]
pub(crate) mod statistics;

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
    /// v2: schema name, then table id, both descending, each table the
    /// extractor admits by its lowercased name.
    fn show_affinity_rows(
        &mut self,
        extractor: tidb_executor::show::Extractor<'_>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
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
                    let lower = go_to_lower(&table.name());
                    if !tidb_executor::show::extractor_admits(extractor, &lower, &lower) {
                        continue;
                    }
                    let row = |partition: Option<String>, group_id: String| AffinityRow {
                        db_lower: go_to_lower(&database),
                        table_id: table.table_id,
                        db: database.clone(),
                        table: table.name(),
                        partition,
                        group_id,
                    };
                    match affinity.level.as_str() {
                        "table" => infos.push(row(
                            None,
                            tidb_ddl::ddl_affinity::get_table_affinity_group_id(table.table_id),
                        )),
                        "partition" => {
                            for definition in
                                table.partition().map_or(&[][..], |spec| &spec.definitions)
                            {
                                infos.push(row(
                                    Some(definition.name.clone()),
                                    tidb_ddl::ddl_affinity::get_partition_affinity_group_id(
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
            DriverError::Mysql(tidb_executor::MysqlError::unknown(error.to_string()))
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
        Ok(rows)
    }

    /// Go `Compile` + `runStmt` for a SHOW the planner builds: the statement
    /// preprocessed (`resolveShowStmt`) and planned (`buildShow` and the
    /// optimizer), its `PhysicalShow`'s rows fetched ([`Self::fetch_show`],
    /// Go `ShowExec.fetchAll`), and the plan's selections, limit, projection
    /// and sort run over them. Planning and running read the statement's
    /// catalog (its snapshot and temporary tables); fetching holds none, so
    /// a fetcher reads it as it needs.
    pub(crate) fn run_planned_show(
        &mut self,
        show: &tidb_ast::ShowStmt,
    ) -> Result<StmtOutput, DriverError> {
        let auth_user = self.vars.current_user.clone().and_then(|identity| {
            identity
                .split_once('@')
                .map(|(user, host)| (user.to_owned(), host.to_owned()))
        });
        let ctx = self.statement_context(false);
        let current_db = self.vars.current_db.clone();
        let (plan, visits) = self.with_catalog_mut(|catalog| {
            tidb_executor::driver::plan_show_stmt(
                show,
                auth_user.as_ref().map(|(user, host)| (user.as_str(), host.as_str())),
                catalog,
                &current_db,
                &ctx,
            )
        })?;
        // Go `CheckPrivilege` over `buildShow`'s visit infos.
        self.check_visit_infos(&visits)?;
        let mut physical = match plan {
            tidb_planner::common_plans::Plan::Physical(physical) => physical,
            // Go `executorBuilder.buildShowNextRowID`.
            tidb_planner::common_plans::Plan::ShowNextRowID(plan) => {
                let rows = self.show_next_row_id_rows(&plan.table_name)?;
                let columns = plan
                    .names
                    .iter()
                    .zip(&plan.schema.columns)
                    .map(|(name, column)| {
                        (
                            name.names.column.original.clone(),
                            column.ret_type.clone().unwrap_or_else(|| {
                                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar)
                            }),
                        )
                    })
                    .collect();
                return Ok(StmtOutput::Rows { columns, rows });
            }
            _ => return Err(DriverError::unsupported("a SHOW planned as no SHOW plan")),
        };
        // A selection the optimizer folded to false leaves no `PhysicalShow`
        // to fetch for, as Go builds no `ShowExec` under a `TableDual`.
        if let Some(leaf) = tidb_executor::driver::physical_show_of(&physical).cloned() {
            let rows = self.fetch_show(&leaf)?;
            ctx.show_retrieval().supply(leaf.base.base.id(), rows);
        }
        let (columns, rows) = self.with_catalog_retrieving(&ctx, |catalog| {
            tidb_executor::driver::open_show_plan(&mut physical, catalog, &ctx)?.collect()
        })?;
        Ok(StmtOutput::Rows { columns, rows })
    }

    /// Go `ShowExec.fetchShowStatus`: one `Variable_name | Value` row per
    /// status variable that `variable.GetStatusVars` collects from the
    /// registered `Statistics` providers, sorted by name, with `GLOBAL` scope
    /// skipping session-only variables.
    ///
    /// This tier serves only `SHOW_STATUS_VARS` (see its doc comment for what
    /// is not modelled). As for SHOW VARIABLES, GLOBAL and SESSION read the
    /// same values because this tier keeps no persisted global tier; GLOBAL
    /// still drops session-only rows, which the Go capture confirms (`SHOW
    /// GLOBAL STATUS` omits the `Compression*` family).
    fn show_status_rows(&self, global: bool) -> Result<Vec<Vec<Datum>>, DriverError> {
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
            if global && session_only {
                continue;
            }
            let row = vec![
                Datum::Bytes(name.as_bytes().to_vec()),
                Datum::Bytes(value.as_bytes().to_vec()),
            ];
            rows.push(row);
        }
        Ok(rows)
    }

    /// Go `ShowExec.fetchShowCreate{Database,ResourceGroup,PlacementPolicy,
    /// Table,Sequence,View}`: the one row a SHOW CREATE answers.
    fn show_create_rows(
        &mut self,
        contents: &tidb_planner::logical::ShowContents,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        if contents.tp == tidb_ast::ShowStmtType::CreateDatabase {
            let database = &contents.db_name;
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
            return Ok(crate::show_create_database::rows(
                reported,
                charset,
                placement.as_deref(),
                contents.if_not_exists,
                self.vars.sql_mode().has_ansi_quotes_mode(),
            ));
        }
        // Go `fetchShowCreateResourceGroup`: the group by name
        // (`ErrResourceGroupNotExists` when absent), reported under
        // the name as written beside `CREATE RESOURCE GROUP`
        // followed by its settings.
        if contents.tp == tidb_ast::ShowStmtType::CreateResourceGroup {
            let group_name = &contents.resource_group_name;
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
            return Ok(vec![vec![
                Datum::Bytes(group_name.clone().into_bytes()),
                Datum::Bytes(text.into_bytes()),
            ]]);
        }
        // Go `fetchShowCreatePlacementPolicy` (`executor/show.go:1774`)
        // looks the policy up and reports `ErrPlacementPolicyNotExists`
        // (8239) for a name that is not there. The row is the policy's
        // ORIGINAL-cased name beside
        // `ConstructResultOfShowCreatePlacementPolicy` (`:1742`).
        if contents.tp == tidb_ast::ShowStmtType::CreatePlacementPolicy {
            let policy_name = &contents.db_name;
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
            return Ok(crate::show_create_placement_policy::rows(&reported, &clause));
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
        let want_view = match contents.tp {
            tidb_ast::ShowStmtType::CreateTable | tidb_ast::ShowStmtType::CreateSequence => {
                false
            }
            tidb_ast::ShowStmtType::CreateView => true,
            other => {
                return Err(DriverError::unsupported(format!(
                    "SHOW {other:?} has no SHOW CREATE row"
                )))
            }
        };
        let want_sequence = contents.tp == tidb_ast::ShowStmtType::CreateSequence;
        let (database, table_name) =
            (contents.db_name.clone(), contents.table_name.clone().unwrap_or_default());
        self.require_show_create_privilege(&database, &table_name)?;
        let ctx = self.statement_context(false);
        // A view answers either spelling with the same row, which
        // is Go's own behaviour; only `SHOW CREATE VIEW` on a base
        // table is refused.
        let ansi_quotes = self.vars.sql_mode().has_ansi_quotes_mode();
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
                    let stored = match table.name() {
                        name if name.is_empty() => table_name.clone(),
                        name => name,
                    };
                    Ok((
                        show_create_table_text(&database, &stored, table, &ctx)?,
                        stored,
                        None,
                    ))
                }
                tidb_executor::TableEntry::Mem(_) => Err(DriverError::unsupported(
                    "SHOW CREATE TABLE needs a storage-backed table",
                )),
            }
        });
        let (text, reported, view_charset) = shown?;
        // Go's view form carries the session's character set and
        // collation; `buildShowSchema` names the columns from the
        // object, not the keyword written.
        if let Some((character_set_client, collation_connection)) = view_charset {
            return Ok(vec![vec![
                Datum::Bytes(reported.into_bytes()),
                Datum::Bytes(text.into_bytes()),
                Datum::Bytes(character_set_client.into_bytes()),
                Datum::Bytes(collation_connection.into_bytes()),
            ]]);
        }
        Ok(vec![vec![
            Datum::Bytes(reported.into_bytes()),
            Datum::Bytes(text.into_bytes()),
        ]])
    }

    /// Go `ShowNextRowIDExec.Next`: one row per allocator of the table, read
    /// through the domain's infoschema, which holds no local temporary table.
    fn show_next_row_id_rows(&mut self, path: &[String]) -> Result<Vec<Vec<Datum>>, DriverError> {
        let [database, table_name] = path else {
            return Err(DriverError::unsupported("SHOW TABLE NEXT_ROW_ID without its table"));
        };
        let ids = self.with_catalog_mut(|catalog| {
            let Some(entry) = catalog.persistent_table_in(database, table_name) else {
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
        Ok(ids
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
            .collect())
    }

    /// Go `ShowExec.fetchAll`: the rows of one planned SHOW, by its type.
    /// Only the kinds [`is_planned_show`] lists reach it.
    fn fetch_show(
        &mut self,
        leaf: &tidb_planner::physical::PhysicalShow,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        use tidb_ast::ShowStmtType as T;
        let contents = &leaf.contents;
        let extractor = base_extractor(leaf);
        let shared = self.shared_catalog();
        let strings = |names: Vec<String>| -> Vec<Vec<Datum>> {
            names.into_iter().map(|name| vec![Datum::new_string(name)]).collect()
        };
        Ok(match contents.tp {
            T::Charset => tidb_executor::show::fetch_show_charset(self),
            T::Collation => tidb_executor::show::fetch_show_collation(self, extractor),
            T::Databases => {
                let catalog = shared.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                strings(tidb_executor::show::fetch_show_databases(self, &catalog, extractor))
            }
            T::Engines => tidb_executor::show::fetch_show_engines(),
            // Go `fetchShowProcedureStatus`, `fetchShowTriggers` and the
            // `ShowEvents` / `ShowProfiles` / `ShowFunctionStatus` arms
            // answer nothing; `fetchShowOpenTables` answers the empty set;
            // `fetchShowPlugins` lists an empty `plugin.GetAll()`; the BRIE
            // listings read a queue this server never fills.
            // Go `fetchShowImportJobs` / `fetchShowImportGroups` read the
            // dist-task framework, which runs no IMPORT job here.
            T::ImportJobs | T::ImportGroups => Vec::new(),
            T::ProcedureStatus
            | T::FunctionStatus
            | T::Triggers
            | T::Events
            | T::Profiles
            | T::OpenTables
            | T::Plugins
            | T::Backups
            | T::Restores => Vec::new(),
            T::Tables => {
                let catalog = shared.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                tidb_executor::show::fetch_show_tables(
                    self,
                    &catalog,
                    &contents.db_name,
                    contents.full,
                    extractor,
                )?
                .into_iter()
                .map(|(name, table_type)| {
                    let mut row = vec![Datum::new_string(name)];
                    row.extend(table_type.map(Datum::new_string));
                    row
                })
                .collect()
            }
            T::Variables => {
                tidb_executor::show::fetch_show_variables(self, contents.global_scope, extractor)
            }
            // Go `executorBuilder.buildShow`: "show master status need start
            // ts", then `fetchShowMasterStatus`.
            T::MasterStatus | T::BinlogStatus => {
                self.activate_statement_txn();
                crate::show_admin::master_status_rows(self.current_tso().value())
            }
            T::Privileges => crate::show_admin::show_privileges_rows(),
            T::Status => self.show_status_rows(contents.global_scope)?,
            T::CreateTable
            | T::CreateView
            | T::CreateSequence
            | T::CreateDatabase
            | T::CreatePlacementPolicy
            | T::CreateResourceGroup => self.show_create_rows(contents)?,
            T::Grants => self.show_grants_rows(contents.user.as_ref(), &contents.roles)?,
            // Pinned Go `ShowExec.fetchAll` keeps the grammar entry but
            // rejects execution because extended statistics was removed.
            T::StatsExtended => {
                return Err(DriverError::unsupported(
                    "Extended statistics feature has been removed",
                ))
            }
            // Go `ShowExec.fetchShowStatsMeta` (`executor/show_stats.go:36`).
            T::StatsMeta => {
                self.require_show_stats_privilege("stats_meta")?;
                self.stats_meta_rows(leaf.extractor.as_ref())?
            }
            T::StatsHealthy => self.stats_healthy_rows(leaf.extractor.as_ref())?,
            T::ColumnStatsUsage => self.column_stats_usage_rows()?,
            T::HistogramsInFlight => {
                let count =
                    self.with_catalog_mut(|catalog| Ok(catalog.clean_needed_statistics_items()))?;
                vec![tidb_executor::show_stats::histograms_in_flight_row(count)]
            }
            T::AnalyzeStatus => self.analyze_status_rows()?,
            // Go `fetchShowBind` (LIKE is a case-sensitive selection on
            // `Original_sql`: Go gives BINDINGS no extractor).
            T::Bindings => self.show_bindings_rows(contents.global_scope)?,
            T::BindingCacheStatus => self.binding_cache_status_rows()?,
            // Go `ShowExec.fetchShowProcessList`: one row per live connection
            // of this server, read from the session manager.
            T::ProcessList => self.process_list_rows(contents.full),
            T::SessionStates => self.show_session_states_rows()?,
            // Go `fetchShowClusterConfigs`; Go gives CONFIG no extractor, so
            // LIKE is the plan's selection on `Type`.
            T::Config => self.cluster_config_table_rows(&[Default::default()])?,
            T::Affinity => self.show_affinity_rows(extractor)?,
            // Go `fetchShowPlacement` and its FOR DATABASE / TABLE /
            // PARTITION and LABELS forms (`executor/show_placement.go`); Go
            // gives them no extractor, so LIKE is the plan's selection.
            T::Placement
            | T::PlacementLabels
            | T::PlacementForDatabase
            | T::PlacementForTable
            | T::PlacementForPartition => self.placement_rows(contents)?,
            // Go `fetchShowTableRegions`.
            T::Regions => {
                let table_name = contents.table_name.clone().unwrap_or_default();
                let store = self.region_store();
                self.with_catalog_mut(|catalog| {
                    tidb_executor::show_regions::show_table_regions(
                        catalog,
                        store.as_ref(),
                        &contents.db_name,
                        &table_name,
                        &contents.table_partitions,
                        (!contents.index_name.is_empty()).then_some(contents.index_name.as_str()),
                    )
                })?
            }
            // Go `fetchShowDistributions`, which needs PD's HTTP client: the
            // table and partitions are checked, then PD's error answers.
            T::Distributions => {
                let table_name = contents.table_name.clone().unwrap_or_default();
                self.with_catalog_mut(|catalog| {
                    tidb_executor::show_regions::show_table_distributions(
                        catalog,
                        &contents.db_name,
                        &table_name,
                        &contents.table_partitions,
                    )
                })?;
                Vec::new()
            }
            T::StatsTopN => self.stats_topn_rows()?,
            T::StatsBuckets => {
                self.require_show_stats_privilege("stats_buckets")?;
                self.stats_buckets_rows()?
            }
            T::StatsHistograms => {
                self.require_show_stats_privilege("stats_histograms")?;
                self.stats_histograms_rows()?
            }
            T::StatsLocked => {
                self.require_show_stats_privilege("stats_table_locked")?;
                self.stats_locked_rows()?
            }
            T::CreateUser => {
                let spec = contents
                    .user
                    .as_ref()
                    .ok_or_else(|| DriverError::unsupported("SHOW CREATE USER without a user"))?;
                self.show_create_user_rows(spec)?
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
            T::Index => {
                let database = contents.db_name.clone();
                let table_name = contents.table_name.clone().unwrap_or_default();
                self.with_catalog_mut(|catalog| {
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
                })?
            }
            // Go `fetchShowColumns`.
            T::Columns => {
                let table = [
                    contents.db_name.clone(),
                    contents.table_name.clone().unwrap_or_default(),
                ];
                self.show_column_rows(
                    &contents.db_name,
                    &table,
                    extractor,
                    contents.full,
                    contents.extended,
                )?
            }
            // Go `fetchShowTableStatus`: the 1044 gate SHOW TABLES applies,
            // the unknown schema, then its restricted SQL over
            // `information_schema.tables` for the schema, ordered by table
            // name (so every size and count is the TABLES reader's), each
            // table the extractor admits by its lowercased name.
            T::TableStatus => {
                let database = contents.db_name.clone();
                self.require_visible_database(&database)?;
                let zone = self.session_time_zone();
                let visibility = self.schema_visibility();
                let catalog = self.table_storage_statistics_catalog(true)?;
                if catalog.table_names(&database).is_none() {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(database)));
                }
                crate::infoschema::show_table_status_rows(&catalog, &visibility, &zone, &database)
                    .into_iter()
                    .filter(|row| {
                        let name = go_to_lower(datum_text(&row[0]).unwrap_or_default());
                        tidb_executor::show::extractor_admits(extractor, &name, &name)
                    })
                    .collect()
            }
            T::Warnings | T::Errors => {
                self.warning_rows(contents.count_warnings_or_errors, contents.tp == T::Errors)
            }
            // Go `fetchShowBuiltins` over `expression.GetBuiltinList`.
            T::Builtins => strings(tidb_executor::builtin_list()),
            other => {
                return Err(DriverError::unsupported(format!(
                    "SHOW {other:?} is not planned"
                )))
            }
        })
    }

}

impl Session {
    /// Go `ShowExec.fetchShowColumns`: one row per column of the table (its
    /// hidden ones too with `extended`), the `FULL` shape with `full`, each
    /// column the extractor admits by its lowercased name.
    fn show_column_rows(
        &mut self,
        database: &str,
        table_path: &[String],
        extractor: tidb_executor::show::Extractor<'_>,
        full: bool,
        extended: bool,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let admits = |name: &str| {
            let lower = go_to_lower(name);
            tidb_executor::show::extractor_admits(extractor, &lower, &lower)
        };
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
                    .filter(|(candidate, _)| admits(candidate))
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
                .filter(|(_, candidate)| admits(&candidate.name))
                .map(|(offset, candidate)| column_description(candidate, offset, table, full, &ctx))
                .collect::<Result<Vec<_>, _>>()
        })?;
        Ok(rows)
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
                    tidb_mysql::ShowViewPriv,
                ),
                "SHOW VIEW",
            )
        } else {
            let mut mask = crate::privilege::any_priv_mask();
            if !self.is_local_temporary_table(database, table_name) {
                mask &= !tidb_mysql::CreateTMPTablePriv.bit();
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

    /// The `AdminStmt` dispatch for the whole admin/inspection surface: the `SHOW` family here, plus
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
                        let role = self
                            .bdr_role_store()?
                            .get()
                            .map_err(DriverError::unsupported)?;
                        let mut field_type =
                            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::String);
                        field_type.set_flen(1);
                        return Ok(Some(StmtOutput::Rows {
                            columns: vec![("BDR_ROLE".to_owned(), field_type)],
                            rows: vec![vec![Datum::Bytes(role.into_bytes())]],
                        }));
                    }
                };
                self.bdr_role_store()?
                    .set(role)
                    .map_err(DriverError::unsupported)?;
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
            // ANALYZE, LOAD STATS and DROP STATS write the statistics storage
            // and the domain's cache: the session's node runs them
            // (`StatementRoute::Analyze` / `LoadStats` / `DropStats`).
            tidb_ast::AdminStmt::AnalyzeTable(_)
            | tidb_ast::AdminStmt::AnalyzeIncremental(_)
            | tidb_ast::AdminStmt::LoadStats(_)
            | tidb_ast::AdminStmt::DropStats(_)
            | tidb_ast::AdminStmt::LockStats(_)
            | tidb_ast::AdminStmt::UnlockStats(_) => Err(DriverError::unsupported(
                "statistics statements run on the session's node",
            )),
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
                        && !self.has_scoped_privilege("", "", tidb_mysql::SuperPriv)
                    {
                        return Err(DriverError::Mysql(tidb_executor::MysqlError::terror(
                            &tidb_error::catalog::privileges::ERR_ACCESS_DENIED,
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
            // Go `TraceExec`: runs the statement, then reports the spans.
            tidb_ast::AdminStmt::Trace(trace) => {
                crate::trace::check_options(trace)
                    .map_err(|message| DriverError::Mysql(tidb_executor::MysqlError::unknown(
                        message,
                    )))?;
                let inner_sql = crate::trace::statement_text(&trace.statement);
                let inner = (*trace.statement).clone();
                let recorder = crate::trace::StatementTrace::start();
                let entered = recorder.enter();
                // Go `session.ExecuteStmt`; this path drains the record set
                // inside it.
                let execute_stmt =
                    tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt");
                let outcome = self.execute_statement_owned(inner, &inner_sql);
                drop(execute_stmt);
                drop(entered);
                // Go `executeChild` and `drainRecordSet`'s events.
                let affected = match outcome {
                    Ok(StmtOutput::Rows { rows, .. }) => {
                        recorder.event(&format!(
                            "execute done, ReturnRow: {}, ModifyRow: 0",
                            rows.len()
                        ));
                        0
                    }
                    Ok(StmtOutput::Affected(count)) => count,
                    Ok(_) => 0,
                    Err(error) => {
                        let mysql = error.to_mysql_error();
                        recorder.event(&format!(
                            "execute with error({}): {}",
                            mysql.code, mysql.message
                        ));
                        0
                    }
                };
                recorder.event(&format!("execute done, modify row: {affected}"));
                let (spans, started) = recorder.finish();
                let (columns, rows) = crate::trace::render(&trace.format, &spans, started)
                    .map_err(|message| DriverError::Mysql(tidb_executor::MysqlError::unknown(
                        message,
                    )))?;
                Ok(Some(StmtOutput::Rows { columns, rows }))
            }
            // Go `getTiProxyAddrs`: every TRAFFIC statement talks to a
            // TiProxy, and this deployment has none.
            tidb_ast::AdminStmt::Traffic(_) => Err(DriverError::Mysql(tidb_executor::MysqlError::unknown(
                "no tiproxy server found",
            ))),
            // Go `RecommendIndexExec` (`executor/recommend_index.go`).
            tidb_ast::AdminStmt::RecommendIndex(recommend) => {
                self.recommend_index_stmt(recommend).map(Some)
            }
            tidb_ast::AdminStmt::Grant(grant) => {
                self.grant_stmt(grant)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            tidb_ast::AdminStmt::Revoke(revoke) => {
                self.revoke_stmt(revoke)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            tidb_ast::AdminStmt::GrantRole(grant) => {
                self.grant_role_stmt(grant)?;
                Ok(Some(StmtOutput::Affected(0)))
            }
            tidb_ast::AdminStmt::RevokeRole(revoke) => {
                self.revoke_role_stmt(revoke)?;
                Ok(Some(StmtOutput::Affected(0)))
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
                return crate::show_admin::flush_stmt(flush).map(Some);
            }
            tidb_ast::AdminStmt::ShowDdl => {
                return Ok(Some(crate::show_admin::show_ddl_output(
                    &self.show_ddl_rows(),
                )));
            }
            // Go `Compile` + `runStmt` over `buildShow`'s plan.
            tidb_ast::AdminStmt::Show(show) if is_planned_show(show.tp) => {
                self.run_planned_show(show).map(Some)
            }
            // Go has no `ShowImports` fetcher or schema (`buildShowSchema`
            // names no columns for it); this tier answers the empty
            // `Job_ID | State` listing.
            tidb_ast::AdminStmt::Show(show) if show.tp == tidb_ast::ShowStmtType::Imports => {
                Ok(Some(crate::show_admin::show_imports_output()))
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
            // Go `fetchShowCreateProcedure`'s object is not stored here: the
            // statement answers nothing.
            tidb_ast::AdminStmt::Show(show) if show.tp == tidb_ast::ShowStmtType::CreateProcedure => {
                Ok(None)
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
                    proto: &tidb_error::plannererrors::ERR_UNSUPPORTED_TYPE,
                    message: "Unsupported type *ast.RestartStmt".to_owned(),
                }),
                tidb_ast::ServerControlStmt::Help(_) => Err(DriverError::DdlCoded {
                    proto: &tidb_error::plannererrors::ERR_UNSUPPORTED_TYPE,
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
            tidb_planner::show_schema::col_desc_field_names(false),
            ["Field", "Type", "Null", "Key", "Default", "Extra"]
        );
        assert_eq!(
            tidb_planner::show_schema::col_desc_field_names(true),
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

/// The SHOW kinds Go's `buildShow` plans here ([`Session::run_planned_show`]);
/// the rest still answer in the session's SHOW arms.
fn is_planned_show(tp: tidb_ast::ShowStmtType) -> bool {
    use tidb_ast::ShowStmtType as T;
    matches!(
        tp,
        T::Charset
            | T::Collation
            | T::Databases
            | T::Engines
            | T::ProcedureStatus
            | T::FunctionStatus
            | T::Triggers
            | T::Events
            | T::Profiles
            | T::OpenTables
            | T::Plugins
            | T::Backups
            | T::Restores
            | T::Tables
            | T::Variables
            | T::MasterStatus
            | T::BinlogStatus
            | T::Privileges
            | T::Builtins
            | T::Warnings
            | T::Errors
            | T::Status
            | T::TableStatus
            | T::Columns
            | T::Index
            | T::CreateTable
            | T::CreateView
            | T::CreateSequence
            | T::CreateDatabase
            | T::CreatePlacementPolicy
            | T::CreateResourceGroup
            | T::Grants
            | T::CreateUser
            | T::StatsExtended
            | T::StatsMeta
            | T::StatsHealthy
            | T::ColumnStatsUsage
            | T::HistogramsInFlight
            | T::AnalyzeStatus
            | T::StatsTopN
            | T::StatsBuckets
            | T::StatsHistograms
            | T::StatsLocked
            | T::Bindings
            | T::BindingCacheStatus
            | T::ProcessList
            | T::SessionStates
            | T::Config
            | T::Affinity
            | T::ReplicaStatus
            | T::Placement
            | T::PlacementLabels
            | T::PlacementForDatabase
            | T::PlacementForTable
            | T::PlacementForPartition
            | T::Regions
            | T::Distributions
            | T::ImportJobs
            | T::ImportGroups
            | T::TableNextRowId
    )
}

/// The `ShowBaseExtractor` `buildShow` installed on a planned SHOW.
fn base_extractor(
    leaf: &tidb_planner::physical::PhysicalShow,
) -> Option<&tidb_planner::show_predicate_extractor::ShowBaseExtractor> {
    match &leaf.extractor {
        Some(tidb_planner::logical::ShowPredicateExtractor::Base(extractor)) => Some(extractor),
        _ => None,
    }
}

