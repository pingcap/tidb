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

//! Go `br/pkg/restore/snap_client/systable_restore.go`: system tables
//! restored logically from their temporary databases (`REPLACE INTO ...
//! SELECT` or `RENAME TABLE`), and the compatibility checks before it. The
//! physical stats and system table loads (`--fast-load-sys-tables`), the
//! stats schema upgrades and the `txn-total-size-limit` memory quota are
//! `br` CLI options and are not ported.

use std::collections::HashMap;

use tidb_ast::CiString;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::{ColumnInfo, TableInfo};
use tidb_table_filter::Filter;

use super::client::SnapClient;
use crate::errors::{self, Result, ERR_RESTORE_INCOMPATIBLE_SYS, ERR_UNKNOWN};
use crate::glue::Domain;
use crate::metautil;
use crate::restore::misc::get_table_schema;
use crate::utils::misc::{
    enclose_db_and_table, enclose_name, get_sys_db_cistr_name, is_type_compatible,
    strip_temp_db_prefix, temporary_db_name, SYSTEM_DB, SYS_DB, WORKLOAD_SCHEMA,
};

/// Go `sysUserTableName`.
const SYS_USER_TABLE_NAME: &str = "user";
/// Go `sysDBTableName`.
const SYS_DB_TABLE_NAME: &str = "db";
/// Go `sysBindInfoTableName`.
const SYS_BIND_INFO_TABLE_NAME: &str = "bind_info";

/// Go `bindinfo.StmtRemoveDuplicatedPseudoBinding`.
const STMT_REMOVE_DUPLICATED_PSEUDO_BINDING: &str = "DELETE FROM mysql.bind_info
       WHERE original_sql='builtin_pseudo_sql_for_bind_lock' AND
       _tidb_rowid NOT IN ( -- keep one arbitrary pseudo binding
         SELECT _tidb_rowid FROM mysql.bind_info WHERE original_sql='builtin_pseudo_sql_for_bind_lock' limit 1)";

/// Go `compatibleMissingBackupSystemTableColumn`.
#[derive(Clone, Copy)]
struct CompatibleMissingColumn {
    required_columns: &'static [&'static str],
    select_expression: &'static str,
}

/// Go `compatibleMissingBackupSystemTableColumns`: the columns newer
/// system tables gained, with how to fill them from an older backup.
fn get_compatible_missing_backup_system_table_column(
    table: &str,
    column: &str,
) -> Option<CompatibleMissingColumn> {
    // Go `strings.ToLower(mysql.OperateViewPriv.ColumnString())`.
    const OPERATE_VIEW_PRIV: &str = "operate_view_priv";
    match (table, column) {
        (SYS_USER_TABLE_NAME, OPERATE_VIEW_PRIV) => Some(CompatibleMissingColumn {
            required_columns: &["super_priv"],
            select_expression: "IF(`Super_priv` = 'Y', 'Y', 'N')",
        }),
        (SYS_DB_TABLE_NAME, OPERATE_VIEW_PRIV) => Some(CompatibleMissingColumn {
            required_columns: &[],
            select_expression: "'N'",
        }),
        (SYS_BIND_INFO_TABLE_NAME, "last_used_date") => Some(CompatibleMissingColumn {
            required_columns: &[],
            select_expression: "NULL",
        }),
        _ => None,
    }
}

/// Go `planPeplayerTables`.
const PLAN_REPLAYER_TABLES: &[&str] = &["plan_replayer_status", "plan_replayer_task"];

/// Go `statsTables`.
const STATS_TABLES: &[&str] = &[
    "stats_buckets",
    "stats_extended",
    "stats_feedback",
    "stats_fm_sketch",
    "stats_histograms",
    "stats_history",
    "stats_meta",
    "stats_meta_history",
    "stats_table_locked",
    "stats_top_n",
    "column_stats_usage",
];

/// Go `renameableSysTables`.
const RENAMEABLE_SYS_TABLES: &[&str] = &[
    "bind_info",
    "user",
    "db",
    "tables_priv",
    "columns_priv",
    "default_roles",
    "role_edges",
    "global_priv",
    "global_grants",
];

/// Go `unRecoverableTable["mysql"]`.
const UNRECOVERABLE_MYSQL_TABLES: &[&str] = &[
    // Some variables in tidb (e.g. gc_safe_point) cannot be overridden
    // because it will cause severe consequences.
    "tidb",
    // Not overriding these tables keeps the downstream cluster's settings.
    "global_variables",
    "capture_plan_baselines_blacklist",
    "advisory_locks",
    "analyze_jobs",
    "analyze_options",
    "dist_framework_meta",
    "tidb_global_task",
    "tidb_global_task_history",
    "tidb_background_subtask",
    "tidb_background_subtask_history",
    "tidb_ddl_history",
    "tidb_ddl_job",
    "tidb_ddl_reorg",
    "tidb_ddl_notifier",
    "tidb_import_jobs",
    "help_topic",
    "request_unit_by_group",
    "table_cache_meta",
    "tidb_runaway_queries",
    "tidb_runaway_watch",
    "tidb_runaway_watch_done",
    "tidb_ttl_job_history",
    "tidb_ttl_table_status",
    "tidb_ttl_task",
    "tidb_timers",
    "tidb_mview_refresh_info",
    "tidb_mview_fast_refresh_source_info",
    "tidb_mlog_purge_info",
    "tidb_mview_refresh_hist",
    "tidb_mview_refresh_alert",
    "tidb_mlog_purge_hist",
    "tidb_storage_class_transition_history",
    "gc_delete_range",
    "gc_delete_range_done",
    "index_advisor_results",
    "tidb_mdl_info",
    "tidb_mdl_view",
    "tidb_pitr_id_map",
    "tidb_restore_registry",
    "tidb_masking_policy",
];

/// Go `unRecoverableTable["sys"]`.
const UNRECOVERABLE_SYS_TABLES: &[&str] = &["schema_unused_indexes"];

/// Go `checkPrivilegeTableRowsCollateCompatibilitySQLPair`.
struct CollatePair {
    upstream_collate_sql: &'static str,
    downstream_collate_sql: &'static str,
    columns: &'static [&'static str],
}

/// Go `collateCompatibilityTables["mysql"]`.
fn collate_compatibility_table(db: &str, table: &str) -> Option<CollatePair> {
    if db != SYSTEM_DB {
        return None;
    }
    match table {
        "db" => Some(CollatePair {
            upstream_collate_sql: "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.db",
            downstream_collate_sql: "SELECT COUNT(1) FROM (SELECT Host, DB COLLATE utf8mb4_general_ci, User FROM __TiDB_BR_Temporary_mysql.db GROUP BY Host, DB COLLATE utf8mb4_general_ci, User) as a",
            columns: &["db"],
        }),
        "tables_priv" => Some(CollatePair {
            upstream_collate_sql: "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.tables_priv",
            downstream_collate_sql: "SELECT COUNT(1) FROM (SELECT Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci FROM __TiDB_BR_Temporary_mysql.tables_priv GROUP BY Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci) as a",
            columns: &["db", "table_name"],
        }),
        "columns_priv" => Some(CollatePair {
            upstream_collate_sql: "SELECT COUNT(1) FROM __TiDB_BR_Temporary_mysql.columns_priv",
            downstream_collate_sql: "SELECT COUNT(1) FROM (SELECT Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci, Column_name COLLATE utf8mb4_general_ci FROM __TiDB_BR_Temporary_mysql.columns_priv GROUP BY Host, DB COLLATE utf8mb4_general_ci, User, Table_name COLLATE utf8mb4_general_ci, Column_name COLLATE utf8mb4_general_ci) as a",
            columns: &["db", "table_name", "column_name"],
        }),
        _ => None,
    }
}

/// Go `isUnrecoverableTable`; the whole `workload_schema` is
/// unrecoverable (Go `unRecoverableSchema`).
fn is_unrecoverable_table(schema: &str, table: &str) -> bool {
    match schema {
        WORKLOAD_SCHEMA => true,
        SYSTEM_DB => UNRECOVERABLE_MYSQL_TABLES.contains(&table),
        SYS_DB => UNRECOVERABLE_SYS_TABLES.contains(&table),
        _ => false,
    }
}

/// Go `isStatsTable`.
fn is_stats_table(schema: &str, table: &str) -> bool {
    schema == SYSTEM_DB && STATS_TABLES.contains(&table)
}

/// Go `isRenameableSysTable`.
pub(crate) fn is_renameable_sys_table(schema: &str, table: &str) -> bool {
    schema == SYSTEM_DB && RENAMEABLE_SYS_TABLES.contains(&table)
}

/// Go `isPlanReplayerTables`.
fn is_plan_replayer_tables(schema: &str, table: &str) -> bool {
    schema == SYSTEM_DB && PLAN_REPLAYER_TABLES.contains(&table)
}

/// Go `GetDBNameIfStatsTemporaryTable`.
#[must_use]
pub fn get_db_name_if_stats_temporary_table(temp_schema: &str, table: &str) -> Option<String> {
    match strip_temp_db_prefix(temp_schema) {
        (db, true) if is_stats_table(db, table) => Some(db.to_owned()),
        _ => None,
    }
}

/// Go `GetDBNameIfRenameableSysTemporaryTable`.
#[must_use]
pub fn get_db_name_if_renameable_sys_temporary_table(
    temp_schema: &str,
    table: &str,
) -> Option<String> {
    match strip_temp_db_prefix(temp_schema) {
        (db, true) if is_renameable_sys_table(db, table) => Some(db.to_owned()),
        _ => None,
    }
}

/// Go `TemporaryTableChecker`: the temporary tables a physical load
/// renames into place.
#[derive(Clone, Copy, Debug, Default)]
pub struct TemporaryTableChecker {
    /// Go `loadStatsPhysical`.
    pub load_stats_physical: bool,
    /// Go `loadSysTablePhysical`.
    pub load_sys_table_physical: bool,
}

impl TemporaryTableChecker {
    /// Go `CheckTemporaryTables`: the system database the table belongs to.
    #[must_use]
    pub fn check_temporary_tables(&self, temp_schema: &str, table: &str) -> Option<String> {
        if self.load_stats_physical {
            if let Some(db) = get_db_name_if_stats_temporary_table(temp_schema, table) {
                return Some(db);
            }
        }
        if self.load_sys_table_physical {
            return get_db_name_if_renameable_sys_temporary_table(temp_schema, table);
        }
        None
    }
}

/// Go `berrors.ErrUnknown.Wrap(err).GenWithStack(message)`.
fn unknown(err: errors::Error, message: impl Into<String>) -> errors::Error {
    std::sync::Arc::new(ERR_UNKNOWN.wrap(err).gen_with_stack(message))
}

/// Go `multierr.Append`.
fn append_err(acc: Option<errors::Error>, err: errors::Error) -> Option<errors::Error> {
    Some(match acc {
        None => err,
        Some(prev) => errors::errorf(format!("{prev}; {err}")),
    })
}

fn columns_by_name(t: &TableInfo) -> HashMap<String, ColumnInfo> {
    t.columns
        .iter_deref()
        .map(|c| c.read().clone())
        .map(|c| (c.name.lowercase().to_owned(), c))
        .collect()
}

fn columns(t: &TableInfo) -> Vec<ColumnInfo> {
    t.columns.iter_deref().map(|c| c.read().clone()).collect()
}

/// Go `removeUserResourceGroup`: resource groups are not restored, so they
/// are dropped from the restored users.
fn remove_user_resource_group(
    ctx: &Context,
    db_name: &str,
    exec_sql: &dyn Fn(&Context, &str) -> Result<()>,
) -> Result<()> {
    let sql = format!(
        "UPDATE {} SET User_attributes = JSON_REMOVE(User_attributes, '$.resource_group');",
        enclose_db_and_table(db_name, SYS_USER_TABLE_NAME)
    );
    if let Err(err) = exec_sql(ctx, &sql) {
        if !err
            .to_string()
            .contains("Unknown column 'User_attributes' in 'field list'")
        {
            return Err(err);
        }
        tidb_log::warn(
            "remove resource group meta failed, please ensure target cluster is newer than v6.6.0",
            &[Field::new("error", Value::Str(err.to_string()))],
        );
    }
    Ok(())
}

/// Go `buildSystemTableReplaceColumns`: the target's columns, in its
/// order, with the expressions selecting them from the backup.
fn build_system_table_replace_columns(
    db_name: &str,
    table_name: &str,
    upstream: &TableInfo,
    downstream: &TableInfo,
) -> Result<(Vec<String>, Vec<String>)> {
    let upstream_cols = columns_by_name(upstream);
    let downstream_cols = columns_by_name(downstream);
    for col in columns(upstream) {
        if !downstream_cols.contains_key(col.name.lowercase()) {
            return Err(errors::annotatef(
                &ERR_RESTORE_INCOMPATIBLE_SYS,
                format!(
                    "missing column in cluster data, table: {}, col: {} {}",
                    upstream.name.original(),
                    col.name,
                    col.field_type
                ),
            ));
        }
    }
    let mut names = Vec::new();
    let mut exprs = Vec::new();
    for col in columns(downstream) {
        let l = col.name.lowercase();
        if !upstream_cols.contains_key(l) {
            if let Some(cfg) = get_compatible_missing_backup_system_table_column(table_name, l) {
                if can_load_system_table_with_missing_backup_column(
                    db_name,
                    table_name,
                    l,
                    &upstream_cols,
                ) {
                    names.push(enclose_name(l));
                    exprs.push(cfg.select_expression.to_owned());
                    continue;
                }
                return Err(errors::annotatef(
                    &ERR_RESTORE_INCOMPATIBLE_SYS,
                    format!(
                        "missing column in backup data, table: {}, col: {} {}",
                        upstream.name.original(),
                        col.name,
                        col.field_type
                    ),
                ));
            }
            continue;
        }
        names.push(enclose_name(l));
        exprs.push(enclose_name(l));
    }
    if names.is_empty() {
        return Err(errors::annotatef(
            &ERR_RESTORE_INCOMPATIBLE_SYS,
            format!(
                "no compatible columns for system table restore, table: {}",
                upstream.name.original()
            ),
        ));
    }
    Ok((names, exprs))
}

/// Go `canLoadSystemTableWithMissingBackupColumn`.
fn can_load_system_table_with_missing_backup_column(
    db_name: &str,
    table_name: &str,
    column_name: &str,
    backup_columns: &HashMap<String, ColumnInfo>,
) -> bool {
    if db_name != SYSTEM_DB {
        return false;
    }
    get_compatible_missing_backup_system_table_column(table_name, column_name).is_some_and(|cfg| {
        !cfg.select_expression.is_empty()
            && cfg
                .required_columns
                .iter()
                .all(|c| backup_columns.contains_key(*c))
    })
}

/// Go `canSkipUnrecognizedMissingBackupSystemTableColumn`.
fn can_skip_unrecognized_missing_backup_system_table_column(
    db_name: &str,
    table_name: &str,
    column_name: &str,
) -> bool {
    db_name == SYSTEM_DB
        && table_name == SYS_USER_TABLE_NAME
        && get_compatible_missing_backup_system_table_column(table_name, column_name).is_none()
}

/// Go `database`: a system database's existing tables and its temporary
/// name.
struct SysDatabase {
    existing_tables: HashMap<String, TableInfo>,
    name: CiString,
    temporary_name: CiString,
}

impl SnapClient {
    /// Go `RestoreSystemSchemas`: `mysql`, `sys` and `workload_schema`, in
    /// that order.
    ///
    /// # Errors
    /// A table cannot be merged in.
    pub fn restore_system_schemas(
        &self,
        ctx: &Context,
        f: &dyn Filter,
        load_sys_table_physical: bool,
    ) -> Result<()> {
        for sys_db in [SYSTEM_DB, SYS_DB, WORKLOAD_SCHEMA] {
            self.restore_system_schema(ctx, f, sys_db, load_sys_table_physical)?;
        }
        Ok(())
    }

    /// Go `restoreSystemSchema`: the temporary database is dropped after a
    /// success; a failure keeps it for a checkpoint retry.
    fn restore_system_schema(
        &self,
        ctx: &Context,
        f: &dyn Filter,
        sys_db: &str,
        load_sys_table_physical: bool,
    ) -> Result<()> {
        let res = self.restore_system_schema_inner(ctx, f, sys_db, load_sys_table_physical);
        if res.is_ok() {
            self.clean_temporary_database(ctx, sys_db);
        }
        res
    }

    fn restore_system_schema_inner(
        &self,
        ctx: &Context,
        f: &dyn Filter,
        sys_db: &str,
        load_sys_table_physical: bool,
    ) -> Result<()> {
        if !f.match_schema(sys_db) || !self.with_sys_table() {
            tidb_log::info(
                "system database filtered out",
                &[Field::new("database", Value::Str(sys_db.into()))],
            );
            return Ok(());
        }
        let Some(origin) = self.databases.get(&temporary_db_name(sys_db)) else {
            tidb_log::info(
                "system database not backed up, skipping",
                &[Field::new("database", Value::Str(sys_db.into()))],
            );
            return Ok(());
        };
        let Some(db) = self.get_system_database_by_name(sys_db)? else {
            tidb_log::warn(
                "target database not exist, aborting",
                &[Field::new("database", Value::Str(sys_db.into()))],
            );
            return Ok(());
        };
        let mut restored = Vec::with_capacity(origin.tables.len());
        for table in &origin.tables {
            let name = &table.info().name;
            if !f.match_table(sys_db, name.original()) {
                continue;
            }
            if load_sys_table_physical && is_renameable_sys_table(sys_db, name.original()) {
                continue;
            }
            if let Err(err) = self.replace_temporary_table_to_systable(ctx, table.info(), &db) {
                tidb_log::warn(
                    "error during merging temporary tables into system tables",
                    &[
                        Field::new("error", Value::Str(err.to_string())),
                        Field::new("table", Value::Str(name.to_string())),
                    ],
                );
                return Err(errors::annotate(
                    err,
                    format!(
                        "error during merging temporary tables into system tables, table: {name}"
                    ),
                ));
            }
            restored.push(name.lowercase().to_owned());
        }
        self.after_system_tables_replaced(ctx, sys_db, &restored)
            .map_err(|e| {
                errors::annotate(e, "error during extra works after system tables replaced")
            })
    }

    /// Go `getSystemDatabaseByName`.
    fn get_system_database_by_name(&self, name: &str) -> Result<Option<SysDatabase>> {
        let is = self.dom()?.info_schema()?;
        if is.schema_by_name(name).is_none() {
            return Ok(None);
        }
        let existing_tables = is
            .schema_table_infos(name)
            .into_iter()
            .map(|t| (t.name.lowercase().to_owned(), t))
            .collect();
        Ok(Some(SysDatabase {
            existing_tables,
            name: CiString::new(name),
            temporary_name: CiString::new(temporary_db_name(name)),
        }))
    }

    /// Go `afterSystemTablesReplaced`: privileges reloaded after
    /// `mysql.user`, duplicated pseudo bindings removed after
    /// `mysql.bind_info`.
    fn after_system_tables_replaced(
        &self,
        ctx: &Context,
        db: &str,
        tables: &[String],
    ) -> Result<()> {
        if db != SYSTEM_DB {
            return Ok(());
        }
        let mut err = None;
        for table in tables {
            if table == "user" {
                match self.dom()?.notify_update_all_users_privilege() {
                    Err(serr) => {
                        tidb_log::warn("failed to flush privileges, please manually execute `FLUSH PRIVILEGES`", &[]);
                        err = append_err(err, unknown(serr, "failed to flush privileges"));
                    }
                    Ok(()) => tidb_log::info(
                        "privilege system table restored, please reconnect to make it effective",
                        &[],
                    ),
                }
            } else if table == "bind_info" {
                match self
                    .db()?
                    .with_session(|se| se.execute(ctx, STMT_REMOVE_DUPLICATED_PSEUDO_BINDING))
                {
                    Err(serr) => {
                        tidb_log::warn(
                            "failed to delete duplicated pseudo binding",
                            &[Field::new("error", Value::Str(serr.to_string()))],
                        );
                        err = append_err(
                            err,
                            unknown(serr, format!("failed to delete duplicated pseudo binding {STMT_REMOVE_DUPLICATED_PSEUDO_BINDING}")),
                        );
                    }
                    Ok(()) => tidb_log::info("success to remove duplicated pseudo binding", &[]),
                }
            }
        }
        err.map_or(Ok(()), Err)
    }

    /// Go `replaceTemporaryTableToSystable`: an existing system table gets
    /// the backed-up rows by `REPLACE INTO ... SELECT`, a missing one is
    /// renamed into place. Statistics, plan replayer and unrecoverable
    /// tables are skipped.
    fn replace_temporary_table_to_systable(
        &self,
        ctx: &Context,
        ti: &TableInfo,
        db: &SysDatabase,
    ) -> Result<()> {
        let db_name = db.name.lowercase();
        let table_name = ti.name.lowercase();
        let exec_sql = |ctx: &Context, sql: &str| -> Result<()> {
            let fields = |err: Option<&errors::Error>| {
                let mut f = vec![
                    Field::new("table", Value::Str(table_name.into())),
                    Field::new("database", Value::Str(db.name.to_string())),
                    Field::new("sql", Value::Str(sql.into())),
                ];
                if let Some(e) = err {
                    f.push(Field::new("error", Value::Str(e.to_string())));
                }
                f
            };
            if let Err(err) = self.db()?.with_session(|se| se.execute(ctx, sql)) {
                tidb_log::warn(
                    "failed to execute SQL restore system database",
                    &fields(Some(&err)),
                );
                return Err(unknown(err, format!("failed to execute {sql}")));
            }
            tidb_log::info("successfully restore system database", &fields(None));
            Ok(())
        };
        // The new tables have other table IDs, so the old statistics are
        // invalid.
        if is_stats_table(db_name, table_name)
            || is_plan_replayer_tables(db_name, table_name)
            || is_unrecoverable_table(db_name, table_name)
        {
            return Ok(());
        }
        if db_name == SYSTEM_DB && table_name == SYS_USER_TABLE_NAME {
            remove_user_resource_group(ctx, db.temporary_name.lowercase(), &exec_sql)?;
        }
        if let Some(downstream) = db.existing_tables.get(table_name) {
            tidb_log::info(
                "replace into existing table",
                &[
                    Field::new("table", Value::Str(table_name.into())),
                    Field::new("schema", Value::Str(db.name.to_string())),
                ],
            );
            if self.privilege_table_rows_collate_compatibility {
                self.check_privilege_table_rows_collate_compatibility(
                    ctx, db_name, table_name, ti, downstream,
                )?;
            }
            // The target's column order may differ from the backup's.
            let (names, exprs) =
                build_system_table_replace_columns(db_name, table_name, ti, downstream)?;
            let sql = format!(
                "REPLACE INTO {}({}) SELECT {} FROM {};",
                enclose_db_and_table(db.name.lowercase(), table_name),
                names.join(","),
                exprs.join(","),
                enclose_db_and_table(db.temporary_name.lowercase(), table_name)
            );
            return exec_sql(ctx, &sql);
        }
        let sql = format!(
            "RENAME TABLE {} TO {};",
            enclose_db_and_table(db.temporary_name.lowercase(), table_name),
            enclose_db_and_table(db.name.lowercase(), table_name)
        );
        exec_sql(ctx, &sql)
    }

    /// Go `cleanTemporaryDatabase`: a failure is only logged.
    fn clean_temporary_database(&self, ctx: &Context, origin_db: &str) {
        let database = temporary_db_name(origin_db).to_lowercase();
        tidb_log::debug(
            "dropping temporary database",
            &[Field::new("database", Value::Str(database.clone()))],
        );
        let sql = format!("DROP DATABASE IF EXISTS {}", enclose_name(&database));
        let res = self
            .db()
            .and_then(|db| db.with_session(|se| se.execute(ctx, &sql)));
        if let Err(err) = res {
            tidb_log::warn(
                "failed to drop temporary database, it should be dropped manually",
                &[
                    Field::new("database", Value::Str(database)),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
        }
    }
}

/// Go `CheckSysTableCompatibility`: each backed-up renameable system table
/// must match the cluster's columns (order aside); whether the tables can
/// also be loaded physically.
///
/// # Errors
/// [`ERR_RESTORE_INCOMPATIBLE_SYS`] for a missing table or column, or an
/// incompatible type or collation.
pub fn check_sys_table_compatibility(
    dom: &dyn Domain,
    tables: &[metautil::Table],
    collation_check: bool,
) -> Result<bool> {
    tidb_log::info(
        "checking target cluster system table compatibility with backed up data",
        &[],
    );
    let mut can_load_physical = true;
    let incompatible = |m: String| errors::annotatef(&ERR_RESTORE_INCOMPATIBLE_SYS, m);
    for table in tables {
        let (sys_db, ok) = get_sys_db_cistr_name(&table.db.name);
        let backup_ti = table.info();
        if !ok || !is_renameable_sys_table(sys_db.lowercase(), backup_ti.name.lowercase()) {
            continue;
        }
        let Ok(ti) = get_table_schema(dom, sys_db.original(), backup_ti.name.original()) else {
            tidb_log::error(
                "missing table on target cluster",
                &[Field::new("table", Value::Str(backup_ti.name.to_string()))],
            );
            return Err(errors::annotate(
                errors::new(&ERR_RESTORE_INCOMPATIBLE_SYS),
                format!("missed system table: {}", backup_ti.name.original()),
            ));
        };
        let backup_cols = columns_by_name(backup_ti);
        // The order can differ but the types must be compatible.
        for col in columns(&ti) {
            let Some(backup_col) = backup_cols.get(col.name.lowercase()) else {
                // Newer system tables gain columns: a logical restore fills
                // them, a physical load cannot.
                if can_load_system_table_with_missing_backup_column(
                    sys_db.lowercase(),
                    backup_ti.name.lowercase(),
                    col.name.lowercase(),
                    &backup_cols,
                ) || can_skip_unrecognized_missing_backup_system_table_column(
                    sys_db.lowercase(),
                    backup_ti.name.lowercase(),
                    col.name.lowercase(),
                ) {
                    tidb_log::warn(
                        "missing column in backup data",
                        &[
                            Field::new("table", Value::Str(backup_ti.name.to_string())),
                            Field::new(
                                "col",
                                Value::Str(format!("{} {}", col.name, col.field_type)),
                            ),
                        ],
                    );
                    can_load_physical = false;
                    continue;
                }
                return Err(incompatible(format!(
                    "missing column in backup data, table: {}, col: {} {}",
                    backup_ti.name.original(),
                    col.name,
                    col.field_type
                )));
            };
            let (type_eq, collate_eq) = is_type_compatible(&backup_col.field_type, &col.field_type);
            can_load_physical = can_load_physical && collate_eq && type_eq;
            let mut collate_compatible = collate_eq;
            if type_eq && !collate_eq && collation_check {
                collate_compatible = check_sys_table_column_collate_compatibility(
                    sys_db.lowercase(),
                    backup_ti.name.lowercase(),
                    col.name.lowercase(),
                    backup_col.field_type.collation_name(),
                    col.field_type.collation_name(),
                );
            }
            if !(type_eq && collate_compatible) {
                return Err(incompatible(format!(
                    "incompatible column, table: {}, col in cluster: {} {}, col in backup: {} {}",
                    backup_ti.name.original(),
                    col.name,
                    col.field_type,
                    backup_col.name,
                    backup_col.field_type
                )));
            }
        }
        // The cluster's table must not have fewer columns than the backup.
        let cluster_cols = columns_by_name(&ti);
        for col in columns(backup_ti) {
            if !cluster_cols.contains_key(col.name.lowercase()) {
                return Err(incompatible(format!(
                    "missing column in cluster data, table: {}, col: {} {}",
                    backup_ti.name.original(),
                    col.name,
                    col.field_type
                )));
            }
        }
    }
    Ok(can_load_physical)
}

/// Go `checkSysTableColumnCollateCompatibility`: a `utf8mb4_bin` privilege
/// column may restore into a `utf8mb4_general_ci` one.
fn check_sys_table_column_collate_compatibility(
    db: &str,
    table: &str,
    column: &str,
    upstream_collate: &str,
    downstream_collate: &str,
) -> bool {
    if upstream_collate != "utf8mb4_bin" || downstream_collate != "utf8mb4_general_ci" {
        return false;
    }
    collate_compatibility_table(db, table).is_some_and(|p| p.columns.contains(&column))
}

impl SnapClient {
    /// Go `checkPrivilegeTableRowsCollateCompatibility`: the privilege rows
    /// stay distinct under `utf8mb4_general_ci`.
    fn check_privilege_table_rows_collate_compatibility(
        &self,
        ctx: &Context,
        db: &str,
        table: &str,
        upstream: &TableInfo,
        downstream: &TableInfo,
    ) -> Result<()> {
        let Some(pair) = collate_compatibility_table(db, table) else {
            return Ok(());
        };
        let incompatible = |m: String| errors::annotatef(&ERR_RESTORE_INCOMPATIBLE_SYS, m);
        let mut count = 0;
        for col in columns(upstream) {
            if pair.columns.contains(&col.name.lowercase()) {
                let collate = col.field_type.collation_name();
                if collate != "utf8mb4_bin" && collate != "utf8mb4_general_ci" {
                    return Err(incompatible(format!(
                        "incompatible column collate, upstream table {db}.{table} column {} collate is {collate} but should be utf8mb4_bin",
                        col.name.lowercase()
                    )));
                }
                count += 1;
            }
        }
        if count != pair.columns.len() {
            return Err(incompatible(format!(
                "incompatible column collate, upstream table {db}.{table} has only {count} columns with collate utf8mb4_bin"
            )));
        }
        count = 0;
        for col in columns(downstream) {
            if pair.columns.contains(&col.name.lowercase()) {
                let collate = col.field_type.collation_name();
                if collate != "utf8mb4_general_ci" {
                    return Err(incompatible(format!(
                        "incompatible column collate, downstream table {db}.{table} column {} collate should be {collate}",
                        col.name.lowercase()
                    )));
                }
                count += 1;
            }
        }
        if count != pair.columns.len() {
            return Err(incompatible(format!(
                "incompatible column collate, downstream table {db}.{table} has only {count} columns with collate utf8mb4_bin"
            )));
        }
        let count_of = |sql: &str| -> Result<i64> {
            let rows = self
                .db()?
                .with_session(|se| se.exec_restricted_sql(ctx, sql, &[]))
                .map_err(|e| errors::annotate(e, "failed to get the count of privilege rows"))?;
            rows.first()
                .and_then(|r| r.first())
                .map(tidb_datatype::Datum::get_int64)
                .ok_or_else(|| errors::errorf("failed to get the count of privilege rows"))
        };
        let upstream_count = count_of(pair.upstream_collate_sql)?;
        let downstream_count = count_of(pair.downstream_collate_sql)?;
        if upstream_count != downstream_count {
            return Err(errors::errorf(format!(
                "there are duplicated rows with collate utf8mb4_general_ci [upstream count {upstream_count} != downstream count {downstream_count}]"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
