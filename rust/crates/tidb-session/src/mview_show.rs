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

//! Go master `pkg/executor/show.go`'s materialized view forms: SHOW
//! MATERIALIZED VIEWS / MATERIALIZED VIEW LOGS, SHOW CREATE MATERIALIZED
//! VIEW (LOG), and SHOW MATERIALIZED VIEW ... REMAIN_LOGS / LOG ...
//! WAIT_PURGE, whose counts read the logs on a system session of the
//! domain (Go `beginMViewLogShowInternalTxn`).

use std::sync::Arc;

use tidb_ast::ShowStmtType;
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_executor::show::exec::ShowExec as _;
use tidb_model::TableInfo;
use tidb_planner_core_operator_logicalop::ShowContents;
use tidb_sqlescape::SqlArg;
use tidb_terror::MysqlError;

use crate::mview_refresh::{datum_i64, datum_u64, is_table_not_exists, plain, sys_sql};
use crate::Session;

/// Go master `materializedViewTablePrivs`.
const MATERIALIZED_VIEW_TABLE_PRIVS: [tidb_mysql::PrivilegeType; 5] = [
    tidb_mysql::SelectPriv,
    tidb_mysql::ShowViewPriv,
    tidb_mysql::AlterPriv,
    tidb_mysql::DropPriv,
    tidb_mysql::OperateViewPriv,
];

fn string(value: &str) -> Datum {
    Datum::new_string_datum(value.to_owned())
}

fn wrong_object(schema: &str, name: &str, kind: &str) -> DriverError {
    DriverError::Mysql(MysqlError::from_terror(
        &tidb_exeerrors::ERR_WRONG_OBJECT.gen_with_stack_by_args(&[schema.to_owned().into(), name.to_owned().into(), kind.to_owned().into()]),
    ))
}

fn database_not_exists(schema: &str) -> DriverError {
    DriverError::Mysql(MysqlError::from_terror(
        &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.to_owned().into()]),
    ))
}

/// Go `ShowExec.getTable`'s `ErrTableNotExists`.
fn table_not_exists(schema: &str, name: &str) -> DriverError {
    DriverError::Mysql(MysqlError::from_terror(
        &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[schema.to_owned().into(), name.to_owned().into()]),
    ))
}

/// The catalog facts one SHOW reads, taken under the statement's catalog.
struct Catalog(Arc<tidb_infoschema::SessionExtendedInfoSchema>);

impl Catalog {
    fn table(&self, schema: &str, name: &str) -> Option<TableInfo> {
        self.0.table_info_by_name(schema, name).map(|(_, table)| table.read().clone_like_go())
    }

    fn table_by_id(&self, id: i64) -> Option<TableInfo> {
        self.0.table_info_by_id(id).map(|table| table.read().clone_like_go())
    }
}

/// Go master `fetchShowCreateTable4MaterializedView`.
fn show_create_materialized_view(table: &TableInfo, ansi_quotes: bool) -> String {
    let escape = |name: &str| tidb_ddl_build::table_partition::escape_identifier(name, ansi_quotes);
    let Some(info) = table.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return String::new();
    };
    let columns: Vec<String> =
        table.cols().iter_deref().map(|column| column.read().clone()).filter(|column| !column.hidden).map(|column| escape(column.name.original())).collect();
    let mut out = format!("CREATE MATERIALIZED VIEW {} ({})", escape(table.name.original()), columns.join(", "));
    if !table.comment.is_empty() {
        out.push_str(&format!(" COMMENT = '{}'", tidb_parser_format::output_format(&table.comment)));
    }
    if table.shard_row_id_bits > 0 {
        out.push_str(&format!(" SHARD_ROW_ID_BITS = {}", table.shard_row_id_bits));
    }
    if table.pre_split_regions > 0 {
        out.push_str(&format!(" PRE_SPLIT_REGIONS = {}", table.pre_split_regions));
    }
    let refresh_method = if info.refresh_method.is_empty() { "FAST" } else { info.refresh_method.as_str() };
    out.push_str(&format!(" REFRESH {refresh_method}"));
    if refresh_method.eq_ignore_ascii_case("FAST") {
        if !info.refresh_start_with.is_empty() {
            out.push_str(&format!(" START WITH {}", info.refresh_start_with));
        }
        if !info.refresh_next.is_empty() {
            out.push_str(&format!(" NEXT {}", info.refresh_next));
        }
    }
    let mut attributes = Vec::new();
    if info.alert_warning_sec > 0 {
        attributes.push(format!("mview_alert_warning={}", info.alert_warning_sec));
    }
    if info.alert_overdue_sec > 0 {
        attributes.push(format!("mview_alert_overdue={}", info.alert_overdue_sec));
    }
    if info.alert_refresh_failed {
        attributes.push("mview_alert_refresh_failed=yes".to_owned());
    }
    if !attributes.is_empty() {
        out.push_str(&format!(" ATTRIBUTES='{}'", attributes.join(",")));
    }
    out.push_str(&format!(" AS {}", info.sql_content));
    out
}

/// Go master `fetchShowCreateTable4MaterializedViewLog`.
fn show_create_materialized_view_log(base: &TableInfo, mlog: &TableInfo, ansi_quotes: bool) -> String {
    let escape = |name: &str| tidb_ddl_build::table_partition::escape_identifier(name, ansi_quotes);
    let Some(info) = mlog.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return String::new();
    };
    let columns: Vec<String> = info.columns.iter().map(|column| escape(column.original())).collect();
    let mut out = format!("CREATE MATERIALIZED VIEW LOG ON {} ({})", escape(base.name.original()), columns.join(", "));
    if mlog.shard_row_id_bits > 0 {
        out.push_str(&format!(" SHARD_ROW_ID_BITS = {}", mlog.shard_row_id_bits));
    }
    if mlog.pre_split_regions > 0 {
        out.push_str(&format!(" PRE_SPLIT_REGIONS = {}", mlog.pre_split_regions));
    }
    if !info.purge_method.is_empty() || !info.purge_start_with.is_empty() || !info.purge_next.is_empty() {
        out.push_str(" PURGE");
        if info.purge_method.eq_ignore_ascii_case("IMMEDIATE") {
            out.push_str(" IMMEDIATE");
        } else {
            if !info.purge_start_with.is_empty() {
                out.push_str(&format!(" START WITH {}", info.purge_start_with));
            }
            if !info.purge_next.is_empty() {
                out.push_str(&format!(" NEXT {}", info.purge_next));
            }
        }
    }
    if let Some(rows) = info.log_accumulation_alert_rows {
        out.push_str(&format!(" ALERT ROWS {rows}"));
    }
    out
}

/// The base table SHOW CREATE MATERIALIZED VIEW LOG / WAIT_PURGE name
/// (Go master's shared checks; `kind` words the missing-log error).
enum MissingLog {
    WrongObject,
    Errorf,
}

impl Session {
    /// Go master `ShowExec.fetchAll`'s materialized view arms.
    pub(crate) fn show_materialized_view_rows(
        &mut self,
        contents: &ShowContents,
        extractor: tidb_executor::show::Extractor<'_>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let catalog = Catalog(self.with_catalog_scope(false, |catalog| Ok(Arc::new(catalog.clone())))?);
        let schema = contents.db_name.clone();
        let name = contents.table_name.clone().unwrap_or_default();
        let ansi_quotes = self.vars.sql_mode().has_ansi_quotes_mode();
        match contents.tp {
            ShowStmtType::MaterializedViews | ShowStmtType::MaterializedViewLogs => {
                self.show_materialized_views_or_logs(&catalog, contents.tp, &schema, extractor)
            }
            // Go master `fetchShowCreateMaterializedView`.
            ShowStmtType::CreateMaterializedView => {
                if catalog.0.schema_by_name(&schema).is_none() {
                    return Err(database_not_exists(&schema));
                }
                let table = catalog.table(&schema, &name).ok_or_else(|| table_not_exists(&schema, &name))?;
                if table.materialized_view.is_none() {
                    return Err(wrong_object(&schema, table.name.original(), "MATERIALIZED VIEW"));
                }
                Ok(vec![vec![
                    string(table.name.original()),
                    string(&show_create_materialized_view(&table, ansi_quotes)),
                    string(&table.charset),
                    string(&table.collate),
                ]])
            }
            // Go master `fetchShowCreateMaterializedViewLog`.
            ShowStmtType::CreateMaterializedViewLog => {
                let (base, mlog) = materialized_view_log_target(&catalog, &schema, &name, MissingLog::WrongObject)?;
                Ok(vec![vec![
                    string(base.name.original()),
                    string(&show_create_materialized_view_log(&base, &mlog, ansi_quotes)),
                ]])
            }
            ShowStmtType::MaterializedViewRemainLogs => self.show_materialized_view_remain_logs(&catalog, &schema, &name),
            ShowStmtType::MaterializedViewLogWaitPurge => {
                self.show_materialized_view_log_wait_purge(&catalog, &schema, &name)
            }
            other => Err(DriverError::unsupported(format!("SHOW {other:?} is not a materialized view form"))),
        }
    }

    /// Go master `fetchShowMaterializedViews` / `fetchShowMaterializedViewLogs`.
    fn show_materialized_views_or_logs(
        &self,
        catalog: &Catalog,
        tp: ShowStmtType,
        schema: &str,
        extractor: tidb_executor::show::Extractor<'_>,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let Some(names) = catalog.0.table_names(schema) else {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_exeerrors::ERR_BAD_DB.gen_with_stack_by_args(&[schema.to_owned().into()]),
            )));
        };
        let schema_l = tidb_hack::go_to_lower(schema);
        let visible = |name: &str| {
            let name_l = tidb_hack::go_to_lower(name);
            MATERIALIZED_VIEW_TABLE_PRIVS.iter().any(|&privilege| self.has_table_privilege(&schema_l, &name_l, privilege))
        };
        let mut rows: Vec<(String, i64, Vec<Datum>)> = Vec::new();
        for name in names {
            let Some(table) = catalog.table(schema, &name) else {
                continue;
            };
            let lower = tidb_hack::go_to_lower(table.name.original());
            if tp == ShowStmtType::MaterializedViews {
                if table.materialized_view.is_none()
                    || !visible(table.name.original())
                    || !tidb_executor::show::extractor_admits(extractor, &lower, &lower)
                {
                    continue;
                }
                let row = vec![Datum::Int(table.id), string(table.name.original())];
                rows.push((table.name.original().to_owned(), table.id, row));
                continue;
            }
            let Some(base_id) = table.materialized_view_log.as_ref().map(|info| info.read().base_table_id) else {
                continue;
            };
            let base_name = (base_id != 0)
                .then(|| catalog.table_by_id(base_id))
                .flatten()
                .map(|base| base.name.original().to_owned())
                .unwrap_or_default();
            if base_name.is_empty()
                || !tidb_executor::show::extractor_admits(extractor, &lower, &lower)
                || !visible(table.name.original())
            {
                continue;
            }
            let row = vec![Datum::Int(table.id), string(table.name.original()), Datum::Int(base_id), string(&base_name)];
            rows.push((table.name.original().to_owned(), table.id, row));
        }
        rows.sort_by(|(a_name, a_id, _), (b_name, b_id, _)| a_name.cmp(b_name).then(a_id.cmp(b_id)));
        Ok(rows.into_iter().map(|(_, _, row)| row).collect())
    }

    /// Go master `fetchShowMaterializedViewRemainLogs`: for each of the
    /// view's logs, the rows logged after its last successful read.
    fn show_materialized_view_remain_logs(
        &self,
        catalog: &Catalog,
        schema: &str,
        name: &str,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let Some(db) = catalog.0.schema_by_name(schema) else {
            return Err(database_not_exists(schema));
        };
        let db_name = db.read().name.original().to_owned();
        let view = catalog.table(schema, name).ok_or_else(|| table_not_exists(schema, name))?;
        let Some(info) = view.materialized_view.as_ref().map(|info| info.read().clone()) else {
            return Err(wrong_object(&db_name, view.name.original(), "MATERIALIZED VIEW"));
        };
        let view_name = view.name.original();
        if info.base_table_ids.is_empty() {
            return Err(plain(format!("base table does not exist for materialized view {db_name}.{view_name}")));
        }
        let mut mlogs: Vec<(String, TableInfo)> = Vec::new();
        for &base_id in info.base_table_ids.iter() {
            let Some(base) = catalog.table_by_id(base_id) else {
                return Err(plain(format!("base table does not exist for materialized view {db_name}.{view_name}")));
            };
            let mlog_id = base.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
            let missing = || plain(format!("materialized view log does not exist for base table {}", base.name.original()));
            if mlog_id == 0 {
                return Err(missing());
            }
            if mlogs.iter().any(|(_, mlog)| mlog.id == mlog_id) {
                continue;
            }
            let mlog = catalog.table_by_id(mlog_id).ok_or_else(missing)?;
            let Some(mlog_schema) = catalog.0.schema_name_by_table_id(mlog_id) else {
                return Err(plain(format!("schema does not exist for materialized view log {}", mlog.name.original())));
            };
            mlogs.push((mlog_schema, mlog));
        }
        self.with_mview_log_show_txn(|sys, _| {
            let rows = sys_sql(sys, "SELECT LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %?", &[SqlArg::Signed(view.id)])
                .map_err(|error| {
                    if is_table_not_exists(&error) {
                        plain("show materialized view remain logs: required system table mysql.tidb_mview_refresh_info does not exist")
                    } else {
                        error
                    }
                })?;
            let Some(row) = rows.first() else {
                return Err(plain(format!(
                    "show materialized view remain logs: refresh info row missing for materialized view {db_name}.{view_name}"
                )));
            };
            let Some(last_success_read_tso) = row.first().and_then(datum_u64) else {
                return Err(plain(format!(
                    "show materialized view remain logs: last success read tso is null for materialized view {db_name}.{view_name}"
                )));
            };
            let mut out = Vec::new();
            for (mlog_schema, mlog) in &mlogs {
                let counted = sys_sql(
                    sys,
                    "SELECT COUNT(*) FROM %n.%n WHERE _tidb_commit_ts > %?",
                    &[
                        SqlArg::String(mlog_schema.as_bytes()),
                        SqlArg::String(mlog.name.original().as_bytes()),
                        SqlArg::Unsigned(last_success_read_tso),
                    ],
                )?;
                let remain = counted.first().and_then(|row| row.first()).and_then(datum_i64).unwrap_or(0);
                out.push(vec![
                    Datum::Int(view.id),
                    string(view_name),
                    Datum::Int(mlog.id),
                    string(mlog.name.original()),
                    Datum::Int(remain),
                ]);
            }
            Ok(out)
        })
    }

    /// Go master `fetchShowMaterializedViewLogWaitPurge`: the log's rows a
    /// purge now would delete.
    fn show_materialized_view_log_wait_purge(
        &self,
        catalog: &Catalog,
        schema: &str,
        name: &str,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let (base, mlog) = materialized_view_log_target(catalog, schema, name, MissingLog::Errorf)?;
        let Some(mlog_schema) = catalog.0.schema_name_by_table_id(mlog.id) else {
            return Err(plain(format!("schema does not exist for materialized view log {}", mlog.name.original())));
        };
        // Go passes the statement's context.
        let exec_ctx = self.exec_ctx.clone();
        self.with_mview_log_show_txn(|sys, show_start_ts| {
            let rows = sys_sql(sys, "SELECT LAST_PURGED_TSO FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %?", &[SqlArg::Signed(mlog.id)])
                .map_err(|error| {
                    if is_table_not_exists(&error) {
                        plain("show materialized view log wait purge: required system table mysql.tidb_mlog_purge_info does not exist")
                    } else {
                        error
                    }
                })?;
            let Some(row) = rows.first() else {
                return Err(plain(format!(
                    "show materialized view log wait purge: purge info row missing for materialized view log on {schema}.{}",
                    base.name.original()
                )));
            };
            let last_purged_tso = row.first().and_then(datum_u64);
            let (public, building) = crate::mlog_purge::collect_dependent_mview_ids_for_mlog_purge(sys, &exec_ctx, &base, mlog.id)?;
            let safe_purge_tso = crate::mlog_purge::calc_materialized_view_log_safe_purge_tso(
                sys,
                &exec_ctx,
                schema,
                base.name.original(),
                show_start_ts,
                &public,
                &building,
            )?;
            let mut wait_purge = 0;
            if safe_purge_tso > 0 && last_purged_tso.is_none_or(|last| last < safe_purge_tso) {
                let names = [SqlArg::String(mlog_schema.as_bytes()), SqlArg::String(mlog.name.original().as_bytes())];
                let counted = match last_purged_tso {
                    Some(last) => sys_sql(
                        sys,
                        "SELECT COUNT(*) FROM %n.%n WHERE _tidb_commit_ts > %? AND _tidb_commit_ts <= %?",
                        &[names[0].clone(), names[1].clone(), SqlArg::Unsigned(last), SqlArg::Unsigned(safe_purge_tso)],
                    )?,
                    None => sys_sql(
                        sys,
                        "SELECT COUNT(*) FROM %n.%n WHERE _tidb_commit_ts <= %?",
                        &[names[0].clone(), names[1].clone(), SqlArg::Unsigned(safe_purge_tso)],
                    )?,
                };
                wait_purge = counted.first().and_then(|row| row.first()).and_then(datum_i64).unwrap_or(0);
            }
            Ok(vec![vec![
                Datum::Int(mlog.id),
                string(mlog.name.original()),
                Datum::Int(base.id),
                string(base.name.original()),
                Datum::Int(wait_purge),
            ]])
        })
    }

    /// Go master `beginMViewLogShowInternalTxn`: a system session with
    /// materialized views enabled, in a transaction rolled back afterwards;
    /// `body` gets the session and the transaction's start TS.
    fn with_mview_log_show_txn<T>(
        &self,
        body: impl FnOnce(&mut Session, u64) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let mut sys = self.open_refresh_system_session()?;
        let original = sys_sql(&mut sys, "SELECT @@SESSION.tidb_mview_enable", &[])?
            .first()
            .and_then(|row| row.first())
            .map(crate::mview_refresh::datum_text)
            .unwrap_or_default();
        sys_sql(&mut sys, "SET @@SESSION.tidb_mview_enable = ON", &[])?;
        sys.vars.in_mview_maintenance = true;
        let outcome = sys_sql(&mut sys, "BEGIN", &[]).and_then(|_| {
            let start_ts = sys.node_conn().explicit_start_ts().filter(|&ts| ts != 0).ok_or_else(|| plain("invalid transaction start ts"))?;
            body(&mut sys, start_ts)
        });
        let _ = sys_sql(&mut sys, "ROLLBACK", &[]);
        sys.vars.in_mview_maintenance = false;
        let _ = sys_sql(&mut sys, "SET @@SESSION.tidb_mview_enable = %?", &[SqlArg::String(original.as_bytes())]);
        outcome
    }
}

/// Go master's base-table and log checks shared by SHOW CREATE
/// MATERIALIZED VIEW LOG (`ErrWrongObject` for a missing log) and SHOW
/// MATERIALIZED VIEW LOG ... WAIT_PURGE (a plain error).
fn materialized_view_log_target(
    catalog: &Catalog,
    schema: &str,
    name: &str,
    missing: MissingLog,
) -> Result<(TableInfo, TableInfo), DriverError> {
    if catalog.0.schema_by_name(schema).is_none() {
        return Err(database_not_exists(schema));
    }
    let base = catalog.table(schema, name).ok_or_else(|| table_not_exists(schema, name))?;
    let base_name = base.name.original().to_owned();
    if base.is_view() || base.is_sequence() || base.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(wrong_object(schema, &base_name, "BASE TABLE"));
    }
    let no_log = || match missing {
        MissingLog::WrongObject => wrong_object(schema, &base_name, "BASE TABLE WITH MATERIALIZED VIEW LOG"),
        MissingLog::Errorf => plain(format!("materialized view log does not exist for base table {schema}.{base_name}")),
    };
    let mlog_id = base.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
    if mlog_id == 0 {
        return Err(no_log());
    }
    let mlog = catalog.table_by_id(mlog_id).ok_or_else(no_log)?;
    if mlog.materialized_view_log.as_ref().is_none_or(|info| info.read().base_table_id != base.id) {
        let mlog_name = mlog.name.original();
        return Err(match missing {
            MissingLog::WrongObject => wrong_object(
                schema,
                mlog_name,
                &format!("MATERIALIZED VIEW LOG FOR BASE TABLE {schema}.{base_name}"),
            ),
            MissingLog::Errorf => plain(format!(
                "table {schema}.{mlog_name} is not a materialized view log for base table {schema}.{base_name}"
            )),
        });
    }
    Ok((base, mlog))
}
