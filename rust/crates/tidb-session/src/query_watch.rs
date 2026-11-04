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

//! Go `pkg/executor/internal/querywatch`: `QUERY WATCH ADD` (`AddExecutor`)
//! and `QUERY WATCH REMOVE` (`ExecDropQueryWatch`) over the domain's runaway
//! manager.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_driver_error::DriverError;
use tidb_executor::StmtOutput;
use tidb_proto::resource_manager as rmpb;
use tidb_resourcegroup_runaway as runaway;
use tidb_variable::user_vars::UserVarsReader as _;

use crate::Session;

fn unknown(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message.into()))
}

impl Session {
    /// Go `domain.GetDomain(sctx).RunawayManager()`.
    fn runaway_manager(&self) -> Result<std::sync::Arc<runaway::Manager>, DriverError> {
        self.domain_services()
            .and_then(|services| services.domain.upgrade())
            .and_then(|domain| domain.runaway_manager())
            .ok_or_else(|| unknown("runaway manager is not available"))
    }

    /// Go `AddExecutor.Next`: the watch's ID in `WATCH_ID`.
    pub(crate) fn add_query_watch(&mut self, stmt: &tidb_ast::AddQueryWatchStmt) -> Result<StmtOutput, DriverError> {
        let manager = self.runaway_manager()?;
        let mut record = self.from_query_watch_option_list(&stmt.options)?;
        validate_watch_record(&mut record, manager.resource_group_ctl.as_ref())?;
        let id = manager.add_runaway_watch(&record).map_err(unknown)?;
        Ok(StmtOutput::Rows {
            columns: vec![("WATCH_ID".to_owned(), FieldType::new(FieldTypeCode::LongLong))],
            rows: vec![vec![Datum::UInt(id)]],
        })
    }

    /// Go `fromQueryWatchOptionList`.
    fn from_query_watch_option_list(
        &mut self,
        options: &[tidb_ast::QueryWatchOption],
    ) -> Result<runaway::QuarantineRecord, DriverError> {
        let mut record = runaway::QuarantineRecord {
            source: runaway::MANUAL_SOURCE.to_owned(),
            start_time: chrono::Utc::now(),
            end_time: runaway::null_time(),
            exceed_cause: "None".to_owned(),
            ..runaway::QuarantineRecord::default()
        };
        for option in options {
            self.set_watch_option(&mut record, option)?;
        }
        Ok(record)
    }

    /// Go `RewriteAstExprWithPlanCtx` + `EvalString` of a constant
    /// expression; `None` for NULL.
    fn eval_watch_string(&mut self, expr: &tidb_ast::Expr) -> Result<Option<String>, DriverError> {
        match self.eval_value(expr)? {
            Datum::Null => Ok(None),
            datum => datum.sql_string().map(Some).map_err(|error| unknown(error.to_string())),
        }
    }

    /// Go `setWatchOption`.
    fn set_watch_option(
        &mut self,
        record: &mut runaway::QuarantineRecord,
        option: &tidb_ast::QueryWatchOption,
    ) -> Result<(), DriverError> {
        match option {
            tidb_ast::QueryWatchOption::ResourceGroupExpr(expr) => {
                record.resource_group_name = self
                    .eval_watch_string(expr)?
                    .ok_or_else(|| unknown("invalid resource group name expression"))?;
            }
            // Go `GroupNameStr.L`.
            tidb_ast::QueryWatchOption::ResourceGroup(name) => {
                record.resource_group_name = tidb_mysql::to_lowercase(name);
            }
            tidb_ast::QueryWatchOption::Action(action) => {
                (record.action, record.switch_group_name) = match action {
                    tidb_ast::ResourceGroupRunawayAction::DryRun => (rmpb::RunawayAction::DryRun, String::new()),
                    tidb_ast::ResourceGroupRunawayAction::Cooldown => (rmpb::RunawayAction::CoolDown, String::new()),
                    tidb_ast::ResourceGroupRunawayAction::Kill => (rmpb::RunawayAction::Kill, String::new()),
                    tidb_ast::ResourceGroupRunawayAction::SwitchGroup(name) => {
                        (rmpb::RunawayAction::SwitchGroup, name.clone())
                    }
                };
            }
            tidb_ast::QueryWatchOption::Text(text) => {
                let strval = self
                    .eval_watch_string(&text.pattern)?
                    .ok_or_else(|| unknown("invalid watch text expression"))?;
                record.watch = match text.watch_type {
                    tidb_ast::RunawayWatchType::Exact => rmpb::RunawayWatchType::Exact,
                    tidb_ast::RunawayWatchType::Similar => rmpb::RunawayWatchType::Similar,
                    tidb_ast::RunawayWatchType::Plan => rmpb::RunawayWatchType::Plan,
                    tidb_ast::RunawayWatchType::None | tidb_ast::RunawayWatchType::Unknown(_) => {
                        rmpb::RunawayWatchType::NoneWatch
                    }
                };
                if text.type_specified {
                    let stmts = tidb_parser::parse_multi(&strval)
                        .map_err(|error| unknown(error.compatibility_message(&strval)))?;
                    if stmts.len() != 1 {
                        return Err(unknown("only support one SQL"));
                    }
                    let sql = String::from_utf8_lossy(stmts[0].text()).into_owned();
                    record.watch_text = match text.watch_type {
                        tidb_ast::RunawayWatchType::Exact => sql,
                        tidb_ast::RunawayWatchType::Similar => {
                            tidb_parser::normalize_digest(&sql).1.as_str().to_owned()
                        }
                        tidb_ast::RunawayWatchType::Plan => self.plan_digest_of(&sql)?,
                        tidb_ast::RunawayWatchType::None | tidb_ast::RunawayWatchType::Unknown(_) => {
                            return Err(unknown("watch type must be specified"))
                        }
                    };
                } else {
                    if strval.len() != 64 {
                        return Err(unknown("digest format error"));
                    }
                    record.watch_text = strval;
                }
            }
        }
        Ok(())
    }

    /// Go `newSctx.GetSQLExecutor().ExecuteInternal("explain " + sql)` then
    /// `newSctx.GetSessionVars().StmtCtx.GetPlanDigest()`, on a system
    /// session of its own.
    fn plan_digest_of(&self, sql: &str) -> Result<String, DriverError> {
        use crate::domain::cluster_session_node::DomainSessions as _;
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| unknown("no system session to explain the statement"))?;
        let mut session = domain.open_system_session().map_err(|error| unknown(error.error_string()))?;
        session.run(&format!("explain {sql}"))?;
        let digest = session
            .process_plan_info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .plan_digest
            .clone();
        if digest.is_empty() {
            return Err(unknown("no plan digest"));
        }
        Ok(digest)
    }

    /// Go `ExecDropQueryWatch`.
    pub(crate) fn drop_query_watch(&mut self, stmt: &tidb_ast::DropQueryWatchStmt) -> Result<(), DriverError> {
        let manager = self.runaway_manager()?;
        match &stmt.target {
            tidb_ast::QueryWatchRemoveTarget::ResourceGroup(name) => {
                manager.remove_runaway_resource_group_watch(name).map_err(unknown)
            }
            tidb_ast::QueryWatchRemoveTarget::ResourceGroupExpr(expr) => {
                let tidb_ast::Expr::UserVar(name) = expr else {
                    return Err(unknown("invalid group name variable"));
                };
                match self.user_vars.get_user_var_val(name).map(|value| value.sql_string()) {
                    Some(Ok(group_name)) => manager.remove_runaway_resource_group_watch(&group_name).map_err(unknown),
                    _ => Err(unknown("invalid group name variable")),
                }
            }
            tidb_ast::QueryWatchRemoveTarget::Id(id) => manager.remove_runaway_watch(*id).map_err(unknown),
        }
    }
}

/// Go `validateWatchRecord`: no group is the default group; no action takes
/// the group's runaway action, which the group must then have.
fn validate_watch_record(
    record: &mut runaway::QuarantineRecord,
    client: &dyn runaway::ResourceGroupsController,
) -> Result<(), DriverError> {
    if record.resource_group_name.is_empty() {
        record.resource_group_name = tidb_model::DEFAULT_RESOURCE_GROUP_NAME.to_owned();
    }
    let rg = client.get_resource_group(&record.resource_group_name).map_err(unknown)?.ok_or_else(|| {
        DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_NOT_EXISTS
                .gen_with_stack_by_args(&[record.resource_group_name.clone().into()]),
        )
    })?;
    if record.action == rmpb::RunawayAction::NoneAction {
        let Some(settings) = rg.runaway_settings else {
            return Err(unknown(format!(
                "must set runaway config for resource group `{}`",
                record.resource_group_name
            )));
        };
        record.action = rmpb::RunawayAction::try_from(settings.action).unwrap_or(rmpb::RunawayAction::NoneAction);
        record.switch_group_name = settings.switch_group_name;
    }
    if record.watch == rmpb::RunawayWatchType::NoneWatch {
        return Err(unknown("must specify watch type"));
    }
    Ok(())
}

/// Go `memtableRetriever.setDataFromRunawayWatches`: the watch list, synced
/// first.
pub(crate) struct RunawayWatchesRetriever(pub(crate) Option<std::sync::Arc<runaway::Manager>>);

impl tidb_executor::mem_table_retrieval::MemTableRetriever for RunawayWatchesRetriever {
    fn serves(&self, read: &tidb_executor::mem_table_retrieval::MemTableRead) -> bool {
        use tidb_hack::GoCaseCompare as _;
        read.db_name.go_lower_eq(tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA)
            && read.table_name.go_lower_eq("RUNAWAY_WATCHES")
    }

    fn retrieve(
        &self,
        _read: &tidb_executor::mem_table_retrieval::MemTableRead,
        _catalog: &tidb_infoschema::SessionExtendedInfoSchema,
        _ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let Some(manager) = &self.0 else {
            return Ok(Vec::new());
        };
        if let Err(error) = manager.update_new_and_done_watch() {
            tidb_logutil::bg_logger().warn(
                "read runaway watch list",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
            );
        }
        let text = |value: String| Datum::String(tidb_datatype::StringDatum::new(value.as_str(), tidb_charset::Collation::Utf8Mb4Bin));
        // Go `time.DateTime`.
        let date_time = |at: chrono::DateTime<chrono::Utc>| at.format("%Y-%m-%d %H:%M:%S").to_string();
        Ok(manager
            .get_watch_list()
            .into_iter()
            .map(|watch| {
                vec![
                    Datum::Int(watch.id),
                    text(watch.resource_group_name.clone()),
                    text(date_time(watch.start_time)),
                    if watch.end_time == runaway::null_time() {
                        text("UNLIMITED".to_owned())
                    } else {
                        text(date_time(watch.end_time))
                    },
                    text(watch.watch.as_str_name().to_owned()),
                    text(watch.watch_text.clone()),
                    text(watch.source.clone()),
                    text(watch.get_action_string()),
                    text(watch.get_exceed_cause().to_owned()),
                ]
            })
            .collect())
    }
}
