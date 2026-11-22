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

//! Go master `mv_refresh_observability.go` and `mview_refresh_steps.go`:
//! `REFRESH MATERIALIZED VIEW ... DRY RUN` lists the refresh's steps and,
//! under each data-change step, the brief EXPLAIN of the statement it would
//! run, executing nothing.

use tidb_ast::{RefreshMaterializedViewMode, RefreshMaterializedViewStmt};
use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::DriverError;
use tidb_executor::sessionctx::StmtOutput;
use tidb_sqlescape::SqlArg;

use crate::mview_refresh::{
    annotate, apply_refresh_execution_session_vars, capture_refresh_execution_session_vars, plain, set_session_vars, sys_sql,
    validate_refresh_materialized_view_stmt,
};
use crate::Session;

/// Go master `mvRefreshObserveStepKind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StepKind {
    Meta,
    FastMerge,
    CompleteDelete,
    CompleteInsert,
    CompleteDeltaApply,
    OutOfPlaceCreateShadow,
    OutOfPlaceLoadShadow,
    OutOfPlaceCutover,
}

/// Go master `mvRefreshObserveStep`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ObserveStep {
    id: String,
    name: &'static str,
    kind: StepKind,
}

/// Go master `newMVRefreshStepSet(mode).steps`.
fn build_mv_refresh_observe_steps(mode: RefreshMaterializedViewMode) -> Vec<ObserveStep> {
    use RefreshMaterializedViewMode as Mode;
    let named: Vec<(&'static str, StepKind)> = match mode {
        Mode::CompleteOutOfPlace => vec![
            ("INSERT_HIST_RUNNING", StepKind::Meta),
            ("OUT_OF_PLACE_CREATE_SHADOW", StepKind::OutOfPlaceCreateShadow),
            ("OUT_OF_PLACE_LOAD_SHADOW", StepKind::OutOfPlaceLoadShadow),
            ("OUT_OF_PLACE_CUTOVER", StepKind::OutOfPlaceCutover),
            ("FINALIZE_HIST", StepKind::Meta),
        ],
        Mode::CompleteInPlace | Mode::Fast | Mode::CompleteDeltaApply => {
            let mut steps = vec![
                ("TXN_BEGIN", StepKind::Meta),
                ("LOCK_REFRESH_INFO", StepKind::Meta),
                ("INSERT_HIST_RUNNING", StepKind::Meta),
            ];
            match mode {
                Mode::Fast => steps.push(("DATA_CHANGE_FAST_MERGE", StepKind::FastMerge)),
                Mode::CompleteDeltaApply => steps.push(("DATA_CHANGE_COMPLETE_DELTA_APPLY", StepKind::CompleteDeltaApply)),
                _ => {
                    steps.push(("DATA_CHANGE_COMPLETE_DELETE", StepKind::CompleteDelete));
                    steps.push(("DATA_CHANGE_COMPLETE_INSERT", StepKind::CompleteInsert));
                }
            }
            steps.extend([
                ("PERSIST_REFRESH_INFO", StepKind::Meta),
                ("TXN_COMMIT", StepKind::Meta),
                ("FINALIZE_HIST", StepKind::Meta),
            ]);
            steps
        }
    };
    named
        .into_iter()
        .enumerate()
        .map(|(index, (name, kind))| ObserveStep { id: format!("S{:02}", index + 1), name, kind })
        .collect()
}

/// Go master `planIDSuffixPattern.ReplaceAllString(id, "")`: every `_<digits>`
/// ending at a word boundary removed.
fn strip_plan_id_suffixes(id: &str) -> String {
    let bytes = id.as_bytes();
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut out = String::with_capacity(id.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'_' {
            let digits = bytes[index + 1..].iter().take_while(|byte| byte.is_ascii_digit()).count();
            let end = index + 1 + digits;
            if digits > 0 && bytes.get(end).is_none_or(|&next| !word(next)) {
                index = end;
                continue;
            }
        }
        let ch = id[index..].chars().next().expect("a char boundary");
        out.push(ch);
        index += ch.len_utf8();
    }
    out
}

/// Go master `normalizeDryRunPlanField`.
fn normalize_dry_run_plan_field(value: &str) -> &str {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("N/A") {
        ""
    } else {
        value
    }
}

/// Go master `formatMVRefreshDryRunPlanRows`: `id | estRows:.. | task:.. |
/// access object | operator info`, the empty and `N/A` fields left out.
fn format_mv_refresh_dry_run_plan_rows(rows: &[Vec<String>]) -> Vec<String> {
    rows.iter()
        .filter(|row| !row.is_empty())
        .map(|row| {
            let mut parts = vec![strip_plan_id_suffixes(&row[0])];
            let est_rows = row.get(1).map_or("", |value| value.trim());
            let task = row.get(2).map_or("", |value| value.trim());
            let (access, info) = if row.len() >= 5 {
                (row[3].as_str(), row[4].as_str())
            } else if row.len() >= 4 {
                ("", row[row.len() - 1].as_str())
            } else {
                ("", "")
            };
            if !est_rows.is_empty() {
                parts.push(format!("estRows:{est_rows}"));
            }
            if !task.is_empty() {
                parts.push(format!("task:{task}"));
            }
            for field in [access, info] {
                let field = normalize_dry_run_plan_field(field);
                if !field.is_empty() {
                    parts.push(field.to_owned());
                }
            }
            format!("  {}", parts.join(" | "))
        })
        .collect()
}

/// Go `datum.ToString()` of an EXPLAIN cell, `<nil>` for NULL.
fn explain_cell(datum: &Datum) -> String {
    match datum {
        Datum::Null => "<nil>".to_owned(),
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        other => format!("{other:?}"),
    }
}

/// Go master `mvRefreshObserveStepRuntime`.
#[derive(Default)]
struct StepRuntime {
    started: Option<std::time::Instant>,
    ended: Option<std::time::Instant>,
    failed: bool,
    plan_rows: Vec<Vec<String>>,
}

/// Go master `mvRefreshObserveCollector`: each observed step's runtime, by
/// step name.
#[derive(Default)]
pub(crate) struct MvRefreshObserveCollector {
    steps: Vec<(&'static str, StepRuntime)>,
}

impl MvRefreshObserveCollector {
    fn step_mut(&mut self, name: &'static str) -> &mut StepRuntime {
        let index = match self.steps.iter().position(|(step, _)| *step == name) {
            Some(index) => index,
            None => {
                self.steps.push((name, StepRuntime::default()));
                self.steps.len() - 1
            }
        };
        &mut self.steps[index].1
    }

    fn step(&self, name: &str) -> Option<&StepRuntime> {
        self.steps.iter().find(|(step, _)| *step == name).map(|(_, runtime)| runtime)
    }
}

/// Go master `formatMVRefreshProfileStepHeader` and
/// `buildMVRefreshAnalyzeExecutionInfo`.
fn format_mv_refresh_profile_step_header(step: &ObserveStep, runtime: Option<&StepRuntime>) -> String {
    match runtime.and_then(|runtime| Some((runtime.started?, runtime.ended?, runtime.failed))) {
        Some((started, ended, failed)) => format!(
            "[{} {}] status:{}, time:{}",
            step.id,
            step.name,
            if failed { "failed" } else { "success" },
            tidb_execdetails::exec_details::format_go_duration(ended.saturating_duration_since(started))
        ),
        None => format!("[{} {}]", step.id, step.name),
    }
}

/// Go master `formatMVRefreshProfilePlanRows`: an analyzed plan row's
/// estimated and actual rows, task, access object, execution info, operator
/// info, memory and disk, the empty and `N/A` fields left out.
fn format_mv_refresh_profile_plan_rows(rows: &[Vec<String>]) -> Vec<String> {
    rows.iter()
        .filter(|row| !row.is_empty())
        .map(|row| {
            let field = |index: usize| row.get(index).map_or("", String::as_str);
            let mut parts = vec![strip_plan_id_suffixes(&row[0])];
            let operator_info = if row.len() > 6 {
                field(6)
            } else if row.len() > 1 {
                row[row.len() - 1].as_str()
            } else {
                ""
            };
            for (label, value) in [("estRows:", field(1).trim()), ("actRows:", field(2).trim()), ("task:", field(3).trim())] {
                if !value.is_empty() {
                    parts.push(format!("{label}{value}"));
                }
            }
            for value in [field(4), field(5), operator_info] {
                let value = normalize_dry_run_plan_field(value);
                if !value.is_empty() {
                    parts.push(value.to_owned());
                }
            }
            for (label, value) in [("memory:", field(7)), ("disk:", field(8))] {
                let value = normalize_dry_run_plan_field(value);
                if !value.is_empty() {
                    parts.push(format!("{label}{value}"));
                }
            }
            format!("  {}", parts.join(" | "))
        })
        .collect()
}

/// The `refresh steps` result of DRY RUN and WITH PROFILE.
fn refresh_steps_output(rows: Vec<String>) -> StmtOutput {
    let (schema_columns, names) = tidb_planner::show_schema::build_refresh_observe_schema();
    let columns = schema_columns
        .columns
        .iter()
        .zip(names)
        .map(|(column, name)| {
            (
                name.names.column.original.clone(),
                column.ret_type.clone().unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::String)),
            )
        })
        .collect();
    let rows = rows
        .into_iter()
        .map(|row| vec![Datum::String(tidb_datatype::StringDatum::new(row, tidb_charset::Collation::Utf8Mb4Bin))])
        .collect();
    StmtOutput::Rows { columns, rows }
}

impl Session {
    /// Go master `observeMVRefreshStep`: `run` timed under `step` when this
    /// refresh is profiled.
    pub(crate) fn observe_mv_refresh_step<T>(
        &self,
        step: &'static str,
        run: impl FnOnce() -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let Some(observer) = self.mv_refresh_observer.clone() else {
            return run();
        };
        let started = std::time::Instant::now();
        observer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).step_mut(step).started = Some(started);
        let result = run();
        let mut observer = observer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let runtime = observer.step_mut(step);
        runtime.ended = Some(std::time::Instant::now());
        runtime.failed = result.is_err();
        result
    }

    /// Go master `emitMVRefreshStepPlanRows`: the analyzed plan of the
    /// statement `session` just ran, recorded under `step`.
    pub(crate) fn emit_mv_refresh_step_plan_rows(&self, step: &'static str, session: &mut Session) {
        let Some(observer) = self.mv_refresh_observer.clone() else {
            return;
        };
        if let Some(rows) = session.last_statement_analyzed_plan_rows() {
            observer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).step_mut(step).plan_rows = rows;
        }
    }

    /// Go master `Explain{Analyze: true, RuntimeStatsColl}.RenderResult` over
    /// this session's last statement plan (`StmtCtx.GetPlan()`), outside an
    /// EXPLAIN statement: the operator info keeps its plan ids.
    pub(crate) fn last_statement_analyzed_plan_rows(&mut self) -> Option<Vec<Vec<String>>> {
        use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
        let (plan, runtime_stats_coll, statement_memory) = {
            let cell = self.process_plan_info.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (cell.plan.clone()?, cell.runtime_stats_coll.clone(), cell.statement_memory.clone())
        };
        let ctx = self
            .statement_context(false)
            .with_executor_runtime_stats_source(
                runtime_stats_coll
                    .and_then(|coll| coll.get::<std::sync::Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>())
                    .map(|coll| std::sync::Arc::clone(&*coll)),
            )
            .with_explain_target_memory(
                statement_memory.and_then(|memory| memory.get::<tidb_executor::StatementMemory>()).map(|memory| (*memory).clone()),
            );
        let physical = plan.get::<tidb_planner_core_operator_physicalop::PhysicalPlan>()?;
        let (_, rows) = self
            .with_catalog_mut(|catalog| {
                tidb_executor::explain_connection_plan(&physical, catalog, &ctx, tidb_sessionctx::explain_format::ExplainFormat::Row)
            })
            .ok()?;
        Some(rows.iter().map(|row| row.iter().map(explain_cell).collect()).collect())
    }

    /// Go master `RefreshMaterializedViewProfileExec.generateRows`: the
    /// refresh runs with a step collector, then each step's header with its
    /// status and time, and the analyzed plan of each data change.
    pub(crate) fn refresh_materialized_view_profile(
        &mut self,
        s: &RefreshMaterializedViewStmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        validate_refresh_materialized_view_stmt(s, self.vars.restricted_sql)?;
        let steps = build_mv_refresh_observe_steps(s.mode);
        let mut refresh_stmt = s.clone();
        refresh_stmt.observe_type = tidb_ast::RefreshMaterializedViewObserveType::None;
        let collector = std::sync::Arc::new(std::sync::Mutex::new(MvRefreshObserveCollector::default()));
        self.mv_refresh_observer = Some(std::sync::Arc::clone(&collector));
        let refreshed = self.refresh_materialized_view(&refresh_stmt, sql);
        self.mv_refresh_observer = None;
        refreshed?;
        let collector = collector.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut rows = Vec::with_capacity(steps.len() * 2);
        for step in &steps {
            let runtime = collector.step(step.name);
            rows.push(format_mv_refresh_profile_step_header(step, runtime));
            if let Some(runtime) = runtime {
                rows.extend(format_mv_refresh_profile_plan_rows(&runtime.plan_rows));
            }
        }
        Ok(refresh_steps_output(rows))
    }

    /// Go master `RefreshMaterializedViewDryRunExec.generateRows`.
    pub(crate) fn refresh_materialized_view_dry_run(&mut self, s: &RefreshMaterializedViewStmt) -> Result<StmtOutput, DriverError> {
        validate_refresh_materialized_view_stmt(s, self.vars.restricted_sql)?;
        let (schema, info) = self.resolve_refresh_materialized_view_target(s)?;
        let mview = info.materialized_view.as_ref().expect("resolved as a view").read().clone();
        self.check_refresh_materialized_view_base_table_select(&self.refresh_latest_info_schema(), &mview)?;
        let steps = build_mv_refresh_observe_steps(s.mode);
        let view_name = s.view_name.last().cloned().unwrap_or_default();
        let view_args = [SqlArg::String(schema.as_bytes()), SqlArg::String(view_name.as_bytes())];
        let escape = |sql: &str| tidb_sqlescape::escape_sql_text(sql, &view_args).map_err(|error| plain(error.to_string()));

        let (mut delete_rows, mut insert_rows, mut delta_rows, mut load_rows, mut fast_rows) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        match s.mode {
            // Go `buildOutOfPlaceRefreshPlanRows`: the load's statement shape
            // over the view itself, no shadow being created.
            RefreshMaterializedViewMode::CompleteOutOfPlace => {
                load_rows = self.render_mv_refresh_plan_rows_for_internal_sql(&format!(
                    "{}{}",
                    escape("INSERT INTO %n.%n ")?,
                    mview.sql_content
                ))?;
            }
            // Go `buildCompleteRefreshPlanRows`.
            RefreshMaterializedViewMode::CompleteInPlace => {
                delete_rows = self.render_mv_refresh_plan_rows_for_internal_sql(&escape("DELETE FROM %n.%n")?)?;
                insert_rows = self.render_mv_refresh_plan_rows_for_internal_sql(&format!(
                    "{}{}",
                    escape("INSERT INTO %n.%n ")?,
                    mview.sql_content
                ))?;
            }
            // Go `buildCompleteDeltaRefreshPlanRows`.
            RefreshMaterializedViewMode::CompleteDeltaApply => {
                delta_rows = self.render_mv_refresh_plan_rows_for_implement(s, ImplementWindow::default())?;
            }
            // Go `buildFastMergePlanRows`.
            RefreshMaterializedViewMode::Fast => {
                fast_rows = self.build_fast_merge_plan_rows(s, &info)?;
            }
        }

        let mut rows = Vec::with_capacity(steps.len() * 2);
        for step in steps {
            rows.push(format!("[{} {}]", step.id, step.name));
            let step_rows = match step.kind {
                StepKind::CompleteDelete => &delete_rows,
                StepKind::CompleteInsert => &insert_rows,
                StepKind::CompleteDeltaApply => &delta_rows,
                StepKind::FastMerge => &fast_rows,
                StepKind::OutOfPlaceLoadShadow => &load_rows,
                _ => continue,
            };
            rows.extend(format_mv_refresh_dry_run_plan_rows(step_rows));
        }
        Ok(refresh_steps_output(rows))
    }

    /// Go master `prepareMVRefreshObserveInternalSession`: a system session
    /// with the user's materialized view execution variables, materialized
    /// views on, maintenance in restricted SQL, and the user's redaction.
    fn open_mv_refresh_observe_internal_session(&self) -> Result<Session, DriverError> {
        let mut sys = self.open_refresh_system_session()?;
        // The observe session is discarded afterwards, which drops Go's
        // deferred restore.
        let _restore_execution_vars =
            apply_refresh_execution_session_vars(&mut sys, &capture_refresh_execution_session_vars(self), false)?;
        set_session_vars(&mut sys, &[(tidb_vardef::tidb_vars::TIDB_MVIEW_ENABLE.to_owned(), "ON".to_owned())])
            .map_err(|error| annotate(&error, "dry run refresh materialized view: failed to enable materialized view support"))?;
        // Go `setMVRefreshObserveRedaction`: plans follow the caller's mode.
        sys.vars.set_enable_redact_log(self.vars.enable_redact_log().to_owned());
        sys.vars.in_mview_maintenance = true;
        sys.vars.restricted_sql = true;
        Ok(sys)
    }

    /// Go master `renderPlanRowsForInternalSQL`: `EXPLAIN FORMAT='brief'` of
    /// `sql` on the observe session.
    fn render_mv_refresh_plan_rows_for_internal_sql(&self, sql: &str) -> Result<Vec<Vec<String>>, DriverError> {
        let mut sys = self.open_mv_refresh_observe_internal_session()?;
        let rows = sys_sql(&mut sys, &format!("EXPLAIN FORMAT='brief' {sql}"), &[])?;
        Ok(rows.iter().map(|row| row.iter().map(explain_cell).collect()).collect())
    }

    /// Go master `buildFastMergePlanRows` with
    /// `loadFastRefreshImplementOptionsForDryRun`: the window from the view's
    /// last successful read TSO to the AS OF target, if any.
    fn build_fast_merge_plan_rows(
        &mut self,
        s: &RefreshMaterializedViewStmt,
        info: &tidb_model::TableInfo,
    ) -> Result<Vec<Vec<String>>, DriverError> {
        let target = match &s.as_of {
            Some(as_of) => {
                let target = self.calculate_as_of_ts_expr(as_of)?;
                self.validate_snapshot_read_ts(target)?;
                target
            }
            None => 0,
        };
        let mut validation = self.open_refresh_system_session()?;
        let rows = sys_sql(
            &mut validation,
            "SELECT LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %?",
            &[SqlArg::Signed(info.id)],
        )
        .map_err(|error| {
            if crate::mview_refresh::is_table_not_exists(&error) {
                plain("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
            } else {
                error
            }
        })?;
        let Some(row) = rows.first() else {
            return Err(plain("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info"));
        };
        let Some(last) = row.first().and_then(crate::mview_refresh::datum_u64) else {
            return Err(plain("refresh materialized view fast: LAST_SUCCESS_READ_TSO is NULL"));
        };
        let mut window = ImplementWindow { last_successful_refresh_read_tso: last, ..ImplementWindow::default() };
        if target > 0 && target == last {
            return Ok(Vec::new());
        }
        window.mlog_retained_lower_tso = crate::mview_refresh::check_fast_refresh_mlog_integrity(
            &mut validation,
            &self.refresh_latest_info_schema(),
            info,
            last,
        )?;
        if target > 0 && target < last {
            return Err(plain(format!(
                "refresh materialized view fast as of timestamp: target tso {target} is older than LAST_SUCCESS_READ_TSO {last}"
            )));
        }
        window.target_refresh_read_tso = target;
        self.render_mv_refresh_plan_rows_for_implement(s, window)
    }

    /// Go master `renderPlanRowsForInternalStmt` over the internal
    /// `RefreshMaterializedViewImplementStmt`: its plan optimized and
    /// rendered as a brief EXPLAIN.
    fn render_mv_refresh_plan_rows_for_implement(
        &self,
        s: &RefreshMaterializedViewStmt,
        window: ImplementWindow,
    ) -> Result<Vec<Vec<String>>, DriverError> {
        let mut sys = self.open_mv_refresh_observe_internal_session()?;
        let mut refresh_stmt = s.clone();
        refresh_stmt.observe_type = tidb_ast::RefreshMaterializedViewObserveType::None;
        refresh_stmt.as_of = None;
        if refresh_stmt.view_name.len() == 1 {
            refresh_stmt.view_name.insert(0, self.current_database().to_owned());
        }
        let implement = tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::RefreshMaterializedViewImplement(Box::new(
            tidb_ast::RefreshMaterializedViewImplementStmt {
                refresh_stmt,
                last_successful_refresh_read_tso: window.last_successful_refresh_read_tso,
                target_refresh_read_tso: window.target_refresh_read_tso,
                mlog_retained_lower_tso: window.mlog_retained_lower_tso,
                registered_mlog_ids: Vec::new(),
            },
        ))));
        let Some((mut plan, _, ctx)) = sys.build_statement_plan(&implement)? else {
            return Err(plain("dry run refresh materialized view: invalid internal statement"));
        };
        // Go's builder activates the internal session's transaction
        // (`b.ctx.Txn(true)`); with no statement provider its start TS bounds
        // the retained mlog window.
        let start_ts = sys.node_conn().store_current_version().map_err(plain)?;
        sys.optimize_refresh_implement(&mut plan, &ctx, &|| Ok(start_ts))?;

        // Go renders `Explain{Format: brief}` outside an EXPLAIN statement:
        // the operator info keeps its plan ids (`StmtCtx.IgnoreExplainIDSuffix`
        // is unset) and the id column's are stripped by the formatter, which
        // the row format with ids gives once formatted.
        let format = tidb_sessionctx::explain_format::ExplainFormat::Row;
        let (_, rows) = sys.with_planning_catalog(|catalog| match &plan {
            tidb_planner::common_plans::Plan::MViewCompleteDeltaApply(apply) => {
                tidb_executor::explain_mview_complete_delta_apply(apply, catalog, &ctx, format)
            }
            tidb_planner::common_plans::Plan::MViewDeltaMerge(merge) => {
                tidb_executor::explain_mview_delta_merge(merge, catalog, &ctx, format)
            }
            _ => Err(plain("dry run refresh materialized view: unexpected internal plan")),
        })?;
        Ok(rows.iter().map(|row| row.iter().map(explain_cell).collect()).collect())
    }
}

/// Go master `refreshImplementOptions`.
#[derive(Clone, Copy, Debug, Default)]
struct ImplementWindow {
    last_successful_refresh_read_tso: u64,
    target_refresh_read_tso: u64,
    mlog_retained_lower_tso: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_as_go() {
        let names = |mode| build_mv_refresh_observe_steps(mode).into_iter().map(|step| format!("{} {}", step.id, step.name)).collect::<Vec<_>>();
        assert_eq!(
            names(RefreshMaterializedViewMode::CompleteOutOfPlace),
            ["S01 INSERT_HIST_RUNNING", "S02 OUT_OF_PLACE_CREATE_SHADOW", "S03 OUT_OF_PLACE_LOAD_SHADOW", "S04 OUT_OF_PLACE_CUTOVER", "S05 FINALIZE_HIST"]
        );
        assert_eq!(names(RefreshMaterializedViewMode::CompleteInPlace).len(), 8);
        assert_eq!(names(RefreshMaterializedViewMode::CompleteDeltaApply)[3], "S04 DATA_CHANGE_COMPLETE_DELTA_APPLY");
    }

    #[test]
    fn plan_id_suffixes_are_stripped_as_go() {
        assert_eq!(strip_plan_id_suffixes("└─HashAgg_15"), "└─HashAgg");
        assert_eq!(strip_plan_id_suffixes("├─HashAgg_15(Build)"), "├─HashAgg(Build)");
        assert_eq!(strip_plan_id_suffixes("TableReader_1x"), "TableReader_1x");
        assert_eq!(strip_plan_id_suffixes("a_1_2"), "a_1");
    }
}
