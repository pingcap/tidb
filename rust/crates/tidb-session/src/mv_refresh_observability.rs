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

use crate::mview_refresh::{plain, refresh_execution_session_vars, set_session_vars, sys_sql, validate_refresh_materialized_view_stmt};
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

impl Session {
    /// Go master `RefreshMaterializedViewDryRunExec.generateRows`.
    pub(crate) fn refresh_materialized_view_dry_run(&mut self, s: &RefreshMaterializedViewStmt) -> Result<StmtOutput, DriverError> {
        validate_refresh_materialized_view_stmt(s)?;
        let (schema, info) = self.resolve_refresh_materialized_view_target(s)?;
        let mview = info.materialized_view.as_ref().expect("resolved as a view").read().clone();
        self.check_refresh_materialized_view_base_table_select(&self.refresh_latest_info_schema(), &mview)?;
        let steps = build_mv_refresh_observe_steps(s.mode);
        let view_name = s.view_name.last().cloned().unwrap_or_default();
        let view_args = [SqlArg::String(schema.as_bytes()), SqlArg::String(view_name.as_bytes())];
        let escape = |sql: &str| tidb_sqlescape::escape_sql_text(sql, &view_args).map_err(|error| plain(error.to_string()));

        let (mut delete_rows, mut insert_rows, mut delta_rows, mut load_rows) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
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
                delta_rows = self.render_mv_refresh_plan_rows_for_implement(s)?;
            }
            RefreshMaterializedViewMode::Fast => {
                return Err(DriverError::unsupported("REFRESH MATERIALIZED VIEW FAST DRY RUN on this node"));
            }
        }

        let mut rows = Vec::with_capacity(steps.len() * 2);
        for step in steps {
            rows.push(format!("[{} {}]", step.id, step.name));
            let step_rows = match step.kind {
                StepKind::CompleteDelete => &delete_rows,
                StepKind::CompleteInsert => &insert_rows,
                StepKind::CompleteDeltaApply => &delta_rows,
                StepKind::OutOfPlaceLoadShadow => &load_rows,
                _ => continue,
            };
            rows.extend(format_mv_refresh_dry_run_plan_rows(step_rows));
        }
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
        Ok(StmtOutput::Rows { columns, rows })
    }

    /// Go master `prepareMVRefreshObserveInternalSession`: a system session
    /// with the user's materialized view execution variables, materialized
    /// views on, maintenance in restricted SQL, and the user's redaction.
    fn open_mv_refresh_observe_internal_session(&self) -> Result<Session, DriverError> {
        let mut sys = self.open_refresh_system_session()?;
        let mut vars = refresh_execution_session_vars(self);
        vars.push((tidb_vardef::tidb_vars::TIDB_MVIEW_ENABLE.to_owned(), "ON".to_owned()));
        set_session_vars(&mut sys, &vars)?;
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

    /// Go master `renderPlanRowsForInternalStmt` over the internal
    /// `RefreshMaterializedViewImplementStmt`: its plan optimized and
    /// rendered as a brief EXPLAIN.
    fn render_mv_refresh_plan_rows_for_implement(&self, s: &RefreshMaterializedViewStmt) -> Result<Vec<Vec<String>>, DriverError> {
        let sys = self.open_mv_refresh_observe_internal_session()?;
        let mut refresh_stmt = s.clone();
        refresh_stmt.observe_type = tidb_ast::RefreshMaterializedViewObserveType::None;
        if refresh_stmt.view_name.len() == 1 {
            refresh_stmt.view_name.insert(0, self.current_database().to_owned());
        }
        let implement = tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::RefreshMaterializedViewImplement(Box::new(
            tidb_ast::RefreshMaterializedViewImplementStmt {
                refresh_stmt,
                last_successful_refresh_read_tso: 0,
                target_refresh_read_tso: 0,
                mlog_retained_lower_tso: 0,
                registered_mlog_ids: Vec::new(),
            },
        ))));
        let Some((plan, _, ctx)) = sys.build_statement_plan(&implement)? else {
            return Err(plain("dry run refresh materialized view: invalid internal statement"));
        };
        let tidb_planner::common_plans::Plan::MViewCompleteDeltaApply(apply) = plan else {
            return Err(plain("dry run refresh materialized view: unexpected internal plan"));
        };
        let current_db = sys.current_database().to_owned();
        // Go renders `Explain{Format: brief}` outside an EXPLAIN statement:
        // the operator info keeps its plan ids (`StmtCtx.IgnoreExplainIDSuffix`
        // is unset) and the id column's are stripped by the formatter, which
        // the row format with ids gives once formatted.
        let (_, rows) = sys.with_planning_catalog(|catalog| {
            tidb_executor::explain_mview_complete_delta_apply(&apply, catalog, &current_db, &ctx, tidb_sessionctx::explain_format::ExplainFormat::Row)
        })?;
        Ok(rows.iter().map(|row| row.iter().map(explain_cell).collect()).collect())
    }
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
