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


//! Go master `pkg/planner/core/util.go`'s materialized view guards:
//! `CheckMViewUpdatable`, `allowMViewMaintenanceBypass` and
//! `CheckMViewReadable`.

use tidb_exprctx::Columns;
use tidb_model::TableInfo;
use tidb_planner_core_base::PlanError;

/// The `SessionVars` fields Go master's materialized view guards read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MViewSessionVars {
    /// Go master `SessionVars.InMViewMaintenance`.
    pub in_mview_maintenance: bool,
    /// Go `SessionVars.InRestrictedSQL`.
    pub in_restricted_sql: bool,
    /// Go `SessionVars.User`'s `AuthUsername` and `AuthHostname`.
    pub auth_user: Option<(String, String)>,
}

impl MViewSessionVars {
    /// The statement's session, as its context answers.
    pub fn of(ctx: &(impl Columns + ?Sized)) -> Self {
        Self {
            in_mview_maintenance: ctx.in_mview_maintenance(),
            in_restricted_sql: ctx.in_restricted_sql(),
            auth_user: ctx.auth_user(),
        }
    }
}

/// Go master `CheckMViewUpdatable`: a DML operation (`op`) on a
/// materialized view, its log or its shadow table is refused unless the
/// session runs the view's maintenance.
///
/// # Errors
///
/// `ErrNonUpdatableTable` under `alias_name` (the table's own name when
/// empty), or `allowMViewMaintenanceBypass`'s.
pub fn check_mview_updatable(sv: &MViewSessionVars, table_info: &TableInfo, alias_name: &str, op: &str) -> Result<(), PlanError> {
    if table_info.materialized_view.is_none()
        && table_info.materialized_view_log.is_none()
        && table_info.materialized_view_shadow.is_none()
    {
        return Ok(());
    }
    if allow_mview_maintenance_bypass(sv)? {
        return Ok(());
    }
    let alias_name = if alias_name.is_empty() { table_info.name.original() } else { alias_name };
    Err(PlanError::from(
        tidb_plannererrors::ERR_NON_UPDATABLE_TABLE.gen_with_stack_by_args(&[alias_name.into(), op.into()]),
    ))
}

/// Go master `allowMViewMaintenanceBypass`.
///
/// # Errors
///
/// `ErrInternal` when the session claims maintenance outside restricted SQL.
pub fn allow_mview_maintenance_bypass(sv: &MViewSessionVars) -> Result<bool, PlanError> {
    if !sv.in_mview_maintenance {
        return Ok(false);
    }
    // All MV maintenance work should use internal sessions (restricted SQL).
    if !sv.in_restricted_sql {
        return Err(PlanError::from(
            tidb_plannererrors::ERR_INTERNAL
                .gen_with_stack("materialized view maintenance should only run in restricted SQL mode"),
        ));
    }
    Ok(true)
}

/// Go master `CheckMViewReadable`: a read of a shadow table is a user's
/// only during maintenance, and a view still building reads as its build
/// state's error.
///
/// # Errors
///
/// `ErrTableaccessDenied` (SELECT) on a shadow table, the build state's
/// access error, or `allowMViewMaintenanceBypass`'s.
pub fn check_mview_readable(sv: &MViewSessionVars, table_info: &TableInfo, alias_name: &str) -> Result<(), PlanError> {
    if table_info.materialized_view.is_none() && table_info.materialized_view_shadow.is_none() {
        return Ok(());
    }
    let alias_name = if alias_name.is_empty() { table_info.name.original() } else { alias_name };
    if table_info.materialized_view_shadow.is_some() {
        if allow_mview_maintenance_bypass(sv)? {
            return Ok(());
        }
        let Some((user, host)) = sv.auth_user.clone() else {
            return Ok(());
        };
        return Err(PlanError::from(tidb_plannererrors::ERR_TABLEACCESS_DENIED.gen_with_stack_by_args(&[
            "SELECT".into(),
            user.into(),
            host.into(),
            alias_name.into(),
        ])));
    }
    let info = table_info.materialized_view.as_ref().map(|info| info.read().clone());
    let state = tidb_model::table::MaterializedViewInfo::get_init_build_state(info.as_ref());
    if state.is_ready() {
        return Ok(());
    }
    if allow_mview_maintenance_bypass(sv)? {
        return Ok(());
    }
    Err(PlanError::from(tidb_terror::MysqlError::unknown(state.access_error_message(alias_name))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_model::table::{MViewInitBuildState, MaterializedViewInfo, MaterializedViewLogInfo, MaterializedViewShadowInfo};
    use tidb_model::GoShared;

    fn table(name: &str) -> TableInfo {
        TableInfo { name: tidb_ast::CiString::new(name), ..TableInfo::default() }
    }

    fn mview(name: &str, state: MViewInitBuildState) -> TableInfo {
        TableInfo {
            materialized_view: Some(GoShared::new(MaterializedViewInfo { init_build_state: state, ..Default::default() })),
            ..table(name)
        }
    }

    fn shadow() -> TableInfo {
        TableInfo {
            materialized_view_shadow: Some(GoShared::new(MaterializedViewShadowInfo { source_mview_id: 100 })),
            ..table("__mv_shadow_1")
        }
    }

    fn message(result: Result<(), PlanError>) -> String {
        result.expect_err("refused").to_string()
    }

    const NOT_RESTRICTED: &str = "materialized view maintenance should only run in restricted SQL mode";

    /// Go master `TestCheckMViewUpdatable`.
    #[test]
    fn check_mview_updatable_as_go() {
        let mut vars = MViewSessionVars::default();
        let mv = mview("mv", MViewInitBuildState::READY);
        let mlog = TableInfo {
            materialized_view_log: Some(GoShared::new(MaterializedViewLogInfo::default())),
            ..table("$mlog$t")
        };
        let shadow = shadow();
        assert!(check_mview_updatable(&vars, &table("t"), "", "INSERT").is_ok());
        assert_eq!(message(check_mview_updatable(&vars, &mv, "", "INSERT")), "The target table mv of the INSERT is not updatable");
        assert!(check_mview_updatable(&vars, &mlog, "", "INSERT").is_err());
        assert!(check_mview_updatable(&vars, &shadow, "m", "DELETE").is_err_and(|e| e.to_string().contains("table m of the DELETE")));

        vars.in_mview_maintenance = true;
        for target in [&mv, &mlog, &shadow] {
            assert!(message(check_mview_updatable(&vars, target, "", "INSERT")).contains(NOT_RESTRICTED));
        }
        vars.in_restricted_sql = true;
        for target in [&mv, &mlog, &shadow] {
            assert!(check_mview_updatable(&vars, target, "", "INSERT").is_ok());
        }
    }

    /// Go master `TestCheckMViewReadableShadow`.
    #[test]
    fn check_mview_readable_as_go() {
        let mut vars =
            MViewSessionVars { auth_user: Some(("u".to_owned(), "%".to_owned())), ..MViewSessionVars::default() };
        let building = mview("mv_building", MViewInitBuildState::BUILDING);
        let deferred = mview("mv_deferred", MViewInitBuildState::DEFERRED);
        let shadow = shadow();
        assert!(check_mview_readable(&vars, &table("t"), "").is_ok());
        assert!(check_mview_readable(&vars, &mview("mv_ready", MViewInitBuildState::READY), "").is_ok());
        assert!(message(check_mview_readable(&vars, &shadow, "")).contains("SELECT command denied"));
        assert!(message(check_mview_readable(&vars, &building, "")).contains("initial build is in progress"));
        assert!(message(check_mview_readable(&vars, &deferred, "")).contains("is not ready: initial build has not completed"));

        vars.in_mview_maintenance = true;
        for target in [&shadow, &building, &deferred] {
            assert!(message(check_mview_readable(&vars, target, "")).contains(NOT_RESTRICTED));
        }
        vars.in_restricted_sql = true;
        for target in [&shadow, &building, &deferred] {
            assert!(check_mview_readable(&vars, target, "").is_ok());
        }
    }
}
