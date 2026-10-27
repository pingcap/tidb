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

//! Go `pkg/executor/set.go`: `SetExecutor`, which applies a planned `SET`
//! (Go `core.Set`) to the session in source order.

use tidb_hack::go_to_lower;
use tidb_planner::common_plans::{Set, VarAssignment};
use tidb_variable::sysvar::{self, SysVarDef};
use tidb_variable::VarError;

use crate::sessionctx::SessionContext;
use crate::{DriverError, ExecError, MysqlError, StmtContext};

/// Go `SetExecutor.Next`: each assignment in order -- `NAMES` / `CHARSET`,
/// a user variable, or a system variable. A later assignment observes the
/// earlier writes, and an error keeps them.
///
/// # Errors
///
/// The first assignment's evaluation, validation or privilege failure.
pub fn execute_set(
    ctx: &mut dyn SessionContext,
    stmt_ctx: &StmtContext,
    plan: &Set,
) -> Result<(), DriverError> {
    for v in &plan.var_assigns {
        // Variable is case insensitive, we use lower case.
        if v.name == tidb_ast::SET_NAMES || v.name == tidb_ast::SET_CHARSET {
            // This is set charset stmt.
            let is_set_names = v.name == tidb_ast::SET_NAMES;
            if v.is_default {
                set_charset(ctx, tidb_mysql::DefaultCharset, None, is_set_names)?;
                continue;
            }
            let dt = eval(v, stmt_ctx)?;
            let cs = datum_string(&dt)?;
            set_charset(ctx, &cs, v.extend_value.as_deref(), is_set_names)?;
            continue;
        }
        let name = go_to_lower(v.name.as_str());
        if !v.is_system {
            // Set user variable.
            let value = eval(v, stmt_ctx)?;
            let user_vars = ctx.user_vars();
            if value.is_null() {
                user_vars.unset_user_var(&name);
            } else {
                user_vars.set_user_var_val(&name, value);
                if let Some(field_type) = v.expr.as_ref().and_then(|expr| expr.static_type()) {
                    user_vars.set_user_var_type(&name, field_type.clone());
                }
            }
            continue;
        }
        set_sys_variable(ctx, stmt_ctx, &name, v)?;
    }
    Ok(())
}

/// Go `SetExecutor.setSysVariable`.
fn set_sys_variable(
    ctx: &mut dyn SessionContext,
    stmt_ctx: &StmtContext,
    name: &str,
    v: &VarAssignment,
) -> Result<(), DriverError> {
    let Some(sys_var) = sysvar::get_sys_var(name) else {
        if sysvar::removed_sys_var_reason(name).is_some() {
            // Removed vars permit parse-but-ignore.
            return Ok(());
        }
        return Err(VarError::UnknownSystemVariable(name.to_owned()).into());
    };
    // Go `pkg/executor/set.go:124`.
    if let Some(require_dynamic_privileges) = sys_var.require_dynamic_privileges() {
        let sem_enabled = tidb_util::sem_compat::is_enabled();
        let pm = ctx.privilege_manager();
        for privilege in require_dynamic_privileges(v.is_global, sem_enabled) {
            if !pm.request_dynamic_verification(privilege, false) {
                let msg = if sem_enabled {
                    privilege.to_owned()
                } else {
                    format!("SUPER or {privilege}")
                };
                return Err(DriverError::SpecificAccessDenied(msg));
            }
        }
    }
    // Check read-only system variables in SEM mode.
    if tidb_util::sem_v2::is_enabled()
        && tidb_util::sem_v2::is_read_only_variable(name)
        && !ctx
            .privilege_manager()
            .request_dynamic_verification("RESTRICTED_VARIABLES_ADMIN", false)
    {
        return Err(DriverError::SpecificAccessDenied(
            "RESTRICTED_VARIABLES_ADMIN".to_owned(),
        ));
    }
    if sys_var.is_noop() && !ctx.session_vars().noop_variables_enabled() {
        // The variable is a noop. For compatibility we allow it to still be
        // changed, but we append a warning since users might be expecting
        // something that's not going to happen (Go `ErrSettingNoopVariable`).
        ctx.append_sql_warning(tidb_util::context::SqlWarn::of(
            tidb_util::context::WarningLevel::Warning,
            &tidb_util::dbterror::exeerrors::ERR_SETTING_NOOP_VARIABLE,
            format!("setting {} has no effect in TiDB", sys_var.name),
        ));
    }
    let mut is_instance = v.is_instance;
    if sys_var.has_instance_scope() && !v.is_global && enable_legacy_instance_scope(ctx) {
        // For backward compatibility we change the assignment to INSTANCE
        // and warn that this will not be supported in future
        // (Go `ErrInstanceScope`).
        is_instance = true;
        ctx.append_sql_warning(tidb_util::context::SqlWarn::of(
            tidb_util::context::WarningLevel::Warning,
            &tidb_util::dbterror::exeerrors::ERR_INSTANCE_SCOPE,
            format!( "modifying {} will require SET GLOBAL in a future version of TiDB", sys_var.name ),
        ));
    }

    if v.is_global || is_instance {
        let val_str = get_var_value(ctx, stmt_ctx, v, Some(sys_var))?;
        if v.is_global {
            ctx.set_global_sys_var(name, val_str)?;
        } else {
            ctx.set_instance_sys_var(name, val_str)?;
        }
        return Ok(());
    }
    // Set session variable.
    let val_str = get_var_value(ctx, stmt_ctx, v, None)?;
    let is_txn_read_ts = name == tidb_vardef::tidb_vars::TIDB_TXN_READ_TS;
    let is_snapshot = name == tidb_vardef::tidb_vars::TIDB_SNAPSHOT;
    let old_snapshot_ts = if is_snapshot {
        ctx.session_vars().snapshot_ts()
    } else if is_txn_read_ts {
        ctx.session_vars().txn_read_ts()
    } else {
        0
    };
    if ctx.in_txn()
        && (name == "tx_isolation_one_shot"
            || is_txn_read_ts
            || (is_snapshot && ctx.txn_is_staleness()))
    {
        return Err(cant_change_tx_characteristics());
    }
    ctx.set_system_var(name, val_str)?;
    // Go `ValidateSnapshotReadTS`, `gcutil.ValidateSnapshot` and
    // `loadSnapshotInfoSchemaIfNeeded`, falling back to the old timestamp on
    // failure.
    if is_snapshot || is_txn_read_ts {
        ctx.load_snapshot_info_schema_if_needed(old_snapshot_ts, is_txn_read_ts)?;
    }
    Ok(())
}

/// Go `SessionVars.EnableLegacyInstanceScope`, fed by
/// `tidb_enable_legacy_instance_scope`.
fn enable_legacy_instance_scope(ctx: &dyn SessionContext) -> bool {
    ctx.session_vars()
        .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_LEGACY_INSTANCE_SCOPE)
        .is_ok_and(|value| value.eq_ignore_ascii_case("ON"))
}

/// Go `exeerrors.ErrCantChangeTxCharacteristics`.
fn cant_change_tx_characteristics() -> DriverError {
    DriverError::Mysql(MysqlError::terror(
        &tidb_util::dbterror::exeerrors::ERR_CANT_CHANGE_TX_CHARACTERISTICS,
        "Transaction characteristics can't be changed while a transaction is in progress",
    ))
}

/// Go `SetExecutor.getVarValue`.
fn get_var_value(
    ctx: &dyn SessionContext,
    stmt_ctx: &StmtContext,
    v: &VarAssignment,
    sys_var: Option<&SysVarDef>,
) -> Result<String, DriverError> {
    if v.is_default {
        // To set a SESSION variable to the GLOBAL value or a GLOBAL value to
        // the compiled-in MySQL default value, use the DEFAULT keyword.
        if let Some(sys_var) = sys_var {
            return Ok(tidb_variable::vars::global_system_variable_initial_value(
                sys_var,
            ));
        }
        return Ok(ctx
            .session_vars()
            .global_value_for_session_default(&v.name)?);
    }
    let native_val = eval(v, stmt_ctx)?;
    if native_val.is_null() {
        return Ok(String::new());
    }
    datum_string(&native_val)
}

/// Go `v.Expr.Eval(evalCtx, chunk.Row{})`.
fn eval(v: &VarAssignment, stmt_ctx: &StmtContext) -> Result<tidb_datatype::Datum, DriverError> {
    let expr = v
        .expr
        .as_ref()
        .ok_or_else(|| DriverError::unsupported("a SET value without an expression"))?;
    expr.eval(stmt_ctx, tidb_chunk::row::Row::empty())
        .map_err(|error| DriverError::Exec(ExecError::Eval(error)))
}

/// Go `Datum.ToString`.
fn datum_string(datum: &tidb_datatype::Datum) -> Result<String, DriverError> {
    let bytes = datum
        .sql_bytes()
        .map_err(|error| DriverError::unsupported(error.to_string()))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Go `SetExecutor.setCharset`.
fn set_charset(
    ctx: &mut dyn SessionContext,
    cs: &str,
    co: Option<&str>,
    is_set_names: bool,
) -> Result<(), DriverError> {
    ctx.session_vars_mut()
        .set_charset(cs, co, is_set_names)
        .map_err(DriverError::from)?;
    Ok(())
}
