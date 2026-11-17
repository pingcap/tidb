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

//! Go `pkg/expression/expropt/sessionvars.go`.

use std::any::Any;
use std::sync::Arc;

use super::{get_prop_provider, EvalPropContext, ExprOptError, RequireOptionalEvalProps};
use tidb_exprctx::{
    OptionalEvalPropDesc, OptionalEvalPropKey, OptionalEvalPropKeySet, OptionalEvalPropProvider,
};

/// boundary: Go `pkg/sessionctx/variable.SessionVars` — the session variable
/// bundle, which the session owns above this crate. The trait is the part of
/// it that builtins read and write through [`SessionVarsPropReader`]. Each
/// method's default is a zero-valued Go `SessionVars` that keeps nothing; a
/// session implements them all.
pub trait SessionVars: Any + Send + Sync {
    /// Go `SessionVars.User`: the login and the account it matched; `None`
    /// is a session with no user (an internal one).
    fn user(&self) -> Option<Arc<tidb_parser::auth::UserIdentity>> {
        None
    }
    /// Go `SessionVars.ActiveRoles`.
    fn active_roles(&self) -> Vec<Arc<tidb_parser::auth::RoleIdentity>> {
        Vec::new()
    }
    /// Go `SessionVars.ConnectionID`; 0 for a session with no connection.
    fn connection_id(&self) -> u64 {
        0
    }
    /// Go `SessionVars.LastFoundRows`: the rows the previous query found.
    fn last_found_rows(&self) -> u64 {
        0
    }
    /// Go `SessionVars.StmtCtx.PrevAffectedRows`.
    fn prev_affected_rows(&self) -> i64 {
        0
    }
    /// Go `SessionVars.StmtCtx.PrevLastInsertID`.
    fn prev_last_insert_id(&self) -> u64 {
        0
    }
    /// Go `SessionVars.SetLastInsertID`, which `LAST_INSERT_ID(expr)` calls.
    fn set_last_insert_id(&self, id: u64) {
        let _ = id;
    }
    /// Go `getHintResourceGroupName(vars)`: the statement's resource group,
    /// a `RESOURCE_GROUP()` hint's first.
    fn hint_resource_group_name(&self) -> String {
        String::new()
    }
    /// Go `SessionVars.SetUserVarVal` (and `SetStringUserVar`, whose
    /// collation the datum carries), which `SET @v := expr` calls; the name
    /// as given (Go's callers lowercase it).
    fn set_user_var_val(&self, name: &str, value: tidb_datatype::Datum) {
        let _ = (name, value);
    }
    /// Go `SessionVars.SQLKiller`, which `SLEEP` polls.
    fn sql_killer(&self) -> Option<Arc<tidb_sqlkiller::SqlKiller>> {
        None
    }
    /// Go `len(SessionVars.StmtCtx.TableIDs) != 0`: the statement reads a
    /// table.
    fn stmt_has_table_ids(&self) -> bool {
        false
    }
    /// Go `StmtCtx.InInsertStmt || InUpdateStmt || InDeleteStmt`.
    fn stmt_in_insert_update_or_delete(&self) -> bool {
        false
    }
    /// Go `GetSessionOrGlobalSystemVar("tidb_current_ts")`: the active
    /// transaction's start timestamp, 0 outside one.
    fn current_ts(&self) -> i64 {
        0
    }
    /// Go `SessionVars.CurrInsertValues` as `VALUES(col)` reads it: `None`
    /// for an empty row (outside `INSERT ... ON DUPLICATE KEY UPDATE`), else
    /// the row's length and its value at `offset` when the row has one.
    fn curr_insert_values_at(&self, offset: usize) -> Option<(usize, Option<tidb_datatype::Datum>)> {
        let _ = offset;
        None
    }
}

/// boundary: Go `pkg/sessionctx/variable.SessionVarsProvider` — anything that
/// can hand back the session variables.
pub trait SessionVarsProvider: Send + Sync {
    /// Go `SessionVarsProvider.GetSessionVars`.
    fn get_session_vars(&self) -> Arc<dyn SessionVars>;
}

/// Go `SessionVarsPropProvider`.
pub struct SessionVarsPropProvider {
    vars: Arc<dyn SessionVarsProvider>,
}

/// Go `NewSessionVarsProvider`.
///
/// Go asserts the argument is non-nil; `Arc` is non-nullable, so the assertion
/// holds by construction.
#[must_use]
pub fn new_session_vars_provider(
    provider: Arc<dyn SessionVarsProvider>,
) -> SessionVarsPropProvider {
    SessionVarsPropProvider { vars: provider }
}

impl SessionVarsPropProvider {
    /// The wrapped provider, Go's unexported `vars` field.
    #[must_use]
    pub fn vars(&self) -> &Arc<dyn SessionVarsProvider> {
        &self.vars
    }
}

impl OptionalEvalPropProvider for SessionVarsPropProvider {
    fn desc(&self) -> &'static OptionalEvalPropDesc {
        OptionalEvalPropKey::SessionVars.desc()
    }
}

/// Go `SessionVarsPropReader`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionVarsPropReader;

impl RequireOptionalEvalProps for SessionVarsPropReader {
    fn required_optional_eval_props(&self) -> OptionalEvalPropKeySet {
        OptionalEvalPropKey::SessionVars.as_prop_key_set()
    }
}

impl SessionVarsPropReader {
    /// Go `SessionVarsPropReader.GetSessionVars`.
    ///
    /// Go additionally runs `exprctx.AssertLocationWithSessionVars` under the
    /// `intest` build tag; that helper lives in the not-yet-ported half of
    /// `exprctx`, and it is an assertion only, with no release-build effect.
    pub fn get_session_vars(
        self,
        ctx: &(impl EvalPropContext + ?Sized),
    ) -> Result<Arc<dyn SessionVars>, ExprOptError> {
        let provider =
            get_prop_provider::<SessionVarsPropProvider>(ctx, OptionalEvalPropKey::SessionVars)?;
        Ok(provider.vars.get_session_vars())
    }
}
