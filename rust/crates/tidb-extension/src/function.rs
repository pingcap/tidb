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


//! Go `pkg/extension/function.go`.

use std::sync::Arc;

use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType};
use tidb_exprctx::EvalError;
use tidb_parser::auth::{RoleIdentity, UserIdentity};

/// Go `FunctionContext`: what a custom function reads of the statement
/// evaluating it.
pub trait FunctionContext {
    /// Go `FunctionContext.ConnectionInfo`.
    fn connection_info(
        &self,
    ) -> Option<std::sync::Arc<tidb_variable::connection_info::ConnectionInfo>>;

    /// Go `User()`: the session's user, `None` for a session with none.
    fn user(&self) -> Option<UserIdentity>;
    /// Go `ActiveRoles()`.
    fn active_roles(&self) -> Vec<RoleIdentity>;
    /// Go `CurrentDB()`.
    fn current_db(&self) -> String;
    /// Go `EvalArgs(row)`: the function's arguments evaluated over `row`.
    ///
    /// # Errors
    ///
    /// The first argument's evaluation error.
    fn eval_args(&self, row: Row<'_>) -> Result<Vec<Datum>, EvalError>;
}

/// Go `FunctionDef.EvalStringFunc`: the value and whether it is NULL.
pub type EvalStringFunc =
    Arc<dyn Fn(&dyn FunctionContext, Row<'_>) -> Result<(String, bool), EvalError> + Send + Sync>;

/// Go `FunctionDef.EvalIntFunc`: the value and whether it is NULL.
pub type EvalIntFunc =
    Arc<dyn Fn(&dyn FunctionContext, Row<'_>) -> Result<(i64, bool), EvalError> + Send + Sync>;

/// Go `FunctionDef.RequireDynamicPrivileges`: the dynamic privileges to
/// check, given whether SEM is enabled.
pub type RequireDynamicPrivileges = Arc<dyn Fn(bool) -> Vec<String> + Send + Sync>;

/// Go `FunctionDef`: the definition for the custom function.
#[derive(Clone)]
pub struct FunctionDef {
    /// Go `Name`: the function's name.
    pub name: String,
    /// Go `EvalTp`: the type of the return value.
    pub eval_tp: EvalType,
    /// Go `ArgTps`: the argument types.
    pub arg_tps: Vec<EvalType>,
    /// Go `OptionalArgsLen`: how many trailing arguments may be left out.
    pub optional_args_len: i64,
    /// Go `EvalStringFunc`, the evaluation when `eval_tp` is a string.
    pub eval_string_func: Option<EvalStringFunc>,
    /// Go `EvalIntFunc`, the evaluation when `eval_tp` is an integer.
    pub eval_int_func: Option<EvalIntFunc>,
    /// Go `RequireDynamicPrivileges`.
    pub require_dynamic_privileges: Option<RequireDynamicPrivileges>,
}

impl Default for FunctionDef {
    /// Go's zero `FunctionDef` (`EvalTp` 0 is `ETInt`).
    fn default() -> Self {
        Self {
            name: String::new(),
            eval_tp: EvalType::Int,
            arg_tps: Vec::new(),
            optional_args_len: 0,
            eval_string_func: None,
            eval_int_func: None,
            require_dynamic_privileges: None,
        }
    }
}

impl std::fmt::Debug for FunctionDef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FunctionDef")
            .field("name", &self.name)
            .field("eval_tp", &self.eval_tp)
            .field("arg_tps", &self.arg_tps)
            .field("optional_args_len", &self.optional_args_len)
            .finish_non_exhaustive()
    }
}

impl FunctionDef {
    /// Go `Validate`.
    ///
    /// # Errors
    ///
    /// Go's texts for an empty name and an `OptionalArgsLen` outside
    /// `[0, len(ArgTps)]`.
    pub fn validate(&self) -> Result<(), EvalError> {
        if self.name.is_empty() {
            return Err(EvalError::Raw("extension function name should not be empty".to_owned()));
        }
        let arg_count = i64::try_from(self.arg_tps.len()).unwrap_or(i64::MAX);
        if self.optional_args_len < 0 || self.optional_args_len > arg_count {
            return Err(EvalError::Raw(format!(
                "invalid OptionalArgsLen: {}",
                self.optional_args_len
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FunctionDef;
    use tidb_datatype::EvalType;
    use tidb_exprctx::EvalError;

    #[test]
    fn validate_refuses_an_empty_name_and_an_out_of_range_optional_count() {
        let def = FunctionDef {
            name: "f".to_owned(),
            arg_tps: vec![EvalType::Int, EvalType::Int],
            ..FunctionDef::default()
        };
        assert!(def.validate().is_ok());
        let unnamed = FunctionDef { name: String::new(), ..def.clone() };
        assert_eq!(
            unnamed.validate(),
            Err(EvalError::Raw("extension function name should not be empty".to_owned()))
        );
        for optional_args_len in [-1, 3] {
            let invalid = FunctionDef { optional_args_len, ..def.clone() };
            assert_eq!(
                invalid.validate(),
                Err(EvalError::Raw(format!("invalid OptionalArgsLen: {optional_args_len}")))
            );
        }
        assert!(FunctionDef { optional_args_len: 2, ..def }.validate().is_ok());
    }
}
