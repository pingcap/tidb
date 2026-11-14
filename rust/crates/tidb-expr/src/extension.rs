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


//! Go `pkg/expression/extension.go`: the function class an extension's
//! [`FunctionDef`] registers, and the signature it builds.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType};
use tidb_exprctx::optional::OptionalEvalPropKey;
use tidb_extension::{FunctionContext, FunctionDef};
use tidb_parser::auth::{RoleIdentity, UserIdentity};

use crate::builtin::{new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass, SigFields};
use crate::builtin_registry::FunctionArity;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::{Columns, EvalError};

/// Go `mysql.MaxFieldVarCharLength`.
const MAX_FIELD_VAR_CHAR_LENGTH: i64 = 65535;
/// Go `mysql.MaxIntWidth`.
const MAX_INT_WIDTH: i64 = 20;

/// Go `extensionFuncs`, keyed by the lowercased name. A class is leaked:
/// a built signature keeps its definition after the name is removed, as
/// Go's signature keeps its copy of the `FunctionDef`.
static EXTENSION_FUNCS: LazyLock<RwLock<HashMap<&'static str, &'static ExtensionFuncClass>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Go `registerExtensionFunc` (`extension.RegisterExtensionFunc`).
///
/// # Errors
///
/// Go's texts: the definition's `Validate`, a name a builtin has, a
/// definition `newExtensionFuncClass` refuses, and a name already
/// registered.
pub fn register_extension_func(def: &FunctionDef) -> Result<(), EvalError> {
    def.validate()?;
    let lower_name = def.name.to_lowercase();
    if crate::builtin_registry::is_builtin_name(&lower_name) {
        return Err(EvalError::Raw(format!(
            "extension function name '{}' conflict with builtin",
            def.name
        )));
    }
    let class = new_extension_func_class(def)?;
    let mut extensions = EXTENSION_FUNCS
        .write()
        .expect("extension function registry poisoned");
    if extensions.contains_key(lower_name.as_str()) {
        return Err(EvalError::Raw(format!(
            "duplicated extension function name '{}'",
            def.name
        )));
    }
    extensions.insert(Box::leak(lower_name.into_boxed_str()), Box::leak(Box::new(class)));
    Ok(())
}

/// Go `removeExtensionFunc` (`extension.RemoveExtensionFunc`): deletes the
/// entry `name` keys, as given.
pub fn remove_extension_func(name: &str) {
    EXTENSION_FUNCS
        .write()
        .expect("extension function registry poisoned")
        .remove(name);
}

/// Go `extensionFuncs.Load(name)`, with the key it is stored under.
pub(crate) fn extension_func(name: &str) -> Option<(&'static str, &'static ExtensionFuncClass)> {
    EXTENSION_FUNCS
        .read()
        .expect("extension function registry poisoned")
        .get_key_value(name)
        .map(|(key, class)| (*key, *class))
}

/// Go `extensionFuncs.Range`'s keys.
pub(crate) fn extension_func_names() -> Vec<String> {
    EXTENSION_FUNCS
        .read()
        .expect("extension function registry poisoned")
        .keys()
        .map(|name| (*name).to_owned())
        .collect()
}

/// Go `extensionFuncClass`.
#[derive(Debug)]
pub(crate) struct ExtensionFuncClass {
    /// Go `baseFunctionClass.funcName`: the definition's name as given.
    func_name: &'static str,
    /// Go `baseFunctionClass.minArgs` / `maxArgs`.
    pub(crate) arity: FunctionArity,
    /// Go `funcDef`.
    func_def: &'static FunctionDef,
    /// Go `flen`.
    flen: i64,
}

/// Go `newExtensionFuncClass`.
fn new_extension_func_class(def: &FunctionDef) -> Result<ExtensionFuncClass, EvalError> {
    let flen = match def.eval_tp {
        EvalType::String => {
            if def.eval_string_func.is_none() {
                return Err(EvalError::Raw("eval function is nil".to_owned()));
            }
            MAX_FIELD_VAR_CHAR_LENGTH
        }
        EvalType::Int => {
            if def.eval_int_func.is_none() {
                return Err(EvalError::Raw("eval function is nil".to_owned()));
            }
            MAX_INT_WIDTH
        }
        other => {
            return Err(EvalError::Raw(format!(
                "unsupported extension function ret type: '{other}'"
            )))
        }
    };
    let max_args = def.arg_tps.len();
    // `Validate` holds `OptionalArgsLen` within `[0, len(ArgTps)]`.
    let optional_args = usize::try_from(def.optional_args_len).expect("validated OptionalArgsLen");
    Ok(ExtensionFuncClass {
        func_name: Box::leak(def.name.clone().into_boxed_str()),
        arity: FunctionArity {
            min_args: max_args - optional_args,
            max_args: Some(max_args),
        },
        func_def: Box::leak(Box::new(def.clone())),
        flen,
    })
}

impl FunctionClass for ExtensionFuncClass {
    /// Go `extensionFuncClass.getFunction`: the privilege check comes
    /// before the argument count, which Go's `VerifyArgsWrapper` leaves to
    /// this class (an extension function is not in `funcs`).
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let columns = ctx.columns().ok_or_else(|| missing_property(OptionalEvalPropKey::PrivilegeChecker))?;
        check_privileges(columns, self.func_def)?;
        if !self.arity.accepts(args.len()) {
            return Err(EvalError::WrongParameterCount(self.func_name));
        }
        let arg_tps = &self.func_def.arg_tps[..args.len()];
        let mut bf = new_base_builtin_func_with_tp(ctx, self.func_name, args, self.func_def.eval_tp, arg_tps)?;
        // Go: "we still skip the plan cache for extension functions because
        // there are no strong requirements to do it".
        columns.set_skip_plan_cache("extension function should not be cached");
        bf.tp.set_flen(self.flen);
        bf.sig = SigFields::Extension(ExtensionSig(self.func_def));
        Ok(bf.evaluated_by(crate::scalar_function::Kernel::Row(eval_extension_func)))
    }
}

/// Go `extensionFuncSig.FunctionDef`: the definition a built signature
/// evaluates, compared by identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExtensionSig(pub(crate) &'static FunctionDef);

impl PartialEq for ExtensionSig {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for ExtensionSig {}

/// Go `GetPrivilegeChecker` / `GetSessionVars` on a context without the
/// property: `getPropProvider`'s error.
fn missing_property(key: OptionalEvalPropKey) -> EvalError {
    EvalError::Raw(format!("optional property: '{key}' not exists in EvalContext"))
}

/// Go `checkPrivileges`, after `GetPrivilegeChecker` found the property.
fn check_privileges(ctx: &dyn Columns, def: &FunctionDef) -> Result<(), EvalError> {
    if !ctx.has_privilege_checker() {
        return Err(missing_property(OptionalEvalPropKey::PrivilegeChecker));
    }
    let Some(require) = &def.require_dynamic_privileges else {
        return Ok(());
    };
    let sem_enabled = tidb_sem_compat::is_enabled();
    for privilege in require(sem_enabled) {
        if !ctx.request_dynamic_verification(&privilege, false) {
            let message = if sem_enabled {
                privilege
            } else {
                format!("SUPER or {privilege}")
            };
            return Err(EvalError::Conversion(
                crate::expression_errors::ERR_SPECIFIC_ACCESS_DENIED
                    .gen_with_stack_by_args(&[tidb_mysql::errors::FormatArg::from(message)]),
            ));
        }
    }
    Ok(())
}

/// Go `extensionFuncSig.evalString` / `evalInt`: the privileges again (a
/// cached plan's signature outlives a REVOKE), then the definition's
/// evaluation with Go's `extensionFnContext`.
fn eval_extension_func(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let Some(def) = function.extension_def() else {
        return Err(EvalError::Unsupported("an extension signature is built by extensionFuncClass"));
    };
    check_privileges(ctx, def)?;
    let active_roles = ctx
        .active_role_identities()
        .ok_or_else(|| missing_property(OptionalEvalPropKey::SessionVars))?;
    let fn_ctx = ExtensionFnContext { ctx, args: &function.args, active_roles };
    match def.eval_tp {
        EvalType::String => {
            let eval = def.eval_string_func.as_ref().expect("newExtensionFuncClass checked EvalStringFunc");
            let (value, is_null) = eval(&fn_ctx, row)?;
            Ok(if is_null { Datum::Null } else { Datum::new_string_datum(value) })
        }
        EvalType::Int => {
            let eval = def.eval_int_func.as_ref().expect("newExtensionFuncClass checked EvalIntFunc");
            let (value, is_null) = eval(&fn_ctx, row)?;
            Ok(if is_null { Datum::Null } else { Datum::Int(value) })
        }
        _ => Err(EvalError::Unsupported("newExtensionFuncClass admits only string and int results")),
    }
}

/// Go `extensionFnContext`.
struct ExtensionFnContext<'a> {
    ctx: &'a dyn Columns,
    args: &'a [Expression],
    active_roles: Vec<(String, String)>,
}

impl FunctionContext for ExtensionFnContext<'_> {
    /// Go `extensionFuncContext.ConnectionInfo`.
    fn connection_info(
        &self,
    ) -> Option<std::sync::Arc<tidb_variable::connection_info::ConnectionInfo>> {
        self.ctx.connection_info()
    }

    /// Go `vars.User`, which this port's session keeps as its login and
    /// matched identities.
    fn user(&self) -> Option<UserIdentity> {
        let login = self.ctx.login_user()?;
        let (username, hostname) = login.rsplit_once('@').unwrap_or((login.as_str(), ""));
        let (auth_username, auth_hostname) = self
            .ctx
            .current_user()
            .and_then(|identity| {
                identity
                    .rsplit_once('@')
                    .map(|(user, host)| (user.to_owned(), host.to_owned()))
            })
            .unwrap_or_default();
        Some(UserIdentity {
            username: username.to_owned(),
            hostname: hostname.to_owned(),
            current_user: false,
            auth_username,
            auth_hostname,
            auth_plugin: self.ctx.auth_plugin(),
        })
    }

    fn active_roles(&self) -> Vec<RoleIdentity> {
        self.active_roles
            .iter()
            .map(|(username, hostname)| RoleIdentity {
                username: username.clone(),
                hostname: hostname.clone(),
            })
            .collect()
    }

    fn current_db(&self) -> String {
        self.ctx.current_database().unwrap_or_default()
    }

    fn eval_args(&self, row: Row<'_>) -> Result<Vec<Datum>, EvalError> {
        self.args.iter().map(|arg| arg.eval(self.ctx, row)).collect()
    }
}
