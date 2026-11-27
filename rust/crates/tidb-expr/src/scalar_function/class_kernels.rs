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

//! The evaluation of the signatures Go gives no `pbCode`: only their
//! function class builds them, and the class hands the evaluation over.

use super::{advisory_lock_name, uservar_as_kind, ScalarFunction, MAX_ADVISORY_LOCK_TIMEOUT_SECS};
use crate::builtin::SigFields;
use crate::expression::ConstLevel;
use tidb_ast::BinaryOp;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType};
use tidb_exprctx::context::{Columns, EvalError};

/// A signature over one argument value.
macro_rules! unary_values {
    ($($(#[$doc:meta])* $name:ident => |$value:ident, $ctx:ident| $body:expr;)*) => {$(
        $(#[$doc])*
        pub(crate) fn $name(values: &[Datum], $ctx: &dyn Columns) -> Result<Datum, EvalError> {
            let _ = $ctx;
            let [$value] = values else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            $body
        }
    )*};
}

/// A signature over session state alone.
macro_rules! session_values {
    ($($(#[$doc:meta])* $name:ident => |$ctx:ident| $body:expr;)*) => {$(
        $(#[$doc])*
        pub(crate) fn $name(_: &[Datum], $ctx: &dyn Columns) -> Result<Datum, EvalError> {
            Ok($body)
        }
    )*};
}

/// Go `SessionVarsPropReader.GetSessionVars`.
fn session_vars(ctx: &dyn Columns) -> Result<std::sync::Arc<dyn tidb_expropt::SessionVars>, EvalError> {
    tidb_expropt::SessionVarsPropReader.get_session_vars(ctx).map_err(crate::builtin_ext::misc::opt_prop_error)
}

/// Go `builtinCurrentResourceGroupSig.evalString`.
pub(crate) fn current_resource_group(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(Datum::new_string_datum(session_vars(ctx)?.hint_resource_group_name().into_bytes()))
}

/// Go `builtinConnectionIDSig.evalInt`: `SessionVars.ConnectionID`.
pub(crate) fn connection_id(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(Datum::UInt(session_vars(ctx)?.connection_id()))
}

/// Go `builtinFoundRowsSig.evalInt`: `SessionVars.LastFoundRows`.
pub(crate) fn found_rows(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(Datum::UInt(session_vars(ctx)?.last_found_rows()))
}

/// Go `builtinRowCountSig.evalInt`: `SessionVars.StmtCtx.PrevAffectedRows`.
pub(crate) fn row_count(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(Datum::Int(session_vars(ctx)?.prev_affected_rows()))
}

/// Go's error for an identity builtin evaluated with no session user.
fn missing_session_variable() -> EvalError {
    EvalError::Raw("Missing session variable when eval builtin".to_owned())
}

/// Go `builtinCurrentUserSig.evalString`: the matched account,
/// `UserIdentity.String`.
pub(crate) fn current_user(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let user = tidb_expropt::CurrentUserPropReader
        .current_user(ctx)
        .map_err(crate::builtin_ext::misc::opt_prop_error)?
        .ok_or_else(missing_session_variable)?;
    Ok(Datum::new_string_datum(user.to_string().into_bytes()))
}

/// Go `builtinUserSig.evalString`: the login, `UserIdentity.LoginString`.
pub(crate) fn user(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let user = tidb_expropt::CurrentUserPropReader
        .current_user(ctx)
        .map_err(crate::builtin_ext::misc::opt_prop_error)?
        .ok_or_else(missing_session_variable)?;
    Ok(Datum::new_string_datum(user.login_string().into_bytes()))
}

/// Go `builtinCurrentRoleSig.evalString`: the active roles' `String`s
/// sorted and joined by commas, or `NONE`.
pub(crate) fn current_role(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let roles = tidb_expropt::CurrentUserPropReader
        .active_roles(ctx)
        .map_err(crate::builtin_ext::misc::opt_prop_error)?;
    if roles.is_empty() {
        return Ok(Datum::new_string_datum(b"NONE".to_vec()));
    }
    let mut sorted: Vec<String> = roles.iter().map(ToString::to_string).collect();
    sorted.sort_unstable();
    Ok(Datum::new_string_datum(sorted.join(",").into_bytes()))
}

fn string_or_null(value: Option<String>) -> Datum {
    value.map_or(Datum::Null, |value| Datum::new_string_datum(value.into_bytes()))
}

session_values! {
    /// Go `builtinDatabaseSig`: NULL when no database is selected.
    database => |ctx| string_or_null(ctx.current_database());
    /// Go `builtinVersionSig`: the string `@@version` reports.
    version => |ctx| Datum::new_string_datum(tidb_mysql::consts::runtime_versions().server_version.into_bytes());
    /// Go `builtinTiDBVersionSig`.
    tidb_version => |ctx| Datum::new_string_datum(ctx.tidb_info().into_bytes());
    /// Go `builtinTiDBIsDDLOwnerSig.evalInt`: 1 or 0, never NULL.
    tidb_is_ddl_owner => |ctx| Datum::Int(i64::from(
        tidb_expropt::DdlOwnerPropReader.is_ddl_owner(ctx).map_err(crate::builtin_ext::misc::opt_prop_error)?,
    ));
    /// Go `builtinReleaseAllLocksSig`.
    release_all_locks => |ctx| Datum::Int(tidb_expropt::AdvisoryLockContext::release_all_advisory_locks(&*advisory_lock_ctx(ctx)?));
}

unary_values! {
    /// Go `builtinFormatBytesSig`.
    format_bytes => |value, ctx| crate::builtin_ext::info::format_bytes(value, ctx);
    /// Go `builtinFormatNanoTimeSig`.
    format_nano_time => |value, ctx| crate::builtin_ext::info::format_nano_time(value, ctx);
    /// Go `builtinTiDBDecodePlanSig`.
    tidb_decode_plan => |value, ctx| crate::builtin_ext::info::decode_plan(value);
    /// Go `builtinTiDBDecodeBinaryPlanSig`.
    tidb_decode_binary_plan => |value, ctx| crate::builtin_ext::info::decode_binary_plan(value, ctx);
    /// Go `builtinTiDBEncodeSQLDigestSig`.
    tidb_encode_sql_digest => |value, ctx| crate::builtin_ext::info::encode_sql_digest(value);
    /// Go `builtinTiDBDecodeKeySig`.
    tidb_decode_key => |value, ctx| crate::builtin_ext::misc::tidb_decode_key(value, ctx);
    /// Go `builtinRandomBytesSig`.
    random_bytes => |value, ctx| crate::builtin_ext::crypto::random_bytes(value);
    /// Go `builtinSM3Sig`.
    sm3 => |value, ctx| crate::builtin_ext::crypto::sm3_hash(value);
    /// Go `builtinValidatePasswordStrengthSig`.
    validate_password_strength => |value, ctx| {
        crate::builtin_ext::crypto::validate_password_strength(value, ctx)
    };
    /// Go `builtinVecFromTextSig`.
    vec_from_text => |value, ctx| crate::builtin_ext::vec::from_text(value);
    /// Go `builtinLoadFileSig`: a server reads no client file.
    load_file => |value, ctx| { let _ = value; Ok(Datum::Null) };
    /// Go `builtinNextValSig.evalInt`: taken outside the statement's
    /// transaction; the session's sequence state records the value.
    nextval => |path, ctx| {
        let Some((sequence, vars)) = sequence_operator(path, ctx, tidb_mysql::InsertPriv, "INSERT")? else {
            return Ok(Datum::Null);
        };
        let next_val = sequence.get_sequence_next_val().map_err(crate::builtin_ext::misc::opt_prop_error)?;
        vars.update_sequence_state(sequence.get_sequence_id(), next_val);
        Ok(Datum::Int(next_val))
    };
    /// Go `builtinLastValSig.evalInt`: NULL until this session's `NEXTVAL`
    /// or `SETVAL` touched the sequence.
    lastval => |path, ctx| {
        let Some((sequence, vars)) = sequence_operator(path, ctx, tidb_mysql::SelectPriv, "SELECT")? else {
            return Ok(Datum::Null);
        };
        Ok(vars.sequence_last_value(sequence.get_sequence_id()).map_or(Datum::Null, Datum::Int))
    };
    /// Go `builtinGetParamStringSig.evalString`: the plan-cache parameter the
    /// integer selects, as a string; one `ToString` cannot render is NULL.
    getparam => |value, ctx| {
        let index = match *value {
            Datum::Null => return Ok(Datum::Null),
            Datum::Int(index) => {
                usize::try_from(index).map_err(|_| EvalError::ParamIndexExceedParamCounts)?
            }
            Datum::UInt(index) => {
                usize::try_from(index).map_err(|_| EvalError::ParamIndexExceedParamCounts)?
            }
            _ => return Err(EvalError::Unsupported("GETPARAM index is not an integer")),
        };
        let value = ctx.get_param_value(index)?;
        Ok(value.sql_string().map_or(Datum::Null, Datum::new_string_datum))
    };
}

/// Go `builtinNextValSig` / `builtinLastValSig` / `builtinSetValSig`: the
/// sequence `path` names (a bare name is in `ctx.CurrentDB()`), from
/// `GetSequenceOperator`, once `RequestVerification` grants `privilege`
/// (`name`, for the error) on it. `None` is a NULL name.
#[allow(clippy::type_complexity)]
fn sequence_operator(
    path: &Datum,
    ctx: &dyn Columns,
    privilege: tidb_mysql::PrivilegeType,
    name: &'static str,
) -> Result<
    Option<(std::sync::Arc<dyn tidb_expropt::SequenceOperator>, std::sync::Arc<dyn tidb_expropt::SessionVars>)>,
    EvalError,
> {
    if path.is_null() {
        return Ok(None);
    }
    let path = crate::func::sequence_path(path)?;
    let (db, seq) = match path.as_slice() {
        [seq] => (ctx.current_database().unwrap_or_default(), seq.clone()),
        [leading @ .., seq] => (leading.join("."), seq.clone()),
        [] => (String::new(), String::new()),
    };
    let opt_err = crate::builtin_ext::misc::opt_prop_error;
    // Check the table name is valid.
    let sequence = tidb_expropt::SequenceOperatorPropReader.get_sequence_operator(ctx, &db, &seq).map_err(opt_err)?;
    let vars = session_vars(ctx)?;
    let checker = tidb_expropt::PrivilegeCheckerPropReader.get_privilege_checker(ctx).map_err(opt_err)?;
    if !checker.request_verification(&db, &seq, "", privilege) {
        let user = vars.user().unwrap_or_default();
        return Err(EvalError::Sequence(tidb_exprctx::SequenceEvalError::AccessDenied {
            privilege: name,
            user: user.auth_username.clone(),
            host: user.auth_hostname.clone(),
            sequence: seq,
        }));
    }
    Ok(Some((sequence, vars)))
}

/// Go `builtinSetValSig.evalInt`: the sequence and the privilege are
/// checked before the value; a NULL value is NULL and sets nothing.
pub(crate) fn setval(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [path, value] = values else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let Some((sequence, _)) = sequence_operator(path, ctx, tidb_mysql::InsertPriv, "INSERT")? else {
        return Ok(Datum::Null);
    };
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let Some(value) = value.as_int() else {
        return Err(EvalError::Unsupported("SETVAL needs an integer value"));
    };
    let (set_value, is_null) = sequence.set_sequence_val(value).map_err(crate::builtin_ext::misc::opt_prop_error)?;
    Ok(if is_null { Datum::Null } else { Datum::Int(set_value) })
}

/// Go `builtinTiDBDecodeSQLDigestsSig`.
pub(crate) fn tidb_decode_sql_digests(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::misc::tidb_decode_sql_digests(&values[0], values.get(1), ctx)
}

/// Go `builtinTiDBEncodeRecordKeySig`.
pub(crate) fn tidb_encode_record_key(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::misc::tidb_encode_key(values, false, ctx)
}

/// Go `builtinTiDBEncodeIndexKeySig`.
pub(crate) fn tidb_encode_index_key(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::misc::tidb_encode_key(values, true, ctx)
}

/// Go `builtinTiDBMVCCInfoSig`.
pub(crate) fn tidb_mvcc_info(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::misc::tidb_mvcc_info(&values[0], ctx)
}

/// Go `builtinUUIDToBinSig`.
pub(crate) fn uuid_to_bin(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::misc::uuid_to_bin(&values[0], values.get(1))
}

/// Go `builtinBinToUUIDSig`.
pub(crate) fn bin_to_uuid(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::misc::bin_to_uuid(ctx, &values[0], values.get(1))
}

/// Go `builtinNameConst*Sig`: the second argument.
pub(crate) fn name_const(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(values[1].clone())
}

/// Go `builtinBitCountSig`.
pub(crate) fn bit_count(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::bit_count(values, ctx)
}

/// Go `builtinTranslate{Binary,UTF8}Sig`.
pub(crate) fn translate(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::string2::translate(values)
}

/// Go `builtinPeriodAddSig`.
pub(crate) fn period_add(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::time_fn::period_add(values)
}

/// Go `builtinPeriodDiffSig`.
pub(crate) fn period_diff(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::time_fn::period_diff(values)
}

/// Go `builtinTidbParseTsoLogicalSig`.
pub(crate) fn tidb_parse_tso_logical(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::time_fn::tidb_parse_tso_logical(values)
}

/// Go `builtinCurrentDateSig`.
pub(crate) fn current_date(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    super::pb_builtin::temporal_of_ret_type(function, crate::time_fn::current_date(values, ctx)?, ctx)
}

/// Go `builtinUTCDateSig`.
pub(crate) fn utc_date(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    super::pb_builtin::temporal_of_ret_type(function, crate::time_fn::utc_date(values, ctx)?, ctx)
}

/// Go `builtinJSONOverlapsSig`.
pub(crate) fn json_overlaps(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    let types: Vec<Option<FieldType>> =
        function.args.iter().map(|arg| arg.static_type().cloned()).collect();
    crate::builtin_ext::json_dispatch_typed("JSON_OVERLAPS", values, &types)
        .expect("the native JSON family registers JSON_OVERLAPS")
}

/// Go `builtinJSONSchemaValidSig`: the compiled schema is cached per node.
pub(crate) fn json_schema_valid(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    function.json_schema_cache.eval(&function.args, ctx, row)
}

/// Go `builtinValues*Sig`: `SessionVars.CurrInsertValues` at the offset
/// `NewValuesFunc` fixed; an empty row is SQL NULL, and an offset past the
/// row is Go's length/offset error.
pub(crate) fn values(function: &ScalarFunction, ctx: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    let vars = session_vars(ctx)?;
    let offset = function
        .values_offset()
        .ok_or(EvalError::Unsupported("VALUES offset is not initialized"))?;
    let Some((len, value)) = vars.curr_insert_values_at(offset) else {
        return Ok(Datum::Null);
    };
    let Some(value) = value else {
        return Err(EvalError::IncorrectArguments(format!(
            "Session current insert values len {len} and column's offset {offset} don't match"
        )));
    };
    match value {
        // Go `builtinValuesStringSig` reads an ENUM/SET value back through
        // `row.GetDatum(offset, retType).ToString()`: the member name.
        value @ (Datum::Enum(..) | Datum::Set(..)) => {
            let bytes = value
                .to_bytes()
                .map_err(|_| EvalError::Unsupported("ENUM/SET VALUES() name"))?;
            Ok(Datum::new_collation_string_datum(bytes, function.derived_collation()))
        }
        value => Ok(value),
    }
}

/// Go `builtinCharsetSig`, `builtinCollationSig` and
/// `builtinCoercibilitySig`: the argument's metadata, never its value.
pub(crate) fn info_metadata(function: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    let [arg] = function.args.as_slice() else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    crate::collation_derive::info_metadata_value(function.func_name.lowercase(), arg)
        .ok_or(EvalError::Unsupported("this scalar function is not yet ported"))
}

/// Go `builtinBenchmarkSig.evalInt`.
pub(crate) fn benchmark(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let [count, expression] = function.args.as_slice() else {
        return Err(EvalError::Unsupported("BENCHMARK arguments"));
    };
    let loop_count = match function.sig {
        SigFields::Benchmark { const_loop_count } if const_loop_count > 0 => const_loop_count,
        _ => match crate::func::benchmark_loop_count(count.eval(ctx, row)?, count.static_type(), ctx)? {
            Some(value) => value,
            None => return Ok(Datum::Null),
        },
    };
    if loop_count < 0 {
        return Ok(Datum::Null);
    }
    crate::func::ensure_benchmark_eval_type(expression.static_type())?;
    for _ in 0..loop_count {
        expression.eval(ctx, row)?;
    }
    Ok(Datum::Int(0))
}

/// Go `doSleep`: waits `duration`, polling the session's killer every 10ms;
/// a kill ends the wait (true) and, for a statement that reads no table and
/// is not an INSERT, UPDATE or DELETE, is reset so the session continues.
fn do_sleep(duration: std::time::Duration, vars: &dyn tidb_expropt::SessionVars) -> bool {
    let started = std::time::Instant::now();
    let tick = std::time::Duration::from_millis(10);
    let killer = vars.sql_killer();
    loop {
        let elapsed = started.elapsed();
        if elapsed >= duration {
            return false;
        }
        std::thread::sleep(tick.min(duration - elapsed));
        if started.elapsed() >= duration {
            return false;
        }
        if let Some(killer) = killer.as_ref() {
            if killer.handle_signal().is_some() {
                if !vars.stmt_has_table_ids() && !vars.stmt_in_insert_update_or_delete() {
                    killer.reset();
                }
                return true;
            }
        }
    }
}

/// Go `builtinSleepSig.evalInt`.
pub(crate) fn sleep(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let vars = session_vars(ctx)?;
    let [value] = values else {
        return Err(EvalError::WrongParameterCount("sleep"));
    };
    if value.is_null() {
        crate::expression_errors::handle_sleep_incorrect_argument(ctx)?;
        return Ok(Datum::Int(0));
    }
    let seconds = crate::ops::to_f64_with_mysql_string(value, ctx)?;
    if seconds < 0.0 {
        crate::expression_errors::handle_sleep_incorrect_argument(ctx)?;
        return Ok(Datum::Int(0));
    }
    if seconds > f64::MAX / 1_000_000_000.0 {
        return Err(EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned()));
    }
    if seconds <= 0.0 {
        return Ok(Datum::Int(0));
    }
    let duration = std::time::Duration::try_from_secs_f64(seconds)
        .map_err(|_| EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned()))?;
    Ok(Datum::Int(i64::from(do_sleep(duration, vars.as_ref()))))
}

/// The lock name the advisory-lock signatures read from their first
/// argument.
fn lock_name(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<String, EvalError> {
    let name = crate::cast::cast_arg_as_string(&values[0], function.args[0].static_type(), ctx)?;
    advisory_lock_name(name)
}

/// Go `AdvisoryLockPropReader.AdvisoryLockCtx`.
fn advisory_lock_ctx(ctx: &dyn Columns) -> Result<std::sync::Arc<tidb_expropt::AdvisoryLockPropProvider>, EvalError> {
    tidb_expropt::AdvisoryLockPropReader
        .advisory_lock_ctx(ctx)
        .map_err(crate::builtin_ext::misc::opt_prop_error)
}

/// Go `builtinLockSig.evalInt`.
pub(crate) fn get_lock(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_expropt::AdvisoryLockContext as _;
    let lock_ctx = advisory_lock_ctx(ctx)?;
    let lock_name = lock_name(function, values, ctx)?;
    let timeout = crate::cast::cast_arg_as_int(&values[1], function.args[1].static_type(), ctx)?;
    let mut timeout = crate::arg_eval_type::eval_int(&timeout)?.unwrap_or(0);
    if !(0..=MAX_ADVISORY_LOCK_TIMEOUT_SECS).contains(&timeout) {
        ctx.append_go_warning(
            &crate::expression_errors::ERR_TRUNCATED_WRONG_VALUE,
            &format!("Truncated incorrect get_lock value: '{timeout}'"),
        );
        timeout = MAX_ADVISORY_LOCK_TIMEOUT_SECS;
    }
    match lock_ctx.get_advisory_lock(&lock_name, timeout) {
        Ok(()) => Ok(Datum::Int(1)),
        // Another user has the lock.
        Err(tidb_expropt::GetAdvisoryLockError::LockWaitTimeout) => Ok(Datum::Int(0)),
        Err(tidb_expropt::GetAdvisoryLockError::LockDeadlock) => Err(EvalError::AdvisoryLock {
            code: 3058,
            message: "Deadlock found when trying to get user-level lock; try rolling back transaction/releasing locks and restarting lock acquisition.".to_owned(),
        }),
        Err(tidb_expropt::GetAdvisoryLockError::Other(error)) => {
            Err(EvalError::AdvisoryLock { code: 1105, message: error.message().to_owned() })
        }
    }
}

/// Go `builtinReleaseLockSig.evalInt`.
pub(crate) fn release_lock(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_expropt::AdvisoryLockContext as _;
    let lock_ctx = advisory_lock_ctx(ctx)?;
    let lock_name = lock_name(function, values, ctx)?;
    Ok(Datum::Int(i64::from(lock_ctx.release_advisory_lock(&lock_name))))
}

/// Go `builtinFreeLockSig.evalInt`.
pub(crate) fn is_free_lock(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_expropt::AdvisoryLockContext as _;
    let lock_ctx = advisory_lock_ctx(ctx)?;
    let lock_name = lock_name(function, values, ctx)?;
    Ok(Datum::Int(i64::from(lock_ctx.is_used_advisory_lock(&lock_name) == 0)))
}

/// Go `builtinUsedLockSig.evalInt`: the owning connection, or NULL.
pub(crate) fn is_used_lock(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_expropt::AdvisoryLockContext as _;
    let lock_ctx = advisory_lock_ctx(ctx)?;
    let lock_name = lock_name(function, values, ctx)?;
    Ok(match lock_ctx.is_used_advisory_lock(&lock_name) {
        0 => Datum::Null,
        owner => Datum::Int(owner as i64),
    })
}

/// Go `builtinGetVar*Sig`: the variable's name is a build-time constant and
/// its value is converted onto the type the build declared.
pub(crate) fn getvar(function: &ScalarFunction, ctx: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    let (Some(var), Some(ret_type)) = (function.uservar_name(), function.ret_type.as_ref()) else {
        return Err(EvalError::Unsupported("GETVAR has no constant variable name"));
    };
    let kind = match ret_type.eval_type() {
        EvalType::Int if ret_type.is_unsigned() => "uint",
        EvalType::Int => "int",
        EvalType::Real => "real",
        EvalType::Decimal => "decimal",
        EvalType::Datetime => "time",
        _ => "string",
    };
    uservar_as_kind(kind, ctx.get_uservar(&var).unwrap_or(Datum::Null), ctx)
}

/// Go `builtinSetVar*Sig`: `GetSessionVars` first, then the value; a NULL
/// value is returned without touching the variable, any other is stored
/// under the lowercased name (`SessionVars.SetUserVarVal`).
pub(crate) fn setvar(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let vars = tidb_expropt::SessionVarsPropReader
        .get_session_vars(ctx)
        .map_err(crate::builtin_ext::misc::opt_prop_error)?;
    let var = function
        .uservar_name()
        .ok_or(EvalError::Unsupported("SETVAR has no constant variable name"))?;
    let value = function.args[1].eval(ctx, row)?;
    if !value.is_null() {
        vars.set_user_var_val(&tidb_hack::go_to_lower(&var), value.clone());
    }
    Ok(value)
}

/// Go `builtinWeightStringNullSig`.
pub(crate) fn weight_string_null(_: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    Ok(Datum::Null)
}

/// Go `builtinWeightStringSig`.
pub(crate) fn weight_string(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let SigFields::WeightString { padding, length } = function.sig else {
        return Err(EvalError::Unsupported("weight_string is built by weightStringFunctionClass"));
    };
    let padding = match padding {
        crate::builtin::WeightStringPadding::None => None,
        crate::builtin::WeightStringPadding::AsChar => Some((false, length)),
        crate::builtin::WeightStringPadding::AsBinary => Some((true, length)),
    };
    let value = function.args[0].eval(ctx, row)?;
    let arg_collation = function.args[0]
        .static_type()
        .and_then(|tp| tidb_charset::Collation::from_name(tp.collation_name()))
        .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION);
    crate::string_packet::weight_string(&value, padding, arg_collation, ctx)
}

/// Go `builtinFindInSetSig`: a constant list is split once per statement.
pub(crate) fn find_in_set(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let collation = function.derived_collation();
    let needle = function.args[0].eval(ctx, row)?;
    if function.args[1].const_level() >= ConstLevel::ONLY_IN_CONTEXT {
        let lookup = function
            .find_in_set_cache
            .get_or_init_cache(ctx.context_id(), || {
                let list = function.args[1].eval(ctx, Row::empty())?;
                crate::builtin_ext::build_find_in_set_lookup(&list, collation)
            })?;
        return crate::builtin_ext::find_in_set_lookup(&needle, &lookup, collation);
    }
    let list = function.args[1].eval(ctx, row)?;
    crate::builtin_ext::find_in_set_with_collation(&[needle, list], collation)
}

/// Go `builtinCastJSONAsArraySig`.
pub(crate) fn cast_json_as_array(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let ret_type = cast_ret_type(function)?;
    crate::cast::eval_json(function.args[0].eval(ctx, row)?, ret_type)
}

/// Go `builtinCastVectorFloat32AsUnsupportedSig`: fails before reading its
/// argument.
pub(crate) fn cast_vector_float32_as_unsupported(function: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    Err(EvalError::Vector(format!(
        "cannot cast from vector to {}",
        tidb_datatype::type_str(cast_ret_type(function)?.code()),
    )))
}

/// Go `builtinCastUnsupportedAsVectorFloat32Sig`.
pub(crate) fn cast_unsupported_as_vector_float32(function: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    cast_ret_type(function)?;
    let source = function.args[0]
        .static_type()
        .ok_or(EvalError::Unsupported("a cast with no source type"))?;
    Err(EvalError::Vector(format!(
        "cannot cast from {} to vector",
        tidb_datatype::type_str(source.code()),
    )))
}

/// Go `builtinCastStringAsVectorFloat32Sig`.
pub(crate) fn cast_string_as_vector_float32(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let ret_type = cast_ret_type(function)?;
    let value = function.args[0].eval_string(ctx, row)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    crate::cast::cast_as_vector_float32(value, ret_type)
}

fn cast_ret_type(function: &ScalarFunction) -> Result<&FieldType, EvalError> {
    function
        .get_static_type()
        .ok_or(EvalError::Unsupported("a cast with no result type"))
}

fn vector_arithmetic(function: &ScalarFunction, op: BinaryOp, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let left = function.args[0].eval(ctx, row)?;
    let right = function.args[1].eval(ctx, row)?;
    function.eval_binary_values(op, left, right, ctx)
}

/// Go `builtinArithmeticPlusVectorFloat32Sig`.
pub(crate) fn plus_vector_float32(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    vector_arithmetic(function, BinaryOp::Plus, ctx, row)
}

/// Go `builtinArithmeticMinusVectorFloat32Sig`.
pub(crate) fn minus_vector_float32(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    vector_arithmetic(function, BinaryOp::Minus, ctx, row)
}

/// Go `builtinArithmeticMultiplyVectorFloat32Sig`.
pub(crate) fn multiply_vector_float32(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    vector_arithmetic(function, BinaryOp::Mul, ctx, row)
}

/// A signature whose Go implementation is not ported.
pub(crate) fn not_yet_ported(_: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    Err(EvalError::Unsupported("this scalar function is not yet ported"))
}

/// Go `builtinTanSig`.
pub(crate) fn tan(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::math_fn::tan(values, ctx).map_err(|error| super::math_overflow_error(function, error, ctx))
}

/// Go `builtinQuoteSig`: a NULL argument is the string `NULL`.
pub(crate) fn quote(_: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::quote(values)
}

/// Go `builtinOrdSig`, which reads the argument's charset.
pub(crate) fn ord(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::ord_with_type(values, function.args.first().and_then(|arg| arg.static_type()))
}

/// Go `builtinRepeatSig`: `vecEvalString` bounds the result by the return
/// type's flen where `evalString` bounds it by `max_allowed_packet`.
pub(crate) fn repeat(_function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_packet::repeat_in_mode(values, None, ctx)
}

/// Go `builtinPasswordSig.evalString` (its `vecEvalString`, which hashes a
/// NULL to "", is `builtin_encryption_vec.rs`'s).
pub(crate) fn password(_function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::builtin_ext::crypto::password_hash(&values[0], ctx)
}

/// Go `builtinLastDaySig`.
pub(crate) fn last_day(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    super::pb_builtin::temporal_of_ret_type(function, crate::time_fn::last_day(values, ctx)?, ctx)
}

/// Go `builtinSoundexSig.evalString`, reading the argument as Go's
/// `soundexModeOf` selects from its type.
pub(crate) fn soundex(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let arg_type = function.args[0].static_type();
    let value = crate::cast::cast_arg_as_string(&values[0], arg_type, ctx)?;
    let Some(bytes) = crate::arg_eval_type::eval_string_bytes(&value)? else {
        return Ok(Datum::Null);
    };
    let mode = arg_type.map_or(crate::string_fn::SoundexMode::Runes, crate::string_fn::soundex_mode_of);
    let code = crate::string_fn::soundex(bytes, mode);
    Ok(if mode == crate::string_fn::SoundexMode::Binary {
        Datum::new_bytes_datum(code)
    } else {
        Datum::new_string_datum(code)
    })
}
