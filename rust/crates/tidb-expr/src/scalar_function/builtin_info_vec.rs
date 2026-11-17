// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_info_vec.go`: session snapshots and column side effects.

use super::{vec_eval, ScalarFunction};
use crate::{builtin::SigFields, Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Go information signatures' `vectorized` methods.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    if let SigFields::Benchmark { const_loop_count } = function.sig {
        return const_loop_count > 0;
    }
    matches!(
        function.func_name.lowercase(),
        "database"
            | "schema"
            | "connection_id"
            | "tidb_version"
            | "row_count"
            | "current_user"
            | "current_resource_group"
            | "current_role"
            | "user"
            | "session_user"
            | "system_user"
            | "tidb_is_ddl_owner"
            | "found_rows"
            | "last_insert_id"
            | "version"
            | "tidb_decode_key"
    )
}

/// Go `builtin{Database,ConnectionID,LastInsertID,Benchmark,...}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = function
        .get_static_type()
        .expect("builtin type")
        .eval_type();
    if let SigFields::Benchmark { const_loop_count } = function.sig {
        let expression = &function.args[1];
        crate::func::ensure_benchmark_eval_type(expression.static_type())?;
        let target = expression
            .static_type()
            .expect("benchmark argument type")
            .eval_type();
        let mut buffer = Column::default();
        for _ in 0..const_loop_count {
            vec_eval::eval_into(expression, target, ctx, input, &mut buffer)?;
        }
        result.resize_int64(input.num_rows(), false);
        result.with_int64s_mut(|values| values.fill(0));
        return Ok(());
    }
    if function.pb_code() == ScalarFuncSig::LastInsertIdWithId {
        // Go `builtinLastInsertIDWithIDSig.vecEvalInt`: `GetSessionVars`
        // first, then one write of the last non-NULL argument.
        let vars = tidb_expropt::SessionVarsPropReader
            .get_session_vars(ctx)
            .map_err(crate::builtin_ext::misc::opt_prop_error)?;
        vec_eval::eval_into(&function.args[0], EvalType::Int, ctx, input, result)?;
        for i in (0..input.num_rows()).rev() {
            if !result.is_null(i) {
                vars.set_last_insert_id(result.get_uint64(i));
                break;
            }
        }
        return Ok(());
    }
    // Go `builtinTiDBDecodeKeySig.vecEvalString`.
    if function.func_name.lowercase() == "tidb_decode_key" {
        let n = input.num_rows();
        let buf = vec_eval::argument(&function.args[0], EvalType::String, ctx, input)?;
        result.reserve_string(n);
        for i in 0..n {
            if buf.is_null(i) {
                result.append_null();
                continue;
            }
            let key = vec_eval::value(&buf, i, EvalType::String, function.args[0].static_type());
            let decoded = function.pb_builtin.eval_values(function, vec![key], ctx)?;
            vec_eval::put(result, i, EvalType::String, &decoded)?;
        }
        return Ok(());
    }
    let value = function.pb_builtin.eval_values(function, Vec::new(), ctx)?;
    crate::chunk_executor::allocate(domain, input.num_rows(), result);
    for i in 0..input.num_rows() {
        vec_eval::put(result, i, domain, &value)?;
    }
    Ok(())
}
