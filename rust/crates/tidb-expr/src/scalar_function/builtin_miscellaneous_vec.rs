// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_miscellaneous_vec.go`: addresses, identifiers and delegated columns.

use super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

/// Go miscellaneous signatures' `vectorized` methods.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        InetAton
            | InetNtoa
            | Inet6Aton
            | Inet6Ntoa
            | IsIPv4
            | IsIPv6
            | IsIPv4Compat
            | IsIPv4Mapped
            | IsUuid
            | Uuid
            | UuiDv4
            | UuiDv7
            | UuidVersion
            | UuidTimestamp
            | VitessHash
            | DecimalAnyValue
            | DurationAnyValue
            | IntAnyValue
            | JsonAnyValue
            | RealAnyValue
            | StringAnyValue
            | TimeAnyValue
    ) || matches!(
        function.func_name.lowercase(),
        "name_const" | "uuid_v4" | "uuid_v7" | "uuid_to_bin" | "bin_to_uuid" | "sleep"
    )
}

/// Go `builtin{NameConst,AnyValue,UUID,Inet,Sleep,...}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let name = function.func_name.lowercase();
    let domain = function
        .get_static_type()
        .expect("misc result type")
        .eval_type();
    if matches!(name, "any_value" | "name_const")
        || matches!(
            function.pb_code(),
            ScalarFuncSig::DecimalAnyValue
                | ScalarFuncSig::DurationAnyValue
                | ScalarFuncSig::IntAnyValue
                | ScalarFuncSig::JsonAnyValue
                | ScalarFuncSig::RealAnyValue
                | ScalarFuncSig::StringAnyValue
                | ScalarFuncSig::TimeAnyValue
        )
    {
        return vec_eval::eval_into(
            &function.args[usize::from(name == "name_const")],
            domain,
            ctx,
            input,
            result,
        );
    }
    if matches!(
        function.pb_code(),
        ScalarFuncSig::Uuid | ScalarFuncSig::UuiDv4 | ScalarFuncSig::UuiDv7
    ) {
        result.reserve_string_with_size_hint(input.num_rows(), 36);
        for row in 0..input.num_rows() {
            let value = function.pb_builtin.eval_values(function, Vec::new(), ctx)?;
            vec_eval::put(result, row, domain, &value)?;
        }
        return Ok(());
    }
    if name == "sleep" {
        let buffer = vec_eval::argument(&function.args[0], EvalType::Real, ctx, input)?;
        result.resize_int64(input.num_rows(), false);
        result.with_int64s_mut(|values| values.fill(0));
        for row in 0..input.num_rows() {
            let value = super::class_kernels::sleep(
                &[vec_eval::value(
                    &buffer,
                    row,
                    EvalType::Real,
                    function.args[0].static_type(),
                )],
                ctx,
            )?;
            if value == Datum::Int(1) {
                result.with_int64s_mut(|values| values[row..].fill(1));
                return Ok(());
            }
        }
        return Ok(());
    }
    vec_eval::map_arguments(function, ctx, input, result, None, |values| match function
        .pb_builtin
        .eval_values(function, values, ctx)
    {
        Err(EvalError::WrongValueForType { .. })
            if matches!(
                function.pb_code(),
                ScalarFuncSig::InetAton | ScalarFuncSig::Inet6Aton
            ) =>
        {
            Ok(Datum::Null)
        }
        value => value,
    })
}
