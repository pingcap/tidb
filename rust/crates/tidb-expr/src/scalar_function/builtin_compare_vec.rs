// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_compare_vec{,_generated}.go` and `builtin_other_vec_generated.go`.

use super::{vec_eval, Kernel, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_ast::BinaryOp;
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `builtin{Greatest,Least,Interval}*Sig.vectorized`.
pub(super) fn supported(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        GreatestInt
            | GreatestReal
            | GreatestDecimal
            | GreatestString
            | GreatestTime
            | GreatestDate
            | GreatestDuration
            | GreatestCmpStringAsTime
            | GreatestCmpStringAsDate
            | LeastInt
            | LeastReal
            | LeastDecimal
            | LeastString
            | LeastTime
            | LeastDate
            | LeastDuration
            | LeastCmpStringAsTime
            | LeastCmpStringAsDate
            | IntervalInt
            | IntervalReal
    )
}

/// Go `builtinGreatest*Sig.vecEval*`, `builtinLeast*Sig.vecEval*`.
pub(super) fn extremum(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    if matches!(sig, IntervalInt | IntervalReal) {
        return interval(function, ctx, input, result);
    }
    let greatest = matches!(
        sig,
        GreatestInt
            | GreatestReal
            | GreatestDecimal
            | GreatestString
            | GreatestTime
            | GreatestDate
            | GreatestDuration
            | GreatestCmpStringAsTime
            | GreatestCmpStringAsDate
    );
    let domain = function
        .get_static_type()
        .expect("extremum type")
        .eval_type();
    if domain == EvalType::String {
        if matches!(
            sig,
            GreatestCmpStringAsTime
                | GreatestCmpStringAsDate
                | LeastCmpStringAsTime
                | LeastCmpStringAsDate
        ) {
            return string_as_time(function, greatest, ctx, input, result);
        }
        return string(function, greatest, ctx, input, result);
    }
    let allocate = matches!(
        domain,
        EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
    );
    if allocate {
        crate::chunk_executor::allocate(domain, input.num_rows(), result);
    } else {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    }
    for j in usize::from(!allocate)..function.args.len() {
        let buffer = vec_eval::argument(&function.args[j], domain, ctx, input)?;
        result.merge_nulls(&[&buffer]);
        for i in 0..input.num_rows() {
            if result.is_null(i) {
                continue;
            }
            let candidate = vec_eval::value(&buffer, i, domain, None);
            let current = vec_eval::value(result, i, domain, None);
            let op = if greatest { BinaryOp::Gt } else { BinaryOp::Lt };
            let wins = j == 0
                || crate::ops::eval_binary_full(
                    op,
                    candidate.clone(),
                    current,
                    ctx.div_precision_increment(),
                    function.derived_collation(),
                    crate::ops::Operands::LITERALS,
                    ctx,
                )? == Datum::Int(1);
            if wins {
                vec_eval::put(result, i, domain, &candidate)?;
            }
        }
    }
    if matches!(domain, EvalType::Datetime | EvalType::Timestamp) {
        for i in 0..input.num_rows() {
            let value = vec_eval::value(result, i, domain, None);
            let value =
                function.extremum_of(greatest, &[value], function.derived_collation(), ctx)?;
            vec_eval::put(result, i, domain, &value)?;
        }
    }
    Ok(())
}

/// Go `builtin{Greatest,Least}StringSig.vecEvalString` ping-pong result buffers.
fn string(
    function: &ScalarFunction,
    greatest: bool,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::eval_into(&function.args[0], EvalType::String, ctx, input, result)?;
    let mut source = std::mem::take(result);
    let mut destination = Column::default();
    let mut argument = Column::default();
    destination.reserve_string(input.num_rows());
    let collator = tidb_datatype::get_collator(function.derived_collation().name());
    let want = if greatest {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Less
    };
    for expr in &function.args[1..] {
        vec_eval::eval_into(expr, EvalType::String, ctx, input, &mut argument)?;
        for i in 0..input.num_rows() {
            if source.is_null(i) || argument.is_null(i) {
                destination.append_null();
                continue;
            }
            let left = source.get_bytes(i);
            let right = argument.get_bytes(i);
            destination.append_bytes(if collator.compare(&left, &right) == want {
                &left
            } else {
                &right
            });
        }
        std::mem::swap(&mut source, &mut destination);
        argument.reserve_string(input.num_rows());
        destination.reserve_string(input.num_rows());
    }
    if function.args.len() % 2 == 0 {
        source.copy_construct_to(&mut destination);
        *result = destination;
    } else {
        *result = source;
    }
    Ok(())
}

/// Go `builtin{Greatest,Least}CmpStringAsTimeSig.vecEvalString`: conversion
/// warnings happen argument-major, and every argument writes into result.
fn string_as_time(
    function: &ScalarFunction,
    greatest: bool,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let as_date = matches!(
        function.pb_code(),
        ScalarFuncSig::GreatestCmpStringAsDate | ScalarFuncSig::LeastCmpStringAsDate
    );
    let mut values = vec![String::new(); input.num_rows()];
    let mut nulls = vec![false; input.num_rows()];
    for (j, arg) in function.args.iter().enumerate() {
        vec_eval::eval_into(arg, EvalType::String, ctx, input, result)?;
        for i in 0..input.num_rows() {
            nulls[i] |= result.is_null(i);
            if nulls[i] {
                continue;
            }
            let text = tidb_goruntime::GoString::from_bytes(result.get_bytes(i).as_ref().to_vec())
                .to_utf8_lossy_go();
            let text = crate::builtin_ext::time_conversion_for_gl(as_date, &text, ctx)?;
            if j == 0 || (greatest && text > values[i]) || (!greatest && text < values[i]) {
                values[i] = text;
            }
        }
    }
    result.reserve_string(input.num_rows());
    for i in 0..input.num_rows() {
        if nulls[i] {
            result.append_null();
        } else {
            result.append_string(&values[i]);
        }
    }
    Ok(())
}

/// Go `builtinInterval{Int,Real}Sig.vecEvalInt`: only the first argument is
/// a vector; each binary/linear search reads its boundaries from that input row.
fn interval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = if function.pb_code() == ScalarFuncSig::IntervalInt {
        EvalType::Int
    } else {
        EvalType::Real
    };
    let mut buffer = Column::default();
    if domain == EvalType::Int {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, result)?;
    } else {
        vec_eval::eval_into(&function.args[0], domain, ctx, input, &mut buffer)?;
        result.resize_int64(input.num_rows(), false);
    }
    let types = function
        .args
        .iter()
        .map(|arg| arg.static_type().cloned())
        .collect::<Vec<_>>();
    for i in 0..input.num_rows() {
        let first = vec_eval::value(
            if domain == EvalType::Int {
                &*result
            } else {
                &buffer
            },
            i,
            domain,
            function.args[0].static_type(),
        );
        let value = crate::builtin_ext::interval_lazy(
            &types,
            |j| {
                if j == 0 {
                    Ok(first.clone())
                } else {
                    function.args[j].eval(ctx, input.get_row(i))
                }
            },
            ctx,
        )?;
        vec_eval::put(result, i, EvalType::Int, &value)?;
    }
    Ok(())
}

/// Go `builtin{LT,LE,GT,GE,EQ,NE,NullEQ}*Sig.vecEvalInt`.
pub(super) fn compare(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let Kernel::Compare(op, domain) = function.pb_builtin.kernel else {
        unreachable!()
    };
    let left = vec_eval::argument(&function.args[0], domain, ctx, input)?;
    let right = vec_eval::argument(&function.args[1], domain, ctx, input)?;
    result.resize_int64(input.num_rows(), false);
    if op != BinaryOp::NullEq {
        result.merge_nulls(&[&left, &right]);
    }
    for i in 0..input.num_rows() {
        if result.is_null(i) {
            continue;
        }
        let left = vec_eval::value(&left, i, domain, function.args[0].static_type());
        let right = vec_eval::value(&right, i, domain, function.args[1].static_type());
        let value = function.eval_binary_values(op, left, right, ctx)?;
        vec_eval::put(result, i, EvalType::Int, &value)?;
    }
    Ok(())
}

/// Go `builtinIn{Int,Real,Decimal,String,Time,Duration,JSON}Sig.vecEvalInt`:
/// every candidate column is evaluated, even after all rows have matched.
pub(super) fn in_list(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let Kernel::In(domain) = function.pb_builtin.kernel else {
        unreachable!()
    };
    let first = &function.args[0];
    let left = vec_eval::argument(first, domain, ctx, input)?;
    result.resize_int64(input.num_rows(), true);
    result.with_int64s_mut(|values| values.fill(0));
    let cache = function.in_cache.as_deref();
    let mut has_null = vec![cache.is_some_and(|cache| cache.has_null); input.num_rows()];
    let candidates: Vec<_> = if let Some(cache) = cache.filter(|cache| !cache.is_empty()) {
        let collator = tidb_datatype::get_collator(function.derived_collation().name());
        for i in 0..input.num_rows() {
            if !left.is_null(i)
                && cache.contains(
                    &vec_eval::value(&left, i, domain, first.static_type()),
                    collator,
                    crate::wrap_cast::type_of(first).is_unsigned(),
                )?
            {
                vec_eval::put(result, i, EvalType::Int, &Datum::Int(1))?;
            }
        }
        cache
            .non_const_args
            .iter()
            .map(|index| &function.args[*index])
            .collect()
    } else {
        function.args[1..].iter().collect()
    };
    for candidate in candidates {
        let right = vec_eval::argument(candidate, domain, ctx, input)?;
        for (i, has_null) in has_null.iter_mut().enumerate() {
            if result.get_int64(i) != 0 {
                continue;
            }
            if left.is_null(i) || right.is_null(i) {
                *has_null = true;
                continue;
            }
            let equal = crate::ops::eval_binary_full(
                BinaryOp::Eq,
                vec_eval::value(&left, i, domain, first.static_type()),
                vec_eval::value(&right, i, domain, candidate.static_type()),
                ctx.div_precision_increment(),
                function.derived_collation(),
                crate::ops::Operands::of(first, candidate),
                ctx,
            )?;
            if equal == Datum::Int(1) {
                vec_eval::put(result, i, EvalType::Int, &equal)?;
            }
        }
    }
    for (i, has_null) in has_null.into_iter().enumerate() {
        if result.is_null(i) {
            result.set_null(i, has_null || left.is_null(i));
        }
    }
    Ok(())
}
