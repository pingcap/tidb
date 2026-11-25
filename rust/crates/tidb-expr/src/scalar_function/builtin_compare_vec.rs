// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_compare_vec{,_generated}.go` and `builtin_other_vec_generated.go`.

use super::{Kernel, ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use std::cmp::Ordering;
use tidb_ast::BinaryOp;
use tidb_chunk::{
    chunk::Chunk,
    column::{Column, RawCells},
};
use tidb_datatype::{Collator, Datum, EvalType, MYDECIMAL_STRUCT_SIZE, MyDecimal};
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
    let wanted = if greatest {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    for j in usize::from(!allocate)..function.args.len() {
        let buffer =
            vec_eval::read_argument(&function.args[j], domain, ctx, input)?.into_contiguous();
        result.merge_nulls(&[&buffer]);
        let source = buffer.raw_cells();
        match domain {
            // Go's integer extrema always compare the signed carriers.
            EvalType::Int => replace_extremum(result, source.fixed_cells(), wanted, |a, b| {
                Some(i64::from_ne_bytes(a).cmp(&i64::from_ne_bytes(b)))
            }),
            // Go's extrema use < and >, retaining the first argument on NaN
            // and on equality (including signed zero).
            EvalType::Real => replace_extremum(result, source.fixed_cells(), wanted, |a, b| {
                f64::from_ne_bytes(a).partial_cmp(&f64::from_ne_bytes(b))
            }),
            EvalType::Decimal => replace_extremum(result, source.fixed_cells(), wanted, |a, b| {
                Some(decimal_cell(a).compare(&decimal_cell(b)))
            }),
            _ => {}
        }
        if matches!(domain, EvalType::Int | EvalType::Real | EvalType::Decimal) {
            continue;
        }
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

fn replace_extremum<const N: usize>(
    result: &mut Column,
    candidates: &[[u8; N]],
    wanted: Ordering,
    compare: impl Fn([u8; N], [u8; N]) -> Option<Ordering>,
) {
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (cell, &candidate)) in cells.iter_mut().zip(candidates).enumerate() {
            if bitmap[row >> 3] & (1 << (row & 7)) != 0 && compare(candidate, *cell) == Some(wanted)
            {
                *cell = candidate;
            }
        }
    });
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
    if native_domain(domain) {
        let left = vec_eval::read_argument(&function.args[0], domain, ctx, input)?;
        let int_null_eq = domain == EvalType::Int && op == BinaryOp::NullEq;
        if int_null_eq {
            result.resize_int64(input.num_rows(), false);
        }
        let right = vec_eval::read_argument(&function.args[1], domain, ctx, input)?;
        let (left, left_selection) = left.source();
        let (right, right_selection) = right.source();
        let (left_cells, right_cells) = (left.raw_cells(), right.raw_cells());
        let [a, b] = [0, 1].map(|i| crate::wrap_cast::type_of(&function.args[i]).is_unsigned());
        let left_values = PackedValues::new(domain, &left_cells, a);
        let right_values = PackedValues::new(domain, &right_cells, b);
        let collator = tidb_datatype::get_collator(function.derived_collation().name());
        if !int_null_eq {
            result.resize_int64(input.num_rows(), false);
        }
        let merge_nulls =
            op != BinaryOp::NullEq && left_selection.is_none() && right_selection.is_none();
        if merge_nulls {
            result.merge_nulls(&[left, right]);
        }
        result.with_fixed_cells_mut(|cells, bitmap| {
            for (row, cell) in cells.iter_mut().enumerate() {
                let left_row = left_selection.map_or(row, |selection| selection[row]);
                let right_row = right_selection.map_or(row, |selection| selection[row]);
                let bit = 1 << (row & 7);
                if merge_nulls {
                    if bitmap[row >> 3] & bit == 0 {
                        continue;
                    }
                } else {
                    let (a_null, b_null) =
                        (left_cells.is_null(left_row), right_cells.is_null(right_row));
                    if a_null || b_null {
                        if op == BinaryOp::NullEq {
                            *cell = i64::from(a_null && b_null).to_ne_bytes();
                        } else {
                            bitmap[row >> 3] &= !bit;
                        }
                        continue;
                    }
                }
                let order = left_values.compare(right_values, left_row, right_row, collator);
                let matches = match op {
                    BinaryOp::Eq | BinaryOp::NullEq => order.is_eq(),
                    BinaryOp::Ne => order.is_ne(),
                    BinaryOp::Lt => order.is_lt(),
                    BinaryOp::Le => order.is_le(),
                    BinaryOp::Gt => order.is_gt(),
                    BinaryOp::Ge => order.is_ge(),
                    _ => unreachable!(),
                };
                *cell = i64::from(matches).to_ne_bytes();
            }
        });
        return Ok(());
    }
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

fn native_domain(domain: EvalType) -> bool {
    matches!(
        domain,
        EvalType::Int | EvalType::Real | EvalType::Decimal | EvalType::String
    )
}

#[derive(Clone, Copy)]
enum PackedValues<'a> {
    Int(&'a [[u8; 8]], bool),
    Real(&'a [[u8; 8]]),
    Decimal(&'a [[u8; MYDECIMAL_STRUCT_SIZE]]),
    String(&'a RawCells<'a>),
}

impl<'a> PackedValues<'a> {
    fn new(domain: EvalType, cells: &'a RawCells<'a>, unsigned: bool) -> Self {
        match domain {
            EvalType::Int => Self::Int(cells.fixed_cells(), unsigned),
            EvalType::Real => Self::Real(cells.fixed_cells()),
            EvalType::Decimal => Self::Decimal(cells.fixed_cells()),
            EvalType::String => Self::String(cells),
            _ => unreachable!(),
        }
    }

    #[inline]
    fn compare(self, other: Self, left: usize, right: usize, collator: Collator) -> Ordering {
        use crate::coerce::{Integer, integer_cmp};
        let integer = |cell, unsigned| {
            if unsigned {
                Integer::Unsigned(u64::from_ne_bytes(cell))
            } else {
                Integer::Signed(i64::from_ne_bytes(cell))
            }
        };
        match (self, other) {
            (Self::Int(a, ua), Self::Int(b, ub)) => {
                integer_cmp(integer(a[left], ua), integer(b[right], ub))
            }
            (Self::Real(a), Self::Real(b)) => {
                compare_real(f64::from_ne_bytes(a[left]), f64::from_ne_bytes(b[right]))
            }
            (Self::Decimal(a), Self::Decimal(b)) => {
                decimal_cell(a[left]).compare(&decimal_cell(b[right]))
            }
            (Self::String(a), Self::String(b)) => collator.compare(a.get(left), b.get(right)),
            _ => unreachable!(),
        }
    }

    fn contains(
        self,
        row: usize,
        cache: &crate::builtin_other::InHashSet,
        collator: Collator,
    ) -> Result<bool, EvalError> {
        Ok(match self {
            Self::Int(cells, unsigned) => {
                cache.contains_int(i64::from_ne_bytes(cells[row]), unsigned)
            }
            Self::Real(cells) => cache.contains_real(f64::from_ne_bytes(cells[row])),
            Self::Decimal(cells) => return cache.contains_decimal(&decimal_cell(cells[row])),
            Self::String(cells) => cache.contains_string(cells.get(row), collator),
        })
    }
}

/// Go cmp.Compare orders NaN before every numeric value, and equates NaNs
/// with each other and positive zero with negative zero.
#[inline]
fn compare_real(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b)
        .unwrap_or_else(|| b.is_nan().cmp(&a.is_nan()))
}

fn decimal_cell(bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> MyDecimal {
    MyDecimal::from_raw_bytes(bytes).expect("chunk decimal cell holds a valid MyDecimal")
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
    let left = vec_eval::read_argument(first, domain, ctx, input)?;
    let (left, left_selection) = left.source();
    let left_row = |row| left_selection.map_or(row, |selection| selection[row]);
    let left_cells = left.raw_cells();
    let unsigned = crate::wrap_cast::type_of(first).is_unsigned();
    let lhs = native_domain(domain).then(|| PackedValues::new(domain, &left_cells, unsigned));
    let collation = function.derived_collation();
    let collator = tidb_datatype::get_collator(collation.name());
    let cache = function.in_cache.as_deref();
    let mut has_null = vec![cache.is_some_and(|cache| cache.has_null); input.num_rows()];
    result.resize_int64(input.num_rows(), false);
    result.set_nulls(0, input.num_rows(), true);
    let cached = cache.filter(|cache| !cache.is_empty());
    if let Some(cache) = cached {
        result.with_fixed_cells_mut(|cells, bitmap| {
            for (row, cell) in cells.iter_mut().enumerate() {
                let physical = left_row(row);
                if left_cells.is_null(physical) {
                    continue;
                }
                let matched = if let Some(lhs) = lhs {
                    lhs.contains(physical, cache, collator)?
                } else {
                    cache.contains(
                        &vec_eval::value(left, physical, domain, first.static_type()),
                        collator,
                        unsigned,
                    )?
                };
                if matched {
                    *cell = 1_i64.to_ne_bytes();
                    bitmap[row >> 3] |= 1 << (row & 7);
                }
            }
            Ok::<_, EvalError>(())
        })?;
    }
    let mut args = 1..function.args.len();
    let mut candidates = cached.map(|cache| cache.non_const_args.iter().copied());
    while let Some(index) = match &mut candidates {
        Some(indices) => indices.next(),
        None => args.next(),
    } {
        let candidate = &function.args[index];
        let right = vec_eval::read_argument(candidate, domain, ctx, input)?;
        let (right, right_selection) = right.source();
        let right_cells = right.raw_cells();
        let rhs = lhs.map(|_| {
            PackedValues::new(
                domain,
                &right_cells,
                crate::wrap_cast::type_of(candidate).is_unsigned(),
            )
        });
        result.with_fixed_cells_mut(|cells, bitmap| {
            for (row, (cell, has_null)) in cells.iter_mut().zip(&mut has_null).enumerate() {
                if i64::from_ne_bytes(*cell) != 0 {
                    continue;
                }
                let a = left_row(row);
                let b = right_selection.map_or(row, |selection| selection[row]);
                if left_cells.is_null(a) || right_cells.is_null(b) {
                    *has_null = true;
                    continue;
                }
                let matched = if let (Some(lhs), Some(rhs)) = (lhs, rhs) {
                    lhs.compare(rhs, a, b, collator).is_eq()
                } else {
                    crate::ops::eval_binary_full(
                        BinaryOp::Eq,
                        vec_eval::value(left, a, domain, first.static_type()),
                        vec_eval::value(right, b, domain, candidate.static_type()),
                        ctx.div_precision_increment(),
                        collation,
                        crate::ops::Operands::of(first, candidate),
                        ctx,
                    )? == Datum::Int(1)
                };
                if matched {
                    *cell = 1_i64.to_ne_bytes();
                    bitmap[row >> 3] |= 1 << (row & 7);
                }
            }
            Ok::<_, EvalError>(())
        })?;
    }
    for (row, has_null) in has_null.into_iter().enumerate() {
        if !has_null && !left_cells.is_null(left_row(row)) {
            result.set_null(row, false);
        }
    }
    Ok(())
}
