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

//! Go `NewDistAggFunc` and its per-group update/partial-result lifecycle.
//! This is the mock coprocessor evaluator, distinct from executor/aggfuncs.

use tidb_datatype::RuntimeCollator;
use super::{names, AggFuncDesc, AggFunctionMode, BaseFuncDesc};
use tidb_exprctx::Columns;
use tidb_exprctx::EvalError;
use tidb_chunk::row::Row;
use tidb_datatype::{ConversionContext, ConversionLocation, Datum, Decimal, DecimalCodecWarning, EvalType, FieldType, FieldTypeCode, SessionTimeZone};
use tidb_charset::Collation;
use tidb_proto::tipb;

/// A decoded distributed aggregate. The descriptor retains typed arguments and
/// mode; group lookup and row storage belong to the coprocessor.
#[derive(Debug)]
pub struct DistAggregate {
    descriptor: AggFuncDesc,
    kind: tipb::ExprType,
    collation: Collation,
}

/// Go `AggEvaluateContext`, excluding GROUP_CONCAT and DISTINCT state: Go's
/// distributed factory does not enable DISTINCT and TiKV rejects GROUP_CONCAT.
#[derive(Debug)]
pub struct DistAggregateState {
    count: i64,
    value: Datum,
    got_first_row: bool,
}

impl DistAggregate {
    /// Decode Go's distributed factory input without re-running SQL type
    /// inference or inserting casts into the already lowered argument tree.
    pub fn from_pb(
        expr: &tipb::Expr,
        columns: &[FieldType],
        zone: &SessionTimeZone,
    ) -> Result<Self, String> {
        let args = expr
            .children
            .iter()
            .map(|arg| tidb_expr::distsql_builtin::pb_to_expr_in(arg, columns, zone))
            .collect::<Result<Vec<_>, _>>()?;
        let kind = tipb::ExprType::try_from(expr.tp.unwrap_or_default())
            .map_err(|_| "unknown aggregate function type".to_owned())?;
        // PBToAggFuncDesc uses Partial1 for old senders without this field.
        let wire_mode = tipb::AggFunctionMode::try_from(
            expr.agg_func_mode
                .unwrap_or(tipb::AggFunctionMode::Partial1Mode as i32),
        )
        .map_err(|_| "unknown aggregate function mode".to_owned())?;
        let mode = match wire_mode {
            tipb::AggFunctionMode::CompleteMode => AggFunctionMode::Complete,
            tipb::AggFunctionMode::FinalMode => AggFunctionMode::Final,
            tipb::AggFunctionMode::Partial1Mode => AggFunctionMode::Partial1,
            tipb::AggFunctionMode::Partial2Mode => AggFunctionMode::Partial2,
            tipb::AggFunctionMode::DedupMode => AggFunctionMode::Dedup,
        };
        Self::new(kind, args, mode)
    }

    /// Go `NewDistAggFunc` over already typed arguments: the function `kind`
    /// names, in `mode`.
    pub fn new(
        kind: tipb::ExprType,
        args: Vec<tidb_expr::expression::Expression>,
        mode: AggFunctionMode,
    ) -> Result<Self, String> {
        use tipb::ExprType::*;
        let name = match kind {
            Count => names::COUNT,
            Sum => names::SUM,
            SumInt => names::SUM_INT,
            Avg => names::AVG,
            Min => names::MIN,
            Max => names::MAX,
            First => names::FIRST_ROW,
            AggBitOr => names::BIT_OR,
            AggBitXor => names::BIT_XOR,
            AggBitAnd => names::BIT_AND,
            _ => return Err(format!("unsupported distributed aggregate {kind:?}")),
        };
        if kind == Avg && mode == AggFunctionMode::Dedup {
            return Err("DedupMode is not supported for AVG".to_owned());
        }
        let partial_input = matches!(mode, AggFunctionMode::Final | AggFunctionMode::Partial2);
        let required = if kind == Avg && partial_input { 2 } else { 1 };
        if kind != Count && args.len() != required {
            return Err(format!(
                "{kind:?} requires {required} arguments in {mode:?} mode"
            ));
        }
        let collation = args
            .first()
            .and_then(|arg| arg.static_type())
            .and_then(|field| field.runtime_collator().new_collation())
            .unwrap_or(Collation::Binary);
        Ok(Self {
            descriptor: AggFuncDesc {
                // newAggFunc leaves RetTp nil. Partial results do not use it;
                // Unspecified preserves that absence in the value descriptor.
                base: BaseFuncDesc::from_parts(
                    name.to_owned(),
                    args,
                    FieldType::new(FieldTypeCode::Unspecified),
                ),
                mode,
                has_distinct: false,
                order_by_items: Vec::new(),
                grouping_id: 0,
            },
            kind,
            collation,
        })
    }

    /// Allocate one group's state (Go CreateContext).
    pub fn create_state(&self) -> DistAggregateState {
        use tipb::ExprType::*;
        DistAggregateState {
            count: 0,
            value: match self.kind {
                AggBitAnd => Datum::UInt(u64::MAX),
                AggBitOr | AggBitXor => Datum::UInt(0),
                _ => Datum::Null,
            },
            got_first_row: false,
        }
    }

    /// Preserve each signature's evaluation order, including NULL short circuits.
    pub fn update(
        &self,
        state: &mut DistAggregateState,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<(), DistAggregateError> {
        use tipb::ExprType::*;
        let partial_input = matches!(
            self.descriptor.mode,
            AggFunctionMode::Final | AggFunctionMode::Partial2
        );
        let args = self.descriptor.args();
        if self.kind == Count {
            for arg in args {
                let value = arg.eval(ctx, row)?;
                if value.is_null() {
                    return Ok(());
                }
                if partial_input {
                    state.count = state.count.wrapping_add(partial_count(&value)?);
                }
            }
            if matches!(
                self.descriptor.mode,
                AggFunctionMode::Complete | AggFunctionMode::Partial1
            ) {
                state.count = state.count.wrapping_add(1);
            }
            return Ok(());
        }
        if self.kind == First && state.got_first_row {
            return Ok(());
        }
        let warnings = tidb_expr::constant::ConversionWarnings(ctx);
        let zone = ctx.time_zone();
        let context = ConversionContext::new(
            ctx.type_flags(),
            ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        let input = if self.kind == SumInt {
            tidb_expr::scalar_function::eval_numeric_row(&args[0], ctx, row, EvalType::Int)?
        } else {
            args[usize::from(self.kind == Avg && partial_input)].eval(ctx, row)?
        };
        if self.kind == First {
            state.value = input;
            state.got_first_row = true;
            return Ok(());
        }
        if input.is_null() {
            return Ok(());
        }
        match self.kind {
            Sum | Avg => {
                state.value = calculate_sum(&state.value, input, &context)?;
                let count = if self.kind == Avg && partial_input {
                    partial_count(&args[0].eval(ctx, row)?)?
                } else {
                    1
                };
                state.count = state.count.wrapping_add(count);
            }
            SumInt => {
                let bits = partial_count(&input)?;
                let input = if args[0].static_type().is_some_and(FieldType::is_unsigned) {
                    Datum::UInt(bits as u64)
                } else {
                    Datum::Int(bits)
                };
                state.value = if state.value.is_null() {
                    input
                } else {
                    tidb_datatype::compute_plus(&state.value, &input)
                        .map_err(DistAggregateError::Arithmetic)?
                };
                state.count = state.count.wrapping_add(1);
            }
            Min | Max => {
                if state.value.is_null() {
                    state.value = input;
                } else {
                    let (ordering, error) =
                        state
                            .value
                            .compare_with_context(&input, self.collation, &context, &zone);
                    if let Some(error) = error {
                        return Err(DistAggregateError::Datum(error));
                    }
                    if (self.kind == Max && ordering.is_lt())
                        || (self.kind == Min && ordering.is_gt())
                    {
                        state.value = input;
                    }
                }
            }
            AggBitOr | AggBitXor | AggBitAnd => {
                let bits = match input {
                    Datum::UInt(value) => value,
                    value => {
                        partial_count(&convert(&value, FieldTypeCode::LongLong, &context, &zone)?)?
                            as u64
                    }
                };
                let Datum::UInt(current) = state.value else {
                    unreachable!("bit aggregate state")
                };
                state.value = Datum::UInt(match self.kind {
                    AggBitOr => current | bits,
                    AggBitXor => current ^ bits,
                    _ => current & bits,
                });
            }
            _ => unreachable!("validated distributed aggregate"),
        }
        Ok(())
    }

    /// Go GetPartialResult: AVG always emits count then sum, even in FinalMode.
    pub fn partial_result(&self, state: DistAggregateState) -> Vec<Datum> {
        match self.kind {
            tipb::ExprType::Count => vec![Datum::Int(state.count)],
            tipb::ExprType::Avg => vec![Datum::Int(state.count), state.value],
            _ => vec![state.value],
        }
    }
}

/// Go `aggExec` (unistore `mpp_exec.go`): the distributed aggregates grouped
/// by `group_by`, one group per distinct `EncodeValue` key in the order the
/// keys first appear, each answering its functions' partial results and then
/// its keys.
#[derive(Debug)]
pub struct DistAggregator {
    group_by: Vec<tidb_expr::expression::Expression>,
    functions: Vec<DistAggregate>,
    row: tidb_chunk::mutrow::MutRow,
    group_indices: std::collections::HashMap<Vec<u8>, usize>,
    groups: Vec<(Vec<Datum>, Vec<DistAggregateState>)>,
}

impl DistAggregator {
    /// An aggregator over rows of `column_types`.
    #[must_use]
    pub fn new(
        group_by: Vec<tidb_expr::expression::Expression>,
        functions: Vec<DistAggregate>,
        column_types: &[FieldType],
    ) -> Self {
        Self {
            group_by,
            functions,
            row: tidb_chunk::mutrow::MutRow::mut_row_from_types(column_types),
            group_indices: std::collections::HashMap::new(),
            groups: Vec::new(),
        }
    }

    /// Go `aggExec.processAllRows` for one row: its group's key (Go
    /// `getGroupKey`: `EncodeValue` of the evaluated keys, raw string bytes
    /// and the request zone, no collation folding), then every function.
    pub fn update(
        &mut self,
        ctx: &dyn Columns,
        zone: &SessionTimeZone,
        row: &[Datum],
    ) -> Result<(), String> {
        if row.len() != self.row.len() {
            return Err("aggregate row does not match input schema".to_owned());
        }
        self.row.set_datums(row);
        let keys = self
            .group_by
            .iter()
            .map(|expr| {
                expr.eval(ctx, self.row.to_row())
                    .map_err(|error| DistAggregateError::from(error).to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (key, err) = tidb_codec::encode_value_in_timezone_with_error(zone, &keys);
        // Go `err = sc.HandleError(err)`: a truncated decimal key follows the
        // request's truncate level.
        match err {
            Ok(()) => {}
            Err(tidb_codec::CodecError::DecimalTruncated) => ctx
                .handle_truncate_error(tidb_datatype::types_errors::ERR_TRUNCATED.gen_with_stack_by_args(&[]))
                .map_err(|error| format!("encode group key failed: {error:?}"))?,
            Err(error) => return Err(format!("encode group key failed: {error:?}")),
        }
        let functions = &self.functions;
        let groups = &mut self.groups;
        let index = *self.group_indices.entry(key).or_insert_with(|| {
            groups.push((keys, functions.iter().map(DistAggregate::create_state).collect()));
            groups.len() - 1
        });
        for (function, value) in self.functions.iter().zip(self.groups[index].1.iter_mut()) {
            function
                .update(value, ctx, self.row.to_row())
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Every group's partial results, then its keys.
    #[must_use]
    pub fn finish(self) -> Vec<Vec<Datum>> {
        let functions = self.functions;
        self.groups
            .into_iter()
            .map(|(keys, values)| {
                functions
                    .iter()
                    .zip(values)
                    .flat_map(|(function, value)| function.partial_result(value))
                    .chain(keys)
                    .collect()
            })
            .collect()
    }
}

fn partial_count(value: &Datum) -> Result<i64, DistAggregateError> {
    match value {
        Datum::Null => Ok(0),
        Datum::Int(value) => Ok(*value),
        Datum::UInt(value) => Ok(*value as i64),
        _ => Err(EvalError::Unsupported("partial aggregate count requires an integer").into()),
    }
}

fn convert(
    value: &Datum,
    code: FieldTypeCode,
    context: &ConversionContext<'_>,
    zone: &SessionTimeZone,
) -> Result<Datum, DistAggregateError> {
    let result = value
        .convert_to_in_context(&FieldType::new(code), context, zone)
        .map_err(DistAggregateError::Datum)?;
    if let Some(error) = result.error {
        return Err(EvalError::Conversion(error).into());
    }
    Ok(result.value)
}

fn calculate_sum(
    sum: &Datum,
    value: Datum,
    context: &ConversionContext<'_>,
) -> Result<Datum, DistAggregateError> {
    let addend = match value {
        Datum::Int(value) => Datum::Decimal(Decimal::from_int(value)),
        Datum::UInt(value) => Datum::Decimal(Decimal::from_uint(value)),
        Datum::Decimal(_) => value,
        value => {
            let converted = value
                .to_f64_in_context(context)
                .map_err(DistAggregateError::Datum)?;
            if let Some(error) = converted.error {
                return Err(EvalError::Conversion(error).into());
            }
            converted.value
        }
    };
    if sum.is_null() {
        return Ok(addend);
    }
    if let (Datum::Decimal(left), Datum::Decimal(right)) = (sum, &addend) {
        let (sum, error) = left.add_mysql(right);
        if let Some(error) = error {
            return Err(EvalError::Conversion(match error {
                DecimalCodecWarning::Overflow => tidb_datatype::types_errors::ERR_OVERFLOW.clone(),
                DecimalCodecWarning::Truncated => tidb_datatype::types_errors::ERR_TRUNCATED.clone(),
            })
            .into());
        }
        return Ok(Datum::Decimal(sum));
    }
    tidb_datatype::compute_plus(sum, &addend).map_err(DistAggregateError::Arithmetic)
}

/// Retain the originating expression/conversion/arithmetic failure until the
/// coprocessor response boundary, without replacing it with a generic range error.
#[derive(Debug)]
pub enum DistAggregateError {
    /// Typed argument evaluation or conversion failure.
    Expression(EvalError),
    /// Datum comparison/conversion failure.
    Datum(tidb_datatype::DatumValueError),
    /// Checked numeric accumulation failure.
    Arithmetic(tidb_datatype::DatumArithmeticError),
}

impl From<EvalError> for DistAggregateError {
    fn from(error: EvalError) -> Self {
        Self::Expression(error)
    }
}

impl std::fmt::Display for DistAggregateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expression(EvalError::Conversion(error)) => error.fmt(f),
            Self::Expression(EvalError::TruncatedWrongValue(message)) => {
                tidb_parser_types::parser_types_errors::ERR_TRUNCATED_WRONG_VALUE
                    .generate(message)
                    .fmt(f)
            }
            Self::Expression(error) => write!(f, "{error:?}"),
            Self::Datum(error) => error.fmt(f),
            Self::Arithmetic(tidb_datatype::DatumArithmeticError::Overflow(error)) => {
                tidb_datatype::types_errors::ERR_OVERFLOW
                    .generate(error.to_string())
                    .fmt(f)
            }
            Self::Arithmetic(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for DistAggregateError {}
