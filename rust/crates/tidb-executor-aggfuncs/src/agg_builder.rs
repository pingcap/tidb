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

//! Go `pkg/executor/aggfuncs/builder.go`: the `AggFunc` an aggregate
//! descriptor runs as, chosen by its name, mode and argument eval type.

use tidb_datatype::{EvalType, FieldTypeCode};
use tidb_expr::expression::Expression;
use tidb_expression_aggregation::AggFuncDesc;
use tidb_expression_aggregation::AggFunctionMode;

use tidb_exprctx::Columns;

use crate::agg_func::{AggFunc, BaseAggFunc};
use crate::func_bitfuncs::{BaseBitAggFunc, BitAndUint64, BitOrUint64, BitXorUint64};
use crate::func_first_row::{FirstRow, FirstRowKind};
use crate::func_group_concat::{
    BaseGroupConcat4String, GroupConcat, GroupConcatDistinctOrder, GroupConcatOrder,
    GroupConcatOrderBase, GroupOriginalConcatDistinct, GroupPartialConcatDistinct,
};
use crate::func_stddevpop::{StdDevPop4Float64, StdDevPopOriginal4DistinctFloat64, StdDevPopPartial4DistinctFloat64};
use crate::func_stddevsamp::{StddevSamp4Float64, StddevSampOriginal4DistinctFloat64, StddevSampPartial4DistinctFloat64};
use crate::func_varpop::{BaseVarPopAggFunc, VarPop4Float64, VarPopOriginal4DistinctFloat64, VarPopPartial4DistinctFloat64};
use crate::func_varsamp::{VarSamp4Float64, VarSampOriginal4DistinctFloat64, VarSampPartial4DistinctFloat64};
use crate::func_max_min::{MaxMin, MaxMinKind};
use crate::func_avg::{
    AvgOriginal4Decimal, AvgOriginal4DistinctDecimal, AvgOriginal4DistinctFloat64,
    AvgOriginal4Float64, AvgOriginal4Float64HighPrecision, AvgPartial4Decimal,
    AvgPartial4DistinctDecimal, AvgPartial4DistinctFloat64, AvgPartial4Float64, BaseAvgDecimal,
    BaseAvgDistinct4Decimal, BaseAvgDistinct4Float64, BaseAvgFloat64,
};
use crate::func_sum::{
    BaseSum4Float64, BaseSumDistinct4Decimal, BaseSumDistinct4Float64, Sum4Decimal, Sum4Float64,
    Sum4Float64HighPrecision, Sum4OriginalDistinct4Decimal, Sum4OriginalDistinct4Float64,
    Sum4PartialDistinct4Decimal, Sum4PartialDistinctFloat64,
};
use crate::func_sum_int::{BaseSumIntAggFunc, SumDistinctInt64, SumDistinctUint64, SumInt, SumUint};
use crate::func_count::{
    BaseCount, CountOriginal4Decimal, CountOriginal4Duration, CountOriginal4Int,
    CountOriginal4JSON, CountOriginal4Real, CountOriginal4String, CountOriginal4Time,
    CountOriginal4VectorFloat32, CountPartial,
};
use crate::func_count_distinct::{
    BaseCountDistinct, BaseCountDistinct4Decimal, BaseCountDistinct4Duration,
    BaseCountDistinct4Int, BaseCountDistinct4MultiArgs, BaseCountDistinct4Real,
    BaseCountDistinct4String, CountOriginalWithDistinct, CountOriginalWithDistinct4Decimal,
    CountOriginalWithDistinct4Duration, CountOriginalWithDistinct4Int,
    CountOriginalWithDistinct4Real, CountOriginalWithDistinct4String, CountPartialWithDistinct,
    CountPartialWithDistinct4Decimal, CountPartialWithDistinct4Duration,
    CountPartialWithDistinct4Int, CountPartialWithDistinct4Real, CountPartialWithDistinct4String,
};

/// Go `buildVarPop`, `buildStdDevPop`, `buildVarSamp` and
/// `buildStddevSamp`: the same choice over each function's three types.
macro_rules! build_var_pop {
    ($desc:expr, $ordinal:expr, $original:ident, $partial:ident, $plain:ident) => {{
        let base = BaseVarPopAggFunc { base: base_agg_func($desc, $ordinal) };
        let func: Option<Box<dyn AggFunc>> = if $desc.has_distinct {
            match $desc.mode {
                AggFunctionMode::Complete | AggFunctionMode::Partial1 => Some(Box::new($original(base))),
                AggFunctionMode::Final | AggFunctionMode::Partial2 => Some(Box::new($partial(base))),
                AggFunctionMode::Dedup => None,
            }
        } else {
            match $desc.mode {
                AggFunctionMode::Dedup => None,
                _ => Some(Box::new($plain(base))),
            }
        };
        func
    }};
}

/// Go `Build`: the `AggFunc` for `agg_func_desc`, writing its final result
/// to column `ordinal`, or `None` (Go `nil`) for one not implemented.
/// `ctx` is Go's `AggFuncBuildContext`.
#[must_use]
pub fn build(ctx: &dyn Columns, agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    match agg_func_desc.base.name.as_str() {
        "count" => build_count(agg_func_desc, ordinal),
        "sum" => build_sum(ctx, agg_func_desc, ordinal),
        "sum_int" => build_sum_int(agg_func_desc, ordinal),
        "avg" => build_avg(ctx, agg_func_desc, ordinal),
        "firstrow" => build_first_row(agg_func_desc, ordinal),
        "max" => build_max_min(agg_func_desc, ordinal, true),
        "min" => build_max_min(agg_func_desc, ordinal, false),
        "group_concat" => build_group_concat(ctx, agg_func_desc, ordinal),
        // Go `buildJSONArrayagg` and `buildJSONObjectAgg`.
        "json_arrayagg" | "json_objectagg" if agg_func_desc.mode == AggFunctionMode::Dedup => None,
        "json_arrayagg" => Some(Box::new(crate::func_json_arrayagg::JsonArrayagg { base: base_agg_func(agg_func_desc, ordinal) })),
        "json_objectagg" => Some(Box::new(crate::func_json_objectagg::JsonObjectAgg { base: base_agg_func(agg_func_desc, ordinal) })),
        "bit_or" => Some(Box::new(BitOrUint64(BaseBitAggFunc { base: base_agg_func(agg_func_desc, ordinal) }))),
        "bit_xor" => Some(Box::new(BitXorUint64(BaseBitAggFunc { base: base_agg_func(agg_func_desc, ordinal) }))),
        "bit_and" => Some(Box::new(BitAndUint64(BaseBitAggFunc { base: base_agg_func(agg_func_desc, ordinal) }))),
        "var_pop" => build_var_pop!(agg_func_desc, ordinal, VarPopOriginal4DistinctFloat64, VarPopPartial4DistinctFloat64, VarPop4Float64),
        "stddev_pop" => build_var_pop!(agg_func_desc, ordinal, StdDevPopOriginal4DistinctFloat64, StdDevPopPartial4DistinctFloat64, StdDevPop4Float64),
        "var_samp" => build_var_pop!(agg_func_desc, ordinal, VarSampOriginal4DistinctFloat64, VarSampPartial4DistinctFloat64, VarSamp4Float64),
        "stddev_samp" => build_var_pop!(agg_func_desc, ordinal, StddevSampOriginal4DistinctFloat64, StddevSampPartial4DistinctFloat64, StddevSamp4Float64),
        _ => None,
    }
}

fn base_agg_func(agg_func_desc: &AggFuncDesc, ordinal: usize) -> BaseAggFunc {
    BaseAggFunc {
        args: agg_func_desc.base.args.clone(),
        ordinal,
        ret_tp: agg_func_desc.base.ret_type.clone(),
    }
}

/// Go `Args[0].GetType(ctx).EvalType()`.
fn arg_eval_type(arg: &Expression) -> Option<EvalType> {
    arg.static_type().map(tidb_datatype::FieldType::eval_type)
}

/// Go `buildCount`.
fn build_count(agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    // If mode is DedupMode, we return nil for not implemented.
    if agg_func_desc.mode == AggFunctionMode::Dedup {
        return None;
    }
    let base = base_agg_func(agg_func_desc, ordinal);
    let first_eval_type = base.args.first().and_then(arg_eval_type);

    // If HasDistinct and mode is CompleteMode or Partial1Mode, we should use
    // countOriginalWithDistinct.
    if agg_func_desc.has_distinct {
        let distinct = BaseCountDistinct { base };
        let single = distinct.base.args.len() == 1;
        return Some(match agg_func_desc.mode {
            AggFunctionMode::Complete | AggFunctionMode::Partial1 => match first_eval_type {
                Some(EvalType::Int) if single => {
                    Box::new(CountOriginalWithDistinct4Int(BaseCountDistinct4Int(distinct)))
                }
                Some(EvalType::Real) if single => {
                    Box::new(CountOriginalWithDistinct4Real(BaseCountDistinct4Real(distinct)))
                }
                Some(EvalType::Decimal) if single => Box::new(CountOriginalWithDistinct4Decimal(
                    BaseCountDistinct4Decimal(distinct),
                )),
                Some(EvalType::Duration) if single => Box::new(CountOriginalWithDistinct4Duration(
                    BaseCountDistinct4Duration(distinct),
                )),
                Some(EvalType::String) if single => {
                    Box::new(CountOriginalWithDistinct4String(BaseCountDistinct4String(distinct)))
                }
                _ => Box::new(CountOriginalWithDistinct(BaseCountDistinct4MultiArgs(distinct))),
            },
            AggFunctionMode::Final | AggFunctionMode::Partial2 => match first_eval_type {
                Some(EvalType::Int) if single => {
                    Box::new(CountPartialWithDistinct4Int(BaseCountDistinct4Int(distinct)))
                }
                Some(EvalType::Real) if single => {
                    Box::new(CountPartialWithDistinct4Real(BaseCountDistinct4Real(distinct)))
                }
                Some(EvalType::Decimal) if single => Box::new(CountPartialWithDistinct4Decimal(
                    BaseCountDistinct4Decimal(distinct),
                )),
                Some(EvalType::Duration) if single => Box::new(CountPartialWithDistinct4Duration(
                    BaseCountDistinct4Duration(distinct),
                )),
                Some(EvalType::String) if single => {
                    Box::new(CountPartialWithDistinct4String(BaseCountDistinct4String(distinct)))
                }
                _ => Box::new(CountPartialWithDistinct(BaseCountDistinct4MultiArgs(distinct))),
            },
            AggFunctionMode::Dedup => panic!("Not implemented"),
        });
    }

    let count = BaseCount { base };
    match agg_func_desc.mode {
        AggFunctionMode::Complete | AggFunctionMode::Partial1 => match first_eval_type? {
            EvalType::Int => Some(Box::new(CountOriginal4Int(count))),
            EvalType::Real => Some(Box::new(CountOriginal4Real(count))),
            EvalType::Decimal => Some(Box::new(CountOriginal4Decimal(count))),
            EvalType::Timestamp | EvalType::Datetime => Some(Box::new(CountOriginal4Time(count))),
            EvalType::Duration => Some(Box::new(CountOriginal4Duration(count))),
            EvalType::Json => Some(Box::new(CountOriginal4JSON(count))),
            EvalType::VectorFloat32 => Some(Box::new(CountOriginal4VectorFloat32(count))),
            EvalType::String => Some(Box::new(CountOriginal4String(count))),
            #[allow(unreachable_patterns)]
            _ => None,
        },
        AggFunctionMode::Partial2 | AggFunctionMode::Final => Some(Box::new(CountPartial(count))),
        AggFunctionMode::Dedup => None,
    }
}

/// Go `buildSum`.
fn build_sum(ctx: &dyn Columns, agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    let base = base_agg_func(agg_func_desc, ordinal);
    let decimal = agg_func_desc.base.ret_type.eval_type() == EvalType::Decimal;
    if agg_func_desc.has_distinct {
        return match agg_func_desc.mode {
            AggFunctionMode::Complete | AggFunctionMode::Partial1 if decimal => {
                Some(Box::new(Sum4OriginalDistinct4Decimal(BaseSumDistinct4Decimal { base })))
            }
            AggFunctionMode::Complete | AggFunctionMode::Partial1 => {
                Some(Box::new(Sum4OriginalDistinct4Float64(BaseSumDistinct4Float64 { base })))
            }
            AggFunctionMode::Final | AggFunctionMode::Partial2 if decimal => {
                Some(Box::new(Sum4PartialDistinct4Decimal(BaseSumDistinct4Decimal { base })))
            }
            AggFunctionMode::Final | AggFunctionMode::Partial2 => {
                Some(Box::new(Sum4PartialDistinctFloat64(BaseSumDistinct4Float64 { base })))
            }
            AggFunctionMode::Dedup => None,
        };
    }
    match agg_func_desc.mode {
        AggFunctionMode::Dedup => None,
        _ if decimal => Some(Box::new(Sum4Decimal { base })),
        _ if ctx.windowing_use_high_precision() => {
            Some(Box::new(Sum4Float64HighPrecision(BaseSum4Float64 { base })))
        }
        _ => Some(Box::new(Sum4Float64(BaseSum4Float64 { base }))),
    }
}

/// Go `buildSumInt`.
fn build_sum_int(agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    let base = BaseSumIntAggFunc {
        base: base_agg_func(agg_func_desc, ordinal),
    };
    if agg_func_desc.mode == AggFunctionMode::Dedup {
        return None;
    }
    let unsigned = agg_func_desc.base.ret_type.is_unsigned();
    Some(match (unsigned, agg_func_desc.has_distinct) {
        (true, true) => Box::new(SumDistinctUint64(base)),
        (true, false) => Box::new(SumUint(base)),
        (false, true) => Box::new(SumDistinctInt64(base)),
        (false, false) => Box::new(SumInt(base)),
    })
}

/// Go `buildAvg`.
fn build_avg(ctx: &dyn Columns, agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    let base = base_agg_func(agg_func_desc, ordinal);
    let distinct = agg_func_desc.has_distinct;
    match agg_func_desc.mode {
        // Build avg functions which consume the original data and remove the
        // duplicated input of the same group.
        AggFunctionMode::Dedup => None,
        // Build avg functions which consume the original data and update
        // their partial results.
        AggFunctionMode::Complete | AggFunctionMode::Partial1 => {
            Some(match agg_func_desc.base.ret_type.eval_type() {
                EvalType::Decimal if distinct => Box::new(AvgOriginal4DistinctDecimal(BaseAvgDistinct4Decimal { base })),
                EvalType::Decimal => Box::new(AvgOriginal4Decimal(BaseAvgDecimal { base })),
                _ if distinct => Box::new(AvgOriginal4DistinctFloat64(BaseAvgDistinct4Float64 { base })),
                _ if ctx.windowing_use_high_precision() => {
                    Box::new(AvgOriginal4Float64HighPrecision(BaseAvgFloat64 { base }))
                }
                _ => Box::new(AvgOriginal4Float64(BaseAvgFloat64 { base })),
            })
        }
        // Build avg functions which consume the partial result of other avg
        // functions and update their partial results.
        AggFunctionMode::Partial2 | AggFunctionMode::Final => match agg_func_desc.base.ret_type.code() {
            FieldTypeCode::NewDecimal if distinct => Some(Box::new(AvgPartial4DistinctDecimal(BaseAvgDistinct4Decimal { base }))),
            FieldTypeCode::NewDecimal => Some(Box::new(AvgPartial4Decimal(BaseAvgDecimal { base }))),
            FieldTypeCode::Double if distinct => Some(Box::new(AvgPartial4DistinctFloat64(BaseAvgDistinct4Float64 { base }))),
            FieldTypeCode::Double => Some(Box::new(AvgPartial4Float64(BaseAvgFloat64 { base }))),
            _ => None,
        },
    }
}

/// Go `buildFirstRow`.
fn build_first_row(agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    let base = base_agg_func(agg_func_desc, ordinal);
    let field_type = &agg_func_desc.base.ret_type;
    let mut eval_type = field_type.eval_type();
    if field_type.code() == FieldTypeCode::Bit {
        eval_type = EvalType::String;
    }
    if agg_func_desc.mode == AggFunctionMode::Dedup {
        return None;
    }
    let kind = match field_type.code() {
        FieldTypeCode::Enum => FirstRowKind::Enum,
        FieldTypeCode::Set => FirstRowKind::Set,
        _ => match eval_type {
            EvalType::Int => FirstRowKind::Int,
            EvalType::Real => match field_type.code() {
                FieldTypeCode::Float => FirstRowKind::Float32,
                FieldTypeCode::Double => FirstRowKind::Float64,
                _ => return None,
            },
            EvalType::Decimal => FirstRowKind::Decimal,
            EvalType::Datetime | EvalType::Timestamp => FirstRowKind::Time,
            EvalType::Duration => FirstRowKind::Duration,
            EvalType::String => FirstRowKind::String,
            EvalType::Json => FirstRowKind::JSON,
            EvalType::VectorFloat32 => FirstRowKind::VectorFloat32,
            #[allow(unreachable_patterns)]
            _ => return None,
        },
    };
    Some(Box::new(FirstRow { base, kind }))
}

/// Go `buildMaxMin`.
fn build_max_min(agg_func_desc: &AggFuncDesc, ordinal: usize, is_max: bool) -> Option<Box<dyn AggFunc>> {
    let base = base_agg_func(agg_func_desc, ordinal);
    let field_type = &agg_func_desc.base.ret_type;
    let mut eval_type = field_type.eval_type();
    if field_type.code() == FieldTypeCode::Bit {
        eval_type = EvalType::String;
    }
    if agg_func_desc.mode == AggFunctionMode::Dedup {
        return None;
    }
    let kind = match field_type.code() {
        FieldTypeCode::Enum => MaxMinKind::Enum,
        FieldTypeCode::Set => MaxMinKind::Set,
        _ => match eval_type {
            EvalType::Int if field_type.is_unsigned() => MaxMinKind::Uint,
            EvalType::Int => MaxMinKind::Int,
            EvalType::Real => match field_type.code() {
                FieldTypeCode::Float => MaxMinKind::Float32,
                FieldTypeCode::Double => MaxMinKind::Float64,
                _ => return None,
            },
            EvalType::Decimal => MaxMinKind::Decimal,
            EvalType::String => MaxMinKind::String,
            EvalType::Datetime | EvalType::Timestamp => MaxMinKind::Time,
            EvalType::Duration => MaxMinKind::Duration,
            EvalType::Json => MaxMinKind::JSON,
            EvalType::VectorFloat32 => MaxMinKind::VectorFloat32,
            #[allow(unreachable_patterns)]
            _ => return None,
        },
    };
    // Go: a string compares under `args[0].GetType().GetCollate()`, an enum
    // or set under `collate.GetCollator(RetTp.GetCollate())`.
    let collation = if kind == MaxMinKind::String {
        crate::aggfuncs::expr_collation(&base.args[0])
    } else {
        tidb_charset::Collation::from_name(field_type.collation_name()).unwrap_or(tidb_charset::Collation::DEFAULT)
    };
    Some(Box::new(MaxMin { base, is_max, kind, collation }))
}

/// Go `buildGroupConcat`.
fn build_group_concat(ctx: &dyn Columns, agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    if agg_func_desc.mode == AggFunctionMode::Dedup {
        return None;
    }
    let args = &agg_func_desc.base.args;
    // The last argument is promised to be a not-null string constant.
    let Some(Expression::Constant(c)) = args.last() else {
        panic!("Error happened when buildGroupConcat: the separator is not a constant");
    };
    let sep = c.value.sql_bytes().expect("Error happened when buildGroupConcat");
    let base = BaseGroupConcat4String {
        base: BaseAggFunc {
            args: args[..args.len() - 1].to_vec(),
            ordinal,
            ret_tp: agg_func_desc.base.ret_type.clone(),
        },
        by_items: agg_func_desc.order_by_items.clone(),
        sep,
        max_len: crate::aggfuncs::group_concat_max_len(ctx),
        truncated: std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0)),
    };
    let order_base = |base: BaseGroupConcat4String| GroupConcatOrderBase {
        desc: base.by_items.iter().map(|by_item| by_item.desc).collect(),
        ctors: base.by_items.iter().map(|by_item| crate::aggfuncs::expr_collation(&by_item.expr)).collect(),
        base,
    };
    if agg_func_desc.has_distinct {
        if !agg_func_desc.order_by_items.is_empty() {
            return Some(Box::new(GroupConcatDistinctOrder(order_base(base))));
        }
        return match agg_func_desc.mode {
            AggFunctionMode::Complete | AggFunctionMode::Partial1 => Some(Box::new(GroupOriginalConcatDistinct(base))),
            AggFunctionMode::Final | AggFunctionMode::Partial2 => Some(Box::new(GroupPartialConcatDistinct(base))),
            AggFunctionMode::Dedup => None,
        };
    }
    if !agg_func_desc.order_by_items.is_empty() {
        return Some(Box::new(GroupConcatOrder(order_base(base))));
    }
    Some(Box::new(GroupConcat(base)))
}
