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

use tidb_datatype::EvalType;
use tidb_expr::expression::Expression;
use tidb_expression_aggregation::AggFuncDesc;
use tidb_expression_aggregation::AggFunctionMode;

use crate::agg_func::{AggFunc, BaseAggFunc};
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

/// Go `Build`: the `AggFunc` for `agg_func_desc`, writing its final result
/// to column `ordinal`, or `None` (Go `nil`) for one not implemented.
#[must_use]
pub fn build(agg_func_desc: &AggFuncDesc, ordinal: usize) -> Option<Box<dyn AggFunc>> {
    match agg_func_desc.base.name.as_str() {
        "count" => build_count(agg_func_desc, ordinal),
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
