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

//! Go `pkg/executor/aggfuncs/builder.go`: lower the planner's descriptor to
//! execution state without rerunning type inference or rebuilding its AST.

use super::{AggFunc, AggKind, BitOp};
use crate::ExecError;
use tidb_datatype::{Datum, EvalType, FieldTypeCode};
use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode};
use tidb_expr::expression::Expression;
use tidb_expr::{eval_expression_once, Columns};

impl AggFunc {
    /// Build one execution's aggregate from a resolved, type-inferred descriptor.
    pub fn from_descriptor(desc: &AggFuncDesc, ctx: &impl Columns) -> Result<Self, ExecError> {
        let name = desc.name();
        let merging = matches!(
            desc.mode,
            AggFunctionMode::Final | AggFunctionMode::Partial2
        );
        let unsupported_mode = || {
            ExecError::unsupported(format!(
                "aggregate {name} in {} mode requires unported partial-state encoding",
                desc.mode.as_str()
            ))
        };
        if desc.mode == AggFunctionMode::Dedup && !matches!(name, "bit_and" | "bit_or" | "bit_xor")
        {
            return Err(unsupported_mode());
        }
        // Go's DISTINCT merge functions exchange partial sets, not scalar
        // final values. Do not treat those sets as original argument rows.
        if desc.has_distinct
            && merging
            && matches!(
                name,
                "count"
                    | "sum"
                    | "avg"
                    | "group_concat"
                    | "var_pop"
                    | "var_samp"
                    | "stddev_pop"
                    | "stddev_samp"
            )
        {
            return Err(unsupported_mode());
        }
        let mut args = desc.args().to_vec();
        let arg_type = |index: usize| {
            desc.args()
                .get(index)
                .and_then(Expression::static_type)
                .cloned()
                .ok_or_else(|| {
                    ExecError::internal(format!("aggregate {name} argument {index} has no type"))
                })
        };
        let kind = match name {
            "count" if merging => AggKind::FinalCount,
            "count" => AggKind::Count,
            "sum" => AggKind::Sum,
            "avg" => AggKind::Avg,
            "firstrow" => AggKind::FirstRow,
            "min" => AggKind::Min,
            "max" => AggKind::Max,
            "bit_and" => AggKind::Bit(BitOp::And),
            "bit_or" => AggKind::Bit(BitOp::Or),
            "bit_xor" => AggKind::Bit(BitOp::Xor),
            "group_concat" => {
                let separator = args
                    .pop()
                    .ok_or_else(|| ExecError::internal("GROUP_CONCAT has no separator"))?;
                let value = eval_expression_once(&separator, ctx)?;
                let bytes = value
                    .as_raw_bytes()
                    .ok_or_else(|| ExecError::internal("GROUP_CONCAT separator is not a string"))?;
                let separator = String::from_utf8(bytes.to_vec()).map_err(|_| {
                    ExecError::unsupported(
                        "binary GROUP_CONCAT separators require a byte-valued runtime separator",
                    )
                })?;
                AggKind::GroupConcat { separator }
            }
            "var_pop" | "var_samp" | "stddev_pop" | "stddev_samp" => AggKind::Variance {
                sample: matches!(name, "var_samp" | "stddev_samp"),
                sqrt: matches!(name, "stddev_pop" | "stddev_samp"),
            },
            "json_arrayagg" => AggKind::JsonArrayAgg {
                value_type: arg_type(0)?,
            },
            "json_objectagg" => AggKind::JsonObjectAgg {
                value_type: arg_type(1)?,
                key_is_binary: arg_type(0)?.is_binary_string(),
            },
            "approx_count_distinct" => {
                if desc.mode != AggFunctionMode::Complete
                    || desc.ret_type().code() != FieldTypeCode::LongLong
                {
                    return Err(unsupported_mode());
                }
                AggKind::ApproxCountDistinct
            }
            "approx_percentile" => {
                if desc.mode == AggFunctionMode::Partial2 {
                    return Err(unsupported_mode());
                }
                let percentage = args
                    .pop()
                    .ok_or_else(|| ExecError::internal("APPROX_PERCENTILE has no percentage"))?;
                let value = eval_expression_once(&percentage, ctx)?;
                let percent = match value {
                    Datum::Int(value) => value,
                    Datum::UInt(value) => i64::try_from(value).map_err(|_| {
                        ExecError::internal("APPROX_PERCENTILE percentage overflow")
                    })?,
                    _ => {
                        return Err(ExecError::internal(
                            "APPROX_PERCENTILE percentage was not resolved to an integer",
                        ))
                    }
                };
                let input_type = arg_type(0)?;
                let ranks = !matches!(
                    input_type.code(),
                    FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
                ) && matches!(
                    input_type.eval_type(),
                    EvalType::Int
                        | EvalType::Real
                        | EvalType::Decimal
                        | EvalType::Datetime
                        | EvalType::Timestamp
                        | EvalType::Duration
                );
                AggKind::ApproxPercentile(ranks.then_some(percent))
            }
            _ => {
                return Err(ExecError::unsupported(format!(
                    "aggregate executor for {name} is not implemented"
                )))
            }
        };
        let arg_orig_name = args
            .first()
            .and_then(Expression::as_column)
            .map(|column| column.orig_name.clone())
            .unwrap_or_default();
        let mut args = args.into_iter();
        Ok(Self {
            kind,
            arg: args.next(),
            extra_args: args.collect(),
            distinct: desc.has_distinct,
            order_by: desc
                .order_by_items
                .iter()
                .map(|item| (item.expr.clone(), item.desc))
                .collect(),
            arg_orig_name,
        })
    }
}
