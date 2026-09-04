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

//! Go `executor/aggfuncs.Build`: choose an executor implementation from the
//! planner-owned aggregate descriptor. Type inference belongs exclusively to
//! `tidb_expr::aggregation::AggFuncDesc`, as it does before Go reaches the
//! executor builder.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::expression::Expression;

use crate::hash_agg::{AggKind, BitOp};

use super::DriverError;

pub(crate) fn aggregate_kind(
    name: &str,
    args: &[Expression],
    separator: Option<String>,
) -> Result<AggKind, DriverError> {
    Ok(match name {
        "COUNT" => AggKind::Count,
        "SUM" | "SUM_INT" => AggKind::Sum,
        "FIRST_ROW" | "FIRSTROW" => AggKind::FirstRow,
        "MIN" => AggKind::Min,
        "MAX" => AggKind::Max,
        "MIN_COUNT" => AggKind::MinCount,
        "MAX_COUNT" => AggKind::MaxCount,
        "AVG" => AggKind::Avg,
        "GROUP_CONCAT" => AggKind::GroupConcat {
            separator: separator.ok_or_else(|| {
                DriverError::unsupported("GROUP_CONCAT has no separator argument")
            })?,
        },
        "BIT_AND" => AggKind::Bit(BitOp::And),
        "BIT_OR" => AggKind::Bit(BitOp::Or),
        "BIT_XOR" => AggKind::Bit(BitOp::Xor),
        "VAR_POP" => AggKind::Variance {
            sample: false,
            sqrt: false,
        },
        "VAR_SAMP" => AggKind::Variance {
            sample: true,
            sqrt: false,
        },
        "STDDEV_POP" => AggKind::Variance {
            sample: false,
            sqrt: true,
        },
        "STDDEV_SAMP" => AggKind::Variance {
            sample: true,
            sqrt: true,
        },
        "JSON_ARRAYAGG" => AggKind::JsonArrayAgg {
            value_type: argument_type(args.first()),
        },
        "JSON_OBJECTAGG" => AggKind::JsonObjectAgg {
            value_type: argument_type(args.get(1)),
            key_is_binary: args
                .first()
                .and_then(Expression::static_type)
                .is_some_and(FieldType::is_binary_string),
        },
        "APPROX_COUNT_DISTINCT" => AggKind::ApproxCountDistinct,
        "APPROX_PERCENTILE" => {
            let [value, percentage] = args else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE should take 2 arguments",
                ));
            };
            let Some(folded) = fold_constant(percentage) else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE should take a constant expression as percentage argument",
                ));
            };
            let Some(percent) = constant_eval_int(&folded) else {
                return Err(DriverError::ApproxPercentileArgument(
                    "APPROX_PERCENTILE: Percentage value cannot be NULL",
                ));
            };
            if !(1..=100).contains(&percent) {
                return Err(DriverError::PercentageOutOfRange(percent));
            }
            let value_type = value.static_type();
            let code = value_type.map_or(FieldTypeCode::LongLong, FieldType::code);
            let ranks = !matches!(
                code,
                FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
            ) && value_type.is_some_and(|field_type| {
                matches!(
                    field_type.eval_type(),
                    tidb_datatype::EvalType::Int
                        | tidb_datatype::EvalType::Real
                        | tidb_datatype::EvalType::Decimal
                        | tidb_datatype::EvalType::Datetime
                        | tidb_datatype::EvalType::Timestamp
                        | tidb_datatype::EvalType::Duration
                )
            });
            AggKind::ApproxPercentile(ranks.then_some(percent))
        }
        _ => {
            return Err(DriverError::unsupported(format!(
                "physical aggregate `{name}` is not executable"
            )))
        }
    })
}

pub(crate) fn separator_text(expression: &Expression) -> Result<String, DriverError> {
    let Some(value) = fold_constant(expression) else {
        return Err(DriverError::unsupported(
            "GROUP_CONCAT separator is not constant",
        ));
    };
    match value {
        Datum::String(value) => Ok(String::from_utf8_lossy(value.bytes()).into_owned()),
        Datum::Bytes(value) => Ok(String::from_utf8_lossy(&value).into_owned()),
        _ => Err(DriverError::unsupported(
            "GROUP_CONCAT separator is not a string",
        )),
    }
}

fn argument_type(expression: Option<&Expression>) -> FieldType {
    expression
        .and_then(Expression::static_type)
        .cloned()
        .unwrap_or_else(|| FieldType::new(FieldTypeCode::VarString))
}

fn fold_constant(expression: &Expression) -> Option<Datum> {
    match expression {
        Expression::Constant(constant) => Some(constant.value.clone()),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => None,
        Expression::ScalarFunction(function) => {
            if !function
                .args
                .iter()
                .all(|argument| fold_constant(argument).is_some())
            {
                return None;
            }
            let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
            chunk.set_num_virtual_rows(1);
            expression
                .eval(&crate::StmtContext::for_query(), chunk.get_row(0))
                .ok()
        }
    }
}

fn constant_eval_int(value: &Datum) -> Option<i64> {
    match value {
        Datum::Null => None,
        Datum::Int(number) => Some(*number),
        Datum::UInt(number) => Some(*number as i64),
        Datum::String(_) | Datum::Bytes(_) => Some(value.to_i64().map_or(0, |result| result.value)),
        Datum::Real(number) | Datum::Float32(number) => Some(number.to_bits() as i64),
        _ => Some(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_min_count_names_build_the_matching_runtime_kind() {
        assert_eq!(
            aggregate_kind("MAX_COUNT", &[], None).unwrap(),
            AggKind::MaxCount
        );
        assert_eq!(
            aggregate_kind("MIN_COUNT", &[], None).unwrap(),
            AggKind::MinCount
        );
    }
}
