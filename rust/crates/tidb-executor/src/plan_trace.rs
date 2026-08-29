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

//! Formatting helpers shared by physical-plan EXPLAIN and range diagnostics.
//!
//! Go renders EXPLAIN from the physical plan itself. This module deliberately
//! contains no independent plan tree or planner decisions.

use tidb_datatype::Datum;
use tidb_expr::expression::Expression;

/// Go `statistics.PseudoRowCount` (`pkg/statistics/table.go`).
pub(crate) const PSEUDO_ROW_COUNT: f64 = 10_000.0;

fn collect_physical_or<'a>(expression: &'a Expression, out: &mut Vec<&'a Expression>) {
    if let Expression::ScalarFunction(function) = expression {
        if function.func_name.lowercase() == "or" && function.args.len() == 2 {
            collect_physical_or(&function.args[0], out);
            collect_physical_or(&function.args[1], out);
            return;
        }
    }
    out.push(expression);
}

/// Renders the physical-expression subset used by physical-plan EXPLAIN.
pub(crate) fn physical_expression_text_with_columns(
    expression: &Expression,
    column_names: &[Option<String>],
) -> Option<String> {
    match expression {
        Expression::Column(column) if column.unique_id < 0 => {
            Some(format!("ScalarQueryCol#{}", -column.unique_id))
        }
        Expression::Column(column) => {
            let index = usize::try_from(column.index).ok()?;
            if let Some(physical_name) = column_names.get(index) {
                return Some(
                    physical_name
                        .clone()
                        .unwrap_or_else(|| format!("Column#{}", column.unique_id)),
                );
            }
            (!column.orig_name.is_empty())
                .then(|| column.orig_name.clone())
                .or_else(|| Some(format!("Column#{}", column.unique_id)))
        }
        Expression::ScalarFunction(function) => {
            if function.func_name.lowercase() == "or" && function.args.len() == 2 {
                let mut parts = Vec::new();
                collect_physical_or(expression, &mut parts);
                let mut rendered = physical_expression_text_with_columns(
                    parts.pop().expect("OR has an operand"),
                    column_names,
                )?;
                for part in parts.into_iter().rev() {
                    rendered = format!(
                        "or({}, {rendered})",
                        physical_expression_text_with_columns(part, column_names)?
                    );
                }
                return Some(rendered);
            }
            let arguments = function
                .args
                .iter()
                .map(|argument| physical_expression_text_with_columns(argument, column_names))
                .collect::<Option<Vec<_>>>()?;
            match function.func_name.lowercase() {
                "cast_decimal" => {
                    if arguments.len() != 1 {
                        return None;
                    }
                    let result_type = function.ret_type.as_ref()?;
                    Some(format!(
                        "cast({}, decimal({},{}) BINARY)",
                        arguments[0],
                        result_type.flen(),
                        result_type.decimal()
                    ))
                }
                "cast_double" => {
                    (arguments.len() == 1).then(|| format!("cast({}, double BINARY)", arguments[0]))
                }
                name => Some(format!("{name}({})", arguments.join(", "))),
            }
        }
        Expression::Constant(constant) if constant.param_marker.is_none() => {
            explain_constant(constant)
        }
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => None,
    }
}

fn explain_constant(constant: &tidb_expr::constant::Constant) -> Option<String> {
    if constant.deferred_expr.is_some() || constant.param_marker.is_some() {
        return None;
    }
    let value = constant
        .value
        .truncated_stringify()
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())?;
    let value = match &constant.value {
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Json(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_) => format!("\"{value}\""),
        _ => value,
    };
    if constant.subquery_ref_id > 0 {
        Some(format!(
            "ScalarQueryCol#{}({value})",
            constant.subquery_ref_id
        ))
    } else {
        Some(value)
    }
}

/// Formats a range as Go's `ranger.Range.String()` does.
pub(crate) fn range_text(range: &crate::kv_table::IndexRange) -> String {
    let low = bound_text(&range.low, "-inf", true);
    let high = bound_text(&range.high, "+inf", false);
    let open = if range.low_exclusive { '(' } else { '[' };
    let close = if range.high_exclusive { ')' } else { ']' };
    format!("{open}{low},{high}{close}")
}

fn bound_text(values: &[Datum], infinity: &str, is_left_side: bool) -> String {
    if values.is_empty() {
        return infinity.to_owned();
    }
    values
        .iter()
        .map(|value| datum_go_text(value, is_left_side))
        .collect::<Vec<_>>()
        .join(" ")
}

fn datum_go_text(value: &Datum, is_left_side: bool) -> String {
    match value {
        Datum::Null => "NULL".to_owned(),
        Datum::MaxValue => "+inf".to_owned(),
        Datum::MinNotNull => "-inf".to_owned(),
        Datum::Int(i64::MIN) if is_left_side => "-inf".to_owned(),
        Datum::Int(i64::MAX) if !is_left_side => "+inf".to_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(u64::MAX) if !is_left_side => "+inf".to_owned(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => value.to_string(),
        Datum::Decimal(value) => value.to_string(),
        Datum::String(value) => tidb_error::mysql::go_quote_bytes(value.bytes()),
        Datum::Bytes(value) => tidb_error::mysql::go_quote_bytes(value),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => format!("\"{value}\""),
        Datum::Json(value) => format!("\"{value}\""),
        Datum::Enum(value, _) => format!("\"{value}\""),
        Datum::Set(value, _) => format!("\"{value}\""),
        other => other
            .sql_string()
            .unwrap_or_else(|_| format!("{other:?}"))
            .to_owned(),
    }
}

/// Splits an AST predicate into Go's CNF item list.
pub(crate) fn collect_and<'a>(expr: &'a tidb_ast::Expr, out: &mut Vec<&'a tidb_ast::Expr>) {
    if let tidb_ast::Expr::Paren(inner) = expr {
        collect_and(inner, out);
        return;
    }
    if let tidb_ast::Expr::Binary(tidb_ast::BinaryOp::LogicAnd, lhs, rhs) = expr {
        collect_and(lhs, out);
        collect_and(rhs, out);
        return;
    }
    out.push(expr);
}
