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

/// Which of Go's two expression renderers a caller needs.
///
/// Go renders a physical plan's operator text through `Expression.ExplainInfo`
/// for conditions (Selection/Join/aggregate arguments) but through
/// `Expression.StringWithCtx` for Projection/Expand expressions
/// (`ExplainExpressionList`, `explain.go:188`). The two differ in how a
/// nested string constant prints: `ExplainInfo` quotes it
/// (`Constant.format`, `explain.go:176`), while `StringWithCtx` prints
/// `TruncatedStringify` bare (`constant.go:181`) -- which is why Go's own q14
/// recording shows `case(like(test.part.p_type, PROMO%, 92), ..., 0.0000)`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpressionTextStyle {
    /// Go `Expression.ExplainInfo`: string constants are quoted.
    Explain,
    /// Go `Expression.StringWithCtx`: string constants print bare.
    StringWithCtx,
}

/// Renders the physical-expression subset used by physical-plan EXPLAIN.
pub(crate) fn physical_expression_text_with_columns(
    ctx: &dyn tidb_expr::Columns,
    expression: &Expression,
    column_names: &[Option<String>],
    style: ExpressionTextStyle,
) -> Option<String> {
    match expression {
        Expression::Column(column) if column.unique_id < 0 => {
            // Go's `-col.UniqueID` wraps at runtime rather than panicking; a
            // placeholder column with `i64::MIN` reached this arm and aborted
            // EXPLAIN. `wrapping_neg` keeps Go's two's-complement result.
            Some(format!(
                "ScalarQueryCol#{}",
                column.unique_id.wrapping_neg()
            ))
        }
        Expression::Column(column) => {
            // Go `Column.string`: a hidden virtual column (an expression
            // index's) prints its expression, without parameter values.
            if column.is_hidden {
                if let Some(virtual_expr) = column.virtual_expr.as_deref() {
                    return physical_expression_text_with_columns(
                        ctx,
                        virtual_expr,
                        column_names,
                        ExpressionTextStyle::StringWithCtx,
                    );
                }
            }
            // Go `Column.StringWithCtx` renders `OrigName` (the
            // table-qualified physical name) when non-empty, falling back to
            // `Column#<UniqueID>` — the output names (aliases) are not part
            // of the explain rendering.
            Some(column_text(ctx, column))
        }
        Expression::ScalarFunction(function) => {
            let arguments = function
                .args
                .iter()
                .map(|argument| {
                    physical_expression_text_with_columns(ctx, argument, column_names, style)
                })
                .collect::<Option<Vec<_>>>()?;
            // Go `ScalarFunction.explainInfo`: `in(_tidb_tid, -1)`, the
            // impossible partition a global index filter uses when pruning
            // left nothing, prints as `in(_tidb_tid, dual)`.
            if function.func_name.lowercase() == "in"
                && arguments.len() == 2
                && arguments[0].ends_with("_tidb_tid")
                && matches!(
                    function.args.get(1),
                    Some(Expression::Constant(constant))
                        if matches!(constant.value, tidb_datatype::Datum::Int(-1))
                )
            {
                return Some(format!("in({}, dual)", arguments[0]));
            }
            match function.func_name.lowercase() {
                // Go names every cast `cast` and renders it as
                // `cast(arg, RetType.String())`; this port keeps one name per
                // cast signature.
                name if (name == "cast" || name.starts_with("cast_")) && arguments.len() == 1 => {
                    let result_type = function.ret_type.as_ref()?;
                    Some(format!("cast({}, {})", arguments[0], result_type.source_string()))
                }
                name => Some(format!("{name}({})", arguments.join(", "))),
            }
        }
        Expression::Constant(constant) => match style {
            ExpressionTextStyle::Explain => explain_constant(constant, ctx),
            ExpressionTextStyle::StringWithCtx => {
                string_with_ctx_constant(constant, ctx, column_names)
            }
        },
        Expression::CorrelatedColumn(correlated) => {
            // Go `CorrelatedColumn.StringWithCtx` renders through the embedded
            // `Column` (expression/column.go): a correlated reference prints
            // like its outer column, keeping the whole condition text visible.
            Some(column_text(ctx, &correlated.column))
        }
    }
}

/// Go `Column.string`: `OrigName`, else `Column#<UniqueID>`, or bare
/// `Column` when the statement removes column numbers (plan_tree).
fn column_text(ctx: &dyn tidb_expr::Columns, column: &tidb_expr::column::Column) -> String {
    if !column.orig_name.is_empty() {
        column.orig_name.clone()
    } else if ctx.remove_column_numbers() {
        "Column".to_owned()
    } else {
        format!("Column#{}", column.unique_id)
    }
}

/// Go `Constant.StringWithCtx` (`constant.go:181`): the Projection
/// renderer, which prints a string constant WITHOUT quotes.
fn string_with_ctx_constant(
    constant: &tidb_expr::constant::Constant,
    ctx: &dyn tidb_expr::Columns,
    column_names: &[Option<String>],
) -> Option<String> {
    let parameter;
    let value = if let Some(marker) = constant.param_marker {
        let Some(value) = usize::try_from(marker.order)
            .ok()
            .and_then(|order| ctx.param_value(order).ok())
        else {
            return Some("?".to_owned());
        };
        parameter = value;
        &parameter
    } else if let Some(deferred) = &constant.deferred_expr {
        // Go returns the expression's text before applying SubqueryRefID.
        return physical_expression_text_with_columns(
            ctx,
            deferred,
            column_names,
            ExpressionTextStyle::StringWithCtx,
        );
    } else {
        &constant.value
    };
    // Go `Constant.StringWithCtx` renders through `fmt.Sprintf("%v", ...)`,
    // which prints a NULL value as `<nil>` (q43's case-else:
    // `case(eq(...), val, <nil>)`). The quoted "NULL" spelling belongs to
    // `Constant.Format`/`ExplainInfo` (Selection conditions), not here.
    let value = if value.is_null() {
        "<nil>".to_owned()
    } else {
        constant_value_text(value)?
    };
    // The reference wraps a NULL result too: `ScalarQueryCol#14(<nil>)`.
    if constant.subquery_ref_id > 0 {
        Some(format!(
            "ScalarQueryCol#{}({value})",
            constant.subquery_ref_id
        ))
    } else {
        Some(value)
    }
}

/// Go's `%v` of `Datum.GetValue()`: a hex or bit literal prints through
/// `BinaryLiteral.String()` (`0x80`), whatever its bytes spell.
fn constant_value_text(value: &Datum) -> Option<String> {
    match value {
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => Some(literal.to_string()),
        value => value
            .truncated_stringify()
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok()),
    }
}

fn explain_constant(
    constant: &tidb_expr::constant::Constant,
    ctx: &dyn tidb_expr::Columns,
) -> Option<String> {
    // Literal datums are immutable; borrowing keeps large strings/JSON from
    // being cloned merely to render a plan. Only lazy constants need evaluation.
    let evaluated;
    let datum = if constant.param_marker.is_some() || constant.deferred_expr.is_some() {
        evaluated = match constant.eval_in(ctx) {
            Ok(value) => value,
            Err(_) => return Some("not recognized const value".to_owned()),
        };
        &evaluated
    } else {
        &constant.value
    };
    // Go `Constant.Format`/`StringWithCtx`: a NULL constant prints `NULL`.
    if datum.is_null() {
        return Some("NULL".to_owned());
    }
    let value = constant_value_text(datum)?;
    let value = match datum {
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

#[cfg(test)]
mod tests {
    use super::{physical_expression_text_with_columns, ExpressionTextStyle};
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;

    /// Go's `-col.UniqueID` wraps at run time rather than panicking. A
    /// placeholder column whose `unique_id` is `i64::MIN` used to abort
    /// EXPLAIN with an arithmetic-overflow panic.
    #[test]
    fn a_min_unique_id_column_renders_without_overflow() {
        let column = Column::new(i64::MIN, FieldType::new(FieldTypeCode::LongLong));
        let rendered = physical_expression_text_with_columns(
            &tidb_expr::NoColumns,
            &Expression::Column(column),
            &[],
            ExpressionTextStyle::Explain,
        );
        assert_eq!(
            rendered.as_deref(),
            Some("ScalarQueryCol#-9223372036854775808")
        );
    }
    #[test]
    fn dynamic_constants_use_go_explain_and_string_semantics() {
        use tidb_datatype::Datum;
        use tidb_expr::constant::{Constant, ParamMarker};
        use tidb_expr::scalar_function::ScalarFunction;
        let ctx = crate::StmtContext::default()
            .with_prepared_params(vec![Datum::new_string("current")].into());
        let mut parameter = Constant::new(
            Datum::new_string("stale"),
            FieldType::new(FieldTypeCode::VarString),
        );
        parameter.param_marker = Some(ParamMarker { order: 0 });
        parameter.subquery_ref_id = 9;
        let integer = |value| {
            Expression::Constant(Constant::new(
                Datum::Int(value),
                FieldType::new(FieldTypeCode::LongLong),
            ))
        };
        let mut deferred = Constant::new(Datum::Int(-1), FieldType::new(FieldTypeCode::LongLong));
        deferred.deferred_expr = Some(Box::new(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![integer(40), integer(2)],
        ))));
        deferred.subquery_ref_id = 7;
        let mut missing = parameter.clone();
        missing.param_marker = Some(ParamMarker { order: 1 });
        let mut overflow = Constant::new(Datum::Int(-1), FieldType::new(FieldTypeCode::LongLong));
        overflow.deferred_expr = Some(Box::new(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![integer(i64::MAX), integer(1)],
        ))));
        overflow.subquery_ref_id = 8;
        let mut converted = Constant::new(
            Datum::new_string("stale"),
            FieldType::new(FieldTypeCode::VarString),
        );
        converted.deferred_expr = Some(Box::new(integer(42)));
        let mut both = parameter.clone();
        both.deferred_expr = overflow.deferred_expr.clone();
        let cases = [
            (
                overflow,
                "not recognized const value",
                "plus(9223372036854775807, 1)",
            ),
            (converted, "\"42\"", "42"),
            (
                both,
                "ScalarQueryCol#9(\"current\")",
                "ScalarQueryCol#9(current)",
            ),
            (
                parameter,
                "ScalarQueryCol#9(\"current\")",
                "ScalarQueryCol#9(current)",
            ),
            (deferred, "ScalarQueryCol#7(42)", "plus(40, 2)"),
            (missing, "not recognized const value", "?"),
        ];
        for (constant, explain, display) in cases {
            let expression = Expression::Constant(constant);
            for (style, expected) in [
                (ExpressionTextStyle::Explain, explain),
                (ExpressionTextStyle::StringWithCtx, display),
            ] {
                assert_eq!(
                    physical_expression_text_with_columns(&ctx, &expression, &[], style).as_deref(),
                    Some(expected)
                );
            }
        }
    }
}
