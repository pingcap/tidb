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

//! Go `pkg/expression/explain.go`: an expression's `ExplainInfo` /
//! `StringWithCtx` text, which physical-plan EXPLAIN and the plan cache's
//! skip reasons print.

use std::borrow::Cow;
use std::fmt::Write;

use tidb_datatype::Datum;
use crate::expression::Expression;

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
pub enum ExpressionTextStyle {
    /// Go `Expression.ExplainInfo`: string constants are quoted, and
    /// redacted as the context's `tidb_redact_log` says.
    Explain,
    /// Go `Expression.StringWithCtx(ctx, redact)`: string constants print
    /// bare, redacted as the caller's `redact` says.
    StringWithCtx(RedactionMode),
}

/// Go's `errors.RedactLog*` value as the expression renderers take it.
pub use tidb_mysql::errors::RedactionMode;

/// Go `SessionVars.EnableRedactLog` (`OFF`, `ON` or `MARKER`) as the
/// `redact` argument of `StringWithCtx`: `ON` replaces values with `?`,
/// `MARKER` wraps them in `‹›`, and anything else renders them.
#[must_use]
pub fn redaction_mode(enable_redact_log: &str) -> RedactionMode {
    if enable_redact_log.eq_ignore_ascii_case("ON") {
        RedactionMode::Enabled
    } else if enable_redact_log.eq_ignore_ascii_case("MARKER") {
        RedactionMode::Marker
    } else {
        RedactionMode::Disabled
    }
}

/// Renders the physical-expression subset used by physical-plan EXPLAIN.
pub fn physical_expression_text_with_columns(
    ctx: &dyn crate::Columns,
    expression: &Expression,
    _column_names: &[Option<String>],
    style: ExpressionTextStyle,
) -> Option<String> {
    let mut output = String::new();
    append_expression_text(ctx, expression, style, &mut output)?;
    Some(output)
}

/// Appends the text of a physical expression. A rendering error leaves the
/// existing output intact.
pub fn append_physical_expression_text(
    ctx: &dyn crate::Columns,
    expression: &Expression,
    style: ExpressionTextStyle,
    output: &mut String,
) -> Option<()> {
    let start = output.len();
    let result = append_expression_text(ctx, expression, style, output);
    if result.is_none() {
        output.truncate(start);
    }
    result
}

fn append_expression_text(
    ctx: &dyn crate::Columns,
    expression: &Expression,
    style: ExpressionTextStyle,
    output: &mut String,
) -> Option<()> {
    match expression {
        Expression::Column(column) => append_physical_column_text(ctx, column, output)?,
        Expression::ScalarFunction(function) => {
            let name = function.func_name.lowercase();
            let args = &function.args;
            output.reserve(name.len() + 2 + 16 * args.len());
            output.push_str(name);
            output.push('(');
            let mut dual_argument = None;
            for (index, argument) in args.iter().enumerate() {
                if index != 0 {
                    output.push_str(", ");
                }
                let start = output.len();
                append_expression_text(ctx, argument, style, output)?;
                // Go prints an impossible physical-table ID as in(..., dual).
                if index == 0
                    && name == "in"
                    && output[start..].ends_with("_tidb_tid")
                    && matches!(&args[..], [_, Expression::Constant(constant)]
                        if matches!(constant.value, Datum::Int(-1)))
                {
                    dual_argument = Some(output.len());
                }
            }
            if let Some(end) = dual_argument {
                output.truncate(end);
                output.push_str(", dual");
            } else if name == "cast" && args.len() == 1 {
                let result_type = function.ret_type.as_ref()?;
                output.push_str(", ");
                output.push_str(&result_type.source_string());
            }
            output.push(')');
        }
        Expression::Constant(constant) => match style {
            ExpressionTextStyle::Explain => append_explain_constant(ctx, constant, output)?,
            ExpressionTextStyle::StringWithCtx(redact) => {
                append_string_constant(ctx, constant, redact, output)?;
            }
        },
        Expression::CorrelatedColumn(correlated) => {
            append_column_text(ctx, &correlated.column, output);
        }
        // A Projection renders the embedded constant until the scalar
        // subquery is evaluated.
        Expression::ScalarSubQuery(subquery) => match style {
            ExpressionTextStyle::Explain => output.push_str(&subquery.explain_info()),
            ExpressionTextStyle::StringWithCtx(redact) => {
                append_string_constant(ctx, &subquery.embedded_constant(), redact, output)?;
            }
        },
    }
    Some(())
}

/// Appends a column's plan text from an existing schema or expression.
pub fn append_physical_column_text(
    ctx: &dyn crate::Columns,
    column: &crate::column::Column,
    output: &mut String,
) -> Option<()> {
    if column.is_hidden {
        if let Some(virtual_expr) = column.virtual_expr.as_deref() {
            return append_physical_expression_text(
                ctx,
                virtual_expr,
                ExpressionTextStyle::StringWithCtx(RedactionMode::Disabled),
                output,
            );
        }
    }
    append_column_text(ctx, column, output);
    Some(())
}

/// Go `Column.string`: use `OrigName`, or the requested numeric identifier.
/// A projection alias does not replace the original name.
fn append_column_text(
    ctx: &dyn crate::Columns,
    column: &crate::column::Column,
    output: &mut String,
) {
    if !column.orig_name.is_empty() {
        output.push_str(&column.orig_name);
    } else if ctx.remove_column_numbers() {
        output.push_str("Column");
    } else {
        write!(output, "Column#{}", column.unique_id).expect("String writes are infallible");
    }
}

/// Go `Constant.StringWithCtx`, with unquoted constants and the `<nil>`
/// spelling of a NULL datum.
fn append_string_constant(
    ctx: &dyn crate::Columns,
    constant: &crate::constant::Constant,
    redact: RedactionMode,
    output: &mut String,
) -> Option<()> {
    let parameter;
    let value = if let Some(marker) = constant.param_marker {
        let Some(value) = usize::try_from(marker.order)
            .ok()
            .and_then(|order| ctx.param_value(order).ok())
        else {
            output.push('?');
            return Some(());
        };
        parameter = value;
        &parameter
    } else if let Some(deferred) = &constant.deferred_expr {
        return append_expression_text(
            ctx,
            deferred,
            ExpressionTextStyle::StringWithCtx(redact),
            output,
        );
    } else {
        &constant.value
    };
    append_constant_value(value, redact, false, constant.subquery_ref_id, output)
}

fn append_explain_constant(
    ctx: &dyn crate::Columns,
    constant: &crate::constant::Constant,
    output: &mut String,
) -> Option<()> {
    let redact = redaction_mode(&ctx.tidb_redact_log_ref());
    let evaluated;
    let datum = if redact != RedactionMode::Enabled
        && (constant.param_marker.is_some() || constant.deferred_expr.is_some())
    {
        evaluated = match constant.eval_in(ctx) {
            Ok(value) => value,
            Err(_) => {
                output.push_str("not recognized const value");
                return Some(());
            }
        };
        &evaluated
    } else {
        &constant.value
    };
    append_constant_value(datum, redact, true, constant.subquery_ref_id, output)
}

fn append_constant_value(
    datum: &Datum,
    redact: RedactionMode,
    explain: bool,
    subquery_ref_id: i64,
    output: &mut String,
) -> Option<()> {
    if redact == RedactionMode::Enabled && subquery_ref_id <= 0 {
        if output.capacity() == 0 {
            *output = "?".to_owned();
        } else {
            output.push('?');
        }
        return Some(());
    }
    let value = if redact == RedactionMode::Enabled {
        Cow::Borrowed("?")
    } else if explain && datum.is_null() {
        Cow::Borrowed("NULL")
    } else {
        match datum.truncated_stringify_ref().ok()? {
            Cow::Borrowed(bytes) => Cow::Borrowed(simdutf8::basic::from_utf8(bytes).ok()?),
            Cow::Owned(bytes) => Cow::Owned(String::from_utf8(bytes).ok()?),
        }
    };
    let quoted = explain
        && redact != RedactionMode::Enabled
        && matches!(
            datum,
            Datum::String(_)
                | Datum::Bytes(_)
                | Datum::Enum(_, _)
                | Datum::Set(_, _)
                | Datum::Json(_)
                | Datum::BinaryLiteral(_)
                | Datum::Bit(_)
        );
    let marked = redact == RedactionMode::Marker;
    if !quoted && !marked && subquery_ref_id <= 0 && output.capacity() == 0 {
        *output = value.into_owned();
        return Some(());
    }
    output.reserve(
        value.len()
            + usize::from(quoted) * 2
            + usize::from(marked) * "‹›".len()
            + usize::from(subquery_ref_id > 0) * ("ScalarQueryCol#()".len() + 19),
    );
    if subquery_ref_id > 0 {
        write!(output, "ScalarQueryCol#{subquery_ref_id}(").expect("String writes are infallible");
    }
    if marked {
        output.push('‹');
    }
    if quoted {
        output.push('"');
    }
    output.push_str(&value);
    if quoted {
        output.push('"');
    }
    if marked {
        output.push('›');
    }
    if subquery_ref_id > 0 {
        output.push(')');
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_exprctx::NoColumns;

    #[test]
    fn nested_explain_rejects_a_constant_cut_mid_codepoint() {
        let context = NoColumns;
        let field = FieldType::new(FieldTypeCode::VarString);
        let expression = |text: String| {
            Expression::Constant(Constant::new(Datum::new_string_datum(text), field.clone()))
        };
        let concat = crate::new_function_base(
            &context,
            "concat",
            field.clone(),
            vec![expression("first".to_owned()), expression(format!("{}é", "x".repeat(63)))],
        )
        .unwrap();
        assert_eq!(
            physical_expression_text_with_columns(&context, &concat, &[], ExpressionTextStyle::Explain),
            None,
        );
        let mut output = "existing: ".to_owned();
        assert_eq!(
            append_physical_expression_text(&context, &concat, ExpressionTextStyle::Explain, &mut output),
            None,
        );
        assert_eq!(output, "existing: ");
        let masked = ExpressionTextStyle::StringWithCtx(RedactionMode::Enabled);
        assert_eq!(
            physical_expression_text_with_columns(&context, &concat, &[], masked).as_deref(),
            Some("concat(?, ?)"),
        );

        let literal = expression("first".to_owned());
        let mut output = String::with_capacity(16);
        let original = output.as_ptr();
        assert_eq!(
            append_physical_expression_text(&context, &literal, masked, &mut output),
            Some(()),
        );
        assert_eq!(output, "?");
        assert_eq!(output.as_ptr(), original);
    }
}
