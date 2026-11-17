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
    column_names: &[Option<String>],
    style: ExpressionTextStyle,
) -> Option<String> {
    match expression {
        Expression::Column(column) => {
            // Go `Column.string`: a hidden virtual column (an expression
            // index's) prints its expression, without parameter values.
            if column.is_hidden {
                if let Some(virtual_expr) = column.virtual_expr.as_deref() {
                    return physical_expression_text_with_columns(
                        ctx,
                        virtual_expr,
                        column_names,
                        ExpressionTextStyle::StringWithCtx(RedactionMode::Disabled),
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
                // Go renders a cast as `cast(arg, RetType.String())`.
                "cast" if arguments.len() == 1 => {
                    let result_type = function.ret_type.as_ref()?;
                    Some(format!("cast({}, {})", arguments[0], result_type.source_string()))
                }
                name => Some(format!("{name}({})", arguments.join(", "))),
            }
        }
        Expression::Constant(constant) => match style {
            ExpressionTextStyle::Explain => explain_constant(constant, ctx),
            ExpressionTextStyle::StringWithCtx(redact) => {
                string_with_ctx_constant(constant, ctx, column_names, redact)
            }
        },
        Expression::CorrelatedColumn(correlated) => {
            // Go `CorrelatedColumn.StringWithCtx` renders through the embedded
            // `Column` (expression/column.go): a correlated reference prints
            // like its outer column, keeping the whole condition text visible.
            Some(column_text(ctx, &correlated.column))
        }
        // Go `ScalarSubQueryExpr.ExplainInfo` is `ScalarQueryCol#N`; it has no
        // `StringWithCtx` of its own, so a Projection prints the embedded
        // `Constant` -- NULL until the subquery is evaluated.
        Expression::ScalarSubQuery(subquery) => match style {
            ExpressionTextStyle::Explain => Some(subquery.explain_info()),
            ExpressionTextStyle::StringWithCtx(redact) => {
                string_with_ctx_constant(&subquery.embedded_constant(), ctx, column_names, redact)
            }
        },
    }
}

/// Go `Column.string`: `OrigName`, else `Column#<UniqueID>`, or bare
/// `Column` when the statement removes column numbers (plan_tree).
fn column_text(ctx: &dyn crate::Columns, column: &crate::column::Column) -> String {
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
    constant: &crate::constant::Constant,
    ctx: &dyn crate::Columns,
    column_names: &[Option<String>],
    redact: RedactionMode,
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
            ExpressionTextStyle::StringWithCtx(redact),
        );
    } else {
        &constant.value
    };
    // Go `Constant.StringWithCtx` renders through `Datum.TruncatedStringify`,
    // which prints a NULL value as `<nil>` (q43's case-else:
    // `case(eq(...), val, <nil>)`). The quoted "NULL" spelling belongs to
    // `Constant.Format`/`ExplainInfo` (Selection conditions), not here.
    let value = match redact {
        RedactionMode::Disabled => constant_value_text(value)?,
        RedactionMode::Marker => format!("‹{}›", constant_value_text(value)?),
        RedactionMode::Enabled => "?".to_owned(),
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

/// Go `Datum.TruncatedStringify` as text.
fn constant_value_text(value: &Datum) -> Option<String> {
    value
        .truncated_stringify()
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

/// Go `Constant.ExplainInfo` (`explain.go:141`), redacted as the context's
/// `tidb_redact_log` says.
fn explain_constant(
    constant: &crate::constant::Constant,
    ctx: &dyn crate::Columns,
) -> Option<String> {
    let redact = redaction_mode(&ctx.tidb_redact_log());
    if redact == RedactionMode::Enabled {
        return Some(if constant.subquery_ref_id > 0 {
            format!("ScalarQueryCol#{}(?)", constant.subquery_ref_id)
        } else {
            "?".to_owned()
        });
    }
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
    // Go `Constant.format`: a NULL constant prints `NULL`.
    let value = if datum.is_null() {
        "NULL".to_owned()
    } else {
        let value = constant_value_text(datum)?;
        match datum {
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Json(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_) => format!("\"{value}\""),
        _ => value,
        }
    };
    let value = if redact == RedactionMode::Marker { format!("‹{value}›") } else { value };
    if constant.subquery_ref_id > 0 {
        Some(format!(
            "ScalarQueryCol#{}({value})",
            constant.subquery_ref_id
        ))
    } else {
        Some(value)
    }
}
