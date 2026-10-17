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

//! Go `pkg/expression/explain.go`, the NORMALIZED half.
//!
//! Ported: `ScalarFunction.explainInfo(nil, normalized=true)` (`:37`),
//! `ScalarFunction.ExplainNormalizedInfo` (`:80`),
//! `ScalarFunction.ExplainNormalizedInfo4InList` (`:85`),
//! `Column.ColumnExplainInfoNormalized` (`:118`),
//! `Column.ExplainNormalizedInfo` (`:131`) and `:136`,
//! `Constant.ExplainNormalizedInfo` (`:167`) and `:172`,
//! `SortedExplainNormalizedExpressionList` (`:266`),
//! `SortedExplainNormalizedScalarFuncList` (`:271`).
//!
//! # Why only this half
//!
//! `// boundary:` The CONTEXT-DEPENDENT explain surface -- `ExplainInfo`,
//! `ExplainExpressionList`, `SortedExplainExpressionList`, `ExplainColumnList`,
//! `Constant.format` -- needs `Expression.StringWithCtx`, the
//! `errors.RedactLog*` modes, `ParamValues`, `Column.StringWithCtxForExplain`
//! and `Datum.TruncatedStringify`. None of those exist in this workspace yet,
//! and guessing at their output would produce plan text that silently differs
//! from TiDB's. The normalized half needs NO context by construction (Go
//! passes `nil` for it), so it is portable exactly as written and is the half
//! plan digests and normalized EXPLAIN actually consume.

use crate::column::Column;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;

/// Go `Column.ColumnExplainInfoNormalized` (`explain.go:118`).
///
/// `?` stands in for a column with no original name -- one synthesized by the
/// planner rather than written by the user.
#[must_use]
pub fn column_explain_info_normalized(col: &Column) -> String {
    if col.orig_name.is_empty() {
        "?".to_owned()
    } else {
        col.orig_name.clone()
    }
}

/// Go `Expression.ExplainNormalizedInfo` dispatched over the node kinds.
///
/// A `Constant` normalizes to `?`: that is the whole point of normalization,
/// since two statements differing only in their literals must produce the same
/// digest.
#[must_use]
pub fn explain_normalized_info(expr: &Expression) -> String {
    match expr {
        Expression::Column(column) => column_explain_info_normalized(column),
        Expression::CorrelatedColumn(cor) => column_explain_info_normalized(&cor.column),
        Expression::Constant(_) => "?".to_owned(),
        Expression::ScalarFunction(function) => scalar_func_explain_normalized_info(function),
    }
}

/// Go's `ScalarFunction.FuncName.L`: every cast is named `cast`, while this
/// port keeps one name per cast signature (`cast_decimal`, `cast_char`, ...).
fn go_function_name(function: &ScalarFunction) -> &str {
    let name = function.func_name.lowercase();
    if name == "cast" || name.starts_with("cast_") {
        "cast"
    } else {
        name
    }
}

/// Go `ScalarFunction.explainInfo(nil, true)` (`explain.go:37`).
#[must_use]
pub fn scalar_func_explain_normalized_info(function: &ScalarFunction) -> String {
    let name = go_function_name(function);
    let args = function.get_args();
    let mut buffer = format!("{name}(");

    // `in(_tidb_tid, -1)` prints as `in(_tidb_tid, dual)`, normalized or not:
    // that -1 is the sentinel partition id for a partition-less scan, and
    // printing it raw reads as a real value.
    if name == "in" && args.len() == 2 {
        let first = explain_normalized_info(&args[0]);
        let is_dual = first.ends_with("_tidb_tid")
            && matches!(
                &args[1],
                Expression::Constant(c) if matches!(c.value, tidb_datatype::Datum::Int(-1))
            );
        if is_dual {
            buffer.push_str(&first);
            buffer.push_str(", dual)");
            return buffer;
        }
    }

    if name == "cast" {
        // A cast prints its target TYPE as a second pseudo-argument.
        for arg in args {
            buffer.push_str(&explain_normalized_info(arg));
            buffer.push_str(", ");
            if let Some(ret_type) = function.ret_type.as_ref() {
                buffer.push_str(&ret_type.source_string());
            }
        }
    } else {
        for (index, arg) in args.iter().enumerate() {
            buffer.push_str(&explain_normalized_info(arg));
            if index + 1 < args.len() {
                buffer.push_str(", ");
            }
        }
    }
    buffer.push(')');
    buffer
}

/// Go `Expression.ExplainNormalizedInfo4InList` dispatched over the node
/// kinds.
///
/// The difference from [`explain_normalized_info`] is one arm: an `IN` list
/// collapses to `...`, so `a IN (1,2,3)` and `a IN (1,2,3,4)` share a digest.
#[must_use]
pub fn explain_normalized_info_4_in_list(expr: &Expression) -> String {
    match expr {
        Expression::Column(column) => column_explain_info_normalized(column),
        Expression::CorrelatedColumn(cor) => column_explain_info_normalized(&cor.column),
        Expression::Constant(_) => "?".to_owned(),
        Expression::ScalarFunction(function) => {
            scalar_func_explain_normalized_info_4_in_list(function)
        }
    }
}

/// Go `ScalarFunction.ExplainNormalizedInfo4InList` (`explain.go:85`).
#[must_use]
pub fn scalar_func_explain_normalized_info_4_in_list(function: &ScalarFunction) -> String {
    let name = go_function_name(function);
    let args = function.get_args();
    let mut buffer = format!("{name}(");
    match name {
        "cast" => {
            for arg in args {
                buffer.push_str(&explain_normalized_info_4_in_list(arg));
                buffer.push_str(", ");
                if let Some(ret_type) = function.ret_type.as_ref() {
                    buffer.push_str(&ret_type.source_string());
                }
            }
        }
        "in" => buffer.push_str("..."),
        _ => {
            for (index, arg) in args.iter().enumerate() {
                buffer.push_str(&explain_normalized_info_4_in_list(arg));
                if index + 1 < args.len() {
                    buffer.push_str(", ");
                }
            }
        }
    }
    buffer.push(')');
    buffer
}

/// Go `sortedExplainExpressionList` (`explain.go:241`) over rendered texts:
/// SORTED and comma-joined. Go sorts with `slices.Sort` and keeps every entry,
/// so two equal conditions print twice.
fn sorted_joined(mut rendered: Vec<String>) -> String {
    rendered.sort();
    rendered.join(", ")
}

/// Go `SortedExplainNormalizedExpressionList` (`explain.go:266`): the
/// normalized forms of `exprs`, SORTED and comma-joined.
///
/// The sort is what makes the output order-independent, so two plans that
/// differ only in the order a rule happened to emit conditions share a digest.
/// Duplicates are kept, as Go does.
#[must_use]
pub fn sorted_explain_normalized_expression_list(exprs: &[Expression]) -> String {
    sorted_joined(exprs.iter().map(explain_normalized_info).collect())
}

/// Go `SortedExplainExpressionListIgnoreInlist` (`explain.go:237`): the
/// in-list-collapsing normalized forms, SORTED and comma-joined; the text
/// selections and projections use while `tidb_ignore_inlist_plan_digest` is on.
#[must_use]
pub fn sorted_explain_expression_list_ignore_inlist(exprs: &[Expression]) -> String {
    sorted_joined(
        exprs
            .iter()
            .map(explain_normalized_info_4_in_list)
            .collect(),
    )
}

/// Go `SortedExplainNormalizedScalarFuncList` (`explain.go:271`): the same for
/// a list already known to hold scalar functions.
#[must_use]
pub fn sorted_explain_normalized_scalar_func_list(exprs: &[ScalarFunction]) -> String {
    sorted_joined(
        exprs
            .iter()
            .map(scalar_func_explain_normalized_info)
            .collect(),
    )
}
