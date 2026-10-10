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

//! Go `buildProjectionFieldNameFromExpressions` (`logical_plan_builder.go`):
//! the name an unaliased select field takes, shared by the plan's output
//! names and the result set's column labels.

use tidb_ast::SelectFieldList;

/// The name an unaliased field takes: a column reference keeps its column
/// name, anything else keeps the text it was WRITTEN with -- Go's
/// `SelectField.Text`, backed here by the parser-recorded per-field source
/// span (see `tidb_ast::SelectFieldList::text`). `count(*)` therefore names
/// the column `count(*)` even though `expr` itself restores as `COUNT(1)`
/// (the parser lowers a bare `*` argument to the AST literal `1`, matching
/// the same lowering Go's own hand-written parser performs -- see
/// `pkg/parser/expr_func_parser.go`'s `parseAggregateFuncCall`). A user who
/// writes `count(1)` literally still gets `count(1)`, since both cases read
/// the same original bytes; nothing here special-cases the star string.
///
/// Falls back to `expr.restore()` when the parser recorded no source text
/// for this field (for example a field synthesized by a rewrite pass rather
/// than parsed from source).
///
/// # The literal switch
///
/// Go's literal handling is a switch on the `driver.ValueExpr`'s DATUM KIND,
/// not on its source text, and every arm below is that switch:
///
/// * `KindString` names the column by the literal's VALUE, not its text, with
///   leading non-graphic characters trimmed (`mysql.RangeGraph`), so
///   `select '\t   col'` is named `col` and `select ('\N')` is named `N`.
/// * `KindNull` is named `NULL`, whatever case the source used.
/// * `KindBinaryLiteral` (a `0x`/`b''` literal) keeps its source text.
/// * `KindInt64` carrying `IsBooleanFlag` -- a `TRUE`/`FALSE` keyword -- is
///   named `TRUE` or `FALSE` by its VALUE, so `select false` is `FALSE`.
/// * adjacent string literals use the decoded value of the first token, via
///   [`SelectFieldList::projection_offset`].
/// * every other literal keeps its source text with `\t\n +(` trimmed from
///   the left and `\t\n )` from the right, so `select +1` is named `1`.
pub fn default_field_display_name(
    fields: &SelectFieldList,
    index: usize,
    expr: &tidb_ast::Expr,
) -> String {
    // Go `getInnerFromParenthesesAndUnaryPlus`: parentheses and a unary `+`
    // are looked through before anything else is asked, because Go asks its
    // questions of the REWRITTEN expression and the rewriter drops both --
    // `(a)` rewrites to `a`, and `unaryOpToExpression`'s `opcode.Plus` arm
    // returns without touching the stack ("expression (+ a) is equal to a").
    // So `select (a)` and `select +a` are named `a`, like `select a`.
    let inner = inner_field_expr(expr);
    if let tidb_ast::Expr::Column(path) = inner {
        return path.last().cloned().unwrap_or_default();
    }
    let text = || {
        fields
            .text(index)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map_or_else(|| expr.restore(), str::to_owned)
    };
    // Go: `NAME_CONST` names the column by its FIRST argument's value, which
    // MySQL documents as the function's whole purpose. Go evaluates that
    // argument with `evalAstExpr`; `preprocess.go` has already refused every
    // call whose first argument is not a literal, so a literal is the only
    // shape that reaches the name.
    if let tidb_ast::Expr::Func { name, args, .. } = inner {
        if name.eq_ignore_ascii_case("name_const") && args.len() == 2 {
            if let Some(label) = literal_label_value(&args[0]) {
                return label;
            }
        }
    }
    // Go asks `field.Expr` -- the PARSED node -- whether this field is a
    // literal, never the rewritten expression: its test is
    // `innerExpr.(*driver.ValueExpr)`, and a `driver.ValueExpr` exists only
    // where the SOURCE wrote a literal. This tier's `expr` has been through
    // passes that substitute literals INTO the tree -- variable binding turns
    // `@@warning_count` into its value, subquery folding turns `(select 1)`
    // into `1` -- so `is_value_literal(inner)` alone would name those columns
    // `0` and `select 1` instead of `@@warning_count` and `(select 1)`. Both
    // were measured as regressions before
    // `SelectFieldList::written_literal` recorded the parse-time answer.
    if fields.written_literal(index) && is_value_literal(inner) {
        return literal_field_display_name(fields, index, inner, &text());
    }
    // Non-literal: named by its source text with MySQL special-result-field
    // comment markers removed -- Go's
    // `SpecFieldPattern.ReplaceAllStringFunc(field.Text(), TrimComment)`, which
    // drops every `*/` and `/*!<version>` marker. A `/*+ hint */` therefore
    // keeps `/*+ hint ` in the label because only the closing `*/` matches.
    strip_spec_field_comment_markers(&text())
}

/// Go `buildProjectionFieldNameFromExpressions`'s literal switch, over the
/// literal `expr` and the field's own source `text`.
fn literal_field_display_name(
    fields: &SelectFieldList,
    index: usize,
    expr: &tidb_ast::Expr,
    text: &str,
) -> String {
    match expr {
        // `types.KindString`: the VALUE names the column, with leading
        // non-graphic characters trimmed. A charset introducer on a string
        // literal still builds a string `ValueExpr` (`parseCharsetIntroducer`).
        tidb_ast::Expr::String(value)
        | tidb_ast::Expr::RawString(value)
        | tidb_ast::Expr::CharsetString { value, .. } => {
            let value = match fields.projection_offset(index) {
                Some(offset) => value
                    .get(..offset)
                    .expect("parser projection offset is a decoded string boundary"),
                None => value,
            };
            trim_leading_non_graphic(value).to_owned()
        }
        // `types.KindNull`.
        tidb_ast::Expr::Null => "NULL".to_owned(),
        // `types.KindBinaryLiteral`: "Don't rewrite BIT literal or HEX
        // literals" -- the source text is kept exactly, untrimmed.
        tidb_ast::Expr::Hex(_) | tidb_ast::Expr::Bit(_) | tidb_ast::Expr::CharsetBinary { .. } => {
            text.to_owned()
        }
        // `types.KindInt64` carrying `mysql.IsBooleanFlag`: the `TRUE` and
        // `FALSE` keywords are int64 literals whose flag says they were
        // written as booleans, and they are named by that value rather than
        // by the text (so `select FaLsE` is named `FALSE`).
        tidb_ast::Expr::Bool(value) => {
            if *value {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        // The `default` arm: every remaining numeric literal keeps its source
        // text with the unary-plus/parenthesis wrapper trimmed off both ends.
        _ => text
            .trim_start_matches(['\t', '\n', ' ', '+', '('])
            .trim_end_matches(['\t', '\n', ' ', ')'])
            .to_owned(),
    }
}

/// The value a literal argument contributes as a column label -- Go's
/// `evalAstExpr(...)` followed by `Datum.ToString()`, reached only from
/// `NAME_CONST`'s first argument, which `preprocess.go` guarantees is a
/// literal.
fn literal_label_value(expr: &tidb_ast::Expr) -> Option<String> {
    match expr {
        tidb_ast::Expr::String(value) | tidb_ast::Expr::RawString(value) => Some(value.clone()),
        tidb_ast::Expr::Int(text) | tidb_ast::Expr::Decimal(text) => Some(text.clone()),
        _ => None,
    }
}

/// Go `strings.TrimLeftFunc(projName, func(r rune) bool { return
/// !unicode.IsOneOf(mysql.RangeGraph, r) })`: drops leading characters that
/// are not "graphic" in MySQL's sense.
///
/// `tidb-mysql` owns the exact source category tables, so this does not depend
/// on the Unicode version bundled with Rust.
fn trim_leading_non_graphic(value: &str) -> &str {
    value.trim_start_matches(|c: char| !tidb_mysql::is_range_graph(c))
}

/// Go `getInnerFromParenthesesAndUnaryPlus`: strips enclosing parentheses and
/// leading unary `+` to reach the expression that decides the column name.
fn inner_field_expr(expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
    match expr {
        tidb_ast::Expr::Paren(inner) | tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Plus, inner) => {
            inner_field_expr(inner)
        }
        other => other,
    }
}

/// Whether `expr` is one of Go's `driver.ValueExpr` literals, which
/// `buildProjectionFieldNameFromExpressions` names by a rule of their own
/// rather than by the field's source text.
fn is_value_literal(expr: &tidb_ast::Expr) -> bool {
    matches!(
        expr,
        tidb_ast::Expr::Null
            | tidb_ast::Expr::Int(_)
            | tidb_ast::Expr::Decimal(_)
            | tidb_ast::Expr::Float(_)
            | tidb_ast::Expr::Hex(_)
            | tidb_ast::Expr::Bit(_)
            | tidb_ast::Expr::String(_)
            | tidb_ast::Expr::RawString(_)
            | tidb_ast::Expr::Bool(_)
            | tidb_ast::Expr::CharsetString { .. }
            | tidb_ast::Expr::CharsetBinary { .. }
    )
}

/// Go `SpecFieldPattern.ReplaceAllStringFunc(text, TrimComment)`: removes each
/// MySQL special-result-field comment marker -- a closing `*/`, and an opening
/// `/*!` optionally followed by a 5-6 digit (optionally `M`-prefixed) version
/// -- leaving the rest of the text, including a `/*+ ...` optimizer-hint
/// opener, untouched.
pub fn strip_spec_field_comment_markers(text: &str) -> String {
    let bytes = text.as_bytes();
    // Only ASCII marker sequences are removed from valid UTF-8, so copying the
    // surviving bytes and reinterpreting them never splits a code point.
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        // A closing `*/`.
        if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            i += 2;
            continue;
        }
        // An opening `/*!`, with an optional `M` and a 5-6 digit version, all
        // of which Go's `SpecFieldPattern` consumes as one match and
        // `TrimComment` drops.
        if bytes[i] == b'/' && i + 2 < bytes.len() && bytes[i + 1] == b'*' && bytes[i + 2] == b'!' {
            let mut j = i + 3;
            if j < bytes.len() && bytes[j] == b'M' {
                j += 1;
            }
            let digit_start = j;
            while j < bytes.len() && j - digit_start < 6 && bytes[j].is_ascii_digit() {
                j += 1;
            }
            // The version group is 5-6 digits, or absent; a 1-4 digit run is
            // not a version, so the marker is still just `/*!`.
            if j - digit_start == 0 || (5..=6).contains(&(j - digit_start)) {
                i = j;
                continue;
            }
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_owned())
}
