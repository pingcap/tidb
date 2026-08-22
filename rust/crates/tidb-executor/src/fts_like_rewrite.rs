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

//! `MATCH ... AGAINST`'s LIKE fallback: Go `expression/fts_to_like.go` and
//! `expressionRewriter.matchAgainstToLike`.
//!
//! A `MATCH` in a DIRECT BOOLEAN CONTEXT -- every ancestor up to the
//! `WHERE`/`HAVING`/`ON` root is `AND`/`OR`/`NOT`/parens -- rewrites to
//! case-insensitive substring predicates: each term becomes
//! `IFNULL(col ILIKE '%term%' ESCAPE '\\', 0)`, composed by mode. Go limits
//! the rewrite to that context deliberately: a scalar position
//! (`MATCH(...) > 0.5`, `MATCH(...) IS NULL`, the field list) needs the
//! FLOAT relevance score, which only the native TiFlash builtin computes,
//! and substituting the rewrite's 0/1 there would silently produce wrong
//! rows. This tier has no TiFlash, so a scalar-position `MATCH` stays
//! unrewritten and fails as it always did -- which is also what Go answers
//! with no FTS replica.
//!
//! The rewrite happens on the AST, before planning, because this tier plans
//! from the AST: the produced tree is ordinary `Like`/`Func`/`Binary` nodes
//! the driver already evaluates, and every later stage -- pushdown, ranger,
//! EXPLAIN -- sees exactly what it would see had the user written the ILIKE
//! predicates by hand. That is Go's own shape: `matchAgainstToLike` runs
//! inside the expression rewriter and everything downstream sees the built
//! predicates.

use tidb_ast::{BinaryOp, Expr, MatchModifier, UnaryOp};

/// Answers whether every column a `MATCH` names resolves, in this SELECT's
/// own scope, to a STRING column. Go checks `EvalType() != ETString` on the
/// resolved expressions (`matchAgainstToLike`) and refuses with "Doesn't
/// support match search on a non-string column without fulltext index"; the
/// caller owns the catalog, so it owns the answer.
pub type ColumnsAreStrings<'a> = dyn Fn(&tidb_ast::SelectStmt, &[Vec<String>]) -> bool + 'a;

/// Rewrites every direct-boolean-context `MATCH ... AGAINST` under the
/// boolean roots of `select`: `WHERE`, `HAVING`, and each join's `ON`.
///
/// Runs only under `tidb_opt_enable_alternative_logical_plans` (default OFF,
/// Go `DefOptEnableAlternativeLogicalPlans = false`): without it Go builds
/// only the native TiFlash builtin, which errors here exactly as it errors
/// there with no FTS replica -- the corpus flips the variable both ways and
/// expects both outcomes.
pub fn rewrite_select_fts(
    select: &mut tidb_ast::SelectStmt,
    columns_are_strings: &ColumnsAreStrings<'_>,
) {
    // The closure needs the SELECT it is resolving in, and the boolean roots
    // need `&mut` -- so take the immutable facts out first.
    let probe = select.clone();
    if let Some(where_clause) = select.where_clause.as_mut() {
        rewrite_boolean_root(where_clause, &probe, columns_are_strings);
    }
    if let Some(having) = select.having.as_mut() {
        rewrite_boolean_root(having, &probe, columns_are_strings);
    }
    if let Some(join) = select.from.as_mut() {
        rewrite_join_fts(join, &probe, columns_are_strings);
    }
}

/// Every `ON` in the join tree is a boolean root of its own.
fn rewrite_join_fts(
    join: &mut tidb_ast::Join,
    probe: &tidb_ast::SelectStmt,
    columns_are_strings: &ColumnsAreStrings<'_>,
) {
    if let Some(on) = join.on.as_mut() {
        rewrite_boolean_root(on, probe, columns_are_strings);
    }
    for node in [Some(&mut join.left), join.right.as_mut()]
        .into_iter()
        .flatten()
    {
        if let tidb_ast::JoinNode::Join(inner) = node {
            rewrite_join_fts(inner, probe, columns_are_strings);
        }
    }
}

/// Go `inDirectMatchBooleanContext`: descend only through the connectives a
/// boolean context is made of. Any other node is a SCALAR position, and its
/// `MATCH` children stay untouched.
fn rewrite_boolean_root(
    expr: &mut Expr,
    probe: &tidb_ast::SelectStmt,
    columns_are_strings: &ColumnsAreStrings<'_>,
) {
    match expr {
        Expr::Paren(inner) => rewrite_boolean_root(inner, probe, columns_are_strings),
        Expr::Unary(UnaryOp::Not | UnaryOp::NotKeyword, inner) => {
            rewrite_boolean_root(inner, probe, columns_are_strings);
        }
        Expr::Binary(BinaryOp::LogicAnd | BinaryOp::LogicOr, lhs, rhs) => {
            rewrite_boolean_root(lhs, probe, columns_are_strings);
            rewrite_boolean_root(rhs, probe, columns_are_strings);
        }
        Expr::MatchAgainst {
            columns,
            against,
            modifier,
        } => {
            // Go refuses a non-string matched column ("Doesn't support match
            // search on a non-string column without fulltext index") BEFORE
            // the NULL fast path, which is why `match(int_col) against(NULL)`
            // errors rather than answering NULL. Leaving the node unrewritten
            // is that refusal here: the evaluator rejects a bare
            // `MatchAgainst`, as Go's native builtin does with no replica.
            if !columns_are_strings(probe, columns) {
                return;
            }
            if let Some(built) = build_fts_like(columns, against, *modifier) {
                *expr = built;
            }
        }
        _ => {}
    }
}

/// Go `matchAgainstToLike` + `BuildFTSToILikeExpression`, over the AST.
///
/// `None` leaves the node unrewritten -- a mutable search string, a search
/// text outside the strict subset, or `WITH QUERY EXPANSION` -- and the later
/// evaluation refuses it, which is Go's own outcome with no FTS replica to
/// fall back to.
fn build_fts_like(
    columns: &[Vec<String>],
    against: &Expr,
    modifier: MatchModifier,
) -> Option<Expr> {
    if columns.is_empty() || modifier.with_query_expansion() {
        return None;
    }
    // Go evaluates the AGAINST constant; a NULL search yields NULL, which is
    // what keeps three-valued logic honest under NOT -- a literal 0 would
    // make `NOT MATCH(...)` admit every row.
    let search = match against {
        Expr::String(text) | Expr::RawString(text) => text.clone(),
        Expr::Null => return Some(Expr::Null),
        // A `?` marker, user variable or expression: Go's
        // `SetSkipPlanCache` case, and outside the literal subset this
        // rewrite bakes into constants.
        _ => return None,
    };
    // Go `ValidateFTSSearchStringForLikeFallback`: tokens are `word`, and in
    // boolean mode optionally `+word`/`-word`, with an alphanumeric-or-UTF8
    // body. Anything else -- phrases, `*`, relevance operators, mid-word
    // punctuation -- tokenizes differently in MySQL FTS than a substring
    // match, and is refused rather than approximated.
    let boolean = modifier.is_boolean_mode();
    for token in search.split_whitespace() {
        let body = if boolean && (token.starts_with('+') || token.starts_with('-')) {
            &token[1..]
        } else {
            token
        };
        if body.is_empty() || !body.bytes().all(is_fts_word_byte) {
            return None;
        }
    }

    if search.split_whitespace().next().is_none() {
        return Some(zero());
    }
    if boolean {
        build_boolean_mode(columns, &search)
    } else {
        build_natural_language_mode(columns, &search)
    }
}

/// Go `isFTSWordByte`: ASCII alphanumerics and every non-ASCII byte.
/// Underscore is NOT a word character -- MySQL's tokenizer treats it as a
/// separator.
fn is_fts_word_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c > 127
}

/// Go `buildFTSNaturalLanguageModeILikeExpression`: any word in any column
/// matches, so the whole thing is one OR -- column-major, as Go's loops are.
fn build_natural_language_mode(columns: &[Vec<String>], search: &str) -> Option<Expr> {
    let mut predicates = Vec::new();
    for column in columns {
        for word in search.split_whitespace() {
            predicates.push(ilike_predicate(column, word));
        }
    }
    Some(compose(BinaryOp::LogicOr, predicates)?)
}

/// Go `buildFTSBooleanModeILikeExpression`.
fn build_boolean_mode(columns: &[Vec<String>], search: &str) -> Option<Expr> {
    let mut required = Vec::new();
    let mut excluded = Vec::new();
    let mut optional = Vec::new();
    for token in search.split_whitespace() {
        if let Some(word) = token.strip_prefix('+') {
            required.push(word);
        } else if let Some(word) = token.strip_prefix('-') {
            excluded.push(word);
        } else {
            optional.push(token);
        }
    }
    // MySQL boolean mode: a query with ONLY excluded terms returns nothing.
    if required.is_empty() && optional.is_empty() {
        return Some(zero());
    }
    let mut all = Vec::new();
    // Each required term: OR over the columns.
    for word in &required {
        let dnf = compose(
            BinaryOp::LogicOr,
            columns.iter().map(|c| ilike_predicate(c, word)).collect(),
        )?;
        all.push(dnf);
    }
    // Each excluded term: NOT(OR over the columns). The IFNULL inside each
    // leaf is what makes a NULL column count as "does not contain the term"
    // rather than poisoning the NOT.
    for word in &excluded {
        let dnf = compose(
            BinaryOp::LogicOr,
            columns.iter().map(|c| ilike_predicate(c, word)).collect(),
        )?;
        all.push(Expr::Unary(UnaryOp::NotKeyword, Box::new(dnf)));
    }
    // Optionals anchor the result only when nothing required does -- LIKE
    // cannot rank, so with required terms present they are ignored.
    if !optional.is_empty() && required.is_empty() {
        let mut preds = Vec::new();
        for word in &optional {
            for column in columns {
                preds.push(ilike_predicate(column, word));
            }
        }
        let dnf = compose(BinaryOp::LogicOr, preds)?;
        if excluded.is_empty() {
            return Some(dnf);
        }
        all.push(dnf);
    }
    if all.is_empty() {
        return Some(zero());
    }
    compose(BinaryOp::LogicAnd, all)
}

/// Go `buildFTSILikePredicate`: `IFNULL(col ILIKE '%term%' ESCAPE '\\', 0)`.
///
/// ILIKE rather than LIKE because MySQL full-text search is
/// case-insensitive regardless of the column's collation; IFNULL because a
/// NULL column is "does not contain the term", not NULL -- without it,
/// `NOT(NULL ILIKE ...)` is NULL and an excluded term filters rows it should
/// keep. The term is LIKE-escaped even though the validated subset admits no
/// `%`/`_`/`\` today, as Go escapes defensively.
fn ilike_predicate(column: &[String], term: &str) -> Expr {
    let mut escaped = String::with_capacity(term.len());
    for ch in term.chars() {
        if ch == '\\' || ch == '%' || ch == '_' {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    let like = Expr::Like {
        expr: Box::new(Expr::Column(column.to_vec())),
        pattern: Box::new(Expr::String(format!("%{escaped}%"))),
        not: false,
        ilike: true,
        // Go's escapeConst is 92, the backslash the AST spells as the
        // unwritten default.
        escape: None,
    };
    Expr::Func {
        name: "IFNULL".to_owned(),
        args: vec![like, Expr::Int("0".to_owned())],
        origin_position: 0,
    }
}

/// Go `ComposeDNFCondition`/`ComposeCNFCondition`: left-deep over the list.
fn compose(op: BinaryOp, mut predicates: Vec<Expr>) -> Option<Expr> {
    let mut iter = predicates.drain(..);
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| {
        Expr::Binary(op, Box::new(acc), Box::new(next))
    }))
}

/// Go `ftsZeroIntConst`: the constant no row satisfies.
fn zero() -> Expr {
    Expr::Int("0".to_owned())
}
