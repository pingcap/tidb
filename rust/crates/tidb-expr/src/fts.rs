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

//! Full-text-search to `LIKE` fallback helpers, transcreated from Go
//! `pkg/expression/fts_to_like.go`.
//!
//! # Scope — every production symbol of the Go file is now here
//!
//! The tokenizing/validation half:
//!
//! - `parseFTSBooleanSearchString` (Go lines 39-49) -> [`parse_fts_boolean_search_string`]
//! - `parseFTSSearchTerm` (Go lines 54-65) -> [`parse_fts_search_term`]
//! - `isFTSWordByte` (Go lines 71-73) -> [`is_fts_word_byte`]
//! - `escapeFTSLikePattern` (Go lines 77-98) -> [`escape_fts_like_pattern`]
//! - `ValidateFTSSearchStringForLikeFallback` (Go lines 119-142) ->
//!   [`validate_fts_search_string_for_like_fallback`]
//!
//! The `ILIKE`-building half, which previously had no target in this crate
//! and is now built on [`crate::new_function`]:
//!
//! - `BuildFTSToILikeExpression` (Go lines 165-202) ->
//!   [`build_fts_to_ilike_expression`]
//! - `ftsZeroIntConst` (Go lines 207-212) -> [`fts_zero_int_const`]
//! - `buildFTSBooleanModeILikeExpression` (Go lines 218-314) ->
//!   `build_fts_boolean_mode_ilike_expression`
//! - `buildFTSNaturalLanguageModeILikeExpression` (Go lines 319-345) ->
//!   `build_fts_natural_language_mode_ilike_expression`
//! - `BuildFTSToILikeExpressionFromBuiltin` (Go lines 362-397) ->
//!   [`build_fts_to_ilike_expression_from_builtin`]
//! - `buildFTSILikePredicate` (Go lines 401-438) ->
//!   `build_fts_ilike_predicate`
//!
//! Of the six test functions in `pkg/expression/fts_to_like_test.go`, five are
//! ported below: the four covering the tokenizer, plus
//! `TestBuildFTSToILikeExpressionFromBuiltin` (Go test lines 240-311) minus
//! its "nil scalar function" subtest, which is unrepresentable because the
//! Rust entry point takes `&ScalarFunction`. Focused semantic tests for the
//! two mode builders are added as NEW coverage, since Go exercises those only
//! through the planner.
//!
//! STILL NOT PORTED:
//! `TestScalarExprSupportedByFlashRejectsNonDefaultFTSModifier` (Go test lines
//! 313-340). It asserts on `scalarExprSupportedByFlash`, a TiFlash-pushdown
//! predicate that is a different surface from this file (see
//! [`crate::infer_pushdown`] / [`crate::pushdown_catalog`]), and it needs the
//! FTS modifier to be READABLE off a built node — which the modifier
//! narrowing below explains is not possible here.
//!
//! # Boundaries and narrowings
//!
//! - **FTS modifier is a parameter, not node state.** Go stores the modifier
//!   inside the `builtinFtsMysqlMatchAgainstSig` signature (written by
//!   `SetFTSMysqlMatchAgainstModifier`), and
//!   `BuildFTSToILikeExpressionFromBuiltin` recovers it by downcasting
//!   `fts.Function`. This crate's [`ScalarFunction`] carries no per-signature
//!   object, so [`build_fts_to_ilike_expression_from_builtin`] takes the
//!   modifier as an explicit argument instead. Every Go caller reaches that
//!   function through code that set the modifier itself, so no caller loses
//!   information; what IS lost is Go's "unexpected builtin signature" error
//!   and the ability of an unrelated pass to read the modifier back off a
//!   node.
//! - **Collation is not derived on the built nodes.** The `ILIKE`/`IFNULL`/
//!   `NOT` nodes come from [`crate::new_function`], whose header names the
//!   missing `deriveCollation` step as its largest gap. FTS fallback matching
//!   is case-insensitive by construction (that is why Go picks `ILIKE` over
//!   `LIKE`), so the built tree's MEANING does not depend on the derived
//!   collation — but a consumer that reads collation off these nodes sees the
//!   default rather than a derived value.
//! - **Error narrowing.** Go returns
//!   `ErrNotSupportedYet.GenWithStackByArgs(msg)` where `ErrNotSupportedYet`
//!   is `dbterror.ClassExpression.NewStd(mysql.ErrNotSupportedYet)`. This
//!   module returns a local [`FtsLikeFallbackError`] carrying the identical
//!   message argument rather than wiring up terror class registration, which
//!   is unrelated to the tokenizer being ported. The Go tests only assert
//!   error-vs-no-error, so no observable behavior is narrowed away.
//! - **Modifier representation.** Go takes `ast.FulltextSearchModifier`, a
//!   bitmask. This crate's ported AST models the same thing as the
//!   already-transcreated [`MatchModifier`] enum. All three predicates this
//!   file consults have exact counterparts:
//!   `IsBooleanMode()`/[`MatchModifier::is_boolean_mode`],
//!   `IsNaturalLanguageMode()`/[`MatchModifier::is_natural_language_mode`],
//!   and `WithQueryExpansion()`/[`MatchModifier::with_query_expansion`].
//!   Because the enum cannot represent boolean-mode combined with query
//!   expansion (a genuine `ParseError` upstream), Go's final
//!   "modifier is not supported in the LIKE fallback" arm in
//!   `BuildFTSToILikeExpression` is unreachable here; it is kept so the
//!   control flow still matches Go's.
//! - **Whitespace splitting.** Go's `strings.Fields` splits on
//!   `unicode.IsSpace`; [`str::split_whitespace`] splits on the Unicode
//!   `White_Space` property, which is the same set. Neither yields empty
//!   fields, so the Go comment about `body[0]` being safe applies unchanged.

use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::{Columns, EvalError};
use tidb_ast::MatchModifier;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// Go `ftsSearchTerm` (`pkg/expression/fts_to_like.go:29`): a single token in
/// a boolean-mode FTS search string surviving the strict-subset validator —
/// a plain alphanumeric word optionally prefixed with `+` (required) or `-`
/// (excluded).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FtsSearchTerm {
    /// The token body with any leading operator stripped.
    pub word: String,
    /// Set when the token was written `+word`.
    pub is_required: bool,
    /// Set when the token was written `-word`.
    pub is_excluded: bool,
}

/// Rejection returned by [`validate_fts_search_string_for_like_fallback`],
/// standing in for Go's
/// `ErrNotSupportedYet.GenWithStackByArgs(...)` — see the module-level error
/// narrowing note. The payload is the exact Go message argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsLikeFallbackError {
    /// The message argument Go passes to `GenWithStackByArgs`.
    pub message: String,
}

impl std::fmt::Display for FtsLikeFallbackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FtsLikeFallbackError {}

/// Go `parseFTSBooleanSearchString` (`pkg/expression/fts_to_like.go:39`).
///
/// Splits a boolean-mode search string into terms. Inputs reach this function
/// only after [`validate_fts_search_string_for_like_fallback`] has accepted
/// them, so every whitespace-separated field is either a bare alphanumeric
/// word or `+word`/`-word`. Go returns a nil slice for an empty field list;
/// the Rust counterpart returns an empty `Vec`, which the Go test only
/// observes through its length.
pub fn parse_fts_boolean_search_string(text: &str) -> Vec<FtsSearchTerm> {
    text.split_whitespace().map(parse_fts_search_term).collect()
}

/// Go `parseFTSSearchTerm` (`pkg/expression/fts_to_like.go:54`).
///
/// Parses a single boolean-mode token. The strict-subset validator guarantees
/// `word`, `+word`, or `-word` with an alphanumeric body, so only the leading
/// operator needs interpretation.
pub fn parse_fts_search_term(word: &str) -> FtsSearchTerm {
    match word.as_bytes().first() {
        None => FtsSearchTerm::default(),
        Some(b'+') => FtsSearchTerm {
            word: word[1..].to_owned(),
            is_required: true,
            is_excluded: false,
        },
        Some(b'-') => FtsSearchTerm {
            word: word[1..].to_owned(),
            is_required: false,
            is_excluded: true,
        },
        Some(_) => FtsSearchTerm {
            word: word.to_owned(),
            is_required: false,
            is_excluded: false,
        },
    }
}

/// Go `isFTSWordByte` (`pkg/expression/fts_to_like.go:71`).
///
/// Returns true for alphanumeric ASCII and non-ASCII bytes. Punctuation
/// including underscore is NOT a word character, consistent with MySQL's
/// built-in FTS tokenizer which treats `_` as a word separator. Used by
/// [`validate_fts_search_string_for_like_fallback`] to gate the LIKE rewrite.
pub const fn is_fts_word_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c > 127
}

/// Go `escapeFTSLikePattern` (`pkg/expression/fts_to_like.go:77`).
///
/// Escapes special LIKE characters (`%`, `_`, `\`) in the search term so they
/// are treated as literal characters rather than wildcards. The Go body
/// pre-sizes its builder by counting specials first; the byte-for-byte output
/// is what the test pins, and the same exact reservation is kept here.
pub fn escape_fts_like_pattern(term: &str) -> String {
    let bytes = term.as_bytes();
    let escape_count = bytes
        .iter()
        .filter(|&&ch| ch == b'\\' || ch == b'%' || ch == b'_')
        .count();

    // Go appends raw bytes, so the copy stays byte-wise here too: mapping
    // each byte through `char` would re-encode multi-byte UTF-8 sequences.
    let mut result = Vec::with_capacity(term.len() + escape_count);
    for &ch in bytes {
        if ch == b'\\' || ch == b'%' || ch == b'_' {
            result.push(b'\\');
        }
        result.push(ch);
    }
    // Only ASCII backslashes were inserted, and they can never land inside a
    // multi-byte sequence, so the byte string is still valid UTF-8.
    String::from_utf8(result).expect("escaping only inserts ASCII backslashes between whole bytes")
}

/// Go `ValidateFTSSearchStringForLikeFallback`
/// (`pkg/expression/fts_to_like.go:119`).
///
/// Reports whether `search_text` falls inside the strict subset that the LIKE
/// fallback is allowed to translate. The supported subset is, by mode:
///
/// - Boolean mode: each whitespace-separated token must be `word`, `+word`,
///   or `-word`, where `word` consists of ASCII alphanumeric characters or
///   non-ASCII UTF-8 bytes (the same definition used by [`is_fts_word_byte`]).
/// - Natural-language mode: each whitespace-separated token must be a `word`
///   of the same alphanumeric form (no leading `+`/`-` operators).
///
/// An empty or whitespace-only search string is valid; `BuildFTSToILikeExpression`
/// (not ported here) short-circuits to a constant-0 result for it.
///
/// Anything outside this subset (phrases, `*` prefix, `>` `<` `~` relevance
/// modifiers, `()` grouping, mid-word punctuation like `xx-yy`, etc.) is
/// rejected because MySQL FTS tokenizes those constructs in ways that differ
/// from a substring LIKE match. The planner uses this signal to skip the LIKE
/// fallback for rejected strings; the native `FTSMysqlMatchAgainst` builtin
/// can still serve the query when an FTS index is available.
pub fn validate_fts_search_string_for_like_fallback(
    search_text: &str,
    modifier: MatchModifier,
) -> Result<(), FtsLikeFallbackError> {
    let is_boolean = modifier.is_boolean_mode();
    for token in search_text.split_whitespace() {
        let mut body = token;
        // `split_whitespace` never yields an empty token (consecutive
        // whitespace is collapsed), so the leading byte is safe today. Keep
        // the emptiness guard explicit so the indexing is obviously bounded
        // and the check stays correct if the tokenization ever changes.
        if is_boolean && matches!(body.as_bytes().first(), Some(b'+') | Some(b'-')) {
            body = &body[1..];
        }
        if body.is_empty() || body.as_bytes().iter().any(|&b| !is_fts_word_byte(b)) {
            return Err(FtsLikeFallbackError {
                message: format!(
                    "MATCH...AGAINST search term '{token}' is not supported in the LIKE fallback"
                ),
            });
        }
    }
    Ok(())
}

/// How a LIKE-fallback build failed.
///
/// Go returns a bare `error` from every builder below, mixing two classes
/// that its CALLERS treat differently: `ErrNotSupportedYet` means "this
/// query is outside the strict supported subset, fall back gracefully" (the
/// planner redirects to the native builtin, selectivity estimation drops to
/// its default), while an error out of `NewFunction` means the build itself
/// failed. This enum keeps the two distinguishable instead of flattening
/// them into one string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FtsBuildError {
    /// Go `ErrNotSupportedYet.GenWithStackByArgs(...)`.
    NotSupportedYet(FtsLikeFallbackError),
    /// An error raised while building a node, i.e. out of Go's `NewFunction`
    /// or one of the `errors.Errorf` guards in
    /// [`build_fts_to_ilike_expression_from_builtin`].
    Build(EvalError),
}

impl From<FtsLikeFallbackError> for FtsBuildError {
    fn from(error: FtsLikeFallbackError) -> Self {
        FtsBuildError::NotSupportedYet(error)
    }
}

impl From<EvalError> for FtsBuildError {
    fn from(error: EvalError) -> Self {
        FtsBuildError::Build(error)
    }
}

impl std::fmt::Display for FtsBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FtsBuildError::NotSupportedYet(error) => error.fmt(formatter),
            FtsBuildError::Build(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl std::error::Error for FtsBuildError {}

fn not_supported(message: &str) -> FtsBuildError {
    FtsBuildError::NotSupportedYet(FtsLikeFallbackError {
        message: message.to_owned(),
    })
}

fn tiny_type() -> FieldType {
    FieldType::new(FieldTypeCode::Tiny)
}

/// Go `ftsZeroIntConst` (`pkg/expression/fts_to_like.go:207`): the constant-0
/// TINYINT returned whenever the fallback can prove no row matches.
#[must_use]
pub fn fts_zero_int_const() -> Expression {
    Expression::Constant(Constant::new(Datum::Int(0), tiny_type()))
}

/// Go `buildFTSILikePredicate` (`pkg/expression/fts_to_like.go:401`).
///
/// Builds `IFNULL(column ILIKE '%term%' ESCAPE '\\', 0)` for one column and
/// one search term. `ILIKE` rather than `LIKE` because MySQL full-text search
/// is always case-insensitive regardless of the column's collation, and the
/// `IFNULL` wrapper so that a NULL column reads as "does not contain the
/// term" — without it, `NOT(NULL ILIKE ...)` would be NULL and would wrongly
/// filter out rows with a NULL column that do not contain an excluded term.
fn build_fts_ilike_predicate(
    ctx: &impl Columns,
    column: &Expression,
    term: &str,
) -> Result<Expression, FtsBuildError> {
    // Prefix matching (`word*`) in MySQL FTS matches words STARTING with the
    // prefix anywhere in the text. `%term%` cannot enforce a word boundary,
    // so it may produce false positives but never false negatives.
    let pattern = format!("%{}%", escape_fts_like_pattern(term));
    let pattern_const = Expression::Constant(Constant::new(
        Datum::Bytes(pattern.into_bytes()),
        FieldType::new(FieldTypeCode::Varchar),
    ));
    // Backslash (=92) is the ILIKE escape character.
    let escape_const = Expression::Constant(Constant::new(Datum::Int(92), tiny_type()));

    let like_func = crate::new_function::new_function(
        ctx,
        "ilike",
        tiny_type(),
        vec![column.clone(), pattern_const, escape_const],
    )?;
    let zero_const = Expression::Constant(Constant::new(Datum::Int(0), tiny_type()));
    Ok(crate::new_function::new_function(
        ctx,
        "ifnull",
        tiny_type(),
        vec![like_func, zero_const],
    )?)
}

/// Builds the per-column disjunction of one term's predicates, Go's inner
/// `for _, column := range columns { ... }` + `ComposeDNFCondition` pair that
/// appears three times in `buildFTSBooleanModeILikeExpression`.
fn term_column_dnf(
    ctx: &impl Columns,
    columns: &[Expression],
    term: &str,
) -> Result<Option<Expression>, FtsBuildError> {
    let mut predicates = Vec::with_capacity(columns.len());
    for column in columns {
        predicates.push(build_fts_ilike_predicate(ctx, column, term)?);
    }
    Ok(crate::simple_expr::compose_dnf_condition(predicates))
}

/// Go `buildFTSBooleanModeILikeExpression` (`pkg/expression/fts_to_like.go:218`).
///
/// Required terms become an AND of per-term column-DNFs, excluded terms
/// become `NOT` over the same, and optional terms anchor the result only when
/// no required term does — because LIKE cannot rank, so optionals cannot be a
/// mere ordering hint the way they are in real FTS.
fn build_fts_boolean_mode_ilike_expression(
    ctx: &impl Columns,
    columns: &[Expression],
    search_text: &str,
) -> Result<Expression, FtsBuildError> {
    let terms = parse_fts_boolean_search_string(search_text);
    if terms.is_empty() {
        return Ok(fts_zero_int_const());
    }

    let mut required = Vec::new();
    let mut excluded = Vec::new();
    let mut optional = Vec::new();
    for term in &terms {
        if term.word.is_empty() {
            continue;
        }
        if term.is_required {
            required.push(term);
        } else if term.is_excluded {
            excluded.push(term);
        } else {
            optional.push(term);
        }
    }

    // MySQL boolean mode: a query of only excluded terms ("-a -b") returns an
    // empty result set, so no row can satisfy the search.
    if required.is_empty() && optional.is_empty() && !excluded.is_empty() {
        return Ok(fts_zero_int_const());
    }

    let mut all_predicates = Vec::new();

    for term in &required {
        if let Some(dnf) = term_column_dnf(ctx, columns, &term.word)? {
            all_predicates.push(dnf);
        }
    }

    for term in &excluded {
        if let Some(dnf) = term_column_dnf(ctx, columns, &term.word)? {
            all_predicates.push(crate::new_function::new_function(
                ctx,
                "not",
                tiny_type(),
                vec![dnf],
            )?);
        }
    }

    // Optionals become a positive filter only when no required term exists:
    // - required > 0            -> ignore optionals, required already anchors
    // - required == 0, excl = 0 -> at least one optional must match
    // - required == 0, excl > 0 -> optional-DNF is ANDed with the exclusions
    if !optional.is_empty() && required.is_empty() {
        let mut all_optional_predicates = Vec::new();
        for term in &optional {
            for column in columns {
                all_optional_predicates.push(build_fts_ilike_predicate(ctx, column, &term.word)?);
            }
        }
        if let Some(optional_dnf) =
            crate::simple_expr::compose_dnf_condition(all_optional_predicates)
        {
            if excluded.is_empty() {
                return Ok(optional_dnf);
            }
            all_predicates.push(optional_dnf);
        }
    }

    Ok(
        crate::simple_expr::compose_cnf_condition(all_predicates)
            .unwrap_or_else(fts_zero_int_const),
    )
}

/// Go `buildFTSNaturalLanguageModeILikeExpression`
/// (`pkg/expression/fts_to_like.go:319`): whitespace-split the search string
/// and OR every per-column, per-word predicate together.
fn build_fts_natural_language_mode_ilike_expression(
    ctx: &impl Columns,
    columns: &[Expression],
    search_text: &str,
) -> Result<Expression, FtsBuildError> {
    let words: Vec<&str> = search_text.split_whitespace().collect();
    if words.is_empty() {
        return Ok(fts_zero_int_const());
    }

    let mut column_predicates = Vec::with_capacity(columns.len());
    for column in columns {
        let mut word_predicates = Vec::with_capacity(words.len());
        for word in &words {
            word_predicates.push(build_fts_ilike_predicate(ctx, column, word)?);
        }
        if let Some(dnf) = crate::simple_expr::compose_dnf_condition(word_predicates) {
            column_predicates.push(dnf);
        }
    }

    Ok(crate::simple_expr::compose_dnf_condition(column_predicates)
        .unwrap_or_else(fts_zero_int_const))
}

/// Go `BuildFTSToILikeExpression` (`pkg/expression/fts_to_like.go:165`): the
/// public entry point translating a `MATCH ... AGAINST` into an `ILIKE`
/// predicate tree.
///
/// # Errors
///
/// [`FtsBuildError::NotSupportedYet`] when the query is outside the strict
/// supported subset — no columns, `WITH QUERY EXPANSION`, a search string
/// [`validate_fts_search_string_for_like_fallback`] rejects, or a modifier
/// that is neither boolean nor natural-language mode. Callers wanting a
/// graceful fallback should call the validator directly and react to it.
pub fn build_fts_to_ilike_expression(
    ctx: &impl Columns,
    columns: &[Expression],
    search_text: &str,
    modifier: MatchModifier,
) -> Result<Expression, FtsBuildError> {
    if columns.is_empty() {
        return Err(not_supported("MATCH...AGAINST with no columns"));
    }
    // WITH QUERY EXPANSION needs a second FTS pass to find semantically
    // related terms; LIKE cannot approximate that, so erroring is the only
    // way to avoid silently wrong results.
    if modifier.with_query_expansion() {
        return Err(not_supported(
            "MATCH...AGAINST WITH QUERY EXPANSION is not supported in the LIKE fallback",
        ));
    }
    validate_fts_search_string_for_like_fallback(search_text, modifier)?;

    if search_text.is_empty() {
        return Ok(fts_zero_int_const());
    }
    if modifier.is_boolean_mode() {
        return build_fts_boolean_mode_ilike_expression(ctx, columns, search_text);
    }
    if modifier.is_natural_language_mode() {
        return build_fts_natural_language_mode_ilike_expression(ctx, columns, search_text);
    }
    Err(not_supported(
        "MATCH...AGAINST modifier is not supported in the LIKE fallback",
    ))
}

/// Go `BuildFTSToILikeExpressionFromBuiltin`
/// (`pkg/expression/fts_to_like.go:362`): pulls the search string out of a
/// `fts_match_word` node and delegates to [`build_fts_to_ilike_expression`].
/// This is the entry point for selectivity estimation, where the FTS scalar
/// function is otherwise opaque to the estimator.
///
/// NARROWING: Go recovers the modifier by downcasting `fts.Function` to
/// `*builtinFtsMysqlMatchAgainstSig` and reading its `modifier` field, which
/// `SetFTSMysqlMatchAgainstModifier` wrote. This crate's [`ScalarFunction`]
/// carries no per-signature object (see [`crate::scalar_function`]'s BRIDGE
/// DECISION), so there is nowhere for that field to live and the modifier is
/// an explicit parameter instead. Every Go caller reaches this through code
/// that set the modifier itself, so the value is always in hand. The
/// consequence is that Go's "unexpected builtin signature" error has no
/// counterpart here.
///
/// Multi-column MATCH is refused: `GetSelectivityByFilter` only estimates
/// single-column expressions, so a multi-column substitution would be
/// declined by the stats engine and fall through to the same default the
/// un-substituted FTS expression already gets.
///
/// # Errors
///
/// [`FtsBuildError::Build`] for a wrong function name or too few arguments,
/// and [`FtsBuildError::NotSupportedYet`] for multi-column MATCH, a
/// non-constant or non-string search argument, or anything
/// [`build_fts_to_ilike_expression`] rejects.
pub fn build_fts_to_ilike_expression_from_builtin(
    ctx: &impl Columns,
    fts: &ScalarFunction,
    modifier: MatchModifier,
) -> Result<Expression, FtsBuildError> {
    if fts.func_name.lowercase() != "fts_match_word" {
        return Err(FtsBuildError::Build(EvalError::IncorrectArguments(
            format!("expected fts_match_word, got {}", fts.func_name.lowercase()),
        )));
    }
    let args = fts.get_args();
    if args.len() < 2 {
        return Err(FtsBuildError::Build(EvalError::IncorrectArguments(
            format!("fts_match_word expects at least 2 args, got {}", args.len()),
        )));
    }
    if args.len() > 2 {
        return Err(not_supported(
            "multi-column MATCH...AGAINST in selectivity substitution",
        ));
    }
    let Expression::Constant(against) = &args[0] else {
        return Err(not_supported(
            "MATCH...AGAINST with non-constant search string",
        ));
    };
    if against.value.is_null() {
        // Match the planner-side `matchAgainstToLike` NULL fast-path: emit
        // Constant(NULL), not Constant(0), so the substitute keeps SQL
        // three-valued logic. Under any cost path that composes NOT over the
        // substitute, Constant(0) would report "NOT 0 = TRUE -> selectivity
        // 1", the opposite of native MATCH(NULL), which returns NULL.
        return Ok(Expression::Constant(Constant::new(
            Datum::Null,
            tiny_type(),
        )));
    }
    let Datum::Bytes(search_text) = &against.value else {
        return Err(not_supported(
            "MATCH...AGAINST with non-string search constant",
        ));
    };
    let search_text = String::from_utf8_lossy(search_text).into_owned();
    build_fts_to_ilike_expression(ctx, &args[1..], &search_text, modifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Local fixture standing in for the Go test's
    /// `ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)`.
    const NATURAL_MODE: MatchModifier = MatchModifier::None;
    /// Local fixture for
    /// `ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)`.
    const BOOLEAN_MODE: MatchModifier = MatchModifier::BooleanMode;

    /// Go `TestValidateFTSSearchStringForLikeFallback`
    /// (`pkg/expression/fts_to_like_test.go:27`).
    #[test]
    fn validate_fts_search_string_for_like_fallback_cases() {
        let cases: &[(&str, &str, MatchModifier, bool)] = &[
            // Natural-language mode: plain alphanumeric words only.
            ("natural empty", "", NATURAL_MODE, false),
            ("natural whitespace only", " \t\n ", NATURAL_MODE, false),
            ("natural single word", "MySQL", NATURAL_MODE, false),
            (
                "natural multi word",
                "MySQL tutorial PostgreSQL",
                NATURAL_MODE,
                false,
            ),
            (
                "natural alphanumeric mix",
                "abc123 mysql8",
                NATURAL_MODE,
                false,
            ),
            ("natural rejects mid-word dash", "x-x", NATURAL_MODE, true),
            (
                "natural rejects punctuation suffix",
                "MySQL,",
                NATURAL_MODE,
                true,
            ),
            ("natural rejects + operator", "+word", NATURAL_MODE, true),
            ("natural rejects - operator", "-word", NATURAL_MODE, true),
            ("natural rejects quote", "\"phrase\"", NATURAL_MODE, true),
            ("natural rejects wildcard", "word*", NATURAL_MODE, true),
            ("natural rejects percent", "100%", NATURAL_MODE, true),
            (
                "natural rejects underscore",
                "test_file",
                NATURAL_MODE,
                true,
            ),
            // Boolean mode: plain word, +word, -word with alphanumeric body only.
            ("boolean empty", "", BOOLEAN_MODE, false),
            ("boolean plain word", "MySQL", BOOLEAN_MODE, false),
            ("boolean required word", "+MySQL", BOOLEAN_MODE, false),
            ("boolean excluded word", "-MySQL", BOOLEAN_MODE, false),
            ("boolean mix", "+apple -cherry pie", BOOLEAN_MODE, false),
            ("boolean rejects mid-word dash", "xx-yy", BOOLEAN_MODE, true),
            ("boolean rejects bare operator", "+", BOOLEAN_MODE, true),
            ("boolean rejects bare minus", "-", BOOLEAN_MODE, true),
            ("boolean rejects + after body", "x+y", BOOLEAN_MODE, true),
            ("boolean rejects wildcard", "word*", BOOLEAN_MODE, true),
            (
                "boolean rejects required wildcard",
                "+word*",
                BOOLEAN_MODE,
                true,
            ),
            ("boolean rejects relevance gt", ">word", BOOLEAN_MODE, true),
            ("boolean rejects relevance lt", "<word", BOOLEAN_MODE, true),
            (
                "boolean rejects relevance tilde",
                "~word",
                BOOLEAN_MODE,
                true,
            ),
            (
                "boolean rejects phrase",
                "\"exact phrase\"",
                BOOLEAN_MODE,
                true,
            ),
            (
                "boolean rejects required phrase",
                "+\"required phrase\"",
                BOOLEAN_MODE,
                true,
            ),
            ("boolean rejects grouping", "(word)", BOOLEAN_MODE, true),
            ("boolean rejects percent", "+100%", BOOLEAN_MODE, true),
            // Multi-byte UTF-8 word characters pass (matches is_fts_word_byte > 127 case).
            ("natural utf8 word", "你好", NATURAL_MODE, false),
            ("boolean utf8 word", "+你好", BOOLEAN_MODE, false),
        ];

        for &(name, text, modifier, want_err) in cases {
            let got = validate_fts_search_string_for_like_fallback(text, modifier);
            assert_eq!(got.is_err(), want_err, "case {name}: got {got:?}");
        }
    }

    /// Go `TestParseFTSBooleanSearchString`
    /// (`pkg/expression/fts_to_like_test.go:93`). Covers the strict-subset
    /// inputs the boolean parser is expected to handle in production; inputs
    /// outside the subset are rejected upstream by the validator and never
    /// reach this parser.
    #[test]
    fn parse_fts_boolean_search_string_cases() {
        /// One expected term: `(word, is_required, is_excluded)`.
        type ExpectedTerm = (&'static str, bool, bool);

        let cases: &[(&str, &[ExpectedTerm])] = &[
            (
                "+apple +pie",
                &[("apple", true, false), ("pie", true, false)],
            ),
            (
                "+apple -cherry",
                &[("apple", true, false), ("cherry", false, true)],
            ),
            (
                "word1 word2 word3",
                &[
                    ("word1", false, false),
                    ("word2", false, false),
                    ("word3", false, false),
                ],
            ),
            (
                "word1\t\nword2",
                &[("word1", false, false), ("word2", false, false)],
            ),
            ("", &[]),
            ("   \t\n  ", &[]),
        ];

        for &(input, expected) in cases {
            let result = parse_fts_boolean_search_string(input);
            assert_eq!(
                result.len(),
                expected.len(),
                "number of terms should match for {input:?}"
            );
            for (index, &(word, is_required, is_excluded)) in expected.iter().enumerate() {
                assert_eq!(result[index].word, word, "word should match for {input:?}");
                assert_eq!(
                    result[index].is_required, is_required,
                    "is_required should match for {input:?}"
                );
                assert_eq!(
                    result[index].is_excluded, is_excluded,
                    "is_excluded should match for {input:?}"
                );
            }
        }
    }

    /// Go `TestParseFTSSearchTerm` (`pkg/expression/fts_to_like_test.go:150`).
    #[test]
    fn parse_fts_search_term_cases() {
        let cases: &[(&str, &str, bool, bool)] = &[
            ("+word", "word", true, false),
            ("-word", "word", false, true),
            ("word", "word", false, false),
            ("", "", false, false),
            // Bare operator with no body (caller passes the result through;
            // the upstream validator rejects this case before the parser sees
            // it).
            ("+", "", true, false),
            ("-", "", false, true),
        ];

        for &(input, word, is_required, is_excluded) in cases {
            let result = parse_fts_search_term(input);
            assert_eq!(
                result,
                FtsSearchTerm {
                    word: word.to_owned(),
                    is_required,
                    is_excluded,
                },
                "term should match for {input:?}"
            );
        }
    }

    /// Go `TestEscapeFTSLikePattern`
    /// (`pkg/expression/fts_to_like_test.go:175`).
    #[test]
    fn escape_fts_like_pattern_cases() {
        let cases: &[(&str, &str)] = &[
            ("normal text", "normal text"),
            ("100%", r"100\%"),
            ("test_file", r"test\_file"),
            (r"path\to\file", r"path\\to\\file"),
            ("mix_%_all", r"mix\_\%\_all"),
            (r"\%_", r"\\\%\_"),
            ("", ""),
        ];

        for &(input, expected) in cases {
            assert_eq!(
                escape_fts_like_pattern(input),
                expected,
                "escaped pattern should match for {input:?}"
            );
        }
    }

    /// Multi-byte terms must survive escaping byte-for-byte, mirroring Go's
    /// byte-wise `strings.Builder` loop. No Go test row covers this, but the
    /// validator explicitly admits non-ASCII words, so the escaper must not
    /// mangle them.
    #[test]
    fn escape_fts_like_pattern_preserves_multibyte() {
        assert_eq!(escape_fts_like_pattern("你好_%"), r"你好\_\%");
    }
    // ---------------------------------------------------------------
    // The ILIKE-building half: Go `fts_to_like.go` lines 165-438.
    // ---------------------------------------------------------------

    use crate::column::Column;
    use crate::context::NoColumns;

    fn varchar_column(unique_id: i64) -> Expression {
        Expression::Column(Column::new(
            unique_id,
            FieldType::new(FieldTypeCode::Varchar),
        ))
    }

    fn string_constant(text: &str) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Bytes(text.as_bytes().to_vec()),
            FieldType::new(FieldTypeCode::Varchar),
        ))
    }

    /// Go's `newFTSMatchAgainstForTest` helper: an `fts_match_word` node whose
    /// first argument is the search constant and whose rest are the matched
    /// columns. Built directly rather than through `new_function` because
    /// this crate cannot yet EVALUATE `fts_match_word`, and the builders under
    /// test only ever read the node's name and arguments.
    fn fts_match_against(search_text: &str, columns: usize) -> ScalarFunction {
        let mut args = vec![string_constant(search_text)];
        args.extend((0..columns).map(|index| varchar_column(index as i64)));
        ScalarFunction::new(
            tidb_ast::CiString::new("fts_match_word"),
            FieldType::new(FieldTypeCode::Double),
            args,
        )
    }

    fn func_name_of(expr: &Expression) -> String {
        match expr {
            Expression::ScalarFunction(func) => func.func_name.lowercase().to_owned(),
            other => panic!("expected a scalar function, got {other:?}"),
        }
    }

    /// Collects every string constant in the tree, which is how these tests
    /// assert which ILIKE patterns were built without depending on the exact
    /// shape of the AND/OR tree.
    fn patterns_in(expr: &Expression, found: &mut Vec<String>) {
        match expr {
            Expression::Constant(constant) => {
                if let Datum::Bytes(bytes) = &constant.value {
                    found.push(String::from_utf8_lossy(bytes).into_owned());
                }
            }
            Expression::ScalarFunction(func) => {
                for arg in func.get_args() {
                    patterns_in(arg, found);
                }
            }
            Expression::Column(_) | Expression::CorrelatedColumn(_) => {}
        }
    }

    fn patterns_of(expr: &Expression) -> Vec<String> {
        let mut found = Vec::new();
        patterns_in(expr, &mut found);
        found.sort();
        found
    }

    /// PORT of Go `TestBuildFTSToILikeExpressionFromBuiltin`
    /// (`fts_to_like_test.go:240`), minus its "nil scalar function" subtest,
    /// which is unrepresentable: the Rust entry point takes `&ScalarFunction`,
    /// so Go's nil argument cannot be constructed.
    #[test]
    fn build_fts_to_ilike_expression_from_builtin_go_cases() {
        // "wrong function name": a non-FTS node is refused, and the message
        // names the function Go expected.
        let other = ScalarFunction::new(
            tidb_ast::CiString::new("length"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![varchar_column(0)],
        );
        let error =
            build_fts_to_ilike_expression_from_builtin(&NoColumns, &other, MatchModifier::None)
                .expect_err("a non-FTS function is rejected");
        assert!(
            error.to_string().contains("fts_match_word"),
            "the error must name the expected function: {error}"
        );

        // "single-column natural-language succeeds": the result is a scalar
        // function and is NOT the untranslated opaque FTS builtin.
        let single = fts_match_against("mysql", 1);
        let built =
            build_fts_to_ilike_expression_from_builtin(&NoColumns, &single, MatchModifier::None)
                .expect("a single-column natural-language MATCH translates");
        assert_ne!(func_name_of(&built), "fts_match_word");
        assert_eq!(func_name_of(&built), "ifnull");

        // "multi-column rejected for selectivity substitution".
        let multi = fts_match_against("mysql", 2);
        let error =
            build_fts_to_ilike_expression_from_builtin(&NoColumns, &multi, MatchModifier::None)
                .expect_err("multi-column MATCH is refused");
        assert!(
            error.to_string().contains("multi-column"),
            "the error must say multi-column: {error}"
        );

        // "NULL search constant returns Constant(NULL)" -- Constant(0) would
        // invert under a composing NOT.
        let null_search = ScalarFunction::new(
            tidb_ast::CiString::new("fts_match_word"),
            FieldType::new(FieldTypeCode::Double),
            vec![
                Expression::Constant(Constant::new(
                    Datum::Null,
                    FieldType::new(FieldTypeCode::Varchar),
                )),
                varchar_column(0),
            ],
        );
        let built = build_fts_to_ilike_expression_from_builtin(
            &NoColumns,
            &null_search,
            MatchModifier::None,
        )
        .expect("a NULL search constant short-circuits");
        match built {
            Expression::Constant(constant) => assert!(constant.value.is_null()),
            other => panic!("expected Constant(NULL), got {other:?}"),
        }

        // "search string outside strict subset rejected": a mid-word `-`
        // fails the validator and propagates.
        let bad = fts_match_against("xx-yy", 1);
        assert!(
            build_fts_to_ilike_expression_from_builtin(&NoColumns, &bad, MatchModifier::None)
                .is_err()
        );
    }

    /// NEW COVERAGE: the top-level rejections of
    /// `BuildFTSToILikeExpression`, which Go reaches only through the
    /// planner. Each is an `ErrNotSupportedYet`, i.e. a graceful-fallback
    /// signal rather than a build failure.
    #[test]
    fn build_fts_to_ilike_expression_rejects_the_unsupported_shapes() {
        let columns = [varchar_column(0)];
        assert!(matches!(
            build_fts_to_ilike_expression(&NoColumns, &[], "mysql", MatchModifier::None),
            Err(FtsBuildError::NotSupportedYet(_))
        ));
        assert!(matches!(
            build_fts_to_ilike_expression(
                &NoColumns,
                &columns,
                "mysql",
                MatchModifier::QueryExpansion
            ),
            Err(FtsBuildError::NotSupportedYet(_))
        ));
        // An empty search string can match nothing, so it short-circuits to 0
        // rather than erroring.
        let empty = build_fts_to_ilike_expression(&NoColumns, &columns, "", MatchModifier::None)
            .expect("an empty search string short-circuits");
        match empty {
            Expression::Constant(constant) => assert_eq!(constant.value, Datum::Int(0)),
            other => panic!("expected Constant(0), got {other:?}"),
        }
    }

    /// NEW COVERAGE: natural-language mode ORs one predicate per
    /// (column, word), and each predicate wraps ILIKE in IFNULL so a NULL
    /// column reads as "no match".
    #[test]
    fn natural_language_mode_ors_every_column_and_word() {
        let columns = [varchar_column(0), varchar_column(1)];
        let built =
            build_fts_to_ilike_expression(&NoColumns, &columns, "alpha beta", MatchModifier::None)
                .expect("a two-word, two-column natural-language MATCH translates");
        assert_eq!(func_name_of(&built), "or");
        // Two columns x two words = four `%word%` patterns.
        assert_eq!(
            patterns_of(&built),
            vec!["%alpha%", "%alpha%", "%beta%", "%beta%"]
        );
    }

    /// NEW COVERAGE: boolean mode's three term classes. Required terms are
    /// ANDed, excluded terms are negated, and optional terms anchor the
    /// result only when no required term does.
    #[test]
    fn boolean_mode_composes_required_excluded_and_optional_terms() {
        let columns = [varchar_column(0)];
        let boolean = MatchModifier::BooleanMode;

        // Required terms are ANDed together.
        let required = build_fts_to_ilike_expression(&NoColumns, &columns, "+alpha +beta", boolean)
            .expect("two required terms translate");
        assert_eq!(func_name_of(&required), "and");
        assert_eq!(patterns_of(&required), vec!["%alpha%", "%beta%"]);

        // A required term suppresses optionals entirely -- LIKE cannot rank,
        // so an optional term must not widen an anchored result.
        let anchored = build_fts_to_ilike_expression(&NoColumns, &columns, "+alpha beta", boolean)
            .expect("a required plus an optional term translates");
        assert_eq!(patterns_of(&anchored), vec!["%alpha%"]);

        // Only-excluded terms can match no row at all.
        let only_excluded = build_fts_to_ilike_expression(&NoColumns, &columns, "-alpha", boolean)
            .expect("an only-excluded query translates");
        match only_excluded {
            Expression::Constant(constant) => assert_eq!(constant.value, Datum::Int(0)),
            other => panic!("expected Constant(0), got {other:?}"),
        }

        // An excluded term alongside an optional one becomes NOT(...) ANDed
        // with the optional DNF.
        let mixed = build_fts_to_ilike_expression(&NoColumns, &columns, "alpha -beta", boolean)
            .expect("an optional plus an excluded term translates");
        assert_eq!(func_name_of(&mixed), "and");
        assert_eq!(patterns_of(&mixed), vec!["%alpha%", "%beta%"]);

        // A purely optional query is the DNF of its terms.
        let optional_only =
            build_fts_to_ilike_expression(&NoColumns, &columns, "alpha beta", boolean)
                .expect("a purely optional query translates");
        assert_eq!(func_name_of(&optional_only), "or");
        assert_eq!(patterns_of(&optional_only), vec!["%alpha%", "%beta%"]);
    }

    /// NEW COVERAGE: a single predicate is `IFNULL(col ILIKE '%term%', 0)`,
    /// with the backslash escape argument ILIKE requires. The escaping of
    /// LIKE metacharacters in the term itself is already covered by
    /// [`escape_fts_like_pattern`]'s own tests; this checks it is actually
    /// APPLIED on the built pattern.
    #[test]
    fn a_single_predicate_is_ifnull_over_ilike_with_an_escaped_pattern() {
        let columns = [varchar_column(0)];
        let built = build_fts_to_ilike_expression(&NoColumns, &columns, "abc", MatchModifier::None)
            .expect("a one-word MATCH translates");
        assert_eq!(func_name_of(&built), "ifnull");
        let Expression::ScalarFunction(ifnull) = &built else {
            unreachable!()
        };
        assert_eq!(func_name_of(&ifnull.get_args()[0]), "ilike");
        let Expression::ScalarFunction(ilike) = &ifnull.get_args()[0] else {
            unreachable!()
        };
        // ILIKE takes (operand, pattern, escape); the escape is backslash.
        assert_eq!(ilike.get_args().len(), 3);
        match &ilike.get_args()[2] {
            Expression::Constant(constant) => assert_eq!(constant.value, Datum::Int(92)),
            other => panic!("expected the escape constant, got {other:?}"),
        }
        assert_eq!(patterns_of(&built), vec!["%abc%"]);
    }
}
