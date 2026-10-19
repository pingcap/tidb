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

package expression

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// ftsSearchTerm represents a single token in a boolean-mode FTS search string
// surviving the strict-subset validator: a plain alphanumeric word optionally
// prefixed with `+` (required) or `-` (excluded).
type ftsSearchTerm struct {
	word       string
	isRequired bool
	isExcluded bool
}

// parseFTSBooleanSearchString splits a boolean-mode search string into terms.
// Inputs reach this function only after ValidateFTSSearchStringForLikeFallback
// has accepted them, so every whitespace-separated field is either a bare
// alphanumeric word or `+word`/`-word`.
func parseFTSBooleanSearchString(text string) []ftsSearchTerm {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return nil
	}
	terms := make([]ftsSearchTerm, 0, len(fields))
	for _, w := range fields {
		terms = append(terms, parseFTSSearchTerm(w))
	}
	return terms
}

// parseFTSSearchTerm parses a single boolean-mode token. The strict-subset
// validator guarantees `word`, `+word`, or `-word` with an alphanumeric body,
// so only the leading operator needs interpretation.
func parseFTSSearchTerm(word string) ftsSearchTerm {
	if word == "" {
		return ftsSearchTerm{}
	}
	switch word[0] {
	case '+':
		return ftsSearchTerm{word: word[1:], isRequired: true}
	case '-':
		return ftsSearchTerm{word: word[1:], isExcluded: true}
	}
	return ftsSearchTerm{word: word}
}

// isFTSWordByte returns true for alphanumeric ASCII and non-ASCII bytes.
// Punctuation including underscore is NOT a word character, consistent with
// MySQL's built-in FTS tokenizer which treats _ as a word separator. Used by
// ValidateFTSSearchStringForLikeFallback to gate the LIKE rewrite.
func isFTSWordByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c > 127
}

// escapeFTSLikePattern escapes special LIKE characters (%, _, \) in the search term
// so they are treated as literal characters rather than wildcards.
func escapeFTSLikePattern(term string) string {
	// Count special characters to pre-allocate the exact buffer size needed
	escapeCount := 0
	for i := range len(term) {
		ch := term[i]
		if ch == '\\' || ch == '%' || ch == '_' {
			escapeCount++
		}
	}

	// Allocate exact size: original length + number of escape characters
	var result strings.Builder
	result.Grow(len(term) + escapeCount)
	for i := range len(term) {
		ch := term[i]
		if ch == '\\' || ch == '%' || ch == '_' {
			result.WriteByte('\\')
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// ValidateFTSSearchStringForLikeFallback reports whether searchText falls
// inside the strict subset that the LIKE fallback is allowed to translate.
// The supported subset is, by mode:
//
//   - Boolean mode: each whitespace-separated token must be `word`, `+word`,
//     or `-word`, where `word` consists of ASCII alphanumeric characters or
//     non-ASCII UTF-8 bytes (the same definition used by isFTSWordByte).
//   - Natural-language mode: each whitespace-separated token must be a `word`
//     of the same alphanumeric form (no leading +/- operators).
//
// An empty or whitespace-only search string is valid; BuildFTSToILikeExpression
// short-circuits to a constant-0 result for it.
//
// Anything outside this subset (phrases, * prefix, > < ~ relevance modifiers,
// () grouping, mid-word punctuation like `xx-yy`, etc.) is rejected because
// MySQL FTS tokenizes those constructs in ways that differ from a substring
// LIKE match. The planner uses this signal to skip the LIKE fallback for
// rejected strings; the native FTSMysqlMatchAgainst builtin can still serve
// the query when an FTS index is available.
func ValidateFTSSearchStringForLikeFallback(searchText string, modifier ast.FulltextSearchModifier) error {
	isBoolean := modifier.IsBooleanMode()
	for _, token := range strings.Fields(searchText) {
		body := token
		// strings.Fields never returns an empty token (consecutive whitespace
		// is collapsed), so body[0] is safe today. Keep the len(body) > 0
		// guard explicit so the indexing is obviously bounded and the check
		// stays correct if the tokenization ever changes.
		if isBoolean && len(body) > 0 && (body[0] == '+' || body[0] == '-') {
			body = body[1:]
		}
		if body == "" {
			return ErrNotSupportedYet.GenWithStackByArgs(
				"MATCH...AGAINST search term '" + token + "' is not supported in the LIKE fallback")
		}
		for i := range len(body) {
			if !isFTSWordByte(body[i]) {
				return ErrNotSupportedYet.GenWithStackByArgs(
					"MATCH...AGAINST search term '" + token + "' is not supported in the LIKE fallback")
			}
		}
	}
	return nil
}

// BuildFTSToILikeExpression converts a MATCH...AGAINST input (a list of column
// expressions, the search-string literal, and the parsed modifier) into an
// equivalent ILIKE-based predicate expression.
//
// Two callers share this conversion:
//   - the planner's MATCH...AGAINST LIKE fallback rewrite, used by the
//     "fts-like-fallback" alternative round when round 1 reports that the
//     native FTSMysqlMatchAgainst builtin cannot serve a predicate-context
//     MATCH (no FTS index on a TiFlash replica, modifier not pushdown-supported);
//   - selectivity estimation, which substitutes the same ILIKE form for the
//     opaque FTSMysqlMatchAgainst builtin so round 1's cost is computed from
//     column statistics rather than a flat default — the native builtin
//     cannot be evaluated in TiDB and would otherwise fall through to a
//     SelectivityFactor (0.8) that ignores the column's histogram.
//
// Returns an integer (0/1) typed expression suitable for direct use as a
// filter predicate.
//
// Semantic differences from MySQL's full-text search are documented in detail
// at the planner-level call site; this helper preserves those approximations
// so both callers see the same translated expression.
func BuildFTSToILikeExpression(
	ctx BuildContext,
	columns []Expression,
	searchText string,
	modifier ast.FulltextSearchModifier,
) (Expression, error) {
	if len(columns) == 0 {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with no columns")
	}

	// WITH QUERY EXPANSION requires a second FTS pass to find semantically related
	// terms; LIKE cannot approximate this. Error explicitly rather than silently
	// producing wrong results.
	if modifier.WithQueryExpansion() {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST WITH QUERY EXPANSION is not supported in the LIKE fallback")
	}

	// Reject search strings outside the strict supported subset before we
	// translate. Callers that want a graceful fallback (e.g. the planner
	// redirecting to the native builtin, or selectivity estimation falling
	// through to a default estimate) should call this validator directly and
	// react to its error.
	if err := ValidateFTSSearchStringForLikeFallback(searchText, modifier); err != nil {
		return nil, err
	}

	if searchText == "" {
		return ftsZeroIntConst(), nil
	}

	if modifier.IsBooleanMode() {
		return buildFTSBooleanModeILikeExpression(ctx, columns, searchText)
	}
	if modifier.IsNaturalLanguageMode() {
		return buildFTSNaturalLanguageModeILikeExpression(ctx, columns, searchText)
	}
	return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST modifier is not supported in the LIKE fallback")
}

// ftsZeroIntConst returns the constant-0 tiny-int expression used whenever
// the LIKE fallback can prove no row will match (empty search string, all
// terms tokenized away, or boolean-mode "only excluded" queries).
func ftsZeroIntConst() Expression {
	return &Constant{
		Value:   types.NewIntDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
}

// buildFTSBooleanModeILikeExpression handles `IN BOOLEAN MODE`. Required
// terms become an AND of per-term column-DNFs, excluded terms become NOT over
// per-term column-DNFs, and optional terms anchor the result only when no
// required terms exist (since LIKE cannot rank).
func buildFTSBooleanModeILikeExpression(ctx BuildContext, columns []Expression, searchText string) (Expression, error) {
	terms := parseFTSBooleanSearchString(searchText)
	if len(terms) == 0 {
		return ftsZeroIntConst(), nil
	}

	var required, excluded, optional []ftsSearchTerm
	for _, term := range terms {
		if term.word == "" {
			continue
		}
		if term.isRequired {
			required = append(required, term)
		} else if term.isExcluded {
			excluded = append(excluded, term)
		} else {
			optional = append(optional, term)
		}
	}

	// MySQL Boolean mode: a query with only excluded terms ("-a -b") returns
	// an empty result set. The LIKE fallback must match this: when there are
	// no required and no optional terms, no row can possibly satisfy the
	// search, so return a constant FALSE immediately.
	if len(required) == 0 && len(optional) == 0 && len(excluded) > 0 {
		return ftsZeroIntConst(), nil
	}

	var allPredicates []Expression

	// For each required term: (col1 ILIKE %term% OR col2 ILIKE %term% ...)
	for _, term := range required {
		var termColumnPreds []Expression
		for _, column := range columns {
			pred, err := buildFTSILikePredicate(ctx, column, term.word)
			if err != nil {
				return nil, err
			}
			termColumnPreds = append(termColumnPreds, pred)
		}
		if len(termColumnPreds) > 0 {
			allPredicates = append(allPredicates, ComposeDNFCondition(ctx, termColumnPreds...))
		}
	}

	// For each excluded term: NOT(col1 ILIKE %term% OR col2 ILIKE %term% ...)
	for _, term := range excluded {
		var termColumnPreds []Expression
		for _, column := range columns {
			pred, err := buildFTSILikePredicate(ctx, column, term.word)
			if err != nil {
				return nil, err
			}
			termColumnPreds = append(termColumnPreds, pred)
		}
		if len(termColumnPreds) > 0 {
			notPred, err := NewFunction(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny),
				ComposeDNFCondition(ctx, termColumnPreds...))
			if err != nil {
				return nil, err
			}
			allPredicates = append(allPredicates, notPred)
		}
	}

	// For optional terms: since LIKE cannot rank, treat optionals as a
	// positive filter when no required terms exist.
	// - required>0: ignore optionals (required terms already anchor the result)
	// - required==0, excluded==0: at least one optional must match (pure optional query)
	// - required==0, excluded>0: at least one optional must match AND excluded terms
	//   must be absent; AND the optional-DNF into allPredicates below
	if len(optional) > 0 && len(required) == 0 {
		var allOptionalPreds []Expression
		for _, term := range optional {
			for _, column := range columns {
				pred, err := buildFTSILikePredicate(ctx, column, term.word)
				if err != nil {
					return nil, err
				}
				allOptionalPreds = append(allOptionalPreds, pred)
			}
		}
		if len(allOptionalPreds) > 0 {
			optionalDNF := ComposeDNFCondition(ctx, allOptionalPreds...)
			if len(excluded) == 0 {
				return optionalDNF, nil
			}
			allPredicates = append(allPredicates, optionalDNF)
		}
	}

	if len(allPredicates) == 0 {
		return ftsZeroIntConst(), nil
	}

	return ComposeCNFCondition(ctx, allPredicates...), nil
}

// buildFTSNaturalLanguageModeILikeExpression handles the default
// natural-language mode by splitting the search string into whitespace
// tokens and OR-ing per-column per-word ILIKE predicates together.
func buildFTSNaturalLanguageModeILikeExpression(ctx BuildContext, columns []Expression, searchText string) (Expression, error) {
	words := strings.Fields(searchText)
	if len(words) == 0 {
		return ftsZeroIntConst(), nil
	}

	var columnPredicates []Expression
	for _, column := range columns {
		var wordPredicates []Expression
		for _, word := range words {
			pred, err := buildFTSILikePredicate(ctx, column, word)
			if err != nil {
				return nil, err
			}
			wordPredicates = append(wordPredicates, pred)
		}
		if len(wordPredicates) > 0 {
			columnPredicates = append(columnPredicates, ComposeDNFCondition(ctx, wordPredicates...))
		}
	}

	if len(columnPredicates) == 0 {
		return ftsZeroIntConst(), nil
	}

	return ComposeDNFCondition(ctx, columnPredicates...), nil
}

// BuildFTSToILikeExpressionFromBuiltin pulls the search string and modifier
// out of a MATCH...AGAINST scalar function (FTSMysqlMatchAgainst) and
// delegates to BuildFTSToILikeExpression. It is the entry point for
// selectivity estimation, where the FTS scalar function is opaque to the
// stats engine; substituting an equivalent ILIKE expression lets the engine
// reuse its TopN/histogram-based estimation paths instead of falling back
// to a flat default that ignores column statistics.
//
// Restricted to single-column MATCH: GetSelectivityByFilter only estimates
// expressions over a single column, so a multi-column substituted ILIKE would
// be declined by the stats engine and fall through to the same str-match
// default that the un-substituted FTS expression already receives. Returning
// an error for the multi-column case lets the selectivity caller's existing
// err-check fall through cleanly, without producing a substitute that would
// never improve the estimate.
func BuildFTSToILikeExpressionFromBuiltin(ctx BuildContext, fts *ScalarFunction) (Expression, error) {
	if fts == nil || fts.FuncName.L != ast.FTSMysqlMatchAgainst {
		return nil, errors.Errorf("expected %s, got %v", ast.FTSMysqlMatchAgainst, fts)
	}
	args := fts.GetArgs()
	if len(args) < 2 {
		return nil, errors.Errorf("%s expects at least 2 args, got %d", ast.FTSMysqlMatchAgainst, len(args))
	}
	if len(args) > 2 {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("multi-column MATCH...AGAINST in selectivity substitution")
	}
	againstConst, ok := args[0].(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with non-constant search string")
	}
	if againstConst.Value.IsNull() {
		// Match the planner-side matchAgainstToLike NULL fast-path: emit
		// Constant(NULL) so the substitute preserves SQL three-valued logic
		// even though selectivity estimation does not currently exploit the
		// difference. Constant(0) here would, under any future cost path that
		// composes NOT over the substitute, report "NOT 0 = TRUE → selectivity
		// 1" — opposite of native MATCH(NULL) which returns NULL.
		return &Constant{
			Value:   types.Datum{},
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}
	if againstConst.Value.Kind() != types.KindString {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with non-string search constant")
	}
	sig, ok := fts.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return nil, errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, fts.Function)
	}
	return BuildFTSToILikeExpression(ctx, args[1:], againstConst.Value.GetString(), sig.modifier)
}

// buildFTSILikePredicate builds a single ILIKE predicate for a column and search term,
// wrapped in IFNULL so that NULL columns are treated as not containing the term.
func buildFTSILikePredicate(ctx BuildContext, column Expression, term string) (Expression, error) {
	escapedTerm := escapeFTSLikePattern(term)

	// NOTE: Prefix matching (word*) in MySQL full-text search matches words that START with
	// the prefix, but the word can appear anywhere in the text. Using LIKE without REGEXP,
	// we cannot perfectly enforce word-start boundaries. We use %term% which may produce
	// false positives but avoids false negatives.
	pattern := "%" + escapedTerm + "%"

	patternConst := &Constant{
		Value:   types.NewStringDatum(pattern),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}

	// Backslash escape character (=92) for ILIKE.
	escapeConst := &Constant{
		Value:   types.NewIntDatum(92),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// MySQL full-text search is always case-insensitive regardless of column
	// collation, so ILIKE matches that semantic rather than plain LIKE which
	// would follow the column's collation.
	likeFunc, err := NewFunction(ctx, ast.Ilike, types.NewFieldType(mysql.TypeTiny), column, patternConst, escapeConst)
	if err != nil {
		return nil, err
	}

	// Wrap with IFNULL so a NULL column is treated as not containing the term
	// (consistent with MySQL FTS semantics where NULL columns are ignored).
	// Without this, NOT(NULL ILIKE %term%) = NOT(NULL) = NULL which incorrectly
	// filters rows that have a NULL column and don't contain the excluded term.
	zeroConst := &Constant{
		Value:   types.NewIntDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	return NewFunction(ctx, ast.Ifnull, types.NewFieldType(mysql.TypeTiny), likeFunc, zeroConst)
}
