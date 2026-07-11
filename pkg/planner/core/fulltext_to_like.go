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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// convertMatchAgainstToLike converts a MATCH...AGAINST expression to ILIKE
// predicates. It is a thin wrapper around expression.BuildFTSToILikeExpression;
// the conversion logic lives in pkg/expression so the same translation can be
// shared with cardinality-based selectivity estimation (which substitutes the
// equivalent ILIKE form for the opaque FTSMysqlMatchAgainst builtin).
//
// This is a fallback rewrite since TiDB does not natively support full-text
// search outside the TiFlash FTS path. The planner only invokes it in
// direct-boolean predicate positions — every ancestor up to the
// WHERE / HAVING / JOIN ON root must be AND / OR / NOT / parens
// (see inDirectMatchBooleanContext in expression_rewriter.go). Scoring
// contexts (SELECT field list, ORDER BY) and scalar predicate positions
// (IS NULL, comparisons, CASE, arithmetic) keep the native
// FTSMysqlMatchAgainst builtin so the result is a float relevance score
// rather than 0/1, even though the native path then requires TiFlash at
// execution time. The semantic differences below therefore apply to
// direct-boolean predicate use only:
//
//  1. No relevance scoring — the synthesized ILIKE predicate produces a 0/1
//     boolean filter result, which is the only thing a direct-boolean
//     predicate position consumes. Relevance-score positions (ORDER BY,
//     scalar SELECT, MATCH ... = 0, MATCH ... > 0.5, etc.) are intentionally
//     NOT routed through this fallback; substituting 0/1 there would
//     silently corrupt the sort or the comparison.
//  2. No stop word filtering — searches for all words regardless of length
//     or commonness.
//  3. No word length limits — MySQL ignores words shorter than
//     ft_min_word_len (default 4); the ILIKE rewrite does not.
//  4. No word boundaries — LIKE %term% matches substrings anywhere, not just
//     complete words. Example: "cat" matches "concatenate", "category",
//     "application"; MySQL FTS only matches "cat" as a standalone word.
//     Enforcing word boundaries would require REGEXP, which we avoid.
//  5. Performance — LIKE predicates cannot use full-text indexes (much
//     slower on large datasets).
//
// Search-string subset accepted by the rewrite (enforced upstream by
// expression.ValidateFTSSearchStringForLikeFallback):
//
//   - Natural-language mode: whitespace-separated alphanumeric words only.
//   - Boolean mode: each token is `word`, `+word` (required), or `-word`
//     (excluded), where `word` is alphanumeric (ASCII or non-ASCII UTF-8).
//
// Anything outside that subset — phrases, * prefix, > < ~ relevance
// modifiers, () grouping, mid-word punctuation like `xx-yy` — is rejected
// at plan time with ErrNotSupportedYet because MySQL FTS tokenizes those
// constructs in ways a substring LIKE cannot reproduce. WITH QUERY
// EXPANSION is likewise rejected (no LIKE approximation exists for the
// second-pass tokenization).
func (er *expressionRewriter) convertMatchAgainstToLike(
	columns []expression.Expression,
	searchText string,
	modifier ast.FulltextSearchModifier,
) (expression.Expression, error) {
	return expression.BuildFTSToILikeExpression(er.sctx, columns, searchText, modifier)
}
