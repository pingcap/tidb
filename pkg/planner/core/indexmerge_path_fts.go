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
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
)

// deriveFTSIndexFilters returns `member of` predicates implied by the
// MATCH ... AGAINST filters on ds, for every multi-valued index those filters
// can use. The returned predicates are redundant - each is entailed by the
// MATCH that produced it - so adding them to the condition set cannot change
// which rows qualify. What it does is let the existing multi-valued index
// machinery recognise an access path, without teaching it anything about
// full-text search: it sees the same `'term' member of (<tokenize expr>)`
// shape a user could have written by hand.
//
// Nothing is derived unless a multi-valued index over the matched column
// already exists, built by the same analyzer the query compiled with. Without
// one there is no path to unlock, and a predicate that no index can answer
// would be pure overhead - re-tokenizing the document for every row.
//
// The MATCH itself must stay in the plan as a residual filter. The predicates
// here are an over-approximation - a phrase contributes its tokens but not
// their adjacency, and a prefix contributes nothing - so the index generates
// candidates and the MATCH decides.
func deriveFTSIndexFilters(ds *logicalop.DataSource) []expression.Expression {
	if ds == nil || len(ds.AllConds) == 0 {
		return nil
	}
	evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()

	var derived []expression.Expression
	for _, cond := range ds.AllConds {
		match := ftsMatchAgainstAsFilter(cond)
		if match == nil {
			continue
		}
		info, isLocal := expression.FTSMysqlMatchAgainstLocalEvalInfo(match)
		if !isLocal || info.MatchNothing {
			// Without local evaluation metadata the planner has not validated
			// this MATCH for local use, so it has no compiled query to read
			// terms from. A match-nothing query needs no index to answer.
			continue
		}
		matchArgs := match.GetArgs()
		if len(matchArgs) != 2 {
			// Multi-column MATCH would need every column covered by the same
			// index, and a multi-valued index covers exactly one expression.
			continue
		}
		matchedCol, ok := matchArgs[1].(*expression.Column)
		if !ok {
			continue
		}

		for _, path := range ds.PossibleAccessPaths {
			if !isMVIndexPath(path) {
				continue
			}
			idxCols, ok := PrepareIdxColsAndUnwrapArrayType(
				ds.Table.Meta(), path.Index, ds.TblColsByID, true)
			if !ok {
				continue
			}
			// The tokenized column need not be the only one. A composite index
			// such as (tenant_id, (CAST(FTS_TOKENIZE(body, ...) AS CHAR(84)
			// ARRAY))) bounds the token lookup to one tenant, and is the shape
			// a multi-tenant table wants. Whether the leading columns are
			// usable is decided below by the ordinary machinery, from the
			// query's own filters: with no equality on tenant_id there is no
			// range to build and no path appears.
			tokenizeExpr, indexedCol, indexConfig, ok := ftsTokenizeIndexColumn(evalCtx, idxCols)
			if !ok {
				continue
			}
			// The index must be built over the column being matched, with the
			// analyzer the query will use. A different analyzer produces a
			// different token stream, so its entries cannot answer this query.
			//
			// Compare by table column ID rather than by expression equality:
			// the reference inside a virtual column's expression is resolved
			// separately from the one in the query, so the two can denote the
			// same column with different unique IDs.
			indexedColRef, ok := indexedCol.(*expression.Column)
			if !ok || indexedColRef.ID != matchedCol.ID {
				continue
			}
			if !indexConfig.Equal(info.AnalyzerConfig) {
				continue
			}

			terms, ok := ftsIndexTermsForMatch(match, indexConfig)
			if !ok {
				continue
			}
			derived = append(derived,
				buildFTSMemberOfFilters(ds, tokenizeExpr, terms)...)
		}
	}
	return derived
}

// ftsMatchAgainstAsFilter returns the MATCH ... AGAINST call that cond keeps a
// row for, or nil when cond is not such a filter.
//
// Only a MATCH in positive position qualifies, and that restriction is a
// correctness requirement rather than a missed opportunity. The derived
// predicates become access conditions, so rows outside the ranges they build
// are never read. Under a negation the implication runs the other way - a row
// satisfying `NOT MATCH(body) AGAINST('+rare')` is precisely one the term
// 'rare' does not select - so deriving from it would drop every qualifying
// row. The same holds for a branch of an OR, where the other branch can keep
// rows the terms do not cover.
func ftsMatchAgainstAsFilter(cond expression.Expression) *expression.ScalarFunction {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return nil
	}
	switch sf.FuncName.L {
	case ast.FTSMysqlMatchAgainst:
		// `WHERE MATCH(...) AGAINST(...)`, the score used directly as a
		// condition, which keeps a row when the score is non-zero.
		return sf
	case ast.IsTruthWithoutNull, ast.IsTruthWithNull:
		// The same, after the wrapping a boolean context adds.
		return ftsMatchAgainstAsFilter(sf.GetArgs()[0])
	case ast.GT, ast.GE:
		// `MATCH(...) AGAINST(...) > 0`, written explicitly. A non-negative
		// bound keeps only matching rows; a negative one keeps everything, so
		// it implies nothing about the terms.
		match := ftsMatchAgainstAsFilter(sf.GetArgs()[0])
		if match == nil {
			return nil
		}
		bound, isConst := sf.GetArgs()[1].(*expression.Constant)
		if !isConst || bound.DeferredExpr != nil || bound.ParamMarker != nil {
			return nil
		}
		val, err := bound.Value.ToFloat64(types.DefaultStmtNoWarningContext)
		if err != nil || val < 0 {
			return nil
		}
		return match
	default:
		return nil
	}
}

// ftsTokenizeIndexColumn finds the tokenized column among an index's columns
// and reports the expression it is built over, the column it tokenizes, and the
// analyzer that produced it.
func ftsTokenizeIndexColumn(
	evalCtx expression.EvalContext,
	idxCols []*expression.Column,
) (tokenizeExpr, indexedCol expression.Expression, config fulltext.AnalyzerConfig, ok bool) {
	for _, idxCol := range idxCols {
		if idxCol.VirtualExpr == nil {
			continue
		}
		expr, isCast := unwrapJSONCast(idxCol.VirtualExpr)
		if !isCast {
			continue
		}
		col, cfg, isTokenize := expression.FTSTokenizeAnalyzerConfig(evalCtx, expr)
		if !isTokenize {
			continue
		}
		return expr, col, cfg, true
	}
	return nil, nil, fulltext.AnalyzerConfig{}, false
}

// ftsIndexTermsForMatch compiles the search string of a MATCH and returns the
// tokens an index may use for it. A non-constant search string is skipped: the
// value is not known until execution, so no access path can be built from it.
func ftsIndexTermsForMatch(
	match *expression.ScalarFunction,
	config fulltext.AnalyzerConfig,
) (fulltext.IndexTerms, bool) {
	searchConst, ok := match.GetArgs()[0].(*expression.Constant)
	if !ok || searchConst.DeferredExpr != nil || searchConst.ParamMarker != nil {
		return fulltext.IndexTerms{}, false
	}
	if searchConst.Value.Kind() != types.KindString {
		return fulltext.IndexTerms{}, false
	}
	query, err := fulltext.CompileBooleanQuery(searchConst.Value.GetString(), config)
	if err != nil {
		return fulltext.IndexTerms{}, false
	}
	return query.IndexTerms()
}

// buildFTSMemberOfFilters turns index terms into the predicates the
// multi-valued index machinery understands. Required terms become separate
// conjuncts so they can intersect; optional terms become one json_overlaps so
// they union.
func buildFTSMemberOfFilters(
	ds *logicalop.DataSource,
	tokenizeExpr expression.Expression,
	terms fulltext.IndexTerms,
) []expression.Expression {
	ctx := ds.SCtx().GetExprCtx()
	if len(terms.Required) > 0 {
		filters := make([]expression.Expression, 0, len(terms.Required))
		for _, token := range terms.Required {
			memberOf, err := expression.NewFunction(ctx, ast.JSONMemberOf,
				types.NewFieldType(mysql.TypeTiny),
				ftsTokenConst(token), tokenizeExpr)
			if err != nil {
				return nil
			}
			filters = append(filters, memberOf)
		}
		return filters
	}
	if len(terms.Optional) == 0 {
		return nil
	}
	tokens := make([]any, 0, len(terms.Optional))
	for _, token := range terms.Optional {
		tokens = append(tokens, token)
	}
	overlaps, err := expression.NewFunction(ctx, ast.JSONOverlaps,
		types.NewFieldType(mysql.TypeTiny),
		tokenizeExpr,
		&expression.Constant{
			Value:   types.NewJSONDatum(types.CreateBinaryJSON(tokens)),
			RetType: types.NewFieldType(mysql.TypeJSON),
		})
	if err != nil {
		return nil
	}
	return []expression.Expression{overlaps}
}

func ftsTokenConst(token string) *expression.Constant {
	tp := types.NewFieldType(mysql.TypeVarString)
	tp.SetCharset(mysql.UTF8MB4Charset)
	tp.SetCollate(mysql.UTF8MB4DefaultCollation)
	return &expression.Constant{Value: types.NewStringDatum(token), RetType: tp}
}
