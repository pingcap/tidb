// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/matchagainst"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// FTSInfo is an easy to use struct for interpreting a FullTextSearch expression.
type FTSInfo struct {
	Query  string
	Column *Column
}

// FTSFuncMap stores the functions related to fulltext search.
var FTSFuncMap map[string]struct{} = map[string]struct{}{
	ast.FTSMatchWord:         {},
	ast.FTSMatchPrefix:       {},
	ast.FTSMatchPhrase:       {},
	ast.FTSMysqlMatchAgainst: {},
}

// ContainsFullTextSearchFn recursively checks whether the expression tree contains a
// possible FullTextSearch function.
func ContainsFullTextSearchFn(exprs ...Expression) bool {
	for _, expr := range exprs {
		switch x := expr.(type) {
		case *ScalarFunction:
			if _, ok := FTSFuncMap[x.FuncName.L]; ok {
				return true
			}
			for _, arg := range x.GetArgs() {
				if ContainsFullTextSearchFn(arg) {
					return true
				}
			}
		}
	}
	return false
}

// ExprCoveredByOneTiCIIndex checks whether the given expr is fully covered by one TiCI index.
// Single-column FTS helper functions (`fts_match_xxx`) only require their matched column to
// belong to the helper-eligible FTS column set, while multi-column FTS expressions must match
// the column set of the regular FULLTEXT index definition.
func ExprCoveredByOneTiCIIndex(
	expr Expression,
	ftsCols *intset.FastIntSet,
	colsInFulltextIdx *intset.FastIntSet,
	invertedCols *intset.FastIntSet,
) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := FTSFuncMap[x.FuncName.L]; ok {
			// For single-column `fts_match_xxx`, the covered check is based on helper-eligible
			// FTS columns; for multi-column forms (and MATCH ... AGAINST), the matched column set
			// must equal the regular FULLTEXT index column set.
			//
			// NOTE: We intentionally don't assume args[1:] are always *Column. After planner
			// rewrite, complex boolean forms may wrap FTS funcs with NOT/ISTRUE etc.
			switch x.FuncName.L {
			case ast.FTSMatchWord, ast.FTSMatchPrefix, ast.FTSMatchPhrase:
				if len(x.GetArgs()) == 2 {
					arg, ok := x.GetArgs()[1].(*Column)
					return ok && ftsCols.Has(int(arg.ID))
				}
			}
			if matchedColSet, ok := collectFTSMatchedColumnSet(x); ok {
				return matchedColSet.Equals(*colsInFulltextIdx)
			}
			// Fallback: if the function is already rewritten into a complex boolean expression,
			// consider it covered iff all referenced columns belong to the FTS helper column set.
			//
			colIDs := intset.NewFastIntSet()
			CollectColumnIDForFTS(x, &colIDs)
			covered := true
			colIDs.ForEach(func(id int) {
				if !ftsCols.Has(id) {
					covered = false
				}
			})
			return covered
		}
		switch x.FuncName.L {
		case ast.LogicAnd, ast.LogicOr, ast.UnaryNot, ast.IsTruthWithNull:
			for _, arg := range x.GetArgs() {
				covered := ExprCoveredByOneTiCIIndex(arg, ftsCols, colsInFulltextIdx, invertedCols)
				if !covered {
					return false
				}
			}
		case ast.GE, ast.GT, ast.LE, ast.LT, ast.EQ, ast.NE, ast.In:
			lhsCol, lhsIsCol := x.GetArgs()[0].(*Column)
			rhsCol, rhsIsCol := x.GetArgs()[1].(*Column)
			_, lhsIsConst := x.GetArgs()[0].(*Constant)
			_, rhsIsConst := x.GetArgs()[1].(*Constant)
			if lhsIsCol && rhsIsConst {
				return invertedCols.Has(int(lhsCol.ID))
			}
			if rhsIsCol && lhsIsConst {
				return invertedCols.Has(int(rhsCol.ID))
			}
			return false
		default:
			return false
		}
	default:
		return true
	}
	return true
}

// DiagnoseUnmatchedFTSIndexReason returns a more specific reason for unsupported FTS usage.
// It currently specializes the hybrid-index multi-column cases and otherwise returns an empty string.
func DiagnoseUnmatchedFTSIndexReason(
	expr Expression,
	regularFulltextColSets []intset.FastIntSet,
	hybridFulltextColSets []intset.FastIntSet,
) string {
	switch x := expr.(type) {
	case *ScalarFunction:
		switch x.FuncName.L {
		case ast.FTSMatchWord, ast.FTSMatchPrefix, ast.FTSMatchPhrase:
			if len(x.GetArgs()) <= 2 {
				return ""
			}
			matchedColSet, ok := collectFTSMatchedColumnSet(x)
			if !ok || matchesFulltextColumnSet(regularFulltextColSets, matchedColSet) {
				return ""
			}
			if matchesFulltextColumnSet(hybridFulltextColSets, matchedColSet) {
				return "Multi-column fts_match_xxx is not supported on hybrid fulltext indexes; use multiple single-column fts_match_xxx functions instead"
			}
			return ""
		case ast.FTSMysqlMatchAgainst:
			if len(x.GetArgs()) <= 2 {
				return ""
			}
			matchedColSet, ok := collectFTSMatchedColumnSet(x)
			if !ok || matchesFulltextColumnSet(regularFulltextColSets, matchedColSet) {
				return ""
			}
			if matchesFulltextColumnSet(hybridFulltextColSets, matchedColSet) {
				return "Multi-column MATCH AGAINST is not supported on hybrid fulltext indexes; use multiple single-column fts_match_xxx functions instead"
			}
			return ""
		default:
			for _, arg := range x.GetArgs() {
				reason := DiagnoseUnmatchedFTSIndexReason(arg, regularFulltextColSets, hybridFulltextColSets)
				if reason != "" {
					return reason
				}
			}
		}
	}
	return ""
}

func collectFTSMatchedColumnSet(expr *ScalarFunction) (intset.FastIntSet, bool) {
	matchedColSet := intset.NewFastIntSet()
	for i := 1; i < len(expr.GetArgs()); i++ {
		arg, ok := expr.GetArgs()[i].(*Column)
		if !ok {
			return intset.FastIntSet{}, false
		}
		matchedColSet.Insert(int(arg.ID))
	}
	return matchedColSet, true
}

func matchesFulltextColumnSet(fulltextColSets []intset.FastIntSet, matchedColSet intset.FastIntSet) bool {
	for _, idxColSet := range fulltextColSets {
		if idxColSet.Equals(matchedColSet) {
			return true
		}
	}
	return false
}

// CollectColumnIDForFTS collects column IDs from a complex FullTextSearch expression.
// You need to make sure that the parameter is a valid one that only contains FullTextSearch functions and logical operators.
func CollectColumnIDForFTS(expr Expression, idSet *intset.FastIntSet) {
	switch x := expr.(type) {
	case *ScalarFunction:
		startIdx := 0
		if _, ok := FTSFuncMap[x.FuncName.L]; ok {
			startIdx = 1 // Skip the first argument which is the query string.
		}
		for i := startIdx; i < len(x.GetArgs()); i++ {
			CollectColumnIDForFTS(x.GetArgs()[i], idSet)
		}
	case *Column:
		idSet.Insert(int(x.ID))
	default:
		return
	}
}

// InterpretFullTextSearchExpr try to interpret a FullText search expression.
// If interpret successfully, return a FTSInfo struct, otherwise return nil.
func InterpretFullTextSearchExpr(expr Expression) *FTSInfo {
	x, ok := expr.(*ScalarFunction)
	if !ok {
		return nil
	}

	if x.FuncName.L != ast.FTSMatchWord {
		return nil
	}

	if len(x.GetArgs()) != 2 {
		return nil
	}
	argQuery := x.GetArgs()[0]
	argColumn := x.GetArgs()[1]

	query, ok := argQuery.(*Constant)
	if !ok {
		return nil
	}
	column, ok := argColumn.(*Column)
	if !ok {
		return nil
	}

	return &FTSInfo{
		Query:  query.Value.GetString(),
		Column: column,
	}
}

// RewriteMySQLMatchAgainstRecursively rewrites MATCH AGAINST expressions recursively.
// parserType controls which boolean parser to use for MATCH AGAINST rewrite.
func RewriteMySQLMatchAgainstRecursively(
	bctx BuildContext,
	expr Expression,
	parserType model.FullTextParserType,
) (Expression, bool, error) {
	scalarFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return expr, false, nil
	}
	if scalarFunc.FuncName.L != ast.FTSMysqlMatchAgainst {
		changed := false
		newArgs := make([]Expression, 0, len(scalarFunc.GetArgs()))
		for _, arg := range scalarFunc.GetArgs() {
			newArg, argChanged, err := RewriteMySQLMatchAgainstRecursively(bctx, arg, parserType)
			if err != nil {
				return expr, false, err
			}
			if argChanged {
				changed = true
			}
			newArgs = append(newArgs, newArg)
		}
		if changed {
			return NewFunctionInternal(bctx, scalarFunc.FuncName.L, scalarFunc.RetType, newArgs...), true, nil
		}
		return expr, false, nil
	}

	newExpr, changed, err := rewriteOneMySQLMatchAgainst(bctx, scalarFunc, parserType)
	if err != nil {
		return expr, false, err
	}
	return newExpr, changed, nil
}

func rewriteOneMySQLMatchAgainst(
	bctx BuildContext,
	scalarFunc *ScalarFunction,
	parserType model.FullTextParserType,
) (Expression, bool, error) {
	sig, ok := scalarFunc.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return nil, false, errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, scalarFunc.Function)
	}
	if sig.modifier != ast.FulltextSearchModifierBooleanMode {
		return nil, false, errors.Errorf("Currently TiDB only supports BOOLEAN MODE in MATCH AGAINST")
	}
	if scalarFunc.GetArgs()[0].(*Constant).Value.IsNull() {
		return &Constant{
			Value:   types.NewDatum(nil),
			RetType: scalarFunc.RetType.Clone(),
		}, true, nil
	}

	patternGroup, err := parseMySQLMatchAgainstBooleanMode(scalarFunc, parserType)
	if err != nil {
		return nil, false, err
	}
	expr, err := rewriteBooleanGroupToFTSExpr(bctx, scalarFunc.GetArgs()[1:], patternGroup, parserType, scalarFunc.RetType)
	if err != nil {
		return nil, false, err
	}
	return expr, true, nil
}

func parseMySQLMatchAgainstBooleanMode(
	scalarFunc *ScalarFunction,
	parserType model.FullTextParserType,
) (*matchagainst.BooleanGroup, error) {
	patternStr := scalarFunc.GetArgs()[0].(*Constant).Value.GetString()
	switch parserType {
	case model.FullTextParserTypeNgramV1:
		return matchagainst.ParseNgramBooleanMode(patternStr)
	case model.FullTextParserTypeStandardV1, model.FullTextParserTypeMultilingualV1:
		// STANDARD and MULTILINGUAL both use the standard BOOLEAN MODE parser semantics in TiDB.
		return matchagainst.ParseStandardBooleanMode(patternStr)
	default:
		return nil, errors.Errorf("unsupported fulltext parser type: %s", parserType)
	}
}

func rewriteBooleanGroupToFTSExpr(
	bctx BuildContext,
	matchCols []Expression,
	group *matchagainst.BooleanGroup,
	parserType model.FullTextParserType,
	foldedRetType *types.FieldType,
) (Expression, error) {
	if err := validateMySQLMatchAgainstBooleanGroup(group); err != nil {
		return nil, err
	}
	if matchNothingForMySQLMatchAgainst(group) {
		return buildMatchAgainstConstant(foldedRetType, false), nil
	}

	// TiCI currently executes MATCH ... AGAINST as a no-score filter. When the
	// boolean query contains at least one MUST term, MySQL treats SHOULD terms as
	// optional score contributors, which cannot be represented in the filter-only
	// path. In that case we keep the MUST / MUST NOT filters and ignore SHOULD.
	includeShouldInFilter := len(group.Must) == 0
	searchFuncs := make([]Expression, 0, len(group.Must)+len(group.MustNot))
	if includeShouldInFilter {
		searchFuncs = make([]Expression, 0, len(group.Must)+len(group.MustNot)+len(group.Should))
	}
	for _, item := range group.Must {
		f, err := rewriteBooleanClauseToFTSExpr(bctx, matchCols, item, parserType)
		if err != nil {
			return nil, err
		}
		searchFuncs = append(searchFuncs, f)
	}
	for _, item := range group.MustNot {
		f, err := rewriteBooleanClauseToFTSExpr(bctx, matchCols, item, parserType)
		if err != nil {
			return nil, err
		}
		nf := NewFunctionInternal(bctx, ast.UnaryNot, types.NewFieldType(mysql.TypeDouble), f)
		searchFuncs = append(searchFuncs, nf)
	}
	if includeShouldInFilter {
		for _, item := range group.Should {
			f, err := rewriteBooleanClauseToFTSExpr(bctx, matchCols, item, parserType)
			if err != nil {
				return nil, err
			}
			searchFuncs = append(searchFuncs, f)
		}
	}
	if includeShouldInFilter && len(group.Should) > 0 {
		startIdx := len(group.Must) + len(group.MustNot)
		endIdx := startIdx + len(group.Should)
		orShould := ComposeDNFCondition(bctx, searchFuncs[startIdx:endIdx]...)
		searchFuncs[startIdx] = orShould
		searchFuncs = searchFuncs[:startIdx+1]
	}
	return ComposeCNFCondition(bctx, searchFuncs...), nil
}

func rewriteBooleanClauseToFTSExpr(
	bctx BuildContext,
	matchCols []Expression,
	item matchagainst.BooleanClause,
	parserType model.FullTextParserType,
) (Expression, error) {
	switch x := item.Expr.(type) {
	case *matchagainst.BooleanTerm:
		funcName := ast.FTSMatchWord
		if x.Wildcard {
			funcName = ast.FTSMatchPrefix
		} else if parserType == model.FullTextParserTypeNgramV1 {
			funcName = ast.FTSMatchPhrase
		}
		return rewriteSingleQueryToFTSExpr(bctx, funcName, x.Text(), matchCols), nil
	case *matchagainst.BooleanPhrase:
		return rewriteSingleQueryToFTSExpr(bctx, ast.FTSMatchPhrase, x.Text(), matchCols), nil
	case *matchagainst.BooleanGroup:
		return rewriteBooleanGroupToFTSExpr(bctx, matchCols, x, parserType, types.NewFieldType(mysql.TypeTiny))
	default:
		return nil, errors.Errorf("unsupported boolean expression: %T", item.Expr)
	}
}

func rewriteSingleQueryToFTSExpr(
	bctx BuildContext,
	funcName string,
	query string,
	matchCols []Expression,
) Expression {
	args := make([]Expression, 0, len(matchCols)+1)
	args = append(args, &Constant{
		Value:   types.NewStringDatum(query),
		RetType: types.NewFieldType(mysql.TypeString),
	})
	args = append(args, matchCols...)
	return NewFunctionInternal(bctx, funcName, types.NewFieldType(mysql.TypeDouble), args...)
}

func buildMatchAgainstConstant(retType *types.FieldType, isNull bool) *Constant {
	clonedRetType := retType.Clone()
	if isNull {
		return &Constant{
			Value:   types.NewDatum(nil),
			RetType: clonedRetType,
		}
	}

	switch clonedRetType.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble:
		return &Constant{
			Value:   types.NewDatum(float64(0)),
			RetType: clonedRetType,
		}
	default:
		return &Constant{
			Value:   types.NewIntDatum(0),
			RetType: clonedRetType,
		}
	}
}

func validateMySQLMatchAgainstBooleanGroup(group *matchagainst.BooleanGroup) error {
	if group == nil {
		return errors.New("invalid nil boolean group")
	}
	return nil
}

// matchNothingForMySQLMatchAgainst reports whether, for MySQL compatibility, a
// BOOLEAN MODE query should be rewritten to a constant false, including an empty
// query and a query that only contains negative terms.
func matchNothingForMySQLMatchAgainst(group *matchagainst.BooleanGroup) bool {
	return (len(group.Must)+len(group.MustNot)+len(group.Should) == 0) ||
		(len(group.Must)+len(group.Should) == 0 && len(group.MustNot) > 0)
}
