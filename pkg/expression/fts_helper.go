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

// ExprCoveredByOneTiCIIndex checks whether the given expr is fully covered by one a TiCI index.
// A TiCI index accepts the
func ExprCoveredByOneTiCIIndex(
	expr Expression,
	ftsColIDs *intset.FastIntSet,
	invertedIndexesColIDs *intset.FastIntSet,
) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := FTSFuncMap[x.FuncName.L]; ok {
			for i := 1; i < len(x.GetArgs()); i++ {
				arg := x.GetArgs()[i].(*Column)
				if !ftsColIDs.Has(int(arg.ID)) {
					return false
				}
			}
			return true
		}
		switch x.FuncName.L {
		case ast.LogicAnd, ast.LogicOr, ast.UnaryNot, ast.IsTruthWithNull:
			for _, arg := range x.GetArgs() {
				covered := ExprCoveredByOneTiCIIndex(arg, ftsColIDs, invertedIndexesColIDs)
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
				return invertedIndexesColIDs.Has(int(lhsCol.ID))
			}
			if rhsIsCol && lhsIsConst {
				return invertedIndexesColIDs.Has(int(rhsCol.ID))
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

// RewriteMySQLMatchAgainstRecursively rewrites the MySQL MATCH AGAINST function into a combination of TiDB's internal FTS functions.
func RewriteMySQLMatchAgainstRecursively(bctx BuildContext, expr Expression) (Expression, bool, error) {
	scalarFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return expr, false, nil
	}
	if scalarFunc.FuncName.L != ast.FTSMysqlMatchAgainst {
		// If it's not a MySQL MATCH AGAINST function, we recursively rewrite its arguments.
		changed := false
		newArgs := make([]Expression, 0, len(scalarFunc.GetArgs()))
		for _, arg := range scalarFunc.GetArgs() {
			newArg, argChanged, err := RewriteMySQLMatchAgainstRecursively(bctx, arg)
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
	var (
		patternGroup matchagainst.StandardBooleanGroup
		err          error
	)
	if scalarFunc.Function.(*builtinFtsMysqlMatchAgainstSig).modifier == ast.FulltextSearchModifierBooleanMode {
		patternStr := scalarFunc.GetArgs()[0].(*Constant).Value.GetString()
		patternGroup, err = matchagainst.ParseStandardBooleanMode(patternStr)
		if err != nil {
			return expr, false, err
		}
	}
	// Current limitation of TiDB.
	if len(patternGroup.Must)+len(patternGroup.MustNot) > 0 && len(patternGroup.Should) > 0 {
		return expr, false, errors.Errorf("TiDB only supports multiple terms with +/- modifiers")
	}
	if len(patternGroup.Must)+len(patternGroup.MustNot) == 0 && len(patternGroup.Should) > 1 {
		return expr, false, errors.Errorf("TiDB only supports multiple terms with +/- modifiers")
	}
	argsBuffer := make([]Expression, len(scalarFunc.GetArgs()))
	searchFuncs := make([]Expression, 0, len(patternGroup.Must)+len(patternGroup.MustNot)+len(patternGroup.Should))
	for i := 1; i < len(scalarFunc.GetArgs()); i++ {
		argsBuffer[i] = scalarFunc.GetArgs()[i]
	}
	rewriteFunc := func(item matchagainst.StandardBooleanClause) (Expression, error) {
		patternConst := &Constant{
			Value:   types.NewStringDatum(item.Expr.Text()),
			RetType: types.NewFieldType(mysql.TypeString),
		}
		argsBuffer[0] = patternConst
		// Map the original pattern to different internal functions based on the type and wildcard property of the pattern.
		switch x := item.Expr.(type) {
		case *matchagainst.StandardBooleanTerm:
			if !x.Wildcard {
				return NewFunctionInternal(bctx, ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble), argsBuffer...), nil
			}
			return NewFunctionInternal(bctx, ast.FTSMatchPrefix, types.NewFieldType(mysql.TypeDouble), argsBuffer...), nil
		case *matchagainst.StandardBooleanPhrase:
			return NewFunctionInternal(bctx, ast.FTSMatchPhrase, types.NewFieldType(mysql.TypeDouble), argsBuffer...), nil
		default:
			return nil, errors.Errorf("unsupported boolean expression: %T", item.Expr)
		}
	}
	for _, item := range patternGroup.Must {
		f, err := rewriteFunc(item)
		if err != nil {
			return expr, false, err
		}
		searchFuncs = append(searchFuncs, f)
	}
	for _, item := range patternGroup.MustNot {
		f, err := rewriteFunc(item)
		if err != nil {
			return expr, false, err
		}
		// For NOT conditions, we wrap the function with a unary NOT operator.
		nf := NewFunctionInternal(bctx, ast.UnaryNot, types.NewFieldType(mysql.TypeDouble), f)
		searchFuncs = append(searchFuncs, nf)
	}
	for _, item := range patternGroup.Should {
		f, err := rewriteFunc(item)
		if err != nil {
			return expr, false, err
		}
		searchFuncs = append(searchFuncs, f)
	}
	if len(patternGroup.Should) > 0 {
		// If there are multiple SHOULD conditions, we combine them with OR operator.
		startIdx := len(patternGroup.Must) + len(patternGroup.MustNot)
		endIdx := startIdx + len(patternGroup.Should)
		orShould := ComposeDNFCondition(bctx, searchFuncs[startIdx:endIdx]...)
		searchFuncs[startIdx] = orShould
		searchFuncs = searchFuncs[:startIdx+1]
	}
	final := ComposeCNFCondition(bctx, searchFuncs...)
	return final, true, nil
}
