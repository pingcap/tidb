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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// FTSInfo is an easy to use struct for interpreting a FullTextSearch expression.
type FTSInfo struct {
	Query  string
	Column *Column
}

// FTSFuncMap stores the functions related to fulltext search.
var FTSFuncMap map[string]struct{} = map[string]struct{}{
	ast.FTSMatchWord:   {},
	ast.FTSMatchPrefix: {},
	ast.FTSMatchPhrase: {},
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
