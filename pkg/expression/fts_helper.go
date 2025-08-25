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
}

// ContainsFullTextSearchFn recursively checks whether the expression tree contains a
// possible FullTextSearch function.
func ContainsFullTextSearchFn(exprs ...Expression) bool {
	for _, expr := range exprs {
		switch x := expr.(type) {
		case *ScalarFunction:
			if x.FuncName.L == ast.FTSMatchWord {
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

// ExprOnlyContainsLogicOpAndFTS checks whether the expression contains only logical operators
// and FullTextSearch functions. It does not allow any other functions or expressions.
func ExprOnlyContainsLogicOpAndFTS(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		// TiDB's FTS functions return float value for potential BM25 score cases.
		// So there'll be a IS TRUE wrapped to convert the float to boolean.
		switch x.FuncName.L {
		case ast.LogicAnd, ast.LogicOr, ast.IsTruthWithNull:
			for _, arg := range x.GetArgs() {
				if !ExprOnlyContainsLogicOpAndFTS(arg) {
					return false
				}
			}
			return true
		default:
			if _, ok := FTSFuncMap[x.FuncName.L]; ok {
				return true
			}
		}
	default:
		return false
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
