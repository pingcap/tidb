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
	"slices"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
)

// FTSInfo is an easy to use struct for interpreting a FullTextSearch expression.
type FTSInfo struct {
	Query          string
	Column         *Column
	Columns        []*Column
	IsMatchAgainst bool
	Modifier       ast.FulltextSearchModifier
}

// ContainsFullTextSearchFn recursively checks whether the expression tree contains a
// possible FullTextSearch function.
func ContainsFullTextSearchFn(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if x.FuncName.L == ast.FTSMatchWord || x.FuncName.L == ast.FTSMysqlMatchAgainst {
			return true
		}
		if slices.ContainsFunc(x.GetArgs(), ContainsFullTextSearchFn) {
			return true
		}
	}
	return false
}

// InterpretFullTextSearchExpr try to interpret a FullText search expression.
// If interpret successfully, return a FTSInfo struct, otherwise return nil.
func InterpretFullTextSearchExpr(expr Expression) *FTSInfo {
	x, ok := expr.(*ScalarFunction)
	if !ok {
		return nil
	}

	args := x.GetArgs()
	if x.FuncName.L != ast.FTSMatchWord && x.FuncName.L != ast.FTSMysqlMatchAgainst {
		return nil
	}
	if x.FuncName.L == ast.FTSMatchWord && len(args) != 2 {
		return nil
	}
	if x.FuncName.L == ast.FTSMysqlMatchAgainst && len(args) < 2 {
		return nil
	}

	modifier := ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)
	isMatchAgainst := x.FuncName.L == ast.FTSMysqlMatchAgainst
	if isMatchAgainst {
		var ok bool
		modifier, ok = GetFTSMysqlMatchAgainstModifier(x)
		if !ok || !modifier.IsBooleanMode() || modifier.WithQueryExpansion() {
			// The native TiFlash FTS query path currently carries only the
			// BOOLEAN-mode AST. Keep natural-language and query-expansion
			// MATCH expressions on their existing scalar-function path.
			return nil
		}
	}

	argQuery := args[0]

	query, ok := argQuery.(*Constant)
	if !ok {
		return nil
	}
	if query.Value.IsNull() || query.Value.Kind() != types.KindString {
		return nil
	}

	columns := make([]*Column, 0, len(args)-1)
	for _, arg := range args[1:] {
		column, ok := arg.(*Column)
		if !ok {
			return nil
		}
		columns = append(columns, column)
	}

	return &FTSInfo{
		Query:          query.Value.GetString(),
		Column:         columns[0],
		Columns:        columns,
		IsMatchAgainst: isMatchAgainst,
		Modifier:       modifier,
	}
}
