// Copyright 2024 PingCAP, Inc.
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

package emptyslice

import (
	"go/ast"
	"go/token"
	"go/types"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

var Analyzer = &analysis.Analyzer{
	Name:     "emptyslice",
	Doc:      "avoid using `arr == nil` to check whether a slice is empty",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const msgForEQ = "should use len(arr) == 0 to check whether a slice is empty. If you indeed need to check whether a slice is nil, use emptyslice.IsNil(arr)."
const msgForNEQ = "should use len(arr) > 0 to check whether a slice is not empty. If you indeed need to check whether a slice is nil, use !emptyslice.IsNil(arr)."

var skipPkg = map[string]struct{}{
	"github.com/pingcap/tidb/pkg/parser/emptyslice": {},
}

func run(pass *analysis.Pass) (any, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{
		(*ast.BinaryExpr)(nil),
	}
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		if _, ok := skipPkg[pass.Pkg.Path()]; ok {
			return
		}

		expr, ok := n.(*ast.BinaryExpr)
		if !ok {
			return
		}
		if expr.Op != token.EQL && expr.Op != token.NEQ {
			return
		}

		if (isNilIdent(expr.X) && isSlice(pass.TypesInfo, expr.Y)) ||
			(isNilIdent(expr.Y) && isSlice(pass.TypesInfo, expr.X)) {
			if expr.Op == token.EQL {
				pass.Reportf(expr.Pos(), msgForEQ)
			} else if expr.Op == token.NEQ {
				pass.Reportf(expr.Pos(), msgForNEQ)
			}
		}
	})

	return nil, nil
}

func isSlice(typInfo *types.Info, x ast.Expr) bool {
	typ := typInfo.Types[x].Type
	if typ == nil {
		return false
	}

	switch typ.Underlying().(type) {
	case *types.Slice:
		return true
	default:
		return false
	}
}

func isNilIdent(x ast.Expr) bool {
	switch x := x.(type) {
	case *ast.Ident:
		return x.Name == "nil"
	}
	return false
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}
