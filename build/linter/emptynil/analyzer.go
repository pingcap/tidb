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

package emptynil

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer defines the linter for `emptynil` check
var Analyzer = &analysis.Analyzer{
	Name:     "emptynil",
	Doc:      "avoid using `x == nil` to check whether a slice/map is empty",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const msgForEQ = "should use len(x) == 0 to check whether a slice or map is empty. If you indeed need to check whether it is nil, use emptynil.IsNilSlice(x) or emptynil.IsNilMap(x)."
const msgForNEQ = "should use len(x) > 0 to check whether a slice or map is not empty. If you indeed need to check whether it is nil, use !emptynil.IsNilSlice(x) or !emptynil.IsNilMap(x)."

var skipPkg = map[string]struct{}{
	"github.com/pingcap/tidb/pkg/parser/emptynil": {},
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

		newNode := getFixedNode(pass.TypesInfo, expr)
		if newNode != nil {
			var buf bytes.Buffer
			if err := format.Node(&buf, token.NewFileSet(), newNode); err != nil {
				return
			}

			suggestedFixes := []analysis.SuggestedFix{{
				Message: "replace with emptynil.IsNilSlice or emptynil.IsNilMap",
				TextEdits: []analysis.TextEdit{
					{
						Pos:     expr.Pos(),
						End:     expr.End(),
						NewText: buf.Bytes(),
					},
				},
			}}

			if expr.Op == token.EQL {
				pass.Report(analysis.Diagnostic{
					Pos:            expr.Pos(),
					Message:        msgForEQ,
					SuggestedFixes: suggestedFixes,
				})
			} else if expr.Op == token.NEQ {
				pass.Report(analysis.Diagnostic{
					Pos:            expr.Pos(),
					Message:        msgForNEQ,
					SuggestedFixes: suggestedFixes,
				})
			}
		}
	})

	return nil, nil
}

func getFixedNode(typInfo *types.Info, expr *ast.BinaryExpr) ast.Expr {
	var newNode ast.Expr
	if isNilIdent(expr.X) {
		if isMap(typInfo, expr.Y) {
			newNode = &ast.CallExpr{
				Fun: &ast.SelectorExpr{
					X:   ast.NewIdent("emptynil"),
					Sel: ast.NewIdent("IsNilMap"),
				},
				Args: []ast.Expr{expr.Y},
			}
		} else if isSlice(typInfo, expr.Y) {
			newNode = &ast.CallExpr{
				Fun: &ast.SelectorExpr{
					X:   ast.NewIdent("emptynil"),
					Sel: ast.NewIdent("IsNilSlice"),
				},
				Args: []ast.Expr{expr.Y},
			}
		}
	} else if isNilIdent(expr.Y) {
		if isMap(typInfo, expr.X) {
			newNode = &ast.CallExpr{
				Fun: &ast.SelectorExpr{
					X:   ast.NewIdent("emptynil"),
					Sel: ast.NewIdent("IsNilMap"),
				},
				Args: []ast.Expr{expr.X},
			}
		} else if isSlice(typInfo, expr.X) {
			newNode = &ast.CallExpr{
				Fun: &ast.SelectorExpr{
					X:   ast.NewIdent("emptynil"),
					Sel: ast.NewIdent("IsNilSlice"),
				},
				Args: []ast.Expr{expr.X},
			}
		}
	}

	if newNode != nil && expr.Op == token.NEQ {
		newNode = &ast.UnaryExpr{
			Op: token.NOT,
			X:  newNode,
		}
	}

	return newNode
}

func isMap(typInfo *types.Info, x ast.Expr) bool {
	typ := typInfo.Types[x].Type
	if typ == nil {
		return false
	}

	_, ok := typ.Underlying().(*types.Map)
	return ok
}

func isSlice(typInfo *types.Info, x ast.Expr) bool {
	typ := typInfo.Types[x].Type
	if typ == nil {
		return false
	}

	_, ok := typ.Underlying().(*types.Slice)
	return ok
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
