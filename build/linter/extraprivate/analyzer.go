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

package extraprivate

import (
	"go/ast"
	"go/types"

	"github.com/fatih/structtag"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the analyzer of extraprivate.
// Access to fields with `extraprivate` tag outside its struct's methods is not allowed. However, this
// analyzer allows to construct the struct manually with the field with `extraprivate` tag.
var Analyzer = &analysis.Analyzer{
	Name:     "extraprivate",
	Doc:      "Check developers don't read or write fields with extraprivate tag outside the method",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{
		(*ast.FuncDecl)(nil),
		(*ast.SelectorExpr)(nil),
	}

	inspect.WithStack(nodeFilter, func(n ast.Node, push bool, stack []ast.Node) (proceed bool) {
		if push {
			return true
		}

		se, ok := n.(*ast.SelectorExpr)
		if !ok {
			return
		}

		xType := pass.TypesInfo.Types[se.X].Type
		if !isExtraPrivateField(xType, se.Sel) {
			return
		}

		if isAccessWithinStructMethod(pass, stack, xType) {
			return
		}

		pass.Reportf(se.Pos(), "access to extraprivate field outside its struct's methods")
		return
	})

	return nil, nil
}

func isExtraPrivateField(xAnyType types.Type, fieldName *ast.Ident) bool {
	switch xType := xAnyType.(type) {
	case *types.Named:
		underlyingType := xType.Underlying()
		structType, ok := underlyingType.(*types.Struct)
		if !ok {
			return false
		}
		for i := 0; i < structType.NumFields(); i++ {
			field := structType.Field(i)
			if field.Name() == fieldName.Name {
				tags, err := structtag.Parse(structType.Tag(i))
				if err != nil {
					continue
				}
				_, err = tags.Get("extraprivate")
				if err != nil {
					continue
				}
				return true
			}
		}

		return false
	case *types.Pointer:
		return isExtraPrivateField(xType.Elem(), fieldName)
	default:
		return false
	}
}

func isAccessWithinStructMethod(pass *analysis.Pass, stack []ast.Node, se types.Type) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		funcDecl, ok := stack[i].(*ast.FuncDecl)
		if !ok {
			continue
		}

		if funcDecl.Recv == nil {
			continue
		}

		recvType := pass.TypesInfo.TypeOf(funcDecl.Recv.List[0].Type)
		if recvType == nil {
			continue
		}

		if resolvePointer(recvType) == resolvePointer(se) {
			return true
		}
	}

	return false
}

func resolvePointer(xType types.Type) types.Type {
	switch xType := xType.(type) {
	case *types.Pointer:
		return xType.Elem()
	default:
		return xType
	}
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
