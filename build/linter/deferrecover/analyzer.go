// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deferrecover

import (
	"go/ast"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the analyzer struct of unconvert.
var Analyzer = &analysis.Analyzer{
	Name:     "recover",
	Doc:      "Check Recover() is directly called by defer",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const (
	packagePath = "github.com/pingcap/tidb/pkg/util"
	packageName = "util"
	funcName    = "Recover"
)

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		packageName := util.GetPackageName(file.Imports, packagePath, packageName)
		if packageName == "" {
			continue
		}

		i := inspector.New([]*ast.File{file})

		i.WithStack([]ast.Node{&ast.CallExpr{}}, func(n ast.Node, push bool, stack []ast.Node) bool {
			if !push {
				return true
			}
			callExpr := n.(*ast.CallExpr)
			sel, ok := callExpr.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			usedPackage, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if usedPackage.Name != packageName {
				return true
			}
			if sel.Sel.Name != funcName {
				return true
			}

			// check defer is directly called
			parentStmt := stack[len(stack)-2]
			_, ok = parentStmt.(*ast.DeferStmt)
			if !ok {
				pass.Reportf(n.Pos(), "Recover() should be directly called by defer")
				return true
			}
			return true
		})
	}
	return nil, nil
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
