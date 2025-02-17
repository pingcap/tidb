// Copyright 2023 PingCAP, Inc.
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

package toomanytests

import (
	"go/ast"
	"go/token"
	"strings"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of toomanytests
var Analyzer = &analysis.Analyzer{
	Name: "toomanytests",
	Doc:  "too many tests in the package",
	Run: func(pass *analysis.Pass) (any, error) {
		cnt := 0
		for _, f := range pass.Files {
			astFile := pass.Fset.File(f.Pos())
			if !isTestFile(astFile) {
				continue
			}
			for _, n := range f.Decls {
				funcDecl, ok := n.(*ast.FuncDecl)
				if ok {
					if strings.HasPrefix(funcDecl.Name.Name, "Test") && funcDecl.Recv == nil &&
						funcDecl.Name.Name != "TestMain" {
						cnt++
					}
				}
			}
			if cnt > 50 {
				pass.Reportf(f.Pos(), "%s: Too many test cases in one package", pass.Pkg.Name())
				return nil, nil
			}
		}
		return nil, nil
	},
}

func isTestFile(file *token.File) bool {
	return strings.HasSuffix(file.Name(), "_test.go")
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}
