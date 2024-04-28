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

package bootstrap

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of unconvert.
var Analyzer = &analysis.Analyzer{
	Name:     "bootstrap",
	Doc:      "Check developers don't forget something in TiDB bootstrap logic",
	Requires: []*analysis.Analyzer{},
	Run:      run,
}

const (
	bootstrapCodeFile = "/bootstrap.go"
)

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		if !strings.HasSuffix(pass.Fset.File(file.Pos()).Name(), bootstrapCodeFile) {
			continue
		}

		var (
			maxVerVariable    int
			maxVerVariablePos token.Pos
			curVerVariable    int
			curVerVariablePos token.Pos
			maxVerFunc        int
			maxVerFuncPos     token.Pos
			maxVerFuncUsed    int
			maxVerFuncUsedPos token.Pos
			err               error
		)

		for _, decl := range file.Decls {
			switch v := decl.(type) {
			case *ast.GenDecl:
				switch {
				case len(v.Specs) == 1:
					spec := v.Specs[0]
					v2, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					if len(v2.Names) != 1 {
						continue
					}
					switch v2.Names[0].Name {
					case "bootstrapVersion":
						composeLit := v2.Values[0].(*ast.CompositeLit)
						lastElm := composeLit.Elts[len(composeLit.Elts)-1]
						ident := lastElm.(*ast.Ident)
						maxVerFuncUsed, err = strconv.Atoi(ident.Name[len("upgradeToVer"):])
						if err != nil {
							panic("unexpected value of bootstrapVersion: " + ident.Name)
						}
						maxVerFuncUsedPos = lastElm.Pos()
					case "currentBootstrapVersion":
						valueIdent := v2.Values[0].(*ast.Ident)
						curVerVariablePos = valueIdent.Pos()
						value := v2.Values[0].(*ast.Ident).Name
						curVerVariable, err = strconv.Atoi(value[len("version"):])
						if err != nil {
							panic("unexpected value of currentBootstrapVersion: " + value)
						}
					default:
						continue
					}
				case v.Tok == token.CONST && len(v.Specs) > 1:
					for _, spec := range v.Specs {
						v2, ok := spec.(*ast.ValueSpec)
						if !ok {
							continue
						}
						if len(v2.Names) != 1 {
							continue
						}
						name := v2.Names[0].Name
						if !strings.HasPrefix(name, "version") {
							continue
						}

						valInName, err := strconv.Atoi(name[len("version"):])
						if err != nil {
							continue
						}

						if valInName < maxVerVariable {
							pass.Reportf(spec.Pos(), "version variable %q is not valid, we should have a increment list of version variables", name)
							continue
						}

						maxVerVariable = valInName
						maxVerVariablePos = v2.Names[0].Pos()

						if len(v2.Values) != 1 {
							pass.Reportf(spec.Pos(), "the value of version variable %q must be specified explicitly", name)
							continue
						}

						valStr := v2.Values[0].(*ast.BasicLit).Value
						val, err := strconv.Atoi(valStr)
						if err != nil {
							pass.Reportf(spec.Pos(), "unexpected value of version variable %q: %q", name, valStr)
							continue
						}

						if val != valInName {
							pass.Reportf(spec.Pos(), "the value of version variable %q must be '%d', but now is '%d'", name, valInName, val)
							continue
						}
					}
				}
			case *ast.FuncDecl:
				name := v.Name.Name
				if !strings.HasPrefix(name, "upgradeToVer") {
					continue
				}
				t, err := strconv.Atoi(name[len("upgradeToVer"):])
				if err != nil {
					continue
				}
				if t > maxVerFunc {
					maxVerFunc = t
					maxVerFuncPos = v.Pos()
				}
			}
		}
		minv := min(maxVerVariable, maxVerFunc, maxVerFuncUsed, curVerVariable)
		maxv := max(maxVerVariable, maxVerFunc, maxVerFuncUsed, curVerVariable)
		if minv == maxv && minv != 0 {
			return nil, nil
		}
		pass.Reportf(maxVerFuncUsedPos, "found inconsistent bootstrap versions:")
		pass.Reportf(maxVerFuncUsedPos, "max version function used: %d", maxVerFuncUsed)
		pass.Reportf(maxVerFuncPos, "max version function: %d", maxVerFunc)
		pass.Reportf(maxVerVariablePos, "max version variable: %d", maxVerVariable)
		pass.Reportf(curVerVariablePos, "current version variable: %d", curVerVariable)
	}
	return nil, nil
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
