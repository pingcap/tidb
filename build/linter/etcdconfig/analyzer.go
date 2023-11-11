// Copyright 2022 PingCAP, Inc.
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

package etcdconfig

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
)

// Analyzer is the analyzer struct of unconvert.
var Analyzer = &analysis.Analyzer{
	Name:     "etcdconfig",
	Doc:      "Check necessary fields of etcd config",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const (
	configPackagePath = "go.etcd.io/etcd/client/v3"
	configPackageName = "clientv3"
	configStructName  = "Config"
)

// Adapted from https://github.com/mdempsky/unconvert/blob/beb68d938016d2dec1d1b078054f4d3db25f97be/unconvert.go#L371-L414.
func run(pass *analysis.Pass) (interface{}, error) {
	for _, file := range pass.Files {
		packageName := ""
		for _, spec := range file.Imports {
			if spec.Path.Value != "\""+configPackagePath+"\"" {
				continue
			}
			if spec.Name != nil {
				packageName = spec.Name.Name
			} else {
				packageName = configPackageName
			}
		}
		if packageName == "" {
			continue
		}

		for _, decl := range file.Decls {
			ast.Inspect(decl, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				tp, ok := lit.Type.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				litPackage, ok := tp.X.(*ast.Ident)
				if !ok {
					return true
				}
				if litPackage.Name != packageName {
					return true
				}
				if tp.Sel.Name != configStructName {
					return true
				}

				found := false
				for _, field := range lit.Elts {
					kv, ok := field.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					if key.Name == "AutoSyncInterval" {
						found = true
						break
					}
				}
				if !found {
					pass.Reportf(lit.Pos(), "missing field AutoSyncInterval")
				}
				return true
			})
		}
	}
	return nil, nil
}
