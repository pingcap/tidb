// Copyright 2026 PingCAP, Inc.
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

package assertionapi

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
)

// Analyzer restricts assertion-related MemBuffer APIs to the table layer.
var Analyzer = &analysis.Analyzer{
	Name:     "assertionapi",
	Doc:      "Restrict txn assertion API usage (UpdateAssertionFlags) to pkg/table/tables",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const kvPkgPath = "github.com/pingcap/tidb/pkg/kv"

func run(pass *analysis.Pass) (any, error) {
	for _, f := range pass.Files {
		filename := pass.Fset.PositionFor(f.Pos(), false).Filename
		if isAllowedFile(filename) {
			continue
		}

		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel == nil || sel.Sel.Name != "UpdateAssertionFlags" {
				return true
			}
			if !isKVUpdateAssertionFlags(pass, sel) {
				return true
			}

			pass.Reportf(sel.Sel.Pos(), "txn assertion API (UpdateAssertionFlags) is restricted to pkg/table/tables")
			return true
		})
	}
	return nil, nil
}

func isAllowedFile(filename string) bool {
	f := filepath.ToSlash(filename)
	return strings.Contains(f, "pkg/table/tables/")
}

func isKVUpdateAssertionFlags(pass *analysis.Pass, sel *ast.SelectorExpr) bool {
	selection := pass.TypesInfo.Selections[sel]
	if selection == nil {
		// No type information; be conservative and don't fail the build on potentially unrelated identifiers.
		return false
	}
	fn, ok := selection.Obj().(*types.Func)
	if !ok {
		return false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok {
		return false
	}

	// Match: UpdateAssertionFlags(kv.Key, kv.AssertionOp)
	//
	// Note: method expressions like `T.UpdateAssertionFlags` have an extra first parameter (the receiver),
	// so we match both forms.
	params := sig.Params()
	var keyIdx, opIdx int
	switch params.Len() {
	case 2:
		keyIdx, opIdx = 0, 1
	case 3:
		keyIdx, opIdx = 1, 2
	default:
		return false
	}
	if !isKVKey(params.At(keyIdx).Type()) {
		return false
	}
	if !isKVAssertionOp(params.At(opIdx).Type()) {
		return false
	}
	return true
}

func isKVKey(t types.Type) bool {
	n, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := n.Obj()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	return obj.Name() == "Key" && obj.Pkg().Path() == kvPkgPath
}

func isKVAssertionOp(t types.Type) bool {
	n, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := n.Obj()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	return obj.Name() == "AssertionOp" && obj.Pkg().Path() == kvPkgPath
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}
