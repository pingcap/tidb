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

// Analyzer restricts assertion-related transaction APIs to the table layer and txn driver layer.
var Analyzer = &analysis.Analyzer{
	Name:     "assertionapi",
	Doc:      "Restrict txn assertion API usage to pkg/table/tables and pkg/store/driver/txn",
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
			if !ok || sel.Sel == nil || sel.Sel.Name != "SetAssertion" {
				return true
			}
			if !isTxnAssertionSetAssertion(pass, sel) {
				return true
			}

			pass.Reportf(sel.Sel.Pos(), "txn assertion API (SetAssertion) is restricted to pkg/table/tables and pkg/store/driver/txn")
			return true
		})
	}
	return nil, nil
}

func isAllowedFile(filename string) bool {
	f := filepath.ToSlash(filename)
	return strings.Contains(f, "pkg/table/tables/") || strings.Contains(f, "pkg/store/driver/txn/")
}

func isTxnAssertionSetAssertion(pass *analysis.Pass, sel *ast.SelectorExpr) bool {
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

	// Match: SetAssertion([]byte, kv.AssertionOp) error
	//
	// Note: method expressions like `T.SetAssertion` have an extra first parameter (the receiver),
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
	if !isByteSlice(params.At(keyIdx).Type()) {
		return false
	}
	if !isKVAssertionOp(params.At(opIdx).Type()) {
		return false
	}
	return true
}

func isByteSlice(t types.Type) bool {
	s, ok := t.(*types.Slice)
	if !ok {
		return false
	}
	b, ok := s.Elem().(*types.Basic)
	return ok && b.Kind() == types.Byte
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
