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

package printexpression

import (
	"go/ast"
	"go/types"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer defines the linter for `emptynil` check
//
// This linter avoids calling `fmt.Println(expr)` or `fmt.Printf(\"%s\", expr)` directly, because
// `Expression` doesn't implement `String()` method, so it will print the address or internal state
// of the expression.
// It handles the following function call:
// 1. `fmt.Println(expr)`
// 2. `fmt.Printf(\"%s\", expr)`
// 4. `fmt.Sprintf(\"%s\", expr)`
// 5. `(*Error).GenWithStack/GenWithStackByArgs/FastGen/FastGenByArgs`
//
// Every struct which implemented `StringWithCtx` but not implemented `String` cannot be used as an argument.
var Analyzer = &analysis.Analyzer{
	Name:     "printexpression",
	Doc:      `Avoid printing expression directly.`,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	nodeFilter := []ast.Node{
		(*ast.CallExpr)(nil),
	}
	inspect.Preorder(nodeFilter, func(n ast.Node) {
		expr, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}

		if !funcIsFormat(expr.Fun) {
			return
		}

		for _, arg := range expr.Args {
			if argIsNotAllowed(pass.TypesInfo, arg) {
				pass.Reportf(arg.Pos(), "avoid printing expression directly. Please use `Expression.StringWithCtx()` to get a string")
			}
		}
	})

	return nil, nil
}

func funcIsFormat(x ast.Expr) bool {
	switch x := x.(type) {
	case *ast.SelectorExpr:
		switch x.Sel.Name {
		case "Printf", "Sprintf", "Println":
			if i, ok := x.X.(*ast.Ident); ok {
				return i.Name == "fmt"
			}
			return false
		case "GenWithStack", "GenWithStackByArgs", "FastGen", "FastGenByArgs":
			// TODO: check whether the receiver is an `*Error`
			return true
		}
	}
	return false
}

func argIsNotAllowed(typInfo *types.Info, x ast.Expr) bool {
	typ := typInfo.Types[x].Type
	if typ == nil {
		return false
	}

	typWithMethods, ok := elementType(typ).(methodLookup)
	if !ok {
		return false
	}

	return typIsNotAllowed(typWithMethods)
}

type methodLookup interface {
	NumMethods() int
	Method(i int) *types.Func
	Underlying() types.Type
}

func typIsNotAllowed(typ methodLookup) bool {
	implString := false
	implStringWithCtx := false
	for i := range typ.NumMethods() {
		method := typ.Method(i)
		name := method.Name()
		if name == "String" {
			implString = true
		}
		if name == "StringWithCtx" {
			implStringWithCtx = true
		}
	}

	if implStringWithCtx && !implString {
		return true
	}

	// the `Underlying` of an interface is still the interface, so we need to avoid unlimited recursion here.
	_, typIsIface := typ.(*types.Interface)
	if iface, underlyingIsIface := typ.Underlying().(*types.Interface); !typIsIface && underlyingIsIface {
		return typIsNotAllowed(iface)
	}

	return false
}

// elementType returns the element type of a pointer or slice recursively.
func elementType(typ types.Type) types.Type {
	switch t := typ.(type) {
	case *types.Pointer:
		return elementType(t.Elem())
	case *types.Slice:
		return elementType(t.Elem())
	}
	return typ
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}
