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

package driver

import (
	"embed"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

//go:embed *.go
var productionSources embed.FS

func TestAcceptInPlaceHonorsSkipChildren(t *testing.T) {
	entries, err := productionSources.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	var methods, violations []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := productionSources.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			method, ok := declaration.(*ast.FuncDecl)
			if !ok || method.Recv == nil || method.Name.Name != "AcceptInPlace" {
				continue
			}
			position := fset.Position(method.Pos())
			methods = append(methods, position.String())
			if !hasSkipChildrenGuard(method) {
				violations = append(violations, position.String())
			}
		}
	}
	if len(methods) == 0 {
		t.Fatal("no production AcceptInPlace methods found")
	}
	if len(violations) != 0 {
		t.Fatalf("AcceptInPlace methods must branch on Enter's skipChildren result and call Leave: %s", strings.Join(violations, ", "))
	}
}

func hasSkipChildrenGuard(method *ast.FuncDecl) bool {
	if len(method.Recv.List) != 1 || len(method.Recv.List[0].Names) != 1 ||
		method.Type.Params == nil || len(method.Type.Params.List) != 1 || len(method.Type.Params.List[0].Names) != 1 ||
		len(method.Body.List) == 0 {
		return false
	}
	receiverName := method.Recv.List[0].Names[0].Name
	visitorName := method.Type.Params.List[0].Names[0].Name
	guard, ok := method.Body.List[0].(*ast.IfStmt)
	if !ok || guard.Else != nil || len(guard.Body.List) != 1 {
		return false
	}
	assignment, ok := guard.Init.(*ast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
		return false
	}
	skipChildren, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok || !identicalIdentifier(guard.Cond, skipChildren.Name) ||
		!visitorCall(assignment.Rhs[0], visitorName, "Enter", receiverName) || countVisitorCalls(method.Body, visitorName, "Enter") != 1 {
		return false
	}
	result, ok := guard.Body.List[0].(*ast.ReturnStmt)
	return ok && len(result.Results) == 1 && visitorCall(result.Results[0], visitorName, "Leave", receiverName)
}

func countVisitorCalls(body *ast.BlockStmt, visitor, method string) int {
	count := 0
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == method && identicalIdentifier(selector.X, visitor) {
			count++
		}
		return true
	})
	return count
}

func visitorCall(expression ast.Expr, visitor, method, argument string) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || !identicalIdentifier(call.Args[0], argument) {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == method && identicalIdentifier(selector.X, visitor)
}

func identicalIdentifier(expression ast.Expr, name string) bool {
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == name
}
