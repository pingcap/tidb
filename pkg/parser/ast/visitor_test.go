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
// See the License for the specific language governing permissions and
// limitations under the License.

package ast_test

import (
	"bytes"
	"embed"
	"fmt"
	goast "go/ast"
	goformat "go/format"
	goparser "go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	parserast "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

//go:embed *.go
var traversalSources embed.FS

type testInPlaceVisitor struct {
	enter func(parserast.Node) bool
	leave func(parserast.Node) bool
}

func (v *testInPlaceVisitor) Enter(n parserast.Node) bool {
	return v.enter(n)
}

func (v *testInPlaceVisitor) Leave(n parserast.Node) bool {
	return v.leave(n)
}

var _ parserast.InPlaceVisitor = (*testInPlaceVisitor)(nil)

type testVisitor struct {
	enter func(parserast.Node) (parserast.Node, bool)
	leave func(parserast.Node) (parserast.Node, bool)
}

func (v *testVisitor) Enter(n parserast.Node) (parserast.Node, bool) {
	return v.enter(n)
}

func (v *testVisitor) Leave(n parserast.Node) (parserast.Node, bool) {
	return v.leave(n)
}

type unsupportedExternalNode struct {
	parserast.Node
	child       parserast.Node
	acceptCalls int
}

func (n *unsupportedExternalNode) Accept(v parserast.Visitor) (parserast.Node, bool) {
	n.acceptCalls++
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*unsupportedExternalNode)
	child, ok := n.child.Accept(v)
	if !ok {
		return n, false
	}
	n.child = child
	return v.Leave(n)
}

type benchmarkVisitor struct{}

func (*benchmarkVisitor) Enter(n parserast.Node) (parserast.Node, bool) {
	return n, false
}

func (*benchmarkVisitor) Leave(n parserast.Node) (parserast.Node, bool) {
	return n, true
}

type benchmarkInPlaceVisitor struct{}

func (*benchmarkInPlaceVisitor) Enter(parserast.Node) bool {
	return false
}

func (*benchmarkInPlaceVisitor) Leave(parserast.Node) bool {
	return true
}

func BenchmarkVisitorTraversal(b *testing.B) {
	for _, replaceableNodes := range []int{10, 100, 500, 1000} {
		root := newBenchmarkSelect(replaceableNodes)
		visitedNodes := 0
		visitor := &testInPlaceVisitor{
			enter: func(parserast.Node) bool {
				visitedNodes++
				return false
			},
			leave: func(parserast.Node) bool {
				return true
			},
		}
		if !parserast.Walk(root, visitor) {
			b.Fatal("benchmark fixture traversal stopped")
		}
		if visitedNodes != replaceableNodes+1 {
			b.Fatalf("expected %d visited nodes, got %d", replaceableNodes+1, visitedNodes)
		}

		b.Run(fmt.Sprintf("%dReplaceableNodes", replaceableNodes), func(b *testing.B) {
			b.Run("Visitor", func(b *testing.B) {
				root := newBenchmarkSelect(replaceableNodes)
				visitor := &benchmarkVisitor{}
				var ok bool
				b.ReportAllocs()
				for b.Loop() {
					_, ok = root.Accept(visitor)
				}
				if !ok {
					b.Fatal("visitor traversal stopped")
				}
			})

			b.Run("InPlaceVisitor", func(b *testing.B) {
				root := newBenchmarkSelect(replaceableNodes)
				visitor := &benchmarkInPlaceVisitor{}
				var ok bool
				b.ReportAllocs()
				for b.Loop() {
					ok = parserast.Walk(root, visitor)
				}
				if !ok {
					b.Fatal("in-place visitor traversal stopped")
				}
			})
		})
	}
}

// newBenchmarkSelect returns a select with exactly replaceableNodes children.
// The legacy Visitor writes every returned child back into its parent, while
// InPlaceVisitor traverses the same nodes without those framework writes.
func newBenchmarkSelect(replaceableNodes int) *parserast.SelectStmt {
	fields := make([]*parserast.SelectField, 0, replaceableNodes/3+1)
	remaining := replaceableNodes - 1 // SelectStmt.Fields accounts for one child.
	for remaining > 0 {
		switch {
		case remaining == 4:
			fields = append(fields,
				&parserast.SelectField{Expr: &parserast.DefaultExpr{}},
				&parserast.SelectField{Expr: &parserast.DefaultExpr{}},
			)
			remaining = 0
		case remaining >= 3:
			fields = append(fields, &parserast.SelectField{
				Expr: &parserast.ColumnNameExpr{Name: &parserast.ColumnName{}},
			})
			remaining -= 3
		case remaining == 2:
			fields = append(fields, &parserast.SelectField{Expr: &parserast.DefaultExpr{}})
			remaining = 0
		default:
			panic("replaceableNodes must be at least 3")
		}
	}
	return &parserast.SelectStmt{Fields: &parserast.FieldList{Fields: fields}}
}

func TestWalk(t *testing.T) {
	t.Run("benchmark_master_fixture_matches", func(t *testing.T) {
		candidateSource, err := traversalSources.ReadFile("visitor_test.go")
		require.NoError(t, err)
		masterSource, err := os.ReadFile("testdata/visitor_benchmark_master_test.go")
		require.NoError(t, err)

		require.Equal(t,
			normalizedFunction(t, "visitor_test.go", candidateSource, "newBenchmarkSelect"),
			normalizedFunction(t, "visitor_benchmark_master_test.go", masterSource, "newBenchmarkSelect"),
		)
	})

	t.Run("traversal_order", func(t *testing.T) {
		leafA := &parserast.DefaultExpr{}
		leafB := &parserast.DefaultExpr{}
		leafC := &parserast.DefaultExpr{}
		unary := &parserast.UnaryOperationExpr{V: leafA}
		root := &parserast.BetweenExpr{Expr: unary, Left: leafB, Right: leafC}

		var events []string
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				events = append(events, "enter "+walkNodeName(n, root, unary, leafA, leafB, leafC))
				return false
			},
			leave: func(n parserast.Node) bool {
				events = append(events, "leave "+walkNodeName(n, root, unary, leafA, leafB, leafC))
				return true
			},
		}

		require.True(t, parserast.Walk(root, visitor))
		require.Equal(t, []string{
			"enter root",
			"enter unary",
			"enter A",
			"leave A",
			"leave unary",
			"enter B",
			"leave B",
			"enter C",
			"leave C",
			"leave root",
		}, events)
	})

	t.Run("skip_children", func(t *testing.T) {
		leafA := &parserast.DefaultExpr{}
		leafB := &parserast.DefaultExpr{}
		leafC := &parserast.DefaultExpr{}
		unary := &parserast.UnaryOperationExpr{V: leafA}
		root := &parserast.BetweenExpr{Expr: unary, Left: leafB, Right: leafC}

		var events []string
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				events = append(events, "enter "+walkNodeName(n, root, unary, leafA, leafB, leafC))
				return n == unary
			},
			leave: func(n parserast.Node) bool {
				events = append(events, "leave "+walkNodeName(n, root, unary, leafA, leafB, leafC))
				return true
			},
		}

		require.True(t, parserast.Walk(root, visitor))
		require.Equal(t, []string{
			"enter root",
			"enter unary",
			"leave unary",
			"enter B",
			"leave B",
			"enter C",
			"leave C",
			"leave root",
		}, events)
	})

	t.Run("stop_traversal", func(t *testing.T) {
		leafA := &parserast.DefaultExpr{}
		leafB := &parserast.DefaultExpr{}
		leafC := &parserast.DefaultExpr{}
		unary := &parserast.UnaryOperationExpr{V: leafA}
		root := &parserast.BetweenExpr{Expr: unary, Left: leafB, Right: leafC}

		var events []string
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				events = append(events, "enter "+walkNodeName(n, root, unary, leafA, leafB, leafC))
				return false
			},
			leave: func(n parserast.Node) bool {
				events = append(events, "leave "+walkNodeName(n, root, unary, leafA, leafB, leafC))
				return n != leafB
			},
		}

		require.False(t, parserast.Walk(root, visitor))
		require.Equal(t, []string{
			"enter root",
			"enter unary",
			"enter A",
			"leave A",
			"leave unary",
			"enter B",
			"leave B",
		}, events)
	})

	t.Run("in_place_mutation", func(t *testing.T) {
		column := &parserast.ColumnName{Name: parserast.NewCIStr("original")}
		root := &parserast.ColumnNameExpr{Name: column}
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				if n == column {
					column.Name = parserast.NewCIStr("changed")
				}
				return false
			},
			leave: func(parserast.Node) bool {
				return true
			},
		}

		require.True(t, parserast.Walk(root, visitor))
		require.Equal(t, parserast.NewCIStr("changed"), column.Name)
	})

	t.Run("value_stored_child_mutates_original_storage", func(t *testing.T) {
		root := &parserast.SelectStmt{WindowSpecs: []parserast.WindowSpec{{}}}
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				if spec, ok := n.(*parserast.WindowSpec); ok {
					spec.OnlyAlias = true
				}
				return false
			},
			leave: func(parserast.Node) bool {
				return true
			},
		}

		require.True(t, parserast.Walk(root, visitor))
		require.True(t, root.WindowSpecs[0].OnlyAlias)
	})

	t.Run("package_local_composite_is_allocation_free", func(t *testing.T) {
		root := newBenchmarkSelect(100)
		visitor := &benchmarkInPlaceVisitor{}
		var ok bool
		allocations := testing.AllocsPerRun(100, func() {
			ok = parserast.Walk(root, visitor)
		})
		require.True(t, ok)
		require.Zero(t, allocations)
	})

	t.Run("unsupported_external_node_owns_adapter_fallback_subtree", func(t *testing.T) {
		leaf := &parserast.DefaultExpr{}
		child := &parserast.ParenthesesExpr{Expr: leaf}
		root := &unsupportedExternalNode{Node: &parserast.DefaultExpr{}, child: child}

		var events []string
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				events = append(events, "enter "+walkFallbackNodeName(n, root, child, leaf))
				return false
			},
			leave: func(n parserast.Node) bool {
				events = append(events, "leave "+walkFallbackNodeName(n, root, child, leaf))
				return true
			},
		}

		require.True(t, parserast.Walk(root, visitor))
		require.Equal(t, 1, root.acceptCalls)
		require.Equal(t, []string{
			"enter external",
			"enter child",
			"enter leaf",
			"leave leaf",
			"leave child",
			"leave external",
		}, events)
	})

	t.Run("parser_driver_nodes", func(t *testing.T) {
		testCases := []struct {
			name string
			node parserast.Node
		}{
			{name: "test_driver_value_expr", node: &test_driver.ValueExpr{}},
			{name: "test_driver_param_marker_expr", node: &test_driver.ParamMarkerExpr{}},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				var events []string
				visitor := &testInPlaceVisitor{
					enter: func(node parserast.Node) bool {
						require.Same(t, testCase.node, node)
						events = append(events, "enter")
						return true
					},
					leave: func(node parserast.Node) bool {
						require.Same(t, testCase.node, node)
						events = append(events, "leave")
						return true
					},
				}

				require.True(t, parserast.Walk(testCase.node, visitor))
				require.Equal(t, []string{"enter", "leave"}, events)

				allocationVisitor := &benchmarkInPlaceVisitor{}
				var ok bool
				allocations := testing.AllocsPerRun(100, func() {
					ok = parserast.Walk(testCase.node, allocationVisitor)
				})
				require.True(t, ok)
				require.Zero(t, allocations)
			})
		}
	})

	t.Run("existing_visitor_replaces_child", func(t *testing.T) {
		original := &parserast.DefaultExpr{}
		replacement := &parserast.DefaultExpr{}
		root := &parserast.ParenthesesExpr{Expr: original}
		visitor := &testVisitor{
			enter: func(n parserast.Node) (parserast.Node, bool) {
				return n, false
			},
			leave: func(n parserast.Node) (parserast.Node, bool) {
				if n == original {
					return replacement, true
				}
				return n, true
			},
		}

		_, ok := root.Accept(visitor)
		require.True(t, ok)
		require.Same(t, replacement, root.Expr)
	})

	t.Run("existing_visitor_replaces_table_hints", func(t *testing.T) {
		original := &parserast.TableOptimizerHint{}
		replacement := &parserast.TableOptimizerHint{}
		root := &parserast.SelectStmt{TableHints: []*parserast.TableOptimizerHint{original}}
		visitor := &testVisitor{
			enter: func(n parserast.Node) (parserast.Node, bool) {
				return n, false
			},
			leave: func(n parserast.Node) (parserast.Node, bool) {
				if n == original {
					return replacement, true
				}
				return n, true
			},
		}

		_, ok := root.Accept(visitor)
		require.True(t, ok)
		require.Same(t, replacement, root.TableHints[0])
	})

	t.Run("no_framework_writes", func(t *testing.T) {
		child := &parserast.DefaultExpr{}
		root := &parserast.BetweenExpr{Expr: child, Left: &parserast.DefaultExpr{}, Right: &parserast.DefaultExpr{}}
		enteredChild := make(chan struct{})
		readerReady := make(chan struct{})
		releaseChild := make(chan struct{})
		readerDone := make(chan struct{})
		readerResult := make(chan parserast.ExprNode, 1)
		walkDone := make(chan bool, 1)
		visitor := &testInPlaceVisitor{
			enter: func(n parserast.Node) bool {
				if n == child {
					close(enteredChild)
					<-releaseChild
				}
				return false
			},
			leave: func(parserast.Node) bool {
				return true
			},
		}

		go func() {
			walkDone <- parserast.Walk(root, visitor)
		}()
		<-enteredChild
		go func() {
			close(readerReady)
			<-releaseChild
			readerResult <- root.Expr
			close(readerDone)
		}()
		<-readerReady
		close(releaseChild)
		<-readerDone
		require.Same(t, child, <-readerResult)
		require.True(t, <-walkDone)
	})
}

func normalizedFunction(t *testing.T, filename string, source []byte, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := goparser.ParseFile(fset, filename, source, 0)
	require.NoError(t, err)
	for _, decl := range file.Decls {
		function, ok := decl.(*goast.FuncDecl)
		if !ok || function.Name.Name != name {
			continue
		}
		function.Doc = nil
		var normalized bytes.Buffer
		require.NoError(t, goformat.Node(&normalized, fset, function))
		return normalized.String()
	}
	require.FailNow(t, "function not found", "%s does not define %s", filename, name)
	return ""
}

func walkNodeName(n parserast.Node, root, unary, leafA, leafB, leafC parserast.Node) string {
	switch n {
	case root:
		return "root"
	case unary:
		return "unary"
	case leafA:
		return "A"
	case leafB:
		return "B"
	case leafC:
		return "C"
	default:
		return fmt.Sprintf("unexpected %T", n)
	}
}

func walkFallbackNodeName(n parserast.Node, root, child, leaf parserast.Node) string {
	switch n {
	case root:
		return "external"
	case child:
		return "child"
	case leaf:
		return "leaf"
	default:
		return fmt.Sprintf("unexpected %T", n)
	}
}

type writebackCandidate struct {
	file     string
	line     int
	receiver string
	method   string
	lhs      string
	guarded  bool
}

type leafAcceptMethod struct {
	file     string
	line     int
	receiver string
	decl     *goast.FuncDecl
}

func TestWalkWritebackInventory(t *testing.T) {
	entries, err := traversalSources.ReadDir(".")
	require.NoError(t, err)

	var acceptCount, legacyInPlaceHelperCount int
	var functionsWithWritebacks int
	var candidates []writebackCandidate
	var cacheIssues []string
	var forbiddenSymbols []string
	var leafAcceptMethods []leafAcceptMethod
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		source, err := traversalSources.ReadFile(name)
		require.NoError(t, err)

		fset := token.NewFileSet()
		file, err := goparser.ParseFile(fset, name, source, 0)
		require.NoError(t, err)
		goast.Inspect(file, func(node goast.Node) bool {
			identifier, ok := node.(*goast.Ident)
			if !ok {
				return true
			}
			switch identifier.Name {
			case "shouldReplaceNode", "inPlaceVisitorMarker", "inPlaceVisitor":
				forbiddenSymbols = append(forbiddenSymbols, fmt.Sprintf("%s:%d %s", name, fset.Position(identifier.Pos()).Line, identifier.Name))
			}
			return true
		})
		for _, decl := range file.Decls {
			function, ok := decl.(*goast.FuncDecl)
			if !ok || function.Recv == nil || function.Body == nil {
				continue
			}

			switch function.Name.Name {
			case "Accept":
				acceptCount++
				if !hasChildTraversalCall(function.Body) {
					leafAcceptMethods = append(leafAcceptMethods, leafAcceptMethod{
						file:     name,
						line:     fset.Position(function.Pos()).Line,
						receiver: renderExpr(t, fset, function.Recv.List[0].Type),
						decl:     function,
					})
				}
			case "acceptInPlace":
				if function.Type.Params == nil || len(function.Type.Params.List) != 1 ||
					renderExpr(t, fset, function.Type.Params.List[0].Type) != "Visitor" {
					continue
				}
				legacyInPlaceHelperCount++
			default:
				continue
			}

			receiver := renderExpr(t, fset, function.Recv.List[0].Type)
			var functionCandidates []writebackCandidate
			collectWritebackCandidates(function.Body, func(lhs goast.Expr, guarded bool) {
				functionCandidates = append(functionCandidates, writebackCandidate{
					file:     name,
					line:     fset.Position(lhs.Pos()).Line,
					receiver: receiver,
					method:   function.Name.Name,
					lhs:      renderExpr(t, fset, lhs),
					guarded:  guarded,
				})
			})
			candidates = append(candidates, functionCandidates...)

			topLevelCaches, cacheCalls, cacheWrites := inspectReplacementModeCache(function.Body)
			if len(functionCandidates) > 0 {
				functionsWithWritebacks++
			}
			if topLevelCaches != 0 || cacheCalls != 0 || cacheWrites != 0 {
				cacheIssues = append(cacheIssues, fmt.Sprintf(
					"%s (%s).%s: top-level caches=%d calls=%d writes=%d",
					name, receiver, function.Name.Name, topLevelCaches, cacheCalls, cacheWrites,
				))
			}
		}
	}

	t.Run("leaf_accept_fast_path", func(t *testing.T) {
		sort.Slice(leafAcceptMethods, func(i, j int) bool {
			if leafAcceptMethods[i].file != leafAcceptMethods[j].file {
				return leafAcceptMethods[i].file < leafAcceptMethods[j].file
			}
			return leafAcceptMethods[i].line < leafAcceptMethods[j].line
		})
		require.Len(t, leafAcceptMethods, 72)

		var fastPaths []string
		for _, method := range leafAcceptMethods {
			if isLeafAcceptFastPath(method.decl) {
				fastPaths = append(fastPaths, method.receiver)
			}
		}
		require.ElementsMatch(t, []string{
			"*CancelDistributionJobStmt",
			"*ImportIntoActionStmt",
		}, fastPaths)
	})

	t.Run("hot_in_place_dispatch_fast_path", func(t *testing.T) {
		source, err := traversalSources.ReadFile("ast.go")
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]string{"ColumnNameExpr", "DefaultExpr", "SelectStmt"},
			inPlaceDispatchFastPaths(t, source),
		)
	})

	require.Equal(t, 213, acceptCount)
	require.Equal(t, 6, legacyInPlaceHelperCount)
	require.Equal(t, 219, acceptCount+legacyInPlaceHelperCount)
	require.Equal(t, 140, functionsWithWritebacks)
	require.Empty(t, cacheIssues, "replacement-mode cache issues: %s", formatCacheIssues(cacheIssues))
	require.Empty(t, forbiddenSymbols, "removed in-place replacement symbols remain: %s", strings.Join(forbiddenSymbols, ", "))
	require.Len(t, candidates, 271, "writeback candidates: %s", formatWritebackCandidates(candidates))

	var guarded []writebackCandidate
	for _, candidate := range candidates {
		if candidate.guarded {
			guarded = append(guarded, candidate)
		}
	}
	require.Empty(t, guarded, "guarded writebacks: %s", formatWritebackCandidates(guarded))
	require.Equal(t, 271, len(candidates)-len(guarded), "unguarded writebacks: %s", formatWritebackCandidates(candidates))
}

func inPlaceDispatchFastPaths(t *testing.T, source []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := goparser.ParseFile(fset, "ast.go", source, 0)
	require.NoError(t, err)
	for _, decl := range file.Decls {
		function, ok := decl.(*goast.FuncDecl)
		if !ok || function.Name.Name != "acceptInPlaceNode" {
			continue
		}
		var fastPaths []string
		goast.Inspect(function.Body, func(node goast.Node) bool {
			clause, ok := node.(*goast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range clause.List {
				pointer, ok := expr.(*goast.StarExpr)
				if !ok {
					continue
				}
				name, ok := pointer.X.(*goast.Ident)
				if ok {
					fastPaths = append(fastPaths, name.Name)
				}
			}
			return true
		})
		return fastPaths
	}
	require.FailNow(t, "acceptInPlaceNode not found")
	return nil
}

func hasChildTraversalCall(body *goast.BlockStmt) bool {
	var found bool
	goast.Inspect(body, func(node goast.Node) bool {
		call, ok := node.(*goast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*goast.SelectorExpr)
		if ok {
			switch selector.Sel.Name {
			case "Accept", "acceptInPlace":
				found = true
				return false
			case "Enter":
				if len(call.Args) == 1 {
					if _, childSelector := call.Args[0].(*goast.SelectorExpr); childSelector {
						found = true
						return false
					}
				}
			}
		}
		return true
	})
	return found
}

func isLeafAcceptFastPath(function *goast.FuncDecl) bool {
	if len(function.Recv.List) != 1 || len(function.Recv.List[0].Names) != 1 || len(function.Body.List) != 2 {
		return false
	}
	receiverName := function.Recv.List[0].Names[0].Name

	assignment, ok := function.Body.List[0].(*goast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
		return false
	}
	newNode, ok := assignment.Lhs[0].(*goast.Ident)
	if !ok || newNode.Name != "newNode" || !isIdentifierNamed(assignment.Lhs[1], "_") ||
		!isVisitorCall(assignment.Rhs[0], "Enter", receiverName) {
		return false
	}

	returnStatement, ok := function.Body.List[1].(*goast.ReturnStmt)
	return ok && len(returnStatement.Results) == 1 && isVisitorCall(returnStatement.Results[0], "Leave", newNode.Name)
}

func isVisitorCall(expr goast.Expr, method, argument string) bool {
	call, ok := expr.(*goast.CallExpr)
	if !ok || len(call.Args) != 1 || !isIdentifierNamed(call.Args[0], argument) {
		return false
	}
	selector, ok := call.Fun.(*goast.SelectorExpr)
	return ok && selector.Sel.Name == method && isIdentifierNamed(selector.X, "v")
}

func isIdentifierNamed(expr goast.Expr, name string) bool {
	identifier, ok := expr.(*goast.Ident)
	return ok && identifier.Name == name
}

func inspectReplacementModeCache(body *goast.BlockStmt) (topLevelCaches, cacheCalls, cacheWrites int) {
	for _, statement := range body.List {
		if isReplacementModeCacheInitialization(statement) {
			topLevelCaches++
		}
	}

	goast.Inspect(body, func(node goast.Node) bool {
		switch node := node.(type) {
		case *goast.CallExpr:
			if isShouldReplaceNodeCall(node) {
				cacheCalls++
			}
		case *goast.AssignStmt:
			for _, lhs := range node.Lhs {
				if isReplacementModeIdentifier(lhs) {
					cacheWrites++
				}
			}
		case *goast.IncDecStmt:
			if isReplacementModeIdentifier(node.X) {
				cacheWrites++
			}
		case *goast.RangeStmt:
			if isReplacementModeIdentifier(node.Key) {
				cacheWrites++
			}
			if isReplacementModeIdentifier(node.Value) {
				cacheWrites++
			}
		}
		return true
	})
	return topLevelCaches, cacheCalls, cacheWrites
}

func isReplacementModeCacheInitialization(statement goast.Stmt) bool {
	assignment, ok := statement.(*goast.AssignStmt)
	return ok &&
		assignment.Tok == token.DEFINE &&
		len(assignment.Lhs) == 1 &&
		len(assignment.Rhs) == 1 &&
		isReplacementModeIdentifier(assignment.Lhs[0]) &&
		isShouldReplaceNodeCall(assignment.Rhs[0])
}

func collectWritebackCandidates(body *goast.BlockStmt, add func(goast.Expr, bool)) {
	goast.Walk(writebackVisitor{add: add}, body)
}

type writebackVisitor struct {
	guarded bool
	add     func(goast.Expr, bool)
}

func (v writebackVisitor) Visit(node goast.Node) goast.Visitor {
	if node == nil {
		return nil
	}

	if ifStmt, ok := node.(*goast.IfStmt); ok {
		if ifStmt.Init != nil {
			goast.Walk(v, ifStmt.Init)
		}
		goast.Walk(v, ifStmt.Cond)
		goast.Walk(writebackVisitor{guarded: v.guarded || isReplacementModeIdentifier(ifStmt.Cond), add: v.add}, ifStmt.Body)
		if ifStmt.Else != nil {
			goast.Walk(v, ifStmt.Else)
		}
		return nil
	}

	if assign, ok := node.(*goast.AssignStmt); ok {
		for _, lhs := range assign.Lhs {
			if _, isIdentifier := lhs.(*goast.Ident); !isIdentifier {
				v.add(lhs, v.guarded)
			}
		}
	}
	return v
}

func isShouldReplaceNodeCall(expr goast.Expr) bool {
	call, ok := expr.(*goast.CallExpr)
	if !ok {
		return false
	}
	name, ok := call.Fun.(*goast.Ident)
	if !ok || name.Name != "shouldReplaceNode" || len(call.Args) != 1 {
		return false
	}
	argument, ok := call.Args[0].(*goast.Ident)
	return ok && argument.Name == "v"
}

func isReplacementModeIdentifier(expr goast.Expr) bool {
	identifier, ok := expr.(*goast.Ident)
	return ok && identifier.Name == "replaceNode"
}

func renderExpr(t *testing.T, fset *token.FileSet, expr goast.Expr) string {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, goformat.Node(&buffer, fset, expr))
	return buffer.String()
}

func formatWritebackCandidates(candidates []writebackCandidate) string {
	if len(candidates) == 0 {
		return "none"
	}
	var buffer bytes.Buffer
	for i, candidate := range candidates {
		if i > 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(&buffer, "%s:%d (%s).%s %s", candidate.file, candidate.line, candidate.receiver, candidate.method, candidate.lhs)
	}
	return buffer.String()
}

func formatCacheIssues(issues []string) string {
	if len(issues) == 0 {
		return "none"
	}
	const maxIssues = 10
	if len(issues) <= maxIssues {
		return strings.Join(issues, ", ")
	}
	return fmt.Sprintf("%s, and %d more", strings.Join(issues[:maxIssues], ", "), len(issues)-maxIssues)
}
