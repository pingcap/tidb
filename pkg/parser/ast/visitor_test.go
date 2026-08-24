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
	"strings"
	"testing"

	parserast "github.com/pingcap/tidb/pkg/parser/ast"
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

func TestWalk(t *testing.T) {
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

type writebackCandidate struct {
	file     string
	line     int
	receiver string
	method   string
	lhs      string
	guarded  bool
}

func TestWalkWritebackInventory(t *testing.T) {
	entries, err := traversalSources.ReadDir(".")
	require.NoError(t, err)

	var acceptCount, acceptInPlaceCount int
	var functionsWithWritebacks, cachedFunctions int
	var candidates []writebackCandidate
	var cacheIssues []string
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
		for _, decl := range file.Decls {
			function, ok := decl.(*goast.FuncDecl)
			if !ok || function.Recv == nil || function.Body == nil {
				continue
			}

			switch function.Name.Name {
			case "Accept":
				acceptCount++
			case "acceptInPlace":
				acceptInPlaceCount++
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
				if topLevelCaches == 1 && cacheCalls == 1 && cacheWrites == 1 {
					cachedFunctions++
				} else {
					cacheIssues = append(cacheIssues, fmt.Sprintf(
						"%s (%s).%s: top-level caches=%d calls=%d writes=%d",
						name, receiver, function.Name.Name, topLevelCaches, cacheCalls, cacheWrites,
					))
				}
			} else if topLevelCaches != 0 || cacheCalls != 0 || cacheWrites != 0 {
				cacheIssues = append(cacheIssues, fmt.Sprintf(
					"%s (%s).%s caches replacement mode without writebacks",
					name, receiver, function.Name.Name,
				))
			}
		}
	}

	require.Equal(t, 213, acceptCount)
	require.Equal(t, 6, acceptInPlaceCount)
	require.Equal(t, 219, acceptCount+acceptInPlaceCount)
	require.Equal(t, 140, functionsWithWritebacks)
	require.Equal(t, 140, cachedFunctions, "replacement-mode cache issues: %s", formatCacheIssues(cacheIssues))
	require.Empty(t, cacheIssues, "replacement-mode cache issues: %s", formatCacheIssues(cacheIssues))
	require.Len(t, candidates, 271, "writeback candidates: %s", formatWritebackCandidates(candidates))

	var unguarded []writebackCandidate
	for _, candidate := range candidates {
		if !candidate.guarded {
			unguarded = append(unguarded, candidate)
		}
	}
	require.Empty(t, unguarded, "unguarded writebacks: %s", formatWritebackCandidates(unguarded))
	require.Equal(t, 271, len(candidates)-len(unguarded), "guarded writebacks: %s", formatWritebackCandidates(candidates))
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
