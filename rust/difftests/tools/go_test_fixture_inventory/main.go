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

// Command go_test_fixture_inventory inventories fixture/file access syntax in
// every upstream *_test.go source. It deliberately works from go/parser ASTs
// only: build-tagged files remain obligations, comments and strings cannot
// manufacture calls, and no package loading or file-system guessing occurs.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var supportedCalls = map[string]struct{}{
	"ReadFile": {}, "Open": {}, "OpenFile": {}, "Stat": {}, "ReadDir": {},
}

type access struct {
	path     string
	line     int
	api      string
	expr     string
	resolved string
}

// tsvField keeps one generated inventory record on one physical line. The
// representation is intentionally reversible enough for review while leaving
// the original Go expression authoritative in source.
func tsvField(value string) string {
	return strings.NewReplacer("\\", "\\\\", "\t", "\\t", "\n", "\\n", "\r", "\\r").Replace(value)
}

func ignoredDirectory(name string) bool {
	return name == ".git" || name == "target" || name == "rust" || name == "vendor" || name == "node_modules"
}

func expressionText(fset *token.FileSet, expr ast.Expr) (string, error) {
	var buffer bytes.Buffer
	if err := format.Node(&buffer, fset, expr); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

// resolveLiteral records only a direct string literal that is lexically local
// to the source file. Calls through helpers, joins, concatenation, absolute
// paths, and repository escapes intentionally stay unresolved obligations.
func resolveLiteral(sourcePath string, expr ast.Expr) string {
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return ""
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil || value == "" || filepath.IsAbs(value) {
		return ""
	}
	candidate := filepath.Clean(filepath.Join(filepath.Dir(sourcePath), value))
	if candidate == "." || candidate == ".." || strings.HasPrefix(candidate, ".."+string(filepath.Separator)) {
		return ""
	}
	return filepath.ToSlash(candidate)
}

func embedExpression(comment string) (string, ast.Expr) {
	rest := strings.TrimSpace(strings.TrimPrefix(comment, "//go:embed"))
	if rest == "" || strings.HasPrefix(rest, "all:") || strings.ContainsAny(rest, "\t\n") || strings.ContainsAny(rest, "*?[") || strings.Contains(rest, " ") {
		return rest, nil
	}
	return rest, &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(rest)}
}

func isEmbedDirective(comment string) bool {
	if !strings.HasPrefix(comment, "//go:embed") {
		return false
	}
	if len(comment) == len("//go:embed") {
		return true
	}
	next := comment[len("//go:embed")]
	return next == ' ' || next == '\t'
}

func osImportNames(file *ast.File) (map[string]bool, bool) {
	names := make(map[string]bool)
	dotImport := false
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != "os" {
			continue
		}
		if spec.Name == nil {
			names["os"] = true
			continue
		}
		switch spec.Name.Name {
		case ".":
			dotImport = true
		case "_":
		default:
			names[spec.Name.Name] = true
		}
	}
	return names, dotImport
}

func collectFile(fset *token.FileSet, sourcePath string, file *ast.File) ([]access, error) {
	var accesses []access
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !isEmbedDirective(comment.Text) {
				continue
			}
			expr, literal := embedExpression(comment.Text)
			resolved := ""
			if literal != nil {
				resolved = resolveLiteral(sourcePath, literal)
			}
			accesses = append(accesses, access{
				path: sourcePath, line: fset.PositionFor(comment.Pos(), true).Line,
				api: "go:embed", expr: expr, resolved: resolved,
			})
		}
	}

	osNames, dotImport := osImportNames(file)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		var api string
		switch function := call.Fun.(type) {
		case *ast.SelectorExpr:
			packageName, ok := function.X.(*ast.Ident)
			if !ok || packageName.Obj != nil || !osNames[packageName.Name] {
				return true
			}
			if _, ok := supportedCalls[function.Sel.Name]; !ok {
				return true
			}
			api = "os." + function.Sel.Name
		case *ast.Ident:
			if !dotImport || function.Obj != nil {
				return true
			}
			if _, ok := supportedCalls[function.Name]; !ok {
				return true
			}
			api = "os." + function.Name
		default:
			return true
		}
		expr, err := expressionText(fset, call.Args[0])
		if err != nil {
			// AST formatter failures are impossible for parsed expressions; retain
			// the source obligation rather than silently dropping the call.
			expr = "<unrenderable-expression>"
		}
		accesses = append(accesses, access{
			path: sourcePath, line: fset.PositionFor(call.Pos(), true).Line,
			api: api, expr: expr, resolved: resolveLiteral(sourcePath, call.Args[0]),
		})
		return true
	})
	return accesses, nil
}

func collect(root string) ([]access, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			isRootChild := filepath.Dir(path) == root
			if path != root && (entry.Name() == ".git" || entry.Name() == "target" || (isRootChild && ignoredDirectory(entry.Name()))) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(entry.Name(), "_test.go") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)

	fileset := token.NewFileSet()
	var accesses []access
	for _, path := range paths {
		file, err := parser.ParseFile(fileset, path, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		found, err := collectFile(fileset, filepath.ToSlash(relative), file)
		if err != nil {
			return nil, err
		}
		accesses = append(accesses, found...)
	}
	sort.Slice(accesses, func(left, right int) bool {
		first, second := accesses[left], accesses[right]
		if first.path != second.path {
			return first.path < second.path
		}
		if first.line != second.line {
			return first.line < second.line
		}
		if first.api != second.api {
			return first.api < second.api
		}
		if first.expr != second.expr {
			return first.expr < second.expr
		}
		return first.resolved < second.resolved
	})
	return accesses, nil
}

func main() {
	root := flag.String("root", ".", "repository root to inventory")
	flag.Parse()
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	accesses, err := collect(absRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("# source_path\tsource_line\tapi\texpression\tresolved_literal_path")
	for _, access := range accesses {
		fmt.Printf("%s\t%d\t%s\t%s\t%s\n", tsvField(access.path), access.line, tsvField(access.api), tsvField(access.expr), tsvField(access.resolved))
	}
}
