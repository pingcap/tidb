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

// Command go_test_declaration_inventory inventories every AST function
// declaration in the repository's *_test.go files. It deliberately does not
// load packages, resolve build tags, or invoke a Go test binary: build-tagged
// source is still an upstream rewrite obligation. The only Go-language
// authority is go/parser and go/ast, so comments and string literals cannot
// manufacture declarations.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type declaration struct {
	path     string
	line     int
	column   int
	receiver string
	name     string
	category string
	valid    bool
}

var lifecycleHooks = map[string]struct{}{
	"SetUpTest": {}, "SetupTest": {}, "TearDownTest": {},
	"SetUpSuite": {}, "SetupSuite": {}, "TearDownSuite": {},
	"SetupSubTest": {}, "TearDownSubTest": {}, "BeforeTest": {},
	"AfterTest": {}, "WithStats": {},
}

func ignoredDirectory(name string) bool {
	return name == ".git" || name == "target" || name == "rust" || name == "vendor" || name == "node_modules"
}

func testName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	rune, _ := utf8.DecodeRuneInString(name[len(prefix):])
	return !unicode.IsLower(rune)
}

// isTestFunc mirrors cmd/go's structural signature check. It intentionally
// accepts *T and *pkg.T forms because import aliases cannot be resolved
// without package loading, which this source inventory must never do.
func isTestFunc(fn *ast.FuncDecl, argument string) bool {
	if fn.Recv != nil ||
		(fn.Type.Results != nil && len(fn.Type.Results.List) > 0) ||
		fn.Type.Params == nil ||
		len(fn.Type.Params.List) != 1 ||
		len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	pointer, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	if ident, ok := pointer.X.(*ast.Ident); ok {
		return ident.Name == argument
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == argument
}

func isExample(fn *ast.FuncDecl) bool {
	return fn.Recv == nil &&
		fn.Type.Params != nil && len(fn.Type.Params.List) == 0 &&
		(fn.Type.Results == nil || len(fn.Type.Results.List) == 0) &&
		fn.Body != nil
}

func classify(fn *ast.FuncDecl) (string, bool) {
	name := fn.Name.Name
	if name == "TestMain" {
		// The Go tool treats TestMain(*testing.T) as an ordinary TestMain
		// test before considering the TestMain(*testing.M) runner hook.
		if isTestFunc(fn, "T") {
			return "Test", true
		}
		return "TestMain", isTestFunc(fn, "M")
	}
	switch {
	case testName(name, "Test"):
		return "Test", isTestFunc(fn, "T")
	case testName(name, "Benchmark"):
		return "Benchmark", isTestFunc(fn, "B")
	case testName(name, "Fuzz"):
		return "Fuzz", isTestFunc(fn, "F")
	case testName(name, "Example"):
		return "Example", isExample(fn)
	}
	if _, ok := lifecycleHooks[name]; ok {
		return "test_hook", false
	}
	return "other", false
}

func receiverKind(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return "function"
	}
	return "method"
}

func collect(root string) ([]declaration, error) {
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
	var declarations []declaration
	for _, path := range paths {
		file, err := parser.ParseFile(fileset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		for _, node := range file.Decls {
			fn, ok := node.(*ast.FuncDecl)
			if !ok {
				continue
			}
			position := fileset.PositionFor(fn.Pos(), true)
			category, valid := classify(fn)
			declarations = append(declarations, declaration{
				path:     filepath.ToSlash(relative),
				line:     position.Line,
				column:   position.Column,
				receiver: receiverKind(fn),
				name:     fn.Name.Name,
				category: category,
				valid:    valid,
			})
		}
	}
	sort.Slice(declarations, func(left, right int) bool {
		first, second := declarations[left], declarations[right]
		if first.path != second.path {
			return first.path < second.path
		}
		if first.line != second.line {
			return first.line < second.line
		}
		if first.column != second.column {
			return first.column < second.column
		}
		if first.name != second.name {
			return first.name < second.name
		}
		return first.receiver < second.receiver
	})
	return declarations, nil
}

func main() {
	root := flag.String("root", ".", "repository root to inventory")
	flag.Parse()
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	declarations, err := collect(absRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("# source_path\tsource_line\tsource_column\treceiver\tfunction_name\tcategory\tvalid_runner_signature")
	for _, declaration := range declarations {
		fmt.Printf("%s\t%d\t%d\t%s\t%s\t%s\t%t\n",
			declaration.path,
			declaration.line,
			declaration.column,
			declaration.receiver,
			declaration.name,
			declaration.category,
			declaration.valid,
		)
	}
}
