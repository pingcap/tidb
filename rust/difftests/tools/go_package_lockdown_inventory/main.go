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

// Command go_package_lockdown_inventory emits a deterministic AST obligation
// set for one non-recursive Go package directory. It intentionally parses every
// .go file regardless of build constraints: excluded source is still an
// upstream rewrite obligation and must be classified explicitly.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type obligation struct {
	ID         string
	Category   string
	SourcePath string
	Anchor     string
	NodeSHA256 string
	Owner      string
}

type collector struct {
	fileset  *token.FileSet
	path     string
	testFile bool
	rows     []obligation
}

func digest(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func formatNode(fileset *token.FileSet, node any) string {
	var output bytes.Buffer
	switch value := node.(type) {
	case string:
		output.WriteString(value)
	case *ast.Field:
		for _, name := range value.Names {
			output.WriteString(name.Name)
			output.WriteByte(',')
		}
		output.WriteString(formatNode(fileset, value.Type))
		if value.Tag != nil {
			output.WriteString(value.Tag.Value)
		}
	default:
		if err := printer.Fprint(&output, fileset, value); err != nil {
			panic(err)
		}
	}
	return output.String()
}

func (c *collector) add(category, anchor, owner string, node any) {
	formatted := formatNode(c.fileset, node)
	nodeHash := digest(formatted)
	identity := digest(c.path, category, anchor, nodeHash)
	c.rows = append(c.rows, obligation{
		ID:         "O" + identity[:16],
		Category:   category,
		SourcePath: c.path,
		Anchor:     anchor,
		NodeSHA256: nodeHash,
		Owner:      owner,
	})
}

func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	typ := fn.Recv.List[0].Type
	if pointer, ok := typ.(*ast.StarExpr); ok {
		typ = pointer.X
	}
	return formatNode(token.NewFileSet(), typ)
}

func functionOwner(fn *ast.FuncDecl) string {
	if receiver := receiverName(fn); receiver != "" {
		return receiver + "." + fn.Name.Name
	}
	return fn.Name.Name
}

func testName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		return true
	}
	next, _ := utf8.DecodeRuneInString(name[len(prefix):])
	return !unicode.IsLower(next)
}

func functionCategory(fn *ast.FuncDecl, testFile bool) string {
	if !testFile {
		return "function"
	}
	switch {
	case fn.Name.Name == "TestMain":
		return "test_main"
	case testName(fn.Name.Name, "Test"):
		return "test"
	case testName(fn.Name.Name, "Benchmark"):
		return "benchmark"
	case testName(fn.Name.Name, "Fuzz"):
		return "fuzz"
	case testName(fn.Name.Name, "Example"):
		return "example"
	default:
		return "test_helper"
	}
}

func fieldLabel(field *ast.Field, index int) string {
	if len(field.Names) == 0 {
		return fmt.Sprintf("embedded:%d:%s", index, formatNode(token.NewFileSet(), field.Type))
	}
	names := make([]string, 0, len(field.Names))
	for _, name := range field.Names {
		names = append(names, name.Name)
	}
	return fmt.Sprintf("field:%d:%s", index, strings.Join(names, ","))
}

func (c *collector) addFields(typeName string, expression ast.Expr) {
	var fields *ast.FieldList
	switch typ := expression.(type) {
	case *ast.StructType:
		fields = typ.Fields
	case *ast.InterfaceType:
		fields = typ.Methods
	}
	if fields == nil {
		return
	}
	for index, field := range fields.List {
		c.add("field", "type:"+typeName+"/"+fieldLabel(field, index), "type:"+typeName, field)
	}
}

func (c *collector) addDeclaration(declaration ast.Decl) {
	switch decl := declaration.(type) {
	case *ast.FuncDecl:
		owner := functionOwner(decl)
		category := functionCategory(decl, c.testFile)
		c.add(category, owner, owner, decl.Type)
		if decl.Body != nil {
			c.addBody(owner, category, decl.Body)
		}
	case *ast.GenDecl:
		for _, rawSpec := range decl.Specs {
			switch spec := rawSpec.(type) {
			case *ast.TypeSpec:
				category := "declaration"
				if c.testFile {
					category = "test_support_declaration"
				}
				anchor := "type:" + spec.Name.Name
				c.add(category, anchor, anchor, spec)
				c.addFields(spec.Name.Name, spec.Type)
			case *ast.ValueSpec:
				category := strings.ToLower(decl.Tok.String())
				if c.testFile {
					category = "test_support_" + category
				}
				for index, name := range spec.Names {
					anchor := fmt.Sprintf("%s:%s:%d", strings.ToLower(decl.Tok.String()), name.Name, index)
					c.add(category, anchor, anchor, spec)
				}
			}
		}
	}
}

var assertionMethods = map[string]struct{}{
	"Assert": {}, "Check": {}, "Contains": {}, "ElementsMatch": {}, "Empty": {},
	"Equal": {}, "EqualError": {}, "Error": {}, "ErrorContains": {}, "ErrorIs": {},
	"False": {}, "Greater": {}, "GreaterOrEqual": {}, "Len": {}, "Less": {},
	"LessOrEqual": {}, "Nil": {}, "NoError": {}, "NotContains": {}, "NotEmpty": {},
	"NotEqual": {}, "NotNil": {}, "NotPanics": {}, "NotSame": {}, "Panics": {},
	"Regexp": {}, "Same": {}, "True": {}, "WithinDuration": {}, "Zero": {},
}

func selectorName(expression ast.Expr) string {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	return selector.Sel.Name
}

func (c *collector) addBody(owner, ownerCategory string, body *ast.BlockStmt) {
	testBody := ownerCategory == "test" || ownerCategory == "benchmark" || ownerCategory == "fuzz" || ownerCategory == "test_main" || ownerCategory == "test_helper" || ownerCategory == "example"
	counters := map[string]int{}
	next := func(kind string) int {
		counters[kind]++
		return counters[kind]
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.FuncLit:
			index := next("closure")
			category := "closure"
			if testBody {
				category = "test_helper_closure"
			}
			c.add(category, fmt.Sprintf("%s/closure:%d", owner, index), owner, value.Type)
		case *ast.IfStmt:
			index := next("if")
			category := "branch"
			if testBody {
				category = "test_branch"
			}
			c.add(category, fmt.Sprintf("%s/if:%d/true", owner, index), owner, value)
			c.add(category, fmt.Sprintf("%s/if:%d/false", owner, index), owner, value)
		case *ast.SwitchStmt:
			index := next("switch")
			c.addSwitch(owner, testBody, fmt.Sprintf("switch:%d", index), value.Body)
		case *ast.TypeSwitchStmt:
			index := next("type_switch")
			c.addSwitch(owner, testBody, fmt.Sprintf("type_switch:%d", index), value.Body)
		case *ast.SelectStmt:
			index := next("select")
			c.addSelect(owner, testBody, index, value.Body)
		case *ast.ForStmt:
			index := next("loop")
			c.addLoop(owner, testBody, index, value)
		case *ast.RangeStmt:
			index := next("loop")
			c.addLoop(owner, testBody, index, value)
		case *ast.BinaryExpr:
			if value.Op == token.LAND || value.Op == token.LOR {
				index := next("logical")
				category := "short_circuit"
				if testBody {
					category = "test_short_circuit"
				}
				prefix := fmt.Sprintf("%s/logical:%d:%s", owner, index, value.Op.String())
				c.add(category, prefix+"/lhs_short_circuits", owner, value)
				c.add(category, prefix+"/rhs_evaluated", owner, value)
			}
		case *ast.CompositeLit:
			if testBody {
				literal := next("composite")
				for element, row := range value.Elts {
					c.add("test_row", fmt.Sprintf("%s/composite:%d/element:%d", owner, literal, element), owner, row)
				}
			}
		case *ast.CallExpr:
			if testBody {
				method := selectorName(value.Fun)
				if _, ok := assertionMethods[method]; ok {
					index := next("assertion")
					c.add("test_assertion", fmt.Sprintf("%s/assertion:%d:%s", owner, index, method), owner, value)
				}
			}
		}
		return true
	})
}

func (c *collector) addSwitch(owner string, testBody bool, label string, body *ast.BlockStmt) {
	category := "switch_case"
	if testBody {
		category = "test_switch_case"
	}
	hasDefault := false
	caseIndex := 0
	for _, statement := range body.List {
		clause, ok := statement.(*ast.CaseClause)
		if !ok {
			continue
		}
		caseIndex++
		kind := "case"
		if len(clause.List) == 0 {
			kind = "default"
			hasDefault = true
		}
		c.add(category, fmt.Sprintf("%s/%s/%s:%d", owner, label, kind, caseIndex), owner, clause)
	}
	if !hasDefault {
		c.add(category, fmt.Sprintf("%s/%s/no_match", owner, label), owner, body)
	}
}

func (c *collector) addSelect(owner string, testBody bool, index int, body *ast.BlockStmt) {
	category := "select_case"
	if testBody {
		category = "test_select_case"
	}
	hasDefault := false
	caseIndex := 0
	for _, statement := range body.List {
		clause, ok := statement.(*ast.CommClause)
		if !ok {
			continue
		}
		caseIndex++
		kind := "case"
		if clause.Comm == nil {
			kind = "default"
			hasDefault = true
		}
		c.add(category, fmt.Sprintf("%s/select:%d/%s:%d", owner, index, kind, caseIndex), owner, clause)
	}
	if !hasDefault {
		c.add(category, fmt.Sprintf("%s/select:%d/blocks_without_ready_case", owner, index), owner, body)
	}
}

func (c *collector) addLoop(owner string, testBody bool, index int, loop ast.Node) {
	category := "loop"
	if testBody {
		category = "test_loop"
	}
	c.add(category, fmt.Sprintf("%s/loop:%d/zero_iterations", owner, index), owner, loop)
	c.add(category, fmt.Sprintf("%s/loop:%d/enters", owner, index), owner, loop)
}

func collect(root, packagePath string) ([]obligation, error) {
	directory := filepath.Join(root, filepath.FromSlash(packagePath))
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			paths = append(paths, filepath.Join(directory, entry.Name()))
		}
	}
	sort.Strings(paths)
	fileset := token.NewFileSet()
	rows := make([]obligation, 0, len(paths)*64)
	for _, path := range paths {
		file, err := parser.ParseFile(fileset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		collector := collector{
			fileset:  fileset,
			path:     filepath.ToSlash(relative),
			testFile: strings.HasSuffix(path, "_test.go"),
		}
		for _, declaration := range file.Decls {
			collector.addDeclaration(declaration)
		}
		rows = append(rows, collector.rows...)
	}
	sort.Slice(rows, func(left, right int) bool {
		if rows[left].SourcePath != rows[right].SourcePath {
			return rows[left].SourcePath < rows[right].SourcePath
		}
		if rows[left].Anchor != rows[right].Anchor {
			return rows[left].Anchor < rows[right].Anchor
		}
		return rows[left].Category < rows[right].Category
	})
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if _, exists := seen[row.ID]; exists {
			return nil, fmt.Errorf("duplicate obligation id %s", row.ID)
		}
		seen[row.ID] = struct{}{}
	}
	return rows, nil
}

func main() {
	root := flag.String("root", ".", "repository root")
	packagePath := flag.String("package", "pkg/types", "non-recursive Go package path")
	flag.Parse()
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rows, err := collect(absRoot, *packagePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("# obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner")
	for _, row := range rows {
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\n", row.ID, row.Category, row.SourcePath, row.Anchor, row.NodeSHA256, row.Owner)
	}
}
