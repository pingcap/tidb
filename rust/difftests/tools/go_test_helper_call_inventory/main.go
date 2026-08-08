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

// Command go_test_helper_call_inventory inventories every call expression in
// the direct *_test.go files of one Go package directory. It parses all such
// files regardless of build constraints and deliberately does not load or
// type-check the package: helper-mediated fixture access remains visible even
// when the helper cannot be resolved locally.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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

const schema = "go-test-helper-call-inventory-v1"

var fixtureCalls = map[string]struct{}{
	"ReadFile": {}, "Open": {}, "OpenFile": {}, "Stat": {}, "ReadDir": {},
}

type callRecord struct {
	ID         string
	SourcePath string
	Line       int
	Column     int
	Callee     string
	NodeSHA256 string
	FixtureAPI string
	FirstArg   string
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func callID(record callRecord) string {
	hash := sha256.New()
	for _, part := range []string{
		schema,
		record.SourcePath,
		strconv.Itoa(record.Line),
		strconv.Itoa(record.Column),
		record.Callee,
		record.NodeSHA256,
		record.FixtureAPI,
		record.FirstArg,
	} {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return "C" + hex.EncodeToString(hash.Sum(nil))
}

func formatASTNode(fileset *token.FileSet, node ast.Node) (string, error) {
	var output bytes.Buffer
	if err := format.Node(&output, fileset, node); err != nil {
		return "", err
	}
	return output.String(), nil
}

// tsvField preserves one logical field on one physical TSV line. Go's
// formatter can render function-literal callees across multiple lines.
func tsvField(value string) string {
	return strings.NewReplacer("\\", "\\\\", "\t", "\\t", "\n", "\\n", "\r", "\\r").Replace(value)
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

func directFixtureCall(call *ast.CallExpr, osNames map[string]bool, dotImport bool) string {
	switch function := call.Fun.(type) {
	case *ast.SelectorExpr:
		packageName, ok := function.X.(*ast.Ident)
		if !ok || packageName.Obj != nil || !osNames[packageName.Name] {
			return "-"
		}
		if _, ok := fixtureCalls[function.Sel.Name]; ok {
			return "os." + function.Sel.Name
		}
	case *ast.Ident:
		if function.Obj == nil && dotImport {
			if _, ok := fixtureCalls[function.Name]; ok {
				return "os." + function.Name
			}
		}
	}
	return "-"
}

func collectFile(fileset *token.FileSet, sourcePath, path string) ([]callRecord, error) {
	file, err := parser.ParseFile(fileset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", sourcePath, err)
	}

	var records []callRecord
	var inspectErr error
	osNames, dotImport := osImportNames(file)
	ast.Inspect(file, func(node ast.Node) bool {
		if inspectErr != nil {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		callee, err := formatASTNode(fileset, call.Fun)
		if err != nil {
			inspectErr = fmt.Errorf("format callee at %s: %w", fileset.PositionFor(call.Pos(), false), err)
			return false
		}
		formattedCall, err := formatASTNode(fileset, call)
		if err != nil {
			inspectErr = fmt.Errorf("format call at %s: %w", fileset.PositionFor(call.Pos(), false), err)
			return false
		}
		position := fileset.PositionFor(call.Pos(), false)
		firstArg := "-"
		if len(call.Args) > 0 {
			firstArg, err = formatASTNode(fileset, call.Args[0])
			if err != nil {
				inspectErr = fmt.Errorf("format first argument at %s: %w", position, err)
				return false
			}
		}
		record := callRecord{
			SourcePath: sourcePath,
			Line:       position.Line,
			Column:     position.Column,
			Callee:     callee,
			NodeSHA256: sha256Hex([]byte(formattedCall)),
			FixtureAPI: directFixtureCall(call, osNames, dotImport),
			FirstArg:   firstArg,
		}
		record.ID = callID(record)
		records = append(records, record)
		return true
	})
	if inspectErr != nil {
		return nil, inspectErr
	}
	return records, nil
}

func collect(root string) ([]callRecord, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		paths = append(paths, entry.Name())
	}
	sort.Strings(paths)

	fileset := token.NewFileSet()
	var records []callRecord
	for _, sourcePath := range paths {
		found, err := collectFile(fileset, filepath.ToSlash(sourcePath), filepath.Join(root, sourcePath))
		if err != nil {
			return nil, err
		}
		records = append(records, found...)
	}
	sort.Slice(records, func(left, right int) bool {
		first, second := records[left], records[right]
		if first.SourcePath != second.SourcePath {
			return first.SourcePath < second.SourcePath
		}
		if first.Line != second.Line {
			return first.Line < second.Line
		}
		if first.Column != second.Column {
			return first.Column < second.Column
		}
		if first.Callee != second.Callee {
			return first.Callee < second.Callee
		}
		if first.NodeSHA256 != second.NodeSHA256 {
			return first.NodeSHA256 < second.NodeSHA256
		}
		return first.ID < second.ID
	})
	return records, nil
}

func main() {
	root := flag.String("root", ".", "Go package directory to inventory")
	flag.Parse()
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	records, err := collect(absRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("# schema: %s\n", schema)
	fmt.Println("# scope: every go/ast.CallExpr in direct *_test.go files; build constraints are ignored")
	fmt.Println("# call_id\tsource_path\tsource_line\tsource_column\tcallee\tcall_node_sha256\tfixture_api\tfirst_argument")
	for _, record := range records {
		fmt.Printf("%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			record.ID,
			tsvField(record.SourcePath),
			record.Line,
			record.Column,
			tsvField(record.Callee),
			record.NodeSHA256,
			record.FixtureAPI,
			tsvField(record.FirstArg),
		)
	}
}
