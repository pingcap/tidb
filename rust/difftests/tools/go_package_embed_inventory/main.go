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

// Command go_package_embed_inventory detects every //go:embed directive in
// the direct source files of explicitly named Go packages. The package-scale
// checker currently fails closed when any row is returned: this detector uses
// go/ast's authoritative directive parser and intentionally does not attempt a
// partial reimplementation of cmd/go's embed resolver.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

type packageFlags []string

func (values *packageFlags) String() string { return strings.Join(*values, ",") }
func (values *packageFlags) Set(value string) error {
	*values = append(*values, value)
	return nil
}

type directiveRecord struct {
	SourcePath string
	Line       int
	Column     int
	Args       string
}

func collect(root string, packages []string) ([]directiveRecord, error) {
	seen := make(map[string]bool)
	fileset := token.NewFileSet()
	var records []directiveRecord
	for _, packagePath := range packages {
		clean := path.Clean(packagePath)
		if clean != packagePath || clean == "." || strings.HasPrefix(clean, "../") || seen[clean] {
			return nil, fmt.Errorf("invalid or duplicate -package %q", packagePath)
		}
		seen[clean] = true
		directory := filepath.Join(root, filepath.FromSlash(packagePath))
		entries, err := os.ReadDir(directory)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || strings.HasPrefix(entry.Name(), "_") || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			filePath := filepath.Join(directory, entry.Name())
			file, err := parser.ParseFile(fileset, filePath, nil, parser.ParseComments)
			if err != nil {
				return nil, fmt.Errorf("parse %s: %w", filePath, err)
			}
			for _, group := range file.Comments {
				for _, comment := range group.List {
					directive, ok := ast.ParseDirective(comment.Pos(), comment.Text)
					if !ok || directive.Tool != "go" || directive.Name != "embed" {
						continue
					}
					args, err := directive.ParseArgs()
					if err != nil {
						return nil, fmt.Errorf("%s: %w", fileset.Position(comment.Pos()), err)
					}
					values := make([]string, 0, len(args))
					for _, arg := range args {
						values = append(values, arg.Arg)
					}
					position := fileset.PositionFor(comment.Pos(), false)
					relative, err := filepath.Rel(root, filePath)
					if err != nil {
						return nil, err
					}
					records = append(records, directiveRecord{
						SourcePath: filepath.ToSlash(relative), Line: position.Line,
						Column: position.Column, Args: strings.Join(values, "\x1f"),
					})
				}
			}
		}
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].SourcePath != records[j].SourcePath {
			return records[i].SourcePath < records[j].SourcePath
		}
		if records[i].Line != records[j].Line {
			return records[i].Line < records[j].Line
		}
		return records[i].Column < records[j].Column
	})
	return records, nil
}

func main() {
	root := flag.String("root", ".", "repository root")
	var packages packageFlags
	flag.Var(&packages, "package", "repository-relative Go package directory (repeatable)")
	flag.Parse()
	if len(packages) == 0 {
		fmt.Fprintln(os.Stderr, "at least one -package is required")
		os.Exit(2)
	}
	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	records, err := collect(absRoot, packages)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("# go-package-embed-directive-inventory-v1")
	fmt.Println("source_path\tsource_line\tsource_column\tpatterns")
	for _, item := range records {
		fmt.Printf("%s\t%d\t%d\t%s\n", item.SourcePath, item.Line, item.Column, item.Args)
	}
}
