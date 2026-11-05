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

package gofmt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis"
)

func TestFormattingDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, source, want string
	}{
		{"formatted", "package example\n\nvar x any\n", ""},
		{"rewriteAny", "package example\n\nvar x interface{}\n", "+var x any"},
		{"simplify", "package example\n\nvar x = map[string][]int{\"x\": []int{1}}\n", "+var x = map[string][]int{\"x\": {1}}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "example.go")
			if err := os.WriteFile(filename, []byte(tc.source), 0600); err != nil {
				t.Fatal(err)
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filename, tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			var diagnostics []analysis.Diagnostic
			_, err = run(&analysis.Pass{
				Fset:   fset,
				Files:  []*ast.File{file},
				Report: func(d analysis.Diagnostic) { diagnostics = append(diagnostics, d) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if len(diagnostics) != 0 {
					t.Fatalf("formatted source reported diagnostics: %v", diagnostics)
				}
			} else if len(diagnostics) != 1 || !strings.Contains(diagnostics[0].Message, tc.want) {
				t.Fatalf("expected diff containing %q, got %v", tc.want, diagnostics)
			}
			after, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != tc.source {
				t.Fatal("analyzer changed the input file")
			}
		})
	}
}
