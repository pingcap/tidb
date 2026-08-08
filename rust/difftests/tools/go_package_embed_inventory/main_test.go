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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFixture(t *testing.T, root, name, contents string) {
	t.Helper()
	filePath := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCollectAllDirectSourceKindsWithAuthoritativeArgumentParsing(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "pkg/sample/main.go", "package sample\nimport _ \"embed\"\n//go:embed assets/*.txt `quoted name`\nvar production string\n")
	writeFixture(t, root, "pkg/sample/main_test.go", "//go:build never\npackage sample\nimport _ \"embed\"\n//go:embed testdata/*\nvar tests string\n")
	writeFixture(t, root, "pkg/sample/_ignored.go", "package sample\n//go:embed hidden\nvar ignored string\n")
	records, err := collect(root, []string{"pkg/sample"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d directives, want two: %#v", len(records), records)
	}
	if records[0].Args != "assets/*.txt\x1fquoted name" || records[1].Args != "testdata/*" {
		t.Fatalf("directive arguments were not parsed by go/ast: %#v", records)
	}
}

func TestCollectIncludesExplicitNestedPackage(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "pkg/sample/main.go", "package sample\n")
	writeFixture(t, root, "pkg/sample/child/main.go", "package child\nimport _ \"embed\"\n//go:embed asset\nvar child string\n")
	records, err := collect(root, []string{"pkg/sample", "pkg/sample/child"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].SourcePath != "pkg/sample/child/main.go" {
		t.Fatalf("nested package directive disappeared: %#v", records)
	}
}

func TestCollectRejectsMalformedQuotedDirective(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "pkg/sample/main.go", "package sample\n//go:embed \"unterminated\nvar value string\n")
	if _, err := collect(root, []string{"pkg/sample"}); err == nil {
		t.Fatal("malformed directive unexpectedly passed")
	}
}
