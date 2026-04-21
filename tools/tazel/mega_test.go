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
	"reflect"
	"testing"

	"github.com/bazelbuild/buildtools/build"
)

func TestMaybeFixMegaBuild(t *testing.T) {
	buildfile := parseBuildFile(t, `
load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "mega_test",
    deps = ["//old"],
)
`)

	cfg := &megaConfig{deps: []string{"//pkg/a", "//pkg/b"}}
	if err := maybeFixMegaBuild(megaBuildPath, buildfile, cfg); err != nil {
		t.Fatalf("maybeFixMegaBuild failed: %v", err)
	}

	got := buildfile.RuleNamed("mega_test").AttrStrings("deps")
	want := []string{"//pkg/a", "//pkg/b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected deps: got %v, want %v", got, want)
	}
}

func TestFixMegaImportedBuildCreatesLibrary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pkg", "foo")
	mustMkdirAll(t, dir)
	mustWriteFile(t, filepath.Join(dir, "case_intest.go"), "package foo\n")
	mustWriteFile(t, filepath.Join(dir, "main_mega_init.go"), "package foo\n")
	mustWriteFile(t, filepath.Join(dir, "register_gen.go"), "package foo\n")
	mustWriteFile(t, filepath.Join(dir, "case_intest_test.go"), "package foo\n")

	buildfile := parseBuildFile(t, `
load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "foo_test",
    deps = [
        "//pkg/testkit",
        "//pkg/testkit/testsetup",
    ],
)
`)

	if err := fixMegaImportedBuild(buildfile, dir, "github.com/pingcap/tidb/pkg/foo"); err != nil {
		t.Fatalf("fixMegaImportedBuild failed: %v", err)
	}

	library := goLibraryForImportPath(buildfile, "github.com/pingcap/tidb/pkg/foo")
	if library == nil {
		t.Fatal("expected go_library to be created")
	}

	gotSrcs, hasBinding, ok := explicitSourceValues(library.Attr("srcs"))
	if !ok {
		t.Fatal("expected explicit src list")
	}
	if hasBinding {
		t.Fatal("did not expect failpoint binding glob")
	}
	wantSrcs := []string{"case_intest.go", "main_mega_init.go", "register_gen.go"}
	if !reflect.DeepEqual(gotSrcs, wantSrcs) {
		t.Fatalf("unexpected srcs: got %v, want %v", gotSrcs, wantSrcs)
	}

	wantDeps := []string{"//pkg/testkit", "//pkg/testkit/testsetup", "//pkg/testkit/mega/register"}
	if got := library.AttrStrings("deps"); !reflect.DeepEqual(got, wantDeps) {
		t.Fatalf("unexpected deps: got %v, want %v", got, wantDeps)
	}
	if got := library.AttrStrings("visibility"); !reflect.DeepEqual(got, []string{"//visibility:public"}) {
		t.Fatalf("unexpected visibility: %v", got)
	}
}

func TestMaybeFixFailpointLibraries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pkg", "bar")
	mustMkdirAll(t, dir)
	mustWriteFile(t, filepath.Join(dir, "bar.go"), "package bar\n\nimport \"github.com/pingcap/failpoint\"\n\nfunc run() { failpoint.Inject(\"x\", nil) }\n")

	buildfile := parseBuildFile(t, `
load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bar",
    srcs = ["bar.go"],
)
`)

	if err := maybeFixFailpointLibraries(filepath.Join(dir, "BUILD.bazel"), buildfile); err != nil {
		t.Fatalf("maybeFixFailpointLibraries failed: %v", err)
	}

	rule := buildfile.RuleNamed("bar")
	gotSrcs, hasBinding, ok := explicitSourceValues(rule.Attr("srcs"))
	if !ok {
		t.Fatal("expected explicit src list")
	}
	if !hasBinding {
		t.Fatal("expected failpoint binding glob to be added")
	}
	if !reflect.DeepEqual(gotSrcs, []string{"bar.go"}) {
		t.Fatalf("unexpected srcs: %v", gotSrcs)
	}
}

func parseBuildFile(t *testing.T, content string) *build.File {
	t.Helper()
	buildfile, err := build.ParseBuild("BUILD.bazel", []byte(content))
	if err != nil {
		t.Fatalf("parse build file: %v", err)
	}
	return buildfile
}

func mustMkdirAll(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
