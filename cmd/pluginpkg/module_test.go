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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
)

func TestPluginInheritsTiDBReplacements(t *testing.T) {
	if os.Getenv("TEST_SRCDIR") != "" {
		goBin, err := bazel.Runfile("bin/go")
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", filepath.Dir(goBin)+string(os.PathListSeparator)+os.Getenv("PATH"))
		t.Setenv("GOROOT", filepath.Dir(filepath.Dir(goBin)))
	}
	t.Setenv("GOCACHE", t.TempDir())
	t.Setenv("GOTOOLCHAIN", "local")
	root := t.TempDir()
	write := func(path, contents string) {
		t.Helper()
		path = filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("old/go.mod", "module example.com/proto\n\ngo 1.23\n")
	write("old/proto.go", "package proto\n")
	write("new/go.mod", "module example.com/proto\n\ngo 1.23\n")
	write("new/proto.go", "package proto\nconst FTS = true\n")
	write("tidb/go.mod", "module github.com/pingcap/tidb\n\ngo 1.23\nrequire example.com/proto v0.0.0\nreplace example.com/proto => ../new\n")
	write("tidb/tidb.go", "package tidb\nimport \"example.com/proto\"\nconst FTS = proto.FTS\n")
	const pluginMod = "module example.com/plugin\n\ngo 1.23\nrequire github.com/pingcap/tidb v0.0.0\nreplace github.com/pingcap/tidb => ../tidb\nreplace example.com/proto => ../old\n"
	write("plugin/go.mod", pluginMod)
	write("plugin/subpackage/plugin.go", "package plugin\nimport \"github.com/pingcap/tidb\"\nvar FTS = tidb.FTS\n")
	pluginModuleDir := filepath.Join(root, "plugin")
	pluginDir := filepath.Join(pluginModuleDir, "subpackage")
	t.Setenv("GOWORK", "off")
	t.Setenv("GOPROXY", "off")
	// The unadapted plugin build ignores dependency-module replacements.
	control := exec.Command("go", "build", "-mod=mod", ".")
	control.Dir = pluginDir
	output, buildErr := control.CombinedOutput()
	if buildErr == nil || !strings.Contains(string(output), "undefined: proto.FTS") {
		t.Fatalf("expected missing FTS API before adaptation: %v\n%s", buildErr, output)
	}
	// Restore the fixture after Go adds its indirect requirement.
	write("plugin/go.mod", pluginMod)
	modfile, cleanup, err := preparePluginModule(context.Background(), pluginDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	cmd := exec.Command("go", "build", "-mod=mod", "-modfile="+modfile, ".")
	cmd.Dir = pluginDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(pluginModuleDir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != pluginMod {
		t.Fatal("plugin go.mod changed")
	}
}
