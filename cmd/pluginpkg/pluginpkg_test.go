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
	"archive/zip"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestBuildPluginWithTiDBReplacements(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Go plugins require Linux or macOS")
	}
	goBin := filepath.Join(runtime.GOROOT(), "bin")
	if _, err := os.Stat(filepath.Join(goBin, "go")); err != nil {
		t.Skip("Go toolchain is required to compile plugins")
	}
	t.Setenv("PATH", goBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("GOFLAGS", "-modcacherw")
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOWORK", "off")
	if output, err := exec.Command("go", "env", "CGO_ENABLED").Output(); err != nil {
		t.Fatal(err)
	} else if string(output) != "1\n" {
		t.Skip("Go plugins require cgo")
	}

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOMODCACHE", filepath.Join(root, "modcache"))
	proxyDir := filepath.Join(root, "proxy", "example.com", "plugin-dependency", "@v")
	if err := os.MkdirAll(proxyDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOPROXY", "file://"+filepath.Join(root, "proxy"))
	// The plugin would normally resolve the published module. TiDB replaces it
	// with a different implementation, which must also be used by the plugin.
	modFile := "module example.com/plugin-dependency\n\ngo 1.23.0\n"
	for name, contents := range map[string]string{
		"v1.0.0.mod":  modFile,
		"v1.0.0.info": `{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}`,
	} {
		if err := os.WriteFile(filepath.Join(proxyDir, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	archiveFile, err := os.Create(filepath.Join(proxyDir, "v1.0.0.zip"))
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(archiveFile)
	for name, contents := range map[string]string{
		"go.mod": modFile,
		"dep.go": "package dep\n\nconst Value = \"published\"\n",
	} {
		file, err := archive.Create("example.com/plugin-dependency@v1.0.0/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(contents)); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"tidb/go.mod": `module github.com/pingcap/tidb

go 1.23.0

require example.com/plugin-dependency v1.0.0

replace example.com/plugin-dependency => ../replacement
`,
		"tidb/tidb.go": `package tidb

import dep "example.com/plugin-dependency"

func Value() string { return dep.Value }
`,
		"replacement/go.mod": "module example.com/plugin-dependency\n\ngo 1.23.0\n",
		"replacement/dep.go": "package dep\n\nconst Value = \"replacement\"\n",
		"plugin/audit/go.mod": `module example.com/audit

go 1.23.0

require (
	github.com/pingcap/tidb v0.0.0
	example.com/plugin-dependency v1.0.0
)

replace github.com/pingcap/tidb => ../../tidb
`,
		"plugin/audit/audit.go": `package main

import "github.com/pingcap/tidb"

var Value = tidb.Value()
`,
		"tidb/loader/main.go": `package main

import (
	"fmt"
	"os"
	"plugin"

	"github.com/pingcap/tidb"
)

func main() {
	p, err := plugin.Open(os.Args[1])
	if err != nil { panic(err) }
	value, err := p.Lookup("Value")
	if err != nil { panic(err) }
	if *value.(*string) != tidb.Value() { panic("plugin and host dependencies differ") }
	fmt.Print(*value.(*string))
}
`,
	}
	for name, contents := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}

	pluginDir := filepath.Join(root, "plugin", "audit")
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = pluginDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("prepare plugin module: %v\n%s", err, output)
	}
	for _, name := range []string{"tidb/go.mod", "plugin/audit/go.mod", "plugin/audit/go.sum"} {
		contents, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		files[name] = string(contents)
	}
	for _, tc := range []struct {
		name      string
		nextGen   bool
		workspace bool
	}{
		{name: "classic"},
		{name: "nextgen", nextGen: true},
		{name: "existing workspace", workspace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.workspace {
				workFile := filepath.Join(root, "go.work")
				t.Setenv("GOWORK", workFile)
				cmd := exec.Command("go", "work", "init", filepath.Join(root, "tidb"), pluginDir)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("prepare workspace: %v\n%s", err, output)
				}
				original, err := os.ReadFile(workFile)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					contents, err := os.ReadFile(workFile)
					if err != nil {
						t.Fatal(err)
					}
					if string(contents) != string(original) {
						t.Fatal("plugin build modified the existing workspace")
					}
				})
			}
			outputFile := filepath.Join(root, "audit.so")
			if err := buildPlugin(context.Background(), pluginDir, outputFile, "", tc.nextGen); err != nil {
				t.Fatal(err)
			}
			tags := "codes"
			if tc.nextGen {
				tags += ",nextgen"
			}
			cmd := exec.Command("go", "run", "-tags="+tags, "./loader", outputFile)
			cmd.Dir = filepath.Join(root, "tidb")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("load plugin: %v\n%s", err, output)
			} else if string(output) != "replacement" {
				t.Fatalf("unexpected plugin value: %s", output)
			}
		})
	}
	for _, name := range []string{"tidb/go.mod", "plugin/audit/go.mod", "plugin/audit/go.sum"} {
		contents, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(contents) != files[name] {
			t.Fatalf("plugin build modified %s", name)
		}
	}
}
