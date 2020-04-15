// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/BurntSushi/toml"
)

var (
	pkgDir string
	outDir string
)

const codeTemplate = `
package main

import (
	"github.com/pingcap/tidb/v4/plugin"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
)

func PluginManifest() *plugin.Manifest {
	return plugin.ExportManifest(&plugin.{{.kind}}Manifest{
		Manifest: plugin.Manifest{
			Kind:           plugin.{{.kind}},
			Name:           "{{.name}}",
			Description:    "{{.description}}",
			Version:        {{.version}},
			RequireVersion: map[string]uint16{},
			License:        "{{.license}}",
			BuildTime:      "{{.buildTime}}",
			SysVars: map[string]*variable.SysVar{
			    {{range .sysVars}}
				"{{.name}}": {
					Scope: variable.Scope{{.scope}},
					Name:  "{{.name}}",
					Value: "{{.value}}",
				},
				{{end}}
			},
			{{if .validate }}
				Validate:   {{.validate}},
			{{end}}
			{{if .onInit }}
				OnInit:     {{.onInit}},
			{{end}}
			{{if .onShutdown }}
				OnShutdown: {{.onShutdown}},
			{{end}}
			{{if .onFlush }}
				OnFlush:    {{.onFlush}},
			{{end}}
		},
		{{range .export}}
		{{.extPoint}}: {{.impl}},
		{{end}}
	})
}
`

func init() {
	flag.StringVar(&pkgDir, "pkg-dir", "", "plugin package folder path")
	flag.StringVar(&outDir, "out-dir", "", "plugin packaged folder path")
	flag.Usage = usage
}

func usage() {
	log.Printf("Usage: %s --pkg-dir [plugin source pkg folder] --out-dir [plugin packaged folder path]\n", filepath.Base(os.Args[0]))
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.Parse()
	if pkgDir == "" || outDir == "" {
		flag.Usage()
	}
	pkgDir, err := filepath.Abs(pkgDir)
	if err != nil {
		log.Printf("unable to resolve absolute representation of package path , %+v\n", err)
		flag.Usage()
	}
	outDir, err := filepath.Abs(outDir)
	if err != nil {
		log.Printf("unable to resolve absolute representation of output path , %+v\n", err)
		flag.Usage()
	}

	var manifest map[string]interface{}
	_, err = toml.DecodeFile(filepath.Join(pkgDir, "manifest.toml"), &manifest)
	if err != nil {
		log.Printf("read pkg %s's manifest failure, %+v\n", pkgDir, err)
		os.Exit(1)
	}
	manifest["buildTime"] = time.Now().String()

	pluginName := manifest["name"].(string)
	if strings.Contains(pluginName, "-") {
		log.Printf("plugin name should not contain '-'\n")
		os.Exit(1)
	}
	if pluginName != filepath.Base(pkgDir) {
		log.Printf("plugin package must be same with plugin name in manifest file\n")
		os.Exit(1)
	}

	version := manifest["version"].(string)
	tmpl, err := template.New("gen-plugin").Parse(codeTemplate)
	if err != nil {
		log.Printf("generate code failure during parse template, %+v\n", err)
		os.Exit(1)
	}

	genFileName := filepath.Join(pkgDir, filepath.Base(pkgDir)+".gen.go")
	genFile, err := os.OpenFile(genFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Printf("generate code failure during prepare output file, %+v\n", err)
		os.Exit(1)
	}
	defer func() {
		err1 := os.Remove(genFileName)
		if err1 != nil {
			log.Printf("remove tmp file %s failure, please clean up manually at %v", genFileName, err1)
		}
	}()

	err = tmpl.Execute(genFile, manifest)
	if err != nil {
		log.Printf("generate code failure during generating code, %+v\n", err)
		os.Exit(1)
	}

	outputFile := filepath.Join(outDir, pluginName+"-"+version+".so")
	ctx := context.Background()
	buildCmd := exec.CommandContext(ctx, "go", "build",
		"-buildmode=plugin",
		"-o", outputFile, pkgDir)
	buildCmd.Dir = pkgDir
	buildCmd.Stderr = os.Stderr
	buildCmd.Stdout = os.Stdout
	buildCmd.Env = append(os.Environ(), "GO111MODULE=on")
	err = buildCmd.Run()
	if err != nil {
		log.Printf("compile plugin source code failure, %+v\n", err)
		os.Exit(1)
	}
	fmt.Printf(`Package "%s" as plugin "%s" success.`+"\nManifest:\n", pkgDir, outputFile)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent(" ", "\t")
	err = encoder.Encode(manifest)
	if err != nil {
		log.Printf("print manifest detail failure, err: %v", err)
	}
}
