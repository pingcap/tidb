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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
	"golang.org/x/mod/modfile"
)

var (
	pkgDir      string
	outDir      string
	pgoFile     string
	tidbRootDir string
	nextGen     bool
)

const codeTemplate = `
package main

import (
	"github.com/pingcap/tidb/pkg/plugin"
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
	flag.StringVar(&pgoFile, "pgo-file", "", "go profile-guided optimization(pgo) file path")
	flag.StringVar(&tidbRootDir, "tidb-dir", "", "TiDB source root used to inherit go.mod replace directives")
	flag.BoolVar(&nextGen, "next-gen", false, "whether to build plugin with next-gen features")
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
	if pgoFile != "" {
		pgoFile, err = filepath.Abs(pgoFile)
		if err != nil {
			log.Printf("unable to resolve absolute representation of pgo-file path , %+v\n", err)
			flag.Usage()
		}
	}

	var manifest map[string]any
	_, err = toml.DecodeFile(filepath.Join(pkgDir, "manifest.toml"), &manifest)
	if err != nil {
		log.Printf("read pkg %s's manifest failure, %+v\n", pkgDir, err)
		os.Exit(1)
	}
	manifest["buildTime"] = time.Now().String()

	pluginName := manifest["name"].(string)
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
	genFile, err := os.OpenFile(genFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700) // #nosec G302
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

	modFile, cleanupModFile, err := preparePluginModFileForBuild(pkgDir)
	if err != nil {
		log.Printf("prepare plugin module file failure, %+v\n", err)
		os.Exit(1)
	}
	defer cleanupModFile()

	outputFile := filepath.Join(outDir, pluginName+"-"+version+".so")
	ctx := context.Background()
	flags := make([]string, 0, 4)
	flags = append(flags, "build")
	if modFile != "" {
		flags = append(flags, "-modfile="+modFile)
	}
	if pgoFile != "" {
		flags = append(flags, "-pgo="+pgoFile)
	}

	buildTags := []string{"codes"}
	if nextGen {
		buildTags = append(buildTags, "nextgen")
	}

	flags = append(flags,
		"-tags="+strings.Join(buildTags, ","),
		"-buildmode=plugin",
		"-o", outputFile, pkgDir)
	buildCmd := exec.CommandContext(ctx, "go", flags...)
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

func preparePluginModFileForBuild(pkgDir string) (string, func(), error) {
	tidbDir, err := findTiDBRoot(pkgDir)
	if err != nil {
		log.Printf("skip inheriting TiDB go.mod replace directives: %v", err)
		return "", func() {}, nil
	}
	return preparePluginModFile(pkgDir, tidbDir)
}

func preparePluginModFile(pkgDir, tidbDir string) (string, func(), error) {
	pluginModPath := filepath.Join(pkgDir, "go.mod")
	pluginModBytes, err := os.ReadFile(pluginModPath)
	if err != nil {
		return "", func() {}, err
	}
	pluginMod, err := modfile.Parse(pluginModPath, pluginModBytes, nil)
	if err != nil {
		return "", func() {}, err
	}

	tidbModPath := filepath.Join(tidbDir, "go.mod")
	tidbModBytes, err := os.ReadFile(tidbModPath)
	if err != nil {
		return "", func() {}, err
	}
	tidbMod, err := modfile.Parse(tidbModPath, tidbModBytes, nil)
	if err != nil {
		return "", func() {}, err
	}

	addedReplace := false
	for _, replace := range tidbMod.Replace {
		if replace.Old.Path == "github.com/pingcap/tidb" || hasReplace(pluginMod, replace.Old.Path, replace.Old.Version) {
			continue
		}
		newPath := replace.New.Path
		if isLocalReplacePath(newPath) && !filepath.IsAbs(newPath) {
			newPath = filepath.Join(tidbDir, newPath)
		}
		if err := pluginMod.AddReplace(replace.Old.Path, replace.Old.Version, newPath, replace.New.Version); err != nil {
			return "", func() {}, err
		}
		addedReplace = true
	}
	if !addedReplace {
		return "", func() {}, nil
	}

	pluginModBytes, err = pluginMod.Format()
	if err != nil {
		return "", func() {}, err
	}
	modFilePath := filepath.Join(pkgDir, ".pluginpkg.mod")
	if err := os.WriteFile(modFilePath, pluginModBytes, 0o644); err != nil {
		return "", func() {}, err
	}
	cleanup := func() {
		if err := os.Remove(modFilePath); err != nil && !os.IsNotExist(err) {
			log.Printf("remove tmp module file %s failure, please clean up manually at %v", modFilePath, err)
		}
		sumFilePath := strings.TrimSuffix(modFilePath, ".mod") + ".sum"
		if err := os.Remove(sumFilePath); err != nil && !os.IsNotExist(err) {
			log.Printf("remove tmp module sum file %s failure, please clean up manually at %v", sumFilePath, err)
		}
	}
	return modFilePath, cleanup, nil
}

func hasReplace(mod *modfile.File, oldPath, oldVersion string) bool {
	for _, replace := range mod.Replace {
		if replace.Old.Path == oldPath && replace.Old.Version == oldVersion {
			return true
		}
	}
	return false
}

func findTiDBRoot(pkgDir string) (string, error) {
	if tidbRootDir != "" {
		return cleanTiDBRoot(tidbRootDir)
	}

	candidates := make([]string, 0, 8)
	if executablePath, err := os.Executable(); err == nil {
		dir := filepath.Dir(executablePath)
		for i := 0; i < 6; i++ {
			candidates = append(candidates, dir)
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	candidates = append(candidates,
		filepath.Join(pkgDir, "..", "..", "tidb"),
		filepath.Join(pkgDir, "..", "tidb"),
	)

	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidate, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		if isTiDBRoot(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("cannot find TiDB source root")
}

func cleanTiDBRoot(dir string) (string, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if !isTiDBRoot(absDir) {
		return "", fmt.Errorf("%s is not a TiDB source root", absDir)
	}
	return absDir, nil
}

func isTiDBRoot(dir string) bool {
	modPath := filepath.Join(dir, "go.mod")
	modBytes, err := os.ReadFile(modPath)
	if err != nil {
		return false
	}
	mod, err := modfile.Parse(modPath, modBytes, nil)
	if err != nil || mod.Module == nil {
		return false
	}
	return mod.Module.Mod.Path == "github.com/pingcap/tidb"
}

func isLocalReplacePath(path string) bool {
	return filepath.IsAbs(path) || strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../")
}
