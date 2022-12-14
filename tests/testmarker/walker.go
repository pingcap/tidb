// Copyright 2022 PingCAP, Inc.
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
	_ "embed"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"log"
	"path/filepath"
	"sort"
	"strings"

	makerpkg "github.com/pingcap/tidb/tests/testmarker/pkg"
	"gopkg.in/yaml.v2"
)

// Config describes the data structure of the mapping spec
type Config struct {
	OnlyFiles    []string `yaml:"only_files"`
	ExcludeFiles []string `yaml:"exclude_files"`

	// onlyFiles    []*regexp.Regexp `yaml:"-"`
	// excludeFiles []*regexp.Regexp `yaml:"-"`
}

func main() {
	fset := token.NewFileSet()
	var fms []*makerpkg.MarkInfo
	// run at the root of the tidb repo
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			d, err := parser.ParseDir(fset, path, nil, parser.ParseComments)
			if err != nil {
				log.Fatal(err)
			}
			for _, f := range d {
				for n, f := range f.Files {
					if strings.HasSuffix(n, "_test.go") {
						fms = append(fms, makerpkg.WalkTestFile(f, n)...)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	showMarkInfo(fms)
}

func showMarkInfo(fms []*makerpkg.MarkInfo) {
	sort.Slice(fms, func(i, j int) bool {
		if fms[i].File != fms[j].File {
			return fms[i].File < fms[j].File
		}
		return fms[i].TestName < fms[j].TestName
	})

	out, err := yaml.Marshal(fms)
	if err != nil {
		log.Fatalf("failed to marshal mark infos: %v", err)
	}
	fmt.Println(string(out))
}
