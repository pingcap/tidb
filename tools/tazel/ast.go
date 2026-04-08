// Copyright 2023 PingCAP, Inc.
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
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var testMap map[string]uint32

func initCount() {
	testMap = make(map[string]uint32)
}

func addTestMap(path string) {
	if _, ok := testMap[path]; !ok {
		testMap[path] = 0
	}
	testMap[path]++
}

func walk() {
	err := filepath.Walk(".", func(path string, d fs.FileInfo, _ error) error {
		if d.IsDir() || !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		return scan(path)
	})
	if err != nil {
		log.Fatal("fail to walk", zap.Error(err))
	}
}

func scan(path string) error {
	fset := token.NewFileSet()
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
	if err != nil {
		return err
	}
	for _, n := range f.Decls {
		funcDecl, ok := n.(*ast.FuncDecl)
		if ok {
			if strings.HasPrefix(funcDecl.Name.Name, "Test") && funcDecl.Recv == nil &&
				funcDecl.Name.Name != "TestMain" {
				addTestMap(filepath.Dir(path))
			}
		}
	}
	return nil
}
