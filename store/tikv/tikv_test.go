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

package tikv

import (
	"go/build"
	"os"
	"path"
	"reflect"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser"
)

// When with-tikv flag is true, there is only one storage, so the test suite have to run one by one.
type oneByOneSuite struct {
}

func (s oneByOneSuite) SetUpSuite(c *C) {
	if *withTiKV {
		withTiKVGlobalLock.Lock()
	}
}

func (s oneByOneSuite) TearDownSuite(c *C) {
	if *withTiKV {
		withTiKVGlobalLock.Unlock()
	}
}

type testTiKVSuite struct {
	oneByOneSuite
}

var _ = Suite(&testTiKVSuite{})

func getImportedPackages(c *C, srcDir string, pkgName string, pkgs *map[string][]string) {
	if pkgName == "C" {
		return
	}
	if _, exists := (*pkgs)[pkgName]; exists {
		return
	}
	if strings.HasPrefix(pkgName, "golang_org") {
		pkgName = path.Join("vendor", pkgName)
	}
	pkg, err := build.Import(pkgName, srcDir, 0)
	c.Assert(err, IsNil)
	(*pkgs)[pkgName] = pkg.Imports
	for _, name := range (*pkgs)[pkgName] {
		getImportedPackages(c, srcDir, name, pkgs)
	}
}

// TestParserNoDep tests whether this package does not depend on tidb/parser.
func (s *testTiKVSuite) TestParserNoDep(c *C) {
	srcDir, err := os.Getwd()
	c.Assert(err, IsNil)

	pkgs := make(map[string][]string)
	currentPkgName := reflect.TypeOf(testTiKVSuite{}).PkgPath()
	getImportedPackages(c, srcDir, currentPkgName, &pkgs)

	parse := parser.New()
	parserPkgName := reflect.TypeOf(*parse).PkgPath()

	for pkgName, imports := range pkgs {
		for _, importName := range imports {
			c.Assert(importName == parserPkgName, IsFalse,
				Commentf("`%s` is imported from `%s`, which is a child dependency of `%s`", parserPkgName, pkgName, currentPkgName))
		}
	}
}
