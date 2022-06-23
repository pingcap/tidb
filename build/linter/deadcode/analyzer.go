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

package deadcode

import (
	"go/token"
	"go/types"

	deadcodeAPI "github.com/golangci/go-misc/deadcode"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/loader"
)

// Name is the name of the analyzer.
const Name = "deadcode"

// Analyzer is the analyzer struct of deadcode.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Finds unused code",
	Run:  run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	var createdPkgs []*loader.PackageInfo
	createdPkgs = append(createdPkgs, util.MakeFakeLoaderPackageInfo(pass))
	allPkgs := map[*types.Package]*loader.PackageInfo{}
	for _, pkg := range createdPkgs {
		pkg := pkg
		allPkgs[pkg.Pkg] = pkg
	}
	prog := &loader.Program{
		Fset:        pass.Fset,
		Imported:    nil,         // not used without .Created in any linter
		Created:     createdPkgs, // all initial packages
		AllPackages: allPkgs,     // all initial packages and their depndencies
	}
	issues, err := deadcodeAPI.Run(prog)
	if err != nil {
		return nil, err
	}
	for _, i := range issues {
		pass.Reportf(token.Pos(i.Pos.Offset), "[%s] %s is unused", Name, util.FormatCode(i.UnusedIdentName))
	}
	return nil, nil
}
