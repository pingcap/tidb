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

package unconvert

import (
	"fmt"
	"go/token"
	"go/types"

	unconvertAPI "github.com/golangci/unconvert"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/loader"
)

// Name is the name of the analyzer.
const Name = "unconvert"

// Analyzer is the analyzer struct of unconvert.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Remove unnecessary type conversions",
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
	positions := unconvertAPI.Run(prog)
	if len(positions) == 0 {
		return nil, nil
	}

	for _, pos := range positions {
		pass.Reportf(token.Pos(pos.Offset), fmt.Sprintf("[%s] Unnecessary conversion", Name))
	}
	return nil, nil
}
