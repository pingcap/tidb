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

package toomanyfiles

import (
	"path/filepath"

	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of toomanyfiles.
var Analyzer = &analysis.Analyzer{
	Name: "toomanyfiles",
	Doc:  "too many files in the package",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	if len(pass.Files) > 10 {
		pos := pass.Fset.PositionFor(pass.Files[0].Pos(), false)
		pass.Reportf(pass.Files[0].Pos(), "%s: Too many files in one package %s", pass.Pkg.Name(), filepath.Dir(pos.Filename))
	}
	return nil, nil
}
