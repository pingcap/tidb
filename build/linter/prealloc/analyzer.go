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

package prealloc

import (
	"go/ast"

	"github.com/golangci/prealloc"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Settings is the settings for preallocation.
type Settings struct {
	Simple     bool
	RangeLoops bool `mapstructure:"range-loops"`
	ForLoops   bool `mapstructure:"range-loops"`
}

// Name is the name of the analyzer.
const Name = "prealloc"

// Analyzer is the analyzer struct of prealloc.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Finds slice declarations that could potentially be preallocated",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	s := &Settings{
		Simple:     true,
		RangeLoops: true,
		ForLoops:   false,
	}
	for _, f := range pass.Files {
		hints := prealloc.Check([]*ast.File{f}, s.Simple, s.RangeLoops, s.ForLoops)
		for _, hint := range hints {
			pass.Reportf(hint.Pos, "[%s] Consider preallocating %s", Name, util.FormatCode(hint.DeclaredSliceName))
		}
	}

	return nil, nil
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}
