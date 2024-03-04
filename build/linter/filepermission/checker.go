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

package filepermission

import (
	"os"

	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Name is the name of the analyzer.
const Name = "filepermission"

// Analyzer is the analyzer struct of prealloc.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Go files should not have execution permission",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, f := range pass.Files {
		fn := pass.Fset.PositionFor(f.Pos(), false).Filename
		if fn != "" {
			stat, err := os.Stat(fn)
			if err != nil {
				return nil, err
			}
			if stat.Mode()&0111 != 0 {
				pass.Reportf(f.Pos(), "[%s] source code file should not have execute permission %s", Name, stat.Mode())
			}
		}
	}

	return nil, nil
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
