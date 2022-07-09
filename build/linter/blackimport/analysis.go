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

package blackimport

import (
	"strconv"

	"golang.org/x/tools/go/analysis"
)

// Analyzer
var Analyzer = &analysis.Analyzer{
	Name: "blackimport",
	Doc:  "avoid using banned libraries",
	Run:  run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	for _, f := range pass.Files {
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return nil, err
			}
			if path == "github.com/pingcap/check" {
				pass.Reportf(imp.Pos(), "It has been replaced by github.com/stretchr/testify. https://github.com/pingcap/tidb/issues/26022")
			}
		}
	}
	return nil, nil
}
