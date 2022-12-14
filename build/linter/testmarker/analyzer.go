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

package testmarker

import (
	_ "embed"
	"reflect"
	"strings"

	markutil "github.com/pingcap/tidb/tests/testmarker/pkg"
	"golang.org/x/tools/go/analysis"
	"gopkg.in/yaml.v2"
)

var (
	//go:embed data.yaml
	data     []byte
	markInfo []*markutil.MarkInfo
)

// Name is the name of the analyzer.
const Name = "testmarker"

// Analyzer is the analyzer struct of unconvert.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Checks marker on testing files",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, f := range pass.Files {
		pos := pass.Fset.PositionFor(f.Pos(), false)
		if strings.HasSuffix(pos.Filename, "_test.go") {
			markers := markutil.WalkTestFile(f, pos.Filename)

			for _, marker := range markers {
				record, exist := tryGetFromRecords(marker.File, marker.TestName)
				if !exist {
					pass.Report(analysis.Diagnostic{
						Pos:     1,
						Message: "the file build/linter/testmarker/data.yaml is out of date, please run `make generate-testmarker-data` to update",
					})
				} else {
					if !reflect.DeepEqual(marker.Features, record.Features) || !reflect.DeepEqual(marker.Issues, record.Issues) {
						pass.Report(analysis.Diagnostic{
							Pos:     1,
							Message: "the file build/linter/testmarker/data.yaml is out of date, please run `make generate-testmarker-data` to update",
						})
					}
				}
			}
		}
	}
	return nil, nil
}

func tryGetFromRecords(file, testName string) (*markutil.MarkInfo, bool) {
	for _, info := range markInfo {
		// file is the full path on the bazel sandbox, we check the suffix to make sure it's the same file
		if info.TestName == testName && strings.HasSuffix(file, info.File) {
			return info, true
		}
	}
	return nil, false
}

func init() {
	err := yaml.Unmarshal(data, &markInfo)
	if err != nil {
		panic(err)
	}
}
