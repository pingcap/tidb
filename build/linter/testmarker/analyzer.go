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
				_, exist := tryGetFromRecords(marker.TestName)
				if !exist && len(marker.Features) == 0 && len(marker.Issues) == 0 {
					pass.Report(analysis.Diagnostic{
						Pos: marker.Pos,
						Message: `add a marker at the start of incremental test, please.
  1) Marking the test is for a feature:
    marker.As(t, marker.Feature, "FD-$ID", "the description...")
  2) Or mark it for an issue:
    marker.As(t, marker.Issue, issue-id)`,
					})
				}
			}
		}
	}
	return nil, nil
}

// tryGetFromRecords tries to get the markInfo from the records according to the test name, if not found, return nil
// here we assume the test name is usually unique, and ignoring the file name can reduce the extra mark burden of migration test cases
func tryGetFromRecords(testName string) (*markutil.MarkInfo, bool) {
	for _, info := range markInfo {
		if info.TestName == testName {
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
