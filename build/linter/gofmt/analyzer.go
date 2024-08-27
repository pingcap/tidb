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

package gofmt

import (
	"fmt"
	"strings"

	"github.com/golangci/gofmt/gofmt"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of gofmt.
var Analyzer = &analysis.Analyzer{
	Name: "gofmt",
	Doc: "gofmt checks whether code was gofmt-ed" +
		"this tool runs with -s option to check for code simplification",
	Run: run,
}

var needSimplify bool

func init() {
	Analyzer.Flags.BoolVar(&needSimplify, "need-simplify", true, "run gofmt with -s for code simplification")
}

func run(pass *analysis.Pass) (any, error) {
	fileNames := make([]string, 0, 10)
	for _, f := range pass.Files {
		pos := pass.Fset.PositionFor(f.Pos(), false)
		if pos.Filename != "" && !strings.HasSuffix(pos.Filename, "failpoint_binding__.go") {
			fileNames = append(fileNames, pos.Filename)
		}
	}
	rules := []gofmt.RewriteRule{{
		Pattern:     "interface{}",
		Replacement: "any",
	}}
	for _, f := range fileNames {
		diff, err := gofmt.RunRewrite(f, needSimplify, rules)
		if err != nil {
			return nil, fmt.Errorf("could not run gofmt: %w (%s)", err, f)
		}

		if diff == nil {
			continue
		}

		pass.Report(analysis.Diagnostic{
			Pos:     1,
			Message: fmt.Sprintf("\n%s", diff),
		})
	}

	return nil, nil
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
