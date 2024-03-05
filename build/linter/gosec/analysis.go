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

package gosec

import (
	"fmt"
	"go/token"
	"go/types"
	"io"
	"log"
	"strconv"

	"github.com/golangci/golangci-lint/pkg/result"
	"github.com/golangci/gosec"
	"github.com/golangci/gosec/rules"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/loader"
)

// Name is the name of the analyzer.
const Name = "gosec"

// Analyzer is the analyzer struct of gosec.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Inspects source code for security problems",
	Run:  run,
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}

func run(pass *analysis.Pass) (any, error) {
	gasConfig := gosec.NewConfig()
	enabledRules := rules.Generate(func(id string) bool {
		if id == "G104" || id == "G103" || id == "G101" || id == "G201" {
			return true
		}
		return false
	})
	logger := log.New(io.Discard, "", 0)
	analyzer := gosec.NewAnalyzer(gasConfig, logger)
	analyzer.LoadRules(enabledRules.Builders())

	var createdPkgs []*loader.PackageInfo
	createdPkgs = append(createdPkgs, util.MakeFakeLoaderPackageInfo(pass))
	allPkgs := make(map[*types.Package]*loader.PackageInfo)
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

	analyzer.ProcessProgram(prog)
	issues, _ := analyzer.Report()
	if len(issues) == 0 {
		return nil, nil
	}
	severity, confidence := gosec.Low, gosec.Low
	issues = filterIssues(issues, severity, confidence)
	for _, i := range issues {
		fileContent, tf, err := util.ReadFile(pass.Fset, i.File)
		if err != nil {
			panic(err)
		}
		text := fmt.Sprintf("[%s] %s: %s", Name, i.RuleID, i.What) // TODO: use severity and confidence
		var r *result.Range
		line, err := strconv.Atoi(i.Line)
		if err != nil {
			r = &result.Range{}
			if n, rerr := fmt.Sscanf(i.Line, "%d-%d", &r.From, &r.To); rerr != nil || n != 2 {
				continue
			}
			line = r.From
		}

		pass.Reportf(token.Pos(tf.Base()+util.FindOffset(string(fileContent), line, 1)), text)
	}

	return nil, nil
}

func filterIssues(issues []*gosec.Issue, severity, confidence gosec.Score) []*gosec.Issue {
	res := make([]*gosec.Issue, 0)
	for _, issue := range issues {
		if issue.Severity >= severity && issue.Confidence >= confidence {
			res = append(res, issue)
		}
	}
	return res
}
