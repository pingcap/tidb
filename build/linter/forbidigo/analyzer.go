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
	"errors"
	"fmt"
	"go/ast"
	"strings"

	"github.com/ashanbrown/forbidigo/forbidigo"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of gofmt.
var Analyzer = &analysis.Analyzer{
	Name: "forbidigo",
	Doc:  "forbid identifiers",
	Run:  run,
}

type listVar struct {
	values *[]string
}

func (v *listVar) Set(value string) error {
	*v.values = append(*v.values, value)
	if value == "" {
		return errors.New("value cannot be empty")
	}
	return nil
}

func (v *listVar) String() string {
	return ""
}

var (
	patterns           = []string{`sessionctx.Context.GetSessionVars`}
	includeExamples    bool
	usePermitDirective bool
	analyzeTypes       bool
)

func init() {
	Analyzer.Flags.Var(&listVar{values: &patterns}, "p", "pattern")
	Analyzer.Flags.BoolVar(&includeExamples, "examples", false, "check godoc examples")
	Analyzer.Flags.BoolVar(&usePermitDirective, "permit", true, `when set, lines with "//permit" directives will be ignored`)
	Analyzer.Flags.BoolVar(&analyzeTypes, "analyze_types", true, `when set, expressions get expanded instead of matching the literal source code`)
}

func run(pass *analysis.Pass) (any, error) {
	linter, err := forbidigo.NewLinter(patterns,
		forbidigo.OptionIgnorePermitDirectives(!usePermitDirective),
		forbidigo.OptionExcludeGodocExamples(!includeExamples),
		forbidigo.OptionAnalyzeTypes(analyzeTypes),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to configure linter: %w", err)
	}
	nodes := make([]ast.Node, 0, len(pass.Files))
	for _, f := range pass.Files {
		nodes = append(nodes, f)
	}
	config := forbidigo.RunConfig{Fset: pass.Fset, DebugLog: nil}
	if analyzeTypes {
		config.TypesInfo = pass.TypesInfo
	}
	issues, err := linter.RunWithConfig(config, nodes...)
	if err != nil {
		return nil, err
	}
	reportIssues(pass, issues)
	return nil, nil
}

func reportIssues(pass *analysis.Pass, issues []forbidigo.Issue) {
	// Because go doesn't support negative lookahead assertion,
	// so we add some allowed usage here.
	whiteLists := []string{
		"StmtCtx", "TimeZone", "Location", "SQLMode", "GetSplitRegionTimeout",
		"CDCWriteSource", "SetInTxn", "DiskFull", "DefaultCollationForUTF8MB4",
		"BuildParserConfig",
	}

	for _, i := range issues {
		detail := i.Details()
		skip := false
		for _, whiteList := range whiteLists {
			if strings.Contains(detail, whiteList) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		diag := analysis.Diagnostic{
			Pos:      i.Pos(),
			Message:  detail,
			Category: "restriction",
		}
		pass.Report(diag)
	}
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
