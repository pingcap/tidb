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

package forbidigo

import (
	"fmt"
	"go/ast"
	"strings"

	"github.com/ashanbrown/forbidigo/v2/forbidigo"
	"github.com/golangci/golangci-lint/v2/pkg/fsutils"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

var lc = fsutils.NewLineCache(fsutils.NewFileCache())

// Analyzer is the analyzer struct of gofmt.
var Analyzer = &analysis.Analyzer{
	Name: "forbidigo",
	Doc:  "forbid identifiers",
	Run:  run,
}

var (
	patterns = []string{`{
		p: "sessionctx.Context.GetSessionVars",
		msg: "Please check if the usage of GetSessionVars is appropriate,\n
		      you can add //nolint:forbidigo to ignore this check if necessary."
	}`}
)

func run(pass *analysis.Pass) (any, error) {
	linter, err := forbidigo.NewLinter(patterns,
		forbidigo.OptionIgnorePermitDirectives(true),
		forbidigo.OptionExcludeGodocExamples(false),
		forbidigo.OptionAnalyzeTypes(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to configure linter: %w", err)
	}
	nodes := make([]ast.Node, 0, len(pass.Files))
	for _, f := range pass.Files {
		nodes = append(nodes, f)
	}
	config := forbidigo.RunConfig{Fset: pass.Fset, DebugLog: nil, TypesInfo: pass.TypesInfo}
	issues, err := linter.RunWithConfig(config, nodes...)
	if err != nil {
		return nil, err
	}
	reportIssues(pass, issues)
	return nil, nil
}

func reportIssues(pass *analysis.Pass, issues []forbidigo.Issue) {
	// Because go doesn't support negative lookahead assertion,
	// that is, filter out some vaild usages like GetSessionVars().Stmt by regex,
	// we add some allowed usages here and do the filtering by opening the file.
	whiteLists := []string{
		"SQLMode",               // used for model.Job
		"CDCWriteSource",        // used for model.Job
		"StmtCtx",               // GetSessionVars().StmtCtx
		"TimeZone",              // GetSessionVars().TimeZone
		"Location",              // GetSessionVars().Location()
		"GetSplitRegionTimeout", // GetSessionVars().GetSplitRegionTimeout()
		"SetInTxn",              // GetSessionVars().SetInTxn(true)
		"BuildParserConfig",     // GetSessionVars().BuildParserConfig()
		"DiskFull",
		"DefaultCollationForUTF8MB4",
		"RowEncoder",
		"SetStatusFlag",
	}

	for _, i := range issues {
		skip := false
		// Copied from golanglint-ci
		if s, err := lc.GetLine(i.Position().Filename, i.Position().Line); err == nil {
			for _, whiteList := range whiteLists {
				if strings.Contains(s, whiteList) {
					skip = true
					break
				}
			}
		}

		if !skip {
			pass.Report(analysis.Diagnostic{
				Pos:      i.Pos(),
				Message:  i.Details(),
				Category: "restriction",
			})
		}
	}
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
}
