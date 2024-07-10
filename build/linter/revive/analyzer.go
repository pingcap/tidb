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

package revive

import (
	"encoding/json"
	"fmt"
	"go/token"
	"os"

	"github.com/mgechev/revive/config"
	"github.com/mgechev/revive/lint"
	"github.com/mgechev/revive/rule"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/build/linter/util"
	"go.uber.org/zap"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of gofmt.
var Analyzer = &analysis.Analyzer{
	Name: "revive",
	Doc:  "~6x faster, stricter, configurable, extensible, and beautiful drop-in replacement for golint",
	Run:  run,
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}

// jsonObject defines a JSON object of a failure
type jsonObject struct {
	Severity     lint.Severity
	lint.Failure `json:",inline"`
}

var defaultRules = []lint.Rule{
	&rule.VarDeclarationsRule{},
	//&rule.PackageCommentsRule{},
	&rule.DotImportsRule{},
	&rule.ExportedRule{},
	&rule.VarNamingRule{},
	&rule.IncrementDecrementRule{},
	//&rule.UnexportedReturnRule{},
	&rule.ContextKeysType{},
}

var allRules = append([]lint.Rule{
	//&rule.ArgumentsLimitRule{},
	//&rule.CyclomaticRule{},
	//&rule.FileHeaderRule{},
	&rule.EmptyBlockRule{},
	//&rule.ConfusingNamingRule{},
	&rule.ConfusingResultsRule{},
	//&rule.DeepExitRule{},
	&rule.UnusedParamRule{},
	//&rule.AddConstantRule{},
	//&rule.FlagParamRule{},
	&rule.UnnecessaryStmtRule{},
	//&rule.StructTagRule{},
	//&rule.ModifiesValRecRule{},
	//&rule.RedefinesBuiltinIDRule{},
	//&rule.FunctionResultsLimitRule{},
	//&rule.MaxPublicStructsRule{},
	//&rule.LineLengthLimitRule{},
	&rule.CallToGCRule{},
	//&rule.ImportShadowingRule{},
	//&rule.BareReturnRule{},
	&rule.UnusedReceiverRule{},
	//&rule.UnhandledErrorRule{},
	//&rule.CognitiveComplexityRule{},
	//&rule.EarlyReturnRule{},
	&rule.UnexportedNamingRule{},
	//&rule.FunctionLength{},
	//&rule.NestedStructs{},
	&rule.UselessBreak{},
	//&rule.BannedCharsRule{},
}, defaultRules...)

func run(pass *analysis.Pass) (any, error) {
	files := make([]string, 0, len(pass.Files))
	for _, file := range pass.Files {
		files = append(files, pass.Fset.PositionFor(file.Pos(), false).Filename)
	}
	packages := [][]string{files}

	revive := lint.New(os.ReadFile, 1024)
	conf := lint.Config{
		IgnoreGeneratedHeader: false,
		Confidence:            0.8,
		Severity:              "error",
		ErrorCode:             -1,
		WarningCode:           -1,
		Rules:                 map[string]lint.RuleConfig{},
	}
	for _, r := range allRules {
		conf.Rules[r.Name()] = lint.RuleConfig{}
	}
	conf.Rules["defer"] = lint.RuleConfig{
		Arguments: []any{[]any{"loop", "method-call", "immediate-recover", "return"}},
	}
	lintingRules, err := config.GetLintingRules(&conf, []lint.Rule{})
	if err != nil {
		return nil, err
	}

	failures, err := revive.Lint(packages, lintingRules, conf)
	if err != nil {
		return nil, err
	}

	formatChan := make(chan lint.Failure)
	exitChan := make(chan bool)

	formatter, err := config.GetFormatter("json")
	if err != nil {
		return nil, err
	}
	var output string
	go func() {
		output, err = formatter.Format(formatChan, conf)
		if err != nil {
			log.Error("Format error", zap.Error(err))
		}
		exitChan <- true
	}()

	for f := range failures {
		if f.Confidence < conf.Confidence {
			continue
		}

		formatChan <- f
	}

	close(formatChan)
	<-exitChan

	var results []jsonObject
	err = json.Unmarshal([]byte(output), &results)
	if err != nil {
		return nil, err
	}
	for i := range results {
		res := &results[i]
		text := fmt.Sprintf("%s: %s", res.RuleName, res.Failure.Failure)
		fileContent, tf, err := util.ReadFile(pass.Fset, res.Position.Start.Filename)
		if err != nil {
			panic(err)
		}
		pass.Reportf(token.Pos(tf.Base()+util.FindOffset(string(fileContent), res.Position.Start.Line, res.Position.Start.Column)), text)
	}
	return nil, nil
}
