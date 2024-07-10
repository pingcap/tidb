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

package misspell

import (
	"fmt"
	"go/token"

	"github.com/golangci/misspell"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
)

// Name is the name of the analyzer.
const Name = "misspell"

// Analyzer is the analyzer struct of misspell.
var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Checks the spelling error in code",
	Run:  run,
}

func init() {
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}

// Misspell is the config of misspell.
type Misspell struct {
	Locale      string
	IgnoreWords []string `mapstructure:"ignore-words"`
}

func run(pass *analysis.Pass) (any, error) {
	r := misspell.Replacer{
		Replacements: misspell.DictMain,
	}

	// Figure out regional variations
	settings := &Misspell{}

	if len(settings.IgnoreWords) != 0 {
		r.RemoveRule(settings.IgnoreWords)
	}

	r.Compile()
	files := make([]string, 0, len(pass.Files))
	for _, file := range pass.Files {
		pos := pass.Fset.PositionFor(file.Pos(), false)
		files = append(files, pos.Filename)
	}
	for _, f := range files {
		err := runOnFile(f, &r, pass)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func runOnFile(fileName string, r *misspell.Replacer, pass *analysis.Pass) error {
	fileContent, tf, err := util.ReadFile(pass.Fset, fileName)
	if err != nil {
		return fmt.Errorf("can't get file %s contents: %s", fileName, err)
	}

	// use r.Replace, not r.ReplaceGo because r.ReplaceGo doesn't find
	// issues inside strings: it searches only inside comments. r.Replace
	// searches all words: it treats input as a plain text. A standalone misspell
	// tool uses r.Replace by default.
	_, diffs := r.Replace(string(fileContent))
	for _, diff := range diffs {
		text := fmt.Sprintf("[%s] `%s` is a misspelling of `%s`", Name, diff.Original, diff.Corrected)
		pass.Reportf(token.Pos(tf.Base()+util.FindOffset(string(fileContent), diff.Line, diff.Column)), text)
	}
	return nil
}
