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

package gci

import (
	"fmt"
	"sync"

	"github.com/daixiang0/gci/pkg/config"
	"github.com/daixiang0/gci/pkg/gci"
	"golang.org/x/tools/go/analysis"
)

// Analyzer is the analyzer struct of gci.
var Analyzer = &analysis.Analyzer{
	Name: "gci",
	Doc:  "Gci controls golang package import order and makes it always deterministic.",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	fileNames := make([]string, 0, len(pass.Files))
	for _, f := range pass.Files {
		pos := pass.Fset.PositionFor(f.Pos(), false)
		fileNames = append(fileNames, pos.Filename)
	}
	rawCfg := config.YamlConfig{
		Cfg: config.BoolConfig{
			NoInlineComments: false,
			NoPrefixComments: false,
			Debug:            false,
			SkipGenerated:    true,
		},
	}
	cfg, _ := rawCfg.Parse()
	var diffs []string
	var lock sync.Mutex
	err := gci.DiffFormattedFilesToArray(fileNames, *cfg, &diffs, &lock)
	if err != nil {
		return nil, err
	}

	for _, diff := range diffs {
		if diff == "" {
			continue
		}

		pass.Report(analysis.Diagnostic{
			Pos:     1,
			Message: fmt.Sprintf("\n%s", diff),
		})
	}

	return nil, nil
}
