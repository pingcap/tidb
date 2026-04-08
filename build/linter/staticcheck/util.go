// Copyright 2021 PingCAP, Inc.
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

package staticcheck

import (
	"fmt"

	"golang.org/x/tools/go/analysis"
	"honnef.co/go/tools/analysis/lint"
	"honnef.co/go/tools/quickfix"
	"honnef.co/go/tools/simple"
	"honnef.co/go/tools/staticcheck"
	"honnef.co/go/tools/stylecheck"
	"honnef.co/go/tools/unused"
)

// Analyzers is the analyzers of staticcheck.
var Analyzers = func() map[string]*analysis.Analyzer {
	resMap := make(map[string]*analysis.Analyzer)

	for _, analyzers := range [][]*lint.Analyzer{
		quickfix.Analyzers,
		simple.Analyzers,
		staticcheck.Analyzers,
		stylecheck.Analyzers,
		{unused.Analyzer},
	} {
		for _, a := range analyzers {
			resMap[a.Analyzer.Name] = a.Analyzer
		}
	}

	return resMap
}()

// FindAnalyzerByName finds the analyzer with the given name.
func FindAnalyzerByName(name string) *analysis.Analyzer {
	if a, ok := Analyzers[name]; ok {
		return a
	}

	panic(fmt.Sprintf("not a valid staticcheck analyzer: %s", name))
}
