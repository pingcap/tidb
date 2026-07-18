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

package errcheck

import (
	"embed"
	"log"

	"github.com/kisielk/errcheck/errcheck"
	"github.com/pingcap/tidb/build/linter/util"
)

// Analyzer is the analyzer struct of errcheck.
var Analyzer = errcheck.Analyzer

//go:embed errcheck_excludes.txt
var excludesContent embed.FS

func init() {
	data, _ := excludesContent.ReadFile("errcheck_excludes.txt")
	err := Analyzer.Flags.Set("excludes", string(data))
	if err != nil {
		log.Fatal(err)
	}
	util.SkipAnalyzerByConfig(Analyzer)
	util.SkipAnalyzer(Analyzer)
}
