// Copyright 2025 PingCAP, Inc.
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

package job

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func Test33B(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("create database imdb")
		tk.MustExec("use imdb")
		createCompanyName(t, tk, dom)
		createInfoType(t, tk, dom)
		createKindype(t, tk, dom)
		createLinkType(t, tk, dom)
		createMovieCompanies(t, tk, dom)
		createMovieInfoIdx(t, tk, dom)
		createMovieLink(t, tk, dom)
		createTitle(t, tk, dom)
		testkit.LoadTableStats(`imdb.company_name.json`, dom)
		testkit.LoadTableStats(`imdb.info_type.json`, dom)
		testkit.LoadTableStats(`imdb.kind_type.json`, dom)
		testkit.LoadTableStats(`imdb.link_type.json`, dom)
		testkit.LoadTableStats(`imdb.movie_companies.json`, dom)
		testkit.LoadTableStats(`imdb.movie_info_idx.json`, dom)
		testkit.LoadTableStats(`imdb.movie_link.json`, dom)
		testkit.LoadTableStats(`imdb.title.json`, dom)
		integrationSuiteData := GetJOBSuiteData()
		var (
			input  []string
			output []struct {
				SQL    string
				Result []string
			}
		)
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		costTraceFormat := `explain format='brief' `
		for i := range input {
			testdata.OnRecord(func() {
				output[i].SQL = input[i]
			})
			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
			})
			tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
			checkCost(t, tk, input[i])
		}
	})
}
