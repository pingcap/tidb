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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestCorrelate(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1 (a int, b int, key(a))")
		tk.MustExec("create table t2 (a int, b int, key(a))")
		tk.MustExec("insert into t1 values (1,1),(2,2),(3,3)")
		tk.MustExec("insert into t2 values (1,10),(2,20)")

		// Enable the correlate rule.
		tk.MustExec("set tidb_opt_enable_correlate_subquery = ON")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCorrelateSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format = 'brief' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}
