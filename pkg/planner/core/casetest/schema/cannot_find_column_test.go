// Copyright 2026 PingCAP, Inc.
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

package schema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestSchemaCannotFindColumnRegression(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`drop table if exists t1, t3`)
		tk.MustExec(`CREATE TABLE t1 (
  id BIGINT NOT NULL,
  k0 BIGINT NOT NULL,
  d0 DATE NOT NULL,
  d1 FLOAT NOT NULL,
  PRIMARY KEY (id)
)`)
		tk.MustExec(`CREATE TABLE t3 (
  id BIGINT NOT NULL,
  k2 VARCHAR(64) NOT NULL,
  k0 BIGINT NOT NULL,
  d0 DOUBLE NOT NULL,
  d1 DATE NOT NULL,
  PRIMARY KEY (id)
)`)
		tk.MustExec("INSERT INTO t1 (id, k0, d0, d1) VALUES (10, 93, '2024-04-14', 14.19)")
		tk.MustExec("INSERT INTO t3 (id, k2, k0, d0, d1) VALUES (10, 's356', 749, 89.26, '2024-04-29')")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetSchemaSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				planRows := testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + sql).Rows())
				if len(planRows) == 0 {
					t.Fatalf("empty plan for sql: %s", sql)
				}
				output[i].SQL = sql
				output[i].Plan = planRows
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format='brief' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}
