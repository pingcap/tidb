// Copyright 2023 PingCAP, Inc.
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
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

type Input []string

// TiFlash cases. TopN pushed down to storage only when no partition by.
func TestPushDerivedTopnFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("set tidb_opt_derive_topn=1")
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, primary key(b,a))")
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		tk.MustExec("set tidb_enforce_mpp=1")
		tk.MustExec("set @@session.tidb_allow_mpp=ON;")
		var input Input
		var output []struct {
			SQL  string
			Plan []string
		}
		suiteData := GetDerivedTopNSuiteData()
		suiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := tk.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestTopNPushdown(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")

		tk.MustExec(`drop table if exists t3`)
		tk.MustExec(`CREATE TABLE t3(c0 INT, primary key(c0))`)
		tk.MustExec(`insert into t3 values(1), (2), (3), (4), (5), (6), (7), (8), (9), (10)`)
		rs := tk.MustQuery(`SELECT /* issue:37986 */ v2.c0 FROM (select rand() as c0 from t3) v2 order by v2.c0 limit 10`).Rows()
		lastVal := -1.0
		for _, r := range rs {
			v := r[0].(string)
			val, err := strconv.ParseFloat(v, 64)
			require.NoError(t, err)
			require.True(t, val >= lastVal)
			lastVal = val
		}

		tk.MustQuery(`explain format='brief' SELECT /* issue:37986 */ v2.c0 FROM (select rand() as c0 from t3) v2 order by v2.c0 limit 10`).
			Check(testkit.Rows(`TopN 10.00 root  Column#3, offset:0, count:10`,
				`└─Projection 10000.00 root  rand()->Column#3`,
				`  └─TableReader 10000.00 root  data:TableFullScan`,
				`    └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
	})
}
