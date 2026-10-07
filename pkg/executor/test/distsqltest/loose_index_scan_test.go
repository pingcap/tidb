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

package distsql_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

const (
	forceLooseIndexScan          = "github.com/pingcap/tidb/pkg/planner/core/forceLooseIndexScan"
	forceLooseIndexScanCandidate = "github.com/pingcap/tidb/pkg/planner/core/operator/physicalop/forceLooseIndexScan"
)

func enableLooseScan(t *testing.T) {
	require.NoError(t, failpoint.Enable(forceLooseIndexScan, "return"))
	require.NoError(t, failpoint.Enable(forceLooseIndexScanCandidate, "return"))
}

func disableLooseScan(t *testing.T) {
	require.NoError(t, failpoint.Disable(forceLooseIndexScan))
	require.NoError(t, failpoint.Disable(forceLooseIndexScanCandidate))
}

func usesLooseScan(tk *testkit.TestKit, sql string) bool {
	for _, row := range tk.MustQuery("explain format='brief' " + sql).Rows() {
		if strings.Contains(fmt.Sprint(row), "loose scan") {
			return true
		}
	}
	return false
}

func TestLooseIndexScanMatchesFullScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (
		a int, b int, c varchar(20) collate utf8mb4_general_ci, d int not null, e int,
		key iab (a, b), key icd (c, d), key iabe (a, b, e))`)
	rng := rand.New(rand.NewSource(1))
	strs := []string{"x", "X", "x ", "y", "Y ", "z", "", "xx", "x\x01"}
	nullable := func(n int) string {
		if rng.Intn(8) == 0 {
			return "null"
		}
		return fmt.Sprint(rng.Intn(n))
	}
	values := make([]string, 0, 2000)
	for range 2000 {
		c := "null"
		if rng.Intn(10) != 0 {
			c = "'" + strs[rng.Intn(len(strs))] + "'"
		}
		values = append(values, fmt.Sprintf("(%s, %s, %s, %d, %s)", nullable(20), nullable(50), c, rng.Intn(100), nullable(10)))
	}
	tk.MustExec("insert into t values " + strings.Join(values, ","))
	tk.MustExec("split table t index iab between (0) and (20) regions 6")
	tk.MustExec("split table t index icd between ('a') and ('z') regions 4")
	tk.MustExec("analyze table t")

	eligible := []string{
		"select a from t use index(iab) group by a",
		"select distinct a from t use index(iab)",
		"select a, min(b) from t use index(iab) group by a",
		"select a, min(b), max(a) from t use index(iab) where b > 30 group by a",
		"select a, min(b) from t use index(iab) where a between 3 and 11 group by a",
		"select a, min(b) from t use index(iab) where a in (2, 5, 7) or a > 15 group by a",
		"select a, max(b) from t use index(iab) group by a order by a desc",
		"select a, b from t use index(iab) group by a, b",
		"select a, b, min(e) from t use index(iabe) group by a, b",
		"select b from t use index(iab) where a = 3 group by b",
		"select a from t use index(iab) where b = 7 group by a",
		// Any member of a _ci group is a valid representative, so compare the
		// normalized value.
		"select lower(trim(c)) from t use index(icd) group by c",
		"select lower(trim(c)), min(d) from t use index(icd) group by c",
		"select lower(trim(c)), max(d) from t use index(icd) group by c order by c desc",
	}
	for _, sql := range eligible {
		enableLooseScan(t)
		require.True(t, usesLooseScan(tk, sql), sql)
		loose := tk.MustQuery(sql).Sort().Rows()
		disableLooseScan(t)
		require.False(t, usesLooseScan(tk, sql), sql)
		tk.MustQuery(sql).Sort().Check(loose)
	}

	enableLooseScan(t)
	defer func() {
		disableLooseScan(t)
	}()
	ineligible := []string{
		"select a, count(*) from t use index(iab) group by a",
		"select a, sum(b) from t use index(iab) group by a",
		"select a, max(b) from t use index(iab) group by a",
		"select a, min(e) from t use index(iabe) group by a",
		"select b from t use index(iab) group by b",
		"select a, count(distinct b) from t use index(iab) group by a",
	}
	for _, sql := range ineligible {
		require.False(t, usesLooseScan(tk, sql), sql)
	}
}

func TestLooseIndexScanPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key iab (a, b))")
	enableLooseScan(t)
	defer func() {
		disableLooseScan(t)
	}()
	tk.MustQuery("explain format='brief' select a, min(b) from t group by a").Check(testkit.Rows(
		"Projection 8000.00 root  test.t.a, Column#5",
		"└─StreamAgg 8000.00 root  group by:test.t.a, funcs:min(test.t.b)->Column#5, funcs:firstrow(test.t.a)->test.t.a",
		"  └─IndexReader 8000.00 root  index:Limit, loose scan prefix:1",
		"    └─Limit 8000.00 cop[tikv]  offset:0, count:1",
		"      └─IndexFullScan 10000.00 cop[tikv] table:t, index:iab(a, b) keep order:true, stats:pseudo"))
}
