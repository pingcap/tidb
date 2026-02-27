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

package rule

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestGeneratePKFilterHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1(a int primary key clustered, b int, c int, key idx_b(b), key idx_c(c), key idx_bc(b,c))")
		tk.MustExec("insert into t1 values(1,1,1),(2,2,2),(3,3,3),(4,1,4),(5,2,5)")

		// Single-column secondary index with hint - should generate PK filter.
		// b=1 matches rows with a=1 and a=4, so PK filter produces range:[1,4].
		// The ge/le conditions are folded into the TableRangeScan range access
		// condition by the ranger, so we verify the bounded range rather than
		// looking for ge(...) in the Selection.
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t1, idx_b) */ * from t1 where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("Single-column idx_b plan:\n%s", planStr)
		require.Contains(t, planStr, "TableRangeScan", "expected TableRangeScan with PK filter")
		require.Contains(t, planStr, "range:[1,4]", "expected PK filter range from idx_b where b=1")
	})
}

func TestGeneratePKFilterSessionVar(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t2")
		tk.MustExec("create table t2(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t2 values(1,1),(2,2),(3,3)")

		// Session variable ON - b=1 matches only row a=1, so MIN=MAX=1.
		// The PK filter range collapses to a Point_Get.
		tk.MustExec("set tidb_opt_generate_pk_filter = ON")
		plan := tk.MustQuery("explain format='brief' select * from t2 where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		require.Contains(t, planStr, "Point_Get", "expected Point_Get with session var ON (PK filter collapsed to point)")

		// Session variable OFF - should use IndexLookUp on idx_b, not PK-bounded scan
		tk.MustExec("set tidb_opt_generate_pk_filter = OFF")
		plan = tk.MustQuery("explain format='brief' select * from t2 where b = 1 order by a limit 10")
		planStr = planToString(plan.Rows())
		require.NotContains(t, planStr, "Point_Get", "should NOT generate Point_Get with session var OFF")
	})
}

func TestGeneratePKFilterCompositePK(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t3")
		tk.MustExec("create table t3(a int, b int, c int, d int, primary key(a,b) clustered, key idx_c(c))")
		tk.MustExec("insert into t3 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")

		// Composite PK, no PK col has filter - should generate on first PK col (a).
		// c=1 matches only row (1,1,1,1), so MIN(a)=1, MAX(a)=1 → range:[1,1].
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where c = 1 order by a, b limit 10")
		planStr := planToString(plan.Rows())
		require.Contains(t, planStr, "TableRangeScan", "expected TableRangeScan with PK filter on first PK col")
		require.Contains(t, planStr, "range:[1,1]", "expected PK filter range on first PK col")

		// Composite PK, first PK col has EQ filter - should generate on second PK col (b).
		// a=1 AND c=1 matches only row (1,1,1,1), so MIN(b)=1, MAX(b)=1.
		// Combined with a=1, this produces a Point_Get on clustered PK (a=1,b=1).
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where a = 1 and c = 1 order by b limit 10")
		planStr = planToString(plan.Rows())
		require.Contains(t, planStr, "Point_Get", "expected Point_Get with PK filter on second PK col")

		// All PK cols have EQ filter - should NOT generate filter (no target PK col).
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where a = 1 and b = 2 and c = 1 order by d limit 10")
		planStr = planToString(plan.Rows())
		require.Contains(t, planStr, "Point_Get", "expected Point_Get when all PK cols have EQ (no PK filter needed)")
	})
}

func planToString(rows [][]any) string {
	var sb strings.Builder
	for _, row := range rows {
		for _, col := range row {
			sb.WriteString(col.(string))
			sb.WriteString(" ")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
