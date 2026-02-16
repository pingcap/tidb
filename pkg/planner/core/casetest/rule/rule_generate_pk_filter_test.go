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

		// Debug: first check with session variable to verify rule works at all
		tk.MustExec("set tidb_opt_generate_pk_filter = ON")
		plan := tk.MustQuery("explain format='brief' select * from t1 where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		t.Logf("Session var enabled plan:\n%s", planStr)
		tk.MustExec("set tidb_opt_generate_pk_filter = OFF")

		// Single-column secondary index with hint - should generate PK filter
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t1, idx_b) */ * from t1 where b = 1 order by a limit 10")
		planStr = planToString(plan.Rows())
		t.Logf("Single-column idx_b plan:\n%s", planStr)
		require.Contains(t, planStr, "ge(test.t1.a", "expected GE condition for PK filter")
	})
}

func TestGeneratePKFilterSessionVar(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t2")
		tk.MustExec("create table t2(a int primary key clustered, b int, key idx_b(b))")
		tk.MustExec("insert into t2 values(1,1),(2,2),(3,3)")

		// Session variable ON
		tk.MustExec("set tidb_opt_generate_pk_filter = ON")
		plan := tk.MustQuery("explain format='brief' select * from t2 where b = 1 order by a limit 10")
		planStr := planToString(plan.Rows())
		require.Contains(t, planStr, "ge(test.t2.a", "expected PK filter with session var ON")

		// Session variable OFF
		tk.MustExec("set tidb_opt_generate_pk_filter = OFF")
		plan = tk.MustQuery("explain format='brief' select * from t2 where b = 1 order by a limit 10")
		planStr = planToString(plan.Rows())
		require.NotContains(t, planStr, "ge(test.t2.a", "should NOT generate PK filter with session var OFF")
	})
}

func TestGeneratePKFilterCompositePK(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, _, _ string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t3")
		tk.MustExec("create table t3(a int, b int, c int, d int, primary key(a,b) clustered, key idx_c(c))")
		tk.MustExec("insert into t3 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")

		// Composite PK, no PK col has filter - should generate on first PK col (a)
		plan := tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where c = 1 order by a, b limit 10")
		planStr := planToString(plan.Rows())
		require.Contains(t, planStr, "ge(test.t3.a", "expected PK filter on first PK col")

		// Composite PK, first PK col has EQ filter - should generate on second PK col (b)
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where a = 1 and c = 1 order by b limit 10")
		planStr = planToString(plan.Rows())
		require.Contains(t, planStr, "ge(test.t3.b", "expected PK filter on second PK col")

		// All PK cols have EQ filter - should NOT generate filter
		plan = tk.MustQuery("explain format='brief' select /*+ pk_filter(t3, idx_c) */ * from t3 where a = 1 and b = 2 and c = 1 order by d limit 10")
		planStr = planToString(plan.Rows())
		require.NotContains(t, planStr, "ge(test.t3.a", "should NOT generate PK filter when all PK cols have EQ")
		require.NotContains(t, planStr, "ge(test.t3.b", "should NOT generate PK filter when all PK cols have EQ")
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
