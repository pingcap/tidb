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
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// TestCorrelateNullSemantics verifies that CorrelateSolver does not break
// 3-valued NULL semantics for scalar IN (LeftOuterSemiJoin).
func TestCorrelateNullSemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_enable_correlate_subquery = ON")

	// Case 1: non-null outer, null inner → must return NULL (not 0).
	tk.MustExec("drop table if exists tn, sn")
	tk.MustExec("create table tn(a int)")
	tk.MustExec("create table sn(a int, key(a))")
	tk.MustExec("insert into tn values (1)")
	tk.MustExec("insert into sn values (null)")
	tk.MustQuery("select tn.a in (select sn.a from sn) as r from tn").Check(testkit.Rows("<nil>"))

	// Case 2: null outer, non-null inner → must return NULL (not 0).
	tk.MustExec("truncate table tn")
	tk.MustExec("truncate table sn")
	tk.MustExec("insert into tn values (null)")
	tk.MustExec("insert into sn values (1)")
	tk.MustQuery("select tn.a in (select sn.a from sn) as r from tn").Check(testkit.Rows("<nil>"))

	// Case 3: both columns NOT NULL → correlate is safe; verify correct results.
	tk.MustExec("drop table if exists tnn, snn")
	tk.MustExec("create table tnn(a int not null)")
	tk.MustExec("create table snn(a int not null, key(a))")
	tk.MustExec("insert into tnn values (1), (2), (3)")
	tk.MustExec("insert into snn values (1), (2)")
	tk.MustQuery("select tnn.a in (select snn.a from snn) as r from tnn order by tnn.a").Check(testkit.Rows("1", "1", "0"))
}

// TestCorrelatePreservesHints verifies that when the CorrelateSolver builds an
// Apply alternative, user-specified join hints (e.g., HASH_JOIN) are preserved
// and respected during physical plan selection.
func TestCorrelatePreservesHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_enable_correlate_subquery = ON")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int not null, b int, key(a))")
	tk.MustExec("create table t2 (a int not null, b int, key(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3)")
	tk.MustExec("insert into t2 values (1,10),(2,20)")

	// With HASH_JOIN hint, the plan should use HashJoin even when the correlate
	// optimization is enabled and could produce an Apply alternative.
	rows := tk.MustQuery("explain format = 'brief' select /*+ HASH_JOIN(t1, t2) */ * from t1 where a in (select a from t2)").Rows()
	hasHashJoin := false
	for _, row := range rows {
		if strings.Contains(row[0].(string), "HashJoin") {
			hasHashJoin = true
			break
		}
	}
	require.True(t, hasHashJoin, "HASH_JOIN hint should be preserved when correlate optimization is enabled")

	// Verify the same query produces correct results.
	tk.MustQuery("select /*+ HASH_JOIN(t1, t2) */ * from t1 where a in (select a from t2) order by t1.a").
		Check(testkit.Rows("1 1", "2 2"))
}

func TestCorrelate(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (a int, b int, key(a))")
		tk.MustExec("create table t2 (a int, b int, key(a))")
		tk.MustExec("create table t3 (a int, b int, key(a))")
		tk.MustExec("insert into t1 values (1,1),(2,2),(3,3)")
		tk.MustExec("insert into t2 values (1,10),(2,20)")
		tk.MustExec("insert into t3 values (10,1),(20,2)")

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
