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

package core_test

import (
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit/testdata"
	"regexp"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSPMForIntersectionIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, e int, index ia(a), index ib(b), index ic(c), index id(d), index ie(e))")
	require.False(t, tk.HasPlan("select * from t where a = 10 and b = 20 and c > 30 and d is null and e in (0, 100)", "IndexMerge"))
	require.True(t,
		tk.HasPlan("select /*+ use_index_merge(t, ia, ib, ic, id, ie) */ * from t where a = 10 and b = 20 and c > 30 and d is null and e in (0, 100)",
			"IndexMerge",
		),
	)
	tk.MustExec(`
create global binding for
	select * from t where a = 10 and b = 20 and c > 30 and d is null and e in (0, 100)
using
	select /*+ use_index_merge(t, ia, ib, ic, id, ie) */ * from t where a = 10 and b = 20 and c > 30 and d is null and e in (0, 100)
`)
	require.True(t, tk.HasPlan("select * from t where a = 10 and b = 20 and c > 30 and d is null and e in (0, 100)", "IndexMerge"))
}

func TestPlanCacheForIntersectionIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, e int, index ia(a), index ib(b), index ic(c), index id(d), index ie(e))")
	tk.MustExec("prepare stmt from 'select /*+ use_index_merge(t, ia, ib, ic, id, ie) */ * from t where a = 10 and b = ? and c > ? and d is null and e in (0, 100)'")
	tk.MustExec("set @a=1, @b=3")
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @a=100, @b=500")
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a,@b").Check(testkit.Rows())
	require.True(t, tk.HasPlanForLastExecution("IndexMerge"))
}

func TestHintForIntersectionIndexMerge(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	handle := domain.StatsHandle()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table tp(a int, b int, c int, d int, e int, index ia(a), index ib(b), index ic(c), index id(d), index ie(e))" +
		"partition by range(c) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (20)," +
		"partition p2 values less than (30)," +
		"partition p3 values less than (maxvalue))")
	tk.MustExec("create definer='root'@'localhost' view vh as " +
		"select /*+ use_index_merge(tp, ia, ib, ic, id) */ * from tp where a = 10 and b > 20 and c < 30 and d in (2,5)")
	tk.MustExec("create definer='root'@'localhost' view v as " +
		"select * from tp where a = 10 and b > 20 and c < 30 and d in (2,5)")
	require.NoError(t, handle.HandleDDLEvent(<-handle.DDLEventCh()))
	require.Nil(t, handle.Update(domain.InfoSchema()))
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table tp")
	require.Nil(t, handle.Update(domain.InfoSchema()))

	var (
		input  []string
		output []struct {
			SQL  string
			Plan []string
		}
	)
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	matchSetStmt, err := regexp.Compile("^set")
	require.NoError(t, err)
	for i, ts := range input {
		testdata.OnRecord(func() {
			output[i].SQL = ts
		})
		ok := matchSetStmt.MatchString(ts)
		if ok {
			tk.MustExec(ts)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + ts).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + ts).Check(testkit.Rows(output[i].Plan...))
	}
}
