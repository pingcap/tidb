// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCTEIssue49096(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mock_cte_exec_panic_avoid_deadlock", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mock_cte_exec_panic_avoid_deadlock"))
	}()
	insertStr := "insert into t1 values(0)"
	rowNum := 10
	vals := make([]int, rowNum)
	vals[0] = 0
	for i := 1; i < rowNum; i++ {
		v := rand.Intn(100)
		vals[i] = v
		insertStr += fmt.Sprintf(", (%d)", v)
	}
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("create table t2(c1 int);")
	tk.MustExec(insertStr)
	// should be insert statement, otherwise it couldn't step int resetCTEStorageMap in handleNoDelay func.
	sql := "insert into t2 with cte1 as ( " +
		"select c1 from t1) " +
		"select c1 from cte1 natural join (select * from cte1 where c1 > 0) cte2 order by c1;"
	tk.MustExec(sql) // No deadlock
}

func TestSpillToDisk(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("SET GLOBAL tidb_enable_tmp_storage_on_oom = 1")
	defer tk.MustExec("SET GLOBAL tidb_enable_tmp_storage_on_oom = 0")
	tk.MustExec("use test;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testCTEStorageSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testCTEStorageSpill"))
		tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill"))
	}()

	// Use duplicated rows to test UNION DISTINCT.
	tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	insertStr := "insert into t1 values(0)"
	rowNum := 1000
	vals := make([]int, rowNum)
	vals[0] = 0
	for i := 1; i < rowNum; i++ {
		v := rand.Intn(100)
		vals[i] = v
		insertStr += fmt.Sprintf(", (%d)", v)
	}
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec(insertStr)
	tk.MustExec("set tidb_mem_quota_query = 40000;")
	tk.MustExec("set cte_max_recursion_depth = 500000;")
	sql := fmt.Sprintf("with recursive cte1 as ( "+
		"select c1 from t1 "+
		"union "+
		"select c1 + 1 c1 from cte1 where c1 < %d) "+
		"select c1 from cte1 order by c1;", rowNum)
	rows := tk.MustQuery(sql)

	memTracker := tk.Session().GetSessionVars().StmtCtx.MemTracker
	diskTracker := tk.Session().GetSessionVars().StmtCtx.DiskTracker
	require.Greater(t, memTracker.MaxConsumed(), int64(0))
	require.Greater(t, diskTracker.MaxConsumed(), int64(0))

	slices.Sort(vals)
	resRows := make([]string, 0, rowNum)
	for i := vals[0]; i <= rowNum; i++ {
		resRows = append(resRows, fmt.Sprintf("%d", i))
	}
	rows.Check(testkit.Rows(resRows...))
}

func TestCTEExecError(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists src;")
	tk.MustExec("create table src(first int, second int);")

	insertStr := fmt.Sprintf("insert into src values (%d, %d)", rand.Intn(1000), rand.Intn(1000))
	for i := 0; i < 1000; i++ {
		insertStr += fmt.Sprintf(",(%d, %d)", rand.Intn(1000), rand.Intn(1000))
	}
	insertStr += ";"
	tk.MustExec(insertStr)

	// Increase projection concurrency and decrease chunk size
	// to increase the probability of reproducing the problem.
	tk.MustExec("set tidb_max_chunk_size = 32")
	tk.MustExec("set tidb_projection_concurrency = 20")
	for i := 0; i < 10; i++ {
		err := tk.QueryToErr("with recursive cte(iter, first, second, result) as " +
			"(select 1, first, second, first+second from src " +
			" union all " +
			"select iter+1, second, result, second+result from cte where iter < 80 )" +
			"select * from cte")
		require.True(t, terror.ErrorEqual(err, types.ErrOverflow))
	}
}

func TestCTEPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(c1 int)")
	tk.MustExec("insert into t1 values(1), (2), (3)")

	fpPathPrefix := "github.com/pingcap/tidb/pkg/executor/"
	fp := "testCTESeedPanic"
	require.NoError(t, failpoint.Enable(fpPathPrefix+fp, fmt.Sprintf(`panic("%s")`, fp)))
	err := tk.QueryToErr("with recursive cte1 as (select c1 from t1 union all select c1 + 1 from cte1 where c1 < 5) select t_alias_1.c1 from cte1 as t_alias_1 inner join cte1 as t_alias_2 on t_alias_1.c1 = t_alias_2.c1 order by c1")
	require.Contains(t, err.Error(), fp)
	require.NoError(t, failpoint.Disable(fpPathPrefix+fp))

	fp = "testCTERecursivePanic"
	require.NoError(t, failpoint.Enable(fpPathPrefix+fp, fmt.Sprintf(`panic("%s")`, fp)))
	err = tk.QueryToErr("with recursive cte1 as (select c1 from t1 union all select c1 + 1 from cte1 where c1 < 5) select t_alias_1.c1 from cte1 as t_alias_1 inner join cte1 as t_alias_2 on t_alias_1.c1 = t_alias_2.c1 order by c1")
	require.Contains(t, err.Error(), fp)
	require.NoError(t, failpoint.Disable(fpPathPrefix+fp))
}

func TestCTEDelSpillFile(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(c1 int, c2 int);")
	tk.MustExec("create table t2(c1 int);")
	tk.MustExec("set @@cte_max_recursion_depth = 1000000;")
	tk.MustExec("set global tidb_mem_oom_action = 'log';")
	tk.MustExec("set @@tidb_mem_quota_query = 100;")
	tk.MustExec("insert into t2 values(1);")
	tk.MustExec("insert into t1 (c1, c2) with recursive cte1 as (select c1 from t2 union select cte1.c1 + 1 from cte1 where cte1.c1 < 100000) select cte1.c1, cte1.c1+1 from cte1;")
	require.Nil(t, tk.Session().GetSessionVars().StmtCtx.CTEStorageMap)
}

func TestCTEShareCorColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(c1 int, c2 varchar(100));")
	tk.MustExec("insert into t1 values(1, '2020-10-10');")
	tk.MustExec("create table t2(c1 int, c2 date);")
	tk.MustExec("insert into t2 values(1, '2020-10-10');")
	for i := 0; i < 100; i++ {
		tk.MustQuery("with cte1 as (select t1.c1, (select t2.c2 from t2 where t2.c2 = str_to_date(t1.c2, '%Y-%m-%d')) from t1 inner join t2 on t1.c1 = t2.c1) select /*+ hash_join_build(alias1) */ * from cte1 alias1 inner join cte1 alias2 on alias1.c1 =   alias2.c1;").Check(testkit.Rows("1 2020-10-10 1 2020-10-10"))
		tk.MustQuery("with cte1 as (select t1.c1, (select t2.c2 from t2 where t2.c2 = str_to_date(t1.c2, '%Y-%m-%d')) from t1 inner join t2 on t1.c1 = t2.c1) select /*+ hash_join_build(alias2) */ * from cte1 alias1 inner join cte1 alias2 on alias1.c1 =   alias2.c1;").Check(testkit.Rows("1 2020-10-10 1 2020-10-10"))
	}

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert into t1 values(1), (2);")
	tk.MustQuery("SELECT * FROM t1 dt WHERE EXISTS( WITH RECURSIVE qn AS (SELECT a AS b UNION ALL SELECT b+1 FROM qn WHERE b=0 or b = 1) SELECT * FROM qn dtqn1 where exists (select /*+ NO_DECORRELATE() */ b from qn where dtqn1.b+1));").Check(testkit.Rows("1", "2"))
}
