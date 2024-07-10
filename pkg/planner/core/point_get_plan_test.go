// Copyright 2018 PingCAP, Inc.
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
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPointGetPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect hit counter in this ut
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, c int, key idx_bc(b,c))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows(
		"Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' select * from t where 1 = a").Check(testkit.Rows(
		"Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' update t set b=b+1, c=c+1 where a = 1").Check(testkit.Rows(
		"Update N/A root  N/A",
		"└─Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' delete from t where a = 1").Check(testkit.Rows(
		"Delete N/A root  N/A",
		"└─Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' select a from t where a = -1").Check(testkit.Rows(
		"TableDual 0.00 root  rows:0",
	))
	tk.MustExec(`prepare stmt0 from "select a from t where a = ?"`)
	tk.MustExec("set @p0 = -1")
	tk.MustQuery("execute stmt0 using @p0").Check(testkit.Rows())
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	var hit float64
	// PointGetPlan for Select.
	tk.MustExec(`prepare stmt1 from "select * from t where a = ?"`)
	tk.MustExec(`prepare stmt2 from "select * from t where b = ? and c = ?"`)
	tk.MustExec("set @param=1")
	tk.MustQuery("execute stmt1 using @param").Check(testkit.Rows("1 1 1"))
	err := counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(0), hit)
	tk.MustExec("set @param=2")
	tk.MustQuery("execute stmt1 using @param").Check(testkit.Rows("2 2 2"))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(1), hit)
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("2 2 2"))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(1), hit)
	tk.MustExec("set @param=1")
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("1 1 1"))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), hit)
	// PointGetPlan for Update.
	tk.MustExec(`prepare stmt3 from "update t set b=b+1, c=c+1 where a = ?"`)
	tk.MustExec(`prepare stmt4 from "update t set a=a+1 where b = ? and c = ?"`)
	tk.MustExec("set @param=3")
	tk.MustExec("execute stmt3 using @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 4 4",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), hit)
	tk.MustExec("set @param=4")
	tk.MustExec("execute stmt4 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 4",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), hit)
	// PointGetPlan for Delete.
	tk.MustExec(`prepare stmt5 from "delete from t where a = ?"`)
	tk.MustExec(`prepare stmt6 from "delete from t where b = ? and c = ?"`)
	tk.MustExec("execute stmt5 using @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), hit)
	tk.MustExec("set @param=2")
	tk.MustExec("execute stmt6 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), hit)
	tk.MustExec("insert into t (a, b, c) values (18446744073709551615, 4, 4)")
	tk.MustExec("set @p1=-1")
	tk.MustExec("set @p2=1")
	tk.MustExec(`prepare stmt7 from "select a from t where a = ?"`)
	tk.MustQuery("execute stmt7 using @p1").Check(testkit.Rows())
	tk.MustQuery("execute stmt7 using @p2").Check(testkit.Rows("1"))
	err = counter.Write(pb)
	require.NoError(t, err)
	hit = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), hit)
}

// Test that the plan id will be reset before optimization every time.
func TestPointGetId(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	defer tk.MustExec("drop table if exists t")
	pointGetQuery := "select c2 from t where c1 = 1"
	for i := 0; i < 2; i++ {
		ctx := tk.Session().(sessionctx.Context)
		stmts, err := session.Parse(ctx, pointGetQuery)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(context.Background(), ctx, stmt, core.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		require.NoError(t, err)
		// Test explain format = 'brief' result is useless, plan id will be reset when running `explain`.
		require.Equal(t, 1, p.ID())
	}
}

func TestIssue20692(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int, vv int, vvv int, unique key u0(id, v, vv));")
	tk.MustExec("insert into t values(1, 1, 1, 1);")
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk1.MustExec("begin pessimistic;")
	tk1.MustExec("use test")
	tk2.MustExec("begin pessimistic;")
	tk2.MustExec("use test")
	tk3.MustExec("begin pessimistic;")
	tk3.MustExec("use test")
	tk1.MustExec("delete from t where id = 1 and v = 1 and vv = 1;")
	stop1, stop2 := make(chan struct{}), make(chan struct{})
	go func() {
		tk2.MustExec("insert into t values(1, 2, 3, 4);")
		stop1 <- struct{}{}
		tk3.MustExec("update t set id = 10, v = 20, vv = 30, vvv = 40 where id = 1 and v = 2 and vv = 3;")
		stop2 <- struct{}{}
	}()
	tk1.MustExec("commit;")
	<-stop1

	// wait 50ms to ensure tk3 is blocked by tk2
	select {
	case <-stop2:
		t.Fail()
	case <-time.After(50 * time.Millisecond):
	}

	tk2.MustExec("commit;")
	<-stop2
	tk3.MustExec("commit;")
	tk3.MustQuery("select * from t;").Check(testkit.Rows("10 20 30 40"))
}

func TestIssue18042(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), index ab(a, b));")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	tk.MustExec("SELECT /*+ MAX_EXECUTION_TIME(100), MEMORY_QUOTA(1 MB) */ * FROM t where a = 1;")
	require.Equal(t, int64(1<<20), tk.Session().GetSessionVars().StmtCtx.MemQuotaQuery)
	require.Equal(t, uint64(100), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)
	tk.MustExec("drop table t")
}
