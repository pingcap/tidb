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
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	dto "github.com/prometheus/client_model/go"
)

var _ = SerialSuites(&testPointGetSuite{})

type testPointGetSuite struct {
	store    kv.Storage
	dom      *domain.Domain
	testData testutil.TestData
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
	s.testData, err = testutil.LoadTestSuiteData("testdata", "point_get_plan")
	c.Assert(err, IsNil)
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPointGetSuite) TestPointGetPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	var err error
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

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
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(0))
	tk.MustExec("set @param=2")
	tk.MustQuery("execute stmt1 using @param").Check(testkit.Rows("2 2 2"))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(1))
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("2 2 2"))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(1))
	tk.MustExec("set @param=1")
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("1 1 1"))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
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
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustExec("set @param=4")
	tk.MustExec("execute stmt4 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 4",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	// PointGetPlan for Delete.
	tk.MustExec(`prepare stmt5 from "delete from t where a = ?"`)
	tk.MustExec(`prepare stmt6 from "delete from t where b = ? and c = ?"`)
	tk.MustExec("execute stmt5 using @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustExec("set @param=2")
	tk.MustExec("execute stmt6 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustExec("insert into t (a, b, c) values (18446744073709551615, 4, 4)")
	tk.MustExec("set @p1=-1")
	tk.MustExec("set @p2=1")
	tk.MustExec(`prepare stmt7 from "select a from t where a = ?"`)
	tk.MustQuery("execute stmt7 using @p1").Check(testkit.Rows())
	tk.MustQuery("execute stmt7 using @p2").Check(testkit.Rows("1"))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
}

func (s *testPointGetSuite) TestPointGetForUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table fu (id int primary key, val int)")
	tk.MustExec("insert into fu values (6, 6)")

	// In autocommit mode, outside a transaction, "for update" doesn't take effect.
	checkUseForUpdate(tk, c, false)

	tk.MustExec("begin")
	checkUseForUpdate(tk, c, true)
	tk.MustExec("rollback")

	tk.MustExec("set @@session.autocommit = 0")
	checkUseForUpdate(tk, c, true)
	tk.MustExec("rollback")
}

func (s *testPointGetSuite) TestPointGetForUpdateWithSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE users (id bigint(20) unsigned NOT NULL primary key, name longtext DEFAULT NULL, company_id bigint(20) DEFAULT NULL)")
	tk.MustExec("create table companies(id bigint primary key, name longtext default null)")
	tk.MustExec("insert into companies values(14, 'Company14')")
	tk.MustExec("insert into companies values(15, 'Company15')")
	tk.MustExec("insert into users(id, company_id, name) values(239, 15, 'xxxx')")
	tk.MustExec("UPDATE users SET name=(SELECT name FROM companies WHERE companies.id = users.company_id)  WHERE id = 239")

	tk.MustQuery("select * from users").Check(testkit.Rows("239 Company15 15"))
}

func checkUseForUpdate(tk *testkit.TestKit, c *C, expectLock bool) {
	res := tk.MustQuery("explain format = 'brief' select * from fu where id = 6 for update")
	// Point_Get_1	1.00	root	table:fu, handle:6
	opInfo := res.Rows()[0][4]
	selectLock := strings.Contains(fmt.Sprintf("%s", opInfo), "lock")
	c.Assert(selectLock, Equals, expectLock)

	tk.MustQuery("select * from fu where id = 6 for update").Check(testkit.Rows("6 6"))
}

func (s *testPointGetSuite) TestWhereIn2BatchPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key auto_increment not null, b int, c int, unique key idx_abc(a, b, c))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 5)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 3 3",
		"4 4 5",
	))
	tk.MustQuery("explain format = 'brief' select * from t where a = 1 and b = 1 and c = 1").Check(testkit.Rows(
		"Selection 1.00 root  eq(test.t.b, 1), eq(test.t.c, 1)",
		"└─Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' select * from t where 1 = a and 1 = b and 1 = c").Check(testkit.Rows(
		"Selection 1.00 root  eq(1, test.t.b), eq(1, test.t.c)",
		"└─Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' select * from t where 1 = a and b = 1 and 1 = c").Check(testkit.Rows(
		"Selection 1.00 root  eq(1, test.t.c), eq(test.t.b, 1)",
		"└─Point_Get 1.00 root table:t handle:1",
	))
	tk.MustQuery("explain format = 'brief' select * from t where (a, b, c) in ((1, 1, 1), (2, 2, 2))").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t, index:idx_abc(a, b, c) keep order:false, desc:false",
	))

	tk.MustQuery("explain format = 'brief' select * from t where a in (1, 2, 3, 4, 5)").Check(testkit.Rows(
		"Batch_Point_Get 5.00 root table:t handle:[1 2 3 4 5], keep order:false, desc:false",
	))

	tk.MustQuery("explain format = 'brief' select * from t where a in (1, 2, 3, 1, 2)").Check(testkit.Rows(
		"Batch_Point_Get 5.00 root table:t handle:[1 2 3 1 2], keep order:false, desc:false",
	))

	tk.MustExec("begin")
	tk.MustQuery("explain format = 'brief' select * from t where a in (1, 2, 3, 1, 2) FOR UPDATE").Check(testkit.Rows(
		"Batch_Point_Get 5.00 root table:t handle:[1 2 3 1 2], keep order:false, desc:false, lock",
	))
	tk.MustExec("rollback")

	tk.MustQuery("explain format = 'brief' select * from t where (a) in ((1), (2), (3), (1), (2))").Check(testkit.Rows(
		"Batch_Point_Get 5.00 root table:t handle:[1 2 3 1 2], keep order:false, desc:false",
	))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, unique key idx_ab(a, b))")
	tk.MustExec("insert into t values(1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"3 4 5",
		"4 5 6",
	))
	tk.MustQuery("explain format = 'brief' select * from t where (a, b) in ((1, 2), (2, 3))").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t, index:idx_ab(a, b) keep order:false, desc:false",
	))
	tk.MustQuery("select * from t where (a, b) in ((1, 2), (2, 3))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
	))
	tk.MustQuery("select * from t where (b, a) in ((1, 2), (2, 3))").Check(testkit.Rows())
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
	))
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2), (2, 1), (5, 4))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"4 5 6",
	))
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2), (2, 1), (5, 4), (3, 4))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"4 5 6",
	))

	tk.MustExec("begin pessimistic")
	tk.MustQuery("explain format = 'brief' select * from t where (a, b) in ((1, 2), (2, 3)) FOR UPDATE").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t, index:idx_ab(a, b) keep order:false, desc:false, lock",
	))
	tk.MustExec("rollback")
}

// Test that the plan id will be reset before optimization every time.
func (s *testPointGetSuite) TestPointGetId(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	defer tk.MustExec("drop table if exists t")
	pointGetQuery := "select c2 from t where c1 = 1"
	for i := 0; i < 2; i++ {
		ctx := tk.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, pointGetQuery)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		ret := &core.PreprocessorReturn{}
		err = core.Preprocess(ctx, stmt, core.WithPreprocessorReturn(ret))
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		c.Assert(err, IsNil)
		// Test explain format = 'brief' result is useless, plan id will be reset when running `explain`.
		c.Assert(p.ID(), Equals, 1)
	}
}

func (s *testPointGetSuite) TestCBOPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t (a varchar(20), b int, c int, d int, primary key(a), unique key(b, c))")
	tk.MustExec("insert into t values('1',4,4,1), ('2',3,3,2), ('3',2,2,3), ('4',1,1,4)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		res := tk.MustQuery(sql)
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(plan.Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testPointGetSuite) TestPartitionBatchPointGetPlanCache(c *C) {
	testKit := testkit.NewTestKit(c, s.store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	var err error
	testKit.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, unique key(a))")
	testKit.MustExec("insert into t values(1,1),(2,2),(3,3)")
	testKit.MustExec("prepare stmt from 'select * from t use index(a) where (a >= ? and a <= ?) or a = 3'")
	testKit.MustExec("set @p=1,@q=2,@u=3")
	testKit.MustQuery("execute stmt using @p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"3 3",
	))
	testKit.MustQuery("execute stmt using @u,@q").Sort().Check(testkit.Rows(
		"3 3",
	))

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	testKit.MustExec("insert into t values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3)")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static'")
	testKit.MustExec("prepare stmt from 'select * from t where ((a >= ? and a <= ?) or a = 2) and b = ?'")
	testKit.MustQuery("execute stmt using @p,@p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@q,@p").Sort().Check(testkit.Rows(
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@q,@q").Sort().Check(testkit.Rows(
		"2 2",
	))
	testKit.MustQuery("execute stmt using @p,@u,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
		"3 1",
	))
	testKit.MustQuery("execute stmt using @u,@p,@p").Sort().Check(testkit.Rows(
		"2 1",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a in (?,?) and b = ?'")
	testKit.MustQuery("execute stmt using @p,@q,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@q,@p").Sort().Check(testkit.Rows(
		"2 1",
	))
	testKit.MustQuery("execute stmt using @p,@q,@q").Sort().Check(testkit.Rows(
		"1 2",
		"2 2",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a = ? and ((b >= ? and b <= ?) or b = 2)'")
	testKit.MustQuery("execute stmt using @p,@p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustQuery("execute stmt using @p,@q,@q").Sort().Check(testkit.Rows(
		"1 2",
	))
	testKit.MustQuery("execute stmt using @q,@q,@q").Sort().Check(testkit.Rows(
		"2 2",
	))
	testKit.MustQuery("execute stmt using @p,@p,@u").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
		"1 3",
	))
	testKit.MustQuery("execute stmt using @p,@u,@p").Sort().Check(testkit.Rows(
		"1 2",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a = ? and b in (?,?)'")
	testKit.MustQuery("execute stmt using @p,@p,@q").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustQuery("execute stmt using @p,@q,@p").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustQuery("execute stmt using @p,@q,@q").Sort().Check(testkit.Rows(
		"1 2",
	))
	testKit.MustQuery("execute stmt using @q,@p,@q").Sort().Check(testkit.Rows(
		"2 1",
		"2 2",
	))

	testKit.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(a int, b int, primary key(a,b)) partition by hash(b) partitions 2")
	testKit.MustExec("insert into t values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3)")
	testKit.MustExec("prepare stmt from 'select * from t where ((a >= ? and a <= ?) or a = 2) and b = ?'")
	testKit.MustQuery("execute stmt using @p,@p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@q,@p").Sort().Check(testkit.Rows(
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@q,@q").Sort().Check(testkit.Rows(
		"2 2",
	))
	testKit.MustQuery("execute stmt using @p,@u,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
		"3 1",
	))
	testKit.MustQuery("execute stmt using @u,@p,@p").Sort().Check(testkit.Rows(
		"2 1",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a in (?,?) and b = ?'")
	testKit.MustQuery("execute stmt using @p,@q,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	testKit.MustQuery("execute stmt using @q,@q,@p").Sort().Check(testkit.Rows(
		"2 1",
	))
	testKit.MustQuery("execute stmt using @p,@q,@q").Sort().Check(testkit.Rows(
		"1 2",
		"2 2",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a = ? and ((b >= ? and b <= ?) or b = 2)'")
	testKit.MustQuery("execute stmt using @p,@p,@p").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustQuery("execute stmt using @p,@q,@q").Sort().Check(testkit.Rows(
		"1 2",
	))
	testKit.MustQuery("execute stmt using @q,@q,@q").Sort().Check(testkit.Rows(
		"2 2",
	))
	testKit.MustQuery("execute stmt using @p,@p,@u").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
		"1 3",
	))
	testKit.MustQuery("execute stmt using @p,@u,@p").Sort().Check(testkit.Rows(
		"1 2",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a = ? and b in (?,?)'")
	testKit.MustQuery("execute stmt using @p,@p,@q").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustQuery("execute stmt using @p,@q,@p").Sort().Check(testkit.Rows(
		"1 1",
		"1 2",
	))
	testKit.MustQuery("execute stmt using @p,@q,@q").Sort().Check(testkit.Rows(
		"1 2",
	))
	testKit.MustQuery("execute stmt using @q,@p,@q").Sort().Check(testkit.Rows(
		"2 1",
		"2 2",
	))

	testKit.MustExec("drop table t")
	testKit.MustExec("create table t(a int, b int, primary key(a)) partition by hash(a) partitions 2")
	testKit.MustExec("insert into t values(1,0),(2,0),(3,0),(4,0)")
	testKit.MustExec("prepare stmt from 'select * from t where ((a >= ? and a <= ?) or a = 2) and 1 = 1'")
	testKit.MustQuery("execute stmt using @p,@p").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("execute stmt using @q,@q").Sort().Check(testkit.Rows(
		"2 0",
	))
	testKit.MustQuery("execute stmt using @p,@u").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
		"3 0",
	))
	testKit.MustQuery("execute stmt using @u,@p").Sort().Check(testkit.Rows(
		"2 0",
	))

	testKit.MustExec("prepare stmt from 'select * from t where a in (?,?) and 1 = 1'")
	testKit.MustQuery("execute stmt using @p,@q").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("execute stmt using @q,@p").Sort().Check(testkit.Rows(
		"1 0",
		"2 0",
	))
	testKit.MustQuery("execute stmt using @q,@q").Sort().Check(testkit.Rows(
		"2 0",
	))
}

func (s *testPointGetSuite) TestBatchPointGetPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	var err error
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustQuery("explain format = 'brief' select * from t where a in (1, 2)").Check(testkit.Rows(
		"Batch_Point_Get 2.00 root table:t handle:[1 2], keep order:false, desc:false",
	))
	tk.MustExec("prepare stmt from 'select * from t where a in (?,?)'")
	tk.MustExec("set @p1 = 1, @p2 = 2")
	tk.MustQuery("execute stmt using @p1, @p2;").Check(testkit.Rows(
		"1 1",
		"2 2",
	))
	tk.MustExec("set @p1 = 3, @p2 = 4")
	tk.MustQuery("execute stmt using @p1, @p2;").Check(testkit.Rows(
		"3 3",
		"4 4",
	))
}

func (s *testPointGetSuite) TestBatchPointGetPartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	var err error
	tk.Se, err = session.CreateSession4TestWithOpt(s.store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int) PARTITION BY HASH(a) PARTITIONS 4")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustQuery("explain format = 'brief' select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows(
		"Batch_Point_Get 4.00 root table:t handle:[1 2 3 4], keep order:false, desc:false",
	))
	tk.MustQuery("select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows("1 1", "2 2", "3 3", "4 4"))

	tk.MustQuery("explain format = 'brief' update t set b = b + 1 where a in (1, 2, 3, 4)").Check(testkit.Rows(
		"Update N/A root  N/A]\n[└─Batch_Point_Get 4.00 root table:t handle:[1 2 3 4], keep order:false, desc:false",
	))
	tk.MustExec("update t set b = b + 1 where a in (1, 2, 3, 4)")
	tk.MustQuery("select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows("1 2", "2 3", "3 4", "4 5"))

	tk.MustQuery("explain format = 'brief' delete from t where a in (1, 2, 3, 4)").Check(testkit.Rows(
		"Delete N/A root  N/A]\n[└─Batch_Point_Get 4.00 root table:t handle:[1 2 3 4], keep order:false, desc:false",
	))
	tk.MustExec("delete from t where a in (1, 2, 3, 4)")
	tk.MustQuery("select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows())

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, primary key (a, b)) PARTITION BY HASH(a) PARTITIONS 4")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	tk.MustQuery("explain format = 'brief' select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").Check(testkit.Rows(
		"Batch_Point_Get 4.00 root table:t, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").
		Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3", "4 4 4"))

	tk.MustQuery("explain format = 'brief' update t set c = c + 1 where (a,b) in ((1,1),(2,2),(3,3),(4,4))").Check(testkit.Rows(
		"Update N/A root  N/A]\n[└─Batch_Point_Get 4.00 root table:t, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	tk.MustExec("update t set c = c + 1 where (a,b) in ((1,1),(2,2),(3,3),(4,4))")
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").Sort().
		Check(testkit.Rows("1 1 2", "2 2 3", "3 3 4", "4 4 5"))

	tk.MustQuery("explain format = 'brief' delete from t where (a,b) in ((1,1),(2,2),(3,3),(4,4))").Check(testkit.Rows(
		"Delete N/A root  N/A]\n[└─Batch_Point_Get 4.00 root table:t, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	tk.MustExec("delete from t where (a,b) in ((1,1),(2,2),(3,3),(4,4))")
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").Check(testkit.Rows())
}

func (s *testPointGetSuite) TestIssue19141(c *C) {
	// For issue 19141, fix partition selection on batch point get.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t19141 (c_int int, primary key (c_int)) partition by hash ( c_int ) partitions 4")
	tk.MustExec("insert into t19141 values (1), (2), (3), (4)")
	tk.MustQuery("select * from t19141 partition (p0)").Check(testkit.Rows("4"))
	tk.MustQuery("select * from t19141 partition (p0) where c_int = 1").Check(testkit.Rows())
	tk.MustExec("update t19141 partition (p0) set c_int = -c_int where c_int = 1") // TableDual after partition selection.
	tk.MustQuery("select * from t19141 order by c_int").Check(testkit.Rows("1", "2", "3", "4"))

	// Bach point get
	tk.MustQuery("select * from t19141 partition (p0, p2) where c_int in (1,2,3)").Check(testkit.Rows("2"))
	tk.MustExec("update t19141 partition (p1) set c_int = -c_int where c_int in (2,3)") // No data changed
	tk.MustQuery("select * from t19141 order by c_int").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("delete from t19141 partition (p0) where c_int in (2,3)") // No data changed
	tk.MustQuery("select * from t19141 order by c_int").Check(testkit.Rows("1", "2", "3", "4"))
}

func (s *testPointGetSuite) TestSelectInMultiColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b int, c int, primary key(a, b, c));")
	tk.MustExec("insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	tk.MustQuery("select * from t2 where (a, b, c) in ((1, 1, 1));").Check(testkit.Rows("1 1 1"))

	_, err := tk.Exec("select * from t2 where (a, b, c) in ((1, 1, 1, 1));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[expression:1241]Operand should contain 3 column(s)")

	_, err = tk.Exec("select * from t2 where (a, b, c) in ((1, 1, 1), (2, 2, 2, 2));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[expression:1241]Operand should contain 3 column(s)")

	_, err = tk.Exec("select * from t2 where (a, b, c) in ((1, 1), (2, 2, 2));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[expression:1241]Operand should contain 3 column(s)")
}

func (s *testPointGetSuite) TestUpdateWithTableReadLockWillFail(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table tbllock(id int, c int);")
	tk.MustExec("insert into tbllock values(1, 2), (2, 2);")
	tk.MustExec("lock table tbllock read;")
	_, err := tk.Exec("update tbllock set c = 3 where id = 2;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schema:1099]Table 'tbllock' was locked with a READ lock and can't be updated")
}

func (s *testPointGetSuite) TestIssue20692(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int, vv int, vvv int, unique key u0(id, v, vv));")
	tk.MustExec("insert into t values(1, 1, 1, 1);")
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	tk1 := testkit.NewTestKitWithSession(c, s.store, se1)
	se2, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	tk2 := testkit.NewTestKitWithSession(c, s.store, se2)
	se3, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	tk3 := testkit.NewTestKitWithSession(c, s.store, se3)
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
		c.Fail()
	case <-time.After(50 * time.Millisecond):
	}

	tk2.MustExec("commit;")
	<-stop2
	tk3.MustExec("commit;")
	tk3.MustQuery("select * from t;").Check(testkit.Rows("10 20 30 40"))
}

func (s *testPointGetSuite) TestPointGetWithInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, unique(c1))")
	tk.MustExec("alter table t alter index c1 invisible")
	tk.MustQuery("explain format = 'brief' select * from t where c1 = 10").Check(testkit.Rows(
		"TableReader 1.00 root  data:Selection",
		"└─Selection 1.00 cop[tikv]  eq(test.t.c1, 10)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
}

func (s *testPointGetSuite) TestBatchPointGetWithInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, unique(c1))")
	tk.MustExec("alter table t alter index c1 invisible")
	tk.MustQuery("explain format = 'brief' select * from t where c1 in (10, 20)").Check(testkit.Rows(
		"TableReader 2.00 root  data:Selection",
		"└─Selection 2.00 cop[tikv]  in(test.t.c1, 10, 20)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
}

func (s *testPointGetSuite) TestCBOShouldNotUsePointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop tables if exists t1, t2, t3, t4, t5")
	tk.MustExec("create table t1(id varchar(20) primary key)")
	tk.MustExec("create table t2(id varchar(20), unique(id))")
	tk.MustExec("create table t3(id varchar(20), d varchar(20), unique(id, d))")
	tk.MustExec("create table t4(id int, d varchar(20), c varchar(20), unique(id, d))")
	tk.MustExec("create table t5(id bit(64) primary key)")
	tk.MustExec("insert into t1 values('asdf'), ('1asdf')")
	tk.MustExec("insert into t2 values('asdf'), ('1asdf')")
	tk.MustExec("insert into t3 values('asdf', 't'), ('1asdf', 't')")
	tk.MustExec("insert into t4 values(1, 'b', 'asdf'), (1, 'c', 'jkl'), (1, 'd', '1jkl')")
	tk.MustExec("insert into t5 values(48)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		res := tk.MustQuery(sql)
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = s.testData.ConvertRowsToStrings(plan.Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testPointGetSuite) TestPointGetWithIndexHints(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	// point get
	tk.MustExec("create table t(a int, b int, unique index ab(a, b), unique index ba(b, a))")
	tk.MustQuery("explain format='brief' select a, b from t where a=1 and b=1").Check(testkit.Rows("Point_Get 1.00 root table:t, index:ab(a, b) "))
	tk.MustQuery("explain format='brief' select a, b from t use index(ba) where a=1 and b=1").Check(testkit.Rows("Point_Get 1.00 root table:t, index:ba(b, a) "))
	tk.MustQuery("explain format='brief' select a, b from t ignore index(ab, ba) where a=1 and b=1").Check(testkit.Rows(
		"TableReader 1.00 root  data:Selection",
		"└─Selection 1.00 cop[tikv]  eq(test.t.a, 1), eq(test.t.b, 1)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// batch get
	tk.MustQuery("explain format='brief' select a, b from t where (a=1 and b=1) or (a=2 and b=2)").Check(testkit.Rows("Batch_Point_Get 2.00 root table:t, index:ab(a, b) keep order:false, desc:false"))
	tk.MustQuery("explain format='brief' select a, b from t use index(ba) where (a=1 and b=1) or (a=2 and b=2)").Check(testkit.Rows("Batch_Point_Get 2.00 root table:t, index:ba(b, a) keep order:false, desc:false"))
	tk.MustQuery("explain format='brief' select a, b from t ignore index(ab, ba) where (a=1 and b=1) or (a=2 and b=2)").Check(testkit.Rows(
		"TableReader 2.00 root  data:Selection",
		"└─Selection 2.00 cop[tikv]  or(and(eq(test.t.a, 1), eq(test.t.b, 1)), and(eq(test.t.a, 2), eq(test.t.b, 2)))",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("explain format='brief' select a, b from t where (a, b) in ((1, 1), (2, 2))").Check(testkit.Rows("Batch_Point_Get 2.00 root table:t, index:ab(a, b) keep order:false, desc:false"))
	tk.MustQuery("explain format='brief' select a, b from t use index(ba) where (a, b) in ((1, 1), (2, 2))").Check(testkit.Rows("Batch_Point_Get 2.00 root table:t, index:ba(b, a) keep order:false, desc:false"))
	tk.MustQuery("explain format='brief' select a, b from t ignore index(ab, ba) where (a, b) in ((1, 1), (2, 2))").Check(testkit.Rows(
		"TableReader 2.00 root  data:Selection",
		"└─Selection 2.00 cop[tikv]  or(and(eq(test.t.a, 1), eq(test.t.b, 1)), and(eq(test.t.a, 2), eq(test.t.b, 2)))",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	// primary key
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int primary key, b int, unique index ab(a, b))")
	tk.MustQuery("explain format='brief' select a from t1 where a=1").Check(testkit.Rows("Point_Get 1.00 root table:t1 handle:1"))
	tk.MustQuery("explain format='brief' select a from t1 use index(ab) where a=1").Check(testkit.Rows(
		"IndexReader 10.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 10.00 cop[tikv] table:t1, index:ab(a, b) range:[1,1], keep order:false, stats:pseudo"))

	// other cases
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a int, b int, unique index aa(a), unique index bb(b))")
	tk.MustQuery("explain format='brief' select a from t2 ignore index(bb) where a=1").Check(testkit.Rows("Point_Get 1.00 root table:t2, index:aa(a) "))
	tk.MustQuery("explain format='brief' select a from t2 use index(bb) where a=1").Check(testkit.Rows(
		"IndexLookUp 1.00 root  ",
		"├─IndexFullScan(Build) 10000.00 cop[tikv] table:t2, index:bb(b) keep order:false, stats:pseudo",
		"└─Selection(Probe) 1.00 cop[tikv]  eq(test.t2.a, 1)",
		"  └─TableRowIDScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
}

func (s *testPointGetSuite) TestIssue18042(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), index ab(a, b));")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	tk.MustExec("SELECT /*+ MAX_EXECUTION_TIME(100), MEMORY_QUOTA(1 MB) */ * FROM t where a = 1;")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemQuotaQuery, Equals, int64(1<<20))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MaxExecutionTime, Equals, uint64(100))
	tk.MustExec("drop table t")
}

func (s *testPointGetSuite) TestIssue23511(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("CREATE TABLE `t1`  (`COL1` bit(11) NOT NULL,PRIMARY KEY (`COL1`));")
	tk.MustExec("CREATE TABLE `t2`  (`COL1` bit(11) NOT NULL);")
	tk.MustExec("insert into t1 values(b'00000111001'), (b'00000000000');")
	tk.MustExec("insert into t2 values(b'00000111001');")
	tk.MustQuery("select * from t1 where col1 = 0x39;").Check(testkit.Rows("\x009"))
	tk.MustQuery("select * from t2 where col1 = 0x39;").Check(testkit.Rows("\x009"))
	tk.MustQuery("select * from t1 where col1 = 0x00;").Check(testkit.Rows("\x00\x00"))
	tk.MustQuery("select * from t1 where col1 = 0x0000;").Check(testkit.Rows("\x00\x00"))
	tk.MustQuery("explain format = 'brief' select * from t1 where col1 = 0x39;").
		Check(testkit.Rows("Point_Get 1.00 root table:t1, index:PRIMARY(COL1) "))
	tk.MustQuery("select * from t1 where col1 = 0x0039;").Check(testkit.Rows("\x009"))
	tk.MustQuery("select * from t2 where col1 = 0x0039;").Check(testkit.Rows("\x009"))
	tk.MustQuery("select * from t1 where col1 = 0x000039;").Check(testkit.Rows("\x009"))
	tk.MustQuery("select * from t2 where col1 = 0x000039;").Check(testkit.Rows("\x009"))
	tk.MustExec("drop table t1, t2;")
}
