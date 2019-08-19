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
	"fmt"
	"math"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	dto "github.com/prometheus/client_model/go"
)

var _ = Suite(&testPointGetSuite{})

type testPointGetSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testPointGetSuite) TestPointGetPlanCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
		core.PreparedPlanCacheCapacity = orgCapacity
		core.PreparedPlanCacheMemoryGuardRatio = orgMemGuardRatio
		core.PreparedPlanCacheMaxMemory = orgMaxMemory
	}()
	core.SetPreparedPlanCache(true)
	core.PreparedPlanCacheCapacity = 100
	core.PreparedPlanCacheMemoryGuardRatio = 0.1
	// PreparedPlanCacheMaxMemory is set to MAX_UINT64 to make sure the cache
	// behavior would not be effected by the uncertain memory utilization.
	core.PreparedPlanCacheMaxMemory.Store(math.MaxUint64)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, c int, key idx_bc(b,c))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("explain select * from t where a = 1").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, handle:1",
	))
	tk.MustQuery("explain select * from t where 1 = a").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, handle:1",
	))
	tk.MustQuery("explain update t set b=b+1, c=c+1 where a = 1").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, handle:1",
	))
	tk.MustQuery("explain delete from t where a = 1").Check(testkit.Rows(
		"Point_Get_1 1.00 root table:t, handle:1",
	))
	tk.MustQuery("explain select a from t where a = -1").Check(testkit.Rows(
		"TableDual_5 0.00 root rows:0"))
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
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(0))
	tk.MustExec("set @param=2")
	tk.MustQuery("execute stmt1 using @param").Check(testkit.Rows("2 2 2"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(1))
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("2 2 2"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(1))
	tk.MustExec("set @param=1")
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("1 1 1"))
	counter.Write(pb)
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
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustExec("set @param=4")
	tk.MustExec("execute stmt4 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 4",
	))
	counter.Write(pb)
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
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustExec("set @param=2")
	tk.MustExec("execute stmt6 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
	))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustExec("insert into t (a, b, c) values (18446744073709551615, 4, 4)")
	tk.MustExec("set @p1=-1")
	tk.MustExec("set @p2=1")
	tk.MustExec(`prepare stmt7 from "select a from t where a = ?"`)
	tk.MustQuery("execute stmt7 using @p1").Check(testkit.Rows())
	tk.MustQuery("execute stmt7 using @p2").Check(testkit.Rows("1"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
}

func (s *testPointGetSuite) TestPointGetForUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table fu (id int primary key)")
	tk.MustExec("insert into fu values (6)")
	tk.MustQuery("select * from fu where id = 6 for update").Check(testkit.Rows("6"))

	// In autocommit mode, outside a transaction, "for update" doesn't take effect.
	res := tk.MustQuery("explain select * from fu where id = 6 for update")
	// Point_Get_1	1.00	root	table:fu, handle:6
	opInfo := res.Rows()[0][3]
	selectLock := strings.Contains(fmt.Sprintf("%s", opInfo), "lock")
	c.Assert(selectLock, IsFalse)

	tk.MustExec("begin")
	tk.MustQuery("select * from fu where id = 6 for update").Check(testkit.Rows("6"))
	res = tk.MustQuery("explain select * from fu where id = 6 for update")
	// Point_Get_1	1.00	root	table:fu, handle:6
	opInfo = res.Rows()[0][3]
	selectLock = strings.Contains(fmt.Sprintf("%s", opInfo), "lock")
	c.Assert(selectLock, IsTrue)

	tk.MustExec("rollback")

}
