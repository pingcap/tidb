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
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var _ = Suite(&testPrepareSuite{})

type testPrepareSuite struct {
}

func (s *testPrepareSuite) TestPrepareCache(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		dom.Close()
		store.Close()
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
	tk.MustExec("create table t(a int primary key, b int, c int, index idx1(b, a), index idx2(b))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 1, 2)")
	tk.MustExec(`prepare stmt1 from "select * from t use index(idx1) where a = ? and b = ?"`)
	tk.MustExec(`prepare stmt2 from "select a, b from t use index(idx2) where b = ?"`)
	tk.MustExec(`prepare stmt3 from "select * from t where a = ?"`)
	tk.MustExec("set @a=1, @b=1")
	// When executing one statement at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt2 using @b").Check(testkit.Rows("1 1", "6 1"))
	tk.MustQuery("execute stmt2 using @b").Check(testkit.Rows("1 1", "6 1"))
	tk.MustQuery("execute stmt3 using @a").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt3 using @a").Check(testkit.Rows("1 1 1"))
	tk.MustExec(`prepare stmt4 from "select * from t where a > ?"`)
	tk.MustExec("set @a=3")
	tk.MustQuery("execute stmt4 using @a").Check(testkit.Rows("4 4 4", "5 5 5", "6 1 2"))
	tk.MustQuery("execute stmt4 using @a").Check(testkit.Rows("4 4 4", "5 5 5", "6 1 2"))
	tk.MustExec(`prepare stmt5 from "select c from t order by c"`)
	tk.MustQuery("execute stmt5").Check(testkit.Rows("1", "2", "2", "3", "4", "5"))
	tk.MustQuery("execute stmt5").Check(testkit.Rows("1", "2", "2", "3", "4", "5"))
	tk.MustExec(`prepare stmt6 from "select distinct a from t order by a"`)
	tk.MustQuery("execute stmt6").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	tk.MustQuery("execute stmt6").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func (s *testPrepareSuite) TestPrepareCacheIndexScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		dom.Close()
		store.Close()
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
	tk.MustExec("create table t(a int, b int, c int, primary key (a, b))")
	tk.MustExec("insert into t values(1, 1, 2), (1, 2, 3), (1, 3, 3), (2, 1, 2), (2, 2, 3), (2, 3, 3)")
	tk.MustExec(`prepare stmt1 from "select a, c from t where a = ? and c = ?"`)
	tk.MustExec("set @a=1, @b=3")
	// When executing one statement at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 3", "1 3"))
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 3", "1 3"))
}

func (s *testPlanSuite) TestPrepareCacheDeferredFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		dom.Close()
		store.Close()
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
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int PRIMARY KEY, c1 TIMESTAMP(3) NOT NULL DEFAULT '2019-01-14 10:43:20', KEY idx1 (c1))")
	tk.MustExec("prepare sel1 from 'select id, c1 from t1 where c1 < now(3)'")

	sql1 := "execute sel1"
	expectedPattern := `IndexReader\(Index\(t1.idx1\)\[\[-inf,[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]\)\]\)`

	var cnt [2]float64
	var planStr [2]string
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	ctx := context.TODO()
	for i := 0; i < 2; i++ {
		stmt, err := s.ParseOneStmt(sql1, "", "")
		c.Check(err, IsNil)
		is := tk.Se.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
		builder := core.NewPlanBuilder(tk.Se, is)
		p, err := builder.Build(ctx, stmt)
		c.Check(err, IsNil)
		execPlan, ok := p.(*core.Execute)
		c.Check(ok, IsTrue)
		executor.ResetContextOfStmt(tk.Se, stmt)
		err = execPlan.OptimizePreparedPlan(ctx, tk.Se, is)
		c.Check(err, IsNil)
		planStr[i] = core.ToString(execPlan.Plan)
		c.Check(planStr[i], Matches, expectedPattern, Commentf("for %s", sql1))
		pb := &dto.Metric{}
		counter.Write(pb)
		cnt[i] = pb.GetCounter().GetValue()
		c.Check(cnt[i], Equals, float64(i))
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(planStr[0] < planStr[1], IsTrue, Commentf("plan 1: %v, plan 2: %v", planStr[0], planStr[1]))
}

func (s *testPrepareSuite) TestPrepareCacheNow(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		dom.Close()
		store.Close()
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
	tk.MustExec(`prepare stmt1 from "select now(), current_timestamp(), utc_timestamp(), unix_timestamp(), sleep(0.1), now(), current_timestamp(), utc_timestamp(), unix_timestamp()"`)
	// When executing one statement at the first time, we don't usTestPrepareCacheDeferredFunctione cache, so we need to execute it at least twice to test the cache.
	_ = tk.MustQuery("execute stmt1").Rows()
	rs := tk.MustQuery("execute stmt1").Rows()
	c.Assert(rs[0][0].(string), Equals, rs[0][5].(string))
	c.Assert(rs[0][1].(string), Equals, rs[0][6].(string))
	c.Assert(rs[0][2].(string), Equals, rs[0][7].(string))
	c.Assert(rs[0][3].(string), Equals, rs[0][8].(string))
}

func (s *testPrepareSuite) TestPrepareOverMaxPreparedStmtCount(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")

	// test prepare and deallocate.
	prePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	tk.MustExec(`prepare stmt1 from "select 1"`)
	onePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared+1, Equals, onePrepared)
	tk.MustExec(`deallocate prepare stmt1`)
	deallocPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared, Equals, deallocPrepared)

	// test change global limit and make it affected in test session.
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("-1"))
	tk.MustExec("set @@global.max_prepared_stmt_count = 2")
	tk.MustQuery("select @@global.max_prepared_stmt_count").Check(testkit.Rows("2"))

	// Disable global variable cache, so load global session variable take effect immediate.
	dom.GetGlobalVarsCache().Disable()

	// test close session to give up all prepared stmt
	tk.MustExec(`prepare stmt2 from "select 1"`)
	prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
	tk.Se.Close()
	drawPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared-1, Equals, drawPrepared)

	// test meet max limit.
	tk.Se = nil
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("2"))
	for i := 1; ; i++ {
		prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
		if prePrepared >= 2 {
			_, err = tk.Exec(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
			c.Assert(terror.ErrorEqual(err, variable.ErrMaxPreparedStmtCountReached), IsTrue)
			break
		}
		tk.Exec(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
	}
}

// unit test for issue https://github.com/pingcap/tidb/issues/8518
func (s *testPrepareSuite) TestPrepareTableAsNameOnGroupByWithCache(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		dom.Close()
		store.Close()
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
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (
		id int(11) unsigned not null primary key auto_increment,
		partner_id varchar(35) not null,
		t1_status_id int(10) unsigned
	  );`)
	tk.MustExec(`insert into t1 values ("1", "partner1", "10"), ("2", "partner2", "10"), ("3", "partner3", "10"), ("4", "partner4", "10");"`)
	tk.MustExec("drop table if exists t3")
	tk.MustExec(`create table t3 (
		id int(11) not null default '0',
		preceding_id int(11) not null default '0',
		primary key  (id,preceding_id)
	  );`)
	tk.MustExec(`prepare stmt from 'SELECT DISTINCT t1.partner_id
	FROM t1
		LEFT JOIN t3 ON t1.id = t3.id
		LEFT JOIN t1 pp ON pp.id = t3.preceding_id
	GROUP BY t1.id ;'`)
	tk.MustQuery("execute stmt").Sort().Check(testkit.Rows("partner1", "partner2", "partner3", "partner4"))
}

func readGaugeInt(g prometheus.Gauge) int {
	ch := make(chan prometheus.Metric, 1)
	g.Collect(ch)
	m := <-ch
	mm := &dto.Metric{}
	m.Write(mm)
	return int(mm.GetGauge().GetValue())
}

// unit test for issue https://github.com/pingcap/tidb/issues/9478
func (s *testPrepareSuite) TestPrepareWithWindowFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("use test")
	tk.MustExec("create table window_prepare(a int, b double)")
	tk.MustExec("insert into window_prepare values(1, 1.1), (2, 1.9)")
	tk.MustExec("prepare stmt1 from 'select row_number() over() from window_prepare';")
	// Test the unnamed window can be executed successfully.
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1", "2"))
	// Test the stmt can be prepared successfully.
	tk.MustExec("prepare stmt2 from 'select count(a) over (order by a rows between ? preceding and ? preceding) from window_prepare'")
	tk.MustExec("set @a=0, @b=1;")
	tk.MustQuery("execute stmt2 using @a, @b").Check(testkit.Rows("0", "0"))
}

func (s *testPrepareSuite) TestPrepareForGroupByItems(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, v int)")
	tk.MustExec("insert into t(id, v) values(1, 2),(1, 2),(2, 3);")
	tk.MustExec("prepare s1 from 'select max(v) from t group by floor(id/?)';")
	tk.MustExec("set @a=2;")
	tk.MustQuery("execute s1 using @a;").Sort().Check(testkit.Rows("2", "3"))

	tk.MustExec("prepare s1 from 'select max(v) from t group by ?';")
	tk.MustExec("set @a=2;")
	err = tk.ExecToErr("execute s1 using @a;")
	c.Assert(err.Error(), Equals, "Unknown column '2' in 'group statement'")
	tk.MustExec("set @a=2.0;")
	tk.MustQuery("execute s1 using @a;").Check(testkit.Rows("3"))
}

func (s *testPrepareSuite) TestPrepareCacheForPartition(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	orgCapacity := core.PreparedPlanCacheCapacity
	orgMemGuardRatio := core.PreparedPlanCacheMemoryGuardRatio
	orgMaxMemory := core.PreparedPlanCacheMaxMemory
	defer func() {
		dom.Close()
		store.Close()
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
	// Test for PointGet and IndexRead.
	tk.MustExec("drop table if exists t_index_read")
	tk.MustExec("create table t_index_read (id int, k int, c varchar(10), primary key (id, k)) partition by hash(id+k) partitions 10")
	tk.MustExec("insert into t_index_read values (1, 2, 'abc'), (3, 4, 'def'), (5, 6, 'xyz')")
	tk.MustExec("prepare stmt1 from 'select c from t_index_read where id = ? and k = ?;'")
	tk.MustExec("set @id=1, @k=2")
	// When executing one statement at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt1 using @id, @k").Check(testkit.Rows("abc"))
	tk.MustQuery("execute stmt1 using @id, @k").Check(testkit.Rows("abc"))
	tk.MustExec("set @id=5, @k=6")
	tk.MustQuery("execute stmt1 using @id, @k").Check(testkit.Rows("xyz"))
	tk.MustExec("prepare stmt2 from 'select c from t_index_read where id = ? and k = ? and 1 = 1;'")
	tk.MustExec("set @id=1, @k=2")
	tk.MustQuery("execute stmt2 using @id, @k").Check(testkit.Rows("abc"))
	tk.MustQuery("execute stmt2 using @id, @k").Check(testkit.Rows("abc"))
	tk.MustExec("set @id=5, @k=6")
	tk.MustQuery("execute stmt2 using @id, @k").Check(testkit.Rows("xyz"))
	// Test for TableScan.
	tk.MustExec("drop table if exists t_table_read")
	tk.MustExec("create table t_table_read (id int, k int, c varchar(10), primary key(id)) partition by hash(id) partitions 10")
	tk.MustExec("insert into t_table_read values (1, 2, 'abc'), (3, 4, 'def'), (5, 6, 'xyz')")
	tk.MustExec("prepare stmt3 from 'select c from t_index_read where id = ?;'")
	tk.MustExec("set @id=1")
	// When executing one statement at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt3 using @id").Check(testkit.Rows("abc"))
	tk.MustQuery("execute stmt3 using @id").Check(testkit.Rows("abc"))
	tk.MustExec("set @id=5")
	tk.MustQuery("execute stmt3 using @id").Check(testkit.Rows("xyz"))
	tk.MustExec("prepare stmt4 from 'select c from t_index_read where id = ? and k = ?'")
	tk.MustExec("set @id=1, @k=2")
	tk.MustQuery("execute stmt4 using @id, @k").Check(testkit.Rows("abc"))
	tk.MustQuery("execute stmt4 using @id, @k").Check(testkit.Rows("abc"))
	tk.MustExec("set @id=5, @k=6")
	tk.MustQuery("execute stmt4 using @id, @k").Check(testkit.Rows("xyz"))
	// Query on range partition tables should not raise error.
	tk.MustExec("create table t_range_index (id int, k int, c varchar(10), primary key(id)) partition by range(id) ( PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN (14),PARTITION p2 VALUES LESS THAN (20) )")
	tk.MustExec("insert into t_range_index values (1, 2, 'abc'), (5, 4, 'def'), (13, 6, 'xyz'), (17, 6, 'hij')")
	tk.MustExec("prepare stmt5 from 'select c from t_range_index where id = ?'")
	tk.MustExec("set @id=1")
	tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("abc"))
	tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("abc"))
	tk.MustExec("set @id=5")
	tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("def"))
	tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("def"))
	tk.MustExec("set @id=13")
	tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("xyz"))
	tk.MustExec("set @id=17")
	tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("hij"))

	tk.MustExec("create table t_range_table (id int, k int, c varchar(10)) partition by range(id) ( PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN (14),PARTITION p2 VALUES LESS THAN (20) )")
	tk.MustExec("insert into t_range_table values (1, 2, 'abc'), (5, 4, 'def'), (13, 6, 'xyz'), (17, 6, 'hij')")
	tk.MustExec("prepare stmt6 from 'select c from t_range_table where id = ?'")
	tk.MustExec("set @id=1")
	tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("abc"))
	tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("abc"))
	tk.MustExec("set @id=5")
	tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("def"))
	tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("def"))
	tk.MustExec("set @id=13")
	tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("xyz"))
	tk.MustExec("set @id=17")
	tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("hij"))
}
