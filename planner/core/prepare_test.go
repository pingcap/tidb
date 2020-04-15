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
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var _ = Suite(&testPrepareSuite{})
var _ = SerialSuites(&testPrepareSerialSuite{})

type testPrepareSuite struct {
}

type testPrepareSerialSuite struct {
}

func (s *testPrepareSerialSuite) TestPrepareCache(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

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

	// test privilege change
	rootSe := tk.Se
	tk.MustExec("drop table if exists tp")
	tk.MustExec(`create table tp(c1 int, c2 int, primary key (c1))`)
	tk.MustExec(`insert into tp values(1, 1), (2, 2), (3, 3)`)

	tk.MustExec(`create user 'u_tp'@'localhost'`)
	tk.MustExec(`grant select on test.tp to u_tp@'localhost';flush privileges;`)

	// user u_tp
	userSess := newSession(c, store, "test")
	c.Assert(userSess.Auth(&auth.UserIdentity{Username: "u_tp", Hostname: "localhost"}, nil, nil), IsTrue)
	mustExec(c, userSess, `prepare ps_stp_r from 'select * from tp where c1 > ?'`)
	mustExec(c, userSess, `set @p2 = 2`)
	tk.Se = userSess
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))

	// root revoke
	tk.Se = rootSe
	tk.MustExec(`revoke all on test.tp from 'u_tp'@'localhost';flush privileges;`)

	// user u_tp
	tk.Se = userSess
	_, err = tk.Exec(`execute ps_stp_r using @p2`)
	c.Assert(err, NotNil)

	// grant again
	tk.Se = rootSe
	tk.MustExec(`grant select on test.tp to u_tp@'localhost';flush privileges;`)

	// user u_tp
	tk.Se = userSess
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))

	// restore
	tk.Se = rootSe
	tk.MustExec("drop table if exists tp")
	tk.MustExec(`DROP USER 'u_tp'@'localhost';`)
}

func (s *testPrepareSerialSuite) TestPrepareCacheIndexScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

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

func (s *testPlanSerialSuite) TestPrepareCacheDeferredFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

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
		builder := core.NewPlanBuilder(tk.Se, is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Check(err, IsNil)
		execPlan, ok := p.(*core.Execute)
		c.Check(ok, IsTrue)
		executor.ResetContextOfStmt(tk.Se, stmt)
		err = execPlan.OptimizePreparedPlan(ctx, tk.Se, is)
		c.Check(err, IsNil)
		planStr[i] = core.ToString(execPlan.Plan)
		c.Check(planStr[i], Matches, expectedPattern, Commentf("for %dth %s", i, sql1))
		pb := &dto.Metric{}
		counter.Write(pb)
		cnt[i] = pb.GetCounter().GetValue()
		c.Check(cnt[i], Equals, float64(i))
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(planStr[0] < planStr[1], IsTrue, Commentf("plan 1: %v, plan 2: %v", planStr[0], planStr[1]))
}

func (s *testPrepareSerialSuite) TestPrepareCacheNow(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

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
func (s *testPrepareSerialSuite) TestPrepareTableAsNameOnGroupByWithCache(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1 (
		id int(11) unsigned not null primary key auto_increment,
		partner_id varchar(35) not null,
		t1_status_id int(10) unsigned
	  );`)
	tk.MustExec(`insert into t1 values ("1", "partner1", "10"), ("2", "partner2", "10"), ("3", "partner3", "10"), ("4", "partner4", "10");`)
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
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

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

func newSession(c *C, store kv.Storage, dbName string) session.Session {
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	mustExec(c, se, "create database if not exists "+dbName)
	mustExec(c, se, "use "+dbName)
	return se
}

func mustExec(c *C, se session.Session, sql string) {
	_, err := se.Execute(context.Background(), sql)
	c.Assert(err, IsNil)
}

func (s *testPrepareSerialSuite) TestConstPropAndPPDWithCache(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(8) not null, b varchar(8) not null)")
	tk.MustExec("insert into t values('1','1')")

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t2.b = ? and t2.b = ?"`)
	tk.MustExec("set @p0 = '1', @p1 = '2';")
	tk.MustQuery("execute stmt using @p0, @p1").Check(testkit.Rows(
		"0",
	))
	tk.MustExec("set @p0 = '1', @p1 = '1'")
	tk.MustQuery("execute stmt using @p0, @p1").Check(testkit.Rows(
		"1",
	))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and ?"`)
	tk.MustExec("set @p0 = 0")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustExec("set @p0 = 1")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where ?"`)
	tk.MustExec("set @p0 = 0")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustExec("set @p0 = 1")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t2.b = '1' and t2.b = ?"`)
	tk.MustExec("set @p0 = '1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))
	tk.MustExec("set @p0 = '2'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.a > ?"`)
	tk.MustExec("set @p0 = '1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustExec("set @p0 = '0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.b > ? and t1.b > ?"`)
	tk.MustExec("set @p0 = '0', @p1 = '0'")
	tk.MustQuery("execute stmt using @p0,@p1").Check(testkit.Rows(
		"1",
	))
	tk.MustExec("set @p0 = '0', @p1 = '1'")
	tk.MustQuery("execute stmt using @p0,@p1").Check(testkit.Rows(
		"0",
	))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.b > ? and t1.b > '1'"`)
	tk.MustExec("set @p0 = '1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustExec("set @p0 = '0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
}

func (s *testPlanSerialSuite) TestPlanCacheUnionScan(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	pb := &dto.Metric{}
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int not null)")
	tk.MustExec("create table t2(a int not null)")
	tk.MustExec("prepare stmt1 from 'select * from t1 where a > ?'")
	tk.MustExec("set @p0 = 0")
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	tk.MustExec("begin")
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt := pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(1))
	tk.MustExec("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(1))
	tk.MustExec("insert into t2 values(1)")
	// Cached plan is chosen, modification on t2 does not impact plan of t1.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(2))
	tk.MustExec("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(3))

	tk.MustExec("prepare stmt2 from 'select * from t1 left join t2 on true where t1.a > ?'")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	tk.MustExec("begin")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	tk.MustExec("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 <nil>",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	tk.MustExec("insert into t2 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	// Cached plan is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(5))
	tk.MustExec("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(6))
}

func (s *testPlanSerialSuite) TestPlanCacheHitInfo(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		store.Close()
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int)")
	tk.MustExec("insert into t values (1),(2),(3),(4)")
	tk.MustExec("prepare stmt from 'select * from t where id=?'")
	tk.MustExec("prepare stmt2 from 'select /*+ ignore_plan_cache() */ * from t where id=?'")
	tk.MustExec("set @doma = 1")
	// Test if last_statement_found_in_plan_cache is appropriately initialized.
	tk.MustQuery(`select @@last_statement_found_in_plan_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	// Test if last_statement_found_in_plan_cache is updated after a plan cache hit.
	tk.MustQuery(`select @@last_statement_found_in_plan_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt2 using @doma").Check(testkit.Rows("1"))
	// Test if last_statement_found_in_plan_cache is updated after a plan cache miss caused by a prepared statement.
	tk.MustQuery(`select @@last_statement_found_in_plan_cache`).Check(testkit.Rows("0"))
	// Test if last_statement_found_in_plan_cache is updated after a plan cache miss caused by a usual statement.
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_statement_found_in_plan_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where id=1").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_statement_found_in_plan_cache`).Check(testkit.Rows("0"))
}

func (s *testPrepareSuite) TestPrepareForGroupByMultiItems(c *C) {
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
	tk.MustExec("create table t(a int, b int, c int , index idx(a));")
	tk.MustExec("insert into t values(1,2, -1), (1,2, 1), (1,2, -1), (4,4,3);")
	tk.MustExec("set @a=1")
	tk.MustExec("set @b=3")
	tk.MustExec(`set sql_mode=""`)
	tk.MustExec(`prepare stmt from "select a, sum(b), c from t group by ?, ? order by ?, ?"`)
	tk.MustQuery("select a, sum(b), c from t group by 1,3 order by 1,3;").Check(testkit.Rows("1 4 -1", "1 2 1", "4 4 3"))
	tk.MustQuery(`execute stmt using @a, @b, @a, @b`).Check(testkit.Rows("1 4 -1", "1 2 1", "4 4 3"))

	tk.MustExec("set @c=10")
	err = tk.ExecToErr("execute stmt using @a, @c, @a, @c")
	c.Assert(err.Error(), Equals, "Unknown column '10' in 'group statement'")

	tk.MustExec("set @v1=1.0")
	tk.MustExec("set @v2=3.0")
	tk.MustExec(`prepare stmt2 from "select sum(b) from t group by ?, ?"`)
	tk.MustQuery(`execute stmt2 using @v1, @v2`).Check(testkit.Rows("10"))
}
