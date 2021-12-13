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
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/israce"
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
		err = store.Close()
		c.Assert(err, IsNil)
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
	tk.MustExec(`grant select on test.tp to u_tp@'localhost';`)

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
	tk.MustExec(`revoke all on test.tp from 'u_tp'@'localhost';`)

	// user u_tp
	tk.Se = userSess
	_, err = tk.Exec(`execute ps_stp_r using @p2`)
	c.Assert(err, NotNil)

	// grant again
	tk.Se = rootSe
	tk.MustExec(`grant select on test.tp to u_tp@'localhost';`)

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
		err = store.Close()
		c.Assert(err, IsNil)
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

// dtype: tinyint, unsigned, float, decimal, year
// rtype: null, valid, out-of-range, invalid, str, exists
func randValue(tk *testkit.TestKit, tbl, col, dtype, rtype string) string {
	if rtype == "" {
		rtypes := []string{"null", "valid", "out-of-range", "invalid", "str", "exists"}
		rtype = rtypes[rand.Intn(len(rtypes))]
	}
	if rtype == "null" {
		return "null"
	}
	if rtype == "exists" {
		res := tk.MustQuery(fmt.Sprintf("select %v from %v limit 1", col, tbl)).Rows()[0][0].(string)
		if res == "<nil>" {
			res = "null"
		}
		return res
	}
	switch dtype {
	case "tinyint":
		switch rtype {
		case "valid":
			return fmt.Sprintf("%v", -128+rand.Intn(256))
		case "out-of-range":
			return fmt.Sprintf("%v", 128+rand.Intn(1024))
		case "invalid":
			return "'invalid-tinyint'"
		case "str":
			return fmt.Sprintf("'%v'", -128+rand.Intn(256))
		}
	case "unsigned":
		switch rtype {
		case "valid":
			return fmt.Sprintf("%v", rand.Intn(4294967295))
		case "out-of-range":
			return fmt.Sprintf("-%v", rand.Intn(4294967295))
		case "invalid":
			return "'invalid-unsigned-int'"
		case "str":
			return fmt.Sprintf("'%v'", rand.Intn(4294967295))
		}
	case "float":
		switch rtype {
		case "valid":
			return fmt.Sprintf("%v%.4fE%v", []string{"+", "-"}[rand.Intn(2)], rand.Float32(), rand.Intn(38))
		case "out-of-range":
			return fmt.Sprintf("%v%.4fE%v", []string{"+", "-"}[rand.Intn(2)], rand.Float32(), rand.Intn(100)+38)
		case "invalid":
			return "'invalid-float'"
		case "str":
			return fmt.Sprintf("'%v%.4fE%v'", []string{"+", "-"}[rand.Intn(2)], rand.Float32(), rand.Intn(38))
		}
	case "decimal": // (10,2)
		switch rtype {
		case "valid":
			return fmt.Sprintf("%v%v.%v", []string{"+", "-"}[rand.Intn(2)], rand.Intn(99999999), rand.Intn(100))
		case "out-of-range":
			switch rand.Intn(2) {
			case 0:
				return fmt.Sprintf("%v%v.%v", []string{"+", "-"}[rand.Intn(2)], rand.Intn(99999999), rand.Intn(100000)+100000)
			case 1:
				return fmt.Sprintf("%v%v.%v", []string{"+", "-"}[rand.Intn(2)], rand.Intn(99999999)+99999999+1, rand.Intn(100))
			}
		case "invalid":
			return "'invalid-decimal'"
		case "str":
			return fmt.Sprintf("'%v%v.%v'", []string{"+", "-"}[rand.Intn(2)], rand.Intn(99999999), rand.Intn(100))
		}
	case "year":
		switch rtype {
		case "valid":
			return fmt.Sprintf("%v", 1901+rand.Intn(2155-1901))
		case "out-of-range":
			return fmt.Sprintf("%v", 2156+rand.Intn(2155-1901))
		case "invalid":
			return "'invalid-year'"
		case "str":
			return fmt.Sprintf("'%v'", 1901+rand.Intn(2155-1901))
		}
	}
	return "'invalid-type-" + dtype + "'"
}

func (s *testPrepareSerialSuite) TestPrepareCacheChangingParamType(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t_tinyint, t_unsigned, t_float, t_decimal, t_year`)
	tk.MustExec(`create table t_tinyint (a tinyint, b tinyint, key(a))`)
	tk.MustExec(`create table t_unsigned (a int unsigned, b int unsigned, key(a))`)
	tk.MustExec(`create table t_float(a float, b float, key(a))`)
	tk.MustExec(`create table t_decimal(a decimal(10,2), b decimal(10,2), key(a))`)
	tk.MustExec(`create table t_year(a year, b year, key(a))`)
	for _, dtype := range []string{"tinyint", "unsigned", "float", "decimal", "year"} {
		tbl := "t_" + dtype
		for i := 0; i < 10; i++ {
			tk.MustExec(fmt.Sprintf("insert into %v values (%v, %v)", tbl, randValue(nil, "", "", dtype, "valid"), randValue(nil, "", "", dtype, "valid")))
		}
		tk.MustExec(fmt.Sprintf("insert into %v values (null, null)", tbl))
		tk.MustExec(fmt.Sprintf("insert into %v values (%v, null)", tbl, randValue(nil, "", "", dtype, "valid")))
		tk.MustExec(fmt.Sprintf("insert into %v values (null, %v)", tbl, randValue(nil, "", "", dtype, "valid")))

		for round := 0; round < 10; round++ {
			tk.MustExec(fmt.Sprintf(`prepare s1 from 'select * from %v where a=?'`, tbl))
			tk.MustExec(fmt.Sprintf(`prepare s2 from 'select * from %v where b=?'`, tbl))
			tk.MustExec(fmt.Sprintf(`prepare s3 from 'select * from %v where a in (?, ?, ?)'`, tbl))
			tk.MustExec(fmt.Sprintf(`prepare s4 from 'select * from %v where b in (?, ?, ?)'`, tbl))
			tk.MustExec(fmt.Sprintf(`prepare s5 from 'select * from %v where a>?'`, tbl))
			tk.MustExec(fmt.Sprintf(`prepare s6 from 'select * from %v where b>?'`, tbl))
			tk.MustExec(fmt.Sprintf(`prepare s7 from 'select * from %v where a>? and b>?'`, tbl))

			for query := 0; query < 10; query++ {
				a1, a2, a3 := randValue(tk, tbl, "a", dtype, ""), randValue(tk, tbl, "a", dtype, ""), randValue(tk, tbl, "a", dtype, "")
				b1, b2, b3 := randValue(tk, tbl, "b", dtype, ""), randValue(tk, tbl, "b", dtype, ""), randValue(tk, tbl, "b", dtype, "")
				tk.MustExec(fmt.Sprintf(`set @a1=%v,@a2=%v,@a3=%v`, a1, a2, a3))
				tk.MustExec(fmt.Sprintf(`set @b1=%v,@b2=%v,@b3=%v`, b1, b2, b3))

				compareResult := func(sql1, sql2 string) {
					raw, err := tk.Exec(sql1)
					if err != nil {
						err := tk.ExecToErr(sql2)
						c.Assert(err, NotNil)
						return
					}
					rs := tk.ResultSetToResult(raw, Commentf("sql1:%s, sql2:%v", sql1, sql2))
					rs.Sort().Check(tk.MustQuery(sql2).Sort().Rows())
				}

				compareResult(`execute s1 using @a1`, fmt.Sprintf(`select * from %v where a=%v`, tbl, a1))
				compareResult(`execute s2 using @b1`, fmt.Sprintf(`select * from %v where b=%v`, tbl, b1))
				compareResult(`execute s3 using @a1,@a2,@a3`, fmt.Sprintf(`select * from %v where a in (%v,%v,%v)`, tbl, a1, a2, a3))
				compareResult(`execute s4 using @b1,@b2,@b3`, fmt.Sprintf(`select * from %v where b in (%v,%v,%v)`, tbl, b1, b2, b3))
				compareResult(`execute s5 using @a1`, fmt.Sprintf(`select * from %v where a>%v`, tbl, a1))
				compareResult(`execute s6 using @b1`, fmt.Sprintf(`select * from %v where b>%v`, tbl, b1))
				compareResult(`execute s7 using @a1,@b1`, fmt.Sprintf(`select * from %v where a>%v and b>%v`, tbl, a1, b1))
			}
		}
	}
}

func (s *testPrepareSerialSuite) TestPrepareCacheChangeCharsetCollation(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a varchar(64))`)
	tk.MustExec(`set character_set_connection=utf8`)

	tk.MustExec(`prepare s from 'select * from t where a=?'`)
	tk.MustExec(`set @x='a'`)
	tk.MustExec(`execute s using @x`)
	tk.MustExec(`set @x='b'`)
	tk.MustExec(`execute s using @x`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`set character_set_connection=latin1`)
	tk.MustExec(`set @x='c'`)
	tk.MustExec(`execute s using @x`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // cannot reuse the previous plan since the charset is changed
	tk.MustExec(`set @x='d'`)
	tk.MustExec(`execute s using @x`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`set collation_connection=binary`)
	tk.MustExec(`set @x='e'`)
	tk.MustExec(`execute s using @x`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // cannot reuse the previous plan since the collation is changed
	tk.MustExec(`set @x='f'`)
	tk.MustExec(`execute s using @x`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestPrepareCacheDeferredFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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
	expectedPattern := `IndexReader\(Index\(t1.idx1\)\[\[-inf,[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]\)\]\)->Sel\(\[lt\(test.t1.c1, now\(3\)\)\]\)`

	var cnt [2]float64
	var planStr [2]string
	metrics.ResettablePlanCacheCounterFortTest = true
	metrics.PlanCacheCounter.Reset()
	counter := metrics.PlanCacheCounter.WithLabelValues("prepare")
	ctx := context.TODO()
	for i := 0; i < 2; i++ {
		stmt, err := s.ParseOneStmt(sql1, "", "")
		c.Check(err, IsNil)
		is := tk.Se.GetInfoSchema().(infoschema.InfoSchema)
		builder, _ := core.NewPlanBuilder().Init(tk.Se, is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Check(err, IsNil)
		execPlan, ok := p.(*core.Execute)
		c.Check(ok, IsTrue)
		err = executor.ResetContextOfStmt(tk.Se, stmt)
		c.Assert(err, IsNil)
		err = execPlan.OptimizePreparedPlan(ctx, tk.Se, is)
		c.Check(err, IsNil)
		planStr[i] = core.ToString(execPlan.Plan)
		c.Check(planStr[i], Matches, expectedPattern, Commentf("for %dth %s", i, sql1))
		pb := &dto.Metric{}
		err = counter.Write(pb)
		c.Assert(err, IsNil)
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
		err = store.Close()
		c.Assert(err, IsNil)
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

func (s *testPrepareSerialSuite) TestPrepareOverMaxPreparedStmtCount(c *C) {
	c.Skip("unstable, skip it and fix it before 20210705")
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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
		_, err = tk.Exec(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
		c.Assert(err, IsNil)
	}
}

// unit test for issue https://github.com/pingcap/tidb/issues/8518
func (s *testPrepareSerialSuite) TestPrepareTableAsNameOnGroupByWithCache(c *C) {
	c.Skip("unstable, skip it and fix it before 20210702")
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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

func (s *testPrepareSerialSuite) TestPrepareCachePointGetInsert(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1 (a int, b int, primary key(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")

	tk.MustExec("create table t2 (a int, b int, primary key(a))")
	tk.MustExec(`prepare stmt1 from "insert into t2 select * from t1 where a=?"`)

	tk.MustExec("set @a=1")
	tk.MustExec("execute stmt1 using @a")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("select * from t2 order by a").Check(testkit.Rows("1 1"))

	tk.MustExec("set @a=2")
	tk.MustExec("execute stmt1 using @a")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2 order by a").Check(testkit.Rows("1 1", "2 2"))

	tk.MustExec("set @a=3")
	tk.MustExec("execute stmt1 using @a")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("select * from t2 order by a").Check(testkit.Rows("1 1", "2 2", "3 3"))
}

// nolint:unused
func readGaugeInt(g prometheus.Gauge) int {
	ch := make(chan prometheus.Metric, 1)
	g.Collect(ch)
	m := <-ch
	mm := &dto.Metric{}
	err := m.Write(mm)
	if err != nil {
		panic(err)
	}

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
		err = store.Close()
		c.Assert(err, IsNil)
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

func (s *testPrepareSuite) TestPrepareWithSnapshot(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
	}()

	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, v int)")
	tk.MustExec("insert into t select 1, 2")
	tk.MustExec("begin")
	ts := tk.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string)
	tk.MustExec("commit")
	tk.MustExec("update t set v = 3 where id = 1")
	tk.MustExec("prepare s1 from 'select * from t where id = 1';")
	tk.MustExec("prepare s2 from 'select * from t';")
	tk.MustExec("set @@tidb_snapshot = " + ts)
	tk.MustQuery("execute s1").Check(testkit.Rows("1 2"))
	tk.MustQuery("execute s2").Check(testkit.Rows("1 2"))
}

func (s *testPrepareSuite) TestPrepareForGroupByItems(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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

func (s *testPrepareSuite) TestPrepareCacheForClusteredIndex(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		c.Assert(store.Close(), IsNil)
	}()
	tk.MustExec("use test")
	tk.MustExec("create table t1(k varchar(100) primary key clustered, v1 int, v2 int)")
	tk.MustExec("insert into t1 (k, v1, v2) values('a', 1, 2), ('b', 1, 1)")
	tk.MustExec("create table t2(k varchar(100) primary key clustered, v int)")
	tk.MustExec("insert into t2 (k, v) values('c', 100)")
	tk.MustExec(`prepare prepare_1 from " select v2, v from t1 left join t2 on v1 != v2 "`)
	tk.MustQuery("execute prepare_1").Check(testkit.Rows("2 100", "1 <nil>"))
	tk.MustQuery("execute prepare_1").Check(testkit.Rows("2 100", "1 <nil>"))
}

func (s *testPrepareSerialSuite) TestPrepareCacheForPartition(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	for _, val := range []string{string(variable.Static), string(variable.Dynamic)} {
		tk.MustExec("set @@tidb_partition_prune_mode = '" + val + "'")
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
		tk.MustExec("drop table if exists t_range_index")
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

		tk.MustExec("drop table if exists t_range_table")
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

		// Test for list partition
		tk.MustExec("drop table if exists t_list_index")
		tk.MustExec("create table t_list_index (id int, k int, c varchar(10), primary key(id)) partition by list (id*2-id) ( PARTITION p0 VALUES IN (1,2,3,4), PARTITION p1 VALUES IN (5,6,7,8),PARTITION p2 VALUES IN (9,10,11,12))")
		tk.MustExec("insert into t_list_index values (1, 1, 'abc'), (5, 5, 'def'), (9, 9, 'xyz'), (12, 12, 'hij')")
		tk.MustExec("prepare stmt7 from 'select c from t_list_index where id = ?'")
		tk.MustExec("set @id=1")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("abc"))
		tk.MustExec("set @id=5")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("def"))
		tk.MustExec("set @id=9")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("xyz"))
		tk.MustExec("set @id=12")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("hij"))
		tk.MustExec("set @id=100")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows())

		// Test for list columns partition
		tk.MustExec("drop table if exists t_list_index")
		tk.MustExec("create table t_list_index (id int, k int, c varchar(10), primary key(id)) partition by list columns (id) ( PARTITION p0 VALUES IN (1,2,3,4), PARTITION p1 VALUES IN (5,6,7,8),PARTITION p2 VALUES IN (9,10,11,12))")
		tk.MustExec("insert into t_list_index values (1, 1, 'abc'), (5, 5, 'def'), (9, 9, 'xyz'), (12, 12, 'hij')")
		tk.MustExec("prepare stmt8 from 'select c from t_list_index where id = ?'")
		tk.MustExec("set @id=1")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("abc"))
		tk.MustExec("set @id=5")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("def"))
		tk.MustExec("set @id=9")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("xyz"))
		tk.MustExec("set @id=12")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("hij"))
		tk.MustExec("set @id=100")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows())
	}
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
		err = store.Close()
		c.Assert(err, IsNil)
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

	// Need to check if contain mutable before RefineCompareConstant() in inToExpression().
	// Otherwise may hit wrong plan.
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 tinyint unsigned);")
	tk.MustExec("insert into t1 values(111);")
	tk.MustExec("prepare stmt from 'select 1 from t1 where c1 in (?)';")
	tk.MustExec("set @a = '1.1';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows())
	tk.MustExec("set @a = '111';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestPlanCacheUnionScan(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt := pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(1))
	tk.MustExec("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(1))
	tk.MustExec("insert into t2 values(1)")
	// Cached plan is chosen, modification on t2 does not impact plan of t1.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(2))
	tk.MustExec("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(3))

	tk.MustExec("prepare stmt2 from 'select * from t1 left join t2 on true where t1.a > ?'")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	tk.MustExec("begin")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	tk.MustExec("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 <nil>",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	tk.MustExec("insert into t2 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	// Cached plan is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(5))
	tk.MustExec("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	err = counter.Write(pb)
	c.Assert(err, IsNil)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(6))
}

func (s *testPlanSerialSuite) TestPlanCacheHitInfo(c *C) {
	c.Skip("unstable, skip it and fix it before 20210705")
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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
	// Test if last_plan_from_cache is appropriately initialized.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	// Test if last_plan_from_cache is updated after a plan cache hit.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt2 using @doma").Check(testkit.Rows("1"))
	// Test if last_plan_from_cache is updated after a plan cache miss caused by a prepared statement.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	// Test if last_plan_from_cache is updated after a plan cache miss caused by a usual statement.
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where id=1").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func (s *testPlanSerialSuite) TestIssue29303(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})

	tk.MustExec(`set tidb_enable_clustered_index=on`)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists PK_MULTI_COL_360`)
	tk.MustExec(`CREATE TABLE PK_MULTI_COL_360 (
	  COL1 blob NOT NULL,
	  COL2 char(1) NOT NULL,
	  PRIMARY KEY (COL1(5),COL2) /*T![clustered_index] CLUSTERED */)`)
	tk.MustExec(`INSERT INTO PK_MULTI_COL_360 VALUES 	('�', '龂')`)
	tk.MustExec(`prepare stmt from 'SELECT/*+ INL_JOIN(t1, t2) */ * FROM PK_MULTI_COL_360 t1 JOIN PK_MULTI_COL_360 t2 ON t1.col1 = t2.col1 WHERE t2.col2 BETWEEN ? AND ? AND t1.col2 BETWEEN ? AND ?'`)
	tk.MustExec(`set @a="捲", @b="颽", @c="睭", @d="詼"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a="龂", @b="龂", @c="龂", @d="龂"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows("� 龂 � 龂"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func (s *testPlanSerialSuite) TestIssue28942(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists IDT_MULTI15853STROBJSTROBJ`)
	tk.MustExec(`
	CREATE TABLE IDT_MULTI15853STROBJSTROBJ (
	  COL1 enum('aa','bb','cc') DEFAULT NULL,
	  COL2 mediumint(41) DEFAULT NULL,
	  KEY U_M_COL4 (COL1,COL2)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`insert into IDT_MULTI15853STROBJSTROBJ values("aa", 1)`)
	tk.MustExec(`prepare stmt from 'SELECT * FROM IDT_MULTI15853STROBJSTROBJ WHERE col1 = ? AND col1 != ?'`)
	tk.MustExec(`set @a="mm", @b="aa"`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows()) // empty result
	tk.MustExec(`set @a="aa", @b="aa"`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows()) // empty result
}

func (s *testPlanSerialSuite) TestPlanCacheUnsignedHandleOverflow(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned primary key)")
	tk.MustExec("insert into t values(18446744073709551615)")
	tk.MustExec("prepare stmt from 'select a from t where a=?'")
	tk.MustExec("set @p = 1")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @p = 18446744073709551615")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func (s *testPlanSerialSuite) TestIssue28254(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})

	tk.MustExec("use test")
	tk.MustExec("drop table if exists PK_GCOL_STORED9816")
	tk.MustExec("CREATE TABLE `PK_GCOL_STORED9816` (`COL102` decimal(55,0) DEFAULT NULL)")
	tk.MustExec("insert into PK_GCOL_STORED9816 values(9710290195629059011)")
	tk.MustExec("prepare stmt from 'select count(*) from PK_GCOL_STORED9816 where col102 > ?'")
	tk.MustExec("set @a=9860178624005968368")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("0"))
	tk.MustExec("set @a=-7235178122860450591")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
	tk.MustExec("set @a=9860178624005968368")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("0"))
	tk.MustExec("set @a=-7235178122860450591")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestIssue29486(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists UK_MULTI_COL_11691`)
	tk.MustExec(`CREATE TABLE UK_MULTI_COL_11691 (
		COL1 binary(20) DEFAULT NULL,
		COL2 tinyint(16) DEFAULT NULL,
		COL3 time DEFAULT NULL,
		UNIQUE KEY U_M_COL (COL1(10),COL2,COL3))`)
	tk.MustExec(`insert into UK_MULTI_COL_11691 values(0x340C604874B52E8D30440E8DC2BB170621D8A088, 126, "-105:17:32"),
	(0x28EC2EDBAC7DF99045BDD0FCEAADAFBAC2ACF76F, 126, "102:54:04"),
	(0x11C38221B3B1E463C94EC39F0D481303A58A50DC, 118, "599:13:47"),
	(0x03E2FC9E0C846FF1A926BF829FA9D7BAED3FD7B1, 118, "-257:45:13")`)

	tk.MustExec(`prepare stmt from 'SELECT/*+ INL_JOIN(t1, t2) */ t2.COL2 FROM UK_MULTI_COL_11691 t1 JOIN UK_MULTI_COL_11691 t2 ON t1.col1 = t2.col1 WHERE t1.col2 BETWEEN ? AND ? AND t2.col2 BETWEEN ? AND ?'`)
	tk.MustExec(`set @a=-29408, @b=-9254, @c=-1849, @d=-2346`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a=126, @b=126, @c=-125, @d=707`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows("126", "126"))
}

func (s *testPlanSerialSuite) TestIssue28867(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`CREATE TABLE t1 (c_int int, c_str varchar(40), PRIMARY KEY (c_int, c_str))`)
	tk.MustExec(`CREATE TABLE t2 (c_str varchar(40), PRIMARY KEY (c_str))`)
	tk.MustExec(`insert into t1 values (1, '1')`)
	tk.MustExec(`insert into t2 values ('1')`)

	tk.MustExec(`prepare stmt from 'select /*+ INL_JOIN(t1,t2) */ * from t1 join t2 on t1.c_str <= t2.c_str where t1.c_int in (?,?)'`)
	tk.MustExec(`set @a=10, @b=20`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows())
	tk.MustExec(`set @a=1, @b=2`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows("1 1 1"))

	// test case for IndexJoin + PlanCache
	tk.MustExec(`drop table t1, t2`)
	tk.MustExec(`create table t1 (a int, b int, c int, index idxab(a, b, c))`)
	tk.MustExec(`create table t2 (a int, b int)`)

	tk.MustExec(`prepare stmt from 'select /*+ INL_JOIN(t1,t2) */ * from t1, t2 where t1.a=t2.a and t1.b=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'select /*+ INL_JOIN(t1,t2) */ * from t1, t2 where t1.a=t2.a and t1.c=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestIssue29565(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists PK_SIGNED_10094`)
	tk.MustExec(`CREATE TABLE PK_SIGNED_10094 (COL1 decimal(55,0) NOT NULL, PRIMARY KEY (COL1))`)
	tk.MustExec(`insert into PK_SIGNED_10094  values(-9999999999999999999999999999999999999999999999999999999)`)
	tk.MustExec(`prepare stmt from 'select * from PK_SIGNED_10094 where col1 != ? and col1 + 10 <=> ? + 10'`)
	tk.MustExec(`set @a=7309027171262036496, @b=-9798213896406520625`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
	tk.MustExec(`set @a=5408499810319315618, @b=-9999999999999999999999999999999999999999999999999999999`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows("-9999999999999999999999999999999999999999999999999999999"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestIssue28828(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("CREATE TABLE t (" +
		"id bigint(20) NOT NULL," +
		"audit_id bigint(20) NOT NULL," +
		"PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */," +
		"KEY index_audit_id (audit_id)" +
		");")
	tk.MustExec("insert into t values(1,9941971237863475), (2,9941971237863476), (3, 0);")
	tk.MustExec("prepare stmt from 'select * from t where audit_id=?';")
	tk.MustExec("set @a='9941971237863475', @b=9941971237863475, @c='xayh7vrWVNqZtzlJmdJQUwAHnkI8Ec', @d='0.0', @e='0.1', @f = '9941971237863476';")

	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1 9941971237863475"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("1 9941971237863475"))
	// When the type of parameters have been changed, the plan cache can not be used.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @c;").Check(testkit.Rows("3 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @d;").Check(testkit.Rows("3 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @e;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @d;").Check(testkit.Rows("3 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @f;").Check(testkit.Rows("2 9941971237863476"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("prepare stmt from 'select count(*) from t where audit_id in (?, ?, ?, ?, ?)';")
	tk.MustQuery("execute stmt using @a, @b, @c, @d, @e;").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @f, @b, @c, @d, @e;").Check(testkit.Rows("3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func (s *testPlanSerialSuite) TestIssue28920(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		c.Assert(store.Close(), IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists UK_GCOL_VIRTUAL_18928`)
	tk.MustExec(`
	CREATE TABLE UK_GCOL_VIRTUAL_18928 (
	  COL102 bigint(20) DEFAULT NULL,
	  COL103 bigint(20) DEFAULT NULL,
	  COL1 bigint(20) GENERATED ALWAYS AS (COL102 & 10) VIRTUAL,
	  COL2 varchar(20) DEFAULT NULL,
	  COL4 datetime DEFAULT NULL,
	  COL3 bigint(20) DEFAULT NULL,
	  COL5 float DEFAULT NULL,
	  UNIQUE KEY UK_COL1 (COL1))`)
	tk.MustExec(`insert into UK_GCOL_VIRTUAL_18928(col102,col2) values("-5175976006730879891", "屘厒镇览錻碛斵大擔觏譨頙硺箄魨搝珄鋧扭趖")`)
	tk.MustExec(`prepare stmt from 'SELECT * FROM UK_GCOL_VIRTUAL_18928 WHERE col1 < ? AND col2 != ?'`)
	tk.MustExec(`set @a=10, @b="aa"`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows("-5175976006730879891 <nil> 8 屘厒镇览錻碛斵大擔觏譨頙硺箄魨搝珄鋧扭趖 <nil> <nil> <nil>"))
}

func (s *testPlanSerialSuite) TestIssue18066(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	tk.GetConnectionID()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare stmt from 'select * from t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("1 0 0"))
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("2 1 1"))
	tk.MustExec("prepare stmt from 'select * from t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("3 1 0"))
}

func (s *testPrepareSuite) TestPrepareForGroupByMultiItems(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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

func (s *testPrepareSuite) TestInvisibleIndex(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, unique idx_a(a))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec(`prepare stmt1 from "select a from t order by a"`)

	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")

	tk.MustExec("alter table t alter index idx_a invisible")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustExec("alter table t alter index idx_a visible")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")
}

// Test for issue https://github.com/pingcap/tidb/issues/22167
func (s *testPrepareSerialSuite) TestPrepareCacheWithJoinTable(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ta, tb")
	tk.MustExec("CREATE TABLE ta(k varchar(32) NOT NULL DEFAULT ' ')")
	tk.MustExec("CREATE TABLE tb (k varchar(32) NOT NULL DEFAULT ' ', s varchar(1) NOT NULL DEFAULT ' ')")
	tk.MustExec("insert into ta values ('a')")
	tk.MustExec("set @a=2, @b=1")
	tk.MustExec(`prepare stmt from "select * from ta a left join tb b on 1 where ? = 1 or b.s is not null"`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows())
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("a <nil> <nil>"))
}

func (s *testPlanSerialSuite) TestPlanCacheSnapshot(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
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

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	timeSafe := time.Now().Add(-48 * 60 * 60 * time.Second).Format("20060102-15:04:05 -0700 MST")
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec(fmt.Sprintf(safePointSQL, timeSafe))

	tk.MustExec("prepare stmt from 'select * from t where id=?'")
	tk.MustExec("set @p = 1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Record the current tso.
	tk.MustExec("begin")
	tso := tk.Se.GetSessionVars().TxnCtx.StartTS
	tk.MustExec("rollback")
	c.Assert(tso > 0, IsTrue)
	// Insert one more row with id = 1.
	tk.MustExec("insert into t values (1)")

	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", tso))
	tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestPlanCachePointGetAndTableDual(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		c.Assert(store.Close(), IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1, t2, t3, t4")

	tk.MustExec("create table t0(c1 varchar(20), c2 varchar(20), c3 bigint(20), primary key(c1, c2))")
	tk.MustExec("insert into t0 values('0000','7777',1)")
	tk.MustExec("prepare s0 from 'select * from t0 where c1=? and c2>=? and c2<=?'")
	tk.MustExec("set @a0='0000', @b0='9999'")
	// TableDual is forbidden for plan-cache, a TableReader be built and cached.
	tk.MustQuery("execute s0 using @a0, @b0, @a0").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s0 using @a0, @a0, @b0").Check(testkit.Rows("0000 7777 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create table t1(c1 varchar(20), c2 varchar(20), c3 bigint(20), primary key(c1, c2))")
	tk.MustExec("insert into t1 values('0000','7777',1)")
	tk.MustExec("prepare s1 from 'select * from t1 where c1=? and c2>=? and c2<=?'")
	tk.MustExec("set @a1='0000', @b1='9999'")
	// IndexLookup plan would be built, we should cache it.
	tk.MustQuery("execute s1 using @a1, @b1, @b1").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s1 using @a1, @a1, @b1").Check(testkit.Rows("0000 7777 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create table t2(c1 bigint(20) primary key, c2 varchar(20))")
	tk.MustExec("insert into t2 values(1,'7777')")
	tk.MustExec("prepare s2 from 'select * from t2 where c1>=? and c1<=?'")
	tk.MustExec("set @a2=0, @b2=9")
	// TableReader plan would be built, we should cache it.
	tk.MustQuery("execute s2 using @a2, @a2").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s2 using @a2, @b2").Check(testkit.Rows("1 7777"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create table t3(c1 int, c2 int, c3 int, unique key(c1), key(c2))")
	tk.MustExec("insert into t3 values(2,1,1)")
	tk.MustExec("prepare s3 from 'select /*+ use_index_merge(t3) */ * from t3 where (c1 >= ? and c1 <= ?) or c2 > 1'")
	tk.MustExec("set @a3=1,@b3=3")
	// TableReader plan would be built, we should cache it.
	tk.MustQuery("execute s3 using @a3,@a3").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s3 using @a3,@b3").Check(testkit.Rows("2 1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare s3 from 'select /*+ use_index_merge(t3) */ * from t3 where (c1 >= ? and c1 <= ?) or c2 > 1'")
	tk.MustExec("set @a3=1,@b3=3")
	// TableReader plan would be built, we should cache it.
	tk.MustQuery("execute s3 using @b3,@a3").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s3 using @a3,@b3").Check(testkit.Rows("2 1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create table t4(c1 int primary key, c2 int, c3 int, key(c2))")
	tk.MustExec("insert into t4 values(2,1,1)")
	tk.MustExec("prepare s4 from 'select /*+ use_index_merge(t4) */ * from t4 where (c1 >= ? and c1 <= ?) or c2 > 1'")
	tk.MustExec("set @a4=1,@b4=3")
	tk.MustQuery("execute s4 using @a4,@a4").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s4 using @a4,@b4").Check(testkit.Rows("2 1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare s4 from 'select /*+ use_index_merge(t4) */ * from t4 where (c1 >= ? and c1 <= ?) or c2 > 1'")
	tk.MustExec("set @a4=1,@b4=3")
	tk.MustQuery("execute s4 using @b4,@a4").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute s4 using @a4,@b4").Check(testkit.Rows("2 1 1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPrepareSuite) TestIssue26873(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		c.Assert(store.Close(), IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("prepare stmt from 'select * from t where a = 2 or a = ?'")
	tk.MustExec("set @p = 3")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPrepareSuite) TestIssue29511(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		c.Assert(store.Close(), IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("CREATE TABLE `t` (`COL1` bigint(20) DEFAULT NULL COMMENT 'WITH DEFAULT', UNIQUE KEY `UK_COL1` (`COL1`))")
	tk.MustExec("insert into t values(-3865356285544170443),(9223372036854775807);")
	tk.MustExec("prepare stmt from 'select/*+ hash_agg() */ max(col1) from t where col1 = ? and col1 > ?;';")
	tk.MustExec("set @a=-3865356285544170443, @b=-4055949188488870713;")
	tk.MustQuery("execute stmt using @a,@b;").Check(testkit.Rows("-3865356285544170443"))
}

func (s *testPlanSerialSuite) TestIssue23671(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		c.Assert(store.Close(), IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)

	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a int, b int, index ab(a, b))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("prepare s1 from 'select * from t use index(ab) where a>=? and b>=? and b<=?'")
	tk.MustExec("set @a=1, @b=1, @c=1")
	tk.MustQuery("execute s1 using @a, @b, @c").Check(testkit.Rows("1 1"))
	tk.MustExec("set @a=1, @b=1, @c=10")
	tk.MustQuery("execute s1 using @a, @b, @c").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPrepareSerialSuite) TestIssue29296(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists UK_MU14722`)
	tk.MustExec(`CREATE TABLE UK_MU14722 (
	  COL1 tinytext DEFAULT NULL,
	  COL2 tinyint(16) DEFAULT NULL,
	  COL3 datetime DEFAULT NULL,
	  COL4 int(11) DEFAULT NULL,
	  UNIQUE KEY U_M_COL (COL1(10)),
	  UNIQUE KEY U_M_COL2 (COL2),
	  UNIQUE KEY U_M_COL3 (COL3))`)
	tk.MustExec(`insert into UK_MU14722 values("輮睅麤敜溺她晁瀪襄頮鹛涓誗钷廔筪惌嶙鎢塴", -121, "3383-02-19 07:58:28" , -639457963),
		("偧孇鱓鼂瘠钻篝醗時鷷聽箌磇砀玸眞扦鸇祈灇", 127, "7902-03-05 08:54:04", -1094128660),
		("浀玡慃淛漉围甧鴎史嬙砊齄w章炢忲噑硓哈樘", -127, "5813-04-16 03:07:20", -333397107),
		("鑝粼啎鸼贖桖弦簼赭蠅鏪鐥蕿捐榥疗耹岜鬓槊", -117, "7753-11-24 10:14:24", 654872077)`)
	tk.MustExec(`prepare stmt from 'SELECT * FROM UK_MU14722 WHERE col2 > ? OR col2 BETWEEN ? AND ? ORDER BY COL2 + ? LIMIT 3'`)
	tk.MustExec(`set @a=30410, @b=3937, @c=22045, @d=-4374`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a=127, @b=127, @c=127, @d=127`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows(`偧孇鱓鼂瘠钻篝醗時鷷聽箌磇砀玸眞扦鸇祈灇 127 7902-03-05 08:54:04 -1094128660`))
}

func (s *testPrepareSerialSuite) TestIssue28246(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists PK_AUTO_RANDOM9111;")
	tk.MustExec("CREATE TABLE `PK_AUTO_RANDOM9111` (   `COL1` bigint(45) NOT NULL  ,   `COL2` varchar(20) DEFAULT NULL,   `COL4` datetime DEFAULT NULL,   `COL3` bigint(20) DEFAULT NULL,   `COL5` float DEFAULT NULL,   PRIMARY KEY (`COL1`)  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into PK_AUTO_RANDOM9111(col1) values (-9223372036854775808), (9223372036854775807);")
	tk.MustExec("set @a=9223372036854775807, @b=1")
	tk.MustExec(`prepare stmt from 'select min(col1) from PK_AUTO_RANDOM9111 where col1 > ?;';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("<nil>"))
	// The plan contains the tableDual, so it will not be cached.
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPrepareSerialSuite) TestIssue29805(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_clustered_index=on;")
	tk.MustExec("drop table if exists PK_TCOLLATION10197;")
	tk.MustExec("CREATE TABLE `PK_TCOLLATION10197` (`COL1` char(1) NOT NULL, PRIMARY KEY (`COL1`(1)) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into PK_TCOLLATION10197 values('龺');")
	tk.MustExec("set @a='畻', @b='龺';")
	tk.MustExec(`prepare stmt from 'select/*+ hash_agg() */ count(distinct col1) from PK_TCOLLATION10197 where col1 > ?;';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'select/*+ hash_agg() */ count(distinct col1) from PK_TCOLLATION10197 where col1 > ?;';`)
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("0"))

	tk.MustQuery("select/*+ hash_agg() */ count(distinct col1) from PK_TCOLLATION10197 where col1 > '龺';").Check(testkit.Rows("0"))
}

func (s *testPrepareSerialSuite) TestIssue29993(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")

	// test PointGet + cluster index
	tk.MustExec("set tidb_enable_clustered_index=on;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL PRIMARY KEY, col2 int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where col1 = ? and col2 in (1, 2);';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("b"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	// The length of range have been changed, so the plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())

	// test batchPointGet + cluster index
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL, col2 int, PRIMARY KEY(col1, col2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where (col1, col2) in ((?, 1));';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())

	// test PointGet + non cluster index
	tk.MustExec("set tidb_enable_clustered_index=off;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL PRIMARY KEY, col2 int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where col1 = ? and col2 in (1, 2);';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("b"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	// The length of range have been changed, so the plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())

	// test batchPointGet + non cluster index
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL, col2 int, PRIMARY KEY(col1, col2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where (col1, col2) in ((?, 1));';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
}

func (s *testPrepareSerialSuite) TestIssue30100(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(col1 enum('aa', 'bb'), col2 int, index(col1, col2));")
	tk.MustExec("insert into t values('aa', 333);")
	tk.MustExec(`prepare stmt from 'SELECT * FROM t t1 JOIN t t2 ON t1.col1 = t2.col1 WHERE t1.col1 <=> NULL';`)
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'SELECT * FROM t t1 JOIN t t2 ON t1.col1 = t2.col1 WHERE t1.col1 <=> NULL and t2.col2 > ?';`)
	tk.MustExec("set @a=0;")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows())
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func (s *testPlanSerialSuite) TestPartitionTable(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}

	// enable plan cache
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	// enable partition table dynamic mode
	tk.MustExec("create database test_plan_cache")
	tk.MustExec("use test_plan_cache")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	type testcase struct {
		t1Create string
		t2Create string
		rowGener func() string
		varGener func() string
		query    string
	}
	randDateTime := func() string {
		return fmt.Sprintf("%v-%v-%v %v:%v:%v",
			1950+rand.Intn(100), 1+rand.Intn(12), 1+rand.Intn(28), // date
			rand.Intn(24), rand.Intn(60), rand.Intn(60)) // time
	}
	randDate := func() string {
		return fmt.Sprintf("%v-%v-%v", 1950+rand.Intn(100), 1+rand.Intn(12), 1+rand.Intn(28))
	}
	testcases := []testcase{
		{ // hash partition + int
			"create table t1(a int, b int) partition by hash(a) partitions 20",
			"create table t2(a int, b int)",
			func() string { return fmt.Sprintf("(%v, %v)", rand.Intn(100000000), rand.Intn(100000000)) },
			func() string { return fmt.Sprintf("%v", rand.Intn(100000000)) },
			`select * from %v where a > ?`,
		},
		{ // range partition + int
			`create table t1(a int, b int) partition by range(a) (
						partition p0 values less than (20000000),
						partition p1 values less than (40000000),
						partition p2 values less than (60000000),
						partition p3 values less than (80000000),
						partition p4 values less than (100000000))`,
			`create table t2(a int, b int)`,
			func() string { return fmt.Sprintf("(%v, %v)", rand.Intn(100000000), rand.Intn(100000000)) },
			func() string { return fmt.Sprintf("%v", rand.Intn(100000000)) },
			`select * from %v where a > ?`,
		},
		{ // range partition + varchar
			`create table t1(a varchar(10), b varchar(10)) partition by range columns(a) (
						partition p0 values less than ('200'),
						partition p1 values less than ('400'),
						partition p2 values less than ('600'),
						partition p3 values less than ('800'),
						partition p4 values less than ('9999'))`,
			`create table t2(a varchar(10), b varchar(10))`,
			func() string { return fmt.Sprintf(`("%v", "%v")`, rand.Intn(1000), rand.Intn(1000)) },
			func() string { return fmt.Sprintf(`"%v"`, rand.Intn(1000)) },
			`select * from %v where a > ?`,
		},
		{ // range partition + datetime
			`create table t1(a datetime, b datetime) partition by range columns(a) (
						partition p0 values less than ('1970-01-01 00:00:00'),
						partition p1 values less than ('1990-01-01 00:00:00'),
						partition p2 values less than ('2010-01-01 00:00:00'),
						partition p3 values less than ('2030-01-01 00:00:00'),
						partition p4 values less than ('2060-01-01 00:00:00'))`,
			`create table t2(a datetime, b datetime)`,
			func() string { return fmt.Sprintf(`("%v", "%v")`, randDateTime(), randDateTime()) },
			func() string { return fmt.Sprintf(`"%v"`, randDateTime()) },
			`select * from %v where a > ?`,
		},
		{ // range partition + date
			`create table t1(a date, b date) partition by range columns(a) (
						partition p0 values less than ('1970-01-01'),
						partition p1 values less than ('1990-01-01'),
						partition p2 values less than ('2010-01-01'),
						partition p3 values less than ('2030-01-01'),
						partition p4 values less than ('2060-01-01'))`,
			`create table t2(a date, b date)`,
			func() string { return fmt.Sprintf(`("%v", "%v")`, randDate(), randDate()) },
			func() string { return fmt.Sprintf(`"%v"`, randDate()) },
			`select * from %v where a > ?`,
		},
		{ // list partition + int
			`create table t1(a int, b int) partition by list(a) (
						partition p0 values in (0, 1, 2, 3, 4),
						partition p1 values in (5, 6, 7, 8, 9),
						partition p2 values in (10, 11, 12, 13, 14),
						partition p3 values in (15, 16, 17, 18, 19))`,
			`create table t2(a int, b int)`,
			func() string { return fmt.Sprintf("(%v, %v)", rand.Intn(20), rand.Intn(20)) },
			func() string { return fmt.Sprintf("%v", rand.Intn(20)) },
			`select * from %v where a > ?`,
		},
	}
	for _, tc := range testcases {
		// create tables and insert some records
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop table if exists t2")
		tk.MustExec(tc.t1Create)
		tk.MustExec(tc.t2Create)
		vals := make([]string, 0, 2048)
		for i := 0; i < 2048; i++ {
			vals = append(vals, tc.rowGener())
		}
		tk.MustExec(fmt.Sprintf("insert into t1 values %s", strings.Join(vals, ",")))
		tk.MustExec(fmt.Sprintf("insert into t2 values %s", strings.Join(vals, ",")))

		// the first query, @last_plan_from_cache should be zero
		tk.MustExec(fmt.Sprintf(`prepare stmt1 from "%s"`, fmt.Sprintf(tc.query, "t1")))
		tk.MustExec(fmt.Sprintf(`prepare stmt2 from "%s"`, fmt.Sprintf(tc.query, "t2")))
		tk.MustExec(fmt.Sprintf("set @a=%v", tc.varGener()))
		result1 := tk.MustQuery("execute stmt1 using @a").Sort().Rows()
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute stmt2 using @a").Sort().Check(result1)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("set @a=%v", tc.varGener()))
			result1 := tk.MustQuery("execute stmt1 using @a").Sort().Rows()
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
			tk.MustQuery("execute stmt2 using @a").Sort().Check(result1)
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		}
	}
}

func (s *testPlanSerialSuite) TestPartitionWithVariedDatasources(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}

	// enable plan cache
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := core.PreparedPlanCacheEnabled()
	defer func() {
		dom.Close()
		err = store.Close()
		c.Assert(err, IsNil)
		core.SetPreparedPlanCache(orgEnable)
	}()
	core.SetPreparedPlanCache(true)
	tk.Se, err = session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	// enable partition table dynamic mode
	tk.MustExec("create database test_plan_cache2")
	tk.MustExec("use test_plan_cache2")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// prepare tables
	tk.MustExec(`create table trangePK (a int primary key, b int) partition by range (a) (
			partition p0 values less than (10000),
			partition p1 values less than (20000),
			partition p2 values less than (30000),
			partition p3 values less than (40000))`)
	tk.MustExec(`create table thashPK (a int primary key, b int) partition by hash (a) partitions 4`)
	tk.MustExec(`create table tnormalPK (a int primary key, b int)`)
	tk.MustExec(`create table trangeIdx (a int unique key, b int) partition by range (a) (
			partition p0 values less than (10000),
			partition p1 values less than (20000),
			partition p2 values less than (30000),
			partition p3 values less than (40000))`)
	tk.MustExec(`create table thashIdx (a int unique key, b int) partition by hash (a) partitions 4`)
	tk.MustExec(`create table tnormalIdx (a int unique key, b int)`)
	uniqueVals := make(map[int]struct{})
	vals := make([]string, 0, 1000)
	for len(vals) < 1000 {
		a := rand.Intn(40000)
		if _, ok := uniqueVals[a]; ok {
			continue
		}
		uniqueVals[a] = struct{}{}
		b := rand.Intn(40000)
		vals = append(vals, fmt.Sprintf("(%v, %v)", a, b))
	}
	for _, tbl := range []string{"trangePK", "thashPK", "tnormalPK", "trangeIdx", "thashIdx", "tnormalIdx"} {
		tk.MustExec(fmt.Sprintf(`insert into %v values %v`, tbl, strings.Join(vals, ", ")))
	}

	// TableReader, PointGet on PK, BatchGet on PK
	for _, tbl := range []string{`trangePK`, `thashPK`, `tnormalPK`} {
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_tablescan from 'select * from %v use index(primary) where a > ? and a < ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_pointget from 'select * from %v use index(primary) where a = ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_batchget from 'select * from %v use index(primary) where a in (?, ?, ?)'`, tbl, tbl))
	}
	for i := 0; i < 100; i++ {
		mina, maxa := rand.Intn(40000), rand.Intn(40000)
		if mina > maxa {
			mina, maxa = maxa, mina
		}
		tk.MustExec(fmt.Sprintf(`set @mina=%v, @maxa=%v`, mina, maxa))
		tk.MustExec(fmt.Sprintf(`set @pointa=%v`, rand.Intn(40000)))
		tk.MustExec(fmt.Sprintf(`set @a0=%v, @a1=%v, @a2=%v`, rand.Intn(40000), rand.Intn(40000), rand.Intn(40000)))

		var rscan, rpoint, rbatch [][]interface{}
		for id, tbl := range []string{`trangePK`, `thashPK`, `tnormalPK`} {
			scan := tk.MustQuery(fmt.Sprintf(`execute stmt%v_tablescan using @mina, @maxa`, tbl)).Sort()
			if id == 0 {
				rscan = scan.Rows()
			} else {
				scan.Check(rscan)
			}

			point := tk.MustQuery(fmt.Sprintf(`execute stmt%v_pointget using @pointa`, tbl)).Sort()
			if id == 0 {
				rpoint = point.Rows()
			} else {
				point.Check(rpoint)
			}

			batch := tk.MustQuery(fmt.Sprintf(`execute stmt%v_batchget using @a0, @a1, @a2`, tbl)).Sort()
			if id == 0 {
				rbatch = batch.Rows()
			} else {
				batch.Check(rbatch)
			}
		}
	}

	// IndexReader, IndexLookUp, PointGet on Idx, BatchGet on Idx
	for _, tbl := range []string{"trangeIdx", "thashIdx", "tnormalIdx"} {
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_indexscan from 'select a from %v use index(a) where a > ? and a < ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_indexlookup from 'select * from %v use index(a) where a > ? and a < ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_pointget_idx from 'select * from %v use index(a) where a = ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_batchget_idx from 'select * from %v use index(a) where a in (?, ?, ?)'`, tbl, tbl))
	}
	for i := 0; i < 100; i++ {
		mina, maxa := rand.Intn(40000), rand.Intn(40000)
		if mina > maxa {
			mina, maxa = maxa, mina
		}
		tk.MustExec(fmt.Sprintf(`set @mina=%v, @maxa=%v`, mina, maxa))
		tk.MustExec(fmt.Sprintf(`set @pointa=%v`, rand.Intn(40000)))
		tk.MustExec(fmt.Sprintf(`set @a0=%v, @a1=%v, @a2=%v`, rand.Intn(40000), rand.Intn(40000), rand.Intn(40000)))

		var rscan, rlookup, rpoint, rbatch [][]interface{}
		for id, tbl := range []string{"trangeIdx", "thashIdx", "tnormalIdx"} {
			scan := tk.MustQuery(fmt.Sprintf(`execute stmt%v_indexscan using @mina, @maxa`, tbl)).Sort()
			if i > 0 {
				tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
			}
			if id == 0 {
				rscan = scan.Rows()
			} else {
				scan.Check(rscan)
			}

			lookup := tk.MustQuery(fmt.Sprintf(`execute stmt%v_indexlookup using @mina, @maxa`, tbl)).Sort()
			if i > 0 {
				tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
			}
			if id == 0 {
				rlookup = lookup.Rows()
			} else {
				lookup.Check(rlookup)
			}

			point := tk.MustQuery(fmt.Sprintf(`execute stmt%v_pointget_idx using @pointa`, tbl)).Sort()
			if tbl == `tnormalPK` && i > 0 {
				// PlanCache cannot support PointGet now since we haven't relocated partition after rebuilding range.
				// Please see Execute.rebuildRange for more details.
				tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
			}
			if id == 0 {
				rpoint = point.Rows()
			} else {
				point.Check(rpoint)
			}

			batch := tk.MustQuery(fmt.Sprintf(`execute stmt%v_batchget_idx using @a0, @a1, @a2`, tbl)).Sort()
			if i > 0 {
				tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
			}
			if id == 0 {
				rbatch = batch.Rows()
			} else {
				batch.Check(rbatch)
			}
		}
	}
}
