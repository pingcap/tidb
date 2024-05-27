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

package prepare_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPointGetPreparedPlan4PlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk1.MustExec("drop database if exists ps_text")
	defer tk1.MustExec("drop database if exists ps_text")
	tk1.MustExec("create database ps_text")
	tk1.MustExec("use ps_text")

	tk1.MustExec(`create table t (a int, b int, c int,
			primary key k_a(a),
			unique key k_b(b))`)
	tk1.MustExec("insert into t values (1, 1, 1)")
	tk1.MustExec("insert into t values (2, 2, 2)")
	tk1.MustExec("insert into t values (3, 3, 3)")

	pspk1Id, _, _, err := tk1.Session().PrepareStmt("select * from t where a = ?")
	require.NoError(t, err)
	tk1.Session().GetSessionVars().PreparedStmts[pspk1Id].(*core.PlanCacheStmt).StmtCacheable = false

	ctx := context.Background()
	// first time plan generated
	_, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(0))
	require.NoError(t, err)

	// using the generated plan but with different params
	_, err = tk1.Session().ExecutePreparedStmt(ctx, pspk1Id, expression.Args2Expressions4Test(nil))
	require.NoError(t, err)
}

func TestRandomFlushPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk2 := testkit.NewTestKit(t, store)
	var err error

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int, a int, b int, key(a))")
	tk.MustExec("create table t2(id int, a int, b int, key(a))")
	tk.MustExec("prepare stmt1 from 'SELECT * from t1,t2 where t1.id = t2.id';")
	tk.MustExec("prepare stmt2 from 'SELECT * from t1';")
	tk.MustExec("prepare stmt3 from 'SELECT * from t1 where id = 1';")
	tk.MustExec("prepare stmt4 from 'SELECT * from t2';")
	tk.MustExec("prepare stmt5 from 'SELECT * from t2 where id = 1';")

	tk2.MustExec("use test")
	tk2.MustExec("prepare stmt1 from 'SELECT * from t1,t2 where t1.id = t2.id';")
	tk2.MustExec("prepare stmt2 from 'SELECT * from t1';")
	tk2.MustExec("prepare stmt3 from 'SELECT * from t1 where id = 1';")
	tk2.MustExec("prepare stmt4 from 'SELECT * from t2';")
	tk2.MustExec("prepare stmt5 from 'SELECT * from t2 where id = 1';")

	prepareNum := 5
	execStmts := make([]string, 0, prepareNum)
	for i := 1; i <= prepareNum; i++ {
		execStmt := fmt.Sprintf("execute stmt%d", i)
		execStmts = append(execStmts, execStmt)
	}

	for i := 0; i < 10; i++ {
		// Warm up to make sure all the plans are in the cache.
		for _, execStmt := range execStmts {
			tk.MustExec(execStmt)
			tk.MustExec(execStmt)
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

			tk2.MustExec(execStmt)
			tk2.MustExec(execStmt)
			tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		}

		for j := 0; j < 10; j++ {
			session1PC, session2PC := "1", "1"
			// random to flush the plan cache
			randNum := rand.Intn(10)
			if randNum == 0 {
				session1PC, session2PC = "0", "0"
				if j%2 == 0 {
					err = tk.ExecToErr("admin flush instance plan_cache;")
				} else {
					err = tk2.ExecToErr("admin flush instance plan_cache;")
				}
				require.NoError(t, err)
			} else if randNum == 1 {
				session1PC = "0"
				err = tk.ExecToErr("admin flush session plan_cache;")
				require.NoError(t, err)
			} else if randNum == 2 {
				session2PC = "0"
				err = tk2.ExecToErr("admin flush session plan_cache;")
				require.NoError(t, err)
			}

			for _, execStmt := range execStmts {
				tk.MustExec(execStmt)
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(session1PC))

				tk2.MustExec(execStmt)
				tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(session2PC))
			}
		}

		err = tk.ExecToErr("admin flush instance plan_cache;")
		require.NoError(t, err)
	}

	err = tk.ExecToErr("admin flush global plan_cache;")
	require.EqualError(t, err, "Do not support the 'admin flush global scope.'")
}

func TestPrepareCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	rootSe := tk.Session()
	tk.MustExec("drop table if exists tp")
	tk.MustExec(`create table tp(c1 int, c2 int, primary key (c1))`)
	tk.MustExec(`insert into tp values(1, 1), (2, 2), (3, 3)`)

	tk.MustExec(`create user 'u_tp'@'localhost'`)
	tk.MustExec(`grant select on test.tp to u_tp@'localhost';`)

	// user u_tp
	userSess := newSession(t, store, "test")
	require.NoError(t, userSess.Auth(&auth.UserIdentity{Username: "u_tp", Hostname: "localhost"}, nil, nil, nil))
	mustExec(t, userSess, `prepare ps_stp_r from 'select * from tp where c1 > ?'`)
	mustExec(t, userSess, `set @p2 = 2`)
	tk.SetSession(userSess)
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))

	// root revoke
	tk.SetSession(rootSe)
	tk.MustExec(`revoke all on test.tp from 'u_tp'@'localhost';`)

	// user u_tp
	tk.SetSession(userSess)
	_, err := tk.Exec(`execute ps_stp_r using @p2`)
	require.Error(t, err)

	// grant again
	tk.SetSession(rootSe)
	tk.MustExec(`grant select on test.tp to u_tp@'localhost';`)

	// user u_tp
	tk.SetSession(userSess)
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))

	// restore
	tk.SetSession(rootSe)
	tk.MustExec("drop table if exists tp")
	tk.MustExec(`DROP USER 'u_tp'@'localhost';`)
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

func TestPrepareCacheChangingParamType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
						require.Error(t, tk.ExecToErr(sql2))
						return
					}
					rs := tk.ResultSetToResult(raw, fmt.Sprintf("sql1:%s, sql2:%v", sql1, sql2))
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

func TestPrepareCacheDeferredFunction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	p := parser.New()
	p.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})
	for i := 0; i < 2; i++ {
		stmt, err := p.ParseOneStmt(sql1, "", "")
		require.NoError(t, err)
		is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
		builder, _ := core.NewPlanBuilder().Init(tk.Session().GetPlanCtx(), is, hint.NewQBHintHandler(nil))
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err)
		execPlan, ok := p.(*core.Execute)
		require.True(t, ok)
		err = executor.ResetContextOfStmt(tk.Session(), stmt)
		require.NoError(t, err)
		plan, _, err := core.GetPlanFromSessionPlanCache(ctx, tk.Session(), false, is, execPlan.PrepStmt, execPlan.Params)
		require.NoError(t, err)
		planStr[i] = core.ToString(plan)
		require.Regexpf(t, expectedPattern, planStr[i], "for %dth %s", i, sql1)
		pb := &dto.Metric{}
		err = counter.Write(pb)
		require.NoError(t, err)
		cnt[i] = pb.GetCounter().GetValue()
		require.Equal(t, float64(i), cnt[i])
		time.Sleep(time.Millisecond * 10)
	}
	require.Lessf(t, planStr[0], planStr[1], "plan 1: %v, plan 2: %v", planStr[0], planStr[1])
}

func TestPrepareCacheNow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec(`prepare stmt1 from "select now(), current_timestamp(), utc_timestamp(), unix_timestamp(), sleep(0.1), now(), current_timestamp(), utc_timestamp(), unix_timestamp()"`)
	// When executing one statement at the first time, we don't usTestPrepareCacheDeferredFunctione cache, so we need to execute it at least twice to test the cache.
	_ = tk.MustQuery("execute stmt1").Rows()
	rs := tk.MustQuery("execute stmt1").Rows()
	require.Equal(t, rs[0][5].(string), rs[0][0].(string))
	require.Equal(t, rs[0][6].(string), rs[0][1].(string))
	require.Equal(t, rs[0][7].(string), rs[0][2].(string))
	require.Equal(t, rs[0][8].(string), rs[0][3].(string))

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values(1);")
	tk.MustExec("set @@tidb_enable_prepared_plan_cache=0;")
	tk.MustExec("set global tidb_sysdate_is_now=0;")
	tk.MustExec("prepare s from 'select sleep(a), now(6), sysdate(6),sysdate(6)=now(6) from t';")
	t1 := tk.MustQuery("execute s").Rows()
	tk.MustExec("set global tidb_sysdate_is_now=1;")
	t2 := tk.MustQuery("execute s").Rows()
	require.NotEqual(t, t1, t2)
}

func TestPrepareOverMaxPreparedStmtCount(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test prepare and deallocate.
	prePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	tk.MustExec(`prepare stmt1 from "select 1"`)
	onePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	require.Equal(t, onePrepared, prePrepared+1)
	tk.MustExec(`deallocate prepare stmt1`)
	deallocPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	require.Equal(t, deallocPrepared, prePrepared)

	// test change global limit and make it affected in test session.
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("-1"))
	tk.MustExec("set @@global.max_prepared_stmt_count = 2")
	tk.MustQuery("select @@global.max_prepared_stmt_count").Check(testkit.Rows("2"))

	// test close session to give up all prepared stmt
	tk.MustExec(`prepare stmt2 from "select 1"`)
	prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
	tk.Session().Close()
	drawPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	require.Equal(t, drawPrepared, prePrepared-1)

	// test meet max limit.
	tk.RefreshSession()
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("2"))
	for i := 1; ; i++ {
		prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
		if prePrepared >= 2 {
			tk.MustGetErrCode(`prepare stmt`+strconv.Itoa(i)+` from "select 1"`, errno.ErrMaxPreparedStmtCountReached)
			break
		}
		tk.MustExec(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
	}
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

func TestPrepareWithSnapshot(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

func TestPrepareCacheForPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	// TODO: if not already exists, include test with explicit partition selection
	// both prunable and non-prunable
	tk.MustExec("use test")
	for _, pruneMode := range []string{string(variable.Static), string(variable.Dynamic)} {
		planCacheUsed := "0"
		if pruneMode == string(variable.Dynamic) {
			planCacheUsed = "1"
		}
		tk.MustExec("set @@tidb_partition_prune_mode = '" + pruneMode + "'")
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
		tk.MustExec(`analyze table t_range_index`)
		tk.MustExec("prepare stmt5 from 'select c from t_range_index where id = ?'")
		tk.MustExec("set @id=1")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=5")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=13")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("xyz"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=17")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("hij"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))

		tk.MustExec("drop table if exists t_range_table")
		tk.MustExec("create table t_range_table (id int, k int, c varchar(10)) partition by range(id) ( PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN (14),PARTITION p2 VALUES LESS THAN (20) )")
		tk.MustExec("insert into t_range_table values (1, 2, 'abc'), (5, 4, 'def'), (13, 6, 'xyz'), (17, 6, 'hij')")
		tk.MustExec(`analyze table t_range_table`)
		tk.MustExec("prepare stmt6 from 'select c from t_range_table where id = ?'")
		tk.MustExec("set @id=1")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustExec(`set @@global.tidb_slow_log_threshold = -1`)
		tk.MustQuery(`select * from information_schema.slow_query`).Check(testkit.Rows())
		// TODO: Check pruning, seems like it did not prune!!!
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=5")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=13")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("xyz"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=17")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("hij"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))

		// Test for list partition
		tk.MustExec("drop table if exists t_list_index")
		tk.MustExec("create table t_list_index (id int, k int, c varchar(10), primary key(id)) partition by list (id*2-id) ( PARTITION p0 VALUES IN (1,2,3,4), PARTITION p1 VALUES IN (5,6,7,8),PARTITION p2 VALUES IN (9,10,11,12))")
		tk.MustExec("insert into t_list_index values (1, 1, 'abc'), (5, 5, 'def'), (9, 9, 'xyz'), (12, 12, 'hij')")
		tk.MustExec(`analyze table t_list_index`)
		tk.MustExec("prepare stmt7 from 'select c from t_list_index where id = ?'")
		tk.MustExec("set @id=1")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=5")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=9")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("xyz"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=12")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows("hij"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=100")
		tk.MustQuery("execute stmt7 using @id").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))

		// Test for list columns partition
		tk.MustExec("drop table if exists t_list_index")
		tk.MustExec("create table t_list_index (id int, k int, c varchar(10), primary key(id)) partition by list columns (id) ( PARTITION p0 VALUES IN (1,2,3,4), PARTITION p1 VALUES IN (5,6,7,8),PARTITION p2 VALUES IN (9,10,11,12))")
		tk.MustExec("insert into t_list_index values (1, 1, 'abc'), (5, 5, 'def'), (9, 9, 'xyz'), (12, 12, 'hij')")
		tk.MustExec(`analyze table t_list_index`)
		tk.MustExec("prepare stmt8 from 'select c from t_list_index where id = ?'")
		tk.MustExec("set @id=1")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=5")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=9")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("xyz"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=12")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows("hij"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustExec("set @id=100")
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
		tk.MustQuery("execute stmt8 using @id").Check(testkit.Rows())
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(planCacheUsed))
	}
}

func TestIssue33031(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkExplain := testkit.NewTestKit(t, store)
	tkExplain.MustExec(`use test`)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`use test`)
	// https://github.com/pingcap/tidb/issues/33031
	tk.MustExec(`drop table if exists Issue33031`)
	tk.MustExec(`CREATE TABLE Issue33031 (COL1 int(16) DEFAULT '29' COMMENT 'NUMERIC UNIQUE INDEX', COL2 bigint(20) DEFAULT NULL, UNIQUE KEY UK_COL1 (COL1)) PARTITION BY RANGE (COL1) (PARTITION P0 VALUES LESS THAN (0))`)
	tk.MustExec(`insert into Issue33031 values(-5, 7)`)
	tk.MustExec(`set @@session.tidb_partition_prune_mode='static'`)
	tk.MustExec(`prepare stmt from 'select *,? from Issue33031 where col2 < ? and col1 in (?, ?)'`)
	tk.MustExec(`set @a=111, @b=1, @c=2, @d=22`)
	tk.MustQuery(`execute stmt using @d,@a,@b,@c`).Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`set @a=112, @b=-2, @c=-5, @d=33`)
	tk.MustQuery(`execute stmt using @d,@a,@b,@c`).Check(testkit.Rows("-5 7 33"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`deallocate prepare stmt`)
	tk.MustExec(`set @@session.tidb_partition_prune_mode='dynamic'`)
	tk.MustExec(`analyze table Issue33031`)
	tk.MustExec(`prepare stmt from 'select *,? from Issue33031 where col2 < ? and col1 in (?, ?)'`)
	tk.MustExec(`set @a=111, @b=1, @c=2, @d=22`)
	tk.MustQuery(`execute stmt using @d,@a,@b,@c`).Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	// TODO: Add another test that will use the plan cache instead of [Batch]PointGet
	// when supported for partitioned tables as well.
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`set @a=112, @b=-2, @c=-5, @d=33`)
	tk.MustQuery(`execute stmt using @d,@a,@b,@c`).Check(testkit.Rows("-5 7 33"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	explain := tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Regexp(t, "IndexLookUp", explain[1][0])
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`alter table Issue33031 remove partitioning`)
	tk.MustQuery(`execute stmt using @d,@a,@b,@c`).Check(testkit.Rows("-5 7 33"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Batch/PointGet plans may be over-optimized"))
	tk.MustQuery(`execute stmt using @d,@a,@b,@c`).Check(testkit.Rows("-5 7 33"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	explain = tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Regexp(t, "Batch_Point_Get", explain[2][0])
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Batch/PointGet plans may be over-optimized"))
}

func newSession(t *testing.T, store kv.Storage, dbName string) sessiontypes.Session {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	mustExec(t, se, "create database if not exists "+dbName)
	mustExec(t, se, "use "+dbName)
	return se
}

func mustExec(t *testing.T, se sessiontypes.Session, sql string) {
	_, err := se.Execute(context.Background(), sql)
	require.NoError(t, err)
}

func TestPlanCacheUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`) // insert-tmt can hit the cache and affect hit counter in this UT
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
	err := counter.Write(pb)
	require.NoError(t, err)
	cnt := pb.GetCounter().GetValue()
	require.Equal(t, float64(1), cnt)
	tk.MustExec("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(1), cnt)
	tk.MustExec("insert into t2 values(1)")
	// Cached plan is chosen, modification on t2 does not impact plan of t1.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(2), cnt)
	tk.MustExec("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(3), cnt)

	tk.MustExec("prepare stmt2 from 'select * from t1 left join t2 on true where t1.a > ?'")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	tk.MustExec("begin")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(4), cnt)
	tk.MustExec("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 <nil>",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(4), cnt)
	tk.MustExec("insert into t2 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(4), cnt)
	// Cached plan is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(5), cnt)
	tk.MustExec("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	err = counter.Write(pb)
	require.NoError(t, err)
	cnt = pb.GetCounter().GetValue()
	require.Equal(t, float64(6), cnt)
}

func TestPlanCacheSwitchDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	// create a table in test
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`insert into t values (-1)`)
	tk.MustExec(`prepare stmt from 'select * from t'`)

	// DB is not specified
	se2, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: core.NewLRUPlanCache(100, 0.1, math.MaxUint64, tk.Session(), false),
	})
	require.NoError(t, err)
	tk2 := testkit.NewTestKitWithSession(t, store, se2)
	require.Equal(t, tk2.ExecToErr(`prepare stmt from 'select * from t'`).Error(), "[planner:1046]No database selected")
	require.Equal(t, tk2.ExecToErr(`prepare stmt from 'select * from test.t'`), nil)

	// switch to a new DB
	tk.MustExec(`drop database if exists plan_cache`)
	tk.MustExec(`create database plan_cache`)
	tk.MustExec(`use plan_cache`)
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`execute stmt`).Check(testkit.Rows("-1")) // read test.t
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute stmt`).Check(testkit.Rows("-1")) // read test.t
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// prepare again
	tk.MustExec(`prepare stmt from 'select * from t'`)
	tk.MustQuery(`execute stmt`).Check(testkit.Rows("1")) // read plan_cache.t
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute stmt`).Check(testkit.Rows("1")) // read plan_cache.t
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// specify DB in the query
	tk.MustExec(`prepare stmt from 'select * from test.t'`)
	tk.MustQuery(`execute stmt`).Check(testkit.Rows("-1")) // read test.t
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute stmt`).Check(testkit.Rows("-1")) // read test.t
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestInvisibleIndexPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, unique idx_a(a))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec(`prepare stmt1 from "select a from t order by a"`)

	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	tk.MustExec("alter table t alter index idx_a invisible")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("alter table t alter index idx_a visible")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
}

func TestPlanCacheSnapshot(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
	tso := tk.Session().GetSessionVars().TxnCtx.StartTS
	tk.MustExec("rollback")
	require.True(t, tso > 0)
	// Insert one more row with id = 1.
	tk.MustExec("insert into t values (1)")

	tk.MustExec(fmt.Sprintf("set @@tidb_snapshot = '%d'", tso))
	tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	// enable partition table dynamic mode
	tk.MustExec("create database test_plan_cache")
	tk.MustExec("use test_plan_cache")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@tidb_enable_list_partition = 1")

	seed := time.Now().UnixNano()
	//seed := int64(1704191012078910000)
	t.Logf("seed: %d", seed)
	randomSeed := rand.New(rand.NewSource(seed))

	type testcase struct {
		t1Create string
		t2Create string
		rowGener func() string
		varGener func() string
		query    string
	}
	randDateTime := func() string {
		return fmt.Sprintf("%v-%v-%v %v:%v:%v",
			1950+randomSeed.Intn(100), 1+randomSeed.Intn(12), 1+randomSeed.Intn(28), // date
			randomSeed.Intn(24), randomSeed.Intn(60), randomSeed.Intn(60)) // time
	}
	randDate := func() string {
		return fmt.Sprintf("%v-%v-%v", 1950+randomSeed.Intn(100), 1+randomSeed.Intn(12), 1+randomSeed.Intn(28))
	}
	testcases := []testcase{
		{ // hash partition + int
			"create table t1(a int, b int) partition by hash(a) partitions 20",
			"create table t2(a int, b int)",
			func() string { return fmt.Sprintf("(%v, %v)", randomSeed.Intn(100000000), randomSeed.Intn(100000000)) },
			func() string { return fmt.Sprintf("%v", randomSeed.Intn(100000000)) },
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
			func() string { return fmt.Sprintf("(%v, %v)", randomSeed.Intn(100000000), randomSeed.Intn(100000000)) },
			func() string { return fmt.Sprintf("%v", randomSeed.Intn(100000000)) },
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
			func() string { return fmt.Sprintf(`("%v", "%v")`, randomSeed.Intn(1000), randomSeed.Intn(1000)) },
			func() string { return fmt.Sprintf(`"%v"`, randomSeed.Intn(1000)) },
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
			// This should be able to use plan cache if @a between 14-18
			`create table t1(a int, b int) partition by list(a) (
						partition p0 values in (0, 1, 2, 3, 4),
						partition p1 values in (5, 6, 7, 8, 9),
						partition p2 values in (10, 11, 12, 13, 14),
						partition p3 values in (15, 16, 17, 18, 19))`,
			`create table t2(a int, b int)`,
			func() string { return fmt.Sprintf("(%v, %v)", randomSeed.Intn(20), randomSeed.Intn(20)) },
			func() string { return fmt.Sprintf("%v", 0) },
			//func() string { return fmt.Sprintf("%v", randomSeed.Intn(20)) },
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
		// if global stats are not ready, then dynamic prune mode is not used!
		// TODO: Test without these to prevent prepared plan cache to switch between dynamic
		// and static prune mode.
		tk.MustExec(`analyze table t1`)
		tk.MustExec(`analyze table t2`)

		// the first query, @last_plan_from_cache should be zero
		tk.MustExec(fmt.Sprintf(`prepare stmt1 from "%s"`, fmt.Sprintf(tc.query, "t1")))
		tk.MustExec(fmt.Sprintf(`prepare stmt2 from "%s"`, fmt.Sprintf(tc.query, "t2")))
		tk.MustExec(fmt.Sprintf("set @a=%v", tc.varGener()))
		result1 := tk.MustQuery("execute stmt1 using @a").Sort().Rows()
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute stmt2 using @a").Sort().Check(result1)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

		commentString := fmt.Sprintf("/*\ntc.query=%s\ntc.t1Create=%s */", tc.query, tc.t1Create)
		numWarns := 0
		for i := 0; i < 100; i++ {
			val := tc.varGener()
			t.Logf("@a=%v", val)
			tk.MustExec(fmt.Sprintf("set @a=%v", val))
			result1 = tk.MustQuery("execute stmt1 using @a /* @a=" + val + " i=" + strconv.Itoa(i) + "  */ " + commentString).Sort().Rows()
			foundInPlanCache := tk.Session().GetSessionVars().FoundInPlanCache
			warnings := tk.MustQuery(`show warnings`)
			if len(warnings.Rows()) > 0 {
				warnings.CheckContain("skip plan-cache: plan rebuild failed, ")
				numWarns++
			} else {
				require.True(t, foundInPlanCache, "select @@last_plan_from_cache /* i=%d prepared statement: (t1) %s\n-- create table: %s*/", i, tc.query, tc.t1Create)
			}
			tk.MustQuery("execute stmt2 using @a /* @a=" + val + " i=" + strconv.Itoa(i) + " */ " + commentString).Sort().Check(result1)
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		}
		require.Less(t, numWarns, 5, "Too many skip plan-cache warnings!")
		t.Logf("Create t1: %s\nstmt: %s\nnumWarns: %d", tc.t1Create, tc.query, numWarns)
	}
}

func helperCheckPlanCache(t *testing.T, tk *testkit.TestKit, sql, expected string, arr []string) []string {
	res := tk.MustQuery(sql)
	got := res.Rows()[0][0]
	if expected == "0" {
		require.Equal(t, expected, got, sql)
	} else {
		if got != expected {
			return append(arr, sql)
		}
	}
	return arr
}

func TestPartitionWithVariedDataSources(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

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
		tk.MustExec(`analyze table ` + tbl)
	}

	missedPlanCache := make([]string, 0, 4)
	// TableReader, PointGet on PK, BatchGet on PK
	for _, tbl := range []string{`trangePK`, `thashPK`, `tnormalPK`} {
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_tablescan from 'select * from %v use index(primary) where a > ? and a < ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_pointget from 'select * from %v use index(primary) where a = ?'`, tbl, tbl))
		tk.MustExec(fmt.Sprintf(`prepare stmt%v_batchget from 'select * from %v use index(primary) where a in (?, ?, ?)'`, tbl, tbl))
	}
	loops := 100
	for i := 0; i < loops; i++ {
		mina, maxa := rand.Intn(40000), rand.Intn(40000)
		pointa := mina
		// Allow out-of-range to trigger edge cases for non-matching partition
		a0, a1, a2 := mina, maxa, rand.Intn(41000)
		if i == 1 {
			// test what happens if we have duplicates
			a2 = a0
		}
		if mina > maxa {
			mina, maxa = maxa, mina
		}
		tk.MustExec(fmt.Sprintf(`set @mina=%v, @maxa=%v`, mina, maxa))
		tk.MustExec(fmt.Sprintf(`set @pointa=%v`, pointa))
		tk.MustExec(fmt.Sprintf(`set @a0=%v, @a1=%v, @a2=%v`, a0, a1, a2))

		var rscan, rpoint, rbatch [][]any
		for id, tbl := range []string{`trangePK`, `thashPK`, `tnormalPK`} {
			scan := tk.MustQuery(fmt.Sprintf(`execute stmt%v_tablescan using @mina, @maxa`, tbl)).Sort()
			if id == 0 {
				rscan = scan.Rows()
			} else {
				scan.Check(rscan)
			}
			if i > 0 {
				tblStr := ` table: ` + tbl + " i :" + strconv.FormatInt(int64(i), 10) + " */"
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* tablescan table: `+tblStr, "1", missedPlanCache)
			}

			point := tk.MustQuery(fmt.Sprintf(`execute stmt%v_pointget using @pointa`, tbl)).Sort()
			if id == 0 {
				rpoint = point.Rows()
			} else {
				point.Check(rpoint)
			}
			if i > 0 {
				tblStr := ` table: ` + tbl + " i :" + strconv.FormatInt(int64(i), 10) + " */"
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* pointget table: `+tblStr, "1", missedPlanCache)
			}

			batch := tk.MustQuery(fmt.Sprintf(`execute stmt%v_batchget using @a0, @a1, @a2`, tbl)).Sort()
			if id == 0 {
				rbatch = batch.Rows()
			} else {
				batch.Check(rbatch)
			}
			if i > 0 {
				tblStr := ` table: ` + tbl + " i :" + strconv.FormatInt(int64(i), 10) + " */"
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* batchget table: `+tblStr, "1", missedPlanCache)
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
	for i := 0; i < loops; i++ {
		mina, maxa := rand.Intn(40000), rand.Intn(40000)
		if mina > maxa {
			mina, maxa = maxa, mina
		}
		tk.MustExec(fmt.Sprintf(`set @mina=%v, @maxa=%v`, mina, maxa))
		tk.MustExec(fmt.Sprintf(`set @pointa=%v`, rand.Intn(40000)))
		// Allow out-of-range to trigger edge cases for non-matching partition
		a0, a1, a2 := rand.Intn(40000), rand.Intn(40000), rand.Intn(41000)
		if i == 1 {
			// test what happens if we have duplicates
			a2 = a0
		}
		tk.MustExec(fmt.Sprintf(`set @a0=%v, @a1=%v, @a2=%v`, a0, a1, a2))

		var rscan, rlookup, rpoint, rbatch [][]any
		for id, tbl := range []string{"trangeIdx", "thashIdx", "tnormalIdx"} {
			scan := tk.MustQuery(fmt.Sprintf(`execute stmt%v_indexscan using @mina, @maxa`, tbl)).Sort()
			tblStr := ` table: ` + tbl + " i :" + strconv.FormatInt(int64(i), 10) + " */"
			if i > 0 {
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* indexscan table: `+tblStr, "1", missedPlanCache)
			}
			if id == 0 {
				rscan = scan.Rows()
			} else {
				scan.Check(rscan)
			}

			lookup := tk.MustQuery(fmt.Sprintf(`execute stmt%v_indexlookup using @mina, @maxa`, tbl)).Sort()
			if i > 0 {
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* indexlookup table: `+tblStr, "1", missedPlanCache)
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
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* pointget table: `+tblStr, "1", missedPlanCache)
			}
			if id == 0 {
				rpoint = point.Rows()
			} else {
				point.Check(rpoint)
			}

			batch := tk.MustQuery(fmt.Sprintf(`execute stmt%v_batchget_idx using @a0, @a1, @a2`, tbl)).Sort()
			if i > 0 {
				missedPlanCache = helperCheckPlanCache(t, tk, `select @@last_plan_from_cache /* batchget table: `+tblStr, "1", missedPlanCache)
			}
			if id == 0 {
				rbatch = batch.Rows()
			} else {
				batch.Check(rbatch)
			}
		}
	}
	// Allow <=10% non-cached queries, due to background changes etc.
	// + 3 intentionally misses, due to duplicate values
	// Notice there are 3 tables * 4 queries per loop :)
	if len(missedPlanCache) > (3 + 10*loops*3*4/100) {
		require.Equal(t, []string{}, missedPlanCache)
	}
}

func TestCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a int, b int, index i_b(b))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("alter table t cache")

	tk.MustExec("prepare tableScan from 'select * from t where a>=?'")
	tk.MustExec("prepare indexScan from 'select b from t use index(i_b) where b>?'")
	tk.MustExec("prepare indexLookup from 'select a from t use index(i_b) where b>? and b<?'")
	tk.MustExec("prepare pointGet from 'select b from t use index(i_b) where b=?'")
	tk.MustExec("set @a=1, @b=3")

	lastReadFromCache := func(tk *testkit.TestKit) bool {
		return tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	}

	var cacheLoaded bool
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 2"))
		if lastReadFromCache(tk) {
			cacheLoaded = true
			break
		}
	}
	require.True(t, cacheLoaded)

	// Cache the plan.
	tk.MustQuery("execute tableScan using @a").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("execute indexScan using @a").Check(testkit.Rows("2"))
	tk.MustQuery("execute indexLookup using @a, @b").Check(testkit.Rows("2"))
	tk.MustQuery("execute pointGet using @a").Check(testkit.Rows("1"))

	// Table Scan
	tk.MustQuery("execute tableScan using @a").Check(testkit.Rows("1 1", "2 2"))
	require.True(t, lastReadFromCache(tk))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Index Scan
	tk.MustQuery("execute indexScan using @a").Check(testkit.Rows("2"))
	require.True(t, lastReadFromCache(tk))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// IndexLookup
	tk.MustQuery("execute indexLookup using @a, @b").Check(testkit.Rows("2"))
	require.True(t, lastReadFromCache(tk))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // b>1 and b<3 --> b=2

	// PointGet
	tk.MustQuery("execute pointGet using @a").Check(testkit.Rows("1"))
	require.True(t, lastReadFromCache(tk))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPlanCacheWithRCWhenInfoSchemaChange(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int primary key, c int, index ic (c))")
	// prepare text protocol
	tk1.MustExec("prepare s from 'select /*+use_index(t1, ic)*/ * from t1 where 1'")
	// prepare binary protocol
	stmtID, _, _, err := tk2.Session().PrepareStmt("select /*+use_index(t1, ic)*/ * from t1 where 1")
	require.Nil(t, err)
	tk1.MustExec("set tx_isolation='READ-COMMITTED'")
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("set tx_isolation='READ-COMMITTED'")
	tk2.MustExec("begin pessimistic")
	tk1.MustQuery("execute s").Check(testkit.Rows())
	rs, err := tk2.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk2.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows())

	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	tk3.MustExec("alter table t1 drop index ic")
	tk3.MustExec("insert into t1 values(1, 0)")

	// The execution after schema changed should not hit plan cache.
	// execute text protocol
	tk1.MustQuery("execute s").Check(testkit.Rows("1 0"))
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	// execute binary protocol
	rs, err = tk2.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk2.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 0"))
	tk2.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestConsistencyBetweenPrepareExecuteAndNormalSql(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk1.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk2.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int primary key, c int)")
	tk1.MustExec("insert into t1 values(1, 1), (2, 2)")
	// prepare text protocol
	tk1.MustExec("prepare s from 'select * from t1'")
	// prepare binary protocol
	stmtID, _, _, err := tk1.Session().PrepareStmt("select * from t1")
	require.Nil(t, err)
	tk1.MustExec("set tx_isolation='READ-COMMITTED'")
	tk1.MustExec("begin pessimistic")
	tk2.MustExec("set tx_isolation='READ-COMMITTED'")
	tk2.MustExec("begin pessimistic")

	// Execute using sql
	tk1.MustQuery("execute s").Check(testkit.Rows("1 1", "2 2"))
	// Execute using binary
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1", "2 2"))
	// Normal sql
	tk1.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))

	// Change infoSchema
	tk2.MustExec("alter table t1 drop column c")
	tk2.MustExec("insert into t1 values (3)")
	// Execute using sql
	tk1.MustQuery("execute s").Check(testkit.Rows("1 1", "2 2", "3 <nil>"))
	// Execute using binary
	rs, err = tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1", "2 2", "3 <nil>"))
	// Normal sql
	tk1.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2", "3 <nil>"))
	tk1.MustExec("commit")

	// After beginning a new txn, the infoSchema should be the latest
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select * from t1").Check(testkit.Rows("1", "2", "3"))
}

func verifyCache(ctx context.Context, t *testing.T, tk1 *testkit.TestKit, tk2 *testkit.TestKit, stmtID uint32) {
	// Cache miss in the firs time.
	tk1.MustExec("execute s")
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// This time, the cache will be hit.
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.NoError(t, err)
	require.NoError(t, rs.Close())
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk1.MustExec("execute s")
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Change infoSchema version which will make the plan cache invalid in the next execute
	// DDL is blocked by MDL.
	//tk2.MustExec("alter table t1 drop column c")
	//tk1.MustExec("execute s")
	//tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	//// Now the plan cache will be valid
	//rs, err = tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	//require.NoError(t, err)
	//require.NoError(t, rs.Close())
	//tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestCacheHitInRc(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("set global tidb_enable_metadata_lock=0")
	tk1.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk2.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int primary key, c int)")
	tk1.MustExec("insert into t1 values(1, 1), (2, 2)")
	// prepare text protocol
	tk1.MustExec("prepare s from 'select * from t1'")
	// prepare binary protocol
	stmtID, _, _, err := tk1.Session().PrepareStmt("select * from t1")
	require.Nil(t, err)

	// Test for RC
	tk1.MustExec("set tx_isolation='READ-COMMITTED'")
	tk1.MustExec("begin pessimistic")

	// Verify for the RC isolation
	verifyCache(ctx, t, tk1, tk2, stmtID)
	tk1.MustExec("rollback")
}

func TestCacheHitInForUpdateRead(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk2.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int primary key, c int)")
	tk1.MustExec("insert into t1 values(1, 1), (2, 2)")

	tk1.MustExec("prepare s from 'select * from t1 where id = 1 for update'")
	stmtID, _, _, err := tk1.Session().PrepareStmt("select * from t1 where id = 1 for update")
	require.Nil(t, err)
	tk1.MustExec("begin pessimistic")

	// Verify for the for update read
	verifyCache(ctx, t, tk1, tk2, stmtID)
	tk1.MustExec("rollback")
}

func TestPointGetForUpdateAutoCommitCache(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk2.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("drop table if exists t1")
	tk1.MustExec("create table t1(id int primary key, c int)")
	tk1.MustExec("insert into t1 values(1, 1), (2, 2)")

	tk1.MustExec("prepare s from 'select * from t1 where id = 1 for update'")
	stmtID, _, _, err := tk1.Session().PrepareStmt("select * from t1 where id = 1 for update")
	require.Nil(t, err)
	rs, err := tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk2.MustExec("alter table t1 drop column c")
	tk2.MustExec("update t1 set id = 10 where id = 1")

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows())
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	rs, err = tk1.Session().ExecutePreparedStmt(ctx, stmtID, expression.Args2Expressions4Test())
	require.Nil(t, err)
	tk1.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows())
	tk1.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPrepareCacheForDynamicPartitionPruning(t *testing.T) {
	// https://github.com/pingcap/tidb/issues/33031
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select @@session.tidb_enable_prepared_plan_cache`).Check(testkit.Rows("1"))

	tk.MustExec("use test")
	tkExplain := testkit.NewTestKit(t, store)
	tkExplain.MustExec("use test")
	for _, pruneMode := range []string{string(variable.Static), string(variable.Dynamic)} {
		tk.MustExec("set @@tidb_partition_prune_mode = '" + pruneMode + "'")

		tk.MustExec(`drop table if exists t`)
		tk.MustExec(`CREATE TABLE t (a int(16), b bigint, UNIQUE KEY (a)) PARTITION BY RANGE (a) (PARTITION P0 VALUES LESS THAN (0))`)
		tk.MustExec(`insert into t values(-5, 7)`)
		tk.MustExec(`analyze table t`)
		tk.MustExec(`prepare stmt from 'select * from t where a = ? and b < ?'`)
		tk.MustExec(`set @a=1, @b=111`)
		// Note that this is not matching any partition!
		tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkProcess := tk.Session().ShowProcess()
		ps := []*util.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		explain := tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
		if pruneMode == string(variable.Dynamic) {
			require.Equal(t, "Selection_6", explain.Rows()[0][0])
		} else {
			require.Equal(t, "TableDual_7", explain.Rows()[0][0])
		}
		tk.MustExec(`set @a=-5, @b=112`)
		tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows("-5 7"))

		explain = tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
		if pruneMode == string(variable.Dynamic) {
			require.Equal(t, "Selection_6", explain.Rows()[0][0])
			require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
			tk.MustQuery(`show warnings`).Check(testkit.Rows())
		} else {
			explain.CheckAt([]int{0},
				[][]any{
					{"Selection_8"},
					{"└─Point_Get_7"},
				})
			require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
			tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query accesses partitioned tables is un-cacheable if tidb_partition_pruning_mode = 'static'"))
		}

		// Test TableDual
		tk.MustExec(`set @b=5, @a=113`)
		tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
		require.Equal(t, pruneMode == string(variable.Dynamic), tk.Session().GetSessionVars().FoundInPlanCache)
	}
}

func TestHashPartitionAndPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select @@session.tidb_enable_prepared_plan_cache`).Check(testkit.Rows("1"))
	tkExplain := testkit.NewTestKit(t, store)
	tkExplain.MustExec("use test")

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (b varchar(255), a int primary key nonclustered, key (b)) PARTITION BY HASH (a) partitions 5`)
	tk.MustExec(`insert into t values(0,0),(1,1),(2,2),(3,3),(4,4)`)
	tk.MustExec(`insert into t select b + 5, a + 5 from t`)
	tk.MustExec(`analyze table t`)

	// Point get PK
	tk.MustExec(`prepare stmt from 'select * from t where a = ?'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1 1"))
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	explain := tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	require.Equal(t, "Point_Get_1", explain.Rows()[0][0])
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	tk.MustExec(`set @a=2`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("2 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	explain = tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	require.Equal(t, "Point_Get_1", explain.Rows()[0][0])
	tk.MustExec(`drop table t`)

	tk.MustExec(`CREATE TABLE t (b varchar(255), a int, key (b), unique key (a)) PARTITION BY HASH (a) partitions 5`)
	tk.MustExec(`insert into t values(0,0),(1,1),(2,2),(3,3),(4,4)`)
	tk.MustExec(`insert into t select b + 5, a + 5 from t`)
	tk.MustExec(`analyze table t`)

	// Point get Unique Key
	tk.MustExec(`prepare stmt from 'select * from t where a = ?'`)
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1 1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`set @a=2`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("2 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	explain = tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
	require.Equal(t, "Point_Get_1", explain.Rows()[0][0])
}
