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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestInitLRUWithSystemVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_prepared_plan_cache_size = 0") // MinValue: 1
	tk.MustQuery("select @@session.tidb_prepared_plan_cache_size").Check(testkit.Rows("1"))
	sessionVar := tk.Session().GetSessionVars()

	lru := plannercore.NewLRUPlanCache(uint(sessionVar.PreparedPlanCacheSize), 0, 0, tk.Session(), false)
	require.NotNil(t, lru)
}

func TestNonPreparedPlanCachePlanString(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)

	ctx := tk.Session()
	planString := func(sql string) string {
		stmts, err := session.Parse(ctx, sql)
		require.NoError(t, err)
		stmt := stmts[0]
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), ctx, stmt, plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		require.NoError(t, err)
		return plannercore.ToString(p)
	}

	require.Equal(t, planString("select a from t where a < 1"), "IndexReader(Index(t.a)[[-inf,1)])")
	require.Equal(t, planString("select a from t where a < 10"), "IndexReader(Index(t.a)[[-inf,10)])") // range 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	require.Equal(t, planString("select * from t where b < 1"), "TableReader(Table(t)->Sel([lt(test.t.b, 1)]))")
	require.Equal(t, planString("select * from t where b < 10"), "TableReader(Table(t)->Sel([lt(test.t.b, 10)]))") // filter 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheInformationSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})

	stmt, err := p.ParseOneStmt("select avg(a),avg(b),avg(c) from t", "", "")
	require.NoError(t, err)
	err = plannercore.Preprocess(context.Background(), tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
	require.NoError(t, err) // no error
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, is)
	require.NoError(t, err) // no error
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, is)
	require.NoError(t, err) // no error
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestNonPreparedPlanTypeRandomly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, key(a))`)
	tk.MustExec(`create table t2 (a varchar(8), b varchar(8), key(a))`)
	tk.MustExec(`create table t3 (a double, b double, key(a))`)
	tk.MustExec(`create table t4 (a decimal(4, 2), b decimal(4, 2), key(a))`)
	tk.MustExec(`create table t5 (a year, b year, key(a))`)
	tk.MustExec(`create table t6 (a date, b date, key(a))`)
	tk.MustExec(`create table t7 (a datetime, b datetime, key(a))`)

	n := 30
	for i := 0; i < n; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t1 values (%v, %v)`, randNonPrepTypeVal(t, n, "int"), randNonPrepTypeVal(t, n, "int")))
		tk.MustExec(fmt.Sprintf(`insert into t2 values (%v, %v)`, randNonPrepTypeVal(t, n, "varchar"), randNonPrepTypeVal(t, n, "varchar")))
		tk.MustExec(fmt.Sprintf(`insert into t3 values (%v, %v)`, randNonPrepTypeVal(t, n, "double"), randNonPrepTypeVal(t, n, "double")))
		tk.MustExec(fmt.Sprintf(`insert into t4 values (%v, %v)`, randNonPrepTypeVal(t, n, "decimal"), randNonPrepTypeVal(t, n, "decimal")))
		// TODO: fix it later
		//tk.MustExec(fmt.Sprintf(`insert into t5 values (%v, %v)`, randNonPrepTypeVal(t, n, "year"), randNonPrepTypeVal(t, n, "year")))
		tk.MustExec(fmt.Sprintf(`insert into t6 values (%v, %v)`, randNonPrepTypeVal(t, n, "date"), randNonPrepTypeVal(t, n, "date")))
		tk.MustExec(fmt.Sprintf(`insert into t7 values (%v, %v)`, randNonPrepTypeVal(t, n, "datetime"), randNonPrepTypeVal(t, n, "datetime")))
	}

	for i := 0; i < 200; i++ {
		q := fmt.Sprintf(`select * from t%v where %v`, rand.Intn(7)+1, randNonPrepFilter(t, n))
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
		r0 := tk.MustQuery(q).Sort()            // the first execution
		tk.MustQuery(q).Sort().Check(r0.Rows()) // may hit the cache
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
		tk.MustQuery(q).Sort().Check(r0.Rows()) // disable the non-prep cache
	}
}

func randNonPrepFilter(t *testing.T, scale int) string {
	switch rand.Intn(4) {
	case 0: // >=
		return fmt.Sprintf(`a >= %v`, randNonPrepVal(t, scale))
	case 1: // <
		return fmt.Sprintf(`a < %v`, randNonPrepVal(t, scale))
	case 2: // =
		return fmt.Sprintf(`a = %v`, randNonPrepVal(t, scale))
	case 3: // in
		return fmt.Sprintf(`a in (%v, %v)`, randNonPrepVal(t, scale), randNonPrepVal(t, scale))
	}
	require.Error(t, errors.New(""))
	return ""
}

func randNonPrepVal(t *testing.T, scale int) string {
	return randNonPrepTypeVal(t, scale, [7]string{"int", "varchar", "double",
		"decimal", "year", "datetime", "date"}[rand.Intn(7)])
}

func randNonPrepTypeVal(t *testing.T, scale int, typ string) string {
	switch typ {
	case "int":
		return fmt.Sprintf("%v", rand.Intn(scale)-(scale/2))
	case "varchar":
		return fmt.Sprintf("'%v'", rand.Intn(scale)-(scale/2))
	case "double", "decimal":
		return fmt.Sprintf("%v", float64(rand.Intn(scale)-(scale/2))/float64(10))
	case "year":
		return fmt.Sprintf("%v", 2000+rand.Intn(scale))
	case "date":
		return fmt.Sprintf("'2023-01-%02d'", rand.Intn(scale)+1)
	case "timestamp", "datetime":
		return fmt.Sprintf("'2023-01-01 00:00:%02d'", rand.Intn(scale))
	default:
		require.Error(t, errors.New(typ))
		return ""
	}
}

func TestNonPreparedPlanCacheBasically(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, key(b), key(c, d))`)
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v, %v, %v)", i, rand.Intn(20), rand.Intn(20), rand.Intn(20)))
	}

	queries := []string{
		"select * from t where a<10",
		"select * from t where a<13 and b<15",
		"select * from t where b=13",
		"select * from t where c<8",
		"select * from t where d>8",
		"select * from t where c=8 and d>10",
		"select * from t where a<12 and b<13 and c<12 and d>2",
		"select * from t where a in (1, 2, 3)",
		"select * from t where a<13 or b<15",
		"select * from t where a<13 or b<15 and c=13",
		"select * from t where a in (1, 2)",
		"select * from t where a in (1, 2) and b in (1, 2, 3)",
		"select * from t where a in (1, 2) and b < 15",
		"select * from t where a between 1 and 10",
		"select * from t where a between 1 and 10 and b < 15",
	}

	for _, query := range queries {
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
		resultNormal := tk.MustQuery(query).Sort()
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
		tk.MustQuery(query)                                                    // first process
		tk.MustQuery(query).Sort().Check(resultNormal.Rows())                  // equal to the result without plan-cache
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // this plan is from plan-cache
	}
}

func TestNonPreparedPlanCacheInternalSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	tk.Session().GetSessionVars().InRestrictedSQL = true
	tk.MustExecWithContext(ctx, "select * from t where a=1")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.Session().GetSessionVars().InRestrictedSQL = false
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestIssue38269(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int, b int, c int, index idx(a, b))")
	tk.MustExec("prepare stmt1 from 'select /*+ inl_join(t2) */ * from t1 join t2 on t1.a = t2.a where t2.b in (?, ?, ?)'")
	tk.MustExec("set @a = 10, @b = 20, @c = 30, @d = 40, @e = 50, @f = 60")
	tk.MustExec("execute stmt1 using @a, @b, @c")
	tk.MustExec("execute stmt1 using @d, @e, @f")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Contains(t, rows[6][4], "range: decided by [eq(test.t2.a, test.t1.a) in(test.t2.b, 40, 50, 60)]")
}

func TestIssue38533(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key (a))")
	tk.MustExec(`prepare st from "select /*+ use_index(t, a) */ a from t where a=? and a=?"`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a, @a`)
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(plan[1][0].(string), "RangeScan")) // range-scan instead of full-scan

	tk.MustExec(`execute st using @a, @a`)
	tk.MustExec(`execute st using @a, @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestInvalidRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select * from t where a>? and a<?'")
	tk.MustExec("set @l=100, @r=10")
	tk.MustExec("execute st using @l, @r")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{{"TableDual_5"}}) // use TableDual directly instead of TableFullScan

	tk.MustExec("execute st using @l, @r")
	tk.MustExec("execute st using @l, @r")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue49344(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int)`)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`prepare s from "select * from t"`)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=0`)
	tk.MustExec(`execute s`) // no error
}

func TestIssue40093(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, b int, key(b, a))")
	tk.MustExec("prepare st from 'select * from t1 left join t2 on t1.a=t2.a where t2.b in (?)'")
	tk.MustExec("set @b=1")
	tk.MustExec("execute st using @b")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"Projection_9"},
			{"└─HashJoin_21"},
			{"  ├─IndexReader_26(Build)"},
			{"  │ └─IndexRangeScan_25"}, // RangeScan instead of FullScan
			{"  └─TableReader_24(Probe)"},
			{"    └─Selection_23"},
			{"      └─TableFullScan_22"},
		})

	tk.MustExec("execute st using @b")
	tk.MustExec("execute st using @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue38205(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("CREATE TABLE `item` (`id` int, `vid` varbinary(16), `sid` int)")
	tk.MustExec("CREATE TABLE `lv` (`item_id` int, `sid` int, KEY (`sid`,`item_id`))")

	tk.MustExec("prepare stmt from 'SELECT /*+ TIDB_INLJ(lv, item) */ * FROM lv LEFT JOIN item ON lv.sid = item.sid AND lv.item_id = item.id WHERE item.sid = ? AND item.vid IN (?, ?)'")
	tk.MustExec("set @a=1, @b='1', @c='3'")
	tk.MustExec("execute stmt using @a, @b, @c")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexJoin_10"},
			{"├─TableReader_19(Build)"},
			{"│ └─Selection_18"},
			{"│   └─TableFullScan_17"}, // RangeScan instead of FullScan
			{"└─IndexReader_9(Probe)"},
			{"  └─Selection_8"},
			{"    └─IndexRangeScan_7"},
		})

	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue40224(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select a from t where a in (?, ?)'")
	tk.MustExec("set @a=1.0, @b=2.0")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1.0' may be converted to INT"))
	tk.MustExec("execute st using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexReader_6"},
			{"└─IndexRangeScan_5"}, // range scan not full scan
		})

	tk.MustExec("set @a=1, @b=2")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows()) // no warning for INT values
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // cacheable for INT
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexReader_6"},
			{"└─IndexRangeScan_5"}, // range scan not full scan
		})
}

func TestIssue40679(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a));")
	tk.MustExec("prepare st from 'select * from t use index(a) where a < ?'")
	tk.MustExec("set @a1=1.1")
	tk.MustExec("execute st using @a1")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(rows[1][0].(string), "RangeScan")) // RangeScan not FullScan

	tk.MustExec("execute st using @a1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1.1' may be converted to INT"))
}

func TestPlanCacheWithLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")

	testCases := []struct {
		sql    string
		params []int
	}{
		{"prepare stmt from 'select * from t limit ?'", []int{1}},
		{"prepare stmt from 'select * from t limit 1, ?'", []int{1}},
		{"prepare stmt from 'select * from t limit ?, 1'", []int{1}},
		{"prepare stmt from 'select * from t limit ?, ?'", []int{1, 2}},
		{"prepare stmt from 'delete from t order by a limit ?'", []int{1}},
		{"prepare stmt from 'insert into t select * from t order by a desc limit ?'", []int{1}},
		{"prepare stmt from 'insert into t select * from t order by a desc limit ?, ?'", []int{1, 2}},
		{"prepare stmt from 'update t set a = 1 limit ?'", []int{1}},
		{"prepare stmt from '(select * from t order by a limit ?) union (select * from t order by a desc limit ?)'", []int{1, 2}},
	}

	for idx, testCase := range testCases {
		tk.MustExec(testCase.sql)
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		if idx < 9 {
			// none point get plan
			tk.MustExec("set @a0 = 6")
			tk.MustExec("execute stmt using " + strings.Join(using, ", "))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}

	tk.MustExec("prepare stmt from 'select * from t limit ?'")
	tk.MustExec("set @a = 10001")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: limit count is too large"))
}

func TestPlanCacheWithSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	testCases := []struct {
		sql            string
		params         []int
		cacheAble      string
		isDecorrelated bool
	}{
		{"select * from t t1 where exists (select 1 from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}, "1", false},      // exist
		{"select * from t t1 where t1.a in (select a from t t2 where t2.b < ?)", []int{1}, "1", false},                     // in
		{"select * from t t1 where t1.a > (select max(a) from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}, "0", false}, // scala
		{"select * from t t1 where t1.a > (select 1 from t t2 where t2.b<?)", []int{1}, "0", true},                         // uncorrelated
		{"select * from t t1 where exists (select b from t t2 where t1.a = t2.a and t2.b<? limit 1)", []int{1}, "1", false},
		{"select * from t t1 where exists (select b from t t2 where t1.a = t2.a and t2.b<? limit ?)", []int{1, 1}, "1", false},
	}

	// switch on
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(testCase.cacheAble))
		if testCase.cacheAble == "0" {
			tk.MustExec("execute stmt using " + strings.Join(using, ", "))
			if testCase.isDecorrelated {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has uncorrelated sub-queries is un-cacheable"))
			} else {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: PhysicalApply plan is un-cacheable"))
			}
		}
	}
	// switch off
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = 0")
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has sub-queries is un-cacheable"))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}

func convertQueryToPrepExecStmt(q string) (normalQuery, prepStmt string, parameters []string) {
	// select ... from t where a = #?1# and b = #?2#
	normalQuery = strings.ReplaceAll(q, "#", "")
	normalQuery = strings.ReplaceAll(normalQuery, "?", "")
	vs := strings.Split(q, "#")
	for i := 0; i < len(vs); i++ {
		if len(vs[i]) == 0 {
			continue
		}
		if vs[i][0] == '?' {
			parameters = append(parameters, vs[i][1:])
			vs[i] = "?"
		}
	}
	return normalQuery, fmt.Sprintf(`prepare st from '%v'`, strings.Join(vs, "")), parameters
}

func planCachePointGetPrepareData(tk *testkit.TestKit) {
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	t := func() string {
		types := []string{"int", "varchar(10)", "decimal(10, 2)", "double"}
		return types[rand.Intn(len(types))]
	}
	tk.MustExec(fmt.Sprintf(`create table t1 (a %v, b %v, c %v, d %v, primary key(a), unique key(b), unique key(c), unique key(c))`, t(), t(), t(), t()))
	tk.MustExec(fmt.Sprintf(`create table t2 (a %v, b %v, c %v, d %v, primary key(a, b), unique key(c, d))`, t(), t(), t(), t()))

	var vals []string
	for i := 0; i < 50; i++ {
		vals = append(vals, fmt.Sprintf("('%v.%v', '%v.%v', '%v.%v', '%v.%v')",
			i-20, rand.Intn(5),
			i-20, rand.Intn(5),
			i-20, rand.Intn(5),
			i-20, rand.Intn(5)))
	}
	tk.MustExec(fmt.Sprintf(`insert into t1 values %v`, strings.Join(vals, ",")))
	tk.MustExec(`insert into t1 values ('31', '31', null, null), ('32', null, 32, null)`)
	tk.MustExec(fmt.Sprintf(`insert into t2 values %v`, strings.Join(vals, ",")))
}

func planCachePointGetQueries(isNonPrep bool) []string {
	v := func() string {
		var vStr string
		switch rand.Intn(3) {
		case 0: // int
			vStr = fmt.Sprintf("%v", rand.Intn(50)-20)
		case 1: // double
			vStr = fmt.Sprintf("%v.%v", rand.Intn(50)-20, rand.Intn(100))
		default: // string
			vStr = fmt.Sprintf("'%v.%v'", rand.Intn(50)-20, rand.Intn(100))
		}
		if !isNonPrep {
			vStr = fmt.Sprintf("#?%v#", vStr)
		}
		return vStr
	}
	f := func() string {
		cols := []string{"a", "b", "c", "d"}
		col := cols[rand.Intn(len(cols))]
		ops := []string{"=", ">", "<", ">=", "<=", "in", "is null"}
		op := ops[rand.Intn(len(ops))]
		if op == "in" {
			return fmt.Sprintf("%v %v (%v, %v, %v)", col, op, v(), v(), v())
		} else if op == "is null" {
			return fmt.Sprintf("%v %v", col, op)
		}
		return fmt.Sprintf("%v %v %v", col, op, v())
	}
	var queries []string
	for i := 0; i < 50; i++ {
		queries = append(queries, fmt.Sprintf("select * from t1 where %v", f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v", f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v and %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v and %v and %v", f(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v or %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t1 where %v and %v or %v and %v", f(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v", f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v", f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v and %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v and %v and %v", f(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v or %v", f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select * from t2 where %v and %v or %v and %v", f(), f(), f(), f()))
	}
	return queries
}

func planCacheIntConvertQueries(isNonPrep bool) []string {
	cols := []string{"a", "b", "c", "d"}
	ops := []string{"=", ">", "<", ">=", "<=", "in", "is null"}
	v := func() string {
		var val string
		switch rand.Intn(3) {
		case 0:
			val = fmt.Sprintf("%v", 2000+rand.Intn(20)-10)
		case 1:
			val = fmt.Sprintf("%v.0", 2000+rand.Intn(20)-10)
		default:
			val = fmt.Sprintf("'%v'", 2000+rand.Intn(20)-10)
		}
		if !isNonPrep {
			val = fmt.Sprintf("#?%v#", val)
		}
		return val
	}
	f := func() string {
		col := cols[rand.Intn(len(cols))]
		op := ops[rand.Intn(len(ops))]
		if op == "is null" {
			return fmt.Sprintf("%v is null", col)
		} else if op == "in" {
			if rand.Intn(2) == 0 {
				return fmt.Sprintf("%v in (%v)", col, v())
			}
			return fmt.Sprintf("%v in (%v, %v, %v)", col, v(), v(), v())
		}
		return fmt.Sprintf("%v %v %v", col, op, v())
	}
	fields := func() string {
		var fs []string
		for _, f := range []string{"a", "b", "c", "d"} {
			if rand.Intn(4) == 0 {
				continue
			}
			fs = append(fs, f)
		}
		if len(fs) == 0 {
			return "*"
		}
		return strings.Join(fs, ", ")
	}
	var queries []string
	for i := 0; i < 50; i++ {
		queries = append(queries, fmt.Sprintf("select %v from t where %v", fields(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v and %v", fields(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v and %v and %v", fields(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v or %v", fields(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v or %v or %v", fields(), f(), f(), f()))
		queries = append(queries, fmt.Sprintf("select %v from t where %v and %v or %v", fields(), f(), f(), f()))
	}
	return queries
}

func planCacheIntConvertPrepareData(tk *testkit.TestKit) {
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int, b year, c double, d varchar(16), key(a), key(b), key(c))`)
	vals := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		a := fmt.Sprintf("%v", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			a = "null"
		}
		b := fmt.Sprintf("%v", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			b = "null"
		}
		c := fmt.Sprintf("%v.0", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			c = "null"
		}
		d := fmt.Sprintf("'%v'", 2000+rand.Intn(20)-10)
		if rand.Intn(10) == 0 {
			d = "null"
		}
		vals = append(vals, fmt.Sprintf("(%s, %s, %s, %s)", a, b, c, d))
	}
	tk.MustExec("insert into t values " + strings.Join(vals, ","))
}

func planCacheIndexMergeQueries(isNonPrep bool) []string {
	ops := []string{"=", ">", "<", ">=", "<=", "in", "mod", "is null"}
	f := func(col string) string {
		n := rand.Intn(20) - 10
		nStr := fmt.Sprintf("%v", n)
		if !isNonPrep {
			nStr = fmt.Sprintf("#?%v#", n)
		}

		op := ops[rand.Intn(len(ops))]
		if op == "in" {
			switch rand.Intn(3) {
			case 0: // 1 element
				return fmt.Sprintf("%s %s (%s)", col, op, nStr)
			case 1: // multiple same elements
				return fmt.Sprintf("%s %s (%s, %s, %s)", col, op, nStr, nStr, nStr)
			default: // multiple different elements
				if isNonPrep {
					return fmt.Sprintf("%s %s (%d, %d)", col, op, n, n+1)
				}
				return fmt.Sprintf("%s %s (#?%d#, #?%d#)", col, op, n, n+1)
			}
		} else if op == "mod" { // this filter cannot be used to build range
			return fmt.Sprintf("mod(%s, %s)=0", col, nStr)
		} else if op == "is null" {
			return fmt.Sprintf("%s %s", col, op)
		} else {
			return fmt.Sprintf("%s %s %s", col, op, nStr)
		}
	}
	fields := func() string {
		switch rand.Intn(5) {
		case 0:
			return "a"
		case 1:
			return "a, b"
		case 2:
			return "a, c"
		case 3:
			return "d"
		default:
			return "*"
		}
	}
	var queries []string
	for i := 0; i < 50; i++ {
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where %s and %s", fields(), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s and %s", fields(), f("a"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s and %s and %s", fields(), f("a"), f("b"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where %s or %s", fields(), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s or %s", fields(), f("a"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s or %s or %s", fields(), f("a"), f("b"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where %s and %s and %s", fields(), f("a"), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s and %s and %s", fields(), f("a"), f("c"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s and %s and %s and %s", fields(), f("a"), f("b"), f("b"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b) */ %s from t where (%s and %s) or %s", fields(), f("a"), f("a"), f("b")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, c) */ %s from t where %s or (%s and %s)", fields(), f("a"), f("c"), f("c")))
		queries = append(queries, fmt.Sprintf("select /*+ use_index_merge(t, a, b, c) */ %s from t where %s or (%s and %s) or %s", fields(), f("a"), f("b"), f("b"), f("c")))
	}
	return queries
}

func planCacheIndexMergePrepareData(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int, key(a), key(b), key(c))")
	vals := make([]string, 0, 50)
	v := func() string {
		if rand.Intn(10) == 0 {
			return "null"
		}
		return fmt.Sprintf("%d", rand.Intn(20)-10)
	}
	for i := 0; i < 50; i++ {
		vals = append(vals, fmt.Sprintf("(%s, %s, %s, %s)", v(), v(), v(), v()))
	}
	tk.MustExec("insert into t values " + strings.Join(vals, ","))
}

func TestPlanCacheRandomCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	testRandomPlanCacheCases(t, planCacheIndexMergePrepareData, planCacheIndexMergeQueries)
	testRandomPlanCacheCases(t, planCacheIntConvertPrepareData, planCacheIntConvertQueries)
	testRandomPlanCacheCases(t, planCachePointGetPrepareData, planCachePointGetQueries)
}

func testRandomPlanCacheCases(t *testing.T,
	prepFunc func(tk *testkit.TestKit),
	queryFunc func(isNonPrep bool) []string) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	prepFunc(tk)

	// prepared plan cache
	for _, q := range queryFunc(true) {
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=0")
		result1 := tk.MustQuery(q).Sort()
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
		result2 := tk.MustQuery(q).Sort()
		require.True(t, result1.Equal(result2.Rows()))
	}

	// non prepared plan cache
	for _, q := range queryFunc(false) {
		q, prepStmt, parameters := convertQueryToPrepExecStmt(q)
		result1 := tk.MustQuery(q).Sort()
		tk.MustExec(prepStmt)
		var xs []string
		for i, p := range parameters {
			tk.MustExec(fmt.Sprintf("set @x%d = %s", i, p))
			xs = append(xs, fmt.Sprintf("@x%d", i))
		}
		var execStmt string
		if len(xs) == 0 {
			execStmt = "execute st"
		} else {
			execStmt = fmt.Sprintf("execute st using %s", strings.Join(xs, ", "))
		}
		result2 := tk.MustQuery(execStmt).Sort()
		require.True(t, result1.Equal(result2.Rows()))
	}
}

func TestPlanCacheSubquerySPMEffective(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	testCases := []struct {
		sql    string
		params []int
	}{
		{"select * from t t1 where exists (select /*/ 1 from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}}, // exist
		{"select * from t t1 where exists (select /*/ b from t t2 where t1.a = t2.a and t2.b < ? limit ?)", []int{1, 1}},
		{"select * from t t1 where t1.a in (select /*/ a from t t2 where t2.a > ? and t1.a = t2.a)", []int{1}},
		{"select * from t t1 where t1.a < (select /*/ sum(t2.a) from t t2 where t2.b = t1.b and t2.a > ?)", []int{1}},
	}

	// hint
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	tk.MustExec("deallocate prepare stmt")

	// binding before prepare
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "", 1)
		bindSQL := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec("create binding for " + sql + " using " + bindSQL)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}

	// binding after prepare
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "", 1)
		bindSQL := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("create binding for " + sql + " using " + bindSQL)
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestIssue42125(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, unique key(a, b))")

	// should use BatchPointGet
	tk.MustExec("prepare st from 'select * from t where a=1 and b in (?, ?)'")
	tk.MustExec("set @a=1, @b=2")
	tk.MustExec("execute st using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[0][0], "Batch_Point_Get_5") // use BatchPointGet
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Batch/PointGet plans may be over-optimized"))

	// should use PointGet: unsafe PointGet
	tk.MustExec("prepare st from 'select * from t where a=1 and b>=? and b<=?'")
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute st using @a, @b")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[0][0], "Point_Get_5") // use Point_Get_5
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // cannot hit

	// safe PointGet
	tk.MustExec("prepare st from 'select * from t where a=1 and b=? and c<?'")
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute st using @a, @b")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Contains(t, rows[0][0], "Selection") // PointGet -> Selection
	require.Contains(t, rows[1][0], "Point_Get")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // can hit
}

func TestNonPreparedPlanExplainWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int, e enum('1', '2', '3'), s set('1', '2', '3'), j json, bt bit(8), key(b), key(c, d))`)
	tk.MustExec("create table t1(a int, b int, index idx_b(b)) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, first_name varchar(50), last_name varchar(50), full_name varchar(101) generated always as (concat(first_name,' ',last_name)))")
	tk.MustExec("create or replace SQL SECURITY INVOKER view v as select a from t")
	tk.MustExec("analyze table t, t1, t2") // eliminate stats warnings
	tk.MustExec("set @@session.tidb_enable_non_prepared_plan_cache = 1")

	supported := []string{
		"select * from t where a<10",
		"select * from t where a<13 and b<15",
		"select * from t where b=13",
		"select * from t where c<8",
		"select * from t where d>8",
		"select * from t where c=8 and d>10",
		"select * from t where a<12 and b<13 and c<12 and d>2",
		"select * from t where a in (1, 2, 3)",
		"select * from t where a<13 or b<15",
		"select * from t where a<13 or b<15 and c=13",
		"select * from t where a in (1, 2)",
		"select * from t where a in (1, 2) and b in (1, 2, 3)",
		"select * from t where a in (1, 2) and b < 15",
		"select * from t where a between 1 and 10",
		"select * from t where a between 1 and 10 and b < 15",
		"select * from t where a+b=13",
		"select * from t where mod(a, 3)=1",
		"select * from t where d>now()",
		"select distinct a from t1 where a > 1 and b < 2",          // distinct
		"select count(*) from t1 where a > 1 and b < 2 group by a", // group by
		"select * from t1 order by a",                              // order by
	}

	unsupported := []string{
		"select /*+ use_index(t1, idx_b) */ * from t1 where a > 1 and b < 2",               // hint
		"select a, sum(b) as c from t1 where a > 1 and b < 2 group by a having sum(b) > 1", // having
		"select * from (select * from t1) t",                                               // sub-query
		"select * from t1 where a in (select a from t)",                                    // uncorrelated sub-query
		"select * from t1 where a in (select a from t where a > t1.a)",                     // correlated sub-query
		"select * from t where j < 1",                                                      // json
		"select * from t where a > 1 and j < 1",
		"select * from t where e < '1'", // enum
		"select * from t where a > 1 and e < '1'",
		"select * from t where s < '1'", // set
		"select * from t where a > 1 and s < '1'",
		"select * from t where bt > 0", // bit
		"select * from t where a > 1 and bt > 0",
		"select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'", // memTable
		"select * from t3 where full_name = 'a b'",                                // generated column
		"select * from t3 where a > 1 and full_name = 'a b'",
		"select * from v",                // view
		"select * from t where a = null", // null
		"select * from t where false",    // table dual
	}

	reasons := []string{
		"skip non-prepared plan-cache: queries that have hints, having-clause, window-function are not supported",
		"skip non-prepared plan-cache: queries that have hints, having-clause, window-function are not supported",
		"skip non-prepared plan-cache: queries that have sub-queries are not supported",
		"skip non-prepared plan-cache: query accesses partitioned tables is un-cacheable",
		"skip non-prepared plan-cache: query accesses partitioned tables is un-cacheable",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: access tables in system schema",
		"skip non-prepared plan-cache: query accesses generated columns is un-cacheable",
		"skip non-prepared plan-cache: query accesses generated columns is un-cacheable",
		"skip non-prepared plan-cache: queries that access views are not supported",
		"skip non-prepared plan-cache: query has null constants",
		"skip non-prepared plan-cache: some parameters may be overwritten when constant propagation",
	}

	all := append(supported, unsupported...)

	explainFormats := []string{
		types.ExplainFormatBrief,
		types.ExplainFormatDOT,
		types.ExplainFormatHint,
		types.ExplainFormatROW,
		types.ExplainFormatVerbose,
		types.ExplainFormatTraditional,
		types.ExplainFormatBinary,
		types.ExplainFormatTiDBJSON,
		types.ExplainFormatCostTrace,
	}
	// all cases no warnings use other format
	for _, q := range all {
		tk.MustExec("explain " + q)
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustExec("explain " + q)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	for _, format := range explainFormats {
		for _, q := range all {
			tk.MustExec(fmt.Sprintf("explain format = '%v' %v", format, q))
			//tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustQuery("show warnings").CheckNotContain("plan cache")
			tk.MustExec(fmt.Sprintf("explain format = '%v' %v", format, q))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}

	// unsupported case with warning use 'plan_cache' format
	for idx, q := range unsupported {
		tk.MustExec("explain format = 'plan_cache'" + q)
		warn := tk.MustQuery("show warnings").Rows()[0]
		require.Equal(t, reasons[idx], warn[2])
	}
}

func TestNonPreparedPlanCachePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustExec("create table t (a varchar(255), b int, c char(10), primary key (c, a));")
	ctx := tk.Session().(sessionctx.Context)

	s := parser.New()
	for _, sql := range []string{
		"select 1 from t where a='x'",
		"select * from t where c='x'",
		"select * from t where a='x' and c='x'",
		"select * from t where a='x' and c='x' and b=1",
	} {
		stmtNode, err := s.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err) // not panic
	}
}

func TestNonPreparedPlanCacheAutoStmtRetry(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int primary key, k int, UNIQUE KEY(k))")
	tk1.MustExec("insert into t values(1, 1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk2.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustExec("update t set k=3 where id=1")

	var wg sync.WaitGroup
	var tk2Err error
	wg.Add(1)
	go func() {
		// trigger statement auto-retry on tk2
		_, tk2Err = tk2.Exec("insert into t values(3, 3)")
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	_, err := tk1.Exec("commit")
	require.NoError(t, err)
	wg.Wait()
	require.ErrorContains(t, tk2Err, "Duplicate entry")
}

func TestIssue43667Concurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cycle (pk int key, val int)")
	var wg sync.WaitGroup
	concurrency := 30
	for i := 0; i < concurrency; i++ {
		tk.MustExec(fmt.Sprintf("insert into cycle values (%v,%v)", i, i))
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
			query := fmt.Sprintf("select (val) from cycle where pk = %v", id)
			for j := 0; j < 5000; j++ {
				tk.MustQuery(query).Check(testkit.Rows(fmt.Sprintf("%v", id)))
			}
		}(i)
	}
	wg.Wait()
}

func TestIssue43667(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`create table cycle (pk int not null primary key, sk int not null, val int)`)
	tk.MustExec(`insert into cycle values (4, 4, 4)`)
	tk.MustExec(`insert into cycle values (7, 7, 7)`)

	tk.MustQuery(`select (val) from cycle where pk = 4`).Check(testkit.Rows("4"))
	tk.MustQuery(`select (val) from cycle where pk = 7`).Check(testkit.Rows("7"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	updateAST := func(stmt ast.StmtNode) {
		v := stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr).R.(*driver.ValueExpr)
		v.Datum.SetInt64(7)
	}

	tctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestIssue43667{}, updateAST)
	tk.MustQueryWithContext(tctx, `select (val) from cycle where pk = 4`).Check(testkit.Rows("4"))
}

func TestIssue47133(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`CREATE TABLE t (id int NOT NULL, personId int NOT NULL,
      name varchar(255) NOT NULL, PRIMARY KEY (id, personId))`)
	tk.MustExec(`insert into t values (1, 1, '')`)

	cnt := 0
	checkFieldNames := func(names []*types.FieldName) {
		require.Equal(t, len(names), 2)
		require.Equal(t, names[0].String(), "test.t.user_id")
		require.Equal(t, names[1].String(), "test.t.user_personid")
		cnt += 1
	}
	tctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestIssue47133{}, checkFieldNames)
	tk.MustQueryWithContext(tctx, `SELECT id AS User_id, personId AS User_personId FROM t WHERE (id = 1 AND personId = 1)`).Check(
		testkit.Rows("1 1"))
	tk.MustQueryWithContext(tctx, `SELECT id AS User_id, personId AS User_personId FROM t WHERE (id = 1 AND personId = 1)`).Check(
		testkit.Rows("1 1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	require.Equal(t, cnt, 2)
}

func TestBuiltinFuncFlen(t *testing.T) {
	// same as TestIssue45378 and TestIssue45253
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1(c1 INT)`)
	tk.MustExec(`INSERT INTO t1 VALUES (1)`)

	funcs := []string{ast.Abs, ast.Acos, ast.Asin, ast.Atan, ast.Ceil, ast.Ceiling, ast.Cos,
		ast.CRC32, ast.Degrees, ast.Floor, ast.Ln, ast.Log, ast.Log2, ast.Log10, ast.Unhex,
		ast.Radians, ast.Rand, ast.Round, ast.Sign, ast.Sin, ast.Sqrt, ast.Tan, ast.SM3,
		ast.Quote, ast.RTrim, ast.ToBase64, ast.Trim, ast.Upper, ast.Ucase, ast.Hex,
		ast.BitLength, ast.CharLength, ast.Compress, ast.MD5, ast.SHA1, ast.SHA}
	args := []string{"2038330881", "'2038330881'", "'牵'", "-1", "''", "0"}

	for _, f := range funcs {
		for _, a := range args {
			q := fmt.Sprintf("SELECT c1 from t1 where %s(%s)", f, a)
			tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
			r1 := tk.MustQuery(q)
			tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
			r2 := tk.MustQuery(q)
			r1.Sort().Check(r2.Sort().Rows())
		}
	}
}

func BenchmarkPlanCacheInsert(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	tk.MustExec("prepare st from 'insert into t values (1)'")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("execute st")
	}
}

func BenchmarkNonPreparedPlanCacheDML(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("insert into t values (1)")
		tk.MustExec("update t set a = 2 where a = 1")
		tk.MustExec("delete from t where a = 2")
	}
}
