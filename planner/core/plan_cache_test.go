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
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

type mockParameterizer struct {
	action string
}

func (mp *mockParameterizer) Parameterize(originSQL string) (paramSQL string, params []expression.Expression, ok bool, err error) {
	switch mp.action {
	case "error":
		return "", nil, false, errors.New("error")
	case "not_support":
		return "", nil, false, nil
	}
	// only support SQL like 'select * from t where col {op} {int} and ...'
	prefix := "select * from t where "
	if !strings.HasPrefix(originSQL, prefix) {
		return "", nil, false, nil
	}
	buf := make([]byte, 0, 32)
	buf = append(buf, prefix...)
	for i, condStr := range strings.Split(originSQL[len(prefix):], "and") {
		if i > 0 {
			buf = append(buf, " and "...)
		}
		tmp := strings.Split(strings.TrimSpace(condStr), " ")
		if len(tmp) != 3 { // col {op} {val}
			return "", nil, false, nil
		}
		buf = append(buf, tmp[0]...)
		buf = append(buf, tmp[1]...)
		buf = append(buf, '?')

		intParam, err := strconv.Atoi(tmp[2])
		if err != nil {
			return "", nil, false, nil
		}
		params = append(params, &expression.Constant{Value: types.NewDatum(intParam), RetType: types.NewFieldType(mysql.TypeLong)})
	}
	return string(buf), params, true, nil
}

func TestInitLRUWithSystemVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_prepared_plan_cache_size = 0") // MinValue: 1
	tk.MustQuery("select @@session.tidb_prepared_plan_cache_size").Check(testkit.Rows("1"))
	sessionVar := tk.Session().GetSessionVars()

	lru := plannercore.NewLRUPlanCache(uint(sessionVar.PreparedPlanCacheSize), 0, 0, tk.Session())
	require.NotNil(t, lru)
}

func TestIssue40296(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database test_40296`)
	tk.MustExec(`use test_40296`)
	tk.MustExec(`CREATE TABLE IDT_MULTI15880STROBJSTROBJ (
  COL1 enum('aa','bb','cc','dd','ff','gg','kk','ll','mm','ee') DEFAULT NULL,
  COL2 decimal(20,0) DEFAULT NULL,
  COL3 date DEFAULT NULL,
  KEY U_M_COL4 (COL1,COL2),
  KEY U_M_COL5 (COL3,COL2))`)
	tk.MustExec(`insert into IDT_MULTI15880STROBJSTROBJ values("ee", -9605492323393070105, "0850-03-15")`)
	tk.MustExec(`set session tidb_enable_non_prepared_plan_cache=on`)
	tk.MustQuery(`select * from IDT_MULTI15880STROBJSTROBJ where col1 in ("dd", "dd") or col2 = 9923875910817805958 or col3 = "9994-11-11"`).Check(
		testkit.Rows())
	tk.MustQuery(`select * from IDT_MULTI15880STROBJSTROBJ where col1 in ("aa", "aa") or col2 = -9605492323393070105 or col3 = "0005-06-22"`).Check(
		testkit.Rows("ee -9605492323393070105 0850-03-15"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // unary operator '-' is not supported now.
}

func TestNonPreparedPlanCacheSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("alter table t add index idx_a(a)")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // cannot hit since the schema changed
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedCacheWithPreparedCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`prepare st from 'select * from t where a=1'`)
	tk.MustExec(`execute st`)
	tk.MustExec(`execute st`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`select * from t where a=1`) // cannot hit since these 2 plan cache are separated
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=0")
	tk.MustExec(`select * from t where a=1`) // the session-level switch can take effect in real time
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheSwitch2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for nonPrep := 0; nonPrep <= 1; nonPrep++ {
		for prep := 0; prep <= 1; prep++ {
			tk.MustExec("create table t(a int)")
			tk.MustExec(fmt.Sprintf(`set tidb_enable_non_prepared_plan_cache=%v`, nonPrep))
			tk.MustExec(fmt.Sprintf(`set tidb_enable_prepared_plan_cache=%v`, prep))

			tk.MustExec(`select * from t where a<1`)
			tk.MustExec(`select * from t where a<2`)
			tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows(fmt.Sprintf("%v", nonPrep)))

			tk.MustExec(`prepare st from 'select * from t where a<?'`)
			tk.MustExec(`set @a=1`)
			tk.MustExec(`execute st using @a`)
			tk.MustExec(`set @a=2`)
			tk.MustExec(`execute st using @a`)
			tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows(fmt.Sprintf("%v", prep)))

			tk.MustExec("drop table t")
		}
	}
}

func TestNonPreparedPlanCacheUnknownSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table tt(a char(2) primary key, b char(2))`)
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	err := tk.ExecToErr(`select tt.* from tt tmp where a='aa'`)
	require.Equal(t, err.Error(), "[planner:1051]Unknown table 'tt'")
}

func TestNonPreparedPlanCacheReason(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`explain select * from t where a=1`)
	tk.MustExec(`explain select * from t where a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`explain select * from t where a+1=1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prep plan cache: query has some unsupported binary operation`))

	tk.MustExec(`explain select * from t t1, t t2`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prep plan cache: queries that access multiple tables are not supported`))

	tk.MustExec(`explain select * from (select * from t) tx`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prep plan cache: queries that have sub-queries are not supported`))

	// no warning if disable this feature
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=0")
	tk.MustExec(`explain select * from t where a+1=1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`explain select * from t t1, t t2`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`explain select * from t where a in (select a from t)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
}

func TestNonPreparedPlanCacheSQLMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @@sql_mode=''") // cannot hit since sql-mode changed
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("analyze table t")
	tk.MustExec(`select * from t where a=1`) // stats changes won't affect non-prep cache hit
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select /*+ use_index(t, a) */ * from t where a=1")
	tk.MustExec("select /*+ use_index(t, a) */ * from t where a=1") // cannot hit since it has a hint
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("create binding for select * from t where a=1 using select /*+ use_index(t, a) */ * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("drop binding for select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheWithExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	tk.MustExec("select * from t where a=1") // cache this plan

	tk.MustQuery("explain select * from t where a=2").Check(testkit.Rows(
		`Selection_8 10.00 root  eq(test.t.a, 2)`,
		`└─TableReader_7 10.00 root  data:Selection_6`,
		`  └─Selection_6 10.00 cop[tikv]  eq(test.t.a, 2)`,
		`    └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustQuery("explain format=verbose select * from t where a=2").Check(testkit.Rows(
		`Selection_8 10.00 169474.57 root  eq(test.t.a, 2)`,
		`└─TableReader_7 10.00 168975.57 root  data:Selection_6`,
		`  └─Selection_6 10.00 2534000.00 cop[tikv]  eq(test.t.a, 2)`,
		`    └─TableFullScan_5 10000.00 2035000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustQuery("explain analyze select * from t where a=2").CheckAt([]int{0, 1, 2, 3}, [][]interface{}{
		{"Selection_8", "10.00", "0", "root"},
		{"└─TableReader_7", "10.00", "0", "root"},
		{"  └─Selection_6", "10.00", "0", "cop[tikv]"},
		{"    └─TableFullScan_5", "10000.00", "0", "cop[tikv]"},
	})
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheFastPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int primary key, b int, unique key(b))`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	// fast plans have a higher priority than non-prep cache plan
	tk.MustQuery(`explain format='brief' select a from t where a in (1, 2)`).Check(testkit.Rows(
		`Batch_Point_Get 2.00 root table:t handle:[1 2], keep order:false, desc:false`))
	tk.MustQuery(`select a from t where a in (1, 2)`).Check(testkit.Rows())
	tk.MustQuery(`select a from t where a in (1, 2)`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustQuery(`explain format='brief' select b from t where b = 1`).Check(testkit.Rows(
		`Point_Get 1.00 root table:t, index:b(b) `))
	tk.MustQuery(`select b from t where b = 1`).Check(testkit.Rows())
	tk.MustQuery(`select b from t where b = 1`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheSetOperations(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	// queries with set operations cannot hit the cache
	for _, q := range []string{
		`select * from t union select * from t`,
		`select * from t union distinct select * from t`,
		`select * from t union all select * from t`,
		`select * from t except select * from t`,
		`select * from t intersect select * from t`,
	} {
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanCacheSpecialTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`create definer='root'@'localhost' view t_v as select * from t`)
	tk.MustExec(`create table t_p (a int) partition by hash(a) partitions 4`)
	tk.MustExec(`create temporary table t_t (a int)`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	// queries that access partitioning tables, view, temporary tables or contain CTE cannot hit the cache.
	for _, q := range []string{
		`select * from t_v`,
		`select * from t_p`,
		`select * from t_t`,
		`with t_cte as (select * from t) select * from t_cte`,
	} {
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanParameterType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, key(a))`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustQuery(`select * from t where a=1`).Check(testkit.Rows())
	tk.MustQuery(`select * from t where a=1`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustQuery(`select * from t where a=1.1`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`select * from t where a=1.1`).Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip plan-cache: '1.1' may be converted to INT`))

	tk.MustQuery(`select * from t where a='1'`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`select * from t where a='1'`).Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip plan-cache: '1' may be converted to INT`))
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

func TestNonPreparedPlanCacheSelectLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue41626(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a year)`)
	tk.MustExec(`insert into t values (2000)`)
	tk.MustExec(`prepare st from 'select * from t where a<?'`)
	tk.MustExec(`set @a=12`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows("2000"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip plan-cache: '12' may be converted to INT"))
}

func TestIssue38269(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
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

func TestIgnoreInsertStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	// do not cache native insert-stmt
	tk.MustExec("prepare st from 'insert into t values (1)'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	// ignore-hint in insert-stmt can work
	tk.MustExec("prepare st from 'insert into t select * from t'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("prepare st from 'insert /*+ ignore_plan_cache() */ into t select * from t'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue38710(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists UK_NO_PRECISION_19392;")
	tk.MustExec("CREATE TABLE `UK_NO_PRECISION_19392` (\n  `COL1` bit(1) DEFAULT NULL,\n  `COL2` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,\n  `COL4` datetime DEFAULT NULL,\n  `COL3` bigint DEFAULT NULL,\n  `COL5` float DEFAULT NULL,\n  UNIQUE KEY `UK_COL1` (`COL1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `UK_NO_PRECISION_19392` VALUES (0x00,'缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈','9294-12-26 06:50:40',-3088380202191555887,-3.33294e38),(NULL,'仲膩蕦圓猴洠飌镂喵疎偌嫺荂踖Ƕ藨蜿諪軁笞','1746-08-30 18:04:04',-4016793239832666288,-2.52633e38),(0x01,'冑溜畁脊乤纊繳蟥哅稐奺躁悼貘飗昹槐速玃沮','1272-01-19 23:03:27',-8014797887128775012,1.48868e38);\n")
	tk.MustExec(`prepare stmt from 'select * from UK_NO_PRECISION_19392 where col1 between ? and ? or col3 = ? or col2 in (?, ?, ?);';`)
	tk.MustExec("set @a=0x01, @b=0x01, @c=-3088380202191555887, @d=\"缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈\", @e=\"缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈\", @f=\"缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈\";")
	rows := tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`) // can not be cached because @a = @b
	require.Equal(t, 2, len(rows.Rows()))

	tk.MustExec(`set @a=NULL, @b=NULL, @c=-4016793239832666288, @d="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈", @e="仲膩蕦圓猴洠飌镂喵疎偌嫺荂踖Ƕ藨蜿諪軁笞", @f="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈";`)
	rows = tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`)
	require.Equal(t, 2, len(rows.Rows()))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	rows = tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`)
	require.Equal(t, 2, len(rows.Rows()))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`set @a=0x01, @b=0x01, @c=-3088380202191555887, @d="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈", @e="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈", @f="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈";`)
	rows = tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`)
	require.Equal(t, 2, len(rows.Rows()))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // can not use the cache because the types for @a and @b are not equal to the cached plan
}

func TestPlanCacheDiagInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a), key(b))")

	tk.MustExec("prepare stmt from 'select /*+ ignore_plan_cache() */ * from t'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: ignore plan cache by hint"))

	tk.MustExec("prepare stmt from 'select * from t order by ?'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: query has 'order by ?' is un-cacheable"))

	tk.MustExec("prepare stmt from 'select * from t where a=?'")
	tk.MustExec("set @a='123'")
	tk.MustExec("execute stmt using @a") // '123' -> 123
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: '123' may be converted to INT"))

	tk.MustExec("prepare stmt from 'select * from t where a=? and a=?'")
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute stmt using @a, @b") // a=1 and a=1 -> a=1
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: some parameters may be overwritten"))
}

func TestIssue40224(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select a from t where a in (?, ?)'")
	tk.MustExec("set @a=1.0, @b=2.0")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: '1.0' may be converted to INT"))
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

func TestIssue40225(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select * from t where a<?'")
	tk.MustExec("set @a='1'")
	tk.MustExec("execute st using @a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 skip plan-cache: '1' may be converted to INT")) // plan-cache limitation
	tk.MustExec("create binding for select * from t where a<1 using select /*+ ignore_plan_cache() */ * from t where a<1")
	tk.MustExec("execute st using @a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 skip plan-cache: ignore plan cache by binding"))
	// no warning about plan-cache limitations('1' -> INT) since plan-cache is totally disabled.

	tk.MustExec("prepare st from 'select * from t where a>?'")
	tk.MustExec("set @a=1")
	tk.MustExec("execute st using @a")
	tk.MustExec("execute st using @a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("create binding for select * from t where a>1 using select /*+ ignore_plan_cache() */ * from t where a>1")
	tk.MustExec("execute st using @a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("execute st using @a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: '1.1' may be converted to INT"))
}

func TestIssue38335(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE PK_LP9463 (
  COL1 mediumint NOT NULL DEFAULT '77' COMMENT 'NUMERIC PK',
  COL2 varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  COL4 datetime DEFAULT NULL,
  COL3 bigint DEFAULT NULL,
  COL5 float DEFAULT NULL,
  PRIMARY KEY (COL1))`)
	tk.MustExec(`
INSERT INTO PK_LP9463 VALUES (-7415279,'笚綷想摻癫梒偆荈湩窐曋繾鏫蘌憬稁渣½隨苆','1001-11-02 05:11:33',-3745331437675076296,-3.21618e38),
(-7153863,'鯷氤衡椻闍饑堀鱟垩啵緬氂哨笂序鉲秼摀巽茊','6800-06-20 23:39:12',-7871155140266310321,-3.04829e38),
(77,'娥藨潰眤徕菗柢礥蕶浠嶲憅榩椻鍙鑜堋ᛀ暵氎','4473-09-13 01:18:59',4076508026242316746,-1.9525e38),
(16614,'阖旕雐盬皪豧篣哙舄糗悄蟊鯴瞶珧赺潴嶽簤彉','2745-12-29 00:29:06',-4242415439257105874,2.71063e37)`)
	tk.MustExec(`prepare stmt from 'SELECT *, rank() OVER (PARTITION BY col2 ORDER BY COL1) FROM PK_LP9463 WHERE col1 != ? AND col1 < ?'`)
	tk.MustExec(`set @a=-8414766051197, @b=-8388608`)
	tk.MustExec(`execute stmt using @a,@b`)
	tk.MustExec(`set @a=16614, @b=16614`)
	rows := tk.MustQuery(`execute stmt using @a,@b`).Sort()
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT *, rank() OVER (PARTITION BY col2 ORDER BY COL1) FROM PK_LP9463 WHERE col1 != 16614 and col1 < 16614`).Sort().Check(rows.Rows())
}

func TestIssue41032(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE PK_SIGNED_10087 (
	 COL1 mediumint(8) unsigned NOT NULL,
	 COL2 varchar(20) DEFAULT NULL,
	 COL4 datetime DEFAULT NULL,
	 COL3 bigint(20) DEFAULT NULL,
	 COL5 float DEFAULT NULL,
	 PRIMARY KEY (COL1) )`)
	tk.MustExec(`insert into PK_SIGNED_10087 values(0, "痥腜蟿鮤枓欜喧檕澙姭袐裄钭僇剕焍哓閲疁櫘", "0017-11-14 05:40:55", -4504684261333179273, 7.97449e37)`)
	tk.MustExec(`prepare stmt from 'SELECT/*+ HASH_JOIN(t1, t2) */ t2.* FROM PK_SIGNED_10087 t1 JOIN PK_SIGNED_10087 t2 ON t1.col1 = t2.col1 WHERE t2.col1 >= ? AND t1.col1 >= ?;'`)
	tk.MustExec(`set @a=0, @b=0`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows("0 痥腜蟿鮤枓欜喧檕澙姭袐裄钭僇剕焍哓閲疁櫘 0017-11-14 05:40:55 -4504684261333179273 79744900000000000000000000000000000000"))
	tk.MustExec(`set @a=8950167, @b=16305982`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
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
		{"prepare stmt from 'select * from t where a = ? limit ?, ?'", []int{1, 1, 1}},
		{"prepare stmt from 'select * from t where a in (?, ?) limit ?, ?'", []int{1, 2, 1, 1}},
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: limit count more than 10000"))
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
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: query has uncorrelated sub-queries is un-cacheable"))
			} else {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: PhysicalApply plan is un-cacheable"))
			}
		}
	}
	// switch off
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = 0")
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: query has sub-queries is un-cacheable"))
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
