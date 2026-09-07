// Copyright 2016 PingCAP, Inc.
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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestPlanCacheWithDifferentVariableTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("create table t1(a varchar(20), b int, c float, key(b, a))")
	tk.MustExec("insert into t1 values('1',1,1.1),('2',2,222),('3',3,333)")
	tk.MustExec("create table t2(a varchar(20), b int, c float, key(b, a))")
	tk.MustExec("insert into t2 values('3',3,3.3),('2',2,222),('3',3,333)")

	var input []struct {
		PrepareStmt string
		Executes    []struct {
			Vars []struct {
				Name  string
				Value string
			}
			ExecuteSQL string
		}
	}
	var output []struct {
		PrepareStmt string
		Executes    []struct {
			SQL  string
			Vars []struct {
				Name  string
				Value string
			}
			Plan             []string
			LastPlanUseCache string
			Result           []string
		}
	}
	prepareMergeSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		tk.MustExec(tt.PrepareStmt)
		testdata.OnRecord(func() {
			output[i].PrepareStmt = tt.PrepareStmt
			output[i].Executes = make([]struct {
				SQL  string
				Vars []struct {
					Name  string
					Value string
				}
				Plan             []string
				LastPlanUseCache string
				Result           []string
			}, len(tt.Executes))
		})
		require.Equal(t, tt.PrepareStmt, output[i].PrepareStmt)
		for j, exec := range tt.Executes {
			for _, v := range exec.Vars {
				tk.MustExec(fmt.Sprintf(`set @%s = %s`, v.Name, v.Value))
			}
			res := tk.MustQuery(exec.ExecuteSQL)
			lastPlanUseCache := tk.MustQuery("select @@last_plan_from_cache").Rows()[0][0]
			tk.MustQuery(exec.ExecuteSQL)
			tkProcess := tk.Session().ShowProcess()
			ps := []*sessmgr.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))
			testdata.OnRecord(func() {
				output[i].Executes[j].SQL = exec.ExecuteSQL
				output[i].Executes[j].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].Executes[j].Vars = exec.Vars
				output[i].Executes[j].LastPlanUseCache = lastPlanUseCache.(string)
				output[i].Executes[j].Result = testdata.ConvertRowsToStrings(res.Rows())
			})

			require.Equal(t, exec.ExecuteSQL, output[i].Executes[j].SQL)
			plan.Check(testkit.Rows(output[i].Executes[j].Plan...))
			require.Equal(t, exec.Vars, output[i].Executes[j].Vars)
			require.Equal(t, lastPlanUseCache.(string), output[i].Executes[j].LastPlanUseCache)
			res.Check(testkit.Rows(output[i].Executes[j].Result...))
		}
	}
}

func TestParameterPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int, b int, c int, key(a))`)
	tk.MustExec(`insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6)`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec(`set @x1=1,@x5=5,@x10=10,@x20=20`)

	var input []struct {
		SQL string
	}
	var output []struct {
		Result    []string
		Plan      []string
		FromCache string
	}
	prepareMergeSuiteData.LoadTestCases(t, &input, &output)

	for i, tt := range input {
		if strings.HasPrefix(tt.SQL, "execute") {
			res := tk.MustQuery(tt.SQL).Sort()
			fromCache := tk.MustQuery("select @@last_plan_from_cache")
			tk.MustQuery(tt.SQL)
			tkProcess := tk.Session().ShowProcess()
			ps := []*sessmgr.ProcessInfo{tkProcess}
			tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
			plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID))

			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(res.Rows())
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].FromCache = fromCache.Rows()[0][0].(string)
			})

			res.Check(testkit.Rows(output[i].Result...))
			plan.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, fromCache.Rows()[0][0].(string), output[i].FromCache)
		} else {
			tk.MustExec(tt.SQL)
			testdata.OnRecord(func() {
				output[i].Result = nil
			})
		}
	}
}

func TestPreparePlanCache4Function(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	checkExplainForConnContains := func(includes []string, excludes []string) {
		tkProcess := tk.Session().ShowProcess()
		ps := []*sessmgr.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		rows := testdata.ConvertRowsToStrings(
			tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10)).Rows(),
		)
		explain := strings.Join(rows, "\n")
		for _, include := range includes {
			require.Contains(t, explain, include)
		}
		for _, exclude := range excludes {
			require.NotContains(t, explain, exclude)
		}
	}

	// Testing for non-deterministic functions
	tk.MustExec("prepare stmt from 'select rand()';")
	res := tk.MustQuery("execute stmt;")
	require.Equal(t, 1, len(res.Rows()))

	res1 := tk.MustQuery("execute stmt;")
	require.Equal(t, 1, len(res1.Rows()))
	require.NotEqual(t, res.Rows()[0][0], res1.Rows()[0][0])
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Testing for control functions
	tk.MustExec("prepare stmt from 'SELECT IFNULL(?,0);';")
	tk.MustExec("set @a = 1, @b = null;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select a, case when a = ? then 0 when a <=> ? then 1 else 2 end b from t order by a;';")
	tk.MustExec("insert into t values(0), (1), (2), (null);")
	tk.MustExec("set @a = 0, @b = 1, @c = 2, @d = null;")
	tk.MustQuery("execute stmt using @a, @b;").Check(testkit.Rows("<nil> 2", "0 0", "1 1", "2 2"))
	tk.MustQuery("execute stmt using @c, @d;").Check(testkit.Rows("<nil> 1", "0 2", "1 2", "2 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec(`CREATE TABLE t0(c0 DOUBLE);`)
	tk.MustExec(`CREATE TABLE t1 LIKE t0;`)
	tk.MustExec(`CREATE INDEX i0 ON t1(c0 ASC);`)
	tk.MustExec(`ALTER TABLE t1  CHANGE c0 c0 DOUBLE NOT NULL ;`)
	// 1 or ((Null <=> t1.c0) AND (Null <=> t1.c0))
	tk.MustExec(`SET @a = 0.7481117056976476;`)
	tk.MustExec(`SET @b = NULL;`)
	tk.MustExec(`SET @c = NULL;`)
	tk.MustExec(`PREPARE prepare_query FROM 'SELECT t0.c0 FROM t0, t1 WHERE ? OR ((? <=> t1.c0) AND (? <=> t1.c0))';`)
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("1"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	checkExplainForConnContains(
		[]string{
			`CARTESIAN inner join`,
			`IndexFullScan_`,
			`table:t1, index:i0(c0)`,
			`TableFullScan_`,
			`table:t0`,
		},
		[]string{
			`TableDual_`,
		},
	)
	// 0 or ((Null <=> t1.c0) AND (Null <=> t1.c0))
	tk.MustExec(`SET @a = 0`)
	tk.MustExec(`SET @b = NULL;`)
	tk.MustExec(`SET @c = NULL;`)
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`show warnings`).Check(
		testkit.Rows(`Warning 1105 skip prepared plan-cache: some parameters may be overwritten when constant propagation`))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	checkExplainForConnContains(
		[]string{
			`CARTESIAN inner join`,
			`TableFullScan_`,
			`table:t0`,
		},
		nil,
	)
	// 0 or ((1 <=> t1.c0) AND (1 <=> t1.c0))
	tk.MustExec(`SET @a = 0`)
	tk.MustExec(`SET @b = 1;`)
	tk.MustExec(`SET @c = 1;`)
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`show warnings`).Check(
		testkit.Rows(`Warning 1105 skip prepared plan-cache: some parameters may be overwritten when constant propagation`))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	checkExplainForConnContains(
		[]string{
			`CARTESIAN inner join`,
			`IndexRangeScan_`,
			`table:t1, index:i0(c0) range:[1,1]`,
			`TableFullScan_`,
			`table:t0`,
		},
		[]string{
			`TableDual_`,
		},
	)
	// 1 or ((1 <=> t1.c0) AND (1 <=> t1.c0))
	tk.MustExec(`SET @a = 1`)
	tk.MustExec(`SET @b = 1;`)
	tk.MustExec(`SET @c = 1;`)
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`show warnings`).Check(
		testkit.Rows(`Warning 1105 skip prepared plan-cache: some parameters may be overwritten`))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	checkExplainForConnContains(
		[]string{
			`CARTESIAN inner join`,
			`IndexFullScan_`,
			`table:t1, index:i0(c0)`,
			`TableFullScan_`,
			`table:t0`,
		},
		[]string{
			`TableDual_`,
		},
	)
	tk.MustExec(`PREPARE prepare_query FROM 'SELECT t0.c0 FROM t0, t1 WHERE ((? <=> t1.c0) AND (? <=> t1.c0) or (? > t1.c0))';`)
	// ((1 <=> t1.c0) AND (1 <=> t1.c0) or (1 > t1.c0))
	tk.MustExec(`SET @a = 1`)
	tk.MustExec(`SET @b = 1;`)
	tk.MustExec(`SET @c = 1;`)
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	tk.MustQuery(`show warnings`).Check(
		testkit.Rows(`Warning 1105 skip prepared plan-cache: some parameters may be overwritten`))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`)
	checkExplainForConnContains(
		[]string{
			`CARTESIAN inner join`,
			`IndexRangeScan_`,
			`table:t1, index:i0(c0) range:[-inf,1]`,
			`TableFullScan_`,
			`table:t0`,
		},
		[]string{
			`TableDual_`,
		},
	)

	tk.MustExec(`CREATE TABLE t2(a int NOT NULL, b int NOT NULL, KEY i0(a, b));`)
	tk.MustExec(`PREPARE prepare_query FROM 'SELECT /* issue:63914 */ * FROM t2 WHERE a = ? AND (? <=> b) AND (? <=> b)';`)
	tk.MustExec(`SET @a = 1`)
	tk.MustExec(`SET @b = NULL;`)
	tk.MustExec(`SET @c = NULL;`)
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`EXECUTE prepare_query USING @a,@b,@c;`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache;`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT 1;`).Check(testkit.Rows("1"))
}

func TestPreparePlanCache4DifferentSystemVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")

	// Testing for 'sql_select_limit'
	tk.MustExec("set @@sql_select_limit = 1")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(0), (1), (null);")
	tk.MustExec("prepare stmt from 'select a from t order by a;';")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>"))

	tk.MustExec("set @@sql_select_limit = 2")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>", "0"))
	// The 'sql_select_limit' will be stored in the cache key. So if the `sql_select_limit`
	// have been changed, the plan cache can not be reused.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("set @@sql_select_limit = 18446744073709551615")
	tk.MustQuery("execute stmt;").Check(testkit.Rows("<nil>", "0", "1"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	// test for 'tidb_enable_index_merge'
	tk.MustExec("set @@tidb_enable_index_merge = 1;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b));")
	tk.MustExec("prepare stmt from 'select * from t use index(idx_a, idx_b) where a > 1 or b > 1;';")
	tk.MustExec("execute stmt;")
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res := tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Equal(t, 4, len(res.Rows()))
	require.Contains(t, res.Rows()[0][0], "IndexMerge")

	tk.MustExec("set @@tidb_enable_index_merge = 0;")
	tk.MustExec("execute stmt;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Equal(t, 4, len(res.Rows()))
	require.Contains(t, res.Rows()[0][0], "IndexMerge")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	// test for 'tidb_enable_parallel_apply'
	tk.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("prepare stmt from 'select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a);';")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.Session().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	require.Contains(t, res.Rows()[1][5], "Concurrency")

	tk.MustExec("set tidb_enable_parallel_apply=false")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.Session().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	executionInfo := fmt.Sprintf("%v", res.Rows()[1][4])
	// Do not use the parallel apply.
	require.False(t, strings.Contains(executionInfo, "Concurrency"))
	tk.MustExec("execute stmt;")
	// The subquery plan with PhysicalApply can't be cached.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("execute stmt;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: PhysicalApply plan is un-cacheable"))

	// test for apply cache
	tk.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tk.MustExec("set tidb_mem_quota_apply_cache=33554432")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	tk.MustExec("prepare stmt from 'select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a);';")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.Session().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	require.Contains(t, res.Rows()[1][5], "cache:ON")

	tk.MustExec("set tidb_mem_quota_apply_cache=0")
	tk.MustQuery("execute stmt;").Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.Session().SetProcessInfo("", time.Now(), mysql.ComSleep, 0)
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	res = tk.MustQuery("explain for connection " + strconv.FormatUint(tkProcess.ID, 10))
	require.Contains(t, res.Rows()[1][0], "Apply")
	executionInfo = fmt.Sprintf("%v", res.Rows()[1][5])
	// Do not use the apply cache.
	require.True(t, strings.Contains(executionInfo, "cache:OFF"))
	tk.MustExec("execute stmt;")
	// The subquery plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}
func TestPrepareStmtAfterIsolationReadChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=0`)
	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// create virtual tiflash replica.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:     1,
		Available: true,
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines='tikv'")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("prepare stmt from \"select * from t\"")
	tk.MustQuery("execute stmt")
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, "cop[tikv]", rows[len(rows)-1][2])

	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	// allowing mpp will generate mpp[tiflash] plan, the test framework will time out due to
	// "retry for TiFlash peer with region missing", so disable mpp mode to use cop mode instead.
	tk.MustExec("set @@session.tidb_allow_mpp=0")
	tk.MustExec("execute stmt")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[len(rows)-1][2], "cop[tiflash]")

	require.Equal(t, 1, len(tk.Session().GetSessionVars().PreparedStmts))
	require.Equal(t, "select * from `t`", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.PlanCacheStmt).NormalizedSQL)
	require.Equal(t, "", tk.Session().GetSessionVars().PreparedStmts[1].(*plannercore.PlanCacheStmt).NormalizedPlan)
}

func TestMaxPreparedStmtCount(t *testing.T) {
	oldVal := atomic.LoadInt64(&variable.PreparedStmtCount)
	atomic.StoreInt64(&variable.PreparedStmtCount, 0)
	defer func() {
		atomic.StoreInt64(&variable.PreparedStmtCount, oldVal)
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.max_prepared_stmt_count = 2")
	tk.MustExec("prepare stmt1 from 'select ? as num from dual'")
	tk.MustExec("prepare stmt2 from 'select ? as num from dual'")
	err := tk.ExecToErr("prepare stmt3 from 'select ? as num from dual'")
	require.True(t, terror.ErrorEqual(err, variable.ErrMaxPreparedStmtCountReached))
}

func TestExecuteWithWrongType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t3 (c1 int, c2 decimal(32, 30))")

	tk.MustExec(`prepare p1 from "update t3 set c1 = 2 where c2 in (?, ?)"`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 0.0`)
	tk.MustExec(`execute p1 using @i0, @i1`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 'aa'`)
	tk.MustExecToErr(`execute p1 using @i0, @i1`)

	tk.MustExec(`prepare p2 from "update t3 set c1 = 2 where c2 in (?, ?)"`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 'aa'`)
	tk.MustExecToErr(`execute p2 using @i0, @i1`)
	tk.MustExec(`set @i0 = 0.0, @i1 = 0.0`)
	tk.MustExec(`execute p2 using @i0, @i1`)
}

func TestIssue58870(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set names GBK")
	tk.MustExec(`CREATE TABLE tsecurity  (
  security_id int(11) NOT NULL DEFAULT 0,
  mkt_id smallint(6) NOT NULL DEFAULT 0,
  security_code varchar(64) CHARACTER SET gbk COLLATE gbk_bin NOT NULL DEFAULT ' ',
  security_name varchar(128) CHARACTER SET gbk COLLATE gbk_bin NOT NULL DEFAULT ' ',
  PRIMARY KEY (security_id) USING BTREE
) ENGINE = InnoDB CHARACTER SET = gbk COLLATE = gbk_bin ROW_FORMAT = Compact;`)
	tk.MustExec("INSERT INTO tsecurity (security_id, security_code, mkt_id, security_name) VALUES (1, '1', 1 ,'\xB2\xE2')")
	tk.MustExec("PREPARE a FROM 'INSERT INTO tsecurity (security_id, security_code, mkt_id, security_name) VALUES (2, 2, 2 ,\"\xB2\xE2\")'")
	tk.MustExec("EXECUTE a")
	stmt, _, _, err := tk.Session().PrepareStmt("INSERT INTO tsecurity (security_id, security_code, mkt_id, security_name) VALUES (3, 3, 3 ,\"\xB2\xE2\")")
	require.Nil(t, err)
	rs, err := tk.Session().ExecutePreparedStmt(context.TODO(), stmt, nil)
	require.Nil(t, err)
	require.Nil(t, rs)
}
