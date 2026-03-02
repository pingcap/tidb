// Copyright 2025 PingCAP, Inc.
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

package bindinfo_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestGenPlanWithSCtx(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int, b int, c int, key(a), key(b))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key(a), key(b))`)

	p := parser.New()
	sctx := tk.Session()
	sctx.GetSessionVars().CostModelVersion = 2
	check := func(sql, expectedHint, expectedPlan string) {
		p.Reset()
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoErrorf(t, err, "sql: %s", sql)
		planDigest, planHint, planText, err := bindinfo.GenBriefPlanWithSCtx(sctx, stmt)
		require.NoErrorf(t, err, "sql: %s", sql)
		require.Greaterf(t, len(planDigest), 0, "sql: %s", sql)
		require.Truef(t, strings.Contains(planHint, expectedHint), "sql: %s", sql)
		planOperators := make([]string, 0, len(planText))
		for _, row := range planText {
			planOperators = append(planOperators, row[0])
		}
		require.Truef(t, strings.Contains(strings.Join(planOperators, ","), expectedPlan), "sql: %s", sql)
	}
	check("select count(1) from t1 where a=1",
		"stream_agg", "StreamAgg")

	sctx.GetSessionVars().StreamAggCostFactor = 10000
	check("select count(1) from t1 where a=1",
		"hash_agg", "HashAgg")
	sctx.GetSessionVars().StreamAggCostFactor = 1

	check("select * from t1, t2 where t1.a=t2.a and t2.b=1",
		"inl_hash_join", "IndexHashJoin")

	sctx.GetSessionVars().IndexJoinCostFactor = 100000
	sctx.GetSessionVars().HashJoinCostFactor = 100000
	check("select * from t1, t2 where t1.a=t2.a and t2.b=1",
		"merge_join", `MergeJoin`)
}

func TestExplainExploreBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	check := func(sql string, expectedRowCount int) {
		rows := tk.MustQuery(sql).Rows()
		require.Equalf(t, expectedRowCount, len(rows), "sql: %s", sql)
		for _, row := range rows {
			planDigest := row[3]
			require.NotEmptyf(t, planDigest, "sql: %s", sql)
		}
	}

	tk.MustExec(`create table t (a int, b int, c varchar(10), key(a))`)
	check(`explain explore select a from t where b=1`, 1)
	tk.MustExec(`create global binding using select a from t where b=1`)
	check(`explain explore select a from t where b=1`, 2)
	check(`explain explore SELECT a FROM t WHERE b=1`, 2)
	check(`explain explore SELECT a FROM t WHERE b= 1`, 2)
	check(`explain explore      SELECT  a FROM test.t WHERE b= 1`, 2)
	require.GreaterOrEqual(t, len(tk.MustQuery(`explain explore "23109784b802bcef5398dd81d3b1c5b79200c257c101a5b9f90758206f3d09ed"`).Rows()), 1)

	check(`explain explore select a from t where b in (1, 2, 3)`, 1)
	tk.MustExec(`create global binding using select a from t where b in (1, 2, 3)`)
	check(`explain explore select a from t where b in (1, 2, 3)`, 2)
	check(`explain explore select a from t where b in (1, 2)`, 2)
	check(`explain explore select a from t where b in (1)`, 2)
	check(`explain explore SELECT a from t WHere b in (1)`, 2)

	check(`explain explore select a from t where c = ''`, 1)
	tk.MustExec(`create global binding using select a from t where c = ''`)
	check(`explain explore select a from t where c = ''`, 2)
	check(`explain explore select a from t where c = '123'`, 2)
	check(`explain explore select a from t where c = '\"'`, 2)
	check(`explain explore select a from t where c = '              '`, 2)
	check(`explain explore select a from t where c = ""`, 2)
	check(`explain explore select a from t where c = "\'"`, 2)

	tk.MustExecToErr("explain explore 'xxx'", "")
	tk.MustExecToErr("explain explore SELECT A FROM", "")
}

func TestExplainExploreIndexHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, key(a), key(b))`)

	rows := tk.MustQuery(`explain explore select * from t where a=1 and b=1`).Rows()
	hasIndexA, hasIndexB := false, false
	for _, row := range rows {
		plan := row[2].(string)
		if strings.Contains(plan, "index:a") {
			hasIndexA = true
		}
		if strings.Contains(plan, "index:b") {
			hasIndexB = true
		}
	}
	require.True(t, hasIndexA, "expected index a plan in explain explore output")
	require.True(t, hasIndexB, "expected index b plan in explain explore output")
}

func TestExplainExploreIndexHintWithAlias(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, x varchar(10), key(a), key(b))`)

	rows := tk.MustQuery(`explain explore select 1 from t t_alias where a=1 and b=1 and x like "%xx%"`).Rows()
	hasIndexA, hasIndexB := false, false
	for _, row := range rows {
		plan := row[2].(string)
		if strings.Contains(plan, "index:a") {
			hasIndexA = true
		}
		if strings.Contains(plan, "index:b") {
			hasIndexB = true
		}
	}
	require.True(t, hasIndexA, "expected index a plan in explain explore output")
	require.True(t, hasIndexB, "expected index b plan in explain explore output")
}

func TestIsSimplePointPlan(t *testing.T) {
	require.True(t, bindinfo.IsSimplePointPlan(`       id  task    estRows operator info  actRows execution info  memory          disk
        Projection_4    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A`))
	require.True(t, bindinfo.IsSimplePointPlan(`       id  task    estRows operator info  actRows execution info  memory          disk
        Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A`))
	require.True(t, bindinfo.IsSimplePointPlan(`Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A`))
	require.True(t, bindinfo.IsSimplePointPlan(`id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Projection_4            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   `))
	require.True(t, bindinfo.IsSimplePointPlan(`id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   `))
	require.True(t, bindinfo.IsSimplePointPlan(`id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Selection ....
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   `))

	require.False(t, bindinfo.IsSimplePointPlan(`       id                      task            estRows operator info                           actRows execution info memory          disk
        TableReader_5           root            10000   data:TableFullScan_4                    0       time:456.3µs, open:141µs, close:6.79µs, loops:1, cop_task: {num: 1, max: 241.3µs, proc_keys: 0, copr_cache_hit_ratio: 0.00, build_task_duration: 91.5µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:203.9µs}}      182 Bytes       N/A
        └─TableFullScan_4       cop[tikv]       10000   table:t, keep order:false, stats:pseudo 0       tikv_task:{time:155.2µs, loops:0}                                                                                                                                                                                                         N/A             N/A `))
	require.False(t, bindinfo.IsSimplePointPlan(`id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        HashAgg            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   `))
	require.False(t, bindinfo.IsSimplePointPlan(`       id  task    estRows operator info  actRows execution info  memory          disk
        HashJoin    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A`))
	require.False(t, bindinfo.IsSimplePointPlan(``))
	require.False(t, bindinfo.IsSimplePointPlan(`  \n   `))
}

func TestRelevantOptVarsAndFixes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int, b int, c varchar(10), key(a), key(b))`)
	tk.MustExec(`create table t2 (a int, b int, c varchar(10), key(a), key(b))`)

	var input []string
	var output []struct {
		Vars  string
		Fixes string
	}
	bindingAutoSuiteData.LoadTestCases(t, &input, &output)
	p := parser.New()
	for i, sql := range input {
		p.Reset()
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoErrorf(t, err, "sql: %s", sql)
		vars, fixes, err := bindinfo.RecordRelevantOptVarsAndFixes(tk.Session(), stmt)
		require.NoErrorf(t, err, "sql: %s", sql)
		testdata.OnRecord(func() {
			output[i].Vars = fmt.Sprintf("%v", vars)
			output[i].Fixes = fmt.Sprintf("%v", fixes)
		})
		require.Equalf(t, fmt.Sprintf("%v", vars), output[i].Vars, "sql: %s", sql)
		require.Equalf(t, fmt.Sprintf("%v", fixes), output[i].Fixes, "sql: %s", sql)
	}
}

func TestExplainExploreAnalyze(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`insert into t values (1, 2), (2, 3), (3, 4), (4, 5)`)

	checkExecInfo := func(sql string, hasExecInfo bool) {
		rs := tk.MustQuery(sql).Rows()
		for _, row := range rs {
			latency := row[4].(string)
			execTimes := row[5].(string)
			retRows := row[7].(string)
			if !hasExecInfo {
				require.Equalf(t, "0", latency, "sql: %s", sql)
				require.Equalf(t, "0", execTimes, "sql: %s", sql)
				require.Equalf(t, "0", retRows, "sql: %s", sql)
			} else {
				require.NotEqualf(t, "0", latency, "sql: %s", sql)
				require.NotEqualf(t, "0", execTimes, "sql: %s", sql)
				require.NotEqualf(t, "0", retRows, "sql: %s", sql)
			}
		}
	}

	checkExecInfo(`explain explore select * from t where a=1`, false)
	checkExecInfo(`explain explore analyze select * from t where a=1`, true)
	checkExecInfo(`explain explore select * from t where b<10`, false)
	checkExecInfo(`explain explore analyze select * from t where b<10`, true)
	checkExecInfo(`explain explore select count(1) from t where b<10`, false)
	checkExecInfo(`explain explore analyze select count(1) from t where b<10`, true)
}

func TestExplainExploreVerifyAndBind(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`insert into t values (1, 2), (2, 3), (3, 4), (4, 5)`)

	tk.MustQuery(`select * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	require.Equal(t, 0, len(tk.MustQuery(`show global bindings`).Rows())) // no binding

	rs := tk.MustQuery(`explain explore select * from t`).Rows()
	runStmt := rs[0][12].(string)    // "EXPLAIN ANALYZE <bind_sql>"
	bindingSQL := rs[0][13].(string) // "CREATE GLOBAL BINDING USING <bind_sql>"

	require.True(t, strings.HasPrefix(runStmt, "EXPLAIN ANALYZE"))
	require.True(t, strings.HasPrefix(bindingSQL, "CREATE GLOBAL BINDING USING"))

	rs = tk.MustQuery(runStmt).Rows()
	require.True(t, strings.Contains(rs[0][0].(string), "TableReader")) // table scan and no error

	tk.MustExec(bindingSQL)
	tk.MustQuery(`select * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	require.Equal(t, 1, len(tk.MustQuery(`show global bindings`).Rows()))
}

func TestPlanGeneration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, key(a))`)
	tk.MustExec(`create table t1 (a int, b int, c int, key(a), key(b))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key(a), key(b))`)
	tk.MustExec(`create table t3 (a int, b int, c int, key(a), key(b))`)

	var input []string
	var output []struct {
		SQL  string
		Plan [][]string
	}
	bindingAutoSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		rows := tk.MustQuery(sql).Rows()
		for rowID, row := range rows {
			plan := strings.Split(strings.Replace(row[2].(string), "\t", "  ", -1), "\n")
			testdata.OnRecord(func() {
				output[i].SQL = sql
				if len(output[i].Plan) < rowID {
					output[i].Plan[rowID] = plan
				} else {
					output[i].Plan = append(output[i].Plan, plan)
				}
			})
			require.Equalf(t, plan, output[i].Plan[rowID], "sql: %s", sql)
		}
	}
}
