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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func testCostQueries(t *testing.T, tk *testkit.TestKit, queries []string) {
	// costs of these queries expected increasing
	var lastCost float64
	var lastPlan []string
	var lastQuery string
	for _, q := range queries {
		rs := tk.MustQuery("explain format='verbose' " + q).Rows()
		cost, err := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err)
		var plan []string
		for _, r := range rs {
			plan = append(plan, fmt.Sprintf("%v", r))
		}
		require.True(t, cost > lastCost, fmt.Sprintf("cost of %v should be larger than\n%v\n%v\n%v\n",
			q, lastQuery, strings.Join(plan, "\n"), strings.Join(lastPlan, "\n")))
		lastCost = cost
		lastPlan = plan
		lastQuery = q
	}
}

func TestCostModelVer2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, key(b))`)
	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", i, i, i))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(vals, ", ")))
	tk.MustExec("analyze table t")
	for _, q := range []string{
		"set @@tidb_distsql_scan_concurrency=1",
		"set @@tidb_executor_concurrency=1",
		"set @@tidb_opt_tiflash_concurrency_factor=1",
		"set @@tidb_index_lookup_concurrency=1",
		"set @@tidb_cost_model_version=2",
	} {
		tk.MustExec(q)
	}

	seriesCases := [][]string{
		{ // table scan more rows
			"select /*+ use_index(t, primary) */ a from t where a<1",
			"select /*+ use_index(t, primary) */ a from t where a<10",
			"select /*+ use_index(t, primary) */ a from t where a<100",
		},
		{ // index scan more rows
			"select /*+ use_index(t, b) */ b from t where b<1",
			"select /*+ use_index(t, b) */ b from t where b<10",
			"select /*+ use_index(t, b) */ b from t where b<100",
		},
		{ // table scan more cols
			"select /*+ use_index(t, primary) */ a from t",
			"select /*+ use_index(t, primary) */ a, b from t",
			"select /*+ use_index(t, primary) */ a, b, c from t",
		},
		{ // index lookup more rows
			"select /*+ use_index(t, b) */ * from t where b<1",
			"select /*+ use_index(t, b) */ * from t where b<10",
			"select /*+ use_index(t, b) */ * from t where b<100",
		},
		{ // selection more filters
			"select /*+ use_index(t, primary) */ a from t where mod(a, 20)<10",
			"select /*+ use_index(t, primary) */ a from t where mod(a, 20)<10 and mod(a, 20)<11",
			"select /*+ use_index(t, primary) */ a from t where mod(a, 20)<10 and mod(a, 20)<11 and mod(a, 20)<12",
		},
		{ // projection more exprs
			"select /*+ use_index(t, primary) */ a+1 from t",
			"select /*+ use_index(t, primary) */ a+1, a+2 from t",
			"select /*+ use_index(t, primary) */ a+1, a+2, a+3 from t",
		},
		{ // hash agg more agg-funcs
			"select /*+ use_index(t, primary), hash_agg() */ sum(a) from t group by b",
			"select /*+ use_index(t, primary), hash_agg() */ sum(a), sum(a+2) from t group by b",
			"select /*+ use_index(t, primary), hash_agg() */ sum(a), sum(a+2), sum(a+4) from t group by b",
		},
		{ // hash agg more group-items
			"select /*+ use_index(t, primary), hash_agg() */ sum(a) from t group by b",
			"select /*+ use_index(t, primary), hash_agg() */ sum(a) from t group by b, b+1",
			"select /*+ use_index(t, primary), hash_agg() */ sum(a) from t group by b, b+1, b+2",
		},
		{ // stream agg more agg-funcs
			"select /*+ use_index(t, primary), stream_agg() */ sum(a) from t group by b",
			"select /*+ use_index(t, primary), stream_agg() */ sum(a), sum(a+2) from t group by b",
			"select /*+ use_index(t, primary), stream_agg() */ sum(a), sum(a+2), sum(a+4) from t group by b",
		},
		{ // hash join uses the small table to build hash table
			"select /*+ hash_join_build(t1) */ * from t t1, t t2 where t1.b=t2.b and t1.a<10",
			"select /*+ hash_join_build(t2) */ * from t t1, t t2 where t1.b=t2.b and t1.a<10",
		},
		{ // hash join more join keys
			"select /*+ hash_join_build(t1) */ * from t t1, t t2 where t1.b=t2.b",
			"select /*+ hash_join_build(t1) */ * from t t1, t t2 where t1.a=t2.a and t1.b=t2.b",
		},
	}

	for _, cases := range seriesCases {
		testCostQueries(t, tk, cases)
	}
}

func TestCostModelShowFormula(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int)`)
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustExec("set @@tidb_cost_model_version=2")

	tk.MustExecToErr("explain format='true_card_cost' select * from t") // 'true_card_cost' must work with 'explain analyze'
	plan := tk.MustQuery("explain analyze format='true_card_cost' select * from t where a<3").Rows()
	actual := make([][]interface{}, 0, len(plan))
	for _, row := range plan {
		actual = append(actual, []interface{}{row[0], row[3]}) // id,costFormula
		fmt.Println(actual)
	}
	require.Equal(t, actual, [][]interface{}{
		{"TableReader_7", "((Selection_6) + (net(2*rowsize(16)*tidb_kv_net_factor(8))) + (seek(tasks(20)*tidb_request_factor(9.5e+06))))/15"},
		{"└─Selection_6", "(cpu(3*filters(1)*tikv_cpu_factor(30))) + (TableFullScan_5)"},
		{"  └─TableFullScan_5", "scan(3*logrowsize(29)*tikv_scan_factor(100))"},
	})
}

func TestCostModelTraceVer2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, c int, key(b))`)
	vals := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", i, i, i))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(vals, ", ")))
	tk.MustExec("analyze table t")
	tk.MustExec("set @@tidb_cost_model_version=2")

	for _, q := range []string{
		"select * from t",
		"select * from t where a<4",
		"select * from t use index(b) where b<4",
		"select * from t where a<4 order by b",
		"select * from t where a<4 order by b limit 3",
		"select sum(a) from t where a<4 group by b, c",
		"select max(a), b, c from t where a<4 group by b, c",
		"select * from t t1, t t2",
		"select * from t t1, t t2 where t1.a=t2.a",
		"select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.b=t2.b",
	} {
		plan := tk.MustQuery("explain analyze format='true_card_cost' " + q).Rows()
		planCost, err := strconv.ParseFloat(plan[0][2].(string), 64)
		require.Nil(t, err)

		// check the accuracy of factor costs
		ok := false
		warns := tk.MustQuery("show warnings").Rows()
		for _, warn := range warns {
			msg := warn[2].(string)
			if strings.HasPrefix(msg, "factor costs: ") {
				costData := msg[len("factor costs: "):]
				var factorCosts map[string]float64
				require.Nil(t, json.Unmarshal([]byte(costData), &factorCosts))
				var sum float64
				for _, factorCost := range factorCosts {
					sum += factorCost
				}
				absDiff := math.Abs(sum - planCost)
				if absDiff < 5 || absDiff/planCost < 0.01 {
					ok = true
				}
			}
		}
		require.True(t, ok)
	}
}
