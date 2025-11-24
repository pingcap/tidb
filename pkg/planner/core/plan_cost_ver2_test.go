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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCostModelVer2ScanRowSize(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t (pk int, a int, b int, c int, d int, primary key(pk), index ab(a, b), index abc(a, b, c))`)
		tk.MustExec("insert into t values (1, 1, 1, 1, 1)")
		tk.MustExec(`set @@tidb_cost_model_version=2`)
		tk.MustExec("set global tidb_enable_collect_execution_info=1;")

		cases := []struct {
			query       string
			scanFormula string
		}{
			// index scan row-size on idx_ab is always equal to row-size(index_ab)
			{"select a from t use index(ab) where a=1", "(scan(1*logrowsize(32)*tikv_scan_factor(40.7)))*1.00"},
			{"select a, b from t use index(ab) where a=1", "(scan(1*logrowsize(32)*tikv_scan_factor(40.7)))*1.00"},
			{"select b from t use index(ab) where a=1 and b=1", "(scan(1*logrowsize(32)*tikv_scan_factor(40.7)))*1.00"},
			// index scan row-size on idx_abc is always equal to row-size(index_abc)
			{"select a from t use index(abc) where a=1", "(scan(1*logrowsize(48)*tikv_scan_factor(40.7)))*1.00"},
			{"select a from t use index(abc) where a=1 and b=1", "(scan(1*logrowsize(48)*tikv_scan_factor(40.7)))*1.00"},
			{"select a, b from t use index(abc) where a=1 and b=1", "(scan(1*logrowsize(48)*tikv_scan_factor(40.7)))*1.00"},
			{"select a, b, c from t use index(abc) where a=1 and b=1 and c=1", "(scan(1*logrowsize(48)*tikv_scan_factor(40.7)))*1.00"},
			// table scan row-size is always equal to row-size(*)
			{"select a from t use index(primary) where a=1", "((scan(1*logrowsize(80)*tikv_scan_factor(40.7))) + (scan(1000*logrowsize(80)*tikv_scan_factor(40.7))))*1.00"},
			{"select a, d from t use index(primary) where a=1", "((scan(1*logrowsize(80)*tikv_scan_factor(40.7))) + (scan(1000*logrowsize(80)*tikv_scan_factor(40.7))))*1.00"},
			{"select * from t use index(primary) where a=1", "((scan(1*logrowsize(80)*tikv_scan_factor(40.7))) + (scan(1000*logrowsize(80)*tikv_scan_factor(40.7))))*1.00"},
		}
		for _, c := range cases {
			rs := tk.MustQuery("explain analyze format=true_card_cost " + c.query).Rows()
			scan := rs[len(rs)-1]
			formula := scan[3]
			require.Equal(t, formula, c.scanFormula)
		}

		tk.MustQuery("explain select a from t where a=1").Check(testkit.Rows(
			`IndexReader_7 10.00 root  index:IndexRangeScan_6`, // use idx_ab automatically since it has the smallest row-size in all access paths.
			`└─IndexRangeScan_6 10.00 cop[tikv] table:t, index:ab(a, b) range:[1,1], keep order:false, stats:pseudo`))
		tk.MustQuery("explain select a, b, c from t where a=1").Check(testkit.Rows(
			`IndexReader_7 10.00 root  index:IndexRangeScan_6`, // use idx_abc automatically
			`└─IndexRangeScan_6 10.00 cop[tikv] table:t, index:abc(a, b, c) range:[1,1], keep order:false, stats:pseudo`))
	})
}

func TestCostModelTraceVer2(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t (a int primary key, b int, c int, key(b))`)
		vals := make([]string, 0, 10)
		for i := range 10 {
			vals = append(vals, fmt.Sprintf("(%v, %v, %v)", i, i, i))
		}
		tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(vals, ", ")))
		tk.MustExec("analyze table t")

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
	})
}

func BenchmarkGetPlanCost(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int);")

	p := parser.New()
	sql := "select sum(t1.b), t1.a from t t1, t t2 where t1.a>0 and t2.a>10 and t1.b=t2.b group by t1.a order by t1.a limit 5"
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		b.Fatal(err)
	}
	sctx := tk.Session()
	sctx.GetSessionVars().CostModelVersion = 2
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	nodeW := resolve.NewNodeW(stmt)
	plan, _, err := planner.Optimize(context.TODO(), sctx, nodeW, is)
	if err != nil {
		b.Fatal(err)
	}
	phyPlan := plan.(base.PhysicalPlan)
	_, err = core.GetPlanCost(phyPlan, property.RootTaskType, costusage.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = core.GetPlanCost(phyPlan, property.RootTaskType, costusage.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate))
	}
}

func TestTableScanCostWithForce(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		store := tk.Session().GetStore()
		defer func() {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec("drop table if exists t")
			dom.StatsHandle().Clear()
		}()

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, primary key (a))")

		// Insert some data
		tk.MustExec("insert into t values (1,1),(2,2),(3,3),(4,4),(5,5)")

		// Analyze table to update statistics
		tk.MustExec("analyze table t")

		// Test TableFullScan with and without FORCE INDEX
		rs := tk.MustQuery("explain format=verbose select * from t").Rows()
		planCost1, err1 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = tk.MustQuery("explain format=verbose select * from t force index(PRIMARY)").Rows()
		planCost2, err2 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)

		// Query with FORCE should be more expensive than query without
		require.Less(t, planCost1, planCost2)

		// Test TableRangeScan with and without FORCE INDEX
		rs = tk.MustQuery("explain format=verbose select * from t where a > 1").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		rs = tk.MustQuery("explain format=verbose select * from t force index(PRIMARY) where a > 1").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)

		// Query costs should be equal since FORCE cost penalty does not apply to range scan
		require.Equal(t, planCost1, planCost2)
	})
}

func TestOptimizerCostFactors(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		store := tk.Session().GetStore()
		defer func() {
			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			tk2.MustExec("drop table if exists t")
			dom.StatsHandle().Clear()
		}()

		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, c int, primary key (a), key(b))")

		// Insert some data
		tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")

		// Analyze table to update statistics
		tk.MustExec("analyze table t")

		// Test tableFullScan cost factor increase
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")
		rs := tk.MustQuery("explain format=verbose select * from t ignore index(b)").Rows()
		planCost1, err1 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t ignore index(b)").Rows()
		planCost2, err2 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test tableFullScan cost factor decrease
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t ignore index(b)").Rows()
		planCost3, err3 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)
		// Reset to default
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test tableReader cost factor increase
		tk.MustExec("set @@session.tidb_opt_table_reader_cost_factor=1")
		rs = tk.MustQuery("explain format=verbose select * from t ignore index(b)").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_table_reader_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t ignore index(b)").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test tableReader cost factor decrease
		tk.MustExec("set @@session.tidb_opt_table_reader_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t ignore index(b)").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)
		// Reset to default
		tk.MustExec("set @@session.tidb_opt_table_reader_cost_factor=1")

		// Test tableRangeScan cost factor increase
		tk.MustExec("set @@session.tidb_opt_table_range_scan_cost_factor=1")
		rs = tk.MustQuery("explain format=verbose select * from t use index(PRIMARY) where a > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_table_range_scan_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t use index(PRIMARY) where a > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test tableRangeScan cost factor decrease
		tk.MustExec("set @@session.tidb_opt_table_range_scan_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t use index(PRIMARY) where a > 3").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_table_range_scan_cost_factor=1")

		// Test IndexScan cost factor increase
		tk.MustExec("set @@session.tidb_opt_index_scan_cost_factor=1")
		// Increase table scan cost factor to isolate testing to IndexScan
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select b from t use index(b) where b > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_index_scan_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select b from t use index(b) where b > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test IndexScan cost factor decrease
		tk.MustExec("set @@session.tidb_opt_index_scan_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select b from t use index(b) where b > 3").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_scan_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test IndexReadercost factor increase
		tk.MustExec("set @@session.tidb_opt_index_reader_cost_factor=1")
		// Increase table scan cost factor to isolate testing to IndexReader
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select b from t use index(b) where b > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_index_reader_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select b from t use index(b) where b > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test IndexReader cost factor decrease
		tk.MustExec("set @@session.tidb_opt_index_reader_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select b from t use index(b) where b > 3").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_reader_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test IndexLookup cost factor increase
		tk.MustExec("set @@session.tidb_opt_index_lookup_cost_factor=1")
		// Increase table scan cost factor to isolate testing to IndexLookup
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_index_lookup_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test IndexLookup cost factor decrease
		tk.MustExec("set @@session.tidb_opt_index_lookup_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_lookup_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test TableRowIDScan cost factor increase
		tk.MustExec("set @@session.tidb_opt_table_rowid_scan_cost_factor=1")
		// Increase table scan cost factor to isolate testing to TableRowIDScan
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_table_rowid_scan_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test TableRowID cost factor decrease
		tk.MustExec("set @@session.tidb_opt_table_rowid_scan_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_table_rowid_scan_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")

		// Test Limit cost factor increase
		tk.MustExec("set @@session.tidb_opt_limit_cost_factor=1")
		// Increase TopN to isolate testing to Limit
		tk.MustExec("set @@session.tidb_opt_topn_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3 order by b limit 1").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_limit_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3 order by b limit 1").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test Limit cost factor decrease
		tk.MustExec("set @@session.tidb_opt_limit_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3 order by b limit 1").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_limit_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_topn_cost_factor=1")

		// Test TopN cost factor increase
		tk.MustExec("set @@session.tidb_opt_topn_cost_factor=1")
		// Increase TopN to isolate testing to Limit
		tk.MustExec("set @@session.tidb_opt_limit_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3 order by b limit 1").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_topn_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3 order by b limit 1").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test TopN cost factor decrease
		tk.MustExec("set @@session.tidb_opt_topn_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t use index(b) where b > 3 order by b limit 1").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_limit_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_topn_cost_factor=1")

		// Test StreamAgg cost factor increase
		tk.MustExec("set @@session.tidb_opt_stream_agg_cost_factor=1")
		// Set HashAgg cost factor higher to isolate testing to StreamAgg
		tk.MustExec("set @@session.tidb_opt_hash_agg_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select /*+ STREAM_AGG() */ b, count(*) from t use index(b) group by b").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_stream_agg_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select /*+ STREAM_AGG() */ b, count(*) from t use index(b) group by b").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test StreamAgg cost factor decrease
		tk.MustExec("set @@session.tidb_opt_stream_agg_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ STREAM_AGG() */ b, count(*) from t use index(b) group by b").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_stream_agg_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_hash_agg_cost_factor=1")

		// Test HashAgg cost factor increase
		tk.MustExec("set @@session.tidb_opt_hash_agg_cost_factor=1")
		// Set StreamAgg cost factor higher to isolate testing to HashAgg
		tk.MustExec("set @@session.tidb_opt_stream_agg_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select /*+ HASH_AGG() */ b, count(*) from t ignore index(b) group by b").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_hash_agg_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select /*+ HASH_AGG() */ b, count(*) from t ignore index(b) group by b").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test HashAgg cost factor decrease
		tk.MustExec("set @@session.tidb_opt_hash_agg_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ HASH_AGG() */ b, count(*) from t ignore index(b) group by b").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_stream_agg_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_hash_agg_cost_factor=1")

		// Test Sort cost factor increase
		tk.MustExec("set @@session.tidb_opt_sort_cost_factor=1")
		rs = tk.MustQuery("explain format=verbose select * from t order by c").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_sort_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select * from t order by c").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test Sort cost factor decrease
		tk.MustExec("set @@session.tidb_opt_sort_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select * from t order by c").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_sort_cost_factor=1")

		// Test IndexJoin cost factor increase
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=1")
		// Increase other join cost factors to isolate test to index join
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=100")
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select /*+ INL_JOIN(t1, t2) */ * from t as t1 inner join t as t2 on t1.b = t2.b").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select /*+ INL_JOIN(t1, t2) */ * from t as t1 inner join t as t2 on t1.b = t2.b").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test IndexJoin cost factor decrease
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ INL_JOIN(t1, t2) */ * from t as t1 inner join t as t2 on t1.b = t2.b").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=1")

		// Test MergeJoin cost factor increase
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=1")
		// Increase other join cost factors to isolate test to merge join
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=100")
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select /*+ MERGE_JOIN(t1, t2) */ * from t as t1 inner join t as t2 on t1.b = t2.b").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select /*+ MERGE_JOIN(t1, t2) */ * from t as t1 inner join t as t2 on t1.b = t2.b").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test MergeJoin cost factor decrease
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ MERGE_JOIN(t1, t2) */ * from t as t1 inner join t as t2 on t1.b = t2.b").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=1")

		// Test HashJoin cost factor increase
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=1")
		// Increase other join cost factors to isolate test to hash join
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=100")
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=100")
		rs = tk.MustQuery("explain format=verbose select /*+ HASH_JOIN(@sel_1 t1@sel_1, t2) */ * from t as t1 inner join t as t2 on t1.c = t2.c").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select/*+ HASH_JOIN(@sel_1 t1@sel_1, t2) */ * from t as t1 inner join t as t2 on t1.c = t2.c").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test HashJoin cost factor decrease
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ HASH_JOIN(@sel_1 t1@sel_1, t2) */ * from t as t1 inner join t as t2 on t1.c = t2.c").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_join_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_hash_join_cost_factor=1")
		tk.MustExec("set @@session.tidb_opt_merge_join_cost_factor=1")

		// Add another index to allow test for index merge
		tk.MustExec("create index ic on t(c)")
		// Analyze table to update statistics
		tk.MustExec("analyze table t")

		// Test IndexMerge cost factor increase
		tk.MustExec("set @@session.tidb_opt_index_merge_cost_factor=1")
		rs = tk.MustQuery("explain format=verbose select /*+ USE_INDEX_MERGE(t, b, ic) */ * from t where b > 4 and c > 4").Rows()
		planCost1, err1 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_index_merge_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select /*+ USE_INDEX_MERGE(t, b, ic) */ * from t where b > 4 and c > 4").Rows()
		planCost2, err2 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test index merge cost factor decrease
		tk.MustExec("set @@session.tidb_opt_index_merge_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ USE_INDEX_MERGE(t, b, ic) */ * from t where b > 4 and c > 4").Rows()
		planCost3, err3 = strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset to default
		tk.MustExec("set @@session.tidb_opt_index_merge_cost_factor=1")
	})
}

func TestIndexLookUpRowsLimit(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t(a int, b int, key ia(a))`)
		rs := tk.MustQuery("explain format='cost_trace' select * from t use index(ia) where a>6 limit 5 offset 100").Rows()
		// the cost formula should consider limit-offset clause, only scan 5 rows
		require.Equal(t, "(scan(5*logrowsize(48)*tikv_scan_factor(40.7)))*1.00", rs[3][3].(string))
		rs = tk.MustQuery("explain format='cost_trace' select * from t use index(ia) where a>6 limit 20 offset 100").Rows()
		require.Equal(t, "(scan(20*logrowsize(48)*tikv_scan_factor(40.7)))*1.00", rs[3][3].(string))
	})
}

func TestMergeJoinCostWithOtherConds(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t1 (id int, a int, b int, c int, primary key(id, a))`)
		tk.MustExec(`create table t2 (id int, a int, b int, c int, primary key(id, a))`)
		cost1Str := tk.MustQuery(`explain format='verbose' select /*+ merge_join(t1, t2) */ * from t1 join t2 on t1.id=t2.id`).Rows()[0][2].(string)
		cost2Str := tk.MustQuery(`explain format='verbose' select /*+ merge_join(t1, t2) */ * from t1 join t2 on t1.id=t2.id and t1.a>t2.a`).Rows()[0][2].(string)

		cost1, err := strconv.ParseFloat(cost1Str, 64)
		require.Nil(t, err)
		cost2, err := strconv.ParseFloat(cost2Str, 64)
		require.Nil(t, err)

		// cost2 should be larger than cost1 since it has an additional condition `t1.a>t2.a`
		require.Less(t, cost1, cost2)
	})
}

func TestTiFlashCostFactors(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int, b int, c int)")
		// Insert some data
		tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
		tk.MustExec("analyze table t;")
		tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual `tiflash` replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")

		// Test TiFlash cost factor increase
		tk.MustExec("set @@session.tidb_opt_table_tiflash_scan_cost_factor=1")
		// Increase table scan cost factor to isolate testing to TiFlash
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1000000")
		rs := tk.MustQuery("explain format=verbose select /*+ READ_FROM_STORAGE(TIFLASH) */ * from t").Rows()
		planCost1, err1 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err1)
		tk.MustExec("set @@session.tidb_opt_table_tiflash_scan_cost_factor=10")
		rs = tk.MustQuery("explain format=verbose select /*+ READ_FROM_STORAGE(TIFLASH) */ * from t").Rows()
		planCost2, err2 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err2)
		// 1st query should be cheaper than 2nd query
		require.Less(t, planCost1, planCost2)

		// Test TiFlash cost factor decrease
		tk.MustExec("set @@session.tidb_opt_table_tiflash_scan_cost_factor=0.1")
		rs = tk.MustQuery("explain format=verbose select /*+ READ_FROM_STORAGE(TIFLASH) */ * from t").Rows()
		planCost3, err3 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err3)
		// 3rd query should be cheaper than 1st query
		require.Less(t, planCost3, planCost1)

		// Reset TiFlash cost factor to default
		tk.MustExec("set @@session.tidb_opt_table_tiflash_scan_cost_factor=1")

		// Test TiFlash cost factor increase via hint
		rs = tk.MustQuery("explain format=verbose select /*+ SET_VAR(tidb_opt_table_tiflash_scan_cost_factor=2) */ * from t").Rows()
		planCost4, err4 := strconv.ParseFloat(rs[0][2].(string), 64)
		require.Nil(t, err4)
		// 1st query should be cheaper than 4th query
		require.Less(t, planCost1, planCost4)

		// Reset table scan cost factor to default
		tk.MustExec("set @@session.tidb_opt_table_full_scan_cost_factor=1")
		// Reset TiFlash cop to default
		tk.MustExec("set @@session.tidb_allow_tiflash_cop=OFF")
	})
}
