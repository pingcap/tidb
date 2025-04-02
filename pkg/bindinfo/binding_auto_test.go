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
	"testing"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowPlanForSQLBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t (a int, b int, c varchar(10), key(a))`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b=1"`).Rows()) == 0)
	tk.MustExec(`create global binding using select a from t where b=1`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b=1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "SELECT a FROM t WHERE b=1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "SELECT a FROM t WHERE b= 1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "     SELECT  a FROM test.t WHERE b= 1"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "23109784b802bcef5398dd81d3b1c5b79200c257c101a5b9f90758206f3d09ed"`).Rows()) == 1)

	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1, 2, 3)"`).Rows()) == 0)
	tk.MustExec(`create global binding using select a from t where b in (1, 2, 3)`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1, 2, 3)"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1, 2)"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where b in (1)"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "SELECT a from t WHere b in (1)"`).Rows()) == 1)

	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = ''"`).Rows()) == 0)
	tk.MustExec(`create global binding using select a from t where c = ''`)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = ''"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = '123'"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = '\"'"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for "select a from t where c = '              '"`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for 'select a from t where c = ""'`).Rows()) == 1)
	require.True(t, len(tk.MustQuery(`show plan for 'select a from t where c = "\'"'`).Rows()) == 1)

	tk.MustExecToErr("show plan for 'xxx'", "")
	tk.MustExecToErr("show plan for 'SELECT A FROM'", "")
}

func TestShowPlanRecommendation(t *testing.T) {
	pointPlan := `       id  task    estRows operator info  actRows execution info  memory          disk
        Projection_4    root    1       plus(test.t.a, 1)->Column#3     0       time:173µs, open:24.9µs, close:8.92µs, loops:1, Concurrency:OFF                         380 Bytes       N/A
        └─Point_Get_5   root    1       table:t, handle:2               0       time:143.2µs, open:1.71µs, close:5.92µs, loops:1, Get:{num_rpc:1, total_time:40µs}      N/A             N/A`
	batchPointPlan := `id                      task    estRows operator info                                           actRows execution info                                                                                                                    memory          disk
        Projection_4            root    3.00    plus(test.t.a, 1)->Column#3                             0       time:218.3µs, open:14.5µs, close:9.79µs, loops:1, Concurrency:OFF                                                                 145 Bytes       N/A
        └─Batch_Point_Get_5     root    3.00    table:t, handle:[1 2 3], keep order:false, desc:false   0       time:201.1µs, open:3.83µs, close:6.46µs, loops:1, BatchGet:{num_rpc:2, total_time:65.7µs}, rpc_errors:{epoch_not_match:1} N/A             N/A   `
	nonPointPlan := `       id                      task            estRows operator info                           actRows execution info memory          disk
        TableReader_5           root            10000   data:TableFullScan_4                    0       time:456.3µs, open:141µs, close:6.79µs, loops:1, cop_task: {num: 1, max: 241.3µs, proc_keys: 0, copr_cache_hit_ratio: 0.00, build_task_duration: 91.5µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:203.9µs}}      182 Bytes       N/A
        └─TableFullScan_4       cop[tikv]       10000   table:t, keep order:false, stats:pseudo 0       tikv_task:{time:155.2µs, loops:0}                                                                                                                                                                                                         N/A             N/A `

	// Test rule 1
	p1 := &bindinfo.BindingPlanInfo{
		Plan:                 nonPointPlan,
		AvgLatency:           100,
		ExecTimes:            100,
		AvgScanRows:          100,
		AvgReturnedRows:      100,
		LatencyPerReturnRow:  100,
		ScanRowsPerReturnRow: 100,
	}
	p2 := &bindinfo.BindingPlanInfo{
		Plan:                 pointPlan,
		AvgLatency:           100,
		ExecTimes:            100,
		AvgScanRows:          100,
		AvgReturnedRows:      100,
		LatencyPerReturnRow:  100,
		ScanRowsPerReturnRow: 100,
	}

	bindinfo.FillRecommendation([]*bindinfo.BindingPlanInfo{p1, p2})
	require.Equal(t, p2.Recommend, "YES (rule-based)")
	require.Equal(t, p2.Reason, "Simple PointGet or BatchPointGet is the best plan")
	p2.Recommend, p2.Reason = "", ""

	p1.Plan, p2.Plan = batchPointPlan, nonPointPlan
	bindinfo.FillRecommendation([]*bindinfo.BindingPlanInfo{p1, p2})
	require.Equal(t, p1.Recommend, "YES (rule-based)")
	require.Equal(t, p1.Reason, "Simple PointGet or BatchPointGet is the best plan")
	p1.Reason, p1.Recommend = "", ""

	// Test rule 2
	p1.Plan, p2.Plan = nonPointPlan, nonPointPlan
	p2.ScanRowsPerReturnRow = 30
	bindinfo.FillRecommendation([]*bindinfo.BindingPlanInfo{p1, p2})
	require.Equal(t, p2.Recommend, "YES (rule-based)")
	require.Equal(t, p2.Reason, "Plan's scan_rows_per_returned_row is 50% better than others'")
	p2.ScanRowsPerReturnRow = 100
	p2.Recommend, p2.Reason = "", ""

	// Test rule 3
	p1.AvgLatency = 30
	p1.AvgScanRows = 30
	p1.LatencyPerReturnRow = 30
	bindinfo.FillRecommendation([]*bindinfo.BindingPlanInfo{p1, p2})
	require.Equal(t, p1.Recommend, "YES (rule-based)")
	require.Equal(t, p1.Reason, "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'")
	p1.Reason, p1.Recommend = "", ""

	p1.AvgLatency = 60
	bindinfo.FillRecommendation([]*bindinfo.BindingPlanInfo{p1, p2})
	require.Equal(t, p1.Recommend, "")
}
