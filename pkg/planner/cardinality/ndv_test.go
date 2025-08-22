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

package cardinality_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestScaleNDV(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().RiskScaleNDVSkewRatio = 0
	type TestCase struct {
		OriginalNDV  float64
		OriginalRows float64
		SelectedRows float64
		NewNDV       float64
	}
	cases := []TestCase{
		{0, 0, 0, 0},
		{10, 0, 100, 0},
		{10, 100, 100, 10},
		{10, 100, 1, 1},
		{10, 100, 2, 1.83},
		{10, 100, 10, 6.51},
		{10, 100, 50, 9.99},
		{10, 100, 80, 10.00},
		{10, 100, 90, 10.00},
	}
	for _, tc := range cases {
		newNDV := cardinality.ScaleNDV(tk.Session().GetSessionVars(), tc.OriginalNDV, tc.OriginalRows, tc.SelectedRows)
		require.Equal(t, fmt.Sprintf("%.2f", tc.NewNDV), fmt.Sprintf("%.2f", newNDV), tc)
	}
}

func TestIssue54812(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().RiskScaleNDVSkewRatio = 0
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int, b int, key(a), key(b));`)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, 1))
	}
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", 100, 2))
	}
	tk.MustExec("analyze table t")
	tk.MustExec(`set @@tidb_stats_load_sync_wait=100`)
	tk.MustQuery(`explain select distinct(a) from t where b=1`).Check(testkit.Rows(
		"HashAgg_15 65.23 root  group by:test.t.a, funcs:firstrow(test.t.a)->test.t.a",
		"└─TableReader_16 65.23 root  data:HashAgg_5",
		"  └─HashAgg_5 65.23 cop[tikv]  group by:test.t.a, ",
		"    └─Selection_14 100.00 cop[tikv]  eq(test.t.b, 1)",
		"      └─TableFullScan_13 1100.00 cop[tikv] table:t keep order:false"))
}
