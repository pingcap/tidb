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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestScaleNDV(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_opt_scale_ndv_skew_ratio = 0`)
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

func TestOptScaleNDVSkewRatioSetVar(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a), key(b));`)
	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%d, %d)", i%20, i))
	}
	tk.MustExec(`insert into t values ` + strings.Join(vals, ","))
	tk.MustExec("analyze table t")
	tk.MustExec(`set @@tidb_stats_load_sync_wait=100`)

	aggEstRows := tk.MustQuery(`explain select /*+ set_var(tidb_opt_scale_ndv_skew_ratio=0) */ distinct(a) from t where b<50`).Rows()[0][1].(string)
	require.Equal(t, aggEstRows, "19.44")
	aggEstRows = tk.MustQuery(`explain select /*+ set_var(tidb_opt_scale_ndv_skew_ratio="0.5") */ distinct(a) from t where b<50`).Rows()[0][1].(string)
	require.Equal(t, aggEstRows, "14.82") // less than the prior one
	aggEstRows = tk.MustQuery(`explain select /*+ set_var(tidb_opt_scale_ndv_skew_ratio=1) */ distinct(a) from t where b<50`).Rows()[0][1].(string)
	require.Equal(t, aggEstRows, "10.20") // less than the prior one
}

func TestIssue54812(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_opt_scale_ndv_skew_ratio = 0`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int, b int, key(a), key(b));`)
	vals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%d, 1)", i))
	}
	tk.MustExec(`insert into t values ` + strings.Join(vals, ","))
	tk.MustExec(`insert into t values ` + strings.Repeat("(100, 2), ", 999) + "(100, 2)")
	tk.MustExec("analyze table t")
	tk.MustExec(`set @@tidb_stats_load_sync_wait=100`)
	aggEstRows := tk.MustQuery(`explain select distinct(a) from t where b=1`).Rows()[0][1].(string)
	require.Equal(t, "65.23", aggEstRows)
}
