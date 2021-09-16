// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/testkit"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestGlobalAndSessionBindingBothExist(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"))
	require.True(t, tk.HasPlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"))

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	// Test bindingUsage, which indicates how many times the binding is used.
	metrics.BindUsageCounter.Reset()
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"))
	pb := &dto.Metric{}
	err := metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Write(pb)
	require.NoError(t, err)
	require.Equal(t, float64(1), pb.GetCounter().GetValue())

	// Test 'tidb_use_plan_baselines'
	tk.MustExec("set @@tidb_use_plan_baselines = 0")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"))
	tk.MustExec("set @@tidb_use_plan_baselines = 1")

	// Test 'drop global binding'
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"))
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"))

	// Test the case when global and session binding both exist
	// PART1 : session binding should totally cover global binding
	// use merge join as session binding here since the optimizer will choose hash join for this stmt in default
	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_HJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"))
	tk.MustExec("create binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"))
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"))

	// PART2 : the dropped session binding should continue to block the effect of global binding
	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	tk.MustExec("drop binding for SELECT * from t1,t2 where t1.id = t2.id")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"))
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	require.True(t, tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"))
}
