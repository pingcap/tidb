// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner_test

import (
	"archive/zip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestTiFlashOnlyRoundIntegration(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, key(b))")
	tk.MustExec("create table no_replica(a int)")
	tk.MustExec("create table replica_t(a int, b int, key(a))")
	testkit.SetTiFlashReplica(t, dom, "test", "replica_t")
	testkit.SetTiFlashReplica(t, dom, "test", "t")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("set tidb_allow_mpp=on, tidb_enforce_mpp=off")
	tk.MustExec("set tidb_isolation_read_engines='tikv,tiflash,tidb'")
	trace := func(sql string) string {
		return tk.MustQuery("trace plan target='debug' " + sql).Rows()[0][0].(string)
	}
	profile := trace("select * from t where b=1")
	require.NotContains(t, profile, `"Alternative round":"tiflash-only"`)
	joinSQL := "select * from t join replica_t n on t.b=n.a"
	tk.MustExec("set tidb_opt_table_tiflash_scan_cost_factor=1000000")
	require.Contains(t, trace(joinSQL), `"Alternative round":"tiflash-only"`)
	require.Contains(t, profile, `"Physical plan risk":{"risk":false,"signals":[]}`)
	require.Contains(t, profile, `"cbo_read_rows":`)
	tk.MustQuery("select @@tidb_isolation_read_engines").Check(testkit.Rows("tikv,tiflash,tidb"))
	require.Contains(t, trace("select /*+ read_from_storage(tikv[t]) */ * from t where b=1"), `"reason":"mode_off"`)
	for _, sql := range []string{
		"select /*+ read_from_storage(tikv[t]) */ * from t join replica_t n on t.b=n.a",
		"select * from t join no_replica n on t.a=n.a",
	} {
		require.NotContains(t, trace(sql), `"Alternative round":"tiflash-only"`)
	}
	finalPlan := func() []*tracing.PlanTrace {
		t.Helper()
		name := tk.MustQuery("trace plan " + joinSQL).Rows()[0][0].(string)
		path := filepath.Join(domain.GetOptimizerTraceDirName(), name)
		t.Cleanup(func() { _ = os.Remove(path) })
		z, err := zip.OpenReader(path)
		require.NoError(t, err)
		defer z.Close()
		f, err := z.Open("trace.json")
		require.NoError(t, err)
		defer f.Close()
		var result tracing.OptimizeTracer
		require.NoError(t, json.NewDecoder(f).Decode(&result))
		require.NotEmpty(t, result.FinalPlan)
		return result.FinalPlan
	}
	withAlternative := finalPlan()
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=off")
	require.Equal(t, finalPlan(), withAlternative)
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("set tidb_enforce_mpp=on")
	require.NotContains(t, trace(joinSQL), `"Alternative round":"tiflash-only"`)
	tk.MustExec("set tidb_enforce_mpp=off, tidb_allow_mpp=off")
	require.NotContains(t, trace(joinSQL), `"Alternative round":"tiflash-only"`)
	tk.MustExec("set tidb_allow_mpp=on, tidb_opt_enable_alternative_logical_plans=off")
	require.NotContains(t, trace(joinSQL), `"Alternative round":"tiflash-only"`)
}

func TestAlternativeRiskPlanCacheIsolation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cache_t(a int primary key, b int, key(b))")
	tk.MustExec("insert into cache_t values(1,10),(2,20)")
	tk.MustExec("set tidb_enable_prepared_plan_cache=on, tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("set tidb_isolation_read_engines='tikv'")
	tk.MustExec("prepare s from 'select a from cache_t where b=?'")
	tk.MustExec("set @b=10")
	for _, mode := range []string{"OFF", "ON"} {
		tk.MustExec("set tidb_opt_enable_risk_based_plan_selection=" + mode)
		tk.MustQuery("execute s using @b").Check(testkit.Rows("1"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("execute s using @b").Check(testkit.Rows("1"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	}
	// Switching back can reuse only the previously cached OFF plan.
	tk.MustExec("set tidb_opt_enable_risk_based_plan_selection=OFF")
	tk.MustQuery("execute s using @b").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}
