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
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestMPPVirtualTextFilter(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table objects(id int primary key, label varchar(32), txt text, txt_lower text as (lower(txt)) virtual) collate utf8mb4_bin")
	tk.MustExec("create table edges(obj_id int)")
	testkit.SetTiFlashReplica(t, dom, "test", "objects")
	testkit.SetTiFlashReplica(t, dom, "test", "edges")
	tk.MustExec("set tidb_allow_mpp=on, tidb_allow_tiflash_cop=off, tidb_enforce_mpp=off")
	tk.MustExec("set tidb_isolation_read_engines='tiflash,tidb', tidb_opt_enable_alternative_logical_plans=on")
	const query = "select o.id, o.label from objects o where o.txt_lower = 'fagor' and exists " +
		"(select 1 from edges d where d.obj_id = o.id) order by o.label limit 1000"
	explain := func(query string) string {
		return fmt.Sprint(tk.MustQuery("explain format='brief' " + query).Rows())
	}
	require.Contains(t, explain(query), "Apply")

	tk.MustExec("set mpp_version=3")
	plan := explain(query)
	require.Regexp(t, `HashJoin[^]]*mpp\[tiflash\]`, plan)
	require.Regexp(t, `Selection[^]]*mpp\[tiflash\][^]]*cast[(]lower`, plan)
	require.NotContains(t, plan, "Apply")
	projected := explain("select txt_lower from objects where txt_lower = 'fagor'")
	require.Regexp(t, `Selection[^]]*root[^]]*txt_lower`, projected)
	require.NotContains(t, projected, "cast(lower")

	tk.MustExec("insert into mysql.opt_rule_blacklist values ('column_prune')")
	tk.MustExec("admin reload opt_rule_blacklist")
	t.Cleanup(func() {
		tk.MustExec("delete from mysql.opt_rule_blacklist where name = 'column_prune'")
		tk.MustExec("admin reload opt_rule_blacklist")
	})
	withoutPruning := explain(strings.Replace(query, "o.id, o.label", "o.txt_lower, o.label", 1))
	require.Regexp(t, `Selection[^]]*root[^]]*txt_lower`, withoutPruning)
	require.NotContains(t, withoutPruning, "cast(lower")
}

func TestMPPVirtualTextFilterWithCommonHandle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table obj_new(
		workspace_id varchar(255) not null, id binary(16) not null,
		obj_type_id binary(16) not null, sequential_id bigint, label varchar(255),
		text_value_7 text, text_value_7_lower text as (lower(text_value_7)) virtual,
		primary key (workspace_id, id) clustered,
		key ix_obj_type_lower (workspace_id(36), obj_type_id, text_value_7_lower(500))
	) collate utf8mb4_bin`)
	tk.MustExec(`create table obj_relationship_new(
		workspace_id varchar(255) not null, id binary(16) not null,
		object_id binary(16) not null, object_type_attribute_id binary(16) not null,
		primary key (workspace_id, id) clustered
	) collate utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "test", "obj_new")
	testkit.SetTiFlashReplica(t, dom, "test", "obj_relationship_new")
	tk.MustExec("set tidb_allow_mpp=on, tidb_allow_tiflash_cop=off, tidb_enforce_mpp=off")
	tk.MustExec("set tidb_isolation_read_engines='tiflash,tidb', tidb_opt_enable_alternative_logical_plans=on")
	const query = `explain format='brief'
		select hex(o.id), o.sequential_id, o.label from obj_new o
		where o.workspace_id = 'workspace' and o.obj_type_id = x'00000000000000000000000000000001'
		and o.text_value_7_lower = 'fagor'
		and exists (
			select 1 from obj_relationship_new d
			where d.object_id = o.id and d.object_type_attribute_id = x'00000000000000000000000000000002'
		)
		order by o.label, o.id limit 1000`

	tk.MustExec("set mpp_version=2")
	require.NotContains(t, fmt.Sprint(tk.MustQuery(query).Rows()), "cast(lower")

	tk.MustExec("set mpp_version=3")
	plan := fmt.Sprint(tk.MustQuery(query).Rows())
	require.Regexp(t, `Selection[^]]*mpp\[tiflash\][^]]*cast[(]lower`, plan)
	require.Regexp(t, `HashJoin[^]]*mpp\[tiflash\][^]]*semi join`, plan)
	require.NotContains(t, plan, "Apply")
}

func TestMPPVirtualIntegerFilter(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(
		id int primary key, a bigint, b bigint as (abs(a)) virtual,
		alias_col bigint as (a) virtual, narrow tinyint as (abs(a)) virtual
	)`)
	tk.MustExec("insert into t (id, a) values (1, -2), (2, null), (3, 4)")
	testkit.SetTiFlashReplica(t, dom, "test", "t")
	tk.MustExec("set tidb_allow_mpp=on, tidb_allow_tiflash_cop=off, tidb_enforce_mpp=on")
	tk.MustExec("set tidb_isolation_read_engines='tiflash,tidb', mpp_version=2")

	const query = "select id from t where b = 2"
	plan := fmt.Sprint(tk.MustQuery("explain format='brief' " + query).Rows())
	require.Regexp(t, `Selection[^]]*mpp\[tiflash\][^]]*abs\(`, plan)
	tk.MustQuery(query).Check(testkit.Rows("1"))
	tk.MustQuery("select id from t where b is null").Check(testkit.Rows("2"))
	alias := fmt.Sprint(tk.MustQuery("explain format='brief' select id from t where alias_col = -2").Rows())
	require.Regexp(t, `mpp\[tiflash\][^]]*eq\(test\.t\.a, -2\)`, alias)

	narrow := fmt.Sprint(tk.MustQuery("explain format='brief' select id from t where narrow = 2").Rows())
	require.NotRegexp(t, `Selection[^]]*mpp\[tiflash\][^]]*abs\(`, narrow)
	projected := fmt.Sprint(tk.MustQuery("explain format='brief' select b from t where b = 2").Rows())
	require.NotRegexp(t, `Selection[^]]*mpp\[tiflash\][^]]*abs\(`, projected)
}
