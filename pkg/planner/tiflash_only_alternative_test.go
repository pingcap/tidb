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
	"testing"

	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTiFlashOnlyRoundIntegration(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key, b int, key(b))")
	tk.MustExec("create table no_replica(a int)")
	testkit.SetTiFlashReplica(t, dom, "test", "t")
	tk.MustExec("set tidb_opt_enable_alternative_logical_plans=on")
	tk.MustExec("set tidb_allow_mpp=on, tidb_enforce_mpp=off")
	tk.MustExec("set tidb_isolation_read_engines='tikv,tiflash,tidb'")
	trace := func(sql string) string {
		return tk.MustQuery("trace plan target='debug' " + sql).Rows()[0][0].(string)
	}
	require.Contains(t, trace("select * from t where b=1"), `"Alternative round":"tiflash-only"`)
	tk.MustQuery("select @@tidb_isolation_read_engines").Check(testkit.Rows("tikv,tiflash,tidb"))
	for _, sql := range []string{
		"select /*+ read_from_storage(tikv[t]) */ * from t where b=1",
		"select * from t join no_replica n on t.a=n.a",
	} {
		require.NotContains(t, trace(sql), `"Alternative round":"tiflash-only"`)
	}
	tk.MustExec("set tidb_enforce_mpp=on")
	require.NotContains(t, trace("select * from t"), `"Alternative round":"tiflash-only"`)
	tk.MustExec("set tidb_enforce_mpp=off, tidb_allow_mpp=off")
	require.NotContains(t, trace("select * from t"), `"Alternative round":"tiflash-only"`)
	tk.MustExec("set tidb_allow_mpp=on, tidb_opt_enable_alternative_logical_plans=off")
	require.NotContains(t, trace("select * from t"), `"Alternative round":"tiflash-only"`)
}
