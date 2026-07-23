// Copyright 2026 PingCAP, Inc.
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

package mpp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

const engineRoundFailpoint = "github.com/pingcap/tidb/pkg/planner/failIfAlternativeLogicalPlanRoundTriggered"

// withMockTiFlashNodes adds TiFlash stores to the mock cluster so plans that
// read from TiFlash can actually execute in this test.
func withMockTiFlashNodes(nodes int) mockstore.MockTiKVStoreOption {
	return mockstore.WithMultipleOptions(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockCluster := c.(*unistore.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			for tiflashIdx := range nodes {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
			}
		}),
		mockstore.WithStoreType(mockstore.EmbedUnistore),
	)
}

func setRealTiFlashReplica(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec("alter table " + tableName + " set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", tableName)
	require.NoError(t, domain.GetDomain(tk.Session()).DDLExecutor().
		UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true))
}

// TestAlternativeEngineRestrictedRounds covers the tikv-only / tiflash-only
// alternative logical plan rounds: they must arm only when round 1's plan mixes
// TiKV and TiFlash reads, respect the missing-replica / hint / enforce-mpp
// gates, and leave tidb_isolation_read_engines untouched after planning.
func TestAlternativeEngineRestrictedRounds(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlashNodes(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table alt_engine_flash(a int, b int, c int)")
	tk.MustExec("create table alt_engine_kv(a int primary key, b int)")
	valsFlash := make([]string, 0, 5000)
	for i := range 5000 {
		valsFlash = append(valsFlash, fmt.Sprintf("(%d, %d, %d)", i%50, i, i%10))
	}
	tk.MustExec("insert into alt_engine_flash values " + strings.Join(valsFlash, ","))
	tk.MustExec("insert into alt_engine_kv values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	tk.MustExec("analyze table alt_engine_flash, alt_engine_kv")
	// Only alt_engine_flash gets a TiFlash replica for the first scenarios.
	setRealTiFlashReplica(t, tk, "alt_engine_flash")

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	enginesBefore := tk.MustQuery("select @@tidb_isolation_read_engines").Rows()

	// Each case below disables the failpoint itself so the next case can arm it
	// with a different value. This cleanup only matters when a require assertion
	// aborts the test while the failpoint is still armed; the error is ignored
	// because the failpoint is already disabled on the normal path.
	t.Cleanup(func() {
		_ = failpoint.Disable(engineRoundFailpoint)
	})

	mixedSQL := "select sum(alt_engine_flash.b) from alt_engine_flash join alt_engine_kv" +
		" on alt_engine_flash.a = alt_engine_kv.a group by alt_engine_flash.c"
	// Rows joining a=1..5, 100 rows per key: sum(b) = 247500 + 100*a per group.
	mixedResult := testkit.Rows("247600", "247700", "247800", "247900", "248000")

	// Round 1's plan must mix engines: the aggregation over alt_engine_flash
	// goes to TiFlash while alt_engine_kv (no replica) stays on TiKV. That
	// arms the tikv-only round.
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+mixedSQL)))
	err := tk.ExecToErr(mixedSQL)
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanMixedStorageEngines)
	require.True(t, stmtCtx.AlternativeLogicalPlanMissingTiFlashPath)
	require.ErrorContains(t, err, "unexpected alternative logical plan round")
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// The tiflash-only round must stay disarmed: alt_engine_kv has no TiFlash
	// path, so a fully-TiFlash plan is impossible. The winning plan executes.
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tiflash-only:"+mixedSQL)))
	tk.MustQuery(mixedSQL).Sort().Check(mixedResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// With the feature variable off, no engine round runs.
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+mixedSQL)))
	tk.MustQuery(mixedSQL).Sort().Check(mixedResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")

	// Enforced MPP skips the engine rounds: its cost discount would make the
	// cross-round comparison meaningless.
	tk.MustExec("set @@tidb_enforce_mpp=1")
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+mixedSQL)))
	tk.MustQuery(mixedSQL).Sort().Check(mixedResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))
	tk.MustExec("set @@tidb_enforce_mpp=0")

	// Give alt_engine_kv a replica too: a mixed plan where every table has a
	// TiFlash path arms the tiflash-only round as well. The point read on the
	// primary key keeps alt_engine_kv on TiKV in round 1 while the aggregation
	// side stays on TiFlash.
	setRealTiFlashReplica(t, tk, "alt_engine_kv")
	bothSQL := "select sum(b) from alt_engine_flash group by c" +
		" union all select b from alt_engine_kv where a = 5"
	// Ten groups on c with sum(b) = 1247500 + 500*c, plus the b=5 point row.
	bothResult := testkit.Rows("1247500", "1248000", "1248500", "1249000", "1249500",
		"1250000", "1250500", "1251000", "1251500", "1252000", "5")
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tiflash-only:"+bothSQL)))
	err = tk.ExecToErr(bothSQL)
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanMixedStorageEngines)
	// The tiflash-only trigger below also proves MissingTiFlashPath was false
	// after round 1. The flag itself cannot be asserted here: the tikv-only
	// round runs first and its TiKV-restricted rebuild re-marks it, which is
	// harmless because round eligibility is precomputed from round 1's signals.
	require.ErrorContains(t, err, "unexpected alternative logical plan round")
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// An explicit READ_FROM_STORAGE hint pins the engine choice; the rounds
	// must not run, or the cost comparison could override the hint.
	hintSQL := "select /*+ read_from_storage(tiflash[alt_engine_flash], tikv[alt_engine_kv]) */" +
		" sum(alt_engine_flash.b) from alt_engine_flash join alt_engine_kv" +
		" on alt_engine_flash.a = alt_engine_kv.a group by alt_engine_flash.c"
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+hintSQL)))
	tk.MustQuery(hintSQL).Sort().Check(mixedResult)
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanHasStoreTypeHint)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// Let the rounds actually run to completion (no failpoint): planning and
	// execution must succeed and the session's isolation read engines must be
	// untouched.
	tk.MustQuery(mixedSQL).Sort().Check(mixedResult)
	tk.MustQuery(bothSQL).Sort().Check(bothResult)
	tk.MustQuery("select @@tidb_isolation_read_engines").Check(enginesBefore)
}
