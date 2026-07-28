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
	"github.com/pingcap/tidb/pkg/testkit/testdata"
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

// createEngineRoundTables builds the fixtures shared by the engine-round
// tests. alt_engine_flash has no index on the join column, so its only TiKV
// path is a full scan and physical optimization favors its TiFlash replica.
// alt_engine_kv joins through its non-indexed column b while the tests filter
// on its primary key a: a point predicate on a makes unique-point-range
// pruning keep only the TiKV point path, hiding the table's TiFlash path (and
// with it any fully-TiFlash plan) from round 1, which then settles on a
// mixed-engine plan. alt_engine_norep never gets a TiFlash replica.
func createEngineRoundTables(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("create table alt_engine_flash(a int, b int, c int)")
	valsFlash := make([]string, 0, 5000)
	for i := range 5000 {
		valsFlash = append(valsFlash, fmt.Sprintf("(%d, %d, %d)", i%50, i, i%10))
	}
	tk.MustExec("insert into alt_engine_flash values " + strings.Join(valsFlash, ","))
	tk.MustExec("create table alt_engine_kv(a int primary key, b int)")
	valsKV := make([]string, 0, 2000)
	for i := range 2000 {
		valsKV = append(valsKV, fmt.Sprintf("(%d, %d)", i, i%50))
	}
	tk.MustExec("insert into alt_engine_kv values " + strings.Join(valsKV, ","))
	tk.MustExec("create table alt_engine_norep(a int primary key, b int)")
	tk.MustExec("insert into alt_engine_norep values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	tk.MustExec("analyze table alt_engine_flash, alt_engine_kv, alt_engine_norep")
	setRealTiFlashReplica(t, tk, "alt_engine_flash")
	setRealTiFlashReplica(t, tk, "alt_engine_kv")
}

// engineRoundPointSQL mixes engines in round 1 (Point_Get on TiKV joined at
// root with a TiFlash read) while the tiflash-only round finds a cheaper
// fully-pushed MPP plan; see createEngineRoundTables for why round 1 cannot
// see that plan itself.
const engineRoundPointSQL = "select sum(alt_engine_flash.b) from alt_engine_flash join alt_engine_kv" +
	" on alt_engine_flash.a = alt_engine_kv.b where alt_engine_kv.a = 5 group by alt_engine_flash.c"

// TestAlternativeEngineRestrictedRounds compares plan shapes with the
// tikv-only / tiflash-only alternative rounds off and on: the mixed-engine
// round-1 plan must be replaced by the cheaper fully-TiFlash plan when the
// rounds run, must survive unchanged under a READ_FROM_STORAGE hint or a
// missing TiFlash replica, and the winning plan must execute correctly while
// leaving tidb_isolation_read_engines untouched.
func TestAlternativeEngineRestrictedRounds(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlashNodes(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createEngineRoundTables(t, tk)
	enginesBefore := tk.MustQuery("select @@tidb_isolation_read_engines").Rows()

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		if strings.HasPrefix(tt, "set ") {
			tk.MustExec(tt)
			testdata.OnRecord(func() {
				output[i].SQL = tt
			})
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}

	// The last testdata case leaves the alternative rounds enabled, so the
	// queries below execute the plans the rounds picked. Joining rows have
	// alt_engine_flash.a = 5, i.e. b = 5+50k for k in 0..99, all in group
	// c = 5: sum(b) = 500 + 50*4950 = 248000.
	tk.MustQuery(engineRoundPointSQL).Check(testkit.Rows("248000"))
	// Rows joining a=1..5, 100 rows per key: sum(b) = 247500 + 100*a per group.
	tk.MustQuery("select sum(alt_engine_flash.b) from alt_engine_flash join alt_engine_norep"+
		" on alt_engine_flash.a = alt_engine_norep.a group by alt_engine_flash.c").Sort().
		Check(testkit.Rows("247600", "247700", "247800", "247900", "248000"))
	tk.MustQuery("select @@tidb_isolation_read_engines").Check(enginesBefore)
}

// TestAlternativeEngineRestrictedRoundGates covers what plan shapes cannot
// show: whether a round ran at all. The failpoint errors out planning when the
// named round is attempted for the given statement, so a successful query
// proves the round stayed disarmed and an error proves it was armed.
func TestAlternativeEngineRestrictedRoundGates(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlashNodes(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table alt_engine_flash(a int, b int, c int)")
	valsFlash := make([]string, 0, 5000)
	for i := range 5000 {
		valsFlash = append(valsFlash, fmt.Sprintf("(%d, %d, %d)", i%50, i, i%10))
	}
	tk.MustExec("insert into alt_engine_flash values " + strings.Join(valsFlash, ","))
	tk.MustExec("create table alt_engine_kv(a int primary key, b int)")
	valsKV := make([]string, 0, 2000)
	for i := range 2000 {
		valsKV = append(valsKV, fmt.Sprintf("(%d, %d)", i, i%50))
	}
	tk.MustExec("insert into alt_engine_kv values " + strings.Join(valsKV, ","))
	tk.MustExec("analyze table alt_engine_flash, alt_engine_kv")
	// Only alt_engine_flash gets a TiFlash replica at first, so the
	// missing-replica gate can be exercised before alt_engine_kv's replica is
	// added below.
	setRealTiFlashReplica(t, tk, "alt_engine_flash")

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	// See TestAlternativeEngineRestrictedRounds for the result derivation.
	pointResult := testkit.Rows("248000")

	// Each case below disables the failpoint itself so the next case can arm it
	// with a different value. This cleanup only matters when a require assertion
	// aborts the test while the failpoint is still armed; the error is ignored
	// because the failpoint is already disabled on the normal path.
	t.Cleanup(func() {
		_ = failpoint.Disable(engineRoundFailpoint)
	})

	// Round 1's plan mixes engines (Point_Get on TiKV, TiFlash read for
	// alt_engine_flash), so the tikv-only round must be armed.
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+engineRoundPointSQL)))
	require.ErrorContains(t, tk.ExecToErr(engineRoundPointSQL), "unexpected alternative logical plan round")
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// The tiflash-only round must stay disarmed: alt_engine_kv has no TiFlash
	// replica yet, so a fully-TiFlash plan is impossible. The winning plan
	// executes.
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tiflash-only:"+engineRoundPointSQL)))
	tk.MustQuery(engineRoundPointSQL).Check(pointResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// With the feature variable off, no engine round runs.
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+engineRoundPointSQL)))
	tk.MustQuery(engineRoundPointSQL).Check(pointResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")

	// Enforced MPP skips the engine rounds: its cost discount would make the
	// cross-round comparison meaningless.
	tk.MustExec("set @@tidb_enforce_mpp=1")
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+engineRoundPointSQL)))
	tk.MustQuery(engineRoundPointSQL).Check(pointResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))
	tk.MustExec("set @@tidb_enforce_mpp=0")

	// An explicit READ_FROM_STORAGE hint pins the engine choice; the rounds
	// must not run, or the cost comparison could override the hint.
	hintSQL := "select /*+ read_from_storage(tiflash[alt_engine_flash], tikv[alt_engine_kv]) */" +
		" sum(alt_engine_flash.b) from alt_engine_flash join alt_engine_kv" +
		" on alt_engine_flash.a = alt_engine_kv.b where alt_engine_kv.a = 5 group by alt_engine_flash.c"
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tikv-only:"+hintSQL)))
	tk.MustQuery(hintSQL).Check(pointResult)
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))

	// Once alt_engine_kv has a TiFlash replica too, every table has a TiFlash
	// path and the tiflash-only round must be armed as well: the point read on
	// the primary key keeps alt_engine_kv on TiKV in round 1, so the plan
	// still mixes engines.
	setRealTiFlashReplica(t, tk, "alt_engine_kv")
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", "tiflash-only:"+engineRoundPointSQL)))
	require.ErrorContains(t, tk.ExecToErr(engineRoundPointSQL), "unexpected alternative logical plan round")
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))
}
