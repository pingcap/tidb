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

// armEngineRound makes the named round fail loudly if it runs, so a test can
// assert that the round was or was not armed. The failpoint matches on the
// statement text, so callers must run plain SQL: EXPLAIN's inner statement has
// an empty OriginalText and would never match.
func armEngineRound(t *testing.T, round, sql string) {
	require.NoError(t, failpoint.Enable(engineRoundFailpoint, fmt.Sprintf("return(%q)", round+":"+sql)))
	t.Cleanup(func() {
		_ = failpoint.Disable(engineRoundFailpoint)
	})
}

func disarmEngineRound(t *testing.T) {
	require.NoError(t, failpoint.Disable(engineRoundFailpoint))
}

// requireRoundRan asserts the named round was armed for this statement: the
// failpoint aborts planning, so the statement must report the sentinel error.
func requireRoundRan(t *testing.T, tk *testkit.TestKit, round, sql string) {
	armEngineRound(t, round, sql)
	defer disarmEngineRound(t)
	require.ErrorContains(t, tk.ExecToErr(sql), "unexpected alternative logical plan round",
		"expected the %s round to run for: %s", round, sql)
}

// requireRoundSkipped asserts the named round was not armed: with the failpoint
// active the statement still plans and executes normally.
func requireRoundSkipped(t *testing.T, tk *testkit.TestKit, round, sql string, expected [][]any) {
	armEngineRound(t, round, sql)
	defer disarmEngineRound(t)
	tk.MustQuery(sql).Sort().Check(expected)
}

// TestAlternativeEngineRoundArming covers when the tikv-only / tiflash-only
// alternative logical plan rounds arm. Unlike a gate that requires round 1 to
// have mixed engines, these rounds arm from the engines round 1 actually read:
// an all-TiKV plan still gets a whole-statement TiFlash/MPP alternative costed
// against it. The join-or-agg, replica, hint and enforce-mpp gates keep the
// extra rounds off statements that cannot benefit.
func TestAlternativeEngineRoundArming(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlashNodes(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Two small indexed tables with replicas: round 1 reads both from TiKV.
	tk.MustExec("create table alt_small_a(a int primary key, b int)")
	tk.MustExec("create table alt_small_b(a int primary key, b int)")
	// A large table with a replica: round 1 pushes its aggregation to TiFlash.
	tk.MustExec("create table alt_large(a int, b int, c int)")
	// A table with no TiFlash replica at all.
	tk.MustExec("create table alt_no_replica(a int primary key, b int)")

	tk.MustExec("insert into alt_small_a values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	tk.MustExec("insert into alt_small_b values (1, 11), (2, 22), (3, 33), (4, 44), (5, 55)")
	tk.MustExec("insert into alt_no_replica values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	valsLarge := make([]string, 0, 5000)
	for i := range 5000 {
		valsLarge = append(valsLarge, fmt.Sprintf("(%d, %d, %d)", i%50, i, i%10))
	}
	tk.MustExec("insert into alt_large values " + strings.Join(valsLarge, ","))
	tk.MustExec("analyze table alt_small_a, alt_small_b, alt_large, alt_no_replica")

	setRealTiFlashReplica(t, tk, "alt_small_a")
	setRealTiFlashReplica(t, tk, "alt_small_b")
	setRealTiFlashReplica(t, tk, "alt_large")

	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")
	enginesBefore := tk.MustQuery("select @@tidb_isolation_read_engines").Rows()

	// The headline case: round 1 reads only TiKV, so no plan mixing engines ever
	// appears, yet the whole-statement TiFlash/MPP alternative is still uncosted
	// and worth building.
	allTiKVSQL := "select count(*) from alt_small_a join alt_small_b" +
		" on alt_small_a.a = alt_small_b.a"
	tk.MustQuery(allTiKVSQL).Check(testkit.Rows("5"))
	stmtCtx := tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanReadsFromTiKV)
	require.False(t, stmtCtx.AlternativeLogicalPlanReadsFromTiFlash)
	require.True(t, stmtCtx.AlternativeLogicalPlanHasJoinOrAgg)
	requireRoundRan(t, tk, "tiflash-only", allTiKVSQL)
	// The tikv-only round stays disarmed: round 1's plan is already all TiKV, so
	// rebuilding it under a TiKV-only restriction could only repeat that work.
	requireRoundSkipped(t, tk, "tikv-only", allTiKVSQL, testkit.Rows("5"))

	// A missing TiFlash replica anywhere in the statement makes a fully-TiFlash
	// plan impossible, so the tiflash-only round must not run.
	noReplicaSQL := "select count(*) from alt_small_a join alt_no_replica" +
		" on alt_small_a.a = alt_no_replica.a"
	requireRoundSkipped(t, tk, "tiflash-only", noReplicaSQL, testkit.Rows("5"))
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanMissingTiFlashPath)

	// Mirror direction: round 1's aggregation over the large table goes to
	// TiFlash, so the all-TiKV alternative is the uncosted one.
	tiFlashSQL := "select sum(b) from alt_large group by c"
	tiFlashResult := testkit.Rows("1247500", "1248000", "1248500", "1249000", "1249500",
		"1250000", "1250500", "1251000", "1251500", "1252000")
	tk.MustQuery(tiFlashSQL).Sort().Check(tiFlashResult)
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanReadsFromTiFlash)
	requireRoundRan(t, tk, "tikv-only", tiFlashSQL)

	// No join and no aggregation: a wholesale engine switch has nothing to offer,
	// and this gate is what keeps the extra rounds off the OLTP fast path.
	scanOnlySQL := "select b from alt_small_a where b > 30"
	requireRoundSkipped(t, tk, "tiflash-only", scanOnlySQL, testkit.Rows("40", "50"))
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.False(t, stmtCtx.AlternativeLogicalPlanHasJoinOrAgg)

	// An explicit READ_FROM_STORAGE hint pins the engine choice; the rounds must
	// not run, or the cost comparison could override the hint.
	hintSQL := "select /*+ read_from_storage(tikv[alt_small_a, alt_small_b]) */ count(*)" +
		" from alt_small_a join alt_small_b on alt_small_a.a = alt_small_b.a"
	requireRoundSkipped(t, tk, "tiflash-only", hintSQL, testkit.Rows("5"))
	stmtCtx = tk.Session().GetSessionVars().StmtCtx
	require.True(t, stmtCtx.AlternativeLogicalPlanHasStoreTypeHint)

	// Enforced MPP skips the rounds: its cost discount would make the cross-round
	// comparison meaningless.
	tk.MustExec("set @@tidb_enforce_mpp=1")
	requireRoundSkipped(t, tk, "tiflash-only", allTiKVSQL, testkit.Rows("5"))
	tk.MustExec("set @@tidb_enforce_mpp=0")

	// With the feature variable off, no engine round runs.
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=off")
	requireRoundSkipped(t, tk, "tiflash-only", allTiKVSQL, testkit.Rows("5"))
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=on")

	// Let the rounds run to completion (no failpoint): planning and execution
	// must succeed and the session's isolation read engines must be untouched.
	tk.MustQuery(allTiKVSQL).Check(testkit.Rows("5"))
	tk.MustQuery(tiFlashSQL).Sort().Check(tiFlashResult)
	tk.MustQuery("select @@tidb_isolation_read_engines").Check(enginesBefore)
}
