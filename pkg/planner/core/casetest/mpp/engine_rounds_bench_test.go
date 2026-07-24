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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// setBenchTiFlashReplica mirrors setRealTiFlashReplica but accepts testing.TB
// so it can be used from a benchmark. It is kept local to avoid changing the
// shared *testing.T helper signature.
func setBenchTiFlashReplica(b *testing.B, tk *testkit.TestKit, tableName string) {
	tk.MustExec("alter table " + tableName + " set tiflash replica 1")
	dom := domain.GetDomain(tk.Session())
	require.NoError(b, dom.Reload())
	tb, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tableName))
	require.NoError(b, err)
	require.NoError(b, dom.DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true))
}

// BenchmarkEngineRoundOptimize measures the per-statement optimization time of
// the alternative logical plan driver. Running it on both candidate branches
// shows the compile-time cost of each design's arming strategy: the same query
// is optimized with the feature off (baseline) and on.
//
// The two designs differ only on all-TiKV plans. This branch arms the
// tiflash-only round whenever round 1 read from TiKV and every table has a
// replica; the mixed-engine-only design (#70005) never arms there, so its "on"
// time tracks the baseline. The benchmark focuses on that differentiator with
// an all-TiKV join, plus a scan-only fast-path baseline that no design arms.
//
// Cases whose armed round builds an MPP plan (round-1 TiFlash reads, or an
// aggregation whose tiflash-only round shuffles) are intentionally omitted:
// they either arm identically in both designs (so they do not differentiate
// them) or probe the mock TiFlash store's liveness, which is not reliably
// routable when the planner runs in isolation in a unit test.
func BenchmarkEngineRoundOptimize(b *testing.B) {
	store := testkit.CreateMockStore(b, withMockTiFlashNodes(2))
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")

	tk.MustExec("create table bench_small_a(a int primary key, b int)")
	tk.MustExec("create table bench_small_b(a int primary key, b int)")

	tk.MustExec("insert into bench_small_a values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	tk.MustExec("insert into bench_small_b values (1, 11), (2, 22), (3, 33), (4, 44), (5, 55)")
	tk.MustExec("analyze table bench_small_a, bench_small_b")

	setBenchTiFlashReplica(b, tk, "bench_small_a")
	setBenchTiFlashReplica(b, tk, "bench_small_b")

	queries := []struct {
		name string
		sql  string
	}{
		{
			// Round 1 reads both small tables from TiKV. This branch arms the
			// tiflash-only round here; a mixed-engine-only design does not.
			name: "oltp-indexed-join",
			sql: "select count(*) from bench_small_a join bench_small_b" +
				" on bench_small_a.a = bench_small_b.a",
		},
		{
			// No join or aggregation: the join/agg gate keeps every design off
			// the rounds, so this measures the untouched fast path.
			name: "scan-only",
			sql:  "select b from bench_small_a where a = 3",
		},
	}

	for _, q := range queries {
		ctx := tk.Session()
		stmts, err := session.Parse(ctx, q.sql)
		require.NoError(b, err)
		require.Len(b, stmts, 1)
		ret := &core.PreprocessorReturn{}
		nodeW := resolve.NewNodeW(stmts[0])
		require.NoError(b, core.Preprocess(context.Background(), ctx, nodeW, core.WithPreprocessorReturn(ret)))

		for _, feature := range []string{"off", "on"} {
			tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans=" + feature)
			b.Run(q.name+"/"+feature, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_, _, err := planner.Optimize(context.TODO(), ctx, nodeW, ret.InfoSchema)
					require.NoError(b, err)
				}
			})
		}
	}
}
