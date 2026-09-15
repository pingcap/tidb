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

package tiflashtest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

// TestHashJoinEmptyBuildMPPProbeEarlyCloseShape reproduces the plan shape of the
// production issue (TCOC-5973 style):
//
//	HashJoin (LEFT OUTER) @ root
//	  ├─ IndexLookUp / Selection on TiKV  (build, actRows=0 after filter)
//	  └─ TableReader -> ExchangeSender    (probe, MPP on TiFlash)
//
// HashJoinV2 canSkipProbeIfHashTableIsEmpty still performs one probe Next() before
// skipping. That early close can drop TiFlash's trailing execution-summary packet.
//
// Note: unistore MPP currently does not emit ExecutorId summary packets like real
// TiFlash, so this test asserts the plan/runtime shape (empty build + probe Next
// happened) rather than EXPLAIN ANALYZE summary text. The packet-loss mechanism is
// covered by distsql.TestMPPExecutionSummaryLostOnEarlyClose.
func TestHashJoinEmptyBuildMPPProbeEarlyCloseShape(t *testing.T) {
	store := testkit.CreateMockStore(t, withMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_build, t_probe")
	tk.MustExec(`create table t_build (
		id int primary key,
		customer_id int,
		link_id int,
		link_type int,
		index idx_customer(customer_id)
	)`)
	tk.MustExec(`create table t_probe (
		id int,
		mid int,
		index idx_mid(mid)
	)`)
	tk.MustExec("alter table t_probe set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t_probe")
	require.NoError(t, domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true))

	// Build-side rows exist on the index path, but Selection on link_id filters all out.
	tk.MustExec("insert into t_build values (1,1,10,1),(2,1,20,1),(3,1,30,1)")
	vals := make([]string, 0, 200)
	for i := range 200 {
		vals = append(vals, fmt.Sprintf("(%d,%d)", i, i%5))
	}
	tk.MustExec("insert into t_probe values " + strings.Join(vals, ","))
	tk.MustExec("analyze table t_build, t_probe")

	tk.MustExec("set @@session.tidb_allow_mpp=1")
	tk.MustExec("set @@session.tidb_enforce_mpp=0")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tikv,tiflash'")
	tk.MustExec("set @@session.tidb_opt_enable_late_materialization=0")
	tk.MustExec("set @@session.tidb_hash_join_version=" + joinversion.HashJoinVersionOptimized)
	// Prefer MPP aggregation over TiKV index stream-agg for the probe side.
	tk.MustExec("set @@session.tidb_opt_prefer_range_scan=0")

	// Correlated-count style LOJ: left TiKV side filtered empty; right TiFlash MPP agg.
	// Put read_from_storage hint inside the subquery so it attaches to t_probe.
	sql := `select /*+ HASH_JOIN(b, c) */
		b.id, c.cnt
	from t_build b
	left join (
		select /*+ read_from_storage(tiflash[t_probe]), mpp_1phase_agg() */
			mid, count(*) as cnt
		from t_probe
		group by mid
	) c on b.id = c.mid
	where b.customer_id = 1 and b.link_id = 999`

	tk.MustQuery(sql).Check(testkit.Rows())

	rows := tk.MustQuery("explain analyze " + sql).Rows()
	plan := formatExplainRows(rows)
	t.Logf("explain analyze plan:\n%s", plan)

	require.Contains(t, plan, "HashJoin", "should use HashJoin at root")
	require.Contains(t, plan, "mpp[tiflash]", "probe side should be MPP on TiFlash")
	require.Contains(t, plan, "ExchangeSender", "probe side should include ExchangeSender")

	// Build side filtered to empty (same as IndexLookUp+Selection -> 0 in the incident).
	require.True(t, hasOperatorWithActRows(rows, "Selection", 0) || hasOperatorWithActRows(rows, "TableReader", 0),
		"build/filter side should show actRows=0, plan:\n%s", plan)

	// Probe TableReader should have done at least one Next (actRows>0) even though
	// join result is empty — this is the early-close prerequisite for summary loss.
	require.True(t, hasMPPTableReaderWithPositiveActRows(rows),
		"probe TableReader reading MPP should have actRows>0 after one Next before skipProbe, plan:\n%s", plan)
}

func formatExplainRows(rows [][]any) string {
	var b strings.Builder
	for _, row := range rows {
		parts := make([]string, 0, len(row))
		for _, col := range row {
			parts = append(parts, fmt.Sprint(col))
		}
		b.WriteString(strings.Join(parts, " | "))
		b.WriteByte('\n')
	}
	return b.String()
}

func hasOperatorWithActRows(rows [][]any, opSubstring string, wantActRows int64) bool {
	for _, row := range rows {
		if len(row) < 5 {
			continue
		}
		id := fmt.Sprint(row[0])
		if !strings.Contains(id, opSubstring) {
			continue
		}
		var actRows int64
		_, err := fmt.Sscan(fmt.Sprint(row[2]), &actRows)
		if err != nil {
			continue
		}
		if actRows == wantActRows {
			return true
		}
	}
	return false
}

func hasMPPTableReaderWithPositiveActRows(rows [][]any) bool {
	for i, row := range rows {
		if len(row) < 5 {
			continue
		}
		id := fmt.Sprint(row[0])
		task := fmt.Sprint(row[3])
		if !strings.Contains(id, "TableReader") || task != "root" {
			continue
		}
		var actRows int64
		if _, err := fmt.Sscan(fmt.Sprint(row[2]), &actRows); err != nil || actRows <= 0 {
			continue
		}
		// Child line should be mpp ExchangeSender / similar.
		if i+1 < len(rows) {
			child := fmt.Sprint(rows[i+1][0])
			childTask := ""
			if len(rows[i+1]) > 3 {
				childTask = fmt.Sprint(rows[i+1][3])
			}
			if strings.Contains(child, "ExchangeSender") || strings.Contains(childTask, "mpp") {
				return true
			}
		}
	}
	return false
}
