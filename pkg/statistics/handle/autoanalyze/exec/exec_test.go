// Copyright 2024 PingCAP, Inc.
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

package exec_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestExecAutoAnalyzes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()

	exec.AutoAnalyze(
		sctx,
		handle,
		dom.SysProcTracker(),
		2, false, "analyze table %n", "t",
	)

	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.Equal(t, int64(3), tblStats.RealtimeCount)
}

func TestExecAutoAnalyzeRewritesLegacyStatsVersionToV2(t *testing.T) {
	core, recorded := observer.New(zap.WarnLevel)
	logger := zap.New(core)
	restore := log.ReplaceGlobals(logger, &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
	defer restore()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")

	se := tk.Session()
	sctx := se.(sessionctx.Context)
	handle := dom.StatsHandle()

	require.NotPanics(t, func() {
		ok := exec.AutoAnalyze(
			sctx,
			handle,
			dom.SysProcTracker(),
			statistics.Version2,
			true,
			"analyze table %n",
			"t",
		)
		require.True(t, ok)
	})

	warnLogs := recorded.FilterMessage("auto analyze rewrites legacy statistics version 1 to version 2").All()
	require.Len(t, warnLogs, 1)
	require.Equal(t, "analyze table `t`", warnLogs[0].ContextMap()["sql"])

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.Equal(t, statistics.Version2, tblStats.StatsVer)

	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec(`create table pt (a int, b int, index idx(a))
partition by range (a) (
	partition p0 values less than (10),
	partition p1 values less than (20)
)`)
	tk.MustExec("insert into pt values (1, 1), (2, 2), (3, 3), (11, 11), (12, 12)")
	tk.MustExec("analyze table pt")

	is = dom.InfoSchema()
	partitionedTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("pt"))
	require.NoError(t, err)
	pi := partitionedTbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	legacyTableIDs := []int64{partitionedTbl.Meta().ID, pi.Definitions[0].ID, pi.Definitions[1].ID}
	tk.MustExec(
		"update mysql.stats_histograms set stats_ver = 1 where table_id in (?,?,?)",
		legacyTableIDs[0], legacyTableIDs[1], legacyTableIDs[2],
	)
	handle.Clear()
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema(), legacyTableIDs...))
	require.Equal(t, statistics.Version1, handle.GetPhysicalTableStats(partitionedTbl.Meta().ID, partitionedTbl.Meta()).StatsVer)
	require.Equal(t, statistics.Version1, handle.GetPhysicalTableStats(pi.Definitions[0].ID, partitionedTbl.Meta()).StatsVer)
	require.Equal(t, statistics.Version1, handle.GetPhysicalTableStats(pi.Definitions[1].ID, partitionedTbl.Meta()).StatsVer)

	require.NotPanics(t, func() {
		ok := exec.AutoAnalyze(
			sctx,
			handle,
			dom.SysProcTracker(),
			statistics.Version2,
			true,
			"analyze table %n partition %n",
			"pt",
			"p0",
		)
		require.True(t, ok)
	})

	warnLogs = recorded.FilterMessage("auto analyze rewrites legacy statistics version 1 to version 2").All()
	require.Len(t, warnLogs, 2)
	require.Equal(t, "analyze table `pt` partition `p0`", warnLogs[1].ContextMap()["sql"])
	tk.MustQuery(
		"select table_id, stats_ver from mysql.stats_histograms where table_id in (?,?,?) group by table_id, stats_ver order by table_id",
		legacyTableIDs[0], legacyTableIDs[1], legacyTableIDs[2],
	).Check(testkit.Rows(
		fmt.Sprintf("%d 2", legacyTableIDs[0]),
		fmt.Sprintf("%d 2", legacyTableIDs[1]),
		fmt.Sprintf("%d 2", legacyTableIDs[2]),
	))
}

func TestKillInWindows(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (14))")
	tk.MustExec("insert into t1 values (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13)")
	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	now := time.Now()
	startTime := now.Add(1 * time.Hour).Format("15:04 -0700")
	endTime := now.Add(2 * time.Hour).Format("15:04 -0700")
	tk.MustExec(fmt.Sprintf("SET GLOBAL tidb_auto_analyze_start_time='%s'", startTime))
	tk.MustExec(fmt.Sprintf("SET GLOBAL tidb_auto_analyze_end_time='%s'", endTime))
	var wg util.WaitGroupWrapper
	exitCh := make(chan struct{})
	wg.Run(func() {
		for {
			select {
			case <-exitCh:
				return
			default:
				dom.CheckAutoAnalyzeWindows()
			}
		}
	})
	sctx := tk.Session()
	_, _, err := exec.RunAnalyzeStmt(sctx, handle, sysProcTracker, 2, false, "analyze table %n", "t1")
	require.ErrorContains(t, err, "[executor:1317]Query execution was interrupted")
	close(exitCh)
	wg.Wait()
}
