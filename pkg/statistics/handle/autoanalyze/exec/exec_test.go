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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
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
		2, "analyze table %n", "t",
	)

	// Check the result of analyze.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblStats := handle.GetTableStats(tbl.Meta())
	require.Equal(t, int64(3), tblStats.RealtimeCount)
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
	_, _, err := exec.RunAnalyzeStmt(sctx, handle, sysProcTracker, 2, "analyze table %n", "t1")
	require.ErrorContains(t, err, "[executor:1317]Query execution was interrupted")
	close(exitCh)
	wg.Wait()
}
