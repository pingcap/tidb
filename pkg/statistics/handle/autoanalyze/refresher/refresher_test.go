// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package refresher

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPickOneTableAndAnalyzeByPriority(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (a int, b int, index idx(a)) partition by range (a) (partition p0 values less than (2), partition p1 values less than (4))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")

	handle := dom.StatsHandle()
	sysProcTracker := dom.SysProcTracker()
	r, err := NewRefresher(handle, sysProcTracker)
	require.NoError(t, err)
	// No jobs in the queue.
	r.pickOneTableAndAnalyzeByPriority()
	// The table is not analyzed.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tblStats := handle.GetPartitionStats(tbl.Meta(), pid)
	require.True(t, tblStats.Pseudo)

	// Add a job to the queue.
	job := &priorityqueue.TableAnalysisJob{
		TableID:          tbl.Meta().ID,
		TableSchema:      "test",
		TableName:        "t",
		ChangePercentage: 0.5,
		Weight:           1,
	}
	r.jobs.Push(job)
	r.pickOneTableAndAnalyzeByPriority()
	// The table is analyzed.
	tblStats = handle.GetPartitionStats(tbl.Meta(), pid)
	require.False(t, tblStats.Pseudo)
}
