// Copyright 2023 PingCAP, Inc.
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

package sortexec_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestSortSpillDisk(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill"))
	}()
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuota.MemQuotaQuery = 1
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	cas := &testutil.SortCase{Rows: 2048, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}
	opt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(cas.Columns()...),
		Rows:       cas.Rows,
		Ctx:        cas.Ctx,
		Ndvs:       cas.Ndvs,
	}
	dataSource := testutil.BuildMockDataSource(opt)
	exe := &sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(cas.Ctx, dataSource.Schema(), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.OrderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}
	for _, idx := range cas.OrderByIdx {
		exe.ByItems = append(exe.ByItems, &plannerutil.ByItems{Expr: cas.Columns()[idx]})
	}
	tmpCtx := context.Background()
	chk := exec.NewFirstChunk(exe)
	dataSource.PrepareChunks()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition and all data in memory.
	require.Len(t, exe.Unparallel.PartitionList, 1)
	require.Equal(t, false, exe.Unparallel.PartitionList[0].AlreadySpilledSafeForTest())
	require.Equal(t, 2048, exe.Unparallel.PartitionList[0].NumRow())
	err = exe.Close()
	require.NoError(t, err)

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, 1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	dataSource.PrepareChunks()
	err = exe.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test 2 partitions and all data in disk.
	// Now spilling is in parallel.
	// Maybe the second add() will called before spilling, depends on
	// Golang goroutine scheduling. So the result has two possibilities.
	if len(exe.Unparallel.PartitionList) == 2 {
		require.Len(t, exe.Unparallel.PartitionList, 2)
		require.Equal(t, true, exe.Unparallel.PartitionList[0].AlreadySpilledSafeForTest())
		require.Equal(t, true, exe.Unparallel.PartitionList[1].AlreadySpilledSafeForTest())
		require.Equal(t, 1024, exe.Unparallel.PartitionList[0].NumRow())
		require.Equal(t, 1024, exe.Unparallel.PartitionList[1].NumRow())
	} else {
		require.Len(t, exe.Unparallel.PartitionList, 1)
		require.Equal(t, true, exe.Unparallel.PartitionList[0].AlreadySpilledSafeForTest())
		require.Equal(t, 2048, exe.Unparallel.PartitionList[0].NumRow())
	}

	err = exe.Close()
	require.NoError(t, err)

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, 28000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	dataSource.PrepareChunks()
	err = exe.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition but spill disk.
	require.Len(t, exe.Unparallel.PartitionList, 1)
	require.Equal(t, true, exe.Unparallel.PartitionList[0].AlreadySpilledSafeForTest())
	require.Equal(t, 2048, exe.Unparallel.PartitionList[0].NumRow())
	err = exe.Close()
	require.NoError(t, err)

	// Test partition nums.
	ctx = mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, 16864*50)
	ctx.GetSessionVars().MemTracker.Consume(16864 * 45)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	cas = &testutil.SortCase{Rows: 20480, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}
	opt = testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(cas.Columns()...),
		Rows:       cas.Rows,
		Ctx:        cas.Ctx,
		Ndvs:       cas.Ndvs,
	}
	dataSource = testutil.BuildMockDataSource(opt)
	exe = &sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(cas.Ctx, dataSource.Schema(), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.OrderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}
	for _, idx := range cas.OrderByIdx {
		exe.ByItems = append(exe.ByItems, &plannerutil.ByItems{Expr: cas.Columns()[idx]})
	}
	tmpCtx = context.Background()
	chk = exec.NewFirstChunk(exe)
	dataSource.PrepareChunks()
	err = exe.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Don't spill too many partitions.
	require.True(t, len(exe.Unparallel.PartitionList) <= 4)
	err = exe.Close()
	require.NoError(t, err)
}
