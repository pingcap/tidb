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

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func buildDataSource(ctx *mock.Context, sortCase *testutil.SortCase) *testutil.MockDataSource {
	opt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(sortCase.Columns()...),
		Rows:       sortCase.Rows,
		Ctx:        sortCase.Ctx,
		Ndvs:       sortCase.Ndvs,
	}
	return testutil.BuildMockDataSource(opt)
}

func buildSortExec(ctx *mock.Context, sortCase *testutil.SortCase, dataSource *testutil.MockDataSource) *sortexec.SortExec {
	dataSource.PrepareChunks()
	exe := &sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(sortCase.Ctx, dataSource.Schema(), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(sortCase.OrderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}

	for _, idx := range sortCase.OrderByIdx {
		exe.ByItems = append(exe.ByItems, &plannerutil.ByItems{Expr: sortCase.Columns()[idx]})
	}

	return exe
}

func executeSortExecutor(t *testing.T, exe *sortexec.SortExec) {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)

	chk := exec.NewFirstChunk(exe)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
}

func onePartitionAndAllDataInMemoryCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase) {
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 1048576)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	dataSource := buildDataSource(ctx, sortCase)
	exe := buildSortExec(ctx, sortCase, dataSource)
	executeSortExecutor(t, exe)

	require.Equal(t, exe.GetSortPartitionListLenForTest(), 1)
	require.Equal(t, false, exe.IsSpillTriggeredInOnePartitionForTest(0))
	require.Equal(t, 2048, exe.GetRowNumInOnePartitionForTest(0))
	err := exe.Close()
	require.NoError(t, err)
}

func onePartitionAndAllDataInDiskCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase) {
	ctx.GetSessionVars().InitChunkSize = 1024
	ctx.GetSessionVars().MaxChunkSize = 1024
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 50000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	dataSource := buildDataSource(ctx, sortCase)
	exe := buildSortExec(ctx, sortCase, dataSource)
	executeSortExecutor(t, exe)

	require.Equal(t, exe.GetSortPartitionListLenForTest(), 1)
	require.Equal(t, true, exe.IsSpillTriggeredInOnePartitionForTest(0))
	require.Equal(t, 2048, exe.GetRowNumInOnePartitionForTest(0))
	err := exe.Close()
	require.NoError(t, err)
}

func multiPartitionCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase) {
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 10000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	dataSource := buildDataSource(ctx, sortCase)
	exe := buildSortExec(ctx, sortCase, dataSource)
	executeSortExecutor(t, exe)

	require.Greater(t, exe.GetSortPartitionListLenForTest(), 1)
	require.Equal(t, true, exe.IsSpillTriggeredInOnePartitionForTest(0))
	err := exe.Close()
	require.NoError(t, err)
}

// TODO validate the correctness
func TestSortSpillDisk(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 2048, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	onePartitionAndAllDataInMemoryCase(t, ctx, sortCase)
	onePartitionAndAllDataInDiskCase(t, ctx, sortCase)
	multiPartitionCase(t, ctx, sortCase)
}

// TODO test sort spill with random fail
