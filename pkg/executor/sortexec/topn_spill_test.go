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

package sortexec_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func checkTopNCorrectness(schema *expression.Schema, exe *sortexec.TopNExec, dataSource *testutil.MockDataSource, resultChunks []*chunk.Chunk, offset uint64, count uint64) bool {
	keyColumns, keyCmpFuncs, byItemsDesc := exe.GetSortMetaForTest()
	checker := newResultChecker(schema, keyColumns, keyCmpFuncs, byItemsDesc, dataSource.GenData)
	return checker.check(resultChunks, true, offset, count)
}

func buildTopNExec(sortCase *testutil.SortCase, dataSource *testutil.MockDataSource, offset uint64, count uint64) *sortexec.TopNExec {
	dataSource.PrepareChunks()
	sortExec := sortexec.SortExec{
		BaseExecutor: exec.NewBaseExecutor(sortCase.Ctx, dataSource.Schema(), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(sortCase.OrderByIdx)),
		ExecSchema:   dataSource.Schema(),
	}

	for _, idx := range sortCase.OrderByIdx {
		sortExec.ByItems = append(sortExec.ByItems, &plannerutil.ByItems{Expr: sortCase.Columns()[idx]})
	}

	topNexec := &sortexec.TopNExec{
		SortExec: sortExec,
		Limit:    &plannercore.PhysicalLimit{Offset: offset, Count: count},
	}

	return topNexec
}

func executeTopNExecutor(t *testing.T, exe *sortexec.TopNExec) []*chunk.Chunk {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)

	resultChunks := make([]*chunk.Chunk, 0)
	chk := exec.NewFirstChunk(exe)
	for {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
		resultChunks = append(resultChunks, chk.CopyConstruct())
	}
	return resultChunks
}

// No spill will be triggered in this test
func topNNoSpillCase(t *testing.T, exe *sortexec.TopNExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	if exe == nil {
		exe = buildTopNExec(sortCase, dataSource, offset, count)
	}
	dataSource.PrepareChunks()
	resultChunks := executeTopNExecutor(t, exe)

	require.False(t, exe.IsSpillTriggeredForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

// Topn executor has two stage:
//  1. Building heap, in this stage all received rows will be inserted into heap.
//  2. Updating heap, in this stage only rows that is smaller than the heap top could be inserted and we will drop the heap top.
//
// Case1 means that we will trigger spill in stage 1
func topNSpillCase1(t *testing.T, exe *sortexec.TopNExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	if exe == nil {
		exe = buildTopNExec(sortCase, dataSource, offset, count)
	}
	dataSource.PrepareChunks()
	resultChunks := executeTopNExecutor(t, exe)

	require.True(t, exe.IsSpillTriggeredForTest())
	require.True(t, exe.GetIsInStage1ForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

// Case2 means that we will trigger spill in stage 2
func topNSpillCase2(t *testing.T, exe *sortexec.TopNExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	if exe == nil {
		exe = buildTopNExec(sortCase, dataSource, offset, count)
	}
	dataSource.PrepareChunks()
	resultChunks := executeTopNExecutor(t, exe)

	require.True(t, exe.IsSpillTriggeredForTest())
	require.True(t, exe.GetIsInStage1ForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

// After sorted all rows are in memory and the spill is triggered after some chunks have been fetched
func topNInMemoryThenSpillCase(t *testing.T, exe *sortexec.TopNExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	if exe == nil {
		exe = buildTopNExec(sortCase, dataSource, offset, count)
	}
	dataSource.PrepareChunks()
	resultChunks := executeTopNExecutor(t, exe)

	require.True(t, exe.IsSpillTriggeredForTest())
	require.True(t, exe.GetIsInStage1ForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

func TestTopNSpillDisk(t *testing.T) {
	totalRowNum := 10000
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	topNCase := &testutil.SortCase{Rows: totalRowNum, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	offset := uint64(totalRowNum / 10)
	count := uint64(totalRowNum / 3)

	schema := expression.NewSchema(topNCase.Columns()...)
	dataSource := buildDataSource(ctx, topNCase, schema)
	exe := buildTopNExec(topNCase, dataSource, offset, count)
	// for i := 0; i < 5; i++ {
	// 	topNNoSpillCase(t, nil, topNCase, schema, dataSource, 0, count)
	// 	topNNoSpillCase(t, exe, topNCase, schema, dataSource, offset, count)
	// }

	// TODO add slow random fail point for topn failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	count = uint64(totalRowNum - totalRowNum/10)
	exe = buildTopNExec(topNCase, dataSource, offset, count)
	for i := 0; i < 5; i++ {
		// topNSpillCase1(t, nil, topNCase, schema, dataSource, 0, count)
		topNSpillCase1(t, exe, topNCase, schema, dataSource, offset, count)
	}
}

func TestTopNSpillDiskFailpoint(t *testing.T) {
	// TODO spills in stage 1 and stage 2 are all need to be tested
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 10000, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail", `return(true)`)
	failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError", `return(false)`)

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	ctx.GetSessionVars().EnableParallelSort = true

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(ctx, sortCase, schema)
	exe := buildSortExec(ctx, sortCase, dataSource)
	for i := 0; i < 20; i++ {
		failpointNoMemoryDataTest(t, ctx, nil, sortCase, schema, dataSource)
		failpointNoMemoryDataTest(t, ctx, exe, sortCase, schema, dataSource)
	}

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	for i := 0; i < 20; i++ {
		failpointDataInMemoryThenSpillTest(t, ctx, nil, sortCase, schema, dataSource)
		failpointDataInMemoryThenSpillTest(t, ctx, exe, sortCase, schema, dataSource)
	}
}
