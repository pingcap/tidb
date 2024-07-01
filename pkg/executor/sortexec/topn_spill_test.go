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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var totalRowNum = 10000
var noSpillCaseHardLimit = hardLimit2
var spillCase1HardLimit = hardLimit1
var spillCase2HardLimit = hardLimit1
var inMemoryThenSpillHardLimit = hardLimit1 * 2

// Test is successful if there is no hang
func executeTopNInFailpoint(t *testing.T, exe *sortexec.TopNExec, hardLimit int64, tracker *memory.Tracker) {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)

	goRoutineWaiter := sync.WaitGroup{}
	goRoutineWaiter.Add(1)
	defer goRoutineWaiter.Wait()

	once := sync.Once{}

	go func() {
		time.Sleep(time.Duration(rand.Int31n(300)) * time.Millisecond)
		once.Do(func() {
			exe.Close()
		})
		goRoutineWaiter.Done()
	}()

	chk := exec.NewFirstChunk(exe)
	for i := 0; i >= 0; i++ {
		err := exe.Next(tmpCtx, chk)
		if err != nil {
			once.Do(func() {
				err = exe.Close()
				require.Equal(t, nil, err)
			})
			break
		}
		if chk.NumRows() == 0 {
			break
		}

		if i == 10 && hardLimit > 0 {
			// Trigger the spill
			tracker.Consume(hardLimit)
			tracker.Consume(-hardLimit)
		}
	}
	once.Do(func() {
		err = exe.Close()
		require.Equal(t, nil, err)
	})
}

func initTopNNoSpillCaseParams(
	ctx *mock.Context,
	dataSource *testutil.MockDataSource,
	topNCase *testutil.SortCase,
	totalRowNum int,
	count *uint64,
	offset *uint64,
	exe **sortexec.TopNExec,
) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, noSpillCaseHardLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	*count = uint64(totalRowNum / 3)
	*offset = uint64(totalRowNum / 10)

	if exe != nil {
		*exe = buildTopNExec(topNCase, dataSource, *offset, *count)
	}
}

func initTopNSpillCase1Params(
	ctx *mock.Context,
	dataSource *testutil.MockDataSource,
	topNCase *testutil.SortCase,
	totalRowNum int,
	count *uint64,
	offset *uint64,
	exe **sortexec.TopNExec,
) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, spillCase1HardLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	*count = uint64(totalRowNum - totalRowNum/10)
	*offset = uint64(totalRowNum / 10)

	if exe != nil {
		*exe = buildTopNExec(topNCase, dataSource, *offset, *count)
	}
}

func initTopNSpillCase2Params(
	ctx *mock.Context,
	dataSource *testutil.MockDataSource,
	topNCase *testutil.SortCase,
	totalRowNum int,
	count *uint64,
	offset *uint64,
	exe **sortexec.TopNExec,
) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, spillCase2HardLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	*count = uint64(totalRowNum / 5)
	*offset = *count / 5

	if exe != nil {
		*exe = buildTopNExec(topNCase, dataSource, *offset, *count)
	}
}

func initTopNInMemoryThenSpillParams(
	ctx *mock.Context,
	dataSource *testutil.MockDataSource,
	topNCase *testutil.SortCase,
	totalRowNum int,
	count *uint64,
	offset *uint64,
	exe **sortexec.TopNExec,
) {
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, inMemoryThenSpillHardLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	*count = uint64(totalRowNum / 5)
	*offset = *count / 5

	if exe != nil {
		*exe = buildTopNExec(topNCase, dataSource, *offset, *count)
	}
}

func checkTopNCorrectness(schema *expression.Schema, exe *sortexec.TopNExec, dataSource *testutil.MockDataSource, resultChunks []*chunk.Chunk, offset uint64, count uint64) bool {
	keyColumns, keyCmpFuncs, byItemsDesc := exe.GetSortMetaForTest()
	checker := newResultChecker(schema, keyColumns, keyCmpFuncs, byItemsDesc, dataSource.GenData)
	return checker.check(resultChunks, int64(offset), int64(count))
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
		SortExec:    sortExec,
		Limit:       &plannercore.PhysicalLimit{Offset: offset, Count: count},
		Concurrency: 5,
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

func executeTopNAndManuallyTriggerSpill(t *testing.T, exe *sortexec.TopNExec, hardLimit int64, tracker *memory.Tracker) []*chunk.Chunk {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)

	resultChunks := make([]*chunk.Chunk, 0)
	chk := exec.NewFirstChunk(exe)
	for i := 0; i >= 0; i++ {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)

		if i == 10 {
			// Trigger the spill
			tracker.Consume(hardLimit)
			tracker.Consume(-hardLimit)
		}

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
	require.True(t, exe.GetIsSpillTriggeredInStage1ForTest())
	require.False(t, exe.GetInMemoryThenSpillFlagForTest())

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
	require.False(t, exe.GetIsSpillTriggeredInStage1ForTest())
	require.True(t, exe.GetIsSpillTriggeredInStage2ForTest())
	require.False(t, exe.GetInMemoryThenSpillFlagForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

// After all sorted rows are in memory, then the spill will be triggered after some chunks have been fetched
func topNInMemoryThenSpillCase(t *testing.T, ctx *mock.Context, exe *sortexec.TopNExec, sortCase *testutil.SortCase, schema *expression.Schema, dataSource *testutil.MockDataSource, offset uint64, count uint64) {
	if exe == nil {
		exe = buildTopNExec(sortCase, dataSource, offset, count)
	}
	dataSource.PrepareChunks()
	resultChunks := executeTopNAndManuallyTriggerSpill(t, exe, hardLimit1*2, ctx.GetSessionVars().StmtCtx.MemTracker)

	require.True(t, exe.IsSpillTriggeredForTest())
	require.False(t, exe.GetIsSpillTriggeredInStage1ForTest())
	require.False(t, exe.GetIsSpillTriggeredInStage2ForTest())
	require.True(t, exe.GetInMemoryThenSpillFlagForTest())

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkTopNCorrectness(schema, exe, dataSource, resultChunks, offset, count))
}

func topNFailPointTest(t *testing.T, exe *sortexec.TopNExec, sortCase *testutil.SortCase, dataSource *testutil.MockDataSource, offset uint64, count uint64, hardLimit int64, tracker *memory.Tracker) {
	if exe == nil {
		exe = buildTopNExec(sortCase, dataSource, offset, count)
	}
	dataSource.PrepareChunks()
	executeTopNInFailpoint(t, exe, hardLimit, tracker)
}

const spilledChunkMaxSize = 32

func createAndInitDataInDiskByChunks(spilledRowNum uint64) *chunk.DataInDiskByChunks {
	fieldType := types.FieldType{}
	fieldType.SetType(mysql.TypeLonglong)
	inDisk := chunk.NewDataInDiskByChunks([]*types.FieldType{&fieldType})
	var spilledChunk *chunk.Chunk
	for i := uint64(0); i < spilledRowNum; i++ {
		if i%spilledChunkMaxSize == 0 {
			if spilledChunk != nil {
				inDisk.Add(spilledChunk)
			}
			spilledChunk = chunk.NewChunkWithCapacity([]*types.FieldType{&fieldType}, spilledChunkMaxSize)
		}
		spilledChunk.AppendInt64(0, int64(i))
	}
	inDisk.Add(spilledChunk)
	return inDisk
}

func testImpl(t *testing.T, topnExec *sortexec.TopNExec, inDisk *chunk.DataInDiskByChunks, totalRowNumInDisk uint64, offset uint64) {
	sortexec.InitTopNExecForTest(topnExec, offset, inDisk)
	topnExec.GenerateTopNResultsWhenSpillOnlyOnce()
	result := sortexec.GetResultForTest(topnExec)
	require.Equal(t, int(totalRowNumInDisk-offset), len(result))
	for i := range result {
		require.Equal(t, int64(i+int(offset)), result[i])
	}
}

func oneChunkInDiskCase(t *testing.T, topnExec *sortexec.TopNExec) {
	rowNumInDisk := uint64(spilledChunkMaxSize)
	inDisk := createAndInitDataInDiskByChunks(rowNumInDisk)

	testImpl(t, topnExec, inDisk, rowNumInDisk, 0)
	testImpl(t, topnExec, inDisk, rowNumInDisk, uint64(spilledChunkMaxSize-15))
	testImpl(t, topnExec, inDisk, rowNumInDisk, rowNumInDisk-1)
	testImpl(t, topnExec, inDisk, rowNumInDisk, rowNumInDisk)
}

func severalChunksInDiskCase(t *testing.T, topnExec *sortexec.TopNExec) {
	rowNumInDisk := uint64(spilledChunkMaxSize*3 + 10)
	inDisk := createAndInitDataInDiskByChunks(rowNumInDisk)

	testImpl(t, topnExec, inDisk, rowNumInDisk, 0)
	testImpl(t, topnExec, inDisk, rowNumInDisk, spilledChunkMaxSize-15)
	testImpl(t, topnExec, inDisk, rowNumInDisk, spilledChunkMaxSize*2+10)
	testImpl(t, topnExec, inDisk, rowNumInDisk, rowNumInDisk-1)
	testImpl(t, topnExec, inDisk, rowNumInDisk, rowNumInDisk)
}

func TestGenerateTopNResultsWhenSpillOnlyOnce(t *testing.T) {
	topnExec := &sortexec.TopNExec{}
	topnExec.Limit = &plannercore.PhysicalLimit{}

	oneChunkInDiskCase(t, topnExec)
	severalChunksInDiskCase(t, topnExec)
}

func TestTopNSpillDisk(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	topNCase := &testutil.SortCase{Rows: totalRowNum, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`))

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit2)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	offset := uint64(totalRowNum / 10)
	count := uint64(totalRowNum / 3)

	var exe *sortexec.TopNExec
	schema := expression.NewSchema(topNCase.Columns()...)
	dataSource := buildDataSource(topNCase, schema)
	initTopNNoSpillCaseParams(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 20; i++ {
		topNNoSpillCase(t, nil, topNCase, schema, dataSource, 0, count)
		topNNoSpillCase(t, exe, topNCase, schema, dataSource, offset, count)
	}

	initTopNSpillCase1Params(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 20; i++ {
		topNSpillCase1(t, nil, topNCase, schema, dataSource, 0, count)
		topNSpillCase1(t, exe, topNCase, schema, dataSource, offset, count)
	}

	initTopNSpillCase2Params(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 20; i++ {
		topNSpillCase2(t, nil, topNCase, schema, dataSource, 0, count)
		topNSpillCase2(t, exe, topNCase, schema, dataSource, offset, count)
	}

	initTopNInMemoryThenSpillParams(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 20; i++ {
		topNInMemoryThenSpillCase(t, ctx, nil, topNCase, schema, dataSource, 0, count)
		topNInMemoryThenSpillCase(t, ctx, exe, topNCase, schema, dataSource, offset, count)
	}

	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers")
}

func TestTopNSpillDiskFailpoint(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	topNCase := &testutil.SortCase{Rows: totalRowNum, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/TopNRandomFail", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError", `return(true)`))

	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	offset := uint64(totalRowNum / 10)
	count := uint64(totalRowNum / 3)

	var exe *sortexec.TopNExec
	schema := expression.NewSchema(topNCase.Columns()...)
	dataSource := buildDataSource(topNCase, schema)
	initTopNNoSpillCaseParams(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 10; i++ {
		topNFailPointTest(t, nil, topNCase, dataSource, 0, count, 0, ctx.GetSessionVars().MemTracker)
		topNFailPointTest(t, exe, topNCase, dataSource, offset, count, 0, ctx.GetSessionVars().MemTracker)
	}

	initTopNSpillCase1Params(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 10; i++ {
		topNFailPointTest(t, nil, topNCase, dataSource, 0, count, 0, ctx.GetSessionVars().MemTracker)
		topNFailPointTest(t, exe, topNCase, dataSource, offset, count, 0, ctx.GetSessionVars().MemTracker)
	}

	initTopNSpillCase2Params(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 10; i++ {
		topNFailPointTest(t, nil, topNCase, dataSource, 0, count, 0, ctx.GetSessionVars().MemTracker)
		topNFailPointTest(t, exe, topNCase, dataSource, offset, count, 0, ctx.GetSessionVars().MemTracker)
	}

	initTopNInMemoryThenSpillParams(ctx, dataSource, topNCase, totalRowNum, &count, &offset, &exe)
	for i := 0; i < 10; i++ {
		topNFailPointTest(t, nil, topNCase, dataSource, 0, count, inMemoryThenSpillHardLimit, ctx.GetSessionVars().MemTracker)
		topNFailPointTest(t, exe, topNCase, dataSource, offset, count, inMemoryThenSpillHardLimit, ctx.GetSessionVars().MemTracker)
	}

	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/SlowSomeWorkers")
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/TopNRandomFail")
	failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/ParallelSortRandomFail")
	failpoint.Disable("github.com/pingcap/tidb/pkg/util/chunk/ChunkInDiskError")
}
