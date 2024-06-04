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
	"sort"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// It will sort values in memory and compare them with the results produced by sort executor.
type resultChecker struct {
	schema      *expression.Schema
	keyColumns  []int
	keyCmpFuncs []chunk.CompareFunc
	byItemsDesc []bool

	// Initially, savedChunks are not sorted
	savedChunks []*chunk.Chunk
	rowPtrs     []chunk.RowPtr
}

func newResultChecker(schema *expression.Schema, keyColumns []int, keyCmpFuncs []chunk.CompareFunc, byItemsDesc []bool, savedChunks []*chunk.Chunk) *resultChecker {
	checker := resultChecker{}
	checker.schema = schema
	checker.keyColumns = keyColumns
	checker.keyCmpFuncs = keyCmpFuncs
	checker.byItemsDesc = byItemsDesc
	checker.savedChunks = savedChunks
	return &checker
}

func (r *resultChecker) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range r.keyColumns {
		cmpFunc := r.keyCmpFuncs[i]
		if cmpFunc != nil {
			cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
			if r.byItemsDesc[i] {
				cmp = -cmp
			}
			if cmp < 0 {
				return true
			} else if cmp > 0 {
				return false
			}
		}
	}
	return false
}

func (r *resultChecker) keyColumnsLess(i, j int) bool {
	rowI := r.savedChunks[r.rowPtrs[i].ChkIdx].GetRow(int(r.rowPtrs[i].RowIdx))
	rowJ := r.savedChunks[r.rowPtrs[j].ChkIdx].GetRow(int(r.rowPtrs[j].RowIdx))
	return r.lessRow(rowI, rowJ)
}

func (r *resultChecker) getSavedChunksRowNumber() int {
	rowNum := 0
	for _, chk := range r.savedChunks {
		rowNum += chk.NumRows()
	}
	return rowNum
}

func (r *resultChecker) initRowPtrs() {
	r.rowPtrs = make([]chunk.RowPtr, 0, r.getSavedChunksRowNumber())
	chunkNum := len(r.savedChunks)
	for chkIdx := 0; chkIdx < chunkNum; chkIdx++ {
		chk := r.savedChunks[chkIdx]
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			r.rowPtrs = append(r.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (r *resultChecker) check(resultChunks []*chunk.Chunk, offset int64, count int64) bool {
	ctx := contextstatic.NewStaticEvalContext()

	if r.rowPtrs == nil {
		r.initRowPtrs()
		sort.Slice(r.rowPtrs, r.keyColumnsLess)
		if offset < 0 {
			offset = 0
		}
		if count < 0 {
			count = (int64(len(r.rowPtrs)) - offset)
		}
		r.rowPtrs = r.rowPtrs[offset : offset+count]
	}

	cursor := 0
	fieldTypes := make([]*types.FieldType, 0)
	for _, col := range r.schema.Columns {
		fieldTypes = append(fieldTypes, col.GetType(ctx))
	}

	// Check row number
	totalResRowNum := 0
	for _, chk := range resultChunks {
		totalResRowNum += chk.NumRows()
	}
	if totalResRowNum != len(r.rowPtrs) {
		return false
	}

	for _, chk := range resultChunks {
		rowNum := chk.NumRows()
		for i := 0; i < rowNum; i++ {
			resRow := chk.GetRow(i)
			res := resRow.ToString(fieldTypes)

			expectRow := r.savedChunks[r.rowPtrs[cursor].ChkIdx].GetRow(int(r.rowPtrs[cursor].RowIdx))
			expect := expectRow.ToString(fieldTypes)

			if res != expect {
				return false
			}
			cursor++
		}
	}

	return true
}

func buildDataSource(sortCase *testutil.SortCase, schema *expression.Schema) *testutil.MockDataSource {
	opt := testutil.MockDataSourceParameters{
		DataSchema: schema,
		Rows:       sortCase.Rows,
		Ctx:        sortCase.Ctx,
		Ndvs:       sortCase.Ndvs,
	}
	return testutil.BuildMockDataSource(opt)
}

func buildSortExec(sortCase *testutil.SortCase, dataSource *testutil.MockDataSource) *sortexec.SortExec {
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

func executeSortExecutor(t *testing.T, exe *sortexec.SortExec, isParallelSort bool) []*chunk.Chunk {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)
	if isParallelSort {
		exe.IsUnparallel = false
		exe.InitInParallelModeForTest()
	}

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

func executeSortExecutorAndManullyTriggerSpill(t *testing.T, exe *sortexec.SortExec, hardLimit int64, tracker *memory.Tracker, isParallelSort bool) []*chunk.Chunk {
	tmpCtx := context.Background()
	err := exe.Open(tmpCtx)
	require.NoError(t, err)
	if isParallelSort {
		exe.IsUnparallel = false
		exe.InitInParallelModeForTest()
	}

	resultChunks := make([]*chunk.Chunk, 0)
	chk := exec.NewFirstChunk(exe)
	for i := 0; i >= 0; i++ {
		err = exe.Next(tmpCtx, chk)
		require.NoError(t, err)

		if i == 10 {
			// Trigger the spill
			tracker.Consume(hardLimit)
			tracker.Consume(-hardLimit)

			// Wait for the finish of spill, or the spill may not be triggered even data in memory has been drained
			time.Sleep(100 * time.Millisecond)
		}

		if chk.NumRows() == 0 {
			break
		}
		resultChunks = append(resultChunks, chk.CopyConstruct())
	}
	return resultChunks
}

func checkCorrectness(schema *expression.Schema, exe *sortexec.SortExec, dataSource *testutil.MockDataSource, resultChunks []*chunk.Chunk) bool {
	keyColumns, keyCmpFuncs, byItemsDesc := exe.GetSortMetaForTest()
	checker := newResultChecker(schema, keyColumns, keyCmpFuncs, byItemsDesc, dataSource.GenData)
	return checker.check(resultChunks, -1, -1)
}

func onePartitionAndAllDataInMemoryCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase) {
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 1048576)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	// TODO use variable to choose parallel mode after system variable is added
	// ctx.GetSessionVars().EnableParallelSort = false
	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	resultChunks := executeSortExecutor(t, exe, false)

	require.Equal(t, exe.GetSortPartitionListLenForTest(), 1)
	require.Equal(t, false, exe.IsSpillTriggeredInOnePartitionForTest(0))
	require.Equal(t, int64(2048), exe.GetRowNumInOnePartitionMemoryForTest(0))
	require.Equal(t, int64(0), exe.GetRowNumInOnePartitionDiskForTest(0))
	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

func onePartitionAndAllDataInDiskCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase) {
	ctx.GetSessionVars().InitChunkSize = 1024
	ctx.GetSessionVars().MaxChunkSize = 1024
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 50000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	// TODO use variable to choose parallel mode after system variable is added
	// ctx.GetSessionVars().EnableParallelSort = false
	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)

	// To ensure that spill has been trigger before getting chunk, or we may get chunk from memory.
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/waitForSpill", `return(true)`)
	resultChunks := executeSortExecutor(t, exe, false)
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/waitForSpill", `return(false)`)

	require.Equal(t, exe.GetSortPartitionListLenForTest(), 1)
	require.Equal(t, true, exe.IsSpillTriggeredInOnePartitionForTest(0))
	require.Equal(t, int64(0), exe.GetRowNumInOnePartitionMemoryForTest(0))
	require.Equal(t, int64(2048), exe.GetRowNumInOnePartitionDiskForTest(0))
	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

// When we enable the `unholdSyncLock` failpoint, we can ensure that there must be multi partitions.
// However, `unholdSyncLock` failpoint introduces sleep and this will hide some concurrent problems,
// so this failpoint needs to be disabled if we want to test concurrent problem.
func multiPartitionCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase, enableFailPoint bool) {
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, 10000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	// TODO use variable to choose parallel mode after system variable is added
	// ctx.GetSessionVars().EnableParallelSort = false
	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	exe.IsUnparallel = true
	if enableFailPoint {
		failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/unholdSyncLock", `return(true)`)
	}
	resultChunks := executeSortExecutor(t, exe, false)
	if enableFailPoint {
		failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/unholdSyncLock", `return(false)`)
	}
	if enableFailPoint {
		// If we disable the failpoint, there may be only one partition.
		sortPartitionNum := exe.GetSortPartitionListLenForTest()
		require.Greater(t, sortPartitionNum, 1)

		// Ensure all partitions are spilled
		for i := 0; i < sortPartitionNum; i++ {
			// The last partition may not be spilled.
			if i < sortPartitionNum-1 {
				require.Equal(t, true, exe.IsSpillTriggeredInOnePartitionForTest(i))
			}
		}
	}

	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

// Data are all in memory and some of then are fetched, then the spill is triggered
func inMemoryThenSpillCase(t *testing.T, ctx *mock.Context, sortCase *testutil.SortCase) {
	hardLimit := int64(100000)
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, hardLimit)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	// TODO use variable to choose parallel mode after system variable is added
	// ctx.GetSessionVars().EnableParallelSort = false
	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	resultChunks := executeSortExecutorAndManullyTriggerSpill(t, exe, hardLimit, ctx.GetSessionVars().StmtCtx.MemTracker, false)

	require.Equal(t, exe.GetSortPartitionListLenForTest(), 1)
	require.Equal(t, true, exe.IsSpillTriggeredInOnePartitionForTest(0))

	rowNumInDisk := exe.GetRowNumInOnePartitionDiskForTest(0)
	require.Equal(t, int64(0), exe.GetRowNumInOnePartitionMemoryForTest(0))
	require.Greater(t, int64(2048), rowNumInDisk)
	require.Less(t, int64(0), rowNumInDisk)
	err := exe.Close()
	require.NoError(t, err)

	require.True(t, checkCorrectness(schema, exe, dataSource, resultChunks))
}

func TestUnparallelSortSpillDisk(t *testing.T) {
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	sortCase := &testutil.SortCase{Rows: 2048, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)

	for i := 0; i < 50; i++ {
		onePartitionAndAllDataInMemoryCase(t, ctx, sortCase)
		onePartitionAndAllDataInDiskCase(t, ctx, sortCase)
		multiPartitionCase(t, ctx, sortCase, false)
		multiPartitionCase(t, ctx, sortCase, true)
		inMemoryThenSpillCase(t, ctx, sortCase)
	}
}

func TestFallBackAction(t *testing.T) {
	hardLimitBytesNum := int64(1000000)
	newRootExceedAction := new(testutil.MockActionOnExceed)
	sortexec.SetSmallSpillChunkSizeForTest()
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 32
	ctx.GetSessionVars().MaxChunkSize = 32
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, hardLimitBytesNum)
	ctx.GetSessionVars().MemTracker.SetActionOnExceed(newRootExceedAction)
	// Consume lots of memory in advance to help to trigger fallback action.
	ctx.GetSessionVars().MemTracker.Consume(int64(float64(hardLimitBytesNum) * 0.99999))
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	sortCase := &testutil.SortCase{Rows: 2048, OrderByIdx: []int{0, 1}, Ndvs: []int{0, 0}, Ctx: ctx}

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/SignalCheckpointForSort", `return(true)`)

	schema := expression.NewSchema(sortCase.Columns()...)
	dataSource := buildDataSource(sortCase, schema)
	exe := buildSortExec(sortCase, dataSource)
	executeSortExecutor(t, exe, false)
	err := exe.Close()
	require.NoError(t, err)

	require.Less(t, 0, newRootExceedAction.GetTriggeredNum())
}
