// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type oversizedChunkSelectResult struct {
	rows     []int64
	returned bool
}

type partialThenErrorSelectResult struct {
	oversizedChunkSelectResult
	err error
}

func (*oversizedChunkSelectResult) NextRaw(context.Context) ([]byte, error) { return nil, nil }
func (*oversizedChunkSelectResult) Close() error                            { return nil }

func (r *oversizedChunkSelectResult) Next(_ context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if r.returned {
		return nil
	}
	for _, value := range r.rows {
		row := chunk.MutRowFromTypes([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)})
		row.SetValue(0, value)
		chk.AppendRow(row.ToRow())
	}
	r.returned = true
	return nil
}

func (r *partialThenErrorSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	if !r.returned {
		return r.oversizedChunkSelectResult.Next(ctx, chk)
	}
	chk.Reset()
	return r.err
}

func (*oversizedChunkSelectResult) IntoIter([][]*types.FieldType) (distsql.SelectResultIter, error) {
	return nil, nil
}

// CancelAndWaitImportJobForTest exposes cancelAndWaitImportJob to external package tests.
func CancelAndWaitImportJobForTest(ctx context.Context, jobID int64) error {
	return cancelAndWaitImportJob(ctx, jobID)
}

func TestNestedLoopApply(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	con := &expression.Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeLong)}
	outerSchema := expression.NewSchema(col0)
	outerExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: outerSchema,
		Rows:       6,
		Ctx:        sctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			return int64(row + 1)
		},
	})
	outerExec.PrepareChunks()

	innerSchema := expression.NewSchema(col1)
	innerExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: innerSchema,
		Rows:       6,
		Ctx:        sctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			return int64(row + 1)
		},
	})
	innerExec.PrepareChunks()

	outerFilter := expression.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col0, con)
	innerFilter := outerFilter.Clone()
	require.True(t, innerFilter.Equal(sctx, outerFilter))
	otherFilter := expression.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col1)
	joiner := join.NewJoiner(sctx, base.InnerJoin, false,
		make([]types.Datum, innerExec.Schema().Len()), []expression.Expression{otherFilter},
		exec.RetTypes(outerExec), exec.RetTypes(innerExec), nil, false)
	joinSchema := expression.NewSchema(col0, col1)
	join := &join.NestedLoopApplyExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, joinSchema, 0),
		OuterExec:    outerExec,
		InnerExec:    innerExec,
		OuterFilter:  []expression.Expression{outerFilter},
		InnerFilter:  []expression.Expression{innerFilter},
		Joiner:       joiner,
		Sctx:         sctx,
	}
	join.InnerList = chunk.NewList(exec.RetTypes(innerExec), innerExec.InitCap(), innerExec.MaxChunkSize())
	join.InnerChunk = exec.NewFirstChunk(innerExec)
	join.OuterChunk = exec.NewFirstChunk(outerExec)
	joinChk := exec.NewFirstChunk(join)
	it := chunk.NewIterator4Chunk(joinChk)
	for rowIdx := 1; ; {
		err := join.Next(ctx, joinChk)
		require.NoError(t, err)
		if joinChk.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			correctResult := fmt.Sprintf("%v %v", rowIdx, rowIdx)
			obtainedResult := fmt.Sprintf("%v %v", row.GetInt64(0), row.GetInt64(1))
			require.Equal(t, correctResult, obtainedResult)
			rowIdx++
		}
	}
}

func TestAdaptiveLimitEligibility(t *testing.T) {
	require.Equal(t, uint64(7), adaptiveLimitInitialWindow(7, 1024))
	require.Equal(t, uint64(1024), adaptiveLimitInitialWindow(100001, 1024))
	require.Equal(t, uint64(1), adaptiveLimitInitialLookupBatchSize(1, true, 32, 20000))
	require.Equal(t, uint64(32), adaptiveLimitInitialLookupBatchSize(1, false, 32, 20000))
	require.Equal(t, uint64(1000), adaptiveLimitInitialLookupBatchSize(1000, false, 32, 20000))
	require.Equal(t, uint64(128), adaptiveLimitInitialLookupBatchSize(1000, false, 32, 128))

	sctx := mock.NewContext()
	indexJoin := &join.IndexLookUpJoin{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 1),
	}
	projection := &ProjectionExec{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 2, indexJoin),
	}
	selection := &SelectionExec{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 3, indexJoin),
	}

	require.Same(t, indexJoin, findAdaptiveLimitIndexJoin(indexJoin))
	require.Same(t, indexJoin, findAdaptiveLimitIndexJoin(projection))
	require.Nil(t, findAdaptiveLimitIndexJoin(selection))

	directLookup := &IndexLookUpExecutor{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 5),
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			indexLookupConcurrency: 2,
		},
		keepOrder: true,
	}
	directProjection := &ProjectionExec{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 6, directLookup),
	}
	directProjection = &ProjectionExec{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 7, directProjection),
	}
	require.Same(t, directLookup, findAdaptiveLimitIndexLookupCandidate(directProjection))
	directSelection := &SelectionExec{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 8, directLookup),
	}
	require.Nil(t, findAdaptiveLimitIndexLookupCandidate(directSelection))
	directLookup.PushedLimit = &physicalop.PushedDownLimit{Count: 10}
	require.Nil(t, findAdaptiveLimitIndexLookupCandidate(directLookup))
	directLookup.PushedLimit = nil
	directController := exec.NewAdaptiveLimitLookupController(exec.AdaptiveLimitConfig{
		DemandRows:             100,
		InitialLookupWindow:    32,
		MaxLookupWindow:        128,
		InitialLookupBatchSize: 32,
		MaxLookupBatchSize:     128,
	})
	directLookup.adaptiveLimitController = directController
	directLookup.reportAdaptiveLimitStats = true
	require.Same(t, directController, directLookup.adaptiveLimitController)
	require.True(t, directLookup.reportAdaptiveLimitStats)
	require.Nil(t, findAdaptiveLimitIndexLookupCandidate(directLookup))

	controller := exec.NewAdaptiveLimitController(exec.AdaptiveLimitConfig{
		DemandRows: 100, InitialOuterWindow: 32, MaxOuterWindow: 128,
		InitialLookupWindow: 32, MaxLookupWindow: 128,
		InitialLookupBatchSize: 32, MaxLookupBatchSize: 128,
	})
	indexLookup := &IndexLookUpExecutor{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 4),
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			indexLookupConcurrency: 2,
		},
		keepOrder: true,
	}
	require.Same(t, indexLookup, findAdaptiveLimitIndexLookupCandidate(indexLookup))
	indexLookup.adaptiveLimitController = controller
	require.Same(t, controller, indexLookup.adaptiveLimitController)
	reserved, ok, err := controller.ReserveLookup(context.Background(), 32)
	require.NoError(t, err)
	require.True(t, ok)
	task := &lookupTableTask{
		handles:                  make([]kv.Handle, reserved),
		rows:                     make([]chunk.Row, 1),
		cursor:                   1,
		adaptiveLimitReservation: reserved,
	}
	indexLookup.completeAdaptiveLookupTask(task)
	require.Zero(t, task.adaptiveLimitReservation)
	require.Equal(t, uint64(32), controller.Snapshot().LookupHandles)
	require.Equal(t, uint64(1), controller.Snapshot().LookupRows)

	pendingTracker := memory.NewTracker(-1, -1)
	worker := &indexWorker{adaptiveLimitController: controller, batchSize: 2, memTracker: pendingTracker}
	handles := make([]kv.Handle, 0, 2)
	var pendingHandlesMemUsage int64
	for i := range 5 {
		var pendingHandleMemUsage int64
		handles, pendingHandleMemUsage = worker.appendExtractedHandle(handles, kv.IntHandle(i))
		pendingHandlesMemUsage += pendingHandleMemUsage
	}
	worker.consumePendingHandlesMemory(pendingHandlesMemUsage)
	require.Len(t, handles, 2)
	require.Len(t, worker.pendingHandles, 3)
	require.Positive(t, pendingTracker.BytesConsumed())
	handles = handles[:0]
	handles = worker.takePendingHandles(handles)
	require.Len(t, handles, 2)
	require.Len(t, worker.pendingHandles, 1)
	require.Positive(t, pendingTracker.BytesConsumed())
	handles = handles[:0]
	handles = worker.takePendingHandles(handles)
	require.Len(t, handles, 1)
	require.Empty(t, worker.pendingHandles)
	require.Zero(t, pendingTracker.BytesConsumed())
	handles = handles[:0]
	pendingHandlesMemUsage = 0
	for i := range 4 {
		var pendingHandleMemUsage int64
		handles, pendingHandleMemUsage = worker.appendExtractedHandle(handles, kv.IntHandle(i))
		pendingHandlesMemUsage += pendingHandleMemUsage
	}
	worker.consumePendingHandlesMemory(pendingHandlesMemUsage)
	require.Len(t, worker.pendingHandles, 2)
	require.Positive(t, pendingTracker.BytesConsumed())
	worker.releasePendingHandles()
	require.Empty(t, worker.pendingHandles)
	require.Zero(t, worker.pendingHandlesMemUsage)
	require.Zero(t, pendingTracker.BytesConsumed())
	worker.PushedLimit = &physicalop.PushedDownLimit{Count: 5}
	worker.scannedKeys = 5
	worker.pendingHandles = []kv.Handle{kv.IntHandle(5)}
	require.False(t, worker.reachedPushedLimit())
	worker.pendingHandles = nil
	require.True(t, worker.reachedPushedLimit())

	indexLookup.table = tables.MockTableFromMeta(&model.TableInfo{})
	indexLookup.index = &model.IndexInfo{}
	handleColumn := &expression.Column{ID: model.ExtraHandleID, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	indexScan := physicalop.PhysicalIndexScan{}.Init(sctx, 0)
	indexScan.SetSchema(expression.NewSchema(handleColumn))
	indexLookup.idxPlans = []base.PhysicalPlan{indexScan}
	indexLookup.dagPB = &tipb.DAGRequest{OutputOffsets: []uint32{0}}
	indexLookup.handleCols = []*expression.Column{handleColumn}
	extractController := exec.NewAdaptiveLimitController(exec.AdaptiveLimitConfig{
		DemandRows: 100, InitialOuterWindow: 2, MaxOuterWindow: 128,
		InitialLookupWindow: 2, MaxLookupWindow: 128,
		InitialLookupBatchSize: 2, MaxLookupBatchSize: 128,
	})
	worker = &indexWorker{
		idxLookup:               indexLookup,
		adaptiveLimitController: extractController,
		batchSize:               2,
		maxBatchSize:            2,
		maxChunkSize:            32,
		PushedLimit:             &physicalop.PushedDownLimit{Count: 5},
		memTracker:              pendingTracker,
	}
	result := &oversizedChunkSelectResult{rows: []int64{1, 2, 3, 4, 5}}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 32)
	for round, expected := range [][]int64{{1, 2}, {3, 4}, {5}} {
		handles, _, err := worker.extractTaskHandles(context.Background(), chk, result, []int{0})
		require.NoError(t, err)
		extracted := make([]int64, 0, len(handles))
		for _, handle := range handles {
			extracted = append(extracted, handle.IntValue())
		}
		require.Equal(t, expected, extracted)
		require.Len(t, worker.pendingHandles, []int{3, 1, 0}[round])
		if round < 2 {
			require.Positive(t, pendingTracker.BytesConsumed())
		} else {
			require.Zero(t, pendingTracker.BytesConsumed())
		}
	}
	require.True(t, worker.reachedPushedLimit())
	require.Empty(t, worker.pendingHandles)
	require.Equal(t, uint64(5), worker.scannedKeys)

	canceledController := exec.NewAdaptiveLimitController(exec.AdaptiveLimitConfig{
		DemandRows: 100, InitialOuterWindow: 2, MaxOuterWindow: 128,
		InitialLookupWindow: 2, MaxLookupWindow: 128,
		InitialLookupBatchSize: 2, MaxLookupBatchSize: 128,
	})
	canceledWorker := &indexWorker{
		idxLookup:               indexLookup,
		adaptiveLimitController: canceledController,
		batchSize:               2,
		maxBatchSize:            2,
		maxChunkSize:            32,
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = canceledWorker.extractLookupTaskData(
		canceledCtx,
		&oversizedChunkSelectResult{rows: []int64{1}},
		nil,
		chk,
		[]int{0},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, canceledController.Snapshot().LookupReserved)

	assertIneligible := func() {
		indexLookup.adaptiveLimitController = nil
		require.Nil(t, findAdaptiveLimitIndexLookupCandidate(indexLookup))
		require.Nil(t, indexLookup.adaptiveLimitController)
	}

	indexLookup.indexLookupConcurrency = 1
	assertIneligible()
	indexLookup.indexLookupConcurrency = 2
	indexLookup.keepOrder = false
	assertIneligible()
	indexLookup.keepOrder = true
	indexLookup.partitionTableMode = true
	assertIneligible()
	indexLookup.partitionTableMode = false
	indexLookup.groupedRanges = [][]*ranger.Range{{}}
	assertIneligible()
	indexLookup.groupedRanges = nil
	indexLookup.idxPlans = nil
	indexLookup.byItems = []*plannerutil.ByItems{{}}
	assertIneligible()
	indexLookup.idxPlans = []base.PhysicalPlan{indexScan}
	indexLookup.adaptiveLimitController = nil
	require.Same(t, indexLookup, findAdaptiveLimitIndexLookupCandidate(indexLookup))
	indexLookup.adaptiveLimitController = controller
	require.Same(t, controller, indexLookup.adaptiveLimitController)
	indexLookup.idxPlans = []base.PhysicalPlan{
		&physicalop.PhysicalIndexScan{GroupByColIdxs: []int{0}},
	}
	assertIneligible()
	indexLookup.byItems = nil
	indexLookup.idxPlans = []base.PhysicalPlan{indexScan}
	indexLookup.indexLookUpPushDown = true
	assertIneligible()
	indexLookup.indexLookUpPushDown = false
	indexLookup.PushedLimit = &physicalop.PushedDownLimit{Count: 10}
	assertIneligible()
}

func TestAdaptiveLimitReservationExitPaths(t *testing.T) {
	newController := func() *exec.AdaptiveLimitController {
		return exec.NewAdaptiveLimitLookupController(exec.AdaptiveLimitConfig{
			DemandRows:             100,
			InitialLookupWindow:    2,
			MaxLookupWindow:        2,
			InitialLookupBatchSize: 2,
			MaxLookupBatchSize:     2,
		})
	}
	reserveLookup := func(t *testing.T, controller *exec.AdaptiveLimitController) int {
		t.Helper()
		reserved, ok, err := controller.ReserveLookup(context.Background(), 2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, 2, reserved)
		return reserved
	}
	newExtractionWorker := func(controller *exec.AdaptiveLimitController) (*indexWorker, *chunk.Chunk) {
		sctx := mock.NewContext()
		handleColumn := &expression.Column{
			ID:      model.ExtraHandleID,
			Index:   0,
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		indexScan := physicalop.PhysicalIndexScan{}.Init(sctx, 0)
		indexScan.SetSchema(expression.NewSchema(handleColumn))
		indexLookup := &IndexLookUpExecutor{
			BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 1),
			index:          &model.IndexInfo{},
			handleCols:     []*expression.Column{handleColumn},
			dagPB:          &tipb.DAGRequest{OutputOffsets: []uint32{0}},
			idxPlans:       []base.PhysicalPlan{indexScan},
		}
		worker := &indexWorker{
			idxLookup:               indexLookup,
			adaptiveLimitController: controller,
			batchSize:               2,
			maxBatchSize:            2,
			maxChunkSize:            32,
		}
		chk := chunk.NewChunkWithCapacity(
			[]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
			32,
		)
		return worker, chk
	}

	t.Run("partial extraction error", func(t *testing.T) {
		controller := newController()
		worker, chk := newExtractionWorker(controller)
		expectedErr := fmt.Errorf("adaptive limit extraction failed")
		result := &partialThenErrorSelectResult{
			oversizedChunkSelectResult: oversizedChunkSelectResult{rows: []int64{1}},
			err:                        expectedErr,
		}

		data, err := worker.extractLookupTaskData(
			context.Background(), result, nil, chk, []int{0},
		)
		require.ErrorIs(t, err, expectedErr)
		require.Len(t, data.handles, 1)
		require.Zero(t, data.adaptiveLimitReservation)
		snapshot := controller.Snapshot()
		require.Zero(t, snapshot.LookupReserved)
		require.Zero(t, snapshot.LookupHandles)
		require.Zero(t, snapshot.LookupRows)
	})

	t.Run("empty extraction EOF", func(t *testing.T) {
		controller := newController()
		worker, chk := newExtractionWorker(controller)

		data, err := worker.extractLookupTaskData(
			context.Background(), &oversizedChunkSelectResult{}, nil, chk, []int{0},
		)
		require.NoError(t, err)
		require.True(t, data.exhausted)
		require.Empty(t, data.handles)
		require.Zero(t, data.adaptiveLimitReservation)
		require.Zero(t, controller.Snapshot().LookupReserved)
	})

	t.Run("partial extraction dispatch and completion", func(t *testing.T) {
		controller := newController()
		worker, chk := newExtractionWorker(controller)
		resultCh := make(chan *lookupTableTask, 1)
		worker.resultCh = resultCh
		worker.finished = make(chan struct{})
		worker.idxLookup.adaptiveLimitController = controller
		worker.idxLookup.resultCh = resultCh
		worker.idxLookup.tblWorkerWg = &sync.WaitGroup{}
		worker.idxLookup.pool = &workerPool{}
		worker.idxLookup.finished = make(chan struct{})
		close(worker.idxLookup.finished)

		data, err := worker.extractLookupTaskData(
			context.Background(), &oversizedChunkSelectResult{rows: []int64{1}}, nil, chk, []int{0},
		)
		require.NoError(t, err)
		require.Len(t, data.handles, 1)
		require.Equal(t, 1, data.adaptiveLimitReservation)
		require.Equal(t, uint64(1), controller.Snapshot().LookupReserved)

		taskID := 0
		require.False(t, worker.buildAndDispatchLookupTasks(context.Background(), 0, &taskID, &data))
		require.Zero(t, data.adaptiveLimitReservation)
		task := <-resultCh
		worker.idxLookup.tblWorkerWg.Wait()
		require.Len(t, task.handles, 1)
		require.Equal(t, 1, task.adaptiveLimitReservation)
		require.Equal(t, uint64(1), controller.Snapshot().LookupReserved)

		task.rows = []chunk.Row{{}}
		task.cursor = len(task.rows)
		worker.idxLookup.completeAdaptiveLookupTask(task)
		require.Zero(t, task.adaptiveLimitReservation)
		snapshot := controller.Snapshot()
		require.Zero(t, snapshot.LookupReserved)
		require.Equal(t, uint64(1), snapshot.LookupHandles)
		require.Equal(t, uint64(1), snapshot.LookupRows)
	})

	t.Run("canceled before dispatch", func(t *testing.T) {
		controller := newController()
		reserved := reserveLookup(t, controller)
		data := extractedLookupTaskData{
			handles:                  []kv.Handle{kv.IntHandle(1), kv.IntHandle(2)},
			adaptiveLimitReservation: reserved,
		}
		worker := &indexWorker{
			idxLookup: &IndexLookUpExecutor{
				BaseExecutorV2: exec.NewBaseExecutorV2(mock.NewContext().GetSessionVars(), nil, 0),
			},
			adaptiveLimitController: controller,
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		taskID := 0

		require.True(t, worker.buildAndDispatchLookupTasks(ctx, 0, &taskID, &data))
		require.Zero(t, data.adaptiveLimitReservation)
		require.Zero(t, controller.Snapshot().LookupReserved)
	})

	t.Run("table task error", func(t *testing.T) {
		controller := newController()
		reserved := reserveLookup(t, controller)
		expectedErr := fmt.Errorf("adaptive limit table lookup failed")
		task := &lookupTableTask{
			doneCh:                   make(chan error, 1),
			adaptiveLimitReservation: reserved,
		}
		task.doneCh <- expectedErr
		resultCh := make(chan *lookupTableTask, 1)
		resultCh <- task
		indexLookup := &IndexLookUpExecutor{
			BaseExecutorV2:          exec.NewBaseExecutorV2(mock.NewContext().GetSessionVars(), nil, 0),
			adaptiveLimitController: controller,
			resultCh:                resultCh,
		}

		result, err := indexLookup.getResultTask()
		require.Nil(t, result)
		require.ErrorIs(t, err, expectedErr)
		require.Zero(t, task.adaptiveLimitReservation)
		require.Zero(t, controller.Snapshot().LookupReserved)
	})
}

func TestIndexLookUpAdaptiveLimitRuntimeStats(t *testing.T) {
	stats := &IndexLookUpRunTimeStats{
		adaptiveLimitSnapshot: &exec.AdaptiveLimitSnapshot{
			LookupHandles:           32,
			LookupRows:              4,
			LookupOutstandingAtStop: 8,
			LookupAdmissionBlocked:  time.Second,
		},
	}
	require.Contains(t, stats.String(), "adaptive:{lookup:32/4, outstanding:8, blocked:1s}")
	clone := stats.Clone().(*IndexLookUpRunTimeStats)
	require.Equal(t, stats.String(), clone.String())
	stats.Merge(clone)
	require.Equal(t, "adaptive:{lookup:32/4, outstanding:8, blocked:1s}", stats.String())
}

func TestMoveInfoSchemaToFront(t *testing.T) {
	dbss := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"A", "B", "C", "INFORMATION_SCHEMA"},
		{"A", "B", "INFORMATION_SCHEMA", "a"},
		{"INFORMATION_SCHEMA"},
		{"A", "B", "C", "INFORMATION_SCHEMA", "a", "b"},
	}
	wanted := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"INFORMATION_SCHEMA", "A", "B", "C"},
		{"INFORMATION_SCHEMA", "A", "B", "a"},
		{"INFORMATION_SCHEMA"},
		{"INFORMATION_SCHEMA", "A", "B", "C", "a", "b"},
	}

	for _, dbs := range dbss {
		moveInfoSchemaToFront(dbs)
	}

	for i, dbs := range wanted {
		require.Equal(t, len(dbs), len(dbss[i]))
		for j, db := range dbs {
			require.Equal(t, db, dbss[i][j])
		}
	}
}
