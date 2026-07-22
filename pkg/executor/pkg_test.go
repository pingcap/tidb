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

func (*oversizedChunkSelectResult) IntoIter([][]*types.FieldType) (distsql.SelectResultIter, error) {
	return nil, nil
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

	controller := exec.NewAdaptiveLimitController(100, 32, 128, 32, 128)
	indexLookup := &IndexLookUpExecutor{
		BaseExecutorV2: exec.NewBaseExecutorV2(sctx.GetSessionVars(), nil, 4),
		indexLookUpExecutorContext: indexLookUpExecutorContext{
			indexLookupConcurrency: 2,
		},
		keepOrder: true,
	}
	require.True(t, attachAdaptiveLimitIndexLookup(indexLookup, controller))
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
	require.Equal(t, 2, controller.SuggestedScanConcurrency(15))

	scanLimiter := newAdaptiveCoprRequestLimiter(4, 1)
	require.Equal(t, 4, scanLimiter.rateLimit.GetCapacity())
	require.False(t, scanLimiter.rateLimit.GetToken(nil))
	secondToken := make(chan bool, 1)
	go func() {
		secondToken <- !scanLimiter.rateLimit.GetToken(nil)
	}()
	select {
	case <-secondToken:
		require.Fail(t, "second cop request must wait at initial concurrency one")
	case <-time.After(20 * time.Millisecond):
	}
	scanLimiter.growTo(2)
	select {
	case acquired := <-secondToken:
		require.True(t, acquired)
	case <-time.After(time.Second):
		require.Fail(t, "growing scan concurrency did not release a request token")
	}
	scanLimiter.rateLimit.PutToken()
	scanLimiter.rateLimit.PutToken()
	scanLimiter.release()

	starvationController := exec.NewAdaptiveLimitController(1000, 32, 128, 32, 128)
	waitingLookup := &IndexLookUpExecutor{
		adaptiveLimitController: starvationController,
		resultCh:                make(chan *lookupTableTask, 1),
	}
	starvationLimiter := newAdaptiveCoprRequestLimiter(4, 1)
	waitingLookup.adaptiveCoprRequestLimiter.Store(starvationLimiter)
	readyTask := &lookupTableTask{}
	waitingLookup.resultCh <- readyTask
	received, ok := waitingLookup.receiveResultTask()
	require.True(t, ok)
	require.Same(t, readyTask, received)
	require.Equal(t, 1, starvationController.SuggestedScanConcurrency(4))

	type receivedLookupTask struct {
		task *lookupTableTask
		ok   bool
	}
	receiveBlockedTask := func(expected *lookupTableTask, expectedConcurrency int) {
		result := make(chan receivedLookupTask, 1)
		go func() {
			task, ok := waitingLookup.receiveResultTask()
			result <- receivedLookupTask{task: task, ok: ok}
		}()
		require.Eventually(t, func() bool {
			starvationLimiter.mu.Lock()
			defer starvationLimiter.mu.Unlock()
			return starvationLimiter.concurrency == expectedConcurrency
		}, time.Second, time.Millisecond)
		waitingLookup.resultCh <- expected
		select {
		case received := <-result:
			require.True(t, received.ok)
			require.Same(t, expected, received.task)
		case <-time.After(time.Second):
			require.Fail(t, "blocked lookup consumer did not receive the task")
		}
	}

	receiveBlockedTask(&lookupTableTask{}, 2)
	require.Equal(t, 2, starvationController.SuggestedScanConcurrency(4))
	require.False(t, starvationController.ObserveLookupConsumerBlocked())

	reserved, ok, err = starvationController.ReserveLookup(context.Background(), 32)
	require.NoError(t, err)
	require.True(t, ok)
	starvationController.CompleteLookup(reserved, reserved, reserved)
	receiveBlockedTask(&lookupTableTask{}, 4)
	require.Equal(t, 4, starvationController.SuggestedScanConcurrency(4))
	waitingLookup.adaptiveCoprRequestLimiter.Store(nil)
	starvationLimiter.release()

	pendingTracker := memory.NewTracker(-1, -1)
	worker := &indexWorker{adaptiveLimitController: controller, batchSize: 2, memTracker: pendingTracker}
	handles := make([]kv.Handle, 0, 2)
	for i := range 5 {
		handles = worker.appendExtractedHandle(handles, kv.IntHandle(i))
	}
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
	for i := range 4 {
		handles = worker.appendExtractedHandle(handles, kv.IntHandle(i))
	}
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
	worker = &indexWorker{
		idxLookup:               indexLookup,
		adaptiveLimitController: controller,
		batchSize:               2,
		maxBatchSize:            2,
		maxChunkSize:            32,
		PushedLimit:             &physicalop.PushedDownLimit{Count: 5},
	}
	result := &oversizedChunkSelectResult{rows: []int64{1, 2, 3, 4, 5}}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 32)
	for _, expected := range [][]int64{{1, 2}, {3, 4}, {5}} {
		handles, _, err := worker.extractTaskHandles(context.Background(), chk, result, []int{0})
		require.NoError(t, err)
		extracted := make([]int64, 0, len(handles))
		for _, handle := range handles {
			extracted = append(extracted, handle.IntValue())
		}
		require.Equal(t, expected, extracted)
	}
	require.True(t, worker.reachedPushedLimit())
	require.Empty(t, worker.pendingHandles)
	require.Equal(t, uint64(5), worker.scannedKeys)

	canceledController := exec.NewAdaptiveLimitController(100, 2, 128, 2, 128)
	worker.adaptiveLimitController = canceledController
	worker.batchSize = 2
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = worker.extractLookupTaskData(
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
		require.False(t, attachAdaptiveLimitIndexLookup(indexLookup, controller))
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
	require.True(t, attachAdaptiveLimitIndexLookup(indexLookup, controller))
	require.Same(t, controller, indexLookup.adaptiveLimitController)
	indexLookup.idxPlans = []base.PhysicalPlan{
		&physicalop.PhysicalIndexScan{GroupByColIdxs: []int{0}},
	}
	assertIneligible()
	indexLookup.byItems = nil
	indexLookup.idxPlans = nil
	indexLookup.indexLookUpPushDown = true
	assertIneligible()
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
