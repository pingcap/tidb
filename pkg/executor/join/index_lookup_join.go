// Copyright 2017 PingCAP, Inc.
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

package join

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mvmap"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

var _ exec.Executor = &IndexLookUpJoin{}

// IndexLookUpJoin employs one outer worker and N innerWorkers to execute concurrently.
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, builds inner row hash map.
// 3. main thread receives the task, waits for inner worker finish handling the task.
// 4. main thread join each outer row by look up the inner rows hash map in the task.
type IndexLookUpJoin struct {
	exec.BaseExecutor

	resultCh   <-chan *lookUpJoinTask
	cancelFunc context.CancelFunc
	WorkerWg   *sync.WaitGroup

	OuterCtx OuterCtx
	InnerCtx InnerCtx

	task       *lookUpJoinTask
	JoinResult *chunk.Chunk
	innerIter  *chunk.Iterator4Slice

	Joiner      Joiner
	IsOuterJoin bool

	requiredRows int64

	IndexRanges   ranger.MutableRanges
	KeyOff2IdxOff []int
	innerPtrBytes [][]byte

	// LastColHelper store the information for last col if there's complicated filter like col > x_col and col < x_col + 100.
	LastColHelper *physicalop.ColWithCmpFuncManager

	memTracker *memory.Tracker // track memory usage.

	stats    *indexLookUpJoinRuntimeStats
	Finished *atomic.Value
	prepared bool
}

// OuterCtx is the outer ctx used in index lookup join
type OuterCtx struct {
	RowTypes  []*types.FieldType
	KeyCols   []int
	HashTypes []*types.FieldType
	HashCols  []int
	Filter    expression.CNFExprs
}

// IndexJoinExecutorBuilder is the interface used by index lookup join to build the executor, this interface
// is added to avoid cycle import
type IndexJoinExecutorBuilder interface {
	BuildExecutorForIndexJoin(ctx context.Context, lookUpContents []*IndexJoinLookUpContent,
		indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value) (exec.Executor, error)
}

// InnerCtx is the inner side ctx used in index lookup join
type InnerCtx struct {
	ReaderBuilder IndexJoinExecutorBuilder
	RowTypes      []*types.FieldType
	KeyCols       []int
	KeyColIDs     []int64 // the original ID in its table, used by dynamic partition pruning
	KeyCollators  []collate.Collator
	HashTypes     []*types.FieldType
	HashCols      []int
	HashCollators []collate.Collator
	// HashIsNullEQ marks which hash keys are null-safe equal (<=>).
	// The slice aligns with HashCols; positions corresponding to join keys can be true.
	HashIsNullEQ []bool
	ColLens      []int
	HasPrefixCol bool
}

type lookUpJoinTask struct {
	outerResult *chunk.List
	outerMatch  [][]bool

	innerResult       *chunk.List
	encodedLookUpKeys []*chunk.Chunk
	lookupMap         *mvmap.MVMap
	matchedInners     []chunk.Row
	innerExec         exec.Executor

	doneCh   chan error
	cursor   chunk.RowPtr
	hasMatch bool
	hasNull  bool

	memTracker *memory.Tracker // track memory usage.
}

type outerWorker struct {
	OuterCtx

	lookup *IndexLookUpJoin

	ctx      sessionctx.Context
	executor exec.Executor

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpJoinTask
	innerCh  chan<- *lookUpJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	InnerCtx

	taskCh   <-chan *lookUpJoinTask
	outerCtx OuterCtx
	ctx      sessionctx.Context
	lookup   *IndexLookUpJoin

	indexRanges           []*ranger.Range
	nextColCompareFilters *physicalop.ColWithCmpFuncManager
	keyOff2IdxOff         []int
	maxFetchSize          int
	stats                 *innerWorkerRuntimeStats
	memTracker            *memory.Tracker
}

// Open implements the Executor interface.
func (e *IndexLookUpJoin) Open(ctx context.Context) error {
	err := exec.Open(ctx, e.Children(0))
	if err != nil {
		return err
	}
	if len(e.InnerCtx.HashIsNullEQ) != len(e.InnerCtx.HashCols) {
		return errors.New("index lookup join: hash null-eq flags length must match hash cols length")
	}
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.Finished.Store(false)
	if e.RuntimeStats() != nil {
		e.stats = &indexLookUpJoinRuntimeStats{}
	}
	e.cancelFunc = nil
	return nil
}

func (e *IndexLookUpJoin) startWorkers(ctx context.Context, initBatchSize int) {
	concurrency := e.Ctx().GetSessionVars().IndexLookupJoinConcurrency()
	if e.stats != nil {
		e.stats.concurrency = concurrency
	}
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.WorkerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh, initBatchSize).run(workerCtx, e.WorkerWg)
	for range concurrency {
		innerWorker := e.newInnerWorker(innerCh)
		e.WorkerWg.Add(1)
		go innerWorker.run(workerCtx, e.WorkerWg)
	}
}

func (e *IndexLookUpJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask, initBatchSize int) *outerWorker {
	maxBatchSize := e.Ctx().GetSessionVars().IndexJoinBatchSize
	batchSize := min(initBatchSize, maxBatchSize)
	ow := &outerWorker{
		OuterCtx:         e.OuterCtx,
		ctx:              e.Ctx(),
		executor:         e.Children(0),
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        batchSize,
		maxBatchSize:     maxBatchSize,
		parentMemTracker: e.memTracker,
		lookup:           e,
	}
	return ow
}

func (e *IndexLookUpJoin) newInnerWorker(taskCh chan *lookUpJoinTask) *innerWorker {
	// Since multiple inner workers run concurrently, we should copy join's IndexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.IndexRanges.Range()))
	for _, ran := range e.IndexRanges.Range() {
		copiedRanges = append(copiedRanges, ran.Clone())
	}

	var innerStats *innerWorkerRuntimeStats
	if e.stats != nil {
		innerStats = &e.stats.innerWorker
	}
	iw := &innerWorker{
		InnerCtx:      e.InnerCtx,
		outerCtx:      e.OuterCtx,
		taskCh:        taskCh,
		ctx:           e.Ctx(),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.KeyOff2IdxOff,
		stats:         innerStats,
		lookup:        e,
		memTracker:    memory.NewTracker(memory.LabelForIndexJoinInnerWorker, -1),
	}
	failpoint.Inject("inlNewInnerPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("test inlNewInnerPanic")
		}
	})
	iw.memTracker.AttachTo(e.memTracker)
	if len(copiedRanges) != 0 {
		// We should not consume this memory usage in `iw.memTracker`. The
		// memory usage of inner worker will be reset the end of iw.handleTask.
		// While the life cycle of this memory consumption exists throughout the
		// whole active period of inner worker.
		e.Ctx().GetSessionVars().StmtCtx.MemTracker.Consume(2 * types.EstimatedMemUsage(copiedRanges[0].LowVal, len(copiedRanges)))
	}
	if e.LastColHelper != nil {
		// nextCwf.TmpConstant needs to be reset for every individual
		// inner worker to avoid data race when the inner workers is running
		// concurrently.
		nextCwf := *e.LastColHelper
		nextCwf.TmpConstant = make([]*expression.Constant, len(e.LastColHelper.TmpConstant))
		for i := range e.LastColHelper.TmpConstant {
			nextCwf.TmpConstant[i] = &expression.Constant{RetType: nextCwf.TargetCol.RetType}
		}
		iw.nextColCompareFilters = &nextCwf
	}
	return iw
}

// Next implements the Executor interface.
func (e *IndexLookUpJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.startWorkers(ctx, req.RequiredRows())
		e.prepared = true
	}
	if e.IsOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()
	e.JoinResult.Reset()
	for {
		task, err := e.getFinishedTask(ctx)
		if err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		startTime := time.Now()
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			e.lookUpMatchedInners(task, task.cursor)
			if e.innerIter == nil {
				e.innerIter = chunk.NewIterator4Slice(task.matchedInners)
			}
			e.innerIter.Reset(task.matchedInners)
			e.innerIter.Begin()
		}

		outerRow := task.outerResult.GetRow(task.cursor)
		if e.innerIter.Current() != e.innerIter.End() {
			matched, isNull, err := e.Joiner.TryToMatchInners(outerRow, e.innerIter, req)
			if err != nil {
				return err
			}
			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull
		}
		if e.innerIter.Current() == e.innerIter.End() {
			if !task.hasMatch {
				e.Joiner.OnMissMatch(task.hasNull, outerRow, req)
			}
			task.cursor.RowIdx++
			if int(task.cursor.RowIdx) == task.outerResult.GetChunk(int(task.cursor.ChkIdx)).NumRows() {
				task.cursor.ChkIdx++
				task.cursor.RowIdx = 0
			}
			task.hasMatch = false
			task.hasNull = false
		}
		if e.stats != nil {
			atomic.AddInt64(&e.stats.probe, int64(time.Since(startTime)))
		}
		if req.IsFull() {
			return nil
		}
	}
}

func (e *IndexLookUpJoin) getFinishedTask(ctx context.Context) (*lookUpJoinTask, error) {
	task := e.task
	if task != nil && int(task.cursor.ChkIdx) < task.outerResult.NumChunks() {
		return task, nil
	}

	// The previous task has been processed, so release the occupied memory
	if task != nil {
		task.memTracker.Detach()
	}
	select {
	case task = <-e.resultCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if task == nil {
		return nil, nil
	}

	select {
	case err := <-task.doneCh:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	e.task = task
	return task, nil
}

func (e *IndexLookUpJoin) lookUpMatchedInners(task *lookUpJoinTask, rowPtr chunk.RowPtr) {
	outerKey := task.encodedLookUpKeys[rowPtr.ChkIdx].GetRow(int(rowPtr.RowIdx)).GetBytes(0)
	e.innerPtrBytes = task.lookupMap.Get(outerKey, e.innerPtrBytes[:0])
	task.matchedInners = task.matchedInners[:0]

	for _, b := range e.innerPtrBytes {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := task.innerResult.GetRow(ptr)
		task.matchedInners = append(task.matchedInners, matchedInner)
	}
}

func (ow *outerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer trace.StartRegion(ctx, "IndexLookupJoinOuterWorker").End()
	defer func() {
		if r := recover(); r != nil {
			ow.lookup.Finished.Store(true)
			logutil.Logger(ctx).Warn("outerWorker panicked", zap.Any("recover", r), zap.Stack("stack"))
			task := &lookUpJoinTask{doneCh: make(chan error, 1)}
			err := util.GetRecoverError(r)
			task.doneCh <- err
			ow.pushToChan(ctx, task, ow.resultCh)
		}
		close(ow.resultCh)
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		failpoint.Inject("TestIssue30211", nil)
		failpoint.Inject("ConsumeRandomPanic", nil)
		task, err := ow.buildTask(ctx)
		if err != nil {
			task.doneCh <- err
			ow.pushToChan(ctx, task, ow.resultCh)
			return
		}
		if task == nil {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.resultCh); finished {
			return
		}
	}
}

func (*outerWorker) pushToChan(ctx context.Context, task *lookUpJoinTask, dst chan<- *lookUpJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

// newList creates a new List to buffer current executor's result.
func newList(e exec.Executor) *chunk.List {
	return chunk.NewList(e.RetFieldTypes(), e.InitCap(), e.MaxChunkSize())
}

// buildTask builds a lookUpJoinTask and read Outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*lookUpJoinTask, error) {
	task := &lookUpJoinTask{
		doneCh:      make(chan error, 1),
		outerResult: newList(ow.executor),
		lookupMap:   mvmap.NewMVMap(),
	}
	task.memTracker = memory.NewTracker(-1, -1)
	task.outerResult.GetMemTracker().AttachTo(task.memTracker)
	task.memTracker.AttachTo(ow.parentMemTracker)
	failpoint.Inject("ConsumeRandomPanic", nil)

	ow.increaseBatchSize()
	requiredRows := ow.batchSize
	if ow.lookup.IsOuterJoin {
		// If it is outerJoin, push the requiredRows down.
		// Note: buildTask is triggered when `Open` is called, but
		// ow.lookup.requiredRows is set when `Next` is called. Thus we check
		// whether it's 0 here.
		if parentRequired := int(atomic.LoadInt64(&ow.lookup.requiredRows)); parentRequired != 0 {
			requiredRows = parentRequired
		}
	}
	maxChunkSize := ow.ctx.GetSessionVars().MaxChunkSize
	for requiredRows > task.outerResult.Len() {
		chk := ow.executor.NewChunkWithCapacity(ow.OuterCtx.RowTypes, requiredRows, maxChunkSize)
		chk = chk.SetRequiredRows(requiredRows, maxChunkSize)
		err := exec.Next(ctx, ow.executor, chk)
		if err != nil {
			return task, err
		}
		if chk.NumRows() == 0 {
			break
		}

		task.outerResult.Add(chk)
	}
	if task.outerResult.Len() == 0 {
		return nil, nil
	}
	numChks := task.outerResult.NumChunks()
	if ow.Filter != nil {
		task.outerMatch = make([][]bool, task.outerResult.NumChunks())
		var err error
		exprCtx := ow.ctx.GetExprCtx()
		for i := range numChks {
			chk := task.outerResult.GetChunk(i)
			outerMatch := make([]bool, 0, chk.NumRows())
			task.memTracker.Consume(int64(cap(outerMatch)))
			task.outerMatch[i], err = expression.VectorizedFilter(exprCtx.GetEvalCtx(), ow.ctx.GetSessionVars().EnableVectorizedExpression, ow.Filter, chunk.NewIterator4Chunk(chk), outerMatch)
			if err != nil {
				return task, err
			}
		}
	}
	task.encodedLookUpKeys = make([]*chunk.Chunk, task.outerResult.NumChunks())
	for i := range task.encodedLookUpKeys {
		task.encodedLookUpKeys[i] = ow.executor.NewChunkWithCapacity(
			[]*types.FieldType{types.NewFieldType(mysql.TypeBlob)},
			task.outerResult.GetChunk(i).NumRows(),
			task.outerResult.GetChunk(i).NumRows(),
		)
	}
	return task, nil
}

func (ow *outerWorker) increaseBatchSize() {
	if ow.batchSize < ow.maxBatchSize {
		ow.batchSize *= 2
	}
	if ow.batchSize > ow.maxBatchSize {
		ow.batchSize = ow.maxBatchSize
	}
}

