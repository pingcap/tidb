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

package executor

import (
	"bytes"
	"context"
	"runtime"
	"runtime/trace"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

var _ Executor = &IndexLookUpJoin{}

// IndexLookUpJoin employs one outer worker and N innerWorkers to execute concurrently.
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, builds inner row hash map.
// 3. main thread receives the task, waits for inner worker finish handling the task.
// 4. main thread join each outer row by look up the inner rows hash map in the task.
type IndexLookUpJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	task       *lookUpJoinTask
	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner      joiner
	isOuterJoin bool

	requiredRows int64

	indexRanges   ranger.MutableRanges
	keyOff2IdxOff []int
	innerPtrBytes [][]byte

	// lastColHelper store the information for last col if there's complicated filter like col > x_col and col < x_col + 100.
	lastColHelper *plannercore.ColWithCmpFuncManager

	memTracker *memory.Tracker // track memory usage.

	stats    *indexLookUpJoinRuntimeStats
	finished *atomic.Value
	prepared bool
}

type outerCtx struct {
	rowTypes  []*types.FieldType
	keyCols   []int
	hashTypes []*types.FieldType
	hashCols  []int
	filter    expression.CNFExprs
}

type innerCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
	keyColIDs     []int64 // the original ID in its table, used by dynamic partition pruning
	keyCollators  []collate.Collator
	hashTypes     []*types.FieldType
	hashCols      []int
	hashCollators []collate.Collator
	colLens       []int
	hasPrefixCol  bool
}

type lookUpJoinTask struct {
	outerResult *chunk.List
	outerMatch  [][]bool

	innerResult       *chunk.List
	encodedLookUpKeys []*chunk.Chunk
	lookupMap         *mvmap.MVMap
	matchedInners     []chunk.Row

	doneCh   chan error
	cursor   chunk.RowPtr
	hasMatch bool
	hasNull  bool

	memTracker *memory.Tracker // track memory usage.
}

type outerWorker struct {
	outerCtx

	lookup *IndexLookUpJoin

	ctx      sessionctx.Context
	executor Executor

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpJoinTask
	innerCh  chan<- *lookUpJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *lookUpJoinTask
	outerCtx    outerCtx
	ctx         sessionctx.Context
	executorChk *chunk.Chunk
	lookup      *IndexLookUpJoin

	indexRanges           []*ranger.Range
	nextColCompareFilters *plannercore.ColWithCmpFuncManager
	keyOff2IdxOff         []int
	stats                 *innerWorkerRuntimeStats
	memTracker            *memory.Tracker
}

// Open implements the Executor interface.
func (e *IndexLookUpJoin) Open(ctx context.Context) error {
	err := e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.finished.Store(false)
	if e.runtimeStats != nil {
		e.stats = &indexLookUpJoinRuntimeStats{}
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	e.cancelFunc = nil
	return nil
}

func (e *IndexLookUpJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency()
	if e.stats != nil {
		e.stats.concurrency = concurrency
	}
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerWorker(innerCh).run(workerCtx, e.workerWg)
	}
}

func (e *IndexLookUpJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask) *outerWorker {
	ow := &outerWorker{
		outerCtx:         e.outerCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
		lookup:           e,
	}
	return ow
}

func (e *IndexLookUpJoin) newInnerWorker(taskCh chan *lookUpJoinTask) *innerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges.Range()))
	for _, ran := range e.indexRanges.Range() {
		copiedRanges = append(copiedRanges, ran.Clone())
	}

	var innerStats *innerWorkerRuntimeStats
	if e.stats != nil {
		innerStats = &e.stats.innerWorker
	}
	iw := &innerWorker{
		innerCtx:      e.innerCtx,
		outerCtx:      e.outerCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
		stats:         innerStats,
		lookup:        e,
		memTracker:    memory.NewTracker(memory.LabelForIndexJoinInnerWorker, -1),
	}
	iw.memTracker.AttachTo(e.memTracker)
	if len(copiedRanges) != 0 {
		// We should not consume this memory usage in `iw.memTracker`. The
		// memory usage of inner worker will be reset the end of iw.handleTask.
		// While the life cycle of this memory consumption exists throughout the
		// whole active period of inner worker.
		e.ctx.GetSessionVars().StmtCtx.MemTracker.Consume(2 * types.EstimatedMemUsage(copiedRanges[0].LowVal, len(copiedRanges)))
	}
	if e.lastColHelper != nil {
		// nextCwf.TmpConstant needs to be reset for every individual
		// inner worker to avoid data race when the inner workers is running
		// concurrently.
		nextCwf := *e.lastColHelper
		nextCwf.TmpConstant = make([]*expression.Constant, len(e.lastColHelper.TmpConstant))
		for i := range e.lastColHelper.TmpConstant {
			nextCwf.TmpConstant[i] = &expression.Constant{RetType: nextCwf.TargetCol.RetType}
		}
		iw.nextColCompareFilters = &nextCwf
	}
	return iw
}

// Next implements the Executor interface.
func (e *IndexLookUpJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.startWorkers(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()
	e.joinResult.Reset()
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
			e.innerIter = chunk.NewIterator4Slice(task.matchedInners)
			e.innerIter.Begin()
		}

		outerRow := task.outerResult.GetRow(task.cursor)
		if e.innerIter.Current() != e.innerIter.End() {
			matched, isNull, err := e.joiner.tryToMatchInners(outerRow, e.innerIter, req)
			if err != nil {
				return err
			}
			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull
		}
		if e.innerIter.Current() == e.innerIter.End() {
			if !task.hasMatch {
				e.joiner.onMissMatch(task.hasNull, outerRow, req)
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
			ow.lookup.finished.Store(true)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("outerWorker panicked", zap.String("stack", string(buf)))
			task := &lookUpJoinTask{doneCh: make(chan error, 1)}
			err := errors.Errorf("%v", r)
			task.doneCh <- err
			ow.pushToChan(ctx, task, ow.resultCh)
		}
		close(ow.resultCh)
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		failpoint.Inject("TestIssue30211", nil)
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

func (ow *outerWorker) pushToChan(ctx context.Context, task *lookUpJoinTask, dst chan<- *lookUpJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

// buildTask builds a lookUpJoinTask and read outer rows.
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

	ow.increaseBatchSize()
	requiredRows := ow.batchSize
	if ow.lookup.isOuterJoin {
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
		chk := chunk.NewChunkWithCapacity(ow.outerCtx.rowTypes, maxChunkSize)
		chk = chk.SetRequiredRows(requiredRows, maxChunkSize)
		err := Next(ctx, ow.executor, chk)
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
	if ow.filter != nil {
		task.outerMatch = make([][]bool, task.outerResult.NumChunks())
		var err error
		for i := 0; i < numChks; i++ {
			chk := task.outerResult.GetChunk(i)
			outerMatch := make([]bool, 0, chk.NumRows())
			task.memTracker.Consume(int64(cap(outerMatch)))
			task.outerMatch[i], err = expression.VectorizedFilter(ow.ctx, ow.filter, chunk.NewIterator4Chunk(chk), outerMatch)
			if err != nil {
				return task, err
			}
		}
	}
	task.encodedLookUpKeys = make([]*chunk.Chunk, task.outerResult.NumChunks())
	for i := range task.encodedLookUpKeys {
		task.encodedLookUpKeys[i] = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, task.outerResult.GetChunk(i).NumRows())
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

func (iw *innerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer trace.StartRegion(ctx, "IndexLookupJoinInnerWorker").End()
	var task *lookUpJoinTask
	defer func() {
		if r := recover(); r != nil {
			iw.lookup.finished.Store(true)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("innerWorker panicked", zap.String("stack", string(buf)))
			err := errors.Errorf("%v", r)
			// "task != nil" is guaranteed when panic happened.
			task.doneCh <- err
		}
		wg.Done()
	}()

	for ok := true; ok; {
		select {
		case task, ok = <-iw.taskCh:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		err := iw.handleTask(ctx, task)
		task.doneCh <- err
	}
}

type indexJoinLookUpContent struct {
	keys      []types.Datum
	row       chunk.Row
	keyCols   []int
	keyColIDs []int64 // the original ID in its table, used by dynamic partition pruning
}

func (iw *innerWorker) handleTask(ctx context.Context, task *lookUpJoinTask) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.totalTime, int64(time.Since(start)))
		}()
	}
	defer func() {
		iw.memTracker.Consume(-iw.memTracker.BytesConsumed())
	}()
	lookUpContents, err := iw.constructLookupContent(task)
	if err != nil {
		return err
	}
	err = iw.fetchInnerResults(ctx, task, lookUpContents)
	if err != nil {
		return err
	}
	err = iw.buildLookUpMap(task)
	if err != nil {
		return err
	}
	return nil
}

func (iw *innerWorker) constructLookupContent(task *lookUpJoinTask) ([]*indexJoinLookUpContent, error) {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.task, 1)
			atomic.AddInt64(&iw.stats.construct, int64(time.Since(start)))
		}()
	}
	lookUpContents := make([]*indexJoinLookUpContent, 0, task.outerResult.Len())
	keyBuf := make([]byte, 0, 64)
	for chkIdx := 0; chkIdx < task.outerResult.NumChunks(); chkIdx++ {
		chk := task.outerResult.GetChunk(chkIdx)
		numRows := chk.NumRows()
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			dLookUpKey, dHashKey, err := iw.constructDatumLookupKey(task, chkIdx, rowIdx)
			if err != nil {
				if terror.ErrorEqual(err, types.ErrWrongValue) {
					// We ignore rows with invalid datetime.
					task.encodedLookUpKeys[chkIdx].AppendNull(0)
					continue
				}
				return nil, err
			}
			if rowIdx == 0 {
				iw.lookup.memTracker.Consume(types.EstimatedMemUsage(dLookUpKey, numRows))
			}
			if dHashKey == nil {
				// Append null to make lookUpKeys the same length as outer Result.
				task.encodedLookUpKeys[chkIdx].AppendNull(0)
				continue
			}
			keyBuf = keyBuf[:0]
			keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, dHashKey...)
			if err != nil {
				if terror.ErrorEqual(err, types.ErrWrongValue) {
					// we ignore rows with invalid datetime
					task.encodedLookUpKeys[chkIdx].AppendNull(0)
					continue
				}
				return nil, err
			}
			// Store the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
			task.encodedLookUpKeys[chkIdx].AppendBytes(0, keyBuf)
			if iw.hasPrefixCol {
				for i, outerOffset := range iw.keyOff2IdxOff {
					// If it's a prefix column. Try to fix it.
					joinKeyColPrefixLen := iw.colLens[outerOffset]
					if joinKeyColPrefixLen != types.UnspecifiedLength {
						ranger.CutDatumByPrefixLen(&dLookUpKey[i], joinKeyColPrefixLen, iw.rowTypes[iw.keyCols[i]])
					}
				}
				// dLookUpKey is sorted and deduplicated at sortAndDedupLookUpContents.
				// So we don't need to do it here.
			}
			lookUpContents = append(lookUpContents, &indexJoinLookUpContent{keys: dLookUpKey, row: chk.GetRow(rowIdx), keyCols: iw.keyCols, keyColIDs: iw.keyColIDs})
		}
	}

	for i := range task.encodedLookUpKeys {
		task.memTracker.Consume(task.encodedLookUpKeys[i].MemoryUsage())
	}
	lookUpContents = iw.sortAndDedupLookUpContents(lookUpContents)
	return lookUpContents, nil
}

func (iw *innerWorker) constructDatumLookupKey(task *lookUpJoinTask, chkIdx, rowIdx int) ([]types.Datum, []types.Datum, error) {
	if task.outerMatch != nil && !task.outerMatch[chkIdx][rowIdx] {
		return nil, nil, nil
	}
	outerRow := task.outerResult.GetChunk(chkIdx).GetRow(rowIdx)
	sc := iw.ctx.GetSessionVars().StmtCtx
	keyLen := len(iw.keyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	dHashKey := make([]types.Datum, 0, len(iw.hashCols))
	for i, hashCol := range iw.outerCtx.hashCols {
		outerValue := outerRow.GetDatum(hashCol, iw.outerCtx.rowTypes[hashCol])
		// Join-on-condition can be promised to be equal-condition in
		// IndexNestedLoopJoin, thus the filter will always be false if
		// outerValue is null, and we don't need to lookup it.
		if outerValue.IsNull() {
			return nil, nil, nil
		}
		innerColType := iw.rowTypes[iw.hashCols[i]]
		innerValue, err := outerValue.ConvertTo(sc, innerColType)
		if err != nil && !(terror.ErrorEqual(err, types.ErrTruncated) && (innerColType.Tp == mysql.TypeSet || innerColType.Tp == mysql.TypeEnum)) {
			// If the converted outerValue overflows or invalid to innerValue, we don't need to lookup it.
			if terror.ErrorEqual(err, types.ErrOverflow) || terror.ErrorEqual(err, types.ErrWarnDataOutOfRange) {
				return nil, nil, nil
			}
			return nil, nil, err
		}
		cmp, err := outerValue.Compare(sc, &innerValue, iw.hashCollators[i])
		if err != nil {
			return nil, nil, err
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil, nil
		}
		if i < keyLen {
			dLookupKey = append(dLookupKey, innerValue)
		}
		dHashKey = append(dHashKey, innerValue)
	}
	return dLookupKey, dHashKey, nil
}

func (iw *innerWorker) sortAndDedupLookUpContents(lookUpContents []*indexJoinLookUpContent) []*indexJoinLookUpContent {
	if len(lookUpContents) < 2 {
		return lookUpContents
	}
	sc := iw.ctx.GetSessionVars().StmtCtx
	sort.Slice(lookUpContents, func(i, j int) bool {
		cmp := compareRow(sc, lookUpContents[i].keys, lookUpContents[j].keys, iw.keyCollators)
		if cmp != 0 || iw.nextColCompareFilters == nil {
			return cmp < 0
		}
		return iw.nextColCompareFilters.CompareRow(lookUpContents[i].row, lookUpContents[j].row) < 0
	})
	deDupedLookupKeys := lookUpContents[:1]
	for i := 1; i < len(lookUpContents); i++ {
		cmp := compareRow(sc, lookUpContents[i].keys, lookUpContents[i-1].keys, iw.keyCollators)
		if cmp != 0 || (iw.nextColCompareFilters != nil && iw.nextColCompareFilters.CompareRow(lookUpContents[i].row, lookUpContents[i-1].row) != 0) {
			deDupedLookupKeys = append(deDupedLookupKeys, lookUpContents[i])
		}
	}
	return deDupedLookupKeys
}

func compareRow(sc *stmtctx.StatementContext, left, right []types.Datum, ctors []collate.Collator) int {
	for idx := 0; idx < len(left); idx++ {
		cmp, err := left[idx].Compare(sc, &right[idx], ctors[idx])
		// We only compare rows with the same type, no error to return.
		terror.Log(err)
		if cmp > 0 {
			return 1
		} else if cmp < 0 {
			return -1
		}
	}
	return 0
}

func (iw *innerWorker) fetchInnerResults(ctx context.Context, task *lookUpJoinTask, lookUpContent []*indexJoinLookUpContent) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.fetch, int64(time.Since(start)))
		}()
	}
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(ctx, lookUpContent, iw.indexRanges, iw.keyOff2IdxOff, iw.nextColCompareFilters, true, iw.memTracker, iw.lookup.finished)
	if innerExec != nil {
		defer terror.Call(innerExec.Close)
	}
	if err != nil {
		return err
	}

	innerResult := chunk.NewList(retTypes(innerExec), iw.ctx.GetSessionVars().MaxChunkSize, iw.ctx.GetSessionVars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel(memory.LabelForBuildSideResult)
	innerResult.GetMemTracker().AttachTo(task.memTracker)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err := Next(ctx, innerExec, iw.executorChk)
		if err != nil {
			return err
		}
		if iw.executorChk.NumRows() == 0 {
			break
		}
		innerResult.Add(iw.executorChk)
		iw.executorChk = newFirstChunk(innerExec)
	}
	task.innerResult = innerResult
	return nil
}

func (iw *innerWorker) buildLookUpMap(task *lookUpJoinTask) error {
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.build, int64(time.Since(start)))
		}()
	}
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.innerResult.NumChunks(); i++ {
		chk := task.innerResult.GetChunk(i)
		for j := 0; j < chk.NumRows(); j++ {
			innerRow := chk.GetRow(j)
			if iw.hasNullInJoinKey(innerRow) {
				continue
			}

			keyBuf = keyBuf[:0]
			for _, keyCol := range iw.hashCols {
				d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
				var err error
				keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, d)
				if err != nil {
					return err
				}
			}
			rowPtr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			task.lookupMap.Put(keyBuf, valBuf)
		}
	}
	return nil
}

func (iw *innerWorker) hasNullInJoinKey(row chunk.Row) bool {
	for _, ordinal := range iw.hashCols {
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// Close implements the Executor interface.
func (e *IndexLookUpJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.workerWg.Wait()
	e.memTracker = nil
	e.task = nil
	e.finished.Store(false)
	e.prepared = false
	return e.baseExecutor.Close()
}

type indexLookUpJoinRuntimeStats struct {
	concurrency int
	probe       int64
	innerWorker innerWorkerRuntimeStats
}

type innerWorkerRuntimeStats struct {
	totalTime int64
	task      int64
	construct int64
	fetch     int64
	build     int64
	join      int64
}

func (e *indexLookUpJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if e.innerWorker.totalTime > 0 {
		buf.WriteString("inner:{total:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.innerWorker.totalTime)))
		buf.WriteString(", concurrency:")
		if e.concurrency > 0 {
			buf.WriteString(strconv.Itoa(e.concurrency))
		} else {
			buf.WriteString("OFF")
		}
		buf.WriteString(", task:")
		buf.WriteString(strconv.FormatInt(e.innerWorker.task, 10))
		buf.WriteString(", construct:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.innerWorker.construct)))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.innerWorker.fetch)))
		buf.WriteString(", build:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.innerWorker.build)))
		if e.innerWorker.join > 0 {
			buf.WriteString(", join:")
			buf.WriteString(execdetails.FormatDuration(time.Duration(e.innerWorker.join)))
		}
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.probe)))
	}
	return buf.String()
}

func (e *indexLookUpJoinRuntimeStats) Clone() execdetails.RuntimeStats {
	return &indexLookUpJoinRuntimeStats{
		concurrency: e.concurrency,
		probe:       e.probe,
		innerWorker: e.innerWorker,
	}
}

func (e *indexLookUpJoinRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*indexLookUpJoinRuntimeStats)
	if !ok {
		return
	}
	e.probe += tmp.probe
	e.innerWorker.totalTime += tmp.innerWorker.totalTime
	e.innerWorker.task += tmp.innerWorker.task
	e.innerWorker.construct += tmp.innerWorker.construct
	e.innerWorker.fetch += tmp.innerWorker.fetch
	e.innerWorker.build += tmp.innerWorker.build
	e.innerWorker.join += tmp.innerWorker.join
}

// Tp implements the RuntimeStats interface.
func (e *indexLookUpJoinRuntimeStats) Tp() int {
	return execdetails.TpIndexLookUpJoinRuntimeStats
}
