package executor

import (
	"context"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"sort"
)

//IndexNestedLoopHashJoin is the hash join executor
type IndexNestedLoopHashJoin struct {
	IndexLookUpJoin
	resultCh          chan *indexLookUpResult
	joinChkResourceCh []chan *chunk.Chunk
}

type indexHashJoinOuterWorker struct {
	outerWorker
	innerCh chan *indexHashJoinTask
}

type indexHashJoinInnerWorker struct {
	innerWorker
	matchedOuterPtrs  [][]byte
	joiner            joiner
	joinChkResourceCh chan *chunk.Chunk
	resultCh          chan *indexLookUpResult
	taskCh            <-chan *indexHashJoinTask
}

type outerRowStatusFlag byte

const (
	outerRowUnmatched outerRowStatusFlag = iota
	outerRowMatched
	outerRowHasNull
)

type indexHashJoinTask struct {
	*lookUpJoinTask
	// nullOuterRowIdx indicates the offset of the outer rows which contains null
	// join key(s).
	outerRowStatus     []outerRowStatusFlag
	nullOuterRowIdx    set.IntSet
	matchedOuterRowIdx set.IntSet
}

// Open implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Open(ctx context.Context) error {
	// Be careful, very dirty hack in this line!!!
	// IndexLookUpJoin need to rebuild executor (the dataReaderBuilder) during
	// executing. However `executor.Next()` is lazy evaluation when the RecordSet
	// result is drained.
	// Lazy evaluation means the saved session context may change during executor's
	// building and its running.
	// A specific sequence for example:
	//
	// e := buildExecutor()   // txn at build time
	// recordSet := runStmt(e)
	// session.CommitTxn()    // txn closed
	// recordSet.Next()
	// e.dataReaderBuilder.Build() // txn is used again, which is already closed
	//
	// The trick here is `getStartTS` will cache start ts in the dataReaderBuilder,
	// so even txn is destroyed later, the dataReaderBuilder could still use the
	// cached start ts to construct DAG.
	_, err := e.innerCtx.readerBuilder.getStartTS()
	if err != nil {
		return err
	}

	err = e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexNestedLoopHashJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *indexHashJoinTask, concurrency)
	e.workerWg.Add(1)
	ow := e.newOuterWorker(innerCh)
	go util.WithRecovery(func() { ow.run(workerCtx) }, e.finishJoinWorkers)

	e.resultCh = make(chan *indexLookUpResult, concurrency+1)
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := int(0); i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}
	e.workerWg.Add(concurrency)
	for i := int(0); i < concurrency; i++ {
		workerID := i
		go util.WithRecovery(func() { e.newInnerWorker(innerCh, workerID).run(workerCtx) }, e.finishJoinWorkers)
	}
	go util.WithRecovery(func() { e.workerWg.Wait() }, e.finishWaiter)
}

func (e *IndexNestedLoopHashJoin) finishJoinWorkers(r interface{}) {
	if r != nil {
		e.resultCh <- &indexLookUpResult{err: errors.Errorf("%v", r)}
	}
	e.workerWg.Done()
}

func (e *IndexNestedLoopHashJoin) finishWaiter(r interface{}) {
	if r != nil {
		e.resultCh <- &indexLookUpResult{err: errors.Errorf("%v", r)}
	}
	close(e.resultCh)
}

// Next implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), req.NumRows()) }()
	}
	req.Reset()
	result, ok := <-e.resultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
		e.cancelFunc = nil
	}
	if e.resultCh != nil {
		for range e.resultCh {
		}
		e.resultCh = nil
	}
	for i := range e.joinChkResourceCh {
		close(e.joinChkResourceCh[i])
		for range e.joinChkResourceCh[i] {
		}
	}
	e.joinChkResourceCh = nil
	return e.children[0].Close()
}

func (ow *indexHashJoinOuterWorker) run(ctx context.Context) {
	defer close(ow.innerCh)
	for {
		task, err := ow.buildTask(ctx)
		if task == nil {
			return
		}
		if err != nil {
			task.buildError = err
			ow.pushToChan(ctx, task, ow.innerCh)
			return
		}
		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}
	}
}

func (ow *indexHashJoinOuterWorker) buildTask(ctx context.Context) (*indexHashJoinTask, error) {
	task, err := ow.outerWorker.buildTask(ctx)
	if err != nil {
		return nil, err
	}
	return &indexHashJoinTask{
		lookUpJoinTask: task,
		outerRowStatus: make([]outerRowStatusFlag, task.outerResult.NumRows()),
	}, nil
}

func (ow *indexHashJoinOuterWorker) pushToChan(ctx context.Context, task *indexHashJoinTask, dst chan<- *indexHashJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

func (e *IndexNestedLoopHashJoin) newOuterWorker(innerCh chan *indexHashJoinTask) *indexHashJoinOuterWorker {
	ow := &indexHashJoinOuterWorker{
		outerWorker: outerWorker{
			outerCtx:         e.outerCtx,
			ctx:              e.ctx,
			executor:         e.children[0],
			executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
			batchSize:        32,
			maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
			parentMemTracker: e.memTracker,
			lookup:           &e.IndexLookUpJoin,
		},
		innerCh: innerCh,
	}
	return ow
}

func (e *IndexNestedLoopHashJoin) newInnerWorker(taskCh chan *indexHashJoinTask, workerID int) *indexHashJoinInnerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	iw := &indexHashJoinInnerWorker{
		innerWorker: innerWorker{
			innerCtx:      e.innerCtx,
			outerCtx:      e.outerCtx,
			ctx:           e.ctx,
			executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
			indexRanges:   copiedRanges,
			keyOff2IdxOff: e.keyOff2IdxOff,
		},
		taskCh:            taskCh,
		joiner:            e.joiner,
		joinChkResourceCh: e.joinChkResourceCh[workerID],
		resultCh:          e.resultCh,
		matchedOuterPtrs:  make([][]byte, 0, 8),
	}

	return iw
}

func (iw *indexHashJoinInnerWorker) run(ctx context.Context) {
	var task *indexHashJoinTask
	ok, joinResult := iw.getNewJoinResult()
	if !ok {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok = <-iw.taskCh:
		}
		if !ok {
			break
		}
		if task.buildError != nil {
			joinResult.err = task.buildError
			break
		}
		err := iw.handleTask(ctx, task, joinResult)
		if err != nil {
			joinResult.err = err
			break
		}
	}
	if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		iw.resultCh <- joinResult
	}
}

func (iw *indexHashJoinInnerWorker) getNewJoinResult() (bool, *indexLookUpResult) {
	joinResult := &indexLookUpResult{
		src: iw.joinChkResourceCh,
	}
	ok := true
	select {
	case joinResult.chk, ok = <-iw.joinChkResourceCh:
	}
	return ok, joinResult
}

func (iw *indexHashJoinInnerWorker) buildHashTableForOuterResult(task *indexHashJoinTask) (err error) {
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for rowIdx, numRows := 0, task.outerResult.NumRows(); rowIdx < numRows; rowIdx++ {
		var hasNull bool
		if task.outerMatch != nil && !task.outerMatch[rowIdx] {
			continue
		}
		row := task.outerResult.GetRow(rowIdx)
		keyColIdx := iw.outerCtx.keyCols
		for _, i := range keyColIdx {
			if row.IsNull(i) {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}
		keyBuf, err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, keyBuf[:0], row, iw.outerCtx.rowTypes, keyColIdx)
		if err != nil {
			break
		}
		*(*int)(unsafe.Pointer(&valBuf[0])) = rowIdx
		task.lookupMap.Put(keyBuf, valBuf)
	}
	return err
}

func (iw *indexHashJoinInnerWorker) constructSortedLookupContent(task *indexHashJoinTask) (lookUpContents []*indexJoinLookUpContent, err error) {
	lookUpContents = make([]*indexJoinLookUpContent, 0, task.lookupMap.Len())
	keyBuf := make([]byte, 0, 64)
	iter := task.lookupMap.NewIterator()
	for _, value := iter.Next(); value != nil; _, value = iter.Next() {
		rowIdx := *(*int)(unsafe.Pointer(&value[0]))
		outerRow := task.outerResult.GetRow(rowIdx)
		dLookUpKey, err := iw.constructDatumLookupKey(task.lookUpJoinTask, outerRow)
		if err != nil {
			return nil, err
		}
		//if dLookUpKey == nil {
		//	// Append null to make looUpKeys the same length as outer Result.
		//	task.encodedLookUpKeys.AppendNull(0)
		//	continue
		//}

		keyBuf = keyBuf[:0]
		keyBuf, err = codec.HashValues(iw.ctx.GetSessionVars().StmtCtx, keyBuf, dLookUpKey...)
		if err != nil {
			return nil, err
		}
		lookUpContents = append(lookUpContents, &indexJoinLookUpContent{keys: dLookUpKey, row: task.outerResult.GetRow(rowIdx)})
	}
	sc := iw.ctx.GetSessionVars().StmtCtx
	sort.Slice(lookUpContents, func(i, j int) bool {
		cmp := compareRow(sc, lookUpContents[i].keys, lookUpContents[j].keys)
		if cmp != 0 || iw.nextColCompareFilters == nil {
			return cmp < 0
		}
		return iw.nextColCompareFilters.CompareRow(lookUpContents[i].row, lookUpContents[j].row) < 0
	})

	return lookUpContents, nil
}

func (iw *indexHashJoinInnerWorker) handleTask(ctx context.Context, task *indexHashJoinTask, joinResult *indexLookUpResult) error {
	err := iw.buildHashTableForOuterResult(task)
	if err != nil {
		return err
	}
	lookUpContents, err := iw.constructLookupContent(task.lookUpJoinTask)
	//lookUpContents, err := iw.constructSortedLookupContent(task)
	if err != nil {
		return err
	}
	lookUpContents = iw.sortAndDedupLookUpContents(lookUpContents)
	if err = iw.fetchInnerResults(ctx, task.lookUpJoinTask, lookUpContents); err != nil {
		return err
	}
	var ok bool
	iter := chunk.NewIterator4List(task.innerResult)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		ok, joinResult = iw.joinMatchedInnerRow2Chunk(row, task, joinResult)
		if !ok {
			return errors.New("indexHashJoinInnerWorker.handleTask failed")
		}
	}
	for rowIdx, val := range task.outerRowStatus {
		if val == outerRowMatched {
			continue
		}
		iw.joiner.onMissMatch(val == outerRowHasNull, task.outerResult.GetRow(rowIdx), joinResult.chk)
		if joinResult.chk.IsFull() {
			iw.resultCh <- joinResult
			ok, joinResult = iw.getNewJoinResult()
			if !ok {
				return errors.New("indexHashJoinInnerWorker.handleTask failed")
			}
		}
	}
	return nil

}

func (iw *indexHashJoinInnerWorker) joinMatchedInnerRow2Chunk(innerRow chunk.Row, task *indexHashJoinTask,
	joinResult *indexLookUpResult) (bool, *indexLookUpResult) {
	var err error
	keyBuf := make([]byte, 0, 64)
	keyBuf, err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, keyBuf, innerRow, iw.rowTypes, iw.keyCols)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	iw.matchedOuterPtrs = task.lookupMap.Get(keyBuf, iw.matchedOuterPtrs[:0])
	if len(iw.matchedOuterPtrs) == 0 {
		return true, joinResult
	}
	innerIter := chunk.NewIterator4Slice([]chunk.Row{innerRow})
	var ok bool
	for _, ptr := range iw.matchedOuterPtrs {
		innerIter.Begin()
		rowIdx := *(*int)(unsafe.Pointer(&ptr[0]))
		outerRow := task.outerResult.GetRow(rowIdx)
		matched, isNull, err := iw.joiner.tryToMatch(outerRow, innerIter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		if matched {
			task.outerRowStatus[rowIdx] = outerRowMatched
		}
		if isNull {
			task.outerRowStatus[rowIdx] = outerRowHasNull
		}
		if joinResult.chk.IsFull() {
			iw.resultCh <- joinResult
			ok, joinResult = iw.getNewJoinResult()
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}
