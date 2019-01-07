package executor

import (
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"golang.org/x/net/context"
)
type IndexLookUpHashJoin struct {
	IndexLookUpJoin
}

type indexHashJoinOuterWorker struct {
	outerWorker
}

type innerHashWorker struct {
	innerWorker
	matchPtrBytes [][]byte
}

func (e *IndexLookUpHashJoin) finishInnerWorker(r interface{}) {
	if r != nil {
		e.joinResultCh <- &indexLookUpResult{err: errors.Errorf("%v", r)}
	}
	e.workerWg.Done()
}

func (e *IndexLookUpHashJoin) waitInnerHashWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	close(e.joinResultCh)
}
// Open implements the IndexLookUpHashJoin Executor interface.
func (e *IndexLookUpHashJoin) Open(ctx context.Context) error {
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
	e.innerCtx.readerBuilder.getStartTS()

	err := e.children[0].Open(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexLookUpHashJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.workerWg.Add(1)
	ow := e.newOuterWorker(resultCh, innerCh)
	go util.WithRecovery(func() {
		defer close(ow.innerCh)
		defer e.workerWg.Done()
		ow.run(workerCtx)
	}, nil)

	e.joinResultCh = make(chan *indexLookUpResult, concurrency+1)
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := int(0); i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- e.newFirstChunk()
	}
	e.workerWg.Add(concurrency)
	for i := int(0); i < concurrency; i++ {
		workerId := i
		go util.WithRecovery(func() { e.newInnerWorker(innerCh, workerId).run(workerCtx, e.workerWg) }, e.finishInnerWorker)

	}
	go util.WithRecovery(e.waitInnerHashWorkersAndCloseResultChan, nil)
}

// Next implements the IndexLookUpHashJoin Executor interface.
func (e *IndexLookUpHashJoin) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}
	chk.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the IndexLookUpHashJoin Executor interface.
func (e *IndexLookUpHashJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.cancelFunc = nil
	if e.joinResultCh != nil {
		for range e.joinResultCh {
		}
	}
	e.joinResultCh = nil
	for i := range e.joinChkResourceCh {
		close(e.joinChkResourceCh[i])
		for range e.joinChkResourceCh[i] {
		}
	}
	e.joinChkResourceCh = nil

	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}

func (ow *indexHashJoinOuterWorker) run(ctx context.Context) {
	for {
		task, _ := ow.buildTask(ctx)
		if task == nil {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}
	}
}

func (e *IndexLookUpHashJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask) *indexHashJoinOuterWorker {
	ow := &indexHashJoinOuterWorker{
		outerWorker: outerWorker{
			outerCtx:         e.outerCtx,
			ctx:              e.ctx,
			executor:         e.children[0],
			executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
			resultCh:         resultCh,
			innerCh:          innerCh,
			batchSize:        32,
			maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
			parentMemTracker: e.memTracker,
		},
	}
	return ow
}

func (e *IndexLookUpHashJoin) newInnerWorker(taskCh chan *lookUpJoinTask, workerId int) *innerHashWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	iw := &innerHashWorker{
		innerWorker: innerWorker{
			innerCtx:          e.innerCtx,
			outerCtx:          e.outerCtx,
			taskCh:            taskCh,
			ctx:               e.ctx,
			executorChk:       chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
			indexRanges:       copiedRanges,
			keyOff2IdxOff:     e.keyOff2IdxOff,
			joiner:            e.joiner,
			maxChunkSize:      e.maxChunkSize,
			workerId:          workerId,
			joinChkResourceCh: e.joinChkResourceCh,
			joinResultCh:      e.joinResultCh,
		},
		matchPtrBytes: make([][]byte, 0, 8),
	}
	return iw
}

func (iw *innerHashWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	var task *lookUpJoinTask
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
			joinResult.err = errors.Trace(task.buildError)
			break
		}
		err := iw.handleTask(ctx, task, joinResult)
		if err != nil {
			joinResult.err = err
			break
		}
	}
	if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		iw.joinResultCh <- joinResult
	}
}

func (iw *innerHashWorker) getNewJoinResult() (bool, *indexLookUpResult) {
	joinResult := &indexLookUpResult{
		src: iw.joinChkResourceCh[iw.workerId],
	}
	ok := true
	select {
	case joinResult.chk, ok = <-iw.joinChkResourceCh[iw.workerId]:
	}
	return ok, joinResult
}
func (iw *innerHashWorker) handleTask(ctx context.Context, task *lookUpJoinTask, joinResult *indexLookUpResult) error {
	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	err = iw.fetchInnerResults(ctx, task, dLookUpKeys)
	if err != nil {
		return errors.Trace(err)
	}
	var ok bool
	ok, joinResult = iw.join2Chunk(joinResult, task)
	if !ok {
		return errors.New("join2Chunk failed")
	}
	it := task.lookupMap.NewIterator()
	for key, rowPtr := it.Next(); key != nil; key, rowPtr = it.Next() {
		iw.matchPtrBytes = task.matchKeyMap.Get(key, iw.matchPtrBytes[:0])
		if len(iw.matchPtrBytes) == 0 {
			ptr := *(*uint32)(unsafe.Pointer(&rowPtr[0]))
			misMatchedRow := task.outerResult.GetRow(int(ptr))
			iw.joiner.onMissMatch(misMatchedRow, joinResult.chk)
		}
		if joinResult.chk.NumRows() == iw.maxChunkSize {
			ok := true
			iw.joinResultCh <- joinResult
			ok, joinResult = iw.getNewJoinResult()
			if !ok {
				return nil
			}
		}
	}

	return nil

}
func (iw *innerHashWorker) join2Chunk(joinResult *indexLookUpResult, task *lookUpJoinTask) (ok bool, _ *indexLookUpResult) {

	for i := 0; i < task.innerResult.NumChunks(); i++ {
		curChk := task.innerResult.GetChunk(i)
		iter := chunk.NewIterator4Chunk(curChk)
		for innerRow := iter.Begin(); innerRow != iter.End(); innerRow = iter.Next() {
			ok, joinResult = iw.joinMatchInnerRow2Chunk(innerRow, task, joinResult)
			if !ok {
				return false, joinResult
			}

		}
	}
	return true, joinResult
}

func (iw *innerHashWorker) joinMatchInnerRow2Chunk(innerRow chunk.Row, task *lookUpJoinTask,
	joinResult *indexLookUpResult) (bool, *indexLookUpResult) {
	keyBuf := make([]byte, 0, 64)
	for _, keyCol := range iw.keyCols {
		d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
		var err error
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, d)
		if err != nil {
			return false, joinResult
		}
	}
	iw.matchPtrBytes = task.lookupMap.Get(keyBuf, iw.matchPtrBytes[:0])
	if len(iw.matchPtrBytes) == 0 {
		return true, joinResult
	}
	task.matchedInners = task.matchedInners[:0]
	var matchedOuters []chunk.Row
	for _, b := range iw.matchPtrBytes {
		ptr := *(*uint32)(unsafe.Pointer(&b[0]))
		matchedOuter := task.outerResult.GetRow(int(ptr))
		matchedOuters = append(matchedOuters, matchedOuter)
	}
	innerIter := chunk.NewIterator4Slice([]chunk.Row{innerRow})
	innerIter.Begin()
	hasMatch := false
	for i := 0; i < len(matchedOuters); i++ {
		innerIter.Begin()
		matched, err := iw.joiner.tryToMatch(matchedOuters[i], innerIter, joinResult.chk)
		if err != nil {
			joinResult.err = errors.Trace(err)
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		if joinResult.chk.NumRows() == iw.maxChunkSize {
			ok := true
			iw.joinResultCh <- joinResult
			ok, joinResult = iw.getNewJoinResult()
			if !ok {
				return false, joinResult
			}
		}
	}
	if hasMatch {
		task.matchKeyMap.Put(keyBuf, []byte{0})
	}
	return true, joinResult
}
