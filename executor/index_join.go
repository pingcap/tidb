package executor

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/ranger"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"runtime"
	"sort"
	"sync"
	"unsafe"
)

var _ Executor = &IndexJoin{}

type IndexJoin struct {
	baseExecutor

	resultCh   <-chan *indexJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	task     *indexJoinTask
	taskChan chan *indexJoinTask

	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner joiner

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int

	memTracker *memory.Tracker // track memory usage.
	prepare    bool
	workerCtx  context.Context
}
type outerCtx struct {
	rowTypes  []*types.FieldType
	keyCols   []int
	filter    expression.CNFExprs
	keepOrder bool
}

type innerCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
}

type indexJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	lookupMap     *mvmap.MVMap
	matchedInners []chunk.Row

	doneCh   chan error
	cursor   int
	hasMatch bool

	memTracker *memory.Tracker // track memory usage.
}
type hashWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type IndexHashJoin struct {
	IndexJoin
	joinResultCh      chan *hashWorkerResult
	joinChkResourceCh []chan *chunk.Chunk
	closeCh           chan struct{} // closeCh add a lock for closing executor.
}

type IndexMergeJoin struct {
	IndexJoin
}
type outerWorker struct {
	outerCtx

	ctx      sessionctx.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int

	resultCh chan<- *indexJoinTask
	innerCh  chan<- *indexJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *indexJoinTask
	outerCtx    outerCtx
	ctx         sessionctx.Context
	executorChk *chunk.Chunk

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	joiner        joiner
	maxChunkSize  int
}

type innerMergeWorker struct {
	innerWorker
}
type innerHashWorker struct {
	innerWorker
	innerPtrBytes     [][]byte
	workerId          int
	joinChkResourceCh []chan *chunk.Chunk
	closeCh           chan struct{}
	joinResultCh      chan *hashWorkerResult
}

func (e *IndexMergeJoin) Open(ctx context.Context) error {
	err, _, _ := e.open(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
func (e *IndexHashJoin) Open(ctx context.Context) error {
	err, innerCh, workerCtx := e.open(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	e.closeCh = make(chan struct{})
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	e.joinResultCh = make(chan *hashWorkerResult, concurrency+1)
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
	return nil
}
func (e *IndexHashJoin) finishInnerWorker(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.workerWg.Done()
}
func (e *IndexHashJoin) waitInnerHashWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	close(e.joinResultCh)

}

func (e *IndexJoin) open(ctx context.Context) (error, chan *indexJoinTask, context.Context) {
	e.innerCtx.readerBuilder.getStartTS()

	err := e.children[0].Open(ctx)
	if err != nil {
		return errors.Trace(err), nil, nil
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	resultCh := make(chan *indexJoinTask, concurrency)
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *indexJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	return nil, innerCh, workerCtx
}

func (e *IndexJoin) newOuterWorker(resultCh, innerCh chan *indexJoinTask) *outerWorker {
	ow := &outerWorker{
		outerCtx:         e.outerCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
	}
	return ow
}
func (iw *innerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	var task *indexJoinTask
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("innerWorker panic stack is:\n%s", buf)
			// "task != nil" is guaranteed when panic happened.
			task.doneCh <- errors.Errorf("%v", r)
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

		task.doneCh <- errors.Trace(err)
	}
}
func (iw *innerHashWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	var task *indexJoinTask
	ok, joinResult := iw.getNewJoinResult()
	if !ok {
		return
	}
	for {
		select {
		case <-iw.closeCh:
			return
		case task, ok = <-iw.taskCh:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
		err := iw.handleTask(ctx, task, joinResult)
		if err != nil {
			return
		}
	}
}

func (iw *innerMergeWorker) handleTask(ctx context.Context, task *indexJoinTask) error {
	return nil
}

func (iw *innerHashWorker) handleTask(ctx context.Context, task *indexJoinTask, joinResult *hashWorkerResult) error {

	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	err = iw.fetchAndJoin(ctx, task, dLookUpKeys, joinResult)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (iw *innerWorker) sortAndDedupDatumLookUpKeys(dLookUpKeys [][]types.Datum) [][]types.Datum {
	if len(dLookUpKeys) < 2 {
		return dLookUpKeys
	}
	sc := iw.ctx.GetSessionVars().StmtCtx
	sort.Slice(dLookUpKeys, func(i, j int) bool {
		cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[j])
		return cmp < 0
	})
	deDupedLookupKeys := dLookUpKeys[:1]
	for i := 1; i < len(dLookUpKeys); i++ {
		cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[i-1])
		if cmp != 0 {
			deDupedLookupKeys = append(deDupedLookupKeys, dLookUpKeys[i])
		}
	}
	return deDupedLookupKeys
}
func compareRow(sc *stmtctx.StatementContext, left, right []types.Datum) int {
	for idx := 0; idx < len(left); idx++ {
		cmp, err := left[idx].CompareDatum(sc, &right[idx])
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

func (ow *outerWorker) buildHashTable(task *indexJoinTask) error {
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.outerResult.NumRows(); i++ {
		if task.outerMatch != nil && !task.outerMatch[i] {
			continue
		}
		outerRow := task.outerResult.GetRow(i)
		if ow.hasNullInJoinKey(outerRow) { //skip outer row?
			continue
		}

		keyBuf = keyBuf[:0]
		for _, keyCol := range ow.keyCols {
			d := outerRow.GetDatum(keyCol, ow.rowTypes[keyCol])
			var err error
			keyBuf, err = codec.EncodeKey(ow.ctx.GetSessionVars().StmtCtx, keyBuf, d)
			if err != nil {
				return errors.Trace(err)
			}
		}
		rowPtr := chunk.RowPtr{ChkIdx: uint32(0), RowIdx: uint32(i)}
		*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
		task.lookupMap.Put(keyBuf, valBuf)
	}
	return nil
}

func (iw *innerHashWorker) joinMatchInnerRow2Chunk(innerRow chunk.Row, task *indexJoinTask,
	joinResult *hashWorkerResult) (bool, *hashWorkerResult) {
	keyBuf := make([]byte, 0, 64)
	for _, keyCol := range iw.keyCols {
		d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
		var err error
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, d)
		if err != nil {
			return false, joinResult
		}
	}
	iw.innerPtrBytes = task.lookupMap.Get(keyBuf, iw.innerPtrBytes[:0])
	task.matchedInners = task.matchedInners[:0]
	if len(iw.innerPtrBytes) == 0 {
		iw.joiner.onMissMatch(innerRow, joinResult.chk)
		return true, joinResult
	}
	for _, b := range iw.innerPtrBytes {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := task.outerResult.GetRow(int(ptr.RowIdx))
		task.matchedInners = append(task.matchedInners, matchedInner)
	}

	outerIter := chunk.NewIterator4Slice(task.matchedInners)

	hasMatch := false
	for outerIter.Begin(); outerIter.Current() != outerIter.End(); {
		matched, err := iw.joiner.tryToMatch(innerRow, outerIter, joinResult.chk)
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
	if !hasMatch {
		iw.joiner.onMissMatch(innerRow, joinResult.chk)
	}

	return true, joinResult

}

func (iw *innerHashWorker) join2Chunk(innerChk *chunk.Chunk, joinResult *hashWorkerResult, task *indexJoinTask) (ok bool, _ *hashWorkerResult) {
	for i := 0; i < innerChk.NumRows(); i++ {
		innerRow := innerChk.GetRow(i)

		ok, joinResult = iw.joinMatchInnerRow2Chunk(innerRow, task, joinResult)
		if !ok {
			return false, joinResult
		}
	}

	return true, joinResult

}
func (ow *outerWorker) hasNullInJoinKey(row chunk.Row) bool {
	for _, ordinal := range ow.keyCols {
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

func (iw *innerWorker) constructDatumLookupKeys(task *indexJoinTask) ([][]types.Datum, error) {
	dLookUpKeys := make([][]types.Datum, 0, task.outerResult.NumRows())
	for i := 0; i < task.outerResult.NumRows(); i++ {
		dLookUpKey, err := iw.constructDatumLookupKey(task, i)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if dLookUpKey == nil {
			// Append null to make looUpKeys the same length as outer Result.
			continue
		}
		// Store the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
	}

	//task.memTracker.Consume(task.encodedLookUpKeys.MemoryUsage())
	return dLookUpKeys, nil
}

func (iw *innerWorker) constructDatumLookupKey(task *indexJoinTask, rowIdx int) ([]types.Datum, error) {
	if task.outerMatch != nil && !task.outerMatch[rowIdx] {
		return nil, nil
	}
	outerRow := task.outerResult.GetRow(rowIdx)
	sc := iw.ctx.GetSessionVars().StmtCtx
	keyLen := len(iw.keyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	for i, keyCol := range iw.outerCtx.keyCols {
		outerValue := outerRow.GetDatum(keyCol, iw.outerCtx.rowTypes[keyCol])

		innerColType := iw.rowTypes[iw.keyCols[i]]
		innerValue, err := outerValue.ConvertTo(sc, innerColType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cmp, err := outerValue.CompareDatum(sc, &innerValue)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		dLookupKey = append(dLookupKey, innerValue)
	}
	return dLookupKey, nil
}

func (iw *innerHashWorker) getNewJoinResult() (bool, *hashWorkerResult) {
	joinResult := &hashWorkerResult{
		src: iw.joinChkResourceCh[iw.workerId],
	}
	ok := true
	select {
	case <-iw.closeCh:
		ok = false
	case joinResult.chk, ok = <-iw.joinChkResourceCh[iw.workerId]:
	}
	return ok, joinResult
}

func (iw *innerHashWorker) fetchAndJoin(ctx context.Context, task *indexJoinTask, dLookUpKeys [][]types.Datum, joinResult *hashWorkerResult) error {
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, iw.indexRanges, iw.keyOff2IdxOff)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(innerExec.retTypes(), iw.ctx.GetSessionVars().MaxChunkSize, iw.ctx.GetSessionVars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel("inner result")
	innerResult.GetMemTracker().AttachTo(task.memTracker)

	iw.executorChk.Reset()
	var ok bool
	for {
		err := innerExec.Next(ctx, iw.executorChk)
		if err != nil {
			return errors.Trace(err)
		}

		if iw.executorChk.NumRows() == 0 {
			break
		}

		ok, joinResult = iw.join2Chunk(iw.executorChk, joinResult, task)
		if !ok {
			break
		}
	}

	if joinResult == nil {
		return nil
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		iw.joinResultCh <- joinResult
	}
	if !ok {
		return errors.New("join2Chunk failed")
	}
	return nil
}
func (e *IndexJoin) newBaseInnerWorker(taskCh chan *indexJoinTask) *innerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}

	iw := &innerWorker{
		innerCtx:      e.innerCtx,
		outerCtx:      e.outerCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
		joiner:        e.joiner,
		maxChunkSize:  e.maxChunkSize,
	}
	return iw
}
func (e *IndexMergeJoin) newInnerWorker(taskCh chan *indexJoinTask, workerId int) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	bw := e.newBaseInnerWorker(taskCh)
	iw := &innerMergeWorker{
		innerWorker: *bw,
	}
	return iw
}
func (e *IndexHashJoin) newInnerWorker(taskCh chan *indexJoinTask, workerId int) *innerHashWorker {

	bw := e.newBaseInnerWorker(taskCh)

	iw := &innerHashWorker{
		innerWorker:       *bw,
		workerId:          workerId,
		closeCh:           e.closeCh,
		joinChkResourceCh: e.joinChkResourceCh,
		joinResultCh:      e.joinResultCh,
	}
	return iw
}

func (ow *outerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("outerWorker panic stack is:\n%s", buf)
			//task := &indexJoinTask{doneCh: make(chan error, 1)}
			//task.doneCh <- errors.Errorf("%v", r)
			//ow.pushToChan(ctx, task, ow.resultCh)
		}
		if ow.keepOrder {
			close(ow.resultCh)
		}
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		task, err := ow.buildTask(ctx)
		if err != nil {
			task.doneCh <- errors.Trace(err)
			//ow.pushToChan(ctx, task, ow.resultCh)
			return
		}
		if task == nil {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}

		if ow.outerCtx.keepOrder {
			if finished := ow.pushToChan(ctx, task, ow.resultCh); finished {
				return
			}
		}
	}
}

// buildTask builds a indexJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*indexJoinTask, error) {
	ow.executor.newFirstChunk()

	task := &indexJoinTask{
		doneCh:      make(chan error, 1),
		outerResult: ow.executor.newFirstChunk(),
		//encodedLookUpKeys: chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, ow.ctx.GetSessionVars().MaxChunkSize),
		lookupMap: mvmap.NewMVMap(),
	}
	task.memTracker = memory.NewTracker(fmt.Sprintf("lookup join task %p", task), -1)
	task.memTracker.AttachTo(ow.parentMemTracker)

	ow.increaseBatchSize()

	task.memTracker.Consume(task.outerResult.MemoryUsage())
	for task.outerResult.NumRows() < ow.batchSize {
		err := ow.executor.Next(ctx, ow.executorChk)
		if err != nil {
			return task, errors.Trace(err)
		}
		if ow.executorChk.NumRows() == 0 {
			break
		}

		oldMemUsage := task.outerResult.MemoryUsage()
		task.outerResult.Append(ow.executorChk, 0, ow.executorChk.NumRows())
		newMemUsage := task.outerResult.MemoryUsage()
		task.memTracker.Consume(newMemUsage - oldMemUsage)
	}
	if task.outerResult.NumRows() == 0 {
		return nil, nil
	}

	if ow.filter != nil {
		outerMatch := make([]bool, 0, task.outerResult.NumRows())
		var err error
		task.outerMatch, err = expression.VectorizedFilter(ow.ctx, ow.filter, chunk.NewIterator4Chunk(task.outerResult), outerMatch)
		if err != nil {
			return task, errors.Trace(err)
		}
		task.memTracker.Consume(int64(cap(task.outerMatch)))
	}
	err := ow.buildHashTable(task)
	if err != nil {
		return task, errors.Trace(err)
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

func (ow *outerWorker) pushToChan(ctx context.Context, task *indexJoinTask, dst chan<- *indexJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

func (e *IndexHashJoin) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.joinResultCh == nil {
		return nil
	}
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

func (e *IndexMergeJoin) Next(ctx context.Context, chk *chunk.Chunk) error {
	return nil
}

func (e *IndexHashJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	close(e.closeCh)
	if e.joinResultCh != nil {
		for range e.joinResultCh {
		}
	}
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
func (e *IndexMergeJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}

	//e.workerWg.Wait()
	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}
