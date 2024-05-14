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

package aggregate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/set"
)

// HashAggInput indicates the input of hash agg exec.
type HashAggInput struct {
	chk *chunk.Chunk
	// giveBackCh is bound with specific partial worker,
	// it's used to reuse the `chk`,
	// and tell the data-fetcher which partial worker it should send data to.
	giveBackCh chan<- *chunk.Chunk
}

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in PartialAggFuncs.
// The parallel execution flow is as the following graph shows:
/*
                            +-------------+
                            | Main Thread |
                            +------+------+
                                   ^
                                   |
                                   +
                              +-+-            +-+
                              | |    ......   | |  finalOutputCh
                              +++-            +-+
                               ^
                               |
                               +---------------+
                               |               |
                 +--------------+             +--------------+
                 | final worker |     ......  | final worker |
                 +------------+-+             +-+------------+
                              ^                 ^
                              |                 |
                             +-+  +-+  ......  +-+
                             | |  | |          | |
                             ...  ...          ...    partialOutputChs
                             | |  | |          | |
                             +++  +++          +++
                              ^    ^            ^
          +-+                 |    |            |
          | |        +--------o----+            |
 inputCh  +-+        |        +-----------------+---+
          | |        |                              |
          ...    +---+------------+            +----+-----------+
          | |    | partial worker |   ......   | partial worker |
          +++    +--------------+-+            +-+--------------+
           |                     ^                ^
           |                     |                |
      +----v---------+          +++ +-+          +++
      | data fetcher | +------> | | | |  ......  | |   partialInputChs
      +--------------+          +-+ +-+          +-+
*/
type HashAggExec struct {
	exec.BaseExecutor

	Sc               *stmtctx.StatementContext
	PartialAggFuncs  []aggfuncs.AggFunc
	FinalAggFuncs    []aggfuncs.AggFunc
	partialResultMap aggfuncs.AggPartialResultMapper
	bInMap           int64 // indicate there are 2^bInMap buckets in partialResultMap
	groupSet         set.StringSetWithMemoryUsage
	groupKeys        []string
	cursor4GroupKey  int
	GroupByItems     []expression.Expression
	groupKeyBuffer   [][]byte

	finishCh         chan struct{}
	finalOutputCh    chan *AfFinalResult
	partialOutputChs []chan *aggfuncs.AggPartialResultMapper
	inputCh          chan *HashAggInput
	partialInputChs  []chan *chunk.Chunk
	partialWorkers   []HashAggPartialWorker
	finalWorkers     []HashAggFinalWorker
	DefaultVal       *chunk.Chunk
	childResult      *chunk.Chunk

	// IsChildReturnEmpty indicates whether the child executor only returns an empty input.
	IsChildReturnEmpty bool
	// After we support parallel execution for aggregation functions with distinct,
	// we can remove this attribute.
	IsUnparallelExec  bool
	parallelExecValid bool
	prepared          atomic.Bool
	executed          atomic.Bool

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker

	stats *HashAggRuntimeStats

	// dataInDisk is the chunks to store row values for spilled data.
	// The HashAggExec may be set to `spill mode` multiple times, and all spilled data will be appended to DataInDiskByRows.
	dataInDisk *chunk.DataInDiskByChunks
	// numOfSpilledChks indicates the number of all the spilled chunks.
	numOfSpilledChks int
	// offsetOfSpilledChks indicates the offset of the chunk be read from the disk.
	// In each round of processing, we need to re-fetch all the chunks spilled in the last one.
	offsetOfSpilledChks int
	// inSpillMode indicates whether HashAgg is in `spill mode`.
	// When HashAgg is in `spill mode`, the size of `partialResultMap` is no longer growing and all the data fetched
	// from the child executor is spilled to the disk.
	inSpillMode uint32
	// tmpChkForSpill is the temp chunk for spilling.
	tmpChkForSpill *chunk.Chunk
	// The `inflightChunkSync` calls `Add(1)` when the data fetcher goroutine inserts a chunk into the channel,
	// and `Done()` when any partial worker retrieves a chunk from the channel and updates it in the `partialResultMap`.
	// In scenarios where it is necessary to wait for all partial workers to finish processing the inflight chunk,
	// `inflightChunkSync` can be used for synchronization.
	inflightChunkSync *sync.WaitGroup
	// spillAction save the Action for spilling.
	spillAction *AggSpillDiskAction
	// parallelAggSpillAction save the Action for spilling of parallel aggregation.
	parallelAggSpillAction *ParallelAggSpillDiskAction
	// spillHelper helps to carry out the spill action
	spillHelper *parallelHashAggSpillHelper
	// isChildDrained indicates whether the all data from child has been taken out.
	isChildDrained bool
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}

	if e.IsUnparallelExec {
		e.childResult = nil
		e.groupSet, _ = set.NewStringSetWithMemoryUsage()
		e.partialResultMap = nil
		if e.memTracker != nil {
			e.memTracker.ReplaceBytesUsed(0)
		}
		if e.dataInDisk != nil {
			e.dataInDisk.Close()
		}
		if e.spillAction != nil {
			e.spillAction.SetFinished()
		}
		e.spillAction, e.tmpChkForSpill = nil, nil
		err := e.BaseExecutor.Close()
		if err != nil {
			return err
		}
		return nil
	}
	if e.parallelExecValid {
		// `Close` may be called after `Open` without calling `Next` in test.
		if e.prepared.CompareAndSwap(false, true) {
			close(e.inputCh)
			for _, ch := range e.partialOutputChs {
				close(ch)
			}
			for _, ch := range e.partialInputChs {
				close(ch)
			}
			close(e.finalOutputCh)
		}
		close(e.finishCh)
		for _, ch := range e.partialOutputChs {
			channel.Clear(ch)
		}
		for _, ch := range e.partialInputChs {
			channel.Clear(ch)
		}
		channel.Clear(e.finalOutputCh)
		e.executed.Store(false)
		if e.memTracker != nil {
			e.memTracker.ReplaceBytesUsed(0)
		}
		e.parallelExecValid = false
		if e.parallelAggSpillAction != nil {
			e.parallelAggSpillAction.SetFinished()
			e.parallelAggSpillAction = nil
			e.spillHelper.close()
		}
	}

	err := e.BaseExecutor.Close()
	failpoint.Inject("injectHashAggClosePanic", func(val failpoint.Value) {
		if enabled := val.(bool); enabled {
			if e.Ctx().GetSessionVars().ConnectionID != 0 {
				panic(errors.New("test"))
			}
		}
	})
	return err
}

// Open implements the Executor Open interface.
func (e *HashAggExec) Open(ctx context.Context) error {
	failpoint.Inject("mockHashAggExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			failpoint.Return(errors.New("mock HashAggExec.baseExecutor.Open returned error"))
		}
	})

	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.OpenSelf()
}

// OpenSelf just opens the hash aggregation executor.
func (e *HashAggExec) OpenSelf() error {
	e.prepared.Store(false)

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	if e.Ctx().GetSessionVars().TrackAggregateMemoryUsage {
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	}

	if e.IsUnparallelExec {
		e.initForUnparallelExec()
		return nil
	}
	return e.initForParallelExec(e.Ctx())
}

func (e *HashAggExec) initForUnparallelExec() {
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(aggfuncs.AggPartialResultMapper)
	e.bInMap = 0
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice*(1<<e.bInMap) + setSize)
	e.groupKeyBuffer = make([][]byte, 0, 8)
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
	e.memTracker.Consume(e.childResult.MemoryUsage())

	e.offsetOfSpilledChks, e.numOfSpilledChks = 0, 0
	e.executed.Store(false)
	e.isChildDrained = false
	e.dataInDisk = chunk.NewDataInDiskByChunks(exec.RetTypes(e.Children(0)))

	e.tmpChkForSpill = exec.TryNewCacheChunk(e.Children(0))
	if vars := e.Ctx().GetSessionVars(); vars.TrackAggregateMemoryUsage && variable.EnableTmpStorageOnOOM.Load() {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(vars.StmtCtx.DiskTracker)
		e.dataInDisk.GetDiskTracker().AttachTo(e.diskTracker)
		vars.MemTracker.FallbackOldAndSetNewActionForSoftLimit(e.ActionSpill())
	}
}

func (e *HashAggExec) initPartialWorkers(partialConcurrency int, finalConcurrency int, ctx sessionctx.Context) {
	for i := 0; i < partialConcurrency; i++ {
		partialResultsMap := make([]aggfuncs.AggPartialResultMapper, finalConcurrency)
		for i := 0; i < finalConcurrency; i++ {
			partialResultsMap[i] = make(aggfuncs.AggPartialResultMapper)
		}

		partialResultsBuffer, groupKeyBuf := getBuffer()
		e.partialWorkers[i] = HashAggPartialWorker{
			baseHashAggWorker:    newBaseHashAggWorker(e.finishCh, e.PartialAggFuncs, e.MaxChunkSize(), e.memTracker),
			idForTest:            i,
			ctx:                  ctx,
			inputCh:              e.partialInputChs[i],
			outputChs:            e.partialOutputChs,
			giveBackCh:           e.inputCh,
			BInMaps:              make([]int, finalConcurrency),
			partialResultsBuffer: *partialResultsBuffer,
			globalOutputCh:       e.finalOutputCh,
			partialResultsMap:    partialResultsMap,
			groupByItems:         e.GroupByItems,
			chk:                  exec.TryNewCacheChunk(e.Children(0)),
			groupKeyBuf:          *groupKeyBuf,
			serializeHelpers:     aggfuncs.NewSerializeHelper(),
			isSpillPrepared:      false,
			spillHelper:          e.spillHelper,
			inflightChunkSync:    e.inflightChunkSync,
		}

		e.partialWorkers[i].partialResultNumInRow = e.partialWorkers[i].getPartialResultSliceLenConsiderByteAlign()
		for j := 0; j < finalConcurrency; j++ {
			e.partialWorkers[i].BInMaps[j] = 0
		}

		// There is a bucket in the empty partialResultsMap.
		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << e.partialWorkers[i].BInMap))
		if e.stats != nil {
			e.partialWorkers[i].stats = &AggWorkerStat{}
			e.stats.PartialStats = append(e.stats.PartialStats, e.partialWorkers[i].stats)
		}
		e.memTracker.Consume(e.partialWorkers[i].chk.MemoryUsage())
		input := &HashAggInput{
			chk:        exec.NewFirstChunk(e.Children(0)),
			giveBackCh: e.partialWorkers[i].inputCh,
		}
		e.memTracker.Consume(input.chk.MemoryUsage())
		e.inputCh <- input
	}
}

func (e *HashAggExec) initFinalWorkers(finalConcurrency int) {
	for i := 0; i < finalConcurrency; i++ {
		e.finalWorkers[i] = HashAggFinalWorker{
			baseHashAggWorker:          newBaseHashAggWorker(e.finishCh, e.FinalAggFuncs, e.MaxChunkSize(), e.memTracker),
			partialResultMap:           make(aggfuncs.AggPartialResultMapper),
			BInMap:                     0,
			inputCh:                    e.partialOutputChs[i],
			outputCh:                   e.finalOutputCh,
			finalResultHolderCh:        make(chan *chunk.Chunk, 1),
			mutableRow:                 chunk.MutRowFromTypes(exec.RetTypes(e)),
			spillHelper:                e.spillHelper,
			restoredAggResultMapperMem: 0,
		}
		// There is a bucket in the empty partialResultsMap.
		e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << e.finalWorkers[i].BInMap))
		if e.stats != nil {
			e.finalWorkers[i].stats = &AggWorkerStat{}
			e.stats.FinalStats = append(e.stats.FinalStats, e.finalWorkers[i].stats)
		}
		e.finalWorkers[i].finalResultHolderCh <- exec.NewFirstChunk(e)
	}
}

func (e *HashAggExec) initForParallelExec(ctx sessionctx.Context) error {
	sessionVars := e.Ctx().GetSessionVars()
	partialConcurrency := sessionVars.HashAggPartialConcurrency()
	finalConcurrency := sessionVars.HashAggFinalConcurrency()

	if partialConcurrency == 0 || finalConcurrency == 0 {
		return errors.New("partialConcurrency or finalConcurrency is 0")
	}

	e.IsChildReturnEmpty = true
	e.finalOutputCh = make(chan *AfFinalResult, finalConcurrency+partialConcurrency+1)
	e.inputCh = make(chan *HashAggInput, partialConcurrency)
	e.finishCh = make(chan struct{}, 1)

	e.partialInputChs = make([]chan *chunk.Chunk, partialConcurrency)
	for i := range e.partialInputChs {
		e.partialInputChs[i] = make(chan *chunk.Chunk, 1)
	}
	e.partialOutputChs = make([]chan *aggfuncs.AggPartialResultMapper, finalConcurrency)
	for i := range e.partialOutputChs {
		e.partialOutputChs[i] = make(chan *aggfuncs.AggPartialResultMapper, partialConcurrency)
	}

	e.inflightChunkSync = &sync.WaitGroup{}

	isTrackerEnabled := e.Ctx().GetSessionVars().TrackAggregateMemoryUsage && variable.EnableTmpStorageOnOOM.Load()
	isParallelHashAggSpillEnabled := e.Ctx().GetSessionVars().EnableParallelHashaggSpill

	baseRetTypeNum := len(e.RetFieldTypes())

	// Intermediate result for aggregate function also need to be spilled,
	// so the number of spillChunkFieldTypes should be added 1.
	spillChunkFieldTypes := make([]*types.FieldType, baseRetTypeNum+1)
	for i := 0; i < baseRetTypeNum; i++ {
		spillChunkFieldTypes[i] = types.NewFieldType(mysql.TypeVarString)
	}
	spillChunkFieldTypes[baseRetTypeNum] = types.NewFieldType(mysql.TypeString)
	e.spillHelper = newSpillHelper(e.memTracker, e.PartialAggFuncs, func() *chunk.Chunk {
		return chunk.New(spillChunkFieldTypes, e.InitCap(), e.MaxChunkSize())
	}, spillChunkFieldTypes)

	if isTrackerEnabled && isParallelHashAggSpillEnabled {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(sessionVars.StmtCtx.DiskTracker)
		e.spillHelper.diskTracker = e.diskTracker
		sessionVars.MemTracker.FallbackOldAndSetNewActionForSoftLimit(e.ActionSpill())
	}

	e.partialWorkers = make([]HashAggPartialWorker, partialConcurrency)
	e.finalWorkers = make([]HashAggFinalWorker, finalConcurrency)
	e.initRuntimeStats()

	e.initPartialWorkers(partialConcurrency, finalConcurrency, ctx)
	e.initFinalWorkers(finalConcurrency)
	e.parallelExecValid = true
	e.executed.Store(false)
	return nil
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.IsUnparallelExec {
		return e.unparallelExec(ctx, req)
	}
	return e.parallelExec(ctx, req)
}

func (e *HashAggExec) fetchChildData(ctx context.Context, waitGroup *sync.WaitGroup) {
	var (
		input *HashAggInput
		chk   *chunk.Chunk
		ok    bool
		err   error
	)
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(e.finalOutputCh, r)
		}

		// Wait for the finish of all partial workers
		e.inflightChunkSync.Wait()

		if !e.spillHelper.isNoSpill() && !e.spillHelper.checkError() {
			// Spill the remaining data
			e.spill()

			for i := range e.partialWorkers {
				e.spillHelper.addListInDisks(e.partialWorkers[i].spilledChunksIO)
			}
		}

		for i := range e.partialInputChs {
			close(e.partialInputChs[i])
		}
		waitGroup.Done()
	}()
	for {
		select {
		case <-e.finishCh:
			return
		case input, ok = <-e.inputCh:
			if !ok {
				return
			}
			chk = input.chk
		}

		mSize := chk.MemoryUsage()
		err = exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			e.finalOutputCh <- &AfFinalResult{err: err}
			e.memTracker.Consume(-mSize)
			return
		}

		if chk.NumRows() == 0 {
			e.memTracker.Consume(-mSize)
			return
		}

		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(chk.MemoryUsage() - mSize)
		e.inflightChunkSync.Add(1)
		input.giveBackCh <- chk

		if hasError := e.spillIfNeed(); hasError {
			return
		}
	}
}

func (e *HashAggExec) spillIfNeed() bool {
	if e.spillHelper.checkError() {
		return true
	}

	if !e.spillHelper.checkNeedSpill() {
		return false
	}

	// Wait for the finish of all partial workers
	e.inflightChunkSync.Wait()
	e.spill()
	return false
}

func (e *HashAggExec) spill() {
	e.spillHelper.setInSpilling()
	defer e.spillHelper.setSpillTriggered()

	spillWaiter := &sync.WaitGroup{}
	spillWaiter.Add(len(e.partialWorkers))

	for i := range e.partialWorkers {
		go func(worker *HashAggPartialWorker) {
			defer spillWaiter.Done()
			err := worker.spillDataToDisk()
			if err != nil {
				worker.processError(err)
			}
		}(&e.partialWorkers[i])
	}

	spillWaiter.Wait()
}

func (e *HashAggExec) waitPartialWorkerAndCloseOutputChs(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.inputCh)
	for input := range e.inputCh {
		e.memTracker.Consume(-input.chk.MemoryUsage())
	}
	for _, ch := range e.partialOutputChs {
		close(ch)
	}
}

func (e *HashAggExec) waitAllWorkersAndCloseFinalOutputCh(waitGroups ...*sync.WaitGroup) {
	for _, waitGroup := range waitGroups {
		waitGroup.Wait()
	}
	close(e.finalOutputCh)
}

func (e *HashAggExec) prepare4ParallelExec(ctx context.Context) {
	fetchChildWorkerWaitGroup := &sync.WaitGroup{}
	fetchChildWorkerWaitGroup.Add(1)
	go e.fetchChildData(ctx, fetchChildWorkerWaitGroup)

	// We get the pointers here instead of when we are all finished and adding the time because:
	// (1) If there is Apply in the plan tree, executors may be reused (Open()ed and Close()ed multiple times)
	// (2) we don't wait all goroutines of HashAgg to exit in HashAgg.Close()
	// So we can't write something like:
	//     atomic.AddInt64(&e.stats.PartialWallTime, int64(time.Since(partialStart)))
	// Because the next execution of HashAgg may have started when this goroutine haven't exited and then there will be data race.
	var partialWallTimePtr, finalWallTimePtr *int64
	if e.stats != nil {
		partialWallTimePtr = &e.stats.PartialWallTime
		finalWallTimePtr = &e.stats.FinalWallTime
	}

	partialWorkerWaitGroup := &sync.WaitGroup{}
	partialWorkerWaitGroup.Add(len(e.partialWorkers))
	partialStart := time.Now()
	for i := range e.partialWorkers {
		go e.partialWorkers[i].run(e.Ctx(), partialWorkerWaitGroup, len(e.finalWorkers))
	}

	go func() {
		e.waitPartialWorkerAndCloseOutputChs(partialWorkerWaitGroup)
		if partialWallTimePtr != nil {
			atomic.AddInt64(partialWallTimePtr, int64(time.Since(partialStart)))
		}
	}()

	finalWorkerWaitGroup := &sync.WaitGroup{}
	finalWorkerWaitGroup.Add(len(e.finalWorkers))
	finalStart := time.Now()
	for i := range e.finalWorkers {
		go e.finalWorkers[i].run(e.Ctx(), finalWorkerWaitGroup, partialWorkerWaitGroup)
	}

	go func() {
		finalWorkerWaitGroup.Wait()
		if finalWallTimePtr != nil {
			atomic.AddInt64(finalWallTimePtr, int64(time.Since(finalStart)))
		}
	}()

	// All workers may send error message to e.finalOutputCh when they panic.
	// And e.finalOutputCh should be closed after all goroutines gone.
	go e.waitAllWorkersAndCloseFinalOutputCh(fetchChildWorkerWaitGroup, partialWorkerWaitGroup, finalWorkerWaitGroup)
}

// HashAggExec employs one input reader, M partial workers and N final workers to execute parallelly.
// The parallel execution flow is:
// 1. input reader reads data from child executor and send them to partial workers.
// 2. partial worker receives the input data, updates the partial results, and shuffle the partial results to the final workers.
// 3. final worker receives partial results from all the partial workers, evaluates the final results and sends the final results to the main thread.
func (e *HashAggExec) parallelExec(ctx context.Context, chk *chunk.Chunk) error {
	if e.prepared.CompareAndSwap(false, true) {
		e.prepare4ParallelExec(ctx)
	}

	failpoint.Inject("parallelHashAggError", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			failpoint.Return(errors.New("HashAggExec.parallelExec error"))
		}
	})

	if e.executed.Load() {
		return nil
	}

	for {
		result, ok := <-e.finalOutputCh
		if !ok {
			e.executed.Store(true)
			if e.IsChildReturnEmpty && e.DefaultVal != nil {
				chk.Append(e.DefaultVal, 0, 1)
			}
			return nil
		}
		if result.err != nil {
			return result.err
		}
		chk.SwapColumns(result.chk)
		result.chk.Reset()

		// So that we can reuse the chunk
		result.giveBackCh <- result.chk
		if chk.NumRows() > 0 {
			e.IsChildReturnEmpty = false
			return nil
		}
	}
}

// unparallelExec executes hash aggregation algorithm in single thread.
func (e *HashAggExec) unparallelExec(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		exprCtx := e.Ctx().GetExprCtx()
		if e.prepared.Load() {
			// Since we return e.MaxChunkSize() rows every time, so we should not traverse
			// `groupSet` because of its randomness.
			for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
				partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
				if len(e.PartialAggFuncs) == 0 {
					chk.SetNumVirtualRows(chk.NumRows() + 1)
				}
				for i, af := range e.PartialAggFuncs {
					if err := af.AppendFinalResult2Chunk(exprCtx.GetEvalCtx(), partialResults[i], chk); err != nil {
						return err
					}
				}
				if chk.IsFull() {
					e.cursor4GroupKey++
					return nil
				}
			}
			e.resetSpillMode()
		}
		if e.executed.Load() {
			return nil
		}
		if err := e.execute(ctx); err != nil {
			return err
		}
		if (len(e.groupSet.StringSet) == 0) && len(e.GroupByItems) == 0 {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.memTracker.Consume(e.groupSet.Insert(""))
			e.groupKeys = append(e.groupKeys, "")
		}
		e.prepared.Store(true)
	}
}

func (e *HashAggExec) resetSpillMode() {
	e.cursor4GroupKey, e.groupKeys = 0, e.groupKeys[:0]
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(aggfuncs.AggPartialResultMapper)
	e.bInMap = 0
	e.prepared.Store(false)
	e.executed.Store(e.numOfSpilledChks == e.dataInDisk.NumChunks()) // No data is spilling again, all data have been processed.
	e.numOfSpilledChks = e.dataInDisk.NumChunks()
	e.memTracker.ReplaceBytesUsed(setSize)
	atomic.StoreUint32(&e.inSpillMode, 0)
}

// execute fetches Chunks from src and update each aggregate function for each row in Chunk.
func (e *HashAggExec) execute(ctx context.Context) (err error) {
	defer func() {
		if e.tmpChkForSpill.NumRows() > 0 && err == nil {
			err = e.dataInDisk.Add(e.tmpChkForSpill)
			e.tmpChkForSpill.Reset()
		}
	}()
	exprCtx := e.Ctx().GetExprCtx()
	for {
		mSize := e.childResult.MemoryUsage()
		if err := e.getNextChunk(ctx); err != nil {
			return err
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}

		failpoint.Inject("unparallelHashAggError", func(val failpoint.Value) {
			if val, _ := val.(bool); val {
				failpoint.Return(errors.New("HashAggExec.unparallelExec error"))
			}
		})

		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.groupKeyBuffer, err = GetGroupKey(e.Ctx(), e.childResult, e.groupKeyBuffer, e.GroupByItems)
		if err != nil {
			return err
		}

		allMemDelta := int64(0)
		sel := make([]int, 0, e.childResult.NumRows())
		var tmpBuf [1]chunk.Row
		for j := 0; j < e.childResult.NumRows(); j++ {
			groupKey := string(e.groupKeyBuffer[j]) // do memory copy here, because e.groupKeyBuffer may be reused.
			if !e.groupSet.Exist(groupKey) {
				if atomic.LoadUint32(&e.inSpillMode) == 1 && e.groupSet.Count() > 0 {
					sel = append(sel, j)
					continue
				}
				allMemDelta += e.groupSet.Insert(groupKey)
				e.groupKeys = append(e.groupKeys, groupKey)
			}
			partialResults := e.getPartialResults(groupKey)
			for i, af := range e.PartialAggFuncs {
				tmpBuf[0] = e.childResult.GetRow(j)
				memDelta, err := af.UpdatePartialResult(exprCtx.GetEvalCtx(), tmpBuf[:], partialResults[i])
				if err != nil {
					return err
				}
				allMemDelta += memDelta
			}
		}

		// spill unprocessed data when exceeded.
		if len(sel) > 0 {
			e.childResult.SetSel(sel)
			err = e.spillUnprocessedData(len(sel) == cap(sel))
			if err != nil {
				return err
			}
		}

		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(allMemDelta)
	}
}

func (e *HashAggExec) spillUnprocessedData(isFullChk bool) (err error) {
	if isFullChk {
		return e.dataInDisk.Add(e.childResult)
	}
	for i := 0; i < e.childResult.NumRows(); i++ {
		e.tmpChkForSpill.AppendRow(e.childResult.GetRow(i))
		if e.tmpChkForSpill.IsFull() {
			err = e.dataInDisk.Add(e.tmpChkForSpill)
			if err != nil {
				return err
			}
			e.tmpChkForSpill.Reset()
		}
	}
	return nil
}

func (e *HashAggExec) getNextChunk(ctx context.Context) (err error) {
	e.childResult.Reset()
	if !e.isChildDrained {
		if err := exec.Next(ctx, e.Children(0), e.childResult); err != nil {
			return err
		}
		if e.childResult.NumRows() != 0 {
			return nil
		}
		e.isChildDrained = true
	}
	if e.offsetOfSpilledChks < e.numOfSpilledChks {
		e.childResult, err = e.dataInDisk.GetChunk(e.offsetOfSpilledChks)
		if err != nil {
			return err
		}
		e.offsetOfSpilledChks++
	}
	return nil
}

func (e *HashAggExec) getPartialResults(groupKey string) []aggfuncs.PartialResult {
	partialResults, ok := e.partialResultMap[groupKey]
	allMemDelta := int64(0)
	if !ok {
		partialResults = make([]aggfuncs.PartialResult, 0, len(e.PartialAggFuncs))
		for _, af := range e.PartialAggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResults = append(partialResults, partialResult)
			allMemDelta += memDelta
		}
		// Map will expand when count > bucketNum * loadFactor. The memory usage will doubled.
		if len(e.partialResultMap)+1 > (1<<e.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
			e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << e.bInMap))
			e.bInMap++
		}
		e.partialResultMap[groupKey] = partialResults
		allMemDelta += int64(len(groupKey))
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(allMemDelta)
	return partialResults
}

func (e *HashAggExec) initRuntimeStats() {
	if e.RuntimeStats() != nil {
		stats := &HashAggRuntimeStats{
			PartialConcurrency: e.Ctx().GetSessionVars().HashAggPartialConcurrency(),
			FinalConcurrency:   e.Ctx().GetSessionVars().HashAggFinalConcurrency(),
		}
		stats.PartialStats = make([]*AggWorkerStat, 0, stats.PartialConcurrency)
		stats.FinalStats = make([]*AggWorkerStat, 0, stats.FinalConcurrency)
		e.stats = stats
	}
}

// IsSpillTriggeredForTest is for test.
func (e *HashAggExec) IsSpillTriggeredForTest() bool {
	for i := range e.spillHelper.lock.spilledChunksIO {
		if len(e.spillHelper.lock.spilledChunksIO[i]) > 0 {
			return true
		}
	}
	return false
}
