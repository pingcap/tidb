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
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/executor/internal/exec"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/channel"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/set"
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
	partialResultMap AggPartialResultMapper
	bInMap           int64 // indicate there are 2^bInMap buckets in partialResultMap
	groupSet         set.StringSetWithMemoryUsage
	groupKeys        []string
	cursor4GroupKey  int
	GroupByItems     []expression.Expression
	groupKeyBuffer   [][]byte

	finishCh         chan struct{}
	finalOutputCh    chan *AfFinalResult
	partialOutputChs []chan *HashAggIntermData
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
	IsUnparallelExec        bool
	parallelExecInitialized bool
	prepared                bool
	executed                bool

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker

	stats *HashAggRuntimeStats

	// listInDisk is the chunks to store row values for spilled data.
	// The HashAggExec may be set to `spill mode` multiple times, and all spilled data will be appended to ListInDisk.
	listInDisk *chunk.ListInDisk
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
	// spillAction save the Action for spilling.
	spillAction *AggSpillDiskAction
	// isChildDrained indicates whether the all data from child has been taken out.
	isChildDrained bool
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	if e.IsUnparallelExec {
		var firstErr error
		e.childResult = nil
		e.groupSet, _ = set.NewStringSetWithMemoryUsage()
		e.partialResultMap = nil
		if e.memTracker != nil {
			e.memTracker.ReplaceBytesUsed(0)
		}
		if e.listInDisk != nil {
			firstErr = e.listInDisk.Close()
		}
		e.spillAction, e.tmpChkForSpill = nil, nil
		if err := e.BaseExecutor.Close(); firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	if e.parallelExecInitialized {
		// `Close` may be called after `Open` without calling `Next` in test.
		if !e.prepared {
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
		e.executed = false
		if e.memTracker != nil {
			e.memTracker.ReplaceBytesUsed(0)
		}
	}
	return e.BaseExecutor.Close()
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
	e.prepared = false

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
	e.initForParallelExec(e.Ctx())
	return nil
}

func (e *HashAggExec) initForUnparallelExec() {
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(AggPartialResultMapper)
	e.bInMap = 0
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice*(1<<e.bInMap) + setSize)
	e.groupKeyBuffer = make([][]byte, 0, 8)
	e.childResult = exec.TryNewCacheChunk(e.Children(0))
	e.memTracker.Consume(e.childResult.MemoryUsage())

	e.offsetOfSpilledChks, e.numOfSpilledChks = 0, 0
	e.executed, e.isChildDrained = false, false
	e.listInDisk = chunk.NewListInDisk(exec.RetTypes(e.Children(0)))

	e.tmpChkForSpill = exec.TryNewCacheChunk(e.Children(0))
	if vars := e.Ctx().GetSessionVars(); vars.TrackAggregateMemoryUsage && variable.EnableTmpStorageOnOOM.Load() {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(vars.StmtCtx.DiskTracker)
		e.listInDisk.GetDiskTracker().AttachTo(e.diskTracker)
		vars.MemTracker.FallbackOldAndSetNewActionForSoftLimit(e.ActionSpill())
	}
}

func (e *HashAggExec) initForParallelExec(_ sessionctx.Context) {
	sessionVars := e.Ctx().GetSessionVars()
	finalConcurrency := sessionVars.HashAggFinalConcurrency()
	partialConcurrency := sessionVars.HashAggPartialConcurrency()
	e.IsChildReturnEmpty = true
	e.finalOutputCh = make(chan *AfFinalResult, finalConcurrency+partialConcurrency+1)
	e.inputCh = make(chan *HashAggInput, partialConcurrency)
	e.finishCh = make(chan struct{}, 1)

	e.partialInputChs = make([]chan *chunk.Chunk, partialConcurrency)
	for i := range e.partialInputChs {
		e.partialInputChs[i] = make(chan *chunk.Chunk, 1)
	}
	e.partialOutputChs = make([]chan *HashAggIntermData, finalConcurrency)
	for i := range e.partialOutputChs {
		e.partialOutputChs[i] = make(chan *HashAggIntermData, partialConcurrency)
	}

	e.partialWorkers = make([]HashAggPartialWorker, partialConcurrency)
	e.finalWorkers = make([]HashAggFinalWorker, finalConcurrency)
	e.initRuntimeStats()

	// Init partial workers.
	for i := 0; i < partialConcurrency; i++ {
		w := HashAggPartialWorker{
			baseHashAggWorker: newBaseHashAggWorker(e.Ctx(), e.finishCh, e.PartialAggFuncs, e.MaxChunkSize(), e.memTracker),
			inputCh:           e.partialInputChs[i],
			outputChs:         e.partialOutputChs,
			giveBackCh:        e.inputCh,
			globalOutputCh:    e.finalOutputCh,
			partialResultsMap: make(AggPartialResultMapper),
			groupByItems:      e.GroupByItems,
			chk:               exec.TryNewCacheChunk(e.Children(0)),
			groupKey:          make([][]byte, 0, 8),
		}
		// There is a bucket in the empty partialResultsMap.
		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMap))
		if e.stats != nil {
			w.stats = &AggWorkerStat{}
			e.stats.PartialStats = append(e.stats.PartialStats, w.stats)
		}
		e.memTracker.Consume(w.chk.MemoryUsage())
		e.partialWorkers[i] = w
		input := &HashAggInput{
			chk:        exec.NewFirstChunk(e.Children(0)),
			giveBackCh: w.inputCh,
		}
		e.memTracker.Consume(input.chk.MemoryUsage())
		e.inputCh <- input
	}

	// Init final workers.
	for i := 0; i < finalConcurrency; i++ {
		groupSet, setSize := set.NewStringSetWithMemoryUsage()
		w := HashAggFinalWorker{
			baseHashAggWorker:   newBaseHashAggWorker(e.Ctx(), e.finishCh, e.FinalAggFuncs, e.MaxChunkSize(), e.memTracker),
			partialResultMap:    make(AggPartialResultMapper),
			groupSet:            groupSet,
			inputCh:             e.partialOutputChs[i],
			outputCh:            e.finalOutputCh,
			finalResultHolderCh: make(chan *chunk.Chunk, 1),
			rowBuffer:           make([]types.Datum, 0, e.Schema().Len()),
			mutableRow:          chunk.MutRowFromTypes(exec.RetTypes(e)),
			groupKeys:           make([][]byte, 0, 8),
		}
		// There is a bucket in the empty partialResultsMap.
		e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice*(1<<w.BInMap) + setSize)
		groupSet.SetTracker(e.memTracker)
		if e.stats != nil {
			w.stats = &AggWorkerStat{}
			e.stats.FinalStats = append(e.stats.FinalStats, w.stats)
		}
		e.finalWorkers[i] = w
		e.finalWorkers[i].finalResultHolderCh <- exec.NewFirstChunk(e)
	}

	e.parallelExecInitialized = true
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
		input.giveBackCh <- chk
	}
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
		go e.finalWorkers[i].run(e.Ctx(), finalWorkerWaitGroup)
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
	if !e.prepared {
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	failpoint.Inject("parallelHashAggError", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			failpoint.Return(errors.New("HashAggExec.parallelExec error"))
		}
	})

	if e.executed {
		return nil
	}
	for {
		result, ok := <-e.finalOutputCh
		if !ok {
			e.executed = true
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
		if e.prepared {
			// Since we return e.MaxChunkSize() rows every time, so we should not traverse
			// `groupSet` because of its randomness.
			for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
				partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
				if len(e.PartialAggFuncs) == 0 {
					chk.SetNumVirtualRows(chk.NumRows() + 1)
				}
				for i, af := range e.PartialAggFuncs {
					if err := af.AppendFinalResult2Chunk(e.Ctx(), partialResults[i], chk); err != nil {
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
		if e.executed {
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
		e.prepared = true
	}
}

func (e *HashAggExec) resetSpillMode() {
	e.cursor4GroupKey, e.groupKeys = 0, e.groupKeys[:0]
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(AggPartialResultMapper)
	e.bInMap = 0
	e.prepared = false
	e.executed = e.numOfSpilledChks == e.listInDisk.NumChunks() // No data is spilling again, all data have been processed.
	e.numOfSpilledChks = e.listInDisk.NumChunks()
	e.memTracker.ReplaceBytesUsed(setSize)
	atomic.StoreUint32(&e.inSpillMode, 0)
}

// execute fetches Chunks from src and update each aggregate function for each row in Chunk.
func (e *HashAggExec) execute(ctx context.Context) (err error) {
	defer func() {
		if e.tmpChkForSpill.NumRows() > 0 && err == nil {
			err = e.listInDisk.Add(e.tmpChkForSpill)
			e.tmpChkForSpill.Reset()
		}
	}()
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
				memDelta, err := af.UpdatePartialResult(e.Ctx(), tmpBuf[:], partialResults[i])
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
		return e.listInDisk.Add(e.childResult)
	}
	for i := 0; i < e.childResult.NumRows(); i++ {
		e.tmpChkForSpill.AppendRow(e.childResult.GetRow(i))
		if e.tmpChkForSpill.IsFull() {
			err = e.listInDisk.Add(e.tmpChkForSpill)
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
		e.childResult, err = e.listInDisk.GetChunk(e.offsetOfSpilledChks)
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
