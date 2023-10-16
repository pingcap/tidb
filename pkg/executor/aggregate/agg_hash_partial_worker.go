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
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/twmb/murmur3"
)

// HashAggIntermData indicates the intermediate data of aggregation execution.
type HashAggIntermData struct {
	groupKeys        []string
	cursor           int
	partialResultMap aggfuncs.AggPartialResultMapper
}

// getPartialResultBatch fetches a batch of partial results from HashAggIntermData.
func (d *HashAggIntermData) getPartialResultBatch(_ *stmtctx.StatementContext, partialResult [][]aggfuncs.PartialResult, _ []aggfuncs.AggFunc, maxChunkSize int) (_ [][]aggfuncs.PartialResult, groupKeys []string, reachEnd bool) {
	keyStart := d.cursor
	for ; d.cursor < len(d.groupKeys) && len(partialResult) < maxChunkSize; d.cursor++ {
		partialResult = append(partialResult, d.partialResultMap[d.groupKeys[d.cursor]])
	}
	if d.cursor == len(d.groupKeys) {
		reachEnd = true
	}
	return partialResult, d.groupKeys[keyStart:d.cursor], reachEnd
}

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker
	ctx sessionctx.Context

	inputCh        chan *chunk.Chunk
	outputChs      []chan *HashAggIntermData
	globalOutputCh chan *AfFinalResult

	// Partial worker transmit the HashAggInput by this channel,
	// so that the data fetcher could get the partial worker's HashAggInput
	giveBackCh chan<- *HashAggInput

	partialResultsMap aggfuncs.AggPartialResultMapper
	groupByItems      []expression.Expression
	groupKey          [][]byte
	// chk stores the input data from child,
	// and is reused by childExec and partial worker.
	chk *chunk.Chunk

	isSpillPrepared       bool
	workerSync            *partialWorkerSync
	spillHelper           *parallelHashAggSpillHelper
	tmpChksForSpill       []*chunk.Chunk
	spillSerializeHelpers []aggfuncs.SpillSerializeHelper
	getNewTmpChunkFunc    func() *chunk.Chunk
	spillChunkFieldTypes  []*types.FieldType
	spilledChunksIO       []*chunk.ListInDisk
}

func (w *HashAggPartialWorker) getChildInput() bool {
	select {
	case <-w.finishCh:
		return false
	case chk, ok := <-w.inputCh:
		if !ok {
			return false
		}
		w.chk.SwapColumns(chk)
		w.giveBackCh <- &HashAggInput{
			chk:        chk,
			giveBackCh: w.inputCh,
		}
	}
	return true
}

func (w *HashAggPartialWorker) waitForTheFinishOfSpill(isLeaving bool) {
	// Notify the action that I'm in waiting status
	w.spillHelper.lock.Lock()
	if isLeaving {
		w.spillHelper.runningPartialWorkerNum--
		if w.spillHelper.runningPartialWorkerNum == 0 {
			w.spillHelper.leavePartialStage()
		}
	}
	w.spillHelper.lock.Unlock()
	w.spillHelper.waitForPartialWorkersSyncer <- struct{}{}

	// Wait for the finish of spill
	<-w.spillHelper.spillActionAndPartialWorkerSyncer
}

func (w *HashAggPartialWorker) waitForSpillDoneBeforeWorkerExit() {
	w.spillHelper.lock.Lock()
	if w.spillHelper.isInSpillingNoLock() {
		w.spillHelper.lock.Unlock()
		w.waitForTheFinishOfSpill(true)
		return
	}
	w.spillHelper.runningPartialWorkerNum--
	if w.spillHelper.runningPartialWorkerNum == 0 {
		w.spillHelper.leavePartialStage()
	}
	w.spillHelper.lock.Unlock()
}

func (w *HashAggPartialWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	start := time.Now()
	needShuffle, sc := false, ctx.GetSessionVars().StmtCtx
	hasError := false
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}

		w.waitForSpillDoneBeforeWorkerExit()
		w.workerSync.waitForRunningWorkers()

		if !hasError {
			isSpilled := w.spillHelper.isSpillTriggered()
			spillError := w.spillHelper.checkError()
			if isSpilled && !spillError {
				// Do not put `w.spillHelper.needSpill()` and `len(w.groupKey) > 0` judgement in one line
				if len(w.groupKey) > 0 {
					if err := w.spillDataToDisk(); err != nil {
						w.globalOutputCh <- &AfFinalResult{err: err}
						w.spillHelper.setError()
						hasError = true
					}
				}
				if !hasError && len(w.spilledChunksIO) == spilledPartitionNum {
					w.spillHelper.addListInDisks(w.spilledChunksIO)
				}
			} else if needShuffle {
				w.shuffleIntermData(sc, finalConcurrency)
			}
		}

		w.workerSync.waitForExitOfAliveWorkers()

		w.memTracker.Consume(-w.chk.MemoryUsage())
		if w.stats != nil {
			w.stats.WorkerTime += int64(time.Since(start))
		}
		waitGroup.Done()
	}()

	enableIntest := false
	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			enableIntest = true
		}
	})

	failpoint.Inject("triggerSpill", func(val failpoint.Value) {
		// 0.9 ensure that it will exceed the soft limit, soft limit factor is 0.8.
		consumeNum := float32(val.(int)) * 0.9
		w.memTracker.Consume(int64(consumeNum))
	})

	if intest.InTest && enableIntest {
		num := rand.Intn(100)
		if num < 2 {
			panic("Intest panic: partial worker is panicked before start")
		} else if num >= 2 && num < 4 {
			time.Sleep(2 * time.Second)
		}
	}

	for {
		waitStart := time.Now()
		ok := w.getChildInput()
		if w.stats != nil {
			w.stats.WaitTime += int64(time.Since(waitStart))
		}

		if !ok {
			return
		}

		execStart := time.Now()
		if err := w.updatePartialResult(ctx, sc, w.chk, len(w.partialResultsMap)); err != nil {
			hasError = true
			w.globalOutputCh <- &AfFinalResult{err: err}
			w.spillHelper.setError()
			return
		}
		if w.stats != nil {
			w.stats.ExecTime += int64(time.Since(execStart))
			w.stats.TaskNum++
		}

		// The intermData can be promised to be not empty if reaching here,
		// so we set needShuffle to be true.
		needShuffle = true

		if intest.InTest && enableIntest {
			num := rand.Intn(10000)
			if num < 7 {
				panic("Intest panic: partial worker is panicked when running")
			} else if num < 14 {
				time.Sleep(1 * time.Second)
			} else if num < 21 {
				hasError = true
				w.globalOutputCh <- &AfFinalResult{err: errors.Errorf("Random fail is triggered in partial worker")}
				w.spillHelper.setError()
				return
			} else if num < 28 {
				w.memTracker.Consume(104857600) // Consume 100MiB
			}
		}

		if w.spillHelper.isInSpilling() {
			w.waitForTheFinishOfSpill(false)
			if w.spillHelper.checkError() {
				hasError = true
				return
			}
		}
	}
}

func (w *HashAggPartialWorker) updatePartialResult(ctx sessionctx.Context, sc *stmtctx.StatementContext, chk *chunk.Chunk, _ int) (err error) {
	memSize := getGroupKeyMemUsage(w.groupKey)
	w.groupKey, err = GetGroupKey(w.ctx, chk, w.groupKey, w.groupByItems)
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKey) - memSize)
	if err != nil {
		return err
	}

	partialResults := w.getPartialResult(sc, w.groupKey, w.partialResultsMap)
	numRows := chk.NumRows()
	rows := make([]chunk.Row, 1)
	allMemDelta := int64(0)
	for i := 0; i < numRows; i++ {
		for j, af := range w.aggFuncs {
			rows[0] = chk.GetRow(i)
			memDelta, err := af.UpdatePartialResult(ctx, rows, partialResults[i][j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
	}
	w.memTracker.Consume(allMemDelta)
	return nil
}

// shuffleIntermData shuffles the intermediate data of partial workers to corresponded final workers.
// We only support parallel execution for single-machine, so process of encode and decode can be skipped.
func (w *HashAggPartialWorker) shuffleIntermData(_ *stmtctx.StatementContext, finalConcurrency int) {
	groupKeysSlice := make([][]string, finalConcurrency)
	for groupKey := range w.partialResultsMap {
		finalWorkerIdx := int(murmur3.Sum32([]byte(groupKey))) % finalConcurrency
		if groupKeysSlice[finalWorkerIdx] == nil {
			groupKeysSlice[finalWorkerIdx] = make([]string, 0, len(w.partialResultsMap)/finalConcurrency)
		}
		groupKeysSlice[finalWorkerIdx] = append(groupKeysSlice[finalWorkerIdx], groupKey)
	}

	for i := range groupKeysSlice {
		if groupKeysSlice[i] == nil {
			continue
		}
		w.outputChs[i] <- &HashAggIntermData{
			groupKeys:        groupKeysSlice[i],
			partialResultMap: w.partialResultsMap,
		}
	}
}

func (w *HashAggPartialWorker) prepareForSpillWhenNeeded() {
	if !w.isSpillPrepared {
		w.isSpillPrepared = true
		w.tmpChksForSpill = make([]*chunk.Chunk, spilledPartitionNum)
		w.spilledChunksIO = make([]*chunk.ListInDisk, spilledPartitionNum)
		for i := 0; i < spilledPartitionNum; i++ {
			w.tmpChksForSpill[i] = w.getNewTmpChunkFunc()
			w.spilledChunksIO[i] = chunk.NewListInDisk(w.spillChunkFieldTypes)
			if w.spillHelper.isTrackerEnabled {
				w.spilledChunksIO[i].GetDiskTracker().AttachTo(w.spillHelper.diskTracker)
			}
		}
	}
}

func (w *HashAggPartialWorker) spillDataToDisk() error {
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}
	}()
	if len(w.partialResultsMap) == 0 {
		return nil
	}

	w.prepareForSpillWhenNeeded()
	for key, partialResults := range w.partialResultsMap {
		partitionNum := int(murmur3.Sum32([]byte(key))) % spilledPartitionNum

		// Spill data when tmp chunk is full
		if w.tmpChksForSpill[partitionNum].IsFull() {
			err := w.spilledChunksIO[partitionNum].Add(w.tmpChksForSpill[partitionNum])
			if err != nil {
				return err
			}
			w.tmpChksForSpill[partitionNum].Reset()
		}

		// Serialize agg meta data to the tmp chunk
		for i, aggFunc := range w.aggFuncs {
			aggFunc.SerializePartialResult(w.ctx, partialResults[i], w.tmpChksForSpill[partitionNum], &(w.spillSerializeHelpers[i]))
		}

		// Append key
		w.tmpChksForSpill[partitionNum].AppendString(len(w.aggFuncs), key)
	}

	// Clear the groupby keys and partialResultsMap
	w.partialResultsMap = make(aggfuncs.AggPartialResultMapper)
	w.memTracker.Consume(-w.partialResultsMem)
	w.BInMap = 0
	w.partialResultsMem = 0

	// Trigger the spill of remaining data
	err := w.spillRemainingDataToDisk(w.ctx)
	if err != nil {
		return err
	}
	return nil
}

// Some tmp chunks may no be full, so we need to manually trigger the spill action.
func (w *HashAggPartialWorker) spillRemainingDataToDisk(ctx sessionctx.Context) error {
	for i := 0; i < spilledPartitionNum; i++ {
		if w.tmpChksForSpill[i].NumRows() > 0 {
			err := w.spilledChunksIO[i].Add(w.tmpChksForSpill[i])
			if err != nil {
				return err
			}
			w.tmpChksForSpill[i].Reset()
		}
	}
	return nil
}

// Exist is split into three stage: 1. wait running workers 2. do something 3. wait alive workers
//
//	Stage 1. decrease one running partial worker number and wait for the existence of the rest of workers
//	Stage 2. do something, include checking if we need to spill data to disk
//	Stage 3. descrease one alive partial worker number and notify final worker to execute
type partialWorkerSync struct {
	lock                    sync.Mutex
	totalFinalWorkerNum     int
	totalPartialWorkerNum   int
	runningPartialWorkerNum int
	alivePartialWorkerNum   int

	// The last running partial worker will send msg to this channel
	// so that the waiting workers can step to the next stage.
	partialWorkDoneNotifier chan struct{}

	// The last alive partial worker will send msg to this channel
	// so that the final workers will start their tasks.
	partialAndFinalNotifier chan struct{}
}

func (p *partialWorkerSync) waitForRunningWorkers() {
	p.lock.Lock()
	p.runningPartialWorkerNum--
	isTheLastWorker := (p.runningPartialWorkerNum == 0)
	p.lock.Unlock()

	if isTheLastWorker {
		// sendNum starts from 1 as it should not send to itself
		for sendNum := 1; sendNum < p.totalPartialWorkerNum; sendNum++ {
			p.partialWorkDoneNotifier <- struct{}{}
		}
	} else {
		<-p.partialWorkDoneNotifier
	}
}

func (p *partialWorkerSync) waitForExitOfAliveWorkers() {
	p.lock.Lock()
	p.alivePartialWorkerNum--
	isTheLastWorker := (p.alivePartialWorkerNum == 0)
	p.lock.Unlock()

	if isTheLastWorker {
		for sendNum := 0; sendNum < p.totalFinalWorkerNum; sendNum++ {
			p.partialAndFinalNotifier <- struct{}{}
		}
	}
}
