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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/twmb/murmur3"
)

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker
	ctx sessionctx.Context

	inputCh        chan *chunk.Chunk
	outputChs      []chan *aggfuncs.AggPartialResultMapper
	globalOutputCh chan *AfFinalResult

	// Partial worker transmit the HashAggInput by this channel,
	// so that the data fetcher could get the partial worker's HashAggInput
	giveBackCh chan<- *HashAggInput

	BInMaps               []int
	partialResultsBuffer  [][]aggfuncs.PartialResult
	partialResultNumInRow int

	// Length of this map is equal to the number of final workers
	// All data in one AggPartialResultMapper are specifically sent to a target final worker.
	// e.g. all data in partialResultsMap[3] should be sent to final worker 3.
	partialResultsMap []aggfuncs.AggPartialResultMapper

	groupByItems []expression.Expression
	groupKey     [][]byte
	// chk stores the input data from child,
	// and is reused by childExec and partial worker.
	chk *chunk.Chunk

	isSpillPrepared       bool
	runningWorkerWaiter   *sync.WaitGroup
	spillHelper           *parallelHashAggSpillHelper
	tmpChksForSpill       []*chunk.Chunk
	spillSerializeHelpers []*aggfuncs.SerializeHelper
	getNewTmpChunkFunc    func() *chunk.Chunk
	spillChunkFieldTypes  []*types.FieldType
	spilledChunksIO       []*chunk.DataInDiskByRows // TODO replace it with DataInDiskByChunks
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

func (w *HashAggPartialWorker) fetchChunkAndProcess(ctx sessionctx.Context, hasError *bool, needShuffle *bool, enableIntest bool) bool {
	w.spillHelper.syncLock.RLock()
	defer w.spillHelper.syncLock.RUnlock()

	if w.spillHelper.isInSpilling() {
		// Repeatedly release lock so that it could stop processing data when
		// spill is in execution and spill action could have more chance to get write lock.
		return true
	}

	waitStart := time.Now()
	ok := w.getChildInput()
	if w.stats != nil {
		w.stats.WaitTime += int64(time.Since(waitStart))
	}

	if !ok {
		return false
	}

	execStart := time.Now()
	if err := w.updatePartialResult(ctx, w.chk, len(w.partialResultsMap)); err != nil {
		*hasError = true
		w.processError(err)
		return false
	}
	if w.stats != nil {
		w.stats.ExecTime += int64(time.Since(execStart))
		w.stats.TaskNum++
	}

	// The intermData can be promised to be not empty if reaching here,
	// so we set needShuffle to be true.
	*needShuffle = true

	if intest.InTest && enableIntest {
		num := rand.Intn(10000)
		if num < 7 {
			panic("Intest panic: partial worker is panicked when running")
		} else if num < 14 {
			time.Sleep(1 * time.Second)
		} else if num < 21 {
			*hasError = true
			w.processError(errors.Errorf("Random fail is triggered in partial worker"))
			return false
		} else if num < 28 {
			w.memTracker.Consume(104857600) // Consume 100MiB
		}
	}

	if w.spillHelper.checkError() {
		*hasError = true
		return false
	}
	return true
}

func (w *HashAggPartialWorker) handleSpillBeforeExit() {
	w.spillHelper.syncLock.RLock()
	defer w.spillHelper.syncLock.RUnlock()

	if w.spillHelper.checkError() {
		return
	}

	hasError := false
	if len(w.groupKey) > 0 {
		if err := w.spillDataToDisk(); err != nil {
			w.processError(err)
			hasError = true
		}
	}

	if !hasError {
		w.spillHelper.addListInDisks(w.spilledChunksIO)
	}
}

func (w *HashAggPartialWorker) sendDataToFinalWorkersBeforeExit(needShuffle bool, finalConcurrency int, hasError bool) {
	if hasError {
		return
	}

	w.spillHelper.syncLock.RLock()
	defer w.spillHelper.syncLock.RUnlock()

	w.runningWorkerWaiter.Done()
	w.runningWorkerWaiter.Wait()

	w.spillHelper.setAllPartialWorkersFinished()

	// We should always check spill status first.
	if w.spillHelper.isSpillTriggered() {
		w.handleSpillBeforeExit()
	} else if needShuffle {
		w.shuffleIntermData(finalConcurrency)
	}
}

func (w *HashAggPartialWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	start := time.Now()
	hasError := false
	needShuffle := false
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}

		w.sendDataToFinalWorkersBeforeExit(needShuffle, finalConcurrency, hasError)

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

	for w.fetchChunkAndProcess(ctx, &hasError, &needShuffle, enableIntest) {
	}
}

// If the group key has appeared before, reuse the partial result.
// If the group key has not appeared before, create empty partial results.
func (w *HashAggPartialWorker) getPartialResultsOfEachRow(groupKey [][]byte, finalConcurrency int) [][]aggfuncs.PartialResult {
	mapper := w.partialResultsMap
	numRows := len(groupKey)
	allMemDelta := int64(0)
	w.partialResultsBuffer = w.partialResultsBuffer[0:0]

	for i := 0; i < numRows; i++ {
		finalWorkerIdx := int(murmur3.Sum32(groupKey[i])) % finalConcurrency
		tmp, ok := mapper[finalWorkerIdx][string(hack.String(groupKey[i]))]

		// This group by key has appeared before, reuse the partial result.
		if ok {
			w.partialResultsBuffer = append(w.partialResultsBuffer, tmp)
			continue
		}

		// It's the first time that this group by key appeared, create it
		w.partialResultsBuffer = append(w.partialResultsBuffer, make([]aggfuncs.PartialResult, w.partialResultNumInRow))
		lastIdx := len(w.partialResultsBuffer) - 1
		for j, af := range w.aggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			w.partialResultsBuffer[lastIdx][j] = partialResult
			allMemDelta += memDelta // the memory usage of PartialResult
		}
		allMemDelta += int64(w.partialResultNumInRow * 8)

		// Map will expand when count > bucketNum * loadFactor. The memory usage will double.
		if len(mapper[finalWorkerIdx])+1 > (1<<w.BInMaps[finalWorkerIdx])*hack.LoadFactorNum/hack.LoadFactorDen {
			w.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMaps[finalWorkerIdx]))
			w.BInMaps[finalWorkerIdx]++
		}

		mapper[finalWorkerIdx][string(groupKey[i])] = w.partialResultsBuffer[lastIdx]
		allMemDelta += int64(len(groupKey[i]))
	}
	w.memTracker.Consume(allMemDelta)
	return w.partialResultsBuffer
}

func (w *HashAggPartialWorker) updatePartialResult(ctx sessionctx.Context, chk *chunk.Chunk, finalConcurrency int) (err error) {
	memSize := getGroupKeyMemUsage(w.groupKey)
	w.groupKey, err = GetGroupKey(w.ctx, chk, w.groupKey, w.groupByItems)
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKey) - memSize)
	if err != nil {
		return err
	}

	partialResultOfEachRow := w.getPartialResultsOfEachRow(w.groupKey, finalConcurrency)

	numRows := chk.NumRows()
	rows := make([]chunk.Row, 1)
	allMemDelta := int64(0)
	for i := 0; i < numRows; i++ {
		partialResult := partialResultOfEachRow[i]
		rows[0] = chk.GetRow(i)
		for j, af := range w.aggFuncs {
			memDelta, err := af.UpdatePartialResult(ctx, rows, partialResult[j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
	}
	w.memTracker.Consume(allMemDelta)
	return nil
}

func (w *HashAggPartialWorker) shuffleIntermData(finalConcurrency int) {
	for i := 0; i < finalConcurrency; i++ {
		w.outputChs[i] <- &w.partialResultsMap[i]
	}
}

func (w *HashAggPartialWorker) prepareForSpill() {
	if !w.isSpillPrepared {
		w.isSpillPrepared = true
		w.tmpChksForSpill = make([]*chunk.Chunk, spilledPartitionNum)
		w.spilledChunksIO = make([]*chunk.DataInDiskByRows, spilledPartitionNum)
		for i := 0; i < spilledPartitionNum; i++ {
			w.tmpChksForSpill[i] = w.getNewTmpChunkFunc()
			w.spilledChunksIO[i] = chunk.NewDataInDiskByRows(w.spillChunkFieldTypes)
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

	w.prepareForSpill()
	for _, partialResultsMap := range w.partialResultsMap {
		for key, partialResults := range partialResultsMap {
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
				aggFunc.SerializePartialResult(partialResults[i], w.tmpChksForSpill[partitionNum], w.spillSerializeHelpers[i])
			}

			// Append key
			w.tmpChksForSpill[partitionNum].AppendString(len(w.aggFuncs), key)
		}
	}

	// Clear the partialResultsMap
	w.partialResultsMap = make([]aggfuncs.AggPartialResultMapper, len(w.partialResultsMap))
	for i := range w.partialResultsMap {
		w.partialResultsMap[i] = make(aggfuncs.AggPartialResultMapper)
	}

	w.memTracker.Consume(-w.partialResultsMem)
	w.partialResultsMem = 0
	for i := range w.BInMaps {
		w.BInMaps[i] = 0
	}

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

func (w *HashAggPartialWorker) processError(err error) {
	w.globalOutputCh <- &AfFinalResult{err: err}
	w.spillHelper.setError()
}
