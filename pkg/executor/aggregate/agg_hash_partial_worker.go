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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/twmb/murmur3"
)

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker
	idForTest int
	ctx       sessionctx.Context

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
	partialResultsMap    []aggfuncs.AggPartialResultMapper
	partialResultsMapMem atomic.Int64

	groupByItems []expression.Expression
	groupKeyBuf  [][]byte
	// chk stores the input data from child,
	// and is reused by childExec and partial worker.
	chk *chunk.Chunk

	isSpillPrepared  bool
	spillHelper      *parallelHashAggSpillHelper
	tmpChksForSpill  []*chunk.Chunk
	serializeHelpers *aggfuncs.SerializeHelper
	spilledChunksIO  []*chunk.DataInDiskByChunks

	// It's useful when spill is triggered and the fetcher could know when partial workers finish their works.
	inflightChunkSync *sync.WaitGroup
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

func (w *HashAggPartialWorker) fetchChunkAndProcess(ctx sessionctx.Context, hasError *bool, needShuffle *bool) bool {
	if w.spillHelper.checkError() {
		*hasError = true
		return false
	}

	waitStart := time.Now()
	ok := w.getChildInput()
	updateWaitTime(w.stats, waitStart)

	if !ok {
		return false
	}

	defer w.inflightChunkSync.Done()

	execStart := time.Now()
	if err := w.updatePartialResult(ctx, w.chk, len(w.partialResultsMap)); err != nil {
		*hasError = true
		w.processError(err)
		return false
	}
	updateExecTime(w.stats, execStart)

	// The intermData can be promised to be not empty if reaching here,
	// so we set needShuffle to be true.
	*needShuffle = true

	w.intestDuringPartialWorkerRun()
	return true
}

func (w *HashAggPartialWorker) intestDuringPartialWorkerRun() {
	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(10000)
			if num < 3 {
				panic("Intest panic: partial worker is panicked when running")
			} else if num < 6 {
				w.processError(errors.Errorf("Random fail is triggered in partial worker"))
			} else if num < 9 {
				consumedMem := int64(500000)
				w.memTracker.Consume(consumedMem)
				w.partialResultsMapMem.Add(consumedMem)
			}

			// Slow some partial workers
			if w.idForTest%2 == 0 && num < 15 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	})

	failpoint.Inject("slowSomePartialWorkers", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(10000)
			// Slow some partial workers
			if w.idForTest%2 == 0 && num < 10 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	})
}

func intestBeforePartialWorkerRun() {
	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(100)
			if num < 2 {
				panic("Intest panic: partial worker is panicked before start")
			} else if num >= 2 && num < 4 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	})
}

func (w *HashAggPartialWorker) finalizeWorkerProcess(needShuffle bool, finalConcurrency int, hasError bool) {
	// Consume all chunks to avoid hang of fetcher
	for range w.inputCh {
		w.inflightChunkSync.Done()
	}

	if w.checkFinishChClosed() {
		return
	}

	if hasError {
		return
	}

	if needShuffle && w.spillHelper.isSpilledChunksIOEmpty() {
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

		w.finalizeWorkerProcess(needShuffle, finalConcurrency, hasError)

		w.memTracker.Consume(-w.chk.MemoryUsage())
		updateWorkerTime(w.stats, start)

		// We must ensure that there is no panic before `waitGroup.Done()` or there will be hang
		waitGroup.Done()

		tryRecycleBuffer(&w.partialResultsBuffer, &w.groupKeyBuf)
	}()

	intestBeforePartialWorkerRun()

	for w.fetchChunkAndProcess(ctx, &hasError, &needShuffle) {
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
			expandMem := hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMaps[finalWorkerIdx])
			w.partialResultsMapMem.Add(int64(expandMem))
			w.memTracker.Consume(int64(expandMem))
			w.BInMaps[finalWorkerIdx]++
		}

		mapper[finalWorkerIdx][string(groupKey[i])] = w.partialResultsBuffer[lastIdx]
		allMemDelta += int64(len(groupKey[i]))
	}
	w.partialResultsMapMem.Add(allMemDelta)
	w.memTracker.Consume(allMemDelta)
	return w.partialResultsBuffer
}

func (w *HashAggPartialWorker) updatePartialResult(ctx sessionctx.Context, chk *chunk.Chunk, finalConcurrency int) (err error) {
	memSize := getGroupKeyMemUsage(w.groupKeyBuf)
	w.groupKeyBuf, err = GetGroupKey(w.ctx, chk, w.groupKeyBuf, w.groupByItems)
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKeyBuf) - memSize)
	if err != nil {
		return err
	}

	partialResultOfEachRow := w.getPartialResultsOfEachRow(w.groupKeyBuf, finalConcurrency)

	numRows := chk.NumRows()
	rows := make([]chunk.Row, 1)
	allMemDelta := int64(0)
	exprCtx := ctx.GetExprCtx()
	for i := 0; i < numRows; i++ {
		partialResult := partialResultOfEachRow[i]
		rows[0] = chk.GetRow(i)
		for j, af := range w.aggFuncs {
			memDelta, err := af.UpdatePartialResult(exprCtx.GetEvalCtx(), rows, partialResult[j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
	}
	w.memTracker.Consume(allMemDelta)
	w.partialResultsMapMem.Add(allMemDelta)
	return nil
}

func (w *HashAggPartialWorker) shuffleIntermData(finalConcurrency int) {
	for i := 0; i < finalConcurrency; i++ {
		w.outputChs[i] <- &w.partialResultsMap[i]
	}
}

func (w *HashAggPartialWorker) prepareForSpill() {
	if !w.isSpillPrepared {
		w.tmpChksForSpill = make([]*chunk.Chunk, spilledPartitionNum)
		w.spilledChunksIO = make([]*chunk.DataInDiskByChunks, spilledPartitionNum)
		for i := 0; i < spilledPartitionNum; i++ {
			w.tmpChksForSpill[i] = w.spillHelper.getNewSpillChunkFunc()
			w.spilledChunksIO[i] = chunk.NewDataInDiskByChunks(w.spillHelper.spillChunkFieldTypes)
			if w.spillHelper.diskTracker != nil {
				w.spilledChunksIO[i].GetDiskTracker().AttachTo(w.spillHelper.diskTracker)
			}
		}
		w.isSpillPrepared = true
	}
}

func (w *HashAggPartialWorker) spillDataToDisk() error {
	err := w.spillDataToDiskImpl()
	if err == nil {
		err = failpointError()
	}
	return err
}

func (w *HashAggPartialWorker) spillDataToDiskImpl() error {
	if len(w.partialResultsMap) == 0 {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}

		// Clear the partialResultsMap
		w.partialResultsMap = make([]aggfuncs.AggPartialResultMapper, len(w.partialResultsMap))
		for i := range w.partialResultsMap {
			w.partialResultsMap[i] = make(aggfuncs.AggPartialResultMapper)
		}

		w.memTracker.Consume(-w.partialResultsMapMem.Load())
		w.partialResultsMapMem.Store(0)
		for i := range w.BInMaps {
			w.BInMaps[i] = 0
		}
	}()

	w.prepareForSpill()
	for _, partialResultsMap := range w.partialResultsMap {
		for key, partialResults := range partialResultsMap {
			partitionNum := int(murmur3.Sum32(hack.Slice(key))) % spilledPartitionNum

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
				aggFunc.SerializePartialResult(partialResults[i], w.tmpChksForSpill[partitionNum], w.serializeHelpers)
			}

			// Append key
			w.tmpChksForSpill[partitionNum].AppendString(len(w.aggFuncs), key)
		}
	}

	// Trigger the spill of remaining data
	err := w.spillRemainingDataToDisk()
	if err != nil {
		return err
	}
	return nil
}

// Some tmp chunks may no be full, so we need to manually trigger the spill action.
func (w *HashAggPartialWorker) spillRemainingDataToDisk() error {
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
