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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/twmb/murmur3"
)

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker

	inputCh        chan *chunk.Chunk
	outputChs      []chan *AggPartialResultMapper
	globalOutputCh chan *AfFinalResult
	giveBackCh     chan<- *HashAggInput

	partialResultsBuffer               [][][]aggfuncs.PartialResult
	finalWorkerIdxOfEachGroupKeyBuffer []int
	partialResultsIdxsBuffer           []int

	BInMaps []int

	// Length of this map is equal to the number of final workers
	partialResultsMap []AggPartialResultMapper
	groupByItems      []expression.Expression
	groupKey          [][]byte
	// chk stores the input data from child,
	// and is reused by childExec and partial worker.
	chk *chunk.Chunk
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

func (w *HashAggPartialWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	start := time.Now()
	needShuffle, sc := false, ctx.GetSessionVars().StmtCtx
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}
		if needShuffle {
			w.shuffleIntermData(finalConcurrency)
		}
		w.memTracker.Consume(-w.chk.MemoryUsage())
		if w.stats != nil {
			w.stats.WorkerTime += int64(time.Since(start))
		}
		waitGroup.Done()
	}()
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
			w.globalOutputCh <- &AfFinalResult{err: err}
			return
		}
		if w.stats != nil {
			w.stats.ExecTime += int64(time.Since(execStart))
			w.stats.TaskNum++
		}
		// The intermData can be promised to be not empty if reaching here,
		// so we set needShuffle to be true.
		needShuffle = true
	}
}

func (w *HashAggPartialWorker) newFinalWorkerIdxs(rowNum int) []int {
	if rowNum > len(w.finalWorkerIdxOfEachGroupKeyBuffer) {
		w.finalWorkerIdxOfEachGroupKeyBuffer = make([]int, rowNum)
	}
	return w.finalWorkerIdxOfEachGroupKeyBuffer[:rowNum]
}

func (w *HashAggPartialWorker) newPartialResults(finalConcurrency int) [][][]aggfuncs.PartialResult {
	partialResults := w.partialResultsBuffer
	for i := 0; i < finalConcurrency; i++ {
		partialResults[i] = partialResults[i][0:0]
	}
	return partialResults
}

func (w *HashAggPartialWorker) expandPartialResults(partialResult [][]aggfuncs.PartialResult, finalWorkerIdx int, partialResultSize int) [][]aggfuncs.PartialResult {
	partialResultLen := len(partialResult)
	if len(w.partialResultsBuffer[finalWorkerIdx]) < partialResultLen+1 {
		w.partialResultsBuffer[finalWorkerIdx] = append(w.partialResultsBuffer[finalWorkerIdx], make([]aggfuncs.PartialResult, partialResultSize))
	}
	return w.partialResultsBuffer[finalWorkerIdx][:partialResultLen+1]
}

// getPartialResultsOfEachGroupKey gets the partial results of each group key.
// If the group key has appeared before, reuse the partial result.
// If the group key has not appeared before, create empty partial results.
//
// All of the partial results will be divided into `finalConcurrency` parts according to the hash value of group key，
// and the partial results belonging to the same part will be sent to the same final worker.
func (w *HashAggPartialWorker) getPartialResultsForAllFinalWorker(_ *stmtctx.StatementContext, groupKey [][]byte, mapper []AggPartialResultMapper, finalConcurrency int) ([]int, [][][]aggfuncs.PartialResult) {
	cntOfGroupKeys := len(groupKey)
	allMemDelta := int64(0)
	partialResultSize := w.getPartialResultSliceLenConsiderByteAlign()

	// partialResultsOfEachGroupKey[i] indicates the partial results that will be sent to the i-th final worker.
	partialResultsOfEachGroupKey := w.newPartialResults(finalConcurrency)
	// finalWorkerIdxOfEachGroupKey indicates the final worker index that this group by key will be sent to.
	finalWorkerIdxOfEachGroupKey := w.newFinalWorkerIdxs(cntOfGroupKeys)

	for i := 0; i < cntOfGroupKeys; i++ {
		finalWorkerIdx := int(murmur3.Sum32(groupKey[i])) % finalConcurrency
		finalWorkerIdxOfEachGroupKey[i] = finalWorkerIdx
		tmp, ok := mapper[finalWorkerIdx][string(groupKey[i])]

		// This group by key has appeared before, reuse the partial result.
		if ok {
			partialResultsOfEachGroupKey[finalWorkerIdx] = append(partialResultsOfEachGroupKey[finalWorkerIdx], tmp)
			continue
		}

		// It's the first time that this group by key appeared, create it
		partialResultsOfEachGroupKey[finalWorkerIdx] = w.expandPartialResults(partialResultsOfEachGroupKey[finalWorkerIdx], finalWorkerIdx, partialResultSize)
		lastIdx := len(partialResultsOfEachGroupKey[finalWorkerIdx]) - 1
		for j, af := range w.aggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResultsOfEachGroupKey[finalWorkerIdx][lastIdx][j] = partialResult
			allMemDelta += memDelta // the memory usage of PartialResult
		}
		allMemDelta += int64(partialResultSize * 8)

		// Map will expand when count > bucketNum * loadFactor. The memory usage will double.
		if len(mapper)+1 > (1<<w.BInMaps[finalWorkerIdx])*hack.LoadFactorNum/hack.LoadFactorDen {
			w.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMaps[finalWorkerIdx]))
			w.BInMaps[finalWorkerIdx]++
		}
		mapper[finalWorkerIdx][string(groupKey[i])] = partialResultsOfEachGroupKey[finalWorkerIdx][lastIdx]
		allMemDelta += int64(len(groupKey[i]))
	}
	w.memTracker.Consume(allMemDelta)
	return finalWorkerIdxOfEachGroupKey, partialResultsOfEachGroupKey
}

func (w *HashAggPartialWorker) updatePartialResult(ctx sessionctx.Context, sc *stmtctx.StatementContext, chk *chunk.Chunk, finalConcurrency int) (err error) {
	memSize := getGroupKeyMemUsage(w.groupKey)
	w.groupKey, err = GetGroupKey(w.ctx, chk, w.groupKey, w.groupByItems)
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKey) - memSize)
	if err != nil {
		return err
	}

	finalWorkerIdxs, partialResults := w.getPartialResultsForAllFinalWorker(sc, w.groupKey, w.partialResultsMap, finalConcurrency)

	partialResultsIdxs := w.partialResultsIdxsBuffer
	for i := 0; i < finalConcurrency; i++ {
		partialResultsIdxs[i] = 0
	}
	numRows := chk.NumRows()
	rows := make([]chunk.Row, 1)
	allMemDelta := int64(0)
	for i := 0; i < numRows; i++ {
		finalWorkerIdx := finalWorkerIdxs[i]
		for j, af := range w.aggFuncs {
			rows[0] = chk.GetRow(i)
			memDelta, err := af.UpdatePartialResult(ctx, rows, partialResults[finalWorkerIdx][partialResultsIdxs[finalWorkerIdx]][j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
		partialResultsIdxs[finalWorkerIdx]++
	}
	w.memTracker.Consume(allMemDelta)
	return nil
}

func (w *HashAggPartialWorker) shuffleIntermData(finalConcurrency int) {
	for i := 0; i < finalConcurrency; i++ {
		w.outputChs[i] <- &w.partialResultsMap[i]
	}
}
