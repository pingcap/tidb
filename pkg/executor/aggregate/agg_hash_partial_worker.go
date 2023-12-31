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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/twmb/murmur3"
)

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker

	inputCh        chan *chunk.Chunk
	outputChs      []chan *aggfuncs.AggPartialResultMapper
	globalOutputCh chan *AfFinalResult
	giveBackCh     chan<- *HashAggInput
	BInMaps        []int

	partialResultsBuffer  [][]aggfuncs.PartialResult
	partialResultNumInRow int

	// Length of this map is equal to the number of final workers
	partialResultsMap []aggfuncs.AggPartialResultMapper
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
	needShuffle := false
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
		if err := w.updatePartialResult(ctx, w.chk, len(w.partialResultsMap)); err != nil {
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
