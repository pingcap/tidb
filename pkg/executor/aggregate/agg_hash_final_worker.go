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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// AfFinalResult indicates aggregation functions final result.
type AfFinalResult struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// HashAggFinalWorker indicates the final workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_final_concurrency`.
type HashAggFinalWorker struct {
	baseHashAggWorker

	mutableRow          chunk.MutRow
	partialResultMap    aggfuncs.AggPartialResultMapper
	BInMap              int
	inputCh             chan *aggfuncs.AggPartialResultMapper
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk

	spillHelper *parallelHashAggSpillHelper

	restoredAggResultMapperMem int64
}

func (w *HashAggFinalWorker) getInputFromDisk(sctx sessionctx.Context) (ret aggfuncs.AggPartialResultMapper, restoredMem int64, err error) {
	ret, restoredMem, err = w.spillHelper.restoreOnePartition(sctx)
	w.intestDuringFinalWorkerRun(&err)
	return ret, restoredMem, err
}

func (w *HashAggFinalWorker) getPartialInput() (input *aggfuncs.AggPartialResultMapper, ok bool) {
	waitStart := time.Now()
	defer updateWaitTime(w.stats, waitStart)
	select {
	case <-w.finishCh:
		return nil, false
	case input, ok = <-w.inputCh:
		if !ok {
			return nil, false
		}
	}
	return
}

func (w *HashAggFinalWorker) initBInMap() {
	w.BInMap = 0
	mapLen := len(w.partialResultMap)
	for mapLen > (1<<w.BInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		w.BInMap++
	}
}

func (w *HashAggFinalWorker) mergeInputIntoResultMap(sctx sessionctx.Context, input *aggfuncs.AggPartialResultMapper) error {
	// As the w.partialResultMap is empty when we get the first input.
	// So it's better to directly assign the input to w.partialResultMap
	if len(w.partialResultMap) == 0 {
		w.partialResultMap = *input
		w.initBInMap()
		return nil
	}

	execStart := time.Now()
	allMemDelta := int64(0)
	exprCtx := sctx.GetExprCtx()
	for key, value := range *input {
		dstVal, ok := w.partialResultMap[key]
		if !ok {
			w.handleNewGroupKey(key, value)
			continue
		}

		for j, af := range w.aggFuncs {
			memDelta, err := af.MergePartialResult(exprCtx.GetEvalCtx(), value[j], dstVal[j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
	}
	w.memTracker.Consume(allMemDelta)
	updateExecTime(w.stats, execStart)
	return nil
}

func (w *HashAggFinalWorker) handleNewGroupKey(key string, value []aggfuncs.PartialResult) {
	if len(w.partialResultMap)+1 > (1<<w.BInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		w.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMap))
		w.BInMap++
	}
	w.partialResultMap[key] = value
}

func (w *HashAggFinalWorker) consumeIntermData(sctx sessionctx.Context) error {
	for {
		input, ok := w.getPartialInput()
		if !ok {
			return nil
		}

		failpoint.Inject("ConsumeRandomPanic", nil)

		if err := w.mergeInputIntoResultMap(sctx, input); err != nil {
			return err
		}
	}
}

func (w *HashAggFinalWorker) generateResultAndSend(sctx sessionctx.Context, result *chunk.Chunk) {
	var finished bool
	exprCtx := sctx.GetExprCtx()
	for _, results := range w.partialResultMap {
		for j, af := range w.aggFuncs {
			if err := af.AppendFinalResult2Chunk(exprCtx.GetEvalCtx(), results[j], result); err != nil {
				logutil.BgLogger().Error("HashAggFinalWorker failed to append final result to Chunk", zap.Error(err))
			}
		}

		if len(w.aggFuncs) == 0 {
			result.SetNumVirtualRows(result.NumRows() + 1)
		}

		if result.IsFull() {
			w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
			result, finished = w.receiveFinalResultHolder()
			if finished {
				return
			}
		}
	}
}

func (w *HashAggFinalWorker) sendFinalResult(sctx sessionctx.Context) {
	waitStart := time.Now()
	result, finished := w.receiveFinalResultHolder()
	updateWaitTime(w.stats, waitStart)
	if finished {
		return
	}

	failpoint.Inject("ConsumeRandomPanic", nil)

	execStart := time.Now()
	updateExecTime(w.stats, execStart)
	if w.spillHelper.isSpilledChunksIOEmpty() {
		w.generateResultAndSend(sctx, result)
	} else {
		for {
			if w.checkFinishChClosed() {
				return
			}

			eof, hasError := w.restoreDataFromDisk(sctx)
			if hasError {
				return
			}
			if eof {
				break
			}
			w.generateResultAndSend(sctx, result)
		}
	}

	w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
}

func (w *HashAggFinalWorker) restoreDataFromDisk(sctx sessionctx.Context) (eof bool, hasError bool) {
	var err error

	// Since data is restored partition by partition, only one partition is in memory at any given time.
	// Therefore, it's necessary to release the memory used by the previous partition.
	w.spillHelper.memTracker.Consume(-w.restoredAggResultMapperMem)
	w.partialResultMap, w.restoredAggResultMapperMem, err = w.getInputFromDisk(sctx)
	if err != nil {
		w.outputCh <- &AfFinalResult{err: err}
		return false, true
	}

	if w.partialResultMap == nil {
		// All partitions have been restored
		return true, false
	}
	return false, false
}

func (w *HashAggFinalWorker) receiveFinalResultHolder() (*chunk.Chunk, bool) {
	select {
	case <-w.finishCh:
		return nil, true
	case result, ok := <-w.finalResultHolderCh:
		return result, !ok
	}
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup, partialWorkerWaiter *sync.WaitGroup) {
	start := time.Now()
	defer w.cleanup(start, waitGroup)

	partialWorkerWaiter.Wait()

	intestBeforeFinalWorkerStart()

	if w.spillHelper.isSpilledChunksIOEmpty() {
		err := w.consumeIntermData(ctx)
		if err != nil {
			w.outputCh <- &AfFinalResult{err: err}
			return
		}
	} else {
		if w.spillHelper.checkError() {
			return
		}
	}
	w.sendFinalResult(ctx)
}

func (w *HashAggFinalWorker) cleanup(start time.Time, waitGroup *sync.WaitGroup) {
	if r := recover(); r != nil {
		recoveryHashAgg(w.outputCh, r)
	}
	updateWorkerTime(w.stats, start)
	waitGroup.Done()
}

func intestBeforeFinalWorkerStart() {
	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(50)
			if num < 3 {
				panic("Intest panic: final worker is panicked before start")
			} else if num < 6 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	})
}

func (w *HashAggFinalWorker) intestDuringFinalWorkerRun(err *error) {
	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(10000)
			if num < 5 {
				panic("Intest panic: final worker is panicked when running")
			} else if num < 10 {
				time.Sleep(1 * time.Millisecond)
			} else if num < 15 {
				w.memTracker.Consume(1000000)
			} else if num < 20 {
				*err = errors.New("Random fail is triggered in final worker")
			}
		}
	})
}
