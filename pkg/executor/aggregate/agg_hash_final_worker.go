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
	"github.com/pingcap/tidb/pkg/types"
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

	rowBuffer           []types.Datum
	mutableRow          chunk.MutRow
	partialResultMap    aggfuncs.AggPartialResultMapper
	BInMap              int
	isFirstInput        bool
	inputCh             chan *aggfuncs.AggPartialResultMapper
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	groupKeys           [][]byte

	spillHelper             *parallelHashAggSpillHelper
	isSpilledTriggered      bool
	restoredAggResultMapper *aggfuncs.AggPartialResultMapper

	restoredMemDelta int64
}

func (w *HashAggFinalWorker) getInputFromDisk(sctx sessionctx.Context) (input *aggfuncs.AggPartialResultMapper, ok bool, err error) {
	ret, err := w.spillHelper.restoreOnePartition(sctx)
	if err != nil {
		return nil, false, err
	}

	if ret == nil {
		return nil, false, nil
	}
	return ret, true, nil
}

func (w *HashAggFinalWorker) getPartialInput() (input *aggfuncs.AggPartialResultMapper, ok bool) {
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

func (w *HashAggFinalWorker) getInputData(sctx sessionctx.Context) (*aggfuncs.AggPartialResultMapper, bool, error) {
	waitStart := time.Now()
	defer w.increaseWaitTime(waitStart)

	var input *aggfuncs.AggPartialResultMapper
	var ok bool
	var err error

	if w.isSpilledTriggered {
		input, ok, err = w.getInputFromDisk(sctx)
	} else {
		input, ok = w.getPartialInput()
	}

	w.intestDuringRun(&input, &ok, &err)
	return input, ok, err
}

func (w *HashAggFinalWorker) handleFirstInput(input *aggfuncs.AggPartialResultMapper) {
	w.isFirstInput = false
	w.partialResultMap = *input
	w.initBInMap()
}

func (w *HashAggFinalWorker) increaseWaitTime(waitStart time.Time) {
	if w.stats != nil {
		w.stats.WaitTime += int64(time.Since(waitStart))
	}
}

func (w *HashAggFinalWorker) increaseExecTime(execStart time.Time) {
	if w.stats != nil {
		w.stats.ExecTime += int64(time.Since(execStart))
		w.stats.TaskNum++
	}
}

func (w *HashAggFinalWorker) mergeInputIntoResultMap(sctx sessionctx.Context, input *aggfuncs.AggPartialResultMapper) error {
	execStart := time.Now()
	allMemDelta := int64(0)
	for key, value := range *input {
		dstVal, ok := w.partialResultMap[key]
		if !ok {
			w.handleNewGroupKey(key, value)
			continue
		}

		for j, af := range w.aggFuncs {
			memDelta, err := af.MergePartialResult(sctx, value[j], dstVal[j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
	}
	w.memTracker.Consume(allMemDelta)
	w.increaseExecTime(execStart)
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
	defer func() { w.isFirstInput = true }()

	for {
		input, ok, err := w.getInputData(sctx)
		if !ok || err != nil {
			return err
		}

		// As the w.partialResultMap is empty when we get the first input.
		// So it's better to directly assign the input to w.partialResultMap
		if w.isFirstInput {
			w.handleFirstInput(input)
			continue
		}

		failpoint.Inject("ConsumeRandomPanic", nil)

		if err := w.mergeInputIntoResultMap(sctx, input); err != nil {
			return err
		}
	}
}

func (w *HashAggFinalWorker) loadFinalResult(sctx sessionctx.Context) {
	waitStart := time.Now()
	result, finished := w.receiveFinalResultHolder()
	w.increaseWaitTime(waitStart)
	if finished {
		return
	}

	failpoint.Inject("ConsumeRandomPanic", nil)

	execStart := time.Now()
	for _, results := range w.partialResultMap {
		for j, af := range w.aggFuncs {
			if err := af.AppendFinalResult2Chunk(sctx, results[j], result); err != nil {
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

	w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
	w.increaseExecTime(execStart)
}

func (w *HashAggFinalWorker) receiveFinalResultHolder() (*chunk.Chunk, bool) {
	select {
	case <-w.finishCh:
		return nil, true
	case result, ok := <-w.finalResultHolderCh:
		return result, !ok
	}
}

func (w *HashAggFinalWorker) mergeResultsAndSend(ctx sessionctx.Context) error {
	err := w.consumeIntermData(ctx)
	if err != nil {
		w.outputCh <- &AfFinalResult{err: err}
		return err
	}

	w.loadFinalResult(ctx)
	return nil
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup) {
	start := time.Now()
	defer w.cleanup(start, waitGroup)

	failpoint.Inject("enableAggSpillIntest", func(val failpoint.Value) {
		if val.(bool) {
			intestBeforeStart()
		}
	})

	w.isSpilledTriggered = w.spillHelper.isSpillTriggered()
	if w.isSpilledTriggered {
		if w.spillHelper.checkError() {
			return
		}
	}
	err := w.mergeResultsAndSend(ctx)
	if err != nil {
		return
	}
}

func (w *HashAggFinalWorker) cleanup(start time.Time, waitGroup *sync.WaitGroup) {
	if r := recover(); r != nil {
		recoveryHashAgg(w.outputCh, r)
	}
	if w.stats != nil {
		w.stats.WorkerTime += int64(time.Since(start))
	}
	waitGroup.Done()
}

func intestBeforeStart() {
	num := rand.Intn(50)
	if num == 0 {
		panic("Intest panic: final worker is panicked before start")
	} else if num == 1 {
		time.Sleep(1 * time.Millisecond)
	}
}

func (w *HashAggFinalWorker) intestDuringRun(input **aggfuncs.AggPartialResultMapper, ok *bool, err *error) {
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
				*ok = false
				*input = nil
				*err = errors.New("Random fail is triggered in final worker")
			}
		}
	})
}
