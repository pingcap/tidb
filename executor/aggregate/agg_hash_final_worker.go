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
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/set"
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
	partialResultMap    AggPartialResultMapper
	groupSet            set.StringSetWithMemoryUsage
	inputCh             chan *HashAggIntermData
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	groupKeys           [][]byte
}

func (w *HashAggFinalWorker) getPartialInput() (input *HashAggIntermData, ok bool) {
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

func (w *HashAggFinalWorker) consumeIntermData(sctx sessionctx.Context) (err error) {
	var (
		input            *HashAggIntermData
		ok               bool
		intermDataBuffer [][]aggfuncs.PartialResult
		groupKeys        []string
		sc               = sctx.GetSessionVars().StmtCtx
	)
	for {
		waitStart := time.Now()
		input, ok = w.getPartialInput()
		if w.stats != nil {
			w.stats.WaitTime += int64(time.Since(waitStart))
		}
		if !ok {
			return nil
		}
		execStart := time.Now()
		if intermDataBuffer == nil {
			intermDataBuffer = make([][]aggfuncs.PartialResult, 0, w.maxChunkSize)
		}
		// Consume input in batches, size of every batch is less than w.maxChunkSize.
		for reachEnd := false; !reachEnd; {
			intermDataBuffer, groupKeys, reachEnd = input.getPartialResultBatch(sc, intermDataBuffer[:0], w.aggFuncs, w.maxChunkSize)
			groupKeysLen := len(groupKeys)
			memSize := getGroupKeyMemUsage(w.groupKeys)
			w.groupKeys = w.groupKeys[:0]
			for i := 0; i < groupKeysLen; i++ {
				w.groupKeys = append(w.groupKeys, []byte(groupKeys[i]))
			}
			failpoint.Inject("ConsumeRandomPanic", nil)
			w.memTracker.Consume(getGroupKeyMemUsage(w.groupKeys) - memSize)
			finalPartialResults := w.getPartialResult(sc, w.groupKeys, w.partialResultMap)
			allMemDelta := int64(0)
			for i, groupKey := range groupKeys {
				if !w.groupSet.Exist(groupKey) {
					allMemDelta += w.groupSet.Insert(groupKey)
				}
				prs := intermDataBuffer[i]
				for j, af := range w.aggFuncs {
					memDelta, err := af.MergePartialResult(sctx, prs[j], finalPartialResults[i][j])
					if err != nil {
						return err
					}
					allMemDelta += memDelta
				}
			}
			w.memTracker.Consume(allMemDelta)
		}
		if w.stats != nil {
			w.stats.ExecTime += int64(time.Since(execStart))
			w.stats.TaskNum++
		}
	}
}

func (w *HashAggFinalWorker) loadFinalResult(sctx sessionctx.Context) {
	waitStart := time.Now()
	result, finished := w.receiveFinalResultHolder()
	if w.stats != nil {
		w.stats.WaitTime += int64(time.Since(waitStart))
	}
	if finished {
		return
	}
	execStart := time.Now()
	memSize := getGroupKeyMemUsage(w.groupKeys)
	w.groupKeys = w.groupKeys[:0]
	for groupKey := range w.groupSet.StringSet {
		w.groupKeys = append(w.groupKeys, []byte(groupKey))
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKeys) - memSize)
	partialResults := w.getPartialResult(sctx.GetSessionVars().StmtCtx, w.groupKeys, w.partialResultMap)
	for i := 0; i < len(w.groupSet.StringSet); i++ {
		for j, af := range w.aggFuncs {
			if err := af.AppendFinalResult2Chunk(sctx, partialResults[i][j], result); err != nil {
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
	if w.stats != nil {
		w.stats.ExecTime += int64(time.Since(execStart))
	}
}

func (w *HashAggFinalWorker) receiveFinalResultHolder() (*chunk.Chunk, bool) {
	select {
	case <-w.finishCh:
		return nil, true
	case result, ok := <-w.finalResultHolderCh:
		return result, !ok
	}
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.outputCh, r)
		}
		if w.stats != nil {
			w.stats.WorkerTime += int64(time.Since(start))
		}
		waitGroup.Done()
	}()
	if err := w.consumeIntermData(ctx); err != nil {
		w.outputCh <- &AfFinalResult{err: err}
	}
	w.loadFinalResult(ctx)
}
