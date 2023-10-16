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
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
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
	groupSet            set.StringSetWithMemoryUsage
	inputCh             chan *HashAggIntermData
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	groupKeys           [][]byte

	// It means that all partial workers have be finished when we receive data from this chan
	partialAndFinalNotifier chan struct{}

	spillHelper        *parallelHashAggSpillHelper
	isSpilledTriggered bool
	spilledDataChan    chan *HashAggIntermData

	// These agg functions are partial agg functions that are same with partial workers'.
	// They only be used for restoring data that are spilled to disk in partial stage.
	aggFuncsForRestoring []aggfuncs.AggFunc

	restoredMemDelta int64
}

func (w *HashAggFinalWorker) getInputFromDisk() (input *HashAggIntermData, ok bool, spillContinue bool) {
	select {
	case <-w.finishCh:
		return nil, false, false
	case input, ok = <-w.spilledDataChan:
		if !ok {
			return nil, false, false
		}
		return input, ok, true
	}
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

func (w *HashAggFinalWorker) consumeIntermData(sctx sessionctx.Context) (err error, spillContinue bool) {
	var (
		input            *HashAggIntermData
		ok               bool
		intermDataBuffer [][]aggfuncs.PartialResult
		groupKeys        []string
		sc               = sctx.GetSessionVars().StmtCtx
	)

	if w.isSpilledTriggered {
		w.groupSet.Reset(w.memTracker)
	}

	loopJudge := true
	for loopJudge {
		waitStart := time.Now()

		if w.isSpilledTriggered {
			// As we restore only one partition each time when spill is triggered,
			// so we should execute in this loop only once.
			loopJudge = false
			var spillContinue bool
			input, ok, spillContinue = w.getInputFromDisk()
			if !spillContinue {
				return nil, false
			}
		} else {
			input, ok = w.getPartialInput()
		}

		if w.stats != nil {
			w.stats.WaitTime += int64(time.Since(waitStart))
		}
		if !ok {
			return nil, false
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
				partialResult := intermDataBuffer[i]
				for j, af := range w.aggFuncs {
					memDelta, err := af.MergePartialResult(sctx, partialResult[j], finalPartialResults[i][j])
					if err != nil {
						return err, false
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
	return nil, true
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

func (w *HashAggFinalWorker) restoreFromOneSpillFile(ctx sessionctx.Context, restoreadData *HashAggIntermData, diskIO *chunk.ListInDisk) (int64, error) {
	totalMemDelta := int64(0)
	chunkNum := diskIO.NumChunks()
	keyColPos := len(w.aggFuncsForRestoring)
	aggFuncNum := len(w.aggFuncsForRestoring)
	partialResultsRestored := make([][]aggfuncs.PartialResult, aggFuncNum)
	for i := 0; i < chunkNum; i++ {
		chunk, err := diskIO.GetChunk(i)
		if err != nil {
			return totalMemDelta, err
		}

		// Deserialize bytes to agg function's meta data
		for aggPos, aggFunc := range w.aggFuncsForRestoring {
			partialResult, memDelta := aggFunc.DeserializePartialResult(ctx, chunk)
			partialResultsRestored[aggPos] = partialResult
			totalMemDelta += memDelta
		}

		// Merge or create results
		rowNum := chunk.NumRows()
		for rowPos := 0; rowPos < rowNum; rowPos++ {
			key := chunk.GetRow(rowPos).GetString(keyColPos)
			prs, ok := restoreadData.partialResultMap[key]
			if ok {
				// The key has appeared before, merge results.
				for aggPos := 0; aggPos < aggFuncNum; aggPos++ {
					memDelta, err := w.aggFuncsForRestoring[aggPos].MergePartialResult(ctx, partialResultsRestored[aggPos][rowPos], prs[aggPos])
					if err != nil {
						return totalMemDelta, err
					}
					totalMemDelta += memDelta
				}
			} else {
				// This is the first time the key has appeared.
				restoreadData.groupKeys = append(restoreadData.groupKeys, key)

				// Both restoreadData.groupKeys and restoreadData.partialResultMap will contain this key
				totalMemDelta += int64(len(key) * 2)

				results := make([]aggfuncs.PartialResult, aggFuncNum)
				restoreadData.partialResultMap[key] = results
				for aggPos := 0; aggPos < aggFuncNum; aggPos++ {
					results[aggPos] = partialResultsRestored[aggPos][rowPos]
				}
			}
		}
	}
	return totalMemDelta, nil
}

func (w *HashAggFinalWorker) restoreOnePartition(ctx sessionctx.Context) (bool, error) {
	restoredData := HashAggIntermData{
		groupKeys:        make([]string, 0),
		cursor:           0,
		partialResultMap: make(aggfuncs.AggPartialResultMapper, 0),
	}

	restoredPartitionNum := w.spillHelper.getRestoredPartitionNum()
	if restoredPartitionNum == spillTasksDoneFlag {
		return false, nil
	}

	spilledFilesIO := w.spillHelper.getListInDisks(restoredPartitionNum)
	for _, diskIO := range spilledFilesIO {
		memDelta, err := w.restoreFromOneSpillFile(ctx, &restoredData, diskIO)
		if err != nil {
			return false, err
		}

		w.memTracker.Consume(memDelta)
	}
	w.spilledDataChan <- &restoredData
	return true, nil
}

func (w *HashAggFinalWorker) mergeResultsAndSend(ctx sessionctx.Context) (error, bool) {
	err, spillContinue := w.consumeIntermData(ctx)
	if err != nil {
		w.outputCh <- &AfFinalResult{err: err}
		return err, false
	}

	w.loadFinalResult(ctx)
	return nil, spillContinue
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup) {
	w.spilledDataChan = make(chan *HashAggIntermData, 1)
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.outputCh, r)
		}
		close(w.spilledDataChan)
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

	if intest.InTest && enableIntest {
		num := rand.Intn(80)
		if num == 0 {
			panic("Intest panic: final worker is panicked before start")
		} else if num == 1 {
			time.Sleep(2 * time.Second)
		}
	}

	// Wait for the finish of all partial workers
	<-w.partialAndFinalNotifier

	w.isSpilledTriggered = w.spillHelper.isSpillTriggered()
	if w.isSpilledTriggered {
		// Do not put `w.isSpilledTriggered` and `w.spillHelper.checkError()` judgement in one line.
		if w.spillHelper.checkError() {
			return
		}

		for {
			hasData, err := w.restoreOnePartition(ctx)
			if err != nil {
				w.outputCh <- &AfFinalResult{err: err}
				return
			}

			if !hasData {
				return
			}

			if intest.InTest && enableIntest {
				num := rand.Intn(100000)
				if num < 7 {
					panic("Intest panic: final worker is panicked when running")
				} else if num < 14 {
					time.Sleep(1 * time.Second)
				} else if num < 21 {
					// Consume 1TB, and the query should be killed
					w.memTracker.Consume(1099511627776)
				} else if num < 28 {
					w.outputCh <- &AfFinalResult{err: errors.Errorf("Random fail is triggered in final worker")}
					return
				}
			}

			var spillContinue bool
			err, spillContinue = w.mergeResultsAndSend(ctx)
			if err != nil || !spillContinue {
				return
			}
		}
	} else {
		w.mergeResultsAndSend(ctx)
	}
}
