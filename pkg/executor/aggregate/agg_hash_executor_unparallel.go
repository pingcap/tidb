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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/set"
)

// unparallelExec executes hash aggregation algorithm in single thread.
func (e *HashAggExec) unparallelExec(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		exprCtx := e.Ctx().GetExprCtx()
		if e.prepared.Load() {
			// Since we return e.MaxChunkSize() rows every time, so we should not traverse
			// `groupSet` because of its randomness.
			for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
				partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
				if len(e.PartialAggFuncs) == 0 {
					chk.SetNumVirtualRows(chk.NumRows() + 1)
				}
				for i, af := range e.PartialAggFuncs {
					if err := af.AppendFinalResult2Chunk(exprCtx.GetEvalCtx(), partialResults[i], chk); err != nil {
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
		if e.executed.Load() {
			return nil
		}
		if err := e.execute(ctx); err != nil {
			return err
		}
		if len(e.groupSet.M) == 0 && len(e.GroupByItems) == 0 {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.memTracker.Consume(e.groupSet.Insert(""))
			e.groupKeys = append(e.groupKeys, "")
		}
		e.prepared.Store(true)
	}
}

func (e *HashAggExec) resetSpillMode() {
	e.cursor4GroupKey, e.groupKeys = 0, e.groupKeys[:0]
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = aggfuncs.NewAggPartialResultMapper()
	e.prepared.Store(false)
	e.executed.Store(e.numOfSpilledChks == e.dataInDisk.NumChunks()) // No data is spilling again, all data have been processed.
	e.numOfSpilledChks = e.dataInDisk.NumChunks()
	e.memTracker.ReplaceBytesUsed(setSize)
	atomic.StoreUint32(&e.inSpillMode, 0)
}

// execute fetches Chunks from src and update each aggregate function for each row in Chunk.
func (e *HashAggExec) execute(ctx context.Context) (err error) {
	defer func() {
		if e.tmpChkForSpill.NumRows() > 0 && err == nil {
			err = e.dataInDisk.Add(e.tmpChkForSpill)
			e.tmpChkForSpill.Reset()
		}
	}()
	exprCtx := e.Ctx().GetExprCtx()
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
		for j := range e.childResult.NumRows() {
			groupKey := string(e.groupKeyBuffer[j]) // do memory copy here, because e.groupKeyBuffer may be reused.
			if _, ok := e.groupSet.M[groupKey]; !ok {
				if atomic.LoadUint32(&e.inSpillMode) == 1 && len(e.groupSet.M) > 0 {
					sel = append(sel, j)
					continue
				}
				allMemDelta += e.groupSet.Insert(groupKey)
				e.groupKeys = append(e.groupKeys, groupKey)
			}
			partialResults := e.getPartialResults(groupKey)
			for i, af := range e.PartialAggFuncs {
				tmpBuf[0] = e.childResult.GetRow(j)
				memDelta, err := af.UpdatePartialResult(exprCtx.GetEvalCtx(), tmpBuf[:], partialResults[i])
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
		return e.dataInDisk.Add(e.childResult)
	}
	for i := range e.childResult.NumRows() {
		e.tmpChkForSpill.AppendRow(e.childResult.GetRow(i))
		if e.tmpChkForSpill.IsFull() {
			err = e.dataInDisk.Add(e.tmpChkForSpill)
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
		e.childResult, err = e.dataInDisk.GetChunk(e.offsetOfSpilledChks)
		if err != nil {
			return err
		}
		e.offsetOfSpilledChks++
	}
	return nil
}

func (e *HashAggExec) getPartialResults(groupKey string) []aggfuncs.PartialResult {
	partialResults, ok := e.partialResultMap.M[groupKey]
	allMemDelta := int64(0)
	if !ok {
		partialResults = make([]aggfuncs.PartialResult, 0, len(e.PartialAggFuncs))
		for _, af := range e.PartialAggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResults = append(partialResults, partialResult)
			allMemDelta += memDelta
		}
		deltaBytes := e.partialResultMap.Set(groupKey, partialResults)
		allMemDelta += int64(len(groupKey))
		if deltaBytes > 0 {
			e.memTracker.Consume(deltaBytes)
		}
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

// IsSpillTriggeredForTest is for test.
func (e *HashAggExec) IsSpillTriggeredForTest() bool {
	for i := range e.spillHelper.lock.spilledChunksIO {
		if len(e.spillHelper.lock.spilledChunksIO[i]) > 0 {
			return true
		}
	}
	return false
}

// IsInvalidMemoryUsageTrackingForTest is for test
func (e *HashAggExec) IsInvalidMemoryUsageTrackingForTest() bool {
	return e.invalidMemoryUsageForTrackingTest
}
