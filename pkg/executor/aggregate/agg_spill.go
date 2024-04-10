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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type spillStatus int32

const (
	noSpill spillStatus = iota
	needSpill
	inSpilling
	spillTriggered
)

const (
	// maxSpillTimes indicates how many times the data can spill at most.
	maxSpillTimes = 10

	spilledPartitionNum = 256

	spillLogInfo string = "memory exceeds quota, set aggregate mode to spill-mode"
)

type parallelHashAggSpillHelper struct {
	lock struct {
		*sync.Mutex
		waitIfInSpilling  *sync.Cond
		nextPartitionIdx  int
		spilledChunksIO   [][]*chunk.DataInDiskByChunks
		status            spillStatus
		memoryConsumption int64
		memoryQuota       int64
	}

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	hasError    atomic.Bool

	// These agg functions are partial agg functions that are same with partial workers'.
	// They only be used for restoring data that are spilled to disk in partial stage.
	aggFuncsForRestoring []aggfuncs.AggFunc

	getNewSpillChunkFunc func() *chunk.Chunk
	spillChunkFieldTypes []*types.FieldType
}

func newSpillHelper(
	tracker *memory.Tracker,
	aggFuncsForRestoring []aggfuncs.AggFunc,
	getNewSpillChunkFunc func() *chunk.Chunk,
	spillChunkFieldTypes []*types.FieldType) *parallelHashAggSpillHelper {
	mu := new(sync.Mutex)
	helper := &parallelHashAggSpillHelper{
		lock: struct {
			*sync.Mutex
			waitIfInSpilling  *sync.Cond
			nextPartitionIdx  int
			spilledChunksIO   [][]*chunk.DataInDiskByChunks
			status            spillStatus
			memoryConsumption int64
			memoryQuota       int64
		}{
			Mutex:             mu,
			waitIfInSpilling:  sync.NewCond(mu),
			spilledChunksIO:   make([][]*chunk.DataInDiskByChunks, spilledPartitionNum),
			status:            noSpill,
			nextPartitionIdx:  spilledPartitionNum - 1,
			memoryConsumption: 0,
			memoryQuota:       0,
		},
		memTracker:           tracker,
		hasError:             atomic.Bool{},
		aggFuncsForRestoring: aggFuncsForRestoring,
		getNewSpillChunkFunc: getNewSpillChunkFunc,
		spillChunkFieldTypes: spillChunkFieldTypes,
	}

	return helper
}

func (p *parallelHashAggSpillHelper) close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, ios := range p.lock.spilledChunksIO {
		for _, io := range ios {
			io.Close()
		}
	}
}

func (p *parallelHashAggSpillHelper) isSpilledChunksIOEmpty() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	for i := range p.lock.spilledChunksIO {
		if len(p.lock.spilledChunksIO[i]) > 0 {
			return false
		}
	}
	return true
}

func (p *parallelHashAggSpillHelper) getNextPartition() (int, bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	partitionIdx := p.lock.nextPartitionIdx
	if partitionIdx < 0 {
		return -1, false
	}

	p.lock.nextPartitionIdx--
	return partitionIdx, true
}

func (p *parallelHashAggSpillHelper) addListInDisks(dataInDisk []*chunk.DataInDiskByChunks) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for i, data := range dataInDisk {
		p.lock.spilledChunksIO[i] = append(p.lock.spilledChunksIO[i], data)
	}
}

func (p *parallelHashAggSpillHelper) getListInDisks(partitionNum int) []*chunk.DataInDiskByChunks {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.lock.spilledChunksIO[partitionNum]
}

func (p *parallelHashAggSpillHelper) setInSpilling() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.lock.status = inSpilling
	logutil.BgLogger().Info(spillLogInfo,
		zap.Int64("consumed", p.lock.memoryConsumption),
		zap.Int64("quota", p.lock.memoryQuota))
}

func (p *parallelHashAggSpillHelper) isNoSpill() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.lock.status == noSpill
}

func (p *parallelHashAggSpillHelper) setSpillTriggered() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.lock.status = spillTriggered
	p.lock.waitIfInSpilling.Broadcast()
}

func (p *parallelHashAggSpillHelper) checkNeedSpill() bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.lock.status == needSpill
}

// Return true if we successfully set flag
func (p *parallelHashAggSpillHelper) setNeedSpill(executorTracker *memory.Tracker, triggeredTracker *memory.Tracker) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if hasEnoughDataToSpill(executorTracker, triggeredTracker) {
		p.lock.status = needSpill
		p.lock.memoryConsumption = triggeredTracker.BytesConsumed()
		p.lock.memoryQuota = triggeredTracker.GetBytesLimit()
		return true
	}
	return false
}

func (p *parallelHashAggSpillHelper) waitForTheEndOfSpill() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for p.lock.status == inSpilling {
		p.lock.waitIfInSpilling.Wait()
	}
}

// We need to check error with atmoic as multi partial workers may access it.
func (p *parallelHashAggSpillHelper) checkError() bool {
	return p.hasError.Load()
}

// We need to set error with atmoic as multi partial workers may access it.
func (p *parallelHashAggSpillHelper) setError() {
	p.hasError.Store(true)
}

func (p *parallelHashAggSpillHelper) restoreOnePartition(ctx sessionctx.Context) (aggfuncs.AggPartialResultMapper, int64, error) {
	restoredData := make(aggfuncs.AggPartialResultMapper)
	bInMap := 0
	restoredMem := int64(0)

	restoredPartitionIdx, isSuccess := p.getNextPartition()
	if !isSuccess {
		return nil, restoredMem, nil
	}

	spilledFilesIO := p.getListInDisks(restoredPartitionIdx)
	for _, spilledFile := range spilledFilesIO {
		memDelta, expandMem, err := p.restoreFromOneSpillFile(ctx, &restoredData, spilledFile, &bInMap)
		if err != nil {
			return nil, restoredMem, err
		}

		p.memTracker.Consume(memDelta)
		restoredMem += memDelta + expandMem
	}
	return restoredData, restoredMem, nil
}

type processRowContext struct {
	ctx                    sessionctx.Context
	chunk                  *chunk.Chunk
	rowPos                 int
	keyColPos              int
	aggFuncNum             int
	restoreadData          *aggfuncs.AggPartialResultMapper
	partialResultsRestored [][]aggfuncs.PartialResult
	bInMap                 *int
}

func (p *parallelHashAggSpillHelper) restoreFromOneSpillFile(ctx sessionctx.Context, restoreadData *aggfuncs.AggPartialResultMapper, diskIO *chunk.DataInDiskByChunks, bInMap *int) (totalMemDelta int64, totalExpandMem int64, err error) {
	chunkNum := diskIO.NumChunks()
	aggFuncNum := len(p.aggFuncsForRestoring)
	processRowContext := &processRowContext{
		ctx:                    ctx,
		chunk:                  nil, // Will be set in the loop
		rowPos:                 0,   // Will be set in the loop
		keyColPos:              aggFuncNum,
		aggFuncNum:             aggFuncNum,
		restoreadData:          restoreadData,
		partialResultsRestored: make([][]aggfuncs.PartialResult, aggFuncNum),
		bInMap:                 bInMap,
	}
	for i := 0; i < chunkNum; i++ {
		chunk, err := diskIO.GetChunk(i)
		if err != nil {
			return totalMemDelta, totalExpandMem, err
		}

		// Deserialize bytes to agg function's meta data
		for aggPos, aggFunc := range p.aggFuncsForRestoring {
			partialResult, memDelta := aggFunc.DeserializePartialResult(chunk)
			processRowContext.partialResultsRestored[aggPos] = partialResult
			totalMemDelta += memDelta
		}

		// Merge or create results
		rowNum := chunk.NumRows()
		processRowContext.chunk = chunk
		for rowPos := 0; rowPos < rowNum; rowPos++ {
			processRowContext.rowPos = rowPos
			memDelta, expandMem, err := p.processRow(processRowContext)
			if err != nil {
				return totalMemDelta, totalExpandMem, err
			}
			totalMemDelta += memDelta
			totalExpandMem += expandMem
		}
	}
	return totalMemDelta, totalExpandMem, nil
}

func (p *parallelHashAggSpillHelper) processRow(context *processRowContext) (totalMemDelta int64, expandMem int64, err error) {
	key := context.chunk.GetRow(context.rowPos).GetString(context.keyColPos)
	prs, ok := (*context.restoreadData)[key]
	if ok {
		exprCtx := context.ctx.GetExprCtx()
		// The key has appeared before, merge results.
		for aggPos := 0; aggPos < context.aggFuncNum; aggPos++ {
			memDelta, err := p.aggFuncsForRestoring[aggPos].MergePartialResult(exprCtx.GetEvalCtx(), context.partialResultsRestored[aggPos][context.rowPos], prs[aggPos])
			if err != nil {
				return totalMemDelta, 0, err
			}
			totalMemDelta += memDelta
		}
	} else {
		totalMemDelta += int64(len(key))

		if len(*context.restoreadData)+1 > (1<<*context.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
			expandMem = hack.DefBucketMemoryUsageForMapStrToSlice * (1 << *context.bInMap)
			p.memTracker.Consume(expandMem)
			(*context.bInMap)++
		}

		results := make([]aggfuncs.PartialResult, context.aggFuncNum)
		(*context.restoreadData)[key] = results

		for aggPos := 0; aggPos < context.aggFuncNum; aggPos++ {
			results[aggPos] = context.partialResultsRestored[aggPos][context.rowPos]
		}
	}
	return totalMemDelta, expandMem, nil
}

// Guarantee that processed data is at least 20% of the threshold, to avoid spilling too frequently.
func hasEnoughDataToSpill(aggTracker *memory.Tracker, passedInTracker *memory.Tracker) bool {
	return aggTracker.BytesConsumed() >= passedInTracker.GetBytesLimit()/5
}

// AggSpillDiskAction implements memory.ActionOnExceed for unparalleled HashAgg.
// If the memory quota of a query is exceeded, AggSpillDiskAction.Action is
// triggered.
type AggSpillDiskAction struct {
	memory.BaseOOMAction
	e          *HashAggExec
	spillTimes uint32
}

// Action set HashAggExec spill mode.
func (a *AggSpillDiskAction) Action(t *memory.Tracker) {
	if atomic.LoadUint32(&a.e.inSpillMode) == 0 && hasEnoughDataToSpill(a.e.memTracker, t) && a.spillTimes < maxSpillTimes {
		a.spillTimes++
		logutil.BgLogger().Info(spillLogInfo,
			zap.Uint32("spillTimes", a.spillTimes),
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()))
		atomic.StoreUint32(&a.e.inSpillMode, 1)
		memory.QueryForceDisk.Add(1)
		return
	}
	if fallback := a.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// GetPriority get the priority of the Action
func (*AggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

// ParallelAggSpillDiskAction implements memory.ActionOnExceed for parallel HashAgg.
type ParallelAggSpillDiskAction struct {
	memory.BaseOOMAction
	e           *HashAggExec
	spillHelper *parallelHashAggSpillHelper
}

// Action set HashAggExec spill mode.
func (p *ParallelAggSpillDiskAction) Action(t *memory.Tracker) {
	if p.actionImpl(t) {
		return
	}

	p.triggerFallBackAction(t)
}

// Return true if we successfully set flag
func (p *ParallelAggSpillDiskAction) actionImpl(t *memory.Tracker) bool {
	p.spillHelper.waitForTheEndOfSpill()
	return p.spillHelper.setNeedSpill(p.e.memTracker, t)
}

func (p *ParallelAggSpillDiskAction) triggerFallBackAction(t *memory.Tracker) {
	if fallback := p.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// GetPriority get the priority of the Action
func (*ParallelAggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}
