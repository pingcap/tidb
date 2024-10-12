// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"bytes"
	"context"
	"math"
	"runtime/trace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var (
	_ exec.Executor = &HashJoinV2Exec{}
	// EnableHashJoinV2 enable hash join v2, used for test
	EnableHashJoinV2 = "set tidb_hash_join_version = " + joinversion.HashJoinVersionOptimized
	// DisableHashJoinV2 disable hash join v2, used for test
	DisableHashJoinV2 = "set tidb_hash_join_version = " + joinversion.HashJoinVersionLegacy
	// HashJoinV2Strings is used for test
	HashJoinV2Strings = []string{DisableHashJoinV2, EnableHashJoinV2}
)

// IsHashJoinV2Supported return true if hash join v2 is supported in current env
func IsHashJoinV2Supported() bool {
	// sizeOfUintptr should always equal to sizeOfUnsafePointer, because according to golang's doc,
	// a Pointer can be converted to an uintptr. Add this check here in case in the future go runtime
	// change this
	return !heapObjectsCanMove() && sizeOfUintptr >= sizeOfUnsafePointer
}

type hashTableContext struct {
	// rowTables is used during split partition stage, each buildWorker has
	// its own rowTable
	rowTables     [][]*rowTable
	hashTable     *hashTableV2
	tagHelper     *tagPtrHelper
	memoryTracker *memory.Tracker
}

func (htc *hashTableContext) reset() {
	htc.rowTables = nil
	htc.hashTable = nil
	htc.tagHelper = nil
	htc.memoryTracker.Detach()
}

func (htc *hashTableContext) build(task *buildTask) {
	htc.hashTable.tables[task.partitionIdx].build(task.segStartIdx, task.segEndIdx, htc.tagHelper)
}

func (htc *hashTableContext) lookup(partitionIndex int, hashValue uint64) taggedPtr {
	return htc.hashTable.tables[partitionIndex].lookup(hashValue, htc.tagHelper)
}

func (htc *hashTableContext) getCurrentRowSegment(workerID, partitionID int, tableMeta *joinTableMeta, allowCreate bool, firstSegSizeHint uint) *rowTableSegment {
	if htc.rowTables[workerID][partitionID] == nil {
		htc.rowTables[workerID][partitionID] = newRowTable(tableMeta)
	}
	segNum := len(htc.rowTables[workerID][partitionID].segments)
	if segNum == 0 || htc.rowTables[workerID][partitionID].segments[segNum-1].finalized {
		if !allowCreate {
			panic("logical error, should not reach here")
		}
		// do not pre-allocate too many memory for the first seg because for query that only has a few rows, it may waste memory and may hurt the performance in high concurrency scenarios
		rowSizeHint := maxRowTableSegmentSize
		if segNum == 0 {
			rowSizeHint = int(firstSegSizeHint)
		}
		seg := newRowTableSegment(uint(rowSizeHint))
		htc.rowTables[workerID][partitionID].segments = append(htc.rowTables[workerID][partitionID].segments, seg)
		segNum++
	}
	return htc.rowTables[workerID][partitionID].segments[segNum-1]
}

func (htc *hashTableContext) finalizeCurrentSeg(workerID, partitionID int, builder *rowTableBuilder) {
	seg := htc.getCurrentRowSegment(workerID, partitionID, nil, false, 0)
	builder.rowNumberInCurrentRowTableSeg[partitionID] = 0
	failpoint.Inject("finalizeCurrentSegPanic", nil)
	seg.initTaggedBits()
	seg.finalized = true
	htc.memoryTracker.Consume(seg.totalUsedBytes())
}

func (htc *hashTableContext) mergeRowTablesToHashTable(tableMeta *joinTableMeta, partitionNumber uint) int {
	rowTables := make([]*rowTable, partitionNumber)
	for i := 0; i < int(partitionNumber); i++ {
		rowTables[i] = newRowTable(tableMeta)
	}
	totalSegmentCnt := 0
	for _, rowTablesPerWorker := range htc.rowTables {
		for partIdx, rt := range rowTablesPerWorker {
			if rt == nil {
				continue
			}
			rowTables[partIdx].merge(rt)
			totalSegmentCnt += len(rt.segments)
		}
	}
	taggedBits := uint8(maxTaggedBits)
	for i := 0; i < int(partitionNumber); i++ {
		for _, seg := range rowTables[i].segments {
			taggedBits = min(taggedBits, seg.taggedBits)
		}
		htc.hashTable.tables[i] = newSubTable(rowTables[i])
	}
	htc.tagHelper = &tagPtrHelper{}
	htc.tagHelper.init(taggedBits)
	htc.rowTables = nil
	return totalSegmentCnt
}

// HashJoinCtxV2 is the hash join ctx used in hash join v2
type HashJoinCtxV2 struct {
	hashJoinCtxBase
	partitionNumber     uint
	partitionMaskOffset int
	ProbeKeyTypes       []*types.FieldType
	BuildKeyTypes       []*types.FieldType
	stats               *hashJoinRuntimeStatsV2

	RightAsBuildSide               bool
	BuildFilter                    expression.CNFExprs
	ProbeFilter                    expression.CNFExprs
	OtherCondition                 expression.CNFExprs
	hashTableContext               *hashTableContext
	hashTableMeta                  *joinTableMeta
	needScanRowTableAfterProbeDone bool

	LUsed, RUsed                                 []int
	LUsedInOtherCondition, RUsedInOtherCondition []int
}

// partitionNumber is always power of 2
func genHashJoinPartitionNumber(partitionHint uint) uint {
	prevRet := uint(16)
	currentRet := uint(8)
	for currentRet != 0 {
		if currentRet < partitionHint {
			return prevRet
		}
		prevRet = currentRet
		currentRet = currentRet >> 1
	}
	return 1
}

func getPartitionMaskOffset(partitionNumber uint) int {
	getMSBPos := func(num uint64) int {
		ret := 0
		for num&1 != 1 {
			num = num >> 1
			ret++
		}
		if num != 1 {
			// partitionNumber is always pow of 2
			panic("should not reach here")
		}
		return ret
	}
	msbPos := getMSBPos(uint64(partitionNumber))
	// top MSB bits in hash value will be used to partition data
	return 64 - msbPos
}

// SetupPartitionInfo set up partitionNumber and partitionMaskOffset based on concurrency
func (hCtx *HashJoinCtxV2) SetupPartitionInfo() {
	hCtx.partitionNumber = genHashJoinPartitionNumber(hCtx.Concurrency)
	hCtx.partitionMaskOffset = getPartitionMaskOffset(hCtx.partitionNumber)
}

// initHashTableContext create hashTableContext for current HashJoinCtxV2
func (hCtx *HashJoinCtxV2) initHashTableContext() {
	hCtx.hashTableContext = &hashTableContext{}
	hCtx.hashTableContext.rowTables = make([][]*rowTable, hCtx.Concurrency)
	for index := range hCtx.hashTableContext.rowTables {
		hCtx.hashTableContext.rowTables[index] = make([]*rowTable, hCtx.partitionNumber)
	}
	hCtx.hashTableContext.hashTable = &hashTableV2{
		tables:          make([]*subTable, hCtx.partitionNumber),
		partitionNumber: uint64(hCtx.partitionNumber),
	}
	hCtx.hashTableContext.memoryTracker = memory.NewTracker(memory.LabelForHashTableInHashJoinV2, -1)
}

// ProbeSideTupleFetcherV2 reads tuples from ProbeSideExec and send them to ProbeWorkers.
type ProbeSideTupleFetcherV2 struct {
	probeSideTupleFetcherBase
	*HashJoinCtxV2
	canSkipProbeIfHashTableIsEmpty bool
}

// ProbeWorkerV2 is the probe worker used in hash join v2
type ProbeWorkerV2 struct {
	probeWorkerBase
	HashJoinCtx *HashJoinCtxV2
	// We build individual joinProbe for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	JoinProbe ProbeV2
}

// BuildWorkerV2 is the build worker used in hash join v2
type BuildWorkerV2 struct {
	buildWorkerBase
	HashJoinCtx    *HashJoinCtxV2
	BuildTypes     []*types.FieldType
	HasNullableKey bool
	WorkerID       uint
}

// NewJoinBuildWorkerV2 create a BuildWorkerV2
func NewJoinBuildWorkerV2(ctx *HashJoinCtxV2, workID uint, buildSideExec exec.Executor, buildKeyColIdx []int, buildTypes []*types.FieldType) *BuildWorkerV2 {
	hasNullableKey := false
	for _, idx := range buildKeyColIdx {
		if !mysql.HasNotNullFlag(buildTypes[idx].GetFlag()) {
			hasNullableKey = true
			break
		}
	}
	worker := &BuildWorkerV2{
		HashJoinCtx:    ctx,
		BuildTypes:     buildTypes,
		WorkerID:       workID,
		HasNullableKey: hasNullableKey,
	}
	worker.BuildSideExec = buildSideExec
	worker.BuildKeyColIdx = buildKeyColIdx
	return worker
}

// HashJoinV2Exec implements the hash join algorithm.
type HashJoinV2Exec struct {
	exec.BaseExecutor
	*HashJoinCtxV2

	ProbeSideTupleFetcher *ProbeSideTupleFetcherV2
	ProbeWorkers          []*ProbeWorkerV2
	BuildWorkers          []*BuildWorkerV2

	workerWg util.WaitGroupWrapper
	waiterWg util.WaitGroupWrapper

	prepared bool
}

// Close implements the Executor Close interface.
func (e *HashJoinV2Exec) Close() error {
	if e.closeCh != nil {
		close(e.closeCh)
	}
	e.finished.Store(true)
	if e.prepared {
		if e.buildFinished != nil {
			channel.Clear(e.buildFinished)
		}
		if e.joinResultCh != nil {
			channel.Clear(e.joinResultCh)
		}
		if e.ProbeSideTupleFetcher.probeChkResourceCh != nil {
			close(e.ProbeSideTupleFetcher.probeChkResourceCh)
			channel.Clear(e.ProbeSideTupleFetcher.probeChkResourceCh)
		}
		for i := range e.ProbeSideTupleFetcher.probeResultChs {
			channel.Clear(e.ProbeSideTupleFetcher.probeResultChs[i])
		}
		for i := range e.ProbeWorkers {
			close(e.ProbeWorkers[i].joinChkResourceCh)
			channel.Clear(e.ProbeWorkers[i].joinChkResourceCh)
			e.ProbeWorkers[i].JoinProbe.ClearProbeState()
		}
		e.ProbeSideTupleFetcher.probeChkResourceCh = nil
		e.waiterWg.Wait()
		e.hashTableContext.reset()
	}
	for _, w := range e.ProbeWorkers {
		w.joinChkResourceCh = nil
	}

	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	err := e.BaseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinV2Exec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		e.closeCh = nil
		e.prepared = false
		return err
	}
	e.prepared = false
	needScanRowTableAfterProbeDone := e.ProbeWorkers[0].JoinProbe.NeedScanRowTable()
	e.HashJoinCtxV2.needScanRowTableAfterProbeDone = needScanRowTableAfterProbeDone
	if e.RightAsBuildSide {
		e.hashTableMeta = newTableMeta(e.BuildWorkers[0].BuildKeyColIdx, e.BuildWorkers[0].BuildTypes,
			e.BuildKeyTypes, e.ProbeKeyTypes, e.RUsedInOtherCondition, e.RUsed, needScanRowTableAfterProbeDone)
	} else {
		e.hashTableMeta = newTableMeta(e.BuildWorkers[0].BuildKeyColIdx, e.BuildWorkers[0].BuildTypes,
			e.BuildKeyTypes, e.ProbeKeyTypes, e.LUsedInOtherCondition, e.LUsed, needScanRowTableAfterProbeDone)
	}
	e.HashJoinCtxV2.ChunkAllocPool = e.AllocPool
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.ID(), -1)
	e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)

	e.workerWg = util.WaitGroupWrapper{}
	e.waiterWg = util.WaitGroupWrapper{}
	e.closeCh = make(chan struct{})
	e.finished.Store(false)

	if e.RuntimeStats() != nil {
		e.stats = &hashJoinRuntimeStatsV2{}
		e.stats.concurrent = int(e.Concurrency)
	}
	return nil
}

func (fetcher *ProbeSideTupleFetcherV2) shouldLimitProbeFetchSize() bool {
	if fetcher.JoinType == logicalop.LeftOuterJoin && fetcher.RightAsBuildSide {
		return true
	}
	if fetcher.JoinType == logicalop.RightOuterJoin && !fetcher.RightAsBuildSide {
		return true
	}
	return false
}

func (w *BuildWorkerV2) splitPartitionAndAppendToRowTable(typeCtx types.Context, srcChkCh chan *chunk.Chunk) (err error) {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			atomic.AddInt64(&w.HashJoinCtx.stats.partitionData, cost)
			setMaxValue(&w.HashJoinCtx.stats.maxPartitionData, cost)
		}
	}()
	partitionNumber := w.HashJoinCtx.partitionNumber
	hashJoinCtx := w.HashJoinCtx

	builder := createRowTableBuilder(w.BuildKeyColIdx, hashJoinCtx.BuildKeyTypes, partitionNumber, w.HasNullableKey, hashJoinCtx.BuildFilter != nil, hashJoinCtx.needScanRowTableAfterProbeDone)

	for chk := range srcChkCh {
		start := time.Now()
		err = builder.processOneChunk(chk, typeCtx, w.HashJoinCtx, int(w.WorkerID))
		failpoint.Inject("splitPartitionPanic", nil)
		cost += int64(time.Since(start))
		if err != nil {
			return err
		}
	}
	start := time.Now()
	builder.appendRemainingRowLocations(int(w.WorkerID), w.HashJoinCtx.hashTableContext)
	cost += int64(time.Since(start))
	return nil
}

func (e *HashJoinV2Exec) canSkipProbeIfHashTableIsEmpty() bool {
	switch e.JoinType {
	case logicalop.InnerJoin:
		return true
	case logicalop.LeftOuterJoin:
		return !e.RightAsBuildSide
	case logicalop.RightOuterJoin:
		return e.RightAsBuildSide
	case logicalop.SemiJoin:
		return e.RightAsBuildSide
	default:
		return false
	}
}

func (e *HashJoinV2Exec) initializeForProbe() {
	e.ProbeSideTupleFetcher.HashJoinCtxV2 = e.HashJoinCtxV2
	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.Concurrency+1)
	e.ProbeSideTupleFetcher.initializeForProbeBase(e.Concurrency, e.joinResultCh)
	e.ProbeSideTupleFetcher.canSkipProbeIfHashTableIsEmpty = e.canSkipProbeIfHashTableIsEmpty()

	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeWorkers[i].initializeForProbe(e.ProbeSideTupleFetcher.probeChkResourceCh, e.ProbeSideTupleFetcher.probeResultChs[i], e)
		e.ProbeWorkers[i].JoinProbe.ResetProbeCollision()
	}
}

func (e *HashJoinV2Exec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	fetchProbeSideChunksFunc := func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.ProbeSideTupleFetcher.fetchProbeSideChunks(
			ctx,
			e.MaxChunkSize(),
			func() bool { return e.ProbeSideTupleFetcher.hashTableContext.hashTable.isHashTableEmpty() },
			e.ProbeSideTupleFetcher.canSkipProbeIfHashTableIsEmpty,
			e.ProbeSideTupleFetcher.needScanRowTableAfterProbeDone,
			e.ProbeSideTupleFetcher.shouldLimitProbeFetchSize(),
			&e.ProbeSideTupleFetcher.hashJoinCtxBase)
	}
	e.workerWg.RunWithRecover(fetchProbeSideChunksFunc, e.ProbeSideTupleFetcher.handleProbeSideFetcherPanic)

	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.workerWg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "HashJoinWorker").End()
			e.ProbeWorkers[workerID].runJoinWorker()
		}, e.ProbeWorkers[workerID].handleProbeWorkerPanic)
	}
	e.waiterWg.RunWithRecover(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (w *ProbeWorkerV2) handleProbeWorkerPanic(r any) {
	if r != nil {
		w.HashJoinCtx.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (e *HashJoinV2Exec) handleJoinWorkerPanic(r any) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (e *HashJoinV2Exec) waitJoinWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	if e.stats != nil {
		for _, prober := range e.ProbeWorkers {
			e.stats.hashStat.probeCollision += int64(prober.JoinProbe.GetProbeCollision())
		}
	}
	if e.ProbeWorkers[0] != nil && e.ProbeWorkers[0].JoinProbe.NeedScanRowTable() {
		for i := uint(0); i < e.Concurrency; i++ {
			var workerID = i
			e.workerWg.RunWithRecover(func() {
				e.ProbeWorkers[workerID].scanRowTableAfterProbeDone()
			}, e.handleJoinWorkerPanic)
		}
		e.workerWg.Wait()
	}
	for _, probeWorker := range e.ProbeWorkers {
		probeWorker.JoinProbe.ClearProbeState()
	}
	close(e.joinResultCh)
}

func (w *ProbeWorkerV2) scanRowTableAfterProbeDone() {
	w.JoinProbe.InitForScanRowTable()
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}
	for !w.JoinProbe.IsScanRowTableDone() {
		joinResult = w.JoinProbe.ScanRowTable(joinResult, &w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller)
		if joinResult.err != nil {
			w.HashJoinCtx.joinResultCh <- joinResult
			return
		}
		if joinResult.chk.IsFull() {
			w.HashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			if !ok {
				return
			}
		}
	}
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	}
}

func (w *ProbeWorkerV2) processOneProbeChunk(probeChunk *chunk.Chunk, joinResult *hashjoinWorkerResult) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	waitTime = 0
	joinResult.err = w.JoinProbe.SetChunkForProbe(probeChunk)
	if joinResult.err != nil {
		return false, waitTime, joinResult
	}
	for !w.JoinProbe.IsCurrentChunkProbeDone() {
		ok, joinResult = w.JoinProbe.Probe(joinResult, &w.HashJoinCtx.SessCtx.GetSessionVars().SQLKiller)
		if !ok || joinResult.err != nil {
			return ok, waitTime, joinResult
		}
		failpoint.Inject("processOneProbeChunkPanic", nil)
		if joinResult.chk.IsFull() {
			waitStart := time.Now()
			w.HashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			waitTime += int64(time.Since(waitStart))
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

func (w *ProbeWorkerV2) runJoinWorker() {
	probeTime := int64(0)
	if w.HashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&w.HashJoinCtx.stats.probe, probeTime)
			atomic.AddInt64(&w.HashJoinCtx.stats.fetchAndProbe, int64(t))
			setMaxValue(&w.HashJoinCtx.stats.maxFetchAndProbe, int64(t))
		}()
	}

	var (
		probeSideResult *chunk.Chunk
	)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: w.probeResultCh,
	}
	for ok := true; ok; {
		if w.HashJoinCtx.finished.Load() {
			break
		}
		select {
		case <-w.HashJoinCtx.closeCh:
			return
		case probeSideResult, ok = <-w.probeResultCh:
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		if !ok {
			break
		}

		start := time.Now()
		waitTime := int64(0)
		ok, waitTime, joinResult = w.processOneProbeChunk(probeSideResult, joinResult)
		probeTime += int64(time.Since(start)) - waitTime
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		w.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
	}
}

func (w *ProbeWorkerV2) getNewJoinResult() (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: w.joinChkResourceCh,
	}
	ok := true
	select {
	case <-w.HashJoinCtx.closeCh:
		ok = false
	case joinResult.chk, ok = <-w.joinChkResourceCh:
	}
	return ok, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinV2Exec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.initHashTableContext()
		e.hashTableContext.memoryTracker.AttachTo(e.memTracker)
		e.buildFinished = make(chan error, 1)
		e.workerWg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
			e.fetchAndBuildHashTable(ctx)
		}, e.handleFetchAndBuildHashTablePanic)
		e.fetchAndProbeHashTable(ctx)
		e.prepared = true
	}
	if e.ProbeSideTupleFetcher.shouldLimitProbeFetchSize() {
		atomic.StoreInt64(&e.ProbeSideTupleFetcher.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinV2Exec) handleFetchAndBuildHashTablePanic(r any) {
	if r != nil {
		e.buildFinished <- util.GetRecoverError(r)
	}
	close(e.buildFinished)
}

// checkBalance checks whether the segment count of each partition is balanced.
func (e *HashJoinV2Exec) checkBalance(totalSegmentCnt int) bool {
	isBalanced := e.Concurrency == e.partitionNumber
	if !isBalanced {
		return false
	}
	avgSegCnt := totalSegmentCnt / int(e.partitionNumber)
	balanceThreshold := int(float64(avgSegCnt) * 0.8)
	subTables := e.HashJoinCtxV2.hashTableContext.hashTable.tables

	for _, subTable := range subTables {
		if math.Abs(float64(len(subTable.rowData.segments)-avgSegCnt)) > float64(balanceThreshold) {
			isBalanced = false
			break
		}
	}
	return isBalanced
}

func (e *HashJoinV2Exec) createTasks(buildTaskCh chan<- *buildTask, totalSegmentCnt int, doneCh chan struct{}) {
	isBalanced := e.checkBalance(totalSegmentCnt)
	segStep := max(1, totalSegmentCnt/int(e.Concurrency))
	subTables := e.HashJoinCtxV2.hashTableContext.hashTable.tables
	createBuildTask := func(partIdx int, segStartIdx int, segEndIdx int) *buildTask {
		return &buildTask{partitionIdx: partIdx, segStartIdx: segStartIdx, segEndIdx: segEndIdx}
	}
	failpoint.Inject("createTasksPanic", nil)

	if isBalanced {
		for partIdx, subTable := range subTables {
			segmentsLen := len(subTable.rowData.segments)
			select {
			case <-doneCh:
				return
			case buildTaskCh <- createBuildTask(partIdx, 0, segmentsLen):
			}
		}
		return
	}

	partitionStartIndex := make([]int, len(subTables))
	partitionSegmentLength := make([]int, len(subTables))
	for i := 0; i < len(subTables); i++ {
		partitionStartIndex[i] = 0
		partitionSegmentLength[i] = len(subTables[i].rowData.segments)
	}

	for {
		hasNewTask := false
		for partIdx := range subTables {
			// create table by round-robin all the partitions so the build thread is likely to build different partition at the same time
			if partitionStartIndex[partIdx] < partitionSegmentLength[partIdx] {
				startIndex := partitionStartIndex[partIdx]
				endIndex := min(startIndex+segStep, partitionSegmentLength[partIdx])
				select {
				case <-doneCh:
					return
				case buildTaskCh <- createBuildTask(partIdx, startIndex, endIndex):
				}
				partitionStartIndex[partIdx] = endIndex
				hasNewTask = true
			}
		}
		if !hasNewTask {
			break
		}
	}
}

func (e *HashJoinV2Exec) fetchAndBuildHashTable(ctx context.Context) {
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashTable = time.Since(start)
		}()
	}

	waitJobDone := func(wg *sync.WaitGroup, errCh chan error) bool {
		wg.Wait()
		close(errCh)
		if err := <-errCh; err != nil {
			e.buildFinished <- err
			return false
		}
		return true
	}

	wg := new(sync.WaitGroup)
	errCh := make(chan error, 1+e.Concurrency)
	// doneCh is used by the consumer(splitAndAppendToRowTable) to info the producer(fetchBuildSideRows) that the consumer meet error and stop consume data
	doneCh := make(chan struct{}, e.Concurrency)
	srcChkCh := e.fetchBuildSideRows(ctx, wg, errCh, doneCh)
	e.splitAndAppendToRowTable(srcChkCh, wg, errCh, doneCh)
	success := waitJobDone(wg, errCh)
	if !success {
		return
	}

	totalSegmentCnt := e.hashTableContext.mergeRowTablesToHashTable(e.hashTableMeta, e.partitionNumber)

	wg = new(sync.WaitGroup)
	errCh = make(chan error, 1+e.Concurrency)
	// doneCh is used by the consumer(buildHashTable) to info the producer(createBuildTasks) that the consumer meet error and stop consume data
	doneCh = make(chan struct{}, e.Concurrency)
	buildTaskCh := e.createBuildTasks(totalSegmentCnt, wg, errCh, doneCh)
	e.buildHashTable(buildTaskCh, wg, errCh, doneCh)
	waitJobDone(wg, errCh)
}

func (e *HashJoinV2Exec) fetchBuildSideRows(ctx context.Context, wg *sync.WaitGroup, errCh chan error, doneCh chan struct{}) chan *chunk.Chunk {
	srcChkCh := make(chan *chunk.Chunk, 1)
	wg.Add(1)
	e.workerWg.RunWithRecover(
		func() {
			defer trace.StartRegion(ctx, "HashJoinBuildSideFetcher").End()
			fetcher := e.BuildWorkers[0]
			fetcher.fetchBuildSideRows(ctx, &fetcher.HashJoinCtx.hashJoinCtxBase, srcChkCh, errCh, doneCh)
		},
		func(r any) {
			if r != nil {
				errCh <- util.GetRecoverError(r)
			}
			wg.Done()
		},
	)
	return srcChkCh
}

func (e *HashJoinV2Exec) splitAndAppendToRowTable(srcChkCh chan *chunk.Chunk, wg *sync.WaitGroup, errCh chan error, doneCh chan struct{}) {
	for i := uint(0); i < e.Concurrency; i++ {
		wg.Add(1)
		workIndex := i
		e.workerWg.RunWithRecover(
			func() {
				err := e.BuildWorkers[workIndex].splitPartitionAndAppendToRowTable(e.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), srcChkCh)
				if err != nil {
					errCh <- err
					doneCh <- struct{}{}
				}
			},
			func(r any) {
				if r != nil {
					errCh <- util.GetRecoverError(r)
					doneCh <- struct{}{}
				}
				wg.Done()
			},
		)
	}
}

func (e *HashJoinV2Exec) createBuildTasks(totalSegmentCnt int, wg *sync.WaitGroup, errCh chan error, doneCh chan struct{}) chan *buildTask {
	buildTaskCh := make(chan *buildTask, e.Concurrency)
	wg.Add(1)
	e.workerWg.RunWithRecover(
		func() { e.createTasks(buildTaskCh, totalSegmentCnt, doneCh) },
		func(r any) {
			if r != nil {
				errCh <- util.GetRecoverError(r)
			}
			close(buildTaskCh)
			wg.Done()
		},
	)
	return buildTaskCh
}

func (e *HashJoinV2Exec) buildHashTable(buildTaskCh chan *buildTask, wg *sync.WaitGroup, errCh chan error, doneCh chan struct{}) {
	for i := uint(0); i < e.Concurrency; i++ {
		wg.Add(1)
		workID := i
		e.workerWg.RunWithRecover(
			func() {
				err := e.BuildWorkers[workID].buildHashTable(buildTaskCh)
				if err != nil {
					errCh <- err
					doneCh <- struct{}{}
				}
			},
			func(r any) {
				if r != nil {
					errCh <- util.GetRecoverError(r)
					doneCh <- struct{}{}
				}
				wg.Done()
			},
		)
	}
}

type buildTask struct {
	partitionIdx int
	segStartIdx  int
	segEndIdx    int
}

// buildHashTableForList builds hash table from `list`.
func (w *BuildWorkerV2) buildHashTable(taskCh chan *buildTask) error {
	cost := int64(0)
	defer func() {
		if w.HashJoinCtx.stats != nil {
			atomic.AddInt64(&w.HashJoinCtx.stats.buildHashTable, cost)
			setMaxValue(&w.HashJoinCtx.stats.maxBuildHashTable, cost)
		}
	}()
	for task := range taskCh {
		start := time.Now()
		w.HashJoinCtx.hashTableContext.build(task)
		failpoint.Inject("buildHashTablePanic", nil)
		cost += int64(time.Since(start))
	}
	return nil
}

type hashJoinRuntimeStatsV2 struct {
	hashJoinRuntimeStats
	partitionData     int64
	maxPartitionData  int64
	buildHashTable    int64
	maxBuildHashTable int64
}

func setMaxValue(addr *int64, currentValue int64) {
	for {
		value := atomic.LoadInt64(addr)
		if currentValue <= value {
			return
		}
		if atomic.CompareAndSwapInt64(addr, value, currentValue) {
			return
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*hashJoinRuntimeStatsV2) Tp() int {
	return execdetails.TpHashJoinRuntimeStats
}

func (e *hashJoinRuntimeStatsV2) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashTable > 0 {
		buf.WriteString("build_hash_table:{total:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(int64(e.fetchAndBuildHashTable) - e.maxBuildHashTable - e.maxPartitionData)))
		buf.WriteString(", build:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.buildHashTable)))
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		buf.WriteString(", total:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe)))
		buf.WriteString(", max:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(atomic.LoadInt64(&e.maxFetchAndProbe))))
		buf.WriteString(", probe:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.probe)))
		buf.WriteString(", fetch and wait:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe - e.probe)))
		if e.hashStat.probeCollision > 0 {
			buf.WriteString(", probe_collision:")
			buf.WriteString(strconv.FormatInt(e.hashStat.probeCollision, 10))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

func (e *hashJoinRuntimeStatsV2) Clone() execdetails.RuntimeStats {
	stats := hashJoinRuntimeStats{
		fetchAndBuildHashTable: e.fetchAndBuildHashTable,
		hashStat:               e.hashStat,
		fetchAndProbe:          e.fetchAndProbe,
		probe:                  e.probe,
		concurrent:             e.concurrent,
		maxFetchAndProbe:       e.maxFetchAndProbe,
	}
	return &hashJoinRuntimeStatsV2{
		hashJoinRuntimeStats: stats,
		partitionData:        e.partitionData,
		maxPartitionData:     e.maxPartitionData,
		buildHashTable:       e.buildHashTable,
		maxBuildHashTable:    e.maxBuildHashTable,
	}
}

func (e *hashJoinRuntimeStatsV2) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*hashJoinRuntimeStatsV2)
	if !ok {
		return
	}
	e.fetchAndBuildHashTable += tmp.fetchAndBuildHashTable
	e.buildHashTable += tmp.buildHashTable
	if e.maxBuildHashTable < tmp.maxBuildHashTable {
		e.maxBuildHashTable = tmp.maxBuildHashTable
	}
	e.partitionData += tmp.partitionData
	if e.maxPartitionData < tmp.maxPartitionData {
		e.maxPartitionData = tmp.maxPartitionData
	}
	e.hashStat.buildTableElapse += tmp.hashStat.buildTableElapse
	e.hashStat.probeCollision += tmp.hashStat.probeCollision
	e.fetchAndProbe += tmp.fetchAndProbe
	e.probe += tmp.probe
	if e.maxFetchAndProbe < tmp.maxFetchAndProbe {
		e.maxFetchAndProbe = tmp.maxFetchAndProbe
	}
}
