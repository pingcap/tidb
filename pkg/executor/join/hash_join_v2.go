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
	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
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
	// enableHashJoinV2 is a variable used only in test
	enableHashJoinV2 = atomic.Bool{}
)

// IsHashJoinV2Enabled return true if hash join v2 is enabled
func IsHashJoinV2Enabled() bool {
	// sizeOfUintptr should always equal to sizeOfUnsafePointer, because according to golang's doc,
	// a Pointer can be converted to an uintptr. Add this check here in case in the future go runtime
	// change this
	return enableHashJoinV2.Load() && sizeOfUintptr >= sizeOfUnsafePointer
}

// SetEnableHashJoinV2 enable/disable hash join v2
func SetEnableHashJoinV2(enable bool) {
	enableHashJoinV2.Store(enable)
}

type hashTableContext struct {
	// rowTables is used during split partition stage, each buildWorker has
	// its own rowTable
	rowTables         [][]*rowTable
	hashTable         *hashTableV2
	memoryTracker     *memory.Tracker
	spilledPartitions []bool
}

func (htc *hashTableContext) reset() {
	htc.rowTables = nil
	htc.hashTable = nil
	htc.memoryTracker.Detach()
	htc.spilledPartitions = nil
}

func (htc *hashTableContext) getSpilledPartitions() []int {
	spilledPartitions := make([]int, 0)
	for i, spilled := range htc.spilledPartitions {
		if spilled {
			spilledPartitions = append(spilledPartitions, i)
		}
	}
	return spilledPartitions
}

func (htc *hashTableContext) getUnspilledPartitions() []int {
	unspilledPartitions := make([]int, 0)
	for i, spilled := range htc.spilledPartitions {
		if !spilled {
			unspilledPartitions = append(unspilledPartitions, i)
		}
	}
	return unspilledPartitions
}

func (htc *hashTableContext) setPartitionSpilled(partID int) {
	htc.spilledPartitions[partID] = true
}

func (htc *hashTableContext) getPartitionMemoryUsage(partID int) int64 {
	totalMemoryUsage := int64(0)
	for _, tables := range htc.rowTables {
		totalMemoryUsage += tables[partID].getTotalUsedBytesInSegments()
	}
	return totalMemoryUsage
}

func (htc *hashTableContext) getSegments(workerID, partitionID int) []*rowTableSegment {
	if workerID == -1 {
		panic("xzxdebug") // TODO remove it in the future
	}

	return htc.rowTables[workerID][partitionID].getSegments()
}

func (htc *hashTableContext) clearSegments(workerID, partitionID int) {
	if workerID == -1 {
		htc.hashTable.tables[partitionID].rowData.clearSegments()
		htc.hashTable.tables[partitionID].hashTable = nil
		return
	}
	htc.rowTables[workerID][partitionID].clearSegments()
}

func (htc *hashTableContext) getCurrentRowSegment(workerID, partitionID int, allowCreate bool) *rowTableSegment {
	if htc.rowTables[workerID][partitionID] == nil {
		htc.rowTables[workerID][partitionID] = newRowTable()
	}
	segNum := len(htc.rowTables[workerID][partitionID].segments)
	if segNum == 0 || htc.rowTables[workerID][partitionID].segments[segNum-1].finalized {
		if !allowCreate {
			panic("logical error, should not reach here")
		}
		seg := newRowTableSegment()
		htc.rowTables[workerID][partitionID].segments = append(htc.rowTables[workerID][partitionID].segments, seg)
		segNum++
	}
	return htc.rowTables[workerID][partitionID].segments[segNum-1]
}

func (htc *hashTableContext) finalizeCurrentSeg(workerID, partitionID int, builder *rowTableBuilder, needConsume bool) {
	seg := htc.getCurrentRowSegment(workerID, partitionID, false)
	seg.rowStartOffset = append(seg.rowStartOffset, builder.startPosInRawData[partitionID]...)
	builder.crrntSizeOfRowTable[partitionID] = 0
	builder.startPosInRawData[partitionID] = builder.startPosInRawData[partitionID][:0]
	failpoint.Inject("finalizeCurrentSegPanic", nil)
	seg.finalized = true
	if needConsume {
		htc.memoryTracker.Consume(seg.totalUsedBytes())
	}
}

func (htc *hashTableContext) mergeRowTablesToHashTable(partitionNumber int) int {
	rowTables := make([]*rowTable, partitionNumber)
	for i := 0; i < partitionNumber; i++ {
		rowTables[i] = newRowTable()
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
	for i := 0; i < partitionNumber; i++ {
		htc.hashTable.tables[i] = newSubTable(rowTables[i], htc.memoryTracker)
	}
	htc.rowTables = nil
	return totalSegmentCnt
}

// HashJoinCtxV2 is the hash join ctx used in hash join v2
type HashJoinCtxV2 struct {
	hashJoinCtxBase
	PartitionNumber int
	ProbeKeyTypes   []*types.FieldType
	BuildKeyTypes   []*types.FieldType
	stats           *hashJoinRuntimeStatsV2

	RightAsBuildSide               bool
	BuildFilter                    expression.CNFExprs
	ProbeFilter                    expression.CNFExprs
	OtherCondition                 expression.CNFExprs
	hashTableContext               *hashTableContext
	hashTableMeta                  *TableMeta
	needScanRowTableAfterProbeDone bool

	LUsed, RUsed                                 []int
	LUsedInOtherCondition, RUsedInOtherCondition []int

	// final worker wakes up build fetcher by this channel
	finalSync chan struct{}

	maxSpillRound int
	spillHelper   *hashJoinSpillHelper
	spillAction   *hashJoinSpillAction
}

// initHashTableContext create hashTableContext for current HashJoinCtxV2
func (hCtx *HashJoinCtxV2) initHashTableContext() {
	hCtx.hashTableContext = &hashTableContext{}
	hCtx.hashTableContext.rowTables = make([][]*rowTable, hCtx.Concurrency)
	for index := range hCtx.hashTableContext.rowTables {
		hCtx.hashTableContext.rowTables[index] = make([]*rowTable, hCtx.PartitionNumber)
	}
	hCtx.hashTableContext.hashTable = &hashTableV2{
		tables:          make([]*subTable, hCtx.PartitionNumber),
		partitionNumber: uint64(hCtx.PartitionNumber),
	}
	hCtx.finalSync = make(chan struct{})
	hCtx.hashTableContext.memoryTracker = memory.NewTracker(memory.LabelForHashTableInHashJoinV2, -1)
	hCtx.hashTableContext.spilledPartitions = make([]bool, hCtx.Concurrency)
}

// ProbeSideTupleFetcherV2 reads tuples from ProbeSideExec and send them to ProbeWorkers.
type ProbeSideTupleFetcherV2 struct {
	probeSideTupleFetcherBase
	*HashJoinCtxV2
	canSkipProbeIfHashTableIsEmpty bool
}

// HashJoinV2Exec implements the hash join algorithm.
type HashJoinV2Exec struct {
	exec.BaseExecutor
	*HashJoinCtxV2

	ProbeSideTupleFetcher *ProbeSideTupleFetcherV2
	ProbeWorkers          []*ProbeWorkerV2
	BuildWorkers          []*BuildWorkerV2

	waiterWg util.WaitGroupWrapper

	prepared bool
}

// Close implements the Executor Close interface.
func (e *HashJoinV2Exec) Close() error {
	if e.closeCh != nil {
		close(e.closeCh)
	}
	e.finished.Store(true)
	if e.buildFetcherFinishCh != nil {
		close(e.buildFetcherFinishCh)
	}
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
	e.spillHelper.close()
	err := e.BaseExecutor.Close()

	return err
}

func (e *HashJoinV2Exec) needUsedFlag() bool {
	return e.JoinType == plannercore.LeftOuterJoin && !e.RightAsBuildSide
}

// Open implements the Executor Open interface.
func (e *HashJoinV2Exec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		e.closeCh = nil
		e.prepared = false
		return err
	}
	e.prepared = false
	if e.RightAsBuildSide {
		e.hashTableMeta = newTableMeta(e.BuildWorkers[0].BuildKeyColIdx, e.BuildWorkers[0].BuildTypes,
			e.BuildKeyTypes, e.ProbeKeyTypes, e.RUsedInOtherCondition, e.RUsed, e.needUsedFlag())
	} else {
		e.hashTableMeta = newTableMeta(e.BuildWorkers[0].BuildKeyColIdx, e.BuildWorkers[0].BuildTypes,
			e.BuildKeyTypes, e.ProbeKeyTypes, e.LUsedInOtherCondition, e.LUsed, e.needUsedFlag())
	}
	e.HashJoinCtxV2.needScanRowTableAfterProbeDone = e.ProbeWorkers[0].JoinProbe.NeedScanRowTable()
	e.HashJoinCtxV2.ChunkAllocPool = e.AllocPool
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	if e.diskTracker != nil {
		e.diskTracker.Reset()
	} else {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
	}
	e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	e.spillHelper = newHashJoinSpillHelper(e, e.ProbeSideTupleFetcher.ProbeSideExec.RetFieldTypes())

	if variable.EnableTmpStorageOnOOM.Load() && e.PartitionNumber > 1 {
		e.initMaxSpillRound()
		e.spillAction = newHashJoinSpillDiskAction(e.spillHelper)
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
	}

	e.waiterWg = util.WaitGroupWrapper{}
	e.closeCh = make(chan struct{})
	e.buildFetcherFinishCh = make(chan struct{})
	e.finished.Store(false)

	if e.RuntimeStats() != nil {
		e.stats = &hashJoinRuntimeStatsV2{}
		e.stats.concurrent = int(e.Concurrency)
	}
	return nil
}

func (e *HashJoinV2Exec) initMaxSpillRound() {
	e.maxSpillRound = 1
	totalPartitionsNum := e.PartitionNumber
	for {
		if totalPartitionsNum > 1024 {
			break
		}
		totalPartitionsNum *= e.PartitionNumber
		e.maxSpillRound++
	}
	e.maxSpillRound = max(1, e.maxSpillRound-1)
}

func (fetcher *ProbeSideTupleFetcherV2) shouldLimitProbeFetchSize() bool {
	if fetcher.JoinType == plannercore.LeftOuterJoin && fetcher.RightAsBuildSide {
		return true
	}
	if fetcher.JoinType == plannercore.RightOuterJoin && !fetcher.RightAsBuildSide {
		return true
	}
	return false
}

func (e *HashJoinV2Exec) canSkipProbeIfHashTableIsEmpty() bool {
	switch e.JoinType {
	case plannercore.InnerJoin:
		return true
	case plannercore.LeftOuterJoin:
		return !e.RightAsBuildSide
	case plannercore.RightOuterJoin:
		return e.RightAsBuildSide
	case plannercore.SemiJoin:
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
	}
}

func (e *HashJoinV2Exec) startProbeWorkers(ctx context.Context, fetcherAndWorkerSyncer *sync.WaitGroup) {
	fetcherAndWorkerSyncer.Add(int(e.hashJoinCtxBase.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		go func() {
			defer func() {
				trace.StartRegion(ctx, "HashJoinWorker").End()
				if r := recover(); r != nil {
					e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
				}
				fetcherAndWorkerSyncer.Done()
			}()

			e.ProbeWorkers[i].runJoinWorker(fetcherAndWorkerSyncer)
		}()
	}
}

func (e *HashJoinV2Exec) startFinalWorker(syncer chan struct{}) {
	e.waiterWg.RunWithRecover(
		func() {
			e.finalWorker(syncer)
		},
		func(r any) {
			if r != nil {
				e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
			}
		},
	)
}

func (e *HashJoinV2Exec) startProbeFetcher(ctx context.Context) {
	// Fetcher needs to wake up finalWorker after all probe workers finish tasks.
	// This syncer could let fetcher knows if probe workers have finished their tasks.
	fetcherAndWorkerSyncer := &sync.WaitGroup{}

	pfAndFWSync := make(chan struct{})

	e.initializeForProbe()
	e.startProbeWorkers(ctx, fetcherAndWorkerSyncer)
	e.startFinalWorker(pfAndFWSync)

	defer func() {
		if r := recover(); r != nil {
			e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
		}

		close(pfAndFWSync)
	}()

	e.ProbeSideTupleFetcher.fetchProbeSideChunksImpl(
		ctx,
		e.MaxChunkSize(),
		func() bool { return e.ProbeSideTupleFetcher.hashTableContext.hashTable.isHashTableEmpty() },
		e.ProbeSideTupleFetcher.canSkipProbeIfHashTableIsEmpty,
		e.ProbeSideTupleFetcher.needScanRowTableAfterProbeDone,
		e.ProbeSideTupleFetcher.shouldLimitProbeFetchSize(),
		&e.ProbeSideTupleFetcher.hashJoinCtxBase)

	e.ProbeSideTupleFetcher.closeProbeResultChs()
	fetcherAndWorkerSyncer.Wait()

	// Wake up final worker
	pfAndFWSync <- struct{}{}

	for {
		// We use buildFinished as the syncer between build task dispatcher and probe fetcher
		<-e.hashJoinCtxBase.buildFinished

		if e.hashJoinCtxBase.finished.Load() {
			return
		}

		// Wake up final worker
		// pfAndFWSync <- struct{}{} // TODO enable it in the future

		// TODO mock no spill
		return
		// TODO check spill
	}
}

func (e *HashJoinV2Exec) fetchAndProbeHashTable(ctx context.Context) {
	go func() {
		defer func() {
			trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
			if r := recover(); r != nil {
				e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
			}
		}()

		e.startProbeFetcher(ctx)
	}()
}

// finaWorker is responsible for scanning the row table after probe done and wake up the build fetcher
func (e *HashJoinV2Exec) finalWorker(syncer chan struct{}) {
	defer func() {
		close(e.joinResultCh)
		close(e.finalSync)
	}()

	for {
		// Wait for the wake-up from probe fetcher
		<-syncer

		if e.finished.Load() {
			return
		}

		if e.ProbeWorkers[0] != nil && e.ProbeWorkers[0].JoinProbe.NeedScanRowTable() {
			wg := &sync.WaitGroup{}
			wg.Add(int(e.Concurrency))
			for i := uint(0); i < e.Concurrency; i++ {
				go func(workerID uint) {
					defer func() {
						if r := recover(); r != nil {
							e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
						}
						wg.Done()
					}()

					// Error has been handled in the function
					_ = e.ProbeWorkers[workerID].scanRowTableAfterProbeDone(false)
				}(i)
			}
			wg.Wait()
		}

		e.finalSync <- struct{}{}
	}

	// scan row table

	// TODO remove them
	// if e.spillHelper.isSpillTriggered() {
	// 	err := e.restore()
	// 	if err != nil {
	// 		e.joinResultCh <- &hashjoinWorkerResult{err: err}
	// 	}
	// }
}

func (e *HashJoinV2Exec) probeInSpillMode(probeSideChunks *chunk.DataInDiskByChunks, hashTable *hashTableV2) error {
	chunkNum := probeSideChunks.NumChunks()
	// TODO when error happened before channel may have no chunk, do not forget this situation
	ok, joinResult := e.ProbeWorkers[0].getNewJoinResult()
	if !ok {
		return nil
	}

	probeTime := int64(0)
	for i := 0; i < chunkNum; i++ {
		if i%20 == 0 {
			err := checkSQLKiller(&e.HashJoinCtxV2.SessCtx.GetSessionVars().SQLKiller, "probeInSpillMode")
			if err != nil {
				return err
			}
		}

		// TODO reuse probe chunk
		chunk, err := probeSideChunks.GetChunk(i)
		if err != nil {
			return err
		}

		start := time.Now()
		waitTime := int64(0)
		ok, waitTime, joinResult = e.ProbeWorkers[0].processOneRestoredProbeChunk(chunk, hashTable, joinResult)
		probeTime += int64(time.Since(start)) - waitTime
		if !ok {
			break
		}
		// TODO don't forget to reset chunk when it's reusable
	}

	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return nil
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.HashJoinCtxV2.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		e.ProbeWorkers[0].joinChkResourceCh <- joinResult.chk
	}
	return nil
}

// TODO concurrently restore
func (e *HashJoinV2Exec) restore() error {
	err := e.spillHelper.prepareForRestoring()
	if err != nil {
		return err
	}

	// When spill is not triggered the stack will be empty and immediately exit the function
	for {
		restoredPartition := e.spillHelper.stack.pop()
		if restoredPartition == nil {
			break
		}

		// Collect, so that we can close them in the end.
		// We must collect them once they are popped from stack, or the resource may
		// fail to be recycled because panic may lead to the failness of collecting.
		err = e.spillHelper.discardInDisks([]*chunk.DataInDiskByChunks{restoredPartition.buildSideChunks, restoredPartition.probeSideChunks})
		if err != nil {
			return err
		}

		hashTable, spillTriggered, totalBuildMemUsage, err := e.spillHelper.buildHashTable(restoredPartition)
		if err != nil {
			return err
		}

		if spillTriggered {
			// Sometimes spill may be triggered when we build hash table
			continue
		}

		err = e.probeInSpillMode(restoredPartition.probeSideChunks, hashTable)
		if err != nil {
			return err
		}

		if e.ProbeWorkers[0] != nil && e.ProbeWorkers[0].JoinProbe.NeedScanRowTable() {
			e.hashTableContext.hashTable = hashTable
			err := e.ProbeWorkers[0].scanRowTableAfterProbeDone(true)
			if err != nil {
				return err
			}
		}

		// Clear the memory that allocated by restored data
		hashTable = nil
		e.spillHelper.memTracker.Consume(-totalBuildMemUsage)
	}

	return nil
}

func (e *HashJoinV2Exec) fetchAndBuildHashTable(ctx context.Context) {
	go func() {
		defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
		e.startBuildFetcher(ctx)
	}()
}

func (e *HashJoinV2Exec) updateRequiredRows(req *chunk.Chunk) {
	if e.ProbeSideTupleFetcher.shouldLimitProbeFetchSize() {
		atomic.StoreInt64(&e.ProbeSideTupleFetcher.requiredRows, int64(req.RequiredRows()))
	}
}

func (e *HashJoinV2Exec) recycleChunk(req *chunk.Chunk, result *hashjoinWorkerResult) {
	if result != nil && result.chk != nil {
		req.SwapColumns(result.chk)
		result.src <- result.chk
	}
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
		e.fetchAndBuildHashTable(ctx)
		e.fetchAndProbeHashTable(ctx)
		e.prepared = true
	}

	e.updateRequiredRows(req)
	req.Reset()

	result, ok := <-e.joinResultCh
	e.recycleChunk(req, result)

	if !ok {
		return nil
	}

	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	return nil
}

// TODO remove it
func (e *HashJoinV2Exec) handleFetchAndBuildHashTablePanic(r any) {
	if r != nil {
		e.buildFinished <- util.GetRecoverError(r)
	}
	close(e.buildFinished)
}

// checkBalance checks whether the segment count of each partition is balanced.
func (e *HashJoinV2Exec) checkBalance(totalSegmentCnt int) bool {
	isBalanced := e.Concurrency == uint(e.PartitionNumber)
	if !isBalanced {
		return false
	}
	avgSegCnt := totalSegmentCnt / e.PartitionNumber
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

func (e *HashJoinV2Exec) dispatchBuildTasks(syncer chan struct{}) {
	buildTaskCh := make(chan *buildTask, e.Concurrency)

	defer func() {
		close(buildTaskCh)
		close(e.buildFinished)
	}()

	wg := &sync.WaitGroup{}
	e.startBuildWorkers(buildTaskCh, wg)

	for {
		<-syncer
		if e.finished.Load() {
			return
		}

		totalSegmentCnt := e.hashTableContext.mergeRowTablesToHashTable(e.PartitionNumber)
		isBalanced := e.checkBalance(totalSegmentCnt)
		segStep := max(1, totalSegmentCnt/int(e.Concurrency))
		subTables := e.HashJoinCtxV2.hashTableContext.hashTable.tables
		createBuildTask := func(partIdx int, segStartIdx int, segEndIdx int) *buildTask {
			return &buildTask{partitionIdx: partIdx, segStartIdx: segStartIdx, segEndIdx: segEndIdx}
		}
		failpoint.Inject("createTasksPanic", nil)

		for partIdx, subTable := range subTables {
			segmentsLen := len(subTable.rowData.segments)
			if isBalanced {
				select {
				case <-syncer:
					// syncer is closed by build fetcher in advance,
					// this means that there happen some errors.
					return
				case buildTaskCh <- createBuildTask(partIdx, 0, segmentsLen):
				}
				continue
			}
			for startIdx := 0; startIdx < segmentsLen; startIdx += segStep {
				endIdx := min(startIdx+segStep, segmentsLen)
				select {
				case <-syncer:
					// syncer is closed by build fetcher in advance,
					// this means that there happen some errors.
					return
				case buildTaskCh <- createBuildTask(partIdx, startIdx, endIdx):
				}
				continue
			}
		}

		wg.Wait()

		// TODO check spill when build is done

		// TODO set configs for probe fetcher and workers
		// we do not pass error by buildFinished in hash join v2
		e.buildFinished <- nil
	}

}

func (e *HashJoinV2Exec) startBuildFetcher(ctx context.Context) {
	// TODO we need to re-handle it
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashTable = time.Since(start)
		}()
	}

	// Build fetcher wakes up build workers with this channel
	buildFetcherAndBuildWorkerSyncChan := make(chan struct{})

	defer func() {
		e.finished.Store(true)
		close(buildFetcherAndBuildWorkerSyncChan)
	}()

	srcChkCh := make(chan *chunk.Chunk, 1)

	// It's useful when spill is triggered and the fetcher could know when workers finish their works.
	fetcherAndWorkerSyncer := &sync.WaitGroup{}

	preBuildWorkerWg := &sync.WaitGroup{}

	// srcChkCh, fetcherAndWorkerSyncer := e.fetchBuildSideRows(ctx, buildFetcherAndPrebuildWorkerSyncChan, buildFetcherAndBuildWorkerSyncChan, e.finalSync) // TODO remove it
	e.startPrebuildWorkers(srcChkCh, fetcherAndWorkerSyncer, preBuildWorkerWg)

	e.startBuildTaskDispatcher(buildFetcherAndBuildWorkerSyncChan)

	// Actually we can directly return error by the function `fetchBuildSideRowsImpl`.
	// However, `fetchBuildSideRowsImpl` is also used by hash join v1.
	errCh := make(chan error)
	e.BuildWorkers[0].fetchBuildSideRowsImpl(ctx, &e.hashJoinCtxBase, fetcherAndWorkerSyncer, e.spillHelper, srcChkCh, errCh, e.hashJoinCtxBase.buildFetcherFinishCh)
	close(errCh)
	if err := <-errCh; err != nil {
		e.hashJoinCtxBase.joinResultCh <- &hashjoinWorkerResult{err: err}
		return
	}

	if e.hashJoinCtxBase.finished.Load() {
		return
	}

	// Wait for the finish of prebuild workers
	preBuildWorkerWg.Wait()

	// TODO do we need to set some configs for dispatcher when spill is triggered before waking it up?
	// Wake up build task dispatcher
	buildFetcherAndBuildWorkerSyncChan <- struct{}{}

	for {
		select {
		case <-e.hashJoinCtxBase.buildFetcherFinishCh: // executor may be closed in advance
		case <-e.finalSync: // Wait for the wake-up from final worker
		}

		if e.hashJoinCtxBase.finished.Load() {
			return
		}

		// TODO mock no spill
		return
		// TODO check spill, set finished to true if there is no more data to process
	}
}

// Workers in this function receive chunks from fetcher and pre-build hash table
func (e *HashJoinV2Exec) startPrebuildWorkers(srcChkCh chan *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup, wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workIndex := i
		go func() {
			defer func() {
				if r := recover(); r != nil {
					e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
				}
				wg.Done()
			}()

			err := e.BuildWorkers[workIndex].splitPartitionAndAppendToRowTable(e.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), srcChkCh, fetcherAndWorkerSyncer)
			if err != nil {
				e.joinResultCh <- &hashjoinWorkerResult{err: err}
			}
		}()
	}
}

func (e *HashJoinV2Exec) startBuildTaskDispatcher(syncer chan struct{}) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
			}
		}()

		e.dispatchBuildTasks(syncer)
	}()
}

func (e *HashJoinV2Exec) startBuildWorkers(buildTaskCh chan *buildTask, wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		go func(workID uint) {
			defer func() {
				if r := recover(); r != nil {
					e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
				}
				wg.Done()
			}()

			err := e.BuildWorkers[workID].buildHashTable(buildTaskCh)
			if err != nil {
				e.joinResultCh <- &hashjoinWorkerResult{err: err}
			}
		}(i)
	}
}

type buildTask struct {
	partitionIdx int
	segStartIdx  int
	segEndIdx    int
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
		buf.WriteString("build_hash_table:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		buf.WriteString(", total:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(int64(e.fetchAndBuildHashTable) - e.maxBuildHashTable - e.maxPartitionData)))
		buf.WriteString(", partition:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.partitionData)))
		buf.WriteString(", max partition:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxPartitionData)))
		buf.WriteString(", build:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.buildHashTable)))
		buf.WriteString(", max build:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.maxBuildHashTable)))
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
