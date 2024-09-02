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
	"math/rand"
	"runtime/trace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
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
	// enableHashJoinV2 is a variable used only in test
	enableHashJoinV2 = atomic.Bool{}
)

// IsHashJoinV2Enabled return true if hash join v2 is enabled
func IsHashJoinV2Enabled() bool {
	// sizeOfUintptr should always equal to sizeOfUnsafePointer, because according to golang's doc,
	// a Pointer can be converted to an uintptr. Add this check here in case in the future go runtime
	// change this
	return !heapObjectsCanMove() && enableHashJoinV2.Load() && sizeOfUintptr >= sizeOfUnsafePointer
}

// SetEnableHashJoinV2 enable/disable hash join v2
func SetEnableHashJoinV2(enable bool) {
	enableHashJoinV2.Store(enable)
}

type hashTableContext struct {
	// rowTables is used during split partition stage, each buildWorker has
	// its own rowTable
	rowTables     [][]*rowTable
	hashTable     *hashTableV2
	memoryTracker *memory.Tracker
}

func (htc *hashTableContext) reset() {
	htc.rowTables = nil
	htc.hashTable = nil
	htc.memoryTracker.Detach()
}

func (htc *hashTableContext) clearAllSegmentsInRowTable() {
	for _, tables := range htc.rowTables {
		for _, table := range tables {
			if table != nil {
				table.clearSegments()
			}
		}
	}
}

func (htc *hashTableContext) getCurrentRowSegment(workerID, partitionID int, allowCreate bool, firstSegSizeHint uint) *rowTableSegment {
	if htc.rowTables[workerID][partitionID] == nil {
		htc.rowTables[workerID][partitionID] = newRowTable()
	}
	segNum := len(htc.rowTables[workerID][partitionID].segments)
	if segNum == 0 || htc.rowTables[workerID][partitionID].segments[segNum-1].finalized {
		if !allowCreate {
			panic("logical error, should not reach here")
		}
		// do not pre-allocate too many memory for the first seg because for query that only has a few rows, it may waste memory and may hurt the performance in high concurrency scenarios
		rowSizeHint := maxRowTableSegmentSize
		if segNum == 0 {
			rowSizeHint = int64(firstSegSizeHint)
		}
		seg := newRowTableSegment(uint(rowSizeHint))
		htc.rowTables[workerID][partitionID].segments = append(htc.rowTables[workerID][partitionID].segments, seg)
		segNum++
	}
	return htc.rowTables[workerID][partitionID].segments[segNum-1]
}

func (htc *hashTableContext) finalizeCurrentSeg(workerID, partitionID int, builder *rowTableBuilder, needConsume bool) {
	seg := htc.getCurrentRowSegment(workerID, partitionID, false, 0)
	builder.rowNumberInCurrentRowTableSeg[partitionID] = 0
	failpoint.Inject("finalizeCurrentSegPanic", nil)
	seg.finalized = true
	if needConsume {
		htc.memoryTracker.Consume(seg.totalUsedBytes())
	}
}

func (htc *hashTableContext) mergeRowTablesToHashTable(partitionNumber int) (int, error) {
	rowTables := make([]*rowTable, partitionNumber)
	for i := 0; i < partitionNumber; i++ {
		rowTables[i] = newRowTable()
	}

	for _, rowTablesPerWorker := range htc.rowTables {
		for partIdx, rt := range rowTablesPerWorker {
			if rt == nil {
				continue
			}
			rowTables[partIdx].merge(rt)
		}
	}

	for i := 0; i < partitionNumber; i++ {
		// No tracker needs to be passed as memory has been consumed in `tryToSpill`
		htc.hashTable.tables[i] = newSubTable(rowTables[i], nil)
	}

	htc.clearAllSegmentsInRowTable()

	totalSegmentCnt := 0
	for _, table := range htc.hashTable.tables {
		totalSegmentCnt += table.getSegmentNum()
	}

	return totalSegmentCnt, nil
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
	hashTableMeta                  *TableMeta
	needScanRowTableAfterProbeDone bool

	LUsed, RUsed                                 []int
	LUsedInOtherCondition, RUsedInOtherCondition []int

	// final worker wakes up build fetcher by this channel
	finalSync chan struct{}
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
	hCtx.finalSync = make(chan struct{}, 1)
	hCtx.hashTableContext.memoryTracker = memory.NewTracker(memory.LabelForHashTableInHashJoinV2, -1)
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

	isMemoryClearedForTest bool
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
		// e.finalSync must be cleared before e.joinResultCh
		// or final worker will not close e.joinResultCh
		// because it is hung by e.finalSync.
		if e.finalSync != nil {
			channel.Clear(e.finalSync)
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
	e.isMemoryClearedForTest = true
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

	if e.diskTracker != nil {
		e.diskTracker.Reset()
	} else {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
	}
	e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)

	e.waiterWg = util.WaitGroupWrapper{}
	e.closeCh = make(chan struct{}, 1)
	e.buildFetcherFinishCh = make(chan struct{}, 1)
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

func (e *HashJoinV2Exec) startProbeWorkers(ctx context.Context, fetcherAndWorkerSyncer *sync.WaitGroup) {
	fetcherAndWorkerSyncer.Add(int(e.hashJoinCtxBase.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				defer trace.StartRegion(ctx, "HashJoinWorker").End()
				e.ProbeWorkers[workerID].runJoinWorker()
			},
			func(r any) {
				handleError(e.joinResultCh, &e.finished, r)
				fetcherAndWorkerSyncer.Done()
			},
		)
	}
}

func (e *HashJoinV2Exec) startFinalWorker(syncer chan struct{}) {
	e.waiterWg.RunWithRecover(
		func() {
			e.finalWorker(syncer)
		},
		func(r any) {
			handleError(e.joinResultCh, &e.finished, r)
		},
	)
}

func (e *HashJoinV2Exec) startProbeFetcher(ctx context.Context) {
	// Fetcher needs to wake up finalWorker after all probe workers finish tasks.
	// This syncer could let fetcher knows if probe workers have finished their tasks.
	fetcherAndWorkerSyncer := &sync.WaitGroup{}

	// synchronize between probe fetcher and final worker
	pfAndFWSync := make(chan struct{}, 1)

	e.startProbeWorkers(ctx, fetcherAndWorkerSyncer)
	e.startFinalWorker(pfAndFWSync)

	defer func() {
		if r := recover(); r != nil {
			handleError(e.joinResultCh, &e.finished, r)
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

	// We use buildFinished as the syncer between build task dispatcher and probe fetcher
	<-e.hashJoinCtxBase.buildFinished
}

func (e *HashJoinV2Exec) fetchAndProbeHashTable(ctx context.Context) {
	e.waiterWg.RunWithRecover(
		func() {
			defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
			e.startProbeFetcher(ctx)
		},
		func(r any) {
			handleError(e.joinResultCh, &e.finished, r)
		},
	)
}

// finaWorker is responsible for scanning the row table after probe done and wake up the build fetcher
func (e *HashJoinV2Exec) finalWorker(syncer chan struct{}) {
	defer func() {
		close(e.joinResultCh)
		close(e.finalSync)
	}()

	// Wait for the wake-up from probe fetcher
	<-syncer

	if e.stats != nil {
		for _, prober := range e.ProbeWorkers {
			e.stats.hashStat.probeCollision += int64(prober.JoinProbe.GetProbeCollision())
		}
	}

	if e.finished.Load() {
		return
	}

	if e.ProbeWorkers[0] != nil && e.ProbeWorkers[0].JoinProbe.NeedScanRowTable() {
		wg := &sync.WaitGroup{}
		wg.Add(int(e.Concurrency))
		for i := uint(0); i < e.Concurrency; i++ {
			workerID := i
			e.waiterWg.RunWithRecover(
				func() {
					// Error has been handled in the function
					err := e.ProbeWorkers[workerID].scanRowTableAfterProbeDone()
					if err != nil {
						handleError(e.joinResultCh, &e.finished, err)
					}
				},
				func(r any) {
					handleError(e.joinResultCh, &e.finished, r)
					wg.Done()
				},
			)
		}
		wg.Wait()
	}

	e.finalSync <- struct{}{}
}

func (e *HashJoinV2Exec) fetchAndBuildHashTable(ctx context.Context) {
	e.waiterWg.RunWithRecover(func() {
		defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
		e.startBuildFetcher(ctx)
	}, nil)
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
		e.initializeForProbe()
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

func (e *HashJoinV2Exec) dispatchBuildTasksImpl(syncer chan struct{}) (bool, error) {
	buildTaskCh := make(chan *buildTask, e.Concurrency)
	wg := &sync.WaitGroup{}
	defer func() {
		close(buildTaskCh)
		wg.Wait()
	}()

	<-syncer
	if e.finished.Load() {
		return false, nil
	}

	e.startBuildWorkers(buildTaskCh, wg)

	totalSegmentCnt, err := e.hashTableContext.mergeRowTablesToHashTable(int(e.partitionNumber))
	if err != nil {
		return false, err
	}

	isBalanced := e.checkBalance(totalSegmentCnt)
	segStep := max(1, totalSegmentCnt/int(e.Concurrency))
	subTables := e.HashJoinCtxV2.hashTableContext.hashTable.tables
	createBuildTask := func(partIdx int, segStartIdx int, segEndIdx int) *buildTask {
		return &buildTask{partitionIdx: partIdx, segStartIdx: segStartIdx, segEndIdx: segEndIdx}
	}
	failpoint.Inject("createTasksPanic", nil)

	if isBalanced {
		for partIdx, subTable := range subTables {
			err = triggerIntest(8)
			if err != nil {
				return false, err
			}
			segmentsLen := len(subTable.rowData.segments)
			select {
			case <-syncer:
				// syncer is closed by build fetcher in advance,
				// this means that there happen some errors.
				return false, nil
			case buildTaskCh <- createBuildTask(partIdx, 0, segmentsLen):
			}
		}
		return true, nil
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
				case <-syncer:
					// syncer is closed by build fetcher in advance,
					// this means that there happen some errors.
					return false, nil
				case buildTaskCh <- createBuildTask(partIdx, startIndex, endIndex):
				}
				partitionStartIndex[partIdx] = endIndex
				hasNewTask = true
			}
			continue
		}
		if !hasNewTask {
			break
		}
	}

	return true, nil
}

func (e *HashJoinV2Exec) dispatchBuildTasks(syncer chan struct{}) error {
	defer close(e.buildFinished)
	ifContinue := true
	var err error

	for ifContinue {
		ifContinue, err = e.dispatchBuildTasksImpl(syncer)
		if err != nil {
			return err
		}

		// Wake up probe fetcher, we do not pass error by buildFinished in hash join v2
		e.buildFinished <- nil

		if e.stats != nil {
			e.stats.fetchAndBuildHashTable = time.Since(e.stats.fetchAndBuildStartTime)
		}
	}
	return nil
}

func (e *HashJoinV2Exec) startBuildFetcher(ctx context.Context) {
	if e.stats != nil {
		e.stats.fetchAndBuildStartTime = time.Now()
	}

	// Build fetcher wakes up build workers with this channel
	buildFetcherAndDispatcherSyncChan := make(chan struct{}, 1)

	// It's useful when spill is triggered and the fetcher could know when workers finish their works.
	fetcherAndWorkerSyncer := &sync.WaitGroup{}

	defer func() {
		// We must ensure that all prebuild workers have exited before
		// set `finished` flag and close buildFetcherAndDispatcherSyncChan
		fetcherAndWorkerSyncer.Wait()
		e.finished.Store(true)
		close(buildFetcherAndDispatcherSyncChan)
	}()

	srcChkCh := make(chan *chunk.Chunk, 1)

	preBuildWorkerWg := &sync.WaitGroup{}

	e.startPrebuildWorkers(srcChkCh, fetcherAndWorkerSyncer, preBuildWorkerWg)
	e.startBuildTaskDispatcher(buildFetcherAndDispatcherSyncChan)

	// Actually we can directly return error by the function `fetchBuildSideRowsImpl`.
	// However, `fetchBuildSideRowsImpl` is also used by hash join v1.
	errCh := make(chan error, 1)
	e.BuildWorkers[0].fetchBuildSideRowsImpl(ctx, &e.hashJoinCtxBase, fetcherAndWorkerSyncer, srcChkCh, errCh, e.buildFetcherFinishCh)

	// Wait for the finish of prebuild workers
	preBuildWorkerWg.Wait()

	close(errCh)
	if err := <-errCh; err != nil {
		handleError(e.joinResultCh, &e.finished, err)
		return
	}

	if e.finished.Load() {
		return
	}

	// Wake up build task dispatcher
	buildFetcherAndDispatcherSyncChan <- struct{}{}

	select {
	case <-e.buildFetcherFinishCh: // executor may be closed in advance
	case <-e.finalSync: // Wait for the wake-up from final worker
	}
}

// Workers in this function receive chunks from fetcher and pre-build hash table
func (e *HashJoinV2Exec) startPrebuildWorkers(srcChkCh chan *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup, wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				e.BuildWorkers[workerID].splitPartitionAndAppendToRowTable(e.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), srcChkCh, fetcherAndWorkerSyncer)
			},
			func(r any) {
				handleError(e.joinResultCh, &e.finished, r)
				wg.Done()
			},
		)
	}
}

func (e *HashJoinV2Exec) startBuildTaskDispatcher(syncer chan struct{}) {
	e.waiterWg.RunWithRecover(
		func() {
			err := e.dispatchBuildTasks(syncer)
			if err != nil {
				handleError(e.joinResultCh, &e.finished, err)
			}
		},
		func(r any) {
			handleError(e.joinResultCh, &e.finished, r)
		},
	)
}

func (e *HashJoinV2Exec) startBuildWorkers(buildTaskCh chan *buildTask, wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				e.BuildWorkers[workerID].buildHashTable(buildTaskCh)
			},
			func(r any) {
				handleError(e.joinResultCh, &e.finished, r)
				wg.Done()
			},
		)
	}
}

func handleError(joinResultCh chan *hashjoinWorkerResult, finished *atomic.Bool, r any) {
	if r != nil {
		joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
		finished.Store(true)
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

func generatePartitionIndex(hashValue uint64, partitionMaskOffset int) uint64 {
	return hashValue >> uint64(partitionMaskOffset)
}

func triggerIntest(errProbability int) error {
	failpoint.Inject("slowWorkers", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(100000)
			if num < 4 {
				time.Sleep(time.Duration(num) * time.Millisecond)
			}
		}
	})

	var err error
	failpoint.Inject("panicOrError", func(val failpoint.Value) {
		if val.(bool) {
			num := rand.Intn(100000)
			if num < errProbability/2 {
				panic("Random panic")
			} else if num < errProbability {
				err = errors.New("Random error is triggered")
			}
		}
	})

	return err
}
