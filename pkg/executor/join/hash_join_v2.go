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
	"github.com/pingcap/log"
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
	"github.com/pingcap/tidb/pkg/util/serialization"
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
	rowTables     [][]*rowTable
	hashTable     *hashTableV2
	memoryTracker *memory.Tracker
}

func (htc *hashTableContext) reset() {
	htc.rowTables = nil
	htc.hashTable = nil
	htc.memoryTracker.Detach()
}

func (htc *hashTableContext) getPartitionMemoryUsage(partID int) int64 {
	totalMemoryUsage := int64(0)
	for _, tables := range htc.rowTables {
		if tables != nil && tables[partID] != nil {
			totalMemoryUsage += tables[partID].getTotalUsedBytesInSegments()
		}
	}

	totalMemoryUsage += htc.hashTable.getPartitionMemoryUsage(partID)

	return totalMemoryUsage
}

func (htc *hashTableContext) getSegmentsInRowTable(workerID, partitionID int) []*rowTableSegment {
	if htc.rowTables[workerID] != nil && htc.rowTables[workerID][partitionID] != nil {
		return htc.rowTables[workerID][partitionID].getSegments()
	}

	return nil
}

func (htc *hashTableContext) getAllSegmentsMemoryUsageInRowTable() int64 {
	totalMemoryUsage := int64(0)
	for _, tables := range htc.rowTables {
		for _, table := range tables {
			totalMemoryUsage += table.getTotalMemoryUsage()
		}
	}
	return totalMemoryUsage
}

func (htc *hashTableContext) clearAllSegmentsInRowTable() {
	for _, tables := range htc.rowTables {
		for _, table := range tables {
			table.clearSegments()
		}
	}
}

func (htc *hashTableContext) clearSegmentsInRowTable(workerID, partitionID int) {
	if htc.rowTables[workerID] != nil && htc.rowTables[workerID][partitionID] != nil {
		htc.rowTables[workerID][partitionID].clearSegments()
	}
}

func (htc *hashTableContext) getAllMemoryUsageInHashTable() int64 {
	partNum := len(htc.hashTable.tables)
	totalMemoryUsage := int64(0)
	for i := 0; i < partNum; i++ {
		totalMemoryUsage += htc.hashTable.getPartitionMemoryUsage(i)
	}
	return totalMemoryUsage
}

func (htc *hashTableContext) clearHashTable() {
	partNum := len(htc.hashTable.tables)
	for i := 0; i < partNum; i++ {
		htc.hashTable.clearPartitionSegments(i)
	}
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

	if len(seg.rowStartOffset) < len(seg.validJoinKeyPos) {
		panic("xzxdebug panic") // TODO remove it
	}

	builder.crrntSizeOfRowTable[partitionID] = 0
	builder.startPosInRawData[partitionID] = builder.startPosInRawData[partitionID][:0]
	failpoint.Inject("finalizeCurrentSegPanic", nil)
	seg.finalized = true
	if needConsume {
		htc.memoryTracker.Consume(seg.totalUsedBytes())
	}
}

func (htc *hashTableContext) calculateHashTableMemoryUsage(rowTables []*rowTable) int64 {
	totalMemoryUsage := int64(0)
	for _, table := range rowTables {
		hashTableLength := max(nextPowerOfTwo(table.validKeyCount()), uint64(1024))
		totalMemoryUsage += int64(hashTableLength) * serialization.UnsafePointerLen
	}
	return totalMemoryUsage
}

func (htc *hashTableContext) mergeRowTablesToHashTable(partitionNumber int, spillHelper *hashJoinSpillHelper) (int, error) {
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

	memoryUsage := htc.calculateHashTableMemoryUsage(rowTables)
	htc.memoryTracker.Consume(memoryUsage)

	if spillHelper != nil && spillHelper.isSpillNeeded() {
		err := spillHelper.spillRowTable()
		if err != nil {
			return 0, err
		}

		spilledPartition := spillHelper.getSpilledPartitions()
		for _, partID := range spilledPartition {
			// Clear spilled row tables
			rowTables[partID].clearSegments()
		}
	}

	for i := 0; i < partitionNumber; i++ {
		// No tracker needs to be passed as memory has been consumed before
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
}

func (hCtx *HashJoinCtxV2) resetHashTableContextForRestore() {
	memoryUsage := hCtx.hashTableContext.getAllSegmentsMemoryUsageInRowTable()
	hCtx.hashTableContext.clearAllSegmentsInRowTable()
	hCtx.memTracker.Consume(-memoryUsage)

	memoryUsage = hCtx.hashTableContext.getAllMemoryUsageInHashTable()
	hCtx.hashTableContext.clearHashTable()
	hCtx.memTracker.Consume(-memoryUsage)
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

	restoredProbeInDisk []*chunk.DataInDiskByChunks
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
	e.spillHelper = newHashJoinSpillHelper(e, e.PartitionNumber, e.ProbeSideTupleFetcher.ProbeSideExec.RetFieldTypes())

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
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				// log.Info(fmt.Sprintf("xzxdebug start probe worker %d", workerID))
				defer trace.StartRegion(ctx, "HashJoinWorker").End()
				e.ProbeWorkers[workerID].runJoinWorker()
			},
			func(r any) {
				// log.Info(fmt.Sprintf("xzxdebug leave probe worker %d...", workerID))
				handleError(e.joinResultCh, &e.finished, r)
				fetcherAndWorkerSyncer.Done()
			},
		)
	}
}

func (e *HashJoinV2Exec) startFinalWorker(syncer chan struct{}) {
	e.waiterWg.RunWithRecover(
		func() {
			// log.Info("xzxdebug start final worker")
			e.finalWorker(syncer)
		},
		func(r any) {
			// log.Info("xzxdebug leave final worker...")
			handleError(e.joinResultCh, &e.finished, r)
		},
	)
}

func (e *HashJoinV2Exec) startProbeWorkersForRestore(wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				err := e.ProbeWorkers[workerID].restoreAndProbe()
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
}

func (e *HashJoinV2Exec) restoreAndProbe(pfAndFWSync chan struct{}, fetcherAndWorkerSyncer *sync.WaitGroup) {
	e.startProbeWorkersForRestore(fetcherAndWorkerSyncer)
	fetcherAndWorkerSyncer.Wait()
	pfAndFWSync <- struct{}{}
}

func (e *HashJoinV2Exec) startProbeFetcher(ctx context.Context) {
	// Fetcher needs to wake up finalWorker after all probe workers finish tasks.
	// This syncer could let fetcher knows if probe workers have finished their tasks.
	fetcherAndWorkerSyncer := &sync.WaitGroup{}

	// synchronize between probe fetcher and final worker
	pfAndFWSync := make(chan struct{})

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

	for {
		// We use buildFinished as the syncer between build task dispatcher and probe fetcher
		<-e.hashJoinCtxBase.buildFinished

		if e.hashJoinCtxBase.finished.Load() {
			return
		}

		e.restoreAndProbe(pfAndFWSync, fetcherAndWorkerSyncer)
	}
}

func (e *HashJoinV2Exec) fetchAndProbeHashTable(ctx context.Context) {
	e.waiterWg.RunWithRecover(
		func() {
			// log.Info("xzxdebug start probe fetcher")
			defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
			e.startProbeFetcher(ctx)
		},
		func(r any) {
			// log.Info("xzxdebug leave probe fetcher")
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
				workerID := i
				e.waiterWg.RunWithRecover(
					func() {
						// log.Info(fmt.Sprintf("xzxdebug start final subworker %d", workerID))
						// Error has been handled in the function
						_ = e.ProbeWorkers[workerID].scanRowTableAfterProbeDone(false)
					},
					func(r any) {
						// log.Info(fmt.Sprintf("xzxdebug leave final subworker %d...", workerID))
						handleError(e.joinResultCh, &e.finished, r)
						wg.Done()
					},
				)
			}
			wg.Wait()
		}

		e.finalSync <- struct{}{}
	}
}

func (e *HashJoinV2Exec) getRestoredChunkNum(restoredPartition *restorePartition) int {
	chunkNum := 0
	for _, inDisk := range restoredPartition.buildSideChunks {
		chunkNum += inDisk.NumChunks()
	}
	return chunkNum
}

func (e *HashJoinV2Exec) controlPrebuildWorkersForRestore(chunkNum int, syncCh chan struct{}, fetcherAndWorkerSyncer *sync.WaitGroup, doneCh <-chan struct{}) {
	// TODO mock the random fail in `fetchBuildSideRowsImpl`
	defer func() {
		defer close(syncCh)

		if r := recover(); r != nil {
			e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
			return
		}

		err := checkSpillAndExecute(fetcherAndWorkerSyncer, e.spillHelper)
		if err != nil {
			handleError(e.joinResultCh, &e.finished, err)
		}
	}()

	for i := 0; i < chunkNum; i++ {
		err := checkSpillAndExecute(fetcherAndWorkerSyncer, e.spillHelper)
		if err != nil {
			handleError(e.joinResultCh, &e.finished, err)
			return
		}

		fetcherAndWorkerSyncer.Add(1)
		select {
		case <-doneCh:
			fetcherAndWorkerSyncer.Done()
			return
		case <-e.hashJoinCtxBase.closeCh:
			fetcherAndWorkerSyncer.Done()
			return
		case syncCh <- struct{}{}:
		}
	}
}

func (e *HashJoinV2Exec) restoreAndBuild(
	restoredPartition *restorePartition,
	fetcherAndWorkerSyncer *sync.WaitGroup,
	preBuildWorkerWg *sync.WaitGroup,
	buildFetcherAndDispatcherSyncChan chan struct{},
) {
	syncCh := make(chan struct{})
	e.startPrebuildWorkersForRestore(restoredPartition, syncCh, fetcherAndWorkerSyncer, preBuildWorkerWg)

	chunkNum := e.getRestoredChunkNum(restoredPartition)

	e.controlPrebuildWorkersForRestore(chunkNum, syncCh, fetcherAndWorkerSyncer, e.buildFetcherFinishCh)

	preBuildWorkerWg.Wait()

	buildFetcherAndDispatcherSyncChan <- struct{}{}
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

	totalSegmentCnt, err := e.hashTableContext.mergeRowTablesToHashTable(e.PartitionNumber, e.spillHelper)
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

	for partIdx, subTable := range subTables {
		segmentsLen := len(subTable.rowData.segments)
		if isBalanced {
			select {
			case <-syncer:
				// syncer is closed by build fetcher in advance,
				// this means that there happen some errors.
				return false, nil
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
				return false, nil
			case buildTaskCh <- createBuildTask(partIdx, startIdx, endIdx):
			}
			continue
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
	}
	return nil
}

func (e *HashJoinV2Exec) startBuildFetcher(ctx context.Context) {
	// log.Info("xzxdebug start build fetcher")
	// defer log.Info("xzxdebug leave build fetcher...")

	// TODO we need to re-handle it
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashTable = time.Since(start)
		}()
	}

	// Build fetcher wakes up build workers with this channel
	buildFetcherAndDispatcherSyncChan := make(chan struct{})

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

	// srcChkCh, fetcherAndWorkerSyncer := e.fetchBuildSideRows(ctx, buildFetcherAndPrebuildWorkerSyncChan, buildFetcherAndBuildWorkerSyncChan, e.finalSync) // TODO remove it
	e.startPrebuildWorkers(srcChkCh, fetcherAndWorkerSyncer, preBuildWorkerWg)

	e.startBuildTaskDispatcher(buildFetcherAndDispatcherSyncChan)

	// Actually we can directly return error by the function `fetchBuildSideRowsImpl`.
	// However, `fetchBuildSideRowsImpl` is also used by hash join v1.
	errCh := make(chan error)
	e.BuildWorkers[0].fetchBuildSideRowsImpl(ctx, &e.hashJoinCtxBase, fetcherAndWorkerSyncer, e.spillHelper, srcChkCh, errCh, e.buildFetcherFinishCh)

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

	lastRound := 0

	for {
		select {
		case <-e.buildFetcherFinishCh: // executor may be closed in advance
		case <-e.finalSync: // Wait for the wake-up from final worker
		}

		if e.finished.Load() {
			return
		}

		err := e.spillHelper.prepareForRestoring(lastRound)
		if err != nil {
			handleError(e.joinResultCh, &e.finished, err)
			return
		}

		restoredPartition := e.spillHelper.stack.pop()
		if restoredPartition == nil {
			// No more data to restore
			e.finished.Store(true)
			return
		}

		log.Info("xzxdebug start a restore round ------------")

		// Collect, so that we can close them in the end.
		// We must collect them once they are popped from stack, or the resource may
		// fail to be recycled because of the possible panic.
		err = e.spillHelper.discardInDisks([][]*chunk.DataInDiskByChunks{restoredPartition.buildSideChunks, restoredPartition.probeSideChunks})
		if err != nil {
			handleError(e.joinResultCh, &e.finished, err)
			return
		}

		lastRound = restoredPartition.round
		e.restoredProbeInDisk = restoredPartition.probeSideChunks

		e.restoreAndBuild(restoredPartition, fetcherAndWorkerSyncer, preBuildWorkerWg, buildFetcherAndDispatcherSyncChan)
	}
}

// Workers in this function receive chunks from fetcher and pre-build hash table
func (e *HashJoinV2Exec) startPrebuildWorkers(srcChkCh chan *chunk.Chunk, fetcherAndWorkerSyncer *sync.WaitGroup, wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				// log.Info(fmt.Sprintf("xzxdebug start prebuild worker %d", workerID))
				err := e.BuildWorkers[workerID].splitPartitionAndAppendToRowTable(e.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), srcChkCh, fetcherAndWorkerSyncer)
				if err != nil {
					handleError(e.joinResultCh, &e.finished, err)
				}
			},
			func(r any) {
				// log.Info(fmt.Sprintf("xzxdebug leave prebuild worker %d...", workerID))
				handleError(e.joinResultCh, &e.finished, r)
				wg.Done()
			},
		)
	}
}

func (e *HashJoinV2Exec) startPrebuildWorkersForRestore(restoredPartition *restorePartition, syncCh chan struct{}, fetcherAndWorkerSyncer *sync.WaitGroup, wg *sync.WaitGroup) {
	wg.Add(int(e.Concurrency))
	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.waiterWg.RunWithRecover(
			func() {
				// log.Info(fmt.Sprintf("xzxdebug start restore prebuild worker %d", workerID))
				err := e.BuildWorkers[workerID].restoreAndPrebuild(restoredPartition.buildSideChunks[workerID], syncCh, fetcherAndWorkerSyncer)
				if err != nil {
					handleError(e.joinResultCh, &e.finished, err)
				}
			},
			func(r any) {
				// log.Info(fmt.Sprintf("xzxdebug leave restore prebuild worker %d...", workerID))
				handleError(e.joinResultCh, &e.finished, r)
				wg.Done()
			},
		)
	}
}

func (e *HashJoinV2Exec) startBuildTaskDispatcher(syncer chan struct{}) {
	e.waiterWg.RunWithRecover(
		func() {
			// log.Info("xzxdebug start dispatcher")
			err := e.dispatchBuildTasks(syncer)
			if err != nil {
				handleError(e.joinResultCh, &e.finished, err)
			}
		},
		func(r any) {
			// log.Info("xzxdebug leave dispatcher...")
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
				// log.Info(fmt.Sprintf("xzxdebug start build worker %d", workerID))
				err := e.BuildWorkers[workerID].buildHashTable(buildTaskCh)
				if err != nil {
					handleError(e.joinResultCh, &e.finished, err)
				}
			},
			func(r any) {
				// log.Info(fmt.Sprintf("xzxdebug leave build worker %d", workerID))
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
