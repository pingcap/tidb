// Copyright 2017 PingCAP, Inc.
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

package sortexec

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type sortedRows []chunk.Row

// SortExec represents sorting executor.
type SortExec struct {
	exec.BaseExecutor

	ByItems    []*util.ByItems
	fetched    bool
	ExecSchema *expression.Schema

	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	curPartition *sortPartition

	// We can't spill if size of data is lower than the limit
	spillLimit int64

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	IsUnparallel bool

	finishCh              chan struct{}
	isFinishChannelClosed *atomic.Bool

	Unparallel struct {
		Idx int

		// sortPartitions is the chunks to store row values for partitions. Every partition is a sorted list.
		sortPartitions []*sortPartition

		// multiWayMerge uses multi-way merge for spill disk.
		// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
		multiWayMerge *multiWayMerge

		// spillAction save the Action for spill disk.
		spillAction *sortPartitionSpillDiskAction
	}

	Parallel struct {
		result sortedRows
		rowNum int64
		idx    int64

		chunkChannel    chan *chunkWithMemoryUsage
		isChannelClosed *atomic.Bool
		workers         []*parallelSortWorker

		// All workers' sorted rows will be put into this list to be merged
		globalSortedRowsQueue *sortedRowsList

		errRWLock *sync.RWMutex
		err       error
	}

	enableTmpStorageOnOOM bool
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	if e.Unparallel.spillAction != nil {
		e.Unparallel.spillAction.SetFinished()
	}

	if e.IsUnparallel {
		for _, partition := range e.Unparallel.sortPartitions {
			partition.close()
		}
	} else {
		if e.Parallel.err != nil {
			return e.Parallel.err
		}
		e.tryToCloseChunkChannel()
	}

	if e.memTracker != nil {
		e.memTracker.ReplaceBytesUsed(0)
	}

	e.tryToCloseFinishChannel()
	return e.Children(0).Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.enableTmpStorageOnOOM = variable.EnableTmpStorageOnOOM.Load()
	e.isFinishChannelClosed = &atomic.Bool{}
	e.isFinishChannelClosed.Store(false)
	e.finishCh = make(chan struct{}, 1)

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.ID(), -1)
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
		e.spillLimit = e.Ctx().GetSessionVars().MemTracker.GetBytesLimit() / 10
		e.diskTracker = memory.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	}

	e.IsUnparallel = !e.Ctx().GetSessionVars().EnableParallelSort
	if e.IsUnparallel {
		e.Unparallel.Idx = 0
	} else {
		e.Parallel.idx = 0
		e.Parallel.workers = make([]*parallelSortWorker, e.Ctx().GetSessionVars().ExecutorConcurrency)
		e.Parallel.chunkChannel = make(chan *chunkWithMemoryUsage, e.Ctx().GetSessionVars().ExecutorConcurrency)
		e.Parallel.isChannelClosed = &atomic.Bool{}
		e.Parallel.isChannelClosed.Store(false)
		e.Parallel.globalSortedRowsQueue = &sortedRowsList{}
		e.Parallel.errRWLock = &sync.RWMutex{}
		e.Parallel.err = nil
	}

	e.Unparallel.sortPartitions = e.Unparallel.sortPartitions[:0]
	return exec.Open(ctx, e.Children(0))
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step in unparallel mode:
//  1. Read as mush as rows into memory.
//  2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//     the memory quota trigger and return to step 1
//  3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
//  4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//     just like in-memory sort before.
//
// Here we explain the execution flow of the parallel sort implementation.
// There are 2 main components:
//  1. Chunks Fetcher: Fetcher is responsible for fetching chunks from child and send them to channel.
//  2. Parallel Sort Worker: Worker has two stage.
//     stage 1: Worker receives a chunk from channel, sort it, append sorted rows into a slice
//     and put this slice into a global queue which stores many slices that contains sorted rows.
//     stage 2: Worker fetches two slices from global queue, merge them into one slice, put it into
//     global queue and repeat the above processes until global queue has only one slice.
//
/*
Overview of stage 1:
                        ┌─────────┐
                        │  Child  │
                        └────▲────┘
                             │
                           Fetch
                             │
                     ┌───────┴───────┐
                     │ Chunk Fetcher │
                     └───────┬───────┘
                             │
                           Push
                             │
                             ▼
        ┌────────────────►Channel◄───────────────────┐
        │                    ▲                       │
        │                    │                       │
      Fetch                Fetch                   Fetch
        │                    │                       │
   ┌────┴───┐            ┌───┴────┐              ┌───┴────┐
   │ Worker │            │ Worker │   ......     │ Worker │
   └────┬───┘            └───┬────┘              └───┬────┘
        │                    │                       │
        │                    │                       │
  Sort And Put         Sort And Put            Sort And Put
        │                    │                       │
        │                    │                       │
        │             ┌──────▼────────┐              │
        └────────────►│ Global Queue  │◄─────────────┘
                      └───────────────┘
Overview of stage 2:
         ┌────────┐    ┌────────┐          ┌────────┐
         │ Worker │    │ Worker │  ......  │ Worker │
         └──────┬─┘    └──────┬─┘          └─┬──────┘
           ▲    │        ▲    │              │    ▲
           │    │        │    │              │    │
           │    │        │    │              │    │
           │   Put       │   Put            Put   │
           │    │        │    │              │    │
           │    │       Pop   │              │    │
           │    │        │    ▼              │    │
           │    │   ┌────┴─────────┐         │    │
           │    └──►│              │◄────────┘    │
           │        │ Global Queue │              │
          Pop───────┤              ├─────────────Pop
                    └──────────────┘
*/
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	if e.IsUnparallel {
		return e.appendResultToChunkInUnparallelMode(req)
	}
	e.appendResultToChunkInParallelMode(req)
	return nil
}

func (e *SortExec) appendResultToChunkInParallelMode(req *chunk.Chunk) {
	for ; !req.IsFull() && e.Parallel.idx < e.Parallel.rowNum; e.Parallel.idx++ {
		req.AppendRow(e.Parallel.result[e.Parallel.idx])
	}
}

func (e *SortExec) appendResultToChunkInUnparallelMode(req *chunk.Chunk) error {
	sortPartitionListLen := len(e.Unparallel.sortPartitions)
	if sortPartitionListLen == 0 {
		return nil
	}

	if sortPartitionListLen == 1 {
		if err := e.onePartitionSorting(req); err != nil {
			return err
		}
	} else {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	}
	return nil
}

func (e *SortExec) initExternalSorting() error {
	e.Unparallel.multiWayMerge = &multiWayMerge{e.lessRow, make([]rowWithPartition, 0, len(e.Unparallel.sortPartitions))}
	for i := 0; i < len(e.Unparallel.sortPartitions); i++ {
		// We should always get row here
		row, err := e.Unparallel.sortPartitions[i].getNextSortedRow()
		if err != nil {
			return err
		}

		e.Unparallel.multiWayMerge.elements = append(e.Unparallel.multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(e.Unparallel.multiWayMerge)
	return nil
}

func (e *SortExec) onePartitionSorting(req *chunk.Chunk) (err error) {
	err = e.Unparallel.sortPartitions[0].checkError()
	if err != nil {
		return err
	}

	for !req.IsFull() {
		row, err := e.Unparallel.sortPartitions[0].getNextSortedRow()
		if err != nil {
			return err
		}

		if row.IsEmpty() {
			return nil
		}

		req.AppendRow(row)
	}
	return nil
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	// We only need to check error for the last partition as previous partitions
	// have been checked when we call `switchToNewSortPartition` function.
	err = e.Unparallel.sortPartitions[len(e.Unparallel.sortPartitions)-1].checkError()
	if err != nil {
		return err
	}

	if e.Unparallel.multiWayMerge == nil {
		err := e.initExternalSorting()
		if err != nil {
			return err
		}
	}

	for !req.IsFull() && e.Unparallel.multiWayMerge.Len() > 0 {
		// Get and insert data
		element := e.Unparallel.multiWayMerge.elements[0]
		req.AppendRow(element.row)

		// Get a new row from that partition which inserted data belongs to
		partitionID := element.partitionID
		row, err := e.Unparallel.sortPartitions[partitionID].getNextSortedRow()
		if err != nil {
			return err
		}

		if row.IsEmpty() {
			// All data in this partition have been consumed
			heap.Remove(e.Unparallel.multiWayMerge, 0)
			continue
		}

		e.Unparallel.multiWayMerge.elements[0].row = row
		heap.Fix(e.Unparallel.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) fetchChunks(ctx context.Context) error {
	if e.IsUnparallel {
		return e.fetchChunksUnparallel(ctx)
	}
	return e.fetchChunksParallel(ctx)
}

func (e *SortExec) switchToNewSortPartition(fields []*types.FieldType, byItemsDesc []bool, appendPartition bool) error {
	if appendPartition {
		// Put the full partition into list
		e.Unparallel.sortPartitions = append(e.Unparallel.sortPartitions, e.curPartition)
	}

	if e.curPartition != nil {
		err := e.curPartition.checkError()
		if err != nil {
			return err
		}
	}

	e.curPartition = newSortPartition(fields, byItemsDesc, e.keyColumns, e.keyCmpFuncs, e.spillLimit)
	e.curPartition.getMemTracker().AttachTo(e.memTracker)
	e.curPartition.getMemTracker().SetLabel(memory.LabelForRowChunks)
	e.Unparallel.spillAction = e.curPartition.actionSpill()
	if e.enableTmpStorageOnOOM {
		e.curPartition.getDiskTracker().AttachTo(e.diskTracker)
		e.curPartition.getDiskTracker().SetLabel(memory.LabelForRowChunks)
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.Unparallel.spillAction)
	}
	return nil
}

func (e *SortExec) tryToCloseChunkChannel() {
	if e.Parallel.isChannelClosed.CompareAndSwap(false, true) {
		close(e.Parallel.chunkChannel)
	}
}

func (e *SortExec) tryToCloseFinishChannel() {
	if e.isFinishChannelClosed.CompareAndSwap(false, true) {
		close(e.finishCh)
	}
}

func (e *SortExec) checkError() error {
	for _, partition := range e.Unparallel.sortPartitions {
		err := partition.checkError()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SortExec) storeChunk(chk *chunk.Chunk, fields []*types.FieldType, byItemsDesc []bool) error {
	err := e.curPartition.checkError()
	if err != nil {
		return err
	}

	if !e.curPartition.add(chk) {
		err := e.switchToNewSortPartition(fields, byItemsDesc, true)
		if err != nil {
			return err
		}

		if !e.curPartition.add(chk) {
			return errFailToAddChunk
		}
	}
	return nil
}

func (e *SortExec) handleCurrentPartitionBeforeExit() error {
	err := e.checkError()
	if err != nil {
		return err
	}

	err = e.curPartition.sort()
	if err != nil {
		return err
	}

	return nil
}

func (e *SortExec) fetchChunksUnparallel(ctx context.Context) error {
	fields := exec.RetTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}

	err := e.switchToNewSortPartition(fields, byItemsDesc, false)
	if err != nil {
		return err
	}

	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}

		err = e.storeChunk(chk, fields, byItemsDesc)
		if err != nil {
			return err
		}

		failpoint.Inject("unholdSyncLock", func(val failpoint.Value) {
			if val.(bool) {
				// Ensure that spill can get `syncLock`.
				time.Sleep(1 * time.Millisecond)
			}
		})
	}

	failpoint.Inject("waitForSpill", func(val failpoint.Value) {
		if val.(bool) {
			// Ensure that spill is triggered before returning data.
			time.Sleep(50 * time.Millisecond)
		}
	})

	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			if e.Ctx().GetSessionVars().ConnectionID == 123456 {
				e.Ctx().GetSessionVars().MemTracker.Killer.SendKillSignal(sqlkiller.QueryMemoryExceeded)
			}
		}
	})

	err = e.handleCurrentPartitionBeforeExit()
	if err != nil {
		return err
	}

	e.Unparallel.sortPartitions = append(e.Unparallel.sortPartitions, e.curPartition)
	e.curPartition = nil
	return nil
}

func (e *SortExec) fetchChunksParallel(ctx context.Context) error {
	workerNum := len(e.Parallel.workers)

	// Wait for the finish of all workers
	workersWaiter := sync.WaitGroup{}
	// Wait for the finish of chunk fetcher
	fetcherWaiter := sync.WaitGroup{}

	// Add before the start of goroutine to avoid that the counter is minus to negative.
	workersWaiter.Add(workerNum)
	fetcherWaiter.Add(1)

	// Fetch chunks from child and put chunks into chunkChannel
	go e.fetchChunksFromChild(ctx, &fetcherWaiter)

	for i := range e.Parallel.workers {
		e.Parallel.workers[i] = newParallelSortWorker(i, e.lessRow, e.Parallel.globalSortedRowsQueue, &workersWaiter, &e.Parallel.result, e.Parallel.chunkChannel, e.processErrorForParallel, e.finishCh, e.memTracker)
		go e.Parallel.workers[i].run()
	}

	workersWaiter.Wait()

	// Workers may panic, then no one could consume chunkChannel and
	// the chunk fetcher may hang in this channel.
	e.tryToCloseChunkChannel()

	fetcherWaiter.Wait()

	err := e.checkErrorForParallel()
	if err != nil {
		return err
	}
	e.fetchResultFromQueue()
	return nil
}

// Fetch chunks from child and put chunks into chunkChannel
func (e *SortExec) fetchChunksFromChild(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.processErrorForParallel, r)
		}
		e.tryToCloseChunkChannel()
		waitGroup.Done()
	}()

	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			e.processErrorForParallel(err)
			return
		}

		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}

		chkWithMemoryUsage := &chunkWithMemoryUsage{
			Chk:         chk,
			MemoryUsage: chk.MemoryUsage() + chunk.RowSize*int64(rowCount),
		}

		e.memTracker.Consume(chkWithMemoryUsage.MemoryUsage)

		select {
		case <-e.finishCh:
			return
		case e.Parallel.chunkChannel <- chkWithMemoryUsage:
			// chunkChannel may be closed in advance and it's ok to let it panic
			// as workers have panicked and the query can't keep on running.
		}

		injectParallelSortRandomFail()
	}
}

func (e *SortExec) checkErrorForParallel() error {
	e.Parallel.errRWLock.RLock()
	defer e.Parallel.errRWLock.RUnlock()
	return e.Parallel.err
}

func (e *SortExec) processErrorForParallel(err error) {
	e.Parallel.errRWLock.Lock()
	defer e.Parallel.errRWLock.Unlock()
	if e.Parallel.err == nil {
		e.Parallel.err = err
	}
	e.tryToCloseFinishChannel()
}

func (e *SortExec) fetchResultFromQueue() {
	sortedRowsNum := e.Parallel.globalSortedRowsQueue.getSortedRowsNumNoLock()
	if sortedRowsNum > 1 {
		panic("Sort is not completed.")
	}

	if sortedRowsNum == 0 {
		e.Parallel.rowNum = 0
		return
	}

	sortedRows := e.Parallel.globalSortedRowsQueue.fetchSortedRowsNoLock()
	e.Parallel.rowNum = int64(len(sortedRows))
	e.Parallel.result = sortedRows
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) int {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *SortExec) compressRow(rowI, rowJ chunk.Row) int {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// IsSpillTriggeredInOnePartitionForTest tells if spill is triggered in a specific partition, it's only used in test.
func (e *SortExec) IsSpillTriggeredInOnePartitionForTest(idx int) bool {
	return e.Unparallel.sortPartitions[idx].isSpillTriggered()
}

// GetRowNumInOnePartitionDiskForTest returns number of rows a partition holds in disk, it's only used in test.
func (e *SortExec) GetRowNumInOnePartitionDiskForTest(idx int) int64 {
	return e.Unparallel.sortPartitions[idx].numRowInDiskForTest()
}

// GetRowNumInOnePartitionMemoryForTest returns number of rows a partition holds in memory, it's only used in test.
func (e *SortExec) GetRowNumInOnePartitionMemoryForTest(idx int) int64 {
	return e.Unparallel.sortPartitions[idx].numRowInMemoryForTest()
}

// GetSortPartitionListLenForTest returns the number of partitions, it's only used in test.
func (e *SortExec) GetSortPartitionListLenForTest() int {
	return len(e.Unparallel.sortPartitions)
}

// GetSortMetaForTest returns some sort meta, it's only used in test.
func (e *SortExec) GetSortMetaForTest() (keyColumns []int, keyCmpFuncs []chunk.CompareFunc, byItemsDesc []bool) {
	keyColumns = e.keyColumns
	keyCmpFuncs = e.keyCmpFuncs
	byItemsDesc = make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	return
}
