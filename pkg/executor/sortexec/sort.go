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

	Unparallel struct {
		Idx int

		// rowChunks is the chunks to store row values.
		rowChunks *chunk.SortedRowContainer

		// PartitionList is the chunks to store row values for partitions. Every partition is a sorted list.
		PartitionList []*chunk.SortedRowContainer

		// spillAction save the Action for spill disk.
		spillAction *chunk.SortAndSpillDiskAction
	}

	Parallel struct {
		result sortedRows
		rowNum int64
		idx    int64

		// Queue should only be closed by `fetchChunksFromChild` function.
		mpmcQueue *chunk.MPMCQueue
		workers   []*parallelSortWorker

		errRWLock sync.RWMutex
		err       error
	}

	// TODO move them into Unparallel
	// sortPartitions is the chunks to store row values for partitions. Every partition is a sorted list.
	sortPartitions []*sortPartition

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge
	// spillAction save the Action for spill disk.
	spillAction *sortPartitionSpillDiskAction

	enableTmpStorageOnOOM bool
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.enableTmpStorageOnOOM = variable.EnableTmpStorageOnOOM.Load()

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
		e.Unparallel.PartitionList = e.Unparallel.PartitionList[:0]
	} else {
		e.Parallel.idx = 0
		e.Parallel.workers = make([]*parallelSortWorker, e.Ctx().GetSessionVars().ExecutorConcurrency)
		e.Parallel.mpmcQueue = chunk.NewMPMCQueue(chunk.DefaultMPMCQueueLimitNum)
		e.Parallel.err = nil
	}

	e.sortPartitions = e.sortPartitions[:0]
	return exec.Open(ctx, e.Children(0))
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	if e.spillAction != nil {
		e.spillAction.SetFinished()
	}

	if e.IsUnparallel {
		for _, partition := range e.sortPartitions {
			partition.close()
		}

		if e.Unparallel.spillAction != nil {
			e.Unparallel.spillAction.SetFinished()
		}
	} else {
		if e.Parallel.err != nil {
			return e.Parallel.err
		}
	}

	if e.memTracker != nil {
		e.memTracker.ReplaceBytesUsed(0)
	}
	return e.Children(0).Close()
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
// TODO add introduction of parallel mode
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
		return e.getChunkInUnparallelMode(req)
	} else {
		e.getChunkInParallelMode(req)
	}
	return nil
}

func (e *SortExec) getChunkInParallelMode(req *chunk.Chunk) {
	for ; !req.IsFull() && e.Parallel.idx < e.Parallel.rowNum; e.Parallel.idx++ {
		req.AppendRow(e.Parallel.result[e.Parallel.idx])
	}
}

func (e *SortExec) getChunkInUnparallelMode(req *chunk.Chunk) error {
	sortPartitionListLen := len(e.sortPartitions)
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
	e.multiWayMerge = &multiWayMerge{e.lessRow, make([]rowWithPartition, 0, len(e.sortPartitions))}
	for i := 0; i < len(e.sortPartitions); i++ {
		// We should always get row here
		row, err := e.sortPartitions[i].getNextSortedRow()
		if err != nil {
			return err
		}

		e.multiWayMerge.elements = append(e.multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(e.multiWayMerge)
	return nil
}

func (e *SortExec) onePartitionSorting(req *chunk.Chunk) (err error) {
	err = e.sortPartitions[0].checkError()
	if err != nil {
		return err
	}

	for !req.IsFull() {
		row, err := e.sortPartitions[0].getNextSortedRow()
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
	err = e.sortPartitions[len(e.sortPartitions)-1].checkError()
	if err != nil {
		return err
	}

	if e.multiWayMerge == nil {
		err := e.initExternalSorting()
		if err != nil {
			return err
		}
	}

	for !req.IsFull() && e.multiWayMerge.Len() > 0 {
		// Get and insert data
		element := e.multiWayMerge.elements[0]
		req.AppendRow(element.row)

		// Get a new row from that partition which inserted data belongs to
		partitionID := element.partitionID
		row, err := e.sortPartitions[partitionID].getNextSortedRow()
		if err != nil {
			return err
		}

		if row.IsEmpty() {
			// All data in this partition have been consumed
			heap.Remove(e.multiWayMerge, 0)
			continue
		}

		e.multiWayMerge.elements[0].row = row
		heap.Fix(e.multiWayMerge, 0)
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
		e.sortPartitions = append(e.sortPartitions, e.curPartition)
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
	e.spillAction = e.curPartition.actionSpill()
	if e.enableTmpStorageOnOOM {
		e.curPartition.getDiskTracker().AttachTo(e.diskTracker)
		e.curPartition.getDiskTracker().SetLabel(memory.LabelForRowChunks)
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
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

func (e *SortExec) checkError() error {
	for _, partition := range e.sortPartitions {
		err := partition.checkError()
		if err != nil {
			return err
		}
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

	e.sortPartitions = append(e.sortPartitions, e.curPartition)
	e.curPartition = nil
	return nil
}

func (e *SortExec) fetchChunksParallel(ctx context.Context) error {
	workerNum := len(e.Parallel.workers)
	publicSpace := publicMergeSpace{}
	waitGroup := sync.WaitGroup{}

	// Add before the start of goroutine to avoid that the counter in waitGroup is minus to negative.
	waitGroup.Add(workerNum + 1)

	// Fetch chunks from child and put chunks into MPMCQueue
	go e.fetchChunksFromChild(ctx, e.Parallel.mpmcQueue, &waitGroup)

	// Create workers
	for i := range e.Parallel.workers {
		e.Parallel.workers[i] = newParallelSortWorker(e.lessRow, &publicSpace, &waitGroup, &e.Parallel.result, e.Parallel.mpmcQueue, e.checkErrorForParallel, e.processErrorForParallel, e.memTracker)
	}

	// Run workers
	for _, worker := range e.Parallel.workers {
		go worker.run()
	}

	// Wait for the finish of all goroutines
	waitGroup.Wait()
	e.getResult(&publicSpace)
	err := e.checkErrorForParallel()
	return err
}

func (e *SortExec) fetchChunksFromChild(ctx context.Context, mpmcQueue *chunk.MPMCQueue, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			processErrorAndLog(e.processErrorForParallel, r)
		}
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
			status := mpmcQueue.GetStatus()
			// Queue may have been canceled at other place.
			if status == chunk.StatusOpen {
				mpmcQueue.Close()
			}
			break
		}

		chkWithMemoryUsage := &chunk.ChunkWithMemoryUsage{
			Chk:         chk,
			MemoryUsage: chk.MemoryUsage() + chunk.RowSize*int64(rowCount),
		}

		// Push chunk into mpmcQueue.
		res := e.Parallel.mpmcQueue.Push(chkWithMemoryUsage)
		if res != chunk.OK {
			return
		}

		e.memTracker.Consume(chkWithMemoryUsage.MemoryUsage)

		err = e.checkErrorForParallel()
		if err != nil {
			return
		}
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
	e.Parallel.err = err
	e.Parallel.mpmcQueue.Cancel()
}

func (e *SortExec) getResult(publicSpace *publicMergeSpace) {
	partitionNum := publicSpace.publicQueue.Len()
	if partitionNum > 1 {
		panic("Sort is not completed.")
	}

	if partitionNum == 0 {
		e.Parallel.rowNum = 0
		return
	}

	sortedData := popFromList(&publicSpace.publicQueue)
	e.Parallel.rowNum = int64(len(sortedData))
	e.Parallel.result = sortedData
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
	return e.sortPartitions[idx].isSpillTriggered()
}

// GetRowNumInOnePartitionDiskForTest returns number of rows a partition holds in disk, it's only used in test.
func (e *SortExec) GetRowNumInOnePartitionDiskForTest(idx int) int64 {
	return e.sortPartitions[idx].numRowInDiskForTest()
}

// GetRowNumInOnePartitionMemoryForTest returns number of rows a partition holds in memory, it's only used in test.
func (e *SortExec) GetRowNumInOnePartitionMemoryForTest(idx int) int64 {
	return e.sortPartitions[idx].numRowInMemoryForTest()
}

// GetSortPartitionListLenForTest returns the number of partitions, it's only used in test.
func (e *SortExec) GetSortPartitionListLenForTest() int {
	return len(e.sortPartitions)
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
