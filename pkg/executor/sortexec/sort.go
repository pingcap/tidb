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
	"errors"
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
)

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

	partition *sortPartition

	// We can't spill if size of data is lower than the limit
	spillLimit int64

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// sortPartitions is the chunks to store row values for partitions. Every partition is a sorted list.
	sortPartitions []*sortPartition
	cursors        []*dataCursor

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge
	// spillAction save the Action for spill disk.
	spillAction *sortPartitionSpillDiskAction

	helper spillHelper
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	for _, partition := range e.sortPartitions {
		err := partition.close()
		if err != nil {
			return err
		}
	}
	e.sortPartitions = e.sortPartitions[:0]

	e.memTracker = nil
	e.diskTracker = nil
	e.multiWayMerge = nil
	if e.spillAction != nil {
		e.spillAction.SetFinished()
	}
	e.spillAction = nil
	return e.Children(0).Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.ID(), -1)
		e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
		e.spillLimit = e.Ctx().GetSessionVars().MemTracker.GetBytesLimit() / 10
		e.diskTracker = memory.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
	}
	e.sortPartitions = e.sortPartitions[:0]
	return e.Children(0).Open(ctx)
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step:
//  1. Read as mush as rows into memory.
//  2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//     the memory quota trigger and return to step 1
//  3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
//  4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//     just like in-memory sort before.
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	sortPartitionListLen := len(e.sortPartitions)
	if sortPartitionListLen == 0 {
		return nil
	}

	if sortPartitionListLen == 1 {
		if err := e.nonExternalSorting(req); err != nil {
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
	err := e.initCursors()
	if err != nil {
		return err
	}

	e.multiWayMerge = &multiWayMerge{e.lessRow, e.compressRow, make([]rowWithPartition, 0, len(e.cursors))}
	for i := 0; i < len(e.cursors); i++ {
		// We should always get row here
		row := e.cursors[i].getSpilledRow()
		e.cursors[i].advanceRow()
		e.multiWayMerge.elements = append(e.multiWayMerge.elements, rowWithPartition{row: *row, partitionID: i})
	}
	heap.Init(e.multiWayMerge)
	return nil
}

func (e *SortExec) nonExternalSorting(req *chunk.Chunk) (err error) {
	e.helper.syncLock.Lock()
	defer e.helper.syncLock.Unlock()

	if e.helper.spillError != nil {
		return e.helper.spillError
	}

	if e.sortPartitions[0].isSpillTriggeredNoLock() {
		if len(e.cursors) == 0 {
			e.initCursors()
		}

		// Maybe the spill is triggered by the last chunk, so there is only one partition.
		cursor := e.cursors[0]
		for !req.IsFull() {
			row := cursor.getSpilledRow()
			if row == nil {
				success, err := e.reloadCursor(0)
				if err != nil {
					return err
				}

				if !success {
					// All data have been completely consumed
					return nil
				}

				// row shouldn't be nil here
				row = cursor.getSpilledRow()
			}

			cursor.advanceRow()
			req.AppendRow(*row)
		}
	} else {
		// Spill is not triggered and all data are in memory.
		rowNum := e.sortPartitions[0].numRowInMemory()
		for !req.IsFull() && e.sortPartitions[0].getIdxNoLock() < rowNum {
			e.sortPartitions[0].getSortedRowFromMemoryAndAppendToChunk(req)
			e.sortPartitions[0].advanceIdxNoLock()
		}
	}
	return nil
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
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

		// Get a new row from that partition which the inserted data belongs to
		partitionID := element.partitionID
		newRow := e.cursors[partitionID].getSpilledRow()
		if newRow == nil {
			success, err := e.reloadCursor(partitionID)
			if err != nil {
				return err
			}
			if !success {
				// All data in this partition have been consumed
				heap.Remove(e.multiWayMerge, 0)
				continue
			}
			newRow = e.cursors[partitionID].getSpilledRow()
		}

		e.cursors[partitionID].advanceRow()

		e.multiWayMerge.elements[0].row = *newRow
		heap.Fix(e.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) switchToNewSortPartition(fields []*types.FieldType, byItemsDesc []bool) {
	// Put the full partition into list
	e.sortPartitions = append(e.sortPartitions, e.partition)

	e.partition = newSortPartition(fields, e.MaxChunkSize(), byItemsDesc, e.keyColumns, e.keyCmpFuncs, e.spillLimit)
	e.spillAction = e.partition.actionSpill(&e.helper)
	e.partition.getMemTracker().AttachTo(e.memTracker)
	e.partition.getMemTracker().SetLabel(memory.LabelForRowChunks)
	e.partition.getDiskTracker().AttachTo(e.diskTracker)
	e.partition.getDiskTracker().SetLabel(memory.LabelForRowChunks)
	e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
}

func (e *SortExec) storeChunk(chk *chunk.Chunk, fields []*types.FieldType, byItemsDesc []bool) error {
	e.helper.syncLock.Lock()
	defer e.helper.syncLock.Unlock()

	if e.helper.spillError != nil {
		return e.helper.spillError
	}

	err := e.partition.addNoLock(chk)
	if err != nil {
		if errors.Is(err, errSpillIsTriggered) {
			e.switchToNewSortPartition(fields, byItemsDesc)
			err = e.partition.addNoLock(chk)
		}
		if err != nil {
			return err
		}
	}
	return err
}

func (e *SortExec) handleCurrentPartitionBeforeExit() error {
	e.helper.syncLock.Lock()
	defer e.helper.syncLock.Unlock()

	if e.helper.spillError != nil {
		return e.helper.spillError
	}

	if e.isSpillTriggeredNoLock() {
		// If e.partition haven't trigger the spill. We need to manually trigger it.
		// As all data should be in disk when spill is triggered.
		if !e.partition.isSpillTriggeredNoLock() {
			err := e.partition.spillToDisk()
			if err != nil {
				return err
			}
		}
	} else {
		err := e.partition.sort()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *SortExec) fetchRowChunks(ctx context.Context) error {
	fields := exec.RetTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}

	e.partition = newSortPartition(fields, e.MaxChunkSize(), byItemsDesc, e.keyColumns, e.keyCmpFuncs, e.spillLimit)
	e.spillAction = e.partition.actionSpill(&e.helper)
	e.partition.getMemTracker().AttachTo(e.memTracker)
	e.partition.getMemTracker().SetLabel(memory.LabelForRowChunks)

	if variable.EnableTmpStorageOnOOM.Load() {
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
		e.partition.getDiskTracker().AttachTo(e.diskTracker)
		e.partition.getDiskTracker().SetLabel(memory.LabelForRowChunks)
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
				time.Sleep(100 * time.Microsecond)
			}
		})
	}

	err := e.handleCurrentPartitionBeforeExit()
	if err != nil {
		return err
	}

	failpoint.Inject("waitForSpill", func(val failpoint.Value) {
		if val.(bool) {
			// Ensure that spill is triggered before returning data.
			time.Sleep(5 * time.Millisecond)
		}
	})

	e.sortPartitions = append(e.sortPartitions, e.partition)
	e.partition = nil
	return nil
}

func (e *SortExec) reloadCursor(partitionID int) (bool, error) {
	partition := e.sortPartitions[partitionID]
	cursor := e.cursors[partitionID]

	spilledChkNum := partition.inDisk.NumChunks()
	restoredChkID := cursor.getChkID() + 1
	if restoredChkID >= spilledChkNum {
		// All data has been consumed
		return false, nil
	}

	restoredChk, err := partition.inDisk.GetChunk(restoredChkID)
	if err != nil {
		return false, err
	}

	cursor.setChunk(restoredChk, restoredChkID)
	return true, nil
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) initCursors() error {
	partitionNum := len(e.sortPartitions)
	e.cursors = make([]*dataCursor, partitionNum)
	for i := 0; i < partitionNum; i++ {
		e.cursors[i] = NewDataCursor()
		chk, err := e.sortPartitions[i].inDisk.GetChunk(0)
		if err != nil {
			return err
		}
		e.cursors[i].setChunk(chk, 0)
	}
	return nil
}

func (e *SortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
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

func (e *SortExec) isSpillTriggeredNoLock() bool {
	if len(e.sortPartitions) > 0 {
		return e.sortPartitions[0].isSpillTriggeredNoLock()
	} else {
		if e.partition != nil {
			return e.partition.isSpillTriggeredNoLock()
		}
		return false
	}
}

func (e *SortExec) IsSpillTriggeredForTest() bool {
	return e.isSpillTriggeredNoLock()
}

func (e *SortExec) IsSpillTriggeredInOnePartitionForTest(idx int) bool {
	return e.sortPartitions[idx].isSpillTriggeredNoLock()
}

func (e *SortExec) GetRowNumInOnePartitionForTest(idx int) int64 {
	return e.sortPartitions[idx].numRowForTest()
}

func (e *SortExec) GetSortPartitionListLenForTest() int {
	return len(e.sortPartitions)
}

func (e *SortExec) GetSortMetaForTest() (keyColumns []int, keyCmpFuncs []chunk.CompareFunc, byItemsDesc []bool) {
	keyColumns = e.keyColumns
	keyCmpFuncs = e.keyCmpFuncs
	byItemsDesc = make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	return
}
