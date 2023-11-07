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

package sort_exec

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// ErrCannotAddBecauseSorted indicate that the SortPartition is sorted and prohibit inserting data.
var ErrCannotAddBecauseSorted = errors.New("can not add because sorted")

const RowPtrSize = int(unsafe.Sizeof(chunk.RowPtr{}))

type SortPartition struct {
	inMemory *chunk.List

	// TODO replace DataInDiskByRows with DataInDiskByChunks
	inDisk *chunk.DataInDiskByRows

	// spillError stores the error when spilling.
	spillError error

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	actionSpill *chunk.SpillDiskAction

	ptrM struct {
		sync.RWMutex
		// rowPtrs store the chunk index and row index for each row.
		// rowPtrs != nil indicates the pointer is initialized and sorted.
		// It will get an ErrCannotAddBecauseSorted when trying to insert data if rowPtrs != nil.
		rowPtrs []chunk.RowPtr
	}

	ByItemsDesc []bool
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	// Sort is a time-consuming operation, we need to set a checkpoint to detect
	// the outside signal periodically.
	timesOfRowCompare uint
}

// NewSortPartition creates a new SortPartition in memory.
func NewSortPartition(fieldType []*types.FieldType, chunkSize int, byItemsDesc []bool,
	keyColumns []int, keyCmpFuncs []chunk.CompareFunc) *SortPartition {
	return &SortPartition{
		inMemory:    chunk.NewList(fieldType, chunkSize, chunkSize),
		inDisk:      nil, // It's initialized only when spill is triggered
		spillError:  nil,
		memTracker:  memory.NewTracker(memory.LabelForSortPartition, -1),
		diskTracker: disk.NewTracker(memory.LabelForSortPartition, -1),
		actionSpill: nil, // It's set in ActionSpill function
		ByItemsDesc: byItemsDesc,
		keyColumns:  keyColumns,
		keyCmpFuncs: keyCmpFuncs,
	}
}

// Close close the SortPartition
func (c *SortPartition) Close() error {
	c.ptrM.Lock()
	defer c.ptrM.Unlock()
	// TODO do we need to release the memory consumption of chunks
	c.GetMemTracker().Consume(int64(-RowPtrSize * len(c.ptrM.rowPtrs)))
	c.ptrM.rowPtrs = nil
	return nil
}

func (c *SortPartition) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range c.keyColumns {
		cmpFunc := c.keyCmpFuncs[i]
		if cmpFunc != nil {
			cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
			if c.ByItemsDesc[i] {
				cmp = -cmp
			}
			if cmp < 0 {
				return true
			} else if cmp > 0 {
				return false
			}
		}
	}
	return false
}

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 10240

// keyColumnsLess is the less function for key columns.
func (c *SortPartition) keyColumnsLess(i, j int) bool {
	if c.timesOfRowCompare >= SignalCheckpointForSort {
		// Trigger Consume for checking the NeedKill signal
		c.memTracker.Consume(1)
		c.timesOfRowCompare = 0
	}

	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			c.timesOfRowCompare += 1024
		}
	})

	c.timesOfRowCompare++
	rowI := c.inMemory.GetRow(c.ptrM.rowPtrs[i])
	rowJ := c.inMemory.GetRow(c.ptrM.rowPtrs[j])
	return c.lessRow(rowI, rowJ)
}

// Sort inits pointers and sorts the records.
// We shouldn't call this function after spill.
func (c *SortPartition) Sort() (ret error) {
	// TODO do we need these locks?
	c.ptrM.Lock()
	defer c.ptrM.Unlock()
	ret = nil
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				ret = err
			} else {
				ret = fmt.Errorf("%v", r)
			}
		}
	}()

	// TODO do we need to judge this?
	if c.ptrM.rowPtrs != nil {
		return
	}

	c.ptrM.rowPtrs = make([]chunk.RowPtr, 0, c.inMemory.Len()) // The memory usage has been tracked in SortPartition.Add() function
	chunkNum := c.inMemory.NumChunks()
	for chkIdx := 0; chkIdx < chunkNum; chkIdx++ {
		chk := c.inMemory.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			c.ptrM.rowPtrs = append(c.ptrM.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}

	failpoint.Inject("errorDuringSortRowContainer", func(val failpoint.Value) {
		if val.(bool) {
			panic("sort meet error")
		}
	})

	// sort here
	sort.Slice(c.ptrM.rowPtrs, c.keyColumnsLess)
	return
}

// SpillToDisk spills data to disk. This function may be called in parallel.
func (c *SortPartition) SpillToDisk() {
	// TODO
	// err := c.Sort()
	// c.spillToDisk(err)
}

func (c *SortPartition) hasEnoughDataToSpill(t *memory.Tracker) bool {
	// Guarantee that each partition size is at least 10% of the threshold, to avoid opening too many files.
	return c.GetMemTracker().BytesConsumed() > t.GetBytesLimit()/10
}

// Add appends a chunk into the SortPartition.
func (c *SortPartition) Add(chk *chunk.Chunk) (err error) {
	c.ptrM.RLock()
	defer c.ptrM.RUnlock()
	if c.ptrM.rowPtrs != nil {
		return ErrCannotAddBecauseSorted
	}
	// Consume the memory usage of rowPtrs in advance
	c.GetMemTracker().Consume(int64(RowPtrSize * chk.NumRows()))
	// return c.RowContainer.Add(chk)
	return nil
}

// ActionSpill returns a SortAndSpillDiskAction for sorting and spilling over to disk.
func (c *SortPartition) ActionSpill() *chunk.SortAndSpillDiskAction {
	// if c.actionSpill == nil {
	// 	c.actionSpill = &chunk.SortAndSpillDiskAction{
	// 		c:                   c,
	// 		baseSpillDiskAction: c.RowContainer.ActionSpill().baseSpillDiskAction,
	// 	}
	// }
	// return c.actionSpill
	return nil
}

// ActionSpillForTest returns a SortAndSpillDiskAction for sorting and spilling over to disk for test.
func (c *SortPartition) ActionSpillForTest() *SortPartitionSpillDiskAction {
	// c.actionSpill = &SortAndSpillDiskAction{
	// 	c:                   c,
	// 	baseSpillDiskAction: c.RowContainer.ActionSpillForTest().baseSpillDiskAction,
	// }
	// return c.actionSpill
	return nil
}

// GetMemTracker return the memory tracker for the SortPartition
func (c *SortPartition) GetMemTracker() *memory.Tracker {
	return c.memTracker
}

// SortAndSpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SortAndSpillDiskAction.Action is
// triggered.
type SortPartitionSpillDiskAction struct {
	c *SortPartition
	// *baseSpillDiskAction
}

// Action sends a signal to trigger sortAndSpillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SortPartitionSpillDiskAction) Action(t *memory.Tracker) {
	// a.action(t, a.c)
}

// WaitForTest waits all goroutine have gone.
func (a *SortPartitionSpillDiskAction) WaitForTest() {
	// a.testWg.Wait()
}
