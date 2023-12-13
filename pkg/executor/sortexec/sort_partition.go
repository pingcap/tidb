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

package sortexec

import (
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var errSpillEmptyChunk = errors.New("can not spill empty chunk to disk")
var errFailToAddChunk = errors.New("fail to add chunk")

// It should be const, but we need to modify it for test.
var spillChunkSize int = 4096

// signalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const signalCheckpointForSort uint = 10240

type sortPartition struct {
	fieldTypes []*types.FieldType

	// Store data here
	savedRows          []chunk.Row
	totalTrackedMemNum int64
	isSorted           bool

	inDisk *chunk.DataInDiskByChunks

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	spillAction *sortPartitionSpillDiskAction
	helper      *spillHelper

	// We can't spill if size of data is lower than the limit
	spillLimit int64

	byItemsDesc []bool
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	// Sort is a time-consuming operation, we need to set a checkpoint to detect
	// the outside signal periodically.
	timesOfRowCompare uint

	spillTriggered bool

	// It's index that points to the data need to be read next time.
	idx int
}

// Creates a new SortPartition in memory.
func newSortPartition(fieldTypes []*types.FieldType, chunkSize int, byItemsDesc []bool,
	keyColumns []int, keyCmpFuncs []chunk.CompareFunc, helper *spillHelper, spillLimit int64) *sortPartition {
	helper.reset()
	retVal := &sortPartition{
		fieldTypes:         fieldTypes,
		savedRows:          make([]chunk.Row, 0),
		totalTrackedMemNum: 0,
		isSorted:           false,
		inDisk:             nil, // It's initialized only when spill is triggered
		memTracker:         memory.NewTracker(memory.LabelForSortPartition, -1),
		diskTracker:        disk.NewTracker(memory.LabelForSortPartition, -1),
		spillAction:        nil, // It's set in `actionSpill` function
		helper:             helper,
		spillLimit:         spillLimit,
		byItemsDesc:        byItemsDesc,
		keyColumns:         keyColumns,
		keyCmpFuncs:        keyCmpFuncs,
		spillTriggered:     false,
		idx:                0,
	}

	return retVal
}

func (s *sortPartition) close() error {
	s.getMemTracker().Consume(-s.totalTrackedMemNum)
	if s.inDisk != nil {
		s.inDisk.Close()
		s.inDisk = nil
	}
	s.spillAction = nil
	s.memTracker = nil
	s.diskTracker = nil
	return nil
}

// Return false if the spill is triggered in this partition.
func (s *sortPartition) add(chk *chunk.Chunk) bool {
	rowNum := chk.NumRows()
	consumedBytesNum := chunk.RowSize*int64(rowNum) + chk.MemoryUsage()

	s.spillAction.helper.syncLock.Lock()
	defer s.spillAction.helper.syncLock.Unlock()
	if s.isSpillTriggered() {
		return false
	}

	// Convert chunk to rows
	for i := 0; i < rowNum; i++ {
		s.savedRows = append(s.savedRows, chk.GetRow(i))
	}

	s.getMemTracker().Consume(consumedBytesNum)
	s.totalTrackedMemNum += consumedBytesNum
	return true
}

func (s *sortPartition) sort() (ret error) {
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

	if s.isSorted {
		return
	}

	failpoint.Inject("errorDuringSortRowContainer", func(val failpoint.Value) {
		if val.(bool) {
			panic("sort meet error")
		}
	})

	sort.Slice(s.savedRows, s.keyColumnsLess)
	s.isSorted = true
	return
}

func (s *sortPartition) spillToDiskImpl() error {
	s.inDisk = chunk.NewDataInDiskByChunks(s.fieldTypes)
	s.diskTracker.AttachTo(s.diskTracker)
	tmpChk := chunk.NewChunkWithCapacity(s.fieldTypes, spillChunkSize)

	rowNum := len(s.savedRows)
	if rowNum == 0 {
		return errSpillEmptyChunk
	}

	for ; s.idx < rowNum; s.idx++ {
		row := s.savedRows[s.idx]
		tmpChk.AppendRow(row)
		if tmpChk.IsFull() {
			err := s.inDisk.Add(tmpChk)
			if err != nil {
				return err
			}
			tmpChk.Reset()
		}
	}

	// Spill the remaining data in tmpChk.
	// Do not spill when tmpChk is empty as `Add` function requires a non-empty chunk
	if tmpChk.NumRows() > 0 {
		err := s.inDisk.Add(tmpChk)
		if err != nil {
			return err
		}
	}

	// Release memory as all data have been spilled to disk
	s.savedRows = nil
	s.getMemTracker().Consume(-s.totalTrackedMemNum)
	return nil
}

// We can only call this function under the protection of `syncLock`.
func (s *sortPartition) spillToDisk() error {
	err := s.sort()
	if err != nil {
		return err
	}

	// We should consider spill has been triggered once goroutine get `syncLock`.
	// However, `s.sort()` will retrigger the spill and lead to dead lock
	// so we have to set spill flag at here.
	s.setSpillTriggered()
	s.helper.setIsSpilling(true)
	err = s.spillToDiskImpl()
	s.helper.setIsSpilling(false)
	s.helper.cond.Broadcast()
	return err
}

func (s *sortPartition) actionSpill(helper *spillHelper) *sortPartitionSpillDiskAction {
	if s.spillAction == nil {
		s.spillAction = &sortPartitionSpillDiskAction{
			partition: s,
			helper:    helper,
		}
	}
	return s.spillAction
}

func (s *sortPartition) getSortedRowFromMemoryAndAppendToChunk(filledChk *chunk.Chunk) {
	filledChk.AppendRow(s.savedRows[s.idx])
	s.advanceIdx()
}

func (s *sortPartition) getMemTracker() *memory.Tracker {
	return s.memTracker
}

func (s *sortPartition) getDiskTracker() *disk.Tracker {
	return s.diskTracker
}

func (s *sortPartition) hasEnoughDataToSpill() bool {
	// Guarantee that each partition size is not too small, to avoid opening too many files.
	return s.getMemTracker().BytesConsumed() > s.spillLimit
}

func (s *sortPartition) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range s.keyColumns {
		cmpFunc := s.keyCmpFuncs[i]
		if cmpFunc != nil {
			cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
			if s.byItemsDesc[i] {
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

func (s *sortPartition) numRowInMemory() int {
	return len(s.savedRows)
}

// keyColumnsLess is the less function for key columns.
func (s *sortPartition) keyColumnsLess(i, j int) bool {
	if s.timesOfRowCompare >= signalCheckpointForSort {
		// Trigger Consume for checking the NeedKill signal
		s.memTracker.Consume(1)
		s.timesOfRowCompare = 0
	}

	failpoint.Inject("signalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			s.timesOfRowCompare += 1024
		}
	})

	s.timesOfRowCompare++
	return s.lessRow(s.savedRows[i], s.savedRows[j])
}

func (s *sortPartition) getIdx() int {
	return s.idx
}

func (s *sortPartition) advanceIdx() {
	s.idx++
}

func (s *sortPartition) isSpillTriggered() bool {
	return s.spillTriggered
}

func (s *sortPartition) setSpillTriggered() {
	s.spillTriggered = true
}

func (s *sortPartition) numRowForTest() int64 {
	rowNumInMemory := int64(s.numRowInMemory())
	if s.inDisk != nil {
		if rowNumInMemory > 0 {
			panic("Data shouldn't be placed in memory and disk simultaneously")
		}
		return s.inDisk.NumRows()
	}

	return rowNumInMemory
}

func SetSmallSpillChunkSizeForTest() {
	spillChunkSize = 16
}
