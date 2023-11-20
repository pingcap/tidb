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
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var errSpillIsTriggered = errors.New("can not add because spill has been triggered")
var errSpillEmptyChunk = errors.New("can not spill empty chunk to disk")

const rowPtrSize = int(unsafe.Sizeof(chunk.RowPtr{}))

// It should be const, but we need to modify it for test.
var spillChunkSize int = 4096

// signalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const signalCheckpointForSort uint = 10240

type sortPartition struct {
	fieldTypes []*types.FieldType
	inMemory   *chunk.List

	// TODO replace DataInDiskByRows with DataInDiskByChunks
	inDisk *chunk.DataInDiskByRows

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	spillAction *sortPartitionSpillDiskAction

	// We can't spill if size of data is lower than the limit
	spillLimit int64

	// rowPtrs store the chunk index and row index for each row.
	// rowPtrs != nil indicates the pointer is initialized and sorted.
	// It will get an ErrCannotAddBecauseSorted when trying to insert data if rowPtrs != nil.
	rowPtrs []chunk.RowPtr

	byItemsDesc []bool
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	// Sort is a time-consuming operation, we need to set a checkpoint to detect
	// the outside signal periodically.
	timesOfRowCompare uint
}

// Creates a new SortPartition in memory.
func newSortPartition(fieldTypes []*types.FieldType, chunkSize int, byItemsDesc []bool,
	keyColumns []int, keyCmpFuncs []chunk.CompareFunc, spillLimit int64) *sortPartition {
	retVal := &sortPartition{
		fieldTypes:  fieldTypes,
		inMemory:    chunk.NewList(fieldTypes, chunkSize, chunkSize),
		inDisk:      nil, // It's initialized only when spill is triggered
		memTracker:  memory.NewTracker(memory.LabelForSortPartition, -1),
		diskTracker: disk.NewTracker(memory.LabelForSortPartition, -1),
		spillAction: nil, // It's set in `actionSpill` function
		spillLimit:  spillLimit,
		byItemsDesc: byItemsDesc,
		keyColumns:  keyColumns,
		keyCmpFuncs: keyCmpFuncs,
	}

	retVal.inMemory.GetMemTracker().AttachTo(retVal.memTracker)
	return retVal
}

func (s *sortPartition) close() error {
	s.getMemTracker().Consume(int64(-rowPtrSize * len(s.rowPtrs)))
	s.rowPtrs = nil
	s.inMemory.Clear()
	return nil
}

// Appends a chunk into the SortPartition.
func (s *sortPartition) add(chk *chunk.Chunk) error {
	if s.spillAction.isSpillTriggered() {
		return errSpillIsTriggered
	}

	// Consume the memory usage of rowPtrs in advance
	// Memory usage of chunks will be added in `s.inMemory.Add(chk)`
	s.getMemTracker().Consume(int64(rowPtrSize * chk.NumRows()))
	s.inMemory.Add(chk)

	if s.spillAction.needSpill() {
		return s.spillToDisk()
	}

	return nil
}

// sort inits pointers and sorts the records.
// We shouldn't call this function after spill.
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

	if s.rowPtrs != nil {
		return
	}

	s.rowPtrs = make([]chunk.RowPtr, 0, s.inMemory.Len()) // The memory usage has been tracked in SortPartition.add() function
	chunkNum := s.inMemory.NumChunks()
	for chkIdx := 0; chkIdx < chunkNum; chkIdx++ {
		chk := s.inMemory.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			s.rowPtrs = append(s.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}

	failpoint.Inject("errorDuringSortRowContainer", func(val failpoint.Value) {
		if val.(bool) {
			panic("sort meet error")
		}
	})

	// sort here
	sort.Slice(s.rowPtrs, s.keyColumnsLess)
	return
}

func (s *sortPartition) spillToDiskImpl() error {
	s.inDisk = chunk.NewDataInDiskByRows(s.fieldTypes)
	s.diskTracker.AttachTo(s.diskTracker)
	tmpChk := chunk.NewChunkWithCapacity(s.fieldTypes, spillChunkSize)

	rowNum := len(s.rowPtrs)
	if rowNum == 0 {
		return errSpillEmptyChunk
	}

	for i := 0; i < rowNum; i++ {
		chkIdx := s.rowPtrs[i].ChkIdx
		rowIdx := s.rowPtrs[i].RowIdx
		row := s.inMemory.GetChunk(int(chkIdx)).GetRow(int(rowIdx))
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
	s.getMemTracker().Consume(-int64(rowPtrSize * len(s.rowPtrs)))
	s.rowPtrs = nil
	s.inMemory.Clear()
	return nil
}

func (s *sortPartition) spillToDisk() error {
	err := s.sort()
	if err != nil {
		return err
	}

	return s.spillToDiskImpl()
}

func (s *sortPartition) actionSpill() *sortPartitionSpillDiskAction {
	if s.spillAction == nil {
		s.spillAction = &sortPartitionSpillDiskAction{
			partition:     s,
			isSpillNeeded: false,
		}
	}
	return s.spillAction
}

func (s *sortPartition) getSortedRowFromMemoryAndAppendToChunk(idx int, filledChk *chunk.Chunk) {
	rowPtr := s.rowPtrs[idx]
	chk := s.inMemory.GetChunk(int(rowPtr.ChkIdx))
	row := chk.GetRow(int(rowPtr.RowIdx))
	filledChk.AppendRow(row)
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
	return s.inMemory.Len()
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
	rowI := s.inMemory.GetRow(s.rowPtrs[i])
	rowJ := s.inMemory.GetRow(s.rowPtrs[j])
	return s.lessRow(rowI, rowJ)
}

func (s *sortPartition) numRowForTest() int {
	rowNumInMemory := s.numRowInMemory()
	if s.inDisk != nil {
		if rowNumInMemory > 0 {
			panic("Data shouldn't be placed in memory and disk simultaneously")
		}
		return s.inDisk.Len()
	}

	return rowNumInMemory
}

func SetSmallSpillChunkSizeForTest() {
	spillChunkSize = 16
}
