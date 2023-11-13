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
	"sync"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var errCannotAddBecauseSorted = errors.New("can not add because sorted")
var errSpillIsTriggered = errors.New("can not add because spill has been triggered")

const RowPtrSize = int(unsafe.Sizeof(chunk.RowPtr{}))

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 10240

type SortPartition struct {
	lock sync.Mutex

	inMemory *chunk.List

	// TODO replace DataInDiskByRows with DataInDiskByChunks
	inDisk *chunk.DataInDiskByRows

	// spillError stores the error when spilling.
	spillError error

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	spillAction *SortPartitionSpillDiskAction

	// rowPtrs store the chunk index and row index for each row.
	// rowPtrs != nil indicates the pointer is initialized and sorted.
	// It will get an ErrCannotAddBecauseSorted when trying to insert data if rowPtrs != nil.
	rowPtrs []chunk.RowPtr

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
	retVal := &SortPartition{
		inMemory:    chunk.NewList(fieldType, chunkSize, chunkSize),
		inDisk:      nil, // It's initialized only when spill is triggered
		spillError:  nil,
		memTracker:  memory.NewTracker(memory.LabelForSortPartition, -1),
		diskTracker: disk.NewTracker(memory.LabelForSortPartition, -1),
		spillAction: nil, // It's set in ActionSpill function
		ByItemsDesc: byItemsDesc,
		keyColumns:  keyColumns,
		keyCmpFuncs: keyCmpFuncs,
	}

	retVal.inMemory.GetMemTracker().AttachTo(retVal.memTracker)
	return retVal
}

// Close close the SortPartition
func (s *SortPartition) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	// TODO do we need to release the memory consumption of chunks
	s.GetMemTracker().Consume(int64(-RowPtrSize * len(s.rowPtrs)))
	s.rowPtrs = nil
	s.inMemory.Clear()
	return nil
}

// Add appends a chunk into the SortPartition.
func (s *SortPartition) Add(chk *chunk.Chunk) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.spillAction.isSpillTriggered() {
		if s.spillError != nil {
			return s.spillError
		}
		return errSpillIsTriggered
	}

	// Consume the memory usage of rowPtrs in advance
	// Memory usage of chunks will be added in `s.inMemory.Add(chk)`
	s.GetMemTracker().Consume(int64(RowPtrSize * chk.NumRows()))

	s.inMemory.Add(chk)
	return nil
}

// Sort inits pointers and sorts the records.
// We shouldn't call this function after spill.
func (s *SortPartition) Sort() (ret error) {
	s.lock.Lock()
	defer s.lock.Unlock()
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

	s.rowPtrs = make([]chunk.RowPtr, 0, s.inMemory.Len()) // The memory usage has been tracked in SortPartition.Add() function
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

func (s *SortPartition) spillToDisk(err error) {

}

// SpillToDisk spills data to disk.
func (s *SortPartition) SpillToDisk() {
	// TODO
	// err := s.Sort()
	// s.spillToDisk(err)
}

// ActionSpill returns a SortAndSpillDiskAction for sorting and spilling over to disk.
func (s *SortPartition) ActionSpill() *SortPartitionSpillDiskAction {
	if s.spillAction == nil {
		s.spillAction = &SortPartitionSpillDiskAction{
			partition:      s,
			spillTriggered: false,
		}
	}
	return s.spillAction
}

// GetMemTracker return the memory tracker for the SortPartition
func (s *SortPartition) GetMemTracker() *memory.Tracker {
	return s.memTracker
}

func (s *SortPartition) hasEnoughDataToSpill(t *memory.Tracker) bool {
	// Guarantee that each partition size is at least 10% of the threshold, to avoid opening too many files.
	return s.GetMemTracker().BytesConsumed() > t.GetBytesLimit()/10
}

func (s *SortPartition) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range s.keyColumns {
		cmpFunc := s.keyCmpFuncs[i]
		if cmpFunc != nil {
			cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
			if s.ByItemsDesc[i] {
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

// keyColumnsLess is the less function for key columns.
func (s *SortPartition) keyColumnsLess(i, j int) bool {
	if s.timesOfRowCompare >= SignalCheckpointForSort {
		// Trigger Consume for checking the NeedKill signal
		s.memTracker.Consume(1)
		s.timesOfRowCompare = 0
	}

	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			s.timesOfRowCompare += 1024
		}
	})

	s.timesOfRowCompare++
	rowI := s.inMemory.GetRow(s.rowPtrs[i])
	rowJ := s.inMemory.GetRow(s.rowPtrs[j])
	return s.lessRow(rowI, rowJ)
}
