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
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

type sortPartition struct {
	// cond is only used for protecting spillStatus
	cond        *sync.Cond
	spillStatus int

	// syncLock is used to protect variables except `spillStatus`
	syncLock sync.Mutex

	// Data are stored in savedRows
	savedRows []chunk.Row
	sliceIter *chunk.Iterator4Slice
	isSorted  bool

	// cursor iterates the spilled chunks.
	cursor *dataCursor
	inDisk *chunk.DataInDiskByChunks

	spillError error
	closed     bool

	fieldTypes []*types.FieldType

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	spillAction *sortPartitionSpillDiskAction

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
}

// Creates a new SortPartition in memory.
func newSortPartition(fieldTypes []*types.FieldType, byItemsDesc []bool,
	keyColumns []int, keyCmpFuncs []chunk.CompareFunc, spillLimit int64) *sortPartition {
	lock := new(sync.Mutex)
	retVal := &sortPartition{
		cond:        sync.NewCond(lock),
		spillError:  nil,
		spillStatus: notSpilled,
		fieldTypes:  fieldTypes,
		savedRows:   make([]chunk.Row, 0),
		isSorted:    false,
		inDisk:      nil, // It's initialized only when spill is triggered
		memTracker:  memory.NewTracker(memory.LabelForSortPartition, -1),
		diskTracker: disk.NewTracker(memory.LabelForSortPartition, -1),
		spillAction: nil, // It's set in `actionSpill` function
		spillLimit:  spillLimit,
		byItemsDesc: byItemsDesc,
		keyColumns:  keyColumns,
		keyCmpFuncs: keyCmpFuncs,
		cursor:      NewDataCursor(),
		closed:      false,
	}

	return retVal
}

func (s *sortPartition) close() {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	s.closed = true
	if s.inDisk != nil {
		s.inDisk.Close()
	}
	s.getMemTracker().ReplaceBytesUsed(0)
}

// Return false if the spill is triggered in this partition.
func (s *sortPartition) add(chk *chunk.Chunk) bool {
	rowNum := chk.NumRows()
	consumedBytesNum := chunk.RowSize*int64(rowNum) + chk.MemoryUsage()

	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	if s.isSpillTriggered() {
		return false
	}

	// Convert chunk to rows
	for i := 0; i < rowNum; i++ {
		s.savedRows = append(s.savedRows, chk.GetRow(i))
	}

	s.getMemTracker().Consume(consumedBytesNum)
	return true
}

func (s *sortPartition) sort() error {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return s.sortNoLock()
}

func (s *sortPartition) sortNoLock() (ret error) {
	ret = nil
	defer func() {
		if r := recover(); r != nil {
			ret = util.GetRecoverError(r)
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
	s.sliceIter = chunk.NewIterator4Slice(s.savedRows)
	return
}

func (s *sortPartition) spillToDiskImpl() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
	}()

	if s.closed {
		return nil
	}

	s.inDisk = chunk.NewDataInDiskByChunks(s.fieldTypes)
	s.inDisk.GetDiskTracker().AttachTo(s.diskTracker)
	tmpChk := chunk.NewChunkWithCapacity(s.fieldTypes, spillChunkSize)

	rowNum := len(s.savedRows)
	if rowNum == 0 {
		return errSpillEmptyChunk
	}

	for row := s.sliceIter.Next(); !row.IsEmpty(); row = s.sliceIter.Next() {
		tmpChk.AppendRow(row)
		if tmpChk.IsFull() {
			err := s.inDisk.Add(tmpChk)
			if err != nil {
				return err
			}
			tmpChk.Reset()
			s.getMemTracker().HandleKillSignal()
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
	s.sliceIter = nil
	s.getMemTracker().ReplaceBytesUsed(0)
	return nil
}

// We can only call this function under the protection of `syncLock`.
func (s *sortPartition) spillToDisk() error {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	if s.isSpillTriggered() {
		return nil
	}

	err := s.sortNoLock()
	if err != nil {
		return err
	}

	s.setIsSpilling()
	defer s.cond.Broadcast()
	defer s.setSpillTriggered()

	err = s.spillToDiskImpl()
	return err
}

func (s *sortPartition) getNextSortedRow() (chunk.Row, error) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	if s.isSpillTriggered() {
		row := s.cursor.next()
		if row.IsEmpty() {
			success, err := reloadCursor(s.cursor, s.inDisk)
			if err != nil {
				return chunk.Row{}, err
			}
			if !success {
				// All data has been consumed
				return chunk.Row{}, nil
			}

			row = s.cursor.begin()
			if row.IsEmpty() {
				return chunk.Row{}, errors.New("Get an empty row")
			}
		}
		return row, nil
	}

	row := s.sliceIter.Next()
	return row, nil
}

func (s *sortPartition) actionSpill() *sortPartitionSpillDiskAction {
	if s.spillAction == nil {
		s.spillAction = &sortPartitionSpillDiskAction{
			partition: s,
		}
	}
	return s.spillAction
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

// keyColumnsLess is the less function for key columns.
func (s *sortPartition) keyColumnsLess(i, j int) bool {
	if s.timesOfRowCompare >= signalCheckpointForSort {
		// Trigger Consume for checking the NeedKill signal
		s.memTracker.HandleKillSignal()
		s.timesOfRowCompare = 0
	}

	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			s.timesOfRowCompare += 1024
		}
	})

	s.timesOfRowCompare++
	return s.lessRow(s.savedRows[i], s.savedRows[j])
}

func (s *sortPartition) isSpillTriggered() bool {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	return s.spillStatus == spillTriggered
}

func (s *sortPartition) isSpillTriggeredNoLock() bool {
	return s.spillStatus == spillTriggered
}

func (s *sortPartition) setSpillTriggered() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	s.spillStatus = spillTriggered
}

func (s *sortPartition) setIsSpilling() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	s.spillStatus = inSpilling
}

func (s *sortPartition) getIsSpillingNoLock() bool {
	return s.spillStatus == inSpilling
}

func (s *sortPartition) setError(err error) {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	s.spillError = err
}

func (s *sortPartition) checkError() error {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return s.spillError
}

func (s *sortPartition) numRowInDiskForTest() int64 {
	if s.inDisk != nil {
		return s.inDisk.NumRows()
	}
	return 0
}

func (s *sortPartition) numRowInMemoryForTest() int64 {
	if s.sliceIter != nil {
		if s.sliceIter.Len() != len(s.savedRows) {
			panic("length of sliceIter should be equal to savedRows")
		}
	}
	return int64(len(s.savedRows))
}

// SetSmallSpillChunkSizeForTest set spill chunk size for test.
func SetSmallSpillChunkSizeForTest() {
	spillChunkSize = 16
}
