// Copyright 2021 PingCAP, Inc.
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

package cteutil

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
)

var _ Storage = &StorageRC{}

// Storage is a temporary storage to store the intermidate data of CTE.
//
// Common usage as follows:
//
//  storage.Lock()
//  if !storage.Done() {
//      fill all data into storage
//  }
//  storage.UnLock()
//  read data from storage
type Storage interface {
	// If is first called, will open underlying storage. Otherwise will add ref count by one.
	OpenAndRef() error

	// Minus ref count by one, if ref count is zero, close underlying storage.
	DerefAndClose() (err error)

	// SwapData swaps data of two storage.
	// Other metainfo is not touched, such ref count/done flag etc.
	SwapData(other Storage) error

	// Reopen reset storage and related info.
	// So the status of Storage is like a new created one.
	Reopen() error

	// Add chunk into underlying storage.
	// Should return directly if chk is empty.
	Add(chk *chunk.Chunk) error

	// Get Chunk by index.
	GetChunk(chkIdx int) (*chunk.Chunk, error)

	// Get row by RowPtr.
	GetRow(ptr chunk.RowPtr) (chunk.Row, error)

	// NumChunks return chunk number of the underlying storage.
	NumChunks() int

	// NumRows return row number of the underlying storage.
	NumRows() int

	// Storage is not thread-safe.
	// By using Lock(), users can achieve the purpose of ensuring thread safety.
	Lock()
	Unlock()

	// Usually, Storage is filled first, then user can read it.
	// User can check whether Storage is filled first, if not, they can fill it.
	Done() bool
	SetDone()

	// Store error message, so we can return directly.
	Error() error
	SetError(err error)

	// Readers use iter information to determine
	// whether they need to read data from the beginning.
	SetIter(iter int)
	GetIter() int

	GetMemTracker() *memory.Tracker
	GetDiskTracker() *disk.Tracker
	ActionSpill() *chunk.SpillDiskAction
}

// StorageRC implements Storage interface using RowContainer.
type StorageRC struct {
	mu      sync.Mutex
	refCnt  int
	tp      []*types.FieldType
	chkSize int

	done bool
	iter int
	err  error

	rc *chunk.RowContainer
}

// NewStorageRowContainer create a new StorageRC.
func NewStorageRowContainer(tp []*types.FieldType, chkSize int) *StorageRC {
	return &StorageRC{tp: tp, chkSize: chkSize}
}

// OpenAndRef impls Storage OpenAndRef interface.
func (s *StorageRC) OpenAndRef() (err error) {
	if !s.valid() {
		s.rc = chunk.NewRowContainer(s.tp, s.chkSize)
		s.refCnt = 1
		s.iter = 0
	} else {
		s.refCnt += 1
	}
	return nil
}

// DerefAndClose impls Storage DerefAndClose interface.
func (s *StorageRC) DerefAndClose() (err error) {
	if !s.valid() {
		return errors.New("Storage not opend yet")
	}
	s.refCnt -= 1
	if s.refCnt < 0 {
		return errors.New("Storage ref count is less than zero")
	} else if s.refCnt == 0 {
		// TODO: unreg memtracker
		if err = s.rc.Close(); err != nil {
			return err
		}
		if err = s.resetAll(); err != nil {
			return err
		}
	}
	return nil
}

// SwapData impls Storage Swap interface.
func (s *StorageRC) SwapData(other Storage) (err error) {
	otherRC, ok := other.(*StorageRC)
	if !ok {
		return errors.New("cannot swap if underlying storages are different")
	}
	s.tp, otherRC.tp = otherRC.tp, s.tp
	s.chkSize, otherRC.chkSize = otherRC.chkSize, s.chkSize

	s.rc, otherRC.rc = otherRC.rc, s.rc
	return nil
}

// Reopen impls Storage Reopen interface.
func (s *StorageRC) Reopen() (err error) {
	if err = s.rc.Reset(); err != nil {
		return err
	}
	s.iter = 0
	s.done = false
	s.err = nil
	// Create a new RowContainer.
	// Because some meta infos in old RowContainer are not resetted.
	// Such as memTracker/actionSpill etc. So we just use a new one.
	s.rc = chunk.NewRowContainer(s.tp, s.chkSize)
	return nil
}

// Add impls Storage Add interface.
func (s *StorageRC) Add(chk *chunk.Chunk) (err error) {
	if !s.valid() {
		return errors.New("Storage is not valid")
	}
	if chk.NumRows() == 0 {
		return nil
	}
	return s.rc.Add(chk)
}

// GetChunk impls Storage GetChunk interface.
func (s *StorageRC) GetChunk(chkIdx int) (*chunk.Chunk, error) {
	if !s.valid() {
		return nil, errors.New("Storage is not valid")
	}
	return s.rc.GetChunk(chkIdx)
}

// GetRow impls Storage GetRow interface.
func (s *StorageRC) GetRow(ptr chunk.RowPtr) (chunk.Row, error) {
	if !s.valid() {
		return chunk.Row{}, errors.New("Storage is not valid")
	}
	return s.rc.GetRow(ptr)
}

// NumChunks impls Storage NumChunks interface.
func (s *StorageRC) NumChunks() int {
	return s.rc.NumChunks()
}

// NumRows impls Storage NumRows interface.
func (s *StorageRC) NumRows() int {
	return s.rc.NumRow()
}

// Lock impls Storage Lock interface.
func (s *StorageRC) Lock() {
	s.mu.Lock()
}

// Unlock impls Storage Unlock interface.
func (s *StorageRC) Unlock() {
	s.mu.Unlock()
}

// Done impls Storage Done interface.
func (s *StorageRC) Done() bool {
	return s.done
}

// SetDone impls Storage SetDone interface.
func (s *StorageRC) SetDone() {
	s.done = true
}

// Error impls Storage Error interface.
func (s *StorageRC) Error() error {
	return s.err
}

// SetError impls Storage SetError interface.
func (s *StorageRC) SetError(err error) {
	s.err = err
}

// SetIter impls Storage SetIter interface.
func (s *StorageRC) SetIter(iter int) {
	s.iter = iter
}

// GetIter impls Storage GetIter interface.
func (s *StorageRC) GetIter() int {
	return s.iter
}

// GetMemTracker impls Storage GetMemTracker interface.
func (s *StorageRC) GetMemTracker() *memory.Tracker {
	return s.rc.GetMemTracker()
}

// GetDiskTracker impls Storage GetDiskTracker interface.
func (s *StorageRC) GetDiskTracker() *memory.Tracker {
	return s.rc.GetDiskTracker()
}

// ActionSpill impls Storage ActionSpill interface.
func (s *StorageRC) ActionSpill() *chunk.SpillDiskAction {
	return s.rc.ActionSpill()
}

// ActionSpillForTest is for test.
func (s *StorageRC) ActionSpillForTest() *chunk.SpillDiskAction {
	return s.rc.ActionSpillForTest()
}

func (s *StorageRC) resetAll() error {
	s.refCnt = -1
	s.done = false
	s.err = nil
	s.iter = 0
	if err := s.rc.Reset(); err != nil {
		return err
	}
	s.rc = nil
	return nil
}

func (s *StorageRC) valid() bool {
	return s.refCnt > 0 && s.rc != nil
}
