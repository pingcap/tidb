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
// See the License for the specific language governing permissions and
// limitations under the License.

package cte_storage

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
)

var _ CTEStorage = &CTEStorageRC{}

// CTEStorage is a temporary storage to store the intermidate data of CTE.
//
// Common usage as follows:
//
//  storage.Lock()
//  if !storage.Done() {
//      fill all data into storage
//  }
//  storage.UnLock()
//  read data from storage
type CTEStorage interface {
	// If is first called, will open underlying storage. Otherwise will add ref count by one.
	OpenAndRef() error

	// Minus ref count by one, if ref count is zero, close underlying storage.
	DerefAndClose() (err error)

	// Swap data of two storage. Other metainfo is not touched, such ref count/done flag etc.
	SwapData(other CTEStorage) error

	// Reopen reset storage and related info.
	// So the status of CTEStorage is like a new created one.
	Reopen() error

	// Add chunk into underlying storage.
	Add(chk *chunk.Chunk) error

	// Get Chunk by index.
	GetChunk(chkIdx int) (*chunk.Chunk, error)

	// Get row by RowPtr.
	GetRow(ptr chunk.RowPtr) (chunk.Row, error)

	// NumChunks return chunk number of the underlying storage.
	NumChunks() int

	// CTEStorage is not thread-safe.
	// By using Lock(), users can achieve the purpose of ensuring thread safety.
	Lock()
	Unlock()

	// Usually, CTEStorage is filled first, then user can read it.
	// User can check whether CTEStorage is filled first, if not, they can fill it.
	Done() bool
	SetDone()

	SetIter(iter int)
	// Readers use iter information to determine
	// whether they need to read data from the beginning.
	GetIter() int

	// We use this channel to notify reader that CTEStorage is ready to read.
	// It exists only to solve the special implementation of IndexLookUpJoin.
	// We will find a better way and remove this later.
	GetBegCh() chan struct{}

	GetMemTracker() *memory.Tracker
	GetDiskTracker() *disk.Tracker
	ActionSpill() memory.ActionOnExceed
}

// CTEStorage implementation using RowContainer.
type CTEStorageRC struct {
	// meta info
	mu      sync.Mutex
	refCnt  int
	tp      []*types.FieldType
	chkSize int

	// data info
	begCh chan struct{}
	done  bool
	iter  int

	// data
	rc *chunk.RowContainer
}

// NewCTEStorageRC create a new CTEStorageRC.
func NewCTEStorageRC(tp []*types.FieldType, chkSize int) *CTEStorageRC {
	return &CTEStorageRC{tp: tp, chkSize: chkSize}
}

// OpenAndRef impls CTEStorage OpenAndRef interface.
func (s *CTEStorageRC) OpenAndRef() (err error) {
	if !s.valid() {
		s.rc = chunk.NewRowContainer(s.tp, s.chkSize)
		s.refCnt = 1
		s.begCh = make(chan struct{})
		s.iter = 0
	} else {
		s.refCnt += 1
	}
	return nil
}

// DerefAndClose impls CTEStorage DerefAndClose interface.
func (s *CTEStorageRC) DerefAndClose() (err error) {
	if !s.valid() {
		return errors.Trace(errors.New("CTEStorage not opend yet"))
	}
	s.refCnt -= 1
	if s.refCnt < 0 {
		return errors.Trace(errors.New("CTEStorage ref count is less than zero"))
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

// Swap impls CTEStorage Swap interface.
func (s *CTEStorageRC) SwapData(other CTEStorage) (err error) {
	otherRC, ok := other.(*CTEStorageRC)
	if !ok {
		return errors.Trace(errors.New("cannot swap if underlying storages are different"))
	}
	s.tp, otherRC.tp = otherRC.tp, s.tp
	s.chkSize, otherRC.chkSize = otherRC.chkSize, s.chkSize

	s.rc, otherRC.rc = otherRC.rc, s.rc
	return nil
}

// Reopen impls CTEStorage Reopen interface.
func (s *CTEStorageRC) Reopen() (err error) {
	if err = s.rc.Reset(); err != nil {
		return err
	}
	s.iter = 0
	s.begCh = make(chan struct{})
	s.done = false
	// Create a new RowContainer.
	// Because some meta infos in old RowContainer are not resetted.
	// Such as memTracker/actionSpill etc. So we just use a new one.
	s.rc = chunk.NewRowContainer(s.tp, s.chkSize)
	return nil
}

// Add impls CTEStorage Add interface.
func (s *CTEStorageRC) Add(chk *chunk.Chunk) (err error) {
	if !s.valid() {
		return errors.Trace(errors.New("CTEStorage is not valid"))
	}
	if chk.NumRows() == 0 {
		return nil
	}
	return s.rc.Add(chk)
}

// GetChunk impls CTEStorage GetChunk interface.
func (s *CTEStorageRC) GetChunk(chkIdx int) (*chunk.Chunk, error) {
	if !s.valid() {
		return nil, errors.Trace(errors.New("CTEStorage is not valid"))
	}
	return s.rc.GetChunk(chkIdx)
}

// GetRow impls CTEStorage GetRow interface.
func (s *CTEStorageRC) GetRow(ptr chunk.RowPtr) (chunk.Row, error) {
	if !s.valid() {
		return chunk.Row{}, errors.Trace(errors.New("CTEStorage is not valid"))
	}
	return s.rc.GetRow(ptr)
}

// NumChunks impls CTEStorage NumChunks interface.
func (s *CTEStorageRC) NumChunks() int {
	return s.rc.NumChunks()
}

// Lock impls CTEStorage Lock interface.
func (s *CTEStorageRC) Lock() {
	s.mu.Lock()
}

// Unlock impls CTEStorage Unlock interface.
func (s *CTEStorageRC) Unlock() {
	s.mu.Unlock()
}

// Done impls CTEStorage Done interface.
func (s *CTEStorageRC) Done() bool {
	return s.done
}

// SetDone impls CTEStorage SetDone interface.
func (s *CTEStorageRC) SetDone() {
	s.done = true
}

// SetIter impls CTEStorage SetIter interface.
func (s *CTEStorageRC) SetIter(iter int) {
	s.iter = iter
}

// GetIter impls CTEStorage GetIter interface.
func (s *CTEStorageRC) GetIter() int {
	return s.iter
}

// GetBegCh impls CTEStorage GetBegCh interface.
func (s *CTEStorageRC) GetBegCh() chan struct{} {
	return s.begCh
}

// GetMemTracker impls CTEStorage GetMemTracker interface.
func (s *CTEStorageRC) GetMemTracker() *memory.Tracker {
	return s.rc.GetMemTracker()
}

// GetDiskTracker impls CTEStorage GetDiskTracker interface.
func (s *CTEStorageRC) GetDiskTracker() *memory.Tracker {
	return s.rc.GetDiskTracker()
}

// ActionSpill impls CTEStorage ActionSpill interface.
func (s *CTEStorageRC) ActionSpill() memory.ActionOnExceed {
	return s.rc.ActionSpill()
}

// ActionSpillForTest is for test.
func (s *CTEStorageRC) ActionSpillForTest() *chunk.SpillDiskAction {
	return s.rc.ActionSpillForTest()
}

func (s *CTEStorageRC) resetAll() error {
	s.refCnt = -1
	s.begCh = nil
	s.done = false
	s.iter = 0
	if err := s.rc.Reset(); err != nil {
		return err
	}
	s.rc = nil
	return nil
}

func (s *CTEStorageRC) valid() bool {
	return s.refCnt > 0 && s.rc != nil
}
