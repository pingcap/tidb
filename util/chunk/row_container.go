// Copyright 2018 PingCAP, Inc.
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

package chunk

import (
	"errors"
	"sync"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

// RowContainer provides a place for many rows, so many that we might want to spill them into disk.
type RowContainer struct {
	m struct {
		// RWMutex guarantees spill and get operator for rowContainer is mutually exclusive.
		sync.RWMutex
		// records stores the chunks in memory.
		records *List
		// recordsInDisk stores the chunks in disk.
		recordsInDisk *ListInDisk
		// spillError stores the error when spilling.
		spillError error
	}

	fieldType []*types.FieldType
	chunkSize int
	numRow    int

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	actionSpill *SpillDiskAction
}

// NewRowContainer creates a new RowContainer in memory.
func NewRowContainer(fieldType []*types.FieldType, chunkSize int) *RowContainer {
	li := NewList(fieldType, chunkSize, chunkSize)
	rc := &RowContainer{fieldType: fieldType, chunkSize: chunkSize}
	rc.m.records = li
	rc.memTracker = li.memTracker
	rc.diskTracker = disk.NewTracker(stringutil.StringerStr("RowContainer"), -1)
	return rc
}

// SpillToDisk spills data to disk.
func (c *RowContainer) SpillToDisk() {
	c.m.Lock()
	defer c.m.Unlock()
	if c.AlreadySpilled() {
		return
	}
	var err error
	N := c.m.records.NumChunks()
	c.m.recordsInDisk = NewListInDisk(c.m.records.FieldTypes())
	c.m.recordsInDisk.diskTracker.AttachTo(c.diskTracker)
	for i := 0; i < N; i++ {
		chk := c.m.records.GetChunk(i)
		err = c.m.recordsInDisk.Add(chk)
		if err != nil {
			c.m.spillError = err
			return
		}
	}
	c.m.records.Clear()
	return
}

// Reset resets RowContainer.
func (c *RowContainer) Reset() error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.AlreadySpilled() {
		err := c.m.recordsInDisk.Close()
		c.m.recordsInDisk = nil
		if err != nil {
			return err
		}
	} else {
		c.m.records.Reset()
	}
	return nil
}

// AlreadySpilled indicates that records have spilled out into disk.
func (c *RowContainer) AlreadySpilled() bool {
	return c.m.recordsInDisk != nil
}

// AlreadySpilledSafe indicates that records have spilled out into disk. It's thread-safe.
func (c *RowContainer) AlreadySpilledSafe() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.m.recordsInDisk != nil
}

// NumRow returns the number of rows in the container
func (c *RowContainer) NumRow() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.m.recordsInDisk.Len()
	}
	return c.m.records.Len()
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (c *RowContainer) NumRowsOfChunk(chkID int) int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.m.recordsInDisk.NumRowsOfChunk(chkID)
	}
	return c.m.records.NumRowsOfChunk(chkID)
}

// NumChunks returns the number of chunks in the container.
func (c *RowContainer) NumChunks() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		return c.m.recordsInDisk.NumChunks()
	}
	return c.m.records.NumChunks()
}

// Add appends a chunk into the RowContainer.
func (c *RowContainer) Add(chk *Chunk) (err error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		if c.m.spillError != nil {
			return c.m.spillError
		}
		err = c.m.recordsInDisk.Add(chk)
	} else {
		c.m.records.Add(chk)
	}
	return
}

// AppendRow appends a row to the RowContainer, the row is copied to the RowContainer.
func (c *RowContainer) AppendRow(row Row) (RowPtr, error) {
	if c.AlreadySpilled() {
		return RowPtr{}, errors.New("ListInDisk don't support AppendRow")
	}
	return c.m.records.AppendRow(row), nil
}

// AllocChunk allocates a new chunk from RowContainer.
func (c *RowContainer) AllocChunk() (chk *Chunk) {
	return c.m.records.allocChunk()
}

// GetChunk returns chkIdx th chunk of in memory records.
func (c *RowContainer) GetChunk(chkIdx int) *Chunk {
	return c.m.records.GetChunk(chkIdx)
}

// GetList returns the list of in memory records.
func (c *RowContainer) GetList() *List {
	return c.m.records
}

// GetRow returns the row the ptr pointed to.
func (c *RowContainer) GetRow(ptr RowPtr) (Row, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.AlreadySpilled() {
		if c.m.spillError != nil {
			return Row{}, c.m.spillError
		}
		return c.m.recordsInDisk.GetRow(ptr)
	}
	return c.m.records.GetRow(ptr), nil
}

// GetMemTracker returns the memory tracker in records, panics if the RowContainer has already spilled.
func (c *RowContainer) GetMemTracker() *memory.Tracker {
	return c.memTracker
}

// GetDiskTracker returns the underlying disk usage tracker in recordsInDisk.
func (c *RowContainer) GetDiskTracker() *disk.Tracker {
	return c.diskTracker
}

// Close close the RowContainer
func (c *RowContainer) Close() (err error) {
	if c.AlreadySpilled() {
		err = c.m.recordsInDisk.Close()
		c.m.recordsInDisk = nil
	}
	c.m.records.Clear()
	return
}

// ActionSpill returns a SpillDiskAction for spilling over to disk.
func (c *RowContainer) ActionSpill() *SpillDiskAction {
	c.actionSpill = &SpillDiskAction{c: c}
	return c.actionSpill
}

// SpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SpillDiskAction.Action is
// triggered.
type SpillDiskAction struct {
	c              *RowContainer
	fallbackAction memory.ActionOnExceed
	m              sync.Mutex
}

// Action sends a signal to trigger spillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SpillDiskAction) Action(t *memory.Tracker) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.c.AlreadySpilledSafe() {
		if a.fallbackAction != nil {
			a.fallbackAction.Action(t)
		}
	} else {
		// TODO: Refine processing for various errors. Return or Panic.
		logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
			zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
		go func() {
			a.c.SpillToDisk()
		}()
	}
}

// SetFallback sets the fallback action.
func (a *SpillDiskAction) SetFallback(fallback memory.ActionOnExceed) {
	a.fallbackAction = fallback
}

// SetLogHook sets the hook, it does nothing just to form the memory.ActionOnExceed interface.
func (a *SpillDiskAction) SetLogHook(hook func(uint64)) {}

// ResetRowContainer resets the spill action and sets the RowContainer for the SpillDiskAction.
func (a *SpillDiskAction) ResetRowContainer(c *RowContainer) {
	a.m.Lock()
	defer a.m.Unlock()
	a.c = c
}
