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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
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
		// spilled indicates that records have spilled out into disk.
		// It's for concurrency usage, e.g. AlreadySpilledSafe() maybe access it without lock, so access it with atomic.
		spilled uint32
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
	rc.diskTracker = disk.NewTracker(memory.LabelForRowContainer, -1)
	return rc
}

// SpillToDisk spills data to disk. This function may be called in parallel.
func (c *RowContainer) SpillToDisk() {
	c.m.Lock()
	defer c.m.Unlock()
	if c.alreadySpilled() {
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
	atomic.StoreUint32(&c.m.spilled, 1)
	return
}

// Reset resets RowContainer.
func (c *RowContainer) Reset() error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.alreadySpilled() {
		atomic.StoreUint32(&c.m.spilled, 0)
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

// alreadySpilled indicates that records have spilled out into disk.
func (c *RowContainer) alreadySpilled() bool {
	return c.m.recordsInDisk != nil
}

// AlreadySpilledSafe indicates that records have spilled out into disk. It's thread-safe.
// TODO: Use a Rlock to wait spill result when executing spilling in parallel.
// Now using atomic value to avoid potential deadlock.
func (c *RowContainer) AlreadySpilledSafe() bool {
	return atomic.LoadUint32(&c.m.spilled) == 1
}

// NumRow returns the number of rows in the container
func (c *RowContainer) NumRow() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		return c.m.recordsInDisk.Len()
	}
	return c.m.records.Len()
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (c *RowContainer) NumRowsOfChunk(chkID int) int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		return c.m.recordsInDisk.NumRowsOfChunk(chkID)
	}
	return c.m.records.NumRowsOfChunk(chkID)
}

// NumChunks returns the number of chunks in the container.
func (c *RowContainer) NumChunks() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		return c.m.recordsInDisk.NumChunks()
	}
	return c.m.records.NumChunks()
}

// Add appends a chunk into the RowContainer.
func (c *RowContainer) Add(chk *Chunk) (err error) {
	c.m.RLock()
	defer c.m.RUnlock()
	failpoint.Inject("testRowContainerDeadLock", func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(time.Second)
		}
	})
	if c.alreadySpilled() {
		if c.m.spillError != nil {
			return c.m.spillError
		}
		err = c.m.recordsInDisk.Add(chk)
	} else {
		c.m.records.Add(chk)
	}
	return
}

// AllocChunk allocates a new chunk from RowContainer.
func (c *RowContainer) AllocChunk() (chk *Chunk) {
	return c.m.records.allocChunk()
}

// GetChunk returns chkIdx th chunk of in memory records.
func (c *RowContainer) GetChunk(chkIdx int) (*Chunk, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if !c.alreadySpilled() {
		return c.m.records.GetChunk(chkIdx), nil
	}
	if c.m.spillError != nil {
		return nil, c.m.spillError
	}
	return c.m.recordsInDisk.GetChunk(chkIdx)
}

// GetRow returns the row the ptr pointed to.
func (c *RowContainer) GetRow(ptr RowPtr) (Row, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
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
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		atomic.StoreUint32(&c.m.spilled, 0)
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

// ActionSpillForTest returns a SpillDiskAction for spilling over to disk for test.
func (c *RowContainer) ActionSpillForTest() *SpillDiskAction {
	c.actionSpill = &SpillDiskAction{
		c: c,
		testSyncInputFunc: func() {
			c.actionSpill.testWg.Add(1)
		},
		testSyncOutputFunc: func() {
			c.actionSpill.testWg.Done()
		},
	}
	return c.actionSpill
}

// SpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SpillDiskAction.Action is
// triggered.
type SpillDiskAction struct {
	c              *RowContainer
	fallbackAction memory.ActionOnExceed
	m              sync.Mutex
	once           sync.Once

	// test function only used for test sync.
	testSyncInputFunc  func()
	testSyncOutputFunc func()
	testWg             sync.WaitGroup
}

// Action sends a signal to trigger spillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SpillDiskAction) Action(t *memory.Tracker) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.c.AlreadySpilledSafe() {
		if !t.CheckExceed() {
			return
		}
		if a.fallbackAction != nil {
			a.fallbackAction.Action(t)
		}
	} else {
		a.once.Do(func() {
			logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
				zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
			if a.testSyncInputFunc != nil {
				a.testSyncInputFunc()
				c := a.c
				go func() {
					c.SpillToDisk()
					a.testSyncOutputFunc()
				}()
				return
			}
			go a.c.SpillToDisk()
		})
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
	a.once = sync.Once{}
}

// WaitForTest waits all goroutine have gone.
func (a *SpillDiskAction) WaitForTest() {
	a.testWg.Wait()
}

// ErrCannotAddBecauseSorted indicate that the SortedRowContainer is sorted and prohibit inserting data.
var ErrCannotAddBecauseSorted = errors.New("can not add because sorted")

// SortedRowContainer provides a place for many rows, so many that we might want to sort and spill them into disk.
type SortedRowContainer struct {
	*RowContainer
	ptrM struct {
		sync.RWMutex
		// rowPtrs store the chunk index and row index for each row.
		// rowPtrs != nil indicates the pointer is initialized and sorted.
		// It will get an ErrCannotAddBecauseSorted when trying to insert data if rowPtrs != nil.
		rowPtrs []RowPtr
	}

	ByItemsDesc []bool
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []CompareFunc

	actionSpill *SortAndSpillDiskAction
}

// NewSortedRowContainer creates a new SortedRowContainer in memory.
func NewSortedRowContainer(fieldType []*types.FieldType, chunkSize int, ByItemsDesc []bool,
	keyColumns []int, keyCmpFuncs []CompareFunc) *SortedRowContainer {
	return &SortedRowContainer{RowContainer: NewRowContainer(fieldType, chunkSize),
		ByItemsDesc: ByItemsDesc, keyColumns: keyColumns, keyCmpFuncs: keyCmpFuncs}
}

// Close close the SortedRowContainer
func (c *SortedRowContainer) Close() error {
	c.ptrM.Lock()
	defer c.ptrM.Unlock()
	c.GetMemTracker().Consume(int64(-8 * cap(c.ptrM.rowPtrs)))
	c.ptrM.rowPtrs = nil
	return c.RowContainer.Close()
}

func (c *SortedRowContainer) lessRow(rowI, rowJ Row) bool {
	for i, colIdx := range c.keyColumns {
		cmpFunc := c.keyCmpFuncs[i]
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
	return false
}

// keyColumnsLess is the less function for key columns.
func (c *SortedRowContainer) keyColumnsLess(i, j int) bool {
	rowI := c.m.records.GetRow(c.ptrM.rowPtrs[i])
	rowJ := c.m.records.GetRow(c.ptrM.rowPtrs[j])
	return c.lessRow(rowI, rowJ)
}

// Sort inits pointers and sorts the records.
func (c *SortedRowContainer) Sort() {
	c.ptrM.Lock()
	defer c.ptrM.Unlock()
	if c.ptrM.rowPtrs != nil {
		return
	}
	c.ptrM.rowPtrs = make([]RowPtr, 0, c.NumRow())
	for chkIdx := 0; chkIdx < c.NumChunks(); chkIdx++ {
		rowChk, err := c.GetChunk(chkIdx)
		// err must be nil, because the chunk is in memory.
		if err != nil {
			panic(err)
		}
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			c.ptrM.rowPtrs = append(c.ptrM.rowPtrs, RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
	sort.Slice(c.ptrM.rowPtrs, c.keyColumnsLess)
	c.GetMemTracker().Consume(int64(8 * c.numRow))
}

func (c *SortedRowContainer) sortAndSpillToDisk() {
	c.Sort()
	c.RowContainer.SpillToDisk()
	return
}

// Add appends a chunk into the SortedRowContainer.
func (c *SortedRowContainer) Add(chk *Chunk) (err error) {
	c.ptrM.RLock()
	defer c.ptrM.RUnlock()
	if c.ptrM.rowPtrs != nil {
		return ErrCannotAddBecauseSorted
	}
	return c.RowContainer.Add(chk)
}

// GetSortedRow returns the row the idx pointed to.
func (c *SortedRowContainer) GetSortedRow(idx int) (Row, error) {
	c.ptrM.RLock()
	defer c.ptrM.RUnlock()
	ptr := c.ptrM.rowPtrs[idx]
	return c.RowContainer.GetRow(ptr)
}

// ActionSpill returns a SortAndSpillDiskAction for sorting and spilling over to disk.
func (c *SortedRowContainer) ActionSpill() *SortAndSpillDiskAction {
	c.actionSpill = &SortAndSpillDiskAction{c: c}
	return c.actionSpill
}

// ActionSpillForTest returns a SortAndSpillDiskAction for sorting and spilling over to disk for test.
func (c *SortedRowContainer) ActionSpillForTest() *SortAndSpillDiskAction {
	c.actionSpill = &SortAndSpillDiskAction{
		c: c,
		testSyncInputFunc: func() {
			c.actionSpill.testWg.Add(1)
		},
		testSyncOutputFunc: func() {
			c.actionSpill.testWg.Done()
		},
	}
	return c.actionSpill
}

// SortAndSpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SortAndSpillDiskAction.Action is
// triggered.
type SortAndSpillDiskAction struct {
	c              *SortedRowContainer
	fallbackAction memory.ActionOnExceed
	m              sync.Mutex
	once           sync.Once

	// test function only used for test sync.
	testSyncInputFunc  func()
	testSyncOutputFunc func()
	testWg             sync.WaitGroup
}

// Action sends a signal to trigger sortAndSpillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SortAndSpillDiskAction) Action(t *memory.Tracker) {
	a.m.Lock()
	defer a.m.Unlock()
	// Guarantee that each partition size is at least 10% of the threshold, to avoid opening too many files.
	if a.c.AlreadySpilledSafe() || a.c.GetMemTracker().BytesConsumed() <= t.GetBytesLimit()/10 {
		if !t.CheckExceed() {
			return
		}
		if a.fallbackAction != nil {
			a.fallbackAction.Action(t)
		}
	} else {
		a.once.Do(func() {
			logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
				zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
			if a.testSyncInputFunc != nil {
				a.testSyncInputFunc()
				c := a.c
				go func() {
					c.sortAndSpillToDisk()
					a.testSyncOutputFunc()
				}()
				return
			}
			go a.c.sortAndSpillToDisk()
		})
	}
}

// SetFallback sets the fallback action.
func (a *SortAndSpillDiskAction) SetFallback(fallback memory.ActionOnExceed) {
	a.fallbackAction = fallback
}

// SetLogHook sets the hook, it does nothing just to form the memory.ActionOnExceed interface.
func (a *SortAndSpillDiskAction) SetLogHook(hook func(uint64)) {}

// ResetRowContainer resets the spill action and sets the SortedRowContainer for the SortAndSpillDiskAction.
func (a *SortAndSpillDiskAction) ResetRowContainer(c *SortedRowContainer) {
	a.m.Lock()
	defer a.m.Unlock()
	a.c = c
	a.once = sync.Once{}
}

// WaitForTest waits all goroutine have gone.
func (a *SortAndSpillDiskAction) WaitForTest() {
	a.testWg.Wait()
}
