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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
	"golang.org/x/sys/cpu"
)

// ErrCannotAddBecauseSorted indicate that the SortPartition is sorted and prohibit inserting data.
var ErrCannotAddBecauseSorted = errors.New("can not add because sorted")

type rowContainerRecord struct {
	inMemory *List
	inDisk   *DataInDiskByRows
	// spillError stores the error when spilling.
	spillError error
}

type mutexForRowContainer struct {
	// Add cache padding to avoid false sharing issue.
	_ cpu.CacheLinePad
	// RWMutex guarantees spill and get operator for rowContainer is mutually exclusive.
	// `rLock` and `wLocks` is introduced to reduce the contention when multiple
	// goroutine touch the same rowContainer concurrently. If there are multiple
	// goroutines touch the same rowContainer concurrently, it's recommended to
	// use RowContainer.ShallowCopyWithNewMutex to build a new RowContainer for
	// each goroutine. Thus each goroutine holds its own rLock but share the same
	// underlying data, which can reduce the contention on m.rLock remarkably and
	// get better performance.
	rLock   sync.RWMutex
	wLocks  []*sync.RWMutex
	records *rowContainerRecord
	_       cpu.CacheLinePad
}

// Lock locks rw for writing.
func (m *mutexForRowContainer) Lock() {
	for _, l := range m.wLocks {
		l.Lock()
	}
}

// Unlock unlocks rw for writing.
func (m *mutexForRowContainer) Unlock() {
	for _, l := range m.wLocks {
		l.Unlock()
	}
}

// RLock locks rw for reading.
func (m *mutexForRowContainer) RLock() {
	m.rLock.RLock()
}

// RUnlock undoes a single RLock call.
func (m *mutexForRowContainer) RUnlock() {
	m.rLock.RUnlock()
}

type spillHelper interface {
	SpillToDisk()
	hasEnoughDataToSpill(t *memory.Tracker) bool
}

// RowContainer provides a place for many rows, so many that we might want to spill them into disk.
// nolint:structcheck
type RowContainer struct {
	m *mutexForRowContainer

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	actionSpill *SpillDiskAction
}

// NewRowContainer creates a new RowContainer in memory.
func NewRowContainer(fieldType []*types.FieldType, chunkSize int) *RowContainer {
	li := NewList(fieldType, chunkSize, chunkSize)
	rc := &RowContainer{
		m: &mutexForRowContainer{
			records: &rowContainerRecord{inMemory: li},
			rLock:   sync.RWMutex{},
			wLocks:  []*sync.RWMutex{},
		},
		memTracker:  memory.NewTracker(memory.LabelForRowContainer, -1),
		diskTracker: disk.NewTracker(memory.LabelForRowContainer, -1),
	}
	rc.m.wLocks = append(rc.m.wLocks, &rc.m.rLock)
	li.GetMemTracker().AttachTo(rc.GetMemTracker())
	return rc
}

// ShallowCopyWithNewMutex shallow clones a RowContainer.
// The new RowContainer shares the same underlying data with the old one but
// holds an individual rLock.
func (c *RowContainer) ShallowCopyWithNewMutex() *RowContainer {
	newRC := *c
	newRC.m = &mutexForRowContainer{
		records: c.m.records,
		rLock:   sync.RWMutex{},
		wLocks:  []*sync.RWMutex{},
	}
	c.m.wLocks = append(c.m.wLocks, &newRC.m.rLock)
	return &newRC
}

// SpillToDisk spills data to disk. This function may be called in parallel.
func (c *RowContainer) SpillToDisk() {
	c.spillToDisk(nil)
}

func (*RowContainer) hasEnoughDataToSpill(_ *memory.Tracker) bool {
	return true
}

func (c *RowContainer) spillToDisk(preSpillError error) {
	c.m.Lock()
	defer c.m.Unlock()
	if c.alreadySpilled() {
		return
	}
	// c.actionSpill may be nil when testing SpillToDisk directly.
	if c.actionSpill != nil {
		if c.actionSpill.getStatus() == spilledYet {
			// The rowContainer has been closed.
			return
		}
		c.actionSpill.setStatus(spilling)
		defer c.actionSpill.cond.Broadcast()
		defer c.actionSpill.setStatus(spilledYet)
	}
	var err error
	memory.QueryForceDisk.Add(1)
	n := c.m.records.inMemory.NumChunks()
	c.m.records.inDisk = NewDataInDiskByRows(c.m.records.inMemory.FieldTypes())
	c.m.records.inDisk.diskTracker.AttachTo(c.diskTracker)
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("%v", r)
			c.m.records.spillError = err
			logutil.BgLogger().Error("spill to disk failed", zap.Stack("stack"), zap.Error(err))
		}
	}()
	failpoint.Inject("spillToDiskOutOfDiskQuota", func(val failpoint.Value) {
		if val.(bool) {
			panic("out of disk quota when spilling")
		}
	})
	if preSpillError != nil {
		c.m.records.spillError = preSpillError
		return
	}
	for i := 0; i < n; i++ {
		chk := c.m.records.inMemory.GetChunk(i)
		err = c.m.records.inDisk.Add(chk)
		if err != nil {
			c.m.records.spillError = err
			return
		}
		c.m.records.inMemory.GetMemTracker().HandleKillSignal()
	}
	c.m.records.inMemory.Clear()
}

// Reset resets RowContainer.
func (c *RowContainer) Reset() error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.alreadySpilled() {
		err := c.m.records.inDisk.Close()
		c.m.records.inDisk = nil
		if err != nil {
			return err
		}
		c.actionSpill.Reset()
	} else {
		c.m.records.inMemory.Reset()
	}
	return nil
}

// alreadySpilled indicates that records have spilled out into disk.
func (c *RowContainer) alreadySpilled() bool {
	return c.m.records.inDisk != nil
}

// AlreadySpilledSafeForTest indicates that records have spilled out into disk. It's thread-safe.
// The function is only used for test.
func (c *RowContainer) AlreadySpilledSafeForTest() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.m.records.inDisk != nil
}

// NumRow returns the number of rows in the container
func (c *RowContainer) NumRow() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		return c.m.records.inDisk.Len()
	}
	return c.m.records.inMemory.Len()
}

// NumRowsOfChunk returns the number of rows of a chunk in the DataInDiskByRows.
func (c *RowContainer) NumRowsOfChunk(chkID int) int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		return c.m.records.inDisk.NumRowsOfChunk(chkID)
	}
	return c.m.records.inMemory.NumRowsOfChunk(chkID)
}

// NumChunks returns the number of chunks in the container.
func (c *RowContainer) NumChunks() int {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		return c.m.records.inDisk.NumChunks()
	}
	return c.m.records.inMemory.NumChunks()
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
		if err := c.m.records.spillError; err != nil {
			return err
		}
		err = c.m.records.inDisk.Add(chk)
	} else {
		c.m.records.inMemory.Add(chk)
	}
	return
}

// AllocChunk allocates a new chunk from RowContainer.
func (c *RowContainer) AllocChunk() (chk *Chunk) {
	return c.m.records.inMemory.allocChunk()
}

// GetChunk returns chkIdx th chunk of in memory records.
func (c *RowContainer) GetChunk(chkIdx int) (*Chunk, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if !c.alreadySpilled() {
		return c.m.records.inMemory.GetChunk(chkIdx), nil
	}
	if err := c.m.records.spillError; err != nil {
		return nil, err
	}
	return c.m.records.inDisk.GetChunk(chkIdx)
}

// GetRow returns the row the ptr pointed to.
func (c *RowContainer) GetRow(ptr RowPtr) (row Row, err error) {
	row, _, err = c.GetRowAndAppendToChunkIfInDisk(ptr, nil)
	return row, err
}

// GetRowAndAppendToChunkIfInDisk gets a Row from the RowContainer by RowPtr. If the container has spilled, the row will
// be appended to the chunk. It'll return `nil` chunk if the container hasn't spilled, or it returns an error.
func (c *RowContainer) GetRowAndAppendToChunkIfInDisk(ptr RowPtr, chk *Chunk) (row Row, _ *Chunk, err error) {
	c.m.RLock()
	defer c.m.RUnlock()
	if c.alreadySpilled() {
		if err := c.m.records.spillError; err != nil {
			return Row{}, nil, err
		}
		return c.m.records.inDisk.GetRowAndAppendToChunk(ptr, chk)
	}
	return c.m.records.inMemory.GetRow(ptr), nil, nil
}

// GetRowAndAlwaysAppendToChunk gets a Row from the RowContainer by RowPtr. Unlike `GetRowAndAppendToChunkIfInDisk`, this
// function always appends the row to the chunk, without considering whether it has spilled.
// It'll return `nil` chunk if it returns an error, or the chunk will be the same with the argument.
func (c *RowContainer) GetRowAndAlwaysAppendToChunk(ptr RowPtr, chk *Chunk) (row Row, _ *Chunk, err error) {
	row, retChk, err := c.GetRowAndAppendToChunkIfInDisk(ptr, chk)
	if err != nil {
		return row, nil, err
	}

	if retChk == nil {
		// The container hasn't spilled, and the row is not appended to the chunk, so append the chunk explicitly here
		chk.AppendRow(row)
	}

	return row, chk, nil
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
	if c.actionSpill != nil {
		// Set status to spilledYet to avoid spilling.
		c.actionSpill.setStatus(spilledYet)
		c.actionSpill.cond.Broadcast()
		c.actionSpill.SetFinished()
	}
	if c.alreadySpilled() {
		err = c.m.records.inDisk.Close()
		c.m.records.inDisk = nil
	}
	c.m.records.inMemory.Clear()
	return
}

// ActionSpill returns a SpillDiskAction for spilling over to disk.
func (c *RowContainer) ActionSpill() *SpillDiskAction {
	if c.actionSpill == nil {
		c.actionSpill = &SpillDiskAction{
			c:                   c,
			baseSpillDiskAction: &baseSpillDiskAction{cond: spillStatusCond{sync.NewCond(new(sync.Mutex)), notSpilled}},
		}
	}
	return c.actionSpill
}

// ActionSpillForTest returns a SpillDiskAction for spilling over to disk for test.
func (c *RowContainer) ActionSpillForTest() *SpillDiskAction {
	c.actionSpill = &SpillDiskAction{
		c: c,
		baseSpillDiskAction: &baseSpillDiskAction{
			testSyncInputFunc: func() {
				c.actionSpill.testWg.Add(1)
			},
			testSyncOutputFunc: func() {
				c.actionSpill.testWg.Done()
			},
			cond: spillStatusCond{sync.NewCond(new(sync.Mutex)), notSpilled},
		},
	}
	return c.actionSpill
}

type baseSpillDiskAction struct {
	memory.BaseOOMAction
	m    sync.Mutex
	once sync.Once
	cond spillStatusCond

	// test function only used for test sync.
	testSyncInputFunc  func()
	testSyncOutputFunc func()
	testWg             sync.WaitGroup
}

// SpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SpillDiskAction.Action is
// triggered.
type SpillDiskAction struct {
	c *RowContainer
	*baseSpillDiskAction
}

// Action sends a signal to trigger spillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SpillDiskAction) Action(t *memory.Tracker) {
	a.action(t, a.c)
}

type spillStatusCond struct {
	*sync.Cond
	// status indicates different stages for the Action
	// notSpilled indicates the rowContainer is not spilled.
	// spilling indicates the rowContainer is spilling.
	// spilledYet indicates thr rowContainer is spilled.
	status spillStatus
}

type spillStatus uint32

const (
	notSpilled spillStatus = iota
	spilling
	spilledYet
)

func (a *baseSpillDiskAction) setStatus(status spillStatus) {
	a.cond.L.Lock()
	defer a.cond.L.Unlock()
	a.cond.status = status
}

func (a *baseSpillDiskAction) getStatus() spillStatus {
	a.cond.L.Lock()
	defer a.cond.L.Unlock()
	return a.cond.status
}

func (a *baseSpillDiskAction) action(t *memory.Tracker, spillHelper spillHelper) {
	a.m.Lock()
	defer a.m.Unlock()

	if a.getStatus() == notSpilled && spillHelper.hasEnoughDataToSpill(t) {
		a.once.Do(func() {
			logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
				zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
			if a.testSyncInputFunc != nil {
				a.testSyncInputFunc()
				go func() {
					spillHelper.SpillToDisk()
					a.testSyncOutputFunc()
				}()
				return
			}
			go spillHelper.SpillToDisk()
		})
		return
	}

	a.cond.L.Lock()
	for a.cond.status == spilling {
		a.cond.Wait()
	}
	a.cond.L.Unlock()

	if !t.CheckExceed() {
		return
	}
	if fallback := a.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// Reset resets the status for SpillDiskAction.
func (a *baseSpillDiskAction) Reset() {
	a.m.Lock()
	defer a.m.Unlock()
	a.setStatus(notSpilled)
	a.once = sync.Once{}
}

// GetPriority get the priority of the Action.
func (*baseSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

// WaitForTest waits all goroutine have gone.
func (a *baseSpillDiskAction) WaitForTest() {
	a.testWg.Wait()
}

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
	memTracker  *memory.Tracker

	// Sort is a time-consuming operation, we need to set a checkpoint to detect
	// the outside signal periodically.
	timesOfRowCompare uint
}

// NewSortedRowContainer creates a new SortedRowContainer in memory.
func NewSortedRowContainer(fieldType []*types.FieldType, chunkSize int, byItemsDesc []bool,
	keyColumns []int, keyCmpFuncs []CompareFunc) *SortedRowContainer {
	src := SortedRowContainer{RowContainer: NewRowContainer(fieldType, chunkSize),
		ByItemsDesc: byItemsDesc, keyColumns: keyColumns, keyCmpFuncs: keyCmpFuncs}
	src.memTracker = memory.NewTracker(memory.LabelForRowContainer, -1)
	src.RowContainer.GetMemTracker().AttachTo(src.GetMemTracker())
	return &src
}

// Close close the SortedRowContainer
func (c *SortedRowContainer) Close() error {
	c.ptrM.Lock()
	defer c.ptrM.Unlock()
	c.GetMemTracker().Consume(int64(-8 * c.NumRow()))
	c.ptrM.rowPtrs = nil
	return c.RowContainer.Close()
}

func (c *SortedRowContainer) lessRow(rowI, rowJ Row) bool {
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
func (c *SortedRowContainer) keyColumnsLess(i, j int) bool {
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
	rowI := c.m.records.inMemory.GetRow(c.ptrM.rowPtrs[i])
	rowJ := c.m.records.inMemory.GetRow(c.ptrM.rowPtrs[j])
	return c.lessRow(rowI, rowJ)
}

// Sort inits pointers and sorts the records.
func (c *SortedRowContainer) Sort() (ret error) {
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
	if c.ptrM.rowPtrs != nil {
		return
	}
	c.ptrM.rowPtrs = make([]RowPtr, 0, c.NumRow()) // The memory usage has been tracked in SortedRowContainer.Add() function
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
	failpoint.Inject("errorDuringSortRowContainer", func(val failpoint.Value) {
		if val.(bool) {
			panic("sort meet error")
		}
	})
	sort.Slice(c.ptrM.rowPtrs, c.keyColumnsLess)
	return
}

// SpillToDisk spills data to disk. This function may be called in parallel.
func (c *SortedRowContainer) SpillToDisk() {
	err := c.Sort()
	c.RowContainer.spillToDisk(err)
}

func (c *SortedRowContainer) hasEnoughDataToSpill(t *memory.Tracker) bool {
	// Guarantee that each partition size is at least 10% of the threshold, to avoid opening too many files.
	return c.GetMemTracker().BytesConsumed() > t.GetBytesLimit()/10
}

// Add appends a chunk into the SortedRowContainer.
func (c *SortedRowContainer) Add(chk *Chunk) (err error) {
	c.ptrM.RLock()
	defer c.ptrM.RUnlock()
	if c.ptrM.rowPtrs != nil {
		return ErrCannotAddBecauseSorted
	}
	// Consume the memory usage of rowPtrs in advance
	c.GetMemTracker().Consume(int64(chk.NumRows() * 8))
	return c.RowContainer.Add(chk)
}

// GetSortedRow returns the row the idx pointed to.
func (c *SortedRowContainer) GetSortedRow(idx int) (Row, error) {
	c.ptrM.RLock()
	defer c.ptrM.RUnlock()
	ptr := c.ptrM.rowPtrs[idx]
	return c.RowContainer.GetRow(ptr)
}

// GetSortedRowAndAlwaysAppendToChunk returns the row the idx pointed to.
func (c *SortedRowContainer) GetSortedRowAndAlwaysAppendToChunk(idx int, chk *Chunk) (Row, *Chunk, error) {
	c.ptrM.RLock()
	defer c.ptrM.RUnlock()
	ptr := c.ptrM.rowPtrs[idx]
	return c.RowContainer.GetRowAndAlwaysAppendToChunk(ptr, chk)
}

// ActionSpill returns a SortAndSpillDiskAction for sorting and spilling over to disk.
func (c *SortedRowContainer) ActionSpill() *SortAndSpillDiskAction {
	if c.actionSpill == nil {
		c.actionSpill = &SortAndSpillDiskAction{
			c:                   c,
			baseSpillDiskAction: c.RowContainer.ActionSpill().baseSpillDiskAction,
		}
	}
	return c.actionSpill
}

// ActionSpillForTest returns a SortAndSpillDiskAction for sorting and spilling over to disk for test.
func (c *SortedRowContainer) ActionSpillForTest() *SortAndSpillDiskAction {
	c.actionSpill = &SortAndSpillDiskAction{
		c:                   c,
		baseSpillDiskAction: c.RowContainer.ActionSpillForTest().baseSpillDiskAction,
	}
	return c.actionSpill
}

// GetMemTracker return the memory tracker for the sortedRowContainer
func (c *SortedRowContainer) GetMemTracker() *memory.Tracker {
	return c.memTracker
}

// SortAndSpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, SortAndSpillDiskAction.Action is
// triggered.
type SortAndSpillDiskAction struct {
	c *SortedRowContainer
	*baseSpillDiskAction
}

// Action sends a signal to trigger sortAndSpillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *SortAndSpillDiskAction) Action(t *memory.Tracker) {
	a.action(t, a.c)
}

// WaitForTest waits all goroutine have gone.
func (a *SortAndSpillDiskAction) WaitForTest() {
	a.testWg.Wait()
}
