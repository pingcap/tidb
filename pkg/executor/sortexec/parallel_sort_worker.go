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
	"container/list"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"golang.org/x/exp/slices"
)

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 20000

type parallelSortWorker struct {
	// This queue stores rows that are received by the worker itself
	sortedRowsQueue list.List
	// This list is shared by all workers and all workers will put their sorted rows into this list
	publicSortedRows *sortedRowsList
	// Temporarily store rows that will be sorted.
	rowBuffer sortedRows
	result    *sortedRows

	waitGroup *sync.WaitGroup

	mpmcQueue *chunk.MPMCQueue

	checkError   func() error
	processError func(error)

	lessRowFunc       func(chunk.Row, chunk.Row) int
	timesOfRowCompare uint

	memTracker       *memory.Tracker
	totalMemoryUsage int64

	spillHelper *parallelSortSpillHelper
}

func newParallelSortWorker(
	lessRowFunc func(chunk.Row, chunk.Row) int,
	publicSortedRows *sortedRowsList,
	waitGroup *sync.WaitGroup,
	result *sortedRows,
	mpmcQueue *chunk.MPMCQueue,
	checkError func() error,
	processError func(error),
	memTracker *memory.Tracker,
	spillHelper *parallelSortSpillHelper) *parallelSortWorker {
	return &parallelSortWorker{
		lessRowFunc:       lessRowFunc,
		publicSortedRows:  publicSortedRows,
		waitGroup:         waitGroup,
		result:            result,
		mpmcQueue:         mpmcQueue,
		checkError:        checkError,
		processError:      processError,
		totalMemoryUsage:  0,
		timesOfRowCompare: 0,
		memTracker:        memTracker,
		spillHelper:       spillHelper,
	}
}

func (p *parallelSortWorker) fetchFromMPMCQueueAndSort() bool {
	for {
		eof, err := p.fetchFromMPMCQueueAndSortImpl()
		if err != nil {
			return false
		}

		if eof {
			return true
		}
	}
}

// Fetching chunks from MPMCQueue and sort them.
// Rows are sorted only inside a chunk.
func (p *parallelSortWorker) fetchFromMPMCQueueAndSortImpl() (bool, error) {
	p.spillHelper.syncLock.RLock()
	for p.spillHelper.isInSpilling() {
		p.spillHelper.syncLock.RUnlock()
		// Repeatedly release lock so that spill action has more chance to get write lock
		time.Sleep(10 * time.Millisecond)
		p.spillHelper.syncLock.RLock()
	}
	defer p.spillHelper.syncLock.RUnlock()

	err := p.checkError()
	if err != nil {
		return true, err
	}

	// Memory usage of the chunk has been tracked at the producer side.
	chk, res := p.mpmcQueue.Pop()
	if res != chunk.OK {
		// Check if the queue is closed by an error.
		err := p.checkError()
		return err == nil, err
	}

	p.totalMemoryUsage += chk.MemoryUsage

	sortedRows := p.sortChunkAndGetSortedRows(chk.Chk)
	pushIntoList(&p.sortedRowsQueue, sortedRows)
	return false, nil
}

func (p *parallelSortWorker) keyColumnsLess(i, j chunk.Row) int {
	if p.timesOfRowCompare >= SignalCheckpointForSort {
		// Trigger Consume for checking the NeedKill signal
		p.memTracker.Consume(1)
		p.timesOfRowCompare = 0
	}
	// TODO add test with this failpoint
	// failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
	// 	if val.(bool) {
	// 		c.timesOfRowCompare += 1024
	// 	}
	// })
	p.timesOfRowCompare++

	return p.lessRowFunc(i, j)
}

func (p *parallelSortWorker) sortChunkAndGetSortedRows(chk *chunk.Chunk) sortedRows {
	rowNum := chk.NumRows()
	p.rowBuffer = make(sortedRows, rowNum)
	for i := 0; i < rowNum; i++ {
		p.rowBuffer[i] = chk.GetRow(i)
	}
	slices.SortFunc(p.rowBuffer, p.keyColumnsLess)
	return p.rowBuffer
}

func (p *parallelSortWorker) mergeSortLocalRows(calledBySpillAction bool) bool {
	for p.sortedRowsQueue.Len() > 1 {
		hasError := p.mergeSortLocalRowsImpl(calledBySpillAction)
		if hasError {
			return false
		}
	}
	return true
}

// Sort data that received by itself. At last, length of sortedRowsQueue should not be greater than 1.
// Return false if there is some error.
func (p *parallelSortWorker) mergeSortLocalRowsImpl(calledBySpillAction bool) bool {
	if !calledBySpillAction {
		p.spillHelper.syncLock.RLock()

		for p.spillHelper.isInSpilling() {
			p.spillHelper.syncLock.RUnlock()
			// Repeatedly release lock so that spill action has more chance to get write lock
			time.Sleep(10 * time.Millisecond)
			p.spillHelper.syncLock.RLock()
		}
		defer p.spillHelper.syncLock.RUnlock()
	}

	err := p.checkError()
	if err != nil {
		return true
	}

	sortedRows1 := popFromList(&p.sortedRowsQueue)
	sortedRows2 := popFromList(&p.sortedRowsQueue)
	mergedSortedRows := p.mergeTwoSortedRows(sortedRows1, sortedRows2)
	pushIntoList(&p.sortedRowsQueue, mergedSortedRows)
	return false
}

// The worker will check the length of sharedSortedRows. If length > 0, it will fetch a sortedRows and merge it
// with worker's own sorted rows. If length == 0, worker will put it's row into sharedSortedRows and leave.
func (p *parallelSortWorker) mergeSortAllRows(calledBySpillAction bool) {
	continueExecute := true
	for continueExecute {
		continueExecute = p.mergeSortAllRowsImpl(calledBySpillAction)
	}
}

func (p *parallelSortWorker) mergeSortAllRowsImpl(calledBySpillAction bool) bool {
	if !calledBySpillAction {
		p.spillHelper.syncLock.RLock()

		for p.spillHelper.isInSpilling() {
			p.spillHelper.syncLock.RUnlock()
			// Repeatedly release lock so that spill action has more chance to get write lock
			time.Sleep(10 * time.Millisecond)
			p.spillHelper.syncLock.RLock()
		}
		defer p.spillHelper.syncLock.RUnlock()
	}

	err := p.checkError()
	if err != nil {
		return false
	}

	localSortedRows := popFromList(&p.sortedRowsQueue)
	fetchedSortedRows := p.publicSortedRows.fetchOrPutSortedRows(localSortedRows)
	if fetchedSortedRows == nil {
		return false
	}

	mergedSortedRows := p.mergeTwoSortedRows(localSortedRows, fetchedSortedRows)
	pushIntoList(&p.sortedRowsQueue, mergedSortedRows)
	return true
}

func (p *parallelSortWorker) mergeTwoSortedRows(sortedRows1 sortedRows, sortedRows2 sortedRows) sortedRows {
	sortedRows1Len := len(sortedRows1)
	sortedRows2Len := len(sortedRows2)
	mergedSortedRows := make(sortedRows, 0, sortedRows1Len+sortedRows2Len)
	cursor1 := 0 // Point to sortedRows1
	cursor2 := 0 // Point to sortedRows2

	// Merge
	for cursor1 < sortedRows1Len && cursor2 < sortedRows2Len {
		if p.timesOfRowCompare >= SignalCheckpointForSort {
			// Trigger Consume for checking the NeedKill signal
			p.memTracker.Consume(1)
			p.timesOfRowCompare = 0
		}

		p.timesOfRowCompare++

		if p.lessRowFunc(sortedRows1[cursor1], sortedRows2[cursor2]) < 0 {
			mergedSortedRows = append(mergedSortedRows, sortedRows1[cursor1])
			cursor1++
		} else {
			mergedSortedRows = append(mergedSortedRows, sortedRows2[cursor2])
			cursor2++
		}
	}

	// Append the remaining rows
	mergedSortedRows = append(mergedSortedRows, sortedRows1[cursor1:]...)
	mergedSortedRows = append(mergedSortedRows, sortedRows2[cursor2:]...)

	return mergedSortedRows
}

func (p *parallelSortWorker) run() {
	defer func() {
		if r := recover(); r != nil {
			processErrorAndLog(p.processError, r)
		}
		p.waitGroup.Done()
	}()

	ok := p.fetchFromMPMCQueueAndSort()
	if !ok {
		return
	}

	ok = p.mergeSortLocalRows(false)
	if !ok {
		return
	}

	p.mergeSortAllRows(false)
}
