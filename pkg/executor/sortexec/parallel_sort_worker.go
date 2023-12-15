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
	"sort"
	"sync"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 20000

type parallelSortWorker struct {
	sortedRowsQueue list.List // Type is sortedRows

	lessRowFunc func(chunk.Row, chunk.Row) bool

	publicSpace *publicMergeSpace

	waitGroup *sync.WaitGroup
	result    *sortedRows

	mpmcQueue    *chunk.MPMCQueue
	checkError   func() error
	processError func(error)

	totalMemoryUsage  int64
	timesOfRowCompare uint

	memTracker *memory.Tracker

	// Temporarily store rows that will be sorted.
	rowBuffer sortedRows
}

func newParallelSortWorker(
	lessRowFunc func(chunk.Row, chunk.Row) bool,
	publicSpace *publicMergeSpace,
	waitGroup *sync.WaitGroup,
	result *sortedRows,
	mpmcQueue *chunk.MPMCQueue,
	checkError func() error,
	processError func(error),
	memTracker *memory.Tracker) *parallelSortWorker {
	return &parallelSortWorker{
		lessRowFunc:       lessRowFunc,
		publicSpace:       publicSpace,
		waitGroup:         waitGroup,
		result:            result,
		mpmcQueue:         mpmcQueue,
		checkError:        checkError,
		processError:      processError,
		totalMemoryUsage:  0,
		timesOfRowCompare: 0,
		memTracker:        memTracker,
	}
}

// Fetching chunks from MPMCQueue and sort them.
// Rows are sorted only inside a chunk.
// Return false if there is some error.
func (p *parallelSortWorker) fetchFromMPMCQueueAndSort() bool {
	for {
		err := p.checkError()
		if err != nil {
			return false
		}

		// Memory usage of the chunk has been tracked at the producer side.
		chk, res := p.mpmcQueue.Pop()
		if res != chunk.OK {
			// Check if the queue is closed by an error.
			err := p.checkError()
			return err == nil
		}

		p.totalMemoryUsage += chk.MemoryUsage

		sortedRows := p.sortChunkAndGetSortedRows(chk.Chk)
		pushIntoList(&p.sortedRowsQueue, sortedRows)
	}
}

func (p *parallelSortWorker) keyColumnsLess(i, j int) bool {
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

	return p.lessRowFunc(p.rowBuffer[i], p.rowBuffer[j])
}

func (p *parallelSortWorker) sortChunkAndGetSortedRows(chk *chunk.Chunk) sortedRows {
	rowNum := chk.NumRows()
	p.rowBuffer = make(sortedRows, rowNum)
	for i := 0; i < rowNum; i++ {
		p.rowBuffer[i] = chk.GetRow(i)
	}
	sort.Slice(p.rowBuffer, p.keyColumnsLess)
	return p.rowBuffer
}

// Sort data that received by itself. At last, length of sortedRowsQueue should not be greater than 1.
// Return false if there is some error.
func (p *parallelSortWorker) mergeSortLocalData() bool {
	for p.sortedRowsQueue.Len() > 1 {
		err := p.checkError()
		if err != nil {
			return false
		}

		sortedRows1 := popFromList(&p.sortedRowsQueue)
		sortedRows2 := popFromList(&p.sortedRowsQueue)
		mergedSortedRows := p.mergeTwoSortedRows(sortedRows1, sortedRows2)
		pushIntoList(&p.sortedRowsQueue, mergedSortedRows)
	}
	return true
}

// The worker will check the length of sharedSortedRows. If length > 0, it will fetch a sortedRows and merge it
// with worker's own sorted rows. If length == 0, worker will put it's row into sharedSortedRows and leave.
func (p *parallelSortWorker) mergeSortPublicData() {
	for {
		err := p.checkError()
		if err != nil {
			return
		}

		localSortedRows := popFromList(&p.sortedRowsQueue)
		fetchedSortedRows := p.publicSpace.fetchOrPutSortedRows(localSortedRows)
		if fetchedSortedRows == nil {
			break
		}

		mergedSortedRows := p.mergeTwoSortedRows(localSortedRows, fetchedSortedRows)
		pushIntoList(&p.sortedRowsQueue, mergedSortedRows)
	}
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

		if p.lessRowFunc(sortedRows1[cursor1], sortedRows2[cursor2]) {
			mergedSortedRows = append(mergedSortedRows, sortedRows1[cursor1])
			cursor1++
		} else {
			mergedSortedRows = append(mergedSortedRows, sortedRows2[cursor2])
			cursor2++
		}
	}

	// Append the remaining rows
	for cursor1 < sortedRows1Len {
		mergedSortedRows = append(mergedSortedRows, sortedRows1[cursor1])
		cursor1++
	}

	// Append the remaining rows
	for cursor2 < sortedRows2Len {
		mergedSortedRows = append(mergedSortedRows, sortedRows2[cursor2])
		cursor2++
	}

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

	ok = p.mergeSortLocalData()
	if !ok {
		return
	}

	p.mergeSortPublicData()
}
