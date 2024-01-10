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
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"golang.org/x/exp/slices"
)

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 20000

type parallelSortWorker struct {
	// This list is shared by all workers and all workers will put their sorted rows into this list
	globalSortedRowsQueue *sortedRowsList
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
	globalSortedRowsQueue *sortedRowsList,
	waitGroup *sync.WaitGroup,
	result *sortedRows,
	mpmcQueue *chunk.MPMCQueue,
	checkError func() error,
	processError func(error),
	memTracker *memory.Tracker,
	spillHelper *parallelSortSpillHelper) *parallelSortWorker {
	return &parallelSortWorker{
		lessRowFunc:           lessRowFunc,
		globalSortedRowsQueue: globalSortedRowsQueue,
		waitGroup:             waitGroup,
		result:                result,
		mpmcQueue:             mpmcQueue,
		checkError:            checkError,
		processError:          processError,
		totalMemoryUsage:      0,
		timesOfRowCompare:     0,
		memTracker:            memTracker,
		spillHelper:           spillHelper,
	}
}

func (p *parallelSortWorker) fetchChunksAndSort() bool {
	for {
		eof, err := p.fetchChunksAndSortImpl()
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
func (p *parallelSortWorker) fetchChunksAndSortImpl() (bool, error) {
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
	p.globalSortedRowsQueue.add(sortedRows)
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

func (p *parallelSortWorker) mergeSortGlobalRows() {
	mergedSortedRows := p.initMergeGlobalRows()
	for mergedSortedRows != nil {
		mergedSortedRows = p.mergeSortGlobalRowsImpl(mergedSortedRows)
	}
}

func (p *parallelSortWorker) initMergeGlobalRows() sortedRows {
	p.spillHelper.syncLock.RLock()
	defer p.spillHelper.syncLock.RUnlock()
	sortedRowsLeft, sortedRowsRight := p.globalSortedRowsQueue.fetchTwoSortedRows()
	return p.mergeTwoSortedRows(sortedRowsLeft, sortedRowsRight)
}

func (p *parallelSortWorker) mergeSortGlobalRowsImpl(mergedSortedRows sortedRows) sortedRows {
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
		return nil
	}

	sortedRowsLeft, sortedRowsRight := p.globalSortedRowsQueue.addAndFetchTwoSortedRows(mergedSortedRows)
	if sortedRowsLeft == nil {
		return nil
	}

	return p.mergeTwoSortedRows(sortedRowsLeft, sortedRowsRight)
}

func (p *parallelSortWorker) mergeTwoSortedRows(sortedRowsLeft sortedRows, sortedRowsRight sortedRows) sortedRows {
	sortedRowsLeftLen := len(sortedRowsLeft)
	sortedRowsRightLen := len(sortedRowsRight)
	mergedSortedRows := make(sortedRows, 0, sortedRowsLeftLen+sortedRowsRightLen)
	cursor1 := 0 // Point to sortedRows1
	cursor2 := 0 // Point to sortedRows2

	// Merge
	for cursor1 < sortedRowsLeftLen && cursor2 < sortedRowsRightLen {
		if p.timesOfRowCompare >= SignalCheckpointForSort {
			// Trigger Consume for checking the NeedKill signal
			p.memTracker.Consume(1)
			p.timesOfRowCompare = 0
		}

		p.timesOfRowCompare++

		if p.lessRowFunc(sortedRowsLeft[cursor1], sortedRowsRight[cursor2]) < 0 {
			mergedSortedRows = append(mergedSortedRows, sortedRowsLeft[cursor1])
			cursor1++
		} else {
			mergedSortedRows = append(mergedSortedRows, sortedRowsRight[cursor2])
			cursor2++
		}
	}

	// Append the remaining rows
	mergedSortedRows = append(mergedSortedRows, sortedRowsLeft[cursor1:]...)
	mergedSortedRows = append(mergedSortedRows, sortedRowsRight[cursor2:]...)

	return mergedSortedRows
}

func (p *parallelSortWorker) run() {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(p.processError, r)
		}
		p.waitGroup.Done()
	}()

	ok := p.fetchChunksAndSort()
	if !ok {
		return
	}

	p.mergeSortGlobalRows()
}
