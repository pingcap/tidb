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

	chunkChannel chan *chunkWithMemoryUsage
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
	chunkChannel chan *chunkWithMemoryUsage,
	checkError func() error,
	processError func(error),
	memTracker *memory.Tracker,
	spillHelper *parallelSortSpillHelper) *parallelSortWorker {
	return &parallelSortWorker{
		lessRowFunc:           lessRowFunc,
		globalSortedRowsQueue: globalSortedRowsQueue,
		waitGroup:             waitGroup,
		result:                result,
		chunkChannel:          chunkChannel,
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

	// Memory usage of the chunk has been consumed at the producer side.
	chk, ok := <-p.chunkChannel
	if !ok {
		return true, nil
	}

	p.totalMemoryUsage += chk.MemoryUsage

	sortedRows := p.sortChunkAndGetSortedRows(chk.Chk)
	p.globalSortedRowsQueue.add(sortedRows)
	return false, nil
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
	for p.mergeSortGlobalRowsCalledByWorker() {
	}
}

func (p *parallelSortWorker) mergeSortGlobalRowsForSpillAction() {
	for p.mergeSortGlobalRowsImpl() {
	}
}

func (p *parallelSortWorker) mergeSortGlobalRowsCalledByWorker() bool {
	p.spillHelper.syncLock.RLock()
	for p.spillHelper.isInSpilling() {
		p.spillHelper.syncLock.RUnlock()
		// Repeatedly release lock so that spill action has more chance to get write lock
		time.Sleep(10 * time.Millisecond)
		p.spillHelper.syncLock.RLock()
	}
	defer p.spillHelper.syncLock.RUnlock()

	return p.mergeSortGlobalRowsImpl()
}

func (p *parallelSortWorker) mergeSortGlobalRowsImpl() bool {
	err := p.checkError()
	if err != nil {
		return false
	}

	sortedRowsLeft, sortedRowsRight := p.globalSortedRowsQueue.fetchTwoSortedRows()
	if sortedRowsLeft == nil {
		return false
	}

	mergedSortedRows := p.mergeTwoSortedRows(sortedRowsLeft, sortedRowsRight)
	p.globalSortedRowsQueue.add(mergedSortedRows)
	return true
}

func (p *parallelSortWorker) mergeTwoSortedRows(sortedRowsLeft sortedRows, sortedRowsRight sortedRows) sortedRows {
	sortedRowsLeftLen := len(sortedRowsLeft)
	sortedRowsRightLen := len(sortedRowsRight)
	mergedSortedRows := make(sortedRows, 0, sortedRowsLeftLen+sortedRowsRightLen)
	cursorLeft := 0  // Point to sortedRowsLeft
	cursorRight := 0 // Point to sortedRowsRight

	// Merge
	for cursorLeft < sortedRowsLeftLen && cursorRight < sortedRowsRightLen {
		if p.timesOfRowCompare >= SignalCheckpointForSort {
			// Trigger Consume for checking the NeedKill signal
			p.memTracker.Consume(1)
			p.timesOfRowCompare = 0
		}

		p.timesOfRowCompare++

		if p.lessRowFunc(sortedRowsLeft[cursorLeft], sortedRowsRight[cursorRight]) < 0 {
			mergedSortedRows = append(mergedSortedRows, sortedRowsLeft[cursorLeft])
			cursorLeft++
		} else {
			mergedSortedRows = append(mergedSortedRows, sortedRowsRight[cursorRight])
			cursorRight++
		}
	}

	// Append the remaining rows
	mergedSortedRows = append(mergedSortedRows, sortedRowsLeft[cursorLeft:]...)
	mergedSortedRows = append(mergedSortedRows, sortedRowsRight[cursorRight:]...)

	return mergedSortedRows
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
