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
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"golang.org/x/exp/slices"
)

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 20000

type parallelSortWorker struct {
	workerIDForTest int

	waitGroup *sync.WaitGroup

	// Temporarily store rows that will be sorted.
	rowBuffer sortedRows
	result    *sortedRows
	// This list is shared by all workers and all workers will put their sorted rows into this list
	globalSortedRowsQueue *sortedRowsList

	chunkChannel chan *chunkWithMemoryUsage
	fetcherAndWorkerSyncer *sync.WaitGroup
	checkError   func() error
	processError func(error)

	lessRowFunc       func(chunk.Row, chunk.Row) int
	timesOfRowCompare uint

	memTracker       *memory.Tracker
	totalMemoryUsage int64

	spillHelper *parallelSortSpillHelper
}

func newParallelSortWorker(
	workerIDForTest int,
	lessRowFunc func(chunk.Row, chunk.Row) int,
	globalSortedRowsQueue *sortedRowsList,
	waitGroup *sync.WaitGroup,
	result *sortedRows,
	chunkChannel chan *chunkWithMemoryUsage,
	fetcherAndWorkerSyncer *sync.WaitGroup,
	checkError func() error,
	processError func(error),
	memTracker *memory.Tracker,
	spillHelper *parallelSortSpillHelper) *parallelSortWorker {
	return &parallelSortWorker{
		workerIDForTest:       workerIDForTest,
		lessRowFunc:           lessRowFunc,
		globalSortedRowsQueue: globalSortedRowsQueue,
		waitGroup:             waitGroup,
		result:                result,
		chunkChannel:          chunkChannel,
		fetcherAndWorkerSyncer: fetcherAndWorkerSyncer,
		checkError:            checkError,
		processError:          processError,
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

func (p *parallelSortWorker) injectFailPointForParallelSortWorker() {
	injectParallelSortRandomFail()
	failpoint.Inject("SlowSomeWorkers", func(val failpoint.Value) {
		if val.(bool) {
			if p.workerIDForTest%2 == 0 {
				randNum := rand.Int31n(10000)
				if randNum < 100 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}
	})
}

// Fetching chunks from chunkChannel and sort them.
// Rows are sorted only inside a chunk.
func (p *parallelSortWorker) fetchChunksAndSortImpl() (bool, error) {
	err := p.checkError()
	if err != nil {
		return true, err
	}

	// Memory usage of the chunk has been consumed at the producer side.
	chk, ok := <-p.chunkChannel
	if !ok {
		return true, nil
	}

	defer p.fetcherAndWorkerSyncer.Done()

	p.totalMemoryUsage += chk.MemoryUsage

	sortedRows := p.sortChunkAndGetSortedRows(chk.Chk)
	p.globalSortedRowsQueue.add(sortedRows)
	p.injectFailPointForParallelSortWorker()
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
	for {
		err := p.checkError()
		if err != nil {
			return
		}

		sortedRowsLeft, sortedRowsRight := p.globalSortedRowsQueue.fetchTwoSortedRows()
		if sortedRowsLeft == nil {
			return
		}

		mergedSortedRows := p.mergeTwoSortedRows(sortedRowsLeft, sortedRowsRight)
		p.globalSortedRowsQueue.add(mergedSortedRows)
		p.injectFailPointForParallelSortWorker()
	}
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

		failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
			if val.(bool) {
				p.timesOfRowCompare += 1024
			}
		})
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

	failpoint.Inject("SignalCheckpointForSort", func(val failpoint.Value) {
		if val.(bool) {
			p.timesOfRowCompare += 1024
		}
	})
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
