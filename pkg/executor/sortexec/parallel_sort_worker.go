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
	"container/heap"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 20000

type parallelSortWorker struct {
	workerIDForTest int

	chunkChannel           chan *chunkWithMemoryUsage
	fetcherAndWorkerSyncer *sync.WaitGroup
	processError           func(error)
	finishCh               chan struct{}

	lessRowFunc       func(chunk.Row, chunk.Row) int
	timesOfRowCompare uint

	memTracker       *memory.Tracker
	totalMemoryUsage int64

	spillHelper *parallelSortSpillHelper

	localSortedRows    []*chunk.Iterator4Slice
	sortedRowsIter     *chunk.Iterator4Slice
	maxSortedRowsLimit int
	merger             *multiWayMerge
}

func newParallelSortWorker(
	workerIDForTest int,
	lessRowFunc func(chunk.Row, chunk.Row) int,
	chunkChannel chan *chunkWithMemoryUsage,
	fetcherAndWorkerSyncer *sync.WaitGroup,
	processError func(error),
	finishCh chan struct{},
	memTracker *memory.Tracker,
	sortedRowsIter *chunk.Iterator4Slice,
	maxChunkSize int,
	spillHelper *parallelSortSpillHelper) *parallelSortWorker {
	return &parallelSortWorker{
		workerIDForTest:        workerIDForTest,
		lessRowFunc:            lessRowFunc,
		chunkChannel:           chunkChannel,
		fetcherAndWorkerSyncer: fetcherAndWorkerSyncer,
		processError:           processError,
		finishCh:               finishCh,
		timesOfRowCompare:      0,
		memTracker:             memTracker,
		sortedRowsIter:         sortedRowsIter,
		maxSortedRowsLimit:     maxChunkSize * 30,
		spillHelper:            spillHelper,
	}
}

func (p *parallelSortWorker) injectFailPointForParallelSortWorker() {
	injectParallelSortRandomFail()
	failpoint.Inject("SlowSomeWorkers", func(val failpoint.Value) {
		if val.(bool) {
			if p.workerIDForTest%2 == 0 {
				randNum := rand.Int31n(10000)
				if randNum < 10 {
					time.Sleep(1 * time.Millisecond)
				}
			}
		}
	})
}

func (p *parallelSortWorker) multiWayMergeSortedRows() {
	totalRowNum := 0
	for _, rows := range p.localSortedRows {
		totalRowNum += rows.Len()
	}
	resultRows := make([]chunk.Row, 0, totalRowNum)
	p.merger = &multiWayMerge{p.lessRowFunc, make([]rowWithPartition, 0, len(p.localSortedRows))}
	for i := range p.localSortedRows {
		row := p.localSortedRows[i].Begin()
		if row.IsEmpty() {
			continue
		}
		p.merger.elements = append(p.merger.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(p.merger)

	for p.merger.Len() > 0 {
		elem := p.merger.elements[0]
		resultRows = append(resultRows, elem.row)
		newRow := p.localSortedRows[elem.partitionID].Next()
		if newRow.IsEmpty() {
			heap.Remove(p.merger, 0)
			continue
		}
		p.merger.elements[0].row = newRow
		heap.Fix(p.merger, 0)
	}

	// Put resultRows into this iter who will be read by sort executor
	p.sortedRowsIter.Reset(resultRows)
}

func (p *parallelSortWorker) sortBatchRows(batchRows []chunk.Row) {
	slices.SortFunc(batchRows, p.keyColumnsLess)
	p.localSortedRows = append(p.localSortedRows, chunk.NewIterator4Slice(batchRows))
}

// Fetching a bunch of chunks from chunkChannel and sort them.
// After receiving all chunks, we will get several sorted rows slices and we use k-way merge to sort them.
func (p *parallelSortWorker) fetchChunksAndSort() {
	batchRows := make([]chunk.Row, 0, p.maxSortedRowsLimit)
	var keepExecution bool
	for {
		batchRows, keepExecution = p.fetchChunksAndSortImpl(batchRows)
		if !keepExecution {
			break
		}
	}
}

func (p *parallelSortWorker) fetchChunksAndSortImpl(batchRows []chunk.Row) ([]chunk.Row, bool) {
	var (
		chk *chunkWithMemoryUsage
		ok  bool
	)
	select {
	case <-p.finishCh:
		return nil, false
	case chk, ok = <-p.chunkChannel:
		// Memory usage of the chunk has been consumed at the chunk fetcher
		if !ok {
			// Handle Remaining batchRows whose row number is not over the `maxSortedRowsLimit`
			if len(batchRows) > 0 {
				p.sortBatchRows(batchRows)
			}

			p.multiWayMergeSortedRows()
			return batchRows, false
		}
		defer p.fetcherAndWorkerSyncer.Done()
		p.totalMemoryUsage += chk.MemoryUsage
	}

	chkIter := chunk.NewIterator4Chunk(chk.Chk)
	row := chkIter.Begin()
	for !row.IsEmpty() {
		batchRows = append(batchRows, row)
		row = chkIter.Next()
	}

	// Sort a bunch of chunks
	if len(batchRows) >= p.maxSortedRowsLimit {
		p.sortBatchRows(batchRows)
		batchRows = make([]chunk.Row, 0, p.maxSortedRowsLimit)
	}

	p.injectFailPointForParallelSortWorker()
	return batchRows, true
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
	}()

	p.fetchChunksAndSort()
}
