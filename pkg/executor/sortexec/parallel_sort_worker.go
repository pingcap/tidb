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
	"slices"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// SignalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const SignalCheckpointForSort uint = 20000

type parallelSortWorker struct {
	workerIDForTest int

	lessRowFunc func(chunk.Row, chunk.Row) int

	chunkChannel  <-chan *chunk.Chunk
	errOutputChan chan<- rowWithError
	finishCh      <-chan struct{}

	timesOfRowCompare uint

	memTracker *memory.Tracker

	localSortedRows    []*chunk.Iterator4Slice
	sortedRowsIter     *chunk.Iterator4Slice
	maxSortedRowsLimit int
	batchRows          []chunk.Row
	merger             *multiWayMerger
}

func newParallelSortWorker(
	workerIDForTest int,
	lessRowFunc func(chunk.Row, chunk.Row) int,
	chunkChannel chan *chunk.Chunk,
	errOutputChan chan rowWithError,
	finishCh chan struct{},
	memTracker *memory.Tracker,
	sortedRowsIter *chunk.Iterator4Slice,
	maxChunkSize int) *parallelSortWorker {
	maxSortedRowsLimit := maxChunkSize * 30
	return &parallelSortWorker{
		workerIDForTest:    workerIDForTest,
		lessRowFunc:        lessRowFunc,
		chunkChannel:       chunkChannel,
		errOutputChan:      errOutputChan,
		finishCh:           finishCh,
		timesOfRowCompare:  0,
		memTracker:         memTracker,
		sortedRowsIter:     sortedRowsIter,
		maxSortedRowsLimit: maxSortedRowsLimit,
		batchRows:          make([]chunk.Row, 0, maxSortedRowsLimit),
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

func (p *parallelSortWorker) multiWayMergeLocalSortedRows() []chunk.Row {
	totalRowNum := 0
	for _, rows := range p.localSortedRows {
		totalRowNum += rows.Len()
	}
	resultSortedRows := make([]chunk.Row, 0, totalRowNum)
	p.merger = newMultiWayMerger(p.localSortedRows, p.lessRowFunc)
	p.merger.init()

	for {
		row := p.merger.next()
		if row.IsEmpty() {
			break
		}
		resultSortedRows = append(resultSortedRows, row)
	}
	return resultSortedRows
}

func (p *parallelSortWorker) sortBatchRows(batchRows []chunk.Row) {
	slices.SortFunc(batchRows, p.keyColumnsLess)
	p.localSortedRows = append(p.localSortedRows, chunk.NewIterator4Slice(batchRows))
}

func (p *parallelSortWorker) sortLocalRows() []chunk.Row {
	// Handle Remaining batchRows whose row number is not over the `maxSortedRowsLimit`
	if len(p.batchRows) > 0 {
		p.sortBatchRows(p.batchRows)
	}

	return p.multiWayMergeLocalSortedRows()
}

// Fetching a bunch of chunks from chunkChannel and sort them.
// After receiving all chunks, we will get several sorted rows slices and we use k-way merge to sort them.
func (p *parallelSortWorker) fetchChunksAndSort() {
	var (
		chk *chunk.Chunk
		ok  bool
	)

	for {
		select {
		case <-p.finishCh:
			return
		case chk, ok = <-p.chunkChannel:
			// Memory usage of the chunk has been consumed at the chunk fetcher
			if !ok {
				rows := p.sortLocalRows()
				p.sortedRowsIter.Reset(rows)
				return
			}
		}

		chkIter := chunk.NewIterator4Chunk(chk)
		row := chkIter.Begin()
		for !row.IsEmpty() {
			p.batchRows = append(p.batchRows, row)
			row = chkIter.Next()
		}

		// Sort a bunch of chunks
		if len(p.batchRows) >= p.maxSortedRowsLimit {
			p.sortBatchRows(p.batchRows)
			p.batchRows = make([]chunk.Row, 0, p.maxSortedRowsLimit)
		}

		p.injectFailPointForParallelSortWorker()
	}
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
			processPanicAndLog(p.errOutputChan, r)
		}
	}()

	p.fetchChunksAndSort()
}
