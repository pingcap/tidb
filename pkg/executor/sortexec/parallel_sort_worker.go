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
	errOutputChan          chan rowWithError
	finishCh               chan struct{}

	lessRowFunc       func(chunk.Row, chunk.Row) int
	timesOfRowCompare uint

	memTracker       *memory.Tracker
	totalMemoryUsage int64

	spillHelper *parallelSortSpillHelper

	localSortedRows    []*chunk.Iterator4Slice
	sortedRowsIter     *chunk.Iterator4Slice
	maxSortedRowsLimit int
	batchRows          []chunk.Row
	merger             *multiWayMerger
}

func newParallelSortWorker(
	workerIDForTest int,
	lessRowFunc func(chunk.Row, chunk.Row) int,
	chunkChannel chan *chunkWithMemoryUsage,
	fetcherAndWorkerSyncer *sync.WaitGroup,
	errOutputChan chan rowWithError,
	finishCh chan struct{},
	memTracker *memory.Tracker,
	sortedRowsIter *chunk.Iterator4Slice,
	maxChunkSize int,
	spillHelper *parallelSortSpillHelper) *parallelSortWorker {
	maxSortedRowsLimit := maxChunkSize * 30
	return &parallelSortWorker{
		workerIDForTest:        workerIDForTest,
		lessRowFunc:            lessRowFunc,
		chunkChannel:           chunkChannel,
		fetcherAndWorkerSyncer: fetcherAndWorkerSyncer,
		errOutputChan:          errOutputChan,
		finishCh:               finishCh,
		timesOfRowCompare:      0,
		memTracker:             memTracker,
		sortedRowsIter:         sortedRowsIter,
		maxSortedRowsLimit:     maxSortedRowsLimit,
		spillHelper:            spillHelper,
		batchRows:              make([]chunk.Row, 0, maxSortedRowsLimit),
	}
}

func (p *parallelSortWorker) injectFailPointForParallelSortWorker(triggerFactor int32) {
	injectParallelSortRandomFail(triggerFactor)
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

func (p *parallelSortWorker) multiWayMergeLocalSortedRows() ([]chunk.Row, error) {
	totalRowNum := 0
	for _, rows := range p.localSortedRows {
		totalRowNum += rows.Len()
	}
	resultSortedRows := make([]chunk.Row, 0, totalRowNum)
	source := &memorySource{sortedRowsIters: p.localSortedRows}
	p.merger = newMultiWayMerger(source, p.lessRowFunc)
	err := p.merger.init()
	if err != nil {
		return nil, err
	}

	for {
		// It's impossible to return error here as rows are in memory
		row, _ := p.merger.next()
		if row.IsEmpty() {
			break
		}
		resultSortedRows = append(resultSortedRows, row)
	}
	p.localSortedRows = nil
	return resultSortedRows, nil
}

func (p *parallelSortWorker) sortBatchRows() {
	slices.SortFunc(p.batchRows, p.keyColumnsLess)
	p.localSortedRows = append(p.localSortedRows, chunk.NewIterator4Slice(p.batchRows))
	p.batchRows = make([]chunk.Row, 0, p.maxSortedRowsLimit)
}

func (p *parallelSortWorker) sortLocalRows() ([]chunk.Row, error) {
	// Handle Remaining batchRows whose row number is not over the `maxSortedRowsLimit`
	if len(p.batchRows) > 0 {
		p.sortBatchRows()
	}

	return p.multiWayMergeLocalSortedRows()
}

func (p *parallelSortWorker) addChunkToBatchRows(chk *chunk.Chunk) {
	chkIter := chunk.NewIterator4Chunk(chk)
	row := chkIter.Begin()
	for !row.IsEmpty() {
		p.batchRows = append(p.batchRows, row)
		row = chkIter.Next()
	}
}

// Fetching a bunch of chunks from chunkChannel and sort them.
// After receiving all chunks, we will get several sorted rows slices and we use k-way merge to sort them.
func (p *parallelSortWorker) fetchChunksAndSort() {
	for p.fetchChunksAndSortImpl() {
	}
}

func (p *parallelSortWorker) fetchChunksAndSortImpl() bool {
	var (
		chk *chunkWithMemoryUsage
		ok  bool
	)
	select {
	case <-p.finishCh:
		return false
	case chk, ok = <-p.chunkChannel:
		// Memory usage of the chunk has been consumed at the chunk fetcher
		if !ok {
			p.injectFailPointForParallelSortWorker(100)
			// Put local sorted rows into this iter who will be read by sort executor
			sortedRows, err := p.sortLocalRows()
			if err != nil {
				p.errOutputChan <- rowWithError{err: err}
				return false
			}
			p.sortedRowsIter.Reset(sortedRows)
			return false
		}
		defer p.fetcherAndWorkerSyncer.Done()
		p.totalMemoryUsage += chk.MemoryUsage
	}

	p.addChunkToBatchRows(chk.Chk)

	if len(p.batchRows) >= p.maxSortedRowsLimit {
		p.sortBatchRows()
	}

	p.injectFailPointForParallelSortWorker(3)
	return true
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
