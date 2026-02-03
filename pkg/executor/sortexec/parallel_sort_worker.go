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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/aqsort"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
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
	chunkIters         []*chunk.Iterator4Chunk
	rowNumInChunkIters int
	merger             *multiWayMerger

	sqlKiller *sqlkiller.SQLKiller

	fieldTypes  []*types.FieldType
	keyColumns  []int
	byItemsDesc []bool
	loc         *time.Location

	aqsPairs  []aqsort.Pair[chunk.Row]
	aqsSorter aqsort.PairSorter[chunk.Row]
	aqsArena  []byte
	aqsKeyCap int

	useAQSort  bool
	aqsortCtrl *aqsortControl
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
	spillHelper *parallelSortSpillHelper,
	sqlKiller *sqlkiller.SQLKiller,
	fieldTypes []*types.FieldType,
	keyColumns []int,
	byItemsDesc []bool,
	loc *time.Location,
) *parallelSortWorker {
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
		maxSortedRowsLimit:     maxChunkSize * 30,
		spillHelper:            spillHelper,
		sqlKiller:              sqlKiller,
		fieldTypes:             fieldTypes,
		keyColumns:             keyColumns,
		byItemsDesc:            byItemsDesc,
		loc:                    loc,
		aqsKeyCap:              64,
		useAQSort:              isAQSortEnabled(),
	}
}

func (p *parallelSortWorker) reset() {
	p.localSortedRows = nil
	p.sortedRowsIter = nil
	p.merger = nil
	p.memTracker.ReplaceBytesUsed(0)
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

	loopCnt := uint64(0)

	for {
		var err error
		failpoint.Inject("ParallelSortRandomFail", func(val failpoint.Value) {
			if val.(bool) {
				randNum := rand.Int31n(10000)
				if randNum < 2 {
					err = errors.NewNoStackErrorf("failpoint error")
				}
			}
		})

		if err != nil {
			return nil, err
		}

		if loopCnt%100 == 0 && p.sqlKiller != nil {
			err := p.sqlKiller.HandleSignal()
			if err != nil {
				return nil, err
			}
		}

		loopCnt++

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

func (p *parallelSortWorker) convertChunksToRows() []chunk.Row {
	rows := make([]chunk.Row, 0, p.rowNumInChunkIters)
	for _, iter := range p.chunkIters {
		row := iter.Begin()
		for !row.IsEmpty() {
			rows = append(rows, row)
			row = iter.Next()
		}
	}
	p.chunkIters = p.chunkIters[:0]
	p.rowNumInChunkIters = 0
	return rows
}

func (p *parallelSortWorker) sortBatchRows() {
	rows := p.convertChunksToRows()
	useAQSort := p.useAQSort
	if p.aqsortCtrl != nil {
		useAQSort = p.aqsortCtrl.isEnabled()
	}
	if useAQSort {
		p.sortBatchRowsByEncodedKey(rows)
	} else {
		slices.SortFunc(rows, p.keyColumnsLess)
	}
	p.localSortedRows = append(p.localSortedRows, chunk.NewIterator4Slice(rows))
}

func (p *parallelSortWorker) sortBatchRowsByEncodedKey(rows []chunk.Row) {
	if len(rows) <= 1 {
		return
	}
	if p.loc == nil {
		p.loc = time.UTC
	}

	failpoint.Inject("AQSortForceEncodeKeyError", func(val failpoint.Value) {
		if val.(bool) {
			err := errors.NewNoStackError("injected aqsort sort key encode error")
			if p.aqsortCtrl != nil {
				p.aqsortCtrl.disableWithWarn(err, zap.Int("worker_id", p.workerIDForTest))
			} else {
				p.useAQSort = false
			}
			slices.SortFunc(rows, p.keyColumnsLess)
			failpoint.Return()
		}
	})

	if cap(p.aqsPairs) < len(rows) {
		p.aqsPairs = make([]aqsort.Pair[chunk.Row], len(rows))
	} else {
		p.aqsPairs = p.aqsPairs[:len(rows)]
	}

	keyCap := p.aqsKeyCap
	if keyCap < 64 {
		keyCap = 64
	}
	neededArena := len(rows) * keyCap
	if cap(p.aqsArena) < neededArena {
		p.aqsArena = make([]byte, neededArena)
	} else {
		p.aqsArena = p.aqsArena[:neededArena]
	}

	maxKeyLen := 0
	for i := range rows {
		if i%1024 == 0 {
			p.checkKillSignal()
		}
		key := p.aqsArena[i*keyCap : i*keyCap : i*keyCap+keyCap]
		encoded, err := p.encodeRowSortKey(rows[i], key)
		if err != nil {
			if p.aqsortCtrl != nil {
				p.aqsortCtrl.disableWithWarn(err, zap.Int("worker_id", p.workerIDForTest))
			} else {
				p.useAQSort = false
			}
			slices.SortFunc(rows, p.keyColumnsLess)
			return
		}
		if ln := len(encoded); ln > maxKeyLen {
			maxKeyLen = ln
		}
		p.aqsPairs[i] = aqsort.Pair[chunk.Row]{Key: encoded, Val: rows[i]}
	}
	if maxKeyLen > keyCap {
		if maxKeyLen > 1024 {
			p.aqsKeyCap = 1024
		} else {
			p.aqsKeyCap = maxKeyLen
		}
	}

	p.aqsSorter.SortWithCheckpoint(p.aqsPairs, SignalCheckpointForSort, p.checkKillSignal)
	for i := range rows {
		rows[i] = p.aqsPairs[i].Val
	}
}

func (p *parallelSortWorker) checkKillSignal() {
	if p.sqlKiller == nil {
		return
	}
	if err := p.sqlKiller.HandleSignal(); err != nil {
		panic(err)
	}
}

func (p *parallelSortWorker) encodeRowSortKey(row chunk.Row, dst []byte) ([]byte, error) {
	key := dst[:0]
	for i, colIdx := range p.keyColumns {
		start := len(key)
		datum := row.GetDatum(colIdx, p.fieldTypes[colIdx])
		var err error
		key, err = codec.EncodeKey(p.loc, key, datum)
		if err != nil {
			return nil, err
		}
		if p.byItemsDesc[i] {
			for j := start; j < len(key); j++ {
				key[j] = ^key[j]
			}
		}
	}
	return key, nil
}

func (p *parallelSortWorker) sortLocalRows() ([]chunk.Row, error) {
	// Handle Remaining batchRows whose row number is not over the `maxSortedRowsLimit`
	if p.rowNumInChunkIters > 0 {
		p.sortBatchRows()
	}

	return p.multiWayMergeLocalSortedRows()
}

func (p *parallelSortWorker) saveChunk(chk *chunk.Chunk) {
	chkIter := chunk.NewIterator4Chunk(chk)
	p.chunkIters = append(p.chunkIters, chkIter)
	p.rowNumInChunkIters += chkIter.Len()
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

	p.saveChunk(chk.Chk)

	if p.rowNumInChunkIters >= p.maxSortedRowsLimit {
		p.sortBatchRows()
	}

	p.injectFailPointForParallelSortWorker(3)
	return true
}

func (p *parallelSortWorker) keyColumnsLess(i, j chunk.Row) int {
	if p.timesOfRowCompare >= SignalCheckpointForSort && p.sqlKiller != nil {
		err := p.sqlKiller.HandleSignal()
		if err != nil {
			panic(err)
		}

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
