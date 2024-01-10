// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pkg/errors"
)

type parallelSortSpillHelper struct {
	syncLock         sync.RWMutex
	isWorkerFinished []bool

	cond             *sync.Cond
	inSpilling       bool
	isSpillTriggered bool
	sortedRowsInDisk []*chunk.DataInDiskByChunks
	cursor           []*dataCursor
	spillError       error
	// closed           bool // TODO is this field needed?
	sortExec *SortExec

	fieldTypes    []*types.FieldType
	tmpSpillChunk *chunk.Chunk
}

func newParallelSortSpillHelper(sortExec *SortExec, fieldTypes []*types.FieldType) *parallelSortSpillHelper {
	return &parallelSortSpillHelper{
		isWorkerFinished: make([]bool, len(sortExec.Parallel.workers)),
		cond:             sync.NewCond(new(sync.Mutex)),
		inSpilling:       false,
		isSpillTriggered: false,
		spillError:       nil,
		sortExec:         sortExec,
		fieldTypes:       fieldTypes,
		tmpSpillChunk:    chunk.NewChunkWithCapacity(fieldTypes, spillChunkSize),
	}
}

func (p *parallelSortSpillHelper) setSpillError(err error) {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.spillError = err
}

func (p *parallelSortSpillHelper) isInSpillingNoLock() bool {
	return p.inSpilling
}

func (p *parallelSortSpillHelper) isInSpilling() bool {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	return p.inSpilling
}

func (p *parallelSortSpillHelper) setInSpillingNoLock() {
	p.inSpilling = true
}

func (p *parallelSortSpillHelper) resetInSpillingNoLock() {
	p.inSpilling = false
}

func (p *parallelSortSpillHelper) resetInSpilling() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	p.inSpilling = false
}

func (p *parallelSortSpillHelper) isSpillTriggeredNoLock() bool {
	return p.isSpillTriggered
}

func (p *parallelSortSpillHelper) setSpillTriggeredNoLock() {
	p.isSpillTriggered = true
}

// This function should be protected by `syncLock`
func (p *parallelSortSpillHelper) spill() {
	workerNum := len(p.sortExec.Parallel.workers)
	workerWaiter := &sync.WaitGroup{}
	workerWaiter.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func(idx int) {
			defer func() {
				if r := recover(); r != nil {
					processErrorAndLog(p.setSpillError, r)
				}
				workerWaiter.Done()
			}()

			// syncLock has been locked before spill function is called, so it's safe to read `isWorkerFinished`
			if p.isWorkerFinished[idx] {
				return
			}

			// Let workers sort existing rows
			p.spillForOneWorker(idx)
		}(i)
	}

	workerWaiter.Wait()
	p.spillImpl()
}

func (p *parallelSortSpillHelper) spillForOneWorker(idx int) {
	ok := p.sortExec.Parallel.workers[idx].mergeSortLocalRows(true)
	if !ok {
		return
	}

	p.sortExec.Parallel.workers[idx].mergeSortAllRows(true)
}

func (p *parallelSortSpillHelper) getSpilledRows() sortedRows {
	partitionNum := p.sortExec.Parallel.rowsToBeMerged.sortedRowsQueue.Len()
	var spilledRows sortedRows
	if partitionNum > 1 {
		p.setSpillError(errors.New("Sort is not completed."))
		return nil
	}

	if partitionNum == 0 {
		spilledRows = p.sortExec.Parallel.result
		p.sortExec.Parallel.result = nil // Clear the result
	} else {
		spilledRows = popFromList(&p.sortExec.Parallel.rowsToBeMerged.sortedRowsQueue)
	}
	return spilledRows
}

func (p *parallelSortSpillHelper) spillImpl() {


	spilledRows := 

	p.tmpSpillChunk.Reset()
	inDisk := chunk.NewDataInDiskByChunks(p.fieldTypes)

	for _, row := range sortedRows {
		p.tmpSpillChunk.AppendRow(row)
		if p.tmpSpillChunk.IsFull() {
			inDisk.Add(p.tmpSpillChunk)
			p.tmpSpillChunk.Reset()
		}
	}

	p.sortedRowsInDisk = append(p.sortedRowsInDisk, inDisk)

	// TODO unconsume memory
}
