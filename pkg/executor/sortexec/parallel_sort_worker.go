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

	"github.com/pingcap/tidb/pkg/util/chunk"
)

type parallelSortWorker struct {
	queue            []sortedRows
	sharedSortedData *mergeSortedDataContainer

	lock        *sync.Mutex
	cond        *sync.Cond
	publicQueue *[]sortedRows

	waitGroup *sync.WaitGroup
	result    *sortedRows

	mpmcQueue    *chunk.MPMCQueue
	checkError   func() error
	processError func(error)

	totalMemoryUsage int64
}

func newParallelSortWorker(
	sharedSortedData *mergeSortedDataContainer,
	lock *sync.Mutex,
	cond *sync.Cond,
	publicQueue *[]sortedRows,
	waitGroup *sync.WaitGroup,
	result *sortedRows,
	mpmcQueue *chunk.MPMCQueue,
	checkError func() error,
	processError func(error)) *parallelSortWorker {
	return &parallelSortWorker{
		sharedSortedData: sharedSortedData,
		lock:             lock,
		cond:             cond,
		publicQueue:      publicQueue,
		waitGroup:        waitGroup,
		result:           result,
		mpmcQueue:        mpmcQueue,
		checkError:       checkError,
		processError:     processError,
		totalMemoryUsage: 0,
	}
}

// After fetching a chunk from MPMCQueue, we will sort it.
// Return false if there is some error.
func (p *parallelSortWorker) fetchFromMPMCQueueAndSort() bool {
	for {
		err := p.checkError()
		if err != nil {
			return false
		}

		// Memory usage of the chunk has been tracked from the producer side.
		chk, res := p.mpmcQueue.Pop()
		if res != chunk.OK {
			// Check if the queue is closed by an error.
			err := p.checkError()
			return err == nil
		}

		p.totalMemoryUsage += chk.MemoryUsage

	}
}

// Sort data that received by itself.
// Return false if there is some error.
func (p *parallelSortWorker) mergeSortLocalData() bool {
	return true
}

func (p *parallelSortWorker) mergeSortGlobalData() bool {
	return true
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

	p.mergeSortGlobalData()
}
