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
	self struct {
		lock sync.Mutex
		// It's used by itself.
		queue []sortedRows
	}

	lock        *sync.Mutex
	cond        *sync.Cond
	publicQueue *[]sortedRows

	waitGroup *sync.WaitGroup
	result    *sortedRows

	mpmcQueue    *chunk.MPMCQueue
	processError func(error)
}

func newParallelSortWorker(
	lock *sync.Mutex,
	cond *sync.Cond,
	publicQueue *[]sortedRows,
	waitGroup *sync.WaitGroup,
	result *sortedRows,
	mpmcQueue *chunk.MPMCQueue,
	processError func(error)) *parallelSortWorker {
	return &parallelSortWorker{
		lock:         lock,
		cond:         cond,
		publicQueue:  publicQueue,
		waitGroup:    waitGroup,
		result:       result,
		mpmcQueue:    mpmcQueue,
		processError: processError,
	}
}

func (p *parallelSortWorker) run() {
	defer func() {
		if r := recover(); r != nil {
			processErrorAndLog(p.processError, r)
		}
		p.waitGroup.Done()
	}()

	for {
		
	}
}
