// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"container/list"
	"sync"
)

type batchWorkFunc func(jobs []interface{})

type BatchWorker struct {
	mu          sync.Mutex
	pendingJobs *list.List
	batchSize   int
	fn          batchWorkFunc
}

func NewBatchWorker(batchSize int, fn batchWorkFunc) *BatchWorker {
	return &BatchWorker{
		pendingJobs: list.New(),
		batchSize:   batchSize,
		fn:          fn,
	}
}

func (b *BatchWorker) Submit(job interface{}) {
	var jobs []interface{}
	b.mu.Lock()
	b.pendingJobs.PushBack(job)
	if b.pendingJobs.Len() >= b.batchSize {
		jobs = make([]interface{}, 0, b.batchSize)
		// pop first batchSize jobs to workerFn
		for i := 0; i < b.batchSize; i++ {
			ele := b.pendingJobs.Front()
			jobs = append(jobs, ele.Value)
			b.pendingJobs.Remove(ele)
		}
	}
	b.mu.Unlock()
	if len(jobs) > 0 {
		b.fn(jobs)
	}
}

func (b *BatchWorker) Flush() {
	var jobs []interface{}
	b.mu.Lock()
	jobs = make([]interface{}, 0, b.pendingJobs.Len())
	for b.pendingJobs.Len() > 0 {
		ele := b.pendingJobs.Front()
		jobs = append(jobs, ele.Value)
		b.pendingJobs.Remove(ele)
	}
	b.mu.Unlock()
	if len(jobs) > 0 {
		b.fn(jobs)
	}
}
