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

package chunk

import (
	"container/list"
	"sync"
)

type Status int
type Result int

const (
	// Open means the queue is normal.
	Open = iota
	// Closed means the queue has been closed
	Closed

	// OK means that Push or pop is successful
	OK

	DefaultMPMCQueueLimitNum = 100
)

// ChunkWithMemoryUsage contains chunk and memory usage.
// However, some of memory usage may also come from other place,
// not only the chunk's memory usage.
type ChunkWithMemoryUsage struct {
	Chk         *Chunk
	MemoryUsage int64
}

// MPMCQueue means multi producer and multi consumer.
type MPMCQueue struct {
	lock  *sync.Mutex
	cond  *sync.Cond
	queue list.List

	// Maximum number of chunks the queue could store.
	limitNum int

	status Status
}

// NewMPMCQueue creates a new MPMCQueue
func NewMPMCQueue(limit int) *MPMCQueue {
	if limit <= 0 {
		return nil
	}

	lock := sync.Mutex{}
	return &MPMCQueue{
		lock:     &lock,
		cond:     sync.NewCond(&lock),
		queue:    list.List{},
		limitNum: limit,
		status:   Open,
	}
}

func (m *MPMCQueue) checkStatusNoLock() Result {
	switch m.status {
	case Open:
		return Open
	case Closed:
		return Closed
	default:
		panic("Invalid status in MPMCQueue")
	}
}

// Push pushes a chunk into queue with thread safety.
func (m *MPMCQueue) Push(chk *ChunkWithMemoryUsage) Result {
	m.lock.Lock()
	defer m.lock.Unlock()
	res := m.checkStatusNoLock()

	for m.queue.Len() >= m.limitNum && res == Open {
		m.cond.Wait()
		res = m.checkStatusNoLock()
	}

	if res != Open {
		return res
	}

	m.queue.PushBack(chk)
	m.cond.Broadcast()
	return OK
}

// Pop pops a chunk from queue with thread safety.
func (m *MPMCQueue) Pop() (*ChunkWithMemoryUsage, Result) {
	m.lock.Lock()
	defer m.lock.Unlock()
	res := m.checkStatusNoLock()

	for m.queue.Len() == 0 && res == Open {
		m.cond.Wait()
		res = m.checkStatusNoLock()
	}

	if res != Open {
		return nil, res
	}

	elem := m.queue.Front()
	chk, ok := elem.Value.(*ChunkWithMemoryUsage)
	if !ok {
		panic("Data type in MPMCQueue is not *Chunk")
	}
	m.queue.Remove(elem)
	m.cond.Broadcast()
	return chk, OK
}

// ForceClose closes the queue.
func (m *MPMCQueue) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.status = Closed
	m.cond.Broadcast()
}

// GetStatus returns queue's status
func (m *MPMCQueue) GetStatus() Status {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.status
}
