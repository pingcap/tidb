// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spmc

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/resourcemanager/pooltask"
)

var (
	// errQueueIsFull will be returned when the worker queue is full.
	errQueueIsFull = errors.New("the queue is full")

	// errQueueIsReleased will be returned when trying to insert item to a released worker queue.
	errQueueIsReleased = errors.New("the queue is released could not accept item anymore")
)

type loopQueue[T any, U any, C any, CT any, TF pooltask.Context[CT]] struct {
	items  []*goWorker[T, U, C, CT, TF]
	expiry []*goWorker[T, U, C, CT, TF]
	head   int
	tail   int
	size   int
	isFull bool
}

func newWorkerLoopQueue[T any, U any, C any, CT any, TF pooltask.Context[CT]](size int) *loopQueue[T, U, C, CT, TF] {
	return &loopQueue[T, U, C, CT, TF]{
		items: make([]*goWorker[T, U, C, CT, TF], size),
		size:  size,
	}
}

func (wq *loopQueue[T, U, C, CT, TF]) len() int {
	if wq.size == 0 {
		return 0
	}

	if wq.head == wq.tail {
		if wq.isFull {
			return wq.size
		}
		return 0
	}

	if wq.tail > wq.head {
		return wq.tail - wq.head
	}

	return wq.size - wq.head + wq.tail
}

func (wq *loopQueue[T, U, C, CT, TF]) isEmpty() bool {
	return wq.head == wq.tail && !wq.isFull
}

func (wq *loopQueue[T, U, C, CT, TF]) insert(worker *goWorker[T, U, C, CT, TF]) error {
	if wq.size == 0 {
		return errQueueIsReleased
	}

	if wq.isFull {
		return errQueueIsFull
	}
	wq.items[wq.tail] = worker
	wq.tail++

	if wq.tail == wq.size {
		wq.tail = 0
	}
	if wq.tail == wq.head {
		wq.isFull = true
	}

	return nil
}

func (wq *loopQueue[T, U, C, CT, TF]) detach() *goWorker[T, U, C, CT, TF] {
	if wq.isEmpty() {
		return nil
	}

	w := wq.items[wq.head]
	wq.items[wq.head] = nil
	wq.head++
	if wq.head == wq.size {
		wq.head = 0
	}
	wq.isFull = false

	return w
}

func (wq *loopQueue[T, U, C, CT, TF]) retrieveExpiry(duration time.Duration) []*goWorker[T, U, C, CT, TF] {
	expiryTime := time.Now().Add(-duration)
	index := wq.binarySearch(expiryTime)
	if index == -1 {
		return nil
	}
	wq.expiry = wq.expiry[:0]

	if wq.head <= index {
		wq.expiry = append(wq.expiry, wq.items[wq.head:index+1]...)
		for i := wq.head; i < index+1; i++ {
			wq.items[i] = nil
		}
	} else {
		wq.expiry = append(wq.expiry, wq.items[0:index+1]...)
		wq.expiry = append(wq.expiry, wq.items[wq.head:]...)
		for i := 0; i < index+1; i++ {
			wq.items[i] = nil
		}
		for i := wq.head; i < wq.size; i++ {
			wq.items[i] = nil
		}
	}
	head := (index + 1) % wq.size
	wq.head = head
	if len(wq.expiry) > 0 {
		wq.isFull = false
	}

	return wq.expiry
}

// binarySearch is to find the first worker which is idle for more than duration.
func (wq *loopQueue[T, U, C, CT, TF]) binarySearch(expiryTime time.Time) int {
	var mid, nlen, basel, tmid int
	nlen = len(wq.items)

	// if no need to remove work, return -1
	if wq.isEmpty() || expiryTime.Before(wq.items[wq.head].recycleTime.Load()) {
		return -1
	}

	// example
	// size = 8, head = 7, tail = 4
	// [ 2, 3, 4, 5, nil, nil, nil,  1]  true position
	//   0  1  2  3    4   5     6   7
	//              tail          head
	//
	//   1  2  3  4  nil nil   nil   0   mapped position
	//            r                  l

	// base algorithm is a copy from worker_stack
	// map head and tail to effective left and right
	r := (wq.tail - 1 - wq.head + nlen) % nlen
	basel = wq.head
	l := 0
	for l <= r {
		mid = l + ((r - l) >> 1)
		// calculate true mid position from mapped mid position
		tmid = (mid + basel + nlen) % nlen
		if expiryTime.Before(wq.items[tmid].recycleTime.Load()) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	// return true position from mapped position
	return (r + basel + nlen) % nlen
}

func (wq *loopQueue[T, U, C, CT, TF]) reset() {
	if wq.isEmpty() {
		return
	}

Releasing:
	if w := wq.detach(); w != nil {
		w.taskBoxCh <- nil
		goto Releasing
	}
	wq.items = wq.items[:0]
	wq.size = 0
	wq.head = 0
	wq.tail = 0
}
