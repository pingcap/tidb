// Copyright 2022 PingCAP, Inc.
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

package spmc

import "time"

type workerStack[T any, U any, C any, CT any, TF Context[CT]] struct {
	items  []*goWorker[T, U, C, CT, TF]
	expiry []*goWorker[T, U, C, CT, TF]
}

func newWorkerStack[T any, U any, C any, CT any, TF Context[CT]](size int) *workerStack[T, U, C, CT, TF] {
	return &workerStack[T, U, C, CT, TF]{
		items: make([]*goWorker[T, U, C, CT, TF], 0, size),
	}
}

func (wq *workerStack[T, U, C, CT, TF]) len() int {
	return len(wq.items)
}

func (wq *workerStack[T, U, C, CT, TF]) isEmpty() bool {
	return len(wq.items) == 0
}

func (wq *workerStack[T, U, C, CT, TF]) insert(worker *goWorker[T, U, C, CT, TF]) error {
	wq.items = append(wq.items, worker)
	return nil
}

func (wq *workerStack[T, U, C, CT, TF]) detach() *goWorker[T, U, C, CT, TF] {
	l := wq.len()
	if l == 0 {
		return nil
	}

	w := wq.items[l-1]
	wq.items[l-1] = nil // avoid memory leaks
	wq.items = wq.items[:l-1]

	return w
}

func (wq *workerStack[T, U, C, CT, TF]) retrieveExpiry(duration time.Duration) []*goWorker[T, U, C, CT, TF] {
	n := wq.len()
	if n == 0 {
		return nil
	}

	expiryTime := time.Now().Add(-duration)
	index := wq.binarySearch(0, n-1, expiryTime)

	wq.expiry = wq.expiry[:0]
	if index != -1 {
		wq.expiry = append(wq.expiry, wq.items[:index+1]...)
		m := copy(wq.items, wq.items[index+1:])
		for i := m; i < n; i++ {
			wq.items[i] = nil
		}
		wq.items = wq.items[:m]
	}
	return wq.expiry
}

func (wq *workerStack[T, U, C, CT, TF]) binarySearch(l, r int, expiryTime time.Time) int {
	var mid int
	for l <= r {
		mid = (l + r) / 2
		if expiryTime.Before(wq.items[mid].recycleTime) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return r
}

func (wq *workerStack[T, U, C, CT, TF]) reset() {
	for i := 0; i < wq.len(); i++ {
		wq.items[i].task <- nil
		wq.items[i] = nil
	}
	wq.items = wq.items[:0]
}
