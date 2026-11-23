// Copyright 2026 PingCAP, Inc.
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

package globalsort

import (
	"math/bits"
	"slices"
	"sync"
)

const minParallelSortSize = 4096

// parallelSort sorts data in place using at most max(concurrency, 1) goroutines,
// including the caller. compare must support concurrent calls.
func parallelSort[T any](data []T, compare func(T, T) int, concurrency int) {
	concurrency = min(concurrency, len(data)/minParallelSortSize)
	if concurrency <= 1 {
		slices.SortFunc(data, compare)
		return
	}
	// Slots belong to this call and can be reused as workers finish, so an
	// uneven partition does not strand its sibling's concurrency budget.
	tokens := make(chan struct{}, concurrency-1)
	var workers sync.WaitGroup
	var sortRange func([]T, int)
	sortRange = func(data []T, depth int) {
		if len(data) <= minParallelSortSize || depth == 0 {
			slices.SortFunc(data, compare)
			return
		}
		// Sample across the range to balance concatenated sorted file runs.
		var samples [31]T
		step := len(data) / len(samples)
		for i := range samples {
			samples[i] = data[step/2+i*step]
		}
		slices.SortFunc(samples[:], compare)
		pivot := samples[len(samples)/2]
		left, right := 0, len(data)-1
		for left <= right {
			for left <= right && compare(data[left], pivot) < 0 {
				left++
			}
			for left <= right && compare(data[right], pivot) > 0 {
				right--
			}
			if left > right {
				break
			}
			data[left], data[right] = data[right], data[left]
			// Advance both cursors on equal elements, including all-equal inputs.
			left++
			right--
		}
		smaller, larger := data[:right+1], data[left:]
		if len(smaller) > len(larger) {
			smaller, larger = larger, smaller
		}
		select {
		case tokens <- struct{}{}:
			workers.Add(1)
			go func() {
				defer func() {
					<-tokens
					workers.Done()
				}()
				sortRange(larger, depth-1)
			}()
		default:
			sortRange(larger, depth-1)
		}
		sortRange(smaller, depth-1)
	}
	// Fall back to the standard sorter after O(log n) partition levels,
	// bounding work to O(n log n) even when the sampled pivots are poor.
	sortRange(data, 2*bits.Len(uint(len(data))))
	workers.Wait()
}
