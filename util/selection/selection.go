// Copyright 2020 PingCAP, Inc.
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

package selection

import (
	"math/rand"
	"sort"
)

// Interface is alias of sort.Interface
type Interface = sort.Interface

// Select performs introselect algorithm on data and return index of the k-th smallest value.
func Select(data Interface, k int) int {
	if data.Len() > 0 {
		return introselect(data, 0, data.Len()-1, k-1, 6)
	}
	return -1
}

// introselect will perform quickselect at beginning, and switch to linear-time algorithm if it recurses too much times.
// Source paper: http://www.cs.rpi.edu/~musser/gp/introsort.ps
func introselect(data Interface, left, right, k int, depth int) int {
	if left == right {
		return left
	}
	if depth <= 0 {
		// Use median of medians algorithm(linear-time selection) when recurses too much times.
		return medianOfMedians(data, left, right, k)
	}
	// TODO: use a better pivot function
	pivotIndex := randomPivot(data, left, right)
	pivotIndex = partition(data, left, right, pivotIndex)
	if k == pivotIndex {
		return k
	} else if k < pivotIndex {
		return introselect(data, left, pivotIndex-1, k, depth-1)
	} else {
		return introselect(data, pivotIndex+1, right, k, depth-1)
	}
}

// quickselect is used in test for comparison.
// nolint: unused
func quickselect(data Interface, left, right, k int) int {
	if left == right {
		return left
	}
	pivotIndex := randomPivot(data, left, right)
	pivotIndex = partition(data, left, right, pivotIndex)
	if k == pivotIndex {
		return k
	} else if k < pivotIndex {
		return quickselect(data, left, pivotIndex-1, k)
	} else {
		return quickselect(data, pivotIndex+1, right, k)
	}
}

func medianOfMedians(data Interface, left, right, k int) int {
	if left == right {
		return left
	}
	pivotIndex := medianOfMediansPivot(data, left, right)
	pivotIndex = partitionIntro(data, left, right, pivotIndex, k)
	if k == pivotIndex {
		return k
	} else if k < pivotIndex {
		return medianOfMedians(data, left, pivotIndex-1, k)
	} else {
		return medianOfMedians(data, pivotIndex+1, right, k)
	}
}

func randomPivot(data Interface, left, right int) int {
	return left + (rand.Int() % (right - left + 1)) // #nosec G404
}

func medianOfMediansPivot(data Interface, left, right int) int {
	if right-left < 5 {
		return partition5(data, left, right)
	}
	for i := left; i <= right; i += 5 {
		subRight := i + 4
		if subRight > right {
			subRight = right
		}
		median5 := partition5(data, i, subRight)
		data.Swap(median5, left+(i-left)/5)
	}
	mid := (right-left)/10 + left + 1
	return medianOfMedians(data, left, left+(right-left)/5, mid)
}

func partition(data Interface, left, right, pivotIndex int) int {
	data.Swap(pivotIndex, right)
	storeIndex := left
	for i := left; i < right; i++ {
		if data.Less(i, right) {
			data.Swap(storeIndex, i)
			storeIndex++
		}
	}
	data.Swap(right, storeIndex)
	return storeIndex
}

func partitionIntro(data Interface, left, right, pivotIndex int, k int) int {
	data.Swap(pivotIndex, right)
	storeIndex := left
	// Move all elements smaller than pivot to left side
	for i := left; i < right; i++ {
		if data.Less(i, right) {
			data.Swap(storeIndex, i)
			storeIndex++
		}
	}
	storeIndexEq := storeIndex
	// Move all elements equal to pivot right after
	for i := storeIndex; i < right; i++ {
		// data[i] == data[right]
		if !data.Less(i, right) && !data.Less(right, i) {
			data.Swap(storeIndexEq, i)
			storeIndexEq++
		}
	}
	// Move pivot to final place
	data.Swap(right, storeIndexEq)
	if k < storeIndex {
		return storeIndex
	}
	if k <= storeIndexEq {
		return k
	}
	return storeIndexEq
}

func partition5(data Interface, left, right int) int {
	i := left + 1
	for i <= right {
		j := i
		for j > left && data.Less(j, j-1) {
			data.Swap(j, j-1)
			j = j - 1
		}
		i++
	}
	return (left + right) / 2
}
