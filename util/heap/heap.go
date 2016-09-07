// Copyright 2016 PingCAP, Inc.
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

package heap

import (
	"sort"
)

// Heapify will put the top element to the right position in heap, which cost log2n time.
func Heapify(heapPool sort.Interface) {
	pos := 1
	curSize := heapPool.Len()
	for pos*2 < curSize {
		lChild := pos * 2
		rChild := lChild + 1
		if cas(pos, lChild, heapPool) {
			if rChild < curSize {
				cas(pos, rChild, heapPool)
			}
			pos = lChild
		} else if rChild < curSize && cas(pos, rChild, heapPool) {
			pos = rChild
		} else {
			break
		}
	}
}

func cas(i, j int, heapPool sort.Interface) bool {
	if heapPool.Less(i, j) {
		heapPool.Swap(i, j)
		return true
	}
	return false
}

// Update will be called when appending a element to end of heap, which costs log2n time.
func Update(heapPool sort.Interface) {
	pos := heapPool.Len() - 1
	for pos > 1 {
		parent := pos / 2
		if cas(parent, pos, heapPool) {
			pos = parent
		} else {
			break
		}
	}
}
