// Copyright 2026 PingCAP, Inc.
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

package simplesst

import (
	"bytes"
	"slices"

	"github.com/pingcap/tidb/pkg/util/intest"
)

// EndpointTp is the type of Endpoint.Key.
type EndpointTp int

const (
	// ExclusiveEnd represents "..., Endpoint.Key)".
	ExclusiveEnd EndpointTp = iota
	// InclusiveStart represents "[Endpoint.Key, ...".
	InclusiveStart
	// InclusiveEnd represents "..., Endpoint.Key]".
	InclusiveEnd
)

// Endpoint represents an endpoint of an interval which can be used by GetMaxOverlapping.
type Endpoint struct {
	Key    []byte
	Tp     EndpointTp
	Weight int64 // all EndpointTp use positive weight
}

// GetMaxOverlapping returns the maximum overlapping weight treating given
// `points` as endpoints of intervals. `points` are not required to be sorted,
// and will be sorted in-place in this function.
func GetMaxOverlapping(points []Endpoint) int64 {
	slices.SortFunc(points, func(i, j Endpoint) int {
		if cmp := bytes.Compare(i.Key, j.Key); cmp != 0 {
			return cmp
		}
		return int(i.Tp) - int(j.Tp)
	})
	var maxWeight int64
	var curWeight int64
	for _, p := range points {
		switch p.Tp {
		case InclusiveStart:
			curWeight += p.Weight
		case ExclusiveEnd, InclusiveEnd:
			curWeight -= p.Weight
		}
		if curWeight > maxWeight {
			maxWeight = curWeight
		}
	}
	return maxWeight
}

// RemoveDuplicates remove all duplicates inside sorted array in place, i.e.
// input elements will be changed.
func RemoveDuplicates[E any](in []E, keyGetter func(*E) []byte, recordRemoved bool) ([]E, []E, int) {
	return doRemoveDuplicates(in, keyGetter, 0, recordRemoved)
}

// remove all duplicates inside sorted array in place if the duplicate count is
// more than 2, and keep the first two duplicates.
// we also return the total number of duplicates as the third return value.
func RemoveDuplicatesMoreThanTwo[E any](in []E, keyGetter func(*E) []byte) (out []E, removed []E, totalDup int) {
	return doRemoveDuplicates(in, keyGetter, 2, true)
}

// remove duplicates inside the sorted slice 'in', if keptDupCnt=2, we keep the
// first 2 duplicates, if keptDupCnt=0, we remove all duplicates.
// removed duplicates are returned in 'removed' if recordRemoved=true.
// we also return the total number of duplicates, either it's removed or not, as
// the third return value.
func doRemoveDuplicates[E any](
	in []E,
	keyGetter func(*E) []byte,
	keptDupCnt int,
	recordRemoved bool,
) (out []E, removed []E, totalDup int) {
	intest.Assert(keptDupCnt == 0 || keptDupCnt == 2, "keptDupCnt must be 0 or 2")
	if len(in) <= 1 {
		return in, []E{}, 0
	}
	pivotIdx, fillIdx := 0, 0
	pivot := keyGetter(&in[pivotIdx])
	if recordRemoved {
		removed = make([]E, 0, 2)
	}
	for idx := 1; idx <= len(in); idx++ {
		var key []byte
		if idx < len(in) {
			key = keyGetter(&in[idx])
			if bytes.Equal(pivot, key) {
				continue
			}
		}
		dupCount := idx - pivotIdx
		if dupCount >= 2 {
			totalDup += dupCount
			// keep the first keptDupCnt duplicates, and remove the rest
			for startIdx := pivotIdx; startIdx < pivotIdx+keptDupCnt; startIdx++ {
				if startIdx != fillIdx {
					in[fillIdx] = in[startIdx]
				}
				fillIdx++
			}
			if recordRemoved {
				removed = append(removed, in[pivotIdx+keptDupCnt:idx]...)
			}
		} else {
			if pivotIdx != fillIdx {
				in[fillIdx] = in[pivotIdx]
			}
			fillIdx++
		}
		pivotIdx = idx
		pivot = key
	}
	return in[:fillIdx], removed, totalDup
}
