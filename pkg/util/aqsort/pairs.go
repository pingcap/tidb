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

package aqsort

import (
	"bytes"
	"math/bits"
	"sort"
)

// Pair is a key/value record where Key is used for byte-wise lexicographic ordering.
type Pair[T any] struct {
	Key []byte
	Val T
}

// PairSorter reuses internal scratch buffers to reduce allocations across repeated sorts.
//
// It is NOT safe for concurrent use.
type PairSorter[T any] struct {
	tmp []Pair[T]
}

// Sort sorts pairs in-place in ascending lexicographic order of Pair.Key.
func (s *PairSorter[T]) Sort(pairs []Pair[T]) {
	s.sort(pairs, nil)
}

// SortWithCheckpoint sorts pairs in-place and invokes checkpoint periodically during sorting.
func (s *PairSorter[T]) SortWithCheckpoint(pairs []Pair[T], checkpointEvery uint, checkpointFn func()) {
	if checkpointFn == nil || checkpointEvery == 0 {
		s.Sort(pairs)
		return
	}
	cp := &checkpoint{every: checkpointEvery, fn: checkpointFn}
	s.sort(pairs, cp)
}

func (s *PairSorter[T]) sort(pairs []Pair[T], cp *checkpoint) {
	if len(pairs) <= 1 {
		return
	}
	if cap(s.tmp) < len(pairs) {
		s.tmp = make([]Pair[T], len(pairs))
	} else {
		s.tmp = s.tmp[:len(pairs)]
	}
	maxDepth := 2*bits.Len(uint(len(pairs))) + extraDepthBudget
	aqsCPSQSPairs(pairs, s.tmp, 0, maxDepth, cp)
}

func aqsCPSQSPairs[T any](pairs, tmp []Pair[T], commonPrefix, depthBudget int, cp *checkpoint) {
	if len(pairs) <= 1 {
		return
	}
	if commonPrefix < 0 {
		commonPrefix = 0
	}
	if depthBudget <= 0 || commonPrefix >= maxCommonPrefixDepth {
		sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0 })
		return
	}
	if len(pairs) <= insertionSortThreshold {
		insertionSortPairs(pairs, commonPrefix, cp)
		return
	}

	pivot := choosePivotPair(pairs, commonPrefix, cp)

	// 3-way partition: [0:lt) < pivot, [lt:gt) == pivot, [gt:] > pivot.
	const maxInt = int(^uint(0) >> 1)
	lt, i, gt := 0, 0, len(pairs)
	ltCommonPrefix, gtCommonPrefix := maxInt, maxInt
	for i < gt {
		cmp, diffAt := compare(pairs[i].Key, pivot, commonPrefix, cp)
		if cmp < 0 {
			if diffAt < ltCommonPrefix {
				ltCommonPrefix = diffAt
			}
			pairs[lt], pairs[i] = pairs[i], pairs[lt]
			lt++
			i++
			continue
		}
		if cmp > 0 {
			if diffAt < gtCommonPrefix {
				gtCommonPrefix = diffAt
			}
			gt--
			pairs[i], pairs[gt] = pairs[gt], pairs[i]
			continue
		}
		i++
	}

	// Sort the < pivot partition.
	if lt > 1 {
		leftPairs, leftTmp := pairs[:lt], tmp[:lt]
		if ltCommonPrefix != maxInt && ltCommonPrefix > commonPrefix {
			aqsRadixPairs(leftPairs, leftTmp, ltCommonPrefix, depthBudget-1, cp)
		} else {
			aqsCPSQSPairs(leftPairs, leftTmp, commonPrefix, depthBudget-1, cp)
		}
	}

	// Sort the > pivot partition.
	if gt < len(pairs)-1 {
		rightPairs, rightTmp := pairs[gt:], tmp[gt:]
		if gtCommonPrefix != maxInt && gtCommonPrefix > commonPrefix {
			aqsRadixPairs(rightPairs, rightTmp, gtCommonPrefix, depthBudget-1, cp)
		} else {
			aqsCPSQSPairs(rightPairs, rightTmp, commonPrefix, depthBudget-1, cp)
		}
	}
}

func aqsRadixPairs[T any](pairs, tmp []Pair[T], commonPrefix, depthBudget int, cp *checkpoint) {
	if len(pairs) <= 1 {
		return
	}
	if commonPrefix < 0 {
		commonPrefix = 0
	}
	if depthBudget <= 0 || commonPrefix >= maxCommonPrefixDepth {
		sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0 })
		return
	}
	if len(pairs) <= insertionSortThreshold {
		insertionSortPairs(pairs, commonPrefix, cp)
		return
	}

	var counts [radixBucketCount]int
	for i := range pairs {
		counts[bucketIndex(pairs[i].Key, commonPrefix)]++
	}

	var starts [radixBucketCount]int
	sum := 0
	for i := range counts {
		starts[i] = sum
		sum += counts[i]
	}

	// Use starts as the running write position.
	pos := starts
	for i := range pairs {
		b := bucketIndex(pairs[i].Key, commonPrefix)
		tmp[pos[b]] = pairs[i]
		pos[b]++
	}
	copy(pairs, tmp[:len(pairs)])

	// Recurse on "more" buckets (keys that have bytes beyond commonPrefix).
	for byteVal := range 256 {
		moreBucket := 2 + 2*byteVal
		off := starts[moreBucket]
		n := counts[moreBucket]
		if n <= 1 {
			continue
		}
		segPairs := pairs[off : off+n]
		segTmp := tmp[off : off+n]
		aqsCPSQSPairs(segPairs, segTmp, commonPrefix+1, depthBudget-1, cp)
	}
}

func insertionSortPairs[T any](pairs []Pair[T], commonPrefix int, cp *checkpoint) {
	for i := 1; i < len(pairs); i++ {
		v := pairs[i]
		j := i - 1
		for j >= 0 && compareOnly(pairs[j].Key, v.Key, commonPrefix, cp) > 0 {
			pairs[j+1] = pairs[j]
			j--
		}
		pairs[j+1] = v
	}
}

func choosePivotPair[T any](pairs []Pair[T], commonPrefix int, cp *checkpoint) []byte {
	if len(pairs) < 3 {
		return pairs[len(pairs)/2].Key
	}

	a := pairs[0].Key
	b := pairs[len(pairs)/2].Key
	c := pairs[len(pairs)-1].Key

	if compareOnly(a, b, commonPrefix, cp) > 0 {
		a, b = b, a
	}
	if compareOnly(b, c, commonPrefix, cp) > 0 {
		b = c
	}
	if compareOnly(a, b, commonPrefix, cp) > 0 {
		b = a
	}
	return b
}
