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

// This package implements an adaptive sort for byte-orderable keys inspired by
// US7680791B2 ("Method for sorting data using common prefix bytes").
//
// The implementation combines:
// - Common-prefix-skipping quicksort partitioning (CPS-QS): comparisons start at a known common prefix.
// - A radix partitioning step on the first byte after the common prefix, to exploit long shared prefixes.
//
// Notes:
// - The sort is NOT stable.
// - Keys are compared lexicographically by byte values, identical to bytes.Compare.

const (
	insertionSortThreshold = 32
	extraDepthBudget       = 128
	maxCommonPrefixDepth   = 4096
	radixBucketCount       = 1 + 2*256 // 0=end; (done,more) per byte.
)

// Sorter reuses internal scratch buffers to reduce allocations across repeated sorts.
//
// It is NOT safe for concurrent use.
type Sorter struct {
	tmp [][]byte
}

type checkpoint struct {
	every uint
	count uint
	fn    func()
}

func (c *checkpoint) tick() {
	if c == nil || c.fn == nil || c.every == 0 {
		return
	}
	c.count++
	if c.count >= c.every {
		c.count = 0
		c.fn()
	}
}

// SortBytes sorts keys in-place in ascending lexicographic order (byte-wise).
func SortBytes(keys [][]byte) {
	var s Sorter
	s.SortBytes(keys)
}

// SortBytes sorts keys in-place in ascending lexicographic order (byte-wise).
//
// The algorithm assumes keys are byte-orderable (i.e., the order is the raw byte
// order, not collation-aware).
func (s *Sorter) SortBytes(keys [][]byte) {
	s.sortBytes(keys, nil)
}

// SortBytesWithCheckpoint sorts keys in-place and invokes checkpoint periodically
// during sorting.
//
// This is primarily intended for long-running database sorts that need to check
// for cancellation/kill signals.
func (s *Sorter) SortBytesWithCheckpoint(keys [][]byte, checkpointEvery uint, checkpointFn func()) {
	if checkpointFn == nil || checkpointEvery == 0 {
		s.SortBytes(keys)
		return
	}
	cp := &checkpoint{every: checkpointEvery, fn: checkpointFn}
	s.sortBytes(keys, cp)
}

func (s *Sorter) sortBytes(keys [][]byte, cp *checkpoint) {
	if len(keys) <= 1 {
		return
	}
	if cap(s.tmp) < len(keys) {
		s.tmp = make([][]byte, len(keys))
	} else {
		s.tmp = s.tmp[:len(keys)]
	}
	maxDepth := 2*bits.Len(uint(len(keys))) + extraDepthBudget
	aqsCPSQS(keys, s.tmp, 0, maxDepth, cp)
}

func aqsCPSQS(keys, tmp [][]byte, commonPrefix, depthBudget int, cp *checkpoint) {
	if len(keys) <= 1 {
		return
	}
	if commonPrefix < 0 {
		commonPrefix = 0
	}
	if depthBudget <= 0 || commonPrefix >= maxCommonPrefixDepth {
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
		return
	}
	if len(keys) <= insertionSortThreshold {
		insertionSort(keys, commonPrefix, cp)
		return
	}

	pivot := choosePivot(keys, commonPrefix, cp)

	// 3-way partition: [0:lt) < pivot, [lt:gt) == pivot, [gt:] > pivot.
	const maxInt = int(^uint(0) >> 1)
	lt, i, gt := 0, 0, len(keys)
	ltCommonPrefix, gtCommonPrefix := maxInt, maxInt
	for i < gt {
		cmp, diffAt := compare(keys[i], pivot, commonPrefix, cp)
		if cmp < 0 {
			if diffAt < ltCommonPrefix {
				ltCommonPrefix = diffAt
			}
			keys[lt], keys[i] = keys[i], keys[lt]
			lt++
			i++
			continue
		}
		if cmp > 0 {
			if diffAt < gtCommonPrefix {
				gtCommonPrefix = diffAt
			}
			gt--
			keys[i], keys[gt] = keys[gt], keys[i]
			continue
		}
		i++
	}

	// Sort the < pivot partition.
	if lt > 1 {
		leftKeys, leftTmp := keys[:lt], tmp[:lt]
		if ltCommonPrefix != maxInt && ltCommonPrefix > commonPrefix {
			aqsRadix(leftKeys, leftTmp, ltCommonPrefix, depthBudget-1, cp)
		} else {
			aqsCPSQS(leftKeys, leftTmp, commonPrefix, depthBudget-1, cp)
		}
	}

	// Sort the > pivot partition.
	if gt < len(keys)-1 {
		rightKeys, rightTmp := keys[gt:], tmp[gt:]
		if gtCommonPrefix != maxInt && gtCommonPrefix > commonPrefix {
			aqsRadix(rightKeys, rightTmp, gtCommonPrefix, depthBudget-1, cp)
		} else {
			aqsCPSQS(rightKeys, rightTmp, commonPrefix, depthBudget-1, cp)
		}
	}
}

func aqsRadix(keys, tmp [][]byte, commonPrefix, depthBudget int, cp *checkpoint) {
	if len(keys) <= 1 {
		return
	}
	if commonPrefix < 0 {
		commonPrefix = 0
	}
	if depthBudget <= 0 || commonPrefix >= maxCommonPrefixDepth {
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
		return
	}
	if len(keys) <= insertionSortThreshold {
		insertionSort(keys, commonPrefix, cp)
		return
	}

	var counts [radixBucketCount]int
	for _, k := range keys {
		counts[bucketIndex(k, commonPrefix)]++
	}

	var starts [radixBucketCount]int
	sum := 0
	for i := range counts {
		starts[i] = sum
		sum += counts[i]
	}

	// Use starts as the running write position.
	pos := starts
	for _, k := range keys {
		b := bucketIndex(k, commonPrefix)
		tmp[pos[b]] = k
		pos[b]++
	}
	copy(keys, tmp[:len(keys)])

	// Recurse on "more" buckets (keys that have bytes beyond commonPrefix).
	for byteVal := range 256 {
		moreBucket := 2 + 2*byteVal
		off := starts[moreBucket]
		n := counts[moreBucket]
		if n <= 1 {
			continue
		}
		segKeys := keys[off : off+n]
		segTmp := tmp[off : off+n]
		aqsCPSQS(segKeys, segTmp, commonPrefix+1, depthBudget-1, cp)
	}
}

func insertionSort(keys [][]byte, commonPrefix int, cp *checkpoint) {
	for i := 1; i < len(keys); i++ {
		v := keys[i]
		j := i - 1
		for j >= 0 && compareOnly(keys[j], v, commonPrefix, cp) > 0 {
			keys[j+1] = keys[j]
			j--
		}
		keys[j+1] = v
	}
}

func choosePivot(keys [][]byte, commonPrefix int, cp *checkpoint) []byte {
	if len(keys) < 3 {
		return keys[len(keys)/2]
	}

	a := keys[0]
	b := keys[len(keys)/2]
	c := keys[len(keys)-1]

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

// compare returns the lexicographic comparison result between a and b, and diffAt:
// the index of the first byte where they differ, or the length of the shorter key
// if one key ends before a difference.
func compare(a, b []byte, commonPrefix int, cp *checkpoint) (cmp, diffAt int) {
	cp.tick()
	if commonPrefix < 0 {
		commonPrefix = 0
	}

	// We expect all bytes < commonPrefix to be equal, so start from commonPrefix.
	i := commonPrefix

	la, lb := len(a), len(b)
	minLen := la
	if lb < minLen {
		minLen = lb
	}

	if i > minLen {
		// One key ends before the assumed common prefix.
		diffAt = minLen
		switch {
		case la < lb:
			return -1, diffAt
		case la > lb:
			return 1, diffAt
		default:
			return 0, diffAt
		}
	}

	for i < minLen {
		ab, bb := a[i], b[i]
		if ab != bb {
			diffAt = i
			if ab < bb {
				return -1, diffAt
			}
			return 1, diffAt
		}
		i++
	}

	diffAt = minLen
	switch {
	case la < lb:
		return -1, diffAt
	case la > lb:
		return 1, diffAt
	default:
		return 0, diffAt
	}
}

func compareOnly(a, b []byte, commonPrefix int, cp *checkpoint) int {
	cmp, _ := compare(a, b, commonPrefix, cp)
	return cmp
}

// bucketIndex returns an index into [0, radixBucketCount) for the radix step.
//
// Bucket layout:
//
//	0: len(key) == commonPrefix (key ends at the common prefix)
//	1+2*v:  byte at key[commonPrefix] == v, and len(key) == commonPrefix+1 (done bucket)
//	2+2*v:  byte at key[commonPrefix] == v, and len(key) >  commonPrefix+1 (more bucket)
func bucketIndex(key []byte, commonPrefix int) int {
	if commonPrefix < 0 {
		commonPrefix = 0
	}
	if len(key) <= commonPrefix {
		return 0
	}
	v := int(key[commonPrefix])
	if len(key) == commonPrefix+1 {
		return 1 + 2*v
	}
	return 2 + 2*v
}
