// Copyright 2021 PingCAP, Inc.
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

package slice

import (
	"slices"
	"strconv"
)

// AllOf returns true if all elements in the slice match the predict func.
func AllOf[T any](s []T, p func(T) bool) bool {
	// Use the inverse of ContainsFunc with negated predicate
	// AllOf(s, p) is equivalent to !ContainsFunc(s, !p)
	return !slices.ContainsFunc(s, func(x T) bool {
		return !p(x)
	})
}

// Int64sToStrings converts a slice of int64 to a slice of string.
func Int64sToStrings(ints []int64) []string {
	strs := make([]string, len(ints))
	for i, v := range ints {
		strs[i] = strconv.FormatInt(v, 10)
	}
	return strs
}

// DeepClone uses Clone() to clone a slice.
// The elements in the slice must implement func (T) Clone() T.
func DeepClone[T interface{ Clone() T }](s []T) []T {
	if s == nil {
		return nil
	}
	cloned := make([]T, 0, len(s))
	for _, item := range s {
		cloned = append(cloned, item.Clone())
	}
	return cloned
}

// BinarySearchRangeFunc performs a binary search on the slice x to find the range [t1, t2).
func BinarySearchRangeFunc[S ~[]E, E, T any](x S, t1, t2 T, cmp func(E, T) int) (i, j int) {
	// Define cmp(x[-1], target) < 0 and cmp(x[n], target) >= 0 .
	// Invariant: cmp(x[i - 1], target) < 0, cmp(x[j], target) >= 0.
	// which is the same as slices.BinarySearchFunc.
	i, j = 0, len(x)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		cmpt1 := cmp(x[h], t1)
		if cmpt1 < 0 {
			// x[h] < t1 < t2
			i = h + 1 // preserves cmp(x[i - 1], target) < 0
			continue
		} else if cmpt1 == 0 {
			// x[h] = t1 < t2
			return h, BinarySearchByIndexFunc(x, t2, h, j, cmp)
		}
		// t1 < x[h]
		cmpt2 := cmp(x[h], t2)
		if cmpt2 < 0 {
			// t1 < x[h] < t2
			return BinarySearchByIndexFunc(x, t1, i, h, cmp), BinarySearchByIndexFunc(x, t2, h+1, j, cmp)
		} else if cmpt2 > 0 {
			// t1 < t2 < x[h]
			j = h // preserves cmp(x[j], target) >= 0
		} else if cmpt2 == 0 {
			// t1 < x[h] = t2
			return BinarySearchByIndexFunc(x, t1, i, h, cmp), h
		}
	}
	return i, j
}

// BinarySearchByIndexFunc performs a binary search on the slice x to find the target value between the range [start, end).
func BinarySearchByIndexFunc[S ~[]E, E, T any](x S, target T, start, end int, cmp func(E, T) int) int {
	for start < end {
		h := int(uint(start+end) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if cmp(x[h], target) < 0 {
			start = h + 1 // preserves cmp(x[i - 1], target) < 0
		} else {
			end = h // preserves cmp(x[j], target) >= 0
		}
	}
	return start
}

// BinarySearchFunc performs a binary search on the slice x to find the target value.
func BinarySearchFunc[S ~[]E, E, T any](x S, target T, cmp func(E, T) int) int {
	return BinarySearchByIndexFunc(x, target, 0, len(x), cmp)
}
