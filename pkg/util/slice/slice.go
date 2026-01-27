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

// BinarySearchRangeFunc finds the index range [i, j) in slice x that corresponds to the value range [t1, t2).
// It returns 'i' as the smallest index where cmp(x[i], t1) >= 0, and 'j' as the smallest index where cmp(x[j], t2) >= 0.
func BinarySearchRangeFunc[S ~[]E, E, T any](x S, t1, t2 T, cmp func(E, T) int) (i, j int) {
	// Define cmp(x[-1], target) < 0 and cmp(x[n], target) >= 0 .
	// Invariant: cmp(x[i - 1], target) < 0, cmp(x[j], target) >= 0.
	// which is the same as slices.BinarySearchFunc.
	i, j = 0, len(x)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// x[h] < t1 < t2
		cmpt1 := cmp(x[h], t1)
		if cmpt1 < 0 {
			i = h + 1
			continue
		}
		// t1 < t2 <= x[h]
		cmpt2 := cmp(x[h], t2)
		if cmpt2 >= 0 {
			j = h
			continue
		}
		// t1 <= x[h] < t2
		iEnd := h
		// If x[h] == t1, then h is a candidate for left point, so we must include it in the search [i, h+1)
		if cmpt1 == 0 {
			iEnd = h + 1
		}
		return BinarySearchByIndexFunc(x, t1, i, iEnd, cmp), BinarySearchByIndexFunc(x, t2, h+1, j, cmp)
	}
	// Not found, return [i, i)
	return i, j
}

// BinarySearchByIndexFunc searches the sorted sub-slice x[start:end).
// It returns the smallest index i in [start, end] such that cmp(x[i], target) >= 0.
// If no such index exists (i.e., all elements in x[start:end) are less than target), it returns end.
func BinarySearchByIndexFunc[S ~[]E, E, T any](x S, target T, start, end int, cmp func(E, T) int) int {
	idx, _ := slices.BinarySearchFunc(x[start:end], target, cmp)
	return start + idx
}
