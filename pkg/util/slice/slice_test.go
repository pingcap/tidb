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
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	tests := []struct {
		a     []int
		allOf bool
	}{
		{[]int{}, true},
		{[]int{1, 2, 3}, false},
		{[]int{1, 3}, false},
		{[]int{2, 2, 4}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.a), func(t *testing.T) {
			even := func(val int) bool { return val%2 == 0 }
			require.Equal(t, test.allOf, AllOf(test.a, even))
		})
	}
}

func checkBinarySearchByIndex(t *testing.T, arr []int, low, high int) {
	i1, i2 := BinarySearchRangeFunc(arr, low, high, func(a int, b int) int {
		return cmp.Compare(a, b)
	})
	idx, _ := slices.BinarySearch(arr, low)
	require.Equal(t, i1, idx)
	idx, _ = slices.BinarySearch(arr, high)
	require.Equal(t, i2, idx)
}

func TestBinarySearchByIndex(t *testing.T) {
	arr := []int{1, 3, 5, 7, 9, 11, 13, 15}
	checkBinarySearchByIndex(t, arr, 0, 0)     // before first
	checkBinarySearchByIndex(t, arr, 1, 1)     // first
	checkBinarySearchByIndex(t, arr, 1, 8)     // [first, before arr[4])
	checkBinarySearchByIndex(t, arr, 0, 16)    // before first, after last
	checkBinarySearchByIndex(t, arr, 1, 3)     // [arr[0], arr[1]]
	checkBinarySearchByIndex(t, arr, 3, 5)     // [arr[1], arr[2]]
	checkBinarySearchByIndex(t, arr, 5, 7)     // [arr[2], arr[3]]
	checkBinarySearchByIndex(t, arr, 10, 11)   // [arr[5] < low < arr[6], arr[6]]
	checkBinarySearchByIndex(t, arr, 10, 12)   // [arr[5] < low < arr[6], arr[6] < high < arr[7]]
	checkBinarySearchByIndex(t, arr, 10, 13)   // [arr[5] < low < arr[6], arr[7]]
	checkBinarySearchByIndex(t, arr, 11, 12)   // [arr[5], arr[6] < high < arr[7]]
	checkBinarySearchByIndex(t, arr, 11, 13)   // [arr[5], arr[7]]
	checkBinarySearchByIndex(t, arr, 11, 14)   // [arr[5], arr[7] < high < arr[8]]
	checkBinarySearchByIndex(t, arr, 13, 15)   // [arr[len-2], arr[len-1]]
	checkBinarySearchByIndex(t, arr, 10, 16)   // [arr[5] < low < arr[6], after last]
	checkBinarySearchByIndex(t, arr, 10, 15)   // [arr[5] < low < arr[6], last]
	checkBinarySearchByIndex(t, arr, 10, 14)   // [arr[5] < low < arr[6], arr[len-2] < high < arr[len-1]]
	checkBinarySearchByIndex(t, arr, 130, 150) // after last
	arr = []int{-1, 1}
	checkBinarySearchByIndex(t, arr, -2, -2)
	checkBinarySearchByIndex(t, arr, 2, 3)
	checkBinarySearchByIndex(t, arr, 0, 1)
	checkBinarySearchByIndex(t, arr, -1, 0)
	checkBinarySearchByIndex(t, arr, -1, -1)
	checkBinarySearchByIndex(t, arr, 1, 1)
	arr = []int{0}
	checkBinarySearchByIndex(t, arr, -2, -2)
	checkBinarySearchByIndex(t, arr, 2, 3)
	checkBinarySearchByIndex(t, arr, 0, 1)
	checkBinarySearchByIndex(t, arr, -1, 0)
	// zero-length slices
	arr = []int{}
	checkBinarySearchByIndex(t, arr, -1, 1)
	checkBinarySearchByIndex(t, arr, 0, 0)
	checkBinarySearchByIndex(t, arr, 1, 2)
	// slices with duplicated value
	arr = []int{1, 3, 3, 3, 5, 7, 7, 9}
	checkBinarySearchByIndex(t, arr, 0, 2)    // [before first, value < first duplicate]
	checkBinarySearchByIndex(t, arr, 3, 3)    // [on first duplicate]
	checkBinarySearchByIndex(t, arr, 2, 4)    // [value < first duplicate, value > last duplicate]
	checkBinarySearchByIndex(t, arr, 3, 4)    // [on first duplicate, value > last duplicate]
	checkBinarySearchByIndex(t, arr, 3, 5)    // [on first duplicate, on next value]
	checkBinarySearchByIndex(t, arr, 5, 7)    // [on single value, on first of second duplicate]
	checkBinarySearchByIndex(t, arr, 6, 8)    // [value between duplicates, value > last of second duplicate]
	checkBinarySearchByIndex(t, arr, 7, 7)    // [on second duplicate]
	checkBinarySearchByIndex(t, arr, 9, 10)   // [on last value, after last]
	checkBinarySearchByIndex(t, arr, 10, 100) // [after last]
}
