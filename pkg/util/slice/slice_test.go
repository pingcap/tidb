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

func TestBinarySearchByIndx(t *testing.T) {
	arr := []int{1, 3, 5, 7, 9, 11, 13, 15}
	checkBinarySeachByIndx(t, arr, 1, 7)
	checkBinarySeachByIndx(t, arr, 0, 0)
	checkBinarySeachByIndx(t, arr, 1, 8)
	checkBinarySeachByIndx(t, arr, 0, 16)
	checkBinarySeachByIndx(t, arr, 6, 14)
	checkBinarySeachByIndx(t, arr, 1, 3)
	checkBinarySeachByIndx(t, arr, 3, 5)
	checkBinarySeachByIndx(t, arr, 5, 7)
	checkBinarySeachByIndx(t, arr, 7, 9)
	checkBinarySeachByIndx(t, arr, 9, 11)
	checkBinarySeachByIndx(t, arr, 11, 13)
	checkBinarySeachByIndx(t, arr, 13, 15)
	checkBinarySeachByIndx(t, arr, 130, 150)
	arr = []int{-1, 1}
	checkBinarySeachByIndx(t, arr, -2, -2)
	checkBinarySeachByIndx(t, arr, 2, 3)
	checkBinarySeachByIndx(t, arr, 0, 1)
	checkBinarySeachByIndx(t, arr, -1, 0)
	arr = []int{0}
	checkBinarySeachByIndx(t, arr, -2, -2)
	checkBinarySeachByIndx(t, arr, 2, 3)
	checkBinarySeachByIndx(t, arr, 0, 1)
	checkBinarySeachByIndx(t, arr, -1, 0)
}

func checkBinarySeachByIndx(t *testing.T, arr []int, low, high int) {
	i1, i2 := BinarySearchRangeFunc(arr, low, high, func(a int, b int) int {
		return cmp.Compare(a, b)
	})
	idx, _ := slices.BinarySearch(arr, low)
	require.Equal(t, i1, idx)
	idx, _ = slices.BinarySearch(arr, high)
	require.Equal(t, i2, idx)
}
