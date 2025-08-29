// Copyright 2025 PingCAP, Inc.
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

package statistics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopNMeta(t *testing.T) {
	// test TopNMeta with heap
	heap := NewTopNHeap(3, func(a, b TopNMeta) int {
		if a.Count < b.Count {
			return -1 // min-heap for TopN
		} else if a.Count > b.Count {
			return 1
		}
		return 0
	})

	heap.Add(TopNMeta{Encoded: []byte("a"), Count: 10})
	heap.Add(TopNMeta{Encoded: []byte("b"), Count: 8})
	heap.Add(TopNMeta{Encoded: []byte("c"), Count: 9})

	result := heap.ToSortedSlice()
	require.Len(t, result, 3)
	require.Equal(t, uint64(10), result[0].Count)
	require.Equal(t, uint64(9), result[1].Count)
	require.Equal(t, uint64(8), result[2].Count)

	// test with full heap, item qualifies
	heap.Add(TopNMeta{Encoded: []byte("d"), Count: 12})
	result = heap.ToSortedSlice()
	require.Len(t, result, 3)
	require.Equal(t, uint64(12), result[0].Count)
	require.Equal(t, uint64(10), result[1].Count)
	require.Equal(t, uint64(9), result[2].Count)

	// test with full heap, item doesn't qualify
	heap.Add(TopNMeta{Encoded: []byte("e"), Count: 5})
	result = heap.ToSortedSlice()
	require.Len(t, result, 3)
	require.Equal(t, uint64(12), result[0].Count)
	require.Equal(t, uint64(10), result[1].Count)
	require.Equal(t, uint64(9), result[2].Count)
}
