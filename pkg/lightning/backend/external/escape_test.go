// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"bytes"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/stretchr/testify/require"
)

// mockWriterForEscape simulates the Writer methods involved in the escape analysis issue.
type mockWriterForEscape struct {
	data []byte
}

// Global sink to force escape analysis to think the pointer escapes.
// This simulates complex scenarios where the compiler cannot prove the pointer doesn't escape.
var ptrSink *membuf.SliceLocation

// getKeyByLocPtr takes a pointer to SliceLocation.
// We assign to ptrSink to force the argument to escape.
//
//go:noinline
func (m *mockWriterForEscape) getKeyByLocPtr(loc *membuf.SliceLocation) []byte {
	ptrSink = loc
	l := int(loc.Length)
	if l > len(m.data) {
		return nil
	}
	return m.data[:l]
}

// getKeyByLocVal takes SliceLocation by value.
// Even if we did something "escaping" with the value here, it's a copy,
// so the caller's variable doesn't need to escape.
//
//go:noinline
func (m *mockWriterForEscape) getKeyByLocVal(loc membuf.SliceLocation) []byte {
	l := int(loc.Length)
	if l > len(m.data) {
		return nil
	}
	return m.data[:l]
}

type testSortItem struct {
	loc membuf.SliceLocation
}

// run go test -gcflags="-m" -v -run TestSortFuncEscape ./pkg/lightning/backend/external 2>&1 | grep escape_test.go
func TestSortFuncEscape(t *testing.T) {
	// Setup data
	m := &mockWriterForEscape{
		data: make([]byte, 100),
	}
	// Initialize with some data
	for i := range m.data {
		m.data[i] = byte(i)
	}

	// Create a slice of items to sort
	const numItems = 100
	items := make([]testSortItem, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = testSortItem{
			loc: membuf.SliceLocation{
				Length: 10,
			},
		}
	}

	// Measure allocations for Pass-By-Pointer
	// We expect this to allocate because we force the pointer to escape in getKeyByLocPtr.
	allocsPtr := testing.AllocsPerRun(10, func() {
		// Make a copy to sort
		itemsCopy := make([]testSortItem, len(items))
		copy(itemsCopy, items)

		slices.SortFunc(itemsCopy, func(i, j testSortItem) int {
			// Taking address of i.loc and j.loc
			// Since getKeyByLocPtr leaks the pointer, i and j must escape to heap.
			keyI := m.getKeyByLocPtr(&i.loc)
			keyJ := m.getKeyByLocPtr(&j.loc)
			return bytes.Compare(keyI, keyJ)
		})
	})

	// Measure allocations for Pass-By-Value
	allocsVal := testing.AllocsPerRun(10, func() {
		// Make a copy to sort
		itemsCopy := make([]testSortItem, len(items))
		copy(itemsCopy, items)

		slices.SortFunc(itemsCopy, func(i, j testSortItem) int {
			// Passing i.loc and j.loc by value.
			// i and j can stay on stack.
			keyI := m.getKeyByLocVal(i.loc)
			keyJ := m.getKeyByLocVal(j.loc)
			return bytes.Compare(keyI, keyJ)
		})
	})

	t.Logf("Allocations (Pass Pointer): %v", allocsPtr)
	t.Logf("Allocations (Pass Value):   %v", allocsVal)

	// We expect allocsPtr > allocsVal.
	// Note: allocsVal will be at least 1 due to itemsCopy allocation.
	// allocsPtr should be significantly higher due to heap allocation per comparison (or per item).
	require.Less(t, allocsVal, allocsPtr, "Pass by value should have fewer allocations than pass by pointer")
}
