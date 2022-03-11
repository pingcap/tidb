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

package executor

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

// TestConcurrentMap first inserts 1000 entries, then checks them
func TestConcurrentMap(t *testing.T) {
	m := newConcurrentMap()
	const iterations = 1000
	const mod = 111
	wg := &sync.WaitGroup{}
	wg.Add(2)
	// Using go routines insert 1000 entries into the map.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations/2; i++ {
			// Add entry to map.
			m.Insert(uint64(i%mod), &entry{chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(i)}, nil})
		}
	}()

	go func() {
		defer wg.Done()
		for i := iterations / 2; i < iterations; i++ {
			// Add entry to map.
			m.Insert(uint64(i%mod), &entry{chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(i)}, nil})
		}
	}()
	wg.Wait()

	// check whether i exist in the map, surely
	for i := 0; i < iterations; i++ {
		found := false
		for en, ok := m.Get(uint64(i % mod)); en != nil; en = en.next {
			require.True(t, ok)
			if en.ptr.RowIdx == uint32(i) && en.ptr.ChkIdx == uint32(i) {
				found = true
			}
		}
		require.True(t, found)
	}
	// test some unexpected cases
	_, ok := m.Get(uint64(mod))
	require.False(t, ok)

	_, ok = m.Get(uint64(mod + 1))
	require.False(t, ok)
}
