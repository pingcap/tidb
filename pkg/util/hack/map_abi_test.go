// Copyright 2015 PingCAP, Inc.
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

package hack

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const seed = 4992862800126241206 // set the fixed seed for test

func TestSwissTable(t *testing.T) {
	require.True(t, maxTableCapacity == 1024)
	{
		const N = 1024
		mp := make(map[uint64]uint64)
		mp[1234] = 5678
		for i := range N {
			mp[uint64(i)] = uint64(i * 2)
		}
		sm := ToSwissMap(mp)
		sm.Data.MockSeedForTest(seed)
		require.Equal(t, N+1, len(mp))
		require.Equal(t, uint64(N+1), sm.Data.Used)
		found := false
		for _, table := range sm.Data.directory() {
			for i := range table.groups.lengthMask + 1 {
				ref := table.groups.group(sm.Type, i)
				require.True(t, (sm.Type.GroupSize-groupSlotsOffset)%sm.Type.SlotSize == 0)
				capacity := ref.cap(sm.Type)
				require.True(t, capacity == swissMapGroupSlots)
				for j := range capacity {
					k, v := *(*uint64)(ref.key(sm.Type, uintptr(j))), *(*uint64)(ref.elem(sm.Type, uintptr(j)))
					if k == 1234 && v == 5678 {
						require.False(t, found)
						found = true
						break
					}
				}
			}
		}
		require.True(t, found)
	}
	{
		const N = 2000
		mp := make(map[string]int)
		for i := range N {
			mp[fmt.Sprintf("key-%d", i)] = i
		}
		require.Equal(t, N, len(mp))
		sm := ToSwissMap(mp)
		sm.Data.MockSeedForTest(seed)
		require.Equal(t, N, int(sm.Data.Used))
		require.True(t, sm.Type.GroupSize == 200)
		require.True(t, sm.Data.dirLen == 4)
		require.Equal(t, 102608, int(sm.Size()))
	}
	{
		mp := make(map[int]int)
		require.Equal(t, 0, len(mp))
		sm := ToSwissMap(mp)
		sm.Data.MockSeedForTest(seed)
		require.Equal(t, 0, int(sm.Data.Used))
		require.True(t, sm.Type.GroupSize == 136)
		require.Equal(t, 184, int(sm.Size()))
		for i := range 8 {
			mp[i] = i
		}
		require.Equal(t, 8, len(mp))
		require.Equal(t, 184, int(sm.Size()))
		mp[9] = 9
		require.Equal(t, 9, len(mp))
		require.Equal(t, 360, int(sm.Size()))
	}

	{
		mp := make(map[complex128]complex128)
		ToSwissMap(mp).Data.MockSeedForTest(seed)
		m := MemAwareMap[complex128, complex128]{}
		const N = 1024*50 - 1
		delta := m.Init(mp)
		for i := range N {
			k := complex(float64(i), float64(i))
			d := m.Set(k, k)
			delta += d
			if d > 0 {
				sz := m.RealBytes()
				require.True(t, m.Bytes >= sz*75/100, "Size %d, RealSize %d, index %d, expMin %d", m.Bytes, sz, i, sz*75/100)
				require.True(t, approxSizeV2(m.groupSize, uint64(m.Len())) >= sz*75/100, "Size %d, RealSize %d, index %d, expMin %d", m.Bytes, sz, i, sz*75/100)
			}
		}
		sz := m.RealBytes()
		require.True(t, sz == 2165296, sz)
		require.True(t, delta == 2702278, delta)
		require.True(t, delta == int64(m.Bytes))
		clear(m.M)
		require.True(t, sz == m.RealBytes())
		require.True(t, delta == int64(m.Bytes))
	}
}

var result int

var inputs = []struct {
	input int
}{
	{input: 1},
	{input: 100},
	{input: 10000},
	{input: 1000000},
}

func memAwareIntMap(size int) int {
	var x int
	m := NewMemAwareMap[int, int](0)
	for j := range size {
		m.Set(j, j)
	}
	for j := range size {
		x, _ = m.Get(j)
	}
	return x
}

func nativeIntMap(size int) int {
	var x int
	m := make(map[int]int)
	for j := range size {
		m[j] = j
	}

	for j := range size {
		x = m[j]
	}
	return x
}

func BenchmarkMemAwareIntMap(b *testing.B) {
	for _, s := range inputs {
		b.Run("MemAwareIntMap_"+strconv.Itoa(s.input), func(b *testing.B) {
			var x int
			for b.Loop() {
				x = memAwareIntMap(s.input)
			}
			result = x
		})
	}
}

func BenchmarkNativeIntMap(b *testing.B) {
	for _, s := range inputs {
		b.Run("NativeIntMap_"+strconv.Itoa(s.input), func(b *testing.B) {
			var x int
			for b.Loop() {
				x = nativeIntMap(s.input)
			}
			result = x
		})
	}
}
