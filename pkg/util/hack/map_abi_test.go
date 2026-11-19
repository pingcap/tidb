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

//go:build go1.25 && !go1.28

package hack

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

const (
	mapIndirectKey  = 1 << 2
	mapIndirectElem = 1 << 3
	mapMaxKeyBytes  = 128
	mapMaxElemBytes = 128
)

func TestSwissTable(t *testing.T) {
	require.True(t, maxTableCapacity == 1024)
	{
		m := make(map[int]int)
		tp := ToSwissMap(m).Type
		require.True(t, tp.GroupSize == 136)
		testMapSlotLayout(t, tp, 16, 8)
	}
	{
		m := make(map[int32]int32)
		tp := ToSwissMap(m).Type
		require.True(t, tp.GroupSize == 72)
		testMapSlotLayout(t, tp, 8, 4)
	}
	{
		m := make(map[int8]int8)
		tp := ToSwissMap(m).Type
		require.True(t, tp.GroupSize == 24)
		testMapSlotLayout(t, tp, 2, 1)
	}
	{
		m := make(map[int64]float64)
		tp := ToSwissMap(m).Type
		require.True(t, tp.GroupSize == 136)
		testMapSlotLayout(t, tp, 16, 8)
	}
	{
		m := make(map[complex128]complex128)
		tp := ToSwissMap(m).Type
		require.True(t, tp.GroupSize == 264)
		testMapSlotLayout(t, tp, 32, 16)
	}
	{
		const N = 1024
		mp := make(map[uint64]uint64)
		sm := ToSwissMap(mp)
		sm.Data.MockSeedForTest(mockSeedForTest)
		mp[1234] = 5678
		for i := range N {
			mp[uint64(i)] = uint64(i * 2)
		}
		require.Equal(t, N+1, len(mp))
		require.Equal(t, uint64(N+1), sm.Data.Used)
		found := false
		var lastTab *testMapTable

		for i := range sm.Data.dirLen {
			table := sm.Data.directoryAt(uintptr(i))
			if table == lastTab {
				continue
			}
			for i := range table.groups.lengthMask + 1 {
				ref := table.groups.group(sm.Type, i)
				capacity := ref.cap(sm.Type)
				require.True(t, capacity == testMapGroupSlots)
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

		oriSeed := sm.Data.seed
		for k := range mp {
			delete(mp, k)
		}
		require.True(t, oriSeed != sm.Data.seed)
	}
	{
		const N = 2000
		mp := make(map[string]int)
		sm := ToSwissMap(mp)
		sm.Data.MockSeedForTest(mockSeedForTest)
		for i := range N {
			mp[fmt.Sprintf("key-%d", i)] = i
		}
		require.Equal(t, N, len(mp))
		require.Equal(t, N, int(sm.Data.Used))
		require.True(t, sm.Type.GroupSize == 200)
		require.True(t, sm.Data.dirLen == 4)
		require.Equal(t, 102608, int(sm.Size()))
	}
	{
		mp := make(map[int]int)
		require.Equal(t, 0, len(mp))
		sm := ToSwissMap(mp)
		sm.Data.MockSeedForTest(mockSeedForTest)
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
		m := MemAwareMap[complex128, complex128]{}
		const N = 1024*50 - 1
		delta := m.Init(mp)
		m.MockSeedForTest()
		for i := range N {
			k := complex(float64(i), float64(i))
			d := m.Set(k, k)
			delta += d
			if d > 0 {
				sz := m.RealBytes()
				expMin := sz * 75 / 100
				require.True(t, m.Bytes >= expMin, "ApproxSize %d, RealSize %d, index %d, expMin %d", m.Bytes, sz, i, expMin)
				require.True(t, approxSize(m.groupSize, uint64(m.Len())) >= expMin, "ApproxSize %d, RealSize %d, index %d, expMin %d", m.Bytes, sz, i, expMin)
			}
		}
		sz := m.RealBytes()
		require.True(t, sz == 2165296, sz)
		require.True(t, delta == 2702278, delta)
		require.True(t, delta == int64(m.Bytes))
		require.True(t, mockSeedForTest == m.unwrap().seed)
		clearSeq := m.unwrap().clearSeq
		clear(m.M)
		require.True(t, m.Len() == 0)
		require.True(t, clearSeq+1 == m.unwrap().clearSeq)
		require.True(t, m.unwrap().seed != mockSeedForTest)
		require.True(t, sz == m.RealBytes())
		require.True(t, delta == int64(m.Bytes))

		m.MockSeedForTest()
		for i := range 1024 {
			k := complex(float64(i), float64(i))
			d, insert := m.SetExt(k, k)
			require.True(t, d == 0)
			require.True(t, insert)
		}
		require.True(t, m.Len() == 1024)
	}
}

func TestMapABI(t *testing.T) {
	// Log every field offset so toolchain comparisons retain measured layouts.
	m := ToSwissMap(make(map[uint64]uint64))
	for _, typ := range []reflect.Type{reflect.TypeOf(*m.Type), reflect.TypeOf(*m.Data), reflect.TypeOf(testMapTable{}), reflect.TypeOf(abiType{})} {
		t.Logf("%s size=%d align=%d", typ.Name(), typ.Size(), typ.Align())
		for i := range typ.NumField() {
			field := typ.Field(i)
			t.Logf("  %s offset=%d size=%d", field.Name, field.Offset, field.Type.Size())
		}
	}
	t.Run("padded", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) (uint64, byte) { return uint64(i), byte(i) })
	})
	t.Run("paddedElem", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) (uint16, uint64) { return uint16(i), uint64(i) })
	})
	t.Run("zeroElem", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) (uint64, struct{}) { return uint64(i), struct{}{} })
	})
	t.Run("zeroKey", func(t *testing.T) {
		testMapABIEntries(t, 1, func(i int) (struct{}, uint64) { return struct{}{}, uint64(i) })
	})
	t.Run("zeroKeyAndElem", func(t *testing.T) {
		testMapABIEntries(t, 1, func(int) (struct{}, struct{}) { return struct{}{}, struct{}{} })
	})
	t.Run("indirectKey", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) ([129]byte, uint64) {
			var key [129]byte
			key[0], key[128] = byte(i), byte(i>>8)
			return key, uint64(i)
		})
	})
	t.Run("indirectElem", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) (uint64, [129]byte) {
			var elem [129]byte
			elem[0], elem[128] = byte(i), byte(i>>8)
			return uint64(i), elem
		})
	})
	t.Run("indirectKeyAndElem", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) ([129]byte, [129]byte) {
			var key, elem [129]byte
			key[0], key[128] = byte(i), byte(i>>8)
			elem[0], elem[128] = byte(i+1), byte(i>>8)
			return key, elem
		})
	})
	t.Run("interfaceElem", func(t *testing.T) {
		testMapABIEntries(t, 2000, func(i int) (string, any) { return strconv.Itoa(i), []int{i} })
	})
}

func testMapABIEntries[K comparable, V any](t *testing.T, n int, entry func(int) (K, V)) {
	t.Helper()
	for _, hint := range []int{0, n} {
		m := make(map[K]V, hint)
		testMapABISlots(t, m)
		for i := range n {
			k, v := entry(i)
			m[k] = v
			if i == 0 || i == 7 || i == 8 {
				testMapABISlots(t, m)
			}
		}
		testMapABISlots(t, m)
		for i := 0; i < n; i += 3 {
			k, _ := entry(i)
			delete(m, k)
		}
		testMapABISlots(t, m)
		clear(m)
		testMapABISlots(t, m)
	}
}

func testMapABISlots[K comparable, V any](t *testing.T, m map[K]V) {
	t.Helper()
	sm := ToSwissMap(m)
	require.Equal(t, uint64(len(m)), sm.Data.Used)
	require.GreaterOrEqual(t, sm.Data.Cap(), uint64(len(m)))
	var key K
	var elem V
	require.Equal(t, unsafe.Sizeof(key) > mapMaxKeyBytes, sm.Type.Flags&mapIndirectKey != 0)
	require.Equal(t, unsafe.Sizeof(elem) > mapMaxElemBytes, sm.Type.Flags&mapIndirectElem != 0)
	seen := make(map[K]bool)
	checkGroup := func(ref groupReference) {
		ctrls := *(*uint64)(ref.data)
		for i := uintptr(0); i < testMapGroupSlots; i++ {
			if ctrls>>(i*8)&0x80 != 0 { // empty or deleted slot
				continue
			}
			kp, ep := ref.key(sm.Type, i), ref.elem(sm.Type, i)
			if sm.Type.Flags&mapIndirectKey != 0 {
				kp = *(*unsafe.Pointer)(kp)
			}
			if sm.Type.Flags&mapIndirectElem != 0 {
				ep = *(*unsafe.Pointer)(ep)
			}
			k, v := *(*K)(kp), *(*V)(ep)
			expected, ok := m[k]
			require.True(t, ok)
			require.Equal(t, expected, v)
			require.False(t, seen[k])
			seen[k] = true
		}
	}
	if sm.Data.dirLen == 0 {
		if sm.Data.dirPtr != nil {
			checkGroup(groupReference{data: sm.Data.dirPtr})
		}
	} else {
		var lastTab *testMapTable
		for i := range sm.Data.dirLen {
			table := sm.Data.directoryAt(uintptr(i))
			if table == lastTab {
				continue
			}
			lastTab = table
			require.Equal(t, uint64(table.capacity), (table.groups.lengthMask+1)*testMapGroupSlots)
			for j := range table.groups.lengthMask + 1 {
				checkGroup(table.groups.group(sm.Type, j))
			}
		}
	}
	require.Len(t, seen, len(m))
}

func TestMapABIMemoryConstants(t *testing.T) {
	cases := []struct {
		name     string
		actual   SwissMapWrap
		expected int
	}{
		{"MapStringToAny", ToSwissMap(make(map[string]any)), DefBucketMemoryUsageForMapStringToAny},
		{"SetString", ToSwissMap(make(map[string]struct{})), DefBucketMemoryUsageForSetString},
		{"SetFloat64", ToSwissMap(make(map[float64]struct{})), DefBucketMemoryUsageForSetFloat64},
		{"SetInt64", ToSwissMap(make(map[int64]struct{})), DefBucketMemoryUsageForSetInt64},
		// The decimal set stores a pointer, whose target does not affect group size.
		{"MapStringToDecimal", ToSwissMap(make(map[string]*int)), DefBucketMemoryUsageForMapStringToDecimal},
		{"MapStringToString", ToSwissMap(make(map[string]string)), DefBucketMemoryUsageForMapStringToString},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("groupSize=%d memoryConstant=%d", tc.actual.Type.GroupSize, tc.expected)
			metadata := reflect.ValueOf(*tc.actual.Type)
			for _, name := range []string{"SlotSize", "KeysOff", "KeyStride", "ElemsOff", "ElemStride", "ElemOff", "Flags"} {
				if field := metadata.FieldByName(name); field.IsValid() {
					t.Logf("%s=%d", name, field.Uint())
				}
			}
			require.Equal(t, uint64(tc.expected), tc.actual.Size())
		})
	}
}

func TestMemAwareMap(t *testing.T) {
	require.Panics(t, func() { new(MemAwareMap[int, int]).Init(nil) })
	m := NewMemAwareMap[uint64, byte](0)
	require.True(t, m.Empty())
	require.Zero(t, m.Count())
	bytes := int64(m.Bytes)
	for i := range 2000 {
		delta, inserted := m.SetExt(uint64(i), byte(i))
		require.True(t, inserted)
		bytes += delta
		require.Equal(t, uint64(bytes), m.Bytes)
		value, ok := m.Get(uint64(i))
		require.True(t, ok)
		require.Equal(t, byte(i), value)
		require.True(t, m.Exist(uint64(i)))
		_, inserted = m.SetExt(uint64(i), byte(i))
		require.False(t, inserted)
	}
	require.Equal(t, 2000, m.Len())
	require.Equal(t, m.Count(), m.Len())
	testMapABISlots(t, m.M)
	sm := ToSwissMap(m.M)
	require.Equal(t, sm.Size(), m.RealBytes())
	size := m.RealBytes()
	clear(m.M)
	require.True(t, m.Empty())
	require.Equal(t, size, m.RealBytes())
	require.Equal(t, int64(m.Bytes), bytes)
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
