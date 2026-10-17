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

//go:build go1.27 && !go1.28

package hack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testMapTable = mapTable

const testMapGroupSlots = mapGroupSlots

func TestMapABIZeroStrideCapacity(t *testing.T) {
	testMapABIGroupCapacity(t, map[uint64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}})
	testMapABIGroupCapacity(t, map[struct{}]uint64{{}: 1})
	testMapABIGroupCapacity(t, map[struct{}]struct{}{{}: {}})
}

func testMapABIGroupCapacity[K comparable, V any](t *testing.T, m map[K]V) {
	t.Helper()
	sm := ToSwissMap(m)
	ref := groupReference{data: sm.Data.dirPtr}
	capacity := ref.cap(sm.Type)
	require.Equal(t, uint64(mapGroupSlots), capacity)
	seen := make(map[K]bool)
	for i := range uintptr(capacity) {
		kp, ep := ref.key(sm.Type, i), ref.elem(sm.Type, i)
		require.Less(t, uintptr(kp)-uintptr(ref.data), sm.Type.GroupSize)
		require.Less(t, uintptr(ep)-uintptr(ref.data), sm.Type.GroupSize)
		k, v := *(*K)(kp), *(*V)(ep)
		t.Logf("slot=%d key=%v elem=%v", i, k, v)
		if *(*uint64)(ref.data)>>(i*8)&0x80 == 0 {
			expected, ok := m[k]
			require.True(t, ok)
			require.Equal(t, expected, v)
			require.False(t, seen[k])
			seen[k] = true
		}
	}
	require.Len(t, seen, len(m))
}

// These samples have equally sized, nonzero keys and elements.
func testMapSlotLayout(t *testing.T, typ *mapType, slotSize, elemOff uintptr) {
	t.Helper()
	require.Equal(t, groupSlotsOffset, typ.KeysOff)
	if mapSplitGroupEnabled { // selected by the compiler's experiment build tag
		require.Equal(t, elemOff, typ.KeyStride)
		require.Equal(t, slotSize-elemOff, typ.ElemStride)
		require.Equal(t, groupSlotsOffset+mapGroupSlots*elemOff, typ.ElemsOff)
	} else {
		require.Equal(t, slotSize, typ.KeyStride)
		require.Equal(t, slotSize, typ.ElemStride)
		require.Equal(t, elemOff, typ.ElemOff)
		require.Equal(t, groupSlotsOffset+elemOff, typ.ElemsOff)
	}
}
