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

package aggregate

import (
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/stretchr/testify/require"
)

func TestGroupKeyArena(t *testing.T) {
	var arena groupKeyArena
	require.Equal(t, "", arena.copyString(nil))
	require.Nil(t, arena.block)

	buf := []byte("key")
	keys := make([]string, 0, 10000)
	for i := range 10000 {
		buf = append(buf[:3], strconv.Itoa(i)...)
		keys = append(keys, arena.copyString(buf))
	}
	// Overwriting the source buffer and filling more blocks must not change
	// any key handed out before.
	copy(buf, "xxxxxxx")
	for i, key := range keys {
		require.Equal(t, "key"+strconv.Itoa(i), key)
	}
	require.Equal(t, uintptr(unsafe.Pointer(unsafe.StringData(keys[0])))+uintptr(len(keys[0])),
		uintptr(unsafe.Pointer(unsafe.StringData(keys[1]))), "small keys share a block")

	large := []byte(strings.Repeat("a", groupKeyArenaBlockSize/4+1))
	blockLen := len(arena.block)
	require.Equal(t, string(large), arena.copyString(large))
	require.Equal(t, blockLen, len(arena.block), "large keys bypass the arena")

	arena.reset()
	require.Nil(t, arena.block)
	require.Equal(t, "key0", keys[0])
}

func TestPartialResultsArena(t *testing.T) {
	var arena partialResultsArena
	slices := make([][]aggfuncs.PartialResult, 0, 1000)
	vals := make([]int, 3000)
	for i := range 1000 {
		s := arena.alloc(3)
		require.Len(t, s, 3)
		require.Equal(t, 3, cap(s))
		for j := range s {
			require.Nil(t, s[j])
			s[j] = aggfuncs.PartialResult(&vals[i*3+j])
		}
		slices = append(slices, s)
	}
	// Appending to a clipped slice reallocates instead of overwriting the
	// neighbor in the same block.
	_ = append(slices[0], nil)
	for i, s := range slices {
		for j := range s {
			require.Equal(t, unsafe.Pointer(&vals[i*3+j]), unsafe.Pointer(s[j]))
		}
	}

	big := arena.alloc(partialResultsArenaBlockSize + 1)
	require.Len(t, big, partialResultsArenaBlockSize+1)

	arena.reset()
	require.Nil(t, arena.block)
}
