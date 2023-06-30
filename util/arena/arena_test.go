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

package arena

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	arenaCap       = 1000
	allocCapSmall  = 10
	allocCapMedium = 20
	allocCapOut    = 1024
)

func TestSimpleArenaAllocator(t *testing.T) {
	arena := NewAllocator(arenaCap)
	slice := arena.Alloc(allocCapSmall)
	require.Equal(t, allocCapSmall, arena.off)
	require.Len(t, slice, 0)
	require.Equal(t, allocCapSmall, cap(slice))

	slice = arena.Alloc(allocCapMedium)
	require.Equal(t, allocCapSmall+allocCapMedium, arena.off)
	require.Len(t, slice, 0)
	require.Equal(t, allocCapMedium, cap(slice))

	slice = arena.Alloc(allocCapOut)
	require.Equal(t, allocCapSmall+allocCapMedium, arena.off)
	require.Len(t, slice, 0)
	require.Equal(t, allocCapOut, cap(slice))

	slice = arena.AllocWithLen(2, allocCapSmall)
	require.Equal(t, allocCapSmall+allocCapMedium+allocCapSmall, arena.off)
	require.Len(t, slice, 2)
	require.Equal(t, allocCapSmall, cap(slice))

	arena.Reset()
	require.Zero(t, arena.off)
	require.Equal(t, arenaCap, cap(arena.arena))
}

func TestStdAllocator(t *testing.T) {
	slice := StdAllocator.Alloc(allocCapMedium)
	require.Len(t, slice, 0)
	require.Equal(t, allocCapMedium, cap(slice))

	slice = StdAllocator.AllocWithLen(allocCapSmall, allocCapMedium)
	require.Len(t, slice, allocCapSmall)
	require.Equal(t, allocCapMedium, cap(slice))
}
