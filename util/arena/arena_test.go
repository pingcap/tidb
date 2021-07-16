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
// See the License for the specific language governing permissions and
// limitations under the License.

package arena

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	arenaCap       = 1000
	allocCapSmall  = 10
	allocCapMedium = 20
	allocCapOut    = 1024
)

func TestSimpleArenaAllocator(t *testing.T) {
	t.Parallel()

	arena := NewAllocator(arenaCap)
	slice := arena.Alloc(allocCapSmall)
	assert.Equal(t, allocCapSmall, arena.off)
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapSmall, cap(slice))

	slice = arena.Alloc(allocCapMedium)
	assert.Equal(t, allocCapSmall+allocCapMedium, arena.off)
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapMedium, cap(slice))

	slice = arena.Alloc(allocCapOut)
	assert.Equal(t, allocCapSmall+allocCapMedium, arena.off)
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapOut, cap(slice))

	slice = arena.AllocWithLen(2, allocCapSmall)
	assert.Equal(t, allocCapSmall+allocCapMedium+allocCapSmall, arena.off)
	assert.Len(t, slice, 2)
	assert.Equal(t, allocCapSmall, cap(slice))

	arena.Reset()
	assert.Zero(t, arena.off)
	assert.Equal(t, arenaCap, cap(arena.arena))
}

func TestStdAllocator(t *testing.T) {
	t.Parallel()
	slice := StdAllocator.Alloc(allocCapMedium)
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapMedium, cap(slice))

	slice = StdAllocator.AllocWithLen(allocCapSmall, allocCapMedium)
	assert.Len(t, slice, allocCapSmall)
	assert.Equal(t, allocCapMedium, cap(slice))
}
