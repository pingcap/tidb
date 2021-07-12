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

func TestSimpleArenaAllocator(t *testing.T) {
	arena := NewAllocator(1000)
	slice := arena.Alloc(10)
	assert.Equal(t, 10, arena.off, "off not match, expect 10 bug got")
	assert.False(t, len(slice) != 0 || cap(slice) != 10, "slice length or cap not match")

	slice = arena.Alloc(20)
	assert.Equal(t, 30, arena.off, "off not match, expect 30 bug got")
	assert.False(t, len(slice) != 0 || cap(slice) != 20, "slice length or cap not match")

	slice = arena.Alloc(1024)
	assert.Equal(t, 30, arena.off, "off not match, expect 30 bug got")
	assert.False(t, len(slice) != 0 || cap(slice) != 1024, "slice length or cap not match")

	slice = arena.AllocWithLen(2, 10)
	assert.Equal(t, 40, arena.off, "off not match, expect 40 bug got")
	assert.False(t, len(slice) != 2 || cap(slice) != 10, "slice length or cap not match")

	arena.Reset()
	assert.False(t, arena.off != 0 || cap(arena.arena) != 1000, "off or cap not match")
}

func TestStdAllocator(t *testing.T) {
	slice := StdAllocator.Alloc(20)
	assert.Equal(t, 0, len(slice), "length not match")
	assert.Equal(t, 20, cap(slice), "cap not match")

	slice = StdAllocator.AllocWithLen(10, 20)
	assert.Equal(t, 10, len(slice), "length not match")
	assert.Equal(t, 20, cap(slice), "cap not match")
}
