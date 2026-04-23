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

package arena_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/arena"
	"github.com/stretchr/testify/assert"
)

const (
	arenaCap       = 1000
	allocCapSmall  = 10
	allocCapMedium = 20
	allocCapOut    = 1024
)

func RunSimpleArenaAllocator(t *testing.T) {
	a := arena.ExportedNewAllocator(arenaCap)
	slice := a.Alloc(allocCapSmall)
	assert.Equal(t, allocCapSmall, arena.ExportedGetSimpleAllocatorOff(a))
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapSmall, cap(slice))

	slice = a.Alloc(allocCapMedium)
	assert.Equal(t, allocCapSmall+allocCapMedium, arena.ExportedGetSimpleAllocatorOff(a))
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapMedium, cap(slice))

	slice = a.Alloc(allocCapOut)
	assert.Equal(t, allocCapSmall+allocCapMedium, arena.ExportedGetSimpleAllocatorOff(a))
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapOut, cap(slice))

	slice = a.AllocWithLen(2, allocCapSmall)
	assert.Equal(t, allocCapSmall+allocCapMedium+allocCapSmall, arena.ExportedGetSimpleAllocatorOff(a))
	assert.Len(t, slice, 2)
	assert.Equal(t, allocCapSmall, cap(slice))

	a.Reset()
	assert.Zero(t, arena.ExportedGetSimpleAllocatorOff(a))
	assert.Equal(t, arenaCap, cap(arena.ExportedGetSimpleAllocatorArena(a)))
}

func RunStdAllocator(t *testing.T) {
	slice := arena.ExportedStdAllocator().Alloc(allocCapMedium)
	assert.Len(t, slice, 0)
	assert.Equal(t, allocCapMedium, cap(slice))

	slice = arena.ExportedStdAllocator().AllocWithLen(allocCapSmall, allocCapMedium)
	assert.Len(t, slice, allocCapSmall)
	assert.Equal(t, allocCapMedium, cap(slice))
}
