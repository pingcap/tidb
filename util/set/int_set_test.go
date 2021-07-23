// Copyright 2019 PingCAP, Inc.
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

package set

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntSet(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	set := NewIntSet()
	vals := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := range vals {
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
	}
	assert.Equal(len(vals), set.Count())

	assert.Equal(len(vals), len(set))
	for i := range vals {
		assert.True(set.Exist(vals[i]))
	}

	assert.False(set.Exist(11))
}

func TestInt64Set(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	set := NewInt64Set()
	vals := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := range vals {
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
	}

	assert.Equal(len(vals), len(set))
	for i := range vals {
		assert.True(set.Exist(vals[i]))
	}

	assert.False(set.Exist(11))

	set = NewInt64Set(1, 2, 3, 4, 5, 6)
	for i := 1; i < 7; i++ {
		assert.True(set.Exist(int64(i)))
	}
	assert.False(set.Exist(7))
}
