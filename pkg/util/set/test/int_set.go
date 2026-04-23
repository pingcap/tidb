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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package set_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/assert"
)

func RunIntSet(t *testing.T) {
	assert := assert.New(t)

	intSet := set.NewIntSet()
	vals := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := range vals {
		intSet.Insert(vals[i])
		intSet.Insert(vals[i])
		intSet.Insert(vals[i])
		intSet.Insert(vals[i])
		intSet.Insert(vals[i])
	}
	assert.Equal(len(vals), intSet.Count())

	assert.Equal(len(vals), len(intSet))
	for i := range vals {
		assert.True(intSet.Exist(vals[i]))
	}

	assert.False(intSet.Exist(11))
}

func RunInt64Set(t *testing.T) {
	assert := assert.New(t)

	int64Set := set.NewInt64Set()
	vals := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := range vals {
		int64Set.Insert(vals[i])
		int64Set.Insert(vals[i])
		int64Set.Insert(vals[i])
		int64Set.Insert(vals[i])
		int64Set.Insert(vals[i])
	}

	assert.Equal(len(vals), len(int64Set))
	for i := range vals {
		assert.True(int64Set.Exist(vals[i]))
	}

	assert.False(int64Set.Exist(11))

	int64Set = set.NewInt64Set(1, 2, 3, 4, 5, 6)
	for i := 1; i < 7; i++ {
		assert.True(int64Set.Exist(int64(i)))
	}
	assert.False(int64Set.Exist(7))
}
