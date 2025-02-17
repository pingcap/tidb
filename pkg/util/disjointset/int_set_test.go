// Copyright 2018 PingCAP, Inc.
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

package disjointset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntDisjointSet(t *testing.T) {
	set := NewIntSet(10)
	assert.Len(t, set.parent, 10)
	for i := range set.parent {
		assert.Equal(t, i, set.parent[i])
	}
	set.Union(0, 1)
	set.Union(1, 3)
	set.Union(4, 2)
	set.Union(2, 6)
	set.Union(3, 5)
	set.Union(7, 8)
	set.Union(9, 6)
	assert.Equal(t, set.FindRoot(0), set.FindRoot(1))
	assert.Equal(t, set.FindRoot(3), set.FindRoot(1))
	assert.Equal(t, set.FindRoot(5), set.FindRoot(1))
	assert.Equal(t, set.FindRoot(2), set.FindRoot(4))
	assert.Equal(t, set.FindRoot(6), set.FindRoot(4))
	assert.Equal(t, set.FindRoot(9), set.FindRoot(2))
	assert.Equal(t, set.FindRoot(7), set.FindRoot(8))
}
