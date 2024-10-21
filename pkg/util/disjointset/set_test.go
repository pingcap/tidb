// Copyright 2024 PingCAP, Inc.
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

func TestDisjointSet(t *testing.T) {
	set := NewSet[string](10)
	assert.False(t, set.InSameGroup("a", "b"))
	assert.Len(t, set.parent, 2)
	set.Union("a", "b")
	assert.True(t, set.InSameGroup("a", "b"))
	assert.False(t, set.InSameGroup("a", "c"))
	assert.Len(t, set.parent, 3)
	assert.False(t, set.InSameGroup("b", "c"))
	assert.Len(t, set.parent, 3)
	set.Union("b", "c")
	assert.True(t, set.InSameGroup("a", "c"))
	assert.True(t, set.InSameGroup("b", "c"))
	set.Union("d", "e")
	set.Union("e", "f")
	set.Union("f", "g")
	assert.Len(t, set.parent, 7)
	assert.False(t, set.InSameGroup("a", "d"))
	assert.True(t, set.InSameGroup("d", "g"))
	assert.False(t, set.InSameGroup("c", "g"))
	set.Union("a", "g")
	assert.True(t, set.InSameGroup("a", "d"))
	assert.True(t, set.InSameGroup("b", "g"))
	assert.True(t, set.InSameGroup("c", "f"))
	assert.True(t, set.InSameGroup("a", "e"))
	assert.True(t, set.InSameGroup("b", "c"))
}
