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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/assert"
)

func RunStringSet(t *testing.T) {
	assert := assert.New(t)

	stringSet := set.NewStringSet()
	vals := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	for i := range vals {
		stringSet.Insert(vals[i])
		stringSet.Insert(vals[i])
		stringSet.Insert(vals[i])
		stringSet.Insert(vals[i])
		stringSet.Insert(vals[i])
	}
	assert.Equal(len(vals), stringSet.Count())

	assert.Equal(len(vals), len(stringSet))
	for i := range vals {
		assert.True(stringSet.Exist(vals[i]))
	}

	assert.False(stringSet.Exist("11"))

	stringSet = set.NewStringSet("1", "2", "3", "4", "5", "6")
	for i := 1; i < 7; i++ {
		assert.True(stringSet.Exist(fmt.Sprintf("%d", i)))
	}
	assert.False(stringSet.Exist("7"))

	s1 := set.NewStringSet("1", "2", "3")
	s2 := set.NewStringSet("4", "2", "3")
	s3 := s1.Intersection(s2)
	assert.Equal(set.NewStringSet("2", "3"), s3)

	s4 := set.NewStringSet("4", "5", "3")
	assert.Equal(set.NewStringSet("3"), s3.Intersection(s4))

	s5 := set.NewStringSet("4", "5")
	assert.Equal(set.NewStringSet(), s3.Intersection(s5))

	s6 := set.NewStringSet()
	assert.Equal(set.NewStringSet(), s3.Intersection(s6))
}
