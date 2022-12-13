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
	"fmt"

	"github.com/pingcap/check"
)

var _ = check.Suite(&stringSetTestSuite{})

type stringSetTestSuite struct{}

func (s *stringSetTestSuite) TestStringSet(c *check.C) {
	set := NewStringSet()
	vals := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	for i := range vals {
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
		set.Insert(vals[i])
	}
	c.Assert(set.Count(), check.Equals, len(vals))

	c.Assert(len(set), check.Equals, len(vals))
	for i := range vals {
		c.Assert(set.Exist(vals[i]), check.IsTrue)
	}

	c.Assert(set.Exist("11"), check.IsFalse)

	set = NewStringSet("1", "2", "3", "4", "5", "6")
	for i := 1; i < 7; i++ {
		c.Assert(set.Exist(fmt.Sprintf("%d", i)), check.IsTrue)
	}
	c.Assert(set.Exist("7"), check.IsFalse)

	s1 := NewStringSet("1", "2", "3")
	s2 := NewStringSet("4", "2", "3")
	s3 := s1.Intersection(s2)
	c.Assert(s3, check.DeepEquals, NewStringSet("2", "3"))

	s4 := NewStringSet("4", "5", "3")
	c.Assert(s3.Intersection(s4), check.DeepEquals, NewStringSet("3"))

	s5 := NewStringSet("4", "5")
	c.Assert(s3.Intersection(s5), check.DeepEquals, NewStringSet())

	s6 := NewStringSet()
	c.Assert(s3.Intersection(s6), check.DeepEquals, NewStringSet())
}
