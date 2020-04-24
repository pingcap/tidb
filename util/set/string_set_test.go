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
}
