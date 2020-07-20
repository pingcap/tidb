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

	"github.com/pingcap/check"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&float64SetTestSuite{})

type float64SetTestSuite struct{}

func (s *float64SetTestSuite) TestFloat64Set(c *check.C) {
	set := NewFloat64Set()
	vals := []float64{1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0}
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

	c.Assert(set.Exist(3), check.IsFalse)
}
