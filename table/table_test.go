// Copyright 2017 PingCAP, Inc.
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

package table

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct{}

func (ts *testTableSuite) TestSlice(c *C) {
	sl := make(Slice, 2)
	len := sl.Len()
	c.Assert(len, Equals, 2)
	sl.Swap(0, 1)
}
