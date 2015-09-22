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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testAssignSuite{})

type testAssignSuite struct {
}

func (t *testAssignSuite) TestAssign(c *C) {
	a := Assignment{
		ColName: "id",
		Expr:    Value{1},
	}

	s := a.String()
	c.Assert(len(s), Greater, 0)
}
