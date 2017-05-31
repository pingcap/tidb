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

package json

import (
	. "github.com/pingcap/check"
)

func (s *testJSONSuite) TestJSONPathExprLegRe(c *C) {
	var pathExpr = "$.key1[3][*].*.key3"
	matches := jsonPathExprLegRe.FindAllString(pathExpr, -1)
	c.Assert(len(matches), Equals, 5)
	c.Assert(matches[0], Equals, ".key1")
	c.Assert(matches[1], Equals, "[3]")
	c.Assert(matches[2], Equals, "[*]")
	c.Assert(matches[3], Equals, ".*")
	c.Assert(matches[4], Equals, ".key3")
}

func (s *testJSONSuite) TestValidatePathExpr(c *C) {
	var pathExprStr = "   $ .   key1  [  3  ]\t[*].*.key3"
	_, err := ParseJSONPathExpr(pathExprStr)
	c.Assert(err, IsNil)
}
