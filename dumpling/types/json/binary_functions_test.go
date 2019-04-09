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
package json

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testJSONFuncSuite{})

type testJSONFuncSuite struct{}

func (s *testJSONFuncSuite) TestdecodeEscapedUnicode(c *C) {
	c.Parallel()
	in := "597d"
	r, size, err := decodeEscapedUnicode([]byte(in))
	c.Assert(string(r[:]), Equals, "好\x00")
	c.Assert(size, Equals, 3)
	c.Assert(err, IsNil)

}
