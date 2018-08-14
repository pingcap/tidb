// Copyright 2018-present, PingCAP, Inc.
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

package mocktikv

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testRPCHandlerSuite{})

type testRPCHandlerSuite struct {
}

func (s *testRPCHandlerSuite) TestConstructTimezone(c *C) {
	loc, err := constructTimeZone("", 12800)
	c.Assert(err, IsNil)
	c.Assert(loc.String(), Equals, "")

	loc, err = constructTimeZone("", -8000000)
	c.Assert(err, IsNil)
	c.Assert(loc.String(), Equals, "")

	loc, err = constructTimeZone("", 8000000)
	c.Assert(err, IsNil)
	c.Assert(loc.String(), Equals, "")

	loc, err = constructTimeZone("UTC", 12800)
	c.Assert(err, IsNil)
	c.Assert(loc.String(), Equals, "UTC")

	loc, err = constructTimeZone("Asia/Shanghai", 0)
	c.Assert(err, IsNil)
	c.Assert(loc.String(), Equals, "Asia/Shanghai")

	loc, err = constructTimeZone("asia/not-exist", 12800)
	c.Assert(err.Error(), Equals, "invalid name for timezone asia/not-exist")
}
