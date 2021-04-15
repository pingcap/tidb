// Copyright 2021 PingCAP, Inc.
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

package placement

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct{}

func (t *testCommonSuite) TestGroup(c *C) {
	c.Assert(GroupID(1), Equals, "TiDB_DDL_1")
	c.Assert(GroupID(90), Equals, "TiDB_DDL_90")
	c.Assert(GroupID(-1), Equals, "TiDB_DDL_-1")
}
