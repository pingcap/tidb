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

package util_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util"
)

var _ = Suite(&testGlobalConnIDSuite{})

type testGlobalConnIDSuite struct {
}

func (s *testGlobalConnIDSuite) SetUpSuite(c *C) {
}

func (s *testGlobalConnIDSuite) TearDownSuite(c *C) {
}

func (s *testGlobalConnIDSuite) TestParse(c *C) {
	connID := util.GlobalConnID{
		Is64bits:    true,
		ServerID:    1001,
		LocalConnID: 123,
	}
	c.Assert(connID.ID(), Equals, (uint64(1001)<<41)|(uint64(123)<<1)|1)

	var (
		err         error
		isTruncated bool
	)

	// exceeds int64
	_, _, err = util.ParseGlobalConnID(0x80000000_00000321)
	c.Assert(err, NotNil)

	// 64bits truncated
	_, isTruncated, err = util.ParseGlobalConnID(101)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsTrue)

	// 64bits
	id1 := (uint64(1001) << 41) | (uint64(123) << 1) | 1
	connID1, isTruncated, err := util.ParseGlobalConnID(id1)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsFalse)
	c.Assert(connID1.ServerID, Equals, uint64(1001))
	c.Assert(connID1.LocalConnID, Equals, uint64(123))
	c.Assert(connID1.Is64bits, IsTrue)

	// exceeds uint32
	_, _, err = util.ParseGlobalConnID(0x1_00000320)
	c.Assert(err, NotNil)

	// 32bits
	id2 := (uint64(2002) << 21) | (uint64(321) << 1)
	connID2, isTruncated, err := util.ParseGlobalConnID(id2)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsFalse)
	c.Assert(connID2.ServerID, Equals, uint64(2002))
	c.Assert(connID2.LocalConnID, Equals, uint64(321))
	c.Assert(connID2.Is64bits, IsFalse)
}
