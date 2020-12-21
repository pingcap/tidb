// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
)

var _ = Suite(&testProcessInfoSuite{})

type testProcessInfoSuite struct {
}

func (s *testProcessInfoSuite) SetUpSuite(c *C) {
}

func (s *testProcessInfoSuite) TearDownSuite(c *C) {
}

func (s *testProcessInfoSuite) TestGlobalConnID(c *C) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Experimental.EnableGlobalKill = true
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()
	connID := util.GlobalConnID{
		Is64bits:    true,
		ServerID:    1001,
		LocalConnID: 123,
	}
	c.Assert(connID.ID(), Equals, (uint64(1001)<<41)|(uint64(123)<<1)|1)

	next := connID.NextID()
	c.Assert(next, Equals, (uint64(1001)<<41)|(uint64(124)<<1)|1)

	connID1, isTruncated, err := util.ParseGlobalConnID(next)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsFalse)
	c.Assert(connID1.ServerID, Equals, uint64(1001))
	c.Assert(connID1.LocalConnID, Equals, uint64(124))
	c.Assert(connID1.Is64bits, IsTrue)

	_, isTruncated, err = util.ParseGlobalConnID(101)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsTrue)

	_, _, err = util.ParseGlobalConnID(0x80000000_00000321)
	c.Assert(err, NotNil)

	connID2 := util.GlobalConnID{
		Is64bits:       true,
		ServerIDGetter: func() uint64 { return 2002 },
		LocalConnID:    123,
	}
	c.Assert(connID2.ID(), Equals, (uint64(2002)<<41)|(uint64(123)<<1)|1)
}
