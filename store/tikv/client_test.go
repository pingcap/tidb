// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testClientSuite struct {
}

var _ = Suite(&testClientSuite{})

func (s *testClientSuite) TestConn(c *C) {
	client := newRPCClient(config.Security{})

	addr := "127.0.0.1:6379"
	conn1, err := client.getConn(addr)
	c.Assert(err, IsNil)

	conn2, err := client.getConn(addr)
	c.Assert(err, IsNil)
	c.Assert(conn2, Not(Equals), conn1)

	client.Close()
	conn3, err := client.getConn(addr)
	c.Assert(err, NotNil)
	c.Assert(conn3, IsNil)
}
