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

package hbasekv

import . "github.com/pingcap/check"

var _ = Suite(&testHBaseSuite{})

type testHBaseSuite struct {
}

func (t *testHBaseSuite) TestParseDSN(c *C) {
	zks, oracle, table, err := parseDSN("zk1.com,zk2,192.168.0.1|localhost:1234/tidb")
	c.Assert(zks, DeepEquals, []string{"zk1.com", "zk2", "192.168.0.1"})
	c.Assert(oracle, Equals, "localhost:1234")
	c.Assert(table, Equals, "tidb")
	c.Assert(err, IsNil)

	zks, oracle, table, err = parseDSN("zk1,zk2/tidb")
	c.Assert(zks, DeepEquals, []string{"zk1", "zk2"})
	c.Assert(oracle, Equals, "")
	c.Assert(table, Equals, "tidb")
	c.Assert(err, IsNil)

	_, _, _, err = parseDSN("zk1,zk2|localhost:1234")
	c.Assert(err, NotNil)
}
