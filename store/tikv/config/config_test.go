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

package config

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

var _ = SerialSuites(&testConfigSuite{})

func (s *testConfigSuite) TestParsePath(c *C) {
	etcdAddrs, disableGC, err := ParsePath("tikv://node1:2379,node2:2379")
	c.Assert(err, IsNil)
	c.Assert(etcdAddrs, DeepEquals, []string{"node1:2379", "node2:2379"})
	c.Assert(disableGC, IsFalse)

	_, _, err = ParsePath("tikv://node1:2379")
	c.Assert(err, IsNil)
	_, disableGC, err = ParsePath("tikv://node1:2379?disableGC=true")
	c.Assert(err, IsNil)
	c.Assert(disableGC, IsTrue)
}

func (s *testConfigSuite) TestTxnScopeValue(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/config/injectTxnScope", `return("bj")`), IsNil)
	isGlobal, v := GetTxnScopeFromConfig()
	c.Assert(isGlobal, IsFalse)
	c.Assert(v, Equals, "bj")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/config/injectTxnScope"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/config/injectTxnScope", `return("")`), IsNil)
	isGlobal, v = GetTxnScopeFromConfig()
	c.Assert(isGlobal, IsTrue)
	c.Assert(v, Equals, "global")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/config/injectTxnScope"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/config/injectTxnScope", `return("global")`), IsNil)
	isGlobal, v = GetTxnScopeFromConfig()
	c.Assert(isGlobal, IsFalse)
	c.Assert(v, Equals, "global")
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/config/injectTxnScope"), IsNil)
}
