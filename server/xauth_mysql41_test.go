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

package server

import (
	. "github.com/pingcap/check"
)

func (s *testUtilSuite) TestExtractNullTerminatedElement(c *C) {
	xauth41 := &saslMysql41Auth{}
	str := "mysql" + string(byte(0)) + "root" + string(byte(0)) + "0C6382C4"
	authZid, authCid, passwd := xauth41.extractNullTerminatedElement([]byte(str))
	c.Assert(string(authZid), Equals, "mysql")
	c.Assert(string(authCid), Equals, "root")
	c.Assert(string(passwd), Equals, "0C6382C4")
}
