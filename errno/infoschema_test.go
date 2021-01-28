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

package errno

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testErrno{})

type testErrno struct{}

func (s *testErrno) TestGetSetSysVars(c *C) {

	IncrementError(123, "user", "host")
	IncrementError(321, "user2", "host2")

	globalCopy := GlobalStats()
	userCopy := UserStats()
	hostCopy := HostStats()

	IncrementError(123, "user", "host")
	IncrementError(999, "user2", "host2")
	IncrementError(123, "user3", "host")

	// global stats
	c.Assert(stats.global[123].ErrorCount, Equals, 3)
	c.Assert(globalCopy[123].ErrorCount, Equals, 1)

	// user stats
	c.Assert(len(stats.users), Equals, 3)
	c.Assert(len(userCopy), Equals, 2)
	c.Assert(stats.users["user"][123].ErrorCount, Equals, 2)
	c.Assert(userCopy["user"][123].ErrorCount, Equals, 1)

	// ensure there is no user3 in userCopy
	_, ok := userCopy["user3"]
	c.Assert(ok, IsFalse)
	_, ok = stats.users["user3"]
	c.Assert(ok, IsTrue)

	// host stats
	c.Assert(len(stats.hosts), Equals, 2)
	c.Assert(len(hostCopy), Equals, 2)
	IncrementError(123, "user3", "newhost")
	c.Assert(len(stats.hosts), Equals, 3)
	c.Assert(len(hostCopy), Equals, 2)

	// ensure there is no newhost in hostCopy
	_, ok = hostCopy["newhost"]
	c.Assert(ok, IsFalse)
	_, ok = stats.hosts["newhost"]
	c.Assert(ok, IsTrue)

}
