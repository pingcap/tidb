// Copyright 2018 PingCAP, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package timeutil

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTimeSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testTimeSuite struct{}

func (s *testTimeSuite) TestgetTZNameFromFileName(c *C) {
	tz, err := inferTZNameFromFileName("/usr/share/zoneinfo/Asia/Shanghai")

	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "Asia/Shanghai")

	tz, err = inferTZNameFromFileName("/usr/share/zoneinfo.default/Asia/Shanghai")

	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "Asia/Shanghai")
}

func (s *testTimeSuite) TestLocal(c *C) {
	os.Setenv("TZ", "Asia/Shanghai")
	systemTZ.Store(InferSystemTZ())
	loc := SystemLocation()
	c.Assert(systemTZ.Load(), Equals, "Asia/Shanghai")
	c.Assert(loc.String(), Equals, "Asia/Shanghai")

	os.Setenv("TZ", "UTC")
	// reset systemTZ
	systemTZ.Store(InferSystemTZ())
	loc = SystemLocation()
	c.Assert(loc.String(), Equals, "UTC")

	os.Setenv("TZ", "")
	// reset systemTZ
	systemTZ.Store(InferSystemTZ())
	loc = SystemLocation()
	c.Assert(loc.String(), Equals, "UTC")
	os.Unsetenv("TZ")
}
