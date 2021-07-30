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
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTimeSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testTimeSuite struct{}

func (s *testTimeSuite) TestgetTZNameFromFileName(c *C) {
	tz, err := inferTZNameFromFileName("/user/share/zoneinfo/Asia/Shanghai")

	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "Asia/Shanghai")

	tz, err = inferTZNameFromFileName("/usr/share/zoneinfo.default/Asia/Shanghai")

	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "Asia/Shanghai")
}

func (s *testTimeSuite) TestLocal(c *C) {
	os.Setenv("TZ", "Asia/Shanghai")
	systemTZ = InferSystemTZ()
	loc := SystemLocation()
	c.Assert(systemTZ, Equals, "Asia/Shanghai")
	c.Assert(loc.String(), Equals, "Asia/Shanghai")

	os.Setenv("TZ", "UTC")
	// reset systemTZ
	systemTZ = InferSystemTZ()
	loc = SystemLocation()
	c.Assert(loc.String(), Equals, "UTC")

	os.Setenv("TZ", "")
	// reset systemTZ
	systemTZ = InferSystemTZ()
	loc = SystemLocation()
	c.Assert(loc.String(), Equals, "UTC")
	os.Unsetenv("TZ")
}

func (s *testTimeSuite) TestInferOneStepLinkForPath(c *C) {
	os.Remove("/tmp/testlink1")
	os.Remove("/tmp/testlink2")
	os.Remove("/tmp/testlink3")
	var link2, link3 string
	var err error
	var link1 *os.File
	link1, err = os.Create("/tmp/testlink1")
	c.Assert(err, IsNil)
	err = os.Symlink(link1.Name(), "/tmp/testlink2")
	c.Assert(err, IsNil)
	err = os.Symlink("/tmp/testlink2", "/tmp/testlink3")
	c.Assert(err, IsNil)
	link2, err = inferOneStepLinkForPath("/tmp/testlink3")
	c.Assert(err, IsNil)
	c.Assert(link2, Equals, "/tmp/testlink2")
	link3, err = filepath.EvalSymlinks("/tmp/testlink3")
	c.Assert(err, IsNil)
	c.Assert(strings.Index(link3, link1.Name()), Not(Equals), -1)
}
