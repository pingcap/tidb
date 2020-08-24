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

package telemetry

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testClusterHardwareSuite{})

type testClusterHardwareSuite struct{}

func (s *testClusterHardwareSuite) TestNormalizeDiskName(c *C) {
	c.Parallel()

	c.Assert(normalizeDiskName("/dev/sdb"), Equals, "sdb")
	c.Assert(normalizeDiskName("sda"), Equals, "sda")
}

func (s *testClusterHardwareSuite) TestIsNormalizedDiskNameAllowed(c *C) {
	c.Parallel()

	passList := []string{"disk1s4", "rootfs", "devtmpfs", "sda", "sda1", "sdb", "sdb3", "sdc", "nvme0", "nvme0n1", "nvme0n1p0"}
	for _, n := range passList {
		c.Assert(isNormalizedDiskNameAllowed(n), Equals, true)
	}

	failList := []string{"foo", "/rootfs", "asmdisk01p1"}
	for _, n := range failList {
		c.Assert(isNormalizedDiskNameAllowed(n), Equals, false)
	}
}

func (s *testClusterHardwareSuite) TestNormalizeFieldName(c *C) {
	c.Parallel()

	c.Assert(normalizeFieldName("deviceName"), Equals, "deviceName")
	c.Assert(normalizeFieldName("device-name"), Equals, "deviceName")
	c.Assert(normalizeFieldName("device_name"), Equals, "deviceName")
	c.Assert(normalizeFieldName("l1-cache-size"), Equals, "l1CacheSize")
	c.Assert(normalizeFieldName("free-percent"), Equals, "freePercent")
}
