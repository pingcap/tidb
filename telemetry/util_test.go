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
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testUtilSuite) TestHashString(c *C) {
	c.Parallel()

	c.Assert(hashString("127.0.0.1"), Equals, "4b84b15bff6ee5796152495a230e45e3d7e947d9")
}

func (s *testUtilSuite) TestParseAddress(c *C) {
	c.Parallel()

	cases := []struct {
		src          string
		expectedHost string
		expectedPort string
	}{
		{"12345", "12345", ""},
		{"12345:567", "12345", "567"},
		{"store1", "store1", ""},
		{"0.0.0.0:4000", "0.0.0.0", "4000"},
		{"my_addr:12345", "my_addr", "12345"},
		{"my_addr:my_port", "my_addr:my_port", ""},
		{"my_addr:my_port:12345", "my_addr:my_port", "12345"},
		{"my_addr::12345", "my_addr:", "12345"},
		{"my_addr:12345x", "my_addr:12345x", ""},
		{"[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443", "[2001:db8:85a3:8d3:1319:8a2e:370:7348]", "443"},
	}
	for _, tt := range cases {
		host, port := parseAddressAndHash(tt.src)
		c.Assert(host, Equals, hashString(tt.expectedHost))
		c.Assert(port, Equals, tt.expectedPort)
	}
}
