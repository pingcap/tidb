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

package dashbase

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testDashbaseSuite{})

type testDashbaseSuite struct {
}

func checkParseConnectionOption(c *C, input string, expected *ConnectionOption) {
	r, success := ParseConnectionOption(input)
	if expected == nil {
		c.Assert(r, IsNil, Commentf("Input = %s", input))
		c.Assert(success, IsFalse, Commentf("Input = %s", input))
	} else {
		c.Assert(r, NotNil, Commentf("Input = %s", input))
		c.Assert(success, IsTrue, Commentf("Input = %s", input))
		c.Assert(r.FirehoseHostname, Equals, expected.FirehoseHostname, Commentf("Input = %s", input))
		c.Assert(r.FirehosePort, Equals, expected.FirehosePort, Commentf("Input = %s", input))
		c.Assert(r.ProxyHostname, Equals, expected.ProxyHostname, Commentf("Input = %s", input))
		c.Assert(r.ProxyPort, Equals, expected.ProxyPort, Commentf("Input = %s", input))
	}
}

func (s *testDashbaseSuite) TestParseConnectionOption(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		input    string
		expected *ConnectionOption
	}{
		{"", nil},
		{"127.0.0.1", &ConnectionOption{"127.0.0.1", DefaultFirehosePort, "127.0.0.1", DefaultProxyPort}},
		{"localhost", &ConnectionOption{"localhost", DefaultFirehosePort, "localhost", DefaultProxyPort}},
		{"127.0.0.1:1234", &ConnectionOption{"127.0.0.1", 1234, "127.0.0.1", DefaultProxyPort}},
		{"127.0.0.1;192.168.0.1", &ConnectionOption{"127.0.0.1", DefaultFirehosePort, "192.168.0.1", DefaultProxyPort}},
		{"127.0.0.1;192.168.0.1:4321", &ConnectionOption{"127.0.0.1", DefaultFirehosePort, "192.168.0.1", 4321}},
		{"127.0.0.1:1234;192.168.0.1", &ConnectionOption{"127.0.0.1", 1234, "192.168.0.1", DefaultProxyPort}},
		{"127.0.0.1:1234;192.168.0.1:4321", &ConnectionOption{"127.0.0.1", 1234, "192.168.0.1", 4321}},
		{"127.0.0.1:", nil},
		{"127.0.0.1:123;", nil},
		{"127.0.0.1;", nil},
		{"127.0.0.1:123;123:", nil},
		{"127.0.0.1:123;localhost;", nil},
		{":123;localhost", nil},
		{"localhost:123;:345", nil},
	}
	for _, testCase := range tests {
		checkParseConnectionOption(c, testCase.input, testCase.expected)
	}
}
