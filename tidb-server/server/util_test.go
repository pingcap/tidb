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

package server

import (
	. "github.com/pingcap/check"
	mysql "github.com/pingcap/tidb/mysqldef"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (s *testUtilSuite) TestDumpBinaryTime(c *C) {
	tests := []struct {
		Input  string
		Expect []byte
	}{
		{"0000-00-00 00:00:00.0000000", []byte{11, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0}},
		{"0000-00-00", []byte{4, 1, 0, 1, 1}},
		{"0000-00-00 00:00:00.0000000", []byte{0}},
	}

	t, err := mysql.ParseTimestamp(tests[0].Input)
	c.Assert(err, IsNil)
	d := dumpBinaryDateTime(t, nil)
	c.Assert(string(d), Equals, string(tests[0].Expect))
	t, err = mysql.ParseDatetime(tests[0].Input)
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(t, nil)
	c.Assert(string(d), Equals, string(tests[0].Expect))

	t, err = mysql.ParseDate(tests[1].Input)
	c.Assert(err, IsNil)
	d = dumpBinaryDateTime(t, nil)
	c.Assert(string(d), Equals, string(tests[1].Expect))

	myDuration, err := mysql.ParseDuration(tests[2].Input, 6)
	c.Assert(err, IsNil)
	d = dumpBinaryTime(myDuration.Duration)
	c.Assert(string(d), Equals, string(tests[2].Expect))
}
