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

package json

import (
	. "github.com/pingcap/check"
)

func (s *testJSONSuite) TestCastToInt(c *C) {
	var tests = []struct {
		In  string
		Out int64
	}{
		{`{}`, 0},
		{`[]`, 0},
		{`3`, 3},
		{`-3`, -3},
		{`4.5`, 4},
		{`true`, 1},
		{`false`, 0},
		{`null`, 0},
		{`"hello"`, 0},
		{`"1234"`, 1234},
	}
	for _, tt := range tests {
		j := mustParseFromString(tt.In)
		casted, err := j.CastToInt()
		c.Assert(err, IsNil)
		c.Assert(casted, Equals, tt.Out)
	}
}

func (s *testJSONSuite) TestCastToReal(c *C) {
	var tests = []struct {
		In  string
		Out float64
	}{
		{`{}`, 0},
		{`[]`, 0},
		{`3`, 3},
		{`-3`, -3},
		{`4.5`, 4.5},
		{`true`, 1},
		{`false`, 0},
		{`null`, 0},
		{`"hello"`, 0},
		{`"1234"`, 1234},
	}
	for _, tt := range tests {
		j := mustParseFromString(tt.In)
		casted, err := j.CastToReal()
		c.Assert(err, IsNil)
		c.Assert(casted, Equals, tt.Out)
	}
}
