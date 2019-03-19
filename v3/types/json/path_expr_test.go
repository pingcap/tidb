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

func (s *testJSONSuite) TestContainsAnyAsterisk(c *C) {
	var tests = []struct {
		exprString        string
		containsAsterisks bool
	}{
		{"$.a[b]", false},
		{"$.a[*]", true},
		{"$.*[b]", true},
		{"$**.a[b]", true},
	}
	for _, tt := range tests {
		pe, err := ParseJSONPathExpr(tt.exprString)
		c.Assert(err, IsNil)
		c.Assert(pe.flags.containsAnyAsterisk(), Equals, tt.containsAsterisks)
	}
}

func (s *testJSONSuite) TestValidatePathExpr(c *C) {
	var tests = []struct {
		exprString string
		success    bool
		legs       int
	}{
		{`   $  `, true, 0},
		{"   $ .   key1  [  3  ]\t[*].*.key3", true, 5},
		{"   $ .   key1  [  3  ]**[*].*.key3", true, 6},
		{`$."key1 string"[  3  ][*].*.key3`, true, 5},
		{`$."hello \"escaped quotes\" world\\n"[3][*].*.key3`, true, 5},

		{`$.\"escaped quotes\"[3][*].*.key3`, false, 0},
		{`$.hello \"escaped quotes\" world[3][*].*.key3`, false, 0},
		{`$NoValidLegsHere`, false, 0},
		{`$        No Valid Legs Here .a.b.c`, false, 0},
	}

	for _, tt := range tests {
		pe, err := ParseJSONPathExpr(tt.exprString)
		if tt.success {
			c.Assert(err, IsNil)
			c.Assert(len(pe.legs), Equals, tt.legs)
		} else {
			c.Assert(err, NotNil)
		}
	}
}
