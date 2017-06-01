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
	"bytes"

	. "github.com/pingcap/check"
)

func (s *testJSONSuite) TestJSONType(c *C) {
	var tests = []struct {
		In  string
		Out string
	}{
		{`{"a": "b"}`, "OBJECT"},
		{`["a", "b"]`, "ARRAY"},
		{`3`, "INTEGER"},
		{`3.0`, "DOUBLE"},
		{`null`, "NULL"},
		{`true`, "BOOLEAN"},
	}
	for _, tt := range tests {
		j := mustParseFromString(tt.In)
		c.Assert(j.Type(), Equals, tt.Out)
	}
}

func (s *testJSONSuite) TestJSONExtract(c *C) {
	j1 := mustParseFromString(`{"a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"], "\"hello\"": "world"}`)
	j2 := mustParseFromString(`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	var tests = []struct {
		j               JSON
		pathExprStrings []string
		expected        JSON
		found           bool
		err             error
	}{
		// test extract with only one path expression.
		{j1, []string{"$.a"}, j1.object["a"], true, nil},
		{j2, []string{"$.a"}, CreateJSON(nil), false, nil},
		{j1, []string{"$[0]"}, CreateJSON(nil), false, nil},
		{j2, []string{"$[0]"}, j2.array[0], true, nil},
		{j1, []string{"$.a[2].aa"}, CreateJSON("bb"), true, nil},
		{j1, []string{"$.a[*].aa"}, mustParseFromString(`["bb", "cc"]`), true, nil},
		{j1, []string{"$.*[0]"}, mustParseFromString(`[1, "d"]`), true, nil},
		{j1, []string{`$.a[*]."aa"`}, mustParseFromString(`["bb", "cc"]`), true, nil},
		{j1, []string{`$."\"hello\""`}, mustParseFromString(`"world"`), true, nil},
		{j1, []string{`$**[0]`}, mustParseFromString(`[1, "d"]`), true, nil},

		// test extract with multi path expressions.
		{j1, []string{"$.a", "$[0]"}, mustParseFromString(`[[1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}]]`), true, nil},
		{j2, []string{"$.a", "$[0]"}, mustParseFromString(`[{"a": 1, "b": true}]`), true, nil},
	}

	for _, tt := range tests {
		var pathExprList = make([]PathExpression, 0)
		for _, peStr := range tt.pathExprStrings {
			pe, err := ParseJSONPathExpr(peStr)
			c.Assert(err, IsNil)
			pathExprList = append(pathExprList, pe)
		}

		expected, found := tt.j.Extract(pathExprList)
		c.Assert(found, Equals, tt.found)
		if found {
			b1 := Serialize(expected)
			b2 := Serialize(tt.expected)
			c.Assert(bytes.Compare(b1, b2), Equals, 0)
		}
	}
}

func (s *testJSONSuite) TestJSONUnquote(c *C) {
	var tests = []struct {
		j        string
		unquoted string
	}{
		{j: `3`, unquoted: "3"},
		{j: `"3"`, unquoted: "3"},
		{j: `"hello, \"escaped quotes\" world"`, unquoted: "hello, \"escaped quotes\" world"},
		{j: "\"\\u4f60\"", unquoted: "你"},
		{j: `true`, unquoted: "true"},
		{j: `null`, unquoted: "null"},
		{j: `{"a": [1, 2]}`, unquoted: `{"a":[1,2]}`},
	}
	for _, tt := range tests {
		j := mustParseFromString(tt.j)
		unquoted, err := j.Unquote()
		c.Assert(err, IsNil)
		c.Assert(unquoted, Equals, tt.unquoted)
	}
}
