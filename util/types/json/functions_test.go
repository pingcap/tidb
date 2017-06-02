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

func (s *testJSONSuite) TestJSONMerge(c *C) {
	var tests = []struct {
		base     string
		suffixes []string
		expected string
	}{
		{`{"a": 1}`, []string{`{"b": 2}`}, `{"a": 1, "b": 2}`},
		{`[1]`, []string{`[2]`}, `[1, 2]`},
		{`{"a": 1}`, []string{`[1]`}, `[{"a": 1}, 1]`},
		{`[1]`, []string{`{"a": 1}`}, `[1, {"a": 1}]`},
		{`{"a": 1}`, []string{`4`}, `[{"a": 1}, 4]`},
		{`[1]`, []string{`4`}, `[1, 4]`},
		{`4`, []string{`{"a": 1}`}, `[4, {"a": 1}]`},
		{`4`, []string{`1`}, `[4, 1]`},
	}

	for _, tt := range tests {
		base := mustParseFromString(tt.base)
		suffixes := make([]JSON, 0, len(tt.suffixes))
		for _, s := range tt.suffixes {
			suffixes = append(suffixes, mustParseFromString(s))
		}
		base.Merge(suffixes)
		cmp, err := CompareJSON(base, mustParseFromString(tt.expected))
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testJSONSuite) TestJSONSetInsertReplace(c *C) {
	var base = mustParseFromString(`null`)
	var tests = []struct {
		setField string
		setValue string
		mt       ModifyType
		expected string
	}{
		// json_set('null', '$', '{}') => '{}'
		{"$", `{}`, ModifySet, `{}`},
		// json_set('{}', '$.a', '3') => '{"a": 3}'
		{"$.a", `3`, ModifySet, `{"a": 3}`},
		// json_replace('{"a": 3}', '$.a', '[]') => '{"a": []}'
		{"$.a", `[]`, ModifyReplace, `{"a": []}`},
		// json_set('{"a": []}', '$.a[0]', '3') => '{"a": [3]}'
		{"$.a[0]", `3`, ModifySet, `{"a": [3]}`},
		// json_insert('{"a": [3]}', '$.a[1]', '4') => '{"a": [3, 4]}'
		{"$.a[1]", `4`, ModifyInsert, `{"a": [3, 4]}`},

		// For these cases, we can't change input JSON at all:

		// json_set('{"a": [3]}', '$.b[1]', '3') => '{"a": [3]}'
		// because the path without last leg doesn't exist.
		{"$.b[1]", `3`, ModifySet, `{"a": [3, 4]}`},
		// json_set('{"a": [3]}', '$.a[2].b', '3') => '{"a": [3]}'
		// because the path without last leg doesn't exist.
		{"$.a[2].b", `3`, ModifySet, `{"a": [3, 4]}`},

		// json_insert('{"a": [3]}', '$.a[1]', '30') => '{"a": [3]}'
		// because we want to insert but the full path exists.
		{"$.a[0]", `30`, ModifyInsert, `{"a": [3, 4]}`},

		// json_replace('{"a": [3]}', '$.a[1]', '30') =>'{"a": [3]}'
		// because we want to replace but the full path doesn't exist.
		{"$.a[2]", `30`, ModifyReplace, `{"a": [3, 4]}`},
	}
	for _, tt := range tests {
		pathExpr, err := ParseJSONPathExpr(tt.setField)
		c.Assert(err, IsNil)

		value := mustParseFromString(tt.setValue)
		err = base.SetInsertReplace([]PathExpression{pathExpr}, []JSON{value}, tt.mt)
		c.Assert(err, IsNil)

		expected := mustParseFromString(tt.expected)
		cmp, err := CompareJSON(base, expected)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}
