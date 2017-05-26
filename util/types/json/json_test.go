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
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testJSONSuite{})

type testJSONSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func parseFromStringPanic(s string) JSON {
	j, err := ParseFromString(s)
	if err != nil {
		msg := fmt.Sprintf("ParseFromString(%s) fail", s)
		panic(msg)
	}
	return j
}

func (s *testJSONSuite) TestParseFromString(c *C) {
	var jstr1 = `{"a": [1, "2", {"aa": "bb"}, 4, null], "b": true, "c": null}`

	j1, err := ParseFromString(jstr1)
	c.Assert(err, IsNil)

	var jstr2 = j1.String()
	c.Assert(jstr2, Equals, `{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}`)
}

func (s *testJSONSuite) TestJSONSerde(c *C) {
	var jsonNilValue = CreateJSON(nil)
	var jsonBoolValue = CreateJSON(true)
	var jsonDoubleValue = CreateJSON(3.24)
	var jsonStringValue = CreateJSON("hello, 世界")

	j1 := parseFromStringPanic(`{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}`)
	j2 := parseFromStringPanic(`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	var testcses = []struct {
		In  JSON
		Out JSON
	}{
		{In: jsonNilValue, Out: jsonNilValue},
		{In: jsonBoolValue, Out: jsonBoolValue},
		{In: jsonDoubleValue, Out: jsonDoubleValue},
		{In: jsonStringValue, Out: jsonStringValue},
		{In: j1, Out: j1},
		{In: j2, Out: j2},
	}

	for _, s := range testcses {
		data := Serialize(s.In)
		t, err := Deserialize(data)
		c.Assert(err, IsNil)

		v1 := t.String()
		v2 := s.Out.String()
		c.Assert(v1, Equals, v2)
	}
}

func (s *testJSONSuite) TestJSONType(c *C) {
	j1 := parseFromStringPanic(`{"a": "b"}`)
	j2 := parseFromStringPanic(`["a", "b"]`)
	j3 := parseFromStringPanic(`3`)
	j4 := parseFromStringPanic(`3.0`)
	j5 := parseFromStringPanic(`null`)
	j6 := parseFromStringPanic(`true`)

	var jList = []struct {
		In  JSON
		Out string
	}{
		{j1, "OBJECT"},
		{j2, "ARRAY"},
		{j3, "INTEGER"},
		{j4, "DOUBLE"},
		{j5, "NULL"},
		{j6, "BOOLEAN"},
	}

	for _, j := range jList {
		c.Assert(j.In.Type(), Equals, j.Out)
	}
}

func (s *testJSONSuite) TestCompareJSON(c *C) {
	jNull := parseFromStringPanic(`null`)
	jBoolTrue := parseFromStringPanic(`true`)
	jBoolFalse := parseFromStringPanic(`false`)
	jIntegerLarge := parseFromStringPanic(`5`)
	jIntegerSmall := parseFromStringPanic(`3`)
	jStringLarge := parseFromStringPanic(`"hello, world"`)
	jStringSmall := parseFromStringPanic(`"hello"`)
	jArrayLarge := parseFromStringPanic(`["a", "c"]`)
	jArraySmall := parseFromStringPanic(`["a", "b"]`)
	jObject := parseFromStringPanic(`{"a": "b"}`)

	var caseList = []struct {
		left  JSON
		right JSON
	}{
		{jNull, jIntegerSmall},
		{jIntegerSmall, jIntegerLarge},
		{jIntegerLarge, jStringSmall},
		{jStringSmall, jStringLarge},
		{jStringLarge, jObject},
		{jObject, jArraySmall},
		{jArraySmall, jArrayLarge},
		{jArrayLarge, jBoolFalse},
		{jBoolFalse, jBoolTrue},
	}

	for _, cmpCase := range caseList {
		cmp, err := CompareJSON(cmpCase.left, cmpCase.right)
		c.Assert(err, IsNil)
		c.Assert(cmp < 0, IsTrue)
	}
}

func (s *testJSONSuite) TestJSONPathExprLegRe(c *C) {
	var pathExpr = "$.key1[3][*].*.key3"
	matches := jsonPathExprLegRe.FindAllString(pathExpr, -1)
	c.Assert(len(matches), Equals, 5)
	c.Assert(matches[0], Equals, ".key1")
	c.Assert(matches[1], Equals, "[3]")
	c.Assert(matches[2], Equals, "[*]")
	c.Assert(matches[3], Equals, ".*")
	c.Assert(matches[4], Equals, ".key3")
}

func (s *testJSONSuite) TestValidatePathExpr(c *C) {
	var pathExpr = "$ .   key1[3]\t[*].*.key3"
	_, err := validateJSONPathExpr(pathExpr)
	c.Assert(err, IsNil)
}

func (s *testJSONSuite) TestJSONExtract(c *C) {
	j1 := parseFromStringPanic(`{"a": [1, "2", {"aa": "bb"}, 4.0], "b": true, "c": "d"}`)
	j2 := parseFromStringPanic(`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	// j3 is for j1.Extract([]string{"$.a", "$[0]")
	j3 := parseFromStringPanic(`[[1, "2", {"aa": "bb"}, 4.0]]`)
	// j4 is for j2.Extract([]string{"$.a", "$[0]")
	j4 := parseFromStringPanic(`[{"a": 1, "b": true}]`)

	var caseList = []struct {
		j        JSON
		pathExpr []string
		expected JSON
		found    bool
		err      error
	}{
		// test extract with only one path expression.
		{j1, []string{"$.a"}, j1.object["a"], true, nil},
		{j2, []string{"$.a"}, CreateJSON(nil), false, nil},
		{j1, []string{"$[0]"}, CreateJSON(nil), false, nil},
		{j2, []string{"$[0]"}, j2.array[0], true, nil},

		// test extract with multi path expressions.
		{j1, []string{"$.a", "$[0]"}, j3, true, nil},
		{j2, []string{"$.a", "$[0]"}, j4, true, nil},
	}

	for _, caseItem := range caseList {
		expected, found, err := caseItem.j.Extract(caseItem.pathExpr...)

		c.Assert(err, Equals, caseItem.err)
		c.Assert(found, Equals, caseItem.found)
		if found {
			b1 := Serialize(expected)
			b2 := Serialize(caseItem.expected)
			c.Assert(bytes.Compare(b1, b2), Equals, 0)
		}
	}
}

func (s *testJSONSuite) TestJSONUnquote(c *C) {
	var caseList = []struct {
		j        JSON
		unquoted string
	}{
		{j: parseFromStringPanic(`3`), unquoted: "3"},
		{j: parseFromStringPanic(`"3"`), unquoted: "3"},
		{j: parseFromStringPanic(`true`), unquoted: "true"},
		{j: parseFromStringPanic(`null`), unquoted: "null"},
		{j: parseFromStringPanic(`{"a": [1, 2]}`), unquoted: `{"a":[1,2]}`},
	}
	for _, caseItem := range caseList {
		c.Assert(caseItem.j.Unquote(), Equals, caseItem.unquoted)
	}
}

func (s *testJSONSuite) TestJSONMerge(c *C) {
	jstr1 := `{"a": 1}`
	jstr2 := `{"b": 2}`
	jstr3 := `[1]`
	jstr4 := `[2]`
	jstr5 := `4`

	var caseList = []struct {
		base     JSON
		suffixes []JSON
		expected JSON
	}{
		{parseFromStringPanic(jstr1), []JSON{parseFromStringPanic(jstr2)}, parseFromStringPanic(`{"a": 1, "b": 2}`)},
		{parseFromStringPanic(jstr3), []JSON{parseFromStringPanic(jstr4)}, parseFromStringPanic(`[1, 2]`)},
		{parseFromStringPanic(jstr1), []JSON{parseFromStringPanic(jstr3)}, parseFromStringPanic(`[{"a": 1}, 1]`)},
		{parseFromStringPanic(jstr3), []JSON{parseFromStringPanic(jstr1)}, parseFromStringPanic(`[1, {"a": 1}]`)},
		{parseFromStringPanic(jstr1), []JSON{parseFromStringPanic(jstr5)}, parseFromStringPanic(`[{"a": 1}, 4]`)},
		{parseFromStringPanic(jstr3), []JSON{parseFromStringPanic(jstr5)}, parseFromStringPanic(`[1, 4]`)},
		{parseFromStringPanic(jstr5), []JSON{parseFromStringPanic(jstr1)}, parseFromStringPanic(`[4, {"a": 1}]`)},
		{parseFromStringPanic(jstr5), []JSON{parseFromStringPanic(jstr3)}, parseFromStringPanic(`[4, 1]`)},
	}

	for _, caseItem := range caseList {
		caseItem.base.Merge(caseItem.suffixes...)
		cmp, err := CompareJSON(caseItem.base, caseItem.expected)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testJSONSuite) TestJSONSet(c *C) {
	var base = parseFromStringPanic(`null`)
	var caseList = []struct {
		setField string
		setValue JSON
		expected JSON
	}{
		{"$", parseFromStringPanic(`{}`), parseFromStringPanic(`{}`)},
		{"$.a", parseFromStringPanic(`[]`), parseFromStringPanic(`{"a": []}`)},
		{"$.a[1]", parseFromStringPanic(`3`), parseFromStringPanic(`{"a": [3]}`)},

		// won't modify base because path doesn't exist.
		{"$.b[1]", parseFromStringPanic(`3`), parseFromStringPanic(`{"a": [3]}`)},
		{"$.a[2].b", parseFromStringPanic(`3`), parseFromStringPanic(`{"a": [3]}`)},
	}
	for _, caseItem := range caseList {
		err := base.Set([]string{caseItem.setField}, []JSON{caseItem.setValue})
		c.Assert(err, IsNil)

		cmp, err := CompareJSON(base, caseItem.expected)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}
