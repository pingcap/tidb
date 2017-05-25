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
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testJSONSuite{})

type testJSONSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testJSONSuite) TestJSONSerde(c *C) {
	var jsonNilValue = CreateJSON(nil)
	var jsonBoolValue = CreateJSON(true)
	var jsonDoubleValue = CreateJSON(3.24)
	var jsonStringValue = CreateJSON("hello, 世界")

	var jstr1 = `{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}`
	j1, err := ParseFromString(jstr1)
	c.Assert(err, IsNil)

	var jstr2 = `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
	j2, err := ParseFromString(jstr2)
	c.Assert(err, IsNil)

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

func (s *testJSONSuite) TestParseFromString(c *C) {
	var jstr1 = `{"a": [1, "2", {"aa": "bb"}, 4, null], "b": true, "c": null}`

	j1, err := ParseFromString(jstr1)
	c.Assert(err, IsNil)

	var jstr2 = j1.String()
	c.Assert(jstr2, Equals, `{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}`)
}

func (s *testJSONSuite) TestJSONType(c *C) {
	j1, err := ParseFromString(`{"a": "b"}`)
	c.Assert(err, IsNil)

	j2, err := ParseFromString(`["a", "b"]`)
	c.Assert(err, IsNil)

	j3, err := ParseFromString(`3`)
	c.Assert(err, IsNil)

	j4, err := ParseFromString(`3.0`)
	c.Assert(err, IsNil)

	j5, err := ParseFromString(`null`)
	c.Assert(err, IsNil)

	j6, err := ParseFromString(`true`)
	c.Assert(err, IsNil)

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
	jNull, _ := ParseFromString(`null`)
	jBoolTrue, _ := ParseFromString(`true`)
	jBoolFalse, _ := ParseFromString(`false`)
	jIntegerLarge, _ := ParseFromString(`5`)
	jIntegerSmall, _ := ParseFromString(`3`)
	jStringLarge, _ := ParseFromString(`"hello, world"`)
	jStringSmall, _ := ParseFromString(`"hello"`)
	jArrayLarge, _ := ParseFromString(`["a", "c"]`)
	jArraySmall, _ := ParseFromString(`["a", "b"]`)
	jObject, _ := ParseFromString(`{"a": "b"}`)

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
