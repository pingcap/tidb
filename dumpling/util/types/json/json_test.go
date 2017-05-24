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
	var jstr1 = `{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}`
	j1, err := ParseFromString(jstr1)
	c.Assert(err, IsNil)

	var jstr2 = `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
	j2, err := ParseFromString(jstr2)
	c.Assert(err, IsNil)

	var jsonNilValue = jsonLiteral(0x00)
	var jsonBoolValue = jsonLiteral(0x01)
	var jsonDoubleValue = jsonDouble(3.24)
	var jsonStringValue = jsonString("hello, 世界")

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
	var cmp int

	// compare two JSON boolean.
	jBool1, _ := ParseFromString(`true`)
	jBool2, _ := ParseFromString(`false`)
	cmp, _ = CompareJSON(jBool1, jBool2)
	c.Assert(cmp > 0, IsTrue)

	// compare two JSON ARRAY
	jArray1, _ := ParseFromString(`["a", "c"]`)
	jArray2, _ := ParseFromString(`["a", "b"]`)
	cmp, _ = CompareJSON(jArray1, jArray2)
	c.Assert(cmp > 0, IsTrue)

	// compare two JSON OBJECT
	jObject1, _ := ParseFromString(`{"a": "b"}`)
	jObject2, _ := ParseFromString(`{"a": "c"}`)
	cmp, _ = CompareJSON(jObject1, jObject2)
	c.Assert(cmp != 0, IsTrue)

	// compare two JSON string
	jString1, _ := ParseFromString(`"hello"`)
	jString2, _ := ParseFromString(`"hello, world"`)
	cmp, _ = CompareJSON(jString1, jString2)
	c.Assert(cmp < 0, IsTrue)

	// compare two JSON integer
	jInteger1, _ := ParseFromString(`3`)
	jInteger2, _ := ParseFromString(`5`)
	cmp, _ = CompareJSON(jInteger1, jInteger2)
	c.Assert(cmp < 0, IsTrue)

	jNull1, _ := ParseFromString(`null`)
	jNull2, _ := ParseFromString(`null`)
	cmp, _ = CompareJSON(jNull1, jNull2)
	c.Assert(cmp == 0, IsTrue)

	// compare two JSON with different types.
	cmp, _ = CompareJSON(jBool1, jArray1)
	c.Assert(cmp > 0, IsTrue)
	cmp, _ = CompareJSON(jArray1, jObject1)
	c.Assert(cmp > 0, IsTrue)
	cmp, _ = CompareJSON(jObject1, jString1)
	c.Assert(cmp > 0, IsTrue)
	cmp, _ = CompareJSON(jString1, jInteger1)
	c.Assert(cmp > 0, IsTrue)
	cmp, _ = CompareJSON(jInteger1, jNull1)
	c.Assert(cmp > 0, IsTrue)
}
