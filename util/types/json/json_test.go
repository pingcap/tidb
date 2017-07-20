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
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testJSONSuite{})

type testJSONSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

// mustParseFromString parse a JSON from a string.
// Panic if string is not a valid JSON.
func mustParseFromString(s string) JSON {
	j, err := ParseFromString(s)
	if err != nil {
		msg := fmt.Sprintf("ParseFromString(%s) fail", s)
		panic(msg)
	}
	return j
}

func (s *testJSONSuite) TestParseFromString(c *C) {
	jstr1 := `{"a": [1, "2", {"aa": "bb"}, 4, null], "b": true, "c": null}`
	jstr2 := mustParseFromString(jstr1).String()
	c.Assert(jstr2, Equals, `{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}`)
}

func (s *testJSONSuite) TestSerializeAndDeserialize(c *C) {
	var jsonNilValue = CreateJSON(nil)
	var jsonBoolValue = CreateJSON(true)
	var jsonUintValue = CreateJSON(uint64(1 << 63))
	var jsonDoubleValue = CreateJSON(3.24)
	var jsonStringValue = CreateJSON("hello, 世界")
	j1 := mustParseFromString(`{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}`)
	j2 := mustParseFromString(`[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`)

	var testcases = []struct {
		In   JSON
		Out  JSON
		size int
	}{
		{In: jsonNilValue, Out: jsonNilValue, size: 2},
		{In: jsonBoolValue, Out: jsonBoolValue, size: 2},
		{In: jsonUintValue, Out: jsonUintValue, size: 9},
		{In: jsonDoubleValue, Out: jsonDoubleValue, size: 9},
		{In: jsonStringValue, Out: jsonStringValue, size: 15},
		{In: j1, Out: j1, size: 144},
		{In: j2, Out: j2, size: 108},
	}

	for _, s := range testcases {
		data := Serialize(s.In)
		t, err := Deserialize(data)
		c.Assert(err, IsNil)

		v1 := t.String()
		v2 := s.Out.String()
		c.Assert(v1, Equals, v2)

		size, err := PeekBytesAsJSON(data)
		c.Assert(err, IsNil)
		c.Assert(len(data), Equals, size)
		c.Assert(len(data), Equals, s.size)
	}
}

func (s *testJSONSuite) TestCompareJSON(c *C) {
	jNull := mustParseFromString(`null`)
	jBoolTrue := mustParseFromString(`true`)
	jBoolFalse := mustParseFromString(`false`)
	jIntegerLarge := CreateJSON(uint64(1 << 63))
	jIntegerSmall := mustParseFromString(`3`)
	jStringLarge := mustParseFromString(`"hello, world"`)
	jStringSmall := mustParseFromString(`"hello"`)
	jArrayLarge := mustParseFromString(`["a", "c"]`)
	jArraySmall := mustParseFromString(`["a", "b"]`)
	jObject := mustParseFromString(`{"a": "b"}`)

	var tests = []struct {
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
	for _, tt := range tests {
		cmp, err := CompareJSON(tt.left, tt.right)
		c.Assert(err, IsNil)
		c.Assert(cmp < 0, IsTrue)
	}
}
