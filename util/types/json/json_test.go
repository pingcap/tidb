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
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testJSONSuite{})

type testJSONSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testJSONSuite) TestJSONSerde(c *C) {
	var jstr1 = `{"a": [1, "2", {"aa": "bb"}, 4.0], "b": true}`
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

		v1, _ := json.Marshal(t)
		v2, _ := json.Marshal(s.Out)
		c.Assert(string(v1), Equals, string(v2))
	}
}

func (s *testJSONSuite) TestParseFromString(c *C) {
	var jstr1 = `{"a": [1, "2", {"aa": "bb"}, 4, null], "b": true, "c": null}`

	j1, err := ParseFromString(jstr1)
	c.Assert(err, IsNil)

	var jstr2 = j1.String()
	c.Assert(jstr2, Equals, `{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}`)
}
