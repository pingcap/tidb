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

func (s *testJSONSuite) TestJSONSerde(c *C) {
	var j1 interface{}
	var jstr1 = []byte(`{"a": [1, "2", {"aa": "bb"}, 4.0], "b": true}`)
	json.Unmarshal(jstr1, &j1)

	var j2 interface{}
	var jstr2 = []byte(`[{"a": 1, "b": true}, 3, 3.5, "hello, world", nil, true]`)
	json.Unmarshal(jstr2, &j2)

	var testcses = []struct {
		In  interface{}
		Out interface{}
	}{
		{In: nil, Out: nil},
		{In: true, Out: true},
		{In: false, Out: false},
		{In: int16(30), Out: int16(30)},
		{In: uint32(3), Out: uint32(3)},
		{In: float64(0.5), Out: float64(0.5)},
		{In: "abcdefg", Out: "abcdefg"},
		{In: j1, Out: j1},
		{In: j2, Out: j2},
	}

	for _, s := range testcses {
		data, err := serialize(s.In)
		c.Assert(err, IsNil)
		t, err := deserialize(data)
		c.Assert(err, IsNil)

		v1, _ := json.Marshal(t)
		v2, _ := json.Marshal(s.Out)
		c.Assert(string(v1), Equals, string(v2))
	}
}
