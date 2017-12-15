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

	. "github.com/pingcap/check"
)

func (s *testJSONSuite) TestBinaryJSONMarshalUnmarshal(c *C) {
	strs := []string{
		`{"a":[1,"2",{"aa":"bb"},4,null],"b":true,"c":null}`,
		`{"aaaaaaaaaaa":[1,"2",{"aa":"bb"},4.1],"bbbbbbbbbb":true,"ccccccccc":"d"}`,
		`[{"a":1,"b":true},3,3.5,"hello, world",null,true]`,
	}
	for _, str := range strs {
		j := mustParseFromString(str)
		data := Serialize(j)
		bj := BinaryJSON{TypeCode: data[0], Value: data[1:]}
		c.Assert(bj.String(), Equals, str)
		parsedBJ := mustParseBinaryFromString(c, str)
		c.Assert(parsedBJ.String(), Equals, str)

		// Test JSON marshaled string can be unmarshaled by BinaryJSON
		marshaled, err := j.MarshalJSON()
		c.Assert(err, IsNil)
		err = bj.UnmarshalJSON(marshaled)
		c.Assert(err, IsNil)
		j2, err := bj.ToJSON()
		c.Assert(err, IsNil)
		cmp, err := CompareJSON(j, j2)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func mustParseBinaryFromString(c *C, s string) BinaryJSON {
	j, err := ParseBinaryFromString(s)
	if err != nil {
		msg := fmt.Sprintf("ParseFromString(%s) fail", s)
		panic(msg)
	}
	return j
}
