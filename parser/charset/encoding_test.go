// Copyright 2021 PingCAP, Inc.
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

package charset_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/charset"
	"golang.org/x/text/transform"
)

var _ = Suite(&testEncodingSuite{})

type testEncodingSuite struct {
}

func (s *testEncodingSuite) TestEncoding(c *C) {
	enc := charset.NewEncoding("gbk")
	c.Assert(enc.Name(), Equals, "gbk")
	c.Assert(enc.Enabled(), IsTrue)
	enc.UpdateEncoding("utf-8")
	c.Assert(enc.Name(), Equals, "utf-8")
	enc.UpdateEncoding("gbk")
	c.Assert(enc.Name(), Equals, "gbk")
	c.Assert(enc.Enabled(), IsTrue)

	txt := []byte("一二三四")
	e, _ := charset.Lookup("gbk")
	gbkEncodedTxt, _, err := transform.Bytes(e.NewEncoder(), txt)
	c.Assert(err, IsNil)
	result, err := enc.Decode(nil, gbkEncodedTxt)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, txt)

	gbkEncodedTxt2, err := enc.Encode(nil, txt)
	c.Assert(err, IsNil)
	c.Assert(gbkEncodedTxt, DeepEquals, gbkEncodedTxt2)
	result, err = enc.Decode(nil, gbkEncodedTxt2)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, txt)

	GBKCases := []struct {
		utf8Str string
		result  string
		isValid bool
	}{
		{"一二三", "涓?簩涓?", false}, // MySQL reports '涓?簩涓'.
		{"一二三123", "涓?簩涓?23", false},
		{"案1案2", "妗?妗?", false},
		{"焊䏷菡釬", "鐒婁彿鑿￠嚞", true},
		{"鞍杏以伊位依", "闉嶆潖浠ヤ紛浣嶄緷", true},
		{"移維緯胃萎衣謂違", "绉荤董绶?儍钀庤。璎傞仌", false},
		{"仆仂仗仞仭仟价伉佚估", "浠嗕粋浠椾粸浠?粺浠蜂級浣氫及", false},
		{"佝佗佇佶侈侏侘佻佩佰侑佯", "浣濅綏浣囦蕉渚堜緩渚樹交浣╀桨渚戜蒋", true},
	}
	for _, tc := range GBKCases {
		cmt := Commentf("%v", tc)
		result, err = enc.Decode(nil, []byte(tc.utf8Str))
		if tc.isValid {
			c.Assert(err, IsNil, cmt)
		} else {
			c.Assert(err, NotNil, cmt)
		}
		c.Assert(string(result), Equals, tc.result, Commentf("%v", tc))
	}
}
