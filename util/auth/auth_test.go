// Copyright 2015 PingCAP, Inc.
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

package auth

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testAuthSuite{})

type testAuthSuite struct {
}

func (s *testAuthSuite) TestEncodePassword(c *C) {
	pwd := "123"
	c.Assert(EncodePassword(pwd), Equals, "*23AE809DDACAF96AF0FD78ED04B6A265E05AA257")
}

func (s *testAuthSuite) TestDecodePassword(c *C) {
	x, err := DecodePassword(EncodePassword("123"))
	c.Assert(err, IsNil)
	c.Assert(x, DeepEquals, Sha1Hash(Sha1Hash([]byte("123"))))
}

func (s *testAuthSuite) TestCheckScramble(c *C) {
	pwd := "abc"
	salt := []byte{85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82}
	auth := []byte{24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33}
	encodepwd := EncodePassword(pwd)
	hpwd, err := DecodePassword(encodepwd)
	c.Assert(err, IsNil)

	res := CheckScrambledPassword(salt, hpwd, auth)
	c.Assert(res, IsTrue)
}
