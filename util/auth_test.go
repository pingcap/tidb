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

package util

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testAuthSuite{})

type testAuthSuite struct {
}

func (s *testAuthSuite) TestEncodePassword(c *C) {
	defer testleak.AfterTest(c)()
	pwd := "123"
	c.Assert(EncodePassword(pwd), Equals, "40bd001563085fc35165329ea1ff5c5ecbdbbeef")
}

func (s *testAuthSuite) TestDecodePassword(c *C) {
	defer testleak.AfterTest(c)()
	x, err := DecodePassword(EncodePassword("123"))
	c.Assert(err, IsNil)
	c.Assert(x, DeepEquals, Sha1Hash([]byte("123")))
}

func (s *testAuthSuite) TestCalcPassword(c *C) {
	defer testleak.AfterTest(c)()
	salt := []byte{116, 32, 122, 120, 2, 51, 33, 66, 47, 85, 34, 39, 84, 58, 108, 14, 62, 47, 120, 126}
	pwd := Sha1Hash([]byte("123"))
	checkAuth := []byte{126, 168, 249, 64, 180, 223, 60, 240, 69, 249, 184, 57, 21, 34, 214, 219, 8, 193, 208, 55}
	c.Assert(CalcPassword(salt, pwd), DeepEquals, checkAuth)
}
