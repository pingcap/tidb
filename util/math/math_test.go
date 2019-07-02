// Copyright 2019 PingCAP, Inc.
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

package math

import (
	"math/rand"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMath{})

type testMath struct{}

func (s *testMath) TestStrLenOfUint64Fast_RandomTestCases(c *C) {
	for i := 0; i < 1000000; i++ {
		num := rand.Uint64()
		expected := len(strconv.FormatUint(num, 10))
		actual := StrLenOfUint64Fast(num)
		c.Assert(actual, Equals, expected)
	}
}

func (s *testMath) TestStrLenOfUint64Fast_ManualTestCases(c *C) {
	nums := [22]uint64{0,
		1, 12, 123, 1234, 12345,
		123456, 1234567, 12345678, 123456789, 1234567890,
		1234567891, 12345678912, 123456789123, 1234567891234, 12345678912345,
		123456789123456, 1234567891234567, 12345678912345678, 123456789123456789,
		123456789123457890,
		^uint64(0),
	}

	for _, num := range nums {
		expected := len(strconv.FormatUint(num, 10))
		actual := StrLenOfUint64Fast(num)
		c.Assert(actual, Equals, expected)
	}
}
