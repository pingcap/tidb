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

package types

import (
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/testleak"
)

var _ = Suite(&testOverflowSuite{})

type testOverflowSuite struct {
}

func (s *testOverflowSuite) TestAdd(c *C) {
	defer testleak.AfterTest(c)()
	tblUint64 := []struct {
		lsh      uint64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 1, 0, true},
		{math.MaxUint64, 0, math.MaxUint64, false},
		{1, 1, 2, false},
	}

	for _, t := range tblUint64 {
		ret, err := AddUint64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MaxInt64, 1, 0, true},
		{math.MaxInt64, 0, math.MaxInt64, false},
		{0, math.MinInt64, math.MinInt64, false},
		{-1, math.MinInt64, 0, true},
		{math.MaxInt64, math.MinInt64, -1, false},
		{1, 1, 2, false},
		{1, -1, 0, false},
	}

	for _, t := range tblInt64 {
		ret, err := AddInt64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
		ret2, err := AddDuration(time.Duration(t.lsh), time.Duration(t.rsh))
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret2, Equals, time.Duration(t.ret))
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, math.MinInt64, math.MaxUint64 + math.MinInt64, false},
		{math.MaxInt64, math.MinInt64, 0, true},
		{0, -1, 0, true},
		{1, -1, 0, false},
		{0, 1, 1, false},
		{1, 1, 2, false},
	}

	for _, t := range tblInt {
		ret, err := AddInteger(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}
}

func (s *testOverflowSuite) TestSub(c *C) {
	defer testleak.AfterTest(c)()
	tblUint64 := []struct {
		lsh      uint64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 1, math.MaxUint64 - 1, false},
		{math.MaxUint64, 0, math.MaxUint64, false},
		{0, math.MaxUint64, 0, true},
		{0, 1, 0, true},
		{1, math.MaxUint64, 0, true},
		{1, 1, 0, false},
	}

	for _, t := range tblUint64 {
		ret, err := SubUint64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MinInt64, 0, math.MinInt64, false},
		{math.MinInt64, 1, 0, true},
		{math.MaxInt64, -1, 0, true},
		{0, math.MinInt64, 0, true},
		{-1, math.MinInt64, math.MaxInt64, false},
		{math.MinInt64, math.MaxInt64, 0, true},
		{math.MinInt64, math.MinInt64, 0, false},
		{math.MinInt64, -math.MaxInt64, -1, false},
		{1, 1, 0, false},
	}

	for _, t := range tblInt64 {
		ret, err := SubInt64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{0, math.MinInt64, -math.MinInt64, false},
		{0, 1, 0, true},
		{math.MaxUint64, math.MinInt64, 0, true},
		{math.MaxInt64, math.MinInt64, 2*math.MaxInt64 + 1, false},
		{math.MaxUint64, -1, 0, true},
		{0, -1, 1, false},
		{1, 1, 0, false},
	}

	for _, t := range tblInt {
		ret, err := SubUintWithInt(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt2 := []struct {
		lsh      int64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MinInt64, 0, 0, true},
		{math.MaxInt64, 0, math.MaxInt64, false},
		{math.MaxInt64, math.MaxUint64, 0, true},
		{math.MaxInt64, -math.MinInt64, 0, true},
		{-1, 0, 0, true},
		{1, 1, 0, false},
	}

	for _, t := range tblInt2 {
		ret, err := SubIntWithUint(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}
}

func (s *testOverflowSuite) TestMul(c *C) {
	defer testleak.AfterTest(c)()
	tblUint64 := []struct {
		lsh      uint64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 1, math.MaxUint64, false},
		{math.MaxUint64, 0, 0, false},
		{math.MaxUint64, 2, 0, true},
		{1, 1, 1, false},
	}

	for _, t := range tblUint64 {
		ret, err := MulUint64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MaxInt64, 1, math.MaxInt64, false},
		{math.MinInt64, 1, math.MinInt64, false},
		{math.MaxInt64, -1, -math.MaxInt64, false},
		{math.MinInt64, -1, 0, true},
		{math.MinInt64, 0, 0, false},
		{math.MaxInt64, 0, 0, false},
		{math.MaxInt64, math.MaxInt64, 0, true},
		{math.MaxInt64, math.MinInt64, 0, true},
		{math.MinInt64 / 10, 11, 0, true},
		{1, 1, 1, false},
	}

	for _, t := range tblInt64 {
		ret, err := MulInt64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 0, 0, false},
		{0, -1, 0, false},
		{1, -1, 0, true},
		{math.MaxUint64, -1, 0, true},
		{math.MaxUint64, 10, 0, true},
		{1, 1, 1, false},
	}

	for _, t := range tblInt {
		ret, err := MulInteger(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}
}

func (s *testOverflowSuite) TestDiv(c *C) {
	defer testleak.AfterTest(c)()
	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MaxInt64, 1, math.MaxInt64, false},
		{math.MinInt64, 1, math.MinInt64, false},
		{math.MinInt64, -1, 0, true},
		{math.MaxInt64, -1, -math.MaxInt64, false},
		{1, -1, -1, false},
		{-1, 1, -1, false},
		{-1, 2, 0, false},
		{math.MinInt64, 2, math.MinInt64 / 2, false},
	}

	for _, t := range tblInt64 {
		ret, err := DivInt64(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{0, -1, 0, false},
		{1, -1, 0, true},
		{math.MaxInt64, math.MinInt64, 0, false},
		{math.MaxInt64, -1, 0, true},
		{100, 20, 5, false},
	}

	for _, t := range tblInt {
		ret, err := DivUintWithInt(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}

	tblInt2 := []struct {
		lsh      int64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MinInt64, math.MaxInt64, 0, true},
		{0, 1, 0, false},
		{-1, math.MaxInt64, 0, false},
	}

	for _, t := range tblInt2 {
		ret, err := DivIntWithUint(t.lsh, t.rsh)
		if t.overflow {
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, Equals, t.ret)
		}
	}
}
