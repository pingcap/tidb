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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testCompareSuite{})

type testCompareSuite struct {
}

func (s *testCompareSuite) TestCompare(c *C) {
	defer testleak.AfterTest(c)()
	cmpTbl := []struct {
		lhs interface{}
		rhs interface{}
		ret int // 0, 1, -1
	}{
		{float64(1), float64(1), 0},
		{float64(1), "1", 0},
		{int64(1), int64(1), 0},
		{int64(-1), uint64(1), -1},
		{int64(-1), "-1", 0},
		{uint64(1), uint64(1), 0},
		{uint64(1), int64(-1), 1},
		{uint64(1), "1", 0},
		{NewDecFromInt(1), NewDecFromInt(1), 0},
		{NewDecFromInt(1), "1", 0},
		{NewDecFromInt(1), []byte("1"), 0},
		{"1", "1", 0},
		{"1", int64(-1), 1},
		{"1", float64(2), -1},
		{"1", uint64(1), 0},
		{"1", NewDecFromInt(1), 0},
		{"2011-01-01 11:11:11", Time{Time: FromGoTime(time.Now()), Type: mysql.TypeDatetime, Fsp: 0}, -1},
		{"12:00:00", ZeroDuration, 1},
		{ZeroDuration, ZeroDuration, 0},
		{Time{Time: FromGoTime(time.Now().Add(time.Second * 10)), Type: mysql.TypeDatetime, Fsp: 0},
			Time{Time: FromGoTime(time.Now()), Type: mysql.TypeDatetime, Fsp: 0}, 1},

		{nil, 2, -1},
		{nil, nil, 0},

		{false, nil, 1},
		{false, true, -1},
		{true, true, 0},
		{false, false, 0},
		{true, 2, -1},

		{float64(1.23), nil, 1},
		{float64(0.0), float64(3.45), -1},
		{float64(354.23), float64(3.45), 1},
		{float64(3.452), float64(3.452), 0},

		{int(432), nil, 1},
		{-4, int(32), -1},
		{int(4), -32, 1},
		{int(432), int64(12), 1},
		{int(23), int64(128), -1},
		{int(123), int64(123), 0},
		{int(432), int(12), 1},
		{int(23), int(123), -1},
		{int64(133), int(183), -1},

		{uint64(133), uint64(183), -1},
		{uint64(2), int64(-2), 1},
		{uint64(2), int64(1), 1},

		{"", nil, 1},
		{"", "24", -1},
		{"aasf", "4", 1},
		{"", "", 0},

		{[]byte(""), nil, 1},
		{[]byte(""), []byte("sff"), -1},

		{Time{Time: ZeroTime}, nil, 1},
		{Time{Time: ZeroTime}, Time{Time: FromGoTime(time.Now()), Type: mysql.TypeDatetime, Fsp: 3}, -1},
		{Time{Time: FromGoTime(time.Now()), Type: mysql.TypeDatetime, Fsp: 3}, "0000-00-00 00:00:00", 1},

		{Duration{Duration: time.Duration(34), Fsp: 2}, nil, 1},
		{Duration{Duration: time.Duration(34), Fsp: 2}, Duration{Duration: time.Duration(29034), Fsp: 2}, -1},
		{Duration{Duration: time.Duration(3340), Fsp: 2}, Duration{Duration: time.Duration(34), Fsp: 2}, 1},
		{Duration{Duration: time.Duration(34), Fsp: 2}, Duration{Duration: time.Duration(34), Fsp: 2}, 0},

		{[]byte{}, []byte{}, 0},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte("123"), 1234, -1},
		{[]byte{}, nil, 1},

		{[]interface{}{1, 2, 3}, []interface{}{1, 2, 3}, 0},
		{[]interface{}{1, 3, 3}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 2, 3}, []interface{}{2, 2, 3}, -1},

		{Hex{Value: 1}, 1, 0},
		{Hex{Value: 0x4D7953514C}, "MySQL", 0},
		{Hex{Value: 0}, uint64(10), -1},
		{Hex{Value: 1}, float64(0), 1},
		{Hex{Value: 1}, NewDecFromInt(1), 0},
		{Hex{Value: 1}, Bit{Value: 0, Width: 1}, 1},
		{Hex{Value: 1}, Hex{Value: 1}, 0},

		{Bit{Value: 1, Width: 1}, 1, 0},
		{Bit{Value: 0x41, Width: 8}, "A", 0},
		{Bit{Value: 1, Width: 1}, uint64(10), -1},
		{Bit{Value: 1, Width: 1}, float64(0), 1},
		{Bit{Value: 1, Width: 1}, NewDecFromInt(1), 0},
		{Bit{Value: 1, Width: 1}, Hex{Value: 2}, -1},
		{Bit{Value: 1, Width: 1}, Bit{Value: 1, Width: 1}, 0},

		{Enum{Name: "a", Value: 1}, 1, 0},
		{Enum{Name: "a", Value: 1}, "a", 0},
		{Enum{Name: "a", Value: 1}, uint64(10), -1},
		{Enum{Name: "a", Value: 1}, float64(0), 1},
		{Enum{Name: "a", Value: 1}, NewDecFromInt(1), 0},
		{Enum{Name: "a", Value: 1}, Hex{Value: 2}, -1},
		{Enum{Name: "a", Value: 1}, Bit{Value: 1, Width: 1}, 0},
		{Enum{Name: "a", Value: 1}, Hex{Value: 1}, 0},
		{Enum{Name: "a", Value: 1}, Enum{Name: "a", Value: 1}, 0},

		{Set{Name: "a", Value: 1}, 1, 0},
		{Set{Name: "a", Value: 1}, "a", 0},
		{Set{Name: "a", Value: 1}, uint64(10), -1},
		{Set{Name: "a", Value: 1}, float64(0), 1},
		{Set{Name: "a", Value: 1}, NewDecFromInt(1), 0},
		{Set{Name: "a", Value: 1}, Hex{Value: 2}, -1},
		{Set{Name: "a", Value: 1}, Bit{Value: 1, Width: 1}, 0},
		{Set{Name: "a", Value: 1}, Hex{Value: 1}, 0},
		{Set{Name: "a", Value: 1}, Enum{Name: "a", Value: 1}, 0},
		{Set{Name: "a", Value: 1}, Set{Name: "a", Value: 1}, 0},
	}

	for i, t := range cmpTbl {
		comment := Commentf("%d %v %v", i, t.lhs, t.rhs)
		ret, err := compareForTest(t.lhs, t.rhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.ret, comment)

		ret, err = compareForTest(t.rhs, t.lhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, -t.ret, comment)
	}
}

func compareForTest(a, b interface{}) (int, error) {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	aDatum := NewDatum(a)
	bDatum := NewDatum(b)
	return aDatum.CompareDatum(sc, bDatum)
}

func (s *testCompareSuite) TestCompareDatum(c *C) {
	defer testleak.AfterTest(c)()
	cmpTbl := []struct {
		lhs Datum
		rhs Datum
		ret int // 0, 1, -1
	}{
		{MaxValueDatum(), NewDatum("00:00:00"), 1},
		{MinNotNullDatum(), NewDatum("00:00:00"), -1},
		{Datum{}, NewDatum("00:00:00"), -1},
		{Datum{}, Datum{}, 0},
		{MinNotNullDatum(), MinNotNullDatum(), 0},
		{MaxValueDatum(), MaxValueDatum(), 0},
		{Datum{}, MinNotNullDatum(), -1},
		{MinNotNullDatum(), MaxValueDatum(), -1},
	}
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	for i, t := range cmpTbl {
		comment := Commentf("%d %v %v", i, t.lhs, t.rhs)
		ret, err := t.lhs.CompareDatum(sc, t.rhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.ret, comment)

		ret, err = t.rhs.CompareDatum(sc, t.lhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, -t.ret, comment)
	}
}
