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
)

var _ = Suite(&testCompareSuite{})

type testCompareSuite struct {
}

func (s *testCompareSuite) TestCompare(c *C) {
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
		{mysql.NewDecimalFromInt(1, 0), mysql.NewDecimalFromInt(1, 0), 0},
		{mysql.NewDecimalFromInt(1, 0), "1", 0},
		{mysql.NewDecimalFromInt(1, 0), []byte("1"), 0},
		{"1", "1", 0},
		{"1", int64(-1), 1},
		{"1", float64(2), -1},
		{"1", uint64(1), 0},
		{"1", mysql.NewDecimalFromInt(1, 0), 0},
		{"2011-01-01 11:11:11", mysql.Time{Time: time.Now(), Type: mysql.TypeDatetime, Fsp: 0}, -1},
		{"12:00:00", mysql.ZeroDuration, 1},
		{mysql.ZeroDuration, mysql.ZeroDuration, 0},
		{mysql.Time{Time: time.Now().Add(time.Second * 10), Type: mysql.TypeDatetime, Fsp: 0},
			mysql.Time{Time: time.Now(), Type: mysql.TypeDatetime, Fsp: 0}, 1},

		{nil, 2, -1},
		{nil, nil, 0},

		{false, nil, 1},
		{false, true, -1},
		{true, true, 0},
		{false, false, 0},
		{true, 2, -1},

		{float64(1.23), nil, 1},
		{float64(1.23), float32(3.45), -1},
		{float64(354.23), float32(3.45), 1},
		{float64(0.0), float64(3.45), -1},
		{float64(354.23), float64(3.45), 1},
		{float64(3.452), float64(3.452), 0},
		{float32(1.23), nil, 1},

		{int(432), nil, 1},
		{-4, int(32), -1},
		{int(4), -32, 1},
		{int(432), int8(12), 1},
		{int(23), int8(28), -1},
		{int(123), int8(123), 0},
		{int(432), int16(12), 1},
		{int(23), int16(128), -1},
		{int(123), int16(123), 0},
		{int(432), int32(12), 1},
		{int(23), int32(128), -1},
		{int(123), int32(123), 0},
		{int(432), int64(12), 1},
		{int(23), int64(128), -1},
		{int(123), int64(123), 0},
		{int(432), int(12), 1},
		{int(23), int(123), -1},
		{int8(3), int(3), 0},
		{int16(923), int(180), 1},
		{int32(173), int(120), 1},
		{int64(133), int(183), -1},

		{uint(23), nil, 1},
		{uint(23), uint(123), -1},
		{uint8(3), uint8(3), 0},
		{uint16(923), uint16(180), 1},
		{uint32(173), uint32(120), 1},
		{uint64(133), uint64(183), -1},
		{uint64(2), int64(-2), 1},
		{uint64(2), int64(1), 1},

		{"", nil, 1},
		{"", "24", -1},
		{"aasf", "4", 1},
		{"", "", 0},

		{[]byte(""), nil, 1},
		{[]byte(""), []byte("sff"), -1},

		{mysql.Time{}, nil, 1},
		{mysql.Time{}, mysql.Time{Time: time.Now(), Type: mysql.TypeDatetime, Fsp: 3}, -1},
		{mysql.Time{Time: time.Now(), Type: mysql.TypeDatetime, Fsp: 3}, "0000-00-00 00:00:00", 1},

		{mysql.Duration{Duration: time.Duration(34), Fsp: 2}, nil, 1},
		{mysql.Duration{Duration: time.Duration(34), Fsp: 2}, mysql.Duration{Duration: time.Duration(29034), Fsp: 2}, -1},
		{mysql.Duration{Duration: time.Duration(3340), Fsp: 2}, mysql.Duration{Duration: time.Duration(34), Fsp: 2}, 1},
		{mysql.Duration{Duration: time.Duration(34), Fsp: 2}, mysql.Duration{Duration: time.Duration(34), Fsp: 2}, 0},

		{[]byte{}, []byte{}, 0},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte("123"), 1234, -1},
		{[]byte{}, nil, 1},

		{[]interface{}{1, 2, 3}, []interface{}{1, 2, 3}, 0},
		{[]interface{}{1, 3, 3}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 2, 3}, []interface{}{2, 2, 3}, -1},

		{mysql.Hex{Value: 1}, 1, 0},
		{mysql.Hex{Value: 0x4D7953514C}, "MySQL", 0},
		{mysql.Hex{Value: 0}, uint64(10), -1},
		{mysql.Hex{Value: 1}, float64(0), 1},
		{mysql.Hex{Value: 1}, mysql.NewDecimalFromInt(1, 0), 0},
		{mysql.Hex{Value: 1}, mysql.Bit{Value: 0, Width: 1}, 1},
		{mysql.Hex{Value: 1}, mysql.Hex{Value: 1}, 0},

		{mysql.Bit{Value: 1, Width: 1}, 1, 0},
		{mysql.Bit{Value: 0x41, Width: 8}, "A", 0},
		{mysql.Bit{Value: 1, Width: 1}, uint64(10), -1},
		{mysql.Bit{Value: 1, Width: 1}, float64(0), 1},
		{mysql.Bit{Value: 1, Width: 1}, mysql.NewDecimalFromInt(1, 0), 0},
		{mysql.Bit{Value: 1, Width: 1}, mysql.Hex{Value: 2}, -1},
		{mysql.Bit{Value: 1, Width: 1}, mysql.Bit{Value: 1, Width: 1}, 0},

		{mysql.Enum{Name: "a", Value: 1}, 1, 0},
		{mysql.Enum{Name: "a", Value: 1}, "a", 0},
		{mysql.Enum{Name: "a", Value: 1}, uint64(10), -1},
		{mysql.Enum{Name: "a", Value: 1}, float64(0), 1},
		{mysql.Enum{Name: "a", Value: 1}, mysql.NewDecimalFromInt(1, 0), 0},
		{mysql.Enum{Name: "a", Value: 1}, mysql.Hex{Value: 2}, -1},
		{mysql.Enum{Name: "a", Value: 1}, mysql.Bit{Value: 1, Width: 1}, 0},
		{mysql.Enum{Name: "a", Value: 1}, mysql.Hex{Value: 1}, 0},
		{mysql.Enum{Name: "a", Value: 1}, mysql.Enum{Name: "a", Value: 1}, 0},

		{mysql.Set{Name: "a", Value: 1}, 1, 0},
		{mysql.Set{Name: "a", Value: 1}, "a", 0},
		{mysql.Set{Name: "a", Value: 1}, uint64(10), -1},
		{mysql.Set{Name: "a", Value: 1}, float64(0), 1},
		{mysql.Set{Name: "a", Value: 1}, mysql.NewDecimalFromInt(1, 0), 0},
		{mysql.Set{Name: "a", Value: 1}, mysql.Hex{Value: 2}, -1},
		{mysql.Set{Name: "a", Value: 1}, mysql.Bit{Value: 1, Width: 1}, 0},
		{mysql.Set{Name: "a", Value: 1}, mysql.Hex{Value: 1}, 0},
		{mysql.Set{Name: "a", Value: 1}, mysql.Enum{Name: "a", Value: 1}, 0},
		{mysql.Set{Name: "a", Value: 1}, mysql.Set{Name: "a", Value: 1}, 0},
	}

	for _, t := range cmpTbl {
		ret, err := Compare(t.lhs, t.rhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.ret)

		ret, err = Compare(t.rhs, t.lhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, -t.ret)
	}

	cmpError := []struct {
		lhs interface{}
		rhs interface{}
	}{
		{[]interface{}{1}, 1},
		{[]interface{}{1}, []interface{}{1, 2}},
		{mysql.NewDecimalFromInt(1, 0), "++abc"},
		{float64(1.0), "++abc"},
		{[]interface{}{1}, []interface{}{"+-abc"}},
	}

	for _, t := range cmpError {
		_, err := Compare(t.lhs, t.rhs)
		c.Assert(err, NotNil)

		_, err = Compare(t.rhs, t.lhs)
		c.Assert(err, NotNil)
	}

}
