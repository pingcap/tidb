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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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
		{"2011-01-01 11:11:11", NewTime(FromGoTime(time.Now()), mysql.TypeDatetime, 0), -1},
		{"12:00:00", ZeroDuration, 1},
		{ZeroDuration, ZeroDuration, 0},
		{NewTime(FromGoTime(time.Now().Add(time.Second*10)), mysql.TypeDatetime, 0),
			NewTime(FromGoTime(time.Now()), mysql.TypeDatetime, 0), 1},

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

		{432, nil, 1},
		{-4, 32, -1},
		{4, -32, 1},
		{432, int64(12), 1},
		{23, int64(128), -1},
		{123, int64(123), 0},
		{432, 12, 1},
		{23, 123, -1},
		{int64(133), 183, -1},

		{uint64(133), uint64(183), -1},
		{uint64(2), int64(-2), 1},
		{uint64(2), int64(1), 1},

		{"", nil, 1},
		{"", "24", -1},
		{"aasf", "4", 1},
		{"", "", 0},

		{[]byte(""), nil, 1},
		{[]byte(""), []byte("sff"), -1},

		{NewTime(ZeroCoreTime, 0, 0), nil, 1},
		{NewTime(ZeroCoreTime, 0, 0), NewTime(FromGoTime(time.Now()), mysql.TypeDatetime, 3), -1},
		{NewTime(FromGoTime(time.Now()), mysql.TypeDatetime, 3), "0000-00-00 00:00:00", 1},

		{Duration{Duration: time.Duration(34), Fsp: 2}, nil, 1},
		{Duration{Duration: time.Duration(34), Fsp: 2}, Duration{Duration: time.Duration(29034), Fsp: 2}, -1},
		{Duration{Duration: time.Duration(3340), Fsp: 2}, Duration{Duration: time.Duration(34), Fsp: 2}, 1},
		{Duration{Duration: time.Duration(34), Fsp: 2}, Duration{Duration: time.Duration(34), Fsp: 2}, 0},

		{[]byte{}, []byte{}, 0},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte("123"), 1234, -1},
		{[]byte{}, nil, 1},

		{NewBinaryLiteralFromUint(1, -1), 1, 0},
		{NewBinaryLiteralFromUint(0x4D7953514C, -1), "MySQL", 0},
		{NewBinaryLiteralFromUint(0, -1), uint64(10), -1},
		{NewBinaryLiteralFromUint(1, -1), float64(0), 1},
		{NewBinaryLiteralFromUint(1, -1), NewDecFromInt(1), 0},
		{NewBinaryLiteralFromUint(1, -1), NewBinaryLiteralFromUint(0, -1), 1},
		{NewBinaryLiteralFromUint(1, -1), NewBinaryLiteralFromUint(1, -1), 0},

		{Enum{Name: "a", Value: 1}, 1, 0},
		{Enum{Name: "a", Value: 1}, "a", 0},
		{Enum{Name: "a", Value: 1}, uint64(10), -1},
		{Enum{Name: "a", Value: 1}, float64(0), 1},
		{Enum{Name: "a", Value: 1}, NewDecFromInt(1), 0},
		{Enum{Name: "a", Value: 1}, NewBinaryLiteralFromUint(2, -1), -1},
		{Enum{Name: "a", Value: 1}, NewBinaryLiteralFromUint(1, -1), 0},
		{Enum{Name: "a", Value: 1}, Enum{Name: "a", Value: 1}, 0},

		{Set{Name: "a", Value: 1}, 1, 0},
		{Set{Name: "a", Value: 1}, "a", 0},
		{Set{Name: "a", Value: 1}, uint64(10), -1},
		{Set{Name: "a", Value: 1}, float64(0), 1},
		{Set{Name: "a", Value: 1}, NewDecFromInt(1), 0},
		{Set{Name: "a", Value: 1}, NewBinaryLiteralFromUint(2, -1), -1},
		{Set{Name: "a", Value: 1}, NewBinaryLiteralFromUint(1, -1), 0},
		{Set{Name: "a", Value: 1}, Enum{Name: "a", Value: 1}, 0},
		{Set{Name: "a", Value: 1}, Set{Name: "a", Value: 1}, 0},

		{"hello", NewDecFromInt(0), 0}, // compatible with MySQL.
		{NewDecFromInt(0), "hello", 0},
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
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	sc.IgnoreZeroInDate = true
	aDatum := NewDatum(a)
	bDatum := NewDatum(b)
	return aDatum.CompareDatum(sc, &bDatum)
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
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for i, t := range cmpTbl {
		comment := Commentf("%d %v %v", i, t.lhs, t.rhs)
		ret, err := t.lhs.CompareDatum(sc, &t.rhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.ret, comment)

		ret, err = t.rhs.CompareDatum(sc, &t.lhs)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, -t.ret, comment)
	}
}

func (s *testCompareSuite) TestVecCompareIntAndUint(c *C) {
	defer testleak.AfterTest(c)()
	cmpTblUU := []struct {
		lhs []uint64
		rhs []uint64
		ret []int64
	}{
		{[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []uint64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []int64{-1, -1, -1, -1, -1, 1, 1, 1, 1, 1}},
		{[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{[]uint64{math.MaxInt64, math.MaxInt64 + 1, math.MaxInt64 + 2, math.MaxInt64 + 3, math.MaxInt64 + 4, math.MaxInt64 + 5, math.MaxInt64 + 6, math.MaxInt64 + 7, math.MaxInt64 + 8, math.MaxInt64 + 9}, []uint64{math.MaxInt64, math.MaxInt64 + 1, math.MaxInt64 + 2, math.MaxInt64 + 3, math.MaxInt64 + 4, math.MaxInt64 + 5, math.MaxInt64 + 6, math.MaxInt64 + 7, math.MaxInt64 + 8, math.MaxInt64 + 9}, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, t := range cmpTblUU {
		res := []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		VecCompareUU(t.lhs, t.rhs, res)
		c.Assert(len(res), Equals, len(t.ret))
		for i, v := range res {
			c.Assert(v, Equals, t.ret[i])
		}
	}

	cmpTblII := []struct {
		lhs []int64
		rhs []int64
		ret []int64
	}{
		{[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []int64{-1, -1, -1, -1, -1, 1, 1, 1, 1, 1}},
		{[]int64{0, -1, -2, -3, -4, -5, -6, -7, -8, -9}, []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []int64{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1}},
		{[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{-9, -8, -7, -6, -5, -4, -3, -2, -1, 0}, []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{[]int64{0, -1, -2, -3, -4, -5, -6, -7, -8, -9}, []int64{-9, -8, -7, -6, -5, -4, -3, -2, -1, 0}, []int64{1, 1, 1, 1, 1, -1, -1, -1, -1, -1}},
		{[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, t := range cmpTblII {
		res := []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		VecCompareII(t.lhs, t.rhs, res)
		c.Assert(len(res), Equals, len(t.ret))
		for i, v := range res {
			c.Assert(v, Equals, t.ret[i])
		}
	}

	cmpTblIU := []struct {
		lhs []int64
		rhs []uint64
		ret []int64
	}{
		{[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []uint64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []int64{-1, -1, -1, -1, -1, 1, 1, 1, 1, 1}},
		{[]int64{0, -1, -2, -3, -4, -5, -6, -7, -8, -9}, []uint64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []int64{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1}},
		{[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []uint64{math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1}, []int64{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1}},
	}
	for _, t := range cmpTblIU {
		res := []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		VecCompareIU(t.lhs, t.rhs, res)
		c.Assert(len(res), Equals, len(t.ret))
		for i, v := range res {
			c.Assert(v, Equals, t.ret[i])
		}
	}

	cmpTblUI := []struct {
		lhs []uint64
		rhs []int64
		ret []int64
	}{
		{[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, []int64{-1, -1, -1, -1, -1, 1, 1, 1, 1, 1}},
		{[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{-9, -8, -7, -6, -5, -4, -3, -2, -1, 0}, []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{[]uint64{math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1, math.MaxInt64 + 1}, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
	}
	for _, t := range cmpTblUI {
		res := []int64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		VecCompareUI(t.lhs, t.rhs, res)
		c.Assert(len(res), Equals, len(t.ret))
		for i, v := range res {
			c.Assert(v, Equals, t.ret[i])
		}
	}
}
