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
	"io"
	"reflect"
	"testing"
	"time"

	. "github.com/pingcap/check"
	mysql "github.com/pingcap/tidb/mysqldef"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTypeEtcSuite{})

type testTypeEtcSuite struct {
}

func testIsTypeBlob(c *C, tp byte, expect bool) {
	v := IsTypeBlob(tp)
	c.Assert(v, Equals, expect)
}

func testIsTypeChar(c *C, tp byte, expect bool) {
	v := IsTypeChar(tp)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestIsType(c *C) {
	testIsTypeBlob(c, mysql.TypeTinyBlob, true)
	testIsTypeBlob(c, mysql.TypeMediumBlob, true)
	testIsTypeBlob(c, mysql.TypeBlob, true)
	testIsTypeBlob(c, mysql.TypeLongBlob, true)
	testIsTypeBlob(c, mysql.TypeInt24, false)

	testIsTypeChar(c, mysql.TypeString, true)
	testIsTypeChar(c, mysql.TypeVarchar, true)
	testIsTypeChar(c, mysql.TypeLong, false)
}

func testTypeStr(c *C, tp byte, expect string) {
	v := TypeStr(tp)
	c.Assert(v, Equals, expect)
}

func testTypeToStr(c *C, tp byte, binary bool, expect string) {
	v := TypeToStr(tp, binary)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestTypeToStr(c *C) {
	testTypeStr(c, mysql.TypeYear, "year")
	testTypeStr(c, 0xdd, "")

	testTypeToStr(c, mysql.TypeBlob, true, "text")
	testTypeToStr(c, mysql.TypeLongBlob, true, "longtext")
	testTypeToStr(c, mysql.TypeTinyBlob, true, "tinytext")
	testTypeToStr(c, mysql.TypeMediumBlob, true, "mediumtext")
	testTypeToStr(c, mysql.TypeVarchar, true, "varbinary")
	testTypeToStr(c, mysql.TypeString, true, "binary")
	testTypeToStr(c, mysql.TypeTiny, true, "tinyint")
	testTypeToStr(c, mysql.TypeBlob, false, "blob")
	testTypeToStr(c, mysql.TypeLongBlob, false, "longblob")
	testTypeToStr(c, mysql.TypeTinyBlob, false, "tinyblob")
	testTypeToStr(c, mysql.TypeMediumBlob, false, "mediumblob")
	testTypeToStr(c, mysql.TypeVarchar, false, "varchar")
	testTypeToStr(c, mysql.TypeString, false, "char")
	testTypeToStr(c, mysql.TypeShort, true, "smallint")
	testTypeToStr(c, mysql.TypeInt24, true, "mediumint")
	testTypeToStr(c, mysql.TypeLong, true, "int")
	testTypeToStr(c, mysql.TypeLonglong, true, "bigint")
	testTypeToStr(c, mysql.TypeFloat, true, "float")
	testTypeToStr(c, mysql.TypeDouble, true, "double")
	testTypeToStr(c, mysql.TypeYear, true, "year")
	testTypeToStr(c, mysql.TypeDuration, true, "time")
	testTypeToStr(c, mysql.TypeDatetime, true, "datetime")
	testTypeToStr(c, mysql.TypeDate, true, "date")
	testTypeToStr(c, mysql.TypeTimestamp, true, "timestamp")
	testTypeToStr(c, mysql.TypeNewDecimal, true, "decimal")
	testTypeToStr(c, mysql.TypeDecimal, true, "decimal")
	testTypeToStr(c, 0xdd, true, "")
	testTypeToStr(c, mysql.TypeBit, true, "bit")
	testTypeToStr(c, mysql.TypeEnum, true, "enum")
	testTypeToStr(c, mysql.TypeSet, true, "set")
}

func (s *testTypeEtcSuite) TestEOFAsNil(c *C) {
	err := EOFAsNil(io.EOF)
	c.Assert(err, IsNil)
}

func checkCollate(c *C, x, y []interface{}, expect int) {
	v := collate(x, y)
	c.Assert(v, Equals, expect)
}

func checkCollateDesc(c *C, x, y []interface{}, expect int) {
	v := collateDesc(x, y)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestCollate(c *C) {
	checkCollate(c, []interface{}{1, 2}, nil, 1)
	checkCollate(c, nil, []interface{}{1, 2}, -1)
	checkCollate(c, nil, nil, 0)
	checkCollate(c, []interface{}{1, 2}, []interface{}{3}, -1)
	checkCollate(c, []interface{}{1, 2}, []interface{}{1, 2}, 0)
	checkCollate(c, []interface{}{3, 2, 5}, []interface{}{3, 2}, 1)

	checkCollateDesc(c, []interface{}{1, 2}, nil, -1)
	checkCollateDesc(c, nil, []interface{}{1, 2}, 1)
	checkCollateDesc(c, nil, nil, 0)
	checkCollateDesc(c, []interface{}{1, 2}, []interface{}{3}, 1)
	checkCollateDesc(c, []interface{}{1, 2}, []interface{}{1, 2}, 0)
	checkCollateDesc(c, []interface{}{3, 2, 5}, []interface{}{3, 2}, -1)
}

func checkClone(c *C, a interface{}, pass bool) {
	b, err := Clone(a)
	if pass {
		c.Assert(err, DeepEquals, nil)
		c.Assert(a, DeepEquals, b)
		return
	}
	c.Assert(err, NotNil)
}

func (s *testTypeEtcSuite) TestClone(c *C) {
	checkClone(c, nil, true)
	checkClone(c, uint16(111), true)
	checkClone(c, "abcd1.c--/+!%^", true)
	checkClone(c, []byte("aa028*(%^"), true)
	checkClone(c, []interface{}{1, 2}, true)
	checkClone(c, mysql.Duration{Duration: time.Duration(32), Fsp: 0}, true)
	checkClone(c, mysql.Decimal{}, true)
	checkClone(c, mysql.Time{Time: time.Now(), Type: 1, Fsp: 3}, true)
	checkClone(c, make(map[int]string), false)
	checkClone(c, mysql.Hex{Value: 1}, true)
	checkClone(c, mysql.Bit{Value: 1, Width: 1}, true)
	checkClone(c, mysql.Enum{Name: "a", Value: 1}, true)
	checkClone(c, mysql.Set{Name: "a", Value: 1}, true)
}

func checkCoerce(c *C, a, b interface{}) {
	a, b = Coerce(a, b)
	var hasFloat, hasDecimal bool
	switch x := a.(type) {
	case int64:
	case uint64:
	case float64:
		hasFloat = true
	case mysql.Time:
	case mysql.Duration:
	case mysql.Decimal:
		hasDecimal = true
	default:
		c.Error("unexpected type", reflect.TypeOf(x))
	}
	switch x := b.(type) {
	case int64:
	case uint64:
	case float64:
		hasFloat = true
	case mysql.Time:
	case mysql.Duration:
	case mysql.Decimal:
		hasDecimal = true
	default:
		c.Error("unexpected type", reflect.TypeOf(x))
	}
	if hasDecimal {
		_, ok := a.(mysql.Decimal)
		c.Assert(ok, IsTrue)
		_, ok = b.(mysql.Decimal)
		c.Assert(ok, IsTrue)
	} else if hasFloat {
		_, ok := a.(float64)
		c.Assert(ok, IsTrue)
		_, ok = b.(float64)
		c.Assert(ok, IsTrue)
	}
}

func (s *testTypeEtcSuite) TestCoerce(c *C) {
	checkCoerce(c, uint64(3), int16(4))
	checkCoerce(c, uint64(0xffffffffffffffff), float64(2.3))
	checkCoerce(c, float64(1.3), uint64(0xffffffffffffffff))
	checkCoerce(c, int64(11), float64(4.313))
	checkCoerce(c, uint(2), uint16(52))
	checkCoerce(c, uint8(8), true)
	checkCoerce(c, uint32(62), int8(8))
	checkCoerce(c, mysql.NewDecimalFromInt(1, 0), false)
	checkCoerce(c, float32(3.4), mysql.NewDecimalFromUint(1, 0))
	checkCoerce(c, int32(43), 3.235)
}

func (s *testTypeEtcSuite) TestIsOrderedType(c *C) {
	r := IsOrderedType(1)
	c.Assert(r, IsTrue)
	r = IsOrderedType(-1)
	c.Assert(r, IsTrue)
	r = IsOrderedType(uint(1))
	c.Assert(r, IsTrue)

	r = IsOrderedType(mysql.Duration{Duration: time.Duration(0), Fsp: 0})
	c.Assert(r, IsTrue)

	r = IsOrderedType([]byte{1})
	c.Assert(r, IsTrue)
}

func (s *testTypeEtcSuite) TestMaxFloat(c *C) {
	tbl := []struct {
		Flen    int
		Decimal int
		Expect  float64
	}{
		{3, 2, 9.99},
		{5, 2, 999.99},
		{10, 1, 999999999.9},
		{5, 5, 0.99999},
	}

	for _, t := range tbl {
		f := getMaxFloat(t.Flen, t.Decimal)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestRoundFloat(c *C) {
	tbl := []struct {
		Input  float64
		Expect float64
	}{
		{2.5, 2},
		{1.5, 2},
		{0.5, 0},
		{0, 0},
		{-0.5, 0},
		{-2.5, -2},
		{-1.5, -2},
	}

	for _, t := range tbl {
		f := RoundFloat(t.Input)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestTruncate(c *C) {
	tbl := []struct {
		Input   float64
		Decimal int
		Expect  float64
	}{
		{100.114, 2, 100.11},
		{100.115, 2, 100.11},
		{100.1156, 2, 100.12},
	}

	for _, t := range tbl {
		f := truncateFloat(t.Input, t.Decimal)
		c.Assert(f, Equals, t.Expect)
	}
}
