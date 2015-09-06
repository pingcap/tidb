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
	testTypeStr(c, mysql.TypeYear, "YEAR")
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
}

func (s *testTypeEtcSuite) TestEOFAsNil(c *C) {
	err := EOFAsNil(io.EOF)
	c.Assert(err, IsNil)
}

func checkCompare(c *C, x, y interface{}, expect int) {
	v := Compare(x, y)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestCompare(c *C) {
	checkCompare(c, nil, 2, -1)
	checkCompare(c, nil, nil, 0)

	checkCompare(c, false, nil, 1)
	checkCompare(c, false, true, -1)
	checkCompare(c, true, true, 0)
	checkCompare(c, false, false, 0)
	checkCompare(c, true, "", -1)

	checkCompare(c, float64(1.23), nil, 1)
	checkCompare(c, float64(1.23), float32(3.45), -1)
	checkCompare(c, float64(354.23), float32(3.45), 1)
	checkCompare(c, float64(0.0), float64(3.45), -1)
	checkCompare(c, float64(354.23), float64(3.45), 1)
	checkCompare(c, float64(3.452), float64(3.452), 0)
	checkCompare(c, float32(1.23), nil, 1)

	checkCompare(c, int(432), nil, 1)
	checkCompare(c, -4, int(32), -1)
	checkCompare(c, int(4), -32, 1)
	checkCompare(c, int(432), int8(12), 1)
	checkCompare(c, int(23), int8(28), -1)
	checkCompare(c, int(123), int8(123), 0)
	checkCompare(c, int(432), int16(12), 1)
	checkCompare(c, int(23), int16(128), -1)
	checkCompare(c, int(123), int16(123), 0)
	checkCompare(c, int(432), int32(12), 1)
	checkCompare(c, int(23), int32(128), -1)
	checkCompare(c, int(123), int32(123), 0)
	checkCompare(c, int(432), int64(12), 1)
	checkCompare(c, int(23), int64(128), -1)
	checkCompare(c, int(123), int64(123), 0)
	checkCompare(c, int(432), int(12), 1)
	checkCompare(c, int(23), int(123), -1)
	checkCompare(c, int8(3), int(3), 0)
	checkCompare(c, int16(923), int(180), 1)
	checkCompare(c, int32(173), int(120), 1)
	checkCompare(c, int64(133), int(183), -1)

	checkCompare(c, uint(23), nil, 1)
	checkCompare(c, uint(23), uint(123), -1)
	checkCompare(c, uint8(3), uint8(3), 0)
	checkCompare(c, uint16(923), uint16(180), 1)
	checkCompare(c, uint32(173), uint32(120), 1)
	checkCompare(c, uint64(133), uint64(183), -1)

	checkCompare(c, "", nil, 1)
	checkCompare(c, "", "24", -1)
	checkCompare(c, "aasf", "4", 1)
	checkCompare(c, "", "", 0)

	checkCompare(c, []byte(""), nil, 1)
	checkCompare(c, []byte(""), []byte("sff"), -1)

	checkCompare(c, mysql.Time{}, nil, 1)
	checkCompare(c, mysql.Time{}, mysql.Time{Time: time.Now(), Type: 1, Fsp: 3}, -1)
	checkCompare(c, mysql.Time{Time: time.Now(), Type: 1, Fsp: 3}, "11:11", 1)

	checkCompare(c, mysql.Duration{Duration: time.Duration(34), Fsp: 2}, nil, 1)
	checkCompare(c, mysql.Duration{Duration: time.Duration(34), Fsp: 2},
		mysql.Duration{Duration: time.Duration(29034), Fsp: 2}, -1)
	checkCompare(c, mysql.Duration{Duration: time.Duration(3340), Fsp: 2},
		mysql.Duration{Duration: time.Duration(34), Fsp: 2}, 1)
	checkCompare(c, mysql.Duration{Duration: time.Duration(34), Fsp: 2},
		mysql.Duration{Duration: time.Duration(34), Fsp: 2}, 0)

	checkCompare(c, mysql.Decimal{}, mysql.Decimal{}, 0)
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
	_, r, err := IsOrderedType(1)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	_, r, err = IsOrderedType(-1)
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)
	_, r, err = IsOrderedType(uint(1))
	c.Assert(err, IsNil)
	c.Assert(r, IsTrue)

	_, r, err = IsOrderedType(mysql.Duration{Duration: time.Duration(0), Fsp: 0})
	c.Assert(err, IsNil)
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
