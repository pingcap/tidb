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

	. "github.com/pingcap/check"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/charset"
)

var _ = Suite(&testTypeConvertSuite{})

type testTypeConvertSuite struct {
}

type invalidMockType struct {
}

func (s *testTypeConvertSuite) TestConvertType(c *C) {
	ft := NewFieldType(mysql.TypeBlob)
	ft.Flen = 4
	ft.Charset = "utf8"
	v, err := Convert("123456", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "1234")

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(111.114, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(111.11))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.999, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(-999.999, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(-999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(1111.11, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.916, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.92))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.915, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.91))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.9155, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.92))

	// For TypeBlob
	ft = NewFieldType(mysql.TypeBlob)
	v, err = Convert(&invalidMockType{}, ft)
	c.Assert(err, NotNil)

	// Nil
	ft = NewFieldType(mysql.TypeBlob)
	v, err = Convert(nil, ft)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	// TypeDouble
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.9155, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float64(999.92))

	// For TypeString
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	v, err = Convert("12345", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "123")

	// For TypeDuration
	ft = NewFieldType(mysql.TypeDuration)
	ft.Decimal = 3
	v, err = Convert("10:11:12.123456", ft)
	c.Assert(err, IsNil)
	c.Assert(v.(mysql.Duration).String(), Equals, "10:11:12.123")
	ft.Decimal = 1
	vv, err := Convert(v, ft)
	c.Assert(err, IsNil)
	c.Assert(vv.(mysql.Duration).String(), Equals, "10:11:12.1")

	vt, err := mysql.ParseTime("2010-10-10 10:11:11.12345", mysql.TypeTimestamp, 2)
	c.Assert(vt.String(), Equals, "2010-10-10 10:11:11.12")
	c.Assert(err, IsNil)
	v, err = Convert(vt, ft)
	c.Assert(err, IsNil)
	c.Assert(v.(mysql.Duration).String(), Equals, "10:11:11.1")

	// For mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Decimal = 3
	v, err = Convert("2010-10-10 10:11:11.12345", ft)
	c.Assert(err, IsNil)
	c.Assert(v.(mysql.Time).String(), Equals, "2010-10-10 10:11:11.123")
	ft.Decimal = 1
	vv, err = Convert(v, ft)
	c.Assert(err, IsNil)
	c.Assert(vv.(mysql.Time).String(), Equals, "2010-10-10 10:11:11.1")

	// For TypeLonglong
	ft = NewFieldType(mysql.TypeLonglong)
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(100))

	// For TypeNewDecimal
	ft = NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err = Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	c.Assert(v.(mysql.Decimal).String(), Equals, "3.14159")

	// For TypeYear
	ft = NewFieldType(mysql.TypeYear)
	v, err = Convert("2015-11-11", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int16(2015))
	v, err = Convert(2015, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int16(2015))
	v, err = Convert(1800, ft)
	c.Assert(err, NotNil)
}

func testToInt64(c *C, val interface{}, expect int64) {
	b, err := ToInt64(val)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestConvertToInt64(c *C) {
	testToInt64(c, "0", int64(0))
	testToInt64(c, false, int64(0))
	testToInt64(c, true, int64(1))
	testToInt64(c, int(0), int64(0))
	testToInt64(c, int8(0), int64(0))
	testToInt64(c, int16(0), int64(0))
	testToInt64(c, int32(0), int64(0))
	testToInt64(c, int64(0), int64(0))
	testToInt64(c, uint(0), int64(0))
	testToInt64(c, uint8(0), int64(0))
	testToInt64(c, uint16(0), int64(0))
	testToInt64(c, uint32(0), int64(0))
	testToInt64(c, uint64(0), int64(0))
	testToInt64(c, float32(3.1), int64(3))
	testToInt64(c, float64(3.1), int64(3))

	t, err := mysql.ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 0)
	c.Assert(err, IsNil)
	testToInt64(c, t, int64(20111110111112))

	td, err := mysql.ParseDuration("11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToInt64(c, td, int64(111112))

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	testToInt64(c, v, int64(3))

	_, err = ToInt64(&invalidMockType{})
	c.Assert(err, NotNil)
}

func testToFloat64(c *C, val interface{}, expect float64) {
	b, err := ToFloat64(val)
	c.Assert(err, IsNil)
	diff := math.Abs(b - expect)
	Epsilon := float64(0.00000001)
	c.Assert(Epsilon, Greater, diff)
}

func (s *testTypeConvertSuite) TestConvertToFloat64(c *C) {
	testToFloat64(c, "0", float64(0))
	testToFloat64(c, false, float64(0))
	testToFloat64(c, true, float64(1))
	testToFloat64(c, int(0), float64(0))
	testToFloat64(c, int8(0), float64(0))
	testToFloat64(c, int16(0), float64(0))
	testToFloat64(c, int32(0), float64(0))
	testToFloat64(c, int64(0), float64(0))
	testToFloat64(c, uint(0), float64(0))
	testToFloat64(c, uint8(0), float64(0))
	testToFloat64(c, uint16(0), float64(0))
	testToFloat64(c, uint32(0), float64(0))
	testToFloat64(c, uint64(0), float64(0))
	// TODO: check this
	//testToFloat64(c, float32(3.1), float64(3.1))
	testToFloat64(c, float64(3.1), float64(3.1))

	t, err := mysql.ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testToFloat64(c, t, float64(20111110111111.999999))

	td, err := mysql.ParseDuration("11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToFloat64(c, td, float64(111111.999999))

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	testToFloat64(c, v, float64(3.14159))

	_, err = ToFloat64(&invalidMockType{})
	c.Assert(err, NotNil)
}

func testToString(c *C, val interface{}, expect string) {
	b, err := ToString(val)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestConvertToString(c *C) {
	testToString(c, "0", "0")
	testToString(c, true, "1")
	testToString(c, "false", "false")
	testToString(c, int(0), "0")
	testToString(c, int8(0), "0")
	testToString(c, int16(0), "0")
	testToString(c, int32(0), "0")
	testToString(c, int64(0), "0")
	testToString(c, uint(0), "0")
	testToString(c, uint8(0), "0")
	testToString(c, uint16(0), "0")
	testToString(c, uint32(0), "0")
	testToString(c, uint64(0), "0")
	testToString(c, float32(1.6), "1.6")
	testToString(c, float64(-0.6), "-0.6")
	testToString(c, []byte{1}, "\x01")

	t, err := mysql.ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testToString(c, t, "2011-11-10 11:11:11.999999")

	td, err := mysql.ParseDuration("11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToString(c, td, "11:11:11.999999")

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	testToString(c, v, "3.14159")

	_, err = ToString(&invalidMockType{})
	c.Assert(err, NotNil)
}

func testToBool(c *C, val interface{}, expect int8) {
	b, err := ToBool(val)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestConvertToBool(c *C) {
	testToBool(c, false, int8(0))
	testToBool(c, int(0), int8(0))
	testToBool(c, int8(0), int8(0))
	testToBool(c, int16(0), int8(0))
	testToBool(c, int32(0), int8(0))
	testToBool(c, int64(0), int8(0))
	testToBool(c, uint(0), int8(0))
	testToBool(c, uint8(0), int8(0))
	testToBool(c, uint16(0), int8(0))
	testToBool(c, uint32(0), int8(0))
	testToBool(c, uint64(0), int8(0))
	testToBool(c, float32(0), int8(0))
	testToBool(c, float64(0), int8(0))
	testToBool(c, "", int8(0))
	testToBool(c, "0", int8(0))
	testToBool(c, []byte{}, int8(0))
	testToBool(c, []byte("0"), int8(0))

	t, err := mysql.ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testToBool(c, t, int8(1))

	td, err := mysql.ParseDuration("11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToBool(c, td, int8(1))

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	testToBool(c, v, int8(1))

	_, err = ToBool(&invalidMockType{})
	c.Assert(err, NotNil)
}

func testStrToInt(c *C, str string, expect int64) {
	b, _ := StrToInt(str)
	c.Assert(b, Equals, expect)
}

func testStrToUint(c *C, str string, expect uint64) {
	b, _ := StrToUint(str)
	c.Assert(b, Equals, expect)
}

func testStrToFloat(c *C, str string, expect float64) {
	b, _ := StrToFloat(str)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestStrToNum(c *C) {
	testStrToInt(c, "0", 0)
	testStrToInt(c, "-1", -1)
	testStrToInt(c, "100", 100)
	testStrToInt(c, "65.0", 65)
	testStrToInt(c, "xx", 0)
	testStrToInt(c, "11xx", 11)
	testStrToInt(c, "xx11", 0)

	testStrToUint(c, "0", 0)
	testStrToUint(c, "", 0)
	testStrToUint(c, "-1", 0xffffffffffffffff)
	testStrToUint(c, "100", 100)
	testStrToUint(c, "+100", 100)
	testStrToUint(c, "65.0", 65)
	testStrToUint(c, "xx", 0)
	testStrToUint(c, "11xx", 11)
	testStrToUint(c, "xx11", 0)

	testStrToFloat(c, "", 0)
	testStrToFloat(c, "-1", -1.0)
	testStrToFloat(c, "1.11", 1.11)
	testStrToFloat(c, "1.11.00", 0.0)
	testStrToFloat(c, "xx", 0.0)
	testStrToFloat(c, "0x00", 0.0)
	testStrToFloat(c, "11.xx", 0.0)
	testStrToFloat(c, "xx.11", 0.0)
}

func (s *testTypeConvertSuite) TestFieldTypeToStr(c *C) {
	v := FieldTypeToStr(mysql.TypeDecimal, "not binary")
	c.Assert(v, Equals, type2Str[mysql.TypeDecimal])
	v = FieldTypeToStr(mysql.TypeBlob, charset.CharsetBin)
	c.Assert(v, Equals, "BLOB")
	v = FieldTypeToStr(mysql.TypeString, charset.CharsetBin)
	c.Assert(v, Equals, "BINARY")
}
