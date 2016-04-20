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
	"fmt"
	"math"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTypeConvertSuite{})

type testTypeConvertSuite struct {
}

type invalidMockType struct {
}

func (s *testTypeConvertSuite) TestConvertType(c *C) {
	defer testleak.AfterTest(c)()
	ft := NewFieldType(mysql.TypeBlob)
	ft.Flen = 4
	ft.Charset = "utf8"
	v, err := Convert("123456", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "1234")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 4
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("1234"))

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
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("123"))

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
	ft = NewFieldType(mysql.TypeLonglong)
	ft.Flag |= mysql.UnsignedFlag
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(100))

	// For TypeBit
	ft = NewFieldType(mysql.TypeBit)
	ft.Flen = 8
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, mysql.Bit{Value: 100, Width: 8})

	v, err = Convert(mysql.Hex{Value: 100}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, mysql.Bit{Value: 100, Width: 8})

	v, err = Convert(mysql.Bit{Value: 100, Width: 8}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, mysql.Bit{Value: 100, Width: 8})

	ft.Flen = 1
	v, err = Convert(1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, mysql.Bit{Value: 1, Width: 1})

	_, err = Convert(2, ft)
	c.Assert(err, NotNil)

	ft.Flen = 0
	_, err = Convert(2, ft)
	c.Assert(err, NotNil)

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
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(2015, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(1800, ft)
	c.Assert(err, NotNil)
	dt, err := mysql.ParseDate("2015-11-11")
	c.Assert(err, IsNil)
	v, err = Convert(dt, ft)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(mysql.ZeroDuration, ft)
	c.Assert(v, Equals, int64(time.Now().Year()))

	// For enum
	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b", "c"}
	v, err = Convert("a", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, mysql.Enum{Name: "a", Value: 1})
	v, err = Convert(2, ft)
	c.Log(errors.ErrorStack(err))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, mysql.Enum{Name: "b", Value: 2})
	_, err = Convert("d", ft)
	c.Assert(err, NotNil)
	_, err = Convert(4, ft)
	c.Assert(err, NotNil)

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b", "c"}
	v, err = Convert("a", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, mysql.Set{Name: "a", Value: 1})
	v, err = Convert(2, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, mysql.Set{Name: "b", Value: 2})
	v, err = Convert(3, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, mysql.Set{Name: "a,b", Value: 3})
	_, err = Convert("d", ft)
	c.Assert(err, NotNil)
	_, err = Convert(9, ft)
	c.Assert(err, NotNil)
}

func testToInt64(c *C, val interface{}, expect int64) {
	b, err := ToInt64(val)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestConvertToInt64(c *C) {
	defer testleak.AfterTest(c)()
	testToInt64(c, "0", int64(0))
	testToInt64(c, int(0), int64(0))
	testToInt64(c, int64(0), int64(0))
	testToInt64(c, uint64(0), int64(0))
	testToInt64(c, float32(3.1), int64(3))
	testToInt64(c, float64(3.1), int64(3))
	testToInt64(c, mysql.Hex{Value: 100}, int64(100))
	testToInt64(c, mysql.Bit{Value: 100, Width: 8}, int64(100))
	testToInt64(c, mysql.Enum{Name: "a", Value: 1}, int64(1))
	testToInt64(c, mysql.Set{Name: "a", Value: 1}, int64(1))

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
	defer testleak.AfterTest(c)()
	testToFloat64(c, "0", float64(0))
	testToFloat64(c, int(0), float64(0))
	testToFloat64(c, int64(0), float64(0))
	testToFloat64(c, uint64(0), float64(0))
	// TODO: check this
	//testToFloat64(c, float32(3.1), float64(3.1))
	testToFloat64(c, float64(3.1), float64(3.1))
	testToFloat64(c, mysql.Hex{Value: 100}, float64(100))
	testToFloat64(c, mysql.Bit{Value: 100, Width: 8}, float64(100))
	testToFloat64(c, mysql.Enum{Name: "a", Value: 1}, float64(1))
	testToFloat64(c, mysql.Set{Name: "a", Value: 1}, float64(1))

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
	defer testleak.AfterTest(c)()
	testToString(c, "0", "0")
	testToString(c, true, "1")
	testToString(c, "false", "false")
	testToString(c, int(0), "0")
	testToString(c, int64(0), "0")
	testToString(c, uint64(0), "0")
	testToString(c, float32(1.6), "1.6")
	testToString(c, float64(-0.6), "-0.6")
	testToString(c, []byte{1}, "\x01")
	testToString(c, mysql.Hex{Value: 0x4D7953514C}, "MySQL")
	testToString(c, mysql.Bit{Value: 0x41, Width: 8}, "A")
	testToString(c, mysql.Enum{Name: "a", Value: 1}, "a")
	testToString(c, mysql.Set{Name: "a", Value: 1}, "a")

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

func testToBool(c *C, val interface{}, expect int64) {
	b, err := ToBool(val)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestConvertToBool(c *C) {
	defer testleak.AfterTest(c)()
	testToBool(c, int(0), 0)
	testToBool(c, int64(0), 0)
	testToBool(c, uint64(0), 0)
	testToBool(c, float32(0.1), 0)
	testToBool(c, float64(0.1), 0)
	testToBool(c, "", 0)
	testToBool(c, "0.1", 0)
	testToBool(c, []byte{}, 0)
	testToBool(c, []byte("0.1"), 0)
	testToBool(c, mysql.Hex{Value: 0}, 0)
	testToBool(c, mysql.Bit{Value: 0, Width: 8}, 0)
	testToBool(c, mysql.Enum{Name: "a", Value: 1}, 1)
	testToBool(c, mysql.Set{Name: "a", Value: 1}, 1)

	t, err := mysql.ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testToBool(c, t, 1)

	td, err := mysql.ParseDuration("11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToBool(c, td, 1)

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(0.1415926, ft)
	c.Assert(err, IsNil)
	testToBool(c, v, 0)

	_, err = ToBool(&invalidMockType{})
	c.Assert(err, NotNil)
}

func testStrToInt(c *C, str string, expect int64) {
	b, _ := StrToInt(str)
	c.Assert(b, Equals, expect)
}

func testStrToUint(c *C, str string, expect uint64) {
	d := NewDatum(str)
	d, _ = d.convertToUint(NewFieldType(mysql.TypeLonglong))
	c.Assert(d.GetUint64(), Equals, expect)
}

func testStrToFloat(c *C, str string, expect float64) {
	b, _ := StrToFloat(str)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestStrToNum(c *C) {
	defer testleak.AfterTest(c)()
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
	// TODO: makes StrToFloat return truncated value instead of zero to make it pass.
	//	testStrToUint(c, "11xx", 11)
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
	defer testleak.AfterTest(c)()
	v := TypeToStr(mysql.TypeDecimal, "not binary")
	c.Assert(v, Equals, type2Str[mysql.TypeDecimal])
	v = TypeToStr(mysql.TypeBlob, charset.CharsetBin)
	c.Assert(v, Equals, "blob")
	v = TypeToStr(mysql.TypeString, charset.CharsetBin)
	c.Assert(v, Equals, "binary")
}

func accept(c *C, tp byte, value interface{}, unsigned bool, expected string) {
	ft := NewFieldType(tp)
	if unsigned {
		ft.Flag |= mysql.UnsignedFlag
	}
	//	casted, err := col.CastValue(nil, value)
	casted, err := Convert(value, ft)
	c.Assert(err, IsNil, Commentf("%v", ft))
	c.Assert(fmt.Sprintf("%v", casted), Equals, expected)
}

func unsignedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, true, expected)
}

func signedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, false, expected)
}

func deny(c *C, tp byte, value interface{}, unsigned bool, expected string) {
	ft := NewFieldType(tp)
	if unsigned {
		ft.Flag |= mysql.UnsignedFlag
	}
	//	casted, err := col.CastValue(nil, value)
	casted, err := Convert(value, ft)
	c.Assert(err, NotNil)
	switch casted.(type) {
	case mysql.Duration:
		c.Assert(casted.(mysql.Duration).String(), Equals, expected)
	default:
		c.Assert(fmt.Sprintf("%v", casted), Equals, expected)
	}
}

func unsignedDeny(c *C, tp byte, value interface{}, expected string) {
	deny(c, tp, value, true, expected)
}

func signedDeny(c *C, tp byte, value interface{}, expected string) {
	deny(c, tp, value, false, expected)
}

func strvalue(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func (s *testTypeConvertSuite) TestConvert(c *C) {
	defer testleak.AfterTest(c)()
	// integer ranges
	signedDeny(c, mysql.TypeTiny, -129, "-128")
	signedAccept(c, mysql.TypeTiny, -128, "-128")
	signedAccept(c, mysql.TypeTiny, 127, "127")
	signedDeny(c, mysql.TypeTiny, 128, "127")
	unsignedDeny(c, mysql.TypeTiny, -1, "0")
	unsignedAccept(c, mysql.TypeTiny, 0, "0")
	unsignedAccept(c, mysql.TypeTiny, 255, "255")
	unsignedDeny(c, mysql.TypeTiny, 256, "255")

	signedDeny(c, mysql.TypeShort, int64(math.MinInt16)-1, strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MinInt16), strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MaxInt16), strvalue(int64(math.MaxInt16)))
	signedDeny(c, mysql.TypeShort, int64(math.MaxInt16)+1, strvalue(int64(math.MaxInt16)))
	unsignedDeny(c, mysql.TypeShort, -1, "0")
	unsignedAccept(c, mysql.TypeShort, 0, "0")
	unsignedAccept(c, mysql.TypeShort, uint64(math.MaxUint16), strvalue(uint64(math.MaxUint16)))
	unsignedDeny(c, mysql.TypeShort, uint64(math.MaxUint16)+1, strvalue(uint64(math.MaxUint16)))

	signedDeny(c, mysql.TypeInt24, -1<<23-1, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, -1<<23, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, 1<<23-1, strvalue(1<<23-1))
	signedDeny(c, mysql.TypeInt24, 1<<23, strvalue(1<<23-1))
	unsignedDeny(c, mysql.TypeInt24, -1, "0")
	unsignedAccept(c, mysql.TypeInt24, 0, "0")
	unsignedAccept(c, mysql.TypeInt24, 1<<24-1, strvalue(1<<24-1))
	unsignedDeny(c, mysql.TypeInt24, 1<<24, strvalue(1<<24-1))

	signedDeny(c, mysql.TypeLong, int64(math.MinInt32)-1, strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MinInt32), strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MaxInt32), strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, uint64(math.MaxUint64), strvalue(uint64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, int64(math.MaxInt32)+1, strvalue(int64(math.MaxInt32)))
	unsignedDeny(c, mysql.TypeLong, -1, "0")
	unsignedAccept(c, mysql.TypeLong, 0, "0")
	unsignedAccept(c, mysql.TypeLong, uint64(math.MaxUint32), strvalue(uint64(math.MaxUint32)))
	unsignedDeny(c, mysql.TypeLong, uint64(math.MaxUint32)+1, strvalue(uint64(math.MaxUint32)))

	signedDeny(c, mysql.TypeLonglong, math.MinInt64*1.1, strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MinInt64), strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MaxInt64), strvalue(int64(math.MaxInt64)))
	signedDeny(c, mysql.TypeLonglong, math.MaxInt64*1.1, strvalue(int64(math.MaxInt64)))
	unsignedDeny(c, mysql.TypeLonglong, -1, "0")
	unsignedAccept(c, mysql.TypeLonglong, 0, "0")
	unsignedAccept(c, mysql.TypeLonglong, uint64(math.MaxUint64), strvalue(uint64(math.MaxUint64)))
	unsignedDeny(c, mysql.TypeLonglong, math.MaxUint64*1.1, strvalue(uint64(math.MaxUint64)))

	// integer from string
	signedAccept(c, mysql.TypeLong, "	  234  ", "234")
	signedAccept(c, mysql.TypeLong, " 2.35e3  ", "2350")
	signedAccept(c, mysql.TypeLong, " +2.51 ", "3")
	signedAccept(c, mysql.TypeLong, " -3.58", "-4")

	// integer from float
	signedAccept(c, mysql.TypeLong, 234.5456, "235")
	signedAccept(c, mysql.TypeLong, -23.45, "-23")

	// float from string
	signedAccept(c, mysql.TypeFloat, "23.523", "23.523")
	signedAccept(c, mysql.TypeFloat, int64(123), "123")
	signedAccept(c, mysql.TypeFloat, uint64(123), "123")
	signedAccept(c, mysql.TypeFloat, int(123), "123")
	signedAccept(c, mysql.TypeFloat, float32(123), "123")
	signedAccept(c, mysql.TypeFloat, float64(123), "123")
	signedAccept(c, mysql.TypeDouble, " -23.54", "-23.54")

	// year
	signedDeny(c, mysql.TypeYear, 123, "<nil>")
	signedDeny(c, mysql.TypeYear, 3000, "<nil>")
	signedAccept(c, mysql.TypeYear, "2000", "2000")

	// time from string
	signedAccept(c, mysql.TypeDate, "2012-08-23", "2012-08-23")
	signedAccept(c, mysql.TypeDatetime, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDatetime, mysql.ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeDatetime, int64(0), "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeTimestamp, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDuration, "10:11:12", "10:11:12")
	signedAccept(c, mysql.TypeDuration, mysql.ZeroDatetime, "00:00:00")
	signedAccept(c, mysql.TypeDuration, mysql.ZeroDuration, "00:00:00")

	signedDeny(c, mysql.TypeDate, "2012-08-x", "0000-00-00")
	signedDeny(c, mysql.TypeDatetime, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeTimestamp, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeDuration, "2012-08-x", "00:00:00")
	signedDeny(c, mysql.TypeDuration, 0, "<nil>")

	// string from string
	signedAccept(c, mysql.TypeString, "abc", "abc")

	// string from integer
	signedAccept(c, mysql.TypeString, 5678, "5678")
	signedAccept(c, mysql.TypeString, mysql.ZeroDuration, "00:00:00")
	signedAccept(c, mysql.TypeString, mysql.ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeString, []byte("123"), "123")

	//TODO add more tests
	signedAccept(c, mysql.TypeNewDecimal, 123, "123")
	signedAccept(c, mysql.TypeNewDecimal, int64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, float32(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, 123.456, "123.456")
	signedAccept(c, mysql.TypeNewDecimal, "-123.456", "-123.456")
	signedAccept(c, mysql.TypeNewDecimal, mysql.NewDecimalFromInt(123, 5), "12300000")
	signedAccept(c, mysql.TypeNewDecimal, mysql.NewDecimalFromInt(-123, -5), "-0.00123")
}
