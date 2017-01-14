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
	"strconv"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTypeConvertSuite{})

type testTypeConvertSuite struct {
}

type invalidMockType struct {
}

// Convert converts the val with type tp.
func Convert(val interface{}, target *FieldType) (v interface{}, err error) {
	d := NewDatum(val)
	sc := new(variable.StatementContext)
	ret, err := d.ConvertTo(sc, target)
	if err != nil {
		return ret.GetValue(), errors.Trace(err)
	}
	return ret.GetValue(), nil
}

func (s *testTypeConvertSuite) TestConvertType(c *C) {
	defer testleak.AfterTest(c)()
	ft := NewFieldType(mysql.TypeBlob)
	ft.Flen = 4
	ft.Charset = "utf8"
	v, err := Convert("123456", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, Equals, "1234")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 4
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
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
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(-999.999, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(-999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(1111.11, ft)
	c.Assert(err, NotNil)
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
	v, err = Convert(999.914, ft)
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
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, Equals, "123")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, DeepEquals, []byte("123"))

	// For TypeDuration
	ft = NewFieldType(mysql.TypeDuration)
	ft.Decimal = 3
	v, err = Convert("10:11:12.123456", ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Duration).String(), Equals, "10:11:12.123")
	ft.Decimal = 1
	vv, err := Convert(v, ft)
	c.Assert(err, IsNil)
	c.Assert(vv.(Duration).String(), Equals, "10:11:12.1")

	vt, err := ParseTime("2010-10-10 10:11:11.12345", mysql.TypeTimestamp, 2)
	c.Assert(vt.String(), Equals, "2010-10-10 10:11:11.12")
	c.Assert(err, IsNil)
	v, err = Convert(vt, ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Duration).String(), Equals, "10:11:11.1")

	// For mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Decimal = 3
	v, err = Convert("2010-10-10 10:11:11.12345", ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Time).String(), Equals, "2010-10-10 10:11:11.123")
	ft.Decimal = 1
	vv, err = Convert(v, ft)
	c.Assert(err, IsNil)
	c.Assert(vv.(Time).String(), Equals, "2010-10-10 10:11:11.1")

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
	c.Assert(v, Equals, Bit{Value: 100, Width: 8})

	v, err = Convert(Hex{Value: 100}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, Bit{Value: 100, Width: 8})

	v, err = Convert(Bit{Value: 100, Width: 8}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, Bit{Value: 100, Width: 8})

	ft.Flen = 1
	v, err = Convert(1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, Bit{Value: 1, Width: 1})

	_, err = Convert(2, ft)
	c.Assert(err, NotNil)

	ft.Flen = 0
	_, err = Convert(2, ft)
	c.Assert(err, NotNil)

	// For TypeNewDecimal
	ft = NewFieldType(mysql.TypeNewDecimal)
	ft.Flen = 8
	ft.Decimal = 4
	v, err = Convert(3.1416, ft)
	c.Assert(err, IsNil, Commentf(errors.ErrorStack(err)))
	c.Assert(v.(*MyDecimal).String(), Equals, "3.1416")
	v, err = Convert("3.1415926", ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue)
	c.Assert(v.(*MyDecimal).String(), Equals, "3.1416")
	v, err = Convert("99999", ft)
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue)
	c.Assert(v.(*MyDecimal).String(), Equals, "9999.9999")
	v, err = Convert("-10000", ft)
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue)
	c.Assert(v.(*MyDecimal).String(), Equals, "-9999.9999")

	// For TypeYear
	ft = NewFieldType(mysql.TypeYear)
	v, err = Convert("2015", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(2015, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(1800, ft)
	c.Assert(err, NotNil)
	dt, err := ParseDate("2015-11-11")
	c.Assert(err, IsNil)
	v, err = Convert(dt, ft)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(ZeroDuration, ft)
	c.Assert(v, Equals, int64(time.Now().Year()))

	// For enum
	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b", "c"}
	v, err = Convert("a", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Enum{Name: "a", Value: 1})
	v, err = Convert(2, ft)
	c.Log(errors.ErrorStack(err))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Enum{Name: "b", Value: 2})
	_, err = Convert("d", ft)
	c.Assert(err, NotNil)
	_, err = Convert(4, ft)
	c.Assert(err, NotNil)

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b", "c"}
	v, err = Convert("a", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Set{Name: "a", Value: 1})
	v, err = Convert(2, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Set{Name: "b", Value: 2})
	v, err = Convert(3, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Set{Name: "a,b", Value: 3})
	_, err = Convert("d", ft)
	c.Assert(err, NotNil)
	_, err = Convert(9, ft)
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
	testToString(c, Hex{Value: 0x4D7953514C}, "MySQL")
	testToString(c, Bit{Value: 0x41, Width: 8}, "A")
	testToString(c, Enum{Name: "a", Value: 1}, "a")
	testToString(c, Set{Name: "a", Value: 1}, "a")

	t, err := ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testToString(c, t, "2011-11-10 11:11:11.999999")

	td, err := ParseDuration("11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToString(c, td, "11:11:11.999999")

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Flen = 10
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue)
	testToString(c, v, "3.14159")

	_, err = ToString(&invalidMockType{})
	c.Assert(err, NotNil)

	// test truncate
	cases := []struct {
		flen    int
		charset string
		input   string
		output  string
	}{
		{5, "utf8", "你好，世界", "你好，世界"},
		{5, "utf8mb4", "你好，世界", "你好，世界"},
		{4, "utf8", "你好，世界", "你好，世"},
		{4, "utf8mb4", "你好，世界", "你好，世"},
		{15, "binary", "你好，世界", "你好，世界"},
		{12, "binary", "你好，世界", "你好，世"},
		{0, "binary", "你好，世界", ""},
	}
	for _, ca := range cases {
		ft = NewFieldType(mysql.TypeVarchar)
		ft.Flen = ca.flen
		ft.Charset = ca.charset
		inputDatum := NewStringDatum(ca.input)
		sc := new(variable.StatementContext)
		outputDatum, err := inputDatum.ConvertTo(sc, ft)
		if ca.input != ca.output {
			c.Assert(ErrDataTooLong.Equal(err), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(outputDatum.GetString(), Equals, ca.output)
	}
}

func testStrToInt(c *C, str string, expect int64, truncateAsErr bool, expectErr error) {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToInt(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue)
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func testStrToUint(c *C, str string, expect uint64, truncateAsErr bool, expectErr error) {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToUint(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue)
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func testStrToFloat(c *C, str string, expect float64, truncateAsErr bool, expectErr error) {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToFloat(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue)
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func (s *testTypeConvertSuite) TestStrToNum(c *C) {
	defer testleak.AfterTest(c)()
	testStrToInt(c, "0", 0, true, nil)
	testStrToInt(c, "-1", -1, true, nil)
	testStrToInt(c, "100", 100, true, nil)
	testStrToInt(c, "65.0", 65, false, nil)
	testStrToInt(c, "65.0", 65, true, nil)
	testStrToInt(c, "", 0, false, nil)
	testStrToInt(c, "", 0, true, ErrTruncated)
	testStrToInt(c, "xx", 0, true, ErrTruncated)
	testStrToInt(c, "xx", 0, false, nil)
	testStrToInt(c, "11xx", 11, true, ErrTruncated)
	testStrToInt(c, "11xx", 11, false, nil)
	testStrToInt(c, "xx11", 0, false, nil)

	testStrToUint(c, "0", 0, true, nil)
	testStrToUint(c, "", 0, false, nil)
	testStrToUint(c, "", 0, false, nil)
	testStrToUint(c, "-1", 0xffffffffffffffff, false, ErrOverflow)
	testStrToUint(c, "100", 100, true, nil)
	testStrToUint(c, "+100", 100, true, nil)
	testStrToUint(c, "65.0", 65, true, nil)
	testStrToUint(c, "xx", 0, true, ErrTruncated)
	testStrToUint(c, "11xx", 11, true, ErrTruncated)
	testStrToUint(c, "xx11", 0, true, ErrTruncated)

	// TODO: makes StrToFloat return truncated value instead of zero to make it pass.
	testStrToFloat(c, "", 0, true, ErrTruncated)
	testStrToFloat(c, "-1", -1.0, true, nil)
	testStrToFloat(c, "1.11", 1.11, true, nil)
	testStrToFloat(c, "1.11.00", 1.11, false, nil)
	testStrToFloat(c, "1.11.00", 1.11, true, ErrTruncated)
	testStrToFloat(c, "xx", 0.0, false, nil)
	testStrToFloat(c, "0x00", 0.0, false, nil)
	testStrToFloat(c, "11.xx", 11.0, false, nil)
	testStrToFloat(c, "11.xx", 11.0, true, ErrTruncated)
	testStrToFloat(c, "xx.11", 0.0, false, nil)
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
	d := NewDatum(value)
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	casted, err := d.ConvertTo(sc, ft)
	c.Assert(err, IsNil, Commentf("%v", ft))
	if casted.IsNull() {
		c.Assert(expected, Equals, "<nil>")
	} else {
		str, err := casted.ToString()
		c.Assert(err, IsNil)
		c.Assert(str, Equals, expected)
	}
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
	d := NewDatum(value)
	sc := new(variable.StatementContext)
	casted, err := d.ConvertTo(sc, ft)
	c.Assert(err, NotNil)
	if casted.IsNull() {
		c.Assert(expected, Equals, "<nil>")
	} else {
		str, err := casted.ToString()
		c.Assert(err, IsNil)
		c.Assert(str, Equals, expected)
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
	signedAccept(c, mysql.TypeLong, " 2.e3  ", "2000")
	signedAccept(c, mysql.TypeLong, " -2.e3  ", "-2000")
	signedAccept(c, mysql.TypeLong, " 2e2  ", "200")
	signedAccept(c, mysql.TypeLong, " 0.002e3  ", "2")
	signedAccept(c, mysql.TypeLong, " .002e3  ", "2")
	signedAccept(c, mysql.TypeLong, " 20e-2  ", "0")
	signedAccept(c, mysql.TypeLong, " -20e-2  ", "0")
	signedAccept(c, mysql.TypeLong, " +2.51 ", "2")
	signedAccept(c, mysql.TypeLong, " -3.58", "-3")
	signedDeny(c, mysql.TypeLong, " 1a ", "1")
	signedDeny(c, mysql.TypeLong, " +1+ ", "1")

	// integer from float
	signedAccept(c, mysql.TypeLong, 234.5456, "235")
	signedAccept(c, mysql.TypeLong, -23.45, "-23")
	unsignedAccept(c, mysql.TypeLonglong, 234.5456, "235")
	unsignedDeny(c, mysql.TypeLonglong, -23.45, "18446744073709551593")

	// float from string
	signedAccept(c, mysql.TypeFloat, "23.523", "23.523")
	signedAccept(c, mysql.TypeFloat, int64(123), "123")
	signedAccept(c, mysql.TypeFloat, uint64(123), "123")
	signedAccept(c, mysql.TypeFloat, int(123), "123")
	signedAccept(c, mysql.TypeFloat, float32(123), "123")
	signedAccept(c, mysql.TypeFloat, float64(123), "123")
	signedAccept(c, mysql.TypeDouble, " -23.54", "-23.54")
	signedDeny(c, mysql.TypeDouble, "-23.54a", "-23.54")
	signedDeny(c, mysql.TypeDouble, "-23.54e2e", "-2354")
	signedDeny(c, mysql.TypeDouble, "+.e", "0")
	signedAccept(c, mysql.TypeDouble, "1e+1", "10")

	// year
	signedDeny(c, mysql.TypeYear, 123, "<nil>")
	signedDeny(c, mysql.TypeYear, 3000, "<nil>")
	signedAccept(c, mysql.TypeYear, "2000", "2000")

	// time from string
	signedAccept(c, mysql.TypeDate, "2012-08-23", "2012-08-23")
	signedAccept(c, mysql.TypeDatetime, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDatetime, ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeDatetime, int64(0), "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeTimestamp, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDuration, "10:11:12", "10:11:12")
	signedAccept(c, mysql.TypeDuration, ZeroDatetime, "00:00:00")
	signedAccept(c, mysql.TypeDuration, ZeroDuration, "00:00:00")

	signedDeny(c, mysql.TypeDate, "2012-08-x", "0000-00-00")
	signedDeny(c, mysql.TypeDatetime, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeTimestamp, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeDuration, "2012-08-x", "00:00:00")
	signedDeny(c, mysql.TypeDuration, 0, "<nil>")

	// string from string
	signedAccept(c, mysql.TypeString, "abc", "abc")

	// string from integer
	signedAccept(c, mysql.TypeString, 5678, "5678")
	signedAccept(c, mysql.TypeString, ZeroDuration, "00:00:00")
	signedAccept(c, mysql.TypeString, ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeString, []byte("123"), "123")

	//TODO add more tests
	signedAccept(c, mysql.TypeNewDecimal, 123, "123")
	signedAccept(c, mysql.TypeNewDecimal, int64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, float32(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, 123.456, "123.456")
	signedAccept(c, mysql.TypeNewDecimal, "-123.456", "-123.456")
	signedAccept(c, mysql.TypeNewDecimal, NewDecFromInt(12300000), "12300000")
	dec := NewDecFromInt(-123)
	dec.Shift(-5)
	dec.Round(dec, 5)
	signedAccept(c, mysql.TypeNewDecimal, dec, "-0.00123")
}

func (s *testTypeConvertSuite) TestGetValidFloat(c *C) {
	cases := []struct {
		origin string
		valid  string
	}{
		{"-100", "-100"},
		{"1abc", "1"},
		{"-1-1", "-1"},
		{"+1+1", "+1"},
		{"123..34", "123."},
		{"123.23E-10", "123.23E-10"},
		{"1.1e1.3", "1.1e1"},
		{"11e1.3", "11e1"},
		{"1.1e-13a", "1.1e-13"},
		{"1.", "1."},
		{".1", ".1"},
		{"", "0"},
		{"123e+", "123"},
		{"123.e", "123."},
	}
	sc := new(variable.StatementContext)
	for _, ca := range cases {
		prefix, _ := getValidFloatPrefix(sc, ca.origin)
		c.Assert(prefix, Equals, ca.valid)
		_, err := strconv.ParseFloat(prefix, 64)
		c.Assert(err, IsNil)
	}
	_, err := floatStrToIntStr("1e9223372036854775807")
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue)
	_, err = floatStrToIntStr("1e21")
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue)
}
