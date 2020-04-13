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

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types/json"
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
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
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
	_, err = Convert(&invalidMockType{}, ft)
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

	vd, err := ParseTime(nil, "2010-10-10 10:11:11.12345", mysql.TypeDatetime, 2)
	c.Assert(vd.String(), Equals, "2010-10-10 10:11:11.12")
	c.Assert(err, IsNil)
	v, err = Convert(vd, ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Duration).String(), Equals, "10:11:11.1")

	vt, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, "2010-10-10 10:11:11.12345", mysql.TypeTimestamp, 2)
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
	// issue 4287.
	v, err = Convert(math.Pow(2, 63)-1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(math.MaxInt64))
	ft = NewFieldType(mysql.TypeLonglong)
	ft.Flag |= mysql.UnsignedFlag
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(100))
	// issue 3470
	ft = NewFieldType(mysql.TypeLonglong)
	v, err = Convert(Duration{Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second + 555*time.Millisecond), Fsp: 3}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(130000))
	v, err = Convert(Time{
		Time: FromDate(2017, 1, 1, 12, 59, 59, 555000),
		Type: mysql.TypeDatetime,
		Fsp:  MaxFsp}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(20170101130000))

	// For TypeBit
	ft = NewFieldType(mysql.TypeBit)
	ft.Flen = 24 // 3 bytes.
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, NewBinaryLiteralFromUint(3223600, 3))

	v, err = Convert(NewBinaryLiteralFromUint(100, -1), ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, NewBinaryLiteralFromUint(100, 3))

	ft.Flen = 1
	v, err = Convert(1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, NewBinaryLiteralFromUint(1, 1))

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
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue, Commentf("err %v", err))
	c.Assert(v.(*MyDecimal).String(), Equals, "3.1416")
	v, err = Convert("99999", ft)
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue, Commentf("err %v", err))
	c.Assert(v.(*MyDecimal).String(), Equals, "9999.9999")
	v, err = Convert("-10000", ft)
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue, Commentf("err %v", err))
	c.Assert(v.(*MyDecimal).String(), Equals, "-9999.9999")

	// Test Datum.ToDecimal with bad number.
	d := NewDatum("hello")
	sc := new(stmtctx.StatementContext)
	_, err = d.ToDecimal(sc)
	c.Assert(terror.ErrorEqual(err, ErrBadNumber), IsTrue, Commentf("err %v", err))

	sc.IgnoreTruncate = true
	v, err = d.ToDecimal(sc)
	c.Assert(err, IsNil)
	c.Assert(v.(*MyDecimal).String(), Equals, "0")

	// For TypeYear
	ft = NewFieldType(mysql.TypeYear)
	v, err = Convert("2015", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(2015, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	_, err = Convert(1800, ft)
	c.Assert(err, NotNil)
	dt, err := ParseDate(nil, "2015-11-11")
	c.Assert(err, IsNil)
	v, err = Convert(dt, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(ZeroDuration, ft)
	c.Assert(err, IsNil)
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
	v, err = Convert(4, ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue, Commentf("err %v", err))
	c.Assert(v, DeepEquals, Enum{})

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
	testToString(c, NewBinaryLiteralFromUint(0x4D7953514C, -1), "MySQL")
	testToString(c, NewBinaryLiteralFromUint(0x41, -1), "A")
	testToString(c, Enum{Name: "a", Value: 1}, "a")
	testToString(c, Set{Name: "a", Value: 1}, "a")

	t, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC},
		"2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testToString(c, t, "2011-11-10 11:11:11.999999")

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToString(c, td, "11:11:11.999999")

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Flen = 10
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue, Commentf("err %v", err))
	testToString(c, v, "3.14159")

	_, err = ToString(&invalidMockType{})
	c.Assert(err, NotNil)

	// test truncate
	tests := []struct {
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
	for _, tt := range tests {
		ft = NewFieldType(mysql.TypeVarchar)
		ft.Flen = tt.flen
		ft.Charset = tt.charset
		inputDatum := NewStringDatum(tt.input)
		sc := new(stmtctx.StatementContext)
		outputDatum, err := inputDatum.ConvertTo(sc, ft)
		if tt.input != tt.output {
			c.Assert(ErrDataTooLong.Equal(err), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(outputDatum.GetString(), Equals, tt.output)
	}
}

func testStrToInt(c *C, str string, expect int64, truncateAsErr bool, expectErr error) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToInt(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue, Commentf("err %v", err))
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func testStrToUint(c *C, str string, expect uint64, truncateAsErr bool, expectErr error) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToUint(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue, Commentf("err %v", err))
	} else {
		c.Assert(err, IsNil)
		c.Assert(val, Equals, expect)
	}
}

func testStrToFloat(c *C, str string, expect float64, truncateAsErr bool, expectErr error) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = !truncateAsErr
	val, err := StrToFloat(sc, str)
	if expectErr != nil {
		c.Assert(terror.ErrorEqual(err, expectErr), IsTrue, Commentf("err %v", err))
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

	// for issue #5111
	testStrToFloat(c, "1e649", math.MaxFloat64, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "1e649", math.MaxFloat64, false, nil)
	testStrToFloat(c, "-1e649", -math.MaxFloat64, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "-1e649", -math.MaxFloat64, false, nil)

	// for issue #10806, #11179
	testSelectUpdateDeleteEmptyStringError(c)
}

func testSelectUpdateDeleteEmptyStringError(c *C) {
	testCases := []struct {
		inSelect bool
		inDelete bool
	}{
		{true, false},
		{false, true},
	}
	sc := new(stmtctx.StatementContext)
	for _, tc := range testCases {
		sc.InSelectStmt = tc.inSelect
		sc.InDeleteStmt = tc.inDelete

		str := ""
		expect := 0

		val, err := StrToInt(sc, str)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, int64(expect))

		val1, err := StrToUint(sc, str)
		c.Assert(err, IsNil)
		c.Assert(val1, Equals, uint64(expect))

		val2, err := StrToFloat(sc, str)
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, float64(expect))
	}
}

func (s *testTypeConvertSuite) TestFieldTypeToStr(c *C) {
	defer testleak.AfterTest(c)()
	v := TypeToStr(mysql.TypeUnspecified, "not binary")
	c.Assert(v, Equals, TypeStr(mysql.TypeUnspecified))
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
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
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
	sc := new(stmtctx.StatementContext)
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
	signedAccept(c, mysql.TypeTiny, NewBinaryLiteralFromUint(127, -1), "127")
	signedDeny(c, mysql.TypeTiny, NewBinaryLiteralFromUint(128, -1), "127")
	unsignedDeny(c, mysql.TypeTiny, -1, "255")
	unsignedAccept(c, mysql.TypeTiny, 0, "0")
	unsignedAccept(c, mysql.TypeTiny, 255, "255")
	unsignedDeny(c, mysql.TypeTiny, 256, "255")
	unsignedAccept(c, mysql.TypeTiny, NewBinaryLiteralFromUint(0, -1), "0")
	unsignedAccept(c, mysql.TypeTiny, NewBinaryLiteralFromUint(255, -1), "255")
	unsignedDeny(c, mysql.TypeTiny, NewBinaryLiteralFromUint(256, -1), "255")

	signedDeny(c, mysql.TypeShort, int64(math.MinInt16)-1, strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MinInt16), strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MaxInt16), strvalue(int64(math.MaxInt16)))
	signedDeny(c, mysql.TypeShort, int64(math.MaxInt16)+1, strvalue(int64(math.MaxInt16)))
	signedAccept(c, mysql.TypeShort, NewBinaryLiteralFromUint(math.MaxInt16, -1), strvalue(int64(math.MaxInt16)))
	signedDeny(c, mysql.TypeShort, NewBinaryLiteralFromUint(math.MaxInt16+1, -1), strvalue(int64(math.MaxInt16)))
	unsignedDeny(c, mysql.TypeShort, -1, "65535")
	unsignedAccept(c, mysql.TypeShort, 0, "0")
	unsignedAccept(c, mysql.TypeShort, uint64(math.MaxUint16), strvalue(uint64(math.MaxUint16)))
	unsignedDeny(c, mysql.TypeShort, uint64(math.MaxUint16)+1, strvalue(uint64(math.MaxUint16)))
	unsignedAccept(c, mysql.TypeShort, NewBinaryLiteralFromUint(0, -1), "0")
	unsignedAccept(c, mysql.TypeShort, NewBinaryLiteralFromUint(math.MaxUint16, -1), strvalue(uint64(math.MaxUint16)))
	unsignedDeny(c, mysql.TypeShort, NewBinaryLiteralFromUint(math.MaxUint16+1, -1), strvalue(uint64(math.MaxUint16)))

	signedDeny(c, mysql.TypeInt24, -1<<23-1, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, -1<<23, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, 1<<23-1, strvalue(1<<23-1))
	signedDeny(c, mysql.TypeInt24, 1<<23, strvalue(1<<23-1))
	signedAccept(c, mysql.TypeInt24, NewBinaryLiteralFromUint(1<<23-1, -1), strvalue(1<<23-1))
	signedDeny(c, mysql.TypeInt24, NewBinaryLiteralFromUint(1<<23, -1), strvalue(1<<23-1))
	unsignedDeny(c, mysql.TypeInt24, -1, "16777215")
	unsignedAccept(c, mysql.TypeInt24, 0, "0")
	unsignedAccept(c, mysql.TypeInt24, 1<<24-1, strvalue(1<<24-1))
	unsignedDeny(c, mysql.TypeInt24, 1<<24, strvalue(1<<24-1))
	unsignedAccept(c, mysql.TypeInt24, NewBinaryLiteralFromUint(0, -1), "0")
	unsignedAccept(c, mysql.TypeInt24, NewBinaryLiteralFromUint(1<<24-1, -1), strvalue(1<<24-1))
	unsignedDeny(c, mysql.TypeInt24, NewBinaryLiteralFromUint(1<<24, -1), strvalue(1<<24-1))

	signedDeny(c, mysql.TypeLong, int64(math.MinInt32)-1, strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MinInt32), strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MaxInt32), strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, uint64(math.MaxUint64), strvalue(uint64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, int64(math.MaxInt32)+1, strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, "1343545435346432587475", strvalue(int64(math.MaxInt32)))
	signedAccept(c, mysql.TypeLong, NewBinaryLiteralFromUint(math.MaxInt32, -1), strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, NewBinaryLiteralFromUint(math.MaxUint64, -1), strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, NewBinaryLiteralFromUint(math.MaxInt32+1, -1), strvalue(int64(math.MaxInt32)))
	unsignedDeny(c, mysql.TypeLong, -1, "4294967295")
	unsignedAccept(c, mysql.TypeLong, 0, "0")
	unsignedAccept(c, mysql.TypeLong, uint64(math.MaxUint32), strvalue(uint64(math.MaxUint32)))
	unsignedDeny(c, mysql.TypeLong, uint64(math.MaxUint32)+1, strvalue(uint64(math.MaxUint32)))
	unsignedAccept(c, mysql.TypeLong, NewBinaryLiteralFromUint(0, -1), "0")
	unsignedAccept(c, mysql.TypeLong, NewBinaryLiteralFromUint(math.MaxUint32, -1), strvalue(uint64(math.MaxUint32)))
	unsignedDeny(c, mysql.TypeLong, NewBinaryLiteralFromUint(math.MaxUint32+1, -1), strvalue(uint64(math.MaxUint32)))

	signedDeny(c, mysql.TypeLonglong, math.MinInt64*1.1, strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MinInt64), strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MaxInt64), strvalue(int64(math.MaxInt64)))
	signedDeny(c, mysql.TypeLonglong, math.MaxInt64*1.1, strvalue(int64(math.MaxInt64)))
	signedAccept(c, mysql.TypeLonglong, NewBinaryLiteralFromUint(math.MaxInt64, -1), strvalue(int64(math.MaxInt64)))
	signedDeny(c, mysql.TypeLonglong, NewBinaryLiteralFromUint(math.MaxInt64+1, -1), strvalue(int64(math.MaxInt64)))
	unsignedAccept(c, mysql.TypeLonglong, -1, "18446744073709551615")
	unsignedAccept(c, mysql.TypeLonglong, 0, "0")
	unsignedAccept(c, mysql.TypeLonglong, uint64(math.MaxUint64), strvalue(uint64(math.MaxUint64)))

	unsignedAccept(c, mysql.TypeLonglong, NewBinaryLiteralFromUint(0, -1), "0")
	unsignedAccept(c, mysql.TypeLonglong, NewBinaryLiteralFromUint(math.MaxUint64, -1), strvalue(uint64(math.MaxUint64)))

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
	signedAccept(c, mysql.TypeLong, " +2.51 ", "3")
	signedAccept(c, mysql.TypeLong, " -9999.5 ", "-10000")
	signedAccept(c, mysql.TypeLong, " 999.4", "999")
	signedAccept(c, mysql.TypeLong, " -3.58", "-4")
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
	signedDeny(c, mysql.TypeYear, 123, "0")
	signedDeny(c, mysql.TypeYear, 3000, "0")
	signedAccept(c, mysql.TypeYear, "2000", "2000")
	signedAccept(c, mysql.TypeYear, "abc", "0")
	signedAccept(c, mysql.TypeYear, "00abc", "2000")
	signedAccept(c, mysql.TypeYear, "0019", "2019")
	signedAccept(c, mysql.TypeYear, 2155, "2155")
	signedAccept(c, mysql.TypeYear, 2155.123, "2155")
	signedDeny(c, mysql.TypeYear, 2156, "0")
	signedDeny(c, mysql.TypeYear, 123.123, "0")
	signedDeny(c, mysql.TypeYear, 1900, "0")
	signedAccept(c, mysql.TypeYear, 1901, "1901")
	signedAccept(c, mysql.TypeYear, 1900.567, "1901")
	signedDeny(c, mysql.TypeYear, 1900.456, "0")
	signedAccept(c, mysql.TypeYear, 1, "2001")
	signedAccept(c, mysql.TypeYear, 69, "2069")
	signedAccept(c, mysql.TypeYear, 70, "1970")
	signedAccept(c, mysql.TypeYear, 99, "1999")
	signedDeny(c, mysql.TypeYear, 100, "0")
	signedDeny(c, mysql.TypeYear, "99999999999999999999999999999999999", "0")

	// time from string
	signedAccept(c, mysql.TypeDate, "2012-08-23", "2012-08-23")
	signedAccept(c, mysql.TypeDatetime, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDatetime, ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeDatetime, int64(0), "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeDatetime, NewDecFromFloatForTest(20010101100000.123456), "2001-01-01 10:00:00")
	signedAccept(c, mysql.TypeTimestamp, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeTimestamp, NewDecFromFloatForTest(20010101100000.123456), "2001-01-01 10:00:00")
	signedAccept(c, mysql.TypeDuration, "10:11:12", "10:11:12")
	signedAccept(c, mysql.TypeDuration, ZeroDatetime, "00:00:00")
	signedAccept(c, mysql.TypeDuration, ZeroDuration, "00:00:00")
	signedAccept(c, mysql.TypeDuration, 0, "00:00:00")

	signedDeny(c, mysql.TypeDate, "2012-08-x", "0000-00-00")
	signedDeny(c, mysql.TypeDatetime, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeTimestamp, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeDuration, "2012-08-x", "00:00:00")

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
	dec.Round(dec, 5, ModeHalfEven)
	signedAccept(c, mysql.TypeNewDecimal, dec, "-0.00123")
}

func (s *testTypeConvertSuite) TestGetValidInt(c *C) {
	tests := []struct {
		origin  string
		valid   string
		signed  bool
		warning bool
	}{
		{"100", "100", true, false},
		{"-100", "-100", true, false},
		{"9223372036854775808", "9223372036854775808", false, false},
		{"1abc", "1", true, true},
		{"-1-1", "-1", true, true},
		{"+1+1", "+1", true, true},
		{"123..34", "123", true, true},
		{"123.23E-10", "123", true, true},
		{"1.1e1.3", "1", true, true},
		{"11e1.3", "11", true, true},
		{"1.", "1", true, true},
		{".1", "0", true, true},
		{"", "0", true, true},
		{"123e+", "123", true, true},
		{"123de", "123", true, true},
	}
	sc := new(stmtctx.StatementContext)
	sc.TruncateAsWarning = true
	sc.CastStrToIntStrict = true
	warningCount := 0
	for _, tt := range tests {
		prefix, err := getValidIntPrefix(sc, tt.origin)
		c.Assert(err, IsNil)
		c.Assert(prefix, Equals, tt.valid)
		if tt.signed {
			_, err = strconv.ParseInt(prefix, 10, 64)
		} else {
			_, err = strconv.ParseUint(prefix, 10, 64)
		}
		c.Assert(err, IsNil)
		warnings := sc.GetWarnings()
		if tt.warning {
			c.Assert(warnings, HasLen, warningCount+1)
			c.Assert(terror.ErrorEqual(warnings[len(warnings)-1].Err, ErrTruncatedWrongVal), IsTrue)
			warningCount += 1
		} else {
			c.Assert(warnings, HasLen, warningCount)
		}
	}

	tests2 := []struct {
		origin  string
		valid   string
		warning bool
	}{
		{"100", "100", false},
		{"-100", "-100", false},
		{"1abc", "1", true},
		{"-1-1", "-1", true},
		{"+1+1", "+1", true},
		{"123..34", "123.", true},
		{"123.23E-10", "0", false},
		{"1.1e1.3", "1.1e1", true},
		{"11e1.3", "11e1", true},
		{"1.", "1", false},
		{".1", "0", false},
		{"", "0", true},
		{"123e+", "123", true},
		{"123de", "123", true},
	}
	sc.TruncateAsWarning = false
	sc.CastStrToIntStrict = false
	for _, tt := range tests2 {
		prefix, err := getValidIntPrefix(sc, tt.origin)
		if tt.warning {
			c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(prefix, Equals, tt.valid)
	}
}

func (s *testTypeConvertSuite) TestRoundIntStr(c *C) {
	cases := []struct {
		a string
		b byte
		c string
	}{
		{"+999", '5', "+1000"},
		{"999", '5', "1000"},
		{"-999", '5', "-1000"},
	}
	for _, cc := range cases {
		c.Assert(roundIntStr(cc.b, cc.a), Equals, cc.c)
	}
}

func (s *testTypeConvertSuite) TestGetValidFloat(c *C) {
	tests := []struct {
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
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		prefix, _ := getValidFloatPrefix(sc, tt.origin)
		c.Assert(prefix, Equals, tt.valid)
		_, err := strconv.ParseFloat(prefix, 64)
		c.Assert(err, IsNil)
	}

	tests2 := []struct {
		origin   string
		expected string
	}{
		{"1e9223372036854775807", "1"},
		{"125e342", "125"},
		{"1e21", "1"},
		{"1e5", "100000"},
		{"-123.45678e5", "-12345678"},
		{"+0.5", "1"},
		{"-0.5", "-1"},
		{".5e0", "1"},
		{"+.5e0", "+1"},
		{"-.5e0", "-1"},
		{".5", "1"},
		{"123.456789e5", "12345679"},
		{"123.456784e5", "12345678"},
		{"+999.9999e2", "+100000"},
	}
	for _, t := range tests2 {
		str, err := floatStrToIntStr(sc, t.origin, t.origin)
		c.Assert(err, IsNil)
		c.Assert(str, Equals, t.expected, Commentf("%v, %v", t.origin, t.expected))
	}
}

// TestConvertTime tests time related conversion.
// time conversion is complicated including Date/Datetime/Time/Timestamp etc,
// Timestamp may involving timezone.
func (s *testTypeConvertSuite) TestConvertTime(c *C) {
	timezones := []*time.Location{
		time.UTC,
		time.FixedZone("", 3*3600),
		time.Local,
	}

	for _, timezone := range timezones {
		sc := &stmtctx.StatementContext{
			TimeZone: timezone,
		}
		testConvertTimeTimeZone(c, sc)
	}
}

func testConvertTimeTimeZone(c *C, sc *stmtctx.StatementContext) {
	raw := FromDate(2002, 3, 4, 4, 6, 7, 8)
	tests := []struct {
		input  Time
		target *FieldType
		expect Time
	}{
		{
			input:  Time{Type: mysql.TypeDatetime, Time: raw},
			target: NewFieldType(mysql.TypeTimestamp),
			expect: Time{Type: mysql.TypeTimestamp, Time: raw},
		},
		{
			input:  Time{Type: mysql.TypeDatetime, Time: raw},
			target: NewFieldType(mysql.TypeTimestamp),
			expect: Time{Type: mysql.TypeTimestamp, Time: raw},
		},
		{
			input:  Time{Type: mysql.TypeDatetime, Time: raw},
			target: NewFieldType(mysql.TypeTimestamp),
			expect: Time{Type: mysql.TypeTimestamp, Time: raw},
		},
		{
			input:  Time{Type: mysql.TypeTimestamp, Time: raw},
			target: NewFieldType(mysql.TypeDatetime),
			expect: Time{Type: mysql.TypeDatetime, Time: raw},
		},
	}

	for _, test := range tests {
		var d Datum
		d.SetMysqlTime(test.input)
		nd, err := d.ConvertTo(sc, test.target)
		c.Assert(err, IsNil)
		t := nd.GetMysqlTime()
		c.Assert(t.Type, Equals, test.expect.Type)
		c.Assert(t.Time, Equals, test.expect.Time)
	}
}

func (s *testTypeConvertSuite) TestConvertJSONToInt(c *C) {
	var tests = []struct {
		In  string
		Out int64
	}{
		{`{}`, 0},
		{`[]`, 0},
		{`3`, 3},
		{`-3`, -3},
		{`4.5`, 5},
		{`true`, 1},
		{`false`, 0},
		{`null`, 0},
		{`"hello"`, 0},
		{`"123hello"`, 123},
		{`"1234"`, 1234},
	}
	for _, tt := range tests {
		j, err := json.ParseBinaryFromString(tt.In)
		c.Assert(err, IsNil)

		casted, _ := ConvertJSONToInt(new(stmtctx.StatementContext), j, false)
		c.Assert(casted, Equals, tt.Out)
	}
}

func (s *testTypeConvertSuite) TestConvertJSONToFloat(c *C) {
	var tests = []struct {
		In  interface{}
		Out float64
		ty  json.TypeCode
	}{
		{make(map[string]interface{}, 0), 0, json.TypeCodeObject},
		{make([]interface{}, 0), 0, json.TypeCodeArray},
		{int64(3), 3, json.TypeCodeInt64},
		{int64(-3), -3, json.TypeCodeInt64},
		{uint64(1 << 63), 1 << 63, json.TypeCodeUint64},
		{float64(4.5), 4.5, json.TypeCodeFloat64},
		{true, 1, json.TypeCodeLiteral},
		{false, 0, json.TypeCodeLiteral},
		{nil, 0, json.TypeCodeLiteral},
		{"hello", 0, json.TypeCodeString},
		{"123.456hello", 123.456, json.TypeCodeString},
		{"1234", 1234, json.TypeCodeString},
	}
	for _, tt := range tests {
		j := json.CreateBinary(tt.In)
		c.Assert(j.TypeCode, Equals, tt.ty)
		casted, _ := ConvertJSONToFloat(new(stmtctx.StatementContext), j)
		c.Assert(casted, Equals, tt.Out)
	}
}

func (s *testTypeConvertSuite) TestConvertJSONToDecimal(c *C) {
	var tests = []struct {
		In  string
		Out *MyDecimal
	}{
		{`{}`, NewDecFromStringForTest("0")},
		{`[]`, NewDecFromStringForTest("0")},
		{`3`, NewDecFromStringForTest("3")},
		{`-3`, NewDecFromStringForTest("-3")},
		{`4.5`, NewDecFromStringForTest("4.5")},
		{`"1234"`, NewDecFromStringForTest("1234")},
		{`"1234567890123456789012345678901234567890123456789012345"`, NewDecFromStringForTest("1234567890123456789012345678901234567890123456789012345")},
	}
	for _, tt := range tests {
		j, err := json.ParseBinaryFromString(tt.In)
		c.Assert(err, IsNil)
		casted, _ := ConvertJSONToDecimal(new(stmtctx.StatementContext), j)
		c.Assert(casted.Compare(tt.Out), Equals, 0)
	}
}

func (s *testTypeConvertSuite) TestNumberToDuration(c *C) {
	var testCases = []struct {
		number int64
		fsp    int
		hasErr bool
		year   int
		month  int
		day    int
		hour   int
		minute int
		second int
	}{
		{20171222, 0, true, 0, 0, 0, 0, 0, 0},
		{171222, 0, false, 0, 0, 0, 17, 12, 22},
		{20171222020005, 0, false, 2017, 12, 22, 02, 00, 05},
		{10000000000, 0, true, 0, 0, 0, 0, 0, 0},
		{171222, 1, false, 0, 0, 0, 17, 12, 22},
		{176022, 1, true, 0, 0, 0, 0, 0, 0},
		{8391222, 1, true, 0, 0, 0, 0, 0, 0},
		{8381222, 0, false, 0, 0, 0, 838, 12, 22},
		{1001222, 0, false, 0, 0, 0, 100, 12, 22},
		{171260, 1, true, 0, 0, 0, 0, 0, 0},
	}

	for _, tc := range testCases {
		dur, err := NumberToDuration(tc.number, tc.fsp)
		if tc.hasErr {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(dur.Hour(), Equals, tc.hour)
		c.Assert(dur.Minute(), Equals, tc.minute)
		c.Assert(dur.Second(), Equals, tc.second)
	}

	var testCases1 = []struct {
		number int64
		dur    time.Duration
	}{
		{171222, 17*time.Hour + 12*time.Minute + 22*time.Second},
		{-171222, -(17*time.Hour + 12*time.Minute + 22*time.Second)},
	}

	for _, tc := range testCases1 {
		dur, err := NumberToDuration(tc.number, 0)
		c.Assert(err, IsNil)
		c.Assert(dur.Duration, Equals, tc.dur)
	}
}

func (s *testTypeConvertSuite) TestStrToDuration(c *C) {
	sc := new(stmtctx.StatementContext)
	var tests = []struct {
		str        string
		fsp        int
		isDuration bool
	}{
		{"20190412120000", 4, false},
		{"20190101180000", 6, false},
		{"20190101180000", 1, false},
		{"20190101181234", 3, false},
	}
	for _, tt := range tests {
		_, _, isDuration, err := StrToDuration(sc, tt.str, tt.fsp)
		c.Assert(err, IsNil)
		c.Assert(isDuration, Equals, tt.isDuration)
	}
}

func (s *testTypeConvertSuite) TestConvertScientificNotation(c *C) {
	cases := []struct {
		input  string
		output string
		succ   bool
	}{
		{"123.456e0", "123.456", true},
		{"123.456e1", "1234.56", true},
		{"123.456e3", "123456", true},
		{"123.456e4", "1234560", true},
		{"123.456e5", "12345600", true},
		{"123.456e6", "123456000", true},
		{"123.456e7", "1234560000", true},
		{"123.456e-1", "12.3456", true},
		{"123.456e-2", "1.23456", true},
		{"123.456e-3", "0.123456", true},
		{"123.456e-4", "0.0123456", true},
		{"123.456e-5", "0.00123456", true},
		{"123.456e-6", "0.000123456", true},
		{"123.456e-7", "0.0000123456", true},
		{"123.456e-", "", false},
		{"123.456e-7.5", "", false},
		{"123.456e", "", false},
	}
	for _, ca := range cases {
		result, err := convertScientificNotation(ca.input)
		if !ca.succ {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(ca.output, Equals, result)
		}
	}
}

func (s *testTypeConvertSuite) TestConvertDecimalStrToUint(c *C) {
	cases := []struct {
		input  string
		result uint64
		succ   bool
	}{
		{"0.", 0, true},
		{"72.40", 72, true},
		{"072.40", 72, true},
		{"123.456e2", 12346, true},
		{"123.456e-2", 1, true},
		{"072.50000000001", 73, true},
		{".5757", 1, true},
		{".12345E+4", 1235, true},
		{"9223372036854775807.5", 9223372036854775808, true},
		{"9223372036854775807.4999", 9223372036854775807, true},
		{"18446744073709551614.55", 18446744073709551615, true},
		{"18446744073709551615.344", 18446744073709551615, true},
		{"18446744073709551615.544", 0, false},
	}
	for _, ca := range cases {
		result, err := convertDecimalStrToUint(&stmtctx.StatementContext{}, ca.input, math.MaxUint64, 0)
		if !ca.succ {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(result, Equals, ca.result)
		}
	}
}
