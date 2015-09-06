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

package column

import (
	"fmt"
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testColumnSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testColumnSuite struct{}

func accept(c *C, tp byte, value interface{}, unsigned bool, expected string) {
	col := &Col{
		model.ColumnInfo{
			FieldType: *types.NewFieldType(tp),
		},
	}
	if unsigned {
		col.Flag |= mysql.UnsignedFlag
	}
	casted, err := col.CastValue(nil, value)
	c.Assert(err, IsNil)
	c.Assert(fmt.Sprintf("%v", casted), Equals, expected)
}

func unsignedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, true, expected)
}

func signedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, false, expected)
}

func deny(c *C, tp byte, value interface{}, unsigned bool, expected string) {
	column := &Col{}
	column.Tp = tp
	if unsigned {
		column.Flag |= mysql.UnsignedFlag
	}
	casted, err := column.CastValue(nil, value)
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

func (s *testColumnSuite) TestCastValue(c *C) {
	// integer ranges
	signedDeny(c, mysql.TypeTiny, -129, "-128")
	signedAccept(c, mysql.TypeTiny, int8(-128), "-128")
	signedAccept(c, mysql.TypeTiny, uint8(127), "127")
	signedDeny(c, mysql.TypeTiny, uint16(128), "127")
	unsignedDeny(c, mysql.TypeTiny, int16(-1), "0")
	unsignedAccept(c, mysql.TypeTiny, int32(0), "0")
	unsignedAccept(c, mysql.TypeTiny, uint32(255), "255")
	unsignedDeny(c, mysql.TypeTiny, 256, "255")

	signedDeny(c, mysql.TypeShort, math.MinInt16-1, strvalue(math.MinInt16))
	signedAccept(c, mysql.TypeShort, math.MinInt16, strvalue(math.MinInt16))
	signedAccept(c, mysql.TypeShort, math.MaxInt16, strvalue(math.MaxInt16))
	signedDeny(c, mysql.TypeShort, math.MaxInt16+1, strvalue(math.MaxInt16))
	unsignedDeny(c, mysql.TypeShort, int64(-1), "0")
	unsignedAccept(c, mysql.TypeShort, uint64(0), "0")
	unsignedAccept(c, mysql.TypeShort, math.MaxUint16, strvalue(math.MaxUint16))
	unsignedDeny(c, mysql.TypeShort, math.MaxUint16+1, strvalue(math.MaxUint16))

	signedDeny(c, mysql.TypeInt24, -1<<23-1, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, -1<<23, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, 1<<23-1, strvalue(1<<23-1))
	signedDeny(c, mysql.TypeInt24, 1<<23, strvalue(1<<23-1))
	unsignedDeny(c, mysql.TypeInt24, -1, "0")
	unsignedAccept(c, mysql.TypeInt24, 0, "0")
	unsignedAccept(c, mysql.TypeInt24, 1<<24-1, strvalue(1<<24-1))
	unsignedDeny(c, mysql.TypeInt24, 1<<24, strvalue(1<<24-1))

	signedDeny(c, mysql.TypeLong, math.MinInt32-1, strvalue(math.MinInt32))
	signedAccept(c, mysql.TypeLong, math.MinInt32, strvalue(math.MinInt32))
	signedAccept(c, mysql.TypeLong, math.MaxInt32, strvalue(math.MaxInt32))
	signedDeny(c, mysql.TypeLong, uint(math.MaxUint64), strvalue(uint(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, math.MaxInt32+1, strvalue(math.MaxInt32))
	unsignedDeny(c, mysql.TypeLong, -1, "0")
	unsignedAccept(c, mysql.TypeLong, 0, "0")
	unsignedAccept(c, mysql.TypeLong, math.MaxUint32, strvalue(math.MaxUint32))
	unsignedDeny(c, mysql.TypeLong, math.MaxUint32+1, strvalue(math.MaxUint32))

	signedDeny(c, mysql.TypeLonglong, math.MinInt64*1.1, strvalue(math.MinInt64))
	signedAccept(c, mysql.TypeLonglong, math.MinInt64, strvalue(math.MinInt64))
	signedAccept(c, mysql.TypeLonglong, math.MaxInt64, strvalue(math.MaxInt64))
	signedDeny(c, mysql.TypeLonglong, math.MaxInt64*1.1, strvalue(math.MaxInt64))
	unsignedDeny(c, mysql.TypeLonglong, -1, "0")
	unsignedAccept(c, mysql.TypeLonglong, 0, "0")
	unsignedAccept(c, mysql.TypeLonglong, uint(math.MaxUint64), strvalue(uint(math.MaxUint64)))
	unsignedDeny(c, mysql.TypeLonglong, math.MaxUint64*1.1, strvalue(uint(math.MaxUint64)))

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
	signedAccept(c, mysql.TypeFloat, int8(123), "123")
	signedAccept(c, mysql.TypeFloat, uint8(123), "123")
	signedAccept(c, mysql.TypeFloat, int16(123), "123")
	signedAccept(c, mysql.TypeFloat, uint16(123), "123")
	signedAccept(c, mysql.TypeFloat, int32(123), "123")
	signedAccept(c, mysql.TypeFloat, uint32(123), "123")
	signedAccept(c, mysql.TypeFloat, int64(123), "123")
	signedAccept(c, mysql.TypeFloat, uint64(123), "123")
	signedAccept(c, mysql.TypeFloat, int(123), "123")
	signedAccept(c, mysql.TypeFloat, uint(123), "123")
	signedAccept(c, mysql.TypeFloat, float32(123), "123")
	signedAccept(c, mysql.TypeFloat, float64(123), "123")
	signedAccept(c, mysql.TypeDouble, " -23.54", "-23.54")

	// year
	signedDeny(c, mysql.TypeYear, 123, "1901")
	signedDeny(c, mysql.TypeYear, 3000, "2155")
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
	signedAccept(c, mysql.TypeNewDecimal, int8(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint8(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, int16(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint16(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, int32(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint32(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, int64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, float32(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, 123.456, "123.456")
	signedAccept(c, mysql.TypeNewDecimal, "-123.456", "-123.456")
	signedAccept(c, mysql.TypeNewDecimal, mysql.NewDecimalFromInt(123, 5), "12300000")
	signedAccept(c, mysql.TypeNewDecimal, mysql.NewDecimalFromInt(-123, -5), "-0.00123")
}

func (s *testColumnSuite) TestString(c *C) {
	col := &Col{
		model.ColumnInfo{
			FieldType: *types.NewFieldType(mysql.TypeTiny),
		},
	}
	col.Flen = 2
	col.getTypeStr()
	col.Decimal = 1
	col.Charset = mysql.DefaultCharset
	col.Collate = mysql.DefaultCollationName
	col.Flag |= mysql.ZerofillFlag | mysql.UnsignedFlag | mysql.BinaryFlag | mysql.AutoIncrementFlag | mysql.NotNullFlag
	col.getTypeStr()

	cs := col.String()
	c.Assert(len(cs), Greater, 0)
}

func (s *testColumnSuite) TestFind(c *C) {
	cols := []*Col{
		newCol("a"),
		newCol("b"),
		newCol("c"),
	}
	FindCols(cols, []string{"a"})
	FindCols(cols, []string{"d"})
	cols[0].Flag |= mysql.OnUpdateNowFlag
	FindOnUpdateCols(cols)
}

func (s *testColumnSuite) TestCheck(c *C) {
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag
	cols := []*Col{col, col}
	CheckOnce(cols)
	cols = cols[:1]
	CheckNotNull(cols, []interface{}{nil})
	cols[0].Flag |= mysql.NotNullFlag
	CheckNotNull(cols, []interface{}{nil})
}

func (s *testColumnSuite) TestDesc(c *C) {
	col := newCol("a")
	col.Flag = mysql.AutoIncrementFlag | mysql.NotNullFlag | mysql.PriKeyFlag
	NewColDesc(col)
	col.Flag = mysql.MultipleKeyFlag
	NewColDesc(col)
	ColDescFieldNames(false)
	ColDescFieldNames(true)
}

func newCol(name string) *Col {
	return &Col{
		model.ColumnInfo{
			Name: model.NewCIStr(name),
		},
	}
}
