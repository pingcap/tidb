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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testFieldTypeSuite{})

type testFieldTypeSuite struct {
}

func (s *testFieldTypeSuite) TestFieldType(c *C) {
	defer testleak.AfterTest(c)()
	ft := NewFieldType(mysql.TypeDuration)
	c.Assert(ft.Flen, Equals, UnspecifiedLength)
	c.Assert(ft.Decimal, Equals, UnspecifiedLength)
	ft.Decimal = 5
	c.Assert(ft.String(), Equals, "time(5)")

	ft.Tp = mysql.TypeLong
	ft.Flag |= mysql.UnsignedFlag | mysql.ZerofillFlag
	c.Assert(ft.String(), Equals, "int(5) UNSIGNED ZEROFILL")
	c.Assert(ft.InfoSchemaStr(), Equals, "int(5) unsigned")

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 10
	ft.Decimal = 3
	c.Assert(ft.String(), Equals, "float(10,3)")
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 10
	ft.Decimal = -1
	c.Assert(ft.String(), Equals, "float")

	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 10
	ft.Decimal = 3
	c.Assert(ft.String(), Equals, "double(10,3)")
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 10
	ft.Decimal = -1
	c.Assert(ft.String(), Equals, "double")

	ft = NewFieldType(mysql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.Collate = "UTF8_UNICODE_GI"
	c.Assert(ft.String(), Equals, "text(10) CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI")

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= mysql.BinaryFlag
	c.Assert(ft.String(), Equals, "varchar(10) BINARY")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "enum('a','b')")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "enum('''a''','''b''')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "set('a','b')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "set('''a''','''b''')")

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "timestamp(2)")
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "timestamp")

	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "datetime(2)")
	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "datetime")
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "date(2)")
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "date")
}

func (s *testFieldTypeSuite) TestDefaultTypeForValue(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		value     interface{}
		tp        byte
		flen      int
		decimal   int
		charset   string
		collation string
		flag      uint
	}{
		{nil, mysql.TypeNull, 0, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{1, mysql.TypeLonglong, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{uint64(1), mysql.TypeLonglong, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{"abc", mysql.TypeVarString, 9, UnspecifiedLength, charset.CharsetUTF8, charset.CollationUTF8, 0},
		{1.1, mysql.TypeDouble, 3, 1, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{[]byte("abc"), mysql.TypeBlob, 3, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Bit{}, mysql.TypeVarchar, 3, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Hex{}, mysql.TypeVarchar, 3, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Time{Type: mysql.TypeDatetime}, mysql.TypeDatetime, 19, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Duration{}, mysql.TypeDuration, 9, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{&MyDecimal{}, mysql.TypeNewDecimal, 0, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Enum{Name: "a", Value: 1}, mysql.TypeEnum, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Set{Name: "a", Value: 1}, mysql.TypeSet, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
	}
	for _, tt := range tests {
		var ft FieldType
		DefaultTypeForValue(tt.value, &ft)
		c.Assert(ft.Tp, Equals, tt.tp, Commentf("%v %v", ft, tt))
	}
}

func (s *testFieldTypeSuite) TestAggFieldType(c *C) {
	defer testleak.AfterTest(c)()
	fts := []*FieldType{
		NewFieldType(mysql.TypeDecimal),
		NewFieldType(mysql.TypeTiny),
		NewFieldType(mysql.TypeShort),
		NewFieldType(mysql.TypeLong),
		NewFieldType(mysql.TypeFloat),
		NewFieldType(mysql.TypeDouble),
		NewFieldType(mysql.TypeNull),
		NewFieldType(mysql.TypeTimestamp),
		NewFieldType(mysql.TypeLonglong),
		NewFieldType(mysql.TypeInt24),
		NewFieldType(mysql.TypeDate),
		NewFieldType(mysql.TypeDuration),
		NewFieldType(mysql.TypeDatetime),
		NewFieldType(mysql.TypeYear),
		NewFieldType(mysql.TypeNewDate),
		NewFieldType(mysql.TypeVarchar),
		NewFieldType(mysql.TypeBit),
		NewFieldType(mysql.TypeJSON),
		NewFieldType(mysql.TypeNewDecimal),
		NewFieldType(mysql.TypeEnum),
		NewFieldType(mysql.TypeSet),
		NewFieldType(mysql.TypeTinyBlob),
		NewFieldType(mysql.TypeMediumBlob),
		NewFieldType(mysql.TypeLongBlob),
		NewFieldType(mysql.TypeBlob),
		NewFieldType(mysql.TypeVarString),
		NewFieldType(mysql.TypeString),
		NewFieldType(mysql.TypeGeometry),
	}

	for i := range fts {
		aggTp := AggFieldType(fts[i : i+1])
		c.Assert(aggTp.Tp, Equals, fts[i].Tp)

		aggTp = AggFieldType([]*FieldType{fts[i], fts[i]})
		switch fts[i].Tp {
		case mysql.TypeDate:
			c.Assert(aggTp.Tp, Equals, mysql.TypeNewDate)
		case mysql.TypeJSON:
			c.Assert(aggTp.Tp, Equals, mysql.TypeBit)
		case mysql.TypeEnum, mysql.TypeSet, mysql.TypeVarString:
			c.Assert(aggTp.Tp, Equals, mysql.TypeVarchar)
		default:
			c.Assert(aggTp.Tp, Equals, fts[i].Tp)
		}

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(mysql.TypeLong)})
		switch fts[i].Tp {
		case mysql.TypeDecimal, mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
			mysql.TypeYear, mysql.TypeInt24, mysql.TypeNull:
			c.Assert(aggTp.Tp, Equals, mysql.TypeLong)
		case mysql.TypeLonglong:
			c.Assert(aggTp.Tp, Equals, mysql.TypeLonglong)
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggTp.Tp, Equals, mysql.TypeDouble)
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration,
			mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeBit, mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet,
			mysql.TypeVarString, mysql.TypeGeometry:
			c.Assert(aggTp.Tp, Equals, mysql.TypeVarchar)
		case mysql.TypeString:
			c.Assert(aggTp.Tp, Equals, mysql.TypeString)
		case mysql.TypeNewDecimal:
			c.Assert(aggTp.Tp, Equals, mysql.TypeNewDecimal)
		case mysql.TypeTinyBlob:
			c.Assert(aggTp.Tp, Equals, mysql.TypeTinyBlob)
		case mysql.TypeBlob:
			c.Assert(aggTp.Tp, Equals, mysql.TypeBlob)
		case mysql.TypeMediumBlob:
			c.Assert(aggTp.Tp, Equals, mysql.TypeMediumBlob)
		case mysql.TypeLongBlob:
			c.Assert(aggTp.Tp, Equals, mysql.TypeLongBlob)
		}
	}
}

func (s *testFieldTypeSuite) TestAggTypeClass(c *C) {
	defer testleak.AfterTest(c)()
	fts := []*FieldType{
		NewFieldType(mysql.TypeDecimal),
		NewFieldType(mysql.TypeTiny),
		NewFieldType(mysql.TypeShort),
		NewFieldType(mysql.TypeLong),
		NewFieldType(mysql.TypeFloat),
		NewFieldType(mysql.TypeDouble),
		NewFieldType(mysql.TypeNull),
		NewFieldType(mysql.TypeTimestamp),
		NewFieldType(mysql.TypeLonglong),
		NewFieldType(mysql.TypeInt24),
		NewFieldType(mysql.TypeDate),
		NewFieldType(mysql.TypeDuration),
		NewFieldType(mysql.TypeDatetime),
		NewFieldType(mysql.TypeYear),
		NewFieldType(mysql.TypeNewDate),
		NewFieldType(mysql.TypeVarchar),
		NewFieldType(mysql.TypeBit),
		NewFieldType(mysql.TypeJSON),
		NewFieldType(mysql.TypeNewDecimal),
		NewFieldType(mysql.TypeEnum),
		NewFieldType(mysql.TypeSet),
		NewFieldType(mysql.TypeTinyBlob),
		NewFieldType(mysql.TypeMediumBlob),
		NewFieldType(mysql.TypeLongBlob),
		NewFieldType(mysql.TypeBlob),
		NewFieldType(mysql.TypeVarString),
		NewFieldType(mysql.TypeString),
		NewFieldType(mysql.TypeGeometry),
	}

	for i := range fts {
		var flag uint
		aggTc := AggTypeClass(fts[i:i+1], &flag)
		switch fts[i].Tp {
		case mysql.TypeDecimal, mysql.TypeNull, mysql.TypeTimestamp, mysql.TypeDate,
			mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			c.Assert(aggTc, Equals, ClassString)
			c.Assert(flag, Equals, uint(0))
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeInt24, mysql.TypeYear, mysql.TypeBit:
			c.Assert(aggTc, Equals, ClassInt)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggTc, Equals, ClassReal)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		case mysql.TypeNewDecimal:
			c.Assert(aggTc, Equals, ClassDecimal)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		}

		flag = 0
		aggTc = AggTypeClass([]*FieldType{fts[i], fts[i]}, &flag)
		switch fts[i].Tp {
		case mysql.TypeDecimal, mysql.TypeNull, mysql.TypeTimestamp, mysql.TypeDate,
			mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			c.Assert(aggTc, Equals, ClassString)
			c.Assert(flag, Equals, uint(0))
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeInt24, mysql.TypeYear, mysql.TypeBit:
			c.Assert(aggTc, Equals, ClassInt)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggTc, Equals, ClassReal)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		case mysql.TypeNewDecimal:
			c.Assert(aggTc, Equals, ClassDecimal)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		}
		flag = 0
		aggTc = AggTypeClass([]*FieldType{fts[i], NewFieldType(mysql.TypeLong)}, &flag)
		switch fts[i].Tp {
		case mysql.TypeDecimal, mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration,
			mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar, mysql.TypeJSON,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob, mysql.TypeMediumBlob,
			mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString,
			mysql.TypeString, mysql.TypeGeometry:
			c.Assert(aggTc, Equals, ClassString)
			c.Assert(flag, Equals, uint(0))
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeNull,
			mysql.TypeLonglong, mysql.TypeYear, mysql.TypeInt24, mysql.TypeBit:
			c.Assert(aggTc, Equals, ClassInt)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggTc, Equals, ClassReal)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		case mysql.TypeNewDecimal:
			c.Assert(aggTc, Equals, ClassDecimal)
			c.Assert(flag, Equals, uint(mysql.BinaryFlag))
		}
	}
}
