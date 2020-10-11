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
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
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

	ft = NewFieldType(mysql.TypeLong)
	ft.Flen = 5
	ft.Flag = mysql.UnsignedFlag | mysql.ZerofillFlag
	c.Assert(ft.String(), Equals, "int(5) UNSIGNED ZEROFILL")
	c.Assert(ft.InfoSchemaStr(), Equals, "int(5) unsigned")

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 12   // Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "float(12,3)")
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 12    // Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "float")
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "float")
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "float(7,3)")

	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 22   // Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "double(22,3)")
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 22    // Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "double")
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "double")
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "double(7,3)")

	ft = NewFieldType(mysql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.Collate = "UTF8_UNICODE_GI"
	c.Assert(ft.String(), Equals, "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI")

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= mysql.BinaryFlag
	c.Assert(ft.String(), Equals, "varchar(10) BINARY COLLATE utf8mb4_bin")

	ft = NewFieldType(mysql.TypeString)
	ft.Charset = charset.CollationBin
	ft.Flag |= mysql.BinaryFlag
	c.Assert(ft.String(), Equals, "binary(1) COLLATE utf8mb4_bin")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "enum('a','b')")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "enum('''a''','''b''')")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a\tb", "a\rb"}
	c.Assert(ft.String(), Equals, "enum('a\\nb','a\tb','a\\rb')")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	c.Assert(ft.String(), Equals, "enum('a\\nb','a''	\\r\\nb','a\\rb')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "set('a','b')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "set('''a''','''b''')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	c.Assert(ft.String(), Equals, "set('a\\nb','a''	\\r\\nb','a\\rb')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a'\nb", "a'b\tc"}
	c.Assert(ft.String(), Equals, "set('a''\\nb','a''b	c')")

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
	c.Assert(ft.String(), Equals, "date")
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "date")

	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 4
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "year(4)")
	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 2
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "year(2)") // Note: Invalid year.
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
		{1, mysql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{0, mysql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{432, mysql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{4321, mysql.TypeLonglong, 4, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{1234567, mysql.TypeLonglong, 7, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{12345678, mysql.TypeLonglong, 8, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{12345678901234567, mysql.TypeLonglong, 17, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{-42, mysql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{uint64(1), mysql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{uint64(123), mysql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{uint64(1234), mysql.TypeLonglong, 4, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{uint64(1234567), mysql.TypeLonglong, 7, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{uint64(12345678), mysql.TypeLonglong, 8, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{uint64(12345678901234567), mysql.TypeLonglong, 17, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{"abc", mysql.TypeVarString, 3, UnspecifiedLength, charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0},
		{1.1, mysql.TypeDouble, 3, -1, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{[]byte("abc"), mysql.TypeBlob, 3, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{HexLiteral{}, mysql.TypeVarString, 0, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{BitLiteral{}, mysql.TypeVarString, 0, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{NewTime(ZeroCoreTime, mysql.TypeDatetime, DefaultFsp), mysql.TypeDatetime, 19, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{NewTime(FromDate(2017, 12, 12, 12, 59, 59, 0), mysql.TypeDatetime, 3), mysql.TypeDatetime, 23, 3, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Duration{}, mysql.TypeDuration, 8, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{&MyDecimal{}, mysql.TypeNewDecimal, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Enum{Name: "a", Value: 1}, mysql.TypeEnum, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
		{Set{Name: "a", Value: 1}, mysql.TypeSet, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag},
	}
	for _, tt := range tests {
		var ft FieldType
		DefaultTypeForValue(tt.value, &ft, mysql.DefaultCharset, mysql.DefaultCollationName)
		c.Assert(ft.Tp, Equals, tt.tp, Commentf("%v %v", ft.Tp, tt.tp))
		c.Assert(ft.Flen, Equals, tt.flen, Commentf("%v %v", ft.Flen, tt.flen))
		c.Assert(ft.Charset, Equals, tt.charset, Commentf("%v %v", ft.Charset, tt.charset))
		c.Assert(ft.Decimal, Equals, tt.decimal, Commentf("%v %v", ft.Decimal, tt.decimal))
		c.Assert(ft.Collate, Equals, tt.collation, Commentf("%v %v", ft.Collate, tt.collation))
		c.Assert(ft.Flag, Equals, tt.flag, Commentf("%v %v", ft.Flag, tt.flag))
	}
}

func (s *testFieldTypeSuite) TestAggFieldType(c *C) {
	defer testleak.AfterTest(c)()
	fts := []*FieldType{
		NewFieldType(mysql.TypeUnspecified),
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
			c.Assert(aggTp.Tp, Equals, mysql.TypeDate)
		case mysql.TypeJSON:
			c.Assert(aggTp.Tp, Equals, mysql.TypeJSON)
		case mysql.TypeEnum, mysql.TypeSet, mysql.TypeVarString:
			c.Assert(aggTp.Tp, Equals, mysql.TypeVarchar)
		case mysql.TypeUnspecified:
			c.Assert(aggTp.Tp, Equals, mysql.TypeNewDecimal)
		default:
			c.Assert(aggTp.Tp, Equals, fts[i].Tp)
		}

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(mysql.TypeLong)})
		switch fts[i].Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
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
		case mysql.TypeUnspecified, mysql.TypeNewDecimal:
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

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(mysql.TypeJSON)})
		switch fts[i].Tp {
		case mysql.TypeJSON, mysql.TypeNull:
			c.Assert(aggTp.Tp, Equals, mysql.TypeJSON)
		case mysql.TypeLongBlob, mysql.TypeMediumBlob, mysql.TypeTinyBlob, mysql.TypeBlob:
			c.Assert(aggTp.Tp, Equals, mysql.TypeLongBlob)
		case mysql.TypeString:
			c.Assert(aggTp.Tp, Equals, mysql.TypeString)
		default:
			c.Assert(aggTp.Tp, Equals, mysql.TypeVarchar)
		}
	}
}
func (s *testFieldTypeSuite) TestAggFieldTypeForTypeFlag(c *C) {
	types := []*FieldType{
		NewFieldType(mysql.TypeLonglong),
		NewFieldType(mysql.TypeLonglong),
	}

	aggTp := AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, mysql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, uint(0))

	types[0].Flag = mysql.NotNullFlag
	aggTp = AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, mysql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, uint(0))

	types[0].Flag = 0
	types[1].Flag = mysql.NotNullFlag
	aggTp = AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, mysql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, uint(0))

	types[0].Flag = mysql.NotNullFlag
	aggTp = AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, mysql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, mysql.NotNullFlag)
}

func (s *testFieldTypeSuite) TestAggregateEvalType(c *C) {
	defer testleak.AfterTest(c)()
	fts := []*FieldType{
		NewFieldType(mysql.TypeUnspecified),
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
		aggregatedEvalType := AggregateEvalType(fts[i:i+1], &flag)
		switch fts[i].Tp {
		case mysql.TypeUnspecified, mysql.TypeNull, mysql.TypeTimestamp, mysql.TypeDate,
			mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			c.Assert(aggregatedEvalType.IsStringKind(), IsTrue)
			c.Assert(flag, Equals, uint(0))
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeBit,
			mysql.TypeInt24, mysql.TypeYear:
			c.Assert(aggregatedEvalType, Equals, ETInt)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggregatedEvalType, Equals, ETReal)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		case mysql.TypeNewDecimal:
			c.Assert(aggregatedEvalType, Equals, ETDecimal)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		}

		flag = 0
		aggregatedEvalType = AggregateEvalType([]*FieldType{fts[i], fts[i]}, &flag)
		switch fts[i].Tp {
		case mysql.TypeUnspecified, mysql.TypeNull, mysql.TypeTimestamp, mysql.TypeDate,
			mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			c.Assert(aggregatedEvalType.IsStringKind(), IsTrue)
			c.Assert(flag, Equals, uint(0))
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeBit,
			mysql.TypeInt24, mysql.TypeYear:
			c.Assert(aggregatedEvalType, Equals, ETInt)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggregatedEvalType, Equals, ETReal)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		case mysql.TypeNewDecimal:
			c.Assert(aggregatedEvalType, Equals, ETDecimal)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		}
		flag = 0
		aggregatedEvalType = AggregateEvalType([]*FieldType{fts[i], NewFieldType(mysql.TypeLong)}, &flag)
		switch fts[i].Tp {
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration,
			mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar, mysql.TypeJSON,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob, mysql.TypeMediumBlob,
			mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString,
			mysql.TypeString, mysql.TypeGeometry:
			c.Assert(aggregatedEvalType.IsStringKind(), IsTrue)
			c.Assert(flag, Equals, uint(0))
		case mysql.TypeUnspecified, mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeNull, mysql.TypeBit,
			mysql.TypeLonglong, mysql.TypeYear, mysql.TypeInt24:
			c.Assert(aggregatedEvalType, Equals, ETInt)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		case mysql.TypeFloat, mysql.TypeDouble:
			c.Assert(aggregatedEvalType, Equals, ETReal)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		case mysql.TypeNewDecimal:
			c.Assert(aggregatedEvalType, Equals, ETDecimal)
			c.Assert(flag, Equals, mysql.BinaryFlag)
		}
	}
}
