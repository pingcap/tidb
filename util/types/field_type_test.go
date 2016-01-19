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
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
)

var _ = check.Suite(&testFieldTypeSuite{})

type testFieldTypeSuite struct {
}

func (s *testFieldTypeSuite) TestFieldType(c *check.C) {
	ft := NewFieldType(mysql.TypeDuration)
	c.Assert(ft.Flen, check.Equals, UnspecifiedLength)
	c.Assert(ft.Decimal, check.Equals, UnspecifiedLength)
	ft.Decimal = 5
	c.Assert(ft.String(), check.Equals, "time(5)")

	ft.Tp = mysql.TypeLong
	ft.Flag |= mysql.UnsignedFlag | mysql.ZerofillFlag
	c.Assert(ft.String(), check.Equals, "int(5) UNSIGNED ZEROFILL")

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 10
	ft.Decimal = 3
	c.Assert(ft.String(), check.Equals, "float(10,3)")
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 10
	ft.Decimal = -1
	c.Assert(ft.String(), check.Equals, "float")

	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 10
	ft.Decimal = 3
	c.Assert(ft.String(), check.Equals, "double(10,3)")
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 10
	ft.Decimal = -1
	c.Assert(ft.String(), check.Equals, "double")

	ft = NewFieldType(mysql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.Collate = "UTF8_UNICODE_GI"
	c.Assert(ft.String(), check.Equals, "text(10) CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI")

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= mysql.BinaryFlag
	c.Assert(ft.String(), check.Equals, "varchar(10) BINARY")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), check.Equals, "enum('a','b')")

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), check.Equals, "enum('''a''','''b''')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), check.Equals, "set('a','b')")

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), check.Equals, "set('''a''','''b''')")

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), check.Equals, "timestamp(2)")
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), check.Equals, "timestamp")

	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), check.Equals, "datetime(2)")
	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), check.Equals, "datetime")
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), check.Equals, "date(2)")
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), check.Equals, "date")
}

func (s *testFieldTypeSuite) TestDataItem(c *check.C) {
	ft := NewFieldType(mysql.TypeBlob)
	d := &DataItem{
		Type: ft,
	}
	c.Assert(RawData(d), check.IsNil)
	c.Assert(IsNil(d), check.IsTrue)

	d.Data = "string"
	c.Assert(RawData(d), check.Equals, "string")
	c.Assert(IsNil(d), check.IsFalse)
}

func (s *testFieldTypeSuite) TestDefaultTypeForValue(c *check.C) {
	nullType := DefaultTypeForValue(nil)
	di := &DataItem{Type: nullType}
	cases := []struct {
		value interface{}
		tp    byte
	}{
		{nil, mysql.TypeNull},
		{1, mysql.TypeLonglong},
		{uint64(1), mysql.TypeLonglong},
		{"abc", mysql.TypeVarString},
		{1.1, mysql.TypeNewDecimal},
		{[]byte("abc"), mysql.TypeBlob},
		{mysql.Bit{}, mysql.TypeBit},
		{mysql.Hex{}, mysql.TypeVarchar},
		{mysql.Time{Type: mysql.TypeDatetime}, mysql.TypeDatetime},
		{mysql.Duration{}, mysql.TypeDuration},
		{mysql.Decimal{}, mysql.TypeNewDecimal},
		{mysql.Enum{}, mysql.TypeEnum},
		{mysql.Set{}, mysql.TypeSet},
		{di, mysql.TypeNull},
	}
	for _, ca := range cases {
		ft := DefaultTypeForValue(ca.value)
		c.Assert(ft.Tp, check.Equals, ca.tp, check.Commentf("%v %v", ft, ca))
	}
}
