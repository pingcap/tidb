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
		{Bit{}, mysql.TypeBit},
		{Hex{}, mysql.TypeVarchar},
		{Time{Type: mysql.TypeDatetime}, mysql.TypeDatetime},
		{Duration{}, mysql.TypeDuration},
		{&MyDecimal{}, mysql.TypeNewDecimal},
		{Enum{}, mysql.TypeEnum},
		{Set{}, mysql.TypeSet},
		{nil, mysql.TypeNull},
	}
	for _, ca := range cases {
		var ft FieldType
		DefaultTypeForValue(ca.value, &ft)
		c.Assert(ft.Tp, Equals, ca.tp, Commentf("%v %v", ft, ca))
	}
}
