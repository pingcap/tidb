// Copyright 2019 PingCAP, Inc.
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

package types_test

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	. "github.com/pingcap/parser/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testFieldTypeSuite{})

type testFieldTypeSuite struct {
}

func (s *testFieldTypeSuite) TestFieldType(c *C) {
	ft := NewFieldType(mysql.TypeDuration)
	c.Assert(ft.Flen, Equals, UnspecifiedLength)
	c.Assert(ft.Decimal, Equals, UnspecifiedLength)
	ft.Decimal = 5
	c.Assert(ft.String(), Equals, "time(5)")
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeLong)
	ft.Flen = 5
	ft.Flag = mysql.UnsignedFlag | mysql.ZerofillFlag
	c.Assert(ft.String(), Equals, "int(5) UNSIGNED ZEROFILL")
	c.Assert(ft.InfoSchemaStr(), Equals, "int(5) unsigned")
	c.Assert(HasCharset(ft), IsFalse)

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
	c.Assert(HasCharset(ft), IsFalse)

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
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.Collate = "UTF8_UNICODE_GI"
	c.Assert(ft.String(), Equals, "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= mysql.BinaryFlag
	c.Assert(ft.String(), Equals, "varchar(10) BINARY")
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeString)
	ft.Charset = charset.CollationBin
	ft.Flag |= mysql.BinaryFlag
	c.Assert(ft.String(), Equals, "binary(1)")
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "enum('a','b')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "enum('''a''','''b''')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a\tb", "a\rb"}
	c.Assert(ft.String(), Equals, "enum('a\\nb','a\tb','a\\rb')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	c.Assert(ft.String(), Equals, "enum('a\\nb','a''	\\r\\nb','a\\rb')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "set('a','b')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "set('''a''','''b''')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	c.Assert(ft.String(), Equals, "set('a\\nb','a''	\\r\\nb','a\\rb')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a'\nb", "a'b\tc"}
	c.Assert(ft.String(), Equals, "set('a''\\nb','a''b	c')")
	c.Assert(HasCharset(ft), IsTrue)

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "timestamp(2)")
	c.Assert(HasCharset(ft), IsFalse)
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "timestamp")
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "datetime(2)")
	c.Assert(HasCharset(ft), IsFalse)
	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "datetime")
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "date")
	c.Assert(HasCharset(ft), IsFalse)
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "date")
	c.Assert(HasCharset(ft), IsFalse)

	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 4
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "year(4)")
	c.Assert(HasCharset(ft), IsFalse)
	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 2
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "year(2)") // Note: Invalid year.
	c.Assert(HasCharset(ft), IsFalse)
}

func (s *testFieldTypeSuite) TestHasCharsetFromStmt(c *C) {
	template := "CREATE TABLE t(a %s)"

	types := []struct {
		strType    string
		hasCharset bool
	}{
		{"int", false},
		{"real", false},
		{"float", false},
		{"bit", false},
		{"bool", false},
		{"char(1)", true},
		{"national char(1)", true},
		{"binary", false},
		{"varchar(1)", true},
		{"national varchar(1)", true},
		{"varbinary(1)", false},
		{"year", false},
		{"date", false},
		{"time", false},
		{"datetime", false},
		{"timestamp", false},
		{"blob", false},
		{"tinyblob", false},
		{"mediumblob", false},
		{"longblob", false},
		{"bit", false},
		{"text", true},
		{"tinytext", true},
		{"mediumtext", true},
		{"longtext", true},
		{"json", false},
		{"enum('1')", true},
		{"set('1')", true},
	}

	p := parser.New()
	for _, t := range types {
		sql := fmt.Sprintf(template, t.strType)
		stmt, err := p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)

		col := stmt.(*ast.CreateTableStmt).Cols[0]
		c.Assert(HasCharset(col.Tp), Equals, t.hasCharset)
	}
}
