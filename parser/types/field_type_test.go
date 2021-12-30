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

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	. "github.com/pingcap/tidb/parser/types"
	"github.com/stretchr/testify/require"

	// import parser_driver
	_ "github.com/pingcap/tidb/parser/test_driver"
)

func TestFieldType(t *testing.T) {
	ft := NewFieldType(mysql.TypeDuration)
	require.Equal(t, UnspecifiedLength, ft.Flen)
	require.Equal(t, UnspecifiedLength, ft.Decimal)
	ft.Decimal = 5
	require.Equal(t, "time(5)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeLong)
	ft.Flen = 5
	ft.Flag = mysql.UnsignedFlag | mysql.ZerofillFlag
	require.Equal(t, "int(5) UNSIGNED ZEROFILL", ft.String())
	require.Equal(t, "int(5) unsigned", ft.InfoSchemaStr())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 12   // Default
	ft.Decimal = 3 // Not Default
	require.Equal(t, "float(12,3)", ft.String())
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 12    // Default
	ft.Decimal = -1 // Default
	require.Equal(t, "float", ft.String())
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	require.Equal(t, "float", ft.String())
	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	require.Equal(t, "float(7,3)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 22   // Default
	ft.Decimal = 3 // Not Default
	require.Equal(t, "double(22,3)", ft.String())
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 22    // Default
	ft.Decimal = -1 // Default
	require.Equal(t, "double", ft.String())
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	require.Equal(t, "double", ft.String())
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	require.Equal(t, "double(7,3)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.Collate = "UTF8_UNICODE_GI"
	require.Equal(t, "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= mysql.BinaryFlag
	require.Equal(t, "varchar(10) BINARY", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeString)
	ft.Charset = charset.CollationBin
	ft.Flag |= mysql.BinaryFlag
	require.Equal(t, "binary(1)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	require.Equal(t, "enum('a','b')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	require.Equal(t, "enum('''a''','''b''')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a\tb", "a\rb"}
	require.Equal(t, "enum('a\\nb','a\tb','a\\rb')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	require.Equal(t, "enum('a\\nb','a''	\\r\\nb','a\\rb')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b"}
	require.Equal(t, "set('a','b')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	require.Equal(t, "set('''a''','''b''')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	require.Equal(t, "set('a\\nb','a''	\\r\\nb','a\\rb')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a'\nb", "a'b\tc"}
	require.Equal(t, "set('a''\\nb','a''b	c')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 2
	require.Equal(t, "timestamp(2)", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 0
	require.Equal(t, "timestamp", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 2
	require.Equal(t, "datetime(2)", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 0
	require.Equal(t, "datetime", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 2
	require.Equal(t, "date", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	require.Equal(t, "date", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 4
	ft.Decimal = 0
	require.Equal(t, "year(4)", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeYear)
	ft.Flen = 2
	ft.Decimal = 2
	require.Equal(t, "year(2)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeVarchar)
	ft.Flen = 0
	ft.Decimal = 0
	require.Equal(t, "varchar(0)", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 0
	ft.Decimal = 0
	require.Equal(t, "char(0)", ft.String())
	require.True(t, HasCharset(ft))
}

func TestHasCharsetFromStmt(t *testing.T) {
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
	for _, typ := range types {
		sql := fmt.Sprintf(template, typ.strType)
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)

		col := stmt.(*ast.CreateTableStmt).Cols[0]
		require.Equal(t, typ.hasCharset, HasCharset(col.Tp))
	}
}

func TestEnumSetFlen(t *testing.T) {
	p := parser.New()
	cases := []struct {
		sql string
		ex  int
	}{
		{"enum('a')", 1},
		{"enum('a', 'b')", 1},
		{"enum('a', 'bb')", 2},
		{"enum('a', 'b', 'c')", 1},
		{"enum('a', 'bb', 'c')", 2},
		{"enum('a', 'bb', 'c')", 2},
		{"enum('')", 0},
		{"enum('a', '')", 1},
		{"set('a')", 1},
		{"set('a', 'b')", 3},
		{"set('a', 'bb')", 4},
		{"set('a', 'b', 'c')", 5},
		{"set('a', 'bb', 'c')", 6},
		{"set('')", 0},
		{"set('a', '')", 2},
	}

	for _, ca := range cases {
		stmt, err := p.ParseOneStmt(fmt.Sprintf("create table t (e %v)", ca.sql), "", "")
		require.NoError(t, err)
		col := stmt.(*ast.CreateTableStmt).Cols[0]
		require.Equal(t, ca.ex, col.Tp.Flen)

	}
}

func TestFieldTypeEqual(t *testing.T) {
	// Tp not equal
	ft1 := NewFieldType(mysql.TypeDouble)
	ft2 := NewFieldType(mysql.TypeFloat)
	require.Equal(t, false, ft1.Equal(ft2))

	// Decimal not equal
	ft2 = NewFieldType(mysql.TypeDouble)
	ft2.Decimal = 5
	require.Equal(t, false, ft1.Equal(ft2))

	// Flen not equal and decimal not -1
	ft1.Decimal = 5
	ft1.Flen = 22
	require.Equal(t, false, ft1.Equal(ft2))

	// Flen equal
	ft2.Flen = 22
	require.Equal(t, true, ft1.Equal(ft2))

	// Decimal is -1
	ft1.Decimal = -1
	ft2.Decimal = -1
	ft1.Flen = 23
	require.Equal(t, true, ft1.Equal(ft2))
}
