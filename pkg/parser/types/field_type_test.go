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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	// import parser_driver
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	. "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestFieldType(t *testing.T) {
	ft := NewFieldType(mysql.TypeDuration)
	require.Equal(t, UnspecifiedLength, ft.GetFlen())
	require.Equal(t, UnspecifiedLength, ft.GetDecimal())
	ft.SetDecimal(5)
	require.Equal(t, "time(5)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeLong)
	ft.SetFlen(5)
	ft.SetFlag(mysql.UnsignedFlag | mysql.ZerofillFlag)
	require.Equal(t, "int(5) UNSIGNED ZEROFILL", ft.String())
	require.Equal(t, "int(5) unsigned", ft.InfoSchemaStr())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeFloat)
	ft.SetFlen(12)   // Default
	ft.SetDecimal(3) // Not Default
	require.Equal(t, "float(12,3)", ft.String())
	ft = NewFieldType(mysql.TypeFloat)
	ft.SetFlen(12)    // Default
	ft.SetDecimal(-1) // Default
	require.Equal(t, "float", ft.String())
	ft = NewFieldType(mysql.TypeFloat)
	ft.SetFlen(5)     // Not Default
	ft.SetDecimal(-1) // Default
	require.Equal(t, "float", ft.String())
	ft = NewFieldType(mysql.TypeFloat)
	ft.SetFlen(7)    // Not Default
	ft.SetDecimal(3) // Not Default
	require.Equal(t, "float(7,3)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeDouble)
	ft.SetFlen(22)   // Default
	ft.SetDecimal(3) // Not Default
	require.Equal(t, "double(22,3)", ft.String())
	ft = NewFieldType(mysql.TypeDouble)
	ft.SetFlen(22)    // Default
	ft.SetDecimal(-1) // Default
	require.Equal(t, "double", ft.String())
	ft = NewFieldType(mysql.TypeDouble)
	ft.SetFlen(5)     // Not Default
	ft.SetDecimal(-1) // Default
	require.Equal(t, "double", ft.String())
	ft = NewFieldType(mysql.TypeDouble)
	ft.SetFlen(7)    // Not Default
	ft.SetDecimal(3) // Not Default
	require.Equal(t, "double(7,3)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeBlob)
	ft.SetFlen(10)
	ft.SetCharset("UTF8")
	ft.SetCollate("UTF8_UNICODE_GI")
	require.Equal(t, "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(10)
	ft.AddFlag(mysql.BinaryFlag)
	require.Equal(t, "varchar(10) BINARY", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeString)
	ft.SetCharset(charset.CharsetBin)
	ft.AddFlag(mysql.BinaryFlag)
	require.Equal(t, "binary(1)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"a", "b"})
	require.Equal(t, "enum('a','b')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"'a'", "'b'"})
	require.Equal(t, "enum('''a''','''b''')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"a\nb", "a\tb", "a\rb"})
	require.Equal(t, "enum('a\\nb','a\tb','a\\rb')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"a\nb", "a'\t\r\nb", "a\rb"})
	require.Equal(t, "enum('a\\nb','a''	\\r\\nb','a\\rb')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"a", "b"})
	require.Equal(t, "set('a','b')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"'a'", "'b'"})
	require.Equal(t, "set('''a''','''b''')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"a\nb", "a'\t\r\nb", "a\rb"})
	require.Equal(t, "set('a\\nb','a''	\\r\\nb','a\\rb')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"a'\nb", "a'b\tc"})
	require.Equal(t, "set('a''\\nb','a''b	c')", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.SetFlen(8)
	ft.SetDecimal(2)
	require.Equal(t, "timestamp(2)", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.SetFlen(8)
	ft.SetDecimal(0)
	require.Equal(t, "timestamp", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeDatetime)
	ft.SetFlen(8)
	ft.SetDecimal(2)
	require.Equal(t, "datetime(2)", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeDatetime)
	ft.SetFlen(8)
	ft.SetDecimal(0)
	require.Equal(t, "datetime", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeDate)
	ft.SetFlen(8)
	ft.SetDecimal(2)
	require.Equal(t, "date", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeDate)
	ft.SetFlen(8)
	ft.SetDecimal(0)
	require.Equal(t, "date", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeYear)
	ft.SetFlen(4)
	ft.SetDecimal(0)
	require.Equal(t, "year(4)", ft.String())
	require.False(t, HasCharset(ft))
	ft = NewFieldType(mysql.TypeYear)
	ft.SetFlen(2)
	ft.SetDecimal(2)
	require.Equal(t, "year(2)", ft.String())
	require.False(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(0)
	ft.SetDecimal(0)
	require.Equal(t, "varchar(0)", ft.String())
	require.True(t, HasCharset(ft))

	ft = NewFieldType(mysql.TypeString)
	ft.SetFlen(0)
	ft.SetDecimal(0)
	require.Equal(t, "char(0)", ft.String())
	require.True(t, HasCharset(ft))

	t.Run("BinaryAttribute", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			tp      byte
			flen    int
			decimal int
			charset string
			flags   uint
			want    string
		}{
			{"count", mysql.TypeLonglong, 21, 0, charset.CharsetBin, 0, "bigint(21)"},
			{"sum", mysql.TypeNewDecimal, 41, 0, charset.CharsetBin, 0, "decimal(41,0)"},
			{"unsigned", mysql.TypeLong, 11, 0, "", mysql.UnsignedFlag | mysql.ZerofillFlag, "int(11) UNSIGNED ZEROFILL"},
			{"double", mysql.TypeDouble, -1, -1, charset.CharsetBin, 0, "double"},
			{"date", mysql.TypeDate, -1, -1, charset.CharsetBin, 0, "date"},
			{"time", mysql.TypeDuration, -1, 3, charset.CharsetBin, 0, "time(3)"},
			{"datetime", mysql.TypeDatetime, -1, 6, charset.CharsetBin, 0, "datetime(6)"},
			{"timestamp", mysql.TypeTimestamp, -1, 6, charset.CharsetBin, 0, "timestamp(6)"},
			{"json", mysql.TypeJSON, -1, -1, charset.CharsetBin, 0, "json"},
			{"char", mysql.TypeString, 10, -1, charset.CharsetUTF8MB4, 0, "char(10) BINARY CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
			{"varchar", mysql.TypeVarchar, 10, -1, charset.CharsetUTF8MB4, 0, "varchar(10) BINARY CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
			{"text", mysql.TypeBlob, -1, -1, charset.CharsetUTF8MB4, 0, "text BINARY CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"},
			{"enum", mysql.TypeEnum, -1, -1, charset.CharsetUTF8MB4, 0, "enum('x') BINARY"},
			{"set", mysql.TypeSet, -1, -1, charset.CharsetUTF8MB4, 0, "set('x') BINARY"},
			{"binary", mysql.TypeString, 10, -1, charset.CharsetBin, 0, "binary(10)"},
			{"varbinary", mysql.TypeVarchar, 10, -1, charset.CharsetBin, 0, "varbinary(10)"},
			{"blob", mysql.TypeBlob, -1, -1, charset.CharsetBin, 0, "blob"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ft := NewFieldType(tc.tp)
				ft.SetFlen(tc.flen)
				ft.SetDecimal(tc.decimal)
				ft.SetCharset(tc.charset)
				ft.SetFlag(mysql.BinaryFlag | tc.flags)
				if tc.charset == charset.CharsetUTF8MB4 {
					ft.SetCollate(charset.CollationUTF8MB4)
				}
				if tc.tp == mysql.TypeEnum || tc.tp == mysql.TypeSet {
					ft.SetElems([]string{"x"})
				}
				original := ft.Clone()
				p := parser.New()
				_, err := p.ParseOneStmt(fmt.Sprintf("CREATE TABLE t (a %s)", ft.String()), "", "")
				require.NoError(t, err)
				require.Equal(t, tc.want, ft.String())
				var restored strings.Builder
				require.NoError(t, ft.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &restored)))
				_, err = p.ParseOneStmt(fmt.Sprintf("CREATE TABLE t (a %s)", restored.String()), "", "")
				require.NoError(t, err)
				require.Equal(t, original, ft, "formatting must preserve type metadata")
			})
		}
	})
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
		require.Equal(t, ca.ex, col.Tp.GetFlen())
	}
}

func TestFieldTypeEqual(t *testing.T) {
	// tp not equal
	ft1 := NewFieldType(mysql.TypeDouble)
	ft2 := NewFieldType(mysql.TypeFloat)
	require.Equal(t, false, ft1.Equal(ft2))

	// decimal not equal
	ft2 = NewFieldType(mysql.TypeDouble)
	ft2.SetDecimal(5)
	require.Equal(t, false, ft1.Equal(ft2))

	// flen not equal and decimal not -1
	ft1.SetDecimal(5)
	ft1.SetFlen(22)
	require.Equal(t, false, ft1.Equal(ft2))

	// flen equal
	ft2.SetFlen(22)
	require.Equal(t, true, ft1.Equal(ft2))

	// decimal is -1
	ft1.SetDecimal(-1)
	ft2.SetDecimal(-1)
	ft1.SetFlen(23)
	require.Equal(t, true, ft1.Equal(ft2))
}

func TestCompactStr(t *testing.T) {
	cases := []struct {
		t     byte   // Field Type
		flen  int    // Field Length
		flags uint   // Field Flags, e.g. ZEROFILL
		e1    string // Expected string with TiDBStrictIntegerDisplayWidth disabled
		e2    string // Expected string with TiDBStrictIntegerDisplayWidth enabled
	}{
		// TINYINT(1) is considered a bool by connectors, this should always display
		// the display length.
		{mysql.TypeTiny, 1, 0, `tinyint(1)`, `tinyint(1)`},
		{mysql.TypeTiny, 2, 0, `tinyint(2)`, `tinyint`},

		// If the ZEROFILL flag is set the display length should not be hidden.
		{mysql.TypeLong, 10, 0, `int(10)`, `int`},
		{mysql.TypeLong, 10, mysql.ZerofillFlag, `int(10)`, `int(10)`},
	}
	for _, cc := range cases {
		ft := NewFieldType(cc.t)
		ft.SetFlen(cc.flen)
		ft.SetFlag(cc.flags)

		TiDBStrictIntegerDisplayWidth = false
		require.Equal(t, cc.e1, ft.CompactStr())

		TiDBStrictIntegerDisplayWidth = true
		require.Equal(t, cc.e2, ft.CompactStr())
	}
}
