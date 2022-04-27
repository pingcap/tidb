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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"testing"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestFieldType(t *testing.T) {
	ft := NewFieldType(mysql.TypeDuration)
	require.Equal(t, UnspecifiedLength, ft.GetFlen())
	require.Equal(t, UnspecifiedLength, ft.GetDecimal())

	ft.SetDecimal(5)
	require.Equal(t, "time(5)", ft.String())

	ft = NewFieldType(mysql.TypeLong)
	ft.SetFlen(5)
	ft.SetFlen(5)
	ft.SetFlag(mysql.UnsignedFlag | mysql.ZerofillFlag)
	require.Equal(t, "int(5) UNSIGNED ZEROFILL", ft.String())
	require.Equal(t, "int(5) unsigned", ft.InfoSchemaStr())

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

	ft = NewFieldType(mysql.TypeBlob)
	ft.SetFlen(10)
	ft.SetCharset("UTF8")
	ft.SetCollate("UTF8_UNICODE_GI")
	require.Equal(t, "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI", ft.String())

	ft = NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(10)
	ft.AddFlag(mysql.BinaryFlag)
	require.Equal(t, "varchar(10) BINARY CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", ft.String())

	ft = NewFieldType(mysql.TypeString)
	ft.SetCharset(charset.CollationBin)
	ft.AddFlag(mysql.BinaryFlag)
	require.Equal(t, "binary(1) COLLATE utf8mb4_bin", ft.String())

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"a", "b"})
	require.Equal(t, "enum('a','b')", ft.String())

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"'a'", "'b'"})
	require.Equal(t, "enum('''a''','''b''')", ft.String())

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"a\nb", "a\tb", "a\rb"})
	require.Equal(t, "enum('a\\nb','a\tb','a\\rb')", ft.String())

	ft = NewFieldType(mysql.TypeEnum)
	ft.SetElems([]string{"a\nb", "a'\t\r\nb", "a\rb"})
	require.Equal(t, "enum('a\\nb','a''	\\r\\nb','a\\rb')", ft.String())

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"a", "b"})
	require.Equal(t, "set('a','b')", ft.String())

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"'a'", "'b'"})
	require.Equal(t, "set('''a''','''b''')", ft.String())

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"a\nb", "a'\t\r\nb", "a\rb"})
	require.Equal(t, "set('a\\nb','a''	\\r\\nb','a\\rb')", ft.String())

	ft = NewFieldType(mysql.TypeSet)
	ft.SetElems([]string{"a'\nb", "a'b\tc"})
	require.Equal(t, "set('a''\\nb','a''b	c')", ft.String())

	ft = NewFieldType(mysql.TypeTimestamp)
	ft.SetFlen(8)
	ft.SetDecimal(2)
	require.Equal(t, "timestamp(2)", ft.String())
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.SetFlen(8)
	ft.SetDecimal(0)
	require.Equal(t, "timestamp", ft.String())

	ft = NewFieldType(mysql.TypeDatetime)
	ft.SetFlen(8)
	ft.SetDecimal(2)
	require.Equal(t, "datetime(2)", ft.String())
	ft = NewFieldType(mysql.TypeDatetime)
	ft.SetFlen(8)
	ft.SetDecimal(0)
	require.Equal(t, "datetime", ft.String())

	ft = NewFieldType(mysql.TypeDate)
	ft.SetFlen(8)
	ft.SetDecimal(2)
	require.Equal(t, "date", ft.String())
	ft = NewFieldType(mysql.TypeDate)
	ft.SetFlen(8)
	ft.SetDecimal(0)
	require.Equal(t, "date", ft.String())

	ft = NewFieldType(mysql.TypeYear)
	ft.SetFlen(4)
	ft.SetDecimal(0)
	require.Equal(t, "year(4)", ft.String())
	ft = NewFieldType(mysql.TypeYear)
	ft.SetFlen(2)
	ft.SetDecimal(2)
	require.Equal(t, "year(2)", ft.String()) // Note: Invalid year.
}

func TestDefaultTypeForValue(t *testing.T) {
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
		{1, mysql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{0, mysql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{432, mysql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{4321, mysql.TypeLonglong, 4, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{1234567, mysql.TypeLonglong, 7, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{12345678, mysql.TypeLonglong, 8, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{12345678901234567, mysql.TypeLonglong, 17, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{-42, mysql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{uint64(1), mysql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{uint64(123), mysql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{uint64(1234), mysql.TypeLonglong, 4, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{uint64(1234567), mysql.TypeLonglong, 7, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{uint64(12345678), mysql.TypeLonglong, 8, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{uint64(12345678901234567), mysql.TypeLonglong, 17, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{"abc", mysql.TypeVarString, 3, UnspecifiedLength, charset.CharsetUTF8MB4, charset.CollationUTF8MB4, mysql.NotNullFlag},
		{1.1, mysql.TypeDouble, 3, -1, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{[]byte("abc"), mysql.TypeBlob, 3, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{HexLiteral{}, mysql.TypeVarString, 0, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag | mysql.NotNullFlag},
		{BitLiteral{}, mysql.TypeVarString, 0, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{NewTime(ZeroCoreTime, mysql.TypeDatetime, DefaultFsp), mysql.TypeDatetime, 19, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{NewTime(FromDate(2017, 12, 12, 12, 59, 59, 0), mysql.TypeDatetime, 3), mysql.TypeDatetime, 23, 3, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{Duration{}, mysql.TypeDuration, 8, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{&MyDecimal{}, mysql.TypeNewDecimal, 2, 0, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{Enum{Name: "a", Value: 1}, mysql.TypeEnum, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
		{Set{Name: "a", Value: 1}, mysql.TypeSet, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, mysql.BinaryFlag | mysql.NotNullFlag},
	}

	for i, tt := range tests {
		var ft FieldType
		DefaultTypeForValue(tt.value, &ft, mysql.DefaultCharset, mysql.DefaultCollationName)
		require.Equalf(t, tt.tp, ft.GetType(), "%v %v %v", i, ft.GetType(), tt.tp)
		require.Equalf(t, tt.flen, ft.GetFlen(), "%v %v %v", i, ft.GetFlen(), tt.flen)
		require.Equalf(t, tt.charset, ft.GetCharset(), "%v %v %v", i, ft.GetCharset(), tt.charset)
		require.Equalf(t, tt.decimal, ft.GetDecimal(), "%v %v %v", i, ft.GetDecimal(), tt.decimal)
		require.Equalf(t, tt.collation, ft.GetCollate(), "%v %v %v", i, ft.GetCollate(), tt.collation)
		require.Equalf(t, tt.flag, ft.GetFlag(), "%v %v %v", i, ft.GetFlag(), tt.flag)
	}
}

func TestAggFieldType(t *testing.T) {
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
		require.Equal(t, fts[i].GetType(), aggTp.GetType())

		aggTp = AggFieldType([]*FieldType{fts[i], fts[i]})
		switch fts[i].GetType() {
		case mysql.TypeDate:
			require.Equal(t, mysql.TypeDate, aggTp.GetType())
		case mysql.TypeJSON:
			require.Equal(t, mysql.TypeJSON, aggTp.GetType())
		case mysql.TypeEnum, mysql.TypeSet, mysql.TypeVarString:
			require.Equal(t, mysql.TypeVarchar, aggTp.GetType())
		case mysql.TypeUnspecified:
			require.Equal(t, mysql.TypeNewDecimal, aggTp.GetType())
		default:
			require.Equal(t, fts[i].GetType(), aggTp.GetType())
		}

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(mysql.TypeLong)})
		switch fts[i].GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong,
			mysql.TypeYear, mysql.TypeInt24, mysql.TypeNull:
			require.Equal(t, mysql.TypeLong, aggTp.GetType())
		case mysql.TypeLonglong:
			require.Equal(t, mysql.TypeLonglong, aggTp.GetType())
		case mysql.TypeFloat, mysql.TypeDouble:
			require.Equal(t, mysql.TypeDouble, aggTp.GetType())
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration,
			mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet,
			mysql.TypeVarString, mysql.TypeGeometry:
			require.Equal(t, mysql.TypeVarchar, aggTp.GetType())
		case mysql.TypeBit:
			require.Equal(t, mysql.TypeLonglong, aggTp.GetType())
		case mysql.TypeString:
			require.Equal(t, mysql.TypeString, aggTp.GetType())
		case mysql.TypeUnspecified, mysql.TypeNewDecimal:
			require.Equal(t, mysql.TypeNewDecimal, aggTp.GetType())
		case mysql.TypeTinyBlob:
			require.Equal(t, mysql.TypeTinyBlob, aggTp.GetType())
		case mysql.TypeBlob:
			require.Equal(t, mysql.TypeBlob, aggTp.GetType())
		case mysql.TypeMediumBlob:
			require.Equal(t, mysql.TypeMediumBlob, aggTp.GetType())
		case mysql.TypeLongBlob:
			require.Equal(t, mysql.TypeLongBlob, aggTp.GetType())
		}

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(mysql.TypeJSON)})
		switch fts[i].GetType() {
		case mysql.TypeJSON, mysql.TypeNull:
			require.Equal(t, mysql.TypeJSON, aggTp.GetType())
		case mysql.TypeLongBlob, mysql.TypeMediumBlob, mysql.TypeTinyBlob, mysql.TypeBlob:
			require.Equal(t, mysql.TypeLongBlob, aggTp.GetType())
		case mysql.TypeString:
			require.Equal(t, mysql.TypeString, aggTp.GetType())
		default:
			require.Equal(t, mysql.TypeVarchar, aggTp.GetType())
		}
	}
}

func TestAggFieldTypeForTypeFlag(t *testing.T) {
	types := []*FieldType{
		NewFieldType(mysql.TypeLonglong),
		NewFieldType(mysql.TypeLonglong),
	}

	aggTp := AggFieldType(types)
	require.Equal(t, mysql.TypeLonglong, aggTp.GetType())
	require.Equal(t, uint(0), aggTp.GetFlag())

	types[0].SetFlag(mysql.NotNullFlag)
	aggTp = AggFieldType(types)
	require.Equal(t, mysql.TypeLonglong, aggTp.GetType())
	require.Equal(t, uint(0), aggTp.GetFlag())

	types[0].SetFlag(0)
	types[1].SetFlag(mysql.NotNullFlag)
	aggTp = AggFieldType(types)
	require.Equal(t, mysql.TypeLonglong, aggTp.GetType())
	require.Equal(t, uint(0), aggTp.GetFlag())

	types[0].SetFlag(mysql.NotNullFlag)
	aggTp = AggFieldType(types)
	require.Equal(t, mysql.TypeLonglong, aggTp.GetType())
	require.Equal(t, mysql.NotNullFlag, aggTp.GetFlag())
}

func TestAggFieldTypeForIntegralPromotion(t *testing.T) {
	fts := []*FieldType{
		NewFieldType(mysql.TypeTiny),
		NewFieldType(mysql.TypeShort),
		NewFieldType(mysql.TypeInt24),
		NewFieldType(mysql.TypeLong),
		NewFieldType(mysql.TypeLonglong),
		NewFieldType(mysql.TypeNewDecimal),
	}

	for i := 1; i < len(fts)-1; i++ {
		tps := fts[i-1 : i+1]

		tps[0].SetFlag(0)
		tps[1].SetFlag(0)
		aggTp := AggFieldType(tps)
		require.Equal(t, fts[i].GetType(), aggTp.GetType())
		require.Equal(t, uint(0), aggTp.GetFlag())

		tps[0].SetFlag(mysql.UnsignedFlag)
		aggTp = AggFieldType(tps)
		require.Equal(t, fts[i].GetType(), aggTp.GetType())
		require.Equal(t, uint(0), aggTp.GetFlag())

		tps[0].SetFlag(mysql.UnsignedFlag)
		tps[1].SetFlag(mysql.UnsignedFlag)
		aggTp = AggFieldType(tps)
		require.Equal(t, fts[i].GetType(), aggTp.GetType())
		require.Equal(t, mysql.UnsignedFlag, aggTp.GetFlag())

		tps[0].SetFlag(0)
		tps[1].SetFlag(mysql.UnsignedFlag)
		aggTp = AggFieldType(tps)
		require.Equal(t, fts[i+1].GetType(), aggTp.GetType())
		require.Equal(t, uint(0), aggTp.GetFlag())
	}
}

func TestAggregateEvalType(t *testing.T) {
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
		switch fts[i].GetType() {
		case mysql.TypeUnspecified, mysql.TypeNull, mysql.TypeTimestamp, mysql.TypeDate,
			mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			require.True(t, aggregatedEvalType.IsStringKind())
			require.Equal(t, uint(0), flag)
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeBit,
			mysql.TypeInt24, mysql.TypeYear:
			require.Equal(t, ETInt, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		case mysql.TypeFloat, mysql.TypeDouble:
			require.Equal(t, ETReal, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		case mysql.TypeNewDecimal:
			require.Equal(t, ETDecimal, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		}

		flag = 0
		aggregatedEvalType = AggregateEvalType([]*FieldType{fts[i], fts[i]}, &flag)
		switch fts[i].GetType() {
		case mysql.TypeUnspecified, mysql.TypeNull, mysql.TypeTimestamp, mysql.TypeDate,
			mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar,
			mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
			require.True(t, aggregatedEvalType.IsStringKind())
			require.Equal(t, uint(0), flag)
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeBit,
			mysql.TypeInt24, mysql.TypeYear:
			require.Equal(t, ETInt, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		case mysql.TypeFloat, mysql.TypeDouble:
			require.Equal(t, ETReal, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		case mysql.TypeNewDecimal:
			require.Equal(t, ETDecimal, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		}
		flag = 0
		aggregatedEvalType = AggregateEvalType([]*FieldType{fts[i], NewFieldType(mysql.TypeLong)}, &flag)
		switch fts[i].GetType() {
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration,
			mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeVarchar, mysql.TypeJSON,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob, mysql.TypeMediumBlob,
			mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString,
			mysql.TypeString, mysql.TypeGeometry:
			require.True(t, aggregatedEvalType.IsStringKind())
			require.Equal(t, uint(0), flag)
		case mysql.TypeUnspecified, mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeNull, mysql.TypeBit,
			mysql.TypeLonglong, mysql.TypeYear, mysql.TypeInt24:
			require.Equal(t, ETInt, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		case mysql.TypeFloat, mysql.TypeDouble:
			require.Equal(t, ETReal, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		case mysql.TypeNewDecimal:
			require.Equal(t, ETDecimal, aggregatedEvalType)
			require.Equal(t, mysql.BinaryFlag, flag)
		}
	}
}
