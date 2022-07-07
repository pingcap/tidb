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
	"io"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func testIsTypeBlob(t *testing.T, tp byte, expect bool) {
	v := IsTypeBlob(tp)
	require.Equal(t, expect, v)
}

func testIsTypeChar(t *testing.T, tp byte, expect bool) {
	v := IsTypeChar(tp)
	require.Equal(t, expect, v)
}

func TestIsType(t *testing.T) {
	testIsTypeBlob(t, mysql.TypeTinyBlob, true)
	testIsTypeBlob(t, mysql.TypeMediumBlob, true)
	testIsTypeBlob(t, mysql.TypeBlob, true)
	testIsTypeBlob(t, mysql.TypeLongBlob, true)
	testIsTypeBlob(t, mysql.TypeInt24, false)

	testIsTypeChar(t, mysql.TypeString, true)
	testIsTypeChar(t, mysql.TypeVarchar, true)
	testIsTypeChar(t, mysql.TypeLong, false)
}

func testTypeStr(t *testing.T, tp byte, expect string) {
	v := TypeStr(tp)
	require.Equal(t, expect, v)
}

func testTypeToStr(t *testing.T, tp byte, charset string, expect string) {
	v := TypeToStr(tp, charset)
	require.Equal(t, expect, v)
}

func TestTypeToStr(t *testing.T) {
	testTypeStr(t, mysql.TypeYear, "year")
	testTypeStr(t, 0xdd, "")

	testTypeToStr(t, mysql.TypeBlob, "utf8", "text")
	testTypeToStr(t, mysql.TypeLongBlob, "utf8", "longtext")
	testTypeToStr(t, mysql.TypeTinyBlob, "utf8", "tinytext")
	testTypeToStr(t, mysql.TypeMediumBlob, "utf8", "mediumtext")
	testTypeToStr(t, mysql.TypeVarchar, "binary", "varbinary")
	testTypeToStr(t, mysql.TypeString, "binary", "binary")
	testTypeToStr(t, mysql.TypeTiny, "binary", "tinyint")
	testTypeToStr(t, mysql.TypeBlob, "binary", "blob")
	testTypeToStr(t, mysql.TypeLongBlob, "binary", "longblob")
	testTypeToStr(t, mysql.TypeTinyBlob, "binary", "tinyblob")
	testTypeToStr(t, mysql.TypeMediumBlob, "binary", "mediumblob")
	testTypeToStr(t, mysql.TypeVarchar, "utf8", "varchar")
	testTypeToStr(t, mysql.TypeString, "utf8", "char")
	testTypeToStr(t, mysql.TypeShort, "binary", "smallint")
	testTypeToStr(t, mysql.TypeInt24, "binary", "mediumint")
	testTypeToStr(t, mysql.TypeLong, "binary", "int")
	testTypeToStr(t, mysql.TypeLonglong, "binary", "bigint")
	testTypeToStr(t, mysql.TypeFloat, "binary", "float")
	testTypeToStr(t, mysql.TypeDouble, "binary", "double")
	testTypeToStr(t, mysql.TypeYear, "binary", "year")
	testTypeToStr(t, mysql.TypeDuration, "binary", "time")
	testTypeToStr(t, mysql.TypeDatetime, "binary", "datetime")
	testTypeToStr(t, mysql.TypeDate, "binary", "date")
	testTypeToStr(t, mysql.TypeTimestamp, "binary", "timestamp")
	testTypeToStr(t, mysql.TypeNewDecimal, "binary", "decimal")
	testTypeToStr(t, mysql.TypeUnspecified, "binary", "unspecified")
	testTypeToStr(t, 0xdd, "binary", "")
	testTypeToStr(t, mysql.TypeBit, "binary", "bit")
	testTypeToStr(t, mysql.TypeEnum, "binary", "enum")
	testTypeToStr(t, mysql.TypeSet, "binary", "set")
}

func TestEOFAsNil(t *testing.T) {
	err := EOFAsNil(io.EOF)
	require.NoError(t, err)
	err = EOFAsNil(errors.New("test"))
	require.EqualError(t, err, "test")
}

func TestMaxFloat(t *testing.T) {
	tests := []struct {
		flen    int
		decimal int
		expect  float64
	}{
		{3, 2, 9.99},
		{5, 2, 999.99},
		{10, 1, 999999999.9},
		{5, 5, 0.99999},
	}

	for _, test := range tests {
		require.Equal(t, test.expect, GetMaxFloat(test.flen, test.decimal))
	}
}

func TestRoundFloat(t *testing.T) {
	tests := []struct {
		input  float64
		expect float64
	}{
		{2.5, 2},
		{1.5, 2},
		{0.5, 0},
		{0.49999999999999997, 0},
		{0, 0},
		{-0.49999999999999997, 0},
		{-0.5, 0},
		{-2.5, -2},
		{-1.5, -2},
	}

	for _, test := range tests {
		require.Equal(t, test.expect, RoundFloat(test.input))
	}
}

func TestRound(t *testing.T) {
	tests := []struct {
		input  float64
		dec    int
		expect float64
	}{
		{-1.23, 0, -1},
		{-1.58, 0, -2},
		{1.58, 0, 2},
		{1.298, 1, 1.3},
		{1.298, 0, 1},
		{23.298, -1, 20},
	}

	for _, test := range tests {
		require.Equal(t, test.expect, Round(test.input, test.dec))
	}
}

func TestTruncateFloat(t *testing.T) {
	tests := []struct {
		input   float64
		flen    int
		decimal int
		expect  float64
		err     error
	}{
		{100.114, 10, 2, 100.11, nil},
		{100.115, 10, 2, 100.12, nil},
		{100.1156, 10, 3, 100.116, nil},
		{100.1156, 3, 1, 99.9, ErrOverflow},
		{1.36, 10, 2, 1.36, nil},
	}

	for _, test := range tests {
		f, err := TruncateFloat(test.input, test.flen, test.decimal)
		require.Equal(t, test.expect, f)
		require.Truef(t, terror.ErrorEqual(err, test.err), "err: %v", err)
	}
}

func TestIsTypeTemporal(t *testing.T) {
	res := IsTypeTemporal(mysql.TypeDuration)
	require.True(t, res)
	res = IsTypeTemporal(mysql.TypeDatetime)
	require.True(t, res)
	res = IsTypeTemporal(mysql.TypeTimestamp)
	require.True(t, res)
	res = IsTypeTemporal(mysql.TypeDate)
	require.True(t, res)
	res = IsTypeTemporal(mysql.TypeNewDate)
	require.True(t, res)
	res = IsTypeTemporal('t')
	require.False(t, res)
}

func TestIsBinaryStr(t *testing.T) {
	in := &FieldType{}
	in.SetType(mysql.TypeBit)
	in.SetFlag(mysql.UnsignedFlag)
	in.SetFlen(1)
	in.SetDecimal(0)
	in.SetCharset(charset.CharsetUTF8)
	in.SetCollate(charset.CollationUTF8)

	in.SetCollate(charset.CollationUTF8)
	res := IsBinaryStr(in)
	require.False(t, res)

	in.SetCollate(charset.CollationBin)
	res = IsBinaryStr(in)
	require.False(t, res)

	in.SetType(mysql.TypeBlob)
	res = IsBinaryStr(in)
	require.True(t, res)
}

func TestIsNonBinaryStr(t *testing.T) {
	in := NewFieldType(mysql.TypeBit)
	in.SetFlag(mysql.UnsignedFlag)
	in.SetFlen(1)
	in.SetDecimal(0)
	in.SetCharset(charset.CharsetUTF8)
	in.SetCollate(charset.CollationUTF8)

	in.SetCollate(charset.CollationBin)
	res := IsBinaryStr(in)
	require.False(t, res)

	in.SetCollate(charset.CollationUTF8)
	res = IsBinaryStr(in)
	require.False(t, res)

	in.SetType(mysql.TypeBlob)
	res = IsBinaryStr(in)
	require.False(t, res)
}

func TestIsTemporalWithDate(t *testing.T) {
	res := IsTemporalWithDate(mysql.TypeDatetime)
	require.True(t, res)

	res = IsTemporalWithDate(mysql.TypeDate)
	require.True(t, res)

	res = IsTemporalWithDate(mysql.TypeTimestamp)
	require.True(t, res)

	res = IsTemporalWithDate('t')
	require.False(t, res)
}

func TestIsTypePrefixable(t *testing.T) {
	res := IsTypePrefixable('t')
	require.False(t, res)

	res = IsTypePrefixable(mysql.TypeBlob)
	require.True(t, res)
}

func TestIsTypeFractionable(t *testing.T) {
	res := IsTypeFractionable(mysql.TypeDatetime)
	require.True(t, res)

	res = IsTypeFractionable(mysql.TypeDuration)
	require.True(t, res)

	res = IsTypeFractionable(mysql.TypeTimestamp)
	require.True(t, res)

	res = IsTypeFractionable('t')
	require.False(t, res)
}

func TestIsTypeNumeric(t *testing.T) {
	res := IsTypeNumeric(mysql.TypeBit)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeTiny)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeInt24)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeLong)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeLonglong)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeNewDecimal)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeUnspecified)
	require.False(t, res)

	res = IsTypeNumeric(mysql.TypeFloat)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeDouble)
	require.True(t, res)

	res = IsTypeNumeric(mysql.TypeShort)
	require.True(t, res)

	res = IsTypeNumeric('t')
	require.False(t, res)
}
