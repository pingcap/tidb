// Copyright 2026 PingCAP, Inc.
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

package textrow_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

// appendValue is a tiny helper: build a one-column row from a datum and format it.
func appendValue(t *testing.T, col textrow.ColumnInfo, enc *textrow.ResultEncoder, d types.Datum) []byte {
	row := chunk.MutRowFromDatums([]types.Datum{d}).ToRow()
	got, err := textrow.FormatValueText(row, 0, col, enc)
	require.NoError(t, err)
	return got
}

// TestFormatValueText asserts the per-value text bytes (the value that sits
// inside DumpTextRow's length-encoding) match the proven server output for each
// type. Expected values mirror server/internal/column TestDumpTextValue.
func TestFormatValueText(t *testing.T) {
	utf8 := textrow.NewResultEncoder(charset.CharsetUTF8MB4)

	// signed / unsigned integer
	require.Equal(t, "10", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeLonglong, Decimal: mysql.NotFixedDec}, utf8, types.NewIntDatum(10))))
	require.Equal(t, "11", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeLonglong, Flag: uint16(mysql.UnsignedFlag)}, utf8, types.NewUintDatum(11))))

	// float / double precision is applied only when Table is empty
	f32 := types.NewFloat32Datum(1.2)
	require.Equal(t, "1.2", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeFloat, Decimal: 1}, utf8, f32)))
	require.Equal(t, "1.20", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeFloat, Decimal: 2}, utf8, f32)))
	f64 := types.NewFloat64Datum(2.2)
	require.Equal(t, "2.2", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeDouble, Decimal: 1}, utf8, f64)))
	require.Equal(t, "2.20", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeDouble, Decimal: 2}, utf8, f64)))
	// a non-empty Table disables the precision override (full precision kept)
	require.Equal(t, "2.2", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeDouble, Decimal: 2, Table: "t"}, utf8, f64)))

	// strings / blobs
	require.Equal(t, "foo", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeBlob}, utf8, types.NewBytesDatum([]byte("foo")))))
	require.Equal(t, "bar", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeVarchar}, utf8, types.NewStringDatum("bar"))))

	// charset conversion via the result encoder ("一" -> gbk bytes)
	gbk := textrow.NewResultEncoder("gbk")
	require.Equal(t, []byte{0xd2, 0xbb}, appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeVarchar}, gbk, types.NewStringDatum("一")))

	// datetime / duration / decimal
	losAngelesTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	typeCtx := types.NewContext(types.StrictFlags.WithIgnoreZeroInDate(true), losAngelesTz, contextutil.IgnoreWarn)
	tm, err := types.ParseTime(typeCtx, "2017-01-05 23:59:59.575601", mysql.TypeDatetime, 0)
	require.NoError(t, err)
	var d types.Datum
	d.SetMysqlTime(tm)
	require.Equal(t, "2017-01-06 00:00:00", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeDatetime}, utf8, d)))

	duration, _, err := types.ParseDuration(typeCtx, "11:30:45", 0)
	require.NoError(t, err)
	d.SetMysqlDuration(duration)
	require.Equal(t, "11:30:45", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeDuration, Decimal: 0}, utf8, d)))

	d.SetMysqlDecimal(types.NewDecFromStringForTest("1.23"))
	require.Equal(t, "1.23", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeNewDecimal}, utf8, d)))

	// year keeps the 4-digit zero form
	require.Equal(t, "0000", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeYear}, utf8, types.NewIntDatum(0))))
	require.Equal(t, "1984", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeYear}, utf8, types.NewIntDatum(1984))))

	// enum / set / json
	require.Equal(t, "ename", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeEnum}, utf8, types.NewMysqlEnumDatum(types.Enum{Name: "ename", Value: 0}))))
	set := types.Datum{}
	set.SetMysqlSet(types.Set{Name: "sname", Value: 0}, mysql.DefaultCollationName)
	require.Equal(t, "sname", string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeSet}, utf8, set)))
	js := types.Datum{}
	binaryJSON, err := types.ParseBinaryJSONFromString(`{"a": 1, "b": 2}`)
	require.NoError(t, err)
	js.SetMysqlJSON(binaryJSON)
	require.Equal(t, `{"a": 1, "b": 2}`, string(appendValue(t,
		textrow.ColumnInfo{Type: mysql.TypeJSON}, utf8, js)))
}

func TestFormatValueTextInvalidType(t *testing.T) {
	utf8 := textrow.NewResultEncoder(charset.CharsetUTF8MB4)
	row := chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}).ToRow()
	// TypeGeometry is a supported string-like type in the spatial POC, so use
	// TypeUnspecified as the unsupported type.
	_, err := textrow.FormatValueText(row, 0, textrow.ColumnInfo{Type: mysql.TypeUnspecified}, utf8)
	require.Error(t, err)
}

func TestAppendFormatFloat(t *testing.T) {
	infVal, _ := strconv.ParseFloat("+Inf", 64)
	tests := []struct {
		fVal    float64
		out     string
		prec    int
		bitSize int
	}{
		{
			99999999999999999999,
			"1e20",
			-1,
			64,
		},
		{
			1e15,
			"1e15",
			-1,
			64,
		},
		{
			9e14,
			"900000000000000",
			-1,
			64,
		},
		{
			-9999999999999999,
			"-1e16",
			-1,
			64,
		},
		{
			999999999999999,
			"999999999999999",
			-1,
			64,
		},
		{
			0.000000000000001,
			"0.000000000000001",
			-1,
			64,
		},
		{
			0.0000000000000009,
			"9e-16",
			-1,
			64,
		},
		{
			-0.0000000000000009,
			"-9e-16",
			-1,
			64,
		},
		{
			0.11111,
			"0.111",
			3,
			64,
		},
		{
			0.11111,
			"0.111",
			3,
			64,
		},
		{
			0.1111111111111111111,
			"0.11111111",
			-1,
			32,
		},
		{
			0.1111111111111111111,
			"0.1111111111111111",
			-1,
			64,
		},
		{
			0.0000000000000009,
			"9e-16",
			3,
			64,
		},
		{
			0,
			"0",
			-1,
			64,
		},
		{
			-340282346638528860000000000000000000000,
			"-3.40282e38",
			-1,
			32,
		},
		{
			-34028236,
			"-34028236.00",
			2,
			32,
		},
		{
			-17976921.34,
			"-17976921.34",
			2,
			64,
		},
		{
			-3.402823466e+38,
			"-3.40282e38",
			-1,
			32,
		},
		{
			-1.7976931348623157e308,
			"-1.7976931348623157e308",
			-1,
			64,
		},
		{
			10.0e20,
			"1e21",
			-1,
			32,
		},
		{
			1e20,
			"1e20",
			-1,
			32,
		},
		{
			10.0,
			"10",
			-1,
			32,
		},
		{
			999999986991104,
			"1e15",
			-1,
			32,
		},
		{
			1e15,
			"1e15",
			-1,
			32,
		},
		{
			infVal,
			"0",
			-1,
			64,
		},
		{
			-infVal,
			"0",
			-1,
			64,
		},
		{
			1e14,
			"100000000000000",
			-1,
			64,
		},
		{
			1e308,
			"1e308",
			-1,
			64,
		},
	}
	for _, tc := range tests {
		require.Equal(t, tc.out, string(textrow.AppendFormatFloat(nil, tc.fVal, tc.prec, tc.bitSize)))
	}
}
