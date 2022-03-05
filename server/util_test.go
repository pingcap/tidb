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

package server

import (
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDumpBinaryTime(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	parsedTime, err := types.ParseTimestamp(sc, "0000-00-00 00:00:00.000000")
	require.NoError(t, err)
	d := dumpBinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)

	parsedTime, err = types.ParseTimestamp(&stmtctx.StatementContext{TimeZone: time.Local}, "1991-05-01 01:01:01.100001")
	require.NoError(t, err)
	d = dumpBinaryDateTime(nil, parsedTime)
	// 199 & 7 composed to uint16 1991 (litter-endian)
	// 160 & 134 & 1 & 0 composed to uint32 1000001 (litter-endian)
	require.Equal(t, []byte{11, 199, 7, 5, 1, 1, 1, 1, 161, 134, 1, 0}, d)

	parsedTime, err = types.ParseDatetime(sc, "0000-00-00 00:00:00.000000")
	require.NoError(t, err)
	d = dumpBinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)

	parsedTime, err = types.ParseDatetime(sc, "1993-07-13 01:01:01.000000")
	require.NoError(t, err)
	d = dumpBinaryDateTime(nil, parsedTime)
	// 201 & 7 composed to uint16 1993 (litter-endian)
	require.Equal(t, []byte{7, 201, 7, 7, 13, 1, 1, 1}, d)

	parsedTime, err = types.ParseDate(sc, "0000-00-00")
	require.NoError(t, err)
	d = dumpBinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)
	parsedTime, err = types.ParseDate(sc, "1992-06-01")
	require.NoError(t, err)
	d = dumpBinaryDateTime(nil, parsedTime)
	// 200 & 7 composed to uint16 1992 (litter-endian)
	require.Equal(t, []byte{4, 200, 7, 6, 1}, d)

	parsedTime, err = types.ParseDate(sc, "0000-00-00")
	require.NoError(t, err)
	d = dumpBinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)

	myDuration, err := types.ParseDuration(sc, "0000-00-00 00:00:00.000000", 6)
	require.NoError(t, err)
	d = dumpBinaryTime(myDuration.Duration)
	require.Equal(t, []byte{0}, d)

	d = dumpBinaryTime(0)
	require.Equal(t, []byte{0}, d)

	d = dumpBinaryTime(-1)
	require.Equal(t, []byte{12, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, d)

	d = dumpBinaryTime(time.Nanosecond + 86400*1000*time.Microsecond)
	require.Equal(t, []byte{12, 0, 0, 0, 0, 0, 0, 1, 26, 128, 26, 6, 0}, d)
}

func TestResultEncoder(t *testing.T) {
	// Encode bytes to utf-8.
	d := newResultEncoder("utf-8")
	src := []byte("test_string")
	result := d.encodeMeta(src)
	require.Equal(t, src, result)

	// Encode bytes to GBK.
	d = newResultEncoder("gbk")
	result = d.encodeMeta([]byte("一"))
	require.Equal(t, []byte{0xd2, 0xbb}, result)

	// Encode bytes to binary.
	d = newResultEncoder("binary")
	result = d.encodeMeta([]byte("一"))
	require.Equal(t, "一", string(result))
}

func TestDumpTextValue(t *testing.T) {
	columns := []*ColumnInfo{{
		Type:    mysql.TypeLonglong,
		Decimal: mysql.NotFixedDec,
	}}

	dp := newResultEncoder(charset.CharsetUTF8MB4)
	null := types.NewIntDatum(0)
	null.SetNull()
	bs, err := dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{null}).ToRow(), dp)
	require.NoError(t, err)
	_, isNull, _, err := parseLengthEncodedBytes(bs)
	require.NoError(t, err)
	require.True(t, isNull)

	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(10)}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "10", mustDecodeStr(t, bs))

	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(11)}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "11", mustDecodeStr(t, bs))

	columns[0].Flag |= uint16(mysql.UnsignedFlag)
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(11)}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "11", mustDecodeStr(t, bs))

	columns[0].Type = mysql.TypeFloat
	columns[0].Decimal = 1
	f32 := types.NewFloat32Datum(1.2)
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f32}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1.2", mustDecodeStr(t, bs))

	columns[0].Decimal = 2
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f32}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1.20", mustDecodeStr(t, bs))

	f64 := types.NewFloat64Datum(2.2)
	columns[0].Type = mysql.TypeDouble
	columns[0].Decimal = 1
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f64}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2.2", mustDecodeStr(t, bs))

	columns[0].Decimal = 2
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f64}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2.20", mustDecodeStr(t, bs))

	columns[0].Type = mysql.TypeBlob
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewBytesDatum([]byte("foo"))}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "foo", mustDecodeStr(t, bs))

	columns[0].Type = mysql.TypeVarchar
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("bar")}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "bar", mustDecodeStr(t, bs))

	dp = newResultEncoder("gbk")
	columns[0].Type = mysql.TypeVarchar
	dt := []types.Datum{types.NewStringDatum("一")}
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums(dt).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd2, 0xbb}, []byte(mustDecodeStr(t, bs)))

	columns[0].Charset = uint16(mysql.CharsetNameToID("gbk"))
	dp = newResultEncoder("binary")
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums(dt).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd2, 0xbb}, []byte(mustDecodeStr(t, bs)))

	var d types.Datum

	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	losAngelesTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	sc.TimeZone = losAngelesTz

	time, err := types.ParseTime(sc, "2017-01-05 23:59:59.575601", mysql.TypeDatetime, 0)
	require.NoError(t, err)
	d.SetMysqlTime(time)
	columns[0].Type = mysql.TypeDatetime
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2017-01-06 00:00:00", mustDecodeStr(t, bs))

	duration, err := types.ParseDuration(sc, "11:30:45", 0)
	require.NoError(t, err)
	d.SetMysqlDuration(duration)
	columns[0].Type = mysql.TypeDuration
	columns[0].Decimal = 0
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "11:30:45", mustDecodeStr(t, bs))

	d.SetMysqlDecimal(types.NewDecFromStringForTest("1.23"))
	columns[0].Type = mysql.TypeNewDecimal
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1.23", mustDecodeStr(t, bs))

	year := types.NewIntDatum(0)
	columns[0].Type = mysql.TypeYear
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{year}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "0000", mustDecodeStr(t, bs))

	year.SetInt64(1984)
	columns[0].Type = mysql.TypeYear
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{year}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1984", mustDecodeStr(t, bs))

	enum := types.NewMysqlEnumDatum(types.Enum{Name: "ename", Value: 0})
	columns[0].Type = mysql.TypeEnum
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{enum}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "ename", mustDecodeStr(t, bs))

	set := types.Datum{}
	set.SetMysqlSet(types.Set{Name: "sname", Value: 0}, mysql.DefaultCollationName)
	columns[0].Type = mysql.TypeSet
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{set}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "sname", mustDecodeStr(t, bs))

	js := types.Datum{}
	binaryJSON, err := json.ParseBinaryFromString(`{"a": 1, "b": 2}`)
	require.NoError(t, err)
	js.SetMysqlJSON(binaryJSON)
	columns[0].Type = mysql.TypeJSON
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{js}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, `{"a": 1, "b": 2}`, mustDecodeStr(t, bs))
}

func mustDecodeStr(t *testing.T, b []byte) string {
	str, _, _, err := parseLengthEncodedBytes(b)
	require.NoError(t, err)
	return string(str)
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
		require.Equal(t, tc.out, string(appendFormatFloat(nil, tc.fVal, tc.prec, tc.bitSize)))
	}
}

func TestDumpLengthEncodedInt(t *testing.T) {
	testCases := []struct {
		num    uint64
		buffer []byte
	}{
		{
			uint64(0),
			[]byte{0x00},
		},
		{
			uint64(513),
			[]byte{'\xfc', '\x01', '\x02'},
		},
		{
			uint64(197121),
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
		},
		{
			uint64(578437695752307201),
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
		},
	}
	for _, tc := range testCases {
		b := dumpLengthEncodedInt(nil, tc.num)
		require.Equal(t, tc.buffer, b)
	}
}

func TestParseLengthEncodedInt(t *testing.T) {
	testCases := []struct {
		buffer []byte
		num    uint64
		isNull bool
		n      int
	}{
		{
			[]byte{'\xfb'},
			uint64(0),
			true,
			1,
		},
		{
			[]byte{'\x00'},
			uint64(0),
			false,
			1,
		},
		{
			[]byte{'\xfc', '\x01', '\x02'},
			uint64(513),
			false,
			3,
		},
		{
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
			uint64(197121),
			false,
			4,
		},
		{
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
			uint64(578437695752307201),
			false,
			9,
		},
	}

	for _, tc := range testCases {
		num, isNull, n := parseLengthEncodedInt(tc.buffer)
		require.Equal(t, tc.num, num)
		require.Equal(t, tc.isNull, isNull)
		require.Equal(t, tc.n, n)
		require.Equal(t, tc.n, lengthEncodedIntSize(tc.num))
	}
}

func TestDumpUint(t *testing.T) {
	testCases := []uint64{
		0,
		1,
		1<<64 - 1,
	}
	parseUint64 := func(b []byte) uint64 {
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
			uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 |
			uint64(b[6])<<48 | uint64(b[7])<<56
	}
	for _, tc := range testCases {
		b := dumpUint64(nil, tc)
		require.Len(t, b, 8)
		require.Equal(t, tc, parseUint64(b))
	}
}

func TestParseLengthEncodedBytes(t *testing.T) {
	buffer := []byte{'\xfb'}
	b, isNull, n, err := parseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.True(t, isNull)
	require.Equal(t, 1, n)
	require.NoError(t, err)

	buffer = []byte{0}
	b, isNull, n, err = parseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.False(t, isNull)
	require.Equal(t, 1, n)
	require.NoError(t, err)

	buffer = []byte{'\x01'}
	b, isNull, n, err = parseLengthEncodedBytes(buffer)
	require.Nil(t, b)
	require.False(t, isNull)
	require.Equal(t, 2, n)
	require.Equal(t, "EOF", err.Error())
}

func TestParseNullTermString(t *testing.T) {
	for _, tc := range []struct {
		input  string
		str    string
		remain string
	}{
		{
			"abc\x00def",
			"abc",
			"def",
		},
		{
			"\x00def",
			"",
			"def",
		},
		{
			"def\x00hig\x00k",
			"def",
			"hig\x00k",
		},
		{
			"abcdef",
			"",
			"abcdef",
		},
	} {
		str, remain := parseNullTermString([]byte(tc.input))
		require.Equal(t, tc.str, string(str))
		require.Equal(t, tc.remain, string(remain))
	}
}

func newTestConfig() *config.Config {
	cfg := config.NewConfig()
	cfg.Host = "127.0.0.1"
	cfg.Status.StatusHost = "127.0.0.1"
	cfg.Security.AutoTLS = false
	cfg.Socket = ""
	return cfg
}
