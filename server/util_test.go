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
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server/internal/column"
	"github.com/pingcap/tidb/server/internal/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDumpTextValue(t *testing.T) {
	columns := []*column.Info{{
		Type:    mysql.TypeLonglong,
		Decimal: mysql.NotFixedDec,
	}}

	dp := column.NewResultEncoder(charset.CharsetUTF8MB4)
	null := types.NewIntDatum(0)
	null.SetNull()
	bs, err := dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{null}).ToRow(), dp)
	require.NoError(t, err)
	_, isNull, _, err := util.ParseLengthEncodedBytes(bs)
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

	dp = column.NewResultEncoder("gbk")
	columns[0].Type = mysql.TypeVarchar
	dt := []types.Datum{types.NewStringDatum("一")}
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums(dt).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd2, 0xbb}, []byte(mustDecodeStr(t, bs)))

	columns[0].Charset = uint16(mysql.CharsetNameToID("gbk"))
	dp = column.NewResultEncoder("binary")
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums(dt).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd2, 0xbb}, []byte(mustDecodeStr(t, bs)))

	var d types.Datum

	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	losAngelesTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	sc.TimeZone = losAngelesTz

	time, err := types.ParseTime(sc, "2017-01-05 23:59:59.575601", mysql.TypeDatetime, 0, nil)
	require.NoError(t, err)
	d.SetMysqlTime(time)
	columns[0].Type = mysql.TypeDatetime
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2017-01-06 00:00:00", mustDecodeStr(t, bs))

	duration, _, err := types.ParseDuration(sc, "11:30:45", 0)
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
	binaryJSON, err := types.ParseBinaryJSONFromString(`{"a": 1, "b": 2}`)
	require.NoError(t, err)
	js.SetMysqlJSON(binaryJSON)
	columns[0].Type = mysql.TypeJSON
	bs, err = dumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{js}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, `{"a": 1, "b": 2}`, mustDecodeStr(t, bs))
}

func mustDecodeStr(t *testing.T, b []byte) string {
	str, _, _, err := util.ParseLengthEncodedBytes(b)
	require.NoError(t, err)
	return string(str)
}
