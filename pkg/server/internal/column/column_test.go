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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestDumpColumn(t *testing.T) {
	info := Info{
		Schema:       "testSchema",
		Table:        "testTable",
		OrgTable:     "testOrgTable",
		Name:         "testName",
		OrgName:      "testOrgName",
		ColumnLength: 1,
		Charset:      106,
		Flag:         0,
		Decimal:      1,
		Type:         14,
		DefaultValue: []byte{5, 2},
	}
	r := info.Dump(nil, nil)
	exp := []byte{0x3, 0x64, 0x65, 0x66, 0xa, 0x74, 0x65, 0x73, 0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x9, 0x74, 0x65, 0x73, 0x74, 0x54, 0x61, 0x62, 0x6c, 0x65, 0xc, 0x74, 0x65, 0x73, 0x74, 0x4f, 0x72, 0x67, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x8, 0x74, 0x65, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0xb, 0x74, 0x65, 0x73, 0x74, 0x4f, 0x72, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0xc, 0x6a, 0x0, 0x1, 0x0, 0x0, 0x0, 0xe, 0x0, 0x0, 0x1, 0x0, 0x0}
	require.Equal(t, exp, r)

	require.Equal(t, uint16(mysql.SetFlag), DumpFlag(mysql.TypeSet, 0))
	require.Equal(t, uint16(mysql.EnumFlag), DumpFlag(mysql.TypeEnum, 0))
	require.Equal(t, uint16(0), DumpFlag(mysql.TypeString, 0))

	require.Equal(t, mysql.TypeString, dumpType(mysql.TypeSet))
	require.Equal(t, mysql.TypeString, dumpType(mysql.TypeEnum))
	require.Equal(t, mysql.TypeBit, dumpType(mysql.TypeBit))
}

func TestDumpColumnWithDefault(t *testing.T) {
	info := Info{
		Schema:       "testSchema",
		Table:        "testTable",
		OrgTable:     "testOrgTable",
		Name:         "testName",
		OrgName:      "testOrgName",
		ColumnLength: 1,
		Charset:      106,
		Flag:         0,
		Decimal:      1,
		Type:         14,
		DefaultValue: "test",
	}
	r := info.DumpWithDefault(nil, nil)
	exp := []byte{0x3, 0x64, 0x65, 0x66, 0xa, 0x74, 0x65, 0x73, 0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x9, 0x74, 0x65, 0x73, 0x74, 0x54, 0x61, 0x62, 0x6c, 0x65, 0xc, 0x74, 0x65, 0x73, 0x74, 0x4f, 0x72, 0x67, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x8, 0x74, 0x65, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0xb, 0x74, 0x65, 0x73, 0x74, 0x4f, 0x72, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0xc, 0x6a, 0x0, 0x1, 0x0, 0x0, 0x0, 0xe, 0x0, 0x0, 0x1, 0x0, 0x0, 0x4, 0x74, 0x65, 0x73, 0x74}
	require.Equal(t, exp, r)

	require.Equal(t, uint16(mysql.SetFlag), DumpFlag(mysql.TypeSet, 0))
	require.Equal(t, uint16(mysql.EnumFlag), DumpFlag(mysql.TypeEnum, 0))
	require.Equal(t, uint16(0), DumpFlag(mysql.TypeString, 0))

	require.Equal(t, mysql.TypeString, dumpType(mysql.TypeSet))
	require.Equal(t, mysql.TypeString, dumpType(mysql.TypeEnum))
	require.Equal(t, mysql.TypeBit, dumpType(mysql.TypeBit))
}

func TestColumnNameLimit(t *testing.T) {
	aLongName := make([]byte, 0, 300)
	for i := 0; i < 300; i++ {
		aLongName = append(aLongName, 'a')
	}
	info := Info{
		Schema:       "testSchema",
		Table:        "testTable",
		OrgTable:     "testOrgTable",
		Name:         string(aLongName),
		OrgName:      "testOrgName",
		ColumnLength: 1,
		Charset:      106,
		Flag:         0,
		Decimal:      1,
		Type:         14,
		DefaultValue: []byte{5, 2},
	}
	r := info.Dump(nil, nil)
	exp := []byte{0x3, 0x64, 0x65, 0x66, 0xa, 0x74, 0x65, 0x73, 0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x9, 0x74, 0x65, 0x73, 0x74, 0x54, 0x61, 0x62, 0x6c, 0x65, 0xc, 0x74, 0x65, 0x73, 0x74, 0x4f, 0x72, 0x67, 0x54, 0x61, 0x62, 0x6c, 0x65, 0xfc, 0x0, 0x1, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0xb, 0x74, 0x65, 0x73, 0x74, 0x4f, 0x72, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0xc, 0x6a, 0x0, 0x1, 0x0, 0x0, 0x0, 0xe, 0x0, 0x0, 0x1, 0x0, 0x0}
	require.Equal(t, exp, r)
}

func TestDumpTextValue(t *testing.T) {
	columns := []*Info{{
		Type:    mysql.TypeLonglong,
		Decimal: mysql.NotFixedDec,
	}}

	dp := NewResultEncoder(charset.CharsetUTF8MB4)
	null := types.NewIntDatum(0)
	null.SetNull()
	bs, err := DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{null}).ToRow(), dp)
	require.NoError(t, err)
	_, isNull, _, err := util.ParseLengthEncodedBytes(bs)
	require.NoError(t, err)
	require.True(t, isNull)

	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(10)}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "10", mustDecodeStr(t, bs))

	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(11)}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "11", mustDecodeStr(t, bs))

	columns[0].Flag |= uint16(mysql.UnsignedFlag)
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(11)}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "11", mustDecodeStr(t, bs))

	columns[0].Type = mysql.TypeFloat
	columns[0].Decimal = 1
	f32 := types.NewFloat32Datum(1.2)
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f32}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1.2", mustDecodeStr(t, bs))

	columns[0].Decimal = 2
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f32}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1.20", mustDecodeStr(t, bs))

	f64 := types.NewFloat64Datum(2.2)
	columns[0].Type = mysql.TypeDouble
	columns[0].Decimal = 1
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f64}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2.2", mustDecodeStr(t, bs))

	columns[0].Decimal = 2
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{f64}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2.20", mustDecodeStr(t, bs))

	columns[0].Type = mysql.TypeBlob
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewBytesDatum([]byte("foo"))}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "foo", mustDecodeStr(t, bs))

	columns[0].Type = mysql.TypeVarchar
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("bar")}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "bar", mustDecodeStr(t, bs))

	dp = NewResultEncoder("gbk")
	columns[0].Type = mysql.TypeVarchar
	dt := []types.Datum{types.NewStringDatum("一")}
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums(dt).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd2, 0xbb}, []byte(mustDecodeStr(t, bs)))

	columns[0].Charset = uint16(mysql.CharsetNameToID("gbk"))
	dp = NewResultEncoder("binary")
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums(dt).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, []byte{0xd2, 0xbb}, []byte(mustDecodeStr(t, bs)))

	var d types.Datum

	losAngelesTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	typeCtx := types.NewContext(types.StrictFlags.WithIgnoreZeroInDate(true), losAngelesTz, contextutil.IgnoreWarn)

	time, err := types.ParseTime(typeCtx, "2017-01-05 23:59:59.575601", mysql.TypeDatetime, 0)
	require.NoError(t, err)
	d.SetMysqlTime(time)
	columns[0].Type = mysql.TypeDatetime
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "2017-01-06 00:00:00", mustDecodeStr(t, bs))

	duration, _, err := types.ParseDuration(typeCtx, "11:30:45", 0)
	require.NoError(t, err)
	d.SetMysqlDuration(duration)
	columns[0].Type = mysql.TypeDuration
	columns[0].Decimal = 0
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "11:30:45", mustDecodeStr(t, bs))

	d.SetMysqlDecimal(types.NewDecFromStringForTest("1.23"))
	columns[0].Type = mysql.TypeNewDecimal
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{d}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1.23", mustDecodeStr(t, bs))

	year := types.NewIntDatum(0)
	columns[0].Type = mysql.TypeYear
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{year}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "0000", mustDecodeStr(t, bs))

	year.SetInt64(1984)
	columns[0].Type = mysql.TypeYear
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{year}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "1984", mustDecodeStr(t, bs))

	enum := types.NewMysqlEnumDatum(types.Enum{Name: "ename", Value: 0})
	columns[0].Type = mysql.TypeEnum
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{enum}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "ename", mustDecodeStr(t, bs))

	set := types.Datum{}
	set.SetMysqlSet(types.Set{Name: "sname", Value: 0}, mysql.DefaultCollationName)
	columns[0].Type = mysql.TypeSet
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{set}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, "sname", mustDecodeStr(t, bs))

	js := types.Datum{}
	binaryJSON, err := types.ParseBinaryJSONFromString(`{"a": 1, "b": 2}`)
	require.NoError(t, err)
	js.SetMysqlJSON(binaryJSON)
	columns[0].Type = mysql.TypeJSON
	bs, err = DumpTextRow(nil, columns, chunk.MutRowFromDatums([]types.Datum{js}).ToRow(), dp)
	require.NoError(t, err)
	require.Equal(t, `{"a": 1, "b": 2}`, mustDecodeStr(t, bs))
}

func mustDecodeStr(t *testing.T, b []byte) string {
	str, _, _, err := util.ParseLengthEncodedBytes(b)
	require.NoError(t, err)
	return string(str)
}
