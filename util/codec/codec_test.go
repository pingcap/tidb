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

package codec

import (
	"bytes"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestCodecKey(t *testing.T) {
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},

		{
			types.MakeDatums(types.NewBinaryLiteralFromUint(100, -1), types.NewBinaryLiteralFromUint(100, 4)),
			types.MakeDatums(uint64(100), uint64(100)),
		},

		{
			types.MakeDatums(types.Enum{Name: "a", Value: 1}, types.Set{Name: "a", Value: 1}),
			types.MakeDatums(uint64(1), uint64(1)),
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, datums := range table {
		comment := fmt.Sprintf("%d %v", i, datums)
		b, err := EncodeKey(sc, nil, datums.Input...)
		require.NoError(t, err, comment)

		args, err := Decode(b, 1)
		require.NoError(t, err, comment)
		require.Equal(t, datums.Expect, args, comment)

		b, err = EncodeValue(sc, nil, datums.Input...)
		require.NoError(t, err, comment)

		size, err := estimateValuesSize(sc, datums.Input)
		require.NoError(t, err, comment)
		require.Len(t, b, size, comment)

		args, err = Decode(b, 1)
		require.NoError(t, err, comment)
		require.Equal(t, datums.Expect, args, comment)
	}

	var raw types.Datum
	raw.SetRaw([]byte("raw"))
	_, err := EncodeKey(sc, nil, raw)
	require.Error(t, err)
}

func estimateValuesSize(sc *stmtctx.StatementContext, vals []types.Datum) (int, error) {
	size := 0
	for _, val := range vals {
		length, err := EstimateValueSize(sc, val)
		if err != nil {
			return 0, err
		}
		size += length
	}
	return size, nil
}

func TestCodecKeyCompare(t *testing.T) {
	table := []struct {
		Left   []types.Datum
		Right  []types.Datum
		Expect int
	}{
		{
			types.MakeDatums(1),
			types.MakeDatums(1),
			0,
		},
		{
			types.MakeDatums(-1),
			types.MakeDatums(1),
			-1,
		},
		{
			types.MakeDatums(3.15),
			types.MakeDatums(3.12),
			1,
		},
		{
			types.MakeDatums("abc"),
			types.MakeDatums("abcd"),
			-1,
		},
		{
			types.MakeDatums("abcdefgh"),
			types.MakeDatums("abcdefghi"),
			-1,
		},
		{
			types.MakeDatums(1, "abc"),
			types.MakeDatums(1, "abcd"),
			-1,
		},
		{
			types.MakeDatums(1, "abc", "def"),
			types.MakeDatums(1, "abcd", "af"),
			-1,
		},
		{
			types.MakeDatums(3.12, "ebc", "def"),
			types.MakeDatums(2.12, "abcd", "af"),
			1,
		},
		{
			types.MakeDatums([]byte{0x01, 0x00}, []byte{0xFF}),
			types.MakeDatums([]byte{0x01, 0x00, 0xFF}),
			-1,
		},
		{
			types.MakeDatums([]byte{0x01}, uint64(0xFFFFFFFFFFFFFFF)),
			types.MakeDatums([]byte{0x01, 0x10}, 0),
			-1,
		},
		{
			types.MakeDatums(0),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums([]byte{0x00}),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(math.SmallestNonzeroFloat64),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(int64(math.MinInt64)),
			types.MakeDatums(nil),
			1,
		},
		{
			types.MakeDatums(1, int64(math.MinInt64), nil),
			types.MakeDatums(1, nil, uint64(math.MaxUint64)),
			1,
		},
		{
			types.MakeDatums(1, []byte{}, nil),
			types.MakeDatums(1, nil, 123),
			1,
		},
		{
			types.MakeDatums(parseTime(t, "2011-11-11 00:00:00"), 1),
			types.MakeDatums(parseTime(t, "2011-11-11 00:00:00"), 0),
			1,
		},
		{
			types.MakeDatums(parseDuration(t, "00:00:00"), 1),
			types.MakeDatums(parseDuration(t, "00:00:01"), 0),
			-1,
		},
		{
			[]types.Datum{types.MinNotNullDatum()},
			[]types.Datum{types.MaxValueDatum()},
			-1,
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, datums := range table {
		b1, err := EncodeKey(sc, nil, datums.Left...)
		require.NoError(t, err)

		b2, err := EncodeKey(sc, nil, datums.Right...)
		require.NoError(t, err)

		comparedRes := bytes.Compare(b1, b2)
		require.Equalf(t, datums.Expect, comparedRes, "%v - %v - %v - %v - %v", datums.Left, datums.Right, b1, b2, datums.Expect)
	}
}

func TestNumberCodec(t *testing.T) {
	tblInt64 := []int64{
		math.MinInt64,
		math.MinInt32,
		math.MinInt16,
		math.MinInt8,
		0,
		math.MaxInt8,
		math.MaxInt16,
		math.MaxInt32,
		math.MaxInt64,
		1<<47 - 1,
		-1 << 47,
		1<<23 - 1,
		-1 << 23,
		1<<33 - 1,
		-1 << 33,
		1<<55 - 1,
		-1 << 55,
		1,
		-1,
	}

	for _, intNum := range tblInt64 {
		b := EncodeInt(nil, intNum)
		_, v, err := DecodeInt(b)
		require.NoError(t, err)
		require.Equal(t, intNum, v)

		b = EncodeIntDesc(nil, intNum)
		_, v, err = DecodeIntDesc(b)
		require.NoError(t, err)
		require.Equal(t, intNum, v)

		b = EncodeVarint(nil, intNum)
		_, v, err = DecodeVarint(b)
		require.NoError(t, err)
		require.Equal(t, intNum, v)

		b = EncodeComparableVarint(nil, intNum)
		_, v, err = DecodeComparableVarint(b)
		require.NoError(t, err)
		require.Equal(t, intNum, v)
	}

	tblUint64 := []uint64{
		0,
		math.MaxUint8,
		math.MaxUint16,
		math.MaxUint32,
		math.MaxUint64,
		1<<24 - 1,
		1<<48 - 1,
		1<<56 - 1,
		1,
		math.MaxInt16,
		math.MaxInt8,
		math.MaxInt32,
		math.MaxInt64,
	}

	for _, uintNum := range tblUint64 {
		b := EncodeUint(nil, uintNum)
		_, v, err := DecodeUint(b)
		require.NoError(t, err)
		require.Equal(t, uintNum, v)

		b = EncodeUintDesc(nil, uintNum)
		_, v, err = DecodeUintDesc(b)
		require.NoError(t, err)
		require.Equal(t, uintNum, v)

		b = EncodeUvarint(nil, uintNum)
		_, v, err = DecodeUvarint(b)
		require.NoError(t, err)
		require.Equal(t, uintNum, v)

		b = EncodeComparableUvarint(nil, uintNum)
		_, v, err = DecodeComparableUvarint(b)
		require.NoError(t, err)
		require.Equal(t, uintNum, v)
	}

	var b []byte
	b = EncodeComparableVarint(b, -1)
	b = EncodeComparableUvarint(b, 1)
	b = EncodeComparableVarint(b, 2)
	b, i, err := DecodeComparableVarint(b)
	require.NoError(t, err)
	require.Equal(t, int64(-1), i)

	b, u, err := DecodeComparableUvarint(b)
	require.NoError(t, err)
	require.Equal(t, uint64(1), u)

	_, i, err = DecodeComparableVarint(b)
	require.NoError(t, err)
	require.Equal(t, int64(2), i)
}

func TestNumberOrder(t *testing.T) {
	tblInt64 := []struct {
		Arg1 int64
		Arg2 int64
		Ret  int
	}{
		{-1, 1, -1},
		{math.MaxInt64, math.MinInt64, 1},
		{math.MaxInt64, math.MaxInt32, 1},
		{math.MinInt32, math.MaxInt16, -1},
		{math.MinInt64, math.MaxInt8, -1},
		{0, math.MaxInt8, -1},
		{math.MinInt8, 0, -1},
		{math.MinInt16, math.MaxInt16, -1},
		{1, -1, 1},
		{1, 0, 1},
		{-1, 0, -1},
		{0, 0, 0},
		{math.MaxInt16, math.MaxInt16, 0},
	}

	for _, intNums := range tblInt64 {
		b1 := EncodeInt(nil, intNums.Arg1)
		b2 := EncodeInt(nil, intNums.Arg2)
		require.Equal(t, intNums.Ret, bytes.Compare(b1, b2))

		b1 = EncodeIntDesc(nil, intNums.Arg1)
		b2 = EncodeIntDesc(nil, intNums.Arg2)
		require.Equal(t, -intNums.Ret, bytes.Compare(b1, b2))

		b1 = EncodeComparableVarint(nil, intNums.Arg1)
		b2 = EncodeComparableVarint(nil, intNums.Arg2)
		require.Equal(t, intNums.Ret, bytes.Compare(b1, b2))
	}

	tblUint64 := []struct {
		Arg1 uint64
		Arg2 uint64
		Ret  int
	}{
		{0, 0, 0},
		{1, 0, 1},
		{0, 1, -1},
		{math.MaxInt8, math.MaxInt16, -1},
		{math.MaxUint32, math.MaxInt32, 1},
		{math.MaxUint8, math.MaxInt8, 1},
		{math.MaxUint16, math.MaxInt32, -1},
		{math.MaxUint64, math.MaxInt64, 1},
		{math.MaxInt64, math.MaxUint32, 1},
		{math.MaxUint64, 0, 1},
		{0, math.MaxUint64, -1},
	}

	for _, uintNums := range tblUint64 {
		b1 := EncodeUint(nil, uintNums.Arg1)
		b2 := EncodeUint(nil, uintNums.Arg2)
		require.Equal(t, uintNums.Ret, bytes.Compare(b1, b2))

		b1 = EncodeUintDesc(nil, uintNums.Arg1)
		b2 = EncodeUintDesc(nil, uintNums.Arg2)
		require.Equal(t, -uintNums.Ret, bytes.Compare(b1, b2))

		b1 = EncodeComparableUvarint(nil, uintNums.Arg1)
		b2 = EncodeComparableUvarint(nil, uintNums.Arg2)
		require.Equal(t, uintNums.Ret, bytes.Compare(b1, b2))
	}
}

func TestFloatCodec(t *testing.T) {
	tblFloat := []float64{
		-1,
		0,
		1,
		math.MaxFloat64,
		math.MaxFloat32,
		math.SmallestNonzeroFloat32,
		math.SmallestNonzeroFloat64,
		math.Inf(-1),
		math.Inf(1),
	}

	for _, floatNum := range tblFloat {
		b := EncodeFloat(nil, floatNum)
		_, v, err := DecodeFloat(b)
		require.NoError(t, err)
		require.Equal(t, floatNum, v)

		b = EncodeFloatDesc(nil, floatNum)
		_, v, err = DecodeFloatDesc(b)
		require.NoError(t, err)
		require.Equal(t, floatNum, v)
	}

	tblCmp := []struct {
		Arg1 float64
		Arg2 float64
		Ret  int
	}{
		{1, -1, 1},
		{1, 0, 1},
		{0, -1, 1},
		{0, 0, 0},
		{math.MaxFloat64, 1, 1},
		{math.MaxFloat32, math.MaxFloat64, -1},
		{math.MaxFloat64, 0, 1},
		{math.MaxFloat64, math.SmallestNonzeroFloat64, 1},
		{math.Inf(-1), 0, -1},
		{math.Inf(1), 0, 1},
		{math.Inf(-1), math.Inf(1), -1},
	}

	for _, floatNums := range tblCmp {
		b1 := EncodeFloat(nil, floatNums.Arg1)
		b2 := EncodeFloat(nil, floatNums.Arg2)

		ret := bytes.Compare(b1, b2)
		require.Equal(t, floatNums.Ret, ret)

		b1 = EncodeFloatDesc(nil, floatNums.Arg1)
		b2 = EncodeFloatDesc(nil, floatNums.Arg2)

		ret = bytes.Compare(b1, b2)
		require.Equal(t, -floatNums.Ret, ret)
	}
}

func TestBytes(t *testing.T) {
	tblBytes := [][]byte{
		{},
		{0x00, 0x01},
		{0xff, 0xff},
		{0x01, 0x00},
		[]byte("abc"),
		[]byte("hello world"),
	}

	for _, bytesDatum := range tblBytes {
		b := EncodeBytes(nil, bytesDatum)
		_, v, err := DecodeBytes(b, nil)
		require.NoError(t, err)
		require.Equalf(t, bytesDatum, v, "%v - %v - %v", bytesDatum, b, v)

		b = EncodeBytesDesc(nil, bytesDatum)
		_, v, err = DecodeBytesDesc(b, nil)
		require.NoError(t, err)
		require.Equalf(t, bytesDatum, v, "%v - %v - %v", bytesDatum, b, v)

		b = EncodeCompactBytes(nil, bytesDatum)
		_, v, err = DecodeCompactBytes(b)
		require.NoError(t, err)
		require.Equal(t, bytesDatum, v, "%v - %v - %v", bytesDatum, b, v)
	}

	tblCmp := []struct {
		Arg1 []byte
		Arg2 []byte
		Ret  int
	}{
		{[]byte{}, []byte{0x00}, -1},
		{[]byte{0x00}, []byte{0x00}, 0},
		{[]byte{0xFF}, []byte{0x00}, 1},
		{[]byte{0xFF}, []byte{0xFF, 0x00}, -1},
		{[]byte("a"), []byte("b"), -1},
		{[]byte("a"), []byte{0x00}, 1},
		{[]byte{0x00}, []byte{0x01}, -1},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00}, []byte{0x00, 0x00}, 1},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x00}, []byte{0x01, 0x02, 0x03}, 1},
		{[]byte{0x01, 0x03, 0x03, 0x04}, []byte{0x01, 0x03, 0x03, 0x05}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, -1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00}, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 1},
	}

	for _, bytesData := range tblCmp {
		b1 := EncodeBytes(nil, bytesData.Arg1)
		b2 := EncodeBytes(nil, bytesData.Arg2)

		ret := bytes.Compare(b1, b2)
		require.Equal(t, bytesData.Ret, ret)

		b1 = EncodeBytesDesc(nil, bytesData.Arg1)
		b2 = EncodeBytesDesc(nil, bytesData.Arg2)

		ret = bytes.Compare(b1, b2)
		require.Equal(t, -bytesData.Ret, ret)
	}
}

func parseTime(t *testing.T, s string) types.Time {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	m, err := types.ParseTime(sc, s, mysql.TypeDatetime, types.DefaultFsp)
	require.NoError(t, err)
	return m
}

func parseDuration(t *testing.T, s string) types.Duration {
	m, err := types.ParseDuration(nil, s, types.DefaultFsp)
	require.NoError(t, err)
	return m
}

func TestTime(t *testing.T) {
	tbl := []string{
		"2011-01-01 00:00:00",
		"2011-01-01 00:00:00",
		"0001-01-01 00:00:00",
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, timeDatum := range tbl {
		m := types.NewDatum(parseTime(t, timeDatum))

		b, err := EncodeKey(sc, nil, m)
		require.NoError(t, err)
		v, err := Decode(b, 1)
		require.NoError(t, err)

		var rawTime types.Time
		rawTime.SetType(mysql.TypeDatetime)
		err = rawTime.FromPackedUint(v[0].GetUint64())
		require.NoError(t, err)

		require.Equal(t, m, types.NewDatum(rawTime))
	}

	tblCmp := []struct {
		Arg1 string
		Arg2 string
		Ret  int
	}{
		{"2011-10-10 00:00:00", "2000-12-12 11:11:11", 1},
		{"2000-10-10 00:00:00", "2001-10-10 00:00:00", -1},
		{"2000-10-10 00:00:00", "2000-10-10 00:00:00", 0},
	}

	for _, timeData := range tblCmp {
		m1 := types.NewDatum(parseTime(t, timeData.Arg1))
		m2 := types.NewDatum(parseTime(t, timeData.Arg2))

		b1, err := EncodeKey(sc, nil, m1)
		require.NoError(t, err)
		b2, err := EncodeKey(sc, nil, m2)
		require.NoError(t, err)

		ret := bytes.Compare(b1, b2)
		require.Equal(t, timeData.Ret, ret)
	}
}

func TestDuration(t *testing.T) {
	tbl := []string{
		"11:11:11",
		"00:00:00",
		"1 11:11:11",
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, duration := range tbl {
		m := parseDuration(t, duration)

		b, err := EncodeKey(sc, nil, types.NewDatum(m))
		require.NoError(t, err)
		v, err := Decode(b, 1)
		require.NoError(t, err)
		m.Fsp = types.MaxFsp
		require.Equal(t, types.MakeDatums(m), v)
	}

	tblCmp := []struct {
		Arg1 string
		Arg2 string
		Ret  int
	}{
		{"20:00:00", "11:11:11", 1},
		{"00:00:00", "00:00:01", -1},
		{"00:00:00", "00:00:00", 0},
	}

	for _, durations := range tblCmp {
		m1 := parseDuration(t, durations.Arg1)
		m2 := parseDuration(t, durations.Arg2)

		b1, err := EncodeKey(sc, nil, types.NewDatum(m1))
		require.NoError(t, err)
		b2, err := EncodeKey(sc, nil, types.NewDatum(m2))
		require.NoError(t, err)

		ret := bytes.Compare(b1, b2)
		require.Equal(t, durations.Ret, ret)
	}
}

func TestDecimal(t *testing.T) {
	tbl := []string{
		"1234.00",
		"1234",
		"12.34",
		"12.340",
		"0.1234",
		"0.0",
		"0",
		"-0.0",
		"-0.0000",
		"-1234.00",
		"-1234",
		"-12.34",
		"-12.340",
		"-0.1234",
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, decimalNum := range tbl {
		dec := new(types.MyDecimal)
		err := dec.FromString([]byte(decimalNum))
		require.NoError(t, err)

		b, err := EncodeKey(sc, nil, types.NewDatum(dec))
		require.NoError(t, err)
		v, err := Decode(b, 1)
		require.NoError(t, err)
		require.Len(t, v, 1)

		vv := v[0].GetMysqlDecimal()
		require.Equal(t, 0, vv.Compare(dec))
	}

	tblCmp := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Ret  int
	}{
		// Test for float type decimal.
		{"1234", "123400", -1},
		{"12340", "123400", -1},
		{"1234", "1234.5", -1},
		{"1234", "1234.0000", 0},
		{"1234", "12.34", 1},
		{"12.34", "12.35", -1},
		{"0.12", "0.1234", -1},
		{"0.1234", "12.3400", -1},
		{"0.1234", "0.1235", -1},
		{"0.123400", "12.34", -1},
		{"12.34000", "12.34", 0},
		{"0.01234", "0.01235", -1},
		{"0.1234", "0", 1},
		{"0.0000", "0", 0},
		{"0.0001", "0", 1},
		{"0.0001", "0.0000", 1},
		{"0", "-0.0000", 0},
		{"-0.0001", "0", -1},
		{"-0.1234", "0", -1},
		{"-0.1234", "-0.12", -1},
		{"-0.12", "-0.1234", 1},
		{"-0.12", "-0.1200", 0},
		{"-0.1234", "0.1234", -1},
		{"-1.234", "-12.34", 1},
		{"-0.1234", "-12.34", 1},
		{"-12.34", "1234", -1},
		{"-12.34", "-12.35", 1},
		{"-0.01234", "-0.01235", 1},
		{"-1234", "-123400", 1},
		{"-12340", "-123400", 1},

		// Test for int type decimal.
		{int64(-1), int64(1), -1},
		{int64(math.MaxInt64), int64(math.MinInt64), 1},
		{int64(math.MaxInt64), int64(math.MaxInt32), 1},
		{int64(math.MinInt32), int64(math.MaxInt16), -1},
		{int64(math.MinInt64), int64(math.MaxInt8), -1},
		{int64(0), int64(math.MaxInt8), -1},
		{int64(math.MinInt8), int64(0), -1},
		{int64(math.MinInt16), int64(math.MaxInt16), -1},
		{int64(1), int64(-1), 1},
		{int64(1), int64(0), 1},
		{int64(-1), int64(0), -1},
		{int64(0), int64(0), 0},
		{int64(math.MaxInt16), int64(math.MaxInt16), 0},

		// Test for uint type decimal.
		{uint64(0), uint64(0), 0},
		{uint64(1), uint64(0), 1},
		{uint64(0), uint64(1), -1},
		{uint64(math.MaxInt8), uint64(math.MaxInt16), -1},
		{uint64(math.MaxUint32), uint64(math.MaxInt32), 1},
		{uint64(math.MaxUint8), uint64(math.MaxInt8), 1},
		{uint64(math.MaxUint16), uint64(math.MaxInt32), -1},
		{uint64(math.MaxUint64), uint64(math.MaxInt64), 1},
		{uint64(math.MaxInt64), uint64(math.MaxUint32), 1},
		{uint64(math.MaxUint64), uint64(0), 1},
		{uint64(0), uint64(math.MaxUint64), -1},
	}
	for _, decimalNums := range tblCmp {
		d1 := types.NewDatum(decimalNums.Arg1)
		dec1, err := d1.ToDecimal(sc)
		require.NoError(t, err)
		d1.SetMysqlDecimal(dec1)

		d2 := types.NewDatum(decimalNums.Arg2)
		dec2, err := d2.ToDecimal(sc)
		require.NoError(t, err)
		d2.SetMysqlDecimal(dec2)

		d1.SetLength(30)
		d1.SetFrac(6)
		d2.SetLength(30)
		d2.SetFrac(6)

		b1, err := EncodeKey(sc, nil, d1)
		require.NoError(t, err)
		b2, err := EncodeKey(sc, nil, d2)
		require.NoError(t, err)

		ret := bytes.Compare(b1, b2)
		require.Equalf(t, decimalNums.Ret, ret, "%v %x %x", decimalNums, b1, b2)

		b1, err = EncodeValue(sc, b1[:0], d1)
		require.NoError(t, err)
		size, err := EstimateValueSize(sc, d1)
		require.NoError(t, err)
		require.Len(t, b1, size)
	}

	floats := []float64{-123.45, -123.40, -23.45, -1.43, -0.93, -0.4333, -0.068,
		-0.0099, 0, 0.001, 0.0012, 0.12, 1.2, 1.23, 123.3, 2424.242424}
	decs := make([][]byte, 0, len(floats))
	for i := range floats {
		dec := types.NewDecFromFloatForTest(floats[i])
		var d types.Datum
		d.SetLength(20)
		d.SetFrac(6)
		d.SetMysqlDecimal(dec)
		b, err := EncodeDecimal(nil, d.GetMysqlDecimal(), d.Length(), d.Frac())
		require.NoError(t, err)
		decs = append(decs, b)
		size, err := EstimateValueSize(sc, d)
		require.NoError(t, err)
		// size - 1 because the flag occupy 1 bit.
		require.Len(t, b, size-1)
	}
	for i := 0; i < len(decs)-1; i++ {
		cmpRes := bytes.Compare(decs[i], decs[i+1])
		require.LessOrEqual(t, cmpRes, 0)
	}

	d := types.NewDecFromStringForTest("-123.123456789")
	_, err := EncodeDecimal(nil, d, 20, 5)
	require.Truef(t, terror.ErrorEqual(err, types.ErrTruncated), "err %v", err)

	_, err = EncodeDecimal(nil, d, 12, 10)
	require.Truef(t, terror.ErrorEqual(err, types.ErrOverflow), "err %v", err)

	sc.IgnoreTruncate = true
	decimalDatum := types.NewDatum(d)
	decimalDatum.SetLength(20)
	decimalDatum.SetFrac(5)
	_, err = EncodeValue(sc, nil, decimalDatum)
	require.NoError(t, err)

	sc.OverflowAsWarning = true
	decimalDatum.SetLength(12)
	decimalDatum.SetFrac(10)
	_, err = EncodeValue(sc, nil, decimalDatum)
	require.NoError(t, err)
}

func TestJSON(t *testing.T) {
	tbl := []string{
		"1234.00",
		`{"a": "b"}`,
	}

	originalDatums := make([]types.Datum, 0, len(tbl))
	for _, jsonDatum := range tbl {
		var d types.Datum
		j, err := json.ParseBinaryFromString(jsonDatum)
		require.NoError(t, err)
		d.SetMysqlJSON(j)
		originalDatums = append(originalDatums, d)
	}

	buf := make([]byte, 0, 4096)
	buf, err := encode(nil, buf, originalDatums, false)
	require.NoError(t, err)

	decodedDatums, err := Decode(buf, 2)
	require.NoError(t, err)

	for i := range decodedDatums {
		lhs := originalDatums[i].GetMysqlJSON().String()
		rhs := decodedDatums[i].GetMysqlJSON().String()
		require.Equal(t, lhs, rhs)
	}
}

func TestCut(t *testing.T) {
	table := []struct {
		Input  []types.Datum
		Expect []types.Datum
	}{
		{
			types.MakeDatums(int64(1)),
			types.MakeDatums(int64(1)),
		},

		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123"), "123"),
			types.MakeDatums(float64(1), float64(3.15), []byte("123"), []byte("123")),
		},
		{
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
			types.MakeDatums(uint64(1), float64(3.15), []byte("123"), int64(-1)),
		},

		{
			types.MakeDatums(true, false),
			types.MakeDatums(int64(1), int64(0)),
		},

		{
			types.MakeDatums(nil),
			types.MakeDatums(nil),
		},

		{
			types.MakeDatums(types.NewBinaryLiteralFromUint(100, -1), types.NewBinaryLiteralFromUint(100, 4)),
			types.MakeDatums(uint64(100), uint64(100)),
		},

		{
			types.MakeDatums(types.Enum{Name: "a", Value: 1}, types.Set{Name: "a", Value: 1}),
			types.MakeDatums(uint64(1), uint64(1)),
		},
		{
			types.MakeDatums(float32(1), float64(3.15), []byte("123456789012345")),
			types.MakeDatums(float64(1), float64(3.15), []byte("123456789012345")),
		},
		{
			types.MakeDatums(types.NewDecFromInt(0), types.NewDecFromFloatForTest(-1.3)),
			types.MakeDatums(types.NewDecFromInt(0), types.NewDecFromFloatForTest(-1.3)),
		},
		{
			types.MakeDatums(json.CreateBinary("abc")),
			types.MakeDatums(json.CreateBinary("abc")),
		},
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for i, datums := range table {
		b, err := EncodeKey(sc, nil, datums.Input...)
		require.NoErrorf(t, err, "%d %v", i, datums)

		var d []byte
		for j, e := range datums.Expect {
			d, b, err = CutOne(b)
			require.NoError(t, err)
			require.NotNil(t, d)

			ed, err1 := EncodeKey(sc, nil, e)
			require.NoError(t, err1)
			require.Equalf(t, ed, d, "%d:%d %#v", i, j, e)
		}
		require.Len(t, b, 0)
	}

	for i, datums := range table {
		b, err := EncodeValue(sc, nil, datums.Input...)
		require.NoErrorf(t, err, "%d %v", i, datums)

		var d []byte
		for j, e := range datums.Expect {
			d, b, err = CutOne(b)
			require.NoError(t, err)
			require.NotNil(t, d)

			ed, err1 := EncodeValue(sc, nil, e)
			require.NoError(t, err1)
			require.Equalf(t, ed, d, "%d:%d %#v", i, j, e)
		}
		require.Len(t, b, 0)
	}

	input := 42
	b, err := EncodeValue(sc, nil, types.NewDatum(input))
	require.NoError(t, err)
	rem, n, err := CutColumnID(b)
	require.NoError(t, err)
	require.Len(t, rem, 0)
	require.Equal(t, int64(input), n)
}

func TestCutOneError(t *testing.T) {
	var b []byte
	_, _, err := CutOne(b)
	require.Error(t, err)
	require.EqualError(t, err, "invalid encoded key")

	b = []byte{4 /* codec.uintFlag */, 0, 0, 0}
	_, _, err = CutOne(b)
	require.Error(t, err)
	require.Regexp(t, "^invalid encoded key", err.Error())
}

func TestSetRawValues(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(sc, nil, datums...)
	require.NoError(t, err)

	values := make([]types.Datum, 4)
	err = SetRawValues(rowData, values)
	require.NoError(t, err)

	for i, rawVal := range values {
		require.IsType(t, types.KindRaw, rawVal.Kind())
		encoded, encodedErr := EncodeValue(sc, nil, datums[i])
		require.NoError(t, encodedErr)
		require.Equal(t, rawVal.GetBytes(), encoded)
	}
}

func TestDecodeOneToChunk(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	datums, tps := datumsForTest(sc)
	rowCount := 3
	chk := chunkForTest(t, sc, datums, tps, rowCount)
	for colIdx, tp := range tps {
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			got := chk.GetRow(rowIdx).GetDatum(colIdx, tp)
			expect := datums[colIdx]
			if got.IsNull() {
				require.True(t, expect.IsNull())
			} else {
				if got.Kind() != types.KindMysqlDecimal {
					cmp, err := got.Compare(sc, &expect, collate.GetCollator(tp.GetCollate()))
					require.NoError(t, err)
					require.Equalf(t, 0, cmp, "expect: %v, got %v", expect, got)
				} else {
					require.Equal(t, expect.GetString(), got.GetString(), "expect: %v, got %v", expect, got)
				}
			}
		}
	}
}

func TestHashGroup(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tps := []*types.FieldType{tp}
	chk1 := chunk.New(tps, 3, 3)
	chk1.Reset()
	chk1.Column(0).AppendMyDecimal(types.NewDecFromStringForTest("-123.123456789"))
	chk1.Column(0).AppendMyDecimal(types.NewDecFromStringForTest("-123.123456789"))
	chk1.Column(0).AppendMyDecimal(types.NewDecFromStringForTest("-123.123456789"))

	buf1 := make([][]byte, 3)
	tp1 := tp
	tp1.SetFlen(20)
	tp1.SetDecimal(5)
	_, err := HashGroupKey(sc, 3, chk1.Column(0), buf1, tp1)
	require.Error(t, err)

	tp2 := tp
	tp2.SetFlen(12)
	tp2.SetDecimal(10)
	_, err = HashGroupKey(sc, 3, chk1.Column(0), buf1, tp2)
	require.Error(t, err)
}

func datumsForTest(sc *stmtctx.StatementContext) ([]types.Datum, []*types.FieldType) {
	decType := types.NewFieldType(mysql.TypeNewDecimal)
	decType.SetDecimal(2)
	_tp1 := types.NewFieldType(mysql.TypeEnum)
	_tp1.SetElems([]string{"a"})
	_tp2 := types.NewFieldType(mysql.TypeSet)
	_tp2.SetElems([]string{"a"})
	_tp3 := types.NewFieldType(mysql.TypeSet)
	_tp3.SetElems([]string{"a", "b", "c", "d", "e", "f"})
	_tp4 := types.NewFieldType(mysql.TypeBit)
	_tp4.SetFlen(8)
	table := []struct {
		value interface{}
		tp    *types.FieldType
	}{
		{nil, types.NewFieldType(mysql.TypeNull)},
		{nil, types.NewFieldType(mysql.TypeLonglong)},
		{nil, types.NewFieldType(mysql.TypeFloat)},
		{nil, types.NewFieldType(mysql.TypeDate)},
		{nil, types.NewFieldType(mysql.TypeDuration)},
		{nil, types.NewFieldType(mysql.TypeNewDecimal)},
		{nil, types.NewFieldType(mysql.TypeEnum)},
		{nil, types.NewFieldType(mysql.TypeSet)},
		{nil, types.NewFieldType(mysql.TypeBit)},
		{nil, types.NewFieldType(mysql.TypeJSON)},
		{nil, types.NewFieldType(mysql.TypeVarchar)},
		{nil, types.NewFieldType(mysql.TypeDouble)},
		{int64(1), types.NewFieldType(mysql.TypeTiny)},
		{int64(1), types.NewFieldType(mysql.TypeShort)},
		{int64(1), types.NewFieldType(mysql.TypeInt24)},
		{int64(1), types.NewFieldType(mysql.TypeLong)},
		{int64(-1), types.NewFieldType(mysql.TypeLong)},
		{int64(1), types.NewFieldType(mysql.TypeLonglong)},
		{uint64(1), types.NewFieldType(mysql.TypeLonglong)},
		{float32(1), types.NewFieldType(mysql.TypeFloat)},
		{float64(1), types.NewFieldType(mysql.TypeDouble)},
		{types.NewDecFromInt(1), types.NewFieldType(mysql.TypeNewDecimal)},
		{types.NewDecFromStringForTest("1.123"), decType},
		{"abc", types.NewFieldType(mysql.TypeString)},
		{"def", types.NewFieldType(mysql.TypeVarchar)},
		{"ghi", types.NewFieldType(mysql.TypeVarString)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeTinyBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeMediumBlob)},
		{[]byte("abc"), types.NewFieldType(mysql.TypeLongBlob)},
		{types.CurrentTime(mysql.TypeDatetime), types.NewFieldType(mysql.TypeDatetime)},
		{types.CurrentTime(mysql.TypeDate), types.NewFieldType(mysql.TypeDate)},
		{types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, types.DefaultFsp), types.NewFieldType(mysql.TypeTimestamp)},
		{types.Duration{Duration: time.Second, Fsp: 1}, types.NewFieldType(mysql.TypeDuration)},
		{types.Enum{Name: "a", Value: 1}, _tp1},
		{types.Set{Name: "a", Value: 1}, _tp2},
		{types.Set{Name: "f", Value: 32}, _tp3},
		{types.BinaryLiteral{100}, _tp4},
		{json.CreateBinary("abc"), types.NewFieldType(mysql.TypeJSON)},
		{int64(1), types.NewFieldType(mysql.TypeYear)},
	}

	datums := make([]types.Datum, 0, len(table)+2)
	tps := make([]*types.FieldType, 0, len(table)+2)
	for _, t := range table {
		tps = append(tps, t.tp)
		d := types.NewDatum(t.value)
		datums = append(datums, d)
	}
	return datums, tps
}

func chunkForTest(t *testing.T, sc *stmtctx.StatementContext, datums []types.Datum, tps []*types.FieldType, rowCount int) *chunk.Chunk {
	decoder := NewDecoder(chunk.New(tps, 32, 32), sc.TimeZone)
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		encoded, err := EncodeValue(sc, nil, datums...)
		require.NoError(t, err)
		decoder.buf = make([]byte, 0, len(encoded))
		for colIdx, tp := range tps {
			encoded, err = decoder.DecodeOne(encoded, colIdx, tp)
			require.NoError(t, err)
		}
	}
	return decoder.chk
}

func TestDecodeRange(t *testing.T) {
	_, _, err := DecodeRange(nil, 0, nil, nil)
	require.Error(t, err)

	datums := types.MakeDatums(1, "abc", 1.1, []byte("def"))
	rowData, err := EncodeValue(nil, nil, datums...)
	require.NoError(t, err)

	datums1, _, err := DecodeRange(rowData, len(datums), nil, nil)
	require.NoError(t, err)
	for i, datum := range datums1 {
		cmp, err := datum.Compare(nil, &datums[i], collate.GetBinaryCollator())
		require.NoError(t, err)
		require.Equal(t, 0, cmp)
	}

	for _, b := range []byte{NilFlag, bytesFlag, maxFlag, maxFlag + 1} {
		newData := append(rowData, b)
		_, _, err := DecodeRange(newData, len(datums)+1, nil, nil)
		require.NoError(t, err)
	}
}

func testHashChunkRowEqual(t *testing.T, a, b interface{}, equal bool) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf1 := make([]byte, 1)
	buf2 := make([]byte, 1)

	tp1 := new(types.FieldType)
	types.DefaultTypeForValue(a, tp1, mysql.DefaultCharset, mysql.DefaultCollationName)
	chk1 := chunk.New([]*types.FieldType{tp1}, 1, 1)
	d := types.Datum{}
	d.SetValue(a, tp1)
	chk1.AppendDatum(0, &d)

	tp2 := new(types.FieldType)
	types.DefaultTypeForValue(b, tp2, mysql.DefaultCharset, mysql.DefaultCollationName)
	chk2 := chunk.New([]*types.FieldType{tp2}, 1, 1)
	d = types.Datum{}
	d.SetValue(b, tp2)
	chk2.AppendDatum(0, &d)

	h := crc32.NewIEEE()
	err1 := HashChunkRow(sc, h, chk1.GetRow(0), []*types.FieldType{tp1}, []int{0}, buf1)
	sum1 := h.Sum32()
	h.Reset()
	err2 := HashChunkRow(sc, h, chk2.GetRow(0), []*types.FieldType{tp2}, []int{0}, buf2)
	sum2 := h.Sum32()
	require.NoError(t, err1)
	require.NoError(t, err2)
	if equal {
		require.Equal(t, sum2, sum1)
	} else {
		require.NotEqual(t, sum2, sum1)
	}
	e, err := EqualChunkRow(sc,
		chk1.GetRow(0), []*types.FieldType{tp1}, []int{0},
		chk2.GetRow(0), []*types.FieldType{tp2}, []int{0})
	require.NoError(t, err)
	if equal {
		require.True(t, e)
	} else {
		require.False(t, e)
	}
}

func TestHashChunkRow(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf := make([]byte, 1)
	datums, tps := datumsForTest(sc)
	chk := chunkForTest(t, sc, datums, tps, 1)

	colIdx := make([]int, len(tps))
	for i := 0; i < len(tps); i++ {
		colIdx[i] = i
	}
	h := crc32.NewIEEE()
	err1 := HashChunkRow(sc, h, chk.GetRow(0), tps, colIdx, buf)
	sum1 := h.Sum32()
	h.Reset()
	err2 := HashChunkRow(sc, h, chk.GetRow(0), tps, colIdx, buf)
	sum2 := h.Sum32()

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, sum2, sum1)
	e, err := EqualChunkRow(sc,
		chk.GetRow(0), tps, colIdx,
		chk.GetRow(0), tps, colIdx)
	require.NoError(t, err)
	require.True(t, e)

	testHashChunkRowEqual(t, nil, nil, true)
	testHashChunkRowEqual(t, uint64(1), int64(1), true)
	testHashChunkRowEqual(t, uint64(18446744073709551615), int64(-1), false)

	dec1 := types.NewDecFromStringForTest("1.1")
	dec2 := types.NewDecFromStringForTest("01.100")
	testHashChunkRowEqual(t, dec1, dec2, true)
	dec1 = types.NewDecFromStringForTest("1.1")
	dec2 = types.NewDecFromStringForTest("01.200")
	testHashChunkRowEqual(t, dec1, dec2, false)

	testHashChunkRowEqual(t, float32(1.0), float64(1.0), true)
	testHashChunkRowEqual(t, float32(1.0), float64(1.1), false)

	testHashChunkRowEqual(t, "x", []byte("x"), true)
	testHashChunkRowEqual(t, "x", []byte("y"), false)
}

func TestValueSizeOfSignedInt(t *testing.T) {
	testCase := []int64{64, 8192, 1048576, 134217728, 17179869184, 2199023255552, 281474976710656, 36028797018963968, 4611686018427387904}
	var b []byte
	for _, v := range testCase {
		b := encodeSignedInt(b[:0], v-10, false)
		require.Equal(t, valueSizeOfSignedInt(v-10), len(b))

		b = encodeSignedInt(b[:0], v, false)
		require.Equal(t, valueSizeOfSignedInt(v), len(b))

		b = encodeSignedInt(b[:0], v+10, false)
		require.Equal(t, valueSizeOfSignedInt(v+10), len(b))

		// Test for negative value.
		b = encodeSignedInt(b[:0], 0-v, false)
		require.Equal(t, valueSizeOfSignedInt(0-v), len(b))

		b = encodeSignedInt(b[:0], 0-v+10, false)
		require.Equal(t, valueSizeOfSignedInt(0-v+10), len(b))

		b = encodeSignedInt(b[:0], 0-v-10, false)
		require.Equal(t, valueSizeOfSignedInt(0-v-10), len(b))
	}
}

func TestValueSizeOfUnsignedInt(t *testing.T) {
	testCase := []uint64{128, 16384, 2097152, 268435456, 34359738368, 4398046511104, 562949953421312, 72057594037927936, 9223372036854775808}
	var b []byte
	for _, v := range testCase {
		b := encodeUnsignedInt(b[:0], v-10, false)
		require.Equal(t, valueSizeOfUnsignedInt(v-10), len(b))

		b = encodeUnsignedInt(b[:0], v, false)
		require.Equal(t, valueSizeOfUnsignedInt(v), len(b))

		b = encodeUnsignedInt(b[:0], v+10, false)
		require.Equal(t, valueSizeOfUnsignedInt(v+10), len(b))
	}
}

func TestHashChunkColumns(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	buf := make([]byte, 1)
	datums, tps := datumsForTest(sc)
	chk := chunkForTest(t, sc, datums, tps, 4)

	colIdx := make([]int, len(tps))
	for i := 0; i < len(tps); i++ {
		colIdx[i] = i
	}
	hasNull := []bool{false, false, false}
	vecHash := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}
	rowHash := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}

	sel := make([]bool, len(datums))
	for i := 0; i < 3; i++ {
		sel[i] = true
	}

	// Test hash value of the first 12 `Null` columns
	for i := 0; i < 12; i++ {
		require.True(t, chk.GetRow(0).IsNull(i))
		err1 := HashChunkSelected(sc, vecHash, chk, tps[i], i, buf, hasNull, sel, false)
		err2 := HashChunkRow(sc, rowHash[0], chk.GetRow(0), tps[i:i+1], colIdx[i:i+1], buf)
		err3 := HashChunkRow(sc, rowHash[1], chk.GetRow(1), tps[i:i+1], colIdx[i:i+1], buf)
		err4 := HashChunkRow(sc, rowHash[2], chk.GetRow(2), tps[i:i+1], colIdx[i:i+1], buf)
		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)
		require.NoError(t, err4)

		require.True(t, hasNull[0])
		require.True(t, hasNull[1])
		require.True(t, hasNull[2])
		require.Equal(t, rowHash[0].Sum64(), vecHash[0].Sum64())
		require.Equal(t, rowHash[1].Sum64(), vecHash[1].Sum64())
		require.Equal(t, rowHash[2].Sum64(), vecHash[2].Sum64())
	}

	// Test hash value of every single column that is not `Null`
	for i := 12; i < len(tps); i++ {
		hasNull = []bool{false, false, false}
		vecHash = []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}
		rowHash = []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}

		require.False(t, chk.GetRow(0).IsNull(i))

		err1 := HashChunkSelected(sc, vecHash, chk, tps[i], i, buf, hasNull, sel, false)
		err2 := HashChunkRow(sc, rowHash[0], chk.GetRow(0), tps[i:i+1], colIdx[i:i+1], buf)
		err3 := HashChunkRow(sc, rowHash[1], chk.GetRow(1), tps[i:i+1], colIdx[i:i+1], buf)
		err4 := HashChunkRow(sc, rowHash[2], chk.GetRow(2), tps[i:i+1], colIdx[i:i+1], buf)

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)
		require.NoError(t, err4)

		require.False(t, hasNull[0])
		require.False(t, hasNull[1])
		require.False(t, hasNull[2])
		require.Equal(t, rowHash[0].Sum64(), vecHash[0].Sum64())
		require.Equal(t, rowHash[1].Sum64(), vecHash[1].Sum64())
		require.Equal(t, rowHash[2].Sum64(), vecHash[2].Sum64())
	}
}
