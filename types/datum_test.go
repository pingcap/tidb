// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatum(t *testing.T) {
	values := []interface{}{
		int64(1),
		uint64(1),
		1.1,
		"abc",
		[]byte("abc"),
		[]int{1},
	}
	for _, val := range values {
		var d Datum
		d.SetMinNotNull()
		d.SetValueWithDefaultCollation(val)
		x := d.GetValue()
		require.Equal(t, val, x)
		require.Equal(t, int(d.length), d.Length())
		require.Equal(t, d.String(), fmt.Sprint(d))
	}
}

func testDatumToBool(t *testing.T, in interface{}, res int) {
	datum := NewDatum(in)
	res64 := int64(res)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := datum.ToBool(sc)
	require.NoError(t, err)
	require.Equal(t, res64, b)
}

func TestToBool(t *testing.T) {
	testDatumToBool(t, 0, 0)
	testDatumToBool(t, int64(0), 0)
	testDatumToBool(t, uint64(0), 0)
	testDatumToBool(t, float32(0.1), 1)
	testDatumToBool(t, float64(0.1), 1)
	testDatumToBool(t, float64(0.5), 1)
	testDatumToBool(t, float64(0.499), 1)
	testDatumToBool(t, "", 0)
	testDatumToBool(t, "0.1", 1)
	testDatumToBool(t, []byte{}, 0)
	testDatumToBool(t, []byte("0.1"), 1)
	testDatumToBool(t, NewBinaryLiteralFromUint(0, -1), 0)
	testDatumToBool(t, Enum{Name: "a", Value: 1}, 1)
	testDatumToBool(t, Set{Name: "a", Value: 1}, 1)
	testDatumToBool(t, json.CreateBinary(int64(1)), 1)
	testDatumToBool(t, json.CreateBinary(int64(0)), 0)
	testDatumToBool(t, json.CreateBinary("0"), 1)
	testDatumToBool(t, json.CreateBinary("aaabbb"), 1)
	testDatumToBool(t, json.CreateBinary(float64(0.0)), 0)
	testDatumToBool(t, json.CreateBinary(float64(3.1415)), 1)
	testDatumToBool(t, json.CreateBinary([]interface{}{int64(1), int64(2)}), 1)
	testDatumToBool(t, json.CreateBinary(map[string]interface{}{"ke": "val"}), 1)
	testDatumToBool(t, json.CreateBinary("0000-00-00 00:00:00"), 1)
	testDatumToBool(t, json.CreateBinary("0778"), 1)
	testDatumToBool(t, json.CreateBinary("0000"), 1)
	testDatumToBool(t, json.CreateBinary(nil), 1)
	testDatumToBool(t, json.CreateBinary([]interface{}{nil}), 1)
	testDatumToBool(t, json.CreateBinary(true), 1)
	testDatumToBool(t, json.CreateBinary(false), 1)
	testDatumToBool(t, json.CreateBinary(""), 1)
	t1, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	require.NoError(t, err)
	testDatumToBool(t, t1, 1)

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	require.NoError(t, err)
	testDatumToBool(t, td, 1)

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.SetDecimal(5)
	v, err := Convert(0.1415926, ft)
	require.NoError(t, err)
	testDatumToBool(t, v, 1)
	d := NewDatum(&invalidMockType{})
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	_, err = d.ToBool(sc)
	require.Error(t, err)
}

func testDatumToInt64(t *testing.T, val interface{}, expect int64) {
	d := NewDatum(val)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := d.ToInt64(sc)
	require.NoError(t, err)
	require.Equal(t, expect, b)
}

func TestToInt64(t *testing.T) {
	testDatumToInt64(t, "0", int64(0))
	testDatumToInt64(t, 0, int64(0))
	testDatumToInt64(t, int64(0), int64(0))
	testDatumToInt64(t, uint64(0), int64(0))
	testDatumToInt64(t, float32(3.1), int64(3))
	testDatumToInt64(t, float64(3.1), int64(3))
	testDatumToInt64(t, NewBinaryLiteralFromUint(100, -1), int64(100))
	testDatumToInt64(t, Enum{Name: "a", Value: 1}, int64(1))
	testDatumToInt64(t, Set{Name: "a", Value: 1}, int64(1))
	testDatumToInt64(t, json.CreateBinary(int64(3)), int64(3))

	t1, err := ParseTime(&stmtctx.StatementContext{
		TimeZone: time.UTC,
	}, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 0)
	require.NoError(t, err)
	testDatumToInt64(t, t1, int64(20111110111112))

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	require.NoError(t, err)
	testDatumToInt64(t, td, int64(111112))

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.SetDecimal(5)
	v, err := Convert(3.1415926, ft)
	require.NoError(t, err)
	testDatumToInt64(t, v, int64(3))
}

func testDatumToUInt32(t *testing.T, val interface{}, expect uint32, hasError bool) {
	d := NewDatum(val)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true

	ft := NewFieldType(mysql.TypeLong)
	ft.AddFlag(mysql.UnsignedFlag)
	converted, err := d.ConvertTo(sc, ft)

	if hasError {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	require.Equal(t, KindUint64, converted.Kind())
	require.Equal(t, uint64(expect), converted.GetUint64())
}

func TestToUint32(t *testing.T) {
	// test overflow
	testDatumToUInt32(t, 5000000000, 4294967295, true)
	testDatumToUInt32(t, int64(-1), 4294967295, true)
	testDatumToUInt32(t, "5000000000", 4294967295, true)

	testDatumToUInt32(t, 12345, 12345, false)
	testDatumToUInt32(t, int64(0), 0, false)
	testDatumToUInt32(t, 2147483648, 2147483648, false)
	testDatumToUInt32(t, Enum{Name: "a", Value: 1}, 1, false)
	testDatumToUInt32(t, Set{Name: "a", Value: 1}, 1, false)
}

func TestConvertToFloat(t *testing.T) {
	testCases := []struct {
		d      Datum
		tp     byte
		errMsg string
		r64    float64
		r32    float32
	}{
		{NewDatum(float32(3.00)), mysql.TypeDouble, "", 3.00, 3.00},
		{NewDatum(float64(12345.678)), mysql.TypeDouble, "", 12345.678, 12345.678},
		{NewDatum("12345.678"), mysql.TypeDouble, "", 12345.678, 12345.678},
		{NewDatum([]byte("12345.678")), mysql.TypeDouble, "", 12345.678, 12345.678},
		{NewDatum(int64(12345)), mysql.TypeDouble, "", 12345, 12345},
		{NewDatum(uint64(123456)), mysql.TypeDouble, "", 123456, 123456},
		{NewDatum(byte(123)), mysql.TypeDouble, "cannot convert ", 0, 0},
		{NewDatum(math.NaN()), mysql.TypeDouble, "constant .* overflows double", 0, 0},
		{NewDatum(math.Inf(-1)), mysql.TypeDouble, "constant .* overflows double", math.Inf(-1), float32(math.Inf(-1))},
		{NewDatum(math.Inf(1)), mysql.TypeDouble, "constant .* overflows double", math.Inf(1), float32(math.Inf(1))},
		{NewDatum(float32(281.37)), mysql.TypeFloat, "", 281.37, 281.37},
		{NewDatum("281.37"), mysql.TypeFloat, "", 281.37, 281.37},
	}

	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, testCase := range testCases {
		converted, err := testCase.d.ConvertTo(sc, NewFieldType(testCase.tp))
		if testCase.errMsg == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Regexp(t, testCase.errMsg, err.Error())
		}
		require.Equal(t, testCase.r32, converted.GetFloat32())
		if testCase.tp == mysql.TypeDouble {
			require.Equal(t, testCase.r64, converted.GetFloat64())
		} else {
			// Convert to float32 and convert back to float64, we will get a different value.
			require.NotEqual(t, testCase.r64, converted.GetFloat64())
		}
	}
}

// mustParseTimeIntoDatum is similar to ParseTime but panic if any error occurs.
func mustParseTimeIntoDatum(s string, tp byte, fsp int) (d Datum) {
	t, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, s, tp, fsp)
	if err != nil {
		panic("ParseTime fail")
	}
	d.SetMysqlTime(t)
	return
}

func TestToJSON(t *testing.T) {
	ft := NewFieldType(mysql.TypeJSON)
	sc := new(stmtctx.StatementContext)
	tests := []struct {
		datum    Datum
		expected string
		success  bool
	}{
		{NewIntDatum(1), `1.0`, true},
		{NewFloat64Datum(2), `2`, true},
		{NewStringDatum("\"hello, 世界\""), `"hello, 世界"`, true},
		{NewStringDatum("[1, 2, 3]"), `[1, 2, 3]`, true},
		{NewStringDatum("{}"), `{}`, true},
		{mustParseTimeIntoDatum("2011-11-10 11:11:11.111111", mysql.TypeTimestamp, 6), `"2011-11-10 11:11:11.111111"`, true},
		{NewStringDatum(`{"a": "9223372036854775809"}`), `{"a": "9223372036854775809"}`, true},
		{NewBinaryLiteralDatum([]byte{0x81}), ``, false},

		// can not parse JSON from this string, so error occurs.
		{NewStringDatum("hello, 世界"), "", false},
	}
	for _, tt := range tests {
		obtain, err := tt.datum.ConvertTo(sc, ft)
		if tt.success {
			require.NoError(t, err)

			sd := NewStringDatum(tt.expected)
			var expected Datum
			expected, err = sd.ConvertTo(sc, ft)
			require.NoError(t, err)

			var cmp int
			cmp, err = obtain.Compare(sc, &expected, collate.GetBinaryCollator())
			require.NoError(t, err)
			require.Equal(t, 0, cmp)
		} else {
			require.Error(t, err)
		}
	}
}

func TestIsNull(t *testing.T) {
	tests := []struct {
		data   interface{}
		isnull bool
	}{
		{nil, true},
		{0, false},
		{1, false},
		{1.1, false},
		{"string", false},
		{"", false},
	}
	for _, tt := range tests {
		testIsNull(t, tt.data, tt.isnull)
	}
}

func testIsNull(t *testing.T, data interface{}, isnull bool) {
	d := NewDatum(data)
	require.Equalf(t, isnull, d.IsNull(), "data: %v, isnull: %v", data, isnull)
}

func TestToBytes(t *testing.T) {
	tests := []struct {
		a   Datum
		out []byte
	}{
		{NewIntDatum(1), []byte("1")},
		{NewDecimalDatum(NewDecFromInt(1)), []byte("1")},
		{NewFloat64Datum(1.23), []byte("1.23")},
		{NewStringDatum("abc"), []byte("abc")},
		{Datum{}, []byte{}},
	}
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, tt := range tests {
		bin, err := tt.a.ToBytes()
		require.NoError(t, err)
		require.Equal(t, tt.out, bin)
	}
}

func TestComputePlusAndMinus(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		a      Datum
		b      Datum
		plus   Datum
		minus  Datum
		hasErr bool
	}{
		{NewIntDatum(72), NewIntDatum(28), NewIntDatum(100), NewIntDatum(44), false},
		{NewIntDatum(72), NewUintDatum(28), NewIntDatum(100), NewIntDatum(44), false},
		{NewUintDatum(72), NewUintDatum(28), NewUintDatum(100), NewUintDatum(44), false},
		{NewUintDatum(72), NewIntDatum(28), NewUintDatum(100), NewUintDatum(44), false},
		{NewFloat64Datum(72.0), NewFloat64Datum(28.0), NewFloat64Datum(100.0), NewFloat64Datum(44.0), false},
		{NewDecimalDatum(NewDecFromStringForTest("72.5")), NewDecimalDatum(NewDecFromInt(3)), NewDecimalDatum(NewDecFromStringForTest("75.5")), NewDecimalDatum(NewDecFromStringForTest("69.5")), false},
		{NewIntDatum(72), NewFloat64Datum(42), Datum{}, Datum{}, true},
		{NewStringDatum("abcd"), NewIntDatum(42), Datum{}, Datum{}, true},
	}

	for ith, tt := range tests {
		got, err := ComputePlus(tt.a, tt.b)
		require.Equal(t, tt.hasErr, err != nil)
		v, err := got.Compare(sc, &tt.plus, collate.GetBinaryCollator())
		require.NoError(t, err)
		require.Equalf(t, 0, v, "%dth got:%#v, %#v, expect:%#v, %#v", ith, got, got.x, tt.plus, tt.plus.x)
	}
}

func TestCloneDatum(t *testing.T) {
	var raw Datum
	raw.b = []byte("raw")
	raw.k = KindRaw
	tests := []Datum{
		NewIntDatum(72),
		NewUintDatum(72),
		NewStringDatum("abcd"),
		NewBytesDatum([]byte("abcd")),
		raw,
	}

	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, tt := range tests {
		tt1 := *tt.Clone()
		res, err := tt.Compare(sc, &tt1, collate.GetBinaryCollator())
		require.NoError(t, err)
		require.Equal(t, 0, res)
		if tt.b != nil {
			require.NotSame(t, &tt1.b[0], &tt.b[0])
		}
	}
}

func newTypeWithFlag(tp byte, flag uint) *FieldType {
	t := NewFieldType(tp)
	t.AddFlag(flag)
	return t
}

func newMyDecimal(val string, t *testing.T) *MyDecimal {
	d := MyDecimal{}
	err := d.FromString([]byte(val))
	require.NoError(t, err)
	return &d
}

func newRetTypeWithFlenDecimal(tp byte, flen int, decimal int) *FieldType {
	ft := &FieldType{}
	ft.SetType(tp)
	ft.SetFlen(flen)
	ft.SetDecimal(decimal)
	return ft
}

func TestEstimatedMemUsage(t *testing.T) {
	b := []byte{'a', 'b', 'c', 'd'}
	enum := Enum{Name: "a", Value: 1}
	datumArray := []Datum{
		NewIntDatum(1),
		NewFloat64Datum(1.0),
		NewFloat32Datum(1.0),
		NewStringDatum(string(b)),
		NewBytesDatum(b),
		NewDecimalDatum(newMyDecimal("1234.1234", t)),
		NewMysqlEnumDatum(enum),
	}
	bytesConsumed := 10 * (len(datumArray)*sizeOfEmptyDatum +
		sizeOfMyDecimal +
		len(b)*2 +
		len(hack.Slice(enum.Name)))
	require.Equal(t, bytesConsumed, int(EstimatedMemUsage(datumArray, 10)))
}

func TestChangeReverseResultByUpperLowerBound(t *testing.T) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	sc.OverflowAsWarning = true
	// TODO: add more reserve convert tests for each pair of convert type.
	testData := []struct {
		a         Datum
		res       Datum
		retType   *FieldType
		roundType RoundingType
	}{
		// int64 reserve to uint64
		{
			NewIntDatum(1),
			NewUintDatum(2),
			newTypeWithFlag(mysql.TypeLonglong, mysql.UnsignedFlag),
			Ceiling,
		},
		{
			NewIntDatum(1),
			NewUintDatum(1),
			newTypeWithFlag(mysql.TypeLonglong, mysql.UnsignedFlag),
			Floor,
		},
		{
			NewIntDatum(math.MaxInt64),
			NewUintDatum(math.MaxUint64),
			newTypeWithFlag(mysql.TypeLonglong, mysql.UnsignedFlag),
			Ceiling,
		},
		{
			NewIntDatum(math.MaxInt64),
			NewUintDatum(math.MaxInt64),
			newTypeWithFlag(mysql.TypeLonglong, mysql.UnsignedFlag),
			Floor,
		},
		// int64 reserve to float64
		{
			NewIntDatum(1),
			NewFloat64Datum(2),
			newRetTypeWithFlenDecimal(mysql.TypeDouble, mysql.MaxRealWidth, UnspecifiedLength),
			Ceiling,
		},
		{
			NewIntDatum(1),
			NewFloat64Datum(1),
			newRetTypeWithFlenDecimal(mysql.TypeDouble, mysql.MaxRealWidth, UnspecifiedLength),
			Floor,
		},
		{
			NewIntDatum(math.MaxInt64),
			GetMaxValue(newRetTypeWithFlenDecimal(mysql.TypeDouble, mysql.MaxRealWidth, UnspecifiedLength)),
			newRetTypeWithFlenDecimal(mysql.TypeDouble, mysql.MaxRealWidth, UnspecifiedLength),
			Ceiling,
		},
		{
			NewIntDatum(math.MaxInt64),
			NewFloat64Datum(float64(math.MaxInt64)),
			newRetTypeWithFlenDecimal(mysql.TypeDouble, mysql.MaxRealWidth, UnspecifiedLength),
			Floor,
		},
		// int64 reserve to decimal
		{
			NewIntDatum(1),
			NewDecimalDatum(newMyDecimal("2", t)),
			newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3),
			Ceiling,
		},
		{
			NewIntDatum(1),
			NewDecimalDatum(newMyDecimal("1", t)),
			newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3),
			Floor,
		},
		{
			NewIntDatum(math.MaxInt64),
			GetMaxValue(newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3)),
			newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3),
			Ceiling,
		},
		{
			NewIntDatum(math.MaxInt64),
			NewDecimalDatum(newMyDecimal(strconv.FormatInt(math.MaxInt64, 10), t)),
			newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3),
			Floor,
		},
	}
	for ith, test := range testData {
		reverseRes, err := ChangeReverseResultByUpperLowerBound(sc, test.retType, test.a, test.roundType)
		require.NoError(t, err)
		var cmp int
		cmp, err = reverseRes.Compare(sc, &test.res, collate.GetBinaryCollator())
		require.NoError(t, err)
		require.Equalf(t, 0, cmp, "%dth got:%#v, expect:%#v", ith, reverseRes, test.res)
	}
}

func prepareCompareDatums() ([]Datum, []Datum) {
	vals := make([]Datum, 0, 5)
	vals = append(vals, NewIntDatum(1))
	vals = append(vals, NewFloat64Datum(1.23))
	vals = append(vals, NewStringDatum("abcde"))
	vals = append(vals, NewDecimalDatum(NewDecFromStringForTest("1.2345")))
	vals = append(vals, NewTimeDatum(NewTime(FromGoTime(time.Date(2018, 3, 8, 16, 1, 0, 315313000, time.UTC)), mysql.TypeTimestamp, 6)))

	vals1 := make([]Datum, 0, 5)
	vals1 = append(vals1, NewIntDatum(1))
	vals1 = append(vals1, NewFloat64Datum(1.23))
	vals1 = append(vals1, NewStringDatum("abcde"))
	vals1 = append(vals1, NewDecimalDatum(NewDecFromStringForTest("1.2345")))
	vals1 = append(vals1, NewTimeDatum(NewTime(FromGoTime(time.Date(2018, 3, 8, 16, 1, 0, 315313000, time.UTC)), mysql.TypeTimestamp, 6)))
	return vals, vals1
}

func TestStringToMysqlBit(t *testing.T) {
	tests := []struct {
		a   Datum
		out []byte
	}{
		{NewStringDatum("true"), []byte{1}},
		{NewStringDatum("false"), []byte{0}},
		{NewStringDatum("1"), []byte{1}},
		{NewStringDatum("0"), []byte{0}},
		{NewStringDatum("b'1'"), []byte{1}},
		{NewStringDatum("b'0'"), []byte{0}},
	}
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	tp := NewFieldType(mysql.TypeBit)
	tp.SetFlen(1)
	for _, tt := range tests {
		bin, err := tt.a.convertToMysqlBit(nil, tp)
		require.NoError(t, err)
		require.Equal(t, tt.out, bin.b)
	}
}

func BenchmarkCompareDatum(b *testing.B) {
	vals, vals1 := prepareCompareDatums()
	sc := new(stmtctx.StatementContext)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, v := range vals {
			_, err := v.Compare(sc, &vals1[j], collate.GetBinaryCollator())
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCompareDatumByReflect(b *testing.B) {
	vals, vals1 := prepareCompareDatums()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reflect.DeepEqual(vals, vals1)
	}
}

func TestProduceDecWithSpecifiedTp(t *testing.T) {
	tests := []struct {
		dec         string
		flen        int
		frac        int
		newDec      string
		isOverflow  bool
		isTruncated bool
	}{
		{"0.0000", 4, 3, "0.000", false, false},
		{"0.0001", 4, 3, "0.000", false, true},
		{"123", 8, 5, "123.00000", false, false},
		{"-123", 8, 5, "-123.00000", false, false},
		{"123.899", 5, 2, "123.90", false, true},
		{"-123.899", 5, 2, "-123.90", false, true},
		{"123.899", 6, 2, "123.90", false, true},
		{"-123.899", 6, 2, "-123.90", false, true},
		{"123.99", 4, 1, "124.0", false, true},
		{"123.99", 3, 0, "124", false, true},
		{"-123.99", 3, 0, "-124", false, true},
		{"123.99", 3, 1, "99.9", true, false},
		{"-123.99", 3, 1, "-99.9", true, false},
		{"99.9999", 5, 3, "99.999", true, false},
		{"-99.9999", 5, 3, "-99.999", true, false},
		{"99.9999", 6, 3, "100.000", false, true},
		{"-99.9999", 6, 3, "-100.000", false, true},
	}
	sc := new(stmtctx.StatementContext)
	for _, tt := range tests {
		tp := NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(tt.flen).SetDecimal(tt.frac).BuildP()
		dec := NewDecFromStringForTest(tt.dec)
		newDec, err := ProduceDecWithSpecifiedTp(dec, tp, sc)
		if tt.isOverflow {
			if !ErrOverflow.Equal(err) {
				assert.FailNow(t, "Error is not overflow", "err: %v before: %v after: %v", err, tt.dec, dec)
			}
		} else {
			require.NoError(t, err, tt)
		}
		require.Equal(t, tt.newDec, newDec.String())
		warn := sc.TruncateWarnings(0)
		if tt.isTruncated {
			if len(warn) != 1 || !ErrTruncatedWrongVal.Equal(warn[0].Err) {
				assert.FailNow(t, "Warn is not truncated", "warn: %v before: %v after: %v", warn, tt.dec, dec)
			}
		} else {
			if warn != nil {
				assert.FailNow(t, "Warn is not nil", "warn: %v before: %v after: %v", warn, tt.dec, dec)
			}
		}
	}
}
