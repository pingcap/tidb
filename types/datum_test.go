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

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
	"github.com/stretchr/testify/assert"
)

func TestDatum(t *testing.T) {
	t.Parallel()
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
		assert.Equal(t, val, x)
		assert.Equal(t, int(d.length), d.Length())
		assert.Equal(t, d.String(), fmt.Sprint(d))
	}
}

func testDatumToBool(t *testing.T, in interface{}, res int) {
	datum := NewDatum(in)
	res64 := int64(res)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := datum.ToBool(sc)
	assert.Nil(t, err)
	assert.Equal(t, res64, b)
}

func TestToBool(t *testing.T) {
	t.Parallel()
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
	time, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	assert.Nil(t, err)
	testDatumToBool(t, time, 1)

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	assert.Nil(t, err)
	testDatumToBool(t, td, 1)

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(0.1415926, ft)
	assert.Nil(t, err)
	testDatumToBool(t, v, 1)
	d := NewDatum(&invalidMockType{})
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	_, err = d.ToBool(sc)
	assert.NotNil(t, err)
}

func testDatumToInt64(t *testing.T, val interface{}, expect int64) {
	d := NewDatum(val)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := d.ToInt64(sc)
	assert.Nil(t, err)
	assert.Equal(t, expect, b)
}

func TestToInt64(t *testing.T) {
	t.Parallel()
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

	time, err := ParseTime(&stmtctx.StatementContext{
		TimeZone: time.UTC,
	}, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 0)
	assert.Nil(t, err)
	testDatumToInt64(t, time, int64(20111110111112))

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	assert.Nil(t, err)
	testDatumToInt64(t, td, int64(111112))

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	assert.Nil(t, err)
	testDatumToInt64(t, v, int64(3))
}

func TestToFloat32(t *testing.T) {
	t.Parallel()
	ft := NewFieldType(mysql.TypeFloat)
	var datum = NewFloat64Datum(281.37)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	converted, err := datum.ConvertTo(sc, ft)
	assert.Nil(t, err)
	assert.Equal(t, KindFloat32, converted.Kind())
	assert.Equal(t, float32(281.37), converted.GetFloat32())

	datum.SetString("281.37", mysql.DefaultCollationName)
	converted, err = datum.ConvertTo(sc, ft)
	assert.Nil(t, err)
	assert.Equal(t, KindFloat32, converted.Kind())
	assert.Equal(t, float32(281.37), converted.GetFloat32())

	ft = NewFieldType(mysql.TypeDouble)
	datum = NewFloat32Datum(281.37)
	converted, err = datum.ConvertTo(sc, ft)
	assert.Nil(t, err)
	assert.Equal(t, KindFloat64, converted.Kind())
	// Convert to float32 and convert back to float64, we will get a different value.
	assert.NotEqual(t, 281.37, converted.GetFloat64())
	assert.Equal(t, datum.GetFloat64(), converted.GetFloat64())
}

func TestToFloat64(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		d      Datum
		errMsg string
		result float64
	}{
		{NewDatum(float32(3.00)), "", 3.00},
		{NewDatum(float64(12345.678)), "", 12345.678},
		{NewDatum("12345.678"), "", 12345.678},
		{NewDatum([]byte("12345.678")), "", 12345.678},
		{NewDatum(int64(12345)), "", 12345},
		{NewDatum(uint64(123456)), "", 123456},
		{NewDatum(byte(123)), "cannot convert .*", 0},
	}

	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, test := range testCases {
		converted, err := test.d.ToFloat64(sc)
		if test.errMsg == "" {
			assert.Nil(t, err)
		} else {
			assert.Errorf(t, err, test.errMsg)
		}
		assert.Equal(t, test.result, converted)
	}
}

// mustParseTimeIntoDatum is similar to ParseTime but panic if any error occurs.
func mustParseTimeIntoDatum(s string, tp byte, fsp int8) (d Datum) {
	t, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, s, tp, fsp)
	if err != nil {
		panic("ParseTime fail")
	}
	d.SetMysqlTime(t)
	return
}

func TestToJSON(t *testing.T) {
	t.Parallel()
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

		// can not parse JSON from this string, so error occurs.
		{NewStringDatum("hello, 世界"), "", false},
	}
	for _, tt := range tests {
		obtain, err := tt.datum.ConvertTo(sc, ft)
		if tt.success {
			assert.Nil(t, err)

			sd := NewStringDatum(tt.expected)
			var expected Datum
			expected, err = sd.ConvertTo(sc, ft)
			assert.Nil(t, err)

			var cmp int
			cmp, err = obtain.CompareDatum(sc, &expected)
			assert.Nil(t, err)
			assert.Equal(t, 0, cmp)
		} else {
			assert.NotNil(t, err)
		}
	}
}

func TestIsNull(t *testing.T) {
	t.Parallel()
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
	assert.Equal(t, isnull, d.IsNull())
}

func TestToBytes(t *testing.T) {
	t.Parallel()
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
		assert.Nil(t, err)
		assert.Equal(t, bin, tt.out)
	}
}

func TestComputePlusAndMinus(t *testing.T) {
	t.Parallel()
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

	for _, tt := range tests {
		got, err := ComputePlus(tt.a, tt.b)
		assert.Equal(t, tt.hasErr, err != nil)
		v, err := got.CompareDatum(sc, &tt.plus)
		assert.Nil(t, err)
		assert.Equal(t, 0, v)
	}
}

func TestCloneDatum(t *testing.T) {
	t.Parallel()
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
		res, err := tt.CompareDatum(sc, &tt1)
		assert.Nil(t, err)
		assert.Equal(t, 0, res)
		if tt.b != nil {
			assert.True(t, &tt.b[0] != &tt1.b[0])
		}
	}
}

func newTypeWithFlag(tp byte, flag uint) *FieldType {
	t := NewFieldType(tp)
	t.Flag |= flag
	return t
}

func newMyDecimal(val string, t *testing.T) *MyDecimal {
	d := MyDecimal{}
	err := d.FromString([]byte(val))
	assert.Nil(t, err)
	return &d
}

func newRetTypeWithFlenDecimal(tp byte, flen int, decimal int) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    flen,
		Decimal: decimal,
	}
}

func TestEstimatedMemUsage(t *testing.T) {
	t.Parallel()
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
	assert.Equal(t, bytesConsumed, int(EstimatedMemUsage(datumArray, 10)))
}

func TestChangeReverseResultByUpperLowerBound(t *testing.T) {
	t.Parallel()
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
		// int64 reserve to Decimal
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
	for _, test := range testData {
		reverseRes, err := ChangeReverseResultByUpperLowerBound(sc, test.retType, test.a, test.roundType)
		assert.Nil(t, err)
		var cmp int
		cmp, err = reverseRes.CompareDatum(sc, &test.res)
		assert.Nil(t, err)
		assert.Equal(t, 0, cmp)
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
	t.Parallel()
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
	tp.Flen = 1
	for _, tt := range tests {
		bin, err := tt.a.convertToMysqlBit(nil, tp)
		assert.Nil(t, err)
		assert.Equal(t, tt.out, bin.b)
	}
}

func BenchmarkCompareDatum(b *testing.B) {
	vals, vals1 := prepareCompareDatums()
	sc := new(stmtctx.StatementContext)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, v := range vals {
			_, err := v.CompareDatum(sc, &vals1[j])
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
