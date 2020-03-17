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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

var _ = Suite(&testDatumSuite{})

type testDatumSuite struct {
}

func (ts *testDatumSuite) TestDatum(c *C) {
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
		c.Assert(x, DeepEquals, val)
		c.Assert(d.Length(), Equals, int(d.length))
		c.Assert(fmt.Sprint(d), Equals, d.String())
	}
}

func testDatumToBool(c *C, in interface{}, res int) {
	datum := NewDatum(in)
	res64 := int64(res)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := datum.ToBool(sc)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, res64)
}

func (ts *testDatumSuite) TestToBool(c *C) {
	testDatumToBool(c, int(0), 0)
	testDatumToBool(c, int64(0), 0)
	testDatumToBool(c, uint64(0), 0)
	testDatumToBool(c, float32(0.1), 0)
	testDatumToBool(c, float64(0.1), 0)
	testDatumToBool(c, float64(0.5), 1)
	testDatumToBool(c, float64(0.499), 0)
	testDatumToBool(c, "", 0)
	testDatumToBool(c, "0.1", 0)
	testDatumToBool(c, []byte{}, 0)
	testDatumToBool(c, []byte("0.1"), 0)
	testDatumToBool(c, NewBinaryLiteralFromUint(0, -1), 0)
	testDatumToBool(c, Enum{Name: "a", Value: 1}, 1)
	testDatumToBool(c, Set{Name: "a", Value: 1}, 1)

	t, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testDatumToBool(c, t, 1)

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testDatumToBool(c, td, 1)

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(0.1415926, ft)
	c.Assert(err, IsNil)
	testDatumToBool(c, v, 0)
	d := NewDatum(&invalidMockType{})
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	_, err = d.ToBool(sc)
	c.Assert(err, NotNil)
}

func (ts *testDatumSuite) TestEqualDatums(c *C) {
	tests := []struct {
		a    []interface{}
		b    []interface{}
		same bool
	}{
		// Positive cases
		{[]interface{}{1}, []interface{}{1}, true},
		{[]interface{}{1, "aa"}, []interface{}{1, "aa"}, true},
		{[]interface{}{1, "aa", 1}, []interface{}{1, "aa", 1}, true},

		// negative cases
		{[]interface{}{1}, []interface{}{2}, false},
		{[]interface{}{1, "a"}, []interface{}{1, "aaaaaa"}, false},
		{[]interface{}{1, "aa", 3}, []interface{}{1, "aa", 2}, false},

		// Corner cases
		{[]interface{}{}, []interface{}{}, true},
		{[]interface{}{nil}, []interface{}{nil}, true},
		{[]interface{}{}, []interface{}{1}, false},
		{[]interface{}{1}, []interface{}{1, 1}, false},
		{[]interface{}{nil}, []interface{}{1}, false},
	}
	for _, tt := range tests {
		testEqualDatums(c, tt.a, tt.b, tt.same)
	}
}

func testEqualDatums(c *C, a []interface{}, b []interface{}, same bool) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	res, err := EqualDatums(sc, MakeDatums(a...), MakeDatums(b...))
	c.Assert(err, IsNil)
	c.Assert(res, Equals, same, Commentf("a: %v, b: %v", a, b))
}

func testDatumToInt64(c *C, val interface{}, expect int64) {
	d := NewDatum(val)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := d.ToInt64(sc)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (ts *testTypeConvertSuite) TestToInt64(c *C) {
	testDatumToInt64(c, "0", int64(0))
	testDatumToInt64(c, int(0), int64(0))
	testDatumToInt64(c, int64(0), int64(0))
	testDatumToInt64(c, uint64(0), int64(0))
	testDatumToInt64(c, float32(3.1), int64(3))
	testDatumToInt64(c, float64(3.1), int64(3))
	testDatumToInt64(c, NewBinaryLiteralFromUint(100, -1), int64(100))
	testDatumToInt64(c, Enum{Name: "a", Value: 1}, int64(1))
	testDatumToInt64(c, Set{Name: "a", Value: 1}, int64(1))
	testDatumToInt64(c, json.CreateBinary(int64(3)), int64(3))

	t, err := ParseTime(&stmtctx.StatementContext{
		TimeZone: time.UTC,
	}, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 0)
	c.Assert(err, IsNil)
	testDatumToInt64(c, t, int64(20111110111112))

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testDatumToInt64(c, td, int64(111112))

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	testDatumToInt64(c, v, int64(3))
}

func (ts *testTypeConvertSuite) TestToFloat32(c *C) {
	ft := NewFieldType(mysql.TypeFloat)
	var datum = NewFloat64Datum(281.37)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	converted, err := datum.ConvertTo(sc, ft)
	c.Assert(err, IsNil)
	c.Assert(converted.Kind(), Equals, KindFloat32)
	c.Assert(converted.GetFloat32(), Equals, float32(281.37))

	datum.SetString("281.37", mysql.DefaultCollationName)
	converted, err = datum.ConvertTo(sc, ft)
	c.Assert(err, IsNil)
	c.Assert(converted.Kind(), Equals, KindFloat32)
	c.Assert(converted.GetFloat32(), Equals, float32(281.37))

	ft = NewFieldType(mysql.TypeDouble)
	datum = NewFloat32Datum(281.37)
	converted, err = datum.ConvertTo(sc, ft)
	c.Assert(err, IsNil)
	c.Assert(converted.Kind(), Equals, KindFloat64)
	// Convert to float32 and convert back to float64, we will get a different value.
	c.Assert(converted.GetFloat64(), Not(Equals), 281.37)
	c.Assert(converted.GetFloat64(), Equals, datum.GetFloat64())
}

func (ts *testTypeConvertSuite) TestToFloat64(c *C) {
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
	for _, t := range testCases {
		converted, err := t.d.ToFloat64(sc)
		if t.errMsg == "" {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, ErrorMatches, t.errMsg)
		}
		c.Assert(converted, Equals, t.result)
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

func (ts *testDatumSuite) TestToJSON(c *C) {
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
			c.Assert(err, IsNil)

			sd := NewStringDatum(tt.expected)
			var expected Datum
			expected, err = sd.ConvertTo(sc, ft)
			c.Assert(err, IsNil)

			var cmp int
			cmp, err = obtain.CompareDatum(sc, &expected)
			c.Assert(err, IsNil)
			c.Assert(cmp, Equals, 0)
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (ts *testDatumSuite) TestIsNull(c *C) {
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
		testIsNull(c, tt.data, tt.isnull)
	}
}

func testIsNull(c *C, data interface{}, isnull bool) {
	d := NewDatum(data)
	c.Assert(d.IsNull(), Equals, isnull, Commentf("data: %v, isnull: %v", data, isnull))
}

func (ts *testDatumSuite) TestToBytes(c *C) {
	tests := []struct {
		a   Datum
		out []byte
	}{
		{NewIntDatum(1), []byte("1")},
		{NewDecimalDatum(NewDecFromInt(1)), []byte("1")},
		{NewFloat64Datum(1.23), []byte("1.23")},
		{NewStringDatum("abc"), []byte("abc")},
	}
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, tt := range tests {
		bin, err := tt.a.ToBytes()
		c.Assert(err, IsNil)
		c.Assert(bin, BytesEquals, tt.out)
	}
}

func (ts *testDatumSuite) TestComputePlusAndMinus(c *C) {
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
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareDatum(sc, &tt.plus)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, %#v, expect:%#v, %#v", ith, got, got.x, tt.plus, tt.plus.x))
	}
}

func (ts *testDatumSuite) TestCloneDatum(c *C) {
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
		c.Assert(err, IsNil)
		c.Assert(res, Equals, 0)
		if tt.b != nil {
			c.Assert(&tt.b[0], Not(Equals), &tt1.b[0])
		}
	}
}

func newTypeWithFlag(tp byte, flag uint) *FieldType {
	t := NewFieldType(tp)
	t.Flag |= flag
	return t
}

func newMyDecimal(val string, c *C) *MyDecimal {
	t := MyDecimal{}
	err := t.FromString([]byte(val))
	c.Assert(err, IsNil)
	return &t
}

func newRetTypeWithFlenDecimal(tp byte, flen int, decimal int) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    flen,
		Decimal: decimal,
	}
}

func (ts *testDatumSuite) TestEstimatedMemUsage(c *C) {
	b := []byte{'a', 'b', 'c', 'd'}
	enum := Enum{Name: "a", Value: 1}
	datumArray := []Datum{
		NewIntDatum(1),
		NewFloat64Datum(1.0),
		NewFloat32Datum(1.0),
		NewStringDatum(string(b)),
		NewBytesDatum(b),
		NewDecimalDatum(newMyDecimal("1234.1234", c)),
		NewMysqlEnumDatum(enum),
	}
	bytesConsumed := 10 * (len(datumArray)*sizeOfEmptyDatum +
		sizeOfMyDecimal +
		len(b)*2 +
		len(hack.Slice(enum.Name)))
	c.Assert(int(EstimatedMemUsage(datumArray, 10)), Equals, bytesConsumed)
}

func (ts *testDatumSuite) TestChangeReverseResultByUpperLowerBound(c *C) {
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
			NewDecimalDatum(newMyDecimal("2", c)),
			newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3),
			Ceiling,
		},
		{
			NewIntDatum(1),
			NewDecimalDatum(newMyDecimal("1", c)),
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
			NewDecimalDatum(newMyDecimal(strconv.FormatInt(math.MaxInt64, 10), c)),
			newRetTypeWithFlenDecimal(mysql.TypeNewDecimal, 30, 3),
			Floor,
		},
	}
	for ith, test := range testData {
		reverseRes, err := ChangeReverseResultByUpperLowerBound(sc, test.retType, test.a, test.roundType)
		c.Assert(err, IsNil)
		var cmp int
		cmp, err = reverseRes.CompareDatum(sc, &test.res)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, reverseRes, test.res))
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

func BenchmarkCompareDatum(b *testing.B) {
	vals, vals1 := prepareCompareDatums()
	sc := new(stmtctx.StatementContext)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, v := range vals {
			v.CompareDatum(sc, &vals1[j])
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
