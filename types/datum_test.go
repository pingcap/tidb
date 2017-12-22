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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types/json"
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
		d.SetValue(val)
		x := d.GetValue()
		c.Assert(x, DeepEquals, val)
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
	testDatumToBool(c, "", 0)
	testDatumToBool(c, "0.1", 0)
	testDatumToBool(c, []byte{}, 0)
	testDatumToBool(c, []byte("0.1"), 0)
	testDatumToBool(c, NewBinaryLiteralFromUint(0, -1), 0)
	testDatumToBool(c, Enum{Name: "a", Value: 1}, 1)
	testDatumToBool(c, Set{Name: "a", Value: 1}, 1)

	t, err := ParseTime(nil, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testDatumToBool(c, t, 1)

	td, err := ParseDuration("11:11:11.999999", 6)
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

	t, err := ParseTime(nil, "2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 0)
	c.Assert(err, IsNil)
	testDatumToInt64(c, t, int64(20111110111112))

	td, err := ParseDuration("11:11:11.999999", 6)
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

	datum.SetString("281.37")
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

// mustParseTimeIntoDatum is similar to ParseTime but panic if any error occurs.
func mustParseTimeIntoDatum(s string, tp byte, fsp int) (d Datum) {
	t, err := ParseTime(nil, s, tp, fsp)
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

func (ts *testDatumSuite) TestCoerceDatum(c *C) {
	tests := []struct {
		a    Datum
		b    Datum
		kind byte
	}{
		{NewIntDatum(1), NewIntDatum(1), KindInt64},
		{NewUintDatum(1), NewDecimalDatum(NewDecFromInt(1)), KindMysqlDecimal},
		{NewFloat64Datum(1), NewDecimalDatum(NewDecFromInt(1)), KindFloat64},
		{NewFloat64Datum(1), NewFloat64Datum(1), KindFloat64},
	}
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, tt := range tests {
		x, y, err := CoerceDatum(sc, tt.a, tt.b)
		c.Check(err, IsNil)
		c.Check(x.Kind(), Equals, y.Kind())
		c.Check(x.Kind(), Equals, tt.kind)
	}
}

func (ts *testDatumSuite) TestBitOps(c *C) {
	tests := []struct {
		a      Datum
		b      Datum
		bitop  string // bitwise operator
		result Datum
	}{
		// And
		{NewIntDatum(341), NewIntDatum(170), "And", NewIntDatum(0)},
		{NewIntDatum(341), NewUintDatum(170), "And", NewUintDatum(0)},
		{NewUintDatum(341), NewUintDatum(170), "And", NewUintDatum(0)},
		{NewIntDatum(-1), NewFloat64Datum(-2.5), "And", NewUintDatum(18446744073709551613)},
		{NewFloat64Datum(-1.4), NewFloat64Datum(-2.4), "And", NewUintDatum(18446744073709551614)},
		{NewFloat64Datum(-1.5), NewFloat64Datum(-2.5), "And", NewUintDatum(18446744073709551612)},
		// Or
		{NewIntDatum(341), NewIntDatum(170), "Or", NewIntDatum(511)},
		{NewIntDatum(341), NewUintDatum(170), "Or", NewUintDatum(511)},
		{NewUintDatum(341), NewUintDatum(170), "Or", NewUintDatum(511)},
		{NewIntDatum(-1), NewFloat64Datum(-2.5), "Or", NewUintDatum(18446744073709551615)},
		{NewFloat64Datum(-1.4), NewFloat64Datum(-2.4), "Or", NewUintDatum(18446744073709551615)},
		{NewFloat64Datum(-1.5), NewFloat64Datum(-2.5), "Or", NewUintDatum(18446744073709551615)},
		// Xor
		{NewIntDatum(341), NewIntDatum(170), "Xor", NewUintDatum(511)},
		{NewIntDatum(341), NewUintDatum(170), "Xor", NewUintDatum(511)},
		{NewUintDatum(341), NewUintDatum(170), "Xor", NewUintDatum(511)},
		{NewIntDatum(-1), NewFloat64Datum(-2.5), "Xor", NewUintDatum(2)},
		{NewFloat64Datum(-1.4), NewFloat64Datum(-2.4), "Xor", NewUintDatum(1)},
		{NewFloat64Datum(-1.5), NewFloat64Datum(-2.5), "Xor", NewUintDatum(3)},
		// Not
		{NewIntDatum(-1), Datum{}, "Not", NewUintDatum(0)},
		{NewIntDatum(1), Datum{}, "Not", NewUintDatum(18446744073709551614)},
		{NewFloat64Datum(-0.5), Datum{}, "Not", NewUintDatum(0)},
		{NewFloat64Datum(-0.4), Datum{}, "Not", NewUintDatum(18446744073709551615)},
		{NewUintDatum(18446744073709551615), Datum{}, "Not", NewUintDatum(0)},
		// LeftShift
		{NewIntDatum(-1), NewIntDatum(1), "LeftShift", NewUintDatum(18446744073709551614)},
		{NewIntDatum(-1), NewIntDatum(-1), "LeftShift", NewUintDatum(0)},
		{NewIntDatum(1), NewIntDatum(10), "LeftShift", NewUintDatum(1024)},
		{NewFloat64Datum(-1.4), NewFloat64Datum(2.4), "LeftShift", NewUintDatum(18446744073709551612)},
		{NewFloat64Datum(-1.4), NewFloat64Datum(2.5), "LeftShift", NewUintDatum(18446744073709551608)},
		// RightShift
		{NewUintDatum(18446744073709551614), NewIntDatum(1), "RightShift", NewUintDatum(9223372036854775807)},
		{NewIntDatum(-1), NewIntDatum(-1), "RightShift", NewUintDatum(0)},
		{NewIntDatum(1024), NewIntDatum(10), "RightShift", NewUintDatum(1)},
		{NewFloat64Datum(1024), NewFloat64Datum(10.4), "RightShift", NewUintDatum(1)},
		{NewFloat64Datum(1024), NewFloat64Datum(10.5), "RightShift", NewUintDatum(0)},
	}

	for _, tt := range tests {
		var (
			result Datum
			err    error
		)
		sc := new(stmtctx.StatementContext)
		sc.IgnoreTruncate = true
		switch tt.bitop {
		case "And":
			result, err = ComputeBitAnd(sc, tt.a, tt.b)
		case "Or":
			result, err = ComputeBitOr(sc, tt.a, tt.b)
		case "Not":
			result, err = ComputeBitNeg(sc, tt.a)
		case "Xor":
			result, err = ComputeBitXor(sc, tt.a, tt.b)
		case "LeftShift":
			result, err = ComputeLeftShift(sc, tt.a, tt.b)
		case "RightShift":
			result, err = ComputeRightShift(sc, tt.a, tt.b)
		}
		c.Check(err, Equals, nil)
		c.Assert(result.GetUint64(), Equals, tt.result.GetUint64())
	}
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

func mustParseDurationDatum(str string, fsp int) Datum {
	dur, err := ParseDuration(str, fsp)
	if err != nil {
		panic(err)
	}
	return NewDurationDatum(dur)
}

func (ts *testDatumSuite) TestCoerceArithmetic(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		input  Datum
		expect Datum
		hasErr bool
	}{
		{NewStringDatum("12.5"), NewFloat64Datum(12.5), false},
		{NewStringDatum("asdf"), Datum{}, true},
		{NewBytesDatum([]byte("12.527")), NewFloat64Datum(12.527), false},
		{mustParseTimeIntoDatum("2017-07-18 17:21:42.321", mysql.TypeTimestamp, 3), NewDatum(NewDecFromStringForTest("20170718172142.321")), false},
		{mustParseTimeIntoDatum("2017-07-18 17:21:42.32172", mysql.TypeDatetime, 0), NewIntDatum(20170718172142), false},
		{mustParseDurationDatum("10:10:10", 0), NewIntDatum(101010), false},
		{mustParseDurationDatum("10:10:10.100", 3), NewDatum(NewDecFromStringForTest("101010.100")), false},
		{NewBinaryLiteralDatum(NewBinaryLiteralFromUint(0x4D7953514C, -1)), NewUintDatum(332747985228), false},
		{NewBinaryLiteralDatum(NewBinaryLiteralFromUint(1, -1)), NewUintDatum(1), false},
		{NewDatum(Enum{"xxx", 1}), NewFloat64Datum(1), false},
		{NewDatum(Set{"xxx", 1}), NewFloat64Datum(1), false},
		{NewIntDatum(5), NewIntDatum(5), false},
		{NewFloat64Datum(5.5), NewFloat64Datum(5.5), false},
	}

	for _, tt := range tests {
		got, err := CoerceArithmetic(sc, tt.input)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareDatum(sc, &tt.expect)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("got:%#v, expect:%#v", got, tt.expect))
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
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, got, tt.plus))

		got, err = ComputeMinus(tt.a, tt.b)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err = got.CompareDatum(sc, &tt.minus)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, got, tt.minus))
	}
}

func (ts *testDatumSuite) TestComputeMul(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		a      Datum
		b      Datum
		expect Datum
		hasErr bool
	}{
		{NewIntDatum(72), NewIntDatum(28), NewIntDatum(2016), false},
		{NewIntDatum(72), NewUintDatum(28), NewIntDatum(2016), false},
		{NewUintDatum(72), NewUintDatum(28), NewUintDatum(2016), false},
		{NewUintDatum(72), NewIntDatum(28), NewUintDatum(2016), false},
		{NewFloat64Datum(72.0), NewFloat64Datum(28.0), NewFloat64Datum(2016.0), false},
		{NewDecimalDatum(NewDecFromStringForTest("72.5")), NewDecimalDatum(NewDecFromInt(3)), NewDecimalDatum(NewDecFromStringForTest("217.5")), false},
		{NewIntDatum(72), NewFloat64Datum(42), Datum{}, true},
		{NewStringDatum("abcd"), NewIntDatum(42), Datum{}, true},
	}

	for ith, tt := range tests {
		got, err := ComputeMul(tt.a, tt.b)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareDatum(sc, &tt.expect)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, got, tt.expect))
	}
}

func (ts *testDatumSuite) TestComputeDiv(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		a      Datum
		b      Datum
		expect Datum
		hasErr bool
	}{
		{NewFloat64Datum(2016.0), NewFloat64Datum(72.0), NewFloat64Datum(28.0), false},
		{NewFloat64Datum(2016.0), NewIntDatum(0), Datum{}, false},
		{NewFloat64Datum(2016.0), NewStringDatum("a4"), Datum{}, true},
		{NewIntDatum(2016.0), NewIntDatum(28.0), NewDecimalDatum(NewDecFromInt(72)), false},
		{NewUintDatum(2016), NewUintDatum(28), NewDecimalDatum(NewDecFromInt(72)), false},
		{NewDecimalDatum(NewDecFromStringForTest("217.5")), NewDecimalDatum(NewDecFromInt(3)), NewDecimalDatum(NewDecFromStringForTest("72.5")), false},
		{NewIntDatum(72), NewFloat64Datum(42), NewDecimalDatum(NewDecFromStringForTest("1.714285714")), false},
		{NewIntDatum(72), NewFloat64Datum(0), Datum{}, false}, // Div 0 has no error, but no result.
		{NewStringDatum("abcd"), NewIntDatum(42), NewDecimalDatum(NewDecFromStringForTest("0")), false},
		{NewStringDatum("abcd"), NewStringDatum("a4"), Datum{}, true},
	}

	for ith, tt := range tests {
		got, err := ComputeDiv(sc, tt.a, tt.b)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareDatum(sc, &tt.expect)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, got, tt.expect))
	}
}

func (ts *testDatumSuite) TestComputeMod(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		a      Datum
		b      Datum
		expect Datum
		hasErr bool
	}{
		{NewIntDatum(2018), NewIntDatum(28), NewIntDatum(2), false},
		{NewIntDatum(2018), NewUintDatum(0), Datum{}, false},
		{NewIntDatum(2018), NewIntDatum(0), Datum{}, false},
		{NewIntDatum(2018), NewUintDatum(28), NewIntDatum(2), false},
		{NewIntDatum(-2018), NewUintDatum(28), NewIntDatum(-2), false},
		{NewUintDatum(2018), NewUintDatum(28), NewUintDatum(2), false},
		{NewUintDatum(2018), NewUintDatum(0), Datum{}, false},
		{NewUintDatum(2018), NewIntDatum(-28), NewIntDatum(2), false},
		{NewUintDatum(2018), NewIntDatum(28), NewIntDatum(2), false},
		{NewUintDatum(2018), NewIntDatum(0), Datum{}, false},
		{NewFloat64Datum(2018.0), NewFloat64Datum(72.0), NewFloat64Datum(2.0), false},
		{NewDecimalDatum(NewDecFromStringForTest("217.5")), NewDecimalDatum(NewDecFromInt(3)), NewDecimalDatum(NewDecFromStringForTest("1.5")), false},
		{NewDecimalDatum(NewDecFromStringForTest("217.5")), NewDecimalDatum(NewDecFromInt(0)), Datum{}, false},
		{NewIntDatum(72), NewFloat64Datum(42), Datum{}, true},
		{NewStringDatum("abcd"), NewIntDatum(42), Datum{}, true},
	}

	for ith, tt := range tests {
		got, err := ComputeMod(sc, tt.a, tt.b)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareDatum(sc, &tt.expect)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, got, tt.expect))
	}
}

func (ts *testDatumSuite) TestComputeIntDiv(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		a      Datum
		b      Datum
		expect Datum
		hasErr bool
	}{
		{NewIntDatum(2018), NewIntDatum(28), NewIntDatum(72), false},
		{NewUintDatum(2018), NewUintDatum(28), NewUintDatum(72), false},
		{NewUintDatum(2018), NewIntDatum(28), NewUintDatum(72), false},
		{NewIntDatum(2018), NewUintDatum(28), NewIntDatum(72), false},
		{NewFloat64Datum(2018.5), NewFloat64Datum(72.0), NewIntDatum(28), false},
		{NewDecimalDatum(NewDecFromStringForTest("217.5")), NewDecimalDatum(NewDecFromInt(3)), NewIntDatum(72), false},
		{NewIntDatum(72), NewFloat64Datum(42), NewIntDatum(1), false},
		{NewStringDatum("abcd"), NewIntDatum(42), Datum{}, true},
		{NewFloat64Datum(2018.5), NewStringDatum("abcd"), Datum{}, true},
		{NewFloat64Datum(2018.5), NewIntDatum(0), Datum{}, false},
	}

	for ith, tt := range tests {
		got, err := ComputeIntDiv(sc, tt.a, tt.b)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareDatum(sc, &tt.expect)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, got, tt.expect))
	}
}

func (ts *testDatumSuite) TestCopyDatum(c *C) {
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
		tt1 := CopyDatum(tt)
		res, err := tt.CompareDatum(sc, &tt1)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, 0)
		if tt.b != nil {
			c.Assert(&tt.b[0], Not(Equals), &tt1.b[0])
		}
	}
}
