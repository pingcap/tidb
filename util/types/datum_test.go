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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
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
	sc := new(variable.StatementContext)
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
	testDatumToBool(c, Hex{Value: 0}, 0)
	testDatumToBool(c, Bit{Value: 0, Width: 8}, 0)
	testDatumToBool(c, Enum{Name: "a", Value: 1}, 1)
	testDatumToBool(c, Set{Name: "a", Value: 1}, 1)

	t, err := ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 6)
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
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	_, err = d.ToBool(sc)
	c.Assert(err, NotNil)
}

func (ts *testDatumSuite) TestEqualDatums(c *C) {
	testCases := []struct {
		a    []interface{}
		b    []interface{}
		same bool
	}{
		// Positive cases
		{[]interface{}{1}, []interface{}{1}, true},
		{[]interface{}{1, "aa"}, []interface{}{1, "aa"}, true},
		{[]interface{}{1, "aa", 1}, []interface{}{1, "aa", 1}, true},

		// Negative cases
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
	for _, t := range testCases {
		testEqualDatums(c, t.a, t.b, t.same)
	}
}

func testEqualDatums(c *C, a []interface{}, b []interface{}, same bool) {
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	res, err := EqualDatums(sc, MakeDatums(a), MakeDatums(b))
	c.Assert(err, IsNil)
	c.Assert(res, Equals, same, Commentf("a: %v, b: %v", a, b))
}

func testDatumToInt64(c *C, val interface{}, expect int64) {
	d := NewDatum(val)
	sc := new(variable.StatementContext)
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
	testDatumToInt64(c, Hex{Value: 100}, int64(100))
	testDatumToInt64(c, Bit{Value: 100, Width: 8}, int64(100))
	testDatumToInt64(c, Enum{Name: "a", Value: 1}, int64(1))
	testDatumToInt64(c, Set{Name: "a", Value: 1}, int64(1))

	t, err := ParseTime("2011-11-10 11:11:11.999999", mysql.TypeTimestamp, 0)
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
	sc := new(variable.StatementContext)
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

func (ts *testDatumSuite) TestIsNull(c *C) {
	testCases := []struct {
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
	for _, t := range testCases {
		testIsNull(c, t.data, t.isnull)
	}
}

func testIsNull(c *C, data interface{}, isnull bool) {
	d := NewDatum(data)
	c.Assert(d.IsNull(), Equals, isnull, Commentf("data: %v, isnull: %v", data, isnull))
}

func (ts *testDatumSuite) TestCoerceDatum(c *C) {
	testCases := []struct {
		a    Datum
		b    Datum
		kind byte
	}{
		{NewIntDatum(1), NewIntDatum(1), KindInt64},
		{NewUintDatum(1), NewDecimalDatum(NewDecFromInt(1)), KindMysqlDecimal},
		{NewFloat64Datum(1), NewDecimalDatum(NewDecFromInt(1)), KindFloat64},
		{NewFloat64Datum(1), NewFloat64Datum(1), KindFloat64},
	}
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	for _, ca := range testCases {
		x, y, err := CoerceDatum(sc, ca.a, ca.b)
		c.Check(err, IsNil)
		c.Check(x.Kind(), Equals, y.Kind())
		c.Check(x.Kind(), Equals, ca.kind)
	}
}

func (ts *testDatumSuite) TestBitOps(c *C) {
	testCases := []struct {
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

	for _, ca := range testCases {
		var (
			result Datum
			err    error
		)
		sc := new(variable.StatementContext)
		sc.IgnoreTruncate = true
		switch ca.bitop {
		case "And":
			result, err = ComputeBitAnd(sc, ca.a, ca.b)
		case "Or":
			result, err = ComputeBitOr(sc, ca.a, ca.b)
		case "Not":
			result, err = ComputeBitNeg(sc, ca.a)
		case "Xor":
			result, err = ComputeBitXor(sc, ca.a, ca.b)
		case "LeftShift":
			result, err = ComputeLeftShift(sc, ca.a, ca.b)
		case "RightShift":
			result, err = ComputeRightShift(sc, ca.a, ca.b)
		}
		c.Check(err, Equals, nil)
		c.Assert(result.GetUint64(), Equals, ca.result.GetUint64())
	}
}

func (ts *testDatumSuite) TestToBytes(c *C) {
	testCases := []struct {
		a   Datum
		out []byte
	}{
		{NewIntDatum(1), []byte("1")},
		{NewDecimalDatum(NewDecFromInt(1)), []byte("1")},
		{NewFloat64Datum(1.23), []byte("1.23")},
		{NewStringDatum("abc"), []byte("abc")},
	}
	sc := new(variable.StatementContext)
	sc.IgnoreTruncate = true
	for _, ca := range testCases {
		bin, err := ca.a.ToBytes()
		c.Assert(err, IsNil)
		c.Assert(bin, BytesEquals, ca.out)
	}
}
