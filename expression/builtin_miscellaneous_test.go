// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/testutil"
)

func (s *testEvaluatorSuite) TestInetAton(c *C) {
	tbl := []struct {
		Input    interface{}
		Expected interface{}
	}{
		{"", nil},
		{nil, nil},
		{"255.255.255.255", 4294967295},
		{"0.0.0.0", 0},
		{"127.0.0.1", 2130706433},
		{"0.0.0.256", nil},
		{"113.14.22.3", 1896748547},
		{"127", 127},
		{"127.255", 2130706687},
		{"127,256", nil},
		{"127.2.1", 2130837505},
		{"123.2.1.", nil},
		{"127.0.0.1.1", nil},
	}

	dtbl := tblToDtbl(tbl)
	fc := funcs[ast.InetAton]
	for _, t := range dtbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestIsIPv4(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"192.168.1.1", 1},
		{"255.255.255.255", 1},
		{"10.t.255.255", 0},
		{"10.1.2.3.4", 0},
		{"2001:250:207:0:0:eef2::1", 0},
		{"::ffff:1.2.3.4", 0},
		{"1...1", 0},
		{"192.168.1.", 0},
		{".168.1.2", 0},
		{"168.1.2", 0},
		{"1.2.3.4.5", 0},
	}
	fc := funcs[ast.IsIPv4]
	for _, test := range tests {
		ip := types.NewStringDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}
	// test NULL input for is_ipv4
	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(0))
}

func (s *testEvaluatorSuite) TestUUID(c *C) {
	f, err := newFunctionForTest(s.ctx, ast.UUID)
	c.Assert(err, IsNil)
	d, err := f.Eval(chunk.Row{})
	c.Assert(err, IsNil)
	parts := strings.Split(d.GetString(), "-")
	c.Assert(len(parts), Equals, 5)
	for i, p := range parts {
		switch i {
		case 0:
			c.Assert(len(p), Equals, 8)
		case 1:
			c.Assert(len(p), Equals, 4)
		case 2:
			c.Assert(len(p), Equals, 4)
		case 3:
			c.Assert(len(p), Equals, 4)
		case 4:
			c.Assert(len(p), Equals, 12)
		}
	}
	_, err = funcs[ast.UUID].getFunction(s.ctx, s.datumsToConstants(nil))
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestAnyValue(c *C) {
	tbl := []struct {
		arg interface{}
		ret interface{}
	}{
		{nil, nil},
		{1234, 1234},
		{-0x99, -0x99},
		{3.1415926, 3.1415926},
		{"Hello, World", "Hello, World"},
	}
	for _, t := range tbl {
		fc := funcs[ast.AnyValue]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestIsIPv6(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"2001:250:207:0:0:eef2::1", 1},
		{"2001:0250:0207:0001:0000:0000:0000:ff02", 1},
		{"2001:250:207::eff2::1，", 0},
		{"192.168.1.1", 0},
		{"::ffff:1.2.3.4", 1},
	}
	fc := funcs[ast.IsIPv6]
	for _, test := range tests {
		ip := types.NewStringDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}
	// test NULL input for is_ipv6
	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(0))
}

func (s *testEvaluatorSuite) TestInetNtoa(c *C) {
	tests := []struct {
		ip     int
		expect interface{}
	}{
		{167773449, "10.0.5.9"},
		{2063728641, "123.2.0.1"},
		{0, "0.0.0.0"},
		{545460846593, nil},
		{-1, nil},
		{math.MaxUint32, "255.255.255.255"},
	}
	fc := funcs[ast.InetNtoa]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}

	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestInet6NtoA(c *C) {
	tests := []struct {
		ip     []byte
		expect interface{}
	}{
		// Success cases
		{[]byte{0x00, 0x00, 0x00, 0x00}, "0.0.0.0"},
		{[]byte{0x0A, 0x00, 0x05, 0x09}, "10.0.5.9"},
		{[]byte{0xFD, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5A, 0x55, 0xCA, 0xFF, 0xFE,
			0xFA, 0x90, 0x89}, "fdfe::5a55:caff:fefa:9089"},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x01,
			0x02, 0x03, 0x04}, "::ffff:1.2.3.4"},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF}, "::ffff:255.255.255.255"},
		// Fail cases
		{[]byte{}, nil},                 // missing bytes
		{[]byte{0x0A, 0x00, 0x05}, nil}, // missing a byte ipv4
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF}, nil}, // missing a byte ipv6
	}
	fc := funcs[ast.Inet6Ntoa]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}

	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestInet6AtoN(c *C) {
	tests := []struct {
		ip     string
		expect interface{}
	}{
		{"0.0.0.0", []byte{0x00, 0x00, 0x00, 0x00}},
		{"10.0.5.9", []byte{0x0A, 0x00, 0x05, 0x09}},
		{"fdfe::5a55:caff:fefa:9089", []byte{0xFD, 0xFE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5A, 0x55, 0xCA, 0xFF, 0xFE, 0xFA, 0x90, 0x89}},
		{"::ffff:1.2.3.4", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0x01, 0x02, 0x03, 0x04}},
		{"", nil},
		{"Not IP address", nil},
		{"::ffff:255.255.255.255", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	}
	fc := funcs[ast.Inet6Aton]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}

	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestIsIPv4Mapped(c *C) {
	tests := []struct {
		ip     []byte
		expect interface{}
	}{
		{[]byte{}, 0},
		{[]byte{0x10, 0x10, 0x10, 0x10}, 0},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xff, 0xff, 0x1, 0x2, 0x3, 0x4}, 1},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0x1, 0x2, 0x3, 0x4}, 0},
		{[]byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6}, 0},
	}
	fc := funcs[ast.IsIPv4Mapped]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}

	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(int64(0)))
}

func (s *testEvaluatorSuite) TestIsIPv4Compat(c *C) {
	tests := []struct {
		ip     []byte
		expect interface{}
	}{
		{[]byte{}, 0},
		{[]byte{0x10, 0x10, 0x10, 0x10}, 0},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4}, 1},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4}, 0},
		{[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0xff, 0xff, 0x1, 0x2, 0x3, 0x4}, 0},
		{[]byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6}, 0},
	}
	fc := funcs[ast.IsIPv4Compat]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{ip}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
	}

	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(0))
}

func (s *testEvaluatorSuite) TestNameConst(c *C) {
	dec := types.NewDecFromFloatForTest(123.123)
	tm := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 6)
	du := types.Duration{Duration: time.Duration(12*time.Hour + 1*time.Minute + 1*time.Second), Fsp: types.DefaultFsp}
	cases := []struct {
		colName string
		arg     interface{}
		isNil   bool
		asserts func(d types.Datum)
	}{
		{"test_int", 3, false, func(d types.Datum) {
			c.Assert(d.GetInt64(), Equals, int64(3))
		}},
		{"test_float", 3.14159, false, func(d types.Datum) {
			c.Assert(d.GetFloat64(), Equals, 3.14159)
		}},
		{"test_string", "TiDB", false, func(d types.Datum) {
			c.Assert(d.GetString(), Equals, "TiDB")
		}},
		{"test_null", nil, true, func(d types.Datum) {
			c.Assert(d.Kind(), Equals, types.KindNull)
		}},
		{"test_decimal", dec, false, func(d types.Datum) {
			c.Assert(d.GetMysqlDecimal().String(), Equals, dec.String())
		}},
		{"test_time", tm, false, func(d types.Datum) {
			c.Assert(d.GetMysqlTime().String(), Equals, tm.String())
		}},
		{"test_duration", du, false, func(d types.Datum) {
			c.Assert(d.GetMysqlDuration().String(), Equals, du.String())
		}},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.NameConst, s.primitiveValsToConstants([]interface{}{t.colName, t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		c.Assert(err, IsNil)
		t.asserts(d)
	}
}
