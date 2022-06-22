// Copyright 2021 PingCAP, Inc.
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

package expression

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestInetAton(t *testing.T) {
	ctx := createContext(t)
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
	for _, tt := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Input"]))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if tt["Expected"][0].IsNull() && !tt["Input"][0].IsNull() {
			require.True(t, terror.ErrorEqual(err, errWrongValueForType))
		} else {
			require.NoError(t, err)
			testutil.DatumEqual(t, tt["Expected"][0], d)
		}
	}
}

func TestIsIPv4(t *testing.T) {
	ctx := createContext(t)
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
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}
	// test NULL input for is_ipv4
	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(0), r)
}

func TestIsUUID(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		uuid   string
		expect interface{}
	}{
		{"6ccd780c-baba-1026-9564-5b8c656024db", 1},
		{"6CCD780C-BABA-1026-9564-5B8C656024DB", 1},
		{"6ccd780cbaba102695645b8c656024db", 1},
		{"{6ccd780c-baba-1026-9564-5b8c656024db}", 1},
		{"6ccd780c-baba-1026-9564-5b8c6560", 0},
		{"6CCD780C-BABA-1026-9564-5B8C656024DQ", 0},
		// This is a bug in google/uuid#60
		{"{99a9ad03-5298-11ec-8f5c-00ff90147ac3*", 1},
		// This is a format google/uuid support, while mysql doesn't
		{"urn:uuid:99a9ad03-5298-11ec-8f5c-00ff90147ac3", 1},
	}

	fc := funcs[ast.IsUUID]
	for _, test := range tests {
		uuid := types.NewStringDatum(test.uuid)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{uuid}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}

	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, r.IsNull())
}

func TestUUID(t *testing.T) {
	ctx := createContext(t)
	f, err := newFunctionForTest(ctx, ast.UUID)
	require.NoError(t, err)
	d, err := f.Eval(chunk.Row{})
	require.NoError(t, err)
	parts := strings.Split(d.GetString(), "-")
	require.Equal(t, 5, len(parts))
	for i, p := range parts {
		switch i {
		case 0:
			require.Equal(t, 8, len(p))
		case 1:
			require.Equal(t, 4, len(p))
		case 2:
			require.Equal(t, 4, len(p))
		case 3:
			require.Equal(t, 4, len(p))
		case 4:
			require.Equal(t, 12, len(p))
		}
	}
	_, err = funcs[ast.UUID].getFunction(ctx, datumsToConstants(nil))
	require.NoError(t, err)
}

func TestAnyValue(t *testing.T) {
	ctx := createContext(t)
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
	for _, tt := range tbl {
		fc := funcs[ast.AnyValue]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.arg)))
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.ret), r)
	}
}

func TestIsIPv6(t *testing.T) {
	ctx := createContext(t)
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
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}
	// test NULL input for is_ipv6
	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(0), r)
}

func TestInetNtoa(t *testing.T) {
	ctx := createContext(t)
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
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}

	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, r.IsNull())
}

func TestInet6NtoA(t *testing.T) {
	ctx := createContext(t)
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
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}

	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, r.IsNull())
}

func TestInet6AtoN(t *testing.T) {
	ctx := createContext(t)
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
		{"1.0002.3.4", nil},
		{"1.2.256", nil},
		{"::ffff:255.255.255.255", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	}
	fc := funcs[ast.Inet6Aton]
	for _, test := range tests {
		ip := types.NewDatum(test.ip)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		expect := types.NewDatum(test.expect)
		if expect.IsNull() {
			require.True(t, terror.ErrorEqual(err, errWrongValueForType))
		} else {
			require.NoError(t, err)
			testutil.DatumEqual(t, expect, result)
		}
	}

	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, r.IsNull())
}

func TestIsIPv4Mapped(t *testing.T) {
	ctx := createContext(t)
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
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}

	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(int64(0)), r)
}

func TestIsIPv4Compat(t *testing.T) {
	ctx := createContext(t)
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
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{ip}))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(test.expect), result)
	}

	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(0), r)
}

func TestNameConst(t *testing.T) {
	ctx := createContext(t)
	dec := types.NewDecFromFloatForTest(123.123)
	tm := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 6)
	du := types.Duration{Duration: 12*time.Hour + 1*time.Minute + 1*time.Second, Fsp: types.DefaultFsp}
	cases := []struct {
		colName string
		arg     interface{}
		isNil   bool
		asserts func(d types.Datum)
	}{
		{"test_int", 3, false, func(d types.Datum) {
			require.Equal(t, int64(3), d.GetInt64())
		}},
		{"test_float", 3.14159, false, func(d types.Datum) {
			require.Equal(t, 3.14159, d.GetFloat64())
		}},
		{"test_string", "TiDB", false, func(d types.Datum) {
			require.Equal(t, "TiDB", d.GetString())
		}},
		{"test_null", nil, true, func(d types.Datum) {
			require.Equal(t, types.KindNull, d.Kind())
		}},
		{"test_decimal", dec, false, func(d types.Datum) {
			require.Equal(t, dec.String(), d.GetMysqlDecimal().String())
		}},
		{"test_time", tm, false, func(d types.Datum) {
			require.Equal(t, tm.String(), d.GetMysqlTime().String())
		}},
		{"test_duration", du, false, func(d types.Datum) {
			require.Equal(t, du.String(), d.GetMysqlDuration().String())
		}},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.NameConst, primitiveValsToConstants(ctx, []interface{}{c.colName, c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		c.asserts(d)
	}
}

func TestUUIDToBin(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args       []interface{}
		expect     interface{}
		isNil      bool
		getWarning bool
		getError   bool
	}{
		{
			[]interface{}{"6ccd780c-baba-1026-9564-5b8c656024db"},
			[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			false,
			false,
		},
		{
			[]interface{}{"6CCD780C-BABA-1026-9564-5B8C656024DB"},
			[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			false,
			false,
		},
		{
			[]interface{}{"6ccd780cbaba102695645b8c656024db"},
			[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			false,
			false,
		},
		{
			[]interface{}{"{6ccd780c-baba-1026-9564-5b8c656024db}"},
			[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			false,
			false,
		},
		{
			[]interface{}{"6ccd780c-baba-1026-9564-5b8c656024db", 0},
			[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			false,
			false,
		},
		{
			[]interface{}{"6ccd780c-baba-1026-9564-5b8c656024db", 1},
			[]byte{0x10, 0x26, 0xBA, 0xBA, 0x6C, 0xCD, 0x78, 0x0C, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			false,
			false,
		},
		{
			[]interface{}{"6ccd780c-baba-1026-9564-5b8c656024db", "a"},
			[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB},
			false,
			true,
			false,
		},
		{
			[]interface{}{"6ccd780c-baba-1026-9564-5b8c6560"},
			[]byte{},
			false,
			false,
			true,
		},
		{
			[]interface{}{nil},
			[]byte{},
			true,
			false,
			false,
		},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.UUIDToBin, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)

		result, err := f.Eval(chunk.Row{})
		if test.getError {
			require.Error(t, err)
		} else if test.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				testutil.DatumEqual(t, types.NewDatum(test.expect), result)
			}
		}
	}

	_, err := funcs[ast.UUIDToBin].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestBinToUUID(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args       []interface{}
		expect     string
		isNil      bool
		getWarning bool
		getError   bool
	}{
		{
			[]interface{}{[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB}},
			"6ccd780c-baba-1026-9564-5b8c656024db",
			false,
			false,
			false,
		},
		{
			[]interface{}{[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB}, 1},
			"baba1026-780c-6ccd-9564-5b8c656024db",
			false,
			false,
			false,
		},
		{
			[]interface{}{[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60, 0x24, 0xDB}, "a"},
			"6ccd780c-baba-1026-9564-5b8c656024db",
			false,
			true,
			false,
		},
		{
			[]interface{}{[]byte{0x6C, 0xCD, 0x78, 0x0C, 0xBA, 0xBA, 0x10, 0x26, 0x95, 0x64, 0x5B, 0x8C, 0x65, 0x60}},
			"",
			false,
			false,
			true,
		},
		{
			[]interface{}{nil},
			"",
			true,
			false,
			false,
		},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.BinToUUID, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)

		result, err := f.Eval(chunk.Row{})
		if test.getError {
			require.Error(t, err)
		} else if test.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				require.Equal(t, test.expect, result.GetString())
			}
		}
	}

	_, err := funcs[ast.BinToUUID].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestTidbShard(t *testing.T) {
	ctx := createContext(t)

	fc := funcs[ast.TiDBShard]

	// tidb_shard(-1) == 81, ......
	args := makeDatums([]int{-1, 0, 1, 9999999999999999})
	res := makeDatums([]int{81, 167, 214, 63})
	for i, arg := range args {
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{arg}))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, res[i], d)
	}

	// tidb_shard("string") always return 167
	args2 := makeDatums([]string{"abc", "ope", "wopddd"})
	res2 := makeDatums([]int{167})
	for _, arg := range args2 {
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{arg}))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, res2[0], d)
	}

	args3 := makeDatums([]int{-1, 0, 1, 9999999999999999})
	{
		_, err := fc.getFunction(ctx, datumsToConstants(args3))
		require.Error(t, err)
	}
}
