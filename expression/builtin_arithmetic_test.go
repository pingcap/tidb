// Copyright 2017 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestSetFlenDecimal4RealOrDecimal(t *testing.T) {
	ret := &types.FieldType{}
	a := &types.FieldType{}
	a.SetDecimal(1)
	a.SetFlen(3)

	b := &types.FieldType{}
	b.SetDecimal(0)
	b.SetFlag(2)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, false)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, 4, ret.GetFlen())

	b.SetFlen(65)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, false)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, mysql.MaxRealWidth, ret.GetFlen())
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, false, false)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, mysql.MaxDecimalWidth, ret.GetFlen())

	b.SetFlen(types.UnspecifiedLength)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, false)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, types.UnspecifiedLength, ret.GetFlen())

	b.SetDecimal(types.UnspecifiedLength)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, false)
	require.Equal(t, types.UnspecifiedLength, ret.GetDecimal())
	require.Equal(t, types.UnspecifiedLength, ret.GetFlen())

	ret = &types.FieldType{}
	a = &types.FieldType{}
	a.SetDecimal(1)
	a.SetFlen(3)

	b = &types.FieldType{}
	b.SetDecimal(0)
	b.SetFlen(2)

	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, true)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, 6, ret.GetFlen())

	b.SetFlen(65)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, true)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, mysql.MaxRealWidth, ret.GetFlen())
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, false, true)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, mysql.MaxDecimalWidth, ret.GetFlen())

	b.SetFlen(types.UnspecifiedLength)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, true)
	require.Equal(t, 1, ret.GetDecimal())
	require.Equal(t, types.UnspecifiedLength, ret.GetFlen())

	b.SetDecimal(types.UnspecifiedLength)
	setFlenDecimal4RealOrDecimal(mock.NewContext(), ret, &Constant{RetType: a}, &Constant{RetType: b}, true, true)
	require.Equal(t, types.UnspecifiedLength, ret.GetDecimal())
	require.Equal(t, types.UnspecifiedLength, ret.GetFlen())
}

func TestArithmeticPlus(t *testing.T) {
	ctx := createContext(t)
	// case: 1
	args := []interface{}{int64(12), int64(1)}

	bf, err := funcs[ast.Plus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	intSig, ok := bf.(*builtinArithmeticPlusIntSig)
	require.True(t, ok)
	require.NotNil(t, intSig)

	intResult, isNull, err := intSig.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(13), intResult)

	// case 2
	args = []interface{}{1.01001, -0.01}

	bf, err = funcs[ast.Plus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok := bf.(*builtinArithmeticPlusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err := realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 1.00001, realResult)

	// case 3
	args = []interface{}{nil, -0.11101}

	bf, err = funcs[ast.Plus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok = bf.(*builtinArithmeticPlusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Equal(t, float64(0), realResult)

	// case 4
	args = []interface{}{nil, nil}

	bf, err = funcs[ast.Plus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok = bf.(*builtinArithmeticPlusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Equal(t, float64(0), realResult)

	// case 5
	hexStr, err := types.ParseHexStr("0x20000000000000")
	require.NoError(t, err)
	args = []interface{}{hexStr, int64(1)}

	bf, err = funcs[ast.Plus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	intSig, ok = bf.(*builtinArithmeticPlusIntSig)
	require.True(t, ok)
	require.NotNil(t, intSig)

	intResult, _, err = intSig.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(9007199254740993), intResult)

	bitStr, err := types.NewBitLiteral("0b00011")
	require.NoError(t, err)
	args = []interface{}{bitStr, int64(1)}

	bf, err = funcs[ast.Plus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)

	//check the result type is int
	intSig, ok = bf.(*builtinArithmeticPlusIntSig)
	require.True(t, ok)
	require.NotNil(t, intSig)

	intResult, _, err = intSig.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(4), intResult)
}

func TestArithmeticMinus(t *testing.T) {
	ctx := createContext(t)
	// case: 1
	args := []interface{}{int64(12), int64(1)}

	bf, err := funcs[ast.Minus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	intSig, ok := bf.(*builtinArithmeticMinusIntSig)
	require.True(t, ok)
	require.NotNil(t, intSig)

	intResult, isNull, err := intSig.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(11), intResult)

	// case 2
	args = []interface{}{1.01001, -0.01}

	bf, err = funcs[ast.Minus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok := bf.(*builtinArithmeticMinusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err := realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 1.02001, realResult)

	// case 3
	args = []interface{}{nil, -0.11101}

	bf, err = funcs[ast.Minus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Equal(t, float64(0), realResult)

	// case 4
	args = []interface{}{1.01, nil}

	bf, err = funcs[ast.Minus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Equal(t, float64(0), realResult)

	// case 5
	args = []interface{}{nil, nil}

	bf, err = funcs[ast.Minus].getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	require.NotNil(t, bf)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	require.True(t, ok)
	require.NotNil(t, realSig)

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Equal(t, float64(0), realResult)
}

func TestArithmeticMultiply(t *testing.T) {
	ctx := createContext(t)
	testCases := []struct {
		args   []interface{}
		expect []interface{}
		err    error
	}{
		{
			args:   []interface{}{int64(11), int64(11)},
			expect: []interface{}{int64(121), nil},
		},
		{
			args:   []interface{}{int64(-1), int64(math.MinInt64)},
			expect: []interface{}{nil, "BIGINT value is out of range in '\\(-1 \\* -9223372036854775808\\)'$"},
		},
		{
			args:   []interface{}{int64(math.MinInt64), int64(-1)},
			expect: []interface{}{nil, "BIGINT value is out of range in '\\(-9223372036854775808 \\* -1\\)'$"},
		},
		{
			args:   []interface{}{uint64(11), uint64(11)},
			expect: []interface{}{int64(121), nil},
		},
		{
			args:   []interface{}{float64(11), float64(11)},
			expect: []interface{}{float64(121), nil},
		},
		{
			args:   []interface{}{nil, -0.11101},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{1.01, nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{nil, nil},
			expect: []interface{}{nil, nil},
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Mul].getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, sig)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		if tc.expect[1] == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tc.expect[0]), val)
		} else {
			require.Error(t, err)
			require.Regexp(t, tc.expect[1], err.Error())
		}
	}
}

func TestArithmeticDivide(t *testing.T) {
	ctx := createContext(t)

	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{11.1111111, 11.1},
			expect: 1.001001,
		},
		{
			args:   []interface{}{11.1111111, float64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(11), int64(11)},
			expect: float64(1),
		},
		{
			args:   []interface{}{int64(11), int64(2)},
			expect: 5.5,
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{uint64(11), uint64(11)},
			expect: float64(1),
		},
		{
			args:   []interface{}{uint64(11), uint64(2)},
			expect: 5.5,
		},
		{
			args:   []interface{}{uint64(11), uint64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{nil, -0.11101},
			expect: nil,
		},
		{
			args:   []interface{}{1.01, nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, nil},
			expect: nil,
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Div].getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, sig)
		switch sig.(type) {
		case *builtinArithmeticIntDivideIntSig:
			require.Equal(t, tipb.ScalarFuncSig_IntDivideInt, sig.PbCode())
		case *builtinArithmeticIntDivideDecimalSig:
			require.Equal(t, tipb.ScalarFuncSig_IntDivideDecimal, sig.PbCode())
		}
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tc.expect), val)
	}
}

func TestArithmeticIntDivide(t *testing.T) {
	ctx := createContext(t)
	testCases := []struct {
		args   []interface{}
		expect []interface{}
	}{
		{
			args:   []interface{}{int64(13), int64(11)},
			expect: []interface{}{int64(1), nil},
		},
		{
			args:   []interface{}{int64(-13), int64(11)},
			expect: []interface{}{int64(-1), nil},
		},
		{
			args:   []interface{}{int64(13), int64(-11)},
			expect: []interface{}{int64(-1), nil},
		},
		{
			args:   []interface{}{int64(-13), int64(-11)},
			expect: []interface{}{int64(1), nil},
		},
		{
			args:   []interface{}{int64(33), int64(11)},
			expect: []interface{}{int64(3), nil},
		},
		{
			args:   []interface{}{int64(-33), int64(11)},
			expect: []interface{}{int64(-3), nil},
		},
		{
			args:   []interface{}{int64(33), int64(-11)},
			expect: []interface{}{int64(-3), nil},
		},
		{
			args:   []interface{}{int64(-33), int64(-11)},
			expect: []interface{}{int64(3), nil},
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{int64(-11), int64(0)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{11.01, 1.1},
			expect: []interface{}{int64(10), nil},
		},
		{
			args:   []interface{}{-11.01, 1.1},
			expect: []interface{}{int64(-10), nil},
		},
		{
			args:   []interface{}{11.01, -1.1},
			expect: []interface{}{int64(-10), nil},
		},
		{
			args:   []interface{}{-11.01, -1.1},
			expect: []interface{}{int64(10), nil},
		},
		{
			args:   []interface{}{nil, -0.11101},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{1.01, nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{nil, int64(-1001)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{int64(101), nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{nil, nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{123456789100000.0, -0.00001},
			expect: []interface{}{nil, ".*BIGINT value is out of range in '\\(123456789100000 DIV -0.00001\\)'"},
		},
		{
			args:   []interface{}{int64(-9223372036854775808), float64(-1)},
			expect: []interface{}{nil, ".*BIGINT value is out of range in '\\(-9223372036854775808 DIV -1\\)'"},
		},
		{
			args:   []interface{}{uint64(1), float64(-2)},
			expect: []interface{}{0, nil},
		},
		{
			args:   []interface{}{uint64(1), float64(-1)},
			expect: []interface{}{nil, ".*BIGINT UNSIGNED value is out of range in '\\(1 DIV -1\\)'"},
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.IntDiv].getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, sig)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		if tc.expect[1] == nil {
			require.NoError(t, err)
			testutil.DatumEqual(t, types.NewDatum(tc.expect[0]), val)
		} else {
			require.Error(t, err)
			require.Regexp(t, tc.expect[1], err.Error())
		}
	}
}

func TestArithmeticMod(t *testing.T) {
	ctx := createContext(t)
	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{int64(13), int64(11)},
			expect: int64(2),
		},
		{
			args:   []interface{}{int64(13), int64(11)},
			expect: int64(2),
		},
		{
			args:   []interface{}{int64(13), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{uint64(13), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(13), uint64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{uint64(math.MaxInt64 + 1), int64(math.MinInt64)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(-22), uint64(10)},
			expect: int64(-2),
		},
		{
			args:   []interface{}{int64(math.MinInt64), uint64(3)},
			expect: int64(-2),
		},
		{
			args:   []interface{}{int64(-13), int64(11)},
			expect: int64(-2),
		},
		{
			args:   []interface{}{int64(13), int64(-11)},
			expect: int64(2),
		},
		{
			args:   []interface{}{int64(-13), int64(-11)},
			expect: int64(-2),
		},
		{
			args:   []interface{}{int64(33), int64(11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(-33), int64(11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(33), int64(-11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(-33), int64(-11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(-11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(1), 1.1},
			expect: float64(1),
		},
		{
			args:   []interface{}{int64(-1), 1.1},
			expect: float64(-1),
		},
		{
			args:   []interface{}{int64(1), -1.1},
			expect: float64(1),
		},
		{
			args:   []interface{}{int64(-1), -1.1},
			expect: float64(-1),
		},
		{
			args:   []interface{}{nil, -0.11101},
			expect: nil,
		},
		{
			args:   []interface{}{1.01, nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, int64(-1001)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(101), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, nil},
			expect: nil,
		},
		{
			args:   []interface{}{"1231", 12},
			expect: 7,
		},
		{
			args:   []interface{}{"1231", "12"},
			expect: float64(7),
		},
		{
			args:   []interface{}{types.Duration{Duration: 45296 * time.Second}, 122},
			expect: 114,
		},
		{
			args:   []interface{}{types.Set{Value: 7, Name: "abc"}, "12"},
			expect: float64(7),
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Mod].getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, sig)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		switch sig.(type) {
		case *builtinArithmeticModRealSig:
			require.Equal(t, tipb.ScalarFuncSig_ModReal, sig.PbCode())
		case *builtinArithmeticModIntUnsignedUnsignedSig:
			require.Equal(t, tipb.ScalarFuncSig_ModIntUnsignedUnsigned, sig.PbCode())
		case *builtinArithmeticModIntUnsignedSignedSig:
			require.Equal(t, tipb.ScalarFuncSig_ModIntUnsignedSigned, sig.PbCode())
		case *builtinArithmeticModIntSignedUnsignedSig:
			require.Equal(t, tipb.ScalarFuncSig_ModIntSignedUnsigned, sig.PbCode())
		case *builtinArithmeticModIntSignedSignedSig:
			require.Equal(t, tipb.ScalarFuncSig_ModIntSignedSigned, sig.PbCode())
		case *builtinArithmeticModDecimalSig:
			require.Equal(t, tipb.ScalarFuncSig_ModDecimal, sig.PbCode())
		}
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tc.expect), val)
	}
}

func TestDecimalErrOverflow(t *testing.T) {
	ctx := createContext(t)
	testCases := []struct {
		args   []float64
		opd    string
		sig    tipb.ScalarFuncSig
		errStr string
	}{
		{
			args:   []float64{8.1e80, 8.1e80},
			opd:    ast.Plus,
			sig:    tipb.ScalarFuncSig_PlusDecimal,
			errStr: "[types:1690]DECIMAL value is out of range in '(810000000000000000000000000000000000000000000000000000000000000000000000000000000 + 810000000000000000000000000000000000000000000000000000000000000000000000000000000)'",
		},
		{
			args:   []float64{8.1e80, -8.1e80},
			opd:    ast.Minus,
			sig:    tipb.ScalarFuncSig_MinusDecimal,
			errStr: "[types:1690]DECIMAL value is out of range in '(810000000000000000000000000000000000000000000000000000000000000000000000000000000 - -810000000000000000000000000000000000000000000000000000000000000000000000000000000)'",
		},
		{
			args:   []float64{8.1e80, 8.1e80},
			opd:    ast.Mul,
			sig:    tipb.ScalarFuncSig_MultiplyDecimal,
			errStr: "[types:1690]DECIMAL value is out of range in '(810000000000000000000000000000000000000000000000000000000000000000000000000000000 * 810000000000000000000000000000000000000000000000000000000000000000000000000000000)'",
		},
		{
			args:   []float64{8.1e80, 0.1},
			opd:    ast.Div,
			sig:    tipb.ScalarFuncSig_DivideDecimal,
			errStr: "[types:1690]DECIMAL value is out of range in '(810000000000000000000000000000000000000000000000000000000000000000000000000000000 / 0.1)'",
		},
	}
	for _, tc := range testCases {
		dec1, dec2 := types.NewDecFromFloatForTest(tc.args[0]), types.NewDecFromFloatForTest(tc.args[1])
		bf, err := funcs[tc.opd].getFunction(ctx, datumsToConstants(types.MakeDatums(dec1, dec2)))
		require.NoError(t, err)
		require.NotNil(t, bf)
		require.Equal(t, tc.sig, bf.PbCode())
		_, err = evalBuiltinFunc(bf, chunk.Row{})
		require.EqualError(t, err, tc.errStr)
	}
}
