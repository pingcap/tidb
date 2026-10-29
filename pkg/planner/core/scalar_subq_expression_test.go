// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// newTestScalarSubQueryExpr creates a pre-evaluated ScalarSubQueryExpr for testing.
func newTestScalarSubQueryExpr(val types.Datum, tp byte) *ScalarSubQueryExpr {
	ft := types.NewFieldType(tp)
	s := &ScalarSubQueryExpr{
		evaled: true,
	}
	s.Constant = expression.Constant{
		Value:   val,
		RetType: ft,
	}
	s.RetType = ft
	return s
}

// newTestErrScalarSubQueryExpr creates a ScalarSubQueryExpr with an evalErr set.
func newTestErrScalarSubQueryExpr(tp byte) (*ScalarSubQueryExpr, error) {
	expectedErr := errors.New("test eval error")
	s := &ScalarSubQueryExpr{
		evalErr: expectedErr,
		evaled:  true,
	}
	s.RetType = types.NewFieldType(tp)
	return s, expectedErr
}

func TestScalarSubQueryExprEvalInt(t *testing.T) {
	d := types.NewIntDatum(42)
	s := newTestScalarSubQueryExpr(d, mysql.TypeLonglong)

	val, isNull, err := s.EvalInt(nil, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(42), val)
}

func TestScalarSubQueryExprEvalIntError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeLonglong)
	_, _, err := s.EvalInt(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprEvalReal(t *testing.T) {
	d := types.NewFloat64Datum(3.14)
	s := newTestScalarSubQueryExpr(d, mysql.TypeDouble)

	val, isNull, err := s.EvalReal(nil, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.InDelta(t, 3.14, val, 0.001)
}

func TestScalarSubQueryExprEvalRealError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeDouble)
	_, _, err := s.EvalReal(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprEvalString(t *testing.T) {
	d := types.NewStringDatum("hello")
	s := newTestScalarSubQueryExpr(d, mysql.TypeVarchar)

	val, isNull, err := s.EvalString(nil, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "hello", val)
}

func TestScalarSubQueryExprEvalStringError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeVarchar)
	_, _, err := s.EvalString(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprEvalDecimal(t *testing.T) {
	dec := types.NewDecFromStringForTest("123.45")
	d := types.NewDecimalDatum(dec)
	s := newTestScalarSubQueryExpr(d, mysql.TypeNewDecimal)
	evalCtx := exprstatic.NewEvalContext()

	val, isNull, err := s.EvalDecimal(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 0, val.Compare(dec))
}

func TestScalarSubQueryExprEvalDecimalError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeNewDecimal)
	_, _, err := s.EvalDecimal(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprEvalTime(t *testing.T) {
	tm := types.ZeroDatetime
	d := types.NewTimeDatum(tm)
	s := newTestScalarSubQueryExpr(d, mysql.TypeDatetime)
	evalCtx := exprstatic.NewEvalContext()

	val, isNull, err := s.EvalTime(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 0, val.Compare(tm))
}

func TestScalarSubQueryExprEvalTimeError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeDatetime)
	_, _, err := s.EvalTime(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprEvalDuration(t *testing.T) {
	dur := types.Duration{Duration: 3600000000000, Fsp: 0} // 1 hour
	d := types.NewDurationDatum(dur)
	s := newTestScalarSubQueryExpr(d, mysql.TypeDuration)
	evalCtx := exprstatic.NewEvalContext()

	val, isNull, err := s.EvalDuration(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, dur.Duration, val.Duration)
}

func TestScalarSubQueryExprEvalDurationError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeDuration)
	_, _, err := s.EvalDuration(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprEvalJSON(t *testing.T) {
	bj := types.CreateBinaryJSON("test_value")
	d := types.NewJSONDatum(bj)
	s := newTestScalarSubQueryExpr(d, mysql.TypeJSON)
	evalCtx := exprstatic.NewEvalContext()

	val, isNull, err := s.EvalJSON(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, bj.String(), val.String())
}

func TestScalarSubQueryExprEvalJSONError(t *testing.T) {
	s, expectedErr := newTestErrScalarSubQueryExpr(mysql.TypeJSON)
	_, _, err := s.EvalJSON(nil, chunk.Row{})
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarSubQueryExprVecEvalErrors(t *testing.T) {
	// Test that all VecEval* methods return evalErr when set.
	tests := []struct {
		name string
		tp   byte
		fn   func(*ScalarSubQueryExpr) error
	}{
		{"VecEvalInt", mysql.TypeLonglong, func(s *ScalarSubQueryExpr) error { return s.VecEvalInt(nil, nil, nil) }},
		{"VecEvalReal", mysql.TypeDouble, func(s *ScalarSubQueryExpr) error { return s.VecEvalReal(nil, nil, nil) }},
		{"VecEvalString", mysql.TypeVarchar, func(s *ScalarSubQueryExpr) error { return s.VecEvalString(nil, nil, nil) }},
		{"VecEvalDecimal", mysql.TypeNewDecimal, func(s *ScalarSubQueryExpr) error { return s.VecEvalDecimal(nil, nil, nil) }},
		{"VecEvalTime", mysql.TypeDatetime, func(s *ScalarSubQueryExpr) error { return s.VecEvalTime(nil, nil, nil) }},
		{"VecEvalDuration", mysql.TypeDuration, func(s *ScalarSubQueryExpr) error { return s.VecEvalDuration(nil, nil, nil) }},
		{"VecEvalJSON", mysql.TypeJSON, func(s *ScalarSubQueryExpr) error { return s.VecEvalJSON(nil, nil, nil) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, expectedErr := newTestErrScalarSubQueryExpr(tt.tp)
			err := tt.fn(s)
			require.ErrorIs(t, err, expectedErr)
		})
	}
}

func TestScalarSubQueryExprEvalNullValues(t *testing.T) {
	evalCtx := exprstatic.NewEvalContext()

	// Test EvalInt with null
	s := newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeLonglong)
	_, isNull, err := s.EvalInt(nil, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	// Test EvalReal with null
	s = newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeDouble)
	_, isNull, err = s.EvalReal(nil, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	// Test EvalString with null
	s = newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeVarchar)
	_, isNull, err = s.EvalString(nil, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	// Test EvalDecimal with null
	s = newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeNewDecimal)
	_, isNull, err = s.EvalDecimal(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	// Test EvalTime with null
	s = newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeDatetime)
	_, isNull, err = s.EvalTime(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	// Test EvalDuration with null
	s = newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeDuration)
	_, isNull, err = s.EvalDuration(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	// Test EvalJSON with null
	s = newTestScalarSubQueryExpr(types.NewDatum(nil), mysql.TypeJSON)
	_, isNull, err = s.EvalJSON(evalCtx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
}
