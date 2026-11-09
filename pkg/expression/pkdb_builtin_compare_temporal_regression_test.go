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

package expression

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestTemporalComparisonStringConstantKeepsRuntimePrecision(t *testing.T) {
	ctx := createContext(t)
	timeType := types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetDecimal(0).BuildP()
	timeColumn := &Column{RetType: timeType, Index: 0}
	stringConstant := &Constant{
		Value:   types.NewStringDatum("11:11:11.5"),
		RetType: types.NewFieldType(mysql.TypeVarString),
	}

	bf, err := funcs[ast.GT].getFunction(ctx, []Expression{timeColumn, stringConstant})
	require.NoError(t, err)
	res, err := evalBuiltinFunc(bf, ctx, chunk.MutRowFromDatums([]types.Datum{
		types.NewDurationDatum(types.NewDuration(11, 11, 12, 0, 0)),
	}).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(1), res.GetInt64())
}

func TestTemporalComparisonStringParameterUsesRuntimeConversion(t *testing.T) {
	ctx := createContext(t)
	ctx.GetPlanCacheTracker().SetCacheType(contextutil.SessionPrepared)
	ctx.GetPlanCacheTracker().EnablePlanCache()
	ctx.GetSessionVars().PlanCacheParams.Append(types.NewStringDatum("-836:46:08"))
	timeType := types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetDecimal(3).BuildP()
	timeColumn := &Column{RetType: timeType, Index: 0}
	parameter := &Constant{
		ParamMarker: &ParamMarker{order: 0},
		RetType:     types.NewFieldType(mysql.TypeUnspecified),
	}

	bf, err := funcs[ast.EQ].getFunction(ctx, []Expression{timeColumn, parameter})
	require.NoError(t, err)
	require.IsType(t, &builtinEQDurationSig{}, bf)
	requireParameterPreserved(t, bf, parameter)
	require.Equal(t, mysql.TypeDuration, bf.getArgs()[1].GetType(ctx.GetEvalCtx()).GetType())
	require.Equal(t, types.MaxFsp, bf.getArgs()[1].GetType(ctx.GetEvalCtx()).GetDecimal())
	require.Empty(t, ctx.GetPlanCacheTracker().PlanCacheUnqualified())
	require.True(t, ctx.GetPlanCacheTracker().UseCache())
}

func TestTemporalComparisonInvalidZeroParameterKeepsRuntimeConversion(t *testing.T) {
	ctx := createContext(t)
	ctx.GetPlanCacheTracker().SetCacheType(contextutil.SessionPrepared)
	ctx.GetPlanCacheTracker().EnablePlanCache()
	ctx.GetSessionVars().PlanCacheParams.Append(types.NewTimeDatum(types.NewTime(types.ZeroCoreTime, mysql.TypeDatetime, 0)))
	timeType := types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetDecimal(3).BuildP()
	timeColumn := &Column{RetType: timeType, Index: 0}
	parameter := &Constant{
		ParamMarker: &ParamMarker{order: 0},
		RetType:     types.NewFieldType(mysql.TypeUnspecified),
	}

	bf, err := funcs[ast.EQ].getFunction(ctx, []Expression{timeColumn, parameter})
	require.NoError(t, err)
	require.IsType(t, &builtinEQTimeSig{}, bf)
	requireParameterPreserved(t, bf, parameter)
	require.Equal(t, mysql.TypeDatetime, bf.getArgs()[1].GetType(ctx.GetEvalCtx()).GetType())
	require.Equal(t, 0, bf.getArgs()[1].GetType(ctx.GetEvalCtx()).GetDecimal())
	require.Empty(t, ctx.GetPlanCacheTracker().PlanCacheUnqualified())
	require.True(t, ctx.GetPlanCacheTracker().UseCache())
}

func requireParameterPreserved(t *testing.T, bf builtinFunc, parameter *Constant) {
	t.Helper()
	arg := bf.getArgs()[1]
	for {
		switch expr := arg.(type) {
		case *Constant:
			if expr.ParamMarker != nil {
				require.Equal(t, parameter.ParamMarker.order, expr.ParamMarker.order)
				return
			}
			require.NotNil(t, expr.DeferredExpr)
			arg = expr.DeferredExpr
		case *ScalarFunction:
			require.Len(t, expr.GetArgs(), 1)
			arg = expr.GetArgs()[0]
		default:
			require.Failf(t, "parameter was not preserved", "unexpected expression type %T", arg)
			return
		}
	}
}

func TestPKDBCompareTemporalConstantsAndColumns(t *testing.T) {
	ctx := createContext(t)
	ctx.ResetSessionAndStmtTimeZone(time.UTC)
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("timestamp", "69006"))

	newColumn := func(tp byte, index int) *Column {
		return &Column{RetType: types.NewFieldType(tp), Index: index}
	}
	newDatetimeConstant := func() *Constant {
		return &Constant{
			Value:   types.NewTimeDatum(types.NewTime(types.FromDate(1000, 1, 1, 19, 10, 6, 0), mysql.TypeDatetime, 0)),
			RetType: types.NewFieldType(mysql.TypeDatetime),
		}
	}

	timeCol := newColumn(mysql.TypeDuration, 0)
	datetimeCol := newColumn(mysql.TypeDatetime, 1)
	dateCol := newColumn(mysql.TypeDate, 2)
	timeConst := &Constant{
		Value:   types.NewDurationDatum(types.NewDuration(19, 10, 6, 0, 0)),
		RetType: types.NewFieldType(mysql.TypeDuration),
	}
	for _, test := range []struct {
		name string
		lhs  Expression
		rhs  Expression
		want types.EvalType
	}{
		{name: "time column and datetime constant", lhs: timeCol, rhs: newDatetimeConstant(), want: types.ETDatetime},
		{name: "datetime constant and time column", lhs: newDatetimeConstant(), rhs: timeCol, want: types.ETDatetime},
		{name: "datetime column and time constant", lhs: datetimeCol, rhs: timeConst, want: types.ETDatetime},
		{name: "time and datetime columns", lhs: timeCol, rhs: datetimeCol, want: types.ETDatetime},
		{name: "time columns", lhs: timeCol, rhs: newColumn(mysql.TypeDuration, 2), want: types.ETDuration},
		{name: "date column and datetime constant", lhs: dateCol, rhs: newDatetimeConstant(), want: types.ETDatetime},
		{name: "date and datetime columns", lhs: dateCol, rhs: datetimeCol, want: types.ETDatetime},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, GetAccurateCmpType(ctx, test.lhs, test.rhs))
		})
	}

	bf, err := funcs[ast.EQ].getFunction(ctx, []Expression{timeCol, datetimeCol})
	require.NoError(t, err)
	res, err := evalBuiltinFunc(bf, ctx, chunk.MutRowFromDatums([]types.Datum{
		types.NewDurationDatum(types.NewDuration(19, 10, 6, 0, 0)),
		types.NewTimeDatum(types.NewTime(types.FromDate(1970, 1, 1, 19, 10, 6, 0), mysql.TypeDatetime, 0)),
	}).ToRow())
	require.NoError(t, err)
	require.Equal(t, int64(1), res.GetInt64())
}
