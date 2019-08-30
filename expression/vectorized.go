// Copyright 2019 PingCAP, Inc.
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

package expression

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func genVecFromConstExpr(ctx sessionctx.Context, expr Expression, targetType types.EvalType, input *chunk.Chunk, result *chunk.Column) error {
	n := 1
	if input != nil {
		n = input.NumRows()
	}
	switch targetType {
	case types.ETInt:
		result.ResizeInt64(n)
		v, isNull, err := expr.EvalInt(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			result.SetNulls(0, n, true)
			return nil
		}
		i64s := result.Int64s()
		for i := range i64s {
			i64s[i] = v
		}
		result.SetNulls(0, n, false)
	case types.ETReal:
		result.ResizeFloat64(n)
		v, isNull, err := expr.EvalReal(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			result.SetNulls(0, n, true)
			return nil
		}
		f64s := result.Float64s()
		for i := range f64s {
			f64s[i] = v
		}
		result.SetNulls(0, n, false)
	case types.ETDecimal:
		result.ResizeDecimal(n)
		v, isNull, err := expr.EvalDecimal(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			result.SetNulls(0, n, true)
			return nil
		}
		ds := result.Decimals()
		for i := range ds {
			ds[i] = *v
		}
		result.SetNulls(0, n, false)
	case types.ETDatetime, types.ETTimestamp:
		result.Reset()
		v, isNull, err := expr.EvalTime(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			for i := 0; i < n; i++ {
				result.AppendTime(v)
			}
		}
	case types.ETDuration:
		result.ResizeDuration(n)
		v, isNull, err := expr.EvalDuration(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			for i := 0; i < n; i++ {
				result.AppendDuration(v)
			}
		}
	case types.ETJson:
		result.ReserveJSON(n)
		v, isNull, err := expr.EvalJSON(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			for i := 0; i < n; i++ {
				result.AppendJSON(v)
			}
		}
	case types.ETString:
		result.ReserveString(n)
		v, isNull, err := expr.EvalString(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			for i := 0; i < n; i++ {
				result.AppendString(v)
			}
		}
	default:
		return errors.Errorf("unsupported Constant type for vectorized evaluation")
	}
	return nil
}
