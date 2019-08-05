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

func genVecFromConstExpr(ctx sessionctx.Context, expr Expression, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumEffectiveRows()
	sel := input.Sel()
	tp := expr.GetType()
	switch tp.EvalType() {
	case types.ETInt:
		result.PreAllocInt64(n)
		v, isNull, err := expr.EvalInt(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull { // all slots are set to null by PreAlloc()
			return nil
		}
		i64s := result.Int64s()
		if sel == nil {
			for i := range i64s {
				i64s[i] = v
			}
			result.SetNulls(0, n, false)
		} else {
			for _, i := range sel {
				i64s[i] = v
				result.SetNull(i, false)
			}
		}
	case types.ETReal:
		result.PreAllocFloat64(n)
		v, isNull, err := expr.EvalReal(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull { // all slots are set to null by PreAlloc()
			return nil
		}
		f64s := result.Float64s()
		if sel == nil {
			for i := range f64s {
				f64s[i] = v
			}
			result.SetNulls(0, n, false)
		} else {
			for _, i := range sel {
				f64s[i] = v
				result.SetNull(i, false)
			}
		}
	case types.ETDecimal:
		result.PreAllocDecimal(n)
		v, isNull, err := expr.EvalDecimal(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull { // all slots are set to null by PreAlloc()
			return nil
		}
		ds := result.Decimals()
		if sel == nil {
			for i := range ds {
				ds[i] = *v
			}
			result.SetNulls(0, n, false)
		} else {
			for _, i := range sel {
				ds[i] = *v
				result.SetNull(i, false)
			}
		}
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
			if sel == nil {
				for i := 0; i < n; i++ {
					result.AppendTime(v)
				}
			} else {
				pos := 0
				for _, i := range sel {
					for pos < i {
						result.AppendNull()
						pos++
					}
					result.AppendTime(v)
					pos++
				}
			}
		}
	case types.ETDuration:
		result.Reset()
		v, isNull, err := expr.EvalDuration(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			if sel == nil {
				for i := 0; i < n; i++ {
					result.AppendDuration(v)
				}
			} else {
				pos := 0
				for _, i := range sel {
					for pos < i {
						result.AppendNull()
						pos++
					}
					result.AppendDuration(v)
					pos++
				}
			}
		}
	case types.ETJson:
		result.Reset()
		v, isNull, err := expr.EvalJSON(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			if sel == nil {
				for i := 0; i < n; i++ {
					result.AppendJSON(v)
				}
			} else {
				pos := 0
				for _, i := range sel {
					for pos < i {
						result.AppendNull()
						pos++
					}
					result.AppendJSON(v)
					pos++
				}
			}
		}
	case types.ETString:
		result.Reset()
		v, isNull, err := expr.EvalString(ctx, chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			for i := 0; i < n; i++ {
				result.AppendNull()
			}
		} else {
			if sel == nil {
				for i := 0; i < n; i++ {
					result.AppendString(v)
				}
			} else {
				pos := 0
				for _, i := range sel {
					for pos < i {
						result.AppendNull()
						pos++
					}
					result.AppendString(v)
					pos++
				}
			}
		}
	default:
		return errors.Errorf("unsupported Constant type for vectorized evaluation")
	}
	return nil
}
