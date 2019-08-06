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

// genVecFromConstExpr generates a vector, all the elements of this vector are
// filled with the value of `expr`.
func genVecFromConstExpr(ctx sessionctx.Context, expr Expression, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	switch expr.GetType().EvalType() {
	case types.ETInt: // uint64 is also set in this branch.
		result.PreAllocInt64(n)
		v, isNull, err := expr.EvalInt(ctx, chunk.Row{})
		if err != nil || isNull { // all slots are set to null by PreAlloc()
			return err
		}
		for i := 0; i < n; i++ {
			result.SetInt64(i, v)
		}
		result.SetNulls(0, n, false)
	case types.ETReal:
		result.PreAllocFloat64(n)
		v, isNull, err := expr.EvalReal(ctx, chunk.Row{})
		if err != nil || isNull { // all slots are set to null by PreAlloc()
			return err
		}
		for i := 0; i < n; i++ {
			result.SetFloat64(i, v)
		}
		result.SetNulls(0, n, false)
	case types.ETDecimal:
		result.PreAllocDecimal(n)
		v, isNull, err := expr.EvalDecimal(ctx, chunk.Row{})
		if err != nil || isNull { // all slots are set to null by PreAlloc()
			return err
		}
		for i := 0; i < n; i++ {
			result.SetDecimal(i, v)
		}
		result.SetNulls(0, n, false)
	case types.ETDatetime, types.ETTimestamp:
		result.PreAllocTime(n)
		v, isNull, err := expr.EvalTime(ctx, chunk.Row{})
		if err != nil || isNull { // all slots are set to null by PreAlloc()
			return err
		}
		for i := 0; i < n; i++ {
			result.SetTime(i, v)
		}
		result.SetNulls(0, n, false)
	case types.ETDuration:
		result.PreAllocDuration(n)
		v, isNull, err := expr.EvalDuration(ctx, chunk.Row{})
		if err != nil || isNull { // all slots are set to null by PreAlloc()
			return err
		}
		for i := 0; i < n; i++ {
			result.SetDuration(i, v)
		}
		result.SetNulls(0, n, false)
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
			for i := 0; i < n; i++ {
				result.AppendJSON(v)
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
			for i := 0; i < n; i++ {
				result.AppendString(v)
			}
		}
	default:
		return errors.Errorf("unsupported Constant type for vectorized evaluation")
	}
	return nil
}
