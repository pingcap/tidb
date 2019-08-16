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
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

type vecRowConverter struct {
	builtinFunc
}

func (c *vecRowConverter) vecEval(input *chunk.Chunk, result *chunk.Column) error {
	if c.builtinFunc.vectorized() {
		return c.builtinFunc.vecEval(input, result)
	}

	// convert from row-based evaluation to vectorized evaluation.
	// evaluate each row in input according to its row-based evaluation methods and updates the result.
	it := chunk.NewIterator4Chunk(input)
	row := it.Begin()
	var isNull bool
	var err error
	switch c.builtinFunc.getRetTp().EvalType() {
	case types.ETInt:
		result.ResizeInt64(input.NumRows())
		i64s := result.Int64s()
		for i := range i64s {
			if i64s[i], isNull, err = c.builtinFunc.evalInt(row); err != nil {
				return err
			}
			result.SetNull(i, isNull)
			row = it.Next()
		}
	case types.ETReal:
		result.ResizeFloat64(input.NumRows())
		f64s := result.Float64s()
		for i := range f64s {
			if f64s[i], isNull, err = c.builtinFunc.evalReal(row); err != nil {
				return err
			}
			result.SetNull(i, isNull)
			row = it.Next()
		}
	case types.ETDecimal:
		result.ResizeDecimal(input.NumRows())
		ds := result.Decimals()
		var v *types.MyDecimal
		for i := range ds {
			if v, isNull, err = c.builtinFunc.evalDecimal(row); err != nil {
				return err
			}
			if isNull {
				result.SetNull(i, true)
			} else {
				result.SetNull(i, false)
				ds[i] = *v
			}
			row = it.Next()
		}
	case types.ETDuration:
		result.ResizeDuration(input.NumRows())
		ds := result.GoDurations()
		var v types.Duration
		for i := range ds {
			if v, isNull, err = c.builtinFunc.evalDuration(row); err != nil {
				return err
			}
			ds[i] = v.Duration
			result.SetNull(i, isNull)
			row = it.Next()
		}
	case types.ETDatetime, types.ETTimestamp:
		result.ResizeTime(input.NumRows())
		ds := result.Times()
		var v types.Time
		for i := range ds {
			if v, isNull, err = c.builtinFunc.evalTime(row); err != nil {
				return err
			}
			ds[i] = v
			result.SetNull(i, isNull)
			row = it.Next()
		}
	case types.ETJson:
		result.ReserveJSON(input.NumRows())
		var v json.BinaryJSON
		for ; row != it.End(); row = it.Next() {
			if v, isNull, err = c.builtinFunc.evalJSON(row); err != nil {
				return err
			}
			if isNull {
				result.AppendNull()
			} else {
				result.AppendJSON(v)
			}
		}
	case types.ETString:
		result.ReserveString(input.NumRows())
		var v string
		for ; row != it.End(); row = it.Next() {
			if v, isNull, err = c.builtinFunc.evalString(row); err != nil {
				return err
			}
			if isNull {
				result.AppendNull()
			} else {
				result.AppendString(v)
			}
		}
	default:
		return errors.Errorf("unsupported type for converting from row-based to vectorized evaluation, please contact the TiDB team for help")
	}
	return nil
}

func (c *vecRowConverter) vectorized() bool {
	return true
}

func (c *vecRowConverter) equal(bf builtinFunc) bool {
	if converter, ok := bf.(*vecRowConverter); ok {
		bf = converter.builtinFunc
	}
	return c.builtinFunc.equal(bf)
}

func (c *vecRowConverter) Clone() builtinFunc {
	return &vecRowConverter{c.builtinFunc.Clone()}
}

type vecRowConvertFuncClass struct {
	functionClass
}

func (c *vecRowConvertFuncClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	bf, err := c.functionClass.getFunction(ctx, args)
	if err != nil {
		return nil, err
	}
	return &vecRowConverter{bf}, nil
}
