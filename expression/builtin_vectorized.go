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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

// columnBufferAllocator is used to allocate and release column buffer in vectorized evaluation.
type columnBufferAllocator interface {
	// alloc allocates a column buffer with the specific eval type and capacity.
	// the allocator is not responsible for initializing the column, so please initialize it before using.
	alloc(evalType types.EvalType, capacity int) (*chunk.Column, error)
	// release releases a column buffer.
	release(buf *chunk.Column)
}

type localSliceBuffer struct {
	sync.Mutex
	buffers []*chunk.Column
	head    int
	tail    int
	size    int
}

func newLocalSliceBuffer() *localSliceBuffer {
	// in most cases, expressions are called serially, so 1 buffer is enough.
	return &localSliceBuffer{buffers: make([]*chunk.Column, 1)}
}

func (r *localSliceBuffer) newBuffer(evalType types.EvalType, capacity int) (*chunk.Column, error) {
	switch evalType {
	case types.ETInt:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), capacity), nil
	case types.ETReal:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeDouble), capacity), nil
	case types.ETDecimal:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeNewDecimal), capacity), nil
	case types.ETDuration:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeDuration), capacity), nil
	case types.ETDatetime, types.ETTimestamp:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeDatetime), capacity), nil
	case types.ETString:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeString), capacity), nil
	case types.ETJson:
		return chunk.NewColumn(types.NewFieldType(mysql.TypeJSON), capacity), nil
	}
	return nil, errors.Errorf("alloc column buffer for unsupported EvalType=%v", evalType)
}

func (r *localSliceBuffer) alloc(evalType types.EvalType, capacity int) (*chunk.Column, error) {
	r.Lock()
	defer r.Unlock()
	if r.size > 0 {
		buf := r.buffers[r.head]
		r.head++
		if r.head == len(r.buffers) {
			r.head = 0
		}
		r.size--
		return buf, nil
	}
	return r.newBuffer(evalType, capacity)
}

func (r *localSliceBuffer) release(buf *chunk.Column) {
	r.Lock()
	defer r.Unlock()
	if r.size == len(r.buffers) {
		buffers := make([]*chunk.Column, len(r.buffers)*2)
		for pos, i := r.head, 0; i < len(r.buffers); i++ {
			buffers[i] = r.buffers[pos]
			pos++
			if pos > len(r.buffers) {
				pos -= len(r.buffers)
			}
		}
		r.head = 0
		r.tail = len(r.buffers)
		r.buffers = buffers
	}
	r.buffers[r.tail] = buf
	r.tail++
	if r.tail == len(r.buffers) {
		r.tail = 0
	}
	r.size++
}

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
	case types.ETDatetime, types.ETTimestamp:
		result.Reset()
		var v types.Time
		for ; row != it.End(); row = it.Next() {
			if v, isNull, err = c.builtinFunc.evalTime(row); err != nil {
				return err
			}
			if isNull {
				result.AppendNull()
			} else {
				result.AppendTime(v)
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
