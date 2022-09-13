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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"sync"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// columnBufferAllocator is used to allocate and release column buffer in vectorized evaluation.
type columnBufferAllocator interface {
	// get allocates a column. The allocator is not responsible for initializing the column, so please initialize it before using.
	get() (*chunk.Column, error)
	// put releases a column buffer.
	put(buf *chunk.Column)
	// MemoryUsage return the memory usage of columnBufferAllocator
	MemoryUsage() int64
}

// localColumnPool implements columnBufferAllocator interface.
// It works like a concurrency-safe deque which is implemented by lock-free sync.Pool.
type localColumnPool struct {
	sync.Pool
}

func newLocalColumnPool() *localColumnPool {
	newColumn := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), chunk.InitialCapacity)
	return &localColumnPool{
		sync.Pool{
			New: func() interface{} {
				return newColumn.CopyConstruct(nil)
			},
		},
	}
}

var globalColumnAllocator = newLocalColumnPool()

// GetColumn allocates a column. The allocator is not responsible for initializing the column, so please initialize it before using.
func GetColumn(_ types.EvalType, _ int) (*chunk.Column, error) {
	return globalColumnAllocator.get()
}

// PutColumn releases a column buffer.
func PutColumn(buf *chunk.Column) {
	globalColumnAllocator.put(buf)
}

func (r *localColumnPool) get() (*chunk.Column, error) {
	col, ok := r.Pool.Get().(*chunk.Column)
	if !ok {
		return nil, errors.New("unexpected object in localColumnPool")
	}
	return col, nil
}

func (r *localColumnPool) put(col *chunk.Column) {
	r.Pool.Put(col)
}

const emptyLocalColumnPoolSize = int64(unsafe.Sizeof(localColumnPool{}))

func (r *localColumnPool) MemoryUsage() (sum int64) {
	return emptyLocalColumnPoolSize
}

// vecEvalIntByRows uses the non-vectorized(row-based) interface `evalInt` to eval the expression.
func vecEvalIntByRows(sig builtinFunc, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		res, isNull, err := sig.evalInt(input.GetRow(i))
		if err != nil {
			return err
		}
		result.SetNull(i, isNull)
		i64s[i] = res
	}
	return nil
}

// vecEvalStringByRows uses the non-vectorized(row-based) interface `evalString` to eval the expression.
func vecEvalStringByRows(sig builtinFunc, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		res, isNull, err := sig.evalString(input.GetRow(i))
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(res)
	}
	return nil
}
