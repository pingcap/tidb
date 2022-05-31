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

package chunk

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	l := NewList(fields, 2, 2)
	srcChunk := NewChunkWithCapacity(fields, 32)
	srcChunk.AppendInt64(0, 1)
	srcRow := srcChunk.GetRow(0)

	// Test basic append.
	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	require.Equal(t, 3, l.NumChunks())
	require.Equal(t, 5, l.Len())
	require.Empty(t, l.freelist)

	// Test chunk reuse.
	l.Reset()
	require.Len(t, l.freelist, 3)

	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	require.Empty(t, l.freelist)

	// Test add chunk then append row.
	l.Reset()
	nChunk := NewChunkWithCapacity(fields, 32)
	nChunk.AppendNull(0)
	l.Add(nChunk)
	ptr := l.AppendRow(srcRow)
	require.Equal(t, 2, l.NumChunks())
	require.Equal(t, uint32(1), ptr.ChkIdx)
	require.Equal(t, uint32(0), ptr.RowIdx)
	row := l.GetRow(ptr)
	require.Equal(t, int64(1), row.GetInt64(0))

	// Test iteration.
	l.Reset()
	for i := 0; i < 5; i++ {
		tmp := NewChunkWithCapacity(fields, 32)
		tmp.AppendInt64(0, int64(i))
		l.AppendRow(tmp.GetRow(0))
	}
	expected := []int64{0, 1, 2, 3, 4}
	var results []int64
	err := l.Walk(func(r Row) error {
		results = append(results, r.GetInt64(0))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, expected, results)
}

func TestListMemoryUsage(t *testing.T) {
	fieldTypes := make([]*types.FieldType, 0, 5)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeVarchar))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeJSON))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDatetime))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDuration))

	jsonObj, err := json.ParseBinaryFromString("1")
	require.NoError(t, err)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	maxChunkSize := 2
	srcChk := NewChunkWithCapacity(fieldTypes, maxChunkSize)
	srcChk.AppendFloat32(0, 12.4)
	srcChk.AppendString(1, "123")
	srcChk.AppendJSON(2, jsonObj)
	srcChk.AppendTime(3, timeObj)
	srcChk.AppendDuration(4, durationObj)

	list := NewList(fieldTypes, maxChunkSize, maxChunkSize*2)
	require.Equal(t, int64(0), list.GetMemTracker().BytesConsumed())

	list.AppendRow(srcChk.GetRow(0))
	require.Equal(t, int64(0), list.GetMemTracker().BytesConsumed())

	memUsage := list.chunks[0].MemoryUsage()
	list.Reset()
	require.Equal(t, memUsage, list.GetMemTracker().BytesConsumed())

	list.Add(srcChk)
	require.Equal(t, memUsage+srcChk.MemoryUsage(), list.GetMemTracker().BytesConsumed())
}

func BenchmarkListMemoryUsage(b *testing.B) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeFloat))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeVarchar))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDatetime))
	fieldTypes = append(fieldTypes, types.NewFieldType(mysql.TypeDuration))

	chk := NewChunkWithCapacity(fieldTypes, 2)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}
	chk.AppendFloat64(0, 123.123)
	chk.AppendString(1, "123")
	chk.AppendTime(2, timeObj)
	chk.AppendDuration(3, durationObj)
	row := chk.GetRow(0)

	initCap := 50
	list := NewList(fieldTypes, 2, 8)
	for i := 0; i < initCap; i++ {
		list.AppendRow(row)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.GetMemTracker().BytesConsumed()
	}
}

func BenchmarkListAdd(b *testing.B) {
	numChk, numRow := 1, 2
	chks, fields := initChunks(numChk, numRow)
	chk := chks[0]
	l := NewList(fields, numRow, numRow)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Add(chk)
	}
}

func BenchmarkListGetRow(b *testing.B) {
	numChk, numRow := 10000, 2
	chks, fields := initChunks(numChk, numRow)
	l := NewList(fields, numRow, numRow)
	for _, chk := range chks {
		l.Add(chk)
	}
	rand.Seed(0)
	ptrs := make([]RowPtr, 0, b.N)
	for i := 0; i < mathutil.Min(b.N, 10000); i++ {
		ptrs = append(ptrs, RowPtr{
			ChkIdx: rand.Uint32() % uint32(numChk),
			RowIdx: rand.Uint32() % uint32(numRow),
		})
	}
	for i := 10000; i < cap(ptrs); i++ {
		ptrs = append(ptrs, ptrs[i%10000])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.GetRow(ptrs[i])
	}
}
