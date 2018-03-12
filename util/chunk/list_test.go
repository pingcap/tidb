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
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testChunkSuite) TestList(c *check.C) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	l := NewList(fields, 2)
	srcChunk := NewChunk(fields)
	srcChunk.AppendInt64(0, 1)
	srcRow := srcChunk.GetRow(0)

	// Test basic append.
	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	c.Assert(l.NumChunks(), check.Equals, 3)
	c.Assert(l.Len(), check.Equals, 5)
	c.Assert(len(l.freelist), check.Equals, 0)

	// Test chunk reuse.
	l.Reset()
	c.Assert(len(l.freelist), check.Equals, 3)

	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	c.Assert(len(l.freelist), check.Equals, 0)

	// Test add chunk then append row.
	l.Reset()
	nChunk := NewChunk(fields)
	nChunk.AppendNull(0)
	l.Add(nChunk)
	ptr := l.AppendRow(srcRow)
	c.Assert(l.NumChunks(), check.Equals, 2)
	c.Assert(ptr.ChkIdx, check.Equals, uint32(1))
	c.Assert(ptr.RowIdx, check.Equals, uint32(0))
	row := l.GetRow(ptr)
	c.Assert(row.GetInt64(0), check.Equals, int64(1))

	// Test iteration.
	l.Reset()
	for i := 0; i < 5; i++ {
		tmp := NewChunk(fields)
		tmp.AppendInt64(0, int64(i))
		l.AppendRow(tmp.GetRow(0))
	}
	expected := []int64{0, 1, 2, 3, 4}
	var results []int64
	err := l.Walk(func(r Row) error {
		results = append(results, r.GetInt64(0))
		return nil
	})
	c.Assert(err, check.IsNil)
	c.Assert(results, check.DeepEquals, expected)
}

func (s *testChunkSuite) TestListMemoryUsage(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 5)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

	maxChunkSize := 2
	list := NewList(fieldTypes, maxChunkSize)
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, int64(0))

	initCap := 10
	srcChk := NewChunkWithCapacity(fieldTypes, initCap)

	jsonObj, err := json.ParseBinaryFromString("1")
	c.Assert(err, check.IsNil)
	timeObj := types.Time{Time: types.FromGoTime(time.Now()), Fsp: 0, Type: mysql.TypeDatetime}
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	srcChk.AppendFloat32(0, 12.4)
	srcChk.AppendString(1, "123")
	srcChk.AppendJSON(2, jsonObj)
	srcChk.AppendTime(3, timeObj)
	srcChk.AppendDuration(4, durationObj)

	row := srcChk.GetRow(0)
	list.AppendRow(row)

	memUsage := NewChunk(fieldTypes).MemoryUsage()
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, int64(0))

	list.Reset()
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, memUsage)

	list.Add(srcChk)
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, memUsage+srcChk.MemoryUsage())
}

func BenchmarkListMemoryUsage(b *testing.B) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

	chk := NewChunkWithCapacity(fieldTypes, 2)
	timeObj := types.Time{Time: types.FromGoTime(time.Now()), Fsp: 0, Type: mysql.TypeDatetime}
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}
	chk.AppendFloat64(0, 123.123)
	chk.AppendString(1, "123")
	chk.AppendTime(2, timeObj)
	chk.AppendDuration(3, durationObj)
	row := chk.GetRow(0)

	initCap := 50
	list := NewList(fieldTypes, 2)
	for i := 0; i < initCap; i++ {
		list.AppendRow(row)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.GetMemTracker().BytesConsumed()
	}
}
