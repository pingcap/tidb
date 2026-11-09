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

package expression

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type schemaGenerator struct {
	colID int64
}

// generateKeys4Schema will generate keys for a given schema. Used only in this file.
func generateKeys4Schema(schema *Schema) {
	keyCount := len(schema.Columns) - 1
	keys := make([]KeyInfo, 0, keyCount)
	for i := range keyCount {
		keys = append(keys, []*Column{schema.Columns[i]})
	}
	schema.PKOrUK = keys
}

// generateSchema will generate a schema for test. Used only in this file.
func (s *schemaGenerator) generateSchema(colCount int) *Schema {
	cols := make([]*Column, 0, colCount)
	for range colCount {
		s.colID++
		cols = append(cols, &Column{
			UniqueID: s.colID,
		})
	}
	return NewSchema(cols...)
}

func TestSchemaClone(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	generateKeys4Schema(schema)

	uniKeys := make([]KeyInfo, 0, len(schema.Columns)-1)
	for i := range len(schema.Columns) - 1 {
		uniKeys = append(uniKeys, []*Column{schema.Columns[i]})
	}
	schema.SetUniqueKeys(uniKeys)

	clonedSchema := schema.Clone()
	require.Equal(t, schema.String(), clonedSchema.String())

	require.NotSame(t, unsafe.SliceData(schema.PKOrUK), unsafe.SliceData(clonedSchema.PKOrUK))
	require.NotSame(t, unsafe.SliceData(schema.NullableUK), unsafe.SliceData(clonedSchema.NullableUK))
}

func TestSchemaString(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	require.Equal(t, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] PKOrUK: [] NullableUK: []", schema.String())
	generateKeys4Schema(schema)
	require.Equal(t, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] PKOrUK: [[Column#1],[Column#2],[Column#3],[Column#4]] NullableUK: []", schema.String())
}

func TestSchemaRetrieveColumn(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for _, col := range schema.Columns {
		require.Equal(t, col, schema.RetrieveColumn(col))
	}
	require.Nil(t, schema.RetrieveColumn(colOutSchema))
}

func TestSchemaIsUniqueKey(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	generateKeys4Schema(schema)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for i, col := range schema.Columns {
		if i < len(schema.Columns)-1 {
			require.Equal(t, true, schema.IsUnique(true, col))
		} else {
			require.Equal(t, false, schema.IsUnique(true, col))
		}
	}
	require.Equal(t, false, schema.IsUnique(true, colOutSchema))
}

func TestSchemaContains(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for _, col := range schema.Columns {
		require.Equal(t, true, schema.Contains(col))
	}
	require.Equal(t, false, schema.Contains(colOutSchema))
}

func TestSchemaColumnsIndices(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for i := range len(schema.Columns) - 1 {
		colIndices := schema.ColumnsIndices([]*Column{schema.Columns[i], schema.Columns[i+1]})
		for j, res := range colIndices {
			require.Equal(t, i+j, res)
		}
	}
	require.Nil(t, schema.ColumnsIndices([]*Column{schema.Columns[0], schema.Columns[1], colOutSchema, schema.Columns[2]}))
}

func TestSchemaColumnsByIndices(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	indices := []int{0, 1, 2, 3}
	retCols := schema.ColumnsByIndices(indices)
	for i, ret := range retCols {
		require.Equal(t, fmt.Sprintf("%p", ret), fmt.Sprintf("%p", schema.Columns[i]))
	}
}

func TestSchemaMergeSchema(t *testing.T) {
	s := &schemaGenerator{}
	lSchema := s.generateSchema(5)
	generateKeys4Schema(lSchema)

	rSchema := s.generateSchema(5)
	generateKeys4Schema(rSchema)

	require.Nil(t, MergeSchema(nil, nil))
	require.Equal(t, lSchema.String(), MergeSchema(lSchema, nil).String())
	require.Equal(t, rSchema.String(), MergeSchema(nil, rSchema).String())

	schema := MergeSchema(lSchema, rSchema)
	for i := range lSchema.Columns {
		require.Equal(t, lSchema.Columns[i].UniqueID, schema.Columns[i].UniqueID)
	}
	for i := range rSchema.Columns {
		require.Equal(t, rSchema.Columns[i].UniqueID, schema.Columns[i+len(lSchema.Columns)].UniqueID)
	}
}

func TestGetUsedList(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	var usedCols []*Column
	usedCols = append(usedCols, schema.Columns[3])
	usedCols = append(usedCols, s.generateSchema(2).Columns...)
	usedCols = append(usedCols, schema.Columns[1])
	usedCols = append(usedCols, schema.Columns[3])

	used := GetUsedList(mock.NewContext(), usedCols, schema)
	require.Equal(t, []bool{false, true, false, true, false}, used)

	// Cover both the linear-scan path (fewer than usedListSetThreshold used
	// columns) and the UniqueID-set path (at or above it) on a wide schema, and
	// check them against Schema.Contains. usedCols has exactly usedCount entries,
	// so the threshold-1 and threshold cases take different paths. The last three
	// entries (when there is room) are a duplicate, a column from another schema,
	// and a prefix column sharing a UniqueID with a schema column, which must
	// still count as used.
	for _, ids := range []struct{ base, stride int64 }{
		{0, 1}, {1000000, 1}, {1000000, 1024}, {-1000000, 1024}, {1 << 40, 1024},
	} {
		wide := s.generateSchema(300)
		foreign := s.generateSchema(10).Columns
		for _, col := range append(append([]*Column(nil), wide.Columns...), foreign...) {
			col.UniqueID = ids.base + col.UniqueID*ids.stride
		}
		for _, usedCount := range []int{0, 1, usedListSetThreshold - 1, usedListSetThreshold, 150} {
			usedCols = usedCols[:0]
			for i := range usedCount {
				usedCols = append(usedCols, wide.Columns[(i*7)%len(wide.Columns)])
			}
			if usedCount >= 4 {
				prefixCol := wide.Columns[299].Clone().(*Column)
				prefixCol.IsPrefix = true
				usedCols[usedCount-3] = usedCols[0]
				usedCols[usedCount-2] = foreign[usedCount%len(foreign)]
				usedCols[usedCount-1] = prefixCol
			}
			require.Len(t, usedCols, usedCount)
			used = GetUsedList(mock.NewContext(), usedCols, wide)
			usedSchema := NewSchema(usedCols...)
			for i, col := range wide.Columns {
				require.Equal(t, usedSchema.Contains(col), used[i], "usedCount=%d column=%d", usedCount, i)
			}
		}
	}
}

func BenchmarkGetUsedList(b *testing.B) {
	ctx := mock.NewContext()
	for _, ids := range []struct {
		name         string
		base, stride int64
	}{
		{"dense", 0, 1},
		{"offset", 1000000, 1},
		{"sparse", 1000000, 1024},
	} {
		for _, tc := range []struct{ schemaLen, usedLen int }{
			{8, 4}, {64, 16}, {64, 31}, {64, 32}, {64, 64}, {4000, 10}, {4000, 32}, {4000, 4000},
		} {
			s := &schemaGenerator{}
			schema := s.generateSchema(tc.schemaLen)
			for i, col := range schema.Columns {
				col.UniqueID = ids.base + int64(i+1)*ids.stride
			}
			usedCols := make([]*Column, 0, tc.usedLen)
			for i := range tc.usedLen {
				usedCols = append(usedCols, schema.Columns[(i*7)%tc.schemaLen])
			}
			b.Run(fmt.Sprintf("ids=%s/schema=%d/used=%d", ids.name, tc.schemaLen, tc.usedLen), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					GetUsedList(ctx, usedCols, schema)
				}
			})
		}
	}
}
