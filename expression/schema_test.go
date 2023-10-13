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

	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

type schemaGenerator struct {
	colID int64
}

// generateKeys4Schema will generate keys for a given schema. Used only in this file.
func generateKeys4Schema(schema *Schema) {
	keyCount := len(schema.Columns) - 1
	keys := make([]KeyInfo, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		keys = append(keys, []*Column{schema.Columns[i]})
	}
	schema.Keys = keys
}

// generateSchema will generate a schema for test. Used only in this file.
func (s *schemaGenerator) generateSchema(colCount int) *Schema {
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		s.colID++
		cols = append(cols, &Column{
			UniqueID: s.colID,
		})
	}
	return NewSchema(cols...)
}

func TestSchemaString(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(5)
	require.Equal(t, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] Unique key: []", schema.String())
	generateKeys4Schema(schema)
	require.Equal(t, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] Unique key: [[Column#1],[Column#2],[Column#3],[Column#4]]", schema.String())
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
			require.Equal(t, true, schema.IsUniqueKey(col))
		} else {
			require.Equal(t, false, schema.IsUniqueKey(col))
		}
	}
	require.Equal(t, false, schema.IsUniqueKey(colOutSchema))
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
	for i := 0; i < len(schema.Columns)-1; i++ {
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
	for i := 0; i < len(lSchema.Columns); i++ {
		require.Equal(t, lSchema.Columns[i].UniqueID, schema.Columns[i].UniqueID)
	}
	for i := 0; i < len(rSchema.Columns); i++ {
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

	used := GetUsedList(usedCols, schema)
	require.Equal(t, []bool{false, true, false, true, false}, used)
}

func TestGetExtraHandleColumn(t *testing.T) {
	s := &schemaGenerator{}
	schema := s.generateSchema(0)
	extraHandleCol, _ := schema.GetExtraHandleColumn()
	require.Nil(t, extraHandleCol)

	col := &Column{
		UniqueID: 100,
	}
	schema.Append(col)
	extraHandleCol, _ = schema.GetExtraHandleColumn()
	require.Nil(t, extraHandleCol)

	extraCol := &Column{
		UniqueID: 200,
		ID:       model.ExtraHandleID,
	}
	schema.Append(extraCol)
	extraHandleCol, _ = schema.GetExtraHandleColumn()
	require.Equal(t, extraCol.ID, extraHandleCol.ID)
}

func TestCleanExtraColumns(t *testing.T) {
	columns := make([]*Column, 0)
	columns = CleanExtraColumns(columns)
	require.Equal(t, 0, len(columns))

	extraCol := &Column{
		UniqueID: 100,
		ID:       model.ExtraHandleID,
	}
	columns = append(columns, extraCol)
	require.Equal(t, 1, len(columns))
	columns = CleanExtraColumns(columns)
	require.Equal(t, 0, len(columns))

	col := &Column{
		UniqueID: 200,
	}
	columns = append(columns, col)
	columns = CleanExtraColumns(columns)
	require.Equal(t, 1, len(columns))

	columns = append(columns, extraCol)
	require.Equal(t, 2, len(columns))
	columns = CleanExtraColumns(columns)
	require.Equal(t, 1, len(columns))
	require.Equal(t, col.ID, columns[0].ID)
}
