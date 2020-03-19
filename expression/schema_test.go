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

package expression

import (
	"fmt"

	. "github.com/pingcap/check"
)

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
func (s *testEvalSuite) generateSchema(colCount int) *Schema {
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			UniqueID: s.allocColID(),
		})
	}
	return NewSchema(cols...)
}

func (s *testEvalSuite) TestSchemaString(c *C) {
	schema := s.generateSchema(5)
	c.Assert(schema.String(), Equals, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] Unique key: []")
	generateKeys4Schema(schema)
	c.Assert(schema.String(), Equals, "Column: [Column#1,Column#2,Column#3,Column#4,Column#5] Unique key: [[Column#1],[Column#2],[Column#3],[Column#4]]")
}

func (s *testEvalSuite) TestSchemaRetrieveColumn(c *C) {
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for _, col := range schema.Columns {
		c.Assert(schema.RetrieveColumn(col), Equals, col)
	}
	c.Assert(schema.RetrieveColumn(colOutSchema), IsNil)
}

func (s *testEvalSuite) TestSchemaIsUniqueKey(c *C) {
	schema := s.generateSchema(5)
	generateKeys4Schema(schema)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for i, col := range schema.Columns {
		if i < len(schema.Columns)-1 {
			c.Assert(schema.IsUniqueKey(col), Equals, true)
		} else {
			c.Assert(schema.IsUniqueKey(col), Equals, false)
		}
	}
	c.Assert(schema.IsUniqueKey(colOutSchema), Equals, false)
}

func (s *testEvalSuite) TestSchemaContains(c *C) {
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for _, col := range schema.Columns {
		c.Assert(schema.Contains(col), Equals, true)
	}
	c.Assert(schema.Contains(colOutSchema), Equals, false)
}

func (s *testEvalSuite) TestSchemaColumnsIndices(c *C) {
	schema := s.generateSchema(5)
	colOutSchema := &Column{
		UniqueID: 100,
	}
	for i := 0; i < len(schema.Columns)-1; i++ {
		colIndices := schema.ColumnsIndices([]*Column{schema.Columns[i], schema.Columns[i+1]})
		for j, res := range colIndices {
			c.Assert(res, Equals, i+j)
		}
	}
	c.Assert(schema.ColumnsIndices([]*Column{schema.Columns[0], schema.Columns[1], colOutSchema, schema.Columns[2]}), IsNil)
}

func (s *testEvalSuite) TestSchemaColumnsByIndices(c *C) {
	schema := s.generateSchema(5)
	indices := []int{0, 1, 2, 3}
	retCols := schema.ColumnsByIndices(indices)
	for i, ret := range retCols {
		c.Assert(fmt.Sprintf("%p", schema.Columns[i]), Equals, fmt.Sprintf("%p", ret))
	}
}

func (s *testEvalSuite) TestSchemaMergeSchema(c *C) {
	lSchema := s.generateSchema(5)
	generateKeys4Schema(lSchema)

	rSchema := s.generateSchema(5)
	generateKeys4Schema(rSchema)

	c.Assert(MergeSchema(nil, nil), IsNil)
	c.Assert(MergeSchema(lSchema, nil).String(), Equals, lSchema.String())
	c.Assert(MergeSchema(nil, rSchema).String(), Equals, rSchema.String())

	schema := MergeSchema(lSchema, rSchema)
	for i := 0; i < len(lSchema.Columns); i++ {
		c.Assert(schema.Columns[i].UniqueID, Equals, lSchema.Columns[i].UniqueID)
	}
	for i := 0; i < len(rSchema.Columns); i++ {
		c.Assert(schema.Columns[i+len(lSchema.Columns)].UniqueID, Equals, rSchema.Columns[i].UniqueID)
	}
}

func (s *testEvalSuite) TestGetUsedList(c *C) {
	schema := s.generateSchema(5)
	var usedCols []*Column
	usedCols = append(usedCols, schema.Columns[3])
	usedCols = append(usedCols, s.generateSchema(2).Columns...)
	usedCols = append(usedCols, schema.Columns[1])
	usedCols = append(usedCols, schema.Columns[3])

	used := GetUsedList(usedCols, schema)
	c.Assert(used, DeepEquals, []bool{false, true, false, true, false})
}
