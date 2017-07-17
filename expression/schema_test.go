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
	"github.com/pingcap/tidb/model"
)

func (s *testEvalSuite) TestSchemaString(c *C) {
	colCount := 5
	keyInfoCount := 3
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			FromID:   "dual",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			cols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			cols[i].TblName = model.NewCIStr("")
		}
	}
	schema := NewSchema(cols...)
	c.Assert(schema.String(), Equals, "Column: [t.b.c0,t.b.c1,b.c2,c3,c4] Unique key: []")
	keys := make([]KeyInfo, 0, keyInfoCount)
	for i := 0; i < keyInfoCount; i++ {
		keys = append(keys, []*Column{cols[i], cols[i+1]})
	}
	schema.Keys = keys
	c.Assert(schema.String(), Equals, "Column: [t.b.c0,t.b.c1,b.c2,c3,c4] Unique key: [[t.b.c0,t.b.c1],[t.b.c1,b.c2],[b.c2,c3]]")
}

func (s *testEvalSuite) TestSchemaRetrieveColumn(c *C) {
	colCount := 5
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			FromID:   "dual",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			cols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			cols[i].TblName = model.NewCIStr("")
		}
	}
	colOutSchema := &Column{
		FromID:   "dual",
		Position: 100,
	}
	schema := NewSchema(cols...)
	for _, col := range cols {
		c.Assert(schema.RetrieveColumn(col), Equals, col)
	}
	c.Assert(schema.RetrieveColumn(colOutSchema), IsNil)
}

func (s *testEvalSuite) TestSchemaIsUniqueKey(c *C) {
	colCount := 5
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			FromID:   "dual",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			cols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			cols[i].TblName = model.NewCIStr("")
		}
	}
	colOutSchema := &Column{
		FromID:   "dual",
		Position: 100,
	}
	schema := NewSchema(cols...)
	keys := make([]KeyInfo, 0, colCount)
	for i := 0; i < colCount; i++ {
		keys = append(keys, []*Column{cols[i]})
	}
	schema.Keys = keys
	for _, col := range cols {
		c.Assert(schema.IsUniqueKey(col), Equals, true)
	}
	c.Assert(schema.IsUniqueKey(colOutSchema), Equals, false)
}

func (s *testEvalSuite) TestSchemaContains(c *C) {
	colCount := 5
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			FromID:   "dual",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			cols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			cols[i].TblName = model.NewCIStr("")
		}
	}
	colOutSchema := &Column{
		FromID:   "dual",
		Position: 100,
	}
	schema := NewSchema(cols...)
	for _, col := range cols {
		c.Assert(schema.Contains(col), Equals, true)
	}
	c.Assert(schema.Contains(colOutSchema), Equals, false)
}

func (s *testEvalSuite) TestSchemaColumnsIndices(c *C) {
	colCount := 5
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			FromID:   "dual",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			cols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			cols[i].TblName = model.NewCIStr("")
		}
	}
	colOutSchema := &Column{
		FromID:   "dual",
		Position: 100,
	}
	schema := NewSchema(cols...)
	for i := 0; i < len(cols)-1; i++ {
		colIndices := schema.ColumnsIndices([]*Column{cols[i], cols[i+1]})
		for j, res := range colIndices {
			c.Assert(res, Equals, i+j)
		}
	}
	c.Assert(schema.ColumnsIndices([]*Column{cols[0], cols[1], colOutSchema, cols[2]}), IsNil)
}

func (s *testEvalSuite) TestSchemaColumnsByIndices(c *C) {
	colCount := 5
	cols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		cols = append(cols, &Column{
			FromID:   "dual",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			cols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			cols[i].TblName = model.NewCIStr("")
		}
	}
	schema := NewSchema(cols...)
	indices := []int{0, 1, 2, 3}
	retCols := schema.ColumnsByIndices(indices)
	for i, ret := range retCols {
		c.Assert(fmt.Sprintf("%p", cols[i]), Equals, fmt.Sprintf("%p", ret))
	}
}

func (s *testEvalSuite) TestSchemaMergeSchema(c *C) {
	colCount := 5
	keyInfoCount := 3
	lCols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		lCols = append(lCols, &Column{
			FromID:   "left",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			lCols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			lCols[i].TblName = model.NewCIStr("")
		}
	}
	lKeys := make([]KeyInfo, 0, keyInfoCount)
	for i := 0; i < keyInfoCount; i++ {
		lKeys = append(lKeys, []*Column{lCols[i], lCols[i+1]})
	}
	lSchema := NewSchema(lCols...)
	lSchema.Keys = lKeys

	rCols := make([]*Column, 0, colCount)
	for i := 0; i < colCount; i++ {
		rCols = append(rCols, &Column{
			FromID:   "right",
			Position: i,
			DBName:   model.NewCIStr("T"),
			TblName:  model.NewCIStr("B"),
			ColName:  model.NewCIStr(fmt.Sprintf("C%v", i)),
		})
		if i >= 2 {
			rCols[i].DBName = model.NewCIStr("")
		}
		if i >= 3 {
			rCols[i].TblName = model.NewCIStr("")
		}
	}
	rKeys := make([]KeyInfo, 0, keyInfoCount)
	for i := 0; i < keyInfoCount; i++ {
		rKeys = append(rKeys, []*Column{rCols[i], rCols[i+1]})
	}
	rSchema := NewSchema(rCols...)
	rSchema.Keys = rKeys

	c.Assert(MergeSchema(nil, nil), IsNil)
	c.Assert(MergeSchema(lSchema, nil).String(), Equals, lSchema.String())
	c.Assert(MergeSchema(nil, rSchema).String(), Equals, rSchema.String())

	schema := MergeSchema(lSchema, rSchema)
	for i := 0; i < colCount; i++ {
		c.Assert(schema.Columns[i].FromID, Equals, lSchema.Columns[i].FromID)
		c.Assert(schema.Columns[i].Position, Equals, lSchema.Columns[i].Position)
		c.Assert(schema.Columns[i].DBName, Equals, lSchema.Columns[i].DBName)
		c.Assert(schema.Columns[i].TblName, Equals, lSchema.Columns[i].TblName)
		c.Assert(schema.Columns[i].ColName, Equals, lSchema.Columns[i].ColName)
	}
	for i := 0; i < colCount; i++ {
		c.Assert(schema.Columns[i+colCount].FromID, Equals, rSchema.Columns[i].FromID)
		c.Assert(schema.Columns[i+colCount].Position, Equals, rSchema.Columns[i].Position)
		c.Assert(schema.Columns[i+colCount].DBName, Equals, rSchema.Columns[i].DBName)
		c.Assert(schema.Columns[i+colCount].TblName, Equals, rSchema.Columns[i].TblName)
		c.Assert(schema.Columns[i+colCount].ColName, Equals, rSchema.Columns[i].ColName)
	}
	for i := 0; i < keyInfoCount; i++ {
		for j := 0; j < len(schema.Keys[i]); j++ {
			c.Assert(schema.Keys[i][j].Equal(lSchema.Keys[i][j], nil), Equals, true)
		}
	}
	for i := 0; i < keyInfoCount; i++ {
		for j := 0; j < len(schema.Keys[i]); j++ {
			c.Assert(schema.Keys[i+keyInfoCount][j].Equal(rSchema.Keys[i][j], nil), Equals, true)
		}
	}
}
