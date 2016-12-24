// Copyright 2016 PingCAP, Inc.
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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
)

// Schema stands for the row schema get from input.
type Schema struct {
	Columns []*Column
}

// String implements fmt.Stringer interface.
func (s Schema) String() string {
	colStrs := make([]string, 0, len(s.Columns))
	for _, col := range s.Columns {
		colStrs = append(colStrs, col.String())
	}
	return "[" + strings.Join(colStrs, ",") + "]"
}

// Clone copies the total schema.
func (s Schema) Clone() Schema {
	cols := make([]*Column, 0, len(s.Columns))
	for _, col := range s.Columns {
		newCol := *col
		cols = append(cols, &newCol)
	}
	result := Schema{Columns:cols}
	return result
}

// FindColumn finds an Column from schema for a ast.ColumnName. It compares the db/table/column names.
// If there are more than one result, it will raise ambiguous error.
func (s Schema) FindColumn(astCol *ast.ColumnName) (*Column, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, col := range s.Columns {
		if (dbName.L == "" || dbName.L == col.DBName.L) &&
			(tblName.L == "" || tblName.L == col.TblName.L) &&
			(colName.L == col.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return nil, errors.Errorf("Column %s is ambiguous", col.String())
			}
		}
	}
	if idx == -1 {
		return nil, nil
	}
	return s.Columns[idx], nil
}

// InitColumnIndices sets indices for columns in schema.
func (s Schema) InitColumnIndices() {
	for i, c := range s.Columns {
		c.Index = i
	}
}

// RetrieveColumn retrieves column in expression from the columns in schema.
func (s Schema) RetrieveColumn(col *Column) *Column {
	index := s.GetColumnIndex(col)
	if index != -1 {
		return s.Columns[index]
	}
	return nil
}

// GetColumnIndex finds the index for a column.
func (s Schema) GetColumnIndex(col *Column) int {
	for i, c := range s.Columns {
		if c.FromID == col.FromID && c.Position == col.Position {
			return i
		}
	}
	return -1
}

// MergeSchame will merge two schema into one schema.
func MergeSchema(lSchema, rSchema Schema) Schema {
	lSchema.Columns = append(lSchema.Columns, rSchema.Columns...)
	return lSchema
}
