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
type Schema []*Column

// String implements fmt.Stringer interface.
func (s Schema) String() string {
	strs := make([]string, 0, len(s))
	for _, col := range s {
		strs = append(strs, col.String())
	}
	return "[" + strings.Join(strs, ",") + "]"
}

// Clone copies the total schema.
func (s Schema) Clone() Schema {
	result := make(Schema, 0, len(s))
	for _, col := range s {
		newCol := *col
		result = append(result, &newCol)
	}
	return result
}

// FindColumn finds an Column from schema for a ast.ColumnName. It compares the db/table/column names.
// If there are more than one result, it will raise ambiguous error.
func (s Schema) FindColumn(astCol *ast.ColumnName) (*Column, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, col := range s {
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
	return s[idx], nil
}

// InitIndices sets indices for columns in schema.
func (s Schema) InitIndices() {
	for i, c := range s {
		c.Index = i
	}
}

// RetrieveColumn retrieves column in expression from the columns in schema.
func (s Schema) RetrieveColumn(col *Column) *Column {
	index := s.GetIndex(col)
	if index != -1 {
		return s[index]
	}
	return nil
}

// GetIndex finds the index for a column.
func (s Schema) GetIndex(col *Column) int {
	for i, c := range s {
		if c.FromID == col.FromID && c.Position == col.Position {
			return i
		}
	}
	return -1
}

//Schema2Exprs converts []*Column to []Expression.
func Schema2Exprs(schema Schema) []Expression {
	result := make([]Expression, 0, len(schema))
	for _, col := range schema {
		result = append(result, col.Clone())
	}
	return result
}
