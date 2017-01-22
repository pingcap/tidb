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

// KeyInfo stores the columns of one unique key or primary key.
type KeyInfo []*Column

// Clone copies the entire UniqueKey.
func (ki KeyInfo) Clone() KeyInfo {
	result := make([]*Column, 0, len(ki))
	for _, col := range ki {
		newCol := *col
		result = append(result, &newCol)
	}
	return result
}

// Schema stands for the row schema and unique key information get from input.
type Schema struct {
	Columns []*Column
	Keys    []KeyInfo
}

// String implements fmt.Stringer interface.
func (s Schema) String() string {
	colStrs := make([]string, 0, len(s.Columns))
	for _, col := range s.Columns {
		colStrs = append(colStrs, col.String())
	}
	ukStrs := make([]string, 0, len(s.Keys))
	for _, key := range s.Keys {
		ukColStrs := make([]string, 0, len(key))
		for _, col := range key {
			ukColStrs = append(ukColStrs, col.String())
		}
		ukStrs = append(ukStrs, "["+strings.Join(ukColStrs, ",")+"]")
	}
	return "Column: [" + strings.Join(colStrs, ",") + "] Unique key: [" + strings.Join(ukStrs, ",") + "]"
}

// Clone copies the total schema.
func (s Schema) Clone() Schema {
	result := NewSchema(make([]*Column, 0, s.Len()))
	keys := make([]KeyInfo, 0, len(s.Keys))
	for _, col := range s.Columns {
		newCol := *col
		result.Append(&newCol)
	}
	for _, key := range s.Keys {
		keys = append(keys, key.Clone())
	}
	result.SetUniqueKeys(keys)
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

// Len returns the number of columns in schema.
func (s Schema) Len() int {
	return len(s.Columns)
}

// Append append new column to the columns stored in schema.
func (s *Schema) Append(col *Column) {
	s.Columns = append(s.Columns, col)
}

// SetUniqueKeys will set the value of Schema.Keys.
func (s *Schema) SetUniqueKeys(keys []KeyInfo) {
	s.Keys = keys
}

// GetColumnsIndices will return a slice which contains the position of each column in schema.
// If there is one column that doesn't match, nil will be returned.
func (s Schema) GetColumnsIndices(cols []*Column) (ret []int) {
	ret = make([]int, 0, len(cols))
	for _, col := range cols {
		pos := s.GetColumnIndex(col)
		if pos != -1 {
			ret = append(ret, pos)
		} else {
			return nil
		}
	}
	return
}

// MergeSchema will merge two schema into one schema.
func MergeSchema(lSchema, rSchema Schema) Schema {
	tmpL := lSchema.Clone()
	tmpR := rSchema.Clone()
	ret := NewSchema(append(tmpL.Columns, tmpR.Columns...))
	ret.SetUniqueKeys(append(tmpL.Keys, tmpR.Keys...))
	return ret
}

// NewSchema returns a schema made by its parameter.
func NewSchema(cols []*Column) Schema {
	return Schema{Columns: cols}
}
