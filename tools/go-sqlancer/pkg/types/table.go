// Copyright 2022 PingCAP, Inc.
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

package types

// Table defines database table
type Table struct {
	Name      CIStr
	AliasName CIStr
	Columns   Columns
	Indexes   []CIStr
	Type      string
}

// Clone copy table struct
func (t Table) Clone() Table {
	newTable := Table{
		Name:      t.Name,
		AliasName: t.AliasName,
		Columns:   make([]Column, len(t.Columns)),
		Indexes:   t.Indexes,
		Type:      t.Type,
	}
	for i, column := range t.Columns {
		newTable.Columns[i] = column.Clone()
	}
	return newTable
}

// GetColumns get ordered columns
func (t *Table) GetColumns() []Column {
	return t.Columns
}

// Rename is to rename a table.
func (t Table) Rename(name string) Table {
	newTable := Table{
		Name:      t.Name,
		AliasName: CIStr(name),
		Columns:   make([]Column, len(t.Columns)),
		Indexes:   t.Indexes,
		Type:      t.Type,
	}
	for i, column := range t.Columns {
		col := column.Clone()
		col.AliasTable = CIStr(name)
		newTable.Columns[i] = col
	}
	return newTable
}

// JoinWithName is join with name
func (t *Table) JoinWithName(b Table, name string) Table {
	cols := make([]Column, 0, len(t.Columns)+len(b.Columns))
	for _, col := range t.Columns {
		c := col.Clone()
		c.AliasTable = CIStr(name)
		cols = append(cols, c)
	}
	for _, col := range b.Columns {
		c := col.Clone()
		c.AliasTable = CIStr(name)
		cols = append(cols, c)
	}
	return Table{
		Name:    CIStr(name),
		Columns: cols,
		Indexes: nil,
		Type:    "TMP TABLE",
	}
}

// GetAliasName get tmp table name, otherwise origin name
func (t Table) GetAliasName() CIStr {
	if t.AliasName != "" {
		return t.AliasName
	}
	return t.Name
}
