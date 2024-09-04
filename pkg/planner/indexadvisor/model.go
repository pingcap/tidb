// Copyright 2024 PingCAP, Inc.
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

package indexadvisor

import (
	"fmt"
	"math"
	"strings"
)

// Query represents a Query statement.
type Query struct { // DQL or DML
	Alias      string
	SchemaName string
	Text       string
	Frequency  int
	CostPerMon float64
}

// Key returns the key of the Query.
func (q Query) Key() string {
	return q.Text
}

// Column represents a column.
type Column struct {
	SchemaName string
	TableName  string
	ColumnName string
}

// NewColumn creates a new column.
func NewColumn(schemaName, tableName, columnName string) Column {
	return Column{SchemaName: strings.ToLower(schemaName),
		TableName: strings.ToLower(tableName), ColumnName: strings.ToLower(columnName)}
}

// NewColumns creates new columns.
func NewColumns(schemaName, tableName string, columnNames ...string) []Column {
	cols := make([]Column, 0, len(columnNames))
	for _, col := range columnNames {
		cols = append(cols, NewColumn(schemaName, tableName, col))
	}
	return cols
}

// Key returns the key of the column.
func (c Column) Key() string {
	return fmt.Sprintf("%v.%v.%v", c.SchemaName, c.TableName, c.ColumnName)
}

// Index represents an index.
type Index struct {
	SchemaName string
	TableName  string
	IndexName  string
	Columns    []Column
}

// NewIndex creates a new index.
func NewIndex(schemaName, tableName, indexName string, columns ...string) Index {
	return Index{SchemaName: strings.ToLower(schemaName), TableName: strings.ToLower(tableName),
		IndexName: strings.ToLower(indexName), Columns: NewColumns(schemaName, tableName, columns...)}
}

// NewIndexWithColumns creates a new index with columns.
func NewIndexWithColumns(indexName string, columns ...Column) Index {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.ColumnName
	}
	return NewIndex(columns[0].SchemaName, columns[0].TableName, indexName, names...)
}

// Key returns the key of the index.
func (i Index) Key() string {
	names := make([]string, 0, len(i.Columns))
	for _, col := range i.Columns {
		names = append(names, col.ColumnName)
	}
	return fmt.Sprintf("%v.%v(%v)", i.SchemaName, i.TableName, strings.Join(names, ","))
}

// PrefixContain returns whether j is a prefix of i.
func (i Index) PrefixContain(j Index) bool {
	if i.SchemaName != j.SchemaName || i.TableName != j.TableName || len(i.Columns) < len(j.Columns) {
		return false
	}
	for k := range j.Columns {
		if i.Columns[k].ColumnName != j.Columns[k].ColumnName {
			return false
		}
	}
	return true
}

// IndexSetCost is the cost of a index configuration.
type IndexSetCost struct {
	TotalWorkloadQueryCost    float64
	TotalNumberOfIndexColumns int
	IndexKeysStr              string // IndexKeysStr is the string representation of the index keys.
}

// Less returns whether the cost of c is less than the cost of other.
func (c IndexSetCost) Less(other IndexSetCost) bool {
	if c.TotalWorkloadQueryCost == 0 { // not initialized
		return false
	}
	if other.TotalWorkloadQueryCost == 0 { // not initialized
		return true
	}
	cc, cOther := c.TotalWorkloadQueryCost, other.TotalWorkloadQueryCost
	if math.Abs(cc-cOther) > 10 && math.Abs(cc-cOther)/math.Max(cc, cOther) > 0.001 {
		// their cost is very different, then the less cost, the better.
		return cc < cOther
	}

	if c.TotalNumberOfIndexColumns != other.TotalNumberOfIndexColumns {
		// if they have the same cost, then the less columns, the better.
		return c.TotalNumberOfIndexColumns < other.TotalNumberOfIndexColumns
	}

	// to make the result stable.
	return c.IndexKeysStr < other.IndexKeysStr
}
