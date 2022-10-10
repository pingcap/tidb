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

package executor

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
)

var (
	typePattern = regexp.MustCompile(`\(\d+\)`)
)

// ReloadSchema expose reloadSchema
func (e *Executor) ReloadSchema() error {
	return errors.Trace(e.reloadSchema())
}

func (e *Executor) reloadSchema() error {
	schema, err := e.conn.FetchSchema(e.db)
	if err != nil {
		return errors.Trace(err)
	}
	indexes := make(map[string][]types.CIStr)
	for _, col := range schema {
		if _, ok := indexes[col[2]]; ok {
			continue
		}
		index, err := e.conn.FetchIndexes(e.db, col[1])
		// may not return error here
		// just disable indexes
		if err != nil {
			return errors.Trace(err)
		}
		var modelIndex []types.CIStr
		for _, indexName := range index {
			modelIndex = append(modelIndex, types.CIStr(indexName))
		}
		indexes[col[1]] = modelIndex
	}

	e.loadSchema(schema, indexes)
	return nil
}

func (e *Executor) loadSchema(records [][6]string, indexes map[string][]types.CIStr) {
	// init databases
	e.tables = make(map[string]*types.Table)
LOOP:
	for _, record := range records {
		dbname := record[0]
		if dbname != e.db {
			continue
		}
		tableName := record[1]
		tableType := record[2]
		columnName := record[3]
		columnType := record[4]
		columnNull := record[5]
		var options []ast.ColumnOptionType
		if record[5] == "NO" {
			options = append(options, ast.ColumnOptionNotNull)
		}
		index, ok := indexes[tableName]
		if !ok {
			index = []types.CIStr{}
		}
		if _, ok := e.tables[tableName]; !ok {
			e.tables[tableName] = &types.Table{
				Name:    types.CIStr(tableName),
				Columns: []types.Column{},
				Indexes: index,
				Type:    tableType,
			}
		}

		for index, column := range e.tables[tableName].Columns {
			if column.Name.EqString(columnName) {
				e.tables[tableName].Columns[index].Type = columnType
				e.tables[tableName].Columns[index].Null = strings.EqualFold(columnNull, "Yes")
				continue LOOP
			}
		}
		col := types.Column{
			// columnName, columnType, columnNull
			Table: types.CIStr(tableName),
			Name:  types.CIStr(columnName),
			Null:  strings.EqualFold(columnNull, "Yes"),
		}
		col.ParseType(columnType)
		e.tables[tableName].Columns = append(e.tables[tableName].Columns, col)
	}
}
