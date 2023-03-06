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

package dbutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	_ "github.com/pingcap/tidb/planner/core" // to setup expression.EvalAstExpr. See: https://github.com/pingcap/tidb/blob/a94cff903cd1e7f3b050db782da84273ef5592f4/planner/core/optimizer.go#L202
	"github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/util/collate"
)

func init() {
	collate.SetNewCollationEnabledForTest(false)
}

// GetTableInfo returns table information.
func GetTableInfo(ctx context.Context, db QueryExecutor, schemaName string, tableName string) (*model.TableInfo, error) {
	createTableSQL, err := GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	parser2, err := GetParserForDB(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return GetTableInfoBySQL(createTableSQL, parser2)
}

// GetTableInfoBySQL returns table information by given create table sql.
func GetTableInfoBySQL(createTableSQL string, parser2 *parser.Parser) (table *model.TableInfo, err error) {
	stmt, err := parser2.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		table, err := ddl.BuildTableInfoFromAST(s)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// put primary key in indices
		if table.PKIsHandle {
			pkIndex := &model.IndexInfo{
				Name:    model.NewCIStr("PRIMARY"),
				Primary: true,
				State:   model.StatePublic,
				Unique:  true,
				Tp:      model.IndexTypeBtree,
				Columns: []*model.IndexColumn{
					{
						Name:   table.GetPkName(),
						Length: types.UnspecifiedLength,
					},
				},
			}

			table.Indices = append(table.Indices, pkIndex)
		}

		return table, nil
	}

	return nil, errors.Errorf("get table info from sql %s failed", createTableSQL)
}

// FindColumnByName finds column by name.
func FindColumnByName(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	// column name don't distinguish capital and small letter
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}

// EqualTableInfo returns true if this two table info have same columns and indices
func EqualTableInfo(tableInfo1, tableInfo2 *model.TableInfo) (bool, string) {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false, fmt.Sprintf("column num not equal, one is %d another is %d", len(tableInfo1.Columns), len(tableInfo2.Columns))
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false, fmt.Sprintf("column name not equal, one is %s another is %s", col.Name.O, tableInfo2.Columns[j].Name.O)
		}
		if col.GetType() != tableInfo2.Columns[j].GetType() {
			return false, fmt.Sprintf("column %s's type not equal, one is %v another is %v", col.Name.O, col.GetType(), tableInfo2.Columns[j].GetType())
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false, fmt.Sprintf("index num not equal, one is %d another is %d", len(tableInfo1.Indices), len(tableInfo2.Indices))
	}

	index2Map := make(map[string]*model.IndexInfo)
	for _, index := range tableInfo2.Indices {
		index2Map[index.Name.O] = index
	}

	for _, index1 := range tableInfo1.Indices {
		index2, ok := index2Map[index1.Name.O]
		if !ok {
			return false, fmt.Sprintf("index %s not exists", index1.Name.O)
		}

		if len(index1.Columns) != len(index2.Columns) {
			return false, fmt.Sprintf("index %s's columns num not equal, one is %d another is %d", index1.Name.O, len(index1.Columns), len(index2.Columns))
		}
		for j, col := range index1.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false, fmt.Sprintf("index %s's column not equal, one has %s another has %s", index1.Name.O, col.Name.O, index2.Columns[j].Name.O)
			}
		}
	}

	return true, ""
}
