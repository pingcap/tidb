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

// Package dbutiltest is a package for some common used methods for db related testing.
// we put it in a separate package to avoid cyclic import.
package dbutiltest

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/planner/core" // to setup expression.EvalAstExpr. See: https://github.com/pingcap/tidb/blob/a94cff903cd1e7f3b050db782da84273ef5592f4/planner/core/optimizer.go#L202
	"github.com/pingcap/tidb/pkg/types"
)

// GetTableInfoBySQL returns table information by given create table sql.
func GetTableInfoBySQL(createTableSQL string, parser2 *parser.Parser) (table *model.TableInfo, err error) {
	stmt, err := parser2.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		table, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), s)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// put primary key in indices
		if table.PKIsHandle {
			pkIndex := &model.IndexInfo{
				Name:    ast.NewCIStr("PRIMARY"),
				Primary: true,
				State:   model.StatePublic,
				Unique:  true,
				Tp:      ast.IndexTypeBtree,
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
