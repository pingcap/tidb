// Copyright 2019 PingCAP, Inc.
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

package tiniub

import (
	"sync"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
)

var once sync.Once

func Init() {
	initOnce := func() {
		p := parser.New()
		stmt, err := p.ParseOneStmt(tableSlowQuery, "", "")
		if err != nil {
			panic(err)
		}
		meta, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		if err != nil {
			panic(err)
		}
		meta.ID = autoid.GenLocalSchemaID()
		for _, c := range meta.Columns {
			c.ID = autoid.GenLocalSchemaID()
		}
		dbInfo := &model.DBInfo{
			ID:      autoid.GenLocalSchemaID(),
			Name:    model.NewCIStr("TiNiuB"),
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Tables:  []*model.TableInfo{meta},
		}
		infoschema.RegisterVirtualTable(dbInfo, tableFromMeta)
	}
	if expression.EvalAstExpr != nil {
		once.Do(initOnce)
	}
}
