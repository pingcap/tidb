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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perfschema

import (
	"fmt"
	"sync"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util"
)

var once sync.Once

// Init register the PERFORMANCE_SCHEMA virtual tables.
// It should be init(), and the ideal usage should be:
//
// import _ "github.com/pingcap/tidb/perfschema"
//
// This function depends on plan/core.init(), which initialize the expression.EvalAstExpr function.
// The initialize order is a problem if init() is used as the function name.
func Init() {
	initOnce := func() {
		p := parser.New()
		tbls := make([]*model.TableInfo, 0)
		dbID := autoid.PerformanceSchemaDBID
		for _, sql := range perfSchemaTables {
			stmt, err := p.ParseOneStmt(sql, "", "")
			if err != nil {
				panic(err)
			}
			meta, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
			if err != nil {
				panic(err)
			}
			tbls = append(tbls, meta)
			var ok bool
			meta.ID, ok = tableIDMap[meta.Name.O]
			if !ok {
				panic(fmt.Sprintf("get performance_schema table id failed, unknown system table `%v`", meta.Name.O))
			}
			for i, c := range meta.Columns {
				c.ID = int64(i) + 1
			}
		}
		dbInfo := &model.DBInfo{
			ID:      dbID,
			Name:    util.PerformanceSchemaName,
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Tables:  tbls,
		}
		infoschema.RegisterVirtualTable(dbInfo, tableFromMeta)
	}
	if expression.EvalAstExpr != nil {
		once.Do(initOnce)
	}
}
