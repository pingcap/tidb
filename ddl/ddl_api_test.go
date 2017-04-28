// Copyright 2017 PingCAP, Inc.
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

package ddl

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
)

func testFieldCase(c *C) {
	var colDefs []*ast.ColumnDef
	var fields = []string{"field", "Field"}
	for _, name := range fields {
		colDef := &ast.ColumnDef{
			Name: &ast.ColumnName{
				Schema: model.NewCIStr("TestSchema"),
				Table:  model.NewCIStr("TestTable"),
				Name:   model.NewCIStr(name),
			},
		}
		colDefs = append(colDefs, colDef)
	}
	c.Assert(checkDuplicateColumn(colDefs) != nil, Matches, true)
}
