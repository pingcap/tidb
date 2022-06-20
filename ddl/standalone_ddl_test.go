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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDMUseMiniDDL(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()

	d := NewMiniDDL(ctx)
	err := d.CreateSchema(sctx, model.NewCIStr("test2"), nil, nil)
	require.NoError(t, err)
	_, ok := d.(*miniDDL).GetInfoSchemaWithInterceptor(sctx).SchemaByName(model.NewCIStr("test2"))
	require.True(t, ok)

	p := parser.New()

	createTableAST, err := p.ParseOneStmt("create table test2.t1(c1 int, c2 int, index idx1(c1))", "", "")
	require.NoError(t, err)
	err = d.CreateTable(sctx, createTableAST.(*ast.CreateTableStmt))
	require.NoError(t, err)
	ok = d.(*miniDDL).GetInfoSchemaWithInterceptor(sctx).TableExists(model.NewCIStr("test2"), model.NewCIStr("t1"))
	require.True(t, ok)

	err = d.DropIndex(
		sctx,
		ast.Ident{
			Name:   model.NewCIStr("t1"),
			Schema: model.NewCIStr("test2"),
		},
		model.NewCIStr("idx1"),
		false,
	)
	require.NoError(t, err)
	tbl, err := d.(*miniDDL).GetInfoSchemaWithInterceptor(sctx).TableByName(model.NewCIStr("test2"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Len(t, tbl.Meta().Indices, 0)
}
