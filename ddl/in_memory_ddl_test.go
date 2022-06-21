// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInMemoryDDL(t *testing.T) {
	t.Parallel()
	sctx := mock.NewContext()
	p := parser.New()

	testDDL := NewInMemoryDDL(2)
	_, ok := testDDL.SchemaByName(model.NewCIStr("test"))
	require.False(t, ok)

	err := testDDL.CreateSchema(sctx, model.NewCIStr("test"), nil, nil)
	require.NoError(t, err)
	_, ok = testDDL.SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)

	sql := "CREATE TABLE test.t1 (c INT PRIMARY KEY)"
	createAST, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = testDDL.CreateTable(sctx, createAST.(*ast.CreateTableStmt))
	require.NoError(t, err)

	tbl, ok := testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.True(t, ok)
	require.Len(t, tbl.Columns, 1)
	require.Equal(t, "c", tbl.Columns[0].Name.O)

	err = testDDL.DropTable(sctx, ast.Ident{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t1")})
	require.NoError(t, err)
	_, ok = testDDL.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.False(t, ok)

	err = testDDL.DropSchema(sctx, model.NewCIStr("test"))
	require.NoError(t, err)
}
