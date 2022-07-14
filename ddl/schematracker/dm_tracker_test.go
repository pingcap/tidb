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

package schematracker

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCreateTableNoNumLimit(t *testing.T) {
	sql := "create table test.t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		sql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			sql += ","
		}
	}
	sql += ");"

	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	tracker := NewSchemaTracker(2)
	tracker.createTestDB()
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	sql = "create table test.t_too_many_indexes ("
	for i := 0; i < 100; i++ {
		if i != 0 {
			sql += ","
		}
		sql += fmt.Sprintf("c%d int", i)
	}
	for i := 0; i < 100; i++ {
		sql += ","
		sql += fmt.Sprintf("key k%d(c%d)", i, i)
	}
	sql += ");"
	stmt, err = p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
}

func TestCreateTableLongIndex(t *testing.T) {
	sql := "create table test.t (c1 int, c2 blob, c3 varchar(64), index idx_c2(c2(555555)));"

	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	tracker := NewSchemaTracker(2)
	tracker.createTestDB()
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
}

func execAlter(t *testing.T, tracker SchemaTracker, sql string) {
	ctx := context.Background()
	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = tracker.AlterTable(ctx, sctx, stmt.(*ast.AlterTableStmt))
	require.NoError(t, err)
}

func TestAlterPK(t *testing.T) {
	sql := "create table test.t (c1 int primary key, c2 blob);"

	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	tracker := NewSchemaTracker(2)
	tracker.createTestDB()
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	tblInfo, err := tracker.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop primary key;"
	execAlter(t, tracker, sql)
	require.Equal(t, 0, len(tblInfo.Indices))

	sql = "alter table test.t add primary key(c1);"
	execAlter(t, tracker, sql)
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop primary key;"
	execAlter(t, tracker, sql)
	require.Equal(t, 0, len(tblInfo.Indices))
}

func TestDropColumn(t *testing.T) {
	sql := "create table test.t(a int, b int auto_increment, c int, key(b))"

	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	tracker := NewSchemaTracker(2)
	tracker.createTestDB()
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	tblInfo, err := tracker.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop column b"
	execAlter(t, tracker, sql)
	require.Equal(t, 0, len(tblInfo.Indices))

	sql = "alter table test.t add index idx_2_col(a, c)"
	execAlter(t, tracker, sql)
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop column c"
	execAlter(t, tracker, sql)
	require.Equal(t, 1, len(tblInfo.Indices))
	require.Equal(t, 1, len(tblInfo.Columns))
}

func TestTempTest(t *testing.T) {
	sql := "create table test.mc(a int key, b int, c int unique)"

	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	tracker := NewSchemaTracker(2)
	tracker.createTestDB()
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)

	_, err = tracker.TableByName(model.NewCIStr("test"), model.NewCIStr("mc"))
	require.NoError(t, err)

	sql = "alter table test.mc modify column a bigint"
	execAlter(t, tracker, sql)
}
