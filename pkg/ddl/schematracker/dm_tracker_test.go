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

package schematracker_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func execCreate(t *testing.T, tracker schematracker.SchemaTracker, sql string) {
	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = tracker.CreateTable(sctx, stmt.(*ast.CreateTableStmt))
	require.NoError(t, err)
}

func TestNoNumLimit(t *testing.T) {
	sql := "create table test.t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		sql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			sql += ","
		}
	}
	sql += ");"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

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
	execCreate(t, tracker, sql)

	sql = "alter table test.t_too_large add column alter_new_col int"
	execAlter(t, tracker, sql)
}

func TestCreateTableLongIndex(t *testing.T) {
	sql := "create table test.t (c1 int, c2 blob, c3 varchar(64), index idx_c2(c2(555555)));"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)
}

func execAlter(t *testing.T, tracker schematracker.SchemaTracker, sql string) {
	ctx := context.Background()
	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = tracker.AlterTable(ctx, sctx, stmt.(*ast.AlterTableStmt))
	require.NoError(t, err)
}

func mustTableByName(t *testing.T, tracker schematracker.SchemaTracker, schema, table string) *model.TableInfo {
	tblInfo, err := tracker.TableByName(model.NewCIStr(schema), model.NewCIStr(table))
	require.NoError(t, err)
	return tblInfo
}

func TestAlterPK(t *testing.T) {
	sql := "create table test.t (c1 int primary key, c2 blob);"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop primary key;"
	execAlter(t, tracker, sql)
	// TableInfo should be immutable.
	require.Equal(t, 1, len(tblInfo.Indices))
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 0, len(tblInfo.Indices))

	sql = "alter table test.t add primary key(c1);"
	execAlter(t, tracker, sql)
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop primary key;"
	execAlter(t, tracker, sql)
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 0, len(tblInfo.Indices))
}

func TestDropColumn(t *testing.T) {
	sql := "create table test.t(a int, b int auto_increment, c int, key(b))"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop column b"
	execAlter(t, tracker, sql)
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 0, len(tblInfo.Indices))

	sql = "alter table test.t add index idx_2_col(a, c)"
	execAlter(t, tracker, sql)
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 1, len(tblInfo.Indices))

	sql = "alter table test.t drop column c"
	execAlter(t, tracker, sql)
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, 1, len(tblInfo.Indices))
	require.Equal(t, 1, len(tblInfo.Columns))
}

func TestFullTextIndex(t *testing.T) {
	sql := "create table test.t (a text, fulltext key (a))"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)
}

func checkShowCreateTable(t *testing.T, tblInfo *model.TableInfo, expected string) {
	sctx := mock.NewContext()

	result := bytes.NewBuffer(make([]byte, 0, 512))
	err := executor.ConstructResultOfShowCreateTable(sctx, tblInfo, autoid.Allocators{}, result)
	require.NoError(t, err)
	require.Equal(t, expected, result.String())
}

func TestIndexLength(t *testing.T) {
	// copy TestIndexLength in db_integration_test.go
	sql := "create table test.t(a text, b text charset ascii, c blob, index(a(768)), index (b(3072)), index (c(3072)));"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")

	expected := "CREATE TABLE `t` (\n" +
		"  `a` text DEFAULT NULL,\n" +
		"  `b` text CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` blob DEFAULT NULL,\n" +
		"  KEY `a` (`a`(768)),\n" +
		"  KEY `b` (`b`(3072)),\n" +
		"  KEY `c` (`c`(3072))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)

	err := tracker.DeleteTable(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	sql = "create table test.t(a text, b text charset ascii, c blob);"
	execCreate(t, tracker, sql)

	sql = "alter table test.t add index (a(768))"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add index (b(3072))"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add index (c(3072))"
	execAlter(t, tracker, sql)

	tblInfo = mustTableByName(t, tracker, "test", "t")
	checkShowCreateTable(t, tblInfo, expected)
}

func TestIssue5092(t *testing.T) {
	// copy TestIssue5092 in db_integration_test.go
	sql := "create table test.t (a int)"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	sql = "alter table test.t add column (b int, c int)"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add column if not exists (b int, c int)"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add column b1 int after b, add column c1 int after c"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add column d int after b, add column e int first, add column f int after c1, add column g int, add column h int first"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add column if not exists (d int, e int), add column ff text"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add column b2 int after b1, add column c2 int first"
	execAlter(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")

	expected := "CREATE TABLE `t` (\n" +
		"  `c2` int(11) DEFAULT NULL,\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `b2` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL,\n" +
		"  `ff` text DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)
}

func TestBitDefaultValues(t *testing.T) {
	// copy TestBitDefaultValues in db_integration_test.go
	sql := `create table test.testalltypes2 (
    field_1 bit null default null,
    field_2 tinyint null default null,
    field_3 tinyint unsigned null default null,
    field_4 bigint null default null,
    field_5 bigint unsigned null default null,
    field_6 mediumblob null default null,
    field_7 longblob null default null,
    field_8 blob null default null,
    field_9 tinyblob null default null,
    field_10 varbinary(255) null default null,
    field_11 binary(255) null default null,
    field_12 mediumtext null default null,
    field_13 longtext null default null,
    field_14 text null default null,
    field_15 tinytext null default null,
    field_16 char(255) null default null,
    field_17 numeric null default null,
    field_18 decimal null default null,
    field_19 integer null default null,
    field_20 integer unsigned null default null,
    field_21 int null default null,
    field_22 int unsigned null default null,
    field_23 mediumint null default null,
    field_24 mediumint unsigned null default null,
    field_25 smallint null default null,
    field_26 smallint unsigned null default null,
    field_27 float null default null,
    field_28 double null default null,
    field_29 double precision null default null,
    field_30 real null default null,
    field_31 varchar(255) null default null,
    field_32 date null default null,
    field_33 time null default null,
    field_34 datetime null default null,
    field_35 timestamp null default null
	);`

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)
}

func TestAddExpressionIndex(t *testing.T) {
	sql := "create table test.t (a int, b real);"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	sql = "alter table test.t add index idx((a+b))"
	execAlter(t, tracker, sql)
	sql = "alter table test.t add index idx_multi((a+b),(a+1), b);"
	execAlter(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")

	expected := "CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` double DEFAULT NULL,\n" +
		"  KEY `idx` ((`a` + `b`)),\n" +
		"  KEY `idx_multi` ((`a` + `b`),(`a` + 1),`b`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)

	sql = "alter table test.t drop index idx;"
	execAlter(t, tracker, sql)
	sql = "alter table test.t drop index idx_multi;"
	execAlter(t, tracker, sql)

	tblInfo = mustTableByName(t, tracker, "test", "t")

	expected = "CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` double DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)

	sql = "create table test.t2 (a varchar(10), b varchar(10));"
	execCreate(t, tracker, sql)
	sql = "alter table test.t2 add unique index ei_ab ((concat(a, b)));"
	execAlter(t, tracker, sql)

	tblInfo = mustTableByName(t, tracker, "test", "t2")

	expected = "CREATE TABLE `t2` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) DEFAULT NULL,\n" +
		"  UNIQUE KEY `ei_ab` ((concat(`a`, `b`)))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)

	sql = "alter table test.t2 alter index ei_ab invisible;"
	execAlter(t, tracker, sql)

	tblInfo = mustTableByName(t, tracker, "test", "t2")

	expected = "CREATE TABLE `t2` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) DEFAULT NULL,\n" +
		"  UNIQUE KEY `ei_ab` ((concat(`a`, `b`))) /*!80000 INVISIBLE */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)

	sql = "create table test.t3(a int, key((a+1)), key((a+2)), key idx((a+3)), key((a+4)), UNIQUE KEY ((a * 2)));"
	execCreate(t, tracker, sql)

	tblInfo = mustTableByName(t, tracker, "test", "t3")

	expected = "CREATE TABLE `t3` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  KEY `expression_index` ((`a` + 1)),\n" +
		"  KEY `expression_index_2` ((`a` + 2)),\n" +
		"  KEY `idx` ((`a` + 3)),\n" +
		"  KEY `expression_index_3` ((`a` + 4)),\n" +
		"  UNIQUE KEY `expression_index_4` ((`a` * 2))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	checkShowCreateTable(t, tblInfo, expected)

	sql = `create table test.t4(
	a int,
	b varchar(100),
	c int)
	PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	);`
	execCreate(t, tracker, sql)

	sql = "alter table test.t4 add index idx((a+c));"
	execAlter(t, tracker, sql)

	tblInfo = mustTableByName(t, tracker, "test", "t4")

	expected = "CREATE TABLE `t4` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` varchar(100) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  KEY `idx` ((`a` + `c`))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
		"PARTITION BY RANGE (`a`)\n" +
		"(PARTITION `p0` VALUES LESS THAN (6),\n" +
		" PARTITION `p1` VALUES LESS THAN (11),\n" +
		" PARTITION `p2` VALUES LESS THAN (16),\n" +
		" PARTITION `p3` VALUES LESS THAN (21))"
	checkShowCreateTable(t, tblInfo, expected)
}

func TestAtomicMultiSchemaChange(t *testing.T) {
	sql := "create table test.t (a int);"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	sql = "alter table test.t add b int, add c int;"
	execAlter(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")
	require.Len(t, tblInfo.Columns, 3)

	sql = "alter table test.t add d int, add a int;"

	ctx := context.Background()
	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = tracker.AlterTable(ctx, sctx, stmt.(*ast.AlterTableStmt))
	require.True(t, infoschema.ErrColumnExists.Equal(err))

	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Len(t, tblInfo.Columns, 3)
}

func TestImmutableTableInfo(t *testing.T) {
	sql := "create table test.t (a varchar(20)) charset latin1;"

	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	tblInfo := mustTableByName(t, tracker, "test", "t")
	require.Equal(t, "", tblInfo.Comment)

	sql = "alter table test.t comment = '123';"
	execAlter(t, tracker, sql)
	require.Equal(t, "", tblInfo.Comment)

	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, "123", tblInfo.Comment)

	require.Equal(t, "latin1", tblInfo.Charset)
	require.Equal(t, "latin1_bin", tblInfo.Collate)
	require.Equal(t, "latin1", tblInfo.Columns[0].GetCharset())
	require.Equal(t, "latin1_bin", tblInfo.Columns[0].GetCollate())

	sql = "alter table test.t convert to character set utf8mb4 collate utf8mb4_general_ci;"
	execAlter(t, tracker, sql)
	require.Equal(t, "latin1", tblInfo.Charset)
	require.Equal(t, "latin1_bin", tblInfo.Collate)
	require.Equal(t, "latin1", tblInfo.Columns[0].GetCharset())
	require.Equal(t, "latin1_bin", tblInfo.Columns[0].GetCollate())
	tblInfo = mustTableByName(t, tracker, "test", "t")
	require.Equal(t, "utf8mb4", tblInfo.Charset)
	require.Equal(t, "utf8mb4_general_ci", tblInfo.Collate)
	require.Equal(t, "utf8mb4", tblInfo.Columns[0].GetCharset())
	require.Equal(t, "utf8mb4_general_ci", tblInfo.Columns[0].GetCollate())
}

var _ sqlexec.RestrictedSQLExecutor = (*mockRestrictedSQLExecutor)(nil)

type mockRestrictedSQLExecutor struct {
	sessionctx.Context
}

func (m mockRestrictedSQLExecutor) ParseWithParams(ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	return nil, nil
}

func (m mockRestrictedSQLExecutor) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, nil
}

func (m mockRestrictedSQLExecutor) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, nil
}

func (m mockRestrictedSQLExecutor) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return m
}

func TestModifyFromNullToNotNull(t *testing.T) {
	sql := "create table test.t (a int, b int);"
	tracker := schematracker.NewSchemaTracker(2)
	tracker.CreateTestDB(nil)
	execCreate(t, tracker, sql)

	sql = "alter table test.t modify column a int not null;"
	ctx := context.Background()
	sctx := mock.NewContext()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	// converting from NULL to NOT NULL needs to check data, so caller should provide a RestrictedSQLExecutor
	executorCtx := mockRestrictedSQLExecutor{sctx}
	err = tracker.AlterTable(ctx, executorCtx, stmt.(*ast.AlterTableStmt))
	require.NoError(t, err)

	tblInfo := mustTableByName(t, tracker, "test", "t")
	require.Len(t, tblInfo.Columns, 2)
}
