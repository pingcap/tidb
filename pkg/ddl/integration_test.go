// Copyright 2021 PingCAP, Inc.
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

package ddl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestDDLStatementsBackFill(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	needReorg := false
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.SchemaState == model.StateWriteReorganization {
			needReorg = true
		}
	})
	tk.MustExec("create table t (a int, b char(65));")
	tk.MustExec("insert into t values (1, '123');")
	testCases := []struct {
		ddlSQL            string
		expectedNeedReorg bool
	}{
		{"alter table t modify column a bigint;", false},
		{"alter table t modify column b char(255);", false},
		{"alter table t modify column a varchar(100);", true},
		{"create table t1 (a int, b int);", false},
		{"alter table t1 add index idx_a(a);", true},
		{"alter table t1 add primary key(b) nonclustered;", true},
		{"alter table t1 drop primary key;", false},
	}
	for _, tc := range testCases {
		needReorg = false
		tk.MustExec(tc.ddlSQL)
		require.Equal(t, tc.expectedNeedReorg, needReorg, tc)
	}
}

func TestPartialIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// test validate column exists in create table
	tk.MustExec("create table t (a int, b int, key(b) where a = 1);")
	tk.MustGetDBError("create table t1 (a int, b int, key(b) where c = 1);",
		dbterror.ErrUnsupportedAddPartialIndex)
	tk.MustExec("drop table t;")

	// test primary key is not allowed in partial index
	tk.MustExec("create table t (a int, b int, key(b) where a = 1);")
	tk.MustGetDBError("create table t2 (a int, b int, primary key(b) where a = 1);",
		dbterror.ErrUnsupportedAddPartialIndex)
	tk.MustExec("drop table t;")

	checkColumnTypes := func(columnTypes []string, literals []string, shouldAllowed bool) {
		for _, columnType := range columnTypes {
			for _, literal := range literals {
				tk.MustExec("drop table if exists t;")
				sql := fmt.Sprintf("create table t (a %s, b int, key(b) where a = %s);", columnType, literal)
				if shouldAllowed {
					tk.MustExec(sql)
					tk.MustExec("drop table t;")
				} else {
					tk.MustGetDBError(sql, dbterror.ErrUnsupportedAddPartialIndex)
				}
			}
		}
	}

	// test create table type validation
	differentTypeLiterals := [][]string{
		{"1", "true", "1998"}, // int
		{"'1'"},               // string with default collate
		{"1.0"},               // float
		{"b'101010'", "0x1234567890abcdef", "0b10"}, // binary literal
		{"null"}, // null
	}
	differentColumnTypes := [][]string{
		{"int", "bigint", "tinyint", "smallint", "year"},
		{"char(25)", "varchar(123)", "text", "char(25) collate utf8mb4_general_ci", "char(25) collate utf8mb4_bin"},
		{"float", "double"},
		{"binary(25) collate binary", "varbinary(123)", "blob", "char(25) collate binary"},
		{},
	}
	for i, columnTypes := range differentColumnTypes {
		for j, literals := range differentTypeLiterals {
			checkColumnTypes(columnTypes, literals, i == j)
		}
	}

	// test comparing between time column and string constant is allowed.
	timeColumnTypes := []string{"timestamp", "datetime", "date", "time"}
	allowedLiterals := []string{"'2025-07-28 12:34:56'", "'2025-07-28'", "'12:34:56'"}
	notAllowedLiterals := []string{"1", "1.0", "true", "null"}
	checkColumnTypes(timeColumnTypes, allowedLiterals, true)
	checkColumnTypes(timeColumnTypes, notAllowedLiterals, false)

	// test comparing between enum/set column and int/string constant is allowed.
	enumSetColumnTypes := []string{"enum('a', 'b', 'c')", "set('a', 'b', 'c')"}
	allowedLiterals = []string{"1", "'1'", "'a'"}
	notAllowedLiterals = []string{"1.0", "null"}
	checkColumnTypes(enumSetColumnTypes, allowedLiterals, true)
	checkColumnTypes(enumSetColumnTypes, notAllowedLiterals, false)

	// test alter table type validation
	for i, literals := range differentTypeLiterals {
		for _, literal := range literals {
			for j, columnTypes := range differentColumnTypes {
				tk.MustExec("drop table if exists t;")
				for _, columnType := range columnTypes {
					sql := fmt.Sprintf("create table t (a %s, b int, key idx_b(b) where a = %s);", columnType, literal)
					if i == j {
						tk.MustExec(sql)
						tk.MustExec("drop table t;")
					} else {
						tk.MustGetDBError(sql, dbterror.ErrUnsupportedAddPartialIndex)
					}
				}
			}
		}
	}
}

func TestMaintainAffectColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create table t (col2 int, key(col2) where col2 > 0);")
	// Now, the offset of col2 is 0
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 0, tbl.Meta().Indices[0].AffectColumn[0].Offset)

	tk.MustExec("alter table t add column col1 int first;")
	// Now, the offset of col2 should be 1
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 1, tbl.Meta().Indices[0].AffectColumn[0].Offset)

	tk.MustExec("alter table t add column col3 int after col1;")
	// Now, the offset of col2 should be 2
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 2, tbl.Meta().Indices[0].AffectColumn[0].Offset)

	tk.MustExec("alter table t drop column col1;")
	// Now, the offset of col2 should be 1
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	require.Equal(t, 1, tbl.Meta().Indices[0].AffectColumn[0].Offset)
}
