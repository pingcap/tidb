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

package ddl

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	ddltestutil "github.com/pingcap/tidb/pkg/ddl/testutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

// TestInTxnExecDDLFail tests the following case:
//  1. Execute the SQL of "begin";
//  2. A SQL that will fail to execute;
//  3. Execute DDL.
func TestInTxnExecDDLFail(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1);")
	tk.MustGetErrMsg("truncate table t;", "[kv:1062]Duplicate entry '1' for key 't.PRIMARY'")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))
}

func TestCreateTable(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Test create an exist database
	tk.MustExecToErr("CREATE database test")

	// Test create an exist table
	tk.MustExec("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	tk.MustExecToErr("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// Test "if not exist"
	tk.MustExec("CREATE TABLE if not exists test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// Testcase for https://github.com/pingcap/tidb/issues/312
	tk.MustExec(`create table issue312_1 (c float(24));`)
	tk.MustExec(`create table issue312_2 (c float(25));`)
	rs, err := tk.Exec(`desc issue312_1`)
	require.NoError(t, err)
	ctx := context.Background()
	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		require.NoError(t, err1)
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			require.Equal(t, "float", row.GetString(1))
		}
	}
	rs, err = tk.Exec(`desc issue312_2`)
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	it = chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		require.NoError(t, err1)
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			require.Equal(t, "double", req.GetRow(0).GetString(1))
		}
	}
	require.NoError(t, rs.Close())

	// test multiple collate specified in column when create.
	tk.MustExec("drop table if exists test_multiple_column_collate;")
	tk.MustExec("create table test_multiple_column_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	tt, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("test_multiple_column_collate"))
	require.NoError(t, err)
	require.Equal(t, "utf8", tt.Cols()[0].GetCharset())
	require.Equal(t, "utf8_general_ci", tt.Cols()[0].GetCollate())
	require.Equal(t, "utf8mb4", tt.Meta().Charset)
	require.Equal(t, "utf8mb4_bin", tt.Meta().Collate)

	tk.MustExec("drop table if exists test_multiple_column_collate;")
	tk.MustExec("create table test_multiple_column_collate (a char(1) charset utf8 collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	tt, err = domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("test_multiple_column_collate"))
	require.NoError(t, err)
	require.Equal(t, "utf8", tt.Cols()[0].GetCharset())
	require.Equal(t, "utf8_general_ci", tt.Cols()[0].GetCollate())
	require.Equal(t, "utf8mb4", tt.Meta().Charset)
	require.Equal(t, "utf8mb4_bin", tt.Meta().Collate)

	// test Err case for multiple collate specified in column when create.
	tk.MustExec("drop table if exists test_err_multiple_collate;")
	tk.MustGetErrMsg("create table test_err_multiple_collate (a char(1) charset utf8mb4 collate utf8_unicode_ci collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin",
		dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8_unicode_ci", "utf8mb4").Error())

	tk.MustExec("drop table if exists test_err_multiple_collate;")
	tk.MustGetErrMsg("create table test_err_multiple_collate (a char(1) collate utf8_unicode_ci collate utf8mb4_general_ci) charset utf8mb4 collate utf8mb4_bin",
		dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8mb4_general_ci", "utf8").Error())

	// table option is auto-increment
	tk.MustExec("drop table if exists create_auto_increment_test;")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r := tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Rows("999 aa", "1000 bb", "1001 cc"))
	tk.MustExec("drop table create_auto_increment_test")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 1999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Rows("1999 aa", "2000 bb", "2001 cc"))
	tk.MustExec("drop table create_auto_increment_test")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), key(id)) auto_increment = 1000;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Rows("1000 aa"))

	// Test for `drop table if exists`.
	tk.MustExec("drop table if exists t_if_exists;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1051 Unknown table 'test.t_if_exists'"))
	tk.MustExec("create table if not exists t1_if_exists(c int)")
	tk.MustExec("drop table if exists t1_if_exists,t2_if_exists,t3_if_exists")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Note|1051|Unknown table 'test.t2_if_exists'", "Note|1051|Unknown table 'test.t3_if_exists'"))
}

func TestCreateDropDatabase(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithDDLChecker())

	ddlChecker := dom.DDL().(*schematracker.Checker)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists drop_test;")
	tk.MustExec("drop database if exists drop_test;")
	tk.MustExec("create database drop_test;")
	tk.MustExec("use drop_test;")
	tk.MustExec("drop database drop_test;")
	tk.MustGetDBError("drop table t;", plannererrors.ErrNoDB)
	tk.MustGetDBError("select * from t;", plannererrors.ErrNoDB)

	tk.MustExecToErr("drop database mysql")

	tk.MustExec("create database charset_test charset ascii;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET ascii */",
	))
	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test charset binary;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET binary */",
	))
	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test collate utf8_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */",
	))
	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test charset utf8 collate utf8_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */",
	))
	tk.MustGetErrMsg("create database charset_test charset utf8 collate utf8mb4_unicode_ci;", "[ddl:1253]COLLATION 'utf8mb4_unicode_ci' is not valid for CHARACTER SET 'utf8'")

	// ddl.SchemaTracker will not respect session charset
	ddlChecker.Disable()

	tk.MustExec("SET SESSION character_set_server='ascii'")
	tk.MustExec("SET SESSION collation_server='ascii_bin'")

	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET ascii */",
	))

	ddlChecker.Enable()

	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test collate utf8mb4_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */",
	))

	tk.MustExec("drop database charset_test;")
	tk.MustExec("create database charset_test charset utf8mb4;")
	tk.MustQuery("show create database charset_test;").Check(testkit.RowsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
}

func TestAlterTableAddColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists alter_test (c1 int)")
	tk.MustExec("insert into alter_test values(1)")
	tk.MustExec("alter table alter_test add column c2 timestamp default current_timestamp")
	time.Sleep(1 * time.Millisecond)
	now := time.Now().Add(-1 * time.Millisecond).Format(types.TimeFormat)
	r, err := tk.Exec("select c2 from alter_test")
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(context.Background(), req)
	require.NoError(t, err)
	row := req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.GreaterOrEqual(t, now, row.GetTime(0).String())
	require.Nil(t, r.Close())
	tk.MustExec("alter table alter_test add column c3 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustQuery("select c3 from alter_test").Check(testkit.Rows("CURRENT_TIMESTAMP"))
	tk.MustExec("create or replace view alter_view as select c1,c2 from alter_test")
	err = tk.ExecToErr("alter table alter_view add column c4 varchar(50)")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error(), err.Error())
	tk.MustExec("drop view alter_view")
	tk.MustExec("create sequence alter_seq")
	err = tk.ExecToErr("alter table alter_seq add column c int")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error(), err.Error())
	tk.MustExec("alter table alter_test add column c4 date default current_date")
	now = time.Now().Format(types.DateFormat)
	r, err = tk.Exec("select c4 from alter_test")
	require.NoError(t, err)
	req = r.NewChunk(nil)
	err = r.Next(context.Background(), req)
	require.NoError(t, err)
	row = req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.GreaterOrEqual(t, now, row.GetTime(0).String())
	require.Nil(t, r.Close())
	tk.MustExec("drop sequence alter_seq")
}

func TestAlterTableAddColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists alter_test (c1 int)")
	tk.MustExec("insert into alter_test values(1)")
	tk.MustExec("alter table alter_test add column c2 timestamp default current_timestamp, add column c8 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustExec("alter table alter_test add column (c7 timestamp default current_timestamp, c3 varchar(50) default 'CURRENT_TIMESTAMP')")
	r, err := tk.Exec("select c2 from alter_test")
	require.NoError(t, err)
	req := r.NewChunk(nil)
	err = r.Next(context.Background(), req)
	require.NoError(t, err)
	row := req.GetRow(0)
	require.Equal(t, 1, row.Len())
	require.Nil(t, r.Close())
	tk.MustQuery("select c3 from alter_test").Check(testkit.Rows("CURRENT_TIMESTAMP"))
	tk.MustExec("create or replace view alter_view as select c1,c2 from alter_test")
	err = tk.ExecToErr("alter table alter_view add column (c4 varchar(50), c5 varchar(50))")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error(), err.Error())
	tk.MustExec("drop view alter_view")
	tk.MustExec("create sequence alter_seq")
	err = tk.ExecToErr("alter table alter_seq add column (c1 int, c2 varchar(10))")
	require.Equal(t, dbterror.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error(), err.Error())
	tk.MustExec("drop sequence alter_seq")
}

func TestAddNotNullColumnNoDefault(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table nn (c1 int)")
	tk.MustExec("insert nn values (1), (2)")
	tk.MustExec("alter table nn add column c2 int not null")

	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("nn"))
	require.NoError(t, err)
	col2 := tbl.Meta().Columns[1]
	require.Nil(t, col2.DefaultValue)
	require.Equal(t, "0", col2.OriginDefaultValue)

	tk.MustQuery("select * from nn").Check(testkit.Rows("1 0", "2 0"))
	tk.MustExecToErr("insert nn (c1) values (3)")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert nn (c1) values (3)")
	tk.MustQuery("select * from nn").Check(testkit.Rows("1 0", "2 0", "3 0"))
}

func TestAlterTableModifyColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(c1 int, c2 varchar(10), c3 bit)")
	tk.MustExecToErr("alter table mc modify column c1 short")
	tk.MustExec("alter table mc modify column c1 bigint")

	tk.MustExecToErr("alter table mc modify column c2 blob")
	tk.MustExec("alter table mc modify column c2 varchar(8)")
	tk.MustExec("alter table mc modify column c2 varchar(11)")
	tk.MustExec("alter table mc modify column c2 text(13)")
	tk.MustExec("alter table mc modify column c2 text")
	tk.MustExec("alter table mc modify column c3 bit")
	result := tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `c1` bigint(20) DEFAULT NULL,\n  `c2` text DEFAULT NULL,\n  `c3` bit(1) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	require.Equal(t, expected, createSQL)
	tk.MustExec("create or replace view alter_view as select c1,c2 from mc")
	tk.MustGetErrMsg("alter table alter_view modify column c2 text",
		dbterror.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustExec("drop view alter_view")
	tk.MustExec("create sequence alter_seq")
	tk.MustGetErrMsg("alter table alter_seq modify column c int",
		dbterror.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustExec("drop sequence alter_seq")

	// test multiple collate modification in column.
	tk.MustExec("drop table if exists modify_column_multiple_collate")
	tk.MustExec("create table modify_column_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	tk.MustExec("alter table modify_column_multiple_collate modify column a char(1) collate utf8mb4_bin;")
	tt, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("modify_column_multiple_collate"))
	require.NoError(t, err)
	require.Equal(t, "utf8mb4", tt.Cols()[0].GetCharset())
	require.Equal(t, "utf8mb4_bin", tt.Cols()[0].GetCollate())
	require.Equal(t, "utf8mb4", tt.Meta().Charset)
	require.Equal(t, "utf8mb4_bin", tt.Meta().Collate)

	tk.MustExec("drop table if exists modify_column_multiple_collate;")
	tk.MustExec("create table modify_column_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	tk.MustExec("alter table modify_column_multiple_collate modify column a char(1) charset utf8mb4 collate utf8mb4_bin;")
	tt, err = domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("modify_column_multiple_collate"))
	require.NoError(t, err)
	require.Equal(t, "utf8mb4", tt.Cols()[0].GetCharset())
	require.Equal(t, "utf8mb4_bin", tt.Cols()[0].GetCollate())
	require.Equal(t, "utf8mb4", tt.Meta().Charset)
	require.Equal(t, "utf8mb4_bin", tt.Meta().Collate)

	// test Err case for multiple collate modification in column.
	tk.MustExec("drop table if exists err_modify_multiple_collate;")
	tk.MustExec("create table err_modify_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	tk.MustGetErrMsg("alter table err_modify_multiple_collate modify column a char(1) charset utf8mb4 collate utf8_bin;", dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8_bin", "utf8mb4").Error())

	tk.MustExec("drop table if exists err_modify_multiple_collate;")
	tk.MustExec("create table err_modify_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	tk.MustGetErrMsg("alter table err_modify_multiple_collate modify column a char(1) collate utf8_bin collate utf8mb4_bin;", dbterror.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8mb4_bin", "utf8").Error())
}

func TestColumnCharsetAndCollate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "col_charset_collate"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tests := []struct {
		colType     string
		charset     string
		collates    string
		exptCharset string
		exptCollate string
		errMsg      string
	}{
		{
			colType:     "varchar(10)",
			charset:     "charset utf8",
			collates:    "collate utf8_bin",
			exptCharset: "utf8",
			exptCollate: "utf8_bin",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset utf8mb4",
			collates:    "",
			exptCharset: "utf8mb4",
			exptCollate: "utf8mb4_bin",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset utf16",
			collates:    "",
			exptCharset: "",
			exptCollate: "",
			errMsg:      "Unknown charset utf16",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset latin1",
			collates:    "",
			exptCharset: "latin1",
			exptCollate: "latin1_bin",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset binary",
			collates:    "",
			exptCharset: "binary",
			exptCollate: "binary",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset ascii",
			collates:    "",
			exptCharset: "ascii",
			exptCollate: "ascii_bin",
			errMsg:      "",
		},
	}
	sctx := tk.Session()
	dm := domain.GetDomain(sctx)
	for i, tt := range tests {
		tblName := fmt.Sprintf("t%d", i)
		sql := fmt.Sprintf("create table %s (a %s %s %s)", tblName, tt.colType, tt.charset, tt.collates)
		if tt.errMsg == "" {
			tk.MustExec(sql)
			is := dm.InfoSchema()
			require.NotNil(t, is)

			tb, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
			require.NoError(t, err)
			require.Equalf(t, tt.exptCharset, tb.Meta().Columns[0].GetCharset(), sql)
			require.Equalf(t, tt.exptCollate, tb.Meta().Columns[0].GetCollate(), sql)
		} else {
			err := tk.ExecToErr(sql)
			require.Errorf(t, err, sql)
		}
	}
	tk.MustExec("drop database " + dbName)
}

func TestShardRowIDBits(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?)", i)
	}

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	assertCountAndShard := func(tt table.Table, expectCount int) {
		var hasShardedID bool
		var count int
		require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
		err = tables.IterRecords(tt, tk.Session(), nil, func(h kv.Handle, rec []types.Datum, cols []*table.Column) (more bool, err error) {
			require.GreaterOrEqual(t, h.IntValue(), int64(0))
			first8bits := h.IntValue() >> 56
			if first8bits > 0 {
				hasShardedID = true
			}
			count++
			return true, nil
		})
		require.NoError(t, err)
		require.Equal(t, expectCount, count)
		require.True(t, hasShardedID)
	}

	assertCountAndShard(tbl, 100)

	// After PR 10759, shard_row_id_bits is supported with tables with auto_increment column.
	tk.MustExec("create table auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustExec("alter table auto shard_row_id_bits = 5")
	tk.MustExec("drop table auto")
	tk.MustExec("create table auto (id int not null auto_increment unique) shard_row_id_bits = 0")
	tk.MustExec("alter table auto shard_row_id_bits = 5")
	tk.MustExec("drop table auto")
	tk.MustExec("create table auto (id int not null auto_increment unique)")
	tk.MustExec("alter table auto shard_row_id_bits = 5")
	tk.MustExec("drop table auto")
	tk.MustExec("create table auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustExec("alter table auto shard_row_id_bits = 0")
	tk.MustExec("drop table auto")

	errMsg := "[ddl:8200]Unsupported shard_row_id_bits for table with primary key as row id"
	tk.MustGetErrMsg("create table auto (id varchar(255) primary key clustered, b int) shard_row_id_bits = 4;", errMsg)
	tk.MustExec("create table auto (id varchar(255) primary key clustered, b int) shard_row_id_bits = 0;")
	tk.MustGetErrMsg("alter table auto shard_row_id_bits = 5;", errMsg)
	tk.MustExec("alter table auto shard_row_id_bits = 0;")
	tk.MustExec("drop table if exists auto;")

	// After PR 10759, shard_row_id_bits is not supported with pk_is_handle tables.
	tk.MustGetErrMsg("create table auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 4", errMsg)
	tk.MustExec("create table auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 0")
	tk.MustGetErrMsg("alter table auto shard_row_id_bits = 5", errMsg)
	tk.MustExec("alter table auto shard_row_id_bits = 0")

	// Hack an existing table with shard_row_id_bits and primary key as handle
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("auto"))
	tblInfo := tbl.Meta()
	tblInfo.ShardRowIDBits = 5
	tblInfo.MaxShardRowIDBits = 5

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		require.NoError(t, err)
		require.Nil(t, m.UpdateTable(db.ID, tblInfo))
		return nil
	})
	require.NoError(t, err)
	err = dom.Reload()
	require.NoError(t, err)

	tk.MustExec("insert auto(b) values (1), (3), (5)")
	tk.MustQuery("select id from auto order by id").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("alter table auto shard_row_id_bits = 0")
	tk.MustExec("drop table auto")

	// Test shard_row_id_bits with auto_increment column
	tk.MustExec("create table auto (a int, b int auto_increment unique) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into auto(a) values (?)", i)
	}
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("auto"))
	assertCountAndShard(tbl, 100)
	prevB, err := strconv.Atoi(tk.MustQuery("select b from auto where a=0").Rows()[0][0].(string))
	require.NoError(t, err)
	for i := 1; i < 100; i++ {
		b, err := strconv.Atoi(tk.MustQuery(fmt.Sprintf("select b from auto where a=%d", i)).Rows()[0][0].(string))
		require.NoError(t, err)
		require.Greater(t, b, prevB)
		prevB = b
	}

	// Test overflow
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 15")
	defer tk.MustExec("drop table if exists t1")

	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	maxID := 1<<(64-15-1) - 1
	alloc := tbl.Allocators(tk.Session().GetTableCtx()).Get(autoid.RowIDAllocType)
	err = alloc.Rebase(context.Background(), int64(maxID)-1, false)
	require.NoError(t, err)
	tk.MustExec("insert into t1 values(1)")

	// continue inserting will fail.
	tk.MustGetDBError("insert into t1 values(2)", autoid.ErrAutoincReadFailed)
	tk.MustGetDBError("insert into t1 values(3)", autoid.ErrAutoincReadFailed)
}

func TestAutoRandomBitsData(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database if not exists test_auto_random_bits")
	defer tk.MustExec("drop database if exists test_auto_random_bits")
	tk.MustExec("use test_auto_random_bits")
	tk.MustExec("drop table if exists t")

	extractAllHandles := func() []int64 {
		allHds, err := ddltestutil.ExtractAllTableHandles(tk.Session(), "test_auto_random_bits", "t")
		require.NoError(t, err)
		return allHds
	}

	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	tk.MustExec("create table t (a bigint primary key clustered auto_random(15), b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t(b) values (?)", i)
	}
	allHandles := extractAllHandles()
	tk.MustExec("drop table t")

	// Test auto random id number.
	require.Equal(t, 100, len(allHandles))
	// Test the handles are not all zero.
	allZero := true
	for _, h := range allHandles {
		allZero = allZero && (h>>(64-16)) == 0
	}
	require.False(t, allZero)
	// Test non-shard-bits part of auto random id is monotonic increasing and continuous.
	orderedHandles := testutil.MaskSortHandles(allHandles, 15, mysql.TypeLonglong)
	size := int64(len(allHandles))
	for i := int64(1); i <= size; i++ {
		require.Equal(t, orderedHandles[i-1], i)
	}

	// Test explicit insert.
	autoRandBitsUpperBound := 2<<47 - 1
	tk.MustExec("create table t (a bigint primary key clustered auto_random(15), b int)")
	for i := -10; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i+autoRandBitsUpperBound, i))
	}
	tk.MustGetErrMsg("insert into t (b) values (0)", autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustExec("drop table t")

	// Test overflow.
	tk.MustExec("create table t (a bigint primary key auto_random(15), b int)")
	// Here we cannot fill the all values for a `bigint` column,
	// so firstly we rebase auto_rand to the position before overflow.
	tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", autoRandBitsUpperBound, 1))
	tk.MustGetErrMsg("insert into t (b) values (0)", autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustExec("drop table t")

	tk.MustExec("create table t (a bigint primary key auto_random(15), b int)")
	tk.MustExec("insert into t values (1, 2)")
	tk.MustExec(fmt.Sprintf("update t set a = %d where a = 1", autoRandBitsUpperBound))
	tk.MustGetErrMsg("insert into t (b) values (0)", autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustExec("drop table t")

	// Test insert negative integers explicitly won't trigger rebase.
	tk.MustExec("create table t (a bigint primary key auto_random(15), b int)")
	for i := 1; i <= 100; i++ {
		tk.MustExec("insert into t(b) values (?)", i)
		tk.MustExec("insert into t(a, b) values (?, ?)", -i, i)
	}
	// orderedHandles should be [-100, -99, ..., -2, -1, 1, 2, ..., 99, 100]
	orderedHandles = testutil.MaskSortHandles(extractAllHandles(), 15, mysql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < 100; i++ {
		require.Equal(t, i-100, orderedHandles[i])
	}
	for i := int64(100); i < size; i++ {
		require.Equal(t, i-99, orderedHandles[i])
	}
	tk.MustExec("drop table t")

	// Test signed/unsigned types.
	tk.MustExec("create table t (a bigint primary key auto_random(10), b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t (b) values(?)", i)
	}
	for _, h := range extractAllHandles() {
		// Sign bit should be reserved.
		require.True(t, h > 0)
	}
	tk.MustExec("drop table t")

	tk.MustExec("create table t (a bigint unsigned primary key auto_random(10), b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t (b) values(?)", i)
	}
	signBitUnused := true
	for _, h := range extractAllHandles() {
		signBitUnused = signBitUnused && (h > 0)
	}
	// Sign bit should be used for shard.
	require.False(t, signBitUnused)
	tk.MustExec("drop table t;")

	// Test rename table does not affect incremental part of auto_random ID.
	tk.MustExec("create database test_auto_random_bits_rename;")
	tk.MustExec("create table t (a bigint auto_random primary key);")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values ();")
	}
	tk.MustExec("alter table t rename to test_auto_random_bits_rename.t1;")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into test_auto_random_bits_rename.t1 values ();")
	}
	tk.MustExec("alter table test_auto_random_bits_rename.t1 rename to t;")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values ();")
	}
	uniqueHandles := make(map[int64]struct{})
	for _, h := range extractAllHandles() {
		uniqueHandles[h&((1<<(63-5))-1)] = struct{}{}
	}
	require.Equal(t, 30, len(uniqueHandles))
	tk.MustExec("drop database test_auto_random_bits_rename;")
	tk.MustExec("drop table t;")
}

func TestAutoRandomTableOption(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// test table option is auto-random
	tk.MustExec("drop table if exists auto_random_table_option")
	tk.MustExec("create table auto_random_table_option (a bigint auto_random(5) key) auto_random_base = 1000")
	tt, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("auto_random_table_option"))
	require.NoError(t, err)
	require.Equal(t, int64(1000), tt.Meta().AutoRandID)
	tk.MustExec("insert into auto_random_table_option values (),(),(),(),()")
	allHandles, err := ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "auto_random_table_option")
	require.NoError(t, err)
	require.Equal(t, 5, len(allHandles))
	// Test non-shard-bits part of auto random id is monotonic increasing and continuous.
	orderedHandles := testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
	size := int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		require.Equal(t, orderedHandles[i], i+1000)
	}

	tk.MustExec("drop table if exists alter_table_auto_random_option")
	tk.MustExec("create table alter_table_auto_random_option (a bigint primary key auto_random(4), b int)")
	tt, err = domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("alter_table_auto_random_option"))
	require.NoError(t, err)
	require.Equal(t, int64(0), tt.Meta().AutoRandID)
	tk.MustExec("insert into alter_table_auto_random_option values(),(),(),(),()")
	allHandles, err = ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "alter_table_auto_random_option")
	require.NoError(t, err)
	orderedHandles = testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		require.Equal(t, i+1, orderedHandles[i])
	}
	tk.MustExec("delete from alter_table_auto_random_option")

	// alter table to change the auto_random option (it will dismiss the local allocator cache)
	// To avoid the new base is in the range of local cache, which will leading the next
	// value is not what we rebased, because the local cache is dropped, here we choose
	// a quite big value to do this.
	tk.MustExec("alter table alter_table_auto_random_option auto_random_base = 3000000")
	tt, err = domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("alter_table_auto_random_option"))
	require.NoError(t, err)
	require.Equal(t, int64(3000000), tt.Meta().AutoRandID)
	tk.MustExec("insert into alter_table_auto_random_option values(),(),(),(),()")
	allHandles, err = ddltestutil.ExtractAllTableHandles(tk.Session(), "test", "alter_table_auto_random_option")
	require.NoError(t, err)
	orderedHandles = testutil.MaskSortHandles(allHandles, 5, mysql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		require.Equal(t, i+3000000, orderedHandles[i])
	}
	tk.MustExec("drop table alter_table_auto_random_option")

	// Alter auto_random_base on non auto_random table.
	tk.MustExec("create table alter_auto_random_normal (a int)")
	err = tk.ExecToErr("alter table alter_auto_random_normal auto_random_base = 100")
	require.Error(t, err)
	require.Contains(t, err.Error(), autoid.AutoRandomRebaseNotApplicable)
}

func TestSetDDLReorgWorkerCnt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, int32(variable.DefTiDBDDLReorgWorkerCount), variable.GetDDLReorgWorkerCounter())
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, int32(1), variable.GetDDLReorgWorkerCounter())
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, int32(100), variable.GetDDLReorgWorkerCounter())
	tk.MustGetDBError("set @@global.tidb_ddl_reorg_worker_cnt = invalid_val", variable.ErrWrongTypeForVar)
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, int32(100), variable.GetDDLReorgWorkerCounter())
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = -1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_worker_cnt value: '-1'"))
	tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	res := tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))

	res = tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	res = tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))

	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 257")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_worker_cnt value: '257'"))
	tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt").Check(testkit.Rows("256"))
}

func TestSetDDLReorgBatchSize(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, int32(variable.DefTiDBDDLReorgBatchSize), variable.GetDDLReorgBatchSize())

	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '1'"))
	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, variable.MinDDLReorgBatchSize, variable.GetDDLReorgBatchSize())
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_batch_size = %v", variable.MaxDDLReorgBatchSize+1))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '%d'", variable.MaxDDLReorgBatchSize+1)))
	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, variable.MaxDDLReorgBatchSize, variable.GetDDLReorgBatchSize())
	tk.MustGetDBError("set @@global.tidb_ddl_reorg_batch_size = invalid_val", variable.ErrWrongTypeForVar)
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 100")
	err = ddlutil.LoadDDLReorgVars(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, int32(100), variable.GetDDLReorgBatchSize())
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '-1'"))

	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 100")
	res := tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows("100"))

	res = tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows(fmt.Sprintf("%v", 100)))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 1000")
	res = tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows("1000"))
}

func TestSetDDLErrorCountLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLVars(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(variable.DefTiDBDDLErrorCountLimit), variable.GetDDLErrorCountLimit())

	tk.MustExec("set @@global.tidb_ddl_error_count_limit = -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_error_count_limit value: '-1'"))
	err = ddlutil.LoadDDLVars(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(0), variable.GetDDLErrorCountLimit())
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %v", uint64(math.MaxInt64)+1))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_ddl_error_count_limit value: '%d'", uint64(math.MaxInt64)+1)))
	err = ddlutil.LoadDDLVars(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), variable.GetDDLErrorCountLimit())
	tk.MustGetDBError("set @@global.tidb_ddl_error_count_limit = invalid_val", variable.ErrWrongTypeForVar)
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 100")
	err = ddlutil.LoadDDLVars(tk.Session())
	require.NoError(t, err)
	require.Equal(t, int64(100), variable.GetDDLErrorCountLimit())
	res := tk.MustQuery("select @@global.tidb_ddl_error_count_limit")
	res.Check(testkit.Rows("100"))
}

func TestLoadDDLDistributeVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.Equal(t, variable.DefTiDBEnableDistTask, variable.EnableDistTask.Load())
	tk.MustGetDBError("set @@global.tidb_enable_dist_task = invalid_val", variable.ErrWrongValueForVar)
	require.Equal(t, variable.DefTiDBEnableDistTask, variable.EnableDistTask.Load())
	tk.MustExec("set @@global.tidb_enable_dist_task = 'on'")
	require.Equal(t, true, variable.EnableDistTask.Load())
	tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_dist_task = %v", false))
	require.Equal(t, false, variable.EnableDistTask.Load())
}

// this test will change the fail-point `mockAutoIDChange`, so we move it to the `testRecoverTable` suite
func TestRenameTable(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange"))
	}()
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists rename1")
	tk.MustExec("drop database if exists rename2")
	tk.MustExec("drop database if exists rename3")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create database rename3")
	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename table rename1.t to rename2.t")
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t values ()")
	tk.MustExec("rename table rename2.t to rename3.t")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Rows("1", "2", "3"))
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustExec("drop database rename2")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("drop database rename3")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("rename table rename1.t to rename2.t1")
	tk.MustExec("insert rename2.t1 values ()")
	result := tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Rows("1"))
	// Make sure the drop old database doesn't affect the t1's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Rows("1", "2"))
	// Rename a table to another table in the same database.
	tk.MustExec("rename table rename2.t1 to rename2.t2")
	tk.MustExec("insert rename2.t2 values ()")
	result = tk.MustQuery("select * from rename2.t2")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop database rename2")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename table rename1.t to rename2.t1")
	// Make sure the value is greater than autoid.step.
	tk.MustExec("insert rename2.t1 values (100000)")
	tk.MustExec("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Rows("1", "100000", "100001"))
	tk.MustExecToErr("insert rename1.t values ()")
	tk.MustExec("drop database rename1")
	tk.MustExec("drop database rename2")
}

func TestRenameMultiTables(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange"))
	}()
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists rename1")
	tk.MustExec("drop database if exists rename2")
	tk.MustExec("drop database if exists rename3")
	tk.MustExec("drop database if exists rename4")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create database rename3")
	tk.MustExec("create database rename4")
	tk.MustExec("create table rename1.t1 (a int primary key auto_increment)")
	tk.MustExec("create table rename3.t3 (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t1 values ()")
	tk.MustExec("insert rename3.t3 values ()")
	tk.MustExec("rename table rename1.t1 to rename2.t2, rename3.t3 to rename4.t4")
	// Make sure the drop old database doesn't affect t2,t4's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t2 values ()")
	tk.MustExec("drop database rename3")
	tk.MustExec("insert rename4.t4 values ()")
	tk.MustQuery("select * from rename2.t2").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select * from rename4.t4").Check(testkit.Rows("1", "2"))
	// Rename a table to another table in the same database.
	tk.MustExec("rename table rename2.t2 to rename2.t1, rename4.t4 to rename4.t3")
	tk.MustExec("insert rename2.t1 values ()")
	tk.MustQuery("select * from rename2.t1").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("insert rename4.t3 values ()")
	tk.MustQuery("select * from rename4.t3").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop database rename2")
	tk.MustExec("drop database rename4")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create database rename3")
	tk.MustExec("create table rename1.t1 (a int primary key auto_increment)")
	tk.MustExec("create table rename3.t3 (a int primary key auto_increment)")
	tk.MustGetErrCode("rename table rename1.t1 to rename2.t2, rename3.t3 to rename2.t2", errno.ErrTableExists)
	tk.MustExec("rename table rename1.t1 to rename2.t2, rename2.t2 to rename1.t1")
	tk.MustExec("rename table rename1.t1 to rename2.t2, rename3.t3 to rename1.t1")
	tk.MustExec("use rename1")
	tk.MustQuery("show tables").Check(testkit.Rows("t1"))
	tk.MustExec("use rename2")
	tk.MustQuery("show tables").Check(testkit.Rows("t2"))
	tk.MustExec("use rename3")
	tk.MustExec("create table rename3.t3 (a int primary key auto_increment)")
	tk.MustGetErrCode("rename table rename1.t1 to rename1.t2, rename1.t1 to rename3.t3", errno.ErrTableExists)
	tk.MustGetErrCode("rename table rename1.t1 to rename1.t2, rename1.t1 to rename3.t4", errno.ErrNoSuchTable)
	tk.MustExec("drop database rename1")
	tk.MustExec("drop database rename2")
	tk.MustExec("drop database rename3")
}
