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

package ddl_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	testddlutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

const columnModifyLease = 600 * time.Millisecond

func TestAddAndDropColumn(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t2 (c1 int, c2 int, c3 int)")
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")

	// ==========
	// ADD COLUMN
	// ==========

	done := make(chan error, 1)

	num := defaultBatchSize + 10
	// add some rows
	batchInsert(tk, "t2", 0, num)

	testddlutil.SessionExecInGoroutine(store, "test", "alter table t2 add column c4 int default -1", done)

	ticker := time.NewTicker(columnModifyLease / 2)
	defer ticker.Stop()
	step := 10
AddLoop:
	for {
		select {
		case err := <-done:
			if err == nil {
				break AddLoop
			}
			require.NoError(t, err)
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustExec("begin")
				tk.MustExec("delete from t2 where c1 = ?", n)
				tk.MustExec("commit")

				// Make sure that statement of insert and show use the same infoSchema.
				tk.MustExec("begin")
				err := tk.ExecToErr("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the column number must be 4 now.
					values := tk.MustQuery("show columns from t2").Rows()
					require.Len(t, values, 4)
				}
				tk.MustExec("commit")
			}
			num += step
		}
	}

	// add data, here c4 must exist
	for i := num; i < num+step; i++ {
		tk.MustExec("insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	rows := tk.MustQuery("select count(c4) from t2").Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.Greater(t, count, int64(0))

	tk.MustQuery("select count(c4) from t2 where c4 = -1").Check([][]interface{}{
		{fmt.Sprintf("%v", count-int64(step))},
	})

	for i := num; i < num+step; i++ {
		tk.MustQuery("select c4 from t2 where c4 = ?", i).Check([][]interface{}{
			{fmt.Sprintf("%v", i)},
		})
	}

	tbl := external.GetTableByName(t, tk, "test", "t2")
	i := 0
	j := 0
	require.NoError(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	defer func() {
		if txn, err := tk.Session().Txn(true); err == nil {
			require.NoError(t, txn.Rollback())
		}
	}()

	err = tables.IterRecords(tbl, tk.Session(), tbl.Cols(),
		func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			i++
			// c4 must be -1 or > 0
			v, err := data[3].ToInt64(tk.Session().GetSessionVars().StmtCtx)
			require.NoError(t, err)
			if v == -1 {
				j++
			} else {
				require.Greater(t, v, int64(0))
			}
			return true, nil
		})
	require.NoError(t, err)
	require.Equal(t, int(count), i)
	require.LessOrEqual(t, i, num+step)
	require.Equal(t, int(count)-step, j)

	// for modifying columns after adding columns
	tk.MustExec("alter table t2 modify c4 int default 11")
	for i := num + step; i < num+step+10; i++ {
		tk.MustExec("insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}
	tk.MustQuery("select count(c4) from t2 where c4 = -1").Check([][]interface{}{
		{fmt.Sprintf("%v", count-int64(step))},
	})

	// add timestamp type column
	tk.MustExec("create table test_on_update_c (c1 int, c2 timestamp);")
	defer tk.MustExec("drop table test_on_update_c;")
	tk.MustExec("alter table test_on_update_c add column c3 timestamp null default '2017-02-11' on update current_timestamp;")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_on_update_c"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	colC := tblInfo.Columns[2]
	require.Equal(t, mysql.TypeTimestamp, colC.GetType())
	require.False(t, mysql.HasNotNullFlag(colC.GetFlag()))
	// add datetime type column
	tk.MustExec("create table test_on_update_d (c1 int, c2 datetime);")
	tk.MustExec("alter table test_on_update_d add column c3 datetime on update current_timestamp;")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_on_update_d"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	require.Equal(t, mysql.TypeDatetime, colC.GetType())
	require.False(t, mysql.HasNotNullFlag(colC.GetFlag()))

	// add year type column
	tk.MustExec("create table test_on_update_e (c1 int);")
	defer tk.MustExec("drop table test_on_update_e;")
	tk.MustExec("insert into test_on_update_e (c1) values (0);")
	tk.MustExec("alter table test_on_update_e add column c2 year not null;")
	tk.MustQuery("select c2 from test_on_update_e").Check(testkit.Rows("0"))

	// test add unsupported constraint
	tk.MustExec("create table t_add_unsupported_constraint (a int);")
	err = tk.ExecToErr("ALTER TABLE t_add_unsupported_constraint ADD id int AUTO_INCREMENT;")
	require.EqualError(t, err, "[ddl:8200]unsupported add column 'id' constraint AUTO_INCREMENT when altering 'test.t_add_unsupported_constraint'")
	err = tk.ExecToErr("ALTER TABLE t_add_unsupported_constraint ADD id int KEY;")
	require.EqualError(t, err, "[ddl:8200]unsupported add column 'id' constraint PRIMARY KEY when altering 'test.t_add_unsupported_constraint'")
	err = tk.ExecToErr("ALTER TABLE t_add_unsupported_constraint ADD id int UNIQUE;")
	require.EqualError(t, err, "[ddl:8200]unsupported add column 'id' constraint UNIQUE KEY when altering 'test.t_add_unsupported_constraint'")

	// ===========
	// DROP COLUMN
	// ===========

	done = make(chan error, 1)
	tk.MustExec("delete from t2")

	num = 100
	// add some rows
	for i := 0; i < num; i++ {
		tk.MustExec("insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	// get c4 column id
	testddlutil.SessionExecInGoroutine(store, "test", "alter table t2 drop column c4", done)

	ticker = time.NewTicker(columnModifyLease / 2)
	defer ticker.Stop()
	step = 10
DropLoop:
	for {
		select {
		case err := <-done:
			if err == nil {
				break DropLoop
			}
			require.NoError(t, err)
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				// Make sure that statement of insert and show use the same infoSchema.
				tk.MustExec("begin")
				err := tk.ExecToErr("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// If executing is failed, the column number must be 4 now.
					values := tk.MustQuery("show columns from t2").Rows()
					require.Len(t, values, 4)
				}
				tk.MustExec("commit")
			}
			num += step
		}
	}

	// add data, here c4 must not exist
	for i := num; i < num+step; i++ {
		tk.MustExec("insert into t2 values (?, ?, ?)", i, i, i)
	}

	rows = tk.MustQuery("select count(*) from t2").Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	count, err = strconv.ParseInt(rows[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.Greater(t, count, int64(0))
}

// TestDropColumn is for inserting value with a to-be-dropped column when do drop column.
// Column info from schema in build-insert-plan should be public only,
// otherwise they will not be consisted with Table.Col(), then the server will panic.
func TestDropColumn(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	num := 25
	multiDDL := make([]string, 0, num)
	sql := "create table t2 (c1 int, c2 int, c3 int, "
	for i := 4; i < 4+num; i++ {
		multiDDL = append(multiDDL, fmt.Sprintf("alter table t2 drop column c%d", i))

		if i != 3+num {
			sql += fmt.Sprintf("c%d int, ", i)
		} else {
			sql += fmt.Sprintf("c%d int)", i)
		}
	}
	tk.MustExec(sql)
	dmlDone := make(chan error, num)
	ddlDone := make(chan error, num)

	testddlutil.ExecMultiSQLInGoroutine(store, "test", multiDDL, ddlDone)
	for i := 0; i < num; i++ {
		testddlutil.ExecMultiSQLInGoroutine(store, "test", []string{"insert into t2 set c1 = 1, c2 = 1, c3 = 1, c4 = 1"}, dmlDone)
	}
	for i := 0; i < num; i++ {
		err := <-ddlDone
		require.NoError(t, err)
	}

	// Test for drop partition table column.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int) partition by hash(a) partitions 4;")
	err := tk.ExecToErr("alter table t1 drop column a")
	// TODO: refine the error message to compatible with MySQL
	require.EqualError(t, err, "[planner:1054]Unknown column 'a' in 'expression'")
}

func TestChangeColumn(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t3 (a int default '0', b varchar(10), d int not null default '0')")
	tk.MustExec("insert into t3 set b = 'a'")
	tk.MustQuery("select a from t3").Check(testkit.Rows("0"))
	tk.MustExec("alter table t3 change a aa bigint")
	tk.MustExec("insert into t3 set b = 'b'")
	tk.MustQuery("select aa from t3").Check(testkit.Rows("0", "<nil>"))
	// for no default flag
	tk.MustExec("alter table t3 change d dd bigint not null")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	colD := tblInfo.Columns[2]
	require.True(t, mysql.HasNoDefaultValueFlag(colD.GetFlag()))
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	tk.MustExec("alter table t3 change b b varchar(20) null default 'c' comment 'my comment'")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colB := tblInfo.Columns[1]
	require.Equal(t, "my comment", colB.Comment)
	require.False(t, mysql.HasNotNullFlag(colB.GetFlag()))
	tk.MustExec("insert into t3 set aa = 3, dd = 5")
	tk.MustQuery("select b from t3").Check(testkit.Rows("a", "b", "c"))
	// for timestamp
	tk.MustExec("alter table t3 add column c timestamp not null")
	tk.MustExec("alter table t3 change c c timestamp null default '2017-02-11' comment 'col c comment' on update current_timestamp")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[3]
	require.Equal(t, "col c comment", colC.Comment)
	require.False(t, mysql.HasNotNullFlag(colC.GetFlag()))
	// for enum
	tk.MustExec("alter table t3 add column en enum('a', 'b', 'c') not null default 'a'")
	// https://github.com/pingcap/tidb/issues/23488
	// if there is a prefix index on the varchar column, then we can change it to text
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (k char(10), v int, INDEX(k(7)));")
	tk.MustExec("alter table t change column k k tinytext")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// for failing tests
	sql := "alter table t3 change aa a bigint default ''"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "alter table t3 change a testx.t3.aa bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongDBName)
	sql = "alter table t3 change t.a aa bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongTableName)
	tk.MustExec("create table t4 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustExec("insert into t4(c2) values (null);")
	err = tk.ExecToErr("alter table t4 change c1 a1 int not null;")
	require.EqualError(t, err, "[ddl:1265]Data truncated for column 'a1' at row 1")
	sql = "alter table t4 change c2 a bigint not null;"
	tk.MustGetErrCode(sql, mysql.WarnDataTruncated)
	sql = "alter table t3 modify en enum('a', 'z', 'b', 'c') not null default 'a'"
	tk.MustExec(sql)
	// Rename to an existing column.
	tk.MustExec("alter table t3 add column a bigint")
	sql = "alter table t3 change aa a bigint"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	// https://github.com/pingcap/tidb/issues/23488
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5 (k char(10) primary key, v int)")
	sql = "alter table t5 change column k k tinytext;"
	tk.MustGetErrCode(sql, mysql.ErrBlobKeyWithoutLength)
	tk.MustExec("drop table t5")
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5 (k char(10), v int, INDEX(k))")
	sql = "alter table t5 change column k k tinytext;"
	tk.MustGetErrCode(sql, mysql.ErrBlobKeyWithoutLength)
	tk.MustExec("drop table t5")
	tk.MustExec("drop table t3")
}

func TestRenameColumn(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	assertColNames := func(tableName string, colNames ...string) {
		cols := external.GetTableByName(t, tk, "test", tableName).Cols()
		require.Equal(t, len(colNames), len(cols))
		for i := range cols {
			require.Equal(t, strings.ToLower(colNames[i]), cols[i].Name.L)
		}
	}

	tk.MustExec("create table test_rename_column (id int not null primary key auto_increment, col1 int)")
	tk.MustExec("alter table test_rename_column rename column col1 to col1")
	assertColNames("test_rename_column", "id", "col1")
	tk.MustExec("alter table test_rename_column rename column col1 to col2")
	assertColNames("test_rename_column", "id", "col2")

	// Test renaming non-exist columns.
	tk.MustGetErrCode("alter table test_rename_column rename column non_exist_col to col3", errno.ErrBadField)

	// Test renaming to an exist column.
	tk.MustGetErrCode("alter table test_rename_column rename column col2 to id", errno.ErrDupFieldName)

	// Test renaming the column with foreign key.
	tk.MustExec("drop table test_rename_column")
	tk.MustExec("create table test_rename_column_base (base int)")
	tk.MustExec("create table test_rename_column (col int, foreign key (col) references test_rename_column_base(base))")

	tk.MustGetErrCode("alter table test_rename_column rename column col to col1", errno.ErrFKIncompatibleColumns)

	tk.MustExec("drop table test_rename_column_base")

	// Test renaming generated columns.
	tk.MustExec("drop table test_rename_column")
	tk.MustExec("create table test_rename_column (id int, col1 int generated always as (id + 1))")

	tk.MustExec("alter table test_rename_column rename column col1 to col2")
	assertColNames("test_rename_column", "id", "col2")
	tk.MustExec("alter table test_rename_column rename column col2 to col1")
	assertColNames("test_rename_column", "id", "col1")
	tk.MustGetErrCode("alter table test_rename_column rename column id to id1", errno.ErrDependentByGeneratedColumn)

	// Test renaming view columns.
	tk.MustExec("drop table test_rename_column")
	tk.MustExec("create table test_rename_column (id int, col1 int)")
	tk.MustExec("create view test_rename_column_view as select * from test_rename_column")

	tk.MustExec("alter table test_rename_column rename column col1 to col2")
	tk.MustGetErrCode("select * from test_rename_column_view", errno.ErrViewInvalid)

	tk.MustExec("drop view test_rename_column_view")
	tk.MustExec("drop table test_rename_column")

	// Test rename a non-exists column. See https://github.com/pingcap/tidb/issues/34811.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustGetErrCode("alter table t rename column b to b;", errno.ErrBadField)
}

func TestVirtualColumnDDL(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create global temporary table test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored) on commit delete rows;`)
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_gv_ddl"))
	require.NoError(t, err)
	testCases := []struct {
		generatedExprString string
		generatedStored     bool
	}{
		{"", false},
		{"`a` + 8", false},
		{"`b` + 2", true},
	}
	for i, column := range tbl.Meta().Columns {
		require.Equal(t, testCases[i].generatedExprString, column.GeneratedExprString)
		require.Equal(t, testCases[i].generatedStored, column.GeneratedStored)
	}
	result := tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))
	tk.MustExec("begin;")
	tk.MustExec("insert into test_gv_ddl values (1, default, default)")
	tk.MustQuery("select * from test_gv_ddl").Check(testkit.Rows("1 9 11"))
	tk.MustExec("commit")

	// for local temporary table
	tk.MustExec(`create temporary table test_local_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored);`)
	is = tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_local_gv_ddl"))
	require.NoError(t, err)
	for i, column := range tbl.Meta().Columns {
		require.Equal(t, testCases[i].generatedExprString, column.GeneratedExprString)
		require.Equal(t, testCases[i].generatedStored, column.GeneratedStored)
	}
	result = tk.MustQuery(`DESC test_local_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))
	tk.MustExec("begin;")
	tk.MustExec("insert into test_local_gv_ddl values (1, default, default)")
	tk.MustQuery("select * from test_local_gv_ddl").Check(testkit.Rows("1 9 11"))
	tk.MustExec("commit")
	tk.MustQuery("select * from test_local_gv_ddl").Check(testkit.Rows("1 9 11"))
}

func TestGeneratedColumnDDL(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Check create table with virtual and stored generated columns.
	tk.MustExec(`CREATE TABLE test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored)`)

	// Check desc table with virtual and stored generated columns.
	result := tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	// Check show create table with virtual and stored generated columns.
	result = tk.MustQuery(`show create table test_gv_ddl`)
	result.Check(testkit.Rows(
		"test_gv_ddl CREATE TABLE `test_gv_ddl` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) GENERATED ALWAYS AS (`a` + 8) VIRTUAL,\n  `c` int(11) GENERATED ALWAYS AS (`b` + 2) STORED\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Check generated expression with blanks.
	tk.MustExec("create table table_with_gen_col_blanks (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char)), c int as (a+100))")
	result = tk.MustQuery(`show create table table_with_gen_col_blanks`)
	result.Check(testkit.Rows("table_with_gen_col_blanks CREATE TABLE `table_with_gen_col_blanks` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (cast(`a` as char)) VIRTUAL,\n" +
		"  `c` int(11) GENERATED ALWAYS AS (`a` + 100) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Check generated expression with charset latin1 ("latin1" != mysql.DefaultCharset).
	tk.MustExec("create table table_with_gen_col_latin1 (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char charset latin1)), c int as (a+100))")
	result = tk.MustQuery(`show create table table_with_gen_col_latin1`)
	result.Check(testkit.Rows("table_with_gen_col_latin1 CREATE TABLE `table_with_gen_col_latin1` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (cast(`a` as char charset latin1)) VIRTUAL,\n" +
		"  `c` int(11) GENERATED ALWAYS AS (`a` + 100) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Check generated expression with string (issue 9457).
	tk.MustExec("create table table_with_gen_col_string (first_name varchar(10), last_name varchar(10), full_name varchar(255) AS (CONCAT(first_name,' ',last_name)))")
	result = tk.MustQuery(`show create table table_with_gen_col_string`)
	result.Check(testkit.Rows("table_with_gen_col_string CREATE TABLE `table_with_gen_col_string` (\n" +
		"  `first_name` varchar(10) DEFAULT NULL,\n" +
		"  `last_name` varchar(10) DEFAULT NULL,\n" +
		"  `full_name` varchar(255) GENERATED ALWAYS AS (concat(`first_name`, _utf8mb4' ', `last_name`)) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("alter table table_with_gen_col_string modify column full_name varchar(255) GENERATED ALWAYS AS (CONCAT(last_name,' ' ,first_name) ) VIRTUAL")
	result = tk.MustQuery(`show create table table_with_gen_col_string`)
	result.Check(testkit.Rows("table_with_gen_col_string CREATE TABLE `table_with_gen_col_string` (\n" +
		"  `first_name` varchar(10) DEFAULT NULL,\n" +
		"  `last_name` varchar(10) DEFAULT NULL,\n" +
		"  `full_name` varchar(255) GENERATED ALWAYS AS (concat(`last_name`, _utf8mb4' ', `first_name`)) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test incorrect parameter count.
	tk.MustGetErrCode("create table test_gv_incorrect_pc(a double, b int as (lower(a, 2)))", errno.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("create table test_gv_incorrect_pc(a double, b int as (lower(a, 2)) stored)", errno.ErrWrongParamcountToNativeFct)

	genExprTests := []struct {
		stmt string
		err  int
	}{
		// Drop/rename columns dependent by other column.
		{`alter table test_gv_ddl drop column a`, errno.ErrDependentByGeneratedColumn},
		{`alter table test_gv_ddl change column a anew int`, errno.ErrBadField},

		// Modify/change stored status of generated columns.
		{`alter table test_gv_ddl modify column b bigint`, errno.ErrUnsupportedOnGeneratedColumn},
		{`alter table test_gv_ddl change column c cnew bigint as (a+100)`, errno.ErrUnsupportedOnGeneratedColumn},

		// Modify/change generated columns breaking prior.
		{`alter table test_gv_ddl modify column b int as (c+100)`, errno.ErrGeneratedColumnNonPrior},
		{`alter table test_gv_ddl change column b bnew int as (c+100)`, errno.ErrGeneratedColumnNonPrior},

		// Refer not exist columns in generation expression.
		{`create table test_gv_ddl_bad (a int, b int as (c+8))`, errno.ErrBadField},

		// Refer generated columns non prior.
		{`create table test_gv_ddl_bad (a int, b int as (c+1), c int as (a+1))`, errno.ErrGeneratedColumnNonPrior},

		// Virtual generated columns cannot be primary key.
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b) primary key)`, errno.ErrUnsupportedOnGeneratedColumn},
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b), primary key(c))`, errno.ErrUnsupportedOnGeneratedColumn},
		{`create table test_gv_ddl_bad (a int, b int, c int as (a+b), primary key(a, c))`, errno.ErrUnsupportedOnGeneratedColumn},

		// Add stored generated column through alter table.
		{`alter table test_gv_ddl add column d int as (b+2) stored`, errno.ErrUnsupportedOnGeneratedColumn},
		{`alter table test_gv_ddl modify column b int as (a + 8) stored`, errno.ErrUnsupportedOnGeneratedColumn},

		// Add generated column with incorrect parameter count.
		{`alter table test_gv_ddl add column z int as (lower(a, 2))`, errno.ErrWrongParamcountToNativeFct},
		{`alter table test_gv_ddl add column z int as (lower(a, 2)) stored`, errno.ErrWrongParamcountToNativeFct},

		// Modify generated column with incorrect parameter count.
		{`alter table test_gv_ddl modify column b int as (lower(a, 2))`, errno.ErrWrongParamcountToNativeFct},
		{`alter table test_gv_ddl change column b b int as (lower(a, 2))`, errno.ErrWrongParamcountToNativeFct},
	}
	for _, tt := range genExprTests {
		tk.MustGetErrCode(tt.stmt, tt.err)
	}

	// Check alter table modify/change generated column.
	modStoredColErrMsg := "[ddl:3106]'modifying a stored column' is not supported for generated columns."
	tk.MustGetErrMsg(`alter table test_gv_ddl modify column c bigint as (b+200) stored`, modStoredColErrMsg)

	result = tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	tk.MustExec(`alter table test_gv_ddl change column b b bigint as (a+100) virtual`)
	result = tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	tk.MustExec(`alter table test_gv_ddl change column c cnew bigint`)
	result = tk.MustQuery(`DESC test_gv_ddl`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `cnew bigint(20) YES  <nil> `))

	// Test generated column `\\`.
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(c0 TEXT AS ('\\\\'));")
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("\\"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(c0 TEXT AS ('a\\\\b\\\\c\\\\'))")
	tk.MustExec("insert into t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("a\\b\\c\\"))
}

func TestColumnModifyingDefinition(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustExec("alter table test2 change c2 a int not null;")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test2"))
	require.NoError(t, err)
	var c2 *table.Column
	for _, col := range tbl.Cols() {
		if col.Name.L == "a" {
			c2 = col
		}
	}
	require.True(t, mysql.HasNotNullFlag(c2.GetFlag()))

	tk.MustExec("drop table if exists test2;")
	tk.MustExec("create table test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustExec("insert into test2(c2) values (null);")
	tk.MustGetErrMsg("alter table test2 change c2 a int not null", "[ddl:1265]Data truncated for column 'a' at row 1")
	tk.MustGetErrCode("alter table test2 change c1 a1 bigint not null;", mysql.WarnDataTruncated)
}

func TestTransactionWithWriteOnlyColumn(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int key);")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"update t1 set a=2 where a=1",
			"commit",
		},
	}

	hook := &ddl.TestDDLCallback{Do: dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateWriteOnly:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, sql := range transaction {
				if _, checkErr = tk.Exec(sql); checkErr != nil {
					checkErr = errors.Errorf("err: %s, sql: %s, job schema state: %s", checkErr.Error(), sql, job.SchemaState)
					return
				}
			}
		}
	}
	dom.DDL().SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add column.
	go backgroundExec(store, "alter table t1 add column c int not null", done)
	err := <-done
	require.NoError(t, err)
	require.NoError(t, checkErr)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
	tk.MustExec("delete from t1")

	// test transaction on drop column.
	go backgroundExec(store, "alter table t1 drop column c", done)
	err = <-done
	require.NoError(t, err)
	require.NoError(t, checkErr)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
}

func TestColumnCheck(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists column_check")
	tk.MustExec("create table column_check (pk int primary key, a int check (a > 1))")
	defer tk.MustExec("drop table if exists column_check")
	require.Equal(t, uint16(1), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|8231|CONSTRAINT CHECK is not supported"))
}

func TestModifyGeneratedColumn(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	modIdxColErrMsg := "[ddl:3106]'modifying an indexed column' is not supported for generated columns."
	modStoredColErrMsg := "[ddl:3106]'modifying a stored column' is not supported for generated columns."

	// Modify column with single-col-index.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(b));")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustGetErrMsg("alter table t1 modify column b int as (a+2);", modIdxColErrMsg)
	tk.MustExec("drop index idx on t1;")
	tk.MustExec("alter table t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify column with multi-col-index.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(a, b));")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustGetErrMsg("alter table t1 modify column b int as (a+2);", modIdxColErrMsg)
	tk.MustExec("drop index idx on t1;")
	tk.MustExec("alter table t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify column with stored status to a different expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustGetErrMsg("alter table t1 modify column b int as (a+2) stored;", modStoredColErrMsg)

	// Modify column with stored status to the same expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b bigint as (a+1) stored;")
	tk.MustExec("alter table t1 modify column b bigint as (a + 1) stored;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify column with index to the same expression.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1), index idx(b));")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b bigint as (a+1);")
	tk.MustExec("alter table t1 modify column b bigint as (a + 1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify column from non-generated to stored generated.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int);")
	tk.MustGetErrMsg("alter table t1 modify column b bigint as (a+1) stored;", modStoredColErrMsg)

	// Modify column from stored generated to non-generated.
	tk.MustExec("drop table t1;")
	tk.MustExec("create table t1 (a int, b int as (a+1) stored);")
	tk.MustExec("insert into t1 set a=1;")
	tk.MustExec("alter table t1 modify column b int;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))
}

func TestCheckColumnDefaultValue(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists text_default_text;")
	tk.MustGetErrCode("create table text_default_text(c1 text not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_text(c1 text not null default 'scds');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("drop table if exists text_default_json;")
	tk.MustGetErrCode("create table text_default_json(c1 json not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_json(c1 json not null default 'dfew555');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("drop table if exists text_default_blob;")
	tk.MustGetErrCode("create table text_default_blob(c1 blob not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create table text_default_blob(c1 blob not null default 'scds54');", errno.ErrBlobCantHaveDefault)

	tk.MustExec("set sql_mode='';")
	tk.MustExec("create table text_default_text(c1 text not null default '');")
	tk.MustQuery(`show create table text_default_text`).Check(testkit.RowsWithSep("|",
		"text_default_text CREATE TABLE `text_default_text` (\n"+
			"  `c1` text NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tblInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_text"))
	require.NoError(t, err)
	require.Empty(t, tblInfo.Meta().Columns[0].DefaultValue)

	tk.MustExec("create table text_default_blob(c1 blob not null default '');")
	tk.MustQuery(`show create table text_default_blob`).Check(testkit.RowsWithSep("|",
		"text_default_blob CREATE TABLE `text_default_blob` (\n"+
			"  `c1` blob NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_blob"))
	require.NoError(t, err)
	require.Empty(t, tblInfo.Meta().Columns[0].DefaultValue)

	tk.MustExec("create table text_default_json(c1 json not null default '');")
	tk.MustQuery(`show create table text_default_json`).Check(testkit.RowsWithSep("|",
		"text_default_json CREATE TABLE `text_default_json` (\n"+
			"  `c1` json NOT NULL DEFAULT 'null'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tblInfo, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("text_default_json"))
	require.NoError(t, err)
	require.Equal(t, "null", tblInfo.Meta().Columns[0].DefaultValue)
}

func TestCheckConvertToCharacter(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(10) charset binary);")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset utf8 collate utf8_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset utf8mb4 collate utf8mb4_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(10) charset latin1 collate latin1_bin", errno.ErrUnsupportedDDLOperation)
	require.Equal(t, "binary", tbl.Cols()[0].GetCharset())
}

func TestAddMultiColumnsIndex(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists tidb;")
	tk.MustExec("create database tidb;")
	tk.MustExec("use tidb;")
	tk.MustExec("create table tidb.test (a int auto_increment primary key, b int);")
	tk.MustExec("insert tidb.test values (1, 1);")
	tk.MustExec("update tidb.test set b = b + 1 where a = 1;")
	tk.MustExec("insert into tidb.test values (2, 2);")
	// Test that the b value is nil.
	tk.MustExec("insert into tidb.test (a) values (3);")
	tk.MustExec("insert into tidb.test values (4, 4);")
	// Test that the b value is nil again.
	tk.MustExec("insert into tidb.test (a) values (5);")
	tk.MustExec("insert tidb.test values (6, 6);")
	tk.MustExec("alter table tidb.test add index idx1 (a, b);")
	tk.MustExec("admin check table test")
}

// For issue #31735.
func TestAddGeneratedColumnAndInsert(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, unique kye(a))")
	tk.MustExec("insert into t1 value (1), (10)")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	d := dom.DDL()
	hook := &ddl.TestDDLCallback{Do: dom}
	ctx := mock.NewContext()
	ctx.Store = store
	times := 0
	var checkErr error
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (1) on duplicate key update a=a+1")
			if checkErr == nil {
				_, checkErr = tk1.Exec("replace into t1 values (2)")
			}
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t1 values (2) on duplicate key update a=a+1")
			if checkErr == nil {
				_, checkErr = tk1.Exec("replace into t1 values (3)")
			}
		case model.StateWriteReorganization:
			if checkErr == nil && job.SchemaState == model.StateWriteReorganization && times == 0 {
				_, checkErr = tk1.Exec("insert into t1 values (3) on duplicate key update a=a+1")
				if checkErr == nil {
					_, checkErr = tk1.Exec("replace into t1 values (4)")
				}
				times++
			}
		}
	}
	d.SetHook(hook)

	tk.MustExec("alter table t1 add column gc int as ((a+1))")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("4 5", "10 11"))
	require.NoError(t, checkErr)
}

func TestColumnTypeChangeGenUniqueChangingName(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	hook := &ddl.TestDDLCallback{}
	var checkErr error
	assertChangingColName := "_col$_c2_0"
	assertChangingIdxName := "_idx$_idx_0"
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			err := job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			if err != nil {
				checkErr = err
				return
			}
			if changingCol.Name.L != assertChangingColName {
				checkErr = errors.New("changing column name is incorrect")
			} else if changingIdxs[0].Name.L != assertChangingIdxName {
				checkErr = errors.New("changing index name is incorrect")
			}
		}
	}
	d := dom.DDL()
	d.SetHook(hook)

	tk.MustExec("create table if not exists t(c1 varchar(256), c2 bigint, `_col$_c2` varchar(10), unique _idx$_idx(c1), unique idx(c2));")
	tk.MustExec("alter table test.t change column c2 cC2 tinyint after `_col$_c2`")
	require.NoError(t, checkErr)

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().Columns, 3)
	require.Equal(t, "c1", tbl.Meta().Columns[0].Name.O)
	require.Equal(t, 0, tbl.Meta().Columns[0].Offset)
	require.Equal(t, "_col$_c2", tbl.Meta().Columns[1].Name.O)
	require.Equal(t, 1, tbl.Meta().Columns[1].Offset)
	require.Equal(t, "cC2", tbl.Meta().Columns[2].Name.O)
	require.Equal(t, 2, tbl.Meta().Columns[2].Offset)

	require.Len(t, tbl.Meta().Indices, 2)
	require.Equal(t, "_idx$_idx", tbl.Meta().Indices[0].Name.O)
	require.Equal(t, "idx", tbl.Meta().Indices[1].Name.O)

	require.Len(t, tbl.Meta().Indices[0].Columns, 1)
	require.Equal(t, "c1", tbl.Meta().Indices[0].Columns[0].Name.O)
	require.Equal(t, 0, tbl.Meta().Indices[0].Columns[0].Offset)

	require.Len(t, tbl.Meta().Indices[1].Columns, 1)
	require.Equal(t, "cC2", tbl.Meta().Indices[1].Columns[0].Name.O)
	require.Equal(t, 2, tbl.Meta().Indices[1].Columns[0].Offset)

	assertChangingColName1 := "_col$__col$_c1_1"
	assertChangingColName2 := "_col$__col$__col$_c1_0_1"
	query1 := "alter table t modify column _col$_c1 tinyint"
	query2 := "alter table t modify column _col$__col$_c1_0 tinyint"
	hook.OnJobUpdatedExported = func(job *model.Job) {
		if (job.Query == query1 || job.Query == query2) && job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				newCol                *model.ColumnInfo
				oldColName            *model.CIStr
				modifyColumnTp        byte
				updatedAutoRandomBits uint64
				changingCol           *model.ColumnInfo
				changingIdxs          []*model.IndexInfo
			)
			pos := &ast.ColumnPosition{}
			err := job.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
			if err != nil {
				checkErr = err
				return
			}
			if job.Query == query1 && changingCol.Name.L != assertChangingColName1 {
				checkErr = errors.New("changing column name is incorrect")
			}
			if job.Query == query2 && changingCol.Name.L != assertChangingColName2 {
				checkErr = errors.New("changing column name is incorrect")
			}
		}
	}
	d.SetHook(hook)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table if not exists t(c1 bigint, _col$_c1 bigint, _col$__col$_c1_0 bigint, _col$__col$__col$_c1_0_0 bigint)")
	tk.MustExec("alter table t modify column c1 tinyint")
	tk.MustExec("alter table t modify column _col$_c1 tinyint")
	require.NoError(t, checkErr)
	tk.MustExec("alter table t modify column _col$__col$_c1_0 tinyint")
	require.NoError(t, checkErr)
	tk.MustExec("alter table t change column _col$__col$__col$_c1_0_0  _col$__col$__col$_c1_0_0 tinyint")

	tbl = external.GetTableByName(t, tk, "test", "t")
	require.Len(t, tbl.Meta().Columns, 4)
	require.Equal(t, "c1", tbl.Meta().Columns[0].Name.O)
	require.Equal(t, mysql.TypeTiny, tbl.Meta().Columns[0].GetType())
	require.Equal(t, 0, tbl.Meta().Columns[0].Offset)
	require.Equal(t, "_col$_c1", tbl.Meta().Columns[1].Name.O)
	require.Equal(t, mysql.TypeTiny, tbl.Meta().Columns[1].GetType())
	require.Equal(t, 1, tbl.Meta().Columns[1].Offset)
	require.Equal(t, "_col$__col$_c1_0", tbl.Meta().Columns[2].Name.O)
	require.Equal(t, mysql.TypeTiny, tbl.Meta().Columns[2].GetType())
	require.Equal(t, 2, tbl.Meta().Columns[2].Offset)
	require.Equal(t, "_col$__col$__col$_c1_0_0", tbl.Meta().Columns[3].Name.O)
	require.Equal(t, mysql.TypeTiny, tbl.Meta().Columns[3].GetType())
	require.Equal(t, 3, tbl.Meta().Columns[3].Offset)

	tk.MustExec("drop table if exists t")
}

func TestWriteReorgForColumnTypeChangeOnAmendTxn(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_amend_pessimistic_txn = ON")
	defer tk.MustExec("set global tidb_enable_amend_pessimistic_txn = OFF")

	d := dom.DDL()
	testInsertOnModifyColumn := func(sql string, startColState, commitColState model.SchemaState, retStrs []string, retErr error) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1 (c1 int, c2 int, c3 int, unique key(c1))")
		tk.MustExec("insert into t1 values (20, 20, 20);")

		var checkErr error
		tk1 := testkit.NewTestKit(t, store)
		defer func() {
			if tk1.Session() != nil {
				tk1.Session().Close()
			}
		}()
		hook := &ddl.TestDDLCallback{Do: dom}
		times := 0
		hook.OnJobRunBeforeExported = func(job *model.Job) {
			if job.Type != model.ActionModifyColumn || checkErr != nil || job.SchemaState != startColState {
				return
			}

			tk1.MustExec("use test")
			tk1.MustExec("begin pessimistic;")
			tk1.MustExec("insert into t1 values(101, 102, 103)")
		}
		hook.OnJobUpdatedExported = func(job *model.Job) {
			if job.Type != model.ActionModifyColumn || checkErr != nil || job.SchemaState != commitColState {
				return
			}
			if times == 0 {
				_, checkErr = tk1.Exec("commit;")
			}
			times++
		}
		d.SetHook(hook)

		tk.MustExec(sql)
		if retErr == nil {
			require.NoError(t, checkErr)
		} else {
			require.Error(t, checkErr)
			require.Contains(t, checkErr.Error(), retErr.Error())
		}
		tk.MustQuery("select * from t1").Check(testkit.Rows(retStrs...))
		tk.MustExec("admin check table t1")
	}

	// Testing it needs reorg data.
	ddlStatement := "alter table t1 change column c2 cc smallint;"
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateDeleteOnly, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StateWriteReorganization, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateDeleteOnly, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StatePublic, []string{"20 20 20"}, domain.ErrInfoSchemaChanged)

	// Testing it needs not reorg data. This case only have two states: none, public.
	ddlStatement = "alter table t1 change column c2 cc bigint;"
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StateWriteReorganization, []string{"20 20 20"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StateWriteReorganization, []string{"20 20 20"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateNone, model.StatePublic, []string{"20 20 20", "101 102 103"}, nil)
	testInsertOnModifyColumn(ddlStatement, model.StateWriteOnly, model.StatePublic, []string{"20 20 20"}, nil)
}
