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

	testddlutil "github.com/pingcap/tidb/ddl/testutil"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
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

	tbl := tk.GetTableByName("test", "t2")
	i := 0
	j := 0
	require.NoError(t, tk.Session().NewTxn(context.Background()))
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
	require.Equal(t, mysql.TypeTimestamp, colC.Tp)
	require.False(t, mysql.HasNotNullFlag(colC.Flag))
	// add datetime type column
	tk.MustExec("create table test_on_update_d (c1 int, c2 datetime);")
	tk.MustExec("alter table test_on_update_d add column c3 datetime on update current_timestamp;")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_on_update_d"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	require.Equal(t, mysql.TypeDatetime, colC.Tp)
	require.False(t, mysql.HasNotNullFlag(colC.Flag))

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
	require.True(t, mysql.HasNoDefaultValueFlag(colD.Flag))
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	tk.MustExec("alter table t3 change b b varchar(20) null default 'c' comment 'my comment'")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()
	colB := tblInfo.Columns[1]
	require.Equal(t, "my comment", colB.Comment)
	require.False(t, mysql.HasNotNullFlag(colB.Flag))
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
	require.False(t, mysql.HasNotNullFlag(colC.Flag))
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
		cols := tk.GetTableByName("test", tableName).Cols()
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
}
