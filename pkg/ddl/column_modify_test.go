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
	"testing"
	"time"

	"github.com/pingcap/errors"
	testddlutil "github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

const columnModifyLease = 600 * time.Millisecond

func TestAddAndDropColumn(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t2 (c1 int, c2 int, c3 int)")

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

	tk.MustQuery("select count(c4) from t2 where c4 = -1").Check([][]any{
		{fmt.Sprintf("%v", count-int64(step))},
	})

	for i := num; i < num+step; i++ {
		tk.MustQuery("select c4 from t2 where c4 = ?", i).Check([][]any{
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
			v, err := data[3].ToInt64(tk.Session().GetSessionVars().StmtCtx.TypeCtx())
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
	tk.MustQuery("select count(c4) from t2 where c4 = -1").Check([][]any{
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
	store := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)

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
	require.EqualError(t, err, "[ddl:3855]Column 'a' has a partitioning function dependency and cannot be dropped or renamed")
}

func TestChangeColumn(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease, mockstore.WithDDLChecker())

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

func TestVirtualColumnDDL(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, columnModifyLease)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create global temporary table test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored) on commit delete rows;`)
	is := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
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
	is = sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
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

func TestTransactionWithWriteOnlyColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)
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

	hook := &callback.TestDDLCallback{Do: dom}
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
	go backgroundExec(store, "test", "alter table t1 add column c int not null", done)
	err := <-done
	require.NoError(t, err)
	require.NoError(t, checkErr)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
	tk.MustExec("delete from t1")

	// test transaction on drop column.
	go backgroundExec(store, "test", "alter table t1 drop column c", done)
	err = <-done
	require.NoError(t, err)
	require.NoError(t, checkErr)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
}

// For issue #31735.
func TestAddGeneratedColumnAndInsert(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, unique kye(a))")
	tk.MustExec("insert into t1 value (1), (10)")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	d := dom.DDL()
	hook := &callback.TestDDLCallback{Do: dom}
	ctx := mock.NewContext()
	ctx.Store = store
	times := 0
	var checkErr error
	onJobUpdatedExportedFunc := func(job *model.Job) {
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
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	d.SetHook(hook)

	tk.MustExec("alter table t1 add column gc int as ((a+1))")
	tk.MustQuery("select * from t1 order by a").Check(testkit.Rows("4 5", "10 11"))
	require.NoError(t, checkErr)
}

func TestColumnTypeChangeGenUniqueChangingName(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, columnModifyLease)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	hook := &callback.TestDDLCallback{}
	var checkErr error
	assertChangingColName := "_col$_c2_0"
	assertChangingIdxName := "_idx$_idx_0"
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				_newCol                *model.ColumnInfo
				_oldColName            *model.CIStr
				_pos                   = &ast.ColumnPosition{}
				_modifyColumnTp        byte
				_updatedAutoRandomBits uint64
				changingCol            *model.ColumnInfo
				changingIdxs           []*model.IndexInfo
			)

			err := job.DecodeArgs(&_newCol, &_oldColName, _pos, &_modifyColumnTp, &_updatedAutoRandomBits, &changingCol, &changingIdxs)
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
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
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
	onJobUpdatedExportedFunc2 := func(job *model.Job) {
		if (job.Query == query1 || job.Query == query2) && job.SchemaState == model.StateDeleteOnly && job.Type == model.ActionModifyColumn {
			var (
				_newCol                *model.ColumnInfo
				_oldColName            *model.CIStr
				_pos                   = &ast.ColumnPosition{}
				_modifyColumnTp        byte
				_updatedAutoRandomBits uint64
				changingCol            *model.ColumnInfo
				changingIdxs           []*model.IndexInfo
			)
			err := job.DecodeArgs(&_newCol, &_oldColName, _pos, &_modifyColumnTp, &_updatedAutoRandomBits, &changingCol, &changingIdxs)
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
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc2)
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
