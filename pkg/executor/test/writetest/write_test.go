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

package writetest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(t, kv.NewInjectedStore(store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (id int PRIMARY KEY AUTO_INCREMENT, c1 int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 2);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())

	r := tk.MustQuery("select * from t;")
	rowStr := fmt.Sprintf("%v %v", "1", "2")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("insert ignore into t values (1, 3), (2, 3)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v %v", "2", "3")
	r.Check(testkit.Rows(rowStr, rowStr1))

	tk.MustExec("insert ignore into t values (3, 4), (3, 4)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr2 := fmt.Sprintf("%v %v", "3", "4")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2))

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values (4, 4), (4, 5), (4, 6)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 3  Duplicates: 2  Warnings: 2")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v %v", "4", "5")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")

	cfg.SetGetError(errors.New("foo"))
	err := tk.ExecToErr("insert ignore into t values (1, 3)")
	require.Error(t, err)
	cfg.SetGetError(nil)

	// for issue 4268
	testSQL = `drop table if exists t;
	create table t (a bigint);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t select '1a';"
	err = tk.ExecToErr(testSQL)
	require.NoError(t, err)
	require.Equal(t, tk.Session().LastMessage(), "Records: 1  Duplicates: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: '1a'"))
	testSQL = "insert ignore into t values ('1a')"
	err = tk.ExecToErr(testSQL)
	require.NoError(t, err)
	require.Empty(t, tk.Session().LastMessage())
	r = tk.MustQuery("SHOW WARNINGS")
	// TODO: MySQL8.0 reports Warning 1265 Data truncated for column 'a' at row 1
	r.Check(testkit.Rows("Warning 1366 Incorrect bigint value: '1a' for column 'a' at row 1"))

	// for duplicates with warning
	testSQL = `drop table if exists t;
	create table t(a int primary key, b int);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t values (1,1);"
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	err = tk.ExecToErr(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	require.NoError(t, err)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 't.PRIMARY'"))

	testSQL = `drop table if exists test;
create table test (i int primary key, j int unique);
begin;
insert into test values (1,1);
insert ignore into test values (2,1);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1"))

	testSQL = `delete from test;
insert into test values (1, 1);
begin;
delete from test where i = 1;
insert ignore into test values (2, 1);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("2 1"))

	testSQL = `delete from test;
insert into test values (1, 1);
begin;
update test set i = 2, j = 2 where i = 1;
insert ignore into test values (1, 3);
insert ignore into test values (2, 4);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test order by i;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 3", "2 2"))

	testSQL = `create table badnull (i int not null)`
	tk.MustExec(testSQL)
	testSQL = `insert ignore into badnull values (null)`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	testSQL = `select * from badnull`
	tk.MustQuery(testSQL).Check(testkit.Rows("0"))

	tk.MustExec("create table tp (id int) partition by range (id) (partition p0 values less than (1), partition p1 values less than(2))")
	tk.MustExec("insert ignore into tp values (1), (3)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1526 Table has no partition for value 3"))
}

type testCase struct {
	data        []byte
	expected    []string
	expectedMsg string
}

func checkCases(
	tests []testCase,
	loadSQL string,
	t *testing.T,
	tk *testkit.TestKit,
	ctx sessionctx.Context,
	selectSQL, deleteSQL string,
) {
	for _, tt := range tests {
		var reader io.ReadCloser = mydump.NewStringReader(string(tt.data))
		var readerBuilder executor.LoadDataReaderBuilder = func(_ string) (
			r io.ReadCloser, err error,
		) {
			return reader, nil
		}

		ctx.SetValue(executor.LoadDataReaderBuilderKey, readerBuilder)
		tk.MustExec(loadSQL)
		warnings := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		for _, w := range warnings {
			fmt.Printf("warnnig: %#v\n", w.Err.Error())
		}
		require.Equal(t, tt.expectedMsg, tk.Session().LastMessage(), tt.expected)
		tk.MustQuery(selectSQL).Check(testkit.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func TestLoadDataMissingColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createSQL := `create table load_data_missing (id int, t timestamp not null)`
	tk.MustExec(createSQL)
	loadSQL := "load data local infile '/tmp/nonexistence.csv' ignore into table load_data_missing"
	ctx := tk.Session().(sessionctx.Context)

	deleteSQL := "delete from load_data_missing"
	selectSQL := "select id, hour(t), minute(t) from load_data_missing;"

	curTime := types.CurrentTime(mysql.TypeTimestamp)
	timeHour := curTime.Hour()
	timeMinute := curTime.Minute()
	tests := []testCase{
		{[]byte(""), nil, "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("12\n"), []string{fmt.Sprintf("12|%v|%v", timeHour, timeMinute)}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)

	tk.MustExec("alter table load_data_missing add column t2 timestamp null")
	curTime = types.CurrentTime(mysql.TypeTimestamp)
	timeHour = curTime.Hour()
	timeMinute = curTime.Minute()
	selectSQL = "select id, hour(t), minute(t), t2 from load_data_missing;"
	tests = []testCase{
		{[]byte("12\n"), []string{fmt.Sprintf("12|%v|%v|<nil>", timeHour, timeMinute)}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
}

func TestIssue18681(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createSQL := `drop table if exists load_data_test;
		create table load_data_test (a bit(1),b bit(1),c bit(1),d bit(1));`
	tk.MustExec(createSQL)
	loadSQL := "load data local infile '/tmp/nonexistence.csv' ignore into table load_data_test"
	ctx := tk.Session().(sessionctx.Context)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select bin(a), bin(b), bin(c), bin(d) from load_data_test;"
	levels := ctx.GetSessionVars().StmtCtx.ErrLevels()
	levels[errctx.ErrGroupDupKey] = errctx.LevelWarn
	levels[errctx.ErrGroupBadNull] = errctx.LevelWarn

	sc := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := sc.TypeFlags()
	defer func() {
		sc.SetTypeFlags(oldTypeFlags)
	}()
	sc.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))
	tests := []testCase{
		{[]byte("true\tfalse\t0\t1\n"), []string{"1|0|0|1"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, loadSQL, t, tk, ctx, selectSQL, deleteSQL)
	require.Equal(t, uint16(0), sc.WarningCount())
}

func TestIssue34358(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists load_data_test")
	tk.MustExec("create table load_data_test (a varchar(10), b varchar(10))")

	loadSQL := "load data local infile '/tmp/nonexistence.csv' into table load_data_test ( @v1, " +
		"@v2 ) set a = @v1, b = @v2"
	checkCases([]testCase{
		{[]byte("\\N\n"), []string{"<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}, loadSQL, t, tk, ctx, "select * from load_data_test", "delete from load_data_test",
	)
}

func TestLatch(t *testing.T) {
	store, err := mockstore.NewMockStore(
		// Small latch slot size to make conflicts.
		mockstore.WithTxnLocalLatches(64),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	dom, err1 := session.BootstrapSession(store)
	require.Nil(t, err1)
	defer dom.Close()

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t (id int)")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = true")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = true")

	fn := func() {
		tk1.MustExec("begin")
		for i := 0; i < 100; i++ {
			tk1.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}
		tk2.MustExec("begin")
		for i := 100; i < 200; i++ {
			tk1.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}
		tk2.MustExec("commit")
	}

	// txn1 and txn2 data range do not overlap, using latches should not
	// result in txn conflict.
	fn()
	tk1.MustExec("commit")

	tk1.MustExec("truncate table t")
	fn()
	tk1.MustExec("commit")

	// Test the error type of latch and it could be retry if TiDB enable the retry.
	tk1.MustExec("begin")
	tk1.MustExec("update t set id = id + 1")
	tk2.MustExec("update t set id = id + 1")
	tk1.MustGetDBError("commit", kv.ErrWriteConflictInTiDB)
}

func TestReplaceLog(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table testLog (a int not null primary key, b int unique key);`)

	// Make some dangling index.
	ctx := mock.NewContext()
	ctx.Store = store
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("testLog")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("b")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx.GetTableCtx(), txn, types.MakeDatums(1), kv.IntHandle(1), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr(`replace into testLog values (0, 0), (1, 1);`)
	require.Error(t, err)
	require.EqualError(t, err, `can not be duplicated row, due to old row not found. handle 1 not found`)
	tk.MustQuery(`admin cleanup index testLog b;`).Check(testkit.Rows("1"))
}

// TestRebaseIfNeeded is for issue 7422.
// There is no need to do the rebase when updating a record if the auto-increment ID not changed.
// This could make the auto ID increasing speed slower.
func TestRebaseIfNeeded(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int not null primary key auto_increment, b int unique key);`)
	tk.MustExec(`insert into t (b) values (1);`)

	ctx := mock.NewContext()
	ctx.Store = store
	tbl, err := domain.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(context.Background(), ctx))
	// AddRecord directly here will skip to rebase the auto ID in the insert statement,
	// which could simulate another TiDB adds a large auto ID.
	_, err = tbl.AddRecord(ctx.GetTableCtx(), types.MakeDatums(30001, 2))
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(context.Background()))

	tk.MustExec(`update t set b = 3 where a = 30001;`)
	tk.MustExec(`insert into t (b) values (4);`)
	tk.MustQuery(`select a from t where b = 4;`).Check(testkit.Rows("2"))

	tk.MustExec(`insert into t set b = 3 on duplicate key update a = a;`)
	tk.MustExec(`insert into t (b) values (5);`)
	tk.MustQuery(`select a from t where b = 5;`).Check(testkit.Rows("4"))

	tk.MustExec(`insert into t set b = 3 on duplicate key update a = a + 1;`)
	tk.MustExec(`insert into t (b) values (6);`)
	tk.MustQuery(`select a from t where b = 6;`).Check(testkit.Rows("30003"))
}

func TestDeferConstraintCheckForInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`drop table if exists t;create table t (a int primary key, b int);`)
	tk.MustExec(`insert into t values (1,2),(2,2)`)
	err := tk.ExecToErr("update t set a=a+1 where b=2")
	require.Error(t, err)

	tk.MustExec(`drop table if exists t;create table t (i int key);`)
	tk.MustExec(`insert t values (1);`)
	tk.MustExec(`set tidb_constraint_check_in_place = 1;`)
	tk.MustExec(`begin;`)
	err = tk.ExecToErr(`insert t values (1);`)
	require.Error(t, err)
	tk.MustExec(`update t set i = 2 where i = 1;`)
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows("2"))

	tk.MustExec(`set tidb_constraint_check_in_place = 0;`)
	tk.MustExec("replace into t values (1),(2)")
	tk.MustExec("begin")
	err = tk.ExecToErr("update t set i = 2 where i = 1")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t values (1) on duplicate key update i = i + 1")
	require.Error(t, err)
	tk.MustExec("rollback")

	tk.MustExec(`drop table t; create table t (id int primary key, v int unique);`)
	tk.MustExec(`insert into t values (1, 1)`)
	tk.MustExec(`set tidb_constraint_check_in_place = 1;`)
	tk.MustExec(`set @@autocommit = 0;`)

	err = tk.ExecToErr("insert into t values (3, 1)")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t values (1, 3)")
	require.Error(t, err)
	tk.MustExec("commit")

	tk.MustExec(`set tidb_constraint_check_in_place = 0;`)
	tk.MustExec("insert into t values (3, 1)")
	tk.MustExec("insert into t values (1, 3)")
	err = tk.ExecToErr("commit")
	require.Error(t, err)

	// Cover the temporary table.
	for val := range []int{0, 1} {
		tk.MustExec("set tidb_constraint_check_in_place = ?", val)

		tk.MustExec("drop table t")
		tk.MustExec("create global temporary table t (a int primary key, b int) on commit delete rows")
		tk.MustExec("begin")
		tk.MustExec("insert into t values (1, 1)")
		err = tk.ExecToErr(`insert into t values (1, 3)`)
		require.Error(t, err)
		tk.MustExec("insert into t values (2, 2)")
		err = tk.ExecToErr("update t set a = a + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into t values (1, 3) on duplicated key update a = a + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		tk.MustExec("drop table t")
		tk.MustExec("create global temporary table t (a int, b int unique) on commit delete rows")
		tk.MustExec("begin")
		tk.MustExec("insert into t values (1, 1)")
		err = tk.ExecToErr(`insert into t values (3, 1)`)
		require.Error(t, err)
		tk.MustExec("insert into t values (2, 2)")
		err = tk.ExecToErr("update t set b = b + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into t values (3, 1) on duplicated key update b = b + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		// cases for temporary table
		tk.MustExec("drop table if exists tl")
		tk.MustExec("create temporary table tl (a int primary key, b int)")
		tk.MustExec("begin")
		tk.MustExec("insert into tl values (1, 1)")
		err = tk.ExecToErr(`insert into tl values (1, 3)`)
		require.Error(t, err)
		tk.MustExec("insert into tl values (2, 2)")
		err = tk.ExecToErr("update tl set a = a + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (1, 3) on duplicated key update a = a + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		tk.MustExec("begin")
		tk.MustQuery("select * from tl").Check(testkit.Rows("1 1", "2 2"))
		err = tk.ExecToErr(`insert into tl values (1, 3)`)
		require.Error(t, err)
		err = tk.ExecToErr("update tl set a = a + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (1, 3) on duplicated key update a = a + 1")
		require.Error(t, err)
		tk.MustExec("rollback")

		tk.MustExec("drop table tl")
		tk.MustExec("create temporary table tl (a int, b int unique)")
		tk.MustExec("begin")
		tk.MustExec("insert into tl values (1, 1)")
		err = tk.ExecToErr(`insert into tl values (3, 1)`)
		require.Error(t, err)
		tk.MustExec("insert into tl values (2, 2)")
		err = tk.ExecToErr("update tl set b = b + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (3, 1) on duplicated key update b = b + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		tk.MustExec("begin")
		tk.MustQuery("select * from tl").Check(testkit.Rows("1 1", "2 2"))
		err = tk.ExecToErr(`insert into tl values (3, 1)`)
		require.Error(t, err)
		err = tk.ExecToErr("update tl set b = b + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (3, 1) on duplicated key update b = b + 1")
		require.Error(t, err)
		tk.MustExec("rollback")
	}
}

func TestPessimisticDeleteYourWrites(t *testing.T) {
	store := testkit.CreateMockStore(t)

	session1 := testkit.NewTestKit(t, store)
	session1.MustExec("use test")
	session2 := testkit.NewTestKit(t, store)
	session2.MustExec("use test")

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")

	session1.MustExec("set tidb_txn_mode = 'pessimistic'")
	session2.MustExec("set tidb_txn_mode = 'pessimistic'")

	session1.MustExec("begin;")
	session1.MustExec("insert into x select 1, 1")
	session1.MustExec("delete from x where id = 1")
	session2.MustExec("begin;")
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		session2.MustExec("insert into x select 1, 2")
	})
	session1.MustExec("commit;")
	wg.Wait()
	session2.MustExec("commit;")
	session2.MustQuery("select * from x").Check(testkit.Rows("1 2"))
}
