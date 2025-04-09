// Copyright 2025 PingCAP, Inc.
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
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func getClonedTableInfoFromDomain(
	t *testing.T,
	dbName string,
	tableName string,
	dom *domain.Domain,
) *model.TableInfo {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	require.NoError(t, err)
	return tbl.Meta().Clone()
}

func setTableModeTest(
	ctx sessionctx.Context,
	t *testing.T,
	store kv.Storage,
	de ddl.Executor,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	mode model.TableMode,
) error {
	args := &model.AlterTableModeArgs{
		TableMode: mode,
		SchemaID:  dbInfo.ID,
		TableID:   tblInfo.ID,
	}
	err := de.AlterTableMode(ctx, args)
	if err == nil {
		testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)
		checkTableModeTest(t, store, dbInfo, tblInfo, mode)
	}

	return err
}

func checkTableModeTest(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo, mode model.TableMode) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMutator(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, mode, info.Mode)
		return nil
	})
	require.NoError(t, err)
}

func checkErrorCode(t *testing.T, err error, expected int) {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(tErr)
	require.Equal(t, expected, int(sqlErr.Code))
}

func TestTableModeBasic(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)

	ctx := testkit.NewTestKit(t, store).Session()

	// init test
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int, c1 int, c2 int, index idx1(c1))")
	tk.MustExec("create table t2(id int, c1 int, c2 int, index idx1(c1))")
	tk.MustExec("create table t3(id int, pid INT, INDEX idx_pid (pid),FOREIGN KEY fk_1 (pid) REFERENCES t1(c1) ON UPDATE SET NULL)")

	// For testing create foreign key table as ModeImport
	tblInfo := getClonedTableInfoFromDomain(t, "test", "t3", domain)
	tblInfo.Name = ast.NewCIStr("t1_foreign_key")
	tblInfo.Mode = model.TableModeImport
	err := de.CreateTableWithInfo(tk.Session(), ast.NewCIStr("test"), tblInfo, nil, ddl.WithOnExist(ddl.OnExistIgnore))
	require.NoError(t, err)
	dbInfo, ok := domain.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	checkTableModeTest(t, store, dbInfo, tblInfo, model.TableModeImport)
	// not allow delete foreign key constraint
	// TODO: need to verify DML(update/delete) on foreign key father table
	tk.MustGetErrCode("ALTER TABLE t1_foreign_key DROP FOREIGN KEY fk_1", errno.ErrProtectedTableMode)

	// For testing create table as ModeRestore
	tblInfo = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo.Name = ast.NewCIStr("t1_restore_import")
	tblInfo.Mode = model.TableModeRestore
	err = de.CreateTableWithInfo(tk.Session(), ast.NewCIStr("test"), tblInfo, nil, ddl.WithOnExist(ddl.OnExistIgnore))
	require.NoError(t, err)
	dbInfo, ok = domain.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	checkTableModeTest(t, store, dbInfo, tblInfo, model.TableModeRestore)

	// For testing accessing table metadata is allowed when table is in ModeRestore
	tk.MustExec("show create table t1_restore_import")
	tk.MustExec("show table status where Name = 't1_restore_import'")
	tk.MustExec("show columns from t1_restore_import")
	tk.MustExec("show create table t1_restore_import")
	tk.MustExec("show table status where Name = 't1_restore_import'")
	tk.MustExec("show index from t1_restore_import")
	tk.MustExec("describe t1_restore_import")
	tk.MustExec("create table t1_restore_import_2 like t1_restore_import")
	tk.MustExec("create view t1_restore_import_view as select * from t1_restore_import")
	tk.MustExec("create table foreign_key_child(id int, pid INT, INDEX idx_pid (pid),FOREIGN KEY (pid) REFERENCES t1_restore_import(c1) ON DELETE CASCADE)")
	tk.MustExec("drop table foreign_key_child")

	// For testing below stmt is not allowed when table is in ModeImport/ModeRestore
	// DMLs
	tk.MustGetErrCode("select * from t1_restore_import", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("explain select * from t1_restore_import", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("desc select * from t1_restore_import", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("insert into t1_restore_import values(1, 1, 1)", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("replace into t1_restore_import values(1,1,1)", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("update t1_restore_import set id = 2 where id = 1", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("delete from t1_restore_import where id = 2", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("truncate table t1_restore_import", errno.ErrProtectedTableMode)
	// DDLs
	tk.MustGetErrCode("drop table t1_restore_import", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import rename to t1_new", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("rename table t1_restore_import to t1_new", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("rename table t1_restore_import to t1_new, t2 to t2_new", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import modify column c2 bigint", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import add column c3 int", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import drop column c2", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import drop index idx1", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import add index idx2(c2)", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import partition by range(id) (partition p0 values less than (100))", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import comment='new comment'", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import convert to character set utf8mb4", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import rename column c1 to c1_new", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import alter column c1 set default 100", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("alter table t1_restore_import add foreign key fk_1 (c2) REFERENCES t1(c1) ON UPDATE SET NULL ", errno.ErrProtectedTableMode)
	// Transaction related operations
	tk.MustExec("begin")
	tk.MustGetErrCode("insert into t1_restore_import values(1,1,1)", errno.ErrProtectedTableMode)
	tk.MustExec("rollback")

	// For testing AlterTable ModeRestore -> ModeImport is not allowed
	err = setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeImport)
	require.ErrorContains(t, err, "Invalid mode set from (or by default) Restore to Import for table t1_restore_import")

	// For testing AlterTableMode ModeRestore -> ModeNormal
	err = setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeNormal)
	require.NoError(t, err)

	// For testing AlterTableMode ModeNormal -> ModeRestore
	err = setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeRestore)
	require.NoError(t, err)
	// For testing AlterTableMode ModeRestore -> ModeRestore
	err = setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeRestore)
	require.NoError(t, err)

	// For testing an exist table with ModeImport is not allowed recreate with ModeRestore from BR
	err = setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeNormal)
	require.NoError(t, err)
	err = setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeImport)
	require.NoError(t, err)
	tblInfo.Mode = model.TableModeRestore
	err = de.CreateTableWithInfo(tk.Session(), ast.NewCIStr("test"), tblInfo, nil, ddl.WithOnExist(ddl.OnExistIgnore))
	require.ErrorContains(t, err, "Invalid mode set from (or by default) Import to Restore for table t1_restore_import")

	// For testing batch create tables with info
	var tblInfo1, tblInfo2, tblInfo3 *model.TableInfo
	tblInfo1 = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo1.Name = ast.NewCIStr("t1_1")
	tblInfo1.Mode = model.TableModeNormal
	tblInfo2 = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo2.Name = ast.NewCIStr("t1_2")
	tblInfo2.Mode = model.TableModeImport
	tblInfo3 = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	tblInfo3.Name = ast.NewCIStr("t1_3")
	tblInfo3.Mode = model.TableModeRestore
	err = de.BatchCreateTableWithInfo(
		ctx,
		ast.NewCIStr("test"),
		[]*model.TableInfo{tblInfo1, tblInfo2, tblInfo3},
		ddl.WithOnExist(ddl.OnExistIgnore),
	)
	require.NoError(t, err)
	checkTableModeTest(t, store, dbInfo, tblInfo1, model.TableModeNormal)
	checkTableModeTest(t, store, dbInfo, tblInfo2, model.TableModeImport)
	checkTableModeTest(t, store, dbInfo, tblInfo3, model.TableModeRestore)
}

func TestTableModeConcurrent(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)
	ctx := testkit.NewTestKit(t, store).Session()

	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")
	dbInfo, ok := domain.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)

	// Concurrency test1: concurrently alter t1 to ModeImport, expecting one success, one failure.
	t1Infos := []*model.TableInfo{
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
	}
	var wg sync.WaitGroup
	wg.Add(len(t1Infos))
	errs := make(chan error, len(t1Infos))
	for _, info := range t1Infos {
		go func(info *model.TableInfo) {
			defer wg.Done()
			errs <- setTableModeTest(ctx, t, store, de, dbInfo, info, model.TableModeImport)
		}(info)
	}
	wg.Wait()
	close(errs)
	var successCount int
	var failedErr error
	for e := range errs {
		if e == nil {
			successCount++
		} else {
			failedErr = e
		}
	}
	require.Equal(t, 2, successCount)
	require.Nil(t, failedErr)
	checkTableModeTest(t, store, dbInfo, t1Infos[0], model.TableModeImport)

	// Concurrency test2: concurrently alter t1 to ModeNormal, expecting both success.
	t1NormalInfos := []*model.TableInfo{
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
	}
	var wg2 sync.WaitGroup
	wg2.Add(len(t1NormalInfos))
	errs2 := make(chan error, len(t1NormalInfos))
	for _, info := range t1NormalInfos {
		go func(info *model.TableInfo) {
			defer wg2.Done()

			errs2 <- setTableModeTest(ctx, t, store, de, dbInfo, info, model.TableModeNormal)
		}(info)
	}
	wg2.Wait()
	close(errs2)
	for e := range errs2 {
		require.NoError(t, e)
	}
	checkTableModeTest(t, store, dbInfo, t1NormalInfos[0], model.TableModeNormal)

	// Concurrency test3: concurrently alter t1 to ModeRestore, expecting both success.
	t1Infos = []*model.TableInfo{
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
		getClonedTableInfoFromDomain(t, "test", "t1", domain),
	}
	var wg3 sync.WaitGroup
	wg3.Add(len(t1Infos))
	errs = make(chan error, len(t1Infos))
	for _, info := range t1Infos {
		go func(info *model.TableInfo) {
			defer wg3.Done()
			errs <- setTableModeTest(ctx, t, store, de, dbInfo, info, model.TableModeRestore)
		}(info)
	}
	wg3.Wait()
	close(errs)
	successCount = 0
	failedErr = nil
	for e := range errs {
		if e == nil {
			successCount++
		} else {
			failedErr = e
		}
	}
	require.Equal(t, 2, successCount)
	require.Nil(t, failedErr)
	checkTableModeTest(t, store, dbInfo, t1Infos[0], model.TableModeRestore)

	// Concurrency test4: concurrently alter t1 to ModeRestore and ModeImport, expecting one success, one failure.
	modes := []model.TableMode{
		model.TableModeRestore,
		model.TableModeImport,
	}
	clones := make([]*model.TableInfo, len(modes))
	for i := range modes {
		clones[i] = getClonedTableInfoFromDomain(t, "test", "t1", domain)
	}
	var wg4 sync.WaitGroup
	wg4.Add(len(modes))
	errs = make(chan error, len(modes))
	for i, mode := range modes {
		go func(clone *model.TableInfo, m model.TableMode) {
			defer wg4.Done()
			errs <- setTableModeTest(ctx, t, store, de, dbInfo, clone, m)
		}(clones[i], mode)
	}
	wg4.Wait()
	close(errs)
	var successCount3 int
	var failedErr3 error
	for e := range errs {
		if e == nil {
			successCount3++
		} else {
			failedErr3 = e
		}
	}
	require.Equal(t, 1, successCount3)
	require.NotNil(t, failedErr3)
	checkErrorCode(t, failedErr3, errno.ErrInvalidTableModeSet)
}

func TestTableModeFKTables(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	de := domain.DDLExecutor()
	tk := testkit.NewTestKit(t, store)
	ctx := testkit.NewTestKit(t, store).Session()
	tk.MustExec("use test")
	tk.MustExec(`create table parent_1(id int primary key, name varchar(50), index idx_id(id))`)
	tk.MustExec(`create table parent_2(id int primary key, name varchar(50), index idx_id(id))`)
	tk.MustExec(`create table parent_3(id int primary key, name varchar(50), index idx_id(id))`)
	tk.MustExec(`create table parent_4(id int primary key, name varchar(50), index idx_id(id))`)

	// Create child tables with different FK options, include cascade, set null,
	// restrict, no action and set default options.
	childTables := []string{"child_delete_cascade", "child_update_cascade", "child_delete_set_null",
		"child_update_set_null", "child_delete_restrict", "child_update_no_action", "child_update_set_default"}
	// CASCADE option
	tk.MustExec(`create table child_delete_cascade(id int primary key, parent_id int,
        constraint fk_cascade foreign key (parent_id) references parent_1(id) on delete cascade)`)
	tk.MustExec(`create table child_update_cascade(id int primary key, parent_id int,
        constraint fk_cascade foreign key (parent_id) references parent_2(id) on update cascade)`)
	// SET NULL option
	tk.MustExec(`create table child_delete_set_null(id int primary key, parent_id int,
        constraint fk_cascade foreign key (parent_id) references parent_3(id) on delete SET NULL)`)
	tk.MustExec(`create table child_update_set_null(id int primary key, parent_id int,
        constraint fk_cascade foreign key (parent_id) references parent_4(id) on update SET NULL)`)
	// RESTRICT/NO ACTION/SET DEFAULT option
	tk.MustExec(`create table child_delete_restrict(id int primary key, parent_id int,
		constraint fk_cascade foreign key (parent_id) references parent_2(id) on delete restrict)`)
	tk.MustExec(`create table child_update_no_action(id int primary key, parent_id int,
		constraint fk_cascade foreign key (parent_id) references parent_3(id) on update no action)`)
	tk.MustExec(`create table child_update_set_default(id int primary key, parent_id int,
		constraint fk_cascade foreign key (parent_id) references parent_3(id) on update set default)`)
	// Init test data
	tk.MustExec(`insert into parent_1 values(1, 'parent_1')`)
	tk.MustExec(`insert into child_delete_cascade values(111, 1)`)
	tk.MustExec(`insert into parent_2 values(2, 'parent_2'), (22222, 'parent_22')`)
	tk.MustExec(`insert into child_update_cascade values(222, 2)`)
	tk.MustExec(`insert into child_delete_restrict values(222222, 22222)`)
	tk.MustExec(`insert into parent_3 values(3, 'parent_3'), (33333, 'parent_33'), (333333, 'parent_333')`)
	tk.MustExec(`insert into child_delete_set_null values(333, 3)`)
	tk.MustExec(`insert into child_update_no_action values(333333, 33333)`)
	tk.MustExec(`insert into child_update_set_default values(3333333, 333333)`)
	tk.MustExec(`insert into parent_4 values(4, 'parent_4'),(5, 'parent_5')`)
	tk.MustExec(`insert into child_update_set_null values(444, 4), (555, 5)`)
	//
	dbInfo, ok := domain.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	for _, tbl := range childTables {
		tblInfo := getClonedTableInfoFromDomain(t, "test", tbl, domain)
		setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeImport)
	}

	// Test operations for each reference option type
	// 1. CASCADE
	tk.MustGetErrCode("delete from parent_1 where id = 1", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("update parent_2 set id = 22 where id = 2", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("insert into parent_2 values (2, 'parent_11') on duplicate key update id =22", errno.ErrProtectedTableMode)
	// 2. SET NULL
	tk.MustGetErrCode("delete from parent_3 where id = 3", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("update parent_4 set id = 44 where id = 4", errno.ErrProtectedTableMode)
	tk.MustGetErrCode("insert into parent_4 values (4, 'parent_44') on duplicate key update id =44", errno.ErrProtectedTableMode)

	// set table mode to normal, expect all operations are allowed
	for _, tbl := range childTables {
		tblInfo := getClonedTableInfoFromDomain(t, "test", tbl, domain)
		setTableModeTest(ctx, t, store, de, dbInfo, tblInfo, model.TableModeNormal)
	}
	tk.MustExec("delete from parent_1 where id = 1")
	tk.MustQuery("select * from child_delete_cascade").Check(testkit.Rows())
	tk.MustExec("update parent_2 set id = 22 where id = 2")
	tk.MustQuery("select * from child_update_cascade").Check(testkit.Rows("222 22"))
	tk.MustExec("insert into parent_2 values (22, 'parent_11') on duplicate key update id =222")
	tk.MustQuery("select * from child_update_cascade").Check(testkit.Rows("222 222"))
	tk.MustExec("delete from parent_3 where id = 3")
	tk.MustQuery("select * from child_delete_set_null").Check(testkit.Rows("333 <nil>"))
	tk.MustExec("update parent_4 set id = 44 where id = 4")
	tk.MustQuery("select * from child_update_set_null").Check(testkit.Rows("444 <nil>", "555 5"))
	tk.MustExec("insert into parent_4 values (5, 'parent_55') on duplicate key update id =55")
	tk.MustQuery("select * from child_update_set_null").Check(testkit.Rows("444 <nil>", "555 <nil>"))
}
