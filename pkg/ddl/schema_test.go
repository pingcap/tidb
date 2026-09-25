// Copyright 2015 PingCAP, Inc.
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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/testutil"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testCreateTable(t *testing.T, ctx sessionctx.Context, d ddl.ExecutorForTest, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableID:    tblInfo.ID,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	args := &model.CreateTableArgs{TableInfo: tblInfo}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true))
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testCheckTableState(t *testing.T, store kv.Storage, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		info, err := m.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		if state == model.StateNone {
			require.NoError(t, err)
			return nil
		}

		require.Equal(t, info.Name, tblInfo.Name)
		require.Equal(t, info.State, state)
		return nil
	}))
}

// testTableInfo creates a test table with num int columns and with no index.
func testTableInfo(store kv.Storage, name string, num int) (*model.TableInfo, error) {
	tblInfo := &model.TableInfo{
		Name: pmodel.NewCIStr(name),
	}
	genIDs, err := genGlobalIDs(store, 1)

	if err != nil {
		return nil, err
	}
	tblInfo.ID = genIDs[0]

	cols := make([]*model.ColumnInfo, num)
	for i := range cols {
		col := &model.ColumnInfo{
			Name:         pmodel.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset:       i,
			DefaultValue: i + 1,
			State:        model.StatePublic,
		}

		col.FieldType = *types.NewFieldType(mysql.TypeLong)
		tblInfo.MaxColumnID++
		col.ID = tblInfo.MaxColumnID
		cols[i] = col
	}
	tblInfo.Columns = cols
	tblInfo.Charset = "utf8"
	tblInfo.Collate = "utf8_bin"
	return tblInfo, nil
}

func genGlobalIDs(store kv.Storage, count int) ([]int64, error) {
	var ret []int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var err error
		ret, err = m.GenGlobalIDs(count)
		return err
	})
	return ret, err
}

func testSchemaInfo(store kv.Storage, name string) (*model.DBInfo, error) {
	dbInfo := &model.DBInfo{
		Name: pmodel.NewCIStr(name),
	}

	genIDs, err := genGlobalIDs(store, 1)
	if err != nil {
		return nil, err
	}
	dbInfo.ID = genIDs[0]
	return dbInfo, nil
}

func testCreateSchema(t *testing.T, ctx sessionctx.Context, d ddl.ExecutorForTest, dbInfo *model.DBInfo) *model.Job {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: dbInfo.Name.L,
			Table:    model.InvolvingAll,
		}},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.CreateSchemaArgs{DBInfo: dbInfo}, true)))

	v := getSchemaVer(t, ctx)
	dbInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo})
	dbInfo.State = model.StateNone
	return job
}

func buildDropSchemaJob(dbInfo *model.DBInfo) *model.Job {
	j := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: dbInfo.Name.L,
			Table:    model.InvolvingAll,
		}},
	}
	return j
}

func testDropSchema(t *testing.T, ctx sessionctx.Context, d ddl.ExecutorForTest, dbInfo *model.DBInfo) (*model.Job, int64) {
	job := buildDropSchemaJob(dbInfo)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.DropSchemaArgs{FKCheck: true}, true))
	require.NoError(t, err)
	ver := getSchemaVer(t, ctx)
	return job, ver
}

func isDDLJobDone(test *testing.T, t *meta.Mutator, store kv.Storage) bool {
	tk := testkit.NewTestKit(test, store)
	rows := tk.MustQuery("select * from mysql.tidb_ddl_job").Rows()

	if len(rows) == 0 {
		return true
	}
	time.Sleep(testLease)
	return false
}

func testCheckSchemaState(test *testing.T, store kv.Storage, dbInfo *model.DBInfo, state model.SchemaState) {
	isDropped := true

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	for {
		err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
			t := meta.NewMutator(txn)
			info, err := t.GetDatabase(dbInfo.ID)
			require.NoError(test, err)

			if state == model.StateNone {
				isDropped = isDDLJobDone(test, t, store)
				if !isDropped {
					return nil
				}
				require.Nil(test, info)
				return nil
			}

			require.Equal(test, info.Name, dbInfo.Name)
			require.Equal(test, info.State, state)
			return nil
		})
		require.NoError(test, err)

		if isDropped {
			break
		}
	}
}

func TestSchema(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	dbInfo, err := testSchemaInfo(store, "test_schema")
	require.NoError(t, err)

	// create a database.
	tk := testkit.NewTestKit(t, store)
	de := domain.DDLExecutor().(ddl.ExecutorForTest)
	job := testCreateSchema(t, tk.Session(), de, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	/*** to drop the schema with two tables. ***/
	// create table t with 100 records.
	tblInfo1, err := testTableInfo(store, "t", 3)
	require.NoError(t, err)
	tJob1 := testCreateTable(t, tk.Session(), de, dbInfo, tblInfo1)
	testCheckTableState(t, store, dbInfo, tblInfo1, model.StatePublic)
	testCheckJobDone(t, store, tJob1.ID, true)
	tbl1 := testGetTable(t, domain, tblInfo1.ID)
	txn, err := newTxn(tk.Session())
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		_, err := tbl1.AddRecord(tk.Session().GetTableCtx(), txn, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	// create table t1 with 1034 records.
	tblInfo2, err := testTableInfo(store, "t1", 3)
	require.NoError(t, err)
	tk2 := testkit.NewTestKit(t, store)
	tJob2 := testCreateTable(t, tk2.Session(), de, dbInfo, tblInfo2)
	testCheckTableState(t, store, dbInfo, tblInfo2, model.StatePublic)
	testCheckJobDone(t, store, tJob2.ID, true)
	tbl2 := testGetTable(t, domain, tblInfo2.ID)
	txn, err = newTxn(tk.Session())
	require.NoError(t, err)
	for i := 1; i <= 1034; i++ {
		_, err := tbl2.AddRecord(tk2.Session().GetTableCtx(), txn, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	tk3 := testkit.NewTestKit(t, store)
	job, v := testDropSchema(t, tk3.Session(), de, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StateNone)
	ids := make(map[int64]struct{})
	ids[tblInfo1.ID] = struct{}{}
	ids[tblInfo2.ID] = struct{}{}
	checkHistoryJobArgs(t, tk3.Session(), job.ID, &historyJobArgs{ver: v, db: dbInfo, tblIDs: ids})

	// Drop a non-existent database.
	job = &model.Job{
		Version:    model.JobVersion1,
		SchemaID:   dbInfo.ID,
		SchemaName: "test_schema",
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx := testkit.NewTestKit(t, store).Session()
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = de.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, &model.DropSchemaArgs{}, true))
	require.True(t, terror.ErrorEqual(err, infoschema.ErrDatabaseDropExists), "err %v", err)

	// Drop a database without a table.
	dbInfo1, err := testSchemaInfo(store, "test1")
	require.NoError(t, err)
	job = testCreateSchema(t, ctx, de, dbInfo1)
	testCheckSchemaState(t, store, dbInfo1, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	job, _ = testDropSchema(t, ctx, de, dbInfo1)
	testCheckSchemaState(t, store, dbInfo1, model.StateNone)
	testCheckJobDone(t, store, job.ID, false)
}

func TestSchemaWaitJob(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	require.True(t, domain.DDL().OwnerManager().IsOwner())

	d2, de2 := ddl.NewDDL(context.Background(),
		ddl.WithEtcdClient(domain.EtcdClient()),
		ddl.WithStore(store),
		ddl.WithInfoCache(domain.InfoCache()),
		ddl.WithLease(testLease),
		ddl.WithSchemaLoader(domain),
	)
	det2 := de2.(ddl.ExecutorForTest)
	err := d2.Start(ddl.Normal, pools.NewResourcePool(func() (pools.Resource, error) {
		session := testkit.NewTestKit(t, store).Session()
		session.GetSessionVars().CommonGlobalLoaded = true
		return session, nil
	}, 20, 20, 5))
	require.NoError(t, err)
	defer func() {
		err := d2.Stop()
		require.NoError(t, err)
	}()

	// d2 must not be owner.
	d2.OwnerManager().RetireOwner()
	// wait one-second makes d2 stop pick up jobs.
	time.Sleep(1 * time.Second)

	dbInfo, err := testSchemaInfo(store, "test_schema")
	require.NoError(t, err)
	se := testkit.NewTestKit(t, store).Session()
	testCreateSchema(t, se, det2, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StatePublic)

	// d2 must not be owner.
	require.False(t, d2.OwnerManager().IsOwner())

	genIDs, err := genGlobalIDs(store, 1)
	require.NoError(t, err)
	schemaID := genIDs[0]
	doDDLJobErr(t, schemaID, 0, "test_schema", "", model.ActionCreateSchema,
		testkit.NewTestKit(t, store).Session(), det2, store, func(job *model.Job) model.JobArgs {
			return &model.CreateSchemaArgs{DBInfo: dbInfo}
		})
}

func doDDLJobErr(
	t *testing.T,
	schemaID, tableID int64,
	schemaName, tableName string,
	tp model.ActionType,
	ctx sessionctx.Context,
	d ddl.ExecutorForTest,
	store kv.Storage,
	handler func(job *model.Job) model.JobArgs,
) *model.Job {
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   schemaID,
		SchemaName: schemaName,
		TableID:    tableID,
		TableName:  tableName,
		Type:       tp,
		BinlogInfo: &model.HistoryInfo{},
	}
	args := handler(job)
	// TODO: check error detail
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.Error(t, d.DoDDLJobWrapper(ctx, ddl.NewJobWrapperWithArgs(job, args, true)))
	testCheckJobCancelled(t, store, job, nil)

	return job
}

func testCheckJobCancelled(t *testing.T, store kv.Storage, job *model.Job, state *model.SchemaState) {
	se := testkit.NewTestKit(t, store).Session()
	historyJob, err := ddl.GetHistoryJobByID(se, job.ID)
	require.NoError(t, err)
	require.True(t, historyJob.IsCancelled() || historyJob.IsRollbackDone(), "history job %s", historyJob)
	if state != nil {
		require.Equal(t, historyJob.SchemaState, *state)
	}
}

func TestRenameTableAutoIDs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
	tk4 := testkit.NewTestKit(t, store)
	dbName := "RenameTableAutoIDs"
	tk1.MustExec(`create schema ` + dbName)
	tk1.MustExec(`create schema ` + dbName + "2")
	tk1.MustExec(`use ` + dbName)
	tk2.MustExec(`use ` + dbName)
	tk3.MustExec(`use ` + dbName)
	tk1.MustExec(`CREATE TABLE t (a int auto_increment primary key nonclustered, b varchar(255), key (b)) AUTO_ID_CACHE 100`)
	tk1.MustExec(`insert into t values (11,11),(2,2),(null,12)`)
	tk1.MustExec(`insert into t values (null,18)`)
	tk1.MustQuery(`select _tidb_rowid, a, b from t`).Sort().Check(testkit.Rows("13 11 11", "14 2 2", "15 12 12", "17 16 18"))

	waitFor := func(col int, tableName, s string) {
		for {
			sql := `admin show ddl jobs where db_name like '` + strings.ToLower(dbName) + `%' and table_name like '` + tableName + `%' and job_type = 'rename table'`
			res := tk4.MustQuery(sql).Rows()
			if len(res) == 1 && res[0][col] == s {
				break
			}

			logutil.DDLLogger().Info("Could not find match", zap.String("tableName", tableName), zap.String("s", s), zap.Int("colNum", col))

			for i := range res {
				strs := make([]string, 0, len(res[i]))
				for j := range res[i] {
					strs = append(strs, res[i][j].(string))
				}
				logutil.DDLLogger().Info("ddl jobs", zap.Strings("jobs", strs))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	alterChan := make(chan error)
	tk2.MustExec(`set @@session.innodb_lock_wait_timeout = 0`)
	tk2.MustExec(`BEGIN`)
	tk2.MustExec(`insert into t values (null, 4)`)

	v1 := dom.InfoSchema().SchemaMetaVersion()

	go func() {
		alterChan <- tk1.ExecToErr(`rename table t to ` + dbName + `2.t2`)
	}()
	waitFor(11, "t", "running")
	waitFor(4, "t", "public")

	// ddl finish does not mean the infoschema loaded.
	// when infoschema v1->v2 switch, it take more time, so we must wait to ensure
	// the new infoschema is used.
	require.Eventually(t, func() bool { return dom.InfoSchema().SchemaMetaVersion() > v1 }, time.Minute, 2*time.Millisecond)

	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into ` + dbName + `2.t2 values (50, 5)`)
	// TODO: still unstable here.
	// This is caused by a known rename table and autoid compatibility issue.
	// In the past we try to fix it by the same auto id allocator before and after table renames.
	//     https://github.com/pingcap/tidb/pull/47892
	// But during infoschema v1->v2 switch, infoschema full load happen, then both the old and new
	// autoid instance exists. tk2 here use the old autoid allocator, cause txn conflict on index key
	// b=20, conflicting with the next line insert values (20, 5)
	tk2.MustExec(`insert into t values (null, 6)`)
	tk3.MustExec(`insert into ` + dbName + `2.t2 values (20, 5)`)
	// Done: Fix https://github.com/pingcap/tidb/issues/46904
	tk2.MustExec(`insert into t values (null, 6)`)
	tk3.MustExec(`insert into ` + dbName + `2.t2 values (null, 7)`)
	tk2.MustExec(`COMMIT`)

	waitFor(11, "t", "done")
	tk2.MustExec(`BEGIN`)
	tk2.MustExec(`insert into ` + dbName + `2.t2 values (null, 8)`)

	tk3.MustExec(`insert into ` + dbName + `2.t2 values (null, 9)`)
	tk2.MustExec(`insert into ` + dbName + `2.t2 values (null, 10)`)
	tk3.MustExec(`COMMIT`)

	waitFor(11, "t", "synced")
	tk2.MustExec(`COMMIT`)
	tk3.MustQuery(`select _tidb_rowid, a, b from ` + dbName + `2.t2`).Sort().Check(testkit.Rows(""+
		"13 11 11",
		"14 2 2",
		"15 12 12",
		"17 16 18",
		"19 18 4",
		"51 50 5",
		"53 52 6",
		"54 20 5",
		"56 55 6",
		"58 57 7",
		"60 59 8",
		"62 61 9",
		"64 63 10",
	))

	require.NoError(t, <-alterChan)
	tk2.MustQuery(`select _tidb_rowid, a, b from ` + dbName + `2.t2`).Sort().Check(testkit.Rows(""+
		"13 11 11",
		"14 2 2",
		"15 12 12",
		"17 16 18",
		"19 18 4",
		"51 50 5",
		"53 52 6",
		"54 20 5",
		"56 55 6",
		"58 57 7",
		"60 59 8",
		"62 61 9",
		"64 63 10",
	))
}

// enableReadOnlyDDLFp enables the failpoint to mock read-only DDLs.
func enableReadOnlyDDLFp(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockModifySchemaReadOnlyDDL", "return")
}

func TestAlterSchemaReadonlyBasic(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("alter database test read only = 1")
	tk.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* READ ONLY = 1 */"))
	is := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	v := is.SchemaMetaVersion()
	tk.MustExec("alter database test read only = 1")
	tk.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* READ ONLY = 1 */"))
	require.Equal(t, v, is.SchemaMetaVersion())
	tk.MustExec("alter database test read only = 0")
	tk.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	// note
	tk.MustExec("alter database test read only = 0")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 database test is already in the read-write state"))
	tk.MustExec("alter database test read only = 1")
	tk.MustExec("alter database test read only = 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 database test is already in the read-only state"))
	// Can't modify the read-only status when TiDB is in restricted read-only mode.
	tk.MustExec("set global tidb_restricted_read_only = 1")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustGetErrMsg("alter database test read only = 1", "[planner:1836]Running in read-only mode")
}

func TestAlterSchemaReadonlyPrivilege(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("create user 'u1'@'%'")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	ctx := context.Background()
	_, err = se.Execute(ctx, "alter database test read only = 1")
	require.Equal(t, "[planner:1044]Access denied for user 'u1'@'%' to database 'test'", err.Error())
	_, err = se.Execute(ctx, "alter database test read only = 0")
	require.Equal(t, "[planner:1044]Access denied for user 'u1'@'%' to database 'test'", err.Error())

	tk.MustExec("grant alter on test.* to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter database test read only = 1")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "alter database test read only = 0")
	require.NoError(t, err)
}

func TestSchemaReadOnlyAffectAllUsers(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("create table if not exists test.t(a int)")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	tcs := []struct {
		user string
		priv string
	}{
		{"u1", "all"},
		{"u2", "super"},
		{"u3", "insert"},
	}
	tk.MustExec("alter database test read only = 1")
	for _, tc := range tcs {
		tk.MustExec(fmt.Sprintf("create user '%s'@'%%'", tc.user))
		tk.MustExec(fmt.Sprintf("grant %s on *.* to '%s'@'%%'", tc.priv, tc.user))
		require.NoError(t, se.Auth(&auth.UserIdentity{Username: tc.user, Hostname: "%"}, nil, nil, nil))
		tk.MustGetErrMsg("insert into test.t values (1)", "[schema:3989]Schema 'test' is in read only mode.")
	}
	tk.MustExec("alter database test read only = 0")
	for _, tc := range tcs {
		require.NoError(t, se.Auth(&auth.UserIdentity{Username: tc.user, Hostname: "%"}, nil, nil, nil))
		tk.MustExec("insert into test.t values (1)")
	}
}

func TestSchemaReadOnlyUsesWriteTargetSchema(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database ro")
	tk.MustExec("create table ro.t(id int primary key, v int)")
	tk.MustExec("create table ro.heap(v int)")
	tk.MustExec("create table ro.clustered(id int primary key clustered, v int)")
	tk.MustExec("create sequence ro.s")
	tk.MustExec("create database rw")
	tk.MustExec("create table rw.a(id int primary key, v int)")
	tk.MustExec("create table rw.t(id int primary key, w int)")
	tk.MustExec("insert into ro.t values (1, 1)")
	tk.MustExec("insert into ro.heap values (1)")
	tk.MustExec("insert into rw.a values (1, 1)")
	tk.MustExec("insert into rw.t values (1, 1)")
	tk.MustExec("alter database ro read only = 1")

	errMsg := "[schema:3989]Schema 'ro' is in read only mode."
	// The session has no current database. The write target must be derived from
	// the table source instead of the session's CurrentDB.
	tk.MustGetErrMsg("update ro.t set v = 2 where id = 1", errMsg)
	tk.MustGetErrMsg("update ro.t set t.v = 2 where id = 1", errMsg)
	tk.MustGetErrMsg("delete a from ro.t a", errMsg)
	tk.MustGetErrMsg("batch on id limit 100 update ro.t set v = 2", errMsg)
	tk.MustGetErrMsg("create sequence ro.s2", errMsg)
	tk.MustGetErrMsg("alter sequence ro.s increment by 2", errMsg)
	tk.MustGetErrMsg("drop sequence ro.s", errMsg)
	tk.MustExec("set tidb_opt_write_row_id = 1")
	tk.MustGetErrMsg("update ro.heap set _tidb_rowid = 2", errMsg)
	tk.MustGetErrMsg("update ro.heap set heap._tidb_rowid = 2", errMsg)

	// The write target must still be resolved when qualified tables have the
	// same name or a delete alias conflicts with an unaliased table name.
	tk.MustExec("use rw")
	tk.MustGetErrMsg("update ro.t, rw.t set t.v = 2", errMsg)
	tk.MustGetErrMsg("delete t from ro.t t join rw.t", errMsg)
	tk.MustExec("update ro.t, rw.t set t.w = 2")
	tk.MustExec("delete t from rw.t t join ro.t")

	// CurrentDB preserves the spelling used by USE, while schema comparisons
	// are case-insensitive.
	tk.MustExec("use Ro")
	tk.MustGetErrMsg("update t set v = 2 where id = 1", errMsg)
	tk.MustGetErrMsg("delete t from t", errMsg)
	tk.MustGetErrMsg("delete t from Ro.t", errMsg)
	currentDBErrMsg := "[schema:3989]Schema 'Ro' is in read only mode."
	tk.MustGetErrMsg("create sequence s2", currentDBErrMsg)
	tk.MustGetErrMsg("alter sequence s increment by 2", currentDBErrMsg)
	tk.MustGetErrMsg("drop sequence s", currentDBErrMsg)

	// Invalid assignments and delete targets must retain their planner errors
	// instead of being rejected based on the read-only current database.
	tk.MustGetErrCode("update rw.t set missing = 1", errno.ErrBadField)
	tk.MustGetErrCode("update ro.clustered set _tidb_rowid = 2", errno.ErrBadField)
	tk.MustGetErrCode("delete missing from ro.t", errno.ErrUnknownTable)

	// An unqualified assignment that can target both schemas must fail closed.
	tk.MustGetErrMsg("update ro.t, rw.a set v = 2", errMsg)

	// The current database and read sources do not make a writable target read-only.
	tk.MustExec("update rw.a set v = 2 where id = 1")
	tk.MustGetErrMsg("update ro.t set v = 2 where id = 1", errMsg)
	tk.MustGetErrMsg("update rw.a join ro.t on a.id = t.id set t.v = 2", errMsg)
	tk.MustExec("update rw.a join ro.t on a.id = t.id set a.v = 3")
	tk.MustExec("delete a from rw.a a join ro.t t on a.id = t.id")
}

func TestAlterDBReadOnlyBlockByTxn(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database test_db")
	tk1.MustExec("use test_db")
	tk1.MustExec("create table t (a int)")
	tk1.MustExec("begin")
	tk1.MustExec("select * from t")
	r := tk1.MustQuery("select @@tidb_current_ts").Rows()
	txnID, err := strconv.ParseInt(r[0][0].(string), 10, 64)
	require.NoError(t, err)
	var txnIDs map[int64]struct{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Eventually(t, func() bool {
			return len(txnIDs) == 1 && txnIDs[txnID] == struct{}{}
		}, 5*time.Second, 100*time.Millisecond)
		tk1.MustExec("commit")
	}()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
		txnIDs = ids
	})
	tk2.MustExec("alter database test_db read only = 1")
	wg.Wait()
	tk2.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* READ ONLY = 1 */"))
}

func TestAlterDBReadOnlyNotBlockByIrrelevantTxn(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("create database test_db")
	tk1.MustExec("create database if not exists test")
	tk1.MustExec("use test_db")
	tk1.MustExec("create table t (a int)")
	tk1.MustExec("begin")
	tk1.MustExec("select * from t")
	tk2.MustExec("alter database test read only = 1")
	tk2.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* READ ONLY = 1 */"))
}

func TestAccessDBInTxnAfterDDLDone(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("create database test_db")
	tk1.MustExec("create table test_db.t(a int)")
	tk1.MustExec("begin")
	tk2.MustExec("alter database test_db read only = 1")
	tk2.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* READ ONLY = 1 */"))
	tk1.MustGetErrMsg("select * from test_db.t", "[domain:8028]public schema test_db read only state has changed")
}

func TestReadWriteDDLNotBlockByTxn(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("create database test_db")
	tk1.MustExec("create table test_db.t(a int)")
	tk1.MustExec("alter schema test_db read only = 1")
	tk1.MustExec("begin;use test_db;")
	tk1.MustExec("select * from t")
	tk2.MustExec("alter database test_db read only = 0") // won't be blocked
	tk2.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
}

func TestReadOnlyInMiddleState(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)

	tk1.MustExec("create database test_db")
	tk1.MustExec("create table test_db.t(a int)")
	tk1.MustExec("begin;use test_db;")
	tk1.MustExec("select * from t")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustExec("alter database test_db read only = 1")
	}()
	var txnIDs map[int64]struct{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
		txnIDs = ids
	})
	require.Eventually(t, func() bool {
		return len(txnIDs) == 1
	}, 5*time.Second, 100*time.Millisecond)
	is := sessiontxn.GetTxnManager(tk3.Session()).GetTxnInfoSchema()
	dbInfo, ok := is.SchemaByName(pmodel.NewCIStr("test_db"))
	require.True(t, ok)
	require.True(t, dbInfo.ReadOnly)
	tk3.MustGetErrMsg("insert into test_db.t values (1)", "[schema:3989]Schema 'test_db' is in read only mode.")
	tk1.MustExec("commit")
	wg.Wait()
}

func TestKillBlockReadOnlyDDLTxn(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store, dom := testkit.CreateMockStoreAndDomain(t)
	sv := server.CreateMockServer(t, store)
	sv.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(sv)
	defer sv.Close()

	conn1 := server.CreateMockConn(t, sv)
	tk1 := testkit.NewTestKitWithSession(t, store, conn1.Context().Session)
	conn2 := server.CreateMockConn(t, sv)
	tk2 := testkit.NewTestKitWithSession(t, store, conn2.Context().Session)
	tk1.MustExec("use test")
	tk1.MustExec("set global tidb_enable_metadata_lock=1")
	tk1.MustExec("create table t(a int);")
	tk1.MustExec("insert into t values(1);")
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t;")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustExec("alter schema test read only = 1")
	}()
	var txnIDs map[int64]struct{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
		txnIDs = ids
	})
	require.Eventually(t, func() bool {
		return len(txnIDs) == 1
	}, 5*time.Second, 100*time.Millisecond)

	conn1.Close()
	wg.Wait()
}

func TestAlterSchemaReadOnlyDDLRollback(t *testing.T) {
	enableReadOnlyDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("set global tidb_ddl_error_count_limit = 1")
	tk1.MustExec("create database test_db")
	tk1.MustExec("create table test_db.t(a int)")

	// read write -> read only, StateNone -> StatePendingReadOnly error
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaReadOnlyStateNone", "return")
	tk1.MustGetErrMsg("alter schema test_db read only = 1", "[ddl:-1]mock error at StateNone")
	tk1.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk1.MustExec("insert into test_db.t values (1);")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaReadOnlyStateNone")
	r := tk1.MustQuery("admin show ddl jobs where db_name = 'test_db' and job_type = 'modify schema read only'").Rows()
	require.Equal(t, r[0][4], "none")
	require.Equal(t, r[0][11], "rollback done")

	// read only -> read write, StatePendingReadOnly -> StatePublic error
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaReadOnlyStatePendingReadOnly", "return")
	tk1.MustGetErrMsg("alter schema test_db read only = 1", "[ddl:-1]mock error at StatePendingReadOnly")
	tk1.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk1.MustExec("insert into test_db.t values (1);")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaReadOnlyStatePendingReadOnly")
	require.Equal(t, r[0][4], "none")
	require.Equal(t, r[0][11], "rollback done")

	// read write -> read only,  error
	tk1.MustExec("alter schema test_db read only = 1")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaReadOnly2ReadWrite", "return")
	tk1.MustGetErrMsg("alter schema test_db read only = 0", "[ddl:-1]mock error at read only to read write")
	tk1.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* READ ONLY = 1 */"))
	tk1.MustGetErrMsg("insert into test_db.t values (1);", "[schema:3989]Schema 'test_db' is in read only mode.")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaReadOnlyStatePendingReadOnly")
	require.Equal(t, r[0][4], "none")
	require.Equal(t, r[0][11], "rollback done")
}

// enableArchiveDDLFp enables the failpoint to mock archive DDLs.
func enableArchiveDDLFp(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaArchiveDDL", "return")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockModifySchemaReadOnlyDDL", "return")
}

func TestAlterSchemaArchiveBasic(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("alter database test archive = 1")
	tk.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* ARCHIVE = 1 */"))
	is := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	v := is.SchemaMetaVersion()
	tk.MustExec("alter database test archive = 1")
	tk.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* ARCHIVE = 1 */"))
	require.Equal(t, v, is.SchemaMetaVersion())
	tk.MustExec("alter database test archive = 0")
	tk.MustQuery("show create database test").Check(testkit.Rows("test CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	// note
	tk.MustExec("alter database test archive = 0")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 database test is already in the unarchived state"))
	tk.MustExec("alter database test archive = 1")
	tk.MustExec("alter database test archive = 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 database test is already in the archived state"))
}

// TestReadOnlyAndArchiveAreIndependent verifies ReadOnly and Archived are independent flags:
// neither DDL touches the other, so unarchiving a read-only-and-archived DB leaves it read-only.
func TestReadOnlyAndArchiveAreIndependent(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_db")
	tk.MustExec("create table test_db.t(a int)")

	tk.MustExec("alter database test_db read only = 1")
	tk.MustExec("select * from test_db.t")
	tk.MustGetErrMsg("insert into test_db.t values (1)", "[schema:3989]Schema 'test_db' is in read only mode.")

	// Archive on top of read-only blocks reads too - the restrictions are additive.
	tk.MustExec("alter database test_db archive = 1")
	is := sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	dbInfo, ok := is.SchemaByName(pmodel.NewCIStr("test_db"))
	require.True(t, ok)
	require.True(t, dbInfo.ReadOnly)
	require.True(t, dbInfo.Archived)
	tk.MustGetErrMsg("select * from test_db.t", "[schema:3990]Schema 'test_db' is in archived mode.")
	// Archive is checked first, so its error wins even though the schema is also read-only.
	tk.MustGetErrMsg("insert into test_db.t values (1)", "[schema:3990]Schema 'test_db' is in archived mode.")

	tk.MustExec("alter database test_db archive = 0")
	is = sessiontxn.GetTxnManager(tk.Session()).GetTxnInfoSchema()
	dbInfo, ok = is.SchemaByName(pmodel.NewCIStr("test_db"))
	require.True(t, ok)
	require.True(t, dbInfo.ReadOnly)
	require.False(t, dbInfo.Archived)
	tk.MustExec("select * from test_db.t")
	tk.MustGetErrMsg("insert into test_db.t values (1)", "[schema:3989]Schema 'test_db' is in read only mode.")
}

// TestSchemaArchiveBlocksReadOnlyToggle verifies ARCHIVE=0 is the only escape hatch out of
// archived mode - a bare READ ONLY toggle must still be blocked like any other statement.
func TestSchemaArchiveBlocksReadOnlyToggle(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg(
		"alter database archived_db read only = 1",
		"[schema:3990]Schema 'archived_db' is in archived mode.",
	)

	// The actual escape hatch must still work.
	tk.MustExec("alter database archived_db archive = 0")
}

// TestSchemaArchiveExemptsRestrictedSQL verifies internal restricted SQL (auto-analyze, stats
// collection) can still read an archived table, since archive - unlike read-only - blocks reads.
func TestSchemaArchiveExemptsRestrictedSQL(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table archived_db.t(a int)")
	tk.MustExec("insert into archived_db.t values (1)")
	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg("select * from archived_db.t", "[schema:3990]Schema 'archived_db' is in archived mode.")

	tk.Session().GetSessionVars().InRestrictedSQL = true
	defer func() { tk.Session().GetSessionVars().InRestrictedSQL = false }()
	tk.MustExec("select * from archived_db.t")
}

// TestSchemaArchiveDoesNotExemptRestrictedSQLWrites verifies the InRestrictedSQL exemption
// covers only reads: an internal writer (e.g. TTL's row-expiry DELETE) must stay blocked.
func TestSchemaArchiveDoesNotExemptRestrictedSQLWrites(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table archived_db.t(a int)")
	tk.MustExec("insert into archived_db.t values (1)")
	tk.MustExec("alter database archived_db archive = 1")

	tk.Session().GetSessionVars().InRestrictedSQL = true
	defer func() { tk.Session().GetSessionVars().InRestrictedSQL = false }()
	tk.MustExec("select * from archived_db.t")
	tk.MustGetErrMsg("delete from archived_db.t", "[schema:3990]Schema 'archived_db' is in archived mode.")
	tk.MustGetErrMsg("insert into archived_db.t values (2)", "[schema:3990]Schema 'archived_db' is in archived mode.")
	tk.MustGetErrMsg("update archived_db.t set a = 2", "[schema:3990]Schema 'archived_db' is in archived mode.")
}

func TestAlterSchemaArchivePrivilege(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("create user 'u1'@'%'")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	ctx := context.Background()
	_, err = se.Execute(ctx, "alter database test archive = 1")
	require.Equal(t, "[planner:1044]Access denied for user 'u1'@'%' to database 'test'", err.Error())
	_, err = se.Execute(ctx, "alter database test archive = 0")
	require.Equal(t, "[planner:1044]Access denied for user 'u1'@'%' to database 'test'", err.Error())

	tk.MustExec("grant alter on test.* to 'u1'@'%'")
	_, err = se.Execute(ctx, "alter database test archive = 1")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "alter database test archive = 0")
	require.NoError(t, err)
}

// TestSchemaArchiveBlocksReadsForAllUsers is the key behavior that distinguishes ARCHIVE from
// READ ONLY: a plain SELECT (not just a write) must be rejected once a database is archived,
// with no exception for any privilege.
func TestSchemaArchiveBlocksReadsForAllUsers(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("create table if not exists test.t(a int)")
	tk.MustExec("insert into test.t values (1)")
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	// SUPER/ALL are added for u1/u2 to confirm those don't grant a bypass either.
	tcs := []struct {
		user string
		priv string
	}{
		{"u1", "all"},
		{"u2", "select, insert, super"},
		{"u3", "select, insert"},
	}
	tk.MustExec("alter database test archive = 1")
	ctx := context.Background()
	for _, tc := range tcs {
		tk.MustExec(fmt.Sprintf("create user '%s'@'%%'", tc.user))
		tk.MustExec(fmt.Sprintf("grant %s on *.* to '%s'@'%%'", tc.priv, tc.user))
		require.NoError(t, se.Auth(&auth.UserIdentity{Username: tc.user, Hostname: "%"}, nil, nil, nil))
		_, err = se.Execute(ctx, "select * from test.t")
		require.EqualError(t, err, "[schema:3990]Schema 'test' is in archived mode.")
		_, err = se.Execute(ctx, "insert into test.t values (2)")
		require.EqualError(t, err, "[schema:3990]Schema 'test' is in archived mode.")
	}
	tk.MustExec("alter database test archive = 0")
	for _, tc := range tcs {
		require.NoError(t, se.Auth(&auth.UserIdentity{Username: tc.user, Hostname: "%"}, nil, nil, nil))
		_, err = se.Execute(ctx, "select * from test.t")
		require.NoError(t, err)
	}
}

// TestSchemaArchiveReplicaWriterAdminBypass verifies RESTRICTED_REPLICA_WRITER_ADMIN is the one
// exception to archive mode's otherwise unconditional block.
func TestSchemaArchiveReplicaWriterAdminBypass(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("create table if not exists test.t(a int)")
	tk.MustExec("create user 'replica'@'%'")
	tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'replica'@'%'")
	tk.MustExec("grant select, insert on test.* to 'replica'@'%'")
	tk.MustExec("alter database test archive = 1")

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "replica", Hostname: "%"}, nil, nil, nil))
	ctx := context.Background()
	_, err = se.Execute(ctx, "select * from test.t")
	require.NoError(t, err)
	_, err = se.Execute(ctx, "insert into test.t values (1)")
	require.NoError(t, err)

	// A user without the privilege remains fully blocked, even after the bypassed writes above.
	tk.MustGetErrMsg("select * from test.t", "[schema:3990]Schema 'test' is in archived mode.")
}

// TestSchemaArchiveBlocksJoinedReads verifies archive's read block applies to a database only
// joined in to a write against a different database, not just the write's own target.
func TestSchemaArchiveBlocksJoinedReads(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists live_db")
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table live_db.t1(id int, name varchar(20))")
	tk.MustExec("create table archived_db.t2(id int, name varchar(20))")
	tk.MustExec("insert into live_db.t1 values (1, 'old')")
	tk.MustExec("insert into archived_db.t2 values (1, 'new')")
	tk.MustExec("alter database archived_db archive = 1")

	// UPDATE writes only to live_db.t1, but reads archived_db.t2 via the JOIN.
	tk.MustGetErrMsg(
		"update live_db.t1 join archived_db.t2 on t1.id = t2.id set t1.name = t2.name",
		"[schema:3990]Schema 'archived_db' is in archived mode.",
	)

	// Multi-table DELETE targets only live_db.t1, but reads archived_db.t2 via the JOIN.
	tk.MustGetErrMsg(
		"delete t1 from live_db.t1 join archived_db.t2 on t1.id = t2.id",
		"[schema:3990]Schema 'archived_db' is in archived mode.",
	)
}

// TestSchemaArchiveBlocksInsertSelectFromArchivedDB verifies INSERT ... SELECT reading from an
// archived database is blocked (the embedded SELECT is visited as its own AST node).
func TestSchemaArchiveBlocksInsertSelectFromArchivedDB(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists live_db")
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table live_db.t1(id int, name varchar(20))")
	tk.MustExec("create table archived_db.src(id int, name varchar(20))")
	tk.MustExec("insert into archived_db.src values (1, 'x')")
	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg(
		"insert into live_db.t1 select * from archived_db.src",
		"[schema:3990]Schema 'archived_db' is in archived mode.",
	)
}

// TestSchemaArchiveBlocksStaleRead verifies a stale read (AS OF TIMESTAMP, from before the
// database was archived) is still blocked, since checkSchemaArchived looks up the latest schema.
func TestSchemaArchiveBlocksStaleRead(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table archived_db.t(id int)")
	tk.MustExec("insert into archived_db.t values (1)")

	rows := tk.MustQuery("select now(6)").Rows()
	preArchiveTS := rows[0][0].(string)

	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg(
		fmt.Sprintf("select * from archived_db.t as of timestamp '%s'", preArchiveTS),
		"[schema:3990]Schema 'archived_db' is in archived mode.",
	)
}

// TestSchemaArchiveBlocksViewOverArchivedTable verifies a view over an archived table can't be
// used to read its rows, even though a view body bypasses core.Preprocess.
func TestSchemaArchiveBlocksViewOverArchivedTable(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists live_db")
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table archived_db.t(id int)")
	tk.MustExec("insert into archived_db.t values (1)")
	tk.MustExec("create view live_db.v as select * from archived_db.t")
	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg("select * from live_db.v", "[schema:3990]Schema 'archived_db' is in archived mode.")
}

// TestSchemaArchivePreparedStatementSurfacesArchivedError verifies EXECUTE on a prepared
// statement whose table was archived after PREPARE reports [schema:3990] itself, not the
// generically-retryable ErrSchemaChanged that planCachePreprocess otherwise wraps it in.
func TestSchemaArchivePreparedStatementSurfacesArchivedError(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table archived_db.t(id int)")
	tk.MustExec("insert into archived_db.t values (1)")
	tk.MustExec("use archived_db")
	tk.MustExec("prepare s from 'select * from t'")

	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg("execute s", "[schema:3990]Schema 'archived_db' is in archived mode.")
	require.True(t, tk.Session().GetSessionVars().DisconnectAfterResponse)
}

// TestSchemaArchiveBlocksAnalyzeAndAdmin verifies ANALYZE TABLE and ADMIN CHECKSUM/CHECK TABLE
// are blocked against an archived database, same as a plain SELECT.
func TestSchemaArchiveBlocksAnalyzeAndAdmin(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table archived_db.t(id int, name varchar(20))")
	tk.MustExec("insert into archived_db.t values (1, 'x')")
	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg("analyze table archived_db.t", "[schema:3990]Schema 'archived_db' is in archived mode.")
	tk.MustGetErrMsg("admin checksum table archived_db.t", "[schema:3990]Schema 'archived_db' is in archived mode.")
	tk.MustGetErrMsg("admin check table archived_db.t", "[schema:3990]Schema 'archived_db' is in archived mode.")
}

// TestSchemaArchiveBlocksExchangePartition verifies EXCHANGE PARTITION is blocked in both
// directions when either side of the swap is archived.
func TestSchemaArchiveBlocksExchangePartition(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists live_db")
	tk.MustExec("create database if not exists archived_db")
	tk.MustExec("create table live_db.pt(id int) partition by hash(id) partitions 1")
	tk.MustExec("create table archived_db.np(id int)")
	tk.MustExec("alter database archived_db archive = 1")

	tk.MustGetErrMsg(
		"alter table live_db.pt exchange partition p0 with table archived_db.np",
		"[schema:3990]Schema 'archived_db' is in archived mode.",
	)
}

func TestAlterDBArchiveBlockByTxn(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database test_db")
	tk1.MustExec("use test_db")
	tk1.MustExec("create table t (a int)")
	tk1.MustExec("begin")
	tk1.MustExec("select * from t")
	r := tk1.MustQuery("select @@tidb_current_ts").Rows()
	txnID, err := strconv.ParseInt(r[0][0].(string), 10, 64)
	require.NoError(t, err)
	var txnIDs map[int64]struct{}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Eventually(t, func() bool {
			return len(txnIDs) == 1 && txnIDs[txnID] == struct{}{}
		}, 5*time.Second, 100*time.Millisecond)
		tk1.MustExec("commit")
	}()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
		txnIDs = ids
	})
	tk2.MustExec("alter database test_db archive = 1")
	wg.Wait()
	tk2.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* ARCHIVE = 1 */"))
}

func TestArchiveInMiddleState(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)

	tk1.MustExec("create database test_db")
	tk1.MustExec("create table test_db.t(a int)")
	tk1.MustExec("begin;use test_db;")
	tk1.MustExec("select * from t")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk2.MustExec("alter database test_db archive = 1")
	}()
	var txnIDs map[int64]struct{}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/checkUncommittedTxns", func(ids map[int64]struct{}) {
		txnIDs = ids
	})
	require.Eventually(t, func() bool {
		return len(txnIDs) == 1
	}, 5*time.Second, 100*time.Millisecond)
	is := sessiontxn.GetTxnManager(tk3.Session()).GetTxnInfoSchema()
	dbInfo, ok := is.SchemaByName(pmodel.NewCIStr("test_db"))
	require.True(t, ok)
	require.True(t, dbInfo.Archived)
	tk3.MustGetErrMsg("select * from test_db.t", "[schema:3990]Schema 'test_db' is in archived mode.")
	tk1.MustExec("commit")
	wg.Wait()
}

func TestAlterSchemaArchiveDDLRollback(t *testing.T) {
	enableArchiveDDLFp(t)
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("set global tidb_ddl_error_count_limit = 1")
	tk1.MustExec("create database test_db")
	tk1.MustExec("create table test_db.t(a int)")

	// unarchived -> archived, StateNone -> StatePendingArchive error
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaArchiveStateNone", "return")
	tk1.MustGetErrMsg("alter schema test_db archive = 1", "[ddl:-1]mock error at StateNone")
	tk1.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	tk1.MustExec("insert into test_db.t values (1);")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaArchiveStateNone")
	r := tk1.MustQuery("admin show ddl jobs where db_name = 'test_db' and job_type = 'modify schema archive'").Rows()
	require.Equal(t, r[0][4], "none")
	require.Equal(t, r[0][11], "rollback done")

	// archived -> unarchived, error
	tk1.MustExec("alter schema test_db archive = 1")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaArchive2Unarchived", "return")
	tk1.MustGetErrMsg("alter schema test_db archive = 0", "[ddl:-1]mock error at archived to unarchived")
	tk1.MustQuery("show create database test_db").Check(testkit.Rows("test_db CREATE DATABASE `test_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /* ARCHIVE = 1 */"))
	tk1.MustGetErrMsg("select * from test_db.t", "[schema:3990]Schema 'test_db' is in archived mode.")
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockErrorOnModifySchemaArchive2Unarchived")
}

func TestTTLDeleteError(t *testing.T) {
	enableReadOnlyDDLFp(t)
	var ttlError error
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ttl/ttlworker/getTTLDeleteError", func(err error) {
		ttlError = err
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_ttl_job_enable = off")
	tk.MustExec("create database if not exists test; use test;drop table if exists t;")
	tk.MustExec("CREATE TABLE t(id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL) TTL = `ts` + INTERVAL 1 SECOND  TTL_JOB_INTERVAL = '1m';")
	tk.MustExec("insert into t values (1, now() - interval 10 second);")
	tk.MustExec("alter schema test read only = 1")
	tk.MustExec("set global tidb_ttl_job_enable = on")
	require.Eventually(t, func() bool {
		return ttlError != nil && strings.Contains(ttlError.Error(), "Schema 'test' is in read only mode")
	}, 10*time.Second, 100*time.Millisecond)
}

// TestRefreshMetaSchemaReadOnly covers a database whose ReadOnly flag is changed
// in meta kv without a DDL, as PiTR log restore does. A database-level refresh
// meta must reconcile the flag into infoschema in both directions.
func TestRefreshMetaSchemaReadOnly(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("infoschemaV2=%v", v2), func(t *testing.T) {
			enableReadOnlyDDLFp(t)
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			if v2 {
				tk.MustExec("set @@global.tidb_schema_cache_size = 512 * 1024 * 1024")
			} else {
				tk.MustExec("set @@global.tidb_schema_cache_size = 0")
			}
			tk.MustExec("create database test_db")
			tk.MustExec("create table test_db.t (a int)")
			isV2, _ := infoschema.IsV2(dom.InfoSchema())
			require.Equal(t, v2, isV2)

			setReadOnlyInKV := func(readOnly bool) *model.DBInfo {
				dbInfo, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr("test_db"))
				require.True(t, ok)
				dbInfo = dbInfo.Clone()
				dbInfo.ReadOnly = readOnly
				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
					return meta.NewMutator(txn).UpdateDatabase(dbInfo)
				}))
				return dbInfo
			}
			isReadOnly := func() bool {
				dbInfo, ok := dom.InfoSchema().SchemaByName(pmodel.NewCIStr("test_db"))
				require.True(t, ok)
				return dbInfo.ReadOnly
			}
			refreshDB := func(dbInfo *model.DBInfo) {
				testutil.RefreshMeta(tk.Session(), t, dom.DDLExecutor(), dbInfo.ID, 0, dbInfo.Name.O, model.InvolvingAll)
			}

			// read only -> read write
			tk.MustExec("alter database test_db read only = 1")
			require.True(t, isReadOnly())
			dbInfo := setReadOnlyInKV(false)
			require.True(t, isReadOnly())
			tk.MustGetErrMsg("insert into test_db.t values (1)", "[schema:3989]Schema 'test_db' is in read only mode.")
			refreshDB(dbInfo)
			require.False(t, isReadOnly())
			tk.MustExec("insert into test_db.t values (1)")

			// read write -> read only
			dbInfo = setReadOnlyInKV(true)
			require.False(t, isReadOnly())
			tk.MustExec("insert into test_db.t values (2)")
			refreshDB(dbInfo)
			require.True(t, isReadOnly())
			tk.MustGetErrMsg("insert into test_db.t values (3)", "[schema:3989]Schema 'test_db' is in read only mode.")
			tk.MustQuery("select a from test_db.t order by a").Check(testkit.Rows("1", "2"))
		})
	}
}
