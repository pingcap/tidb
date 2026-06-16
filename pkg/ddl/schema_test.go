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
