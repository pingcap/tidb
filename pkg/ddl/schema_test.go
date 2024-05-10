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
	"strings"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testCreateTable(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{tblInfo},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJob(ctx, job)
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
		m := meta.NewMeta(txn)
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
		Name: model.NewCIStr(name),
	}
	genIDs, err := genGlobalIDs(store, 1)

	if err != nil {
		return nil, err
	}
	tblInfo.ID = genIDs[0]

	cols := make([]*model.ColumnInfo, num)
	for i := range cols {
		col := &model.ColumnInfo{
			Name:         model.NewCIStr(fmt.Sprintf("c%d", i+1)),
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
		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenGlobalIDs(count)
		return err
	})
	return ret, err
}

func testSchemaInfo(store kv.Storage, name string) (*model.DBInfo, error) {
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr(name),
	}

	genIDs, err := genGlobalIDs(store, 1)
	if err != nil {
		return nil, err
	}
	dbInfo.ID = genIDs[0]
	return dbInfo, nil
}

func testCreateSchema(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{dbInfo},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.DoDDLJob(ctx, job))

	v := getSchemaVer(t, ctx)
	dbInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo})
	dbInfo.State = model.StateNone
	return job
}

func buildDropSchemaJob(dbInfo *model.DBInfo) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{true},
	}
}

func testDropSchema(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo) (*model.Job, int64) {
	job := buildDropSchemaJob(dbInfo)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJob(ctx, job)
	require.NoError(t, err)
	ver := getSchemaVer(t, ctx)
	return job, ver
}

func isDDLJobDone(test *testing.T, t *meta.Meta, store kv.Storage) bool {
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
			t := meta.NewMeta(txn)
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
	d := domain.DDL()
	job := testCreateSchema(t, tk.Session(), d, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)

	/*** to drop the schema with two tables. ***/
	// create table t with 100 records.
	tblInfo1, err := testTableInfo(store, "t", 3)
	require.NoError(t, err)
	tJob1 := testCreateTable(t, tk.Session(), d, dbInfo, tblInfo1)
	testCheckTableState(t, store, dbInfo, tblInfo1, model.StatePublic)
	testCheckJobDone(t, store, tJob1.ID, true)
	tbl1 := testGetTable(t, domain, tblInfo1.ID)
	err = sessiontxn.NewTxn(context.Background(), tk.Session())
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		_, err := tbl1.AddRecord(tk.Session().GetTableCtx(), types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	// create table t1 with 1034 records.
	tblInfo2, err := testTableInfo(store, "t1", 3)
	require.NoError(t, err)
	tk2 := testkit.NewTestKit(t, store)
	tJob2 := testCreateTable(t, tk2.Session(), d, dbInfo, tblInfo2)
	testCheckTableState(t, store, dbInfo, tblInfo2, model.StatePublic)
	testCheckJobDone(t, store, tJob2.ID, true)
	tbl2 := testGetTable(t, domain, tblInfo2.ID)
	err = sessiontxn.NewTxn(context.Background(), tk2.Session())
	require.NoError(t, err)
	for i := 1; i <= 1034; i++ {
		_, err := tbl2.AddRecord(tk2.Session().GetTableCtx(), types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	tk3 := testkit.NewTestKit(t, store)
	job, v := testDropSchema(t, tk3.Session(), d, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StateNone)
	ids := make(map[int64]struct{})
	ids[tblInfo1.ID] = struct{}{}
	ids[tblInfo2.ID] = struct{}{}
	checkHistoryJobArgs(t, tk3.Session(), job.ID, &historyJobArgs{ver: v, db: dbInfo, tblIDs: ids})

	// Drop a non-existent database.
	job = &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx := testkit.NewTestKit(t, store).Session()
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.DoDDLJob(ctx, job)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrDatabaseDropExists), "err %v", err)

	// Drop a database without a table.
	dbInfo1, err := testSchemaInfo(store, "test1")
	require.NoError(t, err)
	job = testCreateSchema(t, ctx, d, dbInfo1)
	testCheckSchemaState(t, store, dbInfo1, model.StatePublic)
	testCheckJobDone(t, store, job.ID, true)
	job, _ = testDropSchema(t, ctx, d, dbInfo1)
	testCheckSchemaState(t, store, dbInfo1, model.StateNone)
	testCheckJobDone(t, store, job.ID, false)
}

func TestSchemaWaitJob(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	require.True(t, domain.DDL().OwnerManager().IsOwner())

	d2 := ddl.NewDDL(context.Background(),
		ddl.WithEtcdClient(domain.EtcdClient()),
		ddl.WithStore(store),
		ddl.WithInfoCache(domain.InfoCache()),
		ddl.WithLease(testLease),
	)
	err := d2.Start(pools.NewResourcePool(func() (pools.Resource, error) {
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
	testCreateSchema(t, se, d2, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StatePublic)

	// d2 must not be owner.
	require.False(t, d2.OwnerManager().IsOwner())

	genIDs, err := genGlobalIDs(store, 1)
	require.NoError(t, err)
	schemaID := genIDs[0]
	doDDLJobErr(t, schemaID, 0, model.ActionCreateSchema, []any{dbInfo}, testkit.NewTestKit(t, store).Session(), d2, store)
}

func doDDLJobErr(t *testing.T, schemaID, tableID int64, tp model.ActionType, args []any, ctx sessionctx.Context, d ddl.DDL, store kv.Storage) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	// TODO: check error detail
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.Error(t, d.DoDDLJob(ctx, job))
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
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk3 := testkit.NewTestKit(t, store)
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
			tk4 := testkit.NewTestKit(t, store)
			tk4.MustExec(`use test`)
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
	go func() {
		alterChan <- tk1.ExecToErr(`rename table t to ` + dbName + `2.t2`)
	}()
	waitFor(11, "t", "running")
	waitFor(4, "t", "public")
	tk3.MustExec(`BEGIN`)
	tk3.MustExec(`insert into ` + dbName + `2.t2 values (50, 5)`)

	tk2.MustExec(`insert into t values (null, 6)`)
	tk3.MustExec(`insert into ` + dbName + `2.t2 values (20, 5)`)

	// Done: Fix https://github.com/pingcap/tidb/issues/46904
	//tk2.MustContainErrMsg(`insert into t values (null, 6)`, "[tikv:1205]Lock wait timeout exceeded; try restarting transaction")
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
