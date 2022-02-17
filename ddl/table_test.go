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

package ddl

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func testCreateTableT(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testRenameTable(t *testing.T, ctx sessionctx.Context, d *ddl, newSchemaID, oldSchemaID int64, oldSchemaName model.CIStr, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{oldSchemaID, tblInfo.Name, oldSchemaName},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testRenameTables(
	t *testing.T, ctx sessionctx.Context, d *ddl,
	oldSchemaIDs, newSchemaIDs []int64, newTableNames []*model.CIStr,
	oldTableIDs []int64, oldSchemaNames []*model.CIStr,
) *model.Job {
	job := &model.Job{
		Type:       model.ActionRenameTables,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{oldSchemaIDs, newSchemaIDs, newTableNames, oldTableIDs, oldSchemaNames},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: nil})
	return job
}

func testLockTable(t *testing.T, ctx sessionctx.Context, d *ddl, newSchemaID int64, tblInfo *model.TableInfo, lockTp model.TableLockType) *model.Job {
	arg := &lockTablesArg{
		LockTables: []model.TableLockTpInfo{{SchemaID: newSchemaID, TableID: tblInfo.ID, Tp: lockTp}},
		SessionInfo: model.SessionInfo{
			ServerID:  d.GetID(),
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}
	job := &model.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionLockTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{arg},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func checkTableLockedTest(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, serverID string, sessionID uint64, lockTp model.TableLockType) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		require.NotNil(t, info)
		require.NotNil(t, info.Lock)
		require.Len(t, info.Lock.Sessions, 1)
		require.Equal(t, serverID, info.Lock.Sessions[0].ServerID)
		require.Equal(t, sessionID, info.Lock.Sessions[0].SessionID)
		require.Equal(t, lockTp, info.Lock.Tp)
		require.Equal(t, lockTp, info.Lock.Tp)
		require.Equal(t, model.TableLockStatePublic, info.Lock.State)
		return nil
	})
	require.NoError(t, err)
}

func testDropTableT(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testTruncateTable(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	newTableID := genIDs[0]
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	tblInfo.ID = newTableID
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCheckTableStateT(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		if state == model.StateNone {
			require.Nil(t, info)
			return nil
		}

		require.EqualValues(t, tblInfo.Name, info.Name)
		require.Equal(t, state, info.State)
		return nil
	})
	require.NoError(t, err)
}

func testGetTableT(t *testing.T, d *ddl, schemaID int64, tableID int64) table.Table {
	tbl, err := testGetTableWithError(d, schemaID, tableID)
	require.NoError(t, err)
	return tbl
}

func testGetTableWithError(d *ddl, schemaID, tableID int64) (table.Table, error) {
	var tblInfo *model.TableInfo
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		tblInfo, err1 = t.GetTable(schemaID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		return nil, errors.New("table not found")
	}
	alloc := autoid.NewAllocator(d.store, schemaID, tblInfo.ID, false, autoid.RowIDAllocType)
	tbl, err := table.TableFromMeta(autoid.NewAllocators(alloc), tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tbl, nil
}

func ExportTestTable(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	ddl, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)

	dbInfo, err := testSchemaInfo(ddl, "test_table")
	require.NoError(t, err)
	testCreateSchemaT(t, testNewContext(ddl), ddl, dbInfo)

	ctx := testNewContext(ddl)

	tblInfo, err := testTableInfo(ddl, "t", 3)
	require.NoError(t, err)
	job := testCreateTableT(t, ctx, ddl, dbInfo, tblInfo)
	testCheckTableStateT(t, ddl, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)

	// Create an existing table.
	newTblInfo, err := testTableInfo(ddl, "t", 3)
	require.NoError(t, err)
	doDDLJobErrT(t, dbInfo.ID, newTblInfo.ID, model.ActionCreateTable, []interface{}{newTblInfo}, ctx, ddl)

	count := 2000
	tbl := testGetTableT(t, ddl, dbInfo.ID, tblInfo.ID)
	for i := 1; i <= count; i++ {
		_, err := tbl.AddRecord(ctx, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}

	job = testDropTableT(t, ctx, ddl, dbInfo, tblInfo)
	testCheckJobDoneT(t, ddl, job, false)

	// for truncate table
	tblInfo, err = testTableInfo(ddl, "tt", 3)
	require.NoError(t, err)
	job = testCreateTableT(t, ctx, ddl, dbInfo, tblInfo)
	testCheckTableStateT(t, ddl, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)
	job = testTruncateTable(t, ctx, ddl, dbInfo, tblInfo)
	testCheckTableStateT(t, ddl, dbInfo, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)

	// for rename table
	dbInfo1, err := testSchemaInfo(ddl, "test_rename_table")
	require.NoError(t, err)
	testCreateSchemaT(t, testNewContext(ddl), ddl, dbInfo1)
	job = testRenameTable(t, ctx, ddl, dbInfo1.ID, dbInfo.ID, dbInfo.Name, tblInfo)
	testCheckTableStateT(t, ddl, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)

	job = testLockTable(t, ctx, ddl, dbInfo1.ID, tblInfo, model.TableLockWrite)
	testCheckTableStateT(t, ddl, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)
	checkTableLockedTest(t, ddl, dbInfo1, tblInfo, ddl.GetID(), ctx.GetSessionVars().ConnectionID, model.TableLockWrite)
	// for alter cache table
	job = testAlterCacheTable(t, ctx, ddl, dbInfo1.ID, tblInfo)
	testCheckTableStateT(t, ddl, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)
	checkTableCacheTest(t, ddl, dbInfo1, tblInfo)
	// for alter no cache table
	job = testAlterNoCacheTable(t, ctx, ddl, dbInfo1.ID, tblInfo)
	testCheckTableStateT(t, ddl, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDoneT(t, ddl, job, true)
	checkTableNoCacheTest(t, ddl, dbInfo1, tblInfo)

	testDropSchemaT(t, testNewContext(ddl), ddl, dbInfo)
	err = ddl.Stop()
	require.NoError(t, err)
	err = store.Close()
	require.NoError(t, err)
}

func checkTableCacheTest(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.NotNil(t, info.TableCacheStatusType)
		require.Equal(t, model.TableCacheStatusEnable, info.TableCacheStatusType)
		return nil
	})
	require.NoError(t, err)
}

func checkTableNoCacheTest(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		tt := meta.NewMeta(txn)
		info, err := tt.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, model.TableCacheStatusDisable, info.TableCacheStatusType)
		return nil
	})
	require.NoError(t, err)
}

func testAlterCacheTable(t *testing.T, ctx sessionctx.Context, d *ddl, newSchemaID int64, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterCacheTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func testAlterNoCacheTable(t *testing.T, ctx sessionctx.Context, d *ddl, newSchemaID int64, tblInfo *model.TableInfo) *model.Job {

	job := &model.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAlterNoCacheTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVerT(t, ctx)
	checkHistoryJobArgsT(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func ExportTestRenameTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)
	ddl, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		err := ddl.Stop()
		require.NoError(t, err)
	}()

	dbInfo, err := testSchemaInfo(ddl, "test_table")
	require.NoError(t, err)
	testCreateSchemaT(t, testNewContext(ddl), ddl, dbInfo)

	ctx := testNewContext(ddl)
	var tblInfos = make([]*model.TableInfo, 0, 2)
	var newTblInfos = make([]*model.TableInfo, 0, 2)
	for i := 1; i < 3; i++ {
		tableName := fmt.Sprintf("t%d", i)
		tblInfo, err := testTableInfo(ddl, tableName, 3)
		require.NoError(t, err)
		job := testCreateTableT(t, ctx, ddl, dbInfo, tblInfo)
		testCheckTableStateT(t, ddl, dbInfo, tblInfo, model.StatePublic)
		testCheckJobDoneT(t, ddl, job, true)
		tblInfos = append(tblInfos, tblInfo)

		newTableName := fmt.Sprintf("tt%d", i)
		tblInfo, err = testTableInfo(ddl, newTableName, 3)
		require.NoError(t, err)
		newTblInfos = append(newTblInfos, tblInfo)
	}

	job := testRenameTables(
		t, ctx, ddl,
		[]int64{dbInfo.ID, dbInfo.ID},
		[]int64{dbInfo.ID, dbInfo.ID},
		[]*model.CIStr{&newTblInfos[0].Name, &newTblInfos[1].Name},
		[]int64{tblInfos[0].ID, tblInfos[1].ID},
		[]*model.CIStr{&dbInfo.Name, &dbInfo.Name},
	)

	txn, _ := ctx.Txn(true)
	historyJob, _ := meta.NewMeta(txn).GetHistoryDDLJob(job.ID)
	wantTblInfos := historyJob.BinlogInfo.MultipleTableInfos
	require.Equal(t, wantTblInfos[0].Name.L, "tt1")
	require.Equal(t, wantTblInfos[1].Name.L, "tt2")
}

func ExportTestCreateTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	ddl, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		err := ddl.Stop()
		require.NoError(t, err)
	}()

	dbInfo, err := testSchemaInfo(ddl, "test_table")
	require.NoError(t, err)
	testCreateSchemaT(t, testNewContext(ddl), ddl, dbInfo)

	ctx := testNewContext(ddl)

	infos := []*model.TableInfo{}
	genIDs, err := ddl.genGlobalIDs(3)
	require.NoError(t, err)

	infos = append(infos, &model.TableInfo{
		ID:   genIDs[0],
		Name: model.NewCIStr("s1"),
	})
	infos = append(infos, &model.TableInfo{
		ID:   genIDs[1],
		Name: model.NewCIStr("s2"),
	})
	infos = append(infos, &model.TableInfo{
		ID:   genIDs[2],
		Name: model.NewCIStr("s3"),
	})

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateTables,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{infos},
	}
	err = ddl.doDDLJob(ctx, job)
	require.NoError(t, err)

	testGetTableT(t, ddl, dbInfo.ID, genIDs[0])
	testGetTableT(t, ddl, dbInfo.ID, genIDs[1])
	testGetTableT(t, ddl, dbInfo.ID, genIDs[2])
}
