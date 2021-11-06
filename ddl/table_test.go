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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

type tableSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func createTableSuite(t *testing.T) (s *tableSuite, clean func()) {
	s = new(tableSuite)
	s.store = testCreateStore(t, "test_table")
	s.d = testNewDDLAndStart(
		context.Background(),
		t,
		WithStore(s.store),
		WithLease(testLease),
	)

	s.dbInfo = testSchemaInfo(t, s.d, "test_table")
	testCreateSchema(t, testNewContext(s.d), s.d, s.dbInfo)

	clean = func() {
		testDropSchema(t, testNewContext(s.d), s.d, s.dbInfo)
		err := s.d.Stop()
		require.NoError(t, err)
		err = s.store.Close()
		require.NoError(t, err)
	}
	return
}

func testTableInfoWith2IndexOnFirstColumn(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	normalInfo := testTableInfo(t, d, name, num)
	idxs := make([]*model.IndexInfo, 0, 2)
	for i := range idxs {
		idx := &model.IndexInfo{
			Name:    model.NewCIStr(fmt.Sprintf("i%d", i+1)),
			State:   model.StatePublic,
			Columns: []*model.IndexColumn{{Name: model.NewCIStr("c1")}},
		}
		idxs = append(idxs, idx)
	}
	normalInfo.Indices = idxs
	normalInfo.Columns[0].FieldType.Flen = 11
	return normalInfo
}

// testTableInfo creates a test table with num int columns and with no index.
func testTableInfo(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
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
		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
	}
	tblInfo.Columns = cols
	tblInfo.Charset = "utf8"
	tblInfo.Collate = "utf8_bin"
	return tblInfo
}

// testTableInfoWithPartition creates a test table with num int columns and with no index.
func testTableInfoWithPartition(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	tblInfo := testTableInfo(t, d, name, num)
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	pid := genIDs[0]
	tblInfo.Partition = &model.PartitionInfo{
		Type:   model.PartitionTypeRange,
		Expr:   tblInfo.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			ID:       pid,
			Name:     model.NewCIStr("p0"),
			LessThan: []string{"maxvalue"},
		}},
	}

	return tblInfo
}

// testTableInfoWithPartitionLessThan creates a test table with num int columns and one partition specified with lessthan.
func testTableInfoWithPartitionLessThan(t *testing.T, d *ddl, name string, num int, lessthan string) *model.TableInfo {
	tblInfo := testTableInfoWithPartition(t, d, name, num)
	tblInfo.Partition.Definitions[0].LessThan = []string{lessthan}
	return tblInfo
}

func testAddedNewTablePartitionInfo(t *testing.T, d *ddl, tblInfo *model.TableInfo, partName, lessthan string) *model.PartitionInfo {
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	pid := genIDs[0]
	// the new added partition should change the partition state to state none at the beginning.
	return &model.PartitionInfo{
		Type:   model.PartitionTypeRange,
		Expr:   tblInfo.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{{
			ID:       pid,
			Name:     model.NewCIStr(partName),
			LessThan: []string{lessthan},
		}},
	}
}

// testViewInfo creates a test view with num int columns.
func testViewInfo(t *testing.T, d *ddl, name string, num int) *model.TableInfo {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(t, err)
	tblInfo.ID = genIDs[0]

	cols := make([]*model.ColumnInfo, num)
	viewCols := make([]model.CIStr, num)

	var stmtBuffer bytes.Buffer
	stmtBuffer.WriteString("SELECT ")
	for i := range cols {
		col := &model.ColumnInfo{
			Name:   model.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset: i,
			State:  model.StatePublic,
		}

		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
		viewCols[i] = col.Name
		stmtBuffer.WriteString(cols[i].Name.L + ",")
	}
	stmtBuffer.WriteString("1 FROM t")

	view := model.ViewInfo{Cols: viewCols, Security: model.SecurityDefiner, Algorithm: model.AlgorithmMerge,
		SelectStmt: stmtBuffer.String(), CheckOption: model.CheckOptionCascaded, Definer: &auth.UserIdentity{CurrentUser: true}}

	tblInfo.View = &view
	tblInfo.Columns = cols

	return tblInfo
}

func testCreateTable(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testCreateView(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo, false, 0},
	}

	require.True(t, tblInfo.IsView())
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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

	v := getSchemaVer(t, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
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

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

func checkTableLockedTest(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, serverID string, sessionID uint64, lockTp model.TableLockType) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		trans := meta.NewMeta(txn)
		info, err := trans.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		require.NotNil(t, info)
		require.NotNil(t, info.Lock)
		require.True(t, len(info.Lock.Sessions) == 1)
		require.Equal(t, info.Lock.Sessions[0].ServerID, serverID)
		require.Equal(t, info.Lock.Sessions[0].SessionID, sessionID)
		require.Equal(t, info.Lock.Tp, lockTp)
		require.Equal(t, info.Lock.State, model.TableLockStatePublic)
		return nil
	})
	require.NoError(t, err)
}

func testDropTable(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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

	v := getSchemaVer(t, ctx)
	tblInfo.ID = newTableID
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCheckTableState(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		trans := meta.NewMeta(txn)
		info, err := trans.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		if state == model.StateNone {
			require.Nil(t, info)
			return nil
		}
		require.Equal(t, info.Name, tblInfo.Name)
		require.Equal(t, info.State, state)
		return nil
	})
	require.NoError(t, err)
}

func testGetTable(t *testing.T, d *ddl, schemaID int64, tableID int64) table.Table {
	tbl, err := testGetTableWithError(d, schemaID, tableID)
	require.NoError(t, err)
	return tbl
}

func testGetTableWithError(d *ddl, schemaID, tableID int64) (table.Table, error) {
	var tblInfo *model.TableInfo
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		trans := meta.NewMeta(txn)
		var err1 error
		tblInfo, err1 = trans.GetTable(schemaID, tableID)
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

func TestTable(t *testing.T) {
	s, clean := createTableSuite(t)
	defer clean()
	d := s.d

	ctx := testNewContext(d)
	tblInfo := testTableInfo(t, d, "t", 3)
	job := testCreateTable(t, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(t, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)

	// Create an existing table.
	newTblInfo := testTableInfo(t, d, "t", 3)
	doDDLJobErr(t, s.dbInfo.ID, newTblInfo.ID, model.ActionCreateTable, []interface{}{newTblInfo}, ctx, d)

	count := 2000
	tbl := testGetTable(t, d, s.dbInfo.ID, tblInfo.ID)
	for i := 1; i <= count; i++ {
		_, err := tbl.AddRecord(ctx, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}

	job = testDropTable(t, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(t, d, job, false)

	// for truncate table

	tblInfo = testTableInfo(t, d, "tt", 3)
	job = testCreateTable(t, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(t, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)

	job = testTruncateTable(t, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(t, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)

	// for rename table
	dbInfo1 := testSchemaInfo(t, s.d, "test_rename_table")
	testCreateSchema(t, testNewContext(s.d), s.d, dbInfo1)
	job = testRenameTable(t, ctx, d, dbInfo1.ID, s.dbInfo.ID, s.dbInfo.Name, tblInfo)
	testCheckTableState(t, d, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)

	job = testLockTable(t, ctx, d, dbInfo1.ID, tblInfo, model.TableLockWrite)
	testCheckTableState(t, d, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)
	checkTableLockedTest(t, d, dbInfo1, tblInfo, d.GetID(), ctx.GetSessionVars().ConnectionID, model.TableLockWrite)
	// for alter cache table
	job = testAlterCacheTable(t, ctx, d, dbInfo1.ID, tblInfo)
	testCheckTableState(t, d, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)
	checkTableCacheTest(t, d, dbInfo1, tblInfo)
	// for alter no cache table
	job = testAlterNoCacheTable(t, ctx, d, dbInfo1.ID, tblInfo)
	testCheckTableState(t, d, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)
	checkTableNoCacheTest(t, d, dbInfo1, tblInfo)
}

func checkTableCacheTest(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		trans := meta.NewMeta(txn)
		info, err := trans.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.NotNil(t, info.TableCacheStatusType)
		require.Equal(t, info.TableCacheStatusType, model.TableCacheStatusEnable)
		return nil
}

func checkTableNoCacheTest(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, info.TableCacheStatusType, model.TableCacheStatusEnable)
		return nil
	})
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

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v})
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

	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v})
	return job
}

// for drop indexes
func createTestTableForDropIndexes(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, name string, num int) *model.TableInfo {
	tableInfo := testTableInfo(t, d, name, num)
	var idxs []*model.IndexInfo
	for i := 0; i < num; i++ {
		idxName := model.NewCIStr(fmt.Sprintf("i%d", i+1))
		idx := &model.IndexInfo{
			Name:    idxName,
			State:   model.StatePublic,
			Columns: []*model.IndexColumn{{Name: model.NewCIStr(fmt.Sprintf("c%d", i+1))}},
		}
		idxs = append(idxs, idx)
	}
	tableInfo.Indices = idxs
	testCreateTable(t, ctx, d, dbInfo, tableInfo)
	return tableInfo
}
