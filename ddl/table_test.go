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

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func testTableInfoWith2IndexOnFirstColumn(c *C, d *ddl, name string, num int) *model.TableInfo {
	normalInfo, err := testTableInfo(d, name, num)
	c.Assert(err, IsNil)
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
func testTableInfo(d *ddl, name string, num int) (*model.TableInfo, error) {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)

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
		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
	}
	tblInfo.Columns = cols
	tblInfo.Charset = "utf8"
	tblInfo.Collate = "utf8_bin"
	return tblInfo, nil
}

// testTableInfoWithPartition creates a test table with num int columns and with no index.
func testTableInfoWithPartition(c *C, d *ddl, name string, num int) *model.TableInfo {
	tblInfo, err := testTableInfo(d, name, num)
	c.Assert(err, IsNil)
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
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
func testTableInfoWithPartitionLessThan(c *C, d *ddl, name string, num int, lessthan string) *model.TableInfo {
	tblInfo := testTableInfoWithPartition(c, d, name, num)
	tblInfo.Partition.Definitions[0].LessThan = []string{lessthan}
	return tblInfo
}

func testAddedNewTablePartitionInfo(c *C, d *ddl, tblInfo *model.TableInfo, partName, lessthan string) *model.PartitionInfo {
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
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
func testViewInfo(c *C, d *ddl, name string, num int) *model.TableInfo {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
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

func testCreateTable(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

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

func testCreateView(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateView,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo, false, 0},
	}

	c.Assert(tblInfo.IsView(), IsTrue)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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

func testDropTable(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
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

func testCheckTableState(c *C, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetTable(dbInfo.ID, tblInfo.ID)
		c.Assert(err, IsNil)

		if state == model.StateNone {
			c.Assert(info, IsNil)
			return nil
		}

		c.Assert(info.Name, DeepEquals, tblInfo.Name)
		c.Assert(info.State, Equals, state)
		return nil
	})
	c.Assert(err, IsNil)
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

func testGetTable(c *C, d *ddl, schemaID int64, tableID int64) table.Table {
	tbl, err := testGetTableWithError(d, schemaID, tableID)
	c.Assert(err, IsNil)
	return tbl
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

func TestTable(t *testing.T) {
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

// for drop indexes
func createTestTableForDropIndexes(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, name string, num int) *model.TableInfo {
	tableInfo, err := testTableInfo(d, name, num)
	c.Assert(err, IsNil)
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
	testCreateTable(c, ctx, d, dbInfo, tableInfo)
	return tableInfo
}
