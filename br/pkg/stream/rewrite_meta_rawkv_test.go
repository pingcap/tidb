// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func ProduceValue(tableName string, dbID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   dbID,
		Name: model.NewCIStr(tableName),
	}

	return json.Marshal(tableInfo)
}

func TestRewriteValueForTable(t *testing.T) {
	var (
		tableName  = "person"
		tableID    = 57
		newTableID = 63
	)

	v, err := ProduceValue(tableName, int64(tableID))
	require.NoError(t, err)
	log.Info("old-value", zap.Int("value-len", len(v)), zap.ByteString("old-value", v), logutil.Key("old-value", v))

	v, err = ProduceValue(tableName, int64(newTableID))
	require.NoError(t, err)
	log.Info("new-value", zap.Int("value-len", len(v)), zap.ByteString("new-value", v), logutil.Key("new-value", v))
}

// db:70->80 -
//           | - t0:71->81 -
//           |             | - p0:72->82
//           |             | - p1:73->83
//           |             | - p2:74->84
//           | - t1:75->85

const (
	mDDLJobDBOldID int64 = 70 + iota
	mDDLJobTable0OldID
	mDDLJobPartition0OldID
	mDDLJobPartition1OldID
	mDDLJobPartition2OldID
	mDDLJobTable1OldID
)

const (
	mDDLJobDBNewID int64 = 80 + iota
	mDDLJobTable0NewID
	mDDLJobPartition0NewID
	mDDLJobPartition1NewID
	mDDLJobPartition2NewID
	mDDLJobTable1NewID
)

var (
	mDDLJobALLNewTableIDSet = map[int64]struct{}{
		mDDLJobTable0NewID:     {},
		mDDLJobPartition0NewID: {},
		mDDLJobPartition1NewID: {},
		mDDLJobPartition2NewID: {},
		mDDLJobTable1NewID:     {},
	}
	mDDLJobALLNewPartitionIDSet = map[int64]struct{}{
		mDDLJobPartition0NewID: {},
		mDDLJobPartition1NewID: {},
		mDDLJobPartition2NewID: {},
	}
	mDDLJobALLIndexesIDSet = map[int64]struct{}{
		2: {},
		3: {},
	}
)

var (
	dropSchemaJob           = &model.Job{Type: model.ActionDropSchema, SchemaID: mDDLJobDBOldID, RawArgs: json.RawMessage(`[[71,72,73,74,75]]`)}
	dropTable0Job           = &model.Job{Type: model.ActionDropTable, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",[72,73,74],[""]]`)}
	dropTable1Job           = &model.Job{Type: model.ActionDropTable, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",[],[""]]`)}
	dropTable0Partition1Job = &model.Job{Type: model.ActionDropTablePartition, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[73]]`)}
	rollBackTable0IndexJob  = &model.Job{Type: model.ActionAddIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,[72,73,74]]`)}
	rollBackTable1IndexJob  = &model.Job{Type: model.ActionAddIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,[]]`)}
	dropTable0IndexJob      = &model.Job{Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",2,[72,73,74]]`)}
	dropTable1IndexJob      = &model.Job{Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",2,[]]`)}
	dropTable0IndexesJob    = &model.Job{Type: model.ActionDropIndexes, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[72,73,74]]`)}
	dropTable1IndexesJob    = &model.Job{Type: model.ActionDropIndexes, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[]]`)}
	dropTable0ColumnJob     = &model.Job{Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",[2,3],[72,73,74]]`)}
	dropTable1ColumnJob     = &model.Job{Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",[2,3],[]]`)}
	dropTable0ColumnsJob    = &model.Job{Type: model.ActionDropColumns, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[72,73,74]]`)}
	dropTable1ColumnsJob    = &model.Job{Type: model.ActionDropColumns, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[]]`)}
	modifyTable0ColumnJob   = &model.Job{Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[2,3],[72,73,74]]`)}
	modifyTable1ColumnJob   = &model.Job{Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[2,3],[]]`)}
)

type TableDeletQueryArgs struct {
	tableIDs []int64
}

type IndexDeleteQueryArgs struct {
	tableID  int64
	indexIDs []int64
}

type mockInsertDeleteRange struct {
	tableCh chan TableDeletQueryArgs
	indexCh chan IndexDeleteQueryArgs
}

func newMockInsertDeleteRange() *mockInsertDeleteRange {
	// Since there is only single thread, we need to set the channel buf large enough.
	return &mockInsertDeleteRange{
		tableCh: make(chan TableDeletQueryArgs, 10),
		indexCh: make(chan IndexDeleteQueryArgs, 10),
	}
}

func (midr *mockInsertDeleteRange) mockInsertDeleteRangeForTable(jobID int64, tableIDs []int64) {
	midr.tableCh <- TableDeletQueryArgs{
		tableIDs: tableIDs,
	}
}

func (midr *mockInsertDeleteRange) mockInsertDeleteRangeForIndex(jobID int64, elementID *int64, tableID int64, indexIDs []int64) {
	midr.indexCh <- IndexDeleteQueryArgs{
		tableID:  tableID,
		indexIDs: indexIDs,
	}
}

func TestDeleteRangeForMDDLJob(t *testing.T) {
	schemaReplace := MockEmptySchemasReplace()
	partitionMap := map[int64]int64{
		mDDLJobPartition0OldID: mDDLJobPartition0NewID,
		mDDLJobPartition1OldID: mDDLJobPartition1NewID,
		mDDLJobPartition2OldID: mDDLJobPartition2NewID,
	}
	tableReplace0 := &TableReplace{
		NewTableID:   mDDLJobTable0NewID,
		PartitionMap: partitionMap,
	}
	tableReplace1 := &TableReplace{
		NewTableID: mDDLJobTable1NewID,
	}
	tableMap := map[int64]*TableReplace{
		mDDLJobTable0OldID: tableReplace0,
		mDDLJobTable1OldID: tableReplace1,
	}
	dbReplace := &DBReplace{
		NewDBID:  mDDLJobDBNewID,
		TableMap: tableMap,
	}
	schemaReplace.DbMap[mDDLJobDBOldID] = dbReplace

	midr := newMockInsertDeleteRange()

	var targs TableDeletQueryArgs
	var iargs IndexDeleteQueryArgs
	var err error
	// drop schema
	err = schemaReplace.deleteRange(dropSchemaJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), len(mDDLJobALLNewTableIDSet))
	for _, tableID := range targs.tableIDs {
		_, exist := mDDLJobALLNewTableIDSet[tableID]
		require.True(t, exist)
	}

	// drop table0
	err = schemaReplace.deleteRange(dropTable0Job, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), len(mDDLJobALLNewPartitionIDSet))
	for _, tableID := range targs.tableIDs {
		_, exist := mDDLJobALLNewPartitionIDSet[tableID]
		require.True(t, exist)
	}

	// drop table1
	err = schemaReplace.deleteRange(dropTable1Job, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), 1)
	require.Equal(t, targs.tableIDs[0], mDDLJobTable1NewID)

	// drop table partition1
	err = schemaReplace.deleteRange(dropTable0Partition1Job, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), 1)
	require.Equal(t, targs.tableIDs[0], mDDLJobPartition1NewID)

	// roll back add index for table0
	err = schemaReplace.deleteRange(rollBackTable0IndexJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), 1)
		require.Equal(t, iargs.indexIDs[0], int64(2))
	}

	// roll back add index for table1
	err = schemaReplace.deleteRange(rollBackTable1IndexJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), 1)
	require.Equal(t, iargs.indexIDs[0], int64(2))

	// drop index for table0
	err = schemaReplace.deleteRange(dropTable0IndexJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), 1)
		require.Equal(t, iargs.indexIDs[0], int64(2))
	}

	// drop index for table1
	err = schemaReplace.deleteRange(dropTable1IndexJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), 1)
	require.Equal(t, iargs.indexIDs[0], int64(2))

	// drop indexes for table0
	err = schemaReplace.deleteRange(dropTable0IndexesJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
		for _, indexID := range iargs.indexIDs {
			_, exist := mDDLJobALLIndexesIDSet[indexID]
			require.True(t, exist)
		}
	}

	// drop indexes for table1
	err = schemaReplace.deleteRange(dropTable1IndexesJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop column for table0
	err = schemaReplace.deleteRange(dropTable0ColumnJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
		for _, indexID := range iargs.indexIDs {
			_, exist := mDDLJobALLIndexesIDSet[indexID]
			require.True(t, exist)
		}
	}

	// drop column for table1
	err = schemaReplace.deleteRange(dropTable1ColumnJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop columns for table0
	err = schemaReplace.deleteRange(dropTable0ColumnsJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
		for _, indexID := range iargs.indexIDs {
			_, exist := mDDLJobALLIndexesIDSet[indexID]
			require.True(t, exist)
		}
	}

	// drop columns for table1
	err = schemaReplace.deleteRange(dropTable1ColumnsJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop columns for table0
	err = schemaReplace.deleteRange(modifyTable0ColumnJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
		for _, indexID := range iargs.indexIDs {
			_, exist := mDDLJobALLIndexesIDSet[indexID]
			require.True(t, exist)
		}
	}

	// drop columns for table1
	err = schemaReplace.deleteRange(modifyTable1ColumnJob, midr.mockInsertDeleteRangeForTable, midr.mockInsertDeleteRangeForIndex)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}
}
