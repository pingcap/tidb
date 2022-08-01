// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
)

var increaseID int64 = 100

func mockGenGenGlobalID(ctx context.Context) (int64, error) {
	increaseID++
	return increaseID, nil
}

func ProduceValue(tableName string, dbID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   dbID,
		Name: model.NewCIStr(tableName),
	}

	return json.Marshal(tableInfo)
}

func MockEmptySchemasReplace(midr *mockInsertDeleteRange) *SchemasReplace {
	dbMap := make(map[OldID]*DBReplace)
	if midr == nil {
		midr = newMockInsertDeleteRange()
	}
	return NewSchemasReplace(
		dbMap,
		0,
		filter.All(),
		mockGenGenGlobalID,
		nil,
		midr.mockInsertDeleteRangeForTable,
		midr.mockInsertDeleteRangeForIndex,
	)
}

func produceDBInfoValue(dbName string, dbID int64) ([]byte, error) {
	dbInfo := model.DBInfo{
		ID:   dbID,
		Name: model.NewCIStr(dbName),
	}
	return json.Marshal(&dbInfo)
}

func produceTableInfoValue(tableName string, tableID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   tableID,
		Name: model.NewCIStr(tableName),
	}

	return json.Marshal(&tableInfo)
}

func TestRewriteValueForDB(t *testing.T) {
	var (
		dbID   int64 = 1
		dbName       = "db1"
		DBInfo model.DBInfo
	)

	value, err := produceDBInfoValue(dbName, dbID)
	require.Nil(t, err)

	sr := MockEmptySchemasReplace(nil)
	newValue, needWrite, err := sr.rewriteDBInfo(value)
	require.Nil(t, err)
	require.True(t, needWrite)

	err = json.Unmarshal(newValue, &DBInfo)
	require.Nil(t, err)
	require.Equal(t, DBInfo.ID, sr.DbMap[dbID].NewDBID)

	newId := sr.DbMap[dbID].NewDBID
	newValue, needWrite, err = sr.rewriteDBInfo(value)
	require.Nil(t, err)
	require.True(t, needWrite)

	err = json.Unmarshal(newValue, &DBInfo)
	require.Nil(t, err)
	require.Equal(t, DBInfo.ID, sr.DbMap[dbID].NewDBID)
	require.Equal(t, newId, sr.DbMap[dbID].NewDBID)
}

func TestRewriteValueForTable(t *testing.T) {
	var (
		dbId      int64 = 40
		tableID   int64 = 100
		tableName       = "t1"
		tableInfo model.TableInfo
	)

	value, err := produceTableInfoValue(tableName, tableID)
	require.Nil(t, err)

	sr := MockEmptySchemasReplace(nil)
	newValue, needRewrite, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)

	newID := sr.DbMap[dbId].TableMap[tableID].NewTableID
	newValue, needRewrite, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.Equal(t, newID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
}

func TestRewriteValueForPartitionTable(t *testing.T) {
	var (
		dbId      int64 = 40
		tableID   int64 = 100
		pt1ID     int64 = 101
		pt2ID     int64 = 102
		tableName       = "t1"
		pt1Name         = "pt1"
		pt2Name         = "pt2"
		tableInfo model.TableInfo
	)

	pt1 := model.PartitionDefinition{
		ID:   pt1ID,
		Name: model.NewCIStr(pt1Name),
	}
	pt2 := model.PartitionDefinition{
		ID:   pt2ID,
		Name: model.NewCIStr(pt2Name),
	}

	pi := model.PartitionInfo{
		Enable:      true,
		Definitions: make([]model.PartitionDefinition, 0),
	}
	pi.Definitions = append(pi.Definitions, pt1)
	pi.Definitions = append(pi.Definitions, pt2)

	tbl := model.TableInfo{
		ID:        tableID,
		Name:      model.NewCIStr(tableName),
		Partition: &pi,
	}
	value, err := json.Marshal(&tbl)
	require.Nil(t, err)

	sr := MockEmptySchemasReplace(nil)
	newValue, needRewrite, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[0].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt1ID],
	)
	require.Equal(
		t,
		tbl.Partition.Definitions[0].Name,
		tableInfo.Partition.Definitions[0].Name,
	)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[1].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt2ID],
	)
	require.Equal(
		t,
		tbl.Partition.Definitions[1].Name,
		tableInfo.Partition.Definitions[1].Name,
	)

	newID1 := sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt1ID]
	newID2 := sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt2ID]
	newValue, needRewrite, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[0].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt1ID],
	)
	require.Equal(t, tableInfo.Partition.Definitions[0].ID, newID1)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[1].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt2ID],
	)
	require.Equal(t, tableInfo.Partition.Definitions[1].ID, newID2)
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
	rollBackTable0IndexJob  = &model.Job{Type: model.ActionAddIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	rollBackTable1IndexJob  = &model.Job{Type: model.ActionAddIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
	dropTable0IndexJob      = &model.Job{Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",false,2,[72,73,74]]`)}
	dropTable1IndexJob      = &model.Job{Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",false,2,[]]`)}
	dropTable0IndexesJob    = &model.Job{Type: model.ActionDropIndexes, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[72,73,74]]`)}
	dropTable1IndexesJob    = &model.Job{Type: model.ActionDropIndexes, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[]]`)}
	dropTable0ColumnJob     = &model.Job{Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",false,[2,3],[72,73,74]]`)}
	dropTable1ColumnJob     = &model.Job{Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",false,[2,3],[]]`)}
	dropTable0ColumnsJob    = &model.Job{Type: model.ActionDropColumns, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[72,73,74]]`)}
	dropTable1ColumnsJob    = &model.Job{Type: model.ActionDropColumns, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[],[],[2,3],[]]`)}
	modifyTable0ColumnJob   = &model.Job{Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[2,3],[72,73,74]]`)}
	modifyTable1ColumnJob   = &model.Job{Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[2,3],[]]`)}
	multiSchemaChangeJob0   = &model.Job{
		Type:     model.ActionMultiSchemaChange,
		SchemaID: mDDLJobDBOldID,
		TableID:  mDDLJobTable0OldID,
		MultiSchemaInfo: &model.MultiSchemaInfo{
			SubJobs: []*model.SubJob{
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k1","L":"k1"},false,1,[72,73,74]]`),
				},
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k2","L":"k2"},false,2,[72,73,74]]`),
				},
			},
		},
	}
	multiSchemaChangeJob1 = &model.Job{
		Type:     model.ActionMultiSchemaChange,
		SchemaID: mDDLJobDBOldID,
		TableID:  mDDLJobTable1OldID,
		MultiSchemaInfo: &model.MultiSchemaInfo{
			SubJobs: []*model.SubJob{
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k1","L":"k1"},false,1,[]]`),
				},
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k2","L":"k2"},false,2,[]]`),
				},
			},
		},
	}
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
	midr := newMockInsertDeleteRange()
	schemaReplace := MockEmptySchemasReplace(midr)
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

	var targs TableDeletQueryArgs
	var iargs IndexDeleteQueryArgs
	var err error
	// drop schema
	err = schemaReplace.deleteRange(dropSchemaJob)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), len(mDDLJobALLNewTableIDSet))
	for _, tableID := range targs.tableIDs {
		_, exist := mDDLJobALLNewTableIDSet[tableID]
		require.True(t, exist)
	}

	// drop table0
	err = schemaReplace.deleteRange(dropTable0Job)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), len(mDDLJobALLNewPartitionIDSet))
	for _, tableID := range targs.tableIDs {
		_, exist := mDDLJobALLNewPartitionIDSet[tableID]
		require.True(t, exist)
	}

	// drop table1
	err = schemaReplace.deleteRange(dropTable1Job)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), 1)
	require.Equal(t, targs.tableIDs[0], mDDLJobTable1NewID)

	// drop table partition1
	err = schemaReplace.deleteRange(dropTable0Partition1Job)
	require.NoError(t, err)
	targs = <-midr.tableCh
	require.Equal(t, len(targs.tableIDs), 1)
	require.Equal(t, targs.tableIDs[0], mDDLJobPartition1NewID)

	// roll back add index for table0
	err = schemaReplace.deleteRange(rollBackTable0IndexJob)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), 1)
		require.Equal(t, iargs.indexIDs[0], int64(2))
	}

	// roll back add index for table1
	err = schemaReplace.deleteRange(rollBackTable1IndexJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), 1)
	require.Equal(t, iargs.indexIDs[0], int64(2))

	// drop index for table0
	err = schemaReplace.deleteRange(dropTable0IndexJob)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), 1)
		require.Equal(t, iargs.indexIDs[0], int64(2))
	}

	// drop index for table1
	err = schemaReplace.deleteRange(dropTable1IndexJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), 1)
	require.Equal(t, iargs.indexIDs[0], int64(2))

	// drop indexes for table0
	err = schemaReplace.deleteRange(dropTable0IndexesJob)
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
	err = schemaReplace.deleteRange(dropTable1IndexesJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop column for table0
	err = schemaReplace.deleteRange(dropTable0ColumnJob)
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
	err = schemaReplace.deleteRange(dropTable1ColumnJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop columns for table0
	err = schemaReplace.deleteRange(dropTable0ColumnsJob)
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
	err = schemaReplace.deleteRange(dropTable1ColumnsJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop columns for table0
	err = schemaReplace.deleteRange(modifyTable0ColumnJob)
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
	err = schemaReplace.deleteRange(modifyTable1ColumnJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), len(mDDLJobALLIndexesIDSet))
	for _, indexID := range iargs.indexIDs {
		_, exist := mDDLJobALLIndexesIDSet[indexID]
		require.True(t, exist)
	}

	// drop indexes(multi-schema-change) for table0
	err = schemaReplace.tryToGCJob(multiSchemaChangeJob0)
	require.NoError(t, err)
	for l := 0; l < 2; l++ {
		for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
			iargs = <-midr.indexCh
			_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
			require.True(t, exist)
			require.Equal(t, len(iargs.indexIDs), 1)
			require.Equal(t, iargs.indexIDs[0], int64(l+1))
		}
	}

	// drop indexes(multi-schema-change) for table1
	err = schemaReplace.tryToGCJob(multiSchemaChangeJob1)
	require.NoError(t, err)
	for l := 0; l < 2; l++ {
		iargs = <-midr.indexCh
		require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
		require.Equal(t, len(iargs.indexIDs), 1)
		require.Equal(t, iargs.indexIDs[0], int64(l+1))
	}
}
