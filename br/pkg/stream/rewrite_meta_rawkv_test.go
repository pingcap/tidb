// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"

<<<<<<< HEAD
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	filter "github.com/pingcap/tidb/util/table-filter"
=======
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	"github.com/stretchr/testify/require"
)

var increaseID int64 = 100

func mockGenGenGlobalID(ctx context.Context) (int64, error) {
	increaseID++
	return increaseID, nil
}

<<<<<<< HEAD
func MockEmptySchemasReplace(midr *mockInsertDeleteRange) *SchemasReplace {
	dbMap := make(map[OldID]*DBReplace)
=======
func MockEmptySchemasReplace(midr *mockInsertDeleteRange, dbMap map[UpstreamID]*DBReplace) *SchemasReplace {
	if dbMap == nil {
		dbMap = make(map[UpstreamID]*DBReplace)
	}
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	if midr == nil {
		midr = newMockInsertDeleteRange()
	}
	return NewSchemasReplace(
		dbMap,
		0,
		filter.All(),
		mockGenGenGlobalID,
		nil,
		midr.mockRecordDeleteRange,
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

<<<<<<< HEAD
func TestRewriteValueForDB(t *testing.T) {
=======
func TestTidySchemaMaps(t *testing.T) {
	var (
		dbName, tblName            string       = "db1", "t1"
		oldDBID                    UpstreamID   = 100
		newDBID                    DownstreamID = 200
		oldTblID, oldPID1, oldPID2 UpstreamID   = 101, 102, 103
		newTblID, newPID1, newPID2 DownstreamID = 201, 202, 203
	)

	// create table Replace
	tr := NewTableReplace(tblName, newTblID)
	tr.PartitionMap[oldPID1] = newPID1
	tr.PartitionMap[oldPID2] = newPID2

	dr := NewDBReplace(dbName, newDBID)
	dr.TableMap[oldTblID] = tr

	drs := make(map[UpstreamID]*DBReplace)
	drs[oldDBID] = dr

	// create schemas replace and test TidySchemaMaps().
	sr := NewSchemasReplace(drs, true, nil, 0, filter.All(), nil, nil, nil)
	globalTableIdMap := sr.globalTableIdMap
	require.Equal(t, len(globalTableIdMap), 3)
	require.Equal(t, globalTableIdMap[oldTblID], newTblID)
	require.Equal(t, globalTableIdMap[oldPID1], newPID1)
	require.Equal(t, globalTableIdMap[oldPID2], newPID2)

	dbMap := sr.TidySchemaMaps()
	require.Equal(t, len(dbMap), 1)
	require.Equal(t, dbMap[0].Name, dbName)
	require.Equal(t, dbMap[0].IdMap.UpstreamId, oldDBID)
	require.Equal(t, dbMap[0].IdMap.DownstreamId, newDBID)

	tableMap := dbMap[0].Tables
	require.Equal(t, len(tableMap), 1)
	require.Equal(t, tableMap[0].Name, tblName)
	require.Equal(t, tableMap[0].IdMap.UpstreamId, oldTblID)
	require.Equal(t, tableMap[0].IdMap.DownstreamId, newTblID)

	partitionMap := tableMap[0].Partitions
	require.Equal(t, len(partitionMap), 2)

	if partitionMap[0].UpstreamId == oldPID1 {
		require.Equal(t, partitionMap[0].DownstreamId, newPID1)
		require.Equal(t, partitionMap[1].UpstreamId, oldPID2)
		require.Equal(t, partitionMap[1].DownstreamId, newPID2)
	} else {
		require.Equal(t, partitionMap[0].DownstreamId, newPID2)
		require.Equal(t, partitionMap[1].UpstreamId, oldPID1)
		require.Equal(t, partitionMap[1].DownstreamId, newPID1)
	}

	// test FromSchemaMaps()
	drs2 := FromSchemaMaps(dbMap)
	require.Equal(t, drs2, drs)
}

func TestRewriteKeyForDB(t *testing.T) {
	var (
		dbID int64  = 1
		ts   uint64 = 1234
		mDbs        = []byte("DBs")
	)

	encodedKey := encodeTxnMetaKey(mDbs, meta.DBkey(dbID), ts)

	// create schemasReplace.
	sr := MockEmptySchemasReplace(nil, nil)

	// preConstruct Map information.
	sr.SetPreConstructMapStatus()
	newKey, err := sr.rewriteKeyForDB(encodedKey, WriteCF)
	require.Nil(t, err)
	require.Nil(t, newKey)
	require.Equal(t, len(sr.DbMap[dbID].TableMap), 0)
	downID := sr.DbMap[dbID].DbID

	// set restoreKV status and rewrite it.
	sr.SetRestoreKVStatus()
	newKey, err = sr.rewriteKeyForDB(encodedKey, DefaultCF)
	require.Nil(t, err)
	decodedKey, err := ParseTxnMetaKeyFrom(newKey)
	require.Nil(t, err)
	require.Equal(t, decodedKey.Ts, ts)
	newDBID, err := meta.ParseDBKey(decodedKey.Field)
	require.Nil(t, err)
	require.Equal(t, newDBID, downID)

	// rewrite it again, and get the same result.
	newKey, err = sr.rewriteKeyForDB(encodedKey, WriteCF)
	require.Nil(t, err)
	decodedKey, err = ParseTxnMetaKeyFrom(newKey)
	require.Nil(t, err)
	require.Equal(t, decodedKey.Ts, sr.RewriteTS)
	newDBID, err = meta.ParseDBKey(decodedKey.Field)
	require.Nil(t, err)
	require.Equal(t, newDBID, downID)
}

func TestRewriteDBInfo(t *testing.T) {
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	var (
		dbID   int64 = 1
		dbName       = "db1"
		DBInfo model.DBInfo
	)

	value, err := produceDBInfoValue(dbName, dbID)
	require.Nil(t, err)

<<<<<<< HEAD
	sr := MockEmptySchemasReplace(nil)
	newValue, needWrite, err := sr.rewriteDBInfo(value)
=======
	// create schemasReplace.
	sr := MockEmptySchemasReplace(nil, nil)

	// rewrite it directly without preConstruct Map, it will get failed result.
	sr.SetRestoreKVStatus()
	_, err = sr.rewriteDBInfo(value)
	require.Error(t, err)

	// ConstructMap status.
	sr.SetPreConstructMapStatus()
	newValue, err := sr.rewriteDBInfo(value)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
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

func TestRewriteKeyForTable(t *testing.T) {
	var (
		dbID    int64  = 1
		tableID int64  = 57
		ts      uint64 = 400036290571534337
	)
	cases := []struct {
		encodeTableFn func(int64) []byte
		decodeTableFn func([]byte) (int64, error)
	}{
		{
			meta.TableKey,
			meta.ParseTableKey,
		},
		{
			meta.AutoIncrementIDKey,
			meta.ParseAutoIncrementIDKey,
		},
		{
			meta.AutoTableIDKey,
			meta.ParseAutoTableIDKey,
		},
		{
			meta.AutoRandomTableIDKey,
			meta.ParseAutoRandomTableIDKey,
		},
		{
			meta.SequenceKey,
			meta.ParseSequenceKey,
		},
	}

	for _, ca := range cases {
		encodedKey := encodeTxnMetaKey(meta.DBkey(dbID), ca.encodeTableFn(tableID), ts)
		// create schemasReplace.
		sr := MockEmptySchemasReplace(nil, nil)

		newKey, needWrite, err := sr.rewriteKeyForTable(encodedKey, DefaultCF, ca.decodeTableFn, ca.encodeTableFn)
		require.Nil(t, err)
		require.True(t, needWrite)
		require.Equal(t, len(sr.DbMap), 1)
		require.Equal(t, len(sr.DbMap[dbID].TableMap), 1)
		downStreamDbID := sr.DbMap[dbID].NewDBID
		downStreamTblID := sr.DbMap[dbID].TableMap[tableID].NewTableID

		decodedKey, err := ParseTxnMetaKeyFrom(newKey)
		require.Nil(t, err)
		require.Equal(t, decodedKey.Ts, ts)

		newDbID, err := meta.ParseDBKey(decodedKey.Key)
		require.Nil(t, err)
		require.Equal(t, newDbID, downStreamDbID)
		newTblID, err := ca.decodeTableFn(decodedKey.Field)
		require.Nil(t, err)
		require.Equal(t, newTblID, downStreamTblID)

		// rewrite it again, and get the same result.
		newKey, needWrite, err = sr.rewriteKeyForTable(encodedKey, WriteCF, ca.decodeTableFn, ca.encodeTableFn)
		require.True(t, needWrite)
		require.Nil(t, err)
		decodedKey, err = ParseTxnMetaKeyFrom(newKey)
		require.Nil(t, err)
		require.Equal(t, decodedKey.Ts, sr.RewriteTS)

		newDbID, err = meta.ParseDBKey(decodedKey.Key)
		require.Nil(t, err)
		require.Equal(t, newDbID, downStreamDbID)
		newTblID, err = ca.decodeTableFn(decodedKey.Field)
		require.Nil(t, err)
		require.Equal(t, newTblID, downStreamTblID)
	}
}

func TestRewriteTableInfo(t *testing.T) {
	var (
		dbId      int64 = 40
		tableID   int64 = 100
		tableName       = "t1"
		tableInfo model.TableInfo
	)

	value, err := produceTableInfoValue(tableName, tableID)
	require.Nil(t, err)

<<<<<<< HEAD
	sr := MockEmptySchemasReplace(nil)
=======
	// create schemasReplace.
	sr := MockEmptySchemasReplace(nil, nil)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	tableCount := 0
	sr.AfterTableRewritten = func(deleted bool, tableInfo *model.TableInfo) {
		tableCount++
		tableInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count: 1,
		}
	}
	newValue, needRewrite, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.EqualValues(t, tableInfo.TiFlashReplica.Count, 1)

	newID := sr.DbMap[dbId].TableMap[tableID].NewTableID
	newValue, needRewrite, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.Equal(t, newID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.EqualValues(t, tableCount, 2)
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

<<<<<<< HEAD
	sr := MockEmptySchemasReplace(nil)
	newValue, needRewrite, err := sr.rewriteTableInfo(value, dbId)
=======
	// create schemasReplace, and preConstructMap.
	sr := MockEmptySchemasReplace(nil, nil)
	sr.SetPreConstructMapStatus()
	newValue, err := sr.rewriteTableInfo(value, dbId)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
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

func TestRewriteValueForExchangePartition(t *testing.T) {
	var (
		dbID1      int64 = 100
		tableID1   int64 = 101
		pt1ID      int64 = 102
		pt2ID      int64 = 103
		tableName1       = "t1"
		pt1Name          = "pt1"
		pt2Name          = "pt2"

		dbID2      int64 = 105
		tableID2   int64 = 106
		tableName2       = "t2"
		tableInfo  model.TableInfo
	)

	// construct the partition table t1
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
	pi.Definitions = append(pi.Definitions, pt1, pt2)
	t1 := model.TableInfo{
		ID:        tableID1,
		Name:      model.NewCIStr(tableName1),
		Partition: &pi,
	}
	db1 := model.DBInfo{
		ID: dbID1,
	}

	// construct the no partition table t2
	t2 := model.TableInfo{
		ID:   tableID2,
		Name: model.NewCIStr(tableName2),
	}
	db2 := model.DBInfo{
		ID: dbID2,
	}

	// construct the SchemaReplace
	dbMap := make(map[OldID]*DBReplace)
	dbMap[dbID1] = NewDBReplace(&db1, dbID1+100)
	dbMap[dbID1].TableMap[tableID1] = NewTableReplace(&t1, tableID1+100)
	dbMap[dbID1].TableMap[tableID1].PartitionMap[pt1ID] = pt1ID + 100
	dbMap[dbID1].TableMap[tableID1].PartitionMap[pt2ID] = pt2ID + 100

	dbMap[dbID2] = NewDBReplace(&db2, dbID2+100)
	dbMap[dbID2].TableMap[tableID2] = NewTableReplace(&t2, tableID2+100)

	sr := NewSchemasReplace(
		dbMap,
		0,
		filter.All(),
		mockGenGenGlobalID,
		nil,
		nil,
	)
	require.Equal(t, len(sr.globalTableIdMap), 4)

	//exchange parition, t1 parition0 with the t2
	t1Copy := t1.Clone()
	t1Copy.Partition = t1.Partition.Clone()
	t2Copy := t2.Clone()

	t1Copy.Partition.Definitions[0].ID = tableID2
	t2Copy.ID = pt1ID

	// rewrite partition table
	value, err := json.Marshal(&t1Copy)
	require.Nil(t, err)
	value, needRewrite, err := sr.rewriteTableInfo(value, dbID1)
	require.Nil(t, err)
	require.True(t, needRewrite)
	err = json.Unmarshal(value, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, tableID1+100)
	require.Equal(t, tableInfo.Partition.Definitions[0].ID, tableID2+100)
	require.Equal(t, tableInfo.Partition.Definitions[1].ID, pt2ID+100)

	// rewrite no partition table
	value, err = json.Marshal(&t2Copy)
	require.Nil(t, err)
	value, needRewrite, err = sr.rewriteTableInfo(value, dbID2)
	require.Nil(t, err)
	require.True(t, needRewrite)
	err = json.Unmarshal(value, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, pt1ID+100)
}

<<<<<<< HEAD
=======
func TestRewriteTableInfoForTTLTable(t *testing.T) {
	var (
		dbId      int64 = 40
		tableID   int64 = 100
		colID     int64 = 1000
		colName         = "t"
		tableName       = "t1"
		tableInfo model.TableInfo
	)

	tbl := model.TableInfo{
		ID:   tableID,
		Name: model.NewCIStr(tableName),
		Columns: []*model.ColumnInfo{
			{
				ID:        colID,
				Name:      model.NewCIStr(colName),
				FieldType: *types.NewFieldType(mysql.TypeTimestamp),
			},
		},
		TTLInfo: &model.TTLInfo{
			ColumnName:       model.NewCIStr(colName),
			IntervalExprStr:  "1",
			IntervalTimeUnit: int(ast.TimeUnitDay),
			Enable:           true,
		},
	}
	value, err := json.Marshal(&tbl)
	require.Nil(t, err)

	// create empty schemasReplace
	sr := MockEmptySchemasReplace(nil, nil)

	// preConsutruct Map information.
	sr.SetPreConstructMapStatus()
	newValue, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.Nil(t, newValue)

	// set restoreKV status and rewrite it.
	sr.SetRestoreKVStatus()
	newValue, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].TableID)
	require.NotNil(t, tableInfo.TTLInfo)
	require.Equal(t, colName, tableInfo.TTLInfo.ColumnName.O)
	require.Equal(t, "1", tableInfo.TTLInfo.IntervalExprStr)
	require.Equal(t, int(ast.TimeUnitDay), tableInfo.TTLInfo.IntervalTimeUnit)
	require.False(t, tableInfo.TTLInfo.Enable)
}

func TestIsPreConsturctMapStatus(t *testing.T) {
	// create empty schemasReplace
	sr := MockEmptySchemasReplace(nil, nil)
	sr.SetPreConstructMapStatus()
	require.True(t, sr.IsPreConsturctMapStatus())
	require.False(t, sr.IsRestoreKVStatus())

	sr.SetRestoreKVStatus()
	require.False(t, sr.IsPreConsturctMapStatus())
	require.True(t, sr.IsRestoreKVStatus())
}

>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
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
	mDDLJobALLNewTableKeySet = map[string]struct{}{
		encodeTableKey(mDDLJobTable0NewID):     {},
		encodeTableKey(mDDLJobPartition0NewID): {},
		encodeTableKey(mDDLJobPartition1NewID): {},
		encodeTableKey(mDDLJobPartition2NewID): {},
		encodeTableKey(mDDLJobTable1NewID):     {},
	}
	mDDLJobALLNewPartitionIDSet = map[int64]struct{}{
		mDDLJobPartition0NewID: {},
		mDDLJobPartition1NewID: {},
		mDDLJobPartition2NewID: {},
	}
	mDDLJobALLNewPartitionKeySet = map[string]struct{}{
		encodeTableKey(mDDLJobPartition0NewID): {},
		encodeTableKey(mDDLJobPartition1NewID): {},
		encodeTableKey(mDDLJobPartition2NewID): {},
	}
	mDDLJobALLNewPartitionIndex2KeySet = map[string]struct{}{
		encodeTableIndexKey(mDDLJobPartition0NewID, 2): {},
		encodeTableIndexKey(mDDLJobPartition1NewID, 2): {},
		encodeTableIndexKey(mDDLJobPartition2NewID, 2): {},
	}
	mDDLJobALLNewPartitionIndex3KeySet = map[string]struct{}{
		encodeTableIndexKey(mDDLJobPartition0NewID, 3): {},
		encodeTableIndexKey(mDDLJobPartition1NewID, 3): {},
		encodeTableIndexKey(mDDLJobPartition2NewID, 3): {},
	}
	tempIndex2                             = tablecodec.TempIndexPrefix | int64(2)
	mDDLJobALLNewPartitionTempIndex2KeySet = map[string]struct{}{
		encodeTableIndexKey(mDDLJobPartition0NewID, tempIndex2): {},
		encodeTableIndexKey(mDDLJobPartition1NewID, tempIndex2): {},
		encodeTableIndexKey(mDDLJobPartition2NewID, tempIndex2): {},
	}
	mDDLJobALLIndexesIDSet = map[int64]struct{}{
		2: {},
		3: {},
	}
	mDDLJobAllIndexesKeySet = []map[string]struct{}{
		mDDLJobALLNewPartitionIndex2KeySet, mDDLJobALLNewPartitionIndex3KeySet,
	}
)

var (
<<<<<<< HEAD
	dropSchemaJob           = &model.Job{Type: model.ActionDropSchema, SchemaID: mDDLJobDBOldID, RawArgs: json.RawMessage(`[[71,72,73,74,75]]`)}
	dropTable0Job           = &model.Job{Type: model.ActionDropTable, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",[72,73,74],[""]]`)}
	dropTable1Job           = &model.Job{Type: model.ActionDropTable, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",[],[""]]`)}
	dropTable0Partition1Job = &model.Job{Type: model.ActionDropTablePartition, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[73]]`)}
	rollBackTable0IndexJob  = &model.Job{Type: model.ActionAddIndex, State: model.JobStateRollbackDone, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	rollBackTable1IndexJob  = &model.Job{Type: model.ActionAddIndex, State: model.JobStateRollbackDone, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
	addTable0IndexJob       = &model.Job{Type: model.ActionAddIndex, State: model.JobStateSynced, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	addTable1IndexJob       = &model.Job{Type: model.ActionAddIndex, State: model.JobStateSynced, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
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
=======
	dropSchemaJob                 = &model.Job{Type: model.ActionDropSchema, SchemaID: mDDLJobDBOldID, RawArgs: json.RawMessage(`[[71,72,73,74,75]]`)}
	dropTable0Job                 = &model.Job{Type: model.ActionDropTable, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",[72,73,74],[""]]`)}
	dropTable1Job                 = &model.Job{Type: model.ActionDropTable, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",[],[""]]`)}
	dropTable0Partition1Job       = &model.Job{Type: model.ActionDropTablePartition, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[73]]`)}
	reorganizeTable0Partition1Job = &model.Job{Type: model.ActionReorganizePartition, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[73]]`)}
	removeTable0Partition1Job     = &model.Job{Type: model.ActionRemovePartitioning, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[73]]`)}
	alterTable0Partition1Job      = &model.Job{Type: model.ActionAlterTablePartitioning, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[73]]`)}
	rollBackTable0IndexJob        = &model.Job{Type: model.ActionAddIndex, State: model.JobStateRollbackDone, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	rollBackTable1IndexJob        = &model.Job{Type: model.ActionAddIndex, State: model.JobStateRollbackDone, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
	addTable0IndexJob             = &model.Job{Type: model.ActionAddIndex, State: model.JobStateSynced, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[2,false,[72,73,74]]`)}
	addTable1IndexJob             = &model.Job{Type: model.ActionAddIndex, State: model.JobStateSynced, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[2,false,[]]`)}
	dropTable0IndexJob            = &model.Job{Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",false,2,[72,73,74]]`)}
	dropTable1IndexJob            = &model.Job{Type: model.ActionDropIndex, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",false,2,[]]`)}
	dropTable0ColumnJob           = &model.Job{Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`["",false,[2,3],[72,73,74]]`)}
	dropTable1ColumnJob           = &model.Job{Type: model.ActionDropColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`["",false,[2,3],[]]`)}
	modifyTable0ColumnJob         = &model.Job{Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable0OldID, RawArgs: json.RawMessage(`[[2,3],[72,73,74]]`)}
	modifyTable1ColumnJob         = &model.Job{Type: model.ActionModifyColumn, SchemaID: mDDLJobDBOldID, TableID: mDDLJobTable1OldID, RawArgs: json.RawMessage(`[[2,3],[]]`)}
	multiSchemaChangeJob0         = &model.Job{
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
		Type:     model.ActionMultiSchemaChange,
		SchemaID: mDDLJobDBOldID,
		TableID:  mDDLJobTable0OldID,
		MultiSchemaInfo: &model.MultiSchemaInfo{
			SubJobs: []*model.SubJob{
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k1","L":"k1"},false,2,[72,73,74]]`),
				},
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k2","L":"k2"},false,3,[72,73,74]]`),
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
					RawArgs: json.RawMessage(`[{"O":"k1","L":"k1"},false,2,[]]`),
				},
				{
					Type:    model.ActionDropIndex,
					RawArgs: json.RawMessage(`[{"O":"k2","L":"k2"},false,3,[]]`),
				},
			},
		},
	}
)

type mockInsertDeleteRange struct {
	queryCh chan *PreDelRangeQuery
}

func newMockInsertDeleteRange() *mockInsertDeleteRange {
	// Since there is only single thread, we need to set the channel buf large enough.
	return &mockInsertDeleteRange{
		queryCh: make(chan *PreDelRangeQuery, 10),
	}
}

func (midr *mockInsertDeleteRange) mockRecordDeleteRange(query *PreDelRangeQuery) {
	midr.queryCh <- query
}

func encodeTableKey(tableID int64) string {
	key := tablecodec.EncodeTablePrefix(tableID)
	return hex.EncodeToString(key)
}

func encodeTableIndexKey(tableID, indexID int64) string {
	key := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	return hex.EncodeToString(key)
}

func TestDeleteRangeForMDDLJob(t *testing.T) {
	midr := newMockInsertDeleteRange()
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
	schemaReplace := MockEmptySchemasReplace(midr, map[int64]*DBReplace{
		mDDLJobDBOldID: dbReplace,
	})

	var qargs *PreDelRangeQuery
	// drop schema
	err := schemaReplace.restoreFromHistory(dropSchemaJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLNewTableIDSet))
	for _, params := range qargs.ParamsList {
		_, exist := mDDLJobALLNewTableKeySet[params.StartKey]
		require.True(t, exist)
	}

	// drop table0
	err = schemaReplace.restoreFromHistory(dropTable0Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLNewPartitionIDSet))
	for _, params := range qargs.ParamsList {
		_, exist := mDDLJobALLNewPartitionKeySet[params.StartKey]
		require.True(t, exist)
	}
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, qargs.ParamsList[0].StartKey, encodeTableKey(mDDLJobTable0NewID))

	// drop table1
	err = schemaReplace.restoreFromHistory(dropTable1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, qargs.ParamsList[0].StartKey, encodeTableKey(mDDLJobTable1NewID))

	// drop table partition1
	err = schemaReplace.restoreFromHistory(dropTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, qargs.ParamsList[0].StartKey, encodeTableKey(mDDLJobPartition1NewID))

	// reorganize table partition1
	err = schemaReplace.restoreFromHistory(reorganizeTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableKey(mDDLJobPartition1NewID), qargs.ParamsList[0].StartKey)

	// remove table partition1
	err = schemaReplace.restoreFromHistory(removeTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableKey(mDDLJobPartition1NewID), qargs.ParamsList[0].StartKey)

	// alter table partition1
	err = schemaReplace.restoreFromHistory(alterTable0Partition1Job)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableKey(mDDLJobPartition1NewID), qargs.ParamsList[0].StartKey)

	// roll back add index for table0
<<<<<<< HEAD
	err = schemaReplace.tryToGCJob(rollBackTable0IndexJob)
	require.NoError(t, err)
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		iargs = <-midr.indexCh
		_, exist := mDDLJobALLNewPartitionIDSet[iargs.tableID]
		require.True(t, exist)
		require.Equal(t, len(iargs.indexIDs), 2)
		require.Equal(t, iargs.indexIDs[0], int64(2))
		require.Equal(t, iargs.indexIDs[1], int64(tablecodec.TempIndexPrefix|2))
	}

	// roll back add index for table1
	err = schemaReplace.tryToGCJob(rollBackTable1IndexJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), 2)
	require.Equal(t, iargs.indexIDs[0], int64(2))
	require.Equal(t, iargs.indexIDs[1], int64(tablecodec.TempIndexPrefix|2))

	// add index for table 0
	err = schemaReplace.tryToGCJob(addTable0IndexJob)
=======
	err = schemaReplace.restoreFromHistory(rollBackTable0IndexJob)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	require.NoError(t, err)
	oldPartitionIDMap := make(map[string]struct{})
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 2)
		for _, params := range qargs.ParamsList {
			_, exist := oldPartitionIDMap[params.StartKey]
			require.False(t, exist)
			oldPartitionIDMap[params.StartKey] = struct{}{}
		}

		// index ID
		_, exist := mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
		// temp index ID
		_, exist = mDDLJobALLNewPartitionTempIndex2KeySet[qargs.ParamsList[1].StartKey]
		require.True(t, exist)
<<<<<<< HEAD
		require.Equal(t, len(iargs.indexIDs), 1)
		require.Equal(t, iargs.indexIDs[0], int64(tablecodec.TempIndexPrefix|2))
	}

	// add index for table 1
	err = schemaReplace.tryToGCJob(addTable1IndexJob)
	require.NoError(t, err)
	iargs = <-midr.indexCh
	require.Equal(t, iargs.tableID, mDDLJobTable1NewID)
	require.Equal(t, len(iargs.indexIDs), 1)
	require.Equal(t, iargs.indexIDs[0], int64(tablecodec.TempIndexPrefix|2))
=======
	}

	// roll back add index for table1
	err = schemaReplace.restoreFromHistory(rollBackTable1IndexJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 2)
	// index ID
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)
	// temp index ID
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(tablecodec.TempIndexPrefix|2)), qargs.ParamsList[1].StartKey)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))

	// drop index for table0
	err = schemaReplace.restoreFromHistory(dropTable0IndexJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 1)
		_, exist := oldPartitionIDMap[qargs.ParamsList[0].StartKey]
		require.False(t, exist)
		oldPartitionIDMap[qargs.ParamsList[0].StartKey] = struct{}{}
		_, exist = mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
	}

	// drop index for table1
	err = schemaReplace.restoreFromHistory(dropTable1IndexJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)

	// add index for table 0
	err = schemaReplace.restoreFromHistory(addTable0IndexJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 1)
		_, exist := oldPartitionIDMap[qargs.ParamsList[0].StartKey]
		require.False(t, exist)
		oldPartitionIDMap[qargs.ParamsList[0].StartKey] = struct{}{}
		_, exist = mDDLJobALLNewPartitionTempIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
	}

	// add index for table 1
	err = schemaReplace.restoreFromHistory(addTable1IndexJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, tempIndex2), qargs.ParamsList[0].StartKey)

	// drop column for table0
	err = schemaReplace.restoreFromHistory(dropTable0ColumnJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 2)
		for _, params := range qargs.ParamsList {
			_, exist := oldPartitionIDMap[params.StartKey]
			require.False(t, exist)
			oldPartitionIDMap[params.StartKey] = struct{}{}
		}

		// index ID 2
		_, exist := mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
		// index ID 3
		_, exist = mDDLJobALLNewPartitionIndex3KeySet[qargs.ParamsList[1].StartKey]
		require.True(t, exist)
	}

	// drop column for table1
	err = schemaReplace.restoreFromHistory(dropTable1ColumnJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLIndexesIDSet))
	// index ID 2
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)
	// index ID 3
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(3)), qargs.ParamsList[1].StartKey)

	// modify column for table0
	err = schemaReplace.restoreFromHistory(modifyTable0ColumnJob)
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
		qargs = <-midr.queryCh
		require.Equal(t, len(qargs.ParamsList), 2)
		for _, params := range qargs.ParamsList {
			_, exist := oldPartitionIDMap[params.StartKey]
			require.False(t, exist)
			oldPartitionIDMap[params.StartKey] = struct{}{}
		}

		// index ID 2
		_, exist := mDDLJobALLNewPartitionIndex2KeySet[qargs.ParamsList[0].StartKey]
		require.True(t, exist)
		// index ID 3
		_, exist = mDDLJobALLNewPartitionIndex3KeySet[qargs.ParamsList[1].StartKey]
		require.True(t, exist)
	}

	// modify column for table1
	err = schemaReplace.restoreFromHistory(modifyTable1ColumnJob)
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), len(mDDLJobALLIndexesIDSet))
	// index ID 2
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)
	// index ID 3
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(3)), qargs.ParamsList[1].StartKey)

	// drop indexes(multi-schema-change) for table0
<<<<<<< HEAD
	err = schemaReplace.tryToGCJob(multiSchemaChangeJob0)
=======
	err = schemaReplace.restoreFromHistory(multiSchemaChangeJob0)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	require.NoError(t, err)
	oldPartitionIDMap = make(map[string]struct{})
	for l := 0; l < 2; l++ {
		for i := 0; i < len(mDDLJobALLNewPartitionIDSet); i++ {
			qargs = <-midr.queryCh
			require.Equal(t, len(qargs.ParamsList), 1)
			_, exist := oldPartitionIDMap[qargs.ParamsList[0].StartKey]
			require.False(t, exist)
			oldPartitionIDMap[qargs.ParamsList[0].StartKey] = struct{}{}
			_, exist = mDDLJobAllIndexesKeySet[l][qargs.ParamsList[0].StartKey]
			require.True(t, exist)
		}
	}

	// drop indexes(multi-schema-change) for table1
<<<<<<< HEAD
	err = schemaReplace.tryToGCJob(multiSchemaChangeJob1)
=======
	err = schemaReplace.restoreFromHistory(multiSchemaChangeJob1)
>>>>>>> be62f754fb4 (ddl: wrap the sessionctx to public delete range logic to BR (#48050))
	require.NoError(t, err)
	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(2)), qargs.ParamsList[0].StartKey)

	qargs = <-midr.queryCh
	require.Equal(t, len(qargs.ParamsList), 1)
	require.Equal(t, encodeTableIndexKey(mDDLJobTable1NewID, int64(3)), qargs.ParamsList[0].StartKey)
}

func TestCompatibleAlert(t *testing.T) {
	require.Equal(t, ddl.BRInsertDeleteRangeSQLPrefix, `INSERT IGNORE INTO mysql.gc_delete_range VALUES `)
	require.Equal(t, ddl.BRInsertDeleteRangeSQLValue, `(%?, %?, %?, %?, %?)`)
}
