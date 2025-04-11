// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pdhttp "github.com/tikv/pd/client/http"
)

const pb uint64 = units.PB

func TestPreCheckTableTiFlashReplicas(t *testing.T) {
	mockStores := []*metapb.Store{
		{
			Id: 1,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id: 2,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
	}

	pdClient := split.NewFakePDClient(mockStores, false, nil)

	tables := make([]*metautil.Table, 4)
	for i := 0; i < len(tables); i++ {
		tiflashReplica := &model.TiFlashReplicaInfo{
			Count: uint64(i),
		}
		if i == 0 {
			tiflashReplica = nil
		}

		tables[i] = &metautil.Table{
			DB: &model.DBInfo{Name: ast.NewCIStr("test")},
			Info: &model.TableInfo{
				ID:             int64(i),
				Name:           ast.NewCIStr("test" + strconv.Itoa(i)),
				TiFlashReplica: tiflashReplica,
			},
		}
	}
	ctx := context.Background()
	require.Nil(t, task.PreCheckTableTiFlashReplica(ctx, pdClient, tables, nil))

	for i := 0; i < len(tables); i++ {
		if i == 0 || i > 2 {
			require.Nil(t, tables[i].Info.TiFlashReplica)
		} else {
			require.NotNil(t, tables[i].Info.TiFlashReplica)
			obtainCount := int(tables[i].Info.TiFlashReplica.Count)
			require.Equal(t, i, obtainCount)
		}
	}

	require.Nil(t, task.PreCheckTableTiFlashReplica(ctx, pdClient, tables, tiflashrec.New()))
	for i := 0; i < len(tables); i++ {
		require.Nil(t, tables[i].Info.TiFlashReplica)
	}
}

func TestPreCheckTableClusterIndex(t *testing.T) {
	ctx := context.Background()
	m, err := mock.NewCluster()
	if err != nil {
		panic(err)
	}
	err = m.Start()
	if err != nil {
		panic(err)
	}
	defer m.Stop()
	g := gluetidb.New()
	se, err := g.CreateSession(m.Storage)
	require.NoError(t, err)

	info, err := m.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(ast.NewCIStr("test"))
	require.True(t, isExist)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: ast.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      ast.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
		err = se.CreateTable(ctx, tables[i].DB.Name, tables[i].Info, ddl.WithOnExist(ddl.OnExistIgnore))
		require.NoError(t, err)
	}

	// exist different tables
	tables[1].Info.IsCommonHandle = true
	err = task.PreCheckTableClusterIndex(tables, nil, m.Domain)
	require.Error(t, err)
	require.Regexp(t, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`, err.Error())

	// exist different DDLs
	jobs := []*model.Job{{
		ID:         5,
		Type:       model.ActionCreateTable,
		SchemaName: "test",
		Query:      "",
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				Name:           ast.NewCIStr("test1"),
				IsCommonHandle: true,
			},
		},
	}}
	err = task.PreCheckTableClusterIndex(nil, jobs, m.Domain)
	require.Error(t, err)
	require.Regexp(t, `.*@@tidb_enable_clustered_index should be ON \(backup table = true, created table = false\).*`, err.Error())

	// should pass pre-check cluster index
	tables[1].Info.IsCommonHandle = false
	jobs[0].BinlogInfo.TableInfo.IsCommonHandle = false
	require.Nil(t, task.PreCheckTableClusterIndex(tables, jobs, m.Domain))
}

func TestCheckNewCollationEnable(t *testing.T) {
	caseList := []struct {
		backupMeta                  *backuppb.BackupMeta
		newCollationEnableInCluster string
		CheckRequirements           bool
		isErr                       bool
	}{
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "True"},
			newCollationEnableInCluster: "True",
			CheckRequirements:           true,
			isErr:                       false,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "True"},
			newCollationEnableInCluster: "False",
			CheckRequirements:           true,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "False"},
			newCollationEnableInCluster: "True",
			CheckRequirements:           true,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "False"},
			newCollationEnableInCluster: "false",
			CheckRequirements:           true,
			isErr:                       false,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "False"},
			newCollationEnableInCluster: "True",
			CheckRequirements:           false,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: "True"},
			newCollationEnableInCluster: "False",
			CheckRequirements:           false,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: ""},
			newCollationEnableInCluster: "True",
			CheckRequirements:           false,
			isErr:                       false,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: ""},
			newCollationEnableInCluster: "True",
			CheckRequirements:           true,
			isErr:                       true,
		},
		{
			backupMeta:                  &backuppb.BackupMeta{NewCollationsEnabled: ""},
			newCollationEnableInCluster: "False",
			CheckRequirements:           false,
			isErr:                       false,
		},
	}

	for i, ca := range caseList {
		g := &gluemock.MockGlue{
			GlobalVars: map[string]string{"new_collation_enabled": ca.newCollationEnableInCluster},
		}
		enabled, err := task.CheckNewCollationEnable(ca.backupMeta.GetNewCollationsEnabled(), g, nil, ca.CheckRequirements)
		t.Logf("[%d] Got Error: %v\n", i, err)
		if ca.isErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, ca.newCollationEnableInCluster == "True", enabled)
	}
}

func TestFilterDDLJobs(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.Mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.Mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get ts: %s", err)

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	metaWriter := metautil.NewMetaWriter(s.Storage, metautil.MetaFileSize, false, "", &cipher)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	s.MockGlue.SetSession(tk.Session())
	err = backup.WriteBackupDDLJobs(metaWriter, s.MockGlue, s.Mock.Storage, lastTS, ts, false)
	require.NoErrorf(t, err, "Error get ddl jobs: %s", err)
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	require.NoErrorf(t, err, "Flush failed", err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoErrorf(t, err, "Finially flush backupmeta failed", err)
	infoSchema, err := s.Mock.Domain.GetSnapshotInfoSchema(ts)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, ok := infoSchema.SchemaByName(ast.NewCIStr("test_db"))
	require.Truef(t, ok, "DB info not exist")
	tableInfo, err := infoSchema.TableByName(context.Background(), ast.NewCIStr("test_db"), ast.NewCIStr("test_table"))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	tables := []*metautil.Table{{
		DB:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	metaBytes, err := s.Storage.ReadFile(ctx, metautil.MetaFile)
	require.NoError(t, err)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	require.NoError(t, err)
	// check the schema version
	require.Equal(t, int32(metautil.MetaV1), mockMeta.Version)
	metaReader := metautil.NewMetaReader(mockMeta, s.Storage, &cipher)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	require.NoError(t, err)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	require.NoError(t, err)

	ddlJobs := task.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		t.Logf("get ddl job: %s", job.Query)
	}
	require.Equal(t, 7, len(ddlJobs))
}

func TestFilterDDLJobsV2(t *testing.T) {
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.Mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.Mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get ts: %s", err)

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	metaWriter := metautil.NewMetaWriter(s.Storage, metautil.MetaFileSize, true, "", &cipher)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	s.MockGlue.SetSession(tk.Session())
	err = backup.WriteBackupDDLJobs(metaWriter, s.MockGlue, s.Mock.Storage, lastTS, ts, false)
	require.NoErrorf(t, err, "Error get ddl jobs: %s", err)
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	require.NoErrorf(t, err, "Flush failed", err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoErrorf(t, err, "Flush BackupMeta failed", err)

	infoSchema, err := s.Mock.Domain.GetSnapshotInfoSchema(ts)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, ok := infoSchema.SchemaByName(ast.NewCIStr("test_db"))
	require.Truef(t, ok, "DB info not exist")
	tableInfo, err := infoSchema.TableByName(context.Background(), ast.NewCIStr("test_db"), ast.NewCIStr("test_table"))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	tables := []*metautil.Table{{
		DB:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	metaBytes, err := s.Storage.ReadFile(ctx, metautil.MetaFile)
	require.NoError(t, err)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	require.NoError(t, err)
	// check the schema version
	require.Equal(t, int32(metautil.MetaV2), mockMeta.Version)
	metaReader := metautil.NewMetaReader(mockMeta, s.Storage, &cipher)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	require.NoError(t, err)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	require.NoError(t, err)

	ddlJobs := task.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		t.Logf("get ddl job: %s", job.Query)
	}
	require.Equal(t, 7, len(ddlJobs))
}

func TestFilterDDLJobByRules(t *testing.T) {
	ddlJobs := []*model.Job{
		{
			Type: model.ActionSetTiFlashReplica,
		},
		{
			Type: model.ActionAddPrimaryKey,
		},
		{
			Type: model.ActionUpdateTiFlashReplicaStatus,
		},
		{
			Type: model.ActionCreateTable,
		},
		{
			Type: model.ActionLockTable,
		},
		{
			Type: model.ActionAddIndex,
		},
		{
			Type: model.ActionUnlockTable,
		},
		{
			Type: model.ActionCreateSchema,
		},
		{
			Type: model.ActionModifyColumn,
		},
	}

	expectedDDLTypes := []model.ActionType{
		model.ActionAddPrimaryKey,
		model.ActionCreateTable,
		model.ActionAddIndex,
		model.ActionCreateSchema,
		model.ActionModifyColumn,
	}

	ddlJobs = task.FilterDDLJobByRules(ddlJobs, task.DDLJobBlockListRule)

	require.Equal(t, len(expectedDDLTypes), len(ddlJobs))
	for i, ddlJob := range ddlJobs {
		assert.Equal(t, expectedDDLTypes[i], ddlJob.Type)
	}
}

func TestCheckDDLJobByRules(t *testing.T) {
	ddlJobs := []*model.Job{
		{
			Type: model.ActionSetTiFlashReplica,
		},
		{
			Type: model.ActionAddPrimaryKey,
		},
		{
			Type: model.ActionUpdateTiFlashReplicaStatus,
		},
		{
			Type: model.ActionCreateTable,
		},
		{
			Type: model.ActionLockTable,
		},
		{
			Type: model.ActionAddIndex,
		},
		{
			Type: model.ActionUnlockTable,
		},
		{
			Type: model.ActionCreateSchema,
		},
		{
			Type: model.ActionModifyColumn,
		},
		{
			Type: model.ActionReorganizePartition,
		},
	}

	filteredDDlJobs := task.FilterDDLJobByRules(ddlJobs, task.DDLJobLogIncrementalCompactBlockListRule)

	expectedDDLTypes := []model.ActionType{
		model.ActionSetTiFlashReplica,
		model.ActionAddPrimaryKey,
		model.ActionUpdateTiFlashReplicaStatus,
		model.ActionCreateTable,
		model.ActionLockTable,
		model.ActionUnlockTable,
		model.ActionCreateSchema,
	}

	require.Equal(t, len(expectedDDLTypes), len(filteredDDlJobs))
	expectedDDLJobs := make([]*model.Job, 0, len(expectedDDLTypes))
	for i, ddlJob := range filteredDDlJobs {
		assert.Equal(t, expectedDDLTypes[i], ddlJob.Type)
		expectedDDLJobs = append(expectedDDLJobs, ddlJob)
	}

	require.NoError(t, task.CheckDDLJobByRules(expectedDDLJobs, task.DDLJobLogIncrementalCompactBlockListRule))
	require.Error(t, task.CheckDDLJobByRules(ddlJobs, task.DDLJobLogIncrementalCompactBlockListRule))
}

// NOTICE: Once there is a new backfilled type ddl, BR needs to ensure that it is correctly cover by the rules:
func TestMonitorTheIncrementalUnsupportDDLType(t *testing.T) {
	require.Equal(t, int(5), ddl.BackupFillerTypeCount())
}

func TestTikvUsage(t *testing.T) {
	files := []*backuppb.File{
		{Name: "F1", Size_: 1 * pb},
		{Name: "F2", Size_: 2 * pb},
		{Name: "F3", Size_: 3 * pb},
		{Name: "F4", Size_: 4 * pb},
		{Name: "F5", Size_: 5 * pb},
	}
	replica := uint64(3)
	storeCnt := uint64(6)
	total := uint64(0)
	for _, f := range files {
		total += f.GetSize_()
	}
	ret := task.EstimateTikvUsage(total, replica, storeCnt)
	require.Equal(t, 15*pb*3/6, ret)
}

func TestTiflashUsage(t *testing.T) {
	tables := []*metautil.Table{
		{Info: &model.TableInfo{TiFlashReplica: &model.TiFlashReplicaInfo{Count: 0}},
			FilesOfPhysicals: map[int64][]*backuppb.File{1: {{Size_: 1 * pb}}}},
		{Info: &model.TableInfo{TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1}},
			FilesOfPhysicals: map[int64][]*backuppb.File{2: {{Size_: 2 * pb}}}},
		{Info: &model.TableInfo{TiFlashReplica: &model.TiFlashReplicaInfo{Count: 2}},
			FilesOfPhysicals: map[int64][]*backuppb.File{3: {{Size_: 3 * pb}}}},
	}

	var storeCnt uint64 = 3
	ret := task.EstimateTiflashUsage(tables, storeCnt)
	require.Equal(t, 8*pb/3, ret)
}

func TestCheckTikvSpace(t *testing.T) {
	store := pdhttp.StoreInfo{Store: pdhttp.MetaStore{ID: 1}, Status: pdhttp.StoreStatus{Available: "500PB"}}
	require.NoError(t, task.CheckStoreSpace(400*pb, &store))
}

type testCase struct {
	name             string
	filterPattern    []string
	logBackupHistory []struct {
		tableID   int64
		tableName string
		dbID      int64
	}
	dbIDToName       map[int64]string
	snapshotDBMap    map[int64]*metautil.Database
	snapshotTableMap map[int64]*metautil.Table
	expectedTableIDs map[int64][]int64 // dbID -> []tableID
	expectedDBs      []int64
	expectedTables   []int64
	expectedFileMap  map[int64][]*backuppb.File
}

func generateDBMap(snapshotDBMap map[int64]*metautil.Database) map[int64]*metautil.Database {
	m := make(map[int64]*metautil.Database)
	for id, dbinfo := range snapshotDBMap {
		clonedTables := make([]*metautil.Table, 0, len(dbinfo.Tables))
		dbInfo := dbinfo.Info.Clone()
		for _, tableInfo := range dbinfo.Tables {
			clonedTables = append(clonedTables, &metautil.Table{
				DB:   dbInfo,
				Info: tableInfo.Info.Clone(),
			})
		}
		m[id] = &metautil.Database{
			Info:   dbInfo,
			Tables: clonedTables,
		}
	}
	return m
}

func generateTableMap(snapshotDBMap map[int64]*metautil.Database) map[int64]*metautil.Table {
	m := make(map[int64]*metautil.Table)
	for _, dbinfo := range snapshotDBMap {
		for _, tableInfo := range dbinfo.Tables {
			m[tableInfo.Info.ID] = &metautil.Table{
				DB:   tableInfo.DB.Clone(),
				Info: tableInfo.Info.Clone(),
				FilesOfPhysicals: map[int64][]*backuppb.File{
					tableInfo.Info.ID: {{Name: tableInfo.Info.Name.O}},
				},
			}
		}
	}
	return m
}

func generateFiles(tableMap map[int64]*metautil.Table) map[int64][]*backuppb.File {
	m := make(map[int64][]*backuppb.File)
	for _, tableInfo := range tableMap {
		m[tableInfo.Info.ID] = []*backuppb.File{
			{Name: tableInfo.Info.Name.O},
		}
	}
	return m
}

func generateFilesByID(ids ...int64) map[int64][]*backuppb.File {
	m := make(map[int64][]*backuppb.File)
	for _, id := range ids {
		m[id] = []*backuppb.File{
			{Name: fmt.Sprintf("test_table_%d", id)},
		}
	}
	return m
}

func TestAdjustTablesToRestoreAndCreateTableTracker(t *testing.T) {
	// Setup common test database and table maps
	dbInfo1 := model.DBInfo{
		ID:   1,
		Name: ast.NewCIStr("test_db_1"),
	}
	dbInfo2 := model.DBInfo{
		ID:   2,
		Name: ast.NewCIStr("test_db_2"),
	}
	snapshotDBMap := map[int64]*metautil.Database{
		1: {
			Info: &dbInfo1,
			Tables: []*metautil.Table{
				{
					DB: &dbInfo1,
					Info: &model.TableInfo{
						ID:   11,
						Name: ast.NewCIStr("test_table_11"),
					},
					FilesOfPhysicals: map[int64][]*backuppb.File{
						11: {{Name: "test_table_11"}},
					},
				},
				{
					DB: &dbInfo1,
					Info: &model.TableInfo{
						ID:   12,
						Name: ast.NewCIStr("test_table_12"),
					},
					FilesOfPhysicals: map[int64][]*backuppb.File{
						12: {{Name: "test_table_12"}},
					},
				},
			},
		},
		2: {
			Info: &dbInfo2,
			Tables: []*metautil.Table{
				{
					DB: &dbInfo2,
					Info: &model.TableInfo{
						ID:   21,
						Name: ast.NewCIStr("test_table_21"),
					},
					FilesOfPhysicals: map[int64][]*backuppb.File{
						21: {{Name: "test_table_21"}},
					},
				},
			},
		},
	}

	tests := []testCase{
		{
			name:          "Basic table tracking",
			filterPattern: []string{"test_db*.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			snapshotDBMap:    generateDBMap(snapshotDBMap),
			snapshotTableMap: generateTableMap(snapshotDBMap),
			expectedTableIDs: map[int64][]int64{
				1: {11, 12},
				2: {21},
			},
			expectedDBs:     []int64{1, 2},
			expectedTables:  []int64{11, 12, 21},
			expectedFileMap: generateFilesByID(11, 12, 21),
		},
		{
			name:          "Table not in filter",
			filterPattern: []string{"other_db.other_table"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			expectedTableIDs: map[int64][]int64{},
			expectedDBs:      []int64{},
			expectedTables:   []int64{},
			expectedFileMap:  generateFilesByID(),
		},
		{
			name:          "New table created during log backup",
			filterPattern: []string{"test_db*.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
				{13, "new_table", 1},
			},
			snapshotDBMap:    generateDBMap(snapshotDBMap),
			snapshotTableMap: generateTableMap(snapshotDBMap),
			expectedTableIDs: map[int64][]int64{
				1: {11, 12, 13},
				2: {21},
			},
			expectedDBs:     []int64{1, 2},
			expectedTables:  []int64{11, 12, 21}, // 13 not in full backup
			expectedFileMap: generateFilesByID(11, 12, 21),
		},
		{
			name:          "Table renamed into filter during log backup",
			filterPattern: []string{"test_db_2.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1}, // drop
				{11, "renamed_table", 2}, // create
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			snapshotDBMap:    generateDBMap(map[int64]*metautil.Database{2: snapshotDBMap[2]}),
			snapshotTableMap: generateTableMap(map[int64]*metautil.Database{2: snapshotDBMap[2]}),
			expectedTableIDs: map[int64][]int64{
				2: {11, 21},
			},
			expectedDBs:     []int64{1, 2}, // need original db for restore
			expectedTables:  []int64{11, 21},
			expectedFileMap: generateFilesByID(11, 21),
		},
		{
			name:          "Table renamed out of filter during log backup",
			filterPattern: []string{"test_db_1.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{11, "renamed_table", 2},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			snapshotDBMap:    generateDBMap(map[int64]*metautil.Database{1: snapshotDBMap[1]}),
			snapshotTableMap: generateTableMap(map[int64]*metautil.Database{1: snapshotDBMap[1]}),
			expectedTableIDs: map[int64][]int64{
				1: {12},
			},
			expectedDBs:     []int64{1},
			expectedTables:  []int64{12},
			expectedFileMap: generateFilesByID(12),
		},
		{
			name:          "Log backup table not in snapshot",
			filterPattern: []string{"test_db_1.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table", 1},
			},
			// using empty snapshotDBMap for this test
			expectedTableIDs: map[int64][]int64{},
			expectedDBs:      []int64{},
			expectedTables:   []int64{},
			expectedFileMap:  generateFilesByID(),
		},
		{
			name:          "DB created during log backup",
			filterPattern: []string{"test_db_1.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table", 1},
			},
			dbIDToName: map[int64]string{
				1: "test_db_1",
			},
			// using empty snapshotDBMap for this test
			expectedTableIDs: map[int64][]int64{
				1: {11},
			},
			expectedDBs:     []int64{}, // not in full backup
			expectedTables:  []int64{}, // not in full backup
			expectedFileMap: generateFilesByID(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// use local snapshotDBMap for special cases that need empty map
			localSnapshotDBMap := snapshotDBMap
			if tc.name == "Log backup table not in snapshot" || tc.name == "DB created during log backup" {
				localSnapshotDBMap = map[int64]*metautil.Database{}
			}

			tableMap := tc.snapshotTableMap
			dbMap := tc.snapshotDBMap
			logBackupTableHistory := stream.NewTableHistoryManager()

			for _, h := range tc.logBackupHistory {
				logBackupTableHistory.AddTableHistory(h.tableID, h.tableName, h.dbID)
			}

			for dbID, dbName := range tc.dbIDToName {
				logBackupTableHistory.RecordDBIdToName(dbID, dbName)
			}

			testFilter, err := filter.Parse(tc.filterPattern)
			require.NoError(t, err)
			cfg := &task.RestoreConfig{
				Config: task.Config{
					TableFilter: testFilter,
				},
			}

			// Run the function
			err = task.AdjustTablesToRestoreAndCreateTableTracker(
				logBackupTableHistory,
				cfg,
				localSnapshotDBMap,
				tableMap,
				dbMap,
			)
			require.NoError(t, err)

			for dbID, tableIDs := range tc.expectedTableIDs {
				for _, tableID := range tableIDs {
					require.True(t, cfg.PiTRTableTracker.ContainsTableId(dbID, tableID))
				}
			}

			require.Len(t, dbMap, len(tc.expectedDBs))
			for _, dbID := range tc.expectedDBs {
				require.NotNil(t, dbMap[dbID])
			}

			require.Len(t, tableMap, len(tc.expectedTables))
			for _, tableID := range tc.expectedTables {
				require.NotNil(t, tableMap[tableID])
			}

			fileMap := generateFiles(tableMap)
			require.Len(t, fileMap, len(tc.expectedFileMap))
			for tableID, files := range tc.expectedFileMap {
				files2, exists := fileMap[tableID]
				require.True(t, exists)
				require.Len(t, files, 1)
				require.Len(t, files2, 1)
				require.Equal(t, files[0].Name, files2[0].Name)
			}
		})
	}
}
