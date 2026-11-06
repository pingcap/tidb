// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/docker/go-units"
	"github.com/gogo/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/repo"
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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

const pb uint64 = units.PB

func TestStorageCheckpointLog(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core, Level: zap.NewAtomicLevelAt(zap.InfoLevel)}))
	cfg := &task.RestoreConfig{CheckpointStorage: "local://FAKE_CHECKPOINT_USER:FAKE_CHECKPOINT_PASS@" + t.TempDir() + "?access-key=FAKE_CHECKPOINT_AK&secret-access-key=FAKE_CHECKPOINT_SK&sig=FAKE_CHECKPOINT_SAS"}
	originalStorage := cfg.CheckpointStorage

	err := cfg.CreateStorageCheckpointMetaManagersForTest(context.Background(), 1, 2)

	require.NoError(t, err)
	require.Equal(t, originalStorage, cfg.CheckpointStorage)
	entries := logs.FilterMessage("creating storage checkpoint meta managers for PiTR").All()
	require.Len(t, entries, 1)
	require.Equal(t, zapcore.InfoLevel, entries[0].Level)
	field, ok := entries[0].ContextMap()["checkpointStorage"].(string)
	require.True(t, ok)
	for _, secret := range []string{"FAKE_CHECKPOINT_USER", "FAKE_CHECKPOINT_PASS", "FAKE_CHECKPOINT_AK", "FAKE_CHECKPOINT_SK", "FAKE_CHECKPOINT_SAS"} {
		t.Run(secret, func(t *testing.T) { require.NotContains(t, field, secret) })
	}
	require.Contains(t, field, "local://")
}

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
	for i := range tables {
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
	require.Nil(t, task.PreCheckTableTiFlashReplica(ctx, pdClient, tables, nil, false, "ON"))

	for i := range tables {
		if i == 0 || i > 2 {
			require.Nil(t, tables[i].Info.TiFlashReplica)
		} else {
			require.NotNil(t, tables[i].Info.TiFlashReplica)
			obtainCount := int(tables[i].Info.TiFlashReplica.Count)
			require.Equal(t, i, obtainCount)
		}
	}

	require.Nil(t, task.PreCheckTableTiFlashReplica(ctx, pdClient, tables, tiflashrec.New(), false, "ON"))
	for i := range tables {
		require.Nil(t, tables[i].Info.TiFlashReplica)
	}
}

func TestPreCheckTableTiFlashReplicasNextGen(t *testing.T) {
	tables := make([]*metautil.Table, 3)
	for i := range tables {
		tables[i] = &metautil.Table{
			DB: &model.DBInfo{Name: ast.NewCIStr("test")},
			Info: &model.TableInfo{
				ID:   int64(i + 1),
				Name: ast.NewCIStr("test" + strconv.Itoa(i+1)),
				TiFlashReplica: &model.TiFlashReplicaInfo{
					Count: uint64(i + 1),
				},
			},
		}
	}
	recorder := tiflashrec.New()
	ctx := context.Background()
	require.Nil(t, task.PreCheckTableTiFlashReplica(ctx, nil, tables, recorder, true, "OFF"))
	for i := range tables {
		require.Nil(t, tables[i].Info.TiFlashReplica)
	}
	require.Empty(t, recorder.GetItems())

	// ON still strips on next-gen; the sysvar is diagnostic-only.
	tables[0].Info.TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1}
	require.Nil(t, task.PreCheckTableTiFlashReplica(ctx, nil, tables, recorder, true, "ON"))
	require.Nil(t, tables[0].Info.TiFlashReplica)
	require.Empty(t, recorder.GetItems())
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

	// A routed restore checks the target name. An incompatible table at the
	// source name must not block restoring a copy to a new target name.
	routedTable := &metautil.Table{
		DB: &model.DBInfo{Name: ast.NewCIStr("target")},
		Info: &model.TableInfo{
			Name:           ast.NewCIStr("copy"),
			IsCommonHandle: true,
		},
	}
	require.NoError(t, task.PreCheckTableClusterIndex([]*metautil.Table{routedTable}, nil, m.Domain))

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
	require.Equal(t, backuppb.BackupSchemaVersion, mockMeta.BackupSchemaVersion)
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
	require.Equal(t, backuppb.BackupSchemaVersion, mockMeta.BackupSchemaVersion)
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

func TestAdjustTablesToRestoreAndCreateTableTracker(t *testing.T) {
	// Create a mock snapshot database map
	snapshotDBMap := map[int64]*metautil.Database{
		1: {
			Info: &model.DBInfo{
				ID:   1,
				Name: ast.NewCIStr("test_db_1"),
			},
			Tables: []*metautil.Table{
				{
					DB: &model.DBInfo{
						ID:   1,
						Name: ast.NewCIStr("test_db_1"),
					},
					Info: &model.TableInfo{
						ID:   11,
						Name: ast.NewCIStr("test_table_11"),
					},
					FilesOfPhysicals: map[int64][]*backuppb.File{
						11: {{Name: "test_table_11"}},
					},
				},
				{
					DB: &model.DBInfo{
						ID:   1,
						Name: ast.NewCIStr("test_db_1"),
					},
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
			Info: &model.DBInfo{
				ID:   2,
				Name: ast.NewCIStr("test_db_2"),
			},
			Tables: []*metautil.Table{
				{
					DB: &model.DBInfo{
						ID:   2,
						Name: ast.NewCIStr("test_db_2"),
					},
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

	tests := []struct {
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
		snapshotFileMap  map[int64][]*backuppb.File
		expectedTableIDs map[int64][]int64
		expectedDBs      []int64
		expectedTables   []int64
		expectedFileMap  map[int64][]*backuppb.File
	}{
		{
			name:          "No filter",
			filterPattern: []string{"*.*"},
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
			expectedDBs:    []int64{1, 2},
			expectedTables: []int64{11, 12, 21},
		},
		{
			name:          "Filter by database",
			filterPattern: []string{"test_db_1.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			expectedTableIDs: map[int64][]int64{
				1: {11, 12},
			},
			expectedDBs:    []int64{1},
			expectedTables: []int64{11, 12},
		},
		{
			name:          "Filter by table",
			filterPattern: []string{"*.test_table_11"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			expectedTableIDs: map[int64][]int64{
				1: {11},
			},
			expectedDBs:    []int64{1, 2},
			expectedTables: []int64{11},
		},
		{
			name:          "Table renamed during log backup",
			filterPattern: []string{"*.*"},
			logBackupHistory: []struct {
				tableID   int64
				tableName string
				dbID      int64
			}{
				{11, "test_table_11", 1},
				{11, "renamed_table", 1},
				{12, "test_table_12", 1},
				{21, "test_table_21", 2},
			},
			snapshotDBMap:    generateDBMap(snapshotDBMap),
			snapshotTableMap: generateTableMap(snapshotDBMap),
			expectedTableIDs: map[int64][]int64{
				1: {11, 12},
				2: {21},
			},
			expectedDBs:    []int64{1, 2},
			expectedTables: []int64{11, 12, 21}, // 13 not in full backup
		},
		{
			name:          "Table renamed to different database during log backup",
			filterPattern: []string{"test_db_2.*"},
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
			snapshotDBMap:    generateDBMap(snapshotDBMap),
			snapshotTableMap: generateTableMap(snapshotDBMap),
			expectedTableIDs: map[int64][]int64{
				2: {11, 21},
			},
			expectedDBs:    []int64{1, 2},
			expectedTables: []int64{11, 21},
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
			expectedDBs:    []int64{1},
			expectedTables: []int64{12},
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
			expectedDBs:    []int64{}, // not in full backup
			expectedTables: []int64{}, // not in full backup
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// use local snapshotDBMap for special cases that need empty map
			localSnapshotDBMap := snapshotDBMap
			if tc.name == "Log backup table not in snapshot" || tc.name == "DB created during log backup" {
				localSnapshotDBMap = map[int64]*metautil.Database{}
			}

			snapTableMap := tc.snapshotTableMap
			if snapTableMap == nil {
				snapTableMap = generateTableMap(localSnapshotDBMap)
			}

			logBackupTableHistory := stream.NewTableHistoryManager()

			for _, h := range tc.logBackupHistory {
				logBackupTableHistory.AddTableHistory(h.tableID, h.tableName, h.dbID, 1)
			}

			for dbID, dbName := range tc.dbIDToName {
				logBackupTableHistory.RecordDBIdToName(dbID, dbName, 1)
			}

			testFilter, err := filter.Parse(tc.filterPattern)
			require.NoError(t, err)
			cfg := &task.RestoreConfig{
				Config: task.Config{
					TableFilter: testFilter,
				},
			}

			// Create empty partition map for the test
			partitionMap := make(map[int64]*stream.TableLocationInfo)

			currentTableMap := make(map[int64]*metautil.Table)
			currentDBMap := make(map[int64]*metautil.Database)

			filterSnapshotMaps(testFilter, localSnapshotDBMap, currentDBMap, currentTableMap)

			// Run the function
			err = task.AdjustTablesToRestoreAndCreateTableTracker(
				logBackupTableHistory,
				cfg,
				localSnapshotDBMap,
				snapTableMap,
				partitionMap,
				currentTableMap,
				currentDBMap,
			)
			require.NoError(t, err)

			for dbID, tableIDs := range tc.expectedTableIDs {
				for _, tableID := range tableIDs {
					require.True(t, cfg.PiTRTableTracker.ContainsDBAndTableId(dbID, tableID))
				}
			}

			require.Len(t, currentDBMap, len(tc.expectedDBs))
			for _, dbID := range tc.expectedDBs {
				require.NotNil(t, currentDBMap[dbID])
			}

			require.Len(t, currentTableMap, len(tc.expectedTables))
			for _, tableID := range tc.expectedTables {
				require.NotNil(t, currentTableMap[tableID])
			}
		})
	}
}

func TestHash(t *testing.T) {
	t.Run("legacy storage identity", func(t *testing.T) {
		// Fixed fingerprints from upstream cb395494e6, before log redaction
		// switched to query allowlists. Do not derive these from a redactor.
		for _, tc := range []struct{ storage, hash string }{
			{"s3://bucket/prefix?endpoint=https%3A%2F%2FUSER%3APASS%40host%2F%3Fsig%3DPRIVATE", "fe3c78a5c211c56d07879b55a4fc8898c8c010914f23c57e9b5e414510044258"},
			{"s3://bucket/prefix?access-key=OLD&secret-access-key=OLD&session-token=OLD&unknown=PRIVATE#fragment", "233a506b0c6104b8e1a9f0b6b9037746b5feb666f660639f6377fdd506ddba00"},
			{"s3://bucket/prefix?access-key=NEW&secret-access-key=NEW&session-token=NEW&unknown=PRIVATE#fragment", "233a506b0c6104b8e1a9f0b6b9037746b5feb666f660639f6377fdd506ddba00"},
			{"ks3://bucket/prefix?Access_Key=OLD&region=us&region=eu&unknown=PRIVATE", "7c7fda3ac06086e5005499793032e3bb88b4023314a8388080e00a73f924b465"},
			{"oss://bucket/prefix?secret_access_key=OLD&unknown=PRIVATE", "4e2d4f12aa5f3f7fa100c1a51835795b10fb05e71b13638d2a51cd30d863848a"},
			{"azure://bucket/prefix?account-name=a&account-key=OLD&sas-token=OLD&encryption-key=OLD&endpoint=https%3A%2F%2Facct.example%2F%3Fsig%3DOLD&unknown=PRIVATE", "53c7b03446de70c03835d410bd1ef190d19cbf426f4fdc97694119969e540bd8"},
			{"azure://bucket/prefix?account-name=a&account-key=NEW&sas-token=NEW&encryption-key=NEW&endpoint=https%3A%2F%2Facct.example%2F%3Fsig%3DNEW&unknown=PRIVATE", "53c7b03446de70c03835d410bd1ef190d19cbf426f4fdc97694119969e540bd8"},
			{"azblob://bucket/prefix?sas-token=OLD&unknown=PRIVATE", "c393c116cfacdd72a17a362d6d77010bec115c1e8ed02c0239c84dcb8565b918"},
			{"gcs://USER:PASS@bucket/prefix?credentials-file=PRIVATE&endpoint=https://host#fragment", "04035e672cd2a23d49fbe665e32afd841137467cdce1aaec48e53988a49596f8"},
			{"gs://bucket/prefix?credentials-file=PRIVATE#fragment", "6b3cc728a592e0e19a3ea155bb91215ddc648ae5bf5f6f804986298e62d1a7bb"},
			{"local:///tmp/backup?token=PRIVATE#fragment", "0718bbd6d889bac22194be91cc6d839b2ee29eadc413e5473df5c1ac417c6186"},
			{"file:///tmp/backup?token=PRIVATE#fragment", "32c00f90ed7653acf8edb1c74330785dec41ae97a643cce2a6886d33b92eae84"},
			{"hdfs://USER:PASS@host/backup?token=PRIVATE#fragment", "5dad05b451f17eefcb775d59c6670ad23c3e3e9159f3efa8dd4376873320d467"},
			{"noop:///?token=PRIVATE#fragment", "3fc302be854edc6c384670a4fa657babeaa1039757f69b5560e7ed86c0a8a55a"},
			{"https://USER:PASS@host/backup?token=PRIVATE#fragment", "d8ef4d086065cf10124072fdb4b525312fd180d4c538b7ec07c1992333a2fdda"},
			{"/tmp/my files/100%off#name", "661e02efb345b11d525b1b74546af9304cecbf63d70b524b019dfdcc73825353"},
			{"s3://bucket/path%zz?access-key=PRIVATE", "8b0ebe72ce02bf2c4ac35186bbcd858128cdc0310f85bd96df344b450a53e4b5"},
			{"s3://bucket/prefix?sse=aws:kms&sse-kms-context=%7B%7D", "dc9ba73da5fb5694c1465c8d703e1f313363e13ba46ece58c334b2c7f945c386"},
			{`s3://bucket/prefix?sse=aws:kms&sse-kms-context={"a":"x&region=PRIVATE"}`, "d08121c7e023c9f81050ea57d70db8afa8edaa999206b91d369c54b619733dda"},
			{"s3://bucket/prefix?unknown=PRIVATE%zz", "d08121c7e023c9f81050ea57d70db8afa8edaa999206b91d369c54b619733dda"},
			{"s3://bucket/prefix?", "0af7d308ff4fdd6ea242b8675c77c83d6610bd4f7aad4072626ee7f1185b7520"},
			{"gcs://bucket/prefix?", "14561c3b3a958e20ad879ffe3a4ea01c7f9af7b1448fc172b9360fb73de0d59c"},
		} {
			t.Run(tc.storage, func(t *testing.T) {
				core, logs := observer.New(zap.InfoLevel)
				t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core, Level: zap.NewAtomicLevelAt(zap.InfoLevel)}))
				cfg := &task.RestoreConfig{
					Config:              task.Config{Storage: tc.storage, FilterStr: []string{"source.*"}},
					RestoreCommonConfig: task.RestoreCommonConfig{WithSysTable: true},
					UpstreamClusterID:   42,
					FastLoadSysTables:   true,
					LoadStats:           true,
				}

				hash, err := cfg.Hash(task.FullRestoreCmd)

				require.NoError(t, err)
				require.Equal(t, tc.hash, hex.EncodeToString(hash))
				require.Equal(t, tc.storage, cfg.Storage)
				entries := logs.FilterMessage("hash of restore config").TakeAll()
				require.Len(t, entries, 1)
				logged, err := json.Marshal(entries[0].ContextMap())
				require.NoError(t, err)
				for _, secret := range []string{"PRIVATE", "USER", "PASS", "OLD", "NEW"} {
					require.NotContains(t, string(logged), secret)
				}
			})
		}
	})

	t.Run("legacy storage equivalence", func(t *testing.T) {
		for _, tc := range []struct {
			first, second string
			equal         bool
		}{
			{"s3://bucket/prefix?access-key=old", "s3://bucket/prefix?access-key=new", true},
			{"s3://bucket/prefix?access-key=old", "s3://bucket/prefix", false},
			{"s3://bucket/prefix?future-token=old", "s3://bucket/prefix?future-token=new", false},
			{"s3://bucket/prefix?endpoint=https://first", "s3://bucket/prefix?endpoint=https://second", false},
			{"gcs://bucket/prefix?credentials=old#fragment", "gcs://bucket/prefix?credentials=new#fragment", false},
			{"gcs://bucket/prefix", "gcs://bucket/prefix?", false},
			{"azure://bucket/prefix?endpoint=https://first", "azure://bucket/prefix?endpoint=https://second", true},
			{"s3://bucket/first%zz", "s3://bucket/second%zz", true},
		} {
			t.Run(tc.first, func(t *testing.T) {
				first := &task.RestoreConfig{Config: task.Config{Storage: tc.first}}
				second := &task.RestoreConfig{Config: task.Config{Storage: tc.second}}

				firstHash, err := first.Hash(task.FullRestoreCmd)
				require.NoError(t, err)
				secondHash, err := second.Hash(task.FullRestoreCmd)
				require.NoError(t, err)

				if tc.equal {
					require.Equal(t, firstHash, secondHash)
				} else {
					require.NotEqual(t, firstHash, secondHash)
				}
			})
		}
	})

	baseConfig := task.RestoreConfig{
		UpstreamClusterID: 1,
		Config: task.Config{
			Storage:        "default-storage",
			ExplicitFilter: true,
			FilterStr:      []string{"filter1", "filter2"},
		},
		RestoreCommonConfig: task.RestoreCommonConfig{
			WithSysTable: true,
		},
	}

	testCases := []struct {
		name        string
		modifyFunc  func(*task.RestoreConfig)
		changeTask  bool
		expectEqual bool
	}{
		{
			name:        "identical_configuration",
			modifyFunc:  func(*task.RestoreConfig) {},
			expectEqual: true,
		},
		{
			name: "change_upstream_cluster_id",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.UpstreamClusterID = 999
			},
			expectEqual: false,
		},
		{
			name: "change_storage",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Config.Storage = "new-storage"
			},
			expectEqual: false,
		},
		{
			name: "toggle_explicit_filter",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Config.ExplicitFilter = !cfg.Config.ExplicitFilter
			},
			expectEqual: true,
		},
		{
			name: "modify_filter_strings",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Config.FilterStr = append(cfg.Config.FilterStr, "new-filter")
			},
			expectEqual: false,
		},
		{
			name: "reorder_filter_strings",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Config.FilterStr = []string{cfg.Config.FilterStr[1], cfg.Config.FilterStr[0]}
			},
			expectEqual: false,
		},
		{
			name: "toggle_system_tables",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.WithSysTable = !cfg.WithSysTable
			},
			expectEqual: false,
		},
		{
			name: "explicit_legacy_layout_keeps_hash",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Layout = repo.LayoutLegacy
			},
			expectEqual: true,
		},
		{
			name: "repo_v1_layout_changes_hash",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Layout = repo.LayoutRepo
				cfg.BackupID = repo.BackupID(0x1234)
			},
			expectEqual: false,
		},
		{
			name: "repo_v1_backup_id_changes_hash",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.Layout = repo.LayoutRepo
				cfg.BackupID = repo.BackupID(0x5678)
			},
			expectEqual: false,
		},
		{
			name: "empty_vs_base_config",
			modifyFunc: func(cfg *task.RestoreConfig) {
				*cfg = task.RestoreConfig{}
			},
			expectEqual: false,
		},
		{
			name: "multiple_changes",
			modifyFunc: func(cfg *task.RestoreConfig) {
				cfg.UpstreamClusterID = 2
				cfg.Config.Storage = "multi-change-storage"
			},
			expectEqual: false,
		},
		{
			name:        "change_task_type",
			changeTask:  true,
			modifyFunc:  func(*task.RestoreConfig) {},
			expectEqual: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			originalCfg := baseConfig
			modifiedCfg := baseConfig

			if tc.modifyFunc != nil {
				tc.modifyFunc(&modifiedCfg)
			}

			var cfg1, cfg2 *task.RestoreConfig
			if tc.name == "nil_configuration" {
				cfg1 = &originalCfg
				cfg2 = nil
			} else {
				cfg1 = &originalCfg
				cfg2 = &modifiedCfg
			}

			taskType1 := "full"
			taskType2 := "full"
			if tc.changeTask {
				taskType2 = "db"
			}

			hash1, err1 := cfg1.Hash(taskType1)
			hash2, err2 := cfg2.Hash(taskType2)

			if cfg2 == nil {
				require.Error(t, err2, "Expected error for nil config")
				return
			}
			require.NoError(t, err1)
			require.NoError(t, err2)

			if tc.expectEqual {
				require.Equal(t, hash1, hash2, "Hashes should be equal for: %s", tc.name)
			} else {
				require.NotEqual(t, hash1, hash2, "Hashes should differ for: %s", tc.name)
			}
		})
	}
}

// filterSnapshotMaps filters the snapshot maps based on the filter criteria and populates the current maps
func filterSnapshotMaps(
	tableFilter filter.Filter,
	snapDBMap map[int64]*metautil.Database,
	currentDBMap map[int64]*metautil.Database,
	currentTableMap map[int64]*metautil.Table,
) {
	for dbID, db := range snapDBMap {
		dbName := db.Info.Name.O

		if tableFilter.MatchSchema(dbName) {
			currentDBMap[dbID] = db
		}

		for _, table := range db.Tables {
			tableID := table.Info.ID
			tableName := table.Info.Name.O

			if tableFilter.MatchTable(dbName, tableName) {
				currentTableMap[tableID] = table
			}
		}
	}
}
