// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task_test

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/br/pkg/task"
	utiltest "github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

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

	pdClient := utiltest.NewFakePDClient(mockStores, false, nil)

	tables := make([]*metautil.Table, 4)
	for i := 0; i < len(tables); i++ {
		tiflashReplica := &model.TiFlashReplicaInfo{
			Count: uint64(i),
		}
		if i == 0 {
			tiflashReplica = nil
		}

		tables[i] = &metautil.Table{
			DB: &model.DBInfo{Name: model.NewCIStr("test")},
			Info: &model.TableInfo{
				ID:             int64(i),
				Name:           model.NewCIStr("test" + strconv.Itoa(i)),
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
	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	require.True(t, isExist)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: model.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      model.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
		err = se.CreateTable(ctx, tables[i].DB.Name, tables[i].Info, ddl.OnExistIgnore)
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
				Name:           model.NewCIStr("test1"),
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
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	require.Truef(t, ok, "DB info not exist")
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
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
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	require.Truef(t, ok, "DB info not exist")
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
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
