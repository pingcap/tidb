// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type testRestoreSchemaSuite struct {
	mock     *mock.Cluster
	mockGlue *gluetidb.MockGlue
	storage  storage.ExternalStorage
}

func createRestoreSchemaSuite(t *testing.T) *testRestoreSchemaSuite {
	var err error
	s := new(testRestoreSchemaSuite)
	s.mockGlue = &gluetidb.MockGlue{}
	s.mock, err = mock.NewCluster()
	require.NoError(t, err)
	base := t.TempDir()
	s.storage, err = storage.NewLocalStorage(base)
	require.NoError(t, err)
	require.NoError(t, s.mock.Start())
	t.Cleanup(func() {
		s.mock.Stop()
	})
	return s
}

func TestRestoreAutoIncID(t *testing.T) {
	s := createRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("drop table if exists `\"t\"`;")
	// Test SQL Mode
	tk.MustExec("create table `\"t\"` (" +
		"a int not null," +
		"time timestamp not null default '0000-00-00 00:00:00');",
	)
	tk.MustExec("insert into `\"t\"` values (10, '0000-00-00 00:00:00');")
	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	// Get schemas of db and table
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	require.Truef(t, exists, "Error get db info")
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("\"t\""))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	table := metautil.Table{
		Info: tableInfo.Meta(),
		DB:   dbInfo,
	}
	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.mock.Domain, dbInfo.ID, table.Info.ID, false, autoid.RowIDAllocType)
	globalAutoID, err := idAlloc.NextGlobalAutoID()
	require.NoErrorf(t, err, "Error allocate next auto id")
	require.Equal(t, uint64(globalAutoID), autoIncID)
	// Alter AutoIncID to the next AutoIncID + 100
	table.Info.AutoIncID = globalAutoID + 100
	db, _, err := restore.NewDB(gluetidb.New(), s.mock.Storage, "STRICT")
	require.NoErrorf(t, err, "Error create DB")
	tk.MustExec("drop database if exists test;")
	// Test empty collate value
	table.DB.Charset = "utf8mb4"
	table.DB.Collate = ""
	err = db.CreateDatabase(context.Background(), table.DB)
	require.NoErrorf(t, err, "Error create empty collate db: %s %s", err, s.mock.DSN)
	tk.MustExec("drop database if exists test;")
	// Test empty charset value
	table.DB.Charset = ""
	table.DB.Collate = "utf8mb4_bin"
	err = db.CreateDatabase(context.Background(), table.DB)
	require.NoErrorf(t, err, "Error create empty charset db: %s %s", err, s.mock.DSN)
	uniqueMap := make(map[restore.UniqueTableName]bool)
	err = db.CreateTable(context.Background(), &table, uniqueMap, false, nil)
	require.NoErrorf(t, err, "Error create table: %s %s", err, s.mock.DSN)

	tk.MustExec("use test")
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	// Check if AutoIncID is altered successfully.
	require.Equal(t, uint64(globalAutoID+100), autoIncID)

	// try again, failed due to table exists.
	table.Info.AutoIncID = globalAutoID + 200
	err = db.CreateTable(context.Background(), &table, uniqueMap, false, nil)
	require.NoError(t, err)
	// Check if AutoIncID is not altered.
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	require.Equal(t, uint64(globalAutoID+100), autoIncID)

	// try again, success because we use alter sql in unique map.
	table.Info.AutoIncID = globalAutoID + 300
	uniqueMap[restore.UniqueTableName{"test", "\"t\""}] = true
	err = db.CreateTable(context.Background(), &table, uniqueMap, false, nil)
	require.NoError(t, err)
	// Check if AutoIncID is altered to globalAutoID + 300.
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	require.NoErrorf(t, err, "Error query auto inc id: %s", err)
	require.Equal(t, uint64(globalAutoID+300), autoIncID)
}

func TestCreateTablesInDb(t *testing.T) {
	s := createRestoreSchemaSuite(t)
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)

	dbSchema, isExist := info.SchemaByName(model.NewCIStr("test"))
	require.True(t, isExist)

	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	ddlJobMap := make(map[restore.UniqueTableName]bool)
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
		ddlJobMap[restore.UniqueTableName{dbSchema.Name.String(), tables[i].Info.Name.String()}] = false
	}
	db, _, err := restore.NewDB(gluetidb.New(), s.mock.Storage, "STRICT")
	require.NoError(t, err)

	err = db.CreateTables(context.Background(), tables, ddlJobMap, false, nil)
	require.NoError(t, err)
}

func TestFilterDDLJobs(t *testing.T) {
	s := createRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get ts: %s", err)

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	metaWriter := metautil.NewMetaWriter(s.storage, metautil.MetaFileSize, false, "", &cipher)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	s.mockGlue.SetSession(tk.Session())
	err = backup.WriteBackupDDLJobs(metaWriter, s.mockGlue, s.mock.Storage, lastTS, ts, false)
	require.NoErrorf(t, err, "Error get ddl jobs: %s", err)
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	require.NoErrorf(t, err, "Flush failed", err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoErrorf(t, err, "Finially flush backupmeta failed", err)
	infoSchema, err := s.mock.Domain.GetSnapshotInfoSchema(ts)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	require.Truef(t, ok, "DB info not exist")
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	tables := []*metautil.Table{{
		DB:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	metaBytes, err := s.storage.ReadFile(ctx, metautil.MetaFile)
	require.NoError(t, err)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	require.NoError(t, err)
	// check the schema version
	require.Equal(t, int32(metautil.MetaV1), mockMeta.Version)
	metaReader := metautil.NewMetaReader(mockMeta, s.storage, &cipher)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	require.NoError(t, err)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	require.NoError(t, err)

	ddlJobs := restore.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		t.Logf("get ddl job: %s", job.Query)
	}
	require.Equal(t, 7, len(ddlJobs))
}

func TestFilterDDLJobsV2(t *testing.T) {
	s := createRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get ts: %s", err)

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	metaWriter := metautil.NewMetaWriter(s.storage, metautil.MetaFileSize, true, "", &cipher)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	s.mockGlue.SetSession(tk.Session())
	err = backup.WriteBackupDDLJobs(metaWriter, s.mockGlue, s.mock.Storage, lastTS, ts, false)
	require.NoErrorf(t, err, "Error get ddl jobs: %s", err)
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	require.NoErrorf(t, err, "Flush failed", err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoErrorf(t, err, "Flush BackupMeta failed", err)

	infoSchema, err := s.mock.Domain.GetSnapshotInfoSchema(ts)
	require.NoErrorf(t, err, "Error get snapshot info schema: %s", err)
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	require.Truef(t, ok, "DB info not exist")
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
	require.NoErrorf(t, err, "Error get table info: %s", err)
	tables := []*metautil.Table{{
		DB:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	metaBytes, err := s.storage.ReadFile(ctx, metautil.MetaFile)
	require.NoError(t, err)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	require.NoError(t, err)
	// check the schema version
	require.Equal(t, int32(metautil.MetaV2), mockMeta.Version)
	metaReader := metautil.NewMetaReader(mockMeta, s.storage, &cipher)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	require.NoError(t, err)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	require.NoError(t, err)

	ddlJobs := restore.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		t.Logf("get ddl job: %s", job.Query)
	}
	require.Equal(t, 7, len(ddlJobs))
}

func TestDB_ExecDDL(t *testing.T) {
	s := createRestoreSchemaSuite(t)

	ctx := context.Background()
	ddlJobs := []*model.Job{
		{
			Type:       model.ActionAddIndex,
			Query:      "CREATE DATABASE IF NOT EXISTS test_db;",
			BinlogInfo: &model.HistoryInfo{},
		},
		{
			Type:       model.ActionAddIndex,
			Query:      "",
			BinlogInfo: &model.HistoryInfo{},
		},
	}

	db, _, err := restore.NewDB(gluetidb.New(), s.mock.Storage, "STRICT")
	require.NoError(t, err)

	for _, ddlJob := range ddlJobs {
		err = db.ExecDDL(ctx, ddlJob)
		assert.NoError(t, err)
	}
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

	ddlJobs = restore.FilterDDLJobByRules(ddlJobs, restore.DDLJobBlockListRule)

	require.Equal(t, len(expectedDDLTypes), len(ddlJobs))
	for i, ddlJob := range ddlJobs {
		assert.Equal(t, expectedDDLTypes[i], ddlJob.Type)
	}
}

func TestGetExistedUserDBs(t *testing.T) {
	m, err := mock.NewCluster()
	require.Nil(t, err)
	defer m.Stop()
	dom := m.Domain

	dbs := restore.GetExistedUserDBs(dom)
	require.Equal(t, 0, len(dbs))

	builder, err := infoschema.NewBuilder(dom, nil, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{Name: model.NewCIStr("mysql")},
			{Name: model.NewCIStr("test")},
		},
		nil, nil, 1)
	require.Nil(t, err)
	dom.MockInfoCacheAndLoadInfoSchema(builder.Build(math.MaxUint64))
	dbs = restore.GetExistedUserDBs(dom)
	require.Equal(t, 0, len(dbs))

	builder, err = infoschema.NewBuilder(dom, nil, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{Name: model.NewCIStr("mysql")},
			{Name: model.NewCIStr("test")},
			{Name: model.NewCIStr("d1")},
		},
		nil, nil, 1)
	require.Nil(t, err)
	dom.MockInfoCacheAndLoadInfoSchema(builder.Build(math.MaxUint64))
	dbs = restore.GetExistedUserDBs(dom)
	require.Equal(t, 1, len(dbs))

	builder, err = infoschema.NewBuilder(dom, nil, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{Name: model.NewCIStr("mysql")},
			{Name: model.NewCIStr("d1")},
			{
				Name:   model.NewCIStr("test"),
				Tables: []*model.TableInfo{{ID: 1, Name: model.NewCIStr("t1"), State: model.StatePublic}},
				State:  model.StatePublic,
			},
		},
		nil, nil, 1)
	require.Nil(t, err)
	dom.MockInfoCacheAndLoadInfoSchema(builder.Build(math.MaxUint64))
	dbs = restore.GetExistedUserDBs(dom)
	require.Equal(t, 2, len(dbs))
}

// NOTICE: Once there is a new system table, BR needs to ensure that it is correctly classified:
//
// - IF it is an unrecoverable table, please add the table name into `unRecoverableTable`.
// - IF it is an system privilege table, please add the table name into `sysPrivilegeTableMap`.
// - IF it is an statistics table, please add the table name into `statsTables`.
//
// The above variables are in the file br/pkg/restore/systable_restore.go
func TestMonitorTheSystemTableIncremental(t *testing.T) {
	require.Equal(t, int64(199), session.CurrentBootstrapVersion)
}
