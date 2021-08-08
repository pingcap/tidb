// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore_test

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"testing"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/client-go/v2/oracle"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRestoreSchemaSuite{})

type testRestoreSchemaSuite struct {
	mock    *mock.Cluster
	storage storage.ExternalStorage
}

func (s *testRestoreSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
	base := c.MkDir()
	s.storage, err = storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
	c.Assert(s.mock.Start(), IsNil)
}

func (s *testRestoreSchemaSuite) TearDownSuite(c *C) {
	s.mock.Stop()
	testleak.AfterTest(c)()
}

func (s *testRestoreSchemaSuite) TestRestoreAutoIncID(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
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
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	// Get schemas of db and table
	info, err := s.mock.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue, Commentf("Error get db info"))
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("\"t\""))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))
	table := metautil.Table{
		Info: tableInfo.Meta(),
		DB:   dbInfo,
	}
	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.mock.Storage, dbInfo.ID, false, autoid.RowIDAllocType)
	globalAutoID, err := idAlloc.NextGlobalAutoID(table.Info.ID)
	c.Assert(err, IsNil, Commentf("Error allocate next auto id"))
	c.Assert(autoIncID, Equals, uint64(globalAutoID))
	// Alter AutoIncID to the next AutoIncID + 100
	table.Info.AutoIncID = globalAutoID + 100
	db, err := restore.NewDB(gluetidb.New(), s.mock.Storage)
	c.Assert(err, IsNil, Commentf("Error create DB"))
	tk.MustExec("drop database if exists test;")
	// Test empty collate value
	table.DB.Charset = "utf8mb4"
	table.DB.Collate = ""
	err = db.CreateDatabase(context.Background(), table.DB)
	c.Assert(err, IsNil, Commentf("Error create empty collate db: %s %s", err, s.mock.DSN))
	tk.MustExec("drop database if exists test;")
	// Test empty charset value
	table.DB.Charset = ""
	table.DB.Collate = "utf8mb4_bin"
	err = db.CreateDatabase(context.Background(), table.DB)
	c.Assert(err, IsNil, Commentf("Error create empty charset db: %s %s", err, s.mock.DSN))
	err = db.CreateTable(context.Background(), &table)
	c.Assert(err, IsNil, Commentf("Error create table: %s %s", err, s.mock.DSN))
	tk.MustExec("use test")
	// Check if AutoIncID is altered successfully
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show `\"t\"` next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	c.Assert(autoIncID, Equals, uint64(globalAutoID+100))
}

func (s *testRestoreSchemaSuite) TestFilterDDLJobs(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil, Commentf("Error get last ts: %s", err))
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil, Commentf("Error get ts: %s", err))

	metaWriter := metautil.NewMetaWriter(s.storage, metautil.MetaFileSize, false)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	err = backup.WriteBackupDDLJobs(metaWriter, s.mock.Storage, lastTS, ts)
	c.Assert(err, IsNil, Commentf("Error get ddl jobs: %s", err))
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	c.Assert(err, IsNil, Commentf("Flush failed", err))
	infoSchema, err := s.mock.Domain.GetSnapshotInfoSchema(ts)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	c.Assert(ok, IsTrue, Commentf("DB info not exist"))
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))
	tables := []*metautil.Table{{
		DB:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	metaBytes, err := s.storage.ReadFile(ctx, metautil.MetaFile)
	c.Assert(err, IsNil)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	c.Assert(err, IsNil)
	// check the schema version
	c.Assert(mockMeta.Version, Equals, int32(metautil.MetaV1))
	metaReader := metautil.NewMetaReader(mockMeta, s.storage)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	c.Assert(err, IsNil)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	c.Assert(err, IsNil)

	ddlJobs := restore.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		c.Logf("get ddl job: %s", job.Query)
	}
	c.Assert(len(ddlJobs), Equals, 7)
}

func (s *testRestoreSchemaSuite) TestFilterDDLJobsV2(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil, Commentf("Error get last ts: %s", err))
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	ts, err := s.mock.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil, Commentf("Error get ts: %s", err))

	metaWriter := metautil.NewMetaWriter(s.storage, metautil.MetaFileSize, true)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	err = backup.WriteBackupDDLJobs(metaWriter, s.mock.Storage, lastTS, ts)
	c.Assert(err, IsNil, Commentf("Error get ddl jobs: %s", err))
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	c.Assert(err, IsNil, Commentf("Flush failed", err))
	infoSchema, err := s.mock.Domain.GetSnapshotInfoSchema(ts)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, ok := infoSchema.SchemaByName(model.NewCIStr("test_db"))
	c.Assert(ok, IsTrue, Commentf("DB info not exist"))
	tableInfo, err := infoSchema.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_table"))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))
	tables := []*metautil.Table{{
		DB:   dbInfo,
		Info: tableInfo.Meta(),
	}}
	metaBytes, err := s.storage.ReadFile(ctx, metautil.MetaFile)
	c.Assert(err, IsNil)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	c.Assert(err, IsNil)
	// check the schema version
	c.Assert(mockMeta.Version, Equals, int32(metautil.MetaV2))
	metaReader := metautil.NewMetaReader(mockMeta, s.storage)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	c.Assert(err, IsNil)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	c.Assert(err, IsNil)

	ddlJobs := restore.FilterDDLJobs(allDDLJobs, tables)
	for _, job := range ddlJobs {
		c.Logf("get ddl job: %s", job.Query)
	}
	c.Assert(len(ddlJobs), Equals, 7)
}
