// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testBackupSchemaSuite{})

type testBackupSchemaSuite struct {
	mock *mock.Cluster
}

func (s *testBackupSchemaSuite) SetUpSuite(c *C) {
	var err error
	s.mock, err = mock.NewCluster()
	c.Assert(err, IsNil)
	c.Assert(s.mock.Start(), IsNil)
}

func (s *testBackupSchemaSuite) TearDownSuite(c *C) {
	s.mock.Stop()
	testleak.AfterTest(c)()
}

func (s *testBackupSchemaSuite) GetRandomStorage(c *C) storage.ExternalStorage {
	base := c.MkDir()
	es, err := storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
	return es
}

func (s *testBackupSchemaSuite) GetSchemasFromMeta(c *C, es storage.ExternalStorage) []*metautil.Table {
	ctx := context.Background()
	metaBytes, err := es.ReadFile(ctx, metautil.MetaFile)
	c.Assert(err, IsNil)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	c.Assert(err, IsNil)
	metaReader := metautil.NewMetaReader(mockMeta,
		es,
		&backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	)

	output := make(chan *metautil.Table, 4)
	go func() {
		err = metaReader.ReadSchemasFiles(ctx, output)
		c.Assert(err, IsNil)
		close(output)
	}()

	schemas := make([]*metautil.Table, 0, 4)
	for s := range output {
		schemas = append(schemas, s)
	}
	return schemas
}

type simpleProgress struct {
	counter int64
}

func (sp *simpleProgress) Inc() {
	atomic.AddInt64(&sp.counter, 1)
}

func (sp *simpleProgress) Close() {}

func (sp *simpleProgress) reset() {
	atomic.StoreInt64(&sp.counter, 0)
}

func (sp *simpleProgress) get() int64 {
	return atomic.LoadInt64(&sp.counter)
}

func (s *testBackupSchemaSuite) TestBuildBackupRangeAndSchema(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)

	// Table t1 is not exist.
	testFilter, err := filter.Parse([]string{"test.t1"})
	c.Assert(err, IsNil)
	_, backupSchemas, err := backup.BuildBackupRangeAndSchema(
		s.mock.Storage, testFilter, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, IsNil)

	// Database is not exist.
	fooFilter, err := filter.Parse([]string{"foo.t1"})
	c.Assert(err, IsNil)
	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Storage, fooFilter, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, IsNil)

	// Empty database.
	// Filter out system tables manually.
	noFilter, err := filter.Parse([]string{"*.*", "!mysql.*"})
	c.Assert(err, IsNil)
	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Storage, noFilter, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas, IsNil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")

	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Storage, testFilter, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 1)
	updateCh := new(simpleProgress)
	skipChecksum := false
	es := s.GetRandomStorage(c)
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	metaWriter := metautil.NewMetaWriter(es, metautil.MetaFileSize, false, &cipher)
	ctx := context.Background()
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter, s.mock.Storage, nil, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	c.Assert(updateCh.get(), Equals, int64(1))
	c.Assert(err, IsNil)
	err = metaWriter.FlushBackupMeta(ctx)
	c.Assert(err, IsNil)

	schemas := s.GetSchemasFromMeta(c, es)
	c.Assert(len(schemas), Equals, 1)
	// Cluster returns a dummy checksum (all fields are 1).
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[0]))

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("insert into t2 values (10);")
	tk.MustExec("insert into t2 values (11);")

	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(
		s.mock.Storage, noFilter, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 2)
	updateCh.reset()

	es2 := s.GetRandomStorage(c)
	metaWriter2 := metautil.NewMetaWriter(es2, metautil.MetaFileSize, false, &cipher)
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter2, s.mock.Storage, nil, math.MaxUint64, 2, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	c.Assert(updateCh.get(), Equals, int64(2))
	c.Assert(err, IsNil)
	err = metaWriter2.FlushBackupMeta(ctx)
	c.Assert(err, IsNil)

	schemas = s.GetSchemasFromMeta(c, es2)

	c.Assert(len(schemas), Equals, 2)
	// Cluster returns a dummy checksum (all fields are 1).
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[0]))
	c.Assert(schemas[1].Crc64Xor, Not(Equals), 0, Commentf("%v", schemas[1]))
	c.Assert(schemas[1].TotalKvs, Not(Equals), 0, Commentf("%v", schemas[1]))
	c.Assert(schemas[1].TotalBytes, Not(Equals), 0, Commentf("%v", schemas[1]))
}

func (s *testBackupSchemaSuite) TestBuildBackupRangeAndSchemaWithBrokenStats(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t3;")
	tk.MustExec("create table t3 (a char(1));")
	tk.MustExec("insert into t3 values ('1');")
	tk.MustExec("analyze table t3;")
	// corrupt the statistics like pingcap/br#679.
	tk.MustExec(`
		update mysql.stats_buckets set upper_bound = 0xffffffff
		where table_id = (
			select tidb_table_id from information_schema.tables
			where (table_schema, table_name) = ('test', 't3')
		);
	`)

	f, err := filter.Parse([]string{"test.t3"})
	c.Assert(err, IsNil)

	_, backupSchemas, err := backup.BuildBackupRangeAndSchema(s.mock.Storage, f, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 1)

	skipChecksum := false
	updateCh := new(simpleProgress)

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	es := s.GetRandomStorage(c)
	metaWriter := metautil.NewMetaWriter(es, metautil.MetaFileSize, false, &cipher)
	ctx := context.Background()
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter, s.mock.Storage, nil, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	c.Assert(err, IsNil)
	err = metaWriter.FlushBackupMeta(ctx)
	c.Assert(err, IsNil)

	schemas := s.GetSchemasFromMeta(c, es)
	c.Assert(err, IsNil)
	c.Assert(schemas, HasLen, 1)
	// the stats should be empty, but other than that everything should be backed up.
	c.Assert(schemas[0].Stats, IsNil)
	c.Assert(schemas[0].Crc64Xor, Not(Equals), 0)
	c.Assert(schemas[0].TotalKvs, Not(Equals), 0)
	c.Assert(schemas[0].TotalBytes, Not(Equals), 0)
	c.Assert(schemas[0].Info, NotNil)
	c.Assert(schemas[0].DB, NotNil)

	// recover the statistics.
	tk.MustExec("analyze table t3;")

	_, backupSchemas, err = backup.BuildBackupRangeAndSchema(s.mock.Storage, f, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, 1)

	updateCh.reset()
	statsHandle := s.mock.Domain.StatsHandle()
	es2 := s.GetRandomStorage(c)
	metaWriter2 := metautil.NewMetaWriter(es2, metautil.MetaFileSize, false, &cipher)
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter2, s.mock.Storage, statsHandle, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	c.Assert(err, IsNil)
	err = metaWriter2.FlushBackupMeta(ctx)
	c.Assert(err, IsNil)

	schemas2 := s.GetSchemasFromMeta(c, es2)
	c.Assert(schemas2, HasLen, 1)
	// the stats should now be filled, and other than that the result should be equivalent to the first backup.
	c.Assert(schemas2[0].Stats, NotNil)
	c.Assert(schemas2[0].Crc64Xor, Equals, schemas[0].Crc64Xor)
	c.Assert(schemas2[0].TotalKvs, Equals, schemas[0].TotalKvs)
	c.Assert(schemas2[0].TotalBytes, Equals, schemas[0].TotalBytes)
	c.Assert(schemas2[0].Info, DeepEquals, schemas[0].Info)
	c.Assert(schemas2[0].DB, DeepEquals, schemas[0].DB)
}

func (s *testBackupSchemaSuite) TestBackupSchemasForSystemTable(c *C) {
	tk := testkit.NewTestKit(c, s.mock.Storage)
	es2 := s.GetRandomStorage(c)

	systemTablesCount := 32
	tablePrefix := "systable"
	tk.MustExec("use mysql")
	for i := 1; i <= systemTablesCount; i++ {
		query := fmt.Sprintf("create table %s%d (a char(1));", tablePrefix, i)
		tk.MustExec(query)
	}

	f, err := filter.Parse([]string{"mysql.systable*"})
	c.Assert(err, IsNil)
	_, backupSchemas, err := backup.BuildBackupRangeAndSchema(s.mock.Storage, f, math.MaxUint64)
	c.Assert(err, IsNil)
	c.Assert(backupSchemas.Len(), Equals, systemTablesCount)

	ctx := context.Background()
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	updateCh := new(simpleProgress)

	metaWriter2 := metautil.NewMetaWriter(es2, metautil.MetaFileSize, false, &cipher)
	err = backupSchemas.BackupSchemas(ctx, metaWriter2, s.mock.Storage, nil,
		math.MaxUint64, 1, variable.DefChecksumTableConcurrency, true, updateCh)
	c.Assert(err, IsNil)
	err = metaWriter2.FlushBackupMeta(ctx)
	c.Assert(err, IsNil)

	schemas2 := s.GetSchemasFromMeta(c, es2)
	c.Assert(schemas2, HasLen, systemTablesCount)
	for _, schema := range schemas2 {
		c.Assert(schema.DB.Name, Equals, utils.TemporaryDBName("mysql"))
		c.Assert(strings.HasPrefix(schema.Info.Name.O, tablePrefix), Equals, true)
	}
}
