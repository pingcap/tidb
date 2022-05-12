// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
)

func createMockCluster(t *testing.T) (m *mock.Cluster, clean func()) {
	var err error
	m, err = mock.NewCluster()
	require.NoError(t, err)
	require.NoError(t, m.Start())
	clean = func() {
		m.Stop()
	}
	return
}

func GetRandomStorage(t *testing.T) storage.ExternalStorage {
	base := t.TempDir()
	es, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	return es
}

func GetSchemasFromMeta(t *testing.T, es storage.ExternalStorage) []*metautil.Table {
	ctx := context.Background()
	metaBytes, err := es.ReadFile(ctx, metautil.MetaFile)
	require.NoError(t, err)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	require.NoError(t, err)
	metaReader := metautil.NewMetaReader(mockMeta,
		es,
		&backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	)

	output := make(chan *metautil.Table, 4)
	go func() {
		err = metaReader.ReadSchemasFiles(ctx, output)
		require.NoError(t, err)
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

func TestBuildBackupRangeAndSchema(t *testing.T) {
	m, clean := createMockCluster(t)
	defer clean()

	tk := testkit.NewTestKit(t, m.Storage)

	// Table t1 is not exist.
	testFilter, err := filter.Parse([]string{"test.t1"})
	require.NoError(t, err)
	_, backupSchemas, _, err := backup.BuildBackupRangeAndSchema(
		m.Storage, testFilter, math.MaxUint64, false)
	require.NoError(t, err)
	require.NotNil(t, backupSchemas)

	// Database is not exist.
	fooFilter, err := filter.Parse([]string{"foo.t1"})
	require.NoError(t, err)
	_, backupSchemas, _, err = backup.BuildBackupRangeAndSchema(
		m.Storage, fooFilter, math.MaxUint64, false)
	require.NoError(t, err)
	require.Nil(t, backupSchemas)

	// Empty database.
	// Filter out system tables manually.
	noFilter, err := filter.Parse([]string{"*.*", "!mysql.*"})
	require.NoError(t, err)
	_, backupSchemas, _, err = backup.BuildBackupRangeAndSchema(
		m.Storage, noFilter, math.MaxUint64, false)
	require.NoError(t, err)
	require.NotNil(t, backupSchemas)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (10);")
	tk.MustExec("create placement policy fivereplicas followers=4;")

	var policies []*backuppb.PlacementPolicy
	_, backupSchemas, policies, err = backup.BuildBackupRangeAndSchema(
		m.Storage, testFilter, math.MaxUint64, false)
	require.NoError(t, err)
	require.Equal(t, 1, backupSchemas.Len())
	// we expect no policies collected, because it's not full backup.
	require.Equal(t, 0, len(policies))
	updateCh := new(simpleProgress)
	skipChecksum := false
	es := GetRandomStorage(t)
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	metaWriter := metautil.NewMetaWriter(es, metautil.MetaFileSize, false, "", &cipher)
	ctx := context.Background()
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter, m.Storage, nil, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	require.Equal(t, int64(1), updateCh.get())
	require.NoError(t, err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoError(t, err)

	schemas := GetSchemasFromMeta(t, es)
	require.Len(t, schemas, 1)
	// Cluster returns a dummy checksum (all fields are 1).
	require.NotZerof(t, schemas[0].Crc64Xor, "%v", schemas[0])
	require.NotZerof(t, schemas[0].TotalKvs, "%v", schemas[0])
	require.NotZerof(t, schemas[0].TotalBytes, "%v", schemas[0])

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("insert into t2 values (10);")
	tk.MustExec("insert into t2 values (11);")

	_, backupSchemas, policies, err = backup.BuildBackupRangeAndSchema(
		m.Storage, noFilter, math.MaxUint64, true)
	require.NoError(t, err)
	require.Equal(t, 2, backupSchemas.Len())
	// we expect the policy fivereplicas collected in full backup.
	require.Equal(t, 1, len(policies))
	updateCh.reset()

	es2 := GetRandomStorage(t)
	metaWriter2 := metautil.NewMetaWriter(es2, metautil.MetaFileSize, false, "", &cipher)
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter2, m.Storage, nil, math.MaxUint64, 2, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	require.Equal(t, int64(2), updateCh.get())
	require.NoError(t, err)
	err = metaWriter2.FlushBackupMeta(ctx)
	require.NoError(t, err)

	schemas = GetSchemasFromMeta(t, es2)

	require.Len(t, schemas, 2)
	// Cluster returns a dummy checksum (all fields are 1).
	require.NotZerof(t, schemas[0].Crc64Xor, "%v", schemas[0])
	require.NotZerof(t, schemas[0].TotalKvs, "%v", schemas[0])
	require.NotZerof(t, schemas[0].TotalBytes, "%v", schemas[0])
	require.NotZerof(t, schemas[1].Crc64Xor, "%v", schemas[1])
	require.NotZerof(t, schemas[1].TotalKvs, "%v", schemas[1])
	require.NotZerof(t, schemas[1].TotalBytes, "%v", schemas[1])
}

func TestBuildBackupRangeAndSchemaWithBrokenStats(t *testing.T) {
	m, clean := createMockCluster(t)
	defer clean()

	tk := testkit.NewTestKit(t, m.Storage)
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
	require.NoError(t, err)

	_, backupSchemas, _, err := backup.BuildBackupRangeAndSchema(m.Storage, f, math.MaxUint64, false)
	require.NoError(t, err)
	require.Equal(t, 1, backupSchemas.Len())

	skipChecksum := false
	updateCh := new(simpleProgress)

	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}

	es := GetRandomStorage(t)
	metaWriter := metautil.NewMetaWriter(es, metautil.MetaFileSize, false, "", &cipher)
	ctx := context.Background()
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter, m.Storage, nil, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	require.NoError(t, err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoError(t, err)

	schemas := GetSchemasFromMeta(t, es)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	// the stats should be empty, but other than that everything should be backed up.
	require.Nil(t, schemas[0].Stats)
	require.NotZerof(t, schemas[0].Crc64Xor, "%v", schemas[0])
	require.NotZerof(t, schemas[0].TotalKvs, "%v", schemas[0])
	require.NotZerof(t, schemas[0].TotalBytes, "%v", schemas[0])
	require.NotNil(t, schemas[0].Info)
	require.NotNil(t, schemas[0].DB)

	// recover the statistics.
	tk.MustExec("analyze table t3;")

	_, backupSchemas, _, err = backup.BuildBackupRangeAndSchema(m.Storage, f, math.MaxUint64, false)
	require.NoError(t, err)
	require.Equal(t, 1, backupSchemas.Len())

	updateCh.reset()
	statsHandle := m.Domain.StatsHandle()
	es2 := GetRandomStorage(t)
	metaWriter2 := metautil.NewMetaWriter(es2, metautil.MetaFileSize, false, "", &cipher)
	err = backupSchemas.BackupSchemas(
		ctx, metaWriter2, m.Storage, statsHandle, math.MaxUint64, 1, variable.DefChecksumTableConcurrency, skipChecksum, updateCh)
	require.NoError(t, err)
	err = metaWriter2.FlushBackupMeta(ctx)
	require.NoError(t, err)

	schemas2 := GetSchemasFromMeta(t, es2)
	require.Len(t, schemas2, 1)
	// the stats should now be filled, and other than that the result should be equivalent to the first backup.
	require.NotNil(t, schemas2[0].Stats)
	require.Equal(t, schemas[0].Crc64Xor, schemas2[0].Crc64Xor)
	require.Equal(t, schemas[0].TotalKvs, schemas2[0].TotalKvs)
	require.Equal(t, schemas[0].TotalBytes, schemas2[0].TotalBytes)
	require.Equal(t, schemas[0].Info, schemas2[0].Info)
	require.Equal(t, schemas[0].DB, schemas2[0].DB)
}

func TestBackupSchemasForSystemTable(t *testing.T) {
	m, clean := createMockCluster(t)
	defer clean()

	tk := testkit.NewTestKit(t, m.Storage)
	es2 := GetRandomStorage(t)

	systemTablesCount := 32
	tablePrefix := "systable"
	tk.MustExec("use mysql")
	for i := 1; i <= systemTablesCount; i++ {
		query := fmt.Sprintf("create table %s%d (a char(1));", tablePrefix, i)
		tk.MustExec(query)
	}

	f, err := filter.Parse([]string{"mysql.systable*"})
	require.NoError(t, err)
	_, backupSchemas, _, err := backup.BuildBackupRangeAndSchema(m.Storage, f, math.MaxUint64, false)
	require.NoError(t, err)
	require.Equal(t, systemTablesCount, backupSchemas.Len())

	ctx := context.Background()
	cipher := backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
	}
	updateCh := new(simpleProgress)

	metaWriter2 := metautil.NewMetaWriter(es2, metautil.MetaFileSize, false, "", &cipher)
	err = backupSchemas.BackupSchemas(ctx, metaWriter2, m.Storage, nil,
		math.MaxUint64, 1, variable.DefChecksumTableConcurrency, true, updateCh)
	require.NoError(t, err)
	err = metaWriter2.FlushBackupMeta(ctx)
	require.NoError(t, err)

	schemas2 := GetSchemasFromMeta(t, es2)
	require.Len(t, schemas2, systemTablesCount)
	for _, schema := range schemas2 {
		require.Equal(t, utils.TemporaryDBName("mysql"), schema.DB.Name)
		require.Equal(t, true, strings.HasPrefix(schema.Info.Name.O, tablePrefix))
	}
}
