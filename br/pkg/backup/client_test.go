// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
)

type testBackup struct {
	ctx    context.Context
	cancel context.CancelFunc

	mockPDClient pd.Client
	backupClient *backup.Client

	cluster *mock.Cluster
	storage storage.ExternalStorage
}

func createBackupSuite(t *testing.T) (s *testBackup, clean func()) {
	tikvClient, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	s = new(testBackup)
	s.mockPDClient = pdClient
	s.ctx, s.cancel = context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{PdController: &pdutil.PdController{}}
	mockMgr.SetPDClient(s.mockPDClient)
	mockMgr.SetHTTP([]string{"test"}, nil)
	s.backupClient, err = backup.NewBackupClient(s.ctx, mockMgr)
	require.NoError(t, err)

	s.cluster, err = mock.NewCluster()
	require.NoError(t, err)
	base := t.TempDir()
	s.storage, err = storage.NewLocalStorage(base)
	require.NoError(t, err)
	require.NoError(t, s.cluster.Start())

	clean = func() {
		mockMgr.Close()
		s.cluster.Stop()
		tikvClient.Close()
		pdClient.Close()
	}
	return
}

func TestGetTS(t *testing.T) {
	s, clean := createBackupSuite(t)
	defer clean()

	// mockPDClient' physical ts and current ts will have deviation
	// so make this deviation tolerance 100ms
	deviation := 100

	// timeago not work
	expectedDuration := 0
	currentTS := time.Now().UnixNano() / int64(time.Millisecond)
	ts, err := s.backupClient.GetTS(s.ctx, 0, 0)
	require.NoError(t, err)
	pdTS := oracle.ExtractPhysical(ts)
	duration := int(currentTS - pdTS)
	require.Greater(t, duration, expectedDuration-deviation)
	require.Less(t, duration, expectedDuration+deviation)

	// timeago = "1.5m"
	expectedDuration = 90000
	currentTS = time.Now().UnixNano() / int64(time.Millisecond)
	ts, err = s.backupClient.GetTS(s.ctx, 90*time.Second, 0)
	require.NoError(t, err)
	pdTS = oracle.ExtractPhysical(ts)
	duration = int(currentTS - pdTS)
	require.Greater(t, duration, expectedDuration-deviation)
	require.Less(t, duration, expectedDuration+deviation)

	// timeago = "-1m"
	_, err = s.backupClient.GetTS(s.ctx, -time.Minute, 0)
	require.Error(t, err)
	require.Regexp(t, "negative timeago is not allowed.*", err.Error())

	// timeago = "1000000h" overflows
	_, err = s.backupClient.GetTS(s.ctx, 1000000*time.Hour, 0)
	require.Error(t, err)
	require.Regexp(t, ".*backup ts overflow.*", err.Error())

	// timeago = "10h" exceed GCSafePoint
	p, l, err := s.mockPDClient.GetTS(s.ctx)
	require.NoError(t, err)
	now := oracle.ComposeTS(p, l)
	_, err = s.mockPDClient.UpdateGCSafePoint(s.ctx, now)
	require.NoError(t, err)
	_, err = s.backupClient.GetTS(s.ctx, 10*time.Hour, 0)
	require.Error(t, err)
	require.Regexp(t, ".*GC safepoint [0-9]+ exceed TS [0-9]+.*", err.Error())

	// timeago and backupts both exists, use backupts
	backupts := oracle.ComposeTS(p+10, l)
	ts, err = s.backupClient.GetTS(s.ctx, time.Minute, backupts)
	require.NoError(t, err)
	require.Equal(t, backupts, ts)
}

func TestBuildTableRangeIntHandle(t *testing.T) {
	type Case struct {
		ids []int64
		trs []kv.KeyRange
	}
	low := codec.EncodeInt(nil, math.MinInt64)
	high := kv.Key(codec.EncodeInt(nil, math.MaxInt64)).PrefixNext()
	cases := []Case{
		{ids: []int64{1}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
		}},
		{ids: []int64{1, 2, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(2, low), EndKey: tablecodec.EncodeRowKey(2, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
		{ids: []int64{1, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
	}
	for _, cs := range cases {
		t.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := backup.BuildTableRanges(tbl)
		require.NoError(t, err)
		require.Equal(t, cs.trs, ranges)
	}

	tbl := &model.TableInfo{ID: 7}
	ranges, err := backup.BuildTableRanges(tbl)
	require.NoError(t, err)
	require.Equal(t, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	}, ranges)
}

func TestBuildTableRangeCommonHandle(t *testing.T) {
	type Case struct {
		ids []int64
		trs []kv.KeyRange
	}
	low, err_l := codec.EncodeKey(nil, nil, []types.Datum{types.MinNotNullDatum()}...)
	require.NoError(t, err_l)
	high, err_h := codec.EncodeKey(nil, nil, []types.Datum{types.MaxValueDatum()}...)
	require.NoError(t, err_h)
	high = kv.Key(high).PrefixNext()
	cases := []Case{
		{ids: []int64{1}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
		}},
		{ids: []int64{1, 2, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(2, low), EndKey: tablecodec.EncodeRowKey(2, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
		{ids: []int64{1, 3}, trs: []kv.KeyRange{
			{StartKey: tablecodec.EncodeRowKey(1, low), EndKey: tablecodec.EncodeRowKey(1, high)},
			{StartKey: tablecodec.EncodeRowKey(3, low), EndKey: tablecodec.EncodeRowKey(3, high)},
		}},
	}
	for _, cs := range cases {
		t.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}, IsCommonHandle: true}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := backup.BuildTableRanges(tbl)
		require.NoError(t, err)
		require.Equal(t, cs.trs, ranges)
	}

	tbl := &model.TableInfo{ID: 7, IsCommonHandle: true}
	ranges, err_r := backup.BuildTableRanges(tbl)
	require.NoError(t, err_r)
	require.Equal(t, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	}, ranges)
}

func TestOnBackupRegionErrorResponse(t *testing.T) {
	type Case struct {
		storeID           uint64
		bo                *tikv.Backoffer
		backupTS          uint64
		lockResolver      *txnlock.LockResolver
		resp              *backuppb.BackupResponse
		exceptedBackoffMs int
		exceptedErr       bool
	}
	newBackupRegionErrorResp := func(regionError *errorpb.Error) *backuppb.BackupResponse {
		return &backuppb.BackupResponse{Error: &backuppb.Error{Detail: &backuppb.Error_RegionError{RegionError: regionError}}}
	}

	cases := []Case{
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{NotLeader: &errorpb.NotLeader{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{KeyNotInRegion: &errorpb.KeyNotInRegion{}}), exceptedBackoffMs: 0, exceptedErr: true},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{StoreNotMatch: &errorpb.StoreNotMatch{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{}}), exceptedBackoffMs: 0, exceptedErr: true},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}}), exceptedBackoffMs: 1000, exceptedErr: false},
		{storeID: 1, backupTS: 421123291611137, resp: newBackupRegionErrorResp(&errorpb.Error{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}}), exceptedBackoffMs: 1000, exceptedErr: false},
	}
	for _, cs := range cases {
		t.Log(cs)
		_, backoffMs, err := backup.OnBackupResponse(cs.storeID, cs.bo, cs.backupTS, cs.lockResolver, cs.resp)
		require.Equal(t, cs.exceptedBackoffMs, backoffMs)
		if cs.exceptedErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestSendCreds(t *testing.T) {
	s, clean := createBackupSuite(t)
	defer clean()

	accessKey := "ab"
	secretAccessKey := "cd"
	backendOpt := storage.BackendOptions{
		S3: storage.S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		},
	}
	backend, err := storage.ParseBackend("s3://bucket/prefix/", &backendOpt)
	require.NoError(t, err)
	opts := &storage.ExternalStorageOptions{
		SendCredentials: true,
	}
	_, err = storage.New(s.ctx, backend, opts)
	require.NoError(t, err)
	access_key := backend.GetS3().AccessKey
	require.Equal(t, "ab", access_key)
	secret_access_key := backend.GetS3().SecretAccessKey
	require.Equal(t, "cd", secret_access_key)

	backendOpt = storage.BackendOptions{
		S3: storage.S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		},
	}
	backend, err = storage.ParseBackend("s3://bucket/prefix/", &backendOpt)
	require.NoError(t, err)
	opts = &storage.ExternalStorageOptions{
		SendCredentials: false,
	}
	_, err = storage.New(s.ctx, backend, opts)
	require.NoError(t, err)
	access_key = backend.GetS3().AccessKey
	require.Equal(t, "", access_key)
	secret_access_key = backend.GetS3().SecretAccessKey
	require.Equal(t, "", secret_access_key)
}

func TestSkipUnsupportedDDLJob(t *testing.T) {
	s, clean := createBackupSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.cluster.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := s.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("TRUNCATE TABLE test_table;")

	tk.MustExec("CREATE TABLE tb(id INT NOT NULL, stu_id INT NOT NULL) " +
		"PARTITION BY RANGE (stu_id) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11))")
	tk.MustExec("ALTER TABLE tb attributes \"merge_option=allow\"")
	tk.MustExec("ALTER TABLE tb PARTITION p0 attributes \"merge_option=deny\"")

	ts, err := s.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get ts: %s", err)

	cipher := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	metaWriter := metautil.NewMetaWriter(s.storage, metautil.MetaFileSize, false, "", &cipher)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	err = backup.WriteBackupDDLJobs(metaWriter, s.cluster.Storage, lastTS, ts)
	require.NoErrorf(t, err, "Error get ddl jobs: %s", err)
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	require.NoError(t, err, "Flush failed", err)
	err = metaWriter.FlushBackupMeta(ctx)
	require.NoError(t, err, "Finally flush backup meta failed", err)

	metaBytes, err := s.storage.ReadFile(ctx, metautil.MetaFile)
	require.NoError(t, err)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	require.NoError(t, err)
	// check the schema version
	metaReader := metautil.NewMetaReader(mockMeta, s.storage, &cipher)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	require.NoError(t, err)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	require.NoError(t, err)
	require.Len(t, allDDLJobs, 8)
}

func TestCheckBackupIsLocked(t *testing.T) {
	s, clean := createBackupSuite(t)
	defer clean()

	ctx := context.Background()

	// check passed with an empty storage
	err := backup.CheckBackupStorageIsLocked(ctx, s.storage)
	require.NoError(t, err)

	// check passed with only a lock file
	err = s.storage.WriteFile(ctx, metautil.LockFile, nil)
	require.NoError(t, err)
	err = backup.CheckBackupStorageIsLocked(ctx, s.storage)
	require.NoError(t, err)

	// check passed with a lock file and other non-sst files.
	err = s.storage.WriteFile(ctx, "1.txt", nil)
	require.NoError(t, err)
	err = backup.CheckBackupStorageIsLocked(ctx, s.storage)
	require.NoError(t, err)

	// check failed
	err = s.storage.WriteFile(ctx, "1.sst", nil)
	require.NoError(t, err)
	err = backup.CheckBackupStorageIsLocked(ctx, s.storage)
	require.Error(t, err)
	require.Regexp(t, "backup lock file and sst file exist in(.+)", err.Error())
}
