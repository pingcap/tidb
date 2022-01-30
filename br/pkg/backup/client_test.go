// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testkit"
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

var _ = Suite(&testBackup{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testBackup) SetUpSuite(c *C) {
	_, _, pdClient, err := testutils.NewMockTiKV("", nil)
	c.Assert(err, IsNil)
	r.mockPDClient = pdClient
	r.ctx, r.cancel = context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{PdController: &pdutil.PdController{}}
	mockMgr.SetPDClient(r.mockPDClient)
	mockMgr.SetHTTP([]string{"test"}, nil)
	r.backupClient, err = backup.NewBackupClient(r.ctx, mockMgr)
	c.Assert(err, IsNil)

	r.cluster, err = mock.NewCluster()
	c.Assert(err, IsNil)
	base := c.MkDir()
	r.storage, err = storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
	//c.Assert(r.cluster.Start(), IsNil)

}

func (r *testBackup) resetStorage(c *C) {
	var err error
	base := c.MkDir()
	r.storage, err = storage.NewLocalStorage(base)
	c.Assert(err, IsNil)
}

func (r *testBackup) TestGetTS(c *C) {
	var (
		err error
		// mockPDClient' physical ts and current ts will have deviation
		// so make this deviation tolerance 100ms
		deviation = 100
	)

	// timeago not work
	expectedDuration := 0
	currentTS := time.Now().UnixNano() / int64(time.Millisecond)
	ts, err := r.backupClient.GetTS(r.ctx, 0, 0)
	c.Assert(err, IsNil)
	pdTS := oracle.ExtractPhysical(ts)
	duration := int(currentTS - pdTS)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "1.5m"
	expectedDuration = 90000
	currentTS = time.Now().UnixNano() / int64(time.Millisecond)
	ts, err = r.backupClient.GetTS(r.ctx, 90*time.Second, 0)
	c.Assert(err, IsNil)
	pdTS = oracle.ExtractPhysical(ts)
	duration = int(currentTS - pdTS)
	c.Assert(duration, Greater, expectedDuration-deviation)
	c.Assert(duration, Less, expectedDuration+deviation)

	// timeago = "-1m"
	_, err = r.backupClient.GetTS(r.ctx, -time.Minute, 0)
	c.Assert(err, ErrorMatches, "negative timeago is not allowed.*")

	// timeago = "1000000h" overflows
	_, err = r.backupClient.GetTS(r.ctx, 1000000*time.Hour, 0)
	c.Assert(err, ErrorMatches, ".*backup ts overflow.*")

	// timeago = "10h" exceed GCSafePoint
	p, l, err := r.mockPDClient.GetTS(r.ctx)
	c.Assert(err, IsNil)
	now := oracle.ComposeTS(p, l)
	_, err = r.mockPDClient.UpdateGCSafePoint(r.ctx, now)
	c.Assert(err, IsNil)
	_, err = r.backupClient.GetTS(r.ctx, 10*time.Hour, 0)
	c.Assert(err, ErrorMatches, ".*GC safepoint [0-9]+ exceed TS [0-9]+.*")

	// timeago and backupts both exists, use backupts
	backupts := oracle.ComposeTS(p+10, l)
	ts, err = r.backupClient.GetTS(r.ctx, time.Minute, backupts)
	c.Assert(err, IsNil)
	c.Assert(ts, Equals, backupts)
}

func (r *testBackup) TestBuildTableRangeIntHandle(c *C) {
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
		c.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := backup.BuildTableRanges(tbl)
		c.Assert(err, IsNil)
		c.Assert(ranges, DeepEquals, cs.trs)
	}

	tbl := &model.TableInfo{ID: 7}
	ranges, err := backup.BuildTableRanges(tbl)
	c.Assert(err, IsNil)
	c.Assert(ranges, DeepEquals, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	})
}

func (r *testBackup) TestBuildTableRangeCommonHandle(c *C) {
	type Case struct {
		ids []int64
		trs []kv.KeyRange
	}
	low, err_l := codec.EncodeKey(nil, nil, []types.Datum{types.MinNotNullDatum()}...)
	c.Assert(err_l, IsNil)
	high, err_h := codec.EncodeKey(nil, nil, []types.Datum{types.MaxValueDatum()}...)
	c.Assert(err_h, IsNil)
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
		c.Log(cs)
		tbl := &model.TableInfo{Partition: &model.PartitionInfo{Enable: true}, IsCommonHandle: true}
		for _, id := range cs.ids {
			tbl.Partition.Definitions = append(tbl.Partition.Definitions,
				model.PartitionDefinition{ID: id})
		}
		ranges, err := backup.BuildTableRanges(tbl)
		c.Assert(err, IsNil)
		c.Assert(ranges, DeepEquals, cs.trs)
	}

	tbl := &model.TableInfo{ID: 7, IsCommonHandle: true}
	ranges, err_r := backup.BuildTableRanges(tbl)
	c.Assert(err_r, IsNil)
	c.Assert(ranges, DeepEquals, []kv.KeyRange{
		{StartKey: tablecodec.EncodeRowKey(7, low), EndKey: tablecodec.EncodeRowKey(7, high)},
	})
}

func (r *testBackup) TestOnBackupRegionErrorResponse(c *C) {
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
		c.Log(cs)
		_, backoffMs, err := backup.OnBackupResponse(cs.storeID, cs.bo, cs.backupTS, cs.lockResolver, cs.resp)
		c.Assert(backoffMs, Equals, cs.exceptedBackoffMs)
		if cs.exceptedErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (r *testBackup) TestSendCreds(c *C) {
	accessKey := "ab"
	secretAccessKey := "cd"
	backendOpt := storage.BackendOptions{
		S3: storage.S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		},
	}
	backend, err := storage.ParseBackend("s3://bucket/prefix/", &backendOpt)
	c.Assert(err, IsNil)
	opts := &storage.ExternalStorageOptions{
		SendCredentials: true,
	}
	_, err = storage.New(r.ctx, backend, opts)
	c.Assert(err, IsNil)
	access_key := backend.GetS3().AccessKey
	c.Assert(access_key, Equals, "ab")
	secret_access_key := backend.GetS3().SecretAccessKey
	c.Assert(secret_access_key, Equals, "cd")

	backendOpt = storage.BackendOptions{
		S3: storage.S3BackendOptions{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		},
	}
	backend, err = storage.ParseBackend("s3://bucket/prefix/", &backendOpt)
	c.Assert(err, IsNil)
	opts = &storage.ExternalStorageOptions{
		SendCredentials: false,
	}
	_, err = storage.New(r.ctx, backend, opts)
	c.Assert(err, IsNil)
	access_key = backend.GetS3().AccessKey
	c.Assert(access_key, Equals, "")
	secret_access_key = backend.GetS3().SecretAccessKey
	c.Assert(secret_access_key, Equals, "")
}

func (r *testBackup) TestskipUnsupportedDDLJob(c *C) {
	tk := testkit.NewTestKit(c, r.cluster.Storage)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS, err := r.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil, Commentf("Error get last ts: %s", err))
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

	ts, err := r.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil, Commentf("Error get ts: %s", err))

	cipher := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	metaWriter := metautil.NewMetaWriter(r.storage, metautil.MetaFileSize, false, &cipher)
	ctx := context.Background()
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDDL)
	err = backup.WriteBackupDDLJobs(metaWriter, r.cluster.Storage, lastTS, ts)
	c.Assert(err, IsNil, Commentf("Error get ddl jobs: %s", err))
	err = metaWriter.FinishWriteMetas(ctx, metautil.AppendDDL)
	c.Assert(err, IsNil, Commentf("Flush failed", err))
	err = metaWriter.FlushBackupMeta(ctx)
	c.Assert(err, IsNil, Commentf("Finially flush backupmeta failed", err))

	metaBytes, err := r.storage.ReadFile(ctx, metautil.MetaFile)
	c.Assert(err, IsNil)
	mockMeta := &backuppb.BackupMeta{}
	err = proto.Unmarshal(metaBytes, mockMeta)
	c.Assert(err, IsNil)
	// check the schema version
	metaReader := metautil.NewMetaReader(mockMeta, r.storage, &cipher)
	allDDLJobsBytes, err := metaReader.ReadDDLs(ctx)
	c.Assert(err, IsNil)
	var allDDLJobs []*model.Job
	err = json.Unmarshal(allDDLJobsBytes, &allDDLJobs)
	c.Assert(err, IsNil)
	c.Assert(len(allDDLJobs), Equals, 8)
}

func (r *testBackup) TestCheckBackupIsLocked(c *C) {
	ctx := context.Background()

	r.resetStorage(c)
	// check passed with an empty storage
	err := backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, IsNil)

	// check passed with only a lock file
	err = r.storage.WriteFile(ctx, metautil.LockFile, nil)
	c.Assert(err, IsNil)
	err = backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, IsNil)

	// check passed with a lock file and other non-sst files.
	err = r.storage.WriteFile(ctx, "1.txt", nil)
	c.Assert(err, IsNil)
	err = backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, IsNil)

	// check failed
	err = r.storage.WriteFile(ctx, "1.sst", nil)
	c.Assert(err, IsNil)
	err = backup.CheckBackupStorageIsLocked(ctx, r.storage)
	c.Assert(err, ErrorMatches, "backup lock file and sst file exist in(.+)")
}
