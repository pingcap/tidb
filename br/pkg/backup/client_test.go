// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	pd "github.com/tikv/pd/client"
	"go.opencensus.io/stats/view"
)

type testBackup struct {
	ctx    context.Context
	cancel context.CancelFunc

	mockPDClient pd.Client
	mockCluster  *testutils.MockCluster
	mockGlue     *gluemock.MockGlue
	backupClient *backup.Client

	cluster *mock.Cluster
	storage storage.ExternalStorage
}

func createBackupSuite(t *testing.T) *testBackup {
	tikvClient, mockCluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	s := new(testBackup)
	s.mockGlue = &gluemock.MockGlue{}
	s.mockPDClient = pdClient
	s.mockCluster = mockCluster
	s.ctx, s.cancel = context.WithCancel(context.Background())
	mockMgr := &conn.Mgr{PdController: &pdutil.PdController{}}
	mockMgr.SetPDClient(s.mockPDClient)
	s.backupClient = backup.NewBackupClient(s.ctx, mockMgr)

	s.cluster, err = mock.NewCluster()
	require.NoError(t, err)
	base := t.TempDir()
	s.storage, err = storage.NewLocalStorage(base)
	require.NoError(t, err)
	require.NoError(t, s.cluster.Start())

	t.Cleanup(func() {
		mockMgr.Close()
		s.cluster.Stop()
		tikvClient.Close()
		pdClient.Close()
		view.Stop()
	})
	return s
}

func TestGetTS(t *testing.T) {
	s := createBackupSuite(t)

	// mockPDClient' physical ts and current ts will have deviation
	// so make this deviation tolerance 100ms
	deviation := 100

	// timeago not work
	expectedDuration := 0
	currentTS := time.Now().UnixMilli()
	ts, err := s.backupClient.GetTS(s.ctx, 0, 0)
	require.NoError(t, err)
	pdTS := oracle.ExtractPhysical(ts)
	duration := int(currentTS - pdTS)
	require.Greater(t, duration, expectedDuration-deviation)
	require.Less(t, duration, expectedDuration+deviation)

	// timeago = "1.5m"
	expectedDuration = 90000
	currentTS = time.Now().UnixMilli()
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

func TestSkipUnsupportedDDLJob(t *testing.T) {
	s := createBackupSuite(t)

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
	s.mockGlue.SetSession(tk.Session())
	err = backup.WriteBackupDDLJobs(metaWriter, s.mockGlue, s.cluster.Storage, lastTS, ts, false)
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
	s := createBackupSuite(t)

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

func TestOnBackupResponse(t *testing.T) {
	s := createBackupSuite(t)

	ctx := context.Background()

	buildProgressRangeFn := func(startKey []byte, endKey []byte) *rtree.ProgressRange {
		return &rtree.ProgressRange{
			Res: rtree.NewRangeTree(),
			Origin: rtree.Range{
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}

	errContext := utils.NewErrorContext("test", 1)
	require.Nil(t, s.backupClient.OnBackupResponse(ctx, nil, errContext, nil))

	tree := rtree.NewProgressRangeTree()
	r := &backup.ResponseAndStore{
		StoreID: 0,
		Resp: &backuppb.BackupResponse{
			Error: &backuppb.Error{
				Msg: "test",
			},
		},
	}
	// case #1: error resposne
	// first error can be ignored due to errContext.
	require.NoError(t, s.backupClient.OnBackupResponse(ctx, r, errContext, &tree))
	// second error cannot be ignored.
	require.Error(t, s.backupClient.OnBackupResponse(ctx, r, errContext, &tree))

	// case #2: normal resposne
	r = &backup.ResponseAndStore{
		StoreID: 0,
		Resp: &backuppb.BackupResponse{
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	require.NoError(t, tree.Insert(buildProgressRangeFn([]byte("aa"), []byte("c"))))
	// error due to the tree range does not match response range.
	require.Error(t, s.backupClient.OnBackupResponse(ctx, r, errContext, &tree))

	// case #3: partial range success case, find incomplete range
	r.Resp.StartKey = []byte("aa")
	require.NoError(t, s.backupClient.OnBackupResponse(ctx, r, errContext, &tree))

	incomplete := tree.Iter().GetIncompleteRanges()
	require.Len(t, incomplete, 1)
	require.Equal(t, []byte("b"), incomplete[0].StartKey)
	require.Equal(t, []byte("c"), incomplete[0].EndKey)

	// case #4: success case, make up incomplete range
	r.Resp.StartKey = []byte("b")
	r.Resp.EndKey = []byte("c")
	require.NoError(t, s.backupClient.OnBackupResponse(ctx, r, errContext, &tree))
	incomplete = tree.Iter().GetIncompleteRanges()
	require.Len(t, incomplete, 0)
}

func TestBuildProgressRangeTree(t *testing.T) {
	s := createBackupSuite(t)
	ranges := []rtree.Range{
		{
			StartKey: []byte("aa"),
			EndKey:   []byte("b"),
		},
		{
			StartKey: []byte("c"),
			EndKey:   []byte("d"),
		},
	}
	tree, err := s.backupClient.BuildProgressRangeTree(ranges)
	require.NoError(t, err)

	contained, err := tree.FindContained([]byte("a"), []byte("aa"))
	require.Nil(t, contained)
	require.Error(t, err)

	contained, err = tree.FindContained([]byte("b"), []byte("ba"))
	require.Nil(t, contained)
	require.Error(t, err)

	contained, err = tree.FindContained([]byte("e"), []byte("ea"))
	require.Nil(t, contained)
	require.Error(t, err)

	contained, err = tree.FindContained([]byte("aa"), []byte("b"))
	require.NotNil(t, contained)
	require.Equal(t, []byte("aa"), contained.Origin.StartKey)
	require.Equal(t, []byte("b"), contained.Origin.EndKey)
	require.NoError(t, err)
}
