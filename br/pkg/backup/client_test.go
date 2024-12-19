// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	gluemock "github.com/pingcap/tidb/br/pkg/gluetidb/mock"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
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

// locks used to solve race when mock then behaviour when store drops
var lock sync.Mutex

// check the connect behaviour in test.
var connectedStore map[uint64]int

var _ backup.BackupSender = (*mockBackupBackupSender)(nil)

func mockGetBackupClientCallBack(ctx context.Context, storeID uint64, reset bool) (backuppb.BackupClient, error) {
	lock.Lock()
	defer lock.Unlock()
	connectedStore[storeID] += 1
	// we don't need connect real tikv in unit test
	// and we have already mock the backup response in `SendAsync`
	// so just return nil here
	return nil, nil
}

type mockBackupBackupSender struct {
	backupResponses map[uint64][]*backup.ResponseAndStore
}

func (m *mockBackupBackupSender) SendAsync(
	ctx context.Context,
	round uint64,
	storeID uint64,
	request backuppb.BackupRequest,
	concurrency uint,
	cli backuppb.BackupClient,
	respCh chan *backup.ResponseAndStore,
	StateNotifier chan backup.BackupRetryPolicy,
) {
	go func() {
		defer func() {
			close(respCh)
		}()
		lock.Lock()
		resps := m.backupResponses[storeID]
		lock.Unlock()
		for len(resps) == 0 {
			// store has no response
			// block for a while and try again.
			// let this goroutine never return.
			time.Sleep(100 * time.Millisecond)
			lock.Lock()
			resps = m.backupResponses[storeID]
			lock.Unlock()
		}
		for _, r := range resps {
			select {
			case <-ctx.Done():
				return
			case respCh <- r:
			}
		}
	}()
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

func TestGetHistoryDDLJobs(t *testing.T) {
	s := createBackupSuite(t)

	tk := testkit.NewTestKit(t, s.cluster.Storage)
	lastTS1, err := s.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("CREATE DATABASE IF NOT EXISTS test_db;")
	tk.MustExec("CREATE TABLE IF NOT EXISTS test_db.test_table (c1 INT);")
	lastTS2, err := s.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("RENAME TABLE test_db.test_table to test_db.test_table1;")
	tk.MustExec("DROP TABLE test_db.test_table1;")
	tk.MustExec("DROP DATABASE test_db;")
	tk.MustExec("CREATE DATABASE test_db;")
	tk.MustExec("USE test_db;")
	tk.MustExec("CREATE TABLE test_table1 (c2 CHAR(255));")
	tk.MustExec("RENAME TABLE test_table1 to test_table;")
	tk.MustExec("RENAME TABLE test_table to test_table2;")
	tk.MustExec("RENAME TABLE test_table2 to test_table;")
	lastTS3, err := s.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)
	tk.MustExec("TRUNCATE TABLE test_table;")
	ts, err := s.cluster.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoErrorf(t, err, "Error get last ts: %s", err)

	checkFn := func(lastTS uint64, ts uint64, jobsCount int) {
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
		require.Len(t, allDDLJobs, jobsCount)
	}

	checkFn(lastTS1, ts, 11)
	checkFn(lastTS2, ts, 9)
	checkFn(lastTS1, lastTS2, 2)
	checkFn(lastTS3, ts, 1)
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
	lock, err := s.backupClient.OnBackupResponse(ctx, nil, errContext, nil)
	require.NoError(t, err)
	require.Nil(t, lock)

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
	lock, err = s.backupClient.OnBackupResponse(ctx, r, errContext, &tree)
	require.NoError(t, err)
	require.Nil(t, lock)
	// second error cannot be ignored.
	_, err = s.backupClient.OnBackupResponse(ctx, r, errContext, &tree)
	require.Error(t, err)

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
	_, err = s.backupClient.OnBackupResponse(ctx, r, errContext, &tree)
	require.Error(t, err)

	// case #3: partial range success case, find incomplete range
	r.Resp.StartKey = []byte("aa")
	lock, err = s.backupClient.OnBackupResponse(ctx, r, errContext, &tree)
	require.NoError(t, err)
	require.Nil(t, lock)

	incomplete := tree.GetIncompleteRanges()
	require.Len(t, incomplete, 1)
	require.Equal(t, []byte("b"), incomplete[0].StartKey)
	require.Equal(t, []byte("c"), incomplete[0].EndKey)

	// case #4: success case, make up incomplete range
	r.Resp.StartKey = []byte("b")
	r.Resp.EndKey = []byte("c")
	lock, err = s.backupClient.OnBackupResponse(ctx, r, errContext, &tree)
	require.NoError(t, err)
	require.Nil(t, lock)
	incomplete = tree.GetIncompleteRanges()
	require.Len(t, incomplete, 0)

	// case #5: failed case, key is locked
	r = &backup.ResponseAndStore{
		StoreID: 0,
		Resp: &backuppb.BackupResponse{
			Error: &backuppb.Error{
				Detail: &backuppb.Error_KvError{
					KvError: &kvrpcpb.KeyError{
						Locked: &kvrpcpb.LockInfo{
							PrimaryLock: []byte("b"),
							LockVersion: 0,
							Key:         []byte("b"),
							LockTtl:     50,
							TxnSize:     1,
							LockType:    kvrpcpb.Op_Put,
						},
					},
				},
			},
		},
	}
	lock, err = s.backupClient.OnBackupResponse(ctx, r, errContext, &tree)
	require.NoError(t, err)
	require.Equal(t, []byte("b"), lock.Primary)
}

func TestMainBackupLoop(t *testing.T) {
	s := createBackupSuite(t)
	backgroundCtx := context.Background()

	// test 2 stores backup
	s.mockCluster.AddStore(1, "127.0.0.1:20160")
	s.mockCluster.AddStore(2, "127.0.0.1:20161")

	stores, err := s.mockPDClient.GetAllStores(backgroundCtx)
	require.NoError(t, err)
	require.Len(t, stores, 2)

	// random generate bytes in range [a, b)
	genRandBytesFn := func(a, b []byte) ([]byte, error) {
		n := len(a)
		result := make([]byte, n)
		for {
			_, err := rand.Read(result)
			if err != nil {
				return nil, err
			}
			if bytes.Compare(result, a) > 0 && bytes.Compare(result, b) < 0 {
				return result, nil
			}
		}
	}
	// split each range into limit parts
	splitRangesFn := func(ranges []rtree.Range, limit int) [][]byte {
		if len(ranges) == 0 {
			return nil
		}
		res := make([][]byte, 0)
		res = append(res, ranges[0].StartKey)
		for i := 0; i < len(ranges); i++ {
			partRes := make([][]byte, 0)
			for j := 0; j < limit; j++ {
				x, err := genRandBytesFn(ranges[i].StartKey, ranges[i].EndKey)
				require.NoError(t, err)
				partRes = append(partRes, x)
			}
			sort.Slice(partRes, func(i, j int) bool {
				return bytes.Compare(partRes[i], partRes[j]) < 0
			})
			res = append(res, partRes...)
		}
		res = append(res, ranges[len(ranges)-1].EndKey)
		return res
	}

	// Case #1: normal case
	ranges := []rtree.Range{
		{
			StartKey: []byte("aaa"),
			EndKey:   []byte("zzz"),
		},
	}
	tree, err := s.backupClient.BuildProgressRangeTree(ranges)
	require.NoError(t, err)

	mockBackupResponses := make(map[uint64][]*backup.ResponseAndStore)
	splitKeys := splitRangesFn(ranges, 10)
	for i := 0; i < len(splitKeys)-1; i++ {
		randStoreID := uint64(rand.Int()%len(stores) + 1)
		mockBackupResponses[randStoreID] = append(mockBackupResponses[randStoreID], &backup.ResponseAndStore{
			StoreID: randStoreID,
			Resp: &backuppb.BackupResponse{
				StartKey: splitKeys[i],
				EndKey:   splitKeys[i+1],
			},
		})
	}
	ch := make(chan backup.BackupRetryPolicy)
	mainLoop := &backup.MainBackupLoop{
		BackupSender: &mockBackupBackupSender{
			backupResponses: mockBackupResponses,
		},

		BackupReq:               backuppb.BackupRequest{},
		GlobalProgressTree:      &tree,
		ReplicaReadLabel:        nil,
		StateNotifier:           ch,
		ProgressCallBack:        func(backup.ProgressUnit) {},
		GetBackupClientCallBack: mockGetBackupClientCallBack,
	}

	// an offline store still can be backed up.
	s.mockCluster.StopStore(1)
	connectedStore = make(map[uint64]int)
	require.NoError(t, s.backupClient.RunLoop(backgroundCtx, mainLoop))

	// Case #2: canceled case
	ranges = []rtree.Range{
		{
			StartKey: []byte("aaa"),
			EndKey:   []byte("zzz"),
		},
	}
	tree, err = s.backupClient.BuildProgressRangeTree(ranges)
	require.NoError(t, err)

	clear(mockBackupResponses)
	splitKeys = splitRangesFn(ranges, 10)
	// range is not complete
	for i := 0; i < len(splitKeys)-2; i++ {
		randStoreID := uint64(rand.Int()%len(stores) + 1)
		mockBackupResponses[randStoreID] = append(mockBackupResponses[randStoreID], &backup.ResponseAndStore{
			StoreID: randStoreID,
			Resp: &backuppb.BackupResponse{
				StartKey: splitKeys[i],
				EndKey:   splitKeys[i+1],
			},
		})
	}
	mainLoop = &backup.MainBackupLoop{
		BackupSender: &mockBackupBackupSender{
			backupResponses: mockBackupResponses,
		},

		BackupReq:               backuppb.BackupRequest{},
		GlobalProgressTree:      &tree,
		ReplicaReadLabel:        nil,
		StateNotifier:           ch,
		ProgressCallBack:        func(backup.ProgressUnit) {},
		GetBackupClientCallBack: mockGetBackupClientCallBack,
	}

	// cancel the backup in another goroutine
	ctx, cancel := context.WithCancel(backgroundCtx)
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	connectedStore = make(map[uint64]int)
	require.Error(t, s.backupClient.RunLoop(ctx, mainLoop))

	// Case #3: one store drops and never come back
	ranges = []rtree.Range{
		{
			StartKey: []byte("aaa"),
			EndKey:   []byte("zzz"),
		},
	}
	tree, err = s.backupClient.BuildProgressRangeTree(ranges)
	require.NoError(t, err)

	clear(mockBackupResponses)
	splitKeys = splitRangesFn(ranges, 10)
	for i := 0; i < len(splitKeys)-1; i++ {
		randStoreID := uint64(rand.Int()%len(stores) + 1)
		mockBackupResponses[randStoreID] = append(mockBackupResponses[randStoreID], &backup.ResponseAndStore{
			StoreID: randStoreID,
			Resp: &backuppb.BackupResponse{
				StartKey: splitKeys[i],
				EndKey:   splitKeys[i+1],
			},
		})
	}
	remainStoreID := uint64(1)
	dropStoreID := uint64(2)
	s.mockCluster.MarkTombstone(dropStoreID)
	dropBackupResponses := mockBackupResponses[dropStoreID]
	lock.Lock()
	mockBackupResponses[dropStoreID] = nil
	lock.Unlock()

	mainLoop = &backup.MainBackupLoop{
		BackupSender: &mockBackupBackupSender{
			backupResponses: mockBackupResponses,
		},

		BackupReq:               backuppb.BackupRequest{},
		GlobalProgressTree:      &tree,
		ReplicaReadLabel:        nil,
		StateNotifier:           ch,
		ProgressCallBack:        func(backup.ProgressUnit) {},
		GetBackupClientCallBack: mockGetBackupClientCallBack,
	}
	go func() {
		// mock region leader balance behaviour.
		time.Sleep(500 * time.Millisecond)
		lock.Lock()
		// store 1 has the range after some while.
		mockBackupResponses[remainStoreID] = append(mockBackupResponses[remainStoreID], dropBackupResponses...)
		lock.Unlock()
	}()

	connectedStore = make(map[uint64]int)
	// backup can finished until store 1 has response the full ranges.
	require.NoError(t, s.backupClient.RunLoop(backgroundCtx, mainLoop))
	// connect to store 1 more than 1 time
	require.Less(t, 1, connectedStore[remainStoreID])
	// never connect to a dropped store.
	require.Equal(t, 0, connectedStore[dropStoreID])

	// Case #4 one store drops and come back soon
	ranges = []rtree.Range{
		{
			StartKey: []byte("aaa"),
			EndKey:   []byte("zzz"),
		},
	}
	tree, err = s.backupClient.BuildProgressRangeTree(ranges)
	require.NoError(t, err)

	clear(mockBackupResponses)
	splitKeys = splitRangesFn(ranges, 10)
	for i := 0; i < len(splitKeys)-1; i++ {
		randStoreID := uint64(rand.Int()%len(stores) + 1)
		mockBackupResponses[randStoreID] = append(mockBackupResponses[randStoreID], &backup.ResponseAndStore{
			StoreID: randStoreID,
			Resp: &backuppb.BackupResponse{
				StartKey: splitKeys[i],
				EndKey:   splitKeys[i+1],
			},
		})
	}
	remainStoreID = uint64(1)
	dropStoreID = uint64(2)
	s.mockCluster.MarkTombstone(dropStoreID)

	mainLoop = &backup.MainBackupLoop{
		BackupSender: &mockBackupBackupSender{
			backupResponses: mockBackupResponses,
		},

		BackupReq:               backuppb.BackupRequest{},
		GlobalProgressTree:      &tree,
		ReplicaReadLabel:        nil,
		StateNotifier:           ch,
		ProgressCallBack:        func(backup.ProgressUnit) {},
		GetBackupClientCallBack: mockGetBackupClientCallBack,
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		lock.Lock()
		s.mockCluster.StartStore(dropStoreID)
		lock.Unlock()
	}()

	connectedStore = make(map[uint64]int)
	// backup can finished until store 1 has response the full ranges.
	require.NoError(t, s.backupClient.RunLoop(backgroundCtx, mainLoop))
	// connect to store 1 more than 1 time
	require.Less(t, 1, connectedStore[remainStoreID])
	// connect to store 2 once should finished the backup.
	require.Equal(t, 1, connectedStore[dropStoreID])

	// Case #5 one store drops and watch store back
	ranges = []rtree.Range{
		{
			StartKey: []byte("aaa"),
			EndKey:   []byte("zzz"),
		},
	}
	tree, err = s.backupClient.BuildProgressRangeTree(ranges)
	require.NoError(t, err)

	clear(mockBackupResponses)
	splitKeys = splitRangesFn(ranges, 10)
	for i := 0; i < len(splitKeys)-1; i++ {
		randStoreID := uint64(rand.Int()%len(stores) + 1)
		mockBackupResponses[randStoreID] = append(mockBackupResponses[randStoreID], &backup.ResponseAndStore{
			StoreID: randStoreID,
			Resp: &backuppb.BackupResponse{
				StartKey: splitKeys[i],
				EndKey:   splitKeys[i+1],
			},
		})
	}
	remainStoreID = uint64(1)
	dropStoreID = uint64(2)
	// make store 2 never no response.
	dropBackupResponses = mockBackupResponses[dropStoreID]
	lock.Lock()
	mockBackupResponses[dropStoreID] = nil
	lock.Unlock()

	mainLoop = &backup.MainBackupLoop{
		BackupSender: &mockBackupBackupSender{
			backupResponses: mockBackupResponses,
		},

		BackupReq:               backuppb.BackupRequest{},
		GlobalProgressTree:      &tree,
		ReplicaReadLabel:        nil,
		StateNotifier:           ch,
		ProgressCallBack:        func(backup.ProgressUnit) {},
		GetBackupClientCallBack: mockGetBackupClientCallBack,
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		lock.Lock()
		mockBackupResponses[dropStoreID] = dropBackupResponses
		lock.Unlock()
	}()

	connectedStore = make(map[uint64]int)
	// backup can finished until store 1 has response the full ranges.
	require.NoError(t, s.backupClient.RunLoop(backgroundCtx, mainLoop))
	// connect to store 1 time, and then handle loop blocked until watch store 2 back.
	require.Equal(t, 1, connectedStore[remainStoreID])
	// connect to store 2 once should finished the backup.
	require.Equal(t, 1, connectedStore[dropStoreID])
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

func TestObserveStoreChangesAsync(t *testing.T) {
	s := createBackupSuite(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// test 2 stores backup
	s.mockCluster.AddStore(1, "127.0.0.1:20160")
	s.mockCluster.AddStore(2, "127.0.0.1:20161")

	stores, err := s.mockPDClient.GetAllStores(ctx)
	require.NoError(t, err)
	require.Len(t, stores, 2)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/backup/backup-store-change-tick", `return(true)`))

	// case #1: nothing happened
	ch := make(chan backup.BackupRetryPolicy, 1)
	backup.ObserveStoreChangesAsync(ctx, ch, s.mockPDClient)
	// the channel never receive any message
	require.Never(t, func() bool { return len(ch) > 0 }, time.Second, 100*time.Millisecond)

	// case #2: new store joined
	s.mockCluster.AddStore(3, "127.0.0.1:20162")
	// the channel received the new store
	require.Eventually(t, func() bool {
		if len(ch) == 0 {
			return false
		}
		res := <-ch
		return res.One == 3
	}, time.Second, 100*time.Millisecond)

	// case #3: one store disconnected
	s.mockCluster.StopStore(2)
	// the channel received the store disconnected
	require.Eventually(t, func() bool {
		if len(ch) == 0 {
			return false
		}
		res := <-ch
		return res.All
	}, time.Second, 100*time.Millisecond)

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/backup/backup-store-change-tick"))
}

func TestSplitBackupReqRanges(t *testing.T) {
	req := backuppb.BackupRequest{
		SubRanges: []*kvrpcpb.KeyRange{},
	}

	// case #1 empty ranges
	res := backup.SplitBackupReqRanges(req, 1)
	require.Len(t, res, 1)
	// case #2 empty ranges and limit is 0
	res = backup.SplitBackupReqRanges(req, 0)
	require.Len(t, res, 1)

	genSubRanges := func(req *backuppb.BackupRequest, count int) {
		for i := 0; i < count; i++ {
			req.SubRanges = append(req.SubRanges, &kvrpcpb.KeyRange{
				StartKey: []byte{byte(i)},
				EndKey:   []byte{byte(i + 1)},
			})
		}
	}

	genSubRanges(&req, 10)
	// case #3: 10 subranges and split into 10 parts
	res = backup.SplitBackupReqRanges(req, 10)
	require.Len(t, res, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, res[i].SubRanges[0].StartKey, req.SubRanges[i].StartKey)
		require.Equal(t, res[i].SubRanges[0].EndKey, req.SubRanges[i].EndKey)
	}

	// case #3.1: 10 subranges and split into 11 parts(has no difference with 10 parts)
	res = backup.SplitBackupReqRanges(req, 11)
	require.Len(t, res, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, res[i].SubRanges[0].StartKey, req.SubRanges[i].StartKey)
		require.Equal(t, res[i].SubRanges[0].EndKey, req.SubRanges[i].EndKey)
	}

	// case #3.2: 10 subranges and split into 9 parts(has no difference with 10 parts)
	res = backup.SplitBackupReqRanges(req, 9)
	require.Len(t, res, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, res[i].SubRanges[0].StartKey, req.SubRanges[i].StartKey)
		require.Equal(t, res[i].SubRanges[0].EndKey, req.SubRanges[i].EndKey)
	}

	// case #4: 10 subranges and split into 3 parts, each part has 3 subranges
	// but actually it will generate 4 parts due to not divisible.
	res = backup.SplitBackupReqRanges(req, 3)
	require.Len(t, res, 4)
	for i := 0; i < 3; i++ {
		require.Len(t, res[i].SubRanges, 3)
		for j := 0; j < 3; j++ {
			require.Equal(t, res[i].SubRanges[j].StartKey, req.SubRanges[i*3+j].StartKey)
			require.Equal(t, res[i].SubRanges[j].EndKey, req.SubRanges[i*3+j].EndKey)
		}
	}
	require.Len(t, res[3].SubRanges, 1)
	require.Equal(t, res[3].SubRanges[0].StartKey, req.SubRanges[9].StartKey)
	require.Equal(t, res[3].SubRanges[0].EndKey, req.SubRanges[9].EndKey)
}
