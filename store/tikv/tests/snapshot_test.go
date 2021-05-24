// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"go.uber.org/zap"
)

type testSnapshotSuite struct {
	OneByOneSuite
	store   tikv.StoreProbe
	prefix  string
	rowNums []int
}

var _ = Suite(&testSnapshotSuite{})

func (s *testSnapshotSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = tikv.StoreProbe{KVStore: NewTestStore(c)}
	s.prefix = fmt.Sprintf("snapshot_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, 100, 191)
}

func (s *testSnapshotSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSnapshotSuite) beginTxn(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn
}

func (s *testSnapshotSuite) checkAll(keys [][]byte, c *C) {
	txn := s.beginTxn(c)
	snapshot := txn.GetSnapshot()
	m, err := snapshot.BatchGet(context.Background(), keys)
	c.Assert(err, IsNil)

	scan, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(err, IsNil)
	cnt := 0
	for scan.Valid() {
		cnt++
		k := scan.Key()
		v := scan.Value()
		v2, ok := m[string(k)]
		c.Assert(ok, IsTrue, Commentf("key: %q", k))
		c.Assert(v, BytesEquals, v2)
		scan.Next()
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	c.Assert(m, HasLen, cnt)
}

func (s *testSnapshotSuite) deleteKeys(keys [][]byte, c *C) {
	txn := s.beginTxn(c)
	for _, k := range keys {
		err := txn.Delete(k)
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testSnapshotSuite) TestBatchGet(c *C) {
	for _, rowNum := range s.rowNums {
		logutil.BgLogger().Debug("test BatchGet",
			zap.Int("length", rowNum))
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(context.Background())
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

type contextKey string

func (s *testSnapshotSuite) TestSnapshotCache(c *C) {
	txn := s.beginTxn(c)
	c.Assert(txn.Set([]byte("x"), []byte("x")), IsNil)
	c.Assert(txn.Delete([]byte("y")), IsNil) // store data is affected by other tests.
	c.Assert(txn.Commit(context.Background()), IsNil)

	txn = s.beginTxn(c)
	snapshot := txn.GetSnapshot()
	_, err := snapshot.BatchGet(context.Background(), [][]byte{[]byte("x"), []byte("y")})
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/snapshot-get-cache-fail", `return(true)`), IsNil)
	ctx := context.WithValue(context.Background(), contextKey("TestSnapshotCache"), true)
	_, err = snapshot.Get(ctx, []byte("x"))
	c.Assert(err, IsNil)

	_, err = snapshot.Get(ctx, []byte("y"))
	c.Assert(tikverr.IsErrNotFound(err), IsTrue)

	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/snapshot-get-cache-fail"), IsNil)
}

func (s *testSnapshotSuite) TestBatchGetNotExist(c *C) {
	for _, rowNum := range s.rowNums {
		logutil.BgLogger().Debug("test BatchGetNotExist",
			zap.Int("length", rowNum))
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(context.Background())
		c.Assert(err, IsNil)

		keys := makeKeys(rowNum, s.prefix)
		keys = append(keys, []byte("noSuchKey"))
		s.checkAll(keys, c)
		s.deleteKeys(keys, c)
	}
}

func makeKeys(rowNum int, prefix string) [][]byte {
	keys := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSnapshotSuite) TestSkipLargeTxnLock(c *C) {
	x := []byte("x_key_TestSkipLargeTxnLock")
	y := []byte("y_key_TestSkipLargeTxnLock")
	txn := s.beginTxn(c)
	c.Assert(txn.Set(x, []byte("x")), IsNil)
	c.Assert(txn.Set(y, []byte("y")), IsNil)
	ctx := context.Background()
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.SetLockTTL(3000)
	c.Assert(committer.PrewriteAllMutations(ctx), IsNil)

	txn1 := s.beginTxn(c)
	// txn1 is not blocked by txn in the large txn protocol.
	_, err = txn1.Get(ctx, x)
	c.Assert(tikverr.IsErrNotFound(errors.Trace(err)), IsTrue)

	res, err := toTiDBTxn(&txn1).BatchGet(ctx, toTiDBKeys([][]byte{x, y, []byte("z")}))
	c.Assert(err, IsNil)
	c.Assert(res, HasLen, 0)

	// Commit txn, check the final commit ts is pushed.
	committer.SetCommitTS(txn.StartTS() + 1)
	c.Assert(committer.CommitMutations(ctx), IsNil)
	status, err := s.store.GetLockResolver().GetTxnStatus(txn.StartTS(), 0, x)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Greater, txn1.StartTS())
}

func (s *testSnapshotSuite) TestPointGetSkipTxnLock(c *C) {
	x := []byte("x_key_TestPointGetSkipTxnLock")
	y := []byte("y_key_TestPointGetSkipTxnLock")
	txn := s.beginTxn(c)
	c.Assert(txn.Set(x, []byte("x")), IsNil)
	c.Assert(txn.Set(y, []byte("y")), IsNil)
	ctx := context.Background()
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.SetLockTTL(3000)
	c.Assert(committer.PrewriteAllMutations(ctx), IsNil)

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	start := time.Now()
	c.Assert(committer.GetPrimaryKey(), BytesEquals, x)
	// Point get secondary key. Shouldn't be blocked by the lock and read old data.
	_, err = snapshot.Get(ctx, y)
	c.Assert(tikverr.IsErrNotFound(errors.Trace(err)), IsTrue)
	c.Assert(time.Since(start), Less, 500*time.Millisecond)

	// Commit the primary key
	committer.SetCommitTS(txn.StartTS() + 1)
	committer.CommitMutations(ctx)

	snapshot = s.store.GetSnapshot(math.MaxUint64)
	start = time.Now()
	// Point get secondary key. Should read committed data.
	value, err := snapshot.Get(ctx, y)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, []byte("y"))
	c.Assert(time.Since(start), Less, 500*time.Millisecond)
}

func (s *testSnapshotSuite) TestSnapshotThreadSafe(c *C) {
	txn := s.beginTxn(c)
	key := []byte("key_test_snapshot_threadsafe")
	c.Assert(txn.Set(key, []byte("x")), IsNil)
	ctx := context.Background()
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			for i := 0; i < 30; i++ {
				_, err := snapshot.Get(ctx, key)
				c.Assert(err, IsNil)
				_, err = snapshot.BatchGet(ctx, [][]byte{key, []byte("key_not_exist")})
				c.Assert(err, IsNil)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *testSnapshotSuite) TestSnapshotRuntimeStats(c *C) {
	reqStats := tikv.NewRegionRequestRuntimeStats()
	tikv.RecordRegionRequestRuntimeStats(reqStats.Stats, tikvrpc.CmdGet, time.Second)
	tikv.RecordRegionRequestRuntimeStats(reqStats.Stats, tikvrpc.CmdGet, time.Millisecond)
	snapshot := s.store.GetSnapshot(0)
	snapshot.SetRuntimeStats(&tikv.SnapshotRuntimeStats{})
	snapshot.MergeRegionRequestStats(reqStats.Stats)
	snapshot.MergeRegionRequestStats(reqStats.Stats)
	bo := tikv.NewBackofferWithVars(context.Background(), 2000, nil)
	err := bo.BackoffWithMaxSleepTxnLockFast(30, errors.New("test"))
	c.Assert(err, IsNil)
	snapshot.RecordBackoffInfo(bo)
	snapshot.RecordBackoffInfo(bo)
	expect := "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:60ms}"
	c.Assert(snapshot.FormatStats(), Equals, expect)
	detail := &pb.ExecDetailsV2{
		TimeDetail: &pb.TimeDetail{
			WaitWallTimeMs:    100,
			ProcessWallTimeMs: 100,
		},
		ScanDetailV2: &pb.ScanDetailV2{
			ProcessedVersions:         10,
			TotalVersions:             15,
			RocksdbBlockReadCount:     20,
			RocksdbBlockReadByte:      15,
			RocksdbDeleteSkippedCount: 5,
			RocksdbKeySkippedCount:    1,
			RocksdbBlockCacheHitCount: 10,
		},
	}
	snapshot.MergeExecDetail(detail)
	expect = "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:60ms}, " +
		"total_process_time: 100ms, total_wait_time: 100ms, " +
		"scan_detail: {total_process_keys: 10, " +
		"total_keys: 15, " +
		"rocksdb: {delete_skipped_count: 5, " +
		"key_skipped_count: 1, " +
		"block: {cache_hit_count: 10, read_count: 20, read_byte: 15 Bytes}}}"
	c.Assert(snapshot.FormatStats(), Equals, expect)
	snapshot.MergeExecDetail(detail)
	expect = "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:60ms}, " +
		"total_process_time: 200ms, total_wait_time: 200ms, " +
		"scan_detail: {total_process_keys: 20, " +
		"total_keys: 30, " +
		"rocksdb: {delete_skipped_count: 10, " +
		"key_skipped_count: 2, " +
		"block: {cache_hit_count: 20, read_count: 40, read_byte: 30 Bytes}}}"
	c.Assert(snapshot.FormatStats(), Equals, expect)
}
