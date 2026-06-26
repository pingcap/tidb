// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/redact"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBasic(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		if t.Failed() {
			fmt.Println(c)
		}
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx := context.Background()
	minCheckpoint := c.advanceCheckpoints()
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	coll := streamhelper.NewClusterCollector(ctx, env)
	err := adv.GetCheckpointInRange(ctx, []byte{}, []byte{}, coll)
	require.NoError(t, err)
	r, err := coll.Finish(ctx)
	require.NoError(t, err)
	require.Len(t, r.FailureSubRanges, 0)
	require.Equal(t, r.Checkpoint, minCheckpoint, "%d %d", r.Checkpoint, minCheckpoint)
}

func TestTick(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))
	for range 5 {
		cp := c.advanceCheckpoints()
		require.NoError(t, adv.OnTick(ctx))
		require.Equal(t, env.getCheckpoint(), cp)
	}
}

type countingStorage struct {
	storeapi.Storage
	closeCount *atomic.Int32
}

func (s *countingStorage) Close() {
	s.closeCount.Inc()
	s.Storage.Close()
}

type writeFailStorage struct {
	storeapi.Storage
	onWrite func()
}

func (s writeFailStorage) WriteFile(context.Context, string, []byte) error {
	if s.onWrite != nil {
		s.onWrite()
	}
	return errors.New("injected external storage error")
}

type writeBlockStorage struct {
	storeapi.Storage
	onWriteDone func(error)
}

func (s writeBlockStorage) WriteFile(ctx context.Context, _ string, _ []byte) error {
	<-ctx.Done()
	err := ctx.Err()
	if s.onWriteDone != nil {
		s.onWriteDone(err)
	}
	return err
}

type writeRecordStorage struct {
	storeapi.Storage
	writeCount *atomic.Int32
	onWrite    func()
}

func (s writeRecordStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if s.onWrite != nil {
		s.onWrite()
	}
	s.writeCount.Inc()
	return s.Storage.WriteFile(ctx, name, data)
}

type blockGCRecordEnv struct {
	*testEnv
	blockAttempted atomic.Bool
}

func (e *blockGCRecordEnv) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	e.blockAttempted.Store(true)
	return e.testEnv.BlockGCUntil(ctx, at)
}

func TestTickWritesGlobalCheckpointToStorage(t *testing.T) {
	metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")
	defer metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")

	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageDir := t.TempDir()
	createCount := atomic.NewInt32(0)
	closeCount := atomic.NewInt32(0)
	writeCount := atomic.NewInt32(0)
	restoreFactory := streamhelper.SetGlobalCheckpointStorageFactoryForTest(
		func(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (storeapi.Storage, error) {
			createCount.Inc()
			storage, err := objstore.Create(ctx, backend, sendCreds)
			if err != nil {
				return nil, err
			}
			return writeRecordStorage{
				Storage:    &countingStorage{Storage: storage, closeCount: closeCount},
				writeCount: writeCount,
			}, nil
		})
	defer restoreFactory()

	env := newTestEnv(c, t)
	env.task.Info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: storageDir},
		},
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, int32(0), createCount.Load())
	require.Equal(t, int32(0), writeCount.Load())

	_ = c.advanceCheckpoints()
	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, int32(1), createCount.Load())

	checkpoint := c.advanceCheckpoints()
	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, int32(1), createCount.Load())

	writesAfterCheckpoint := writeCount.Load()
	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, writesAfterCheckpoint, writeCount.Load())
	require.Equal(t, int32(1), createCount.Load())

	data, err := os.ReadFile(filepath.Join(storageDir, "v1", "global_checkpoint", "central_global.ts"))
	require.NoError(t, err)
	require.Len(t, data, 8)
	require.Equal(t, checkpoint, binary.LittleEndian.Uint64(data))
	require.Equal(t, float64(checkpoint), promtest.ToFloat64(metrics.ExternalStorageCheckpoint.WithLabelValues("whole")))

	adv.OnStop()
	require.Equal(t, int32(1), closeCount.Load())
}

func TestTickIgnoresGlobalCheckpointStorageFailure(t *testing.T) {
	metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")
	defer metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")

	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageDir := t.TempDir()
	gcBlockedBeforeWrite := atomic.NewBool(false)
	var env *testEnv
	restoreFactory := streamhelper.SetGlobalCheckpointStorageFactoryForTest(
		func(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (storeapi.Storage, error) {
			storage, err := objstore.Create(ctx, backend, sendCreds)
			if err != nil {
				return nil, err
			}
			return writeFailStorage{
				Storage: storage,
				onWrite: func() {
					gcBlockedBeforeWrite.Store(env.ServiceGCSafePointSet)
				},
			}, nil
		})
	defer restoreFactory()

	env = newTestEnv(c, t)
	env.task.Info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: storageDir},
		},
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)

	checkpoint := c.advanceCheckpoints()
	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, checkpoint, env.getCheckpoint())
	require.True(t, env.ServiceGCSafePointSet)
	require.Equal(t, checkpoint-1, env.ServiceGCSafePoint)
	require.True(t, gcBlockedBeforeWrite.Load())
	require.Equal(t, 0.0, promtest.ToFloat64(metrics.ExternalStorageCheckpoint.WithLabelValues("whole")))
}

func TestTickWritesGlobalCheckpointToStorageAfterBlockGCAttemptFailed(t *testing.T) {
	metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")
	defer metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")

	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageDir := t.TempDir()
	writeCount := atomic.NewInt32(0)
	blockAttemptedBeforeWrite := atomic.NewBool(false)
	var env *blockGCRecordEnv
	restoreFactory := streamhelper.SetGlobalCheckpointStorageFactoryForTest(
		func(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (storeapi.Storage, error) {
			storage, err := objstore.Create(ctx, backend, sendCreds)
			if err != nil {
				return nil, err
			}
			return writeRecordStorage{
				Storage:    storage,
				writeCount: writeCount,
				onWrite: func() {
					blockAttemptedBeforeWrite.Store(env.blockAttempted.Load())
				},
			}, nil
		})
	defer restoreFactory()

	env = &blockGCRecordEnv{testEnv: newTestEnv(c, t)}
	env.task.Info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: storageDir},
		},
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)

	checkpoint := c.advanceCheckpoints()
	env.ServiceGCSafePoint = checkpoint
	require.ErrorContains(t, adv.OnTick(ctx), "failed to update service GC safe point")
	require.Equal(t, int32(1), writeCount.Load())
	require.True(t, blockAttemptedBeforeWrite.Load())
	require.Equal(t, float64(checkpoint), promtest.ToFloat64(metrics.ExternalStorageCheckpoint.WithLabelValues("whole")))

	data, err := os.ReadFile(filepath.Join(storageDir, "v1", "global_checkpoint", "central_global.ts"))
	require.NoError(t, err)
	require.Len(t, data, 8)
	require.Equal(t, checkpoint, binary.LittleEndian.Uint64(data))
}

func TestTickTimesOutGlobalCheckpointStorageWrite(t *testing.T) {
	metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")
	defer metrics.ExternalStorageCheckpoint.DeleteLabelValues("whole")

	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageDir := t.TempDir()
	writeErrCh := make(chan error, 1)
	restoreFactory := streamhelper.SetGlobalCheckpointStorageFactoryForTest(
		func(ctx context.Context, backend *backup.StorageBackend, sendCreds bool) (storeapi.Storage, error) {
			storage, err := objstore.Create(ctx, backend, sendCreds)
			if err != nil {
				return nil, err
			}
			return writeBlockStorage{
				Storage: storage,
				onWriteDone: func(err error) {
					writeErrCh <- err
				},
			}, nil
		})
	defer restoreFactory()

	env := newTestEnv(c, t)
	env.task.Info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_Local{
			Local: &backup.Local{Path: storageDir},
		},
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(cfg *config.CommandConfig) {
		cfg.TickDuration = 200 * time.Millisecond
	})
	adv.StartTaskListener(ctx)

	checkpoint := c.advanceCheckpoints()
	start := time.Now()
	require.NoError(t, adv.OnTick(ctx))
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, checkpoint, env.getCheckpoint())
	require.True(t, env.ServiceGCSafePointSet)
	require.Equal(t, checkpoint-1, env.ServiceGCSafePoint)
	select {
	case err := <-writeErrCh:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for external storage write timeout")
	}
	require.Equal(t, 0.0, promtest.ToFloat64(metrics.ExternalStorageCheckpoint.WithLabelValues("whole")))
}

func TestWithFailure(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	c.flushAll()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))

	cp := c.advanceCheckpoints()
	for _, v := range c.storeList() {
		v.Flush()
		break
	}
	require.NoError(t, adv.OnTick(ctx))
	require.Less(t, env.getCheckpoint(), cp, "%d %d", env.getCheckpoint(), cp)

	for _, v := range c.storeList() {
		v.Flush()
	}

	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, env.getCheckpoint(), cp)
}

func shouldFinishInTime(t *testing.T, d time.Duration, name string, f func()) {
	ch := make(chan struct{})
	go func() {
		f()
		close(ch)
	}()
	select {
	case <-time.After(d):
		t.Fatalf("%s should finish in %s, but not", name, d)
	case <-ch:
	}
}

func TestCollectorFailure(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	c.SetOnGetClient(func(u uint64) error {
		return status.Error(codes.DataLoss,
			"Exiled requests from the client, please slow down and listen a story: "+
				"the server has been dropped, we are longing for new nodes, however the goddess(k8s) never allocates new resource. "+
				"May you take the sword named `vim`, refactoring the definition of the nature, in the yaml file hidden at somewhere of the cluster, "+
				"to save all of us and gain the response you desiring?")
	})
	ctx := context.Background()
	splitKeys := make([]string, 0, 10000)
	for i := range 10000 {
		splitKeys = append(splitKeys, fmt.Sprintf("%04d", i))
	}
	c.splitAndScatter(splitKeys...)

	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	coll := streamhelper.NewClusterCollector(ctx, env)

	shouldFinishInTime(t, 30*time.Second, "scan with always fail", func() {
		// At this time, the sending may or may not fail because the sending and batching is doing asynchronously.
		_ = adv.GetCheckpointInRange(ctx, []byte{}, []byte{}, coll)
		// ...but this must fail, not getting stuck.
		_, err := coll.Finish(ctx)
		require.Error(t, err)
	})
}

func oneStoreFailure() func(uint64) error {
	victim := uint64(0)
	mu := new(sync.Mutex)
	return func(u uint64) error {
		mu.Lock()
		defer mu.Unlock()
		if victim == 0 {
			victim = u
		}
		if victim == u {
			return status.Error(codes.NotFound,
				"The place once lit by the warm lamplight has been swallowed up by the debris now.")
		}
		return nil
	}
}

func TestOneStoreFailure(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	ctx := context.Background()
	splitKeys := make([]string, 0, 1000)
	for i := range 1000 {
		splitKeys = append(splitKeys, fmt.Sprintf("%04d", i))
	}
	c.splitAndScatter(splitKeys...)
	c.flushAll()

	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))
	c.SetOnGetClient(oneStoreFailure())

	for range 100 {
		c.advanceCheckpoints()
		c.flushAll()
		require.ErrorContains(t, adv.OnTick(ctx), "the warm lamplight")
	}

	c.SetOnGetClient(nil)
	cp := c.advanceCheckpoints()
	c.flushAll()
	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, cp, env.checkpoint)
}

func TestGCServiceSafePoint(t *testing.T) {
	req := require.New(t)
	c := createFakeCluster(t, 4, true)
	ctx := context.Background()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	env := newTestEnv(c, t)

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	cp := c.advanceCheckpoints()
	c.flushAll()

	req.NoError(adv.OnTick(ctx))
	req.Equal(env.ServiceGCSafePoint, cp-1)
	env.Cluster.Mu.Lock()
	req.True(env.ServiceGCSafePointSet)
	env.Cluster.Mu.Unlock()

	env.unregisterTask()
	req.Eventually(func() bool {
		env.Cluster.Mu.Lock()
		defer env.Cluster.Mu.Unlock()
		return env.ServiceGCSafePointDeleted
	}, 3*time.Second, 100*time.Millisecond)
}

func TestTaskRanges(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	defer fmt.Println(c)
	ctx := context.Background()
	c.splitAndScatter("0001", "0002", "0012", "0034", "0048")
	c.advanceCheckpoints()
	c.flushAllExcept("0000", "0049")
	env := newTestEnv(c, t)
	env.ranges = []kv.KeyRange{{StartKey: []byte("0002"), EndKey: []byte("0048")}}
	env.task.Ranges = env.ranges
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)

	shouldFinishInTime(t, 10*time.Second, "first advancing", func() { require.NoError(t, adv.OnTick(ctx)) })
	// Don't check the return value of advance checkpoints here -- we didn't
	require.Greater(t, env.getCheckpoint(), uint64(0))
}

func TestTaskRangesWithSplit(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	defer fmt.Println(c)
	ctx := context.Background()
	c.splitAndScatter("0012", "0034", "0048")
	c.advanceCheckpoints()
	c.flushAllExcept("0049")
	env := newTestEnv(c, t)
	env.ranges = []kv.KeyRange{{StartKey: []byte("0002"), EndKey: []byte("0048")}}
	env.task.Ranges = env.ranges
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)

	shouldFinishInTime(t, 10*time.Second, "first advancing", func() { require.NoError(t, adv.OnTick(ctx)) })
	fstCheckpoint := env.getCheckpoint()
	require.Greater(t, fstCheckpoint, uint64(0))

	c.splitAndScatter("0002")
	c.advanceCheckpoints()
	c.flushAllExcept("0000", "0049")
	shouldFinishInTime(t, 10*time.Second, "second advancing", func() { require.NoError(t, adv.OnTick(ctx)) })
	require.Greater(t, env.getCheckpoint(), fstCheckpoint)
}

func TestClearCache(t *testing.T) {
	c := createFakeCluster(t, 4, true)
	ctx := context.Background()
	req := require.New(t)
	c.splitAndScatter("0012", "0034", "0048")

	clearedCache := make(map[uint64]bool)
	c.SetOnClearCache(func(u uint64) error {
		// make store u cache cleared
		clearedCache[u] = true
		return nil
	})
	failedStoreID := uint64(0)
	hasFailed := atomic.NewBool(false)
	for _, s := range c.storeList() {
		sid := s.GetID()
		s.SetGetRegionCheckpointHook(func(glftrr *logbackup.GetLastFlushTSOfRegionRequest) error {
			// mark one store failed is enough
			if hasFailed.CompareAndSwap(false, true) {
				// mark this store cache cleared
				failedStoreID = sid
				return errors.New("failed to get checkpoint")
			}
			return nil
		})
	}
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	var err error
	shouldFinishInTime(t, time.Second, "ticking", func() {
		err = adv.OnTick(ctx)
	})
	req.Error(err)
	req.True(failedStoreID > 0, "failed to mark the cluster: ")
	req.Equal(clearedCache[failedStoreID], true)
}

func TestBlocked(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	ctx := context.Background()
	req := require.New(t)
	c.splitAndScatter("0012", "0034", "0048")
	blockReq := make(chan struct{})
	defer close(blockReq)
	firstBlocked := make(chan struct{}, 1)
	firstBlockedOnce := sync.Once{}
	marked := false
	for _, s := range c.storeList() {
		s.SetGetRegionCheckpointHook(func(glftrr *logbackup.GetLastFlushTSOfRegionRequest) error {
			firstBlockedOnce.Do(func() {
				firstBlocked <- struct{}{}
			})
			// blocking the thread.
			// this may happen when TiKV goes down or too busy.
			<-blockReq
			return nil
		})
		marked = true
	}
	req.True(marked, "failed to mark the cluster: ")
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	adv.UpdateConfigWith(func(c *config.CommandConfig) {
		// keep enough headroom so the blocked rpc request is observed before timeout.
		c.TickDuration = 100 * time.Millisecond
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- adv.OnTick(ctx)
	}()
	shouldFinishInTime(t, 5*time.Second, "wait until blocked request observed", func() {
		<-firstBlocked
	})
	var err error
	shouldFinishInTime(t, 5*time.Second, "ticking", func() {
		err = <-errCh
	})
	req.ErrorIs(err, context.DeadlineExceeded)
}

func TestResolveLock(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		if t.Failed() {
			fmt.Println(c)
		}
	}()

	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx := context.Background()
	minCheckpoint := oracle.GoTimeToTS(time.Now().Add(-10 * time.Second))
	c.SetCurrentTS(oracle.GoTimeToTS(oracle.GetTimeFromTS(minCheckpoint).Add(10 * time.Second)))
	env := newTestEnv(c, t)

	lockRegion := c.FindRegionByKey([]byte("01"))
	allLocks := []*txnlock.Lock{
		{
			Key: []byte("011"),
			// TxnID == minCheckpoint
			TxnID: minCheckpoint,
		},
		{
			Key: []byte("012"),
			// TxnID > minCheckpoint
			TxnID: minCheckpoint + 1,
		},
		{
			Key: []byte("013"),
			// this lock cannot be resolved due to scan version
			TxnID: oracle.GoTimeToTS(oracle.GetTimeFromTS(minCheckpoint).Add(2 * time.Minute)),
		},
	}
	c.LockRegion(lockRegion, allLocks)

	// ensure resolve locks triggered and collect all locks from scan locks
	resolveLockRef := atomic.NewBool(false)
	env.resolveLocks = func(locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
		resolveLockRef.Store(true)
		// Locks close to the current checkpoint should be scanned in one round.
		require.ElementsMatch(t, locks, allLocks[:2])
		return loc, nil
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.CommandConfig) {
		c.TickDuration = 50 * time.Millisecond
	})
	adv.StartTaskListener(ctx)

	outsideUpperBoundTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(minCheckpoint).Add(2 * time.Minute))
	adv.WithCheckpoints(func(s *spans.ValueSortedFull) {
		s.Merge(spans.Valued{Key: kv.KeyRange{}, Value: minCheckpoint})
		s.Merge(spans.Valued{Key: kv.KeyRange{EndKey: lockRegion.Range.StartKey}, Value: outsideUpperBoundTS})
		s.Merge(spans.Valued{Key: kv.KeyRange{StartKey: lockRegion.Range.EndKey}, Value: outsideUpperBoundTS})
	})

	adv.TESTSetLastCheckpointToCurrentMin()
	require.Equal(t, 1, adv.TESTResolveLockTargetCount())
	time.Sleep(adv.Config().GetResolveLockInterval() + 10*time.Millisecond)
	adv.TESTTryResolveLocksForCheckpoint()
	require.Eventually(t, func() bool { return resolveLockRef.Load() },
		8*time.Second, 50*time.Microsecond)
	// state must set to false after tick
	require.Eventually(t, func() bool { return !adv.GetInResolvingLock() },
		8*time.Second, 50*time.Microsecond)
}

func TestResolveLockRetryWithLowerMaxVersionOnScanLockLocked(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx := context.Background()
	checkpointTime := time.Now().Add(-30 * time.Second)
	minCheckpoint := oracle.GoTimeToTS(checkpointTime)
	c.SetCurrentTS(oracle.GoTimeToTS(checkpointTime.Add(30 * time.Second)))
	env := newTestEnv(c, t)

	lockRegion := c.FindRegionByKey([]byte("01"))
	allLocks := []*txnlock.Lock{
		{
			Key:   []byte("011"),
			TxnID: oracle.GoTimeToTS(checkpointTime.Add(time.Millisecond)),
		},
	}
	c.LockRegion(lockRegion, allLocks)

	scanLockCount := atomic.NewInt32(0)
	firstMaxVersion := atomic.NewUint64(0)
	secondMaxVersion := atomic.NewUint64(0)
	thirdMaxVersion := atomic.NewUint64(0)
	env.scanLocks = func(_ []byte, _ []byte, maxVersion uint64) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
		switch scanLockCount.Inc() {
		case 1:
			firstMaxVersion.Store(maxVersion)
			return nil, nil, errors.New("unexpected scanlock error: error:<locked:<primary_lock:\"011\" lock_version:1>>")
		case 2:
			secondMaxVersion.Store(maxVersion)
			return nil, nil, errors.New("unexpected scanlock error: error:<locked:<primary_lock:\"011\" lock_version:1>>")
		case 3:
			thirdMaxVersion.Store(maxVersion)
			return allLocks, &tikv.KeyLocation{Region: tikv.NewRegionVerID(lockRegion.ID, 0, 0)}, nil
		default:
			return nil, nil, errors.Errorf("unexpected scan lock retry count %d", scanLockCount.Load())
		}
	}

	resolveLockCount := atomic.NewInt32(0)
	env.resolveLocks = func(locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
		resolveLockCount.Inc()
		require.ElementsMatch(t, locks, allLocks)
		return loc, nil
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.CommandConfig) {
		c.TickDuration = 50 * time.Millisecond
	})
	adv.StartTaskListener(ctx)

	outsideUpperBoundTS := oracle.GoTimeToTS(checkpointTime.Add(2 * time.Minute))
	adv.WithCheckpoints(func(s *spans.ValueSortedFull) {
		s.Merge(spans.Valued{Key: kv.KeyRange{}, Value: minCheckpoint})
		s.Merge(spans.Valued{Key: kv.KeyRange{EndKey: lockRegion.Range.StartKey}, Value: outsideUpperBoundTS})
		s.Merge(spans.Valued{Key: kv.KeyRange{StartKey: lockRegion.Range.EndKey}, Value: outsideUpperBoundTS})
	})

	adv.TESTSetLastCheckpointToCurrentMin()
	require.Equal(t, 1, adv.TESTResolveLockTargetCount())
	time.Sleep(adv.Config().GetResolveLockInterval() + 10*time.Millisecond)
	adv.TESTTryResolveLocksForCheckpoint()
	require.Eventually(t, func() bool { return resolveLockCount.Load() >= 1 },
		8*time.Second, 50*time.Microsecond)
	require.Eventually(t, func() bool { return !adv.GetInResolvingLock() },
		8*time.Second, 50*time.Microsecond)
	require.Equal(t, int32(3), scanLockCount.Load())

	expectedUpperBound := streamhelper.TESTResolveLockTargetUpperBound(
		minCheckpoint, adv.Config().GetResolveLockInterval(), checkpointTime.Add(30*time.Second))
	require.Equal(t, expectedUpperBound, firstMaxVersion.Load())
	retryLowerBound, ok := streamhelper.TESTResolveLockRetryLowerBound(minCheckpoint, expectedUpperBound)
	require.True(t, ok)
	require.Equal(t, oracle.GoTimeToTS(checkpointTime.Add(10*time.Second)), retryLowerBound)
	expectedSecondMaxVersion, ok := streamhelper.TESTLowerResolveLockMaxVersion(expectedUpperBound, retryLowerBound)
	require.True(t, ok)
	require.Equal(t, expectedSecondMaxVersion, secondMaxVersion.Load())
	expectedThirdMaxVersion, ok := streamhelper.TESTLowerResolveLockMaxVersion(expectedSecondMaxVersion, retryLowerBound)
	require.True(t, ok)
	require.Equal(t, expectedThirdMaxVersion, thirdMaxVersion.Load())
}

func TestResolveLockMaxVersion(t *testing.T) {
	checkpointTime := time.Unix(100, 0)
	checkpointTS := oracle.GoTimeToTS(checkpointTime)
	resolveLockInterval := 30 * time.Second

	require.Equal(t,
		checkpointTS,
		streamhelper.TESTResolveLockTargetUpperBound(checkpointTS, resolveLockInterval, checkpointTime.Add(time.Minute)))
	require.Equal(t,
		oracle.GoTimeToTS(checkpointTime.Add(5*time.Second)),
		streamhelper.TESTResolveLockTargetUpperBound(checkpointTS, resolveLockInterval, checkpointTime.Add(65*time.Second)))
	require.Equal(t,
		oracle.GoTimeToTS(checkpointTime.Add(10*time.Second)),
		streamhelper.TESTResolveLockTargetUpperBound(checkpointTS, 0, checkpointTime.Add(time.Minute)))

	maxVersion := oracle.GoTimeToTS(checkpointTime.Add(30 * time.Second))
	retryLowerBound, ok := streamhelper.TESTResolveLockRetryLowerBound(checkpointTS, maxVersion)
	require.True(t, ok)
	require.Equal(t, oracle.GoTimeToTS(checkpointTime.Add(10*time.Second)), retryLowerBound)
	nextMaxVersion, ok := streamhelper.TESTLowerResolveLockMaxVersion(maxVersion, retryLowerBound)
	require.True(t, ok)
	require.Equal(t, retryLowerBound+(maxVersion-retryLowerBound)/2, nextMaxVersion)
	_, ok = streamhelper.TESTResolveLockRetryLowerBound(checkpointTS, oracle.GoTimeToTS(checkpointTime.Add(5*time.Second)))
	require.False(t, ok)
}

func TestResolveLockIntervalUsesTiKVFlushInterval(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.CommandConfig) {
		c.TickDuration = 50 * time.Millisecond
	})
	require.Equal(t, 100*time.Millisecond, adv.TESTResolveLockInterval())
	require.Equal(t, config.DefaultTryAdvanceThreshold, adv.TESTDefaultStartPollThreshold())

	env.getLogBackupFlushInterval = func(context.Context) (time.Duration, error) {
		return 180 * time.Millisecond, nil
	}
	adv.TESTRefreshLogBackupFlushInterval(context.Background())
	require.Equal(t, 180*time.Millisecond, adv.TESTResolveLockInterval())
	require.Equal(t, 240*time.Millisecond, adv.TESTDefaultStartPollThreshold())
	require.Equal(t, 108*time.Millisecond, adv.TESTSubscriberErrorStartPollThreshold())

	env.getLogBackupFlushInterval = func(context.Context) (time.Duration, error) {
		return 0, errors.New("tikv config is temporarily unavailable")
	}
	adv.TESTRefreshLogBackupFlushInterval(context.Background())
	require.Equal(t, 180*time.Millisecond, adv.TESTResolveLockInterval())
	require.Equal(t, 240*time.Millisecond, adv.TESTDefaultStartPollThreshold())
}

func TestGetLogBackupFlushIntervalFromTiKVConfig(t *testing.T) {
	flushInterval, err := streamhelper.GetLogBackupFlushIntervalFromTiKVConfig(
		context.Background(),
		func(_ context.Context, collect func([]byte) error) error {
			if err := collect([]byte(`{"log-backup":{"max-flush-interval":"20s"}}`)); err != nil {
				return err
			}
			return collect([]byte(`{"log-backup":{"max-flush-interval":"30s"}}`))
		})
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, flushInterval)

	_, err = streamhelper.GetLogBackupFlushIntervalFromTiKVConfig(
		context.Background(),
		func(_ context.Context, collect func([]byte) error) error {
			return collect([]byte(`{"log-backup":{"enable":true}}`))
		})
	require.ErrorContains(t, err, "log-backup.max-flush-interval is not found")

	_, err = streamhelper.GetLogBackupFlushIntervalFromTiKVConfig(
		context.Background(),
		func(context.Context, func([]byte) error) error {
			return nil
		})
	require.ErrorContains(t, err, "no TiKV config found")
}

func TestResolveLockTargetsUseUpperBound(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	ctx := context.Background()
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.CommandConfig) {
		c.TickDuration = 10 * time.Second
	})
	adv.StartTaskListener(ctx)

	checkpointTime := time.Now().Add(-50 * time.Second)
	checkpointTS := oracle.GoTimeToTS(checkpointTime)
	withinUpperBoundTS := oracle.GoTimeToTS(checkpointTime.Add(6 * time.Second))
	outsideUpperBoundTS := oracle.GoTimeToTS(checkpointTime.Add(8 * time.Second))
	adv.WithCheckpoints(func(s *spans.ValueSortedFull) {
		s.Merge(spans.Valued{Key: kv.KeyRange{}, Value: checkpointTS})
		s.Merge(spans.Valued{Key: kv.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}, Value: withinUpperBoundTS})
		s.Merge(spans.Valued{Key: kv.KeyRange{StartKey: []byte("b")}, Value: outsideUpperBoundTS})
	})

	adv.TESTSetLastCheckpointToCurrentMin()
	c.SetCurrentTS(oracle.GoTimeToTS(checkpointTime.Add(40 * time.Second)))
	require.Equal(t, 0, adv.TESTResolveLockTargetCount())
	c.SetCurrentTS(oracle.GoTimeToTS(checkpointTime.Add(47 * time.Second)))
	require.Equal(t, 2, adv.TESTResolveLockTargetCount())
}

func TestResolveLockRetryWhenCheckpointNotAdvanced(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		if t.Failed() {
			fmt.Println(c)
		}
	}()

	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx := context.Background()
	minCheckpoint := oracle.GoTimeToTS(time.Now().Add(-10 * time.Second))
	c.SetCurrentTS(oracle.GoTimeToTS(oracle.GetTimeFromTS(minCheckpoint).Add(10 * time.Second)))
	env := newTestEnv(c, t)

	lockRegion := c.FindRegionByKey([]byte("01"))
	allLocks := []*txnlock.Lock{
		{
			Key:   []byte("011"),
			TxnID: minCheckpoint,
		},
	}
	c.LockRegion(lockRegion, allLocks)

	resolveLockCount := atomic.NewInt32(0)
	env.resolveLocks = func(locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
		require.ElementsMatch(t, locks, allLocks)
		resolveLockCount.Inc()
		return loc, nil
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.CommandConfig) {
		c.TickDuration = 50 * time.Millisecond
	})
	adv.StartTaskListener(ctx)

	outsideUpperBoundTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(minCheckpoint).Add(2 * time.Minute))
	adv.WithCheckpoints(func(s *spans.ValueSortedFull) {
		s.Merge(spans.Valued{Key: kv.KeyRange{}, Value: minCheckpoint})
		s.Merge(spans.Valued{Key: kv.KeyRange{EndKey: lockRegion.Range.StartKey}, Value: outsideUpperBoundTS})
		s.Merge(spans.Valued{Key: kv.KeyRange{StartKey: lockRegion.Range.EndKey}, Value: outsideUpperBoundTS})
	})
	adv.TESTSetLastCheckpointToCurrentMin()
	require.Equal(t, 1, adv.TESTResolveLockTargetCount())
	time.Sleep(adv.Config().GetResolveLockInterval() + 10*time.Millisecond)

	adv.TESTTryResolveLocksForCheckpoint()
	require.Eventually(t, func() bool {
		return resolveLockCount.Load() == 1 && !adv.GetInResolvingLock()
	}, time.Second, time.Millisecond)

	lockRegion.Locks = allLocks
	adv.TESTTryResolveLocksForCheckpoint()
	time.Sleep(adv.Config().GetResolveLockInterval() / 2)
	require.Equal(t, int32(1), resolveLockCount.Load())

	time.Sleep(adv.Config().GetResolveLockInterval() + 10*time.Millisecond)
	adv.TESTTryResolveLocksForCheckpoint()
	require.Eventually(t, func() bool {
		return resolveLockCount.Load() == 2 && !adv.GetInResolvingLock()
	}, time.Second, time.Millisecond)
}

func TestOwnerDropped(t *testing.T) {
	ctx := context.Background()
	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	installSubscribeSupport(c)
	env := newTestEnv(c, t)
	fp := "github.com/pingcap/tidb/br/pkg/streamhelper/get_subscriber"
	defer func() {
		if t.Failed() {
			fmt.Println(c)
		}
	}()

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.OnStart(ctx)
	adv.SpawnSubscriptionHandler(ctx)
	require.NoError(t, adv.OnTick(ctx))
	failpoint.Enable(fp, "pause")
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		require.NoError(t, adv.OnTick(ctx))
	}()
	adv.OnStop()
	failpoint.Disable(fp)

	cp := c.advanceCheckpoints()
	c.flushAll()
	<-ch
	adv.WithCheckpoints(func(vsf *spans.ValueSortedFull) {
		// Advancer will manually poll the checkpoint...
		require.Equal(t, vsf.MinValue(), cp)
	})
}

// TestRemoveTaskAndFlush tests the bug has been described in #50839.
func TestRemoveTaskAndFlush(t *testing.T) {
	logutil.OverrideLevelForTest(t, zapcore.DebugLevel)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	installSubscribeSupport(c)
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	adv.SpawnSubscriptionHandler(ctx)
	require.NoError(t, adv.OnTick(ctx))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription-handler-loop", "pause"))
	c.flushAll()
	env.unregisterTask()
	require.Eventually(t, func() bool {
		return !adv.HasTask()
	}, 10*time.Second, 100*time.Millisecond)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription-handler-loop"))
	require.Eventually(t, func() bool {
		return !adv.HasSubscriptions()
	}, 10*time.Second, 100*time.Millisecond)
}

func TestEnableCheckPointLimit(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(1 * time.Minute)),
		},
		Ranges: rngs,
	}
	log.Info("Start Time:", zap.Uint64("StartTs", env.task.Info.StartTs))

	adv := streamhelper.NewCommandCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	c.advanceClusterTimeBy(1 * time.Minute)
	c.advanceCheckpointBy(1 * time.Minute)
	adv.StartTaskListener(ctx)
	for range 5 {
		c.advanceClusterTimeBy(30 * time.Second)
		c.advanceCheckpointBy(20 * time.Second)
		require.NoError(t, adv.OnTick(ctx))
	}
}

func TestOwnerChangeCheckPointLagged(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(1 * time.Minute)),
		},
		Ranges: rngs,
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	ctx1, cancel1 := context.WithCancel(context.Background())
	adv.OnStart(ctx1)
	adv.OnBecomeOwner(ctx1)
	log.Info("advancer1 become owner")
	require.NoError(t, adv.OnTick(ctx1))

	// another advancer but never advance checkpoint before
	adv2 := streamhelper.NewCheckpointAdvancer(env)
	adv2.UpdateCheckPointLagLimit(time.Minute)
	ctx2, cancel2 := context.WithCancel(context.Background())

	for range 5 {
		c.advanceClusterTimeBy(2 * time.Minute)
		c.advanceCheckpointBy(2 * time.Minute)
		require.NoError(t, adv.OnTick(ctx1))
	}
	c.advanceClusterTimeBy(2 * time.Minute)
	require.ErrorContains(t, adv.OnTick(ctx1), "lagged too large")

	// resume task to make next tick normally
	c.advanceCheckpointBy(2 * time.Minute)
	env.ResumeTask(ctx)

	// stop advancer1, and advancer2 should take over
	cancel1()
	log.Info("advancer1 owner canceled, and advancer2 become owner")
	adv2.OnStart(ctx2)
	adv2.OnBecomeOwner(ctx2)
	require.NoError(t, adv2.OnTick(ctx2))

	// advancer2 should take over and tick normally
	for range 10 {
		c.advanceClusterTimeBy(2 * time.Minute)
		c.advanceCheckpointBy(2 * time.Minute)
		require.NoError(t, adv2.OnTick(ctx2))
	}
	c.advanceClusterTimeBy(2 * time.Minute)
	require.ErrorContains(t, adv2.OnTick(ctx2), "lagged too large")
	// stop advancer2, and advancer1 should take over
	c.advanceCheckpointBy(2 * time.Minute)
	env.ResumeTask(ctx)
	cancel2()
	log.Info("advancer2 owner canceled, and advancer1 become owner")

	adv.OnBecomeOwner(ctx)
	// advancer1 should take over and tick normally when come back
	require.NoError(t, adv.OnTick(ctx))
}

func TestCheckPointLagged(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(1 * time.Minute)),
		},
		Ranges: rngs,
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(2 * time.Minute)
	// if global ts is not advanced, the checkpoint will not be lagged
	c.advanceCheckpointBy(2 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	c.advanceClusterTimeBy(3 * time.Minute)
	require.ErrorContains(t, adv.OnTick(ctx), "lagged too large")
	// after some times, the isPaused will be set and ticks are skipped
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
}

// If the paused task are manually resumed, it should run normally
func TestCheckPointResume(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(1 * time.Minute)
	// if global ts is not advanced, the checkpoint will not be lagged
	c.advanceCheckpointBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	c.advanceClusterTimeBy(2 * time.Minute)
	require.ErrorContains(t, adv.OnTick(ctx), "lagged too large")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	//now the checkpoint issue is fixed and resumed
	c.advanceCheckpointBy(1 * time.Minute)
	env.ResumeTask(ctx)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	//with time passed, the checkpoint will exceed the limit again
	c.advanceClusterTimeBy(2 * time.Minute)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ErrorContains(c, adv.OnTick(ctx), "lagged too large")
	}, 5*time.Second, 100*time.Millisecond)
}

func TestUnregisterAfterPause(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	adv.StartTaskListener(ctx)

	// wait for the task to be added
	require.Eventually(t, func() bool {
		return adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)

	// task is should be paused when global checkpoint is laggeod
	// even the global checkpoint is equal to task start ts(not advanced all the time)
	c.advanceClusterTimeBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	env.PauseTask(ctx, "whole")
	c.advanceClusterTimeBy(1 * time.Minute)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	env.unregisterTask()
	require.Eventually(t, func() bool {
		return !adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)
	env.putTask()

	// wait for the task to be added
	require.Eventually(t, func() bool {
		return adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ErrorContains(c, adv.OnTick(ctx), "check point lagged too large")
	}, 5*time.Second, 100*time.Millisecond)

	env.unregisterTask()
	// wait for the task to be deleted
	require.Eventually(t, func() bool {
		return !adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)

	// reset
	c.advanceClusterTimeBy(-1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	env.PauseTask(ctx, "whole")
	c.advanceClusterTimeBy(1 * time.Minute)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	env.unregisterTask()
	require.Eventually(t, func() bool {
		return !adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)
	env.putTask()
	// wait for the task to be add
	require.Eventually(t, func() bool {
		return adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ErrorContains(c, adv.OnTick(ctx), "check point lagged too large")
	}, 5*time.Second, 100*time.Millisecond)
}

// If the start ts is *NOT* lagged, even both the cluster and pd are lagged, the task should run normally.
func TestAddTaskWithLongRunTask0(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(2 * time.Minute)),
		},
		Ranges: rngs,
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	c.advanceClusterTimeBy(3 * time.Minute)
	c.advanceCheckpointBy(1 * time.Minute)
	env.advanceCheckpointBy(1 * time.Minute)
	env.mockPDConnectionError()
	adv.StartTaskListener(ctx)
	// Try update checkpoint
	require.NoError(t, adv.OnTick(ctx))
	// Verify no err raised
	require.NoError(t, adv.OnTick(ctx))
}

// If the start ts is lagged, as long as cluster has advanced, the task should run normally.
func TestAddTaskWithLongRunTask1(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(1 * time.Minute)),
		},
		Ranges: rngs,
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	c.advanceClusterTimeBy(3 * time.Minute)
	c.advanceCheckpointBy(2 * time.Minute)
	env.advanceCheckpointBy(1 * time.Minute)
	adv.StartTaskListener(ctx)
	// Try update checkpoint
	require.NoError(t, adv.OnTick(ctx))
	// Verify no err raised
	require.NoError(t, adv.OnTick(ctx))
}

// If the start ts is lagged, as long as pd stored the advanced checkpoint, the task should run normally.
// Also, temporary connection error won't affect the task.
func TestAddTaskWithLongRunTask2(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(1 * time.Minute)),
		},
		Ranges: rngs,
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(3 * time.Minute)
	c.advanceCheckpointBy(1 * time.Minute)
	env.advanceCheckpointBy(2 * time.Minute)
	env.mockPDConnectionError()
	// if cannot connect to pd, the checkpoint will be rolled back
	// because at this point. the global ts is 2 minutes
	// and the local checkpoint ts is 1 minute
	require.Error(t, adv.OnTick(ctx), "checkpoint rollback")

	// only when local checkpoint > global ts, the next tick will be normal
	c.advanceCheckpointBy(12 * time.Minute)
	// Verify no err raised
	require.NoError(t, adv.OnTick(ctx))
}

// If the start ts, pd, and cluster checkpoint are all lagged, the task should pause.
func TestAddTaskWithLongRunTask3(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(1 * time.Minute)),
		},
		Ranges: rngs,
	}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateCheckPointLagLimit(time.Minute)
	// advance cluster time to 4 minutes, and checkpoint to 1 minutes
	// if start ts equals to checkpoint, the task will not be paused
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(2 * time.Minute)
	c.advanceCheckpointBy(1 * time.Minute)
	env.advanceCheckpointBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))

	c.advanceClusterTimeBy(2 * time.Minute)
	c.advanceCheckpointBy(1 * time.Minute)
	env.advanceCheckpointBy(1 * time.Minute)
	// Try update checkpoint
	require.ErrorContains(t, adv.OnTick(ctx), "lagged too large")
	// Verify no err raised after paused
	require.Eventually(t, func() bool {
		err := adv.OnTick(ctx)
		return err == nil
	}, 5*time.Second, 300*time.Millisecond)
}

func TestOwnershipLost(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	c.splitAndScatter(manyRegions(0, 10240)...)
	installSubscribeSupport(c)
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.OnStart(ctx)
	adv.OnBecomeOwner(ctx)
	require.NoError(t, adv.OnTick(ctx))
	c.advanceCheckpoints()
	c.flushAll()
	failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend", "pause")
	failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/FlushSubscriber.Clear.timeoutMs", "return(500)")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/streamhelper/FlushSubscriber.Clear.timeoutMs"))
	}()
	wg := new(sync.WaitGroup)
	wg.Add(adv.TEST_registerCallbackForSubscriptions(wg.Done))
	cancel()
	failpoint.Disable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend")
	wg.Wait()
}

func TestSubscriptionPanic(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	c.splitAndScatter(manyRegions(0, 20)...)
	installSubscribeSupport(c)
	ctx, cancel := context.WithCancel(context.Background())
	env := newTestEnv(c, t)
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.OnStart(ctx)
	adv.OnBecomeOwner(ctx)
	wg := new(sync.WaitGroup)
	wg.Add(adv.TEST_registerCallbackForSubscriptions(wg.Done))

	require.NoError(t, adv.OnTick(ctx))
	failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend", "5*panic")
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend"))
	}()
	ckpt := c.advanceCheckpoints()
	c.flushAll()
	cnt := 0
	for {
		require.NoError(t, adv.OnTick(ctx))
		cnt++
		if env.checkpoint >= ckpt {
			break
		}
		if cnt > 100 {
			t.Fatalf("After 100 times, the progress cannot be advanced.")
		}
	}
	cancel()
	wg.Wait()
}

func TestGCCheckpoint(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestEnv(c, t)
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: oracle.GoTimeToTS(oracle.GetTimeFromTS(0)),
		},
		Ranges: rngs,
	}
	log.Info("Start Time:", zap.Uint64("StartTs", env.task.Info.StartTs))

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.Eventually(t, func() bool {
		return adv.HasTask()
	}, 5*time.Second, 100*time.Millisecond)
	c.advanceClusterTimeBy(1 * time.Minute)
	c.advanceCheckpointBy(1 * time.Minute)
	env.PauseTask(ctx, "whole")
	c.ServiceGCSafePoint = oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(2 * time.Minute))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	env.ResumeTask(ctx)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ErrorContains(c, adv.OnTick(ctx), "greater than the target")
	}, 5*time.Second, 100*time.Millisecond)
}

func TestRedactBackend(t *testing.T) {
	info := new(backup.StreamBackupTaskInfo)
	info.Name = "test"
	info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_S3{
			S3: &backup.S3{
				Endpoint:        "http://",
				Bucket:          "test",
				Prefix:          "test",
				AccessKey:       "12abCD!@#[]{}?/\\",
				SecretAccessKey: "12abCD!@#[]{}?/\\",
			},
		},
	}

	redacted := redact.TaskInfoRedacted{Info: info}
	require.Equal(t, redacted.String(), "storage:<s3:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" access_key:\"[REDACTED]\" secret_access_key:\"[REDACTED]\" sse_kms_key_id:\"[REDACTED]\" > > name:\"test\" ")
	require.Equal(t, info.String(), "storage:<s3:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" access_key:\"12abCD!@#[]{}?/\\\\\" secret_access_key:\"12abCD!@#[]{}?/\\\\\" > > name:\"test\" ")

	info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_Gcs{
			Gcs: &backup.GCS{
				Endpoint:        "http://",
				Bucket:          "test",
				Prefix:          "test",
				CredentialsBlob: "12abCD!@#[]{}?/\\",
			},
		},
	}
	redacted = redact.TaskInfoRedacted{Info: info}
	require.Equal(t, redacted.String(), "storage:<gcs:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" credentials_blob:\"[REDACTED]\" > > name:\"test\" ")
	require.Equal(t, info.String(), "storage:<gcs:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" credentials_blob:\"12abCD!@#[]{}?/\\\\\" > > name:\"test\" ")

	info.Storage = &backup.StorageBackend{
		Backend: &backup.StorageBackend_AzureBlobStorage{
			AzureBlobStorage: &backup.AzureBlobStorage{
				Endpoint:  "http://",
				Bucket:    "test",
				Prefix:    "test",
				SharedKey: "12abCD!@#[]{}?/\\",
				AccessSig: "12abCD!@#[]{}?/\\",
				EncryptionKey: &backup.AzureCustomerKey{
					EncryptionKey:       "12abCD!@#[]{}?/\\",
					EncryptionKeySha256: "12abCD!@#[]{}?/\\",
				},
			},
		},
	}
	redacted = redact.TaskInfoRedacted{Info: info}
	require.Equal(t, redacted.String(), "storage:<azure_blob_storage:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" shared_key:\"[REDACTED]\" access_sig:\"[REDACTED]\" encryption_key:<encryption_key:\"[REDACTED]\" > > > name:\"test\" ")
	require.Equal(t, info.String(), "storage:<azure_blob_storage:<endpoint:\"http://\" bucket:\"test\" prefix:\"test\" shared_key:\"12abCD!@#[]{}?/\\\\\" access_sig:\"12abCD!@#[]{}?/\\\\\" encryption_key:<encryption_key:\"12abCD!@#[]{}?/\\\\\" encryption_key_sha256:\"12abCD!@#[]{}?/\\\\\" > > > name:\"test\" ")
}
