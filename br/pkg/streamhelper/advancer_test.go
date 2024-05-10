// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/atomic"
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
	env := &testEnv{fakeCluster: c, testCtx: t}
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
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))
	for i := 0; i < 5; i++ {
		cp := c.advanceCheckpoints()
		require.NoError(t, adv.OnTick(ctx))
		require.Equal(t, env.getCheckpoint(), cp)
	}
}

func TestWithFailure(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	c.flushAll()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))

	cp := c.advanceCheckpoints()
	for _, v := range c.stores {
		v.flush()
		break
	}
	require.NoError(t, adv.OnTick(ctx))
	require.Less(t, env.getCheckpoint(), cp, "%d %d", env.getCheckpoint(), cp)

	for _, v := range c.stores {
		v.flush()
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
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	c.onGetClient = func(u uint64) error {
		return status.Error(codes.DataLoss,
			"Exiled requests from the client, please slow down and listen a story: "+
				"the server has been dropped, we are longing for new nodes, however the goddess(k8s) never allocates new resource. "+
				"May you take the sword named `vim`, refactoring the definition of the nature, in the yaml file hidden at somewhere of the cluster, "+
				"to save all of us and gain the response you desiring?")
	}
	ctx := context.Background()
	splitKeys := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		splitKeys = append(splitKeys, fmt.Sprintf("%04d", i))
	}
	c.splitAndScatter(splitKeys...)

	env := &testEnv{fakeCluster: c, testCtx: t}
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
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	ctx := context.Background()
	splitKeys := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		splitKeys = append(splitKeys, fmt.Sprintf("%04d", i))
	}
	c.splitAndScatter(splitKeys...)
	c.flushAll()

	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	require.NoError(t, adv.OnTick(ctx))
	c.onGetClient = oneStoreFailure()

	for i := 0; i < 100; i++ {
		c.advanceCheckpoints()
		c.flushAll()
		require.ErrorContains(t, adv.OnTick(ctx), "the warm lamplight")
	}

	c.onGetClient = nil
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
	env := &testEnv{fakeCluster: c, testCtx: t}

	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	cp := c.advanceCheckpoints()
	c.flushAll()

	req.NoError(adv.OnTick(ctx))
	req.Equal(env.serviceGCSafePoint, cp-1)

	env.unregisterTask()
	req.Eventually(func() bool {
		env.fakeCluster.mu.Lock()
		defer env.fakeCluster.mu.Unlock()
		return env.serviceGCSafePoint != 0 && env.serviceGCSafePointDeleted
	}, 3*time.Second, 100*time.Millisecond)
}

func TestTaskRanges(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	defer fmt.Println(c)
	ctx := context.Background()
	c.splitAndScatter("0001", "0002", "0012", "0034", "0048")
	c.advanceCheckpoints()
	c.flushAllExcept("0000", "0049")
	env := &testEnv{fakeCluster: c, testCtx: t, ranges: []kv.KeyRange{{StartKey: []byte("0002"), EndKey: []byte("0048")}}}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)

	shouldFinishInTime(t, 10*time.Second, "first advancing", func() { require.NoError(t, adv.OnTick(ctx)) })
	// Don't check the return value of advance checkpoints here -- we didn't
	require.Greater(t, env.getCheckpoint(), uint64(0))
}

func TestTaskRangesWithSplit(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	defer fmt.Println(c)
	ctx := context.Background()
	c.splitAndScatter("0012", "0034", "0048")
	c.advanceCheckpoints()
	c.flushAllExcept("0049")
	env := &testEnv{fakeCluster: c, testCtx: t, ranges: []kv.KeyRange{{StartKey: []byte("0002"), EndKey: []byte("0048")}}}
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
	c.onClearCache = func(u uint64) error {
		// make store u cache cleared
		clearedCache[u] = true
		return nil
	}
	failedStoreID := uint64(0)
	hasFailed := atomic.NewBool(false)
	for _, s := range c.stores {
		s.clientMu.Lock()
		sid := s.GetID()
		s.onGetRegionCheckpoint = func(glftrr *logbackup.GetLastFlushTSOfRegionRequest) error {
			// mark one store failed is enough
			if hasFailed.CompareAndSwap(false, true) {
				// mark this store cache cleared
				failedStoreID = sid
				return errors.New("failed to get checkpoint")
			}
			return nil
		}
		s.clientMu.Unlock()
	}
	env := &testEnv{fakeCluster: c, testCtx: t}
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
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(t, 4, true)
	ctx := context.Background()
	req := require.New(t)
	c.splitAndScatter("0012", "0034", "0048")
	marked := false
	for _, s := range c.stores {
		s.clientMu.Lock()
		s.onGetRegionCheckpoint = func(glftrr *logbackup.GetLastFlushTSOfRegionRequest) error {
			// blocking the thread.
			// this may happen when TiKV goes down or too busy.
			<-(chan struct{})(nil)
			return nil
		}
		s.clientMu.Unlock()
		marked = true
	}
	req.True(marked, "failed to mark the cluster: ")
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	adv.UpdateConfigWith(func(c *config.Config) {
		// ... So the tick timeout would be 100ms
		c.TickDuration = 10 * time.Millisecond
	})
	var err error
	shouldFinishInTime(t, time.Second, "ticking", func() {
		err = adv.OnTick(ctx)
	})
	req.ErrorIs(errors.Cause(err), context.DeadlineExceeded)
}

func TestResolveLock(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		if t.Failed() {
			fmt.Println(c)
		}
	}()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/NeedResolveLocks", `return(true)`))
	// make sure asyncResolveLocks stuck in optionalTick later.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/AsyncResolveLocks", `pause`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/streamhelper/NeedResolveLocks"))
	}()

	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx := context.Background()
	minCheckpoint := c.advanceCheckpoints()
	env := &testEnv{fakeCluster: c, testCtx: t}

	lockRegion := c.findRegionByKey([]byte("01"))
	allLocks := []*txnlock.Lock{
		{
			Key: []byte{1},
			// TxnID == minCheckpoint
			TxnID: minCheckpoint,
		},
		{
			Key: []byte{2},
			// TxnID > minCheckpoint
			TxnID: minCheckpoint + 1,
		},
	}
	c.LockRegion(lockRegion, allLocks)

	// ensure resolve locks triggered and collect all locks from scan locks
	resolveLockRef := atomic.NewBool(false)
	env.resolveLocks = func(locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
		resolveLockRef.Store(true)
		require.ElementsMatch(t, locks, allLocks)
		return loc, nil
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	// make lastCheckpoint stuck at 123
	adv.UpdateLastCheckpoint(streamhelper.NewCheckpointWithSpan(spans.Valued{
		Key: kv.KeyRange{
			StartKey: kv.Key([]byte("1")),
			EndKey:   kv.Key([]byte("2")),
		},
		Value: 123,
	}))
	adv.NewCheckpoints(
		spans.Sorted(spans.NewFullWith([]kv.KeyRange{
			{
				StartKey: kv.Key([]byte("1")),
				EndKey:   kv.Key([]byte("2")),
			},
		}, 0)),
	)
	adv.StartTaskListener(ctx)
	require.Eventually(t, func() bool { return adv.OnTick(ctx) == nil },
		time.Second, 50*time.Millisecond)
	coll := streamhelper.NewClusterCollector(ctx, env)
	err := adv.GetCheckpointInRange(ctx, []byte{}, []byte{}, coll)
	require.NoError(t, err)
	// now the lock state must be ture. because tick finished and asyncResolveLocks got stuck.
	require.True(t, adv.GetInResolvingLock())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/streamhelper/AsyncResolveLocks"))
	require.Eventually(t, func() bool { return resolveLockRef.Load() },
		8*time.Second, 50*time.Microsecond)
	// state must set to false after tick
	require.Eventually(t, func() bool { return !adv.GetInResolvingLock() },
		8*time.Second, 50*time.Microsecond)
	r, err := coll.Finish(ctx)
	require.NoError(t, err)
	require.Len(t, r.FailureSubRanges, 0)
	require.Equal(t, r.Checkpoint, minCheckpoint, "%d %d", r.Checkpoint, minCheckpoint)
}

func TestOwnerDropped(t *testing.T) {
	ctx := context.Background()
	c := createFakeCluster(t, 4, false)
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	installSubscribeSupport(c)
	env := &testEnv{testCtx: t, fakeCluster: c}
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
	log.SetLevel(zapcore.DebugLevel)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	installSubscribeSupport(c)
	env := &testEnv{
		fakeCluster: c,
		testCtx:     t,
	}
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
		return !adv.HasSubscribion()
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
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.Config) {
		c.CheckPointLagLimit = 1 * time.Minute
	})
	adv.StartTaskListener(ctx)
	for i := 0; i < 5; i++ {
		c.advanceClusterTimeBy(30 * time.Second)
		c.advanceCheckpointBy(20 * time.Second)
		require.NoError(t, adv.OnTick(ctx))
	}
}

func TestCheckPointLagged(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.Config) {
		c.CheckPointLagLimit = 1 * time.Minute
	})
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	c.advanceClusterTimeBy(1 * time.Minute)
	require.ErrorContains(t, adv.OnTick(ctx), "lagged too large")
	// after some times, the isPaused will be set and ticks are skipped
	require.Eventually(t, func() bool {
		return assert.NoError(t, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
}

func TestCheckPointResume(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.Config) {
		c.CheckPointLagLimit = 1 * time.Minute
	})
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	c.advanceClusterTimeBy(1 * time.Minute)
	require.ErrorContains(t, adv.OnTick(ctx), "lagged too large")
	require.Eventually(t, func() bool {
		return assert.NoError(t, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	//now the checkpoint issue is fixed and resumed
	c.advanceCheckpointBy(1 * time.Minute)
	env.ResumeTask(ctx)
	require.Eventually(t, func() bool {
		return assert.NoError(t, adv.OnTick(ctx))
	}, 5*time.Second, 100*time.Millisecond)
	//with time passed, the checkpoint will exceed the limit again
	c.advanceClusterTimeBy(2 * time.Minute)
	require.ErrorContains(t, adv.OnTick(ctx), "lagged too large")
}

func TestUnregisterAfterPause(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.UpdateConfigWith(func(c *config.Config) {
		c.CheckPointLagLimit = 1 * time.Minute
	})
	adv.StartTaskListener(ctx)
	c.advanceClusterTimeBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	env.PauseTask(ctx, "whole")
	time.Sleep(1 * time.Second)
	c.advanceClusterTimeBy(1 * time.Minute)
	require.NoError(t, adv.OnTick(ctx))
	env.unregisterTask()
	env.putTask()
	require.Eventually(t, func() bool {
		err := adv.OnTick(ctx)
		return err != nil && strings.Contains(err.Error(), "check point lagged too large")
	}, 5*time.Second, 300*time.Millisecond)
}

func TestOwnershipLost(t *testing.T) {
	c := createFakeCluster(t, 4, false)
	c.splitAndScatter(manyRegions(0, 10240)...)
	installSubscribeSupport(c)
	ctx, cancel := context.WithCancel(context.Background())
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.OnStart(ctx)
	adv.OnBecomeOwner(ctx)
	require.NoError(t, adv.OnTick(ctx))
	c.advanceCheckpoints()
	c.flushAll()
	failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend", "pause")
	failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/FlushSubscriber.Clear.timeoutMs", "return(500)")
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
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.OnStart(ctx)
	adv.OnBecomeOwner(ctx)
	wg := new(sync.WaitGroup)
	wg.Add(adv.TEST_registerCallbackForSubscriptions(wg.Done))

	require.NoError(t, adv.OnTick(ctx))
	failpoint.Enable("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend", "5*panic")
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
