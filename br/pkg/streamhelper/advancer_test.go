// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
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
