// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func installSubscribeSupport(c *fakeCluster) {
	for _, s := range c.storeList() {
		s.SetSupportFlushSub(true)
	}
}

func installSubscribeSupportForRandomN(c *fakeCluster, n int) {
	i := 0
	for _, s := range c.storeList() {
		if i == n {
			break
		}
		s.SetSupportFlushSub(true)
		i++
	}
}

func minimumCheckpoint(s *spans.ValuedFull) uint64 {
	minimum := ^uint64(0)
	s.Traverse(func(v spans.Valued) bool { minimum = min(minimum, v.Value); return true })
	return minimum
}

// Consume while producers are running: a bounded batch queue applies
// backpressure, so waiting for producers to finish before receiving would stall.
func collectPendingEvents(t *testing.T, sub *streamhelper.FlushSubscriber) *spans.ValuedFull {
	t.Helper()
	s := spans.NewFullWith(spans.Full(), 1)
	idle := time.NewTimer(100 * time.Millisecond)
	defer idle.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case batch, ok := <-sub.Events():
			if !ok {
				return s
			}
			for _, event := range batch {
				s.Merge(event)
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(100 * time.Millisecond)
		case <-idle.C:
			return s
		case <-timeout.C:
			t.Fatal("subscription did not drain")
			return s
		}
	}
}

func TestSubBasic(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009")
	installSubscribeSupport(c)
	sub := streamhelper.NewSubscriber(c, c)
	req.NoError(sub.UpdateStoreTopology(ctx))
	var cp uint64
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
	s := collectPendingEvents(t, sub)
	sub.Drop()
	defer func() {
		if t.Failed() {
			fmt.Println(c)
			spans.Debug(s)
		}
	}()

	req.Equal(cp, minimumCheckpoint(s), "%d vs %d", cp, minimumCheckpoint(s))
}

func TestNormalError(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009")
	installSubscribeSupport(c)

	sub := streamhelper.NewSubscriber(c, c)
	c.SetOnGetClient(oneStoreFailure())
	req.NoError(sub.UpdateStoreTopology(ctx))
	c.SetOnGetClient(nil)
	req.Error(sub.PendingErrors())
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
	var cp uint64
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	s := collectPendingEvents(t, sub)
	sub.Drop()
	req.Equal(cp, minimumCheckpoint(s), "%d vs %d", cp, minimumCheckpoint(s))
}

func TestHasFailureStores(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009")

	installSubscribeSupportForRandomN(c, 3)
	sub := streamhelper.NewSubscriber(c, c)
	req.NoError(sub.UpdateStoreTopology(ctx))
	sub.HandleErrors(ctx)
	req.Error(sub.PendingErrors())

	installSubscribeSupport(c)
	req.NoError(sub.UpdateStoreTopology(ctx))
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
}

func TestStoreOffline(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009")
	installSubscribeSupport(c)

	c.SetOnGetClient(func(u uint64) error {
		return status.Error(codes.DataLoss, "upon an eclipsed night, some of data (not all data) have fled from the dataset")
	})
	sub := streamhelper.NewSubscriber(c, c)
	req.NoError(sub.UpdateStoreTopology(ctx))
	req.Error(sub.PendingErrors())

	c.SetOnGetClient(nil)
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
}

func TestStoreRemoved(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009", "0010", "0100", "0956", "1000")

	installSubscribeSupport(c)
	sub := streamhelper.NewSubscriber(c, c)
	req.NoError(sub.UpdateStoreTopology(ctx))

	var cp uint64
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
	for _, s := range c.storeList() {
		c.removeStore(s.ID)
		break
	}
	req.NoError(sub.UpdateStoreTopology(ctx))
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())

	s := collectPendingEvents(t, sub)
	sub.Drop()

	defer func() {
		if t.Failed() {
			fmt.Println(c)
			spans.Debug(s)
		}
	}()

	req.Equal(cp, minimumCheckpoint(s), "cp = %d, s = %d", cp, minimumCheckpoint(s))
}

func TestSomeOfStoreUnsupported(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009", "0010", "0100", "0956", "1000")

	sub := streamhelper.NewSubscriber(c, c)
	installSubscribeSupportForRandomN(c, 3)
	req.NoError(sub.UpdateStoreTopology(ctx))

	var cp uint64
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	s := collectPendingEvents(t, sub)
	m := new(sync.Mutex)
	sub.Drop()

	rngs := make([]spans.Span, 0)
	s.Traverse(func(v spans.Valued) bool {
		if v.Value < cp {
			rngs = append(rngs, v.Key)
		}
		return true
	})
	coll := streamhelper.NewClusterCollector(ctx, c)
	coll.SetOnSuccessHook(func(u uint64, kr spans.Span) {
		m.Lock()
		defer m.Unlock()
		s.Merge(spans.Valued{Key: kr, Value: u})
	})
	ld := uint64(0)
	for _, rng := range rngs {
		iter := streamhelper.IterateRegion(c, rng.StartKey, rng.EndKey)
		for !iter.Done() {
			rs, err := iter.Next(ctx)
			req.NoError(err)
			for _, r := range rs {
				if ld == 0 {
					ld = r.Leader.StoreId
				} else {
					req.Equal(r.Leader.StoreId, ld, "the leader is from different store: some of events not pushed")
				}
				coll.CollectRegion(r)
			}
		}
	}
	_, err := coll.Finish(ctx)
	req.NoError(err)
	req.Equal(cp, minimumCheckpoint(s))
}

func TestEncounterError(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009", "0010", "0100", "0956", "1000")

	sub := streamhelper.NewSubscriber(c, c)
	installSubscribeSupport(c)
	req.NoError(sub.UpdateStoreTopology(ctx))

	o := new(sync.Once)
	failpoint.EnableCall("github.com/pingcap/tidb/br/pkg/streamhelper/listen_flush_stream", func(storeID uint64, err *error) {
		o.Do(func() {
			*err = context.Canceled
		})
	})

	c.flushAll()
	require.Eventually(t, func() bool {
		return sub.PendingErrors() != nil
	}, 3*time.Second, 100*time.Millisecond)
	sub.HandleErrors(context.Background())
	require.NoError(t, sub.PendingErrors())
}

func TestSubscriptionIdleTimeoutClearsCacheBeforeRetry(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 1, true)
	installSubscribeSupport(c)

	clearedCache := make(chan uint64, 1)
	c.SetOnClearCache(func(storeID uint64) error {
		select {
		case clearedCache <- storeID:
		default:
		}
		return nil
	})

	sub := streamhelper.NewSubscriber(c, c, streamhelper.WithSubscriptionIdleTimeout(200*time.Millisecond))
	defer sub.Drop()
	req.NoError(sub.UpdateStoreTopology(ctx))
	req.Eventually(func() bool {
		err := sub.PendingErrors()
		return err != nil && strings.Contains(err.Error(), "while receiving from")
	}, 3*time.Second, 10*time.Millisecond)

	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
	req.Eventually(func() bool {
		return len(clearedCache) > 0
	}, 3*time.Second, 10*time.Millisecond)

	cp := c.advanceCheckpoints()
	c.flushAll()
	s := spans.NewFullWith(spans.Full(), 1)
	req.Eventually(func() bool {
		for {
			select {
			case batch := <-sub.Events():
				for _, event := range batch {
					s.Merge(event)
				}
			default:
				return minimumCheckpoint(s) == cp
			}
		}
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSubscriptionIdleTimeoutWhileSendingEvents(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	// The output queue is bounded by batches, not regions. A single region
	// avoids expensive fixture updates timing out an idle Recv before the
	// sender has filled the queue.
	c := createFakeCluster(t, 1, true)
	installSubscribeSupport(c)

	sub := streamhelper.NewSubscriber(c, c, streamhelper.WithSubscriptionIdleTimeout(200*time.Millisecond))
	defer sub.Drop()
	req.NoError(sub.UpdateStoreTopology(ctx))

	// Fill the batch queue without consuming it, so a sender remains blocked
	// until the idle watcher cancels the subscription.
	for i := 0; i <= cap(sub.Events()); i++ {
		c.flushAll()
	}
	req.Eventually(func() bool {
		return len(sub.Events()) == cap(sub.Events())
	}, 3*time.Second, 10*time.Millisecond)

	req.Eventually(func() bool {
		err := sub.PendingErrors()
		return err != nil && strings.Contains(err.Error(), "has no activity")
	}, 3*time.Second, 10*time.Millisecond)
}
