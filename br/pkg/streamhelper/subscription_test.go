// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func installSubscribeSupport(c *fakeCluster) {
	for _, s := range c.stores {
		s.SetSupportFlushSub(true)
	}
}

func installSubscribeSupportForRandomN(c *fakeCluster, n int) {
	i := 0
	for _, s := range c.stores {
		if i == n {
			break
		}
		s.SetSupportFlushSub(true)
		i++
	}
}

func waitEvents(t *testing.T, sub *streamhelper.FlushSubscriber, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(sub.Events()) >= expected
	}, 3*time.Second, 100*time.Millisecond)
}

// collectCheckpointSpans drains the subscription events until the observed
// spans cover the checkpoint. Waiting for the events to be delivered avoids the
// race where the subscription has not pushed all events yet when the test
// drains the channel (see pingcap/tidb#52791, #67839).
func collectCheckpointSpans(t *testing.T, sub *streamhelper.FlushSubscriber, checkpoint uint64) *spans.ValueSortedFull {
	t.Helper()
	observed := spans.Sorted(spans.NewFullWith(spans.Full(), 1))
	require.Eventually(t, func() bool {
		for {
			select {
			case event := <-sub.Events():
				observed.Merge(event)
			default:
				return observed.MinValue() >= checkpoint
			}
		}
	}, 3*time.Second, 100*time.Millisecond)
	return observed
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
	s := collectCheckpointSpans(t, sub, cp)
	sub.Drop()
	defer func() {
		if t.Failed() {
			fmt.Println(c)
			spans.Debug(s)
		}
	}()

	req.GreaterOrEqual(s.MinValue(), cp, "s.MinValue() = %d, cp = %d", s.MinValue(), cp)
}

func TestNormalError(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009")
	installSubscribeSupport(c)

	sub := streamhelper.NewSubscriber(c, c)
	c.onGetClient = oneStoreFailure()
	req.NoError(sub.UpdateStoreTopology(ctx))
	c.onGetClient = nil
	req.Error(sub.PendingErrors())
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())
	var cp uint64
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	s := collectCheckpointSpans(t, sub, cp)
	sub.Drop()
	req.Equal(cp, s.MinValue(), "%d vs %d", cp, s.MinValue())
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

	c.onGetClient = func(u uint64) error {
		return status.Error(codes.DataLoss, "upon an eclipsed night, some of data (not all data) have fled from the dataset")
	}
	sub := streamhelper.NewSubscriber(c, c)
	req.NoError(sub.UpdateStoreTopology(ctx))
	req.Error(sub.PendingErrors())

	c.onGetClient = nil
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
	for _, s := range c.stores {
		c.removeStore(s.id)
		break
	}
	req.NoError(sub.UpdateStoreTopology(ctx))
	for i := 0; i < 10; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	sub.HandleErrors(ctx)
	req.NoError(sub.PendingErrors())

	s := collectCheckpointSpans(t, sub, cp)
	sub.Drop()

	defer func() {
		if t.Failed() {
			fmt.Println(c)
			spans.Debug(s)
		}
	}()

	req.GreaterOrEqual(s.MinValue(), cp, "s.MinValue() = %d, cp = %d", s.MinValue(), cp)
}

func TestSomeOfStoreUnsupported(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	const flushRounds = 10
	c := createFakeCluster(t, 4, true)
	c.splitAndScatter("0001", "0002", "0003", "0008", "0009", "0010", "0100", "0956", "1000")

	sub := streamhelper.NewSubscriber(c, c)
	installSubscribeSupportForRandomN(c, 3)
	req.NoError(sub.UpdateStoreTopology(ctx))

	supportedStores := make(map[uint64]struct{})
	for _, store := range c.stores {
		if store.supportsSub {
			supportedStores[store.id] = struct{}{}
		}
	}
	expectedEventsPerFlush := 0
	for _, region := range c.regions {
		if _, ok := supportedStores[region.leader]; ok {
			expectedEventsPerFlush++
		}
	}

	var cp uint64
	for i := 0; i < flushRounds; i++ {
		cp = c.advanceCheckpoints()
		c.flushAll()
	}
	s := spans.Sorted(spans.NewFullWith(spans.Full(), 1))
	m := new(sync.Mutex)

	waitEvents(t, sub, expectedEventsPerFlush*flushRounds)
	sub.Drop()
	for k := range sub.Events() {
		s.Merge(k)
	}

	rngs := make([]spans.Span, 0)
	s.TraverseValuesLessThan(cp, func(v spans.Valued) bool {
		rngs = append(rngs, v.Key)
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
	req.Equal(cp, s.MinValue())
}
