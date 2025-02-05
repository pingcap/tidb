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

func waitPendingEvents(t *testing.T, sub *streamhelper.FlushSubscriber) {
	last := len(sub.Events())
	time.Sleep(100 * time.Microsecond)
	require.Eventually(t, func() bool {
		noProg := len(sub.Events()) == last
		last = len(sub.Events())
		return noProg
	}, 3*time.Second, 100*time.Millisecond)
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
	waitPendingEvents(t, sub)
	sub.Drop()
	s := spans.Sorted(spans.NewFullWith(spans.Full(), 1))
	for k := range sub.Events() {
		s.Merge(k)
	}
	defer func() {
		if t.Failed() {
			fmt.Println(c)
			spans.Debug(s)
		}
	}()

	req.Equal(cp, s.MinValue(), "%d vs %d", cp, s.MinValue())
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
	waitPendingEvents(t, sub)
	sub.Drop()
	s := spans.Sorted(spans.NewFullWith(spans.Full(), 1))
	for k := range sub.Events() {
		s.Merge(k)
	}
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

	waitPendingEvents(t, sub)
	sub.Drop()
	s := spans.Sorted(spans.NewFullWith(spans.Full(), 1))
	for k := range sub.Events() {
		s.Merge(k)
	}

	defer func() {
		if t.Failed() {
			fmt.Println(c)
			spans.Debug(s)
		}
	}()

	req.Equal(cp, s.MinValue(), "cp = %d, s = %d", cp, s.MinValue())
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
	s := spans.Sorted(spans.NewFullWith(spans.Full(), 1))
	m := new(sync.Mutex)

	waitPendingEvents(t, sub)
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
