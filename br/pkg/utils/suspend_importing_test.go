// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.
package utils_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ImportTargetStore struct {
	mu                  sync.Mutex
	Id                  uint64
	LastSuccessDenyCall time.Time
	SuspendImportFor    time.Duration
	SuspendedImport     bool

	ErrGen func() error
}

type ImportTargetStores struct {
	mu    sync.Mutex
	items map[uint64]*ImportTargetStore
}

func initWithIDs(ids []int) *ImportTargetStores {
	ss := &ImportTargetStores{
		items: map[uint64]*ImportTargetStore{},
	}
	for _, id := range ids {
		store := new(ImportTargetStore)
		store.Id = uint64(id)
		ss.items[uint64(id)] = store
	}
	return ss
}

func (s *ImportTargetStores) GetAllStores(ctx context.Context) ([]*metapb.Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stores := make([]*metapb.Store, 0, len(s.items))
	for _, store := range s.items {
		stores = append(stores, &metapb.Store{Id: store.Id})
	}
	return stores, nil
}

func (s *ImportTargetStores) GetDenyLightningClient(ctx context.Context, storeID uint64) (utils.SuspendImportingClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	store, ok := s.items[storeID]
	if !ok {
		return nil, errors.Trace(fmt.Errorf("store %d not found", storeID))
	}

	return store, nil
}

// Temporarily disable ingest / download / write for data listeners don't support catching import data.
func (s *ImportTargetStore) SuspendImportRPC(ctx context.Context, in *import_sstpb.SuspendImportRPCRequest, opts ...grpc.CallOption) (*import_sstpb.SuspendImportRPCResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ErrGen != nil {
		if err := s.ErrGen(); err != nil {
			return nil, s.ErrGen()
		}
	}

	suspended := s.SuspendedImport
	if in.ShouldSuspendImports {
		s.SuspendedImport = true
		s.SuspendImportFor = time.Duration(in.DurationInSecs) * time.Second
		s.LastSuccessDenyCall = time.Now()
	} else {
		s.SuspendedImport = false
	}
	return &import_sstpb.SuspendImportRPCResponse{
		AlreadySuspended: suspended,
	}, nil
}

func (s *ImportTargetStores) assertAllStoresDenied(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, store := range s.items {
		func() {
			store.mu.Lock()
			defer store.mu.Unlock()

			require.True(t, store.SuspendedImport, "ID = %d", store.Id)
			require.Less(t, time.Since(store.LastSuccessDenyCall), store.SuspendImportFor, "ID = %d", store.Id)
		}()
	}
}

func TestBasic(t *testing.T) {
	req := require.New(t)

	ss := initWithIDs([]int{1, 4, 5})
	deny := utils.NewSuspendImporting(t.Name(), ss)

	ctx := context.Background()
	res, err := deny.DenyAllStores(ctx, 10*time.Second)
	req.NoError(err)
	req.Error(deny.ConsistentWithPrev(res))
	for id, inner := range ss.items {
		req.True(inner.SuspendedImport, "at %d", id)
		req.Equal(inner.SuspendImportFor, 10*time.Second, "at %d", id)
	}

	res, err = deny.DenyAllStores(ctx, 10*time.Second)
	req.NoError(err)
	req.NoError(deny.ConsistentWithPrev(res))

	res, err = deny.AllowAllStores(ctx)
	req.NoError(err)
	req.NoError(deny.ConsistentWithPrev(res))
}

func TestKeeperError(t *testing.T) {
	req := require.New(t)

	ctx := context.Background()
	ss := initWithIDs([]int{1, 4, 5})
	deny := utils.NewSuspendImporting(t.Name(), ss)
	ttl := time.Second

	now := time.Now()
	triggeredErr := uint32(0)
	_, err := deny.DenyAllStores(ctx, ttl)
	req.NoError(err)

	ss.items[4].ErrGen = func() error {
		if time.Since(now) > 600*time.Millisecond {
			return nil
		}
		triggeredErr += 1
		return status.Error(codes.Unavailable, "the store is slacking.")
	}

	cx, cancel := context.WithCancel(ctx)

	wg := new(errgroup.Group)
	wg.Go(func() error { return deny.Keeper(cx, ttl) })
	time.Sleep(ttl)
	cancel()
	req.ErrorIs(wg.Wait(), context.Canceled)
	req.Positive(triggeredErr)
}

func TestKeeperErrorExit(t *testing.T) {
	req := require.New(t)

	ctx := context.Background()
	ss := initWithIDs([]int{1, 4, 5})
	deny := utils.NewSuspendImporting(t.Name(), ss)
	ttl := time.Second

	triggeredErr := uint32(0)
	_, err := deny.DenyAllStores(ctx, ttl)
	req.NoError(err)

	ss.items[4].ErrGen = func() error {
		triggeredErr += 1
		return status.Error(codes.Unavailable, "the store is slacking.")
	}

	wg := new(errgroup.Group)
	wg.Go(func() error { return deny.Keeper(ctx, ttl) })
	time.Sleep(ttl)
	req.Error(wg.Wait())
	req.Positive(triggeredErr)
}

func TestKeeperCalled(t *testing.T) {
	req := require.New(t)

	ctx := context.Background()
	ss := initWithIDs([]int{1, 4, 5})
	deny := utils.NewSuspendImporting(t.Name(), ss)
	ttl := 1 * time.Second

	_, err := deny.DenyAllStores(ctx, ttl)
	req.NoError(err)

	cx, cancel := context.WithCancel(ctx)
	wg := new(errgroup.Group)
	wg.Go(func() error { return deny.Keeper(cx, ttl) })
	for i := 0; i < 20; i++ {
		ss.assertAllStoresDenied(t)
		time.Sleep(ttl / 10)
	}
	cancel()
	req.ErrorIs(wg.Wait(), context.Canceled)
}
