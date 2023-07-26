package storewatch_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/utils/storewatch"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type SequentialReturningStoreMeta struct {
	sequence [][]*metapb.Store
}

func NewSequentialReturningStoreMeta(sequence [][]*metapb.Store) storewatch.StoreMeta {
	return &SequentialReturningStoreMeta{sequence: sequence}
}

func (s *SequentialReturningStoreMeta) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if len(s.sequence) == 0 {
		return nil, fmt.Errorf("too many call to `GetAllStores` in test")
	}
	stores := s.sequence[0]
	s.sequence = s.sequence[1:]
	return stores, nil
}

func TestOnRegister(t *testing.T) {
	// A sequence of store state that we should believe the store is offline.
	seq := NewSequentialReturningStoreMeta([][]*metapb.Store{
		{
			{
				Id:    1,
				State: metapb.StoreState_Up,
			},
		},
	})
	callBackCalled := false
	callback := storewatch.MakeCallback(storewatch.WithOnNewStoreRegistered(func(s *metapb.Store) { callBackCalled = true }))
	ctx := context.Background()

	watcher := storewatch.New(seq, callback)
	require.NoError(t, watcher.Step(ctx))
	require.True(t, callBackCalled)
}

func TestOnOffline(t *testing.T) {
	// A sequence of store state that we should believe the store is offline.
	seq := NewSequentialReturningStoreMeta([][]*metapb.Store{
		{
			{
				Id:    1,
				State: metapb.StoreState_Up,
			},
		},
		{
			{
				Id:    1,
				State: metapb.StoreState_Offline,
			},
		},
	})
	callBackCalled := false
	callback := storewatch.MakeCallback(storewatch.WithOnDisconnect(func(s *metapb.Store) { callBackCalled = true }))
	ctx := context.Background()

	watcher := storewatch.New(seq, callback)
	require.NoError(t, watcher.Step(ctx))
	require.NoError(t, watcher.Step(ctx))
	require.True(t, callBackCalled)
}

func TestOnReboot(t *testing.T) {
	// A sequence of store state that we should believe the store is offline.
	seq := NewSequentialReturningStoreMeta([][]*metapb.Store{
		{
			{
				Id:             1,
				State:          metapb.StoreState_Up,
				StartTimestamp: 1,
			},
		},
		{
			{
				Id:             1,
				State:          metapb.StoreState_Offline,
				StartTimestamp: 1,
			},
		},
		{
			{
				Id:             1,
				State:          metapb.StoreState_Up,
				StartTimestamp: 2,
			},
		},
	})
	callBackCalled := false
	callback := storewatch.MakeCallback(storewatch.WithOnReboot(func(s *metapb.Store) { callBackCalled = true }))
	ctx := context.Background()

	watcher := storewatch.New(seq, callback)
	require.NoError(t, watcher.Step(ctx))
	require.NoError(t, watcher.Step(ctx))
	require.NoError(t, watcher.Step(ctx))
	require.True(t, callBackCalled)
}

func TestOnRealStore(t *testing.T) {
	t.SkipNow()

	req := require.New(t)
	pdAddr := []string{"http://upd-1:2379"}
	cli, err := pd.NewClient(pdAddr, pd.SecurityOption{})
	req.NoError(err)
	cb := storewatch.MakeCallback(
		storewatch.WithOnDisconnect(func(s *metapb.Store) {
			fmt.Printf("Store %d at %s Disconnected\n", s.Id, s.Address)
		}),
		storewatch.WithOnNewStoreRegistered(func(s *metapb.Store) {
			fmt.Printf("Store %d at %s Registered (ts = %d)\n", s.Id, s.Address, s.GetStartTimestamp())
		}),
		storewatch.WithOnReboot(func(s *metapb.Store) {
			fmt.Printf("Store %d at %s Rebooted (ts = %d)\n", s.Id, s.Address, s.StartTimestamp)
		}),
	)

	watcher := storewatch.New(cli, cb)
	for {
		req.NoError(watcher.Step(context.Background()))
		time.Sleep(5 * time.Second)
	}
}
