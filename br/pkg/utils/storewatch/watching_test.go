package storewatch_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/utils/storewatch"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type SequentialReturningStoreMeta struct {
	sequence [][]*metapb.Store
}

func NewSequentialReturningStoreMeta(sequence [][]*metapb.Store) util.StoreMeta {
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
