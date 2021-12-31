// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (c fakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, c.stores...), nil
}

func TestGetAllTiKVStoresWithRetryCancel(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/conn/hint-GetAllTiKVStores-cancel", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/conn/hint-GetAllTiKVStores-cancel")
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    2,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
	}

	fpdc := fakePDClient{
		stores: stores,
	}

	_, err := GetAllTiKVStoresWithRetry(ctx, fpdc, SkipTiFlash)
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(errors.Cause(err)))
}

func TestGetAllTiKVStoresWithUnknown(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/conn/hint-GetAllTiKVStores-error", "return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/conn/hint-GetAllTiKVStores-error")
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    2,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
	}

	fpdc := fakePDClient{
		stores: stores,
	}

	_, err := GetAllTiKVStoresWithRetry(ctx, fpdc, SkipTiFlash)
	require.Error(t, err)
	require.Equal(t, codes.Unknown, status.Code(errors.Cause(err)))
}
func TestCheckStoresAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    2,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tikv",
				},
			},
		},
		{
			Id:    4,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tikv",
				},
			},
		},
	}

	fpdc := fakePDClient{
		stores: stores,
	}

	kvStores, err := GetAllTiKVStoresWithRetry(ctx, fpdc, SkipTiFlash)
	require.NoError(t, err)
	require.Len(t, kvStores, 2)
	require.Equal(t, stores[2:], kvStores)

	err = checkStoresAlive(ctx, fpdc, SkipTiFlash)
	require.NoError(t, err)
}

func TestGetAllTiKVStores(t *testing.T) {
	testCases := []struct {
		stores         []*metapb.Store
		storeBehavior  StoreBehavior
		expectedStores map[uint64]int
		expectedError  string
	}{
		{
			stores: []*metapb.Store{
				{Id: 1},
			},
			storeBehavior:  SkipTiFlash,
			expectedStores: map[uint64]int{1: 1},
		},
		{
			stores: []*metapb.Store{
				{Id: 1},
			},
			storeBehavior:  ErrorOnTiFlash,
			expectedStores: map[uint64]int{1: 1},
		},
		{
			stores: []*metapb.Store{
				{Id: 1},
				{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
			},
			storeBehavior:  SkipTiFlash,
			expectedStores: map[uint64]int{1: 1},
		},
		{
			stores: []*metapb.Store{
				{Id: 1},
				{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
			},
			storeBehavior: ErrorOnTiFlash,
			expectedError: "^cannot restore to a cluster with active TiFlash stores",
		},
		{
			stores: []*metapb.Store{
				{Id: 1},
				{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
				{Id: 3},
				{Id: 4, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tikv"}}},
				{Id: 5, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tikv"}, {Key: "engine", Value: "tiflash"}}},
				{Id: 6, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tiflash"}, {Key: "engine", Value: "tikv"}}},
			},
			storeBehavior:  SkipTiFlash,
			expectedStores: map[uint64]int{1: 1, 3: 1, 4: 1, 6: 1},
		},
		{
			stores: []*metapb.Store{
				{Id: 1},
				{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
				{Id: 3},
				{Id: 4, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tikv"}}},
				{Id: 5, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tikv"}, {Key: "engine", Value: "tiflash"}}},
				{Id: 6, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tiflash"}, {Key: "engine", Value: "tikv"}}},
			},
			storeBehavior: ErrorOnTiFlash,
			expectedError: "^cannot restore to a cluster with active TiFlash stores",
		},
		{
			stores: []*metapb.Store{
				{Id: 1},
				{Id: 2, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
				{Id: 3},
				{Id: 4, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tikv"}}},
				{Id: 5, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tikv"}, {Key: "engine", Value: "tiflash"}}},
				{Id: 6, Labels: []*metapb.StoreLabel{{Key: "else", Value: "tiflash"}, {Key: "engine", Value: "tikv"}}},
			},
			storeBehavior:  TiFlashOnly,
			expectedStores: map[uint64]int{2: 1, 5: 1},
		},
	}

	for _, testCase := range testCases {
		pdClient := fakePDClient{stores: testCase.stores}
		stores, err := GetAllTiKVStores(context.Background(), pdClient, testCase.storeBehavior)
		if len(testCase.expectedError) != 0 {
			require.Error(t, err)
			require.Regexp(t, testCase.expectedError, err.Error())
			continue
		}
		foundStores := make(map[uint64]int)
		for _, store := range stores {
			foundStores[store.Id]++
		}
		require.Equal(t, testCase.expectedStores, foundStores)
	}
}

func TestGetConnOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mgr := &Mgr{PdController: &pdutil.PdController{}}

	_, err := mgr.GetBackupClient(ctx, 42)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")

	_, err = mgr.ResetBackupClient(ctx, 42)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
}
