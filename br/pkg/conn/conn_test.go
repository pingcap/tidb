// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	fpdc := utils.FakePDClient{
		Stores: stores,
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

	fpdc := utils.FakePDClient{
		Stores: stores,
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

	fpdc := utils.FakePDClient{
		Stores: stores,
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
		pdClient := utils.FakePDClient{Stores: testCase.stores}
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

func TestGetMergeRegionSizeAndCount(t *testing.T) {
	cases := []struct {
		stores          []*metapb.Store
		content         []string
		regionSplitSize uint64
		regionSplitKeys uint64
	}{
		{
			stores: []*metapb.Store{
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
			},
			content: []string{""},
			// no tikv detected in this case
			regionSplitSize: DefaultMergeRegionSizeBytes,
			regionSplitKeys: DefaultMergeRegionKeyCount,
		},
		{
			stores: []*metapb.Store{
				{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
			},
			content: []string{
				"{\"log-level\": \"debug\", \"coprocessor\": {\"region-split-keys\": 1, \"region-split-size\": \"1MiB\"}}",
			},
			// one tikv detected in this case we are not update default size and keys because they are too small.
			regionSplitSize: 1 * units.MiB,
			regionSplitKeys: 1,
		},
		{
			stores: []*metapb.Store{
				{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
			},
			content: []string{
				"{\"log-level\": \"debug\", \"coprocessor\": {\"region-split-keys\": 10000000, \"region-split-size\": \"1GiB\"}}",
			},
			// one tikv detected in this case and we update with new size and keys.
			regionSplitSize: 1 * units.GiB,
			regionSplitKeys: 10000000,
		},
		{
			stores: []*metapb.Store{
				{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
				{
					Id:    2,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
			},
			content: []string{
				"{\"log-level\": \"debug\", \"coprocessor\": {\"region-split-keys\": 10000000, \"region-split-size\": \"1GiB\"}}",
				"{\"log-level\": \"debug\", \"coprocessor\": {\"region-split-keys\": 12000000, \"region-split-size\": \"900MiB\"}}",
			},
			// two tikv detected in this case and we choose the small one.
			regionSplitSize: 900 * units.MiB,
			regionSplitKeys: 12000000,
		},
	}

	ctx := context.Background()
	for _, ca := range cases {
		pdCli := utils.FakePDClient{Stores: ca.stores}
		require.Equal(t, len(ca.content), len(ca.stores))
		count := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch strings.TrimSpace(r.URL.Path) {
			case "/config":
				_, _ = fmt.Fprint(w, ca.content[count])
			default:
				http.NotFoundHandler().ServeHTTP(w, r)
			}
			count++
		}))

		for _, s := range ca.stores {
			s.Address = mockServer.URL
			s.StatusAddress = mockServer.URL
		}

		httpCli := mockServer.Client()
		mgr := &Mgr{PdController: &pdutil.PdController{}}
		mgr.PdController.SetPDClient(pdCli)
		rs, rk, err := mgr.GetMergeRegionSizeAndCount(ctx, httpCli)
		require.NoError(t, err)
		require.Equal(t, ca.regionSplitSize, rs)
		require.Equal(t, ca.regionSplitKeys, rk)
		mockServer.Close()
	}
}

func TestHandleTiKVAddress(t *testing.T) {
	cases := []struct {
		store      *metapb.Store
		httpPrefix string
		result     string
	}{
		{
			store: &metapb.Store{
				Id:            1,
				State:         metapb.StoreState_Up,
				Address:       "127.0.0.1:20160",
				StatusAddress: "127.0.0.1:20180",
			},
			httpPrefix: "http://",
			result:     "http://127.0.0.1:20180",
		},
		{
			store: &metapb.Store{
				Id:            1,
				State:         metapb.StoreState_Up,
				Address:       "192.168.1.5:20160",
				StatusAddress: "0.0.0.0:20180",
			},
			httpPrefix: "https://",
			// if status address and node address not match, we use node address as default host name.
			result: "https://192.168.1.5:20180",
		},
	}
	for _, ca := range cases {
		addr, err := handleTiKVAddress(ca.store, ca.httpPrefix)
		require.Nil(t, err)
		require.Equal(t, ca.result, addr.String())
	}
}
