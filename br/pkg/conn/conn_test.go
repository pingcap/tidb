// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	pd "github.com/tikv/pd/client"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
	ctx    context.Context
	cancel context.CancelFunc

	mgr *Mgr
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mgr = &Mgr{PdController: &pdutil.PdController{}}
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.cancel()
}

type fakePDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (fpdc fakePDClient) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return append([]*metapb.Store{}, fpdc.stores...), nil
}

func (s *testClientSuite) TestGetAllTiKVStores(c *C) {
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
			expectedError: "cannot restore to a cluster with active TiFlash stores.*",
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
			expectedError: "cannot restore to a cluster with active TiFlash stores.*",
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
			c.Assert(err, ErrorMatches, testCase.expectedError)
			continue
		}
		foundStores := make(map[uint64]int)
		for _, store := range stores {
			foundStores[store.Id]++
		}
		c.Assert(foundStores, DeepEquals, testCase.expectedStores)
	}
}

func (s *testClientSuite) TestGetConnOnCanceledContext(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.mgr.GetBackupClient(ctx, 42)
	c.Assert(err, ErrorMatches, ".*context canceled.*")
	_, err = s.mgr.ResetBackupClient(ctx, 42)
	c.Assert(err, ErrorMatches, ".*context canceled.*")
}
