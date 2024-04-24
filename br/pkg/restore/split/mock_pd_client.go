// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockPDClientForSplit is a mock PD client for testing split and scatter.
type MockPDClientForSplit struct {
	pd.Client

	mu sync.Mutex

	Regions      *pdtypes.RegionTree
	lastRegionID uint64
	scanRegions  struct {
		errors     []error
		beforeHook func()
	}
	splitRegions struct {
		count    int
		hijacked func() (bool, *kvrpcpb.SplitRegionResponse, error)
	}
	scatterRegion struct {
		eachRegionFailBefore int
		count                map[uint64]int
	}
	scatterRegions struct {
		notImplemented bool
		regionCount    int
	}
	getOperator struct {
		responses map[uint64][]*pdpb.GetOperatorResponse
	}
}

// NewMockPDClientForSplit creates a new MockPDClientForSplit.
func NewMockPDClientForSplit() *MockPDClientForSplit {
	ret := &MockPDClientForSplit{}
	ret.Regions = &pdtypes.RegionTree{}
	ret.scatterRegion.count = make(map[uint64]int)
	return ret
}

func newRegionNotFullyReplicatedErr(regionID uint64) error {
	return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionID)
}

func (c *MockPDClientForSplit) SetRegions(boundaries [][]byte) []*metapb.Region {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.setRegions(boundaries)
}

func (c *MockPDClientForSplit) setRegions(boundaries [][]byte) []*metapb.Region {
	ret := make([]*metapb.Region, 0, len(boundaries)-1)
	for i := 1; i < len(boundaries); i++ {
		c.lastRegionID++
		r := &metapb.Region{
			Id:       c.lastRegionID,
			StartKey: boundaries[i-1],
			EndKey:   boundaries[i],
		}
		p := &metapb.Peer{
			Id:      c.lastRegionID,
			StoreId: 1,
		}
		c.Regions.SetRegion(&pdtypes.Region{
			Meta:   r,
			Leader: p,
		})
		ret = append(ret, r)
	}
	return ret
}

func (c *MockPDClientForSplit) ScanRegions(
	_ context.Context,
	key, endKey []byte,
	limit int,
	_ ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.scanRegions.errors) > 0 {
		err := c.scanRegions.errors[0]
		c.scanRegions.errors = c.scanRegions.errors[1:]
		return nil, err
	}

	if c.scanRegions.beforeHook != nil {
		c.scanRegions.beforeHook()
	}

	regions := c.Regions.ScanRange(key, endKey, limit)
	ret := make([]*pd.Region, 0, len(regions))
	for _, r := range regions {
		ret = append(ret, &pd.Region{
			Meta:   r.Meta,
			Leader: r.Leader,
		})
	}
	return ret, nil
}

func (c *MockPDClientForSplit) GetRegionByID(_ context.Context, regionID uint64, _ ...pd.GetRegionOption) (*pd.Region, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range c.Regions.Regions {
		if r.Meta.Id == regionID {
			return &pd.Region{
				Meta:   r.Meta,
				Leader: r.Leader,
			}, nil
		}
	}
	return nil, errors.New("region not found")
}

func (c *MockPDClientForSplit) SplitRegion(
	region *RegionInfo,
	keys [][]byte,
	isRawKV bool,
) (bool, *kvrpcpb.SplitRegionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.splitRegions.count++
	if c.splitRegions.hijacked != nil {
		return c.splitRegions.hijacked()
	}

	if !isRawKV {
		for i := range keys {
			keys[i] = codec.EncodeBytes(nil, keys[i])
		}
	}

	newRegionBoundaries := make([][]byte, 0, len(keys)+2)
	newRegionBoundaries = append(newRegionBoundaries, region.Region.StartKey)
	newRegionBoundaries = append(newRegionBoundaries, keys...)
	newRegionBoundaries = append(newRegionBoundaries, region.Region.EndKey)
	newRegions := c.setRegions(newRegionBoundaries)
	newRegions[0].Id = region.Region.Id
	return false, &kvrpcpb.SplitRegionResponse{Regions: newRegions}, nil
}

func (c *MockPDClientForSplit) ScatterRegion(_ context.Context, regionID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.scatterRegion.count[regionID]++
	if c.scatterRegion.count[regionID] > c.scatterRegion.eachRegionFailBefore {
		return nil
	}
	return newRegionNotFullyReplicatedErr(regionID)
}

func (c *MockPDClientForSplit) ScatterRegions(_ context.Context, regionIDs []uint64, _ ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.scatterRegions.notImplemented {
		return nil, status.Error(codes.Unimplemented, "Ah, yep")
	}
	c.scatterRegions.regionCount += len(regionIDs)
	return &pdpb.ScatterRegionResponse{}, nil
}

func (c *MockPDClientForSplit) GetOperator(_ context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.getOperator.responses == nil {
		return &pdpb.GetOperatorResponse{Desc: []byte("scatter-region"), Status: pdpb.OperatorStatus_SUCCESS}, nil
	}
	ret := c.getOperator.responses[regionID][0]
	c.getOperator.responses[regionID] = c.getOperator.responses[regionID][1:]
	return ret, nil
}
