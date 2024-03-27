// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockPDClientForSplit struct {
	pd.Client

	regions      *pdtypes.RegionTree
	lastRegionID uint64
	scanRegions  struct {
		errors     []error
		beforeHook func()
	}
	splitRegions struct {
		hijacked func() (bool, *kvrpcpb.SplitRegionResponse, error)
	}
	scatterRegion struct {
		eachRegionFailBefore int
		count                map[uint64]int
	}
	scatterRegions struct {
		notImplemented bool
	}
	getOperator struct {
		responses map[uint64][]*pdpb.GetOperatorResponse
	}
}

func newMockPDClientForSplit() *mockPDClientForSplit {
	ret := &mockPDClientForSplit{}
	ret.regions = &pdtypes.RegionTree{}
	ret.scatterRegion.count = make(map[uint64]int)
	return ret
}

func newRegionNotFullyReplicatedErr(regionID uint64) error {
	return status.Errorf(codes.Unknown, "region %d is not fully replicated", regionID)
}

func (c *mockPDClientForSplit) SetRegions(boundaries [][]byte) []*metapb.Region {
	ret := make([]*metapb.Region, 0, len(boundaries)-1)
	for i := 1; i < len(boundaries); i++ {
		c.lastRegionID++
		r := &metapb.Region{
			Id:       c.lastRegionID,
			StartKey: boundaries[i-1],
			EndKey:   boundaries[i],
		}
		c.regions.SetRegion(&pdtypes.Region{
			Meta: r,
		})
		ret = append(ret, r)
	}
	return ret
}

func (c *mockPDClientForSplit) ScanRegions(
	_ context.Context,
	key, endKey []byte,
	limit int,
	_ ...pd.GetRegionOption,
) ([]*pd.Region, error) {
	if len(c.scanRegions.errors) > 0 {
		err := c.scanRegions.errors[0]
		c.scanRegions.errors = c.scanRegions.errors[1:]
		return nil, err
	}

	if c.scanRegions.beforeHook != nil {
		c.scanRegions.beforeHook()
	}

	regions := c.regions.ScanRange(key, endKey, limit)
	ret := make([]*pd.Region, 0, len(regions))
	for _, r := range regions {
		ret = append(ret, &pd.Region{
			Meta:   r.Meta,
			Leader: r.Leader,
		})
	}
	return ret, nil
}

func (c *mockPDClientForSplit) GetRegionByID(_ context.Context, regionID uint64, _ ...pd.GetRegionOption) (*pd.Region, error) {
	for _, r := range c.regions.Regions {
		if r.Meta.Id == regionID {
			return &pd.Region{
				Meta:   r.Meta,
				Leader: r.Leader,
			}, nil
		}
	}
	return nil, errors.New("region not found")
}

func (c *mockPDClientForSplit) SplitRegion(region *RegionInfo, keys [][]byte) (bool, *kvrpcpb.SplitRegionResponse, error) {
	if c.splitRegions.hijacked != nil {
		return c.splitRegions.hijacked()
	}

	newRegionBoundaries := make([][]byte, 0, len(keys)+2)
	newRegionBoundaries = append(newRegionBoundaries, region.Region.StartKey)
	newRegionBoundaries = append(newRegionBoundaries, keys...)
	newRegionBoundaries = append(newRegionBoundaries, region.Region.EndKey)
	newRegions := c.SetRegions(newRegionBoundaries)
	return false, &kvrpcpb.SplitRegionResponse{Regions: newRegions}, nil
}

func (c *mockPDClientForSplit) ScatterRegion(_ context.Context, regionID uint64) error {
	c.scatterRegion.count[regionID]++
	if c.scatterRegion.count[regionID] > c.scatterRegion.eachRegionFailBefore {
		return nil
	}
	return newRegionNotFullyReplicatedErr(regionID)
}

func (c *mockPDClientForSplit) ScatterRegions(context.Context, []uint64, ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	if c.scatterRegions.notImplemented {
		return nil, status.Error(codes.Unimplemented, "Ah, yep")
	}
	return nil, nil
}

func (c *mockPDClientForSplit) GetOperator(_ context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	ret := c.getOperator.responses[regionID][0]
	c.getOperator.responses[regionID] = c.getOperator.responses[regionID][1:]
	return ret, nil
}
